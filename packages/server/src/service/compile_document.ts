// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   MalloyError,
   type LogMessage,
   type ModelDef,
   type ModelMaterializer,
} from "@malloydata/malloy";
import {
   AccessDeniedError,
   NotQueryableError,
   UnparseableTextError,
} from "../errors";
import {
   buildDashboardManifest,
   compileTileGivens,
   normalizeTileExpression,
   readDashboardModelFacts,
   readDocumentTileExpressions,
   type DashboardManifest,
} from "./dashboard";
import { gateGivenSource } from "./given";
import {
   buildDerivationBaseMap,
   scanIdentifiers,
   stripMalloyCommentsAndLiterals,
} from "./query_text";
import { ownModelNoteObjects } from "./annotations";
import {
   artifactKindOfNotes,
   cellOffsets,
   claimsToBeANotebook,
   documentKind,
   isNotebookReaderError,
   parseNotebookText,
   readNotebookCells,
   type NotebookCellSpan,
} from "./notebook";
import { extractSourcesFromModelDef } from "./source_extraction";

/**
 * Compile of a document: caller-submitted text that carries a model-level
 * `## artifact` tag (a dashboard or a notebook), checked as a fragment on top
 * of its base model and answered with the document it describes.
 *
 * The cell reader and the manifest builder are the ones that serve a saved
 * document, so a document read here is the document the same text would be
 * once saved.
 *
 * AUTHORIZATION IS PER CELL AND PER TILE. A cell or tile whose source the caller
 * may not read is never handed to the compiler: its characters are blanked out of
 * the text that compiles, and it comes back `restricted` with no diagnostic.
 * Dropping the diagnostics of a cell the compiler did see would still leave the
 * rest of the answer (a "no such column" on a neighbor that reads a derived
 * source) to say what the gated source holds; not compiling it leaves nothing
 * to say. Restriction follows names: a name a restricted definition declares
 * taints every later cell and tile that mentions it.
 */

/** The decisions only the owning environment can make, passed in so this module stays a pure reader. */
export interface DocumentGates {
   /** Throws AccessDeniedError when the caller may not read what `text` names; NotQueryableError when that source is hidden. */
   text(text: string): Promise<void>;
   /** The compiled backstop: throws AccessDeniedError when the query actually reads a source the caller may not. */
   compiled(runnable: { getPreparedQuery(): Promise<unknown> }): Promise<void>;
   /** Throws NotQueryableError when `query` targets a source off the package's query surface. */
   boundary(query: string, definitions: string): void;
   /** The same check on the source the compiled query reads, which sees through a derivation the text hides. */
   boundaryCompiled(
      compiledSource: string | undefined,
      query: string,
      definitions: string,
   ): void;
   /** Throws CompileRefusedError when `text` uses a construct append-scope text may not (a data root of its own). */
   constructs(text: string): Promise<void>;
}

export interface DocumentCell {
   type: "markdown" | "code";
   kind: "markdown" | "query" | "definition";
   text: string;
   markdown?: string;
   proseLines?: [number, number][];
   codeLine?: number;
   caption?: string;
   restricted?: boolean;
}

export interface CompiledDocument {
   kind: DashboardManifest["kind"];
   manifest?: Omit<DashboardManifest, "entryFile"> & { path: string };
   cells: DocumentCell[];
}

export interface DocumentCompileResult {
   problems: LogMessage[];
   document?: CompiledDocument;
}

const DECLARATION =
   /\b(?:source|query)\s*:\s*(`(?:[^`\\]|\\.)*`|[\p{L}_][\p{L}\p{N}_]*)\s+is\b/giu;

/** Blank the given character ranges, keeping every newline so no line or column moves. */
export function blankSpans(
   text: string,
   cells: readonly NotebookCellSpan[],
): string {
   if (cells.length === 0) return text;
   // Offsets are UTF-16 units, so blank over the unit array.
   const units = text.split("");
   for (const cell of cells) {
      const span = cellOffsets.get(cell);
      // A cell with no recorded span cannot be blanked, and compiling it would run what the caller may not read.
      if (!span) throw new Error("restricted cell has no recorded span");
      const [start, end] = span;
      for (let i = start; i < end && i < units.length; i++) {
         if (units[i] !== "\n" && units[i] !== "\r") units[i] = " ";
      }
   }
   return units.join("");
}

/** The names a definition statement declares itself: `source: a is …` or `query: q is …`, never a field inside it. */
function declaredNames(text: string): Set<string> {
   const found = new Set<string>(buildDerivationBaseMap(text).keys());
   for (const match of stripMalloyCommentsAndLiterals(text).matchAll(
      DECLARATION,
   )) {
      const name = match[1];
      found.add(name.startsWith("`") ? name.slice(1, -1) : name);
   }
   return found;
}

/** The structRef name of a prepared query, the source Malloy actually reads. */
function readsSource(prepared: unknown): string | undefined {
   const ref = (prepared as { _query?: { structRef?: unknown } })._query
      ?.structRef;
   if (typeof ref === "string") return ref;
   const named = ref as { as?: string; name?: string } | undefined;
   return named?.as || named?.name;
}

async function denied(gates: DocumentGates, text: string): Promise<boolean> {
   try {
      await gates.text(text);
      return false;
   } catch (error) {
      if (error instanceof AccessDeniedError) return true;
      throw error;
   }
}

function problemLine(problem: LogMessage): number | undefined {
   const line = (problem as { at?: { range?: { start?: { line?: number } } } })
      .at?.range?.start?.line;
   return line === undefined ? undefined : line + 1;
}

/**
 * Compile `source` as a document, or return undefined when it is not one (no
 * model-level artifact tag, or text the cell reader cannot read), in which case
 * the ordinary compile answers.
 */
export async function compileDocument(input: {
   base: ModelMaterializer;
   source: string;
   modelName: string;
   gates: DocumentGates;
}): Promise<DocumentCompileResult | undefined> {
   const { base, source, modelName, gates } = input;
   if (!claimsToBeANotebook(source)) return undefined;
   const parse = parseNotebookText(source);
   if (isNotebookReaderError(parse)) return undefined;
   const read = readNotebookCells(parse, undefined, source);
   if (read.error) {
      const line = read.error.line - 1;
      return {
         problems: [
            {
               code: "notebook-cells-unreadable",
               severity: "error",
               message: read.error.message,
               at: {
                  url: "",
                  range: {
                     start: { line, character: 0 },
                     end: { line, character: 0 },
                  },
               },
            } as LogMessage,
         ],
      };
   }

   // Phase 1: decide every unit by its text, in file order so a restricted
   // definition taints what comes after it.
   const code = read.cells.filter((cell) => cell.type === "code");
   const restrictedCells = new Set<NotebookCellSpan>();
   const tainted = new Set<string>();
   const mentionsTainted = (text: string) =>
      tainted.size > 0 &&
      [...scanIdentifiers(text)].some((name) => tainted.has(name));
   for (const cell of code) {
      if (mentionsTainted(cell.text) || (await denied(gates, cell.text))) {
         restrictedCells.add(cell);
         if (cell.kind === "definition") {
            for (const name of declaredNames(cell.text)) tainted.add(name);
         }
      }
   }
   const tileExpressions = readDocumentTileExpressions(read.annotations);
   const restrictedTiles = new Set<string>();
   for (const expression of tileExpressions) {
      const text = `run: ${expression}`;
      if (mentionsTainted(text) || (await denied(gates, text))) {
         restrictedTiles.add(normalizeTileExpression(expression));
      }
   }

   // The query surface is checked against the cells the caller may read, so a
   // document that compiles is one that runs and publishes on a curated package.
   const definitions = code
      .filter(
         (cell) => cell.kind === "definition" && !restrictedCells.has(cell),
      )
      .map((cell) => cell.text)
      .join("\n");
   // A tile expression is a string in the tag, so the construct gate that read
   // the submitted text as Malloy never saw it. Each readable tile is judged
   // here, after the definitions it may name, before anything compiles it.
   const unparsedTiles = new Set<string>();
   const tileProblems: LogMessage[] = [];
   for (const expression of tileExpressions) {
      const key = normalizeTileExpression(expression);
      if (restrictedTiles.has(key)) continue;
      try {
         await gates.constructs(`${definitions}\nrun: ${expression}`);
      } catch (error) {
         if (!(error instanceof UnparseableTextError)) throw error;
         // Nothing compiles it, so it is one tile that fails, not a refusal of the document.
         unparsedTiles.add(key);
         tileProblems.push({
            code: "tile-does-not-compile",
            severity: "error",
            message: `Tile "${expression}" could not be parsed.`,
         } as LogMessage);
      }
   }

   const boundaryProblems: LogMessage[] = [];
   const checkBoundary = (query: string, line: number | undefined) => {
      try {
         gates.boundary(query, definitions);
      } catch (error) {
         if (!(error instanceof NotQueryableError)) throw error;
         boundaryProblems.push({
            code: "query-not-queryable",
            severity: "error",
            message: error.message,
            ...(line !== undefined && {
               at: {
                  url: "",
                  range: {
                     start: { line: line - 1, character: 0 },
                     end: { line: line - 1, character: 0 },
                  },
               },
            }),
         } as LogMessage);
      }
   };
   for (const cell of code) {
      if (cell.kind === "query" && !restrictedCells.has(cell)) {
         checkBoundary(cell.text, cell.startLine);
      }
   }
   for (const expression of tileExpressions) {
      const key = normalizeTileExpression(expression);
      if (!restrictedTiles.has(key) && !unparsedTiles.has(key)) {
         checkBoundary(`run: ${expression}`, undefined);
      }
   }
   if (boundaryProblems.length > 0) return { problems: boundaryProblems };

   // Phase 2: compile what the caller may read, as an extension of the base so
   // the base's own `run:` statements and `##` notes never join the document.
   const compiledText = blankSpans(source, [...restrictedCells]);
   const extended = base.extendModel(compiledText);
   let model: Awaited<ReturnType<typeof extended.getModel>>;
   try {
      model = await extended.getModel();
   } catch (error) {
      if (error instanceof MalloyError)
         return { problems: [...tileProblems, ...error.problems] };
      throw error;
   }
   const modelDef: ModelDef = model._modelDef;
   const registry = modelDef.givens ?? {};
   const sources = extractSourcesFromModelDef(modelDef, []).sources;

   // Phase 3: the compiled backstop, for a name the text could not show.
   const settle = async (
      runnable: { getPreparedQuery(): Promise<unknown> },
      onDenied: () => void,
   ) => {
      try {
         await gates.compiled(runnable);
      } catch (error) {
         if (error instanceof AccessDeniedError) onDenied();
         else throw error;
      }
   };
   const compiledBoundary = (prepared: unknown, query: string) => {
      try {
         gates.boundaryCompiled(readsSource(prepared), query, definitions);
      } catch (error) {
         if (!(error instanceof NotQueryableError)) throw error;
         boundaryProblems.push({
            code: "query-not-queryable",
            severity: "error",
            message: error.message,
         } as LogMessage);
      }
   };
   for (const cell of code) {
      if (cell.kind !== "query" || restrictedCells.has(cell)) continue;
      // Restricted mode, so a construct the text gate could not see still cannot compile.
      const runnable = extended.loadRestrictedQuery(cell.text);
      let denied = false;
      await settle(runnable, () => {
         denied = true;
         restrictedCells.add(cell);
      });
      if (denied) continue;
      try {
         compiledBoundary(await runnable.getPreparedQuery(), cell.text);
      } catch (error) {
         if (!(error instanceof MalloyError)) throw error;
      }
   }
   const compiledTiles = await compileTileGivens(
      tileExpressions,
      { loadQuery: (text) => extended.loadRestrictedQuery(text) },
      registry,
      (sourceName) => gateGivenSource(sources, sourceName),
      new Set([...restrictedTiles, ...unparsedTiles]),
      async (tile, prepared) => {
         let denied = false;
         await settle({ getPreparedQuery: async () => prepared }, () => {
            denied = true;
            restrictedTiles.add(normalizeTileExpression(tile));
         });
         if (!denied) compiledBoundary(prepared, `run: ${tile}`);
      },
   );
   if (boundaryProblems.length > 0) return { problems: boundaryProblems };

   const spans = [...restrictedCells];
   const problems = [...tileProblems, ...model.problems].filter((problem) => {
      const line = problemLine(problem);
      return (
         line === undefined ||
         !spans.some((cell) => line >= cell.startLine && line <= cell.endLine)
      );
   });

   const slug = modelName.replace(/^.*\//, "").replace(/\.malloy$/, "");
   const facts = readDashboardModelFacts(
      `notebooks/${slug}.malloy`,
      modelDef,
      Object.values(registry).map((given) => given.name),
      new Map(
         sources.flatMap((entry) =>
            entry.name
               ? [
                    [
                       entry.name,
                       gateGivenSource(sources, entry.name) ?? [],
                    ] as const,
                 ]
               : [],
         ),
      ),
   );
   const manifest = buildDashboardManifest(
      { ...facts, compiledTileGivens: compiledTiles },
      { restrictedTiles },
   );
   const hasTiles = manifest?.tiles !== undefined;
   const cells = read.cells
      .filter((cell) => !hasTiles || cell.kind === "definition")
      .map((cell): DocumentCell => {
         const restricted = restrictedCells.has(cell);
         if (cell.kind === "markdown") {
            return { type: "markdown", kind: "markdown", text: cell.text };
         }
         return {
            type: "code",
            kind: cell.kind,
            text: cell.text,
            ...(cell.markdown !== undefined && { markdown: cell.markdown }),
            ...(cell.proseLines && { proseLines: cell.proseLines }),
            ...(cell.codeLine !== undefined && { codeLine: cell.codeLine }),
            ...(cell.caption !== undefined && { caption: cell.caption }),
            ...(restricted && { restricted: true }),
         };
      });
   if (!manifest) {
      // Written as cells: no tile layout, so the file's own cells are the document.
      const kind = documentKind(
         `notebooks/${slug}.malloy`,
         artifactKindOfNotes(ownModelNoteObjects(modelDef)),
      );
      return { problems, document: { kind, cells } };
   }
   const { entryFile: _entry, ...rest } = manifest;
   return {
      problems,
      document: {
         kind: manifest.kind,
         manifest: { ...rest, name: slug, path: modelName },
         cells,
      },
   };
}
