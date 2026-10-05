// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   MalloyError,
   type LogMessage,
   type ModelDef,
   type ModelMaterializer,
} from "@malloydata/malloy";
import { AccessDeniedError, NotQueryableError } from "../errors";
import {
   buildDashboardManifest,
   compileTileGivens,
   normalizeTileExpression,
   readDashboardModelFacts,
   readDocumentTileExpressions,
   type DashboardManifest,
} from "./dashboard";
import { gateGivenSource } from "./given";
import { ownModelNoteObjects } from "./annotations";
import {
   artifactKindOfNotes,
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
 * may not read is never handed to the compiler: its lines are blanked out of
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

const IDENTIFIER = /[A-Za-z_][A-Za-z0-9_]*/g;
const DECLARED_NAME = /\b([A-Za-z_][A-Za-z0-9_]*)\s+is\b/g;

/** Blank the given 1-based inclusive line ranges, keeping every other line's number. */
function blankLines(
   text: string,
   ranges: readonly { startLine: number; endLine: number }[],
): string {
   if (ranges.length === 0) return text;
   const lines = text.split("\n");
   for (const { startLine, endLine } of ranges) {
      for (let i = startLine - 1; i < endLine && i < lines.length; i++) {
         lines[i] = "";
      }
   }
   return lines.join("\n");
}

function names(text: string, pattern: RegExp): Set<string> {
   const found = new Set<string>();
   for (const match of text.matchAll(pattern)) {
      found.add(match[1] ?? match[0]);
   }
   return found;
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
      [...names(text, IDENTIFIER)].some((name) => tainted.has(name));
   for (const cell of code) {
      if (mentionsTainted(cell.text) || (await denied(gates, cell.text))) {
         restrictedCells.add(cell);
         if (cell.kind === "definition") {
            for (const name of names(cell.text, DECLARED_NAME)) {
               tainted.add(name);
            }
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
      if (!restrictedTiles.has(normalizeTileExpression(expression))) {
         checkBoundary(`run: ${expression}`, undefined);
      }
   }
   if (boundaryProblems.length > 0) return { problems: boundaryProblems };

   // Phase 2: compile what the caller may read, as an extension of the base so
   // the base's own `run:` statements and `##` notes never join the document.
   const compiledText = blankLines(source, [...restrictedCells]);
   const extended = base.extendModel(compiledText);
   let model: Awaited<ReturnType<typeof extended.getModel>>;
   try {
      model = await extended.getModel();
   } catch (error) {
      if (error instanceof MalloyError) return { problems: error.problems };
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
   for (const cell of code) {
      if (cell.kind !== "query" || restrictedCells.has(cell)) continue;
      await settle(extended.loadQuery(cell.text), () =>
         restrictedCells.add(cell),
      );
   }
   const compiledTiles = await compileTileGivens(
      tileExpressions,
      extended,
      registry,
      (sourceName) => gateGivenSource(sources, sourceName),
      restrictedTiles,
      async (tile, prepared) => {
         await settle({ getPreparedQuery: async () => prepared }, () =>
            restrictedTiles.add(normalizeTileExpression(tile)),
         );
      },
   );

   const spans = [...restrictedCells];
   const problems = model.problems.filter((problem) => {
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
