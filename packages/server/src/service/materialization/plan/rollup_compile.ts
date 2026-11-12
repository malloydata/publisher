// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Compiles the generated `#(preaggregate)` build companion (see rollup_plan.ts),
 * as materialize_compile.ts compiles the `#(materialize)` one: the text is
 * served from an overlay at a URL beside the declaring model, so its `import`
 * resolves while it never shows up in `getModelPaths()`, the API or the
 * package's own compile.
 *
 * **Built over the author's rows, never over a stored base (#6293).** The
 * companion reads the author's files with every source-level `#@` line blanked
 * (positions kept), so Malloy's planner sees no persisted source under a
 * rollup: a rollup never chains onto its base's table. That is what lets a
 * rollup over a `#@ persist storage=` base build at all (the chained storage
 * build lifts a downstream's definition from its file, and a rollup has
 * none), and it reads the same rows the live query reads. The content address
 * is unchanged by the blanking: Malloy keys a target on its fully inlined SQL.
 */

import type {
   Model as MalloyModel,
   ModelMaterializer,
   Runtime,
} from "@malloydata/malloy";
import { readFileSync } from "fs";
import * as path from "path";
import { fileURLToPath, pathToFileURL } from "url";
import { logger } from "../../../logger";
import { routeOfNote } from "../../annotation_reader";
import { overlayKey } from "../../overlay_key";
import type { SynthesizedRuntimeFactory } from "./preaggregation_compile";
import {
   generateRollupBuildFile,
   planModelRollups,
   ROLLUP_COMPANION_SUFFIX,
   type RollupRefusal,
   type RollupVariantPlan,
} from "./rollup_plan";
import { SourceMap } from "./source_map";

export interface GeneratedRollups {
   /** The rollups the companion declares. Never empty. */
   plans: RollupVariantPlan[];
   model: MalloyModel;
   materializer: ModelMaterializer;
   runtime: Runtime;
   text: string;
}

/** Where the rollup companion for `modelPath` lives, as an absolute URL. */
export function rollupCompanionURL(
   packagePath: string,
   modelPath: string,
): URL {
   return pathToFileURL(
      `${path.join(packagePath, modelPath)}${ROLLUP_COMPANION_SUFFIX}`,
   );
}

/**
 * The companion URL of the file a `sourceID` was declared in, or undefined
 * for a declaration that is not a local file.
 */
export function rollupCompanionURLOf(declaringURL: string): URL | undefined {
   try {
      return pathToFileURL(
         `${fileURLToPath(declaringURL)}${ROLLUP_COMPANION_SUFFIX}`,
      );
   } catch {
      return undefined;
   }
}

/**
 * A `sourceID` with its file made relative to the package
 * (`orders@models/orders.malloy`), so a rollup's name does not depend on
 * where the package is checked out.
 */
export function packageRelativeSourceID(
   packagePath: string,
   sourceID: string,
): string {
   const at = /^(.+)@([a-z][a-z0-9+.-]*:.*)$/i.exec(sourceID);
   if (!at) return sourceID;
   try {
      const file = fileURLToPath(at[2] as string);
      const relative = path
         .relative(path.resolve(packagePath), file)
         .split(path.sep)
         .join("/");
      return `${at[1]}@${relative}`;
   } catch {
      return sourceID;
   }
}

/** A zero-based range, as a note's `at.range` carries it. */
interface NoteRange {
   start: { line: number; character: number };
   end: { line: number; character: number };
}

/** Every source-level note on an annotation chain, inherited levels included. */
function sourceNotes(
   annotations: unknown,
   depth = 0,
): { text: string; at?: { url: string; range: NoteRange } }[] {
   if (!annotations || typeof annotations !== "object" || depth > 64) return [];
   const a = annotations as {
      notes?: { text: string; at?: { url: string; range: NoteRange } }[];
      blockNotes?: { text: string; at?: { url: string; range: NoteRange } }[];
      inherits?: unknown;
   };
   return [
      ...(a.blockNotes ?? []),
      ...(a.notes ?? []),
      ...sourceNotes(a.inherits, depth + 1),
   ];
}

/**
 * `text` with each range replaced by spaces, line breaks kept, or undefined
 * when a range does not hold the note the compiler says it does (a file that
 * changed since it compiled, or a range read differently): nothing is blanked
 * by guess.
 */
function blankRanges(
   text: string,
   notes: readonly { text: string; range: NoteRange }[],
): string | undefined {
   const lines = text.split("\n");
   for (const { text: note, range: r } of notes) {
      // A note is one line: `#@ persist …` up to the end of its line.
      if (r.start.line !== r.end.line) return undefined;
      const current = lines[r.start.line];
      if (current === undefined) return undefined;
      const from = r.start.character;
      const to = Math.min(r.end.character, current.length);
      const held = current.slice(from, to);
      if (
         from > current.length ||
         !held.startsWith("#@") ||
         held.trimEnd() !== note.trimEnd()
      ) {
         return undefined;
      }
      lines[r.start.line] =
         current.slice(0, from) + " ".repeat(to - from) + current.slice(to);
   }
   return lines.join("\n");
}

/**
 * The author's files with every source-level `#@` note blanked, keyed by URL,
 * for each file of `modelDef` that carries one: empty when none does, which
 * is the common case. The spans are the compiler's own (each note's
 * `at.range`), not a text search, and each is checked to hold its note before
 * it is blanked. Undefined when a file cannot be read or a span does not
 * hold its note: the caller builds nothing rather than build over a stored
 * base it meant to leave out.
 */
export function persistBlankedFiles(
   modelDef: unknown,
   readText: (url: string) => string | undefined = readLocalFile,
): Map<string, string> | undefined {
   const byFile = new Map<string, { text: string; range: NoteRange }[]>();
   const seen = new Set<string>();
   for (const node of SourceMap.of(modelDef).nodes()) {
      const annotations = (node.def as { annotations?: unknown }).annotations;
      for (const note of sourceNotes(annotations)) {
         if (routeOfNote(note.text) !== "@") continue;
         if (!note.at) return undefined;
         const r = note.at.range;
         const key = `${note.at.url}#${r.start.line}:${r.start.character}`;
         if (seen.has(key)) continue;
         seen.add(key);
         const notes = byFile.get(note.at.url) ?? [];
         notes.push({ text: note.text, range: r });
         byFile.set(note.at.url, notes);
      }
   }
   const out = new Map<string, string>();
   for (const [url, notes] of byFile) {
      const text = readText(url);
      const blanked = text === undefined ? undefined : blankRanges(text, notes);
      if (blanked === undefined) return undefined;
      out.set(overlayKey(url), blanked);
   }
   return out;
}

function readLocalFile(url: string): string | undefined {
   try {
      return readFileSync(fileURLToPath(url), "utf8");
   } catch {
      return undefined;
   }
}

/**
 * Plan, generate and compile the rollup companion for one author model: the
 * rollups of every source the model DECLARES. `generated` is undefined when it
 * declares none, or when the companion does not compile (a publisher bug: the
 * text is generated, so it is logged at `error` and costs that model its
 * rollups and nothing else). `refused` holds the declarations not planned.
 */
export async function tryCompileRollupCompanion(args: {
   packagePath: string;
   modelPath: string;
   modelDef: unknown;
   getRuntime: SynthesizedRuntimeFactory;
}): Promise<{ generated?: GeneratedRollups; refused: RollupRefusal[] }> {
   const modelFile = path.resolve(args.packagePath, args.modelPath);
   let refused: RollupRefusal[] = [];
   try {
      const planned = planModelRollups(
         SourceMap.of(args.modelDef),
         {
            packageRelativeSourceID: (id) =>
               packageRelativeSourceID(args.packagePath, id),
         },
         (declaringURL) => {
            try {
               return path.resolve(fileURLToPath(declaringURL)) === modelFile;
            } catch {
               return true;
            }
         },
      );
      refused = planned.refused;
      const text = generateRollupBuildFile(
         planned.plans,
         path.basename(args.modelPath),
      );
      if (!text) return { refused };
      const url = rollupCompanionURL(args.packagePath, args.modelPath);
      const overlay = persistBlankedFiles(args.modelDef);
      if (!overlay) {
         // Building over the author's files as they are would chain a rollup
         // onto its base's table (#6293); build nothing instead.
         logger.error(
            "A #(preaggregate) companion was not built: a `#@` line of the author's files could not be left out of it",
            { modelPath: args.modelPath },
         );
         return { refused };
      }
      overlay.set(url.href, text);
      const { runtime, importBaseURL } = await args.getRuntime(overlay);
      const materializer = runtime.loadModel(url, { importBaseURL });
      const model = await materializer.getModel();
      return {
         generated: {
            plans: planned.plans,
            model,
            materializer,
            runtime,
            text,
         },
         refused,
      };
   } catch (error) {
      logger.error(
         "Failed to plan or compile the generated #(preaggregate) companion",
         {
            modelPath: args.modelPath,
            error: error instanceof Error ? error.message : String(error),
         },
      );
      return { refused };
   }
}
