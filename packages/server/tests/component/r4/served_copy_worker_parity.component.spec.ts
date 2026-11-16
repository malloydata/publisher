// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A served copy compiled on the load worker is the served copy compiled on
 * the main thread: over the conformance corpus (every shape, every variant,
 * loaded, built and bound through the real package path), each model's copy
 * is compiled both ways and compared byte for byte: its compiled model (the
 * IR the gates are grafted on), its problems, the sources it re-roots, and,
 * for every query of the shape, the SQL it answers with and the tables it
 * reads, or the decline it gives instead. The G3 models are run end to end
 * too, per caller, so each gate's graft on the worker's copy is compared.
 */

import { afterAll, beforeAll, describe, expect, it, spyOn } from "bun:test";
import { DATA_FILES, RAW_MODEL } from "./conformance/data";
import {
   freshLoadPool,
   type LoadedFixture,
   loadFixture,
   withSwitch,
} from "../../../src/test_helpers/conformance/package_runner";
import { installSettings } from "../../../src/test_helpers/settings_env";
import { G3_COLOCATED_MODEL, G3_STORAGE_MODEL } from "./g3_models";
import { renderFiles, SHAPES, VARIANTS } from "./conformance/shapes";
import { PackageLoadPool } from "../../../src/package_load/package_load_pool";
import { GeneratedFiles } from "../../../src/service/materialization/text/generated_files";
import type { ServedCopyServe } from "../../../src/service/materialization/serve/served_copy_serve";
import type { Model } from "../../../src/service/model";

/** Everything a compiled copy answers with, as text. */
async function compiledCopyText(
   copy: ServedCopyServe,
   queries: readonly { malloy: string; givens?: Record<string, unknown> }[],
): Promise<string> {
   const modelDef = await copy.compiledModelDef();
   const answers: unknown[] = [];
   for (const q of queries) {
      try {
         const answer = await copy.loadQuery(q.malloy, q.givens);
         answers.push({
            sql: await answer.runnable.getSQL({
               virtualMap: answer.virtualMap,
               givens: q.givens as never,
            }),
            tablesRead: answer.tablesRead.map((b) => b.tablePath),
            gateTablesRead: answer.gateTablesRead.map((b) => b.tablePath),
         });
      } catch (error) {
         answers.push({
            declined:
               (error as { code?: string }).code ??
               (error instanceof Error ? error.message : String(error)),
         });
      }
   }
   return withMintedIdsNumbered(
      JSON.stringify({
         modelDef,
         problems: copy.problems(),
         rerooted: [...(copy.rerootedSourceIDs() ?? [])].sort(),
         throughBase: [...copy.servedThroughBaseSourceIDs()].sort(),
         gateDeclined: [...copy.gateDeclinedSourceIDs()].sort(),
         answers,
      }),
   );
}

/**
 * `text` with each id a compile mints (a query source's name, a reference
 * id: random per compile, on either thread) numbered by first appearance, so
 * two compiles compare equal exactly when they agree on everything else,
 * which id stands where included.
 */
function withMintedIdsNumbered(text: string): string {
   const seen = new Map<string, number>();
   return text.replace(
      /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/g,
      (id) => {
         if (!seen.has(id)) seen.set(id, seen.size);
         return `<minted ${seen.get(id)}>`;
      },
   );
}

/** One query a copy is compared on, with a caller's givens. */
interface ParityQuery {
   malloy: string;
   givens?: Record<string, unknown>;
}

/**
 * The pool's `compileServedCopy`, waiting for its worker instead of
 * declining while it is busy, so every pass goes to the worker (the
 * inline fallback is the product's, and would make the comparison
 * inline against inline). Counts the passes the worker took.
 */
function everyPassOnTheWorker(): {
   taken: () => number;
   restore: () => void;
} {
   const offer = PackageLoadPool.prototype.compileServedCopy;
   let taken = 0;
   const spy = spyOn(
      PackageLoadPool.prototype,
      "compileServedCopy",
   ).mockImplementation(function (
      this: PackageLoadPool,
      ...args: Parameters<typeof offer>
   ) {
      return (async () => {
         const until = Date.now() + 60_000;
         for (;;) {
            const pending = offer.apply(this, args);
            if (pending) {
               taken++;
               return pending;
            }
            if (Date.now() > until) {
               throw new Error("no load worker became free for a pass");
            }
            await Bun.sleep(5);
         }
      })();
   });
   return { taken: () => taken, restore: () => spy.mockRestore() };
}

/** Each served answer to `queries`, as text, for a copy already compiled. */
async function answersText(
   run: (q: ParityQuery) => Promise<unknown>,
   queries: readonly ParityQuery[],
): Promise<string> {
   const out: unknown[] = [];
   for (const q of queries) out.push(await run(q));
   return withMintedIdsNumbered(JSON.stringify(out));
}

/** Copies a worker compiled and compared with the inline compile. */
let comparedOnWorker = 0;
/** Copies with nothing to compile (no table bound), compared inline with inline. */
let comparedWithoutCompile = 0;
let workerPasses = 0;

/**
 * Compile each model's copy on the worker, then inline, and compare: the
 * copy itself, and, when `run` is given, every query run end to end through
 * the model (its gates grafted on the copy, its rows and metadata).
 */
async function compareModels(
   fixture: LoadedFixture,
   queriesOf: (modelPath: string) => readonly ParityQuery[],
   run?: (modelPath: string, q: ParityQuery) => Promise<unknown>,
): Promise<void> {
   for (const modelPath of fixture.pkg.getModelPaths()) {
      const model = fixture.pkg.getModel(modelPath) as Model;
      if (!model?.servedCopySourceIDs()) continue;
      const copy = (model as unknown as { servedCopy: ServedCopyServe })
         .servedCopy;
      const queries = queriesOf(modelPath);
      const runAll = run
         ? () => answersText((q) => run(modelPath, q), queries)
         : async () => "";

      // On the worker: every pass, none inline.
      const worker = everyPassOnTheWorker();
      const inline = spyOn(GeneratedFiles.prototype, "loadModel");
      copy.invalidate();
      const onWorker = await compiledCopyText(copy, queries);
      // The copy's compiles only: a run compiles other legs' variants too.
      const inlineOnWorker = inline.mock.calls.length;
      const taken = worker.taken();
      const ranOnWorker = await runAll();
      worker.restore();
      inline.mockRestore();

      // On the main thread: the pool declines every pass.
      const declined = spyOn(
         PackageLoadPool.prototype,
         "compileServedCopy",
      ).mockReturnValue(undefined);
      copy.invalidate();
      const onMain = await compiledCopyText(copy, queries);
      const ranOnMain = await runAll();
      declined.mockRestore();

      expect(inlineOnWorker).toBe(0);
      expect(onWorker).toBe(onMain);
      expect(ranOnWorker).toBe(ranOnMain);
      if (taken > 0) {
         comparedOnWorker++;
         workerPasses += taken;
      } else {
         comparedWithoutCompile++;
      }
   }
}

beforeAll(freshLoadPool);
afterAll(async () => {
   await freshLoadPool();
});

for (const shape of SHAPES) {
   for (const variant of shape.variants ?? VARIANTS) {
      describe(`served copy on the load worker: ${shape.name} [${variant}]`, () => {
         it("compiles to what the main thread compiles", async () => {
            const restore = withSwitch("cutover", shape.env);
            await freshLoadPool();
            const loader = await loadFixture({
               ...DATA_FILES,
               ...renderFiles(shape, variant),
            });
            try {
               const fixture = await loader.bind({ includeRollups: true });
               const entry = Object.keys(shape.files)[0];
               await compareModels(fixture, (modelPath) =>
                  shape.queries.filter((q) => (q.model ?? entry) === modelPath),
               );
            } finally {
               await loader.dispose();
               restore();
            }
         }, 120_000);
      });
   }
}

/**
 * The G3 models (a source's own row filter, its own lock, both, and the
 * inherited and joined gates G3 refuses), colocated and `storage=`, each
 * query asked by callers whose givens differ, run end to end: the gate is
 * grafted on the copy the worker compiled exactly as on the inline one.
 */
const G3_CALLERS: Record<string, unknown>[] = [
   { REGIONS: ["north"], ROLE: "admin", STATUS: "placed" },
   { REGIONS: ["east", "west"], ROLE: "viewer" },
   { REGIONS: [], ROLE: "admin" },
   {},
];
const G3_CASES: {
   name: string;
   model: string;
   sources: string[];
}[] = [
   {
      name: "colocated",
      model: G3_COLOCATED_MODEL,
      sources: [
         "gated",
         "gated_ext",
         "gated_ext_tbl",
         "locked",
         "locked_rows",
         "gated_pairs",
         "gated_inline",
         "gated_sto",
         "baked",
      ],
   },
   {
      name: "storage=",
      model: G3_STORAGE_MODEL,
      sources: [
         "mz_gated",
         "v1_gated",
         "mz_ext",
         "v1_ext",
         "mz_lock",
         "v1_lock",
         "mz_lock_rows",
         "v1_lock_rows",
         "mz_inh_lock",
         "v1_inh_lock",
         "mz_inh_rows",
         "v1_inh_rows",
      ],
   },
];

for (const g3 of G3_CASES) {
   describe(`served copy on the load worker: G3 gates [${g3.name}]`, () => {
      it("grafts each caller's gate as the main thread's copy does", async () => {
         const restore = installSettings({
            env: {
               PERSIST_STORAGE_MODE: "on",
               PUBLISHER_MATERIALIZE: "true",
               PUBLISHER_MATERIALIZE_GATED_SOURCES: "false",
            },
         }).restore;
         await freshLoadPool();
         const loader = await loadFixture({
            ...DATA_FILES,
            "raw.malloy": RAW_MODEL,
            "m.malloy": g3.model,
         });
         try {
            const fixture = await loader.bind();
            const queries = g3.sources.flatMap((source) =>
               G3_CALLERS.map((givens) => ({
                  malloy: `run: ${source} -> { group_by: region; aggregate: n is count(); order_by: region }`,
                  givens,
               })),
            );
            let servedByCopy = 0;
            await compareModels(
               fixture,
               (modelPath) => (modelPath === "m.malloy" ? queries : []),
               async (modelPath, q) => {
                  const outcome = await fixture.run(
                     modelPath,
                     q.malloy,
                     q.givens as never,
                  );
                  if (
                     "answer" in outcome &&
                     outcome.answer.via === "served_copy"
                  ) {
                     servedByCopy++;
                  }
                  return outcome;
               },
            );
            // Callers were answered from the copy, gate grafted, not only live.
            expect(servedByCopy).toBeGreaterThan(0);
         } finally {
            await loader.dispose();
            restore();
         }
      }, 180_000);
   });
}

describe("served copy on the load worker: the corpus", () => {
   it("compiled copies on the worker and compared them", () => {
      console.log(
         `worker parity: ${comparedOnWorker} copies compiled on the worker (${workerPasses} passes) and compared; ${comparedWithoutCompile} with nothing to compile`,
      );
      expect(comparedOnWorker).toBeGreaterThan(0);
      expect(workerPasses).toBeGreaterThan(0);
   });
});
