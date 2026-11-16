// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A served copy compiled on the load worker is the served copy compiled on
 * the main thread: over the conformance corpus (every shape, every variant,
 * loaded, built and bound through the real package path), each model's copy
 * is compiled both ways and compared byte for byte: its compiled model (the
 * IR the gates are grafted on), its problems, the sources it re-roots, and,
 * for every query of the shape, the SQL it answers with and the tables it
 * reads, or the decline it gives instead.
 */

import { afterAll, beforeAll, describe, expect, it, spyOn } from "bun:test";
import { DATA_FILES } from "./conformance/data";
import {
   freshLoadPool,
   loadFixture,
   withSwitch,
} from "../../../src/test_helpers/conformance/package_runner";
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

let workerCompiles = 0;
let compared = 0;

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
               for (const modelPath of fixture.pkg.getModelPaths()) {
                  const model = fixture.pkg.getModel(modelPath) as Model;
                  if (!model?.servedCopySourceIDs()) continue;
                  const copy = (
                     model as unknown as { servedCopy: ServedCopyServe }
                  ).servedCopy;
                  const queries = shape.queries.filter(
                     (q) => (q.model ?? entry) === modelPath,
                  );

                  // On the worker: every pass the pool takes, none inline.
                  const offered = spyOn(
                     PackageLoadPool.prototype,
                     "compileServedCopy",
                  );
                  const inline = spyOn(GeneratedFiles.prototype, "loadModel");
                  copy.invalidate();
                  const onWorker = await compiledCopyText(copy, queries);
                  const taken = offered.mock.results.filter(
                     (r) => r.value !== undefined,
                  ).length;
                  if (taken > 0) {
                     expect(inline.mock.calls.length).toBe(0);
                  }
                  workerCompiles += taken;
                  offered.mockRestore();
                  inline.mockRestore();

                  // On the main thread: the pool declines every pass.
                  const declined = spyOn(
                     PackageLoadPool.prototype,
                     "compileServedCopy",
                  ).mockReturnValue(undefined);
                  copy.invalidate();
                  const onMain = await compiledCopyText(copy, queries);
                  declined.mockRestore();

                  expect(onWorker).toBe(onMain);
                  compared++;
               }
            } finally {
               await loader.dispose();
               restore();
            }
         }, 120_000);
      });
   }
}

describe("served copy on the load worker: the corpus", () => {
   it("compiled copies on the worker and compared them", () => {
      expect(compared).toBeGreaterThan(0);
      expect(workerCompiles).toBeGreaterThan(0);
   });
});
