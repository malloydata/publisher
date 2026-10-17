// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   type FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
} from "@malloydata/malloy";
import { beforeAll, describe, expect, it } from "bun:test";
import * as sinon from "sinon";
import { logger } from "../logger";
import { duckdbTestConnections } from "../test_helpers/incremental_test_harness";
import {
   tryCompileSynthesizedPreaggregation,
   type SynthesizedRuntimeFactory,
} from "./preaggregation_compile";

/**
 * tryCompileSynthesizedPreaggregation: the one seam that plans, synthesizes and
 * compiles a model's rollups, for both the build plan and the serve path.
 */

const PACKAGE = "/pkg";
const MODEL_PATH = "orders.malloy";
const MODEL_URL = `file://${PACKAGE}/${MODEL_PATH}`;

const WITH_PREAGGREGATE = `##! experimental { persistence composite_sources }
source: orders is duckdb.sql("select 1 as id, 10 as amount, 'A' as category") extend {
  #@ preaggregate grain="category"
  measure: total is amount.sum()
}
`;

const WITHOUT_PREAGGREGATE = `source: orders is duckdb.sql("select 1 as id") extend {
  measure: n is count()
}
`;

let connections: FixedConnectionMap;
beforeAll(() => {
   ({ connections } = duckdbTestConnections());
});

/**
 * A runtime factory that serves the author model at its package path plus
 * the overlay, the way `Model.getModelRuntime` does; records each overlay.
 */
function runtimeFactory(authorText: string): {
   getRuntime: SynthesizedRuntimeFactory;
   overlays: ReadonlyMap<string, string>[];
} {
   const overlays: ReadonlyMap<string, string>[] = [];
   return {
      overlays,
      getRuntime: async (overlay) => {
         overlays.push(overlay);
         const files = new Map([[MODEL_URL, authorText], ...overlay]);
         return {
            runtime: new Runtime({
               urlReader: new InMemoryURLReader(files),
               connections,
            }),
            importBaseURL: new URL(`file://${PACKAGE}/`),
         };
      },
   };
}

/** The author model's compiled `modelDef.contents`, as callers pass it. */
async function authorContents(text: string): Promise<Record<string, unknown>> {
   const model = await runtimeFactory(text)
      .getRuntime(new Map())
      .then(({ runtime, importBaseURL }) =>
         runtime.loadModel(new URL(MODEL_URL), { importBaseURL }).getModel(),
      );
   // eslint-disable-next-line @typescript-eslint/no-explicit-any
   return (model as any)._modelDef.contents;
}

describe("tryCompileSynthesizedPreaggregation", () => {
   it("compiles the rollups a #@ preaggregate declares, from an overlay beside the author model", async () => {
      const factory = runtimeFactory(WITH_PREAGGREGATE);

      const result = await tryCompileSynthesizedPreaggregation({
         packagePath: PACKAGE,
         modelPath: MODEL_PATH,
         contents: await authorContents(WITH_PREAGGREGATE),
         getRuntime: factory.getRuntime,
      });

      expect(result).toBeDefined();
      expect(result?.plans).toHaveLength(1);
      // Served from the overlay, never written: one entry, at a non-.malloy
      // companion path inside the package, holding the generated text.
      expect(factory.overlays).toHaveLength(1);
      expect([...factory.overlays[0].keys()]).toEqual([
         `${MODEL_URL}.preagg.malloy`,
      ]);
      expect(factory.overlays[0].get(`${MODEL_URL}.preagg.malloy`)).toBe(
         result?.text,
      );
      // The compiled model's build plan is the rollup to build.
      const rollupNames = Object.values(
         result?.model.getBuildPlan().sources ?? {},
      ).map((s) => s.name);
      expect(rollupNames).toContain(
         result?.plans[0].rollupSourceName as string,
      );
   });

   it("returns undefined, without compiling anything, for a model with no #@ preaggregate", async () => {
      const factory = runtimeFactory(WITHOUT_PREAGGREGATE);

      const result = await tryCompileSynthesizedPreaggregation({
         packagePath: PACKAGE,
         modelPath: MODEL_PATH,
         contents: await authorContents(WITHOUT_PREAGGREGATE),
         getRuntime: factory.getRuntime,
      });

      expect(result).toBeUndefined();
      expect(factory.overlays).toHaveLength(0);
   });

   it("logs and swallows a compile failure, so the caller serves live", async () => {
      const error = sinon.stub(logger, "error");
      try {
         const result = await tryCompileSynthesizedPreaggregation({
            packagePath: PACKAGE,
            modelPath: MODEL_PATH,
            contents: await authorContents(WITH_PREAGGREGATE),
            getRuntime: async () => {
               throw new Error("runtime unavailable");
            },
         });

         expect(result).toBeUndefined();
         expect(error.calledOnce).toBe(true);
         expect((error.firstCall.args as unknown[])[1]).toMatchObject({
            modelPath: MODEL_PATH,
            error: "runtime unavailable",
         });
      } finally {
         error.restore();
      }
   });
});
