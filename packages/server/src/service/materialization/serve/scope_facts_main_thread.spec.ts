// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A bind commit records every named source's scope facts over the tables it
 * binds (`scope_facts.ts`). On a wide package that is many sources times many
 * bound tables, and recorded in one main-thread task it holds the event loop
 * for seconds: about 4.5 s for the package here, growing with both. It must
 * record without holding the event loop for long.
 */

import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
   type ModelDef,
} from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { makeSnapshot } from "../domain/binding";
import {
   recordScopeFacts,
   scopeFactsOf,
   takeScopeFactWalksForTesting,
} from "./scope_facts";

const ROOT = "file:///scope-facts-main-thread/";
const FACTS = 4;
const ROLLUPS = 40;
const VIEW_MODELS = 4;
const REGIONS = ["north", "south", "east", "west"];

/** The longest any one task may hold the event loop while recording, ms. */
const MAX_BLOCK_MS = 250;

const fact = (k: number) => `source: fact_${k} is duckdb.sql("""
  SELECT i AS id,
    (['north','south','east','west'])[1 + i % 4] AS region,
    (['books','games','garden','tools','toys'])[1 + i % 5] AS category,
    (['web','store','partner'])[1 + i % 3] AS channel,
    (i * 37 % 997)::DOUBLE AS amount
  FROM range(${100 + k}) t(i)
""")
`;

/**
 * Persisted rollups whose query scopes the rows it reads (a `where:`), each
 * with a gated extension, as a wide package's derived sources are.
 */
const rollup = (i: number) => `#@ persist storage="lake" name="sw_r${i}"
source: r${i} is fact_${i % FACTS} -> {
  where: region = '${REGIONS[i % REGIONS.length]}'
  group_by: category, channel
  aggregate:
    n is count()
    amt is amount.sum()
}

#(access_filter) channel = $CHANNEL
source: r${i}_gated is r${i} extend {
  measure: total is amt.sum()
}
`;

const FILES: Record<string, string> = {
   "base.malloy": `##! experimental { givens persistence }
given: CHANNEL :: string

${Array.from({ length: FACTS }, (_, k) => fact(k)).join("\n")}`,
   "rollups.malloy": `##! experimental { givens persistence }
import "base.malloy"

${Array.from({ length: ROLLUPS }, (_, i) => rollup(i)).join("\n")}`,
   ...Object.fromEntries(
      Array.from({ length: VIEW_MODELS }, (_, j) => [
         `views_${j}.malloy`,
         `##! experimental { givens persistence }
import "rollups.malloy"

source: view_${j} is r${j}_gated extend {
  where: category != 'none'
  view: top is { group_by: category; aggregate: total; limit: 5 }
}
`,
      ]),
   ),
};

describe("recording a wide bind's scope facts", () => {
   let duckdb: DuckDBConnection;
   const models: ModelDef[] = [];

   beforeAll(async () => {
      duckdb = new DuckDBConnection("duckdb");
      const runtime = new Runtime({
         urlReader: new InMemoryURLReader(
            new Map(
               Object.entries(FILES).map(([name, text]) => [
                  `${ROOT}${name}`,
                  text,
               ]),
            ),
         ),
         connections: new FixedConnectionMap(
            new Map([["duckdb", duckdb]]),
            "duckdb",
         ),
      });
      for (let j = 0; j < VIEW_MODELS; j++) {
         const model = await runtime
            .loadModel(new URL(`${ROOT}views_${j}.malloy`), {
               importBaseURL: new URL(ROOT),
            })
            .getModel();
         models.push(model._modelDef);
      }
   });

   afterAll(async () => {
      await duckdb.close();
   });

   it("never holds the event loop for long, and walks a source again only for a bound set it reads", async () => {
      // Every model sees every rollup through its imports, as a package whose
      // models import a shared spine does.
      const rollupIDs = Array.from({ length: ROLLUPS }, (_, i) => {
         const id = (models[0].contents[`r${i}`] as { sourceID?: string })
            ?.sourceID;
         expect(typeof id).toBe("string");
         return id as string;
      });
      for (const modelDef of models) {
         expect(Object.keys(modelDef.contents).length).toBeGreaterThan(
            2 * ROLLUPS,
         );
      }
      const snapshot = makeSnapshot(
         rollupIDs.map(
            (id, i) => ({ sourceIDs: [id], virtualHandle: `h${i}` }) as never,
         ),
      );
      // The served copy's lane re-roots every bound source.
      const lane = {
         stored: new Set(rollupIDs),
         rerooted: new Set(rollupIDs),
      };

      // The event loop's longest gap while recording: a 10 ms timer's
      // lateness, as the main-thread regression probe measures it.
      let maxBlockMs = 0;
      let last = performance.now();
      const timer = setInterval(() => {
         const now = performance.now();
         maxBlockMs = Math.max(maxBlockMs, now - last - 10);
         last = now;
      }, 10);
      const runtimes = models.map(() => ({}));
      try {
         // Let the timer start before the first recording.
         await Bun.sleep(20);
         last = performance.now();
         takeScopeFactWalksForTesting();
         for (const [j, modelDef] of models.entries()) {
            await recordScopeFacts(runtimes[j], { modelDef, snapshot, lane });
         }
         // A last tick, so a block at the very end is measured too.
         await Bun.sleep(20);
      } finally {
         clearInterval(timer);
      }

      const { walked, reused } = takeScopeFactWalksForTesting();
      for (const runtime of runtimes) {
         expect(scopeFactsOf(runtime, snapshot)).toBeDefined();
      }
      expect(maxBlockMs).toBeLessThan(MAX_BLOCK_MS);
      // Each source's walk asks only about the sources it reads, so a bound
      // set holding none of them reuses its first walk: most of a wide bind
      // is reuse. The yield alone keeps the block short, not the CPU.
      expect(walked).toBeGreaterThan(0);
      expect(reused).toBeGreaterThan(10 * walked);
   });
});
