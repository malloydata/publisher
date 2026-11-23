// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Under lookup admission a query makes no catalog read and compiles nothing
 * beyond its own to judge a stored answer's string comparisons: what the
 * source compares under was read at the bind, and what the stored table
 * holds was recorded by its build. Under probe admission the same queries
 * read the warehouse on every answer. Both answer every caller as live does:
 * a gate over a byte-exact string column is served from the stored table,
 * one over a NOCASE column answers live.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   freshLoadPool,
   LAKE,
   type LoadedFixture,
   loadFixture,
   type Outcome,
} from "../../../src/test_helpers/conformance/package_runner";
import {
   type MetricsHarness,
   startMetricsHarness,
} from "../../../src/test_helpers/metrics_harness";
import { resetMaterializationTelemetryForTesting } from "../../../src/materialization_metrics";
import { rowDifference } from "./harness";

const rowsOf = (collate: string) => `duckdb.sql("""
  SELECT CAST(id AS BIGINT) AS id, category, region${collate} AS region
  FROM (VALUES (1, 'A', 'east'), (2, 'A', 'East'), (3, 'B', 'EAST'),
               (4, 'B', 'west')) AS t(id, category, region)
""")`;

const MODEL = `##! experimental { givens persistence access_modifiers }
given: REGION :: string

#(access_filter) region = $REGION
#(materialize) name="bin_eq" storage="${LAKE}"
source: bin_eq is ${rowsOf("")} extend { primary_key: id }

#(access_filter) region = $REGION
#(materialize) name="ci_eq" storage="${LAKE}"
source: ci_eq is ${rowsOf(" COLLATE NOCASE")} extend { primary_key: id }
`;

const query = (s: string) =>
   `run: ${s} -> { group_by: region; aggregate: n is count(); order_by: region }`;
const CALLERS = [{ REGION: "east" }, { REGION: "EAST" }];

for (const mode of ["lookup", "probe"] as const) {
   describe(`${mode} admission`, () => {
      let restore: () => void;
      let loader: Awaited<ReturnType<typeof loadFixture>>;
      let fixture: LoadedFixture;
      let metrics: MetricsHarness;
      const live = new Map<string, unknown>();

      beforeAll(async () => {
         restore = installSettings({
            env: {
               PERSIST_STORAGE_MODE: "on",
               PUBLISHER_MATERIALIZE: "true",
               PUBLISHER_MATERIALIZE_GATED_SOURCES: "true",
               PUBLISHER_ADMISSION_MODE: mode,
            },
         }).restore;
         await freshLoadPool();
         loader = await loadFixture({ "m.malloy": MODEL });
         for (const s of ["bin_eq", "ci_eq"]) {
            for (const [i, g] of CALLERS.entries()) {
               live.set(`${s}${i}`, await loader.run("m.malloy", query(s), g));
            }
         }
         fixture = await loader.bind();
         metrics = await startMetricsHarness();
         resetMaterializationTelemetryForTesting();
      }, 900_000);

      afterAll(async () => {
         await metrics?.shutdown();
         resetMaterializationTelemetryForTesting();
         await fixture?.dispose();
         await loader?.dispose().catch(() => {});
         restore?.();
         await freshLoadPool();
      });

      it("answers every caller as live does: the byte-exact gate from the table, the NOCASE one live", async () => {
         for (const [s, via] of [
            ["bin_eq", "served_copy"],
            ["ci_eq", undefined],
         ] as const) {
            for (const [i, g] of CALLERS.entries()) {
               const got = await fixture.run("m.malloy", query(s), g);
               const was = live.get(`${s}${i}`) as Outcome;
               expect("answer" in was && "answer" in got).toBe(true);
               if ("answer" in was && "answer" in got) {
                  expect(
                     rowDifference(was.answer.rows, got.answer.rows),
                  ).toBeUndefined();
               }
               expect("answer" in got && got.answer.via).toBe(
                  via ?? (undefined as never),
               );
            }
         }
      });

      it(
         mode === "lookup"
            ? "reads no catalog and compiles nothing extra to judge them"
            : "reads the warehouse to judge them",
         async () => {
            const reads = await metrics.collectCounter(
               "publisher_query_catalog_reads_total",
            );
            const compiles = await metrics.collectCounter(
               "publisher_query_probe_compiles_total",
            );
            if (mode === "lookup") {
               expect({ reads, compiles }).toEqual({ reads: 0, compiles: 0 });
            } else {
               expect(reads).toBeGreaterThan(0);
            }
         },
      );
   });
}
