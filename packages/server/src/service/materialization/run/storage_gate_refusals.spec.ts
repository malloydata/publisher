// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Characterization of the `storage=` build's DuckDB gate
 * (`assertStorageServeShapeCompiles`), which every `storage=` build passes
 * before its table is reported: which captured schemas it lets through and
 * which it refuses with a 422. Recorded as it behaves today, so replacing or
 * removing the gate (step R) shows exactly which builds change outcome.
 *
 * Recorded as is, not endorsed: every type the strict mapper does not
 * recognize falls back to `json`, which a Malloy `type:` declaration reads as
 * a reserved word, so such a column refuses the whole build rather than
 * being carried lossily as the fallback's doc says. The refusal also carries
 * no `reason`; only the eligibility metric records `not_duckdb_portable`.
 */

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import {
   internalErrorToHttpError,
   MaterializationEligibilityError,
} from "../../../errors";
import { resetMaterializationTelemetryForTesting } from "../../../materialization_metrics";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "../../../test_helpers/metrics_harness";
import { assertStorageServeShapeCompiles } from "./materialization_build_session";

function gate(
   schema: { name?: string; type?: string }[],
   over: { sourceName?: string; destinationName?: string } = {},
): Promise<void> {
   return assertStorageServeShapeCompiles({
      destinationName: over.destinationName ?? "lake",
      sourceName: over.sourceName ?? "s",
      virtualHandle: "h",
      physicalTableName: "p",
      schema,
   });
}

async function outcome(
   schema: { name?: string; type?: string }[],
   over?: { sourceName?: string; destinationName?: string },
): Promise<"serves" | "refused"> {
   try {
      await gate(schema, over);
      return "serves";
   } catch (err) {
      expect(err).toBeInstanceOf(MaterializationEligibilityError);
      return "refused";
   }
}

/** A column of each DuckDB type `DESCRIBE` can report, and the gate's outcome. */
const BY_TYPE: Record<string, "serves" | "refused"> = {
   BOOLEAN: "serves",
   TINYINT: "serves",
   SMALLINT: "serves",
   INTEGER: "serves",
   BIGINT: "serves",
   HUGEINT: "serves",
   UBIGINT: "serves",
   UHUGEINT: "serves",
   FLOAT: "serves",
   DOUBLE: "serves",
   "DECIMAL(18,2)": "serves",
   "DECIMAL(38,10)": "serves",
   VARCHAR: "serves",
   UUID: "serves",
   DATE: "serves",
   TIME: "serves",
   TIMESTAMP: "serves",
   TIMESTAMP_NS: "serves",
   "TIMESTAMP WITH TIME ZONE": "serves",
   TIMESTAMPTZ: "serves",
   INTERVAL: "refused",
   BLOB: "refused",
   BIT: "refused",
   JSON: "refused",
   "INTEGER[]": "refused",
   "VARCHAR[]": "refused",
   "STRUCT(a INTEGER, b VARCHAR)": "refused",
   "MAP(VARCHAR, INTEGER)": "refused",
   "ENUM('a', 'b')": "refused",
   "UNION(n INTEGER, s VARCHAR)": "refused",
   NOT_A_REAL_TYPE: "refused",
};

describe("the storage= build's DuckDB gate", () => {
   let metrics: MetricsHarness;
   beforeEach(async () => {
      metrics = await startMetricsHarness();
      resetMaterializationTelemetryForTesting();
   });
   afterEach(async () => {
      resetMaterializationTelemetryForTesting();
      await metrics.shutdown();
   });

   it("serves or refuses a one-column table by the column's type", async () => {
      const seen: Record<string, string> = {};
      for (const type of Object.keys(BY_TYPE)) {
         seen[type] = await outcome([{ name: "c", type }]);
      }
      expect(seen).toEqual(BY_TYPE);
   });

   it("refuses a whole table for one column it cannot type", async () => {
      expect(
         await outcome([
            { name: "id", type: "BIGINT" },
            { name: "tags", type: "VARCHAR[]" },
         ]),
      ).toBe("refused");
   });

   it("refuses a table with no typed column, and skips a column without a name or type", async () => {
      expect(await outcome([])).toBe("refused");
      expect(await outcome([{ name: "c" }, { type: "INTEGER" }])).toBe(
         "refused",
      );
      expect(await outcome([{ name: "", type: "INTEGER" }])).toBe("refused");
      expect(
         await outcome([{ name: "c", type: "INTEGER" }, { name: "d" }]),
      ).toBe("serves");
   });

   it("quotes any column, source or destination name", async () => {
      for (const name of ["order", "with space", 'q"uote', "x'y", "1st"]) {
         expect(await outcome([{ name, type: "INTEGER" }])).toBe("serves");
      }
      expect(
         await outcome([{ name: "c", type: "INTEGER" }], {
            sourceName: "with space",
            destinationName: "my lake",
         }),
      ).toBe("serves");
   });

   it("refuses as a 422 with no reason on the error, and counts not_duckdb_portable", async () => {
      const err = await gate([{ name: "c", type: "INTEGER[]" }]).then(
         () => undefined,
         (e: unknown) => e as MaterializationEligibilityError,
      );
      expect(err).toBeInstanceOf(MaterializationEligibilityError);
      expect(err!.reason).toBeUndefined();
      expect(err!.message).toStartWith(
         "Source 's' cannot be served from its storage destination: its " +
            "materialized shape does not compile in DuckDB (",
      );
      expect(err!.message).toContain("'json' is a reserved word");
      expect(err!.message).toEndWith(
         "A source materialized into a DuckDB/DuckLake store must have a " +
            "DuckDB-portable served shape.",
      );
      expect(internalErrorToHttpError(err!, { log: false }).status).toBe(422);
      expect(
         await metrics.collectCounter(
            "publisher_materialization_eligibility_refused_total",
            { reason: "not_duckdb_portable" },
         ),
      ).toBe(1);
   });
});
