// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Characterization of the `storage=` build's DuckDB gate
 * (`assertStorageServeShapeCompiles`), which every `storage=` build passes
 * before its table is reported: which captured schemas it lets through and
 * which it refuses with a 422. Recorded as it behaves today, so replacing or
 * removing the gate (step R) shows exactly which builds change outcome.
 *
 * A list or struct column is declared as Malloy's array or record type and
 * passes. A column no Malloy type reads back faithfully (MAP, UNION, ENUM,
 * BLOB, INTERVAL, BIT, JSON, an unknown type) is left out of the shape, so a
 * query reading it answers live; the gate refuses only a table with no column
 * left, with reason `not_duckdb_portable`.
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
   "INTEGER[]": "serves",
   "VARCHAR[]": "serves",
   "STRUCT(a INTEGER, b VARCHAR)": "serves",
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

   it("leaves out a column it cannot type rather than refusing the table", async () => {
      expect(
         await outcome([
            { name: "id", type: "BIGINT" },
            { name: "tags", type: "VARCHAR[]" },
         ]),
      ).toBe("serves");
      expect(
         await outcome([
            { name: "id", type: "BIGINT" },
            { name: "m", type: "MAP(VARCHAR, INTEGER)" },
         ]),
      ).toBe("serves");
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

   it("refuses a table with no column left as a 422 with reason not_duckdb_portable, naming the columns", async () => {
      const err = await gate([
         { name: "c", type: "MAP(VARCHAR, INTEGER)" },
      ]).then(
         () => undefined,
         (e: unknown) => e as MaterializationEligibilityError,
      );
      expect(err).toBeInstanceOf(MaterializationEligibilityError);
      expect(err!.reason).toBe("not_duckdb_portable");
      expect(err!.message).toStartWith(
         "Source 's' cannot be served from its storage destination: no stored " +
            "column has a Malloy type that reads it back faithfully ('c' (MAP(VARCHAR, INTEGER))).",
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
