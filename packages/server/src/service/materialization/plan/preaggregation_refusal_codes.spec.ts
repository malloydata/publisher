// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Real-compile pins for three refusal codes no other spec reaches:
 *
 *  - `invalid_storage`: a v1 `#@ preaggregate storage=` naming a destination
 *    with a quote or a backslash, which the synthesized `#@ persist` cannot
 *    re-declare.
 *  - `grain_dimension_is_join`: declared, but never reported. A grain naming
 *    a join is reported as `grain_path_through_join`, the join check running
 *    first, so the code is unreachable.
 *  - `public_surface_unknown`: a `storage=` build whose public columns cannot
 *    be read refuses with that reason, before any SQL runs.
 */

import type { FixedConnectionMap, PersistSource } from "@malloydata/malloy";
import { beforeAll, describe, expect, it } from "bun:test";
import { MaterializationEligibilityError } from "../../../errors";
import {
   duckdbTestConnections,
   loadTestModel,
} from "../../../test_helpers/incremental_test_harness";
import { projectToStoredColumns } from "./build_plan";
import { validateSourcePreaggregation } from "./preaggregation_validation";

let connections: FixedConnectionMap;

beforeAll(() => {
   ({ connections } = duckdbTestConnections());
});

/** The violations `#@ preaggregate` validation reports for source `s`. */
async function violationsFor(body: string) {
   const text = `##! experimental { persistence composite_sources }
source: other is duckdb.sql("""SELECT 1 AS order_id, 2 AS qty""")
source: s is duckdb.sql("""SELECT 1 AS order_id, 10 AS amount, 'A' AS category""") extend {
${body}
}
`;
   const compiled = await loadTestModel(connections, text).getModel();
   // eslint-disable-next-line @typescript-eslint/no-explicit-any
   const contents = (compiled as any)._modelDef.contents;
   return validateSourcePreaggregation(
      "s",
      contents["s"] as Parameters<typeof validateSourcePreaggregation>[1],
   );
}

describe("#@ preaggregate refusal codes no other spec reaches", () => {
   it("refuses a storage= destination with a backslash as invalid_storage", async () => {
      const violations =
         await violationsFor(`  #@ preaggregate grain="category" storage="la\\\\ke"
  measure: revenue is amount.sum()`);
      expect(violations.map((v) => v.code)).toEqual(["invalid_storage"]);
      expect(violations[0].message).toContain("quote or backslash");
   });

   it("refuses a storage= destination with a quote as invalid_storage", async () => {
      const violations =
         await violationsFor(`  #@ preaggregate grain="category" storage="la\\"ke"
  measure: revenue is amount.sum()`);
      expect(violations.map((v) => v.code)).toEqual(["invalid_storage"]);
   });

   it("reports a grain naming a join as grain_path_through_join, never grain_dimension_is_join", async () => {
      const violations =
         await violationsFor(`  join_one: o is other on order_id = o.order_id
  #@ preaggregate grain="o"
  measure: revenue is amount.sum()`);
      expect(violations.map((v) => v.code)).toEqual([
         "grain_path_through_join",
      ]);
   });
});

describe("public_surface_unknown", () => {
   it("refuses a storage= build whose field list cannot be read, with that reason", () => {
      const unreadable = {
         name: "daily",
         dialectName: "duckdb",
         _explore: {
            get intrinsicFields(): never {
               throw new Error("shape changed");
            },
         },
      } as unknown as PersistSource;
      let caught: unknown;
      try {
         projectToStoredColumns(unreadable, "SELECT 1 AS a");
      } catch (err) {
         caught = err;
      }
      expect(caught).toBeInstanceOf(MaterializationEligibilityError);
      expect((caught as MaterializationEligibilityError).reason).toBe(
         "public_surface_unknown",
      );
      expect((caught as Error).message).toContain("'daily'");
      expect((caught as Error).message).toContain("shape changed");
   });
});
