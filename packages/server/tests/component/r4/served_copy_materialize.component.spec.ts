// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for the served copy over `#(materialize)` (materialization v2, phase 3
 * PR 3b): with PUBLISHER_MATERIALIZE on, the author's source is re-rooted on
 * its generated build source's table, each computed dimension stored under
 * its own name (its definition dropped, its annotations on the stored field),
 * and every answer equals live.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
} from "./harness";

const seedSql = [
   `CREATE TABLE ev AS SELECT * FROM (VALUES
      (1, 'east', 10.5, DATE '2026-01-01'),
      (2, 'east', 20.0, DATE '2026-02-02'),
      (3, 'west', 5.5, DATE '2026-01-02'),
      (4, 'west', 11.0, DATE '2026-03-03'),
      (5, 'east', 2.0, DATE '2026-03-04')
    ) AS t(id, region, amt, d)`,
];

const model = `source: orders is duckdb.table('ev') extend {
  dimension: tier is pick 'high' when amt > 10 else 'low'
}

#(materialize) storage="lake"
source: east_orders is orders extend {
  where: region = 'east'
  dimension:
    big is amt > 10
    #(doc) "The month of the order"
    #(classification) internal
    order_month is d.month
  measure: total is amt.sum()
  view: by_big is { group_by: big; aggregate: total }
}

source: east_ext is east_orders extend {
  measure: n is count()
}

source: kept is duckdb.table('ev') extend {
  measure: grand is amt.sum()
}

#(materialize) storage="lake"
source: kept_west is kept extend {
  where: region = 'west'
}
`;

describe("R4: the served copy over #(materialize) answers equal to live", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: { "m.malloy": model },
         seedSql,
         materialize: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   const served = [
      [
         "a view over a dimension redefined in place",
         "run: east_orders -> by_big",
      ],
      [
         "a dimension on a newline-separated definition after an annotation",
         "run: east_orders -> { group_by: order_month; aggregate: total }",
      ],
      [
         "a computed dimension the source inherits",
         "run: east_orders -> { group_by: tier; aggregate: total }",
      ],
      [
         "a filter on a redefined dimension",
         "run: east_orders -> { where: big; aggregate: total }",
      ],
      [
         "an extension in the same file",
         "run: east_ext -> { group_by: region; aggregate: n, total }",
      ],
   ] as const;
   for (const [name, malloy] of served) {
      it(name, async () => {
         const run = await fixture.run({ name, malloy });
         expect(run.servedVia).toBe("served_copy");
         expect(run.servedFrom).toBe("storage");
         expect(rowDifference(run.live, run.served)).toBeUndefined();
      });
   }

   it("builds and binds only the sources that declare #(materialize), not an extension that inherits it", async () => {
      // Malloy copies east_orders' #(materialize) onto east_ext, which
      // declares none: east_ext has no build source and no table of its own.
      expect(fixture.bindings.map((b) => b.sourceName).sort()).toEqual([
         "east_orders",
         "kept_west",
      ]);
      fixture.setBindings(fixture.bindings);
      const { path } = await fixture.runAsIs(
         "run: east_ext -> { group_by: region; aggregate: n, total }",
      );
      expect(path?.servedBy.kind).not.toBe("live");
      expect(JSON.stringify(path)).not.toContain("east_ext__mat");
   });

   it("carries a stored dimension's #(doc) and #(classification) onto the served field", async () => {
      const notes = await fixture.servedFieldNotes(
         "east_orders",
         "order_month",
      );
      expect(notes).toContain('#(doc) "The month of the order"');
      expect(notes).toContain("#(classification) internal");
   });

   it("warns only for the source that loses an inherited field, not for an extension", async () => {
      const problems = await fixture.servedCopyProblems();
      expect(problems.map((p) => [p.sourceName, p.reason])).toEqual([
         ["kept_west", "fields_not_carried"],
      ]);
   });

   it("leaves a source whose base declares a measure as written, and answers live", async () => {
      const run = await fixture.run({
         name: "inherited measure",
         malloy: "run: kept_west -> { aggregate: grand }",
      });
      expect(run.servedVia).toBeUndefined();
      expect(rowDifference(run.live, run.served)).toBeUndefined();
   });

   it("reports that source's table as WARNED with the package warning's code, not a missing field", async () => {
      fixture.setBindings(fixture.bindings);
      const { path } = await fixture.runAsIs(
         "run: kept_west -> { aggregate: grand }",
      );
      expect(path?.servedBy.kind).toBe("live");
      expect(
         path?.considered.map((c) => [c.outcome, c.reason, c.code]),
      ).toEqual([["skipped", "WARNED", "served-copy-not-served"]]);
   });
});
