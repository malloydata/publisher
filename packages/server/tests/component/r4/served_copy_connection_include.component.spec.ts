// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for a connection-rooted `#@ persist` source whose declaration curates its
 * columns with an `include { … }` before its `extend { … }`
 * (`conn.sql("…") include { … } extend { … }`): its table is that SQL's rows,
 * so the re-root keeps both blocks and applies them again over it, and its
 * queries read the table, equal to live.
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
   `CREATE TABLE events AS SELECT * FROM (VALUES
      (1, 'page_view', 'a', 120),
      (2, 'page_view', 'b', 30),
      (3, 'cta_click', 'a', NULL),
      (4, 'page_view', 'a', 900),
      (5, 'scroll', 'c', 15)
    ) AS t(id, event_name, visitor, engagement_msec)`,
];

const model = `##! experimental { persistence access_modifiers }

#@ persist name="events_curated" storage="lake"
source: events_curated is duckdb.sql("""
  SELECT id, event_name, visitor, engagement_msec FROM events
""") include {
  public:
    event_name
    visitor
    engagement_msec
} extend {
  dimension: is_page_view is event_name = 'page_view'
  measure:
    event_count is count()
    visitors is count(visitor)
    engaged_msec is engagement_msec.sum()
}
`;

describe("R4: a connection-rooted source with an include before its extend", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on" },
      }).restore;
      fixture = await buildScenario({ files: { "m.malloy": model }, seedSql });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("re-roots it, with no problem", async () => {
      expect(await fixture.servedCopyProblems()).toEqual([]);
   });

   const queries = [
      [
         "a measure by a curated column",
         "run: events_curated -> { group_by: event_name; aggregate: event_count, visitors, engaged_msec }",
      ],
      [
         "a dimension from the extend",
         "run: events_curated -> { group_by: is_page_view; aggregate: event_count }",
      ],
   ] as const;
   for (const [name, malloy] of queries) {
      it(`serves ${name} from the table, equal to live`, async () => {
         const run = await fixture.run({ name, malloy });
         expect(run.servedVia).toBe("served_copy");
         expect(rowDifference(run.live, run.served)).toBeUndefined();
      });
   }

   // `id` is not listed in the include, so neither live nor the served copy
   // exposes it.
   it("keeps a column the include leaves out hidden", async () => {
      fixture.setBindings(fixture.bindings);
      expect(
         fixture.runAsIs("run: events_curated -> { group_by: id }"),
      ).rejects.toThrow(/id/);
   });
});
