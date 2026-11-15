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
import { DATA_FILES } from "./conformance/data";
import {
   freshLoadPool,
   type LoadedFixture,
   loadFixture,
   type Outcome,
   withSwitch,
} from "../../../src/test_helpers/conformance/package_runner";
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

// A v1 `storage=` table stores public columns only, so an `include` that
// hides a column cannot be kept on it. The source is left as written and
// answers live, with a warning that says why and what to change (never a
// "Publisher bug"), whether its base is a connection's SQL or a query.
const ORDERS_SQL = `duckdb.sql("""
  SELECT region, CAST(amount AS DOUBLE) AS amount, CAST(id AS BIGINT) AS id
  FROM read_csv('data/orders.csv', header=true, all_varchar=true)
""")`;
const hiddenFiles = {
   "m.malloy": `##! experimental { persistence access_modifiers }

#@ persist name="raw_hidden" storage="lake"
source: raw_hidden is ${ORDERS_SQL} include {
  public: region, amount
  private: id
} extend {
  measure: total is amount.sum()
}

#@ persist name="raw_hidden_used" storage="lake"
source: raw_hidden_used is ${ORDERS_SQL} include {
  public: region, amount
  private: id
} extend {
  dimension: id_band is floor(id / 10)
  measure: total is amount.sum()
}

#@ persist name="query_hidden_used" storage="lake"
source: query_hidden_used is ${ORDERS_SQL} -> { select: * } include {
  public: region, amount
  private: id
} extend {
  dimension: id_band is floor(id / 10)
  measure: total is amount.sum()
}

#@ persist name="raw_hidden_filtered" storage="lake"
source: raw_hidden_filtered is ${ORDERS_SQL} include {
  public: region, amount
  private: id
} extend {
  where: id is not null
  measure: total is amount.sum()
}

#@ persist name="query_hidden" storage="lake"
source: query_hidden is ${ORDERS_SQL} -> { select: * } include {
  public: region, amount
  internal: id
} extend {
  measure: total is amount.sum()
}
`,
};

describe("an include that hides a column a v1 storage= table does not store", () => {
   let restore: () => void = () => {};
   let fixture: LoadedFixture | undefined;
   const live = new Map<string, Outcome>();
   const served = new Map<string, Outcome>();
   // A hidden column the extend reads (a dimension over it, a where:) is
   // still one the table must store.
   const sources = [
      "raw_hidden",
      "query_hidden",
      "raw_hidden_used",
      "query_hidden_used",
      "raw_hidden_filtered",
   ];
   const queryOf = (source: string) =>
      `run: ${source} -> { group_by: region; aggregate: total }`;

   beforeAll(async () => {
      await freshLoadPool();
      restore = withSwitch("on");
      const loader = await loadFixture({ ...DATA_FILES, ...hiddenFiles });
      for (const s of sources) {
         live.set(s, await loader.run("m.malloy", queryOf(s)));
      }
      fixture = await loader.bind();
      for (const s of sources) {
         served.set(s, await fixture.run("m.malloy", queryOf(s)));
      }
   });
   afterAll(async () => {
      restore();
      await fixture?.dispose();
      await freshLoadPool();
   });

   for (const source of sources) {
      it(`${source}: warns why it is left as written, naming the column`, () => {
         const warnings = (fixture?.warnings ?? []).filter(
            (w) => w.subject === source && w.code === "served-copy-not-served",
         );
         expect(warnings.map((w) => w.message)).toEqual([
            expect.stringContaining("hides `id` (private or internal)"),
         ]);
         expect(warnings[0].message).toContain("make the column public");
         expect(warnings[0].message).not.toContain("Publisher bug");
      });

      it(`${source}: answers live, equal to live`, () => {
         const l = live.get(source);
         const s = served.get(source);
         if (!l || "error" in l) throw new Error(JSON.stringify(l));
         if (!s || "error" in s) throw new Error(JSON.stringify(s));
         expect(s.answer.servedBy).toBe("live");
         expect(rowDifference(l.answer.rows, s.answer.rows)).toBeUndefined();
      });
   }
});
