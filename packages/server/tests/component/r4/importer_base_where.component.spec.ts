// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * S4 (gtme feedback): a stored source over a base declared in another file,
 * whose base carries a plain `where:` (not a gate), is served from its table
 * in the declaring file and in a file that imports it, with the base's rows
 * filtered, and is never refused as "Access denied".
 *
 * The reported shape: `deals.malloy` declares `deals` with `where: not
 * is_deleted`, a second file stores a source built over it, and the query was
 * refused with `Access denied for source …`. On main it is served (the
 * cross-file base resolution of #2041 covers it), so this is a regression
 * guard for both stored kinds: a `#(materialize)` extension and a `#@
 * persist` query source.
 *
 * Through a real package (plan, build, bind, each model's served copy, the
 * router), on DuckDB.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   freshLoadPool,
   type LoadedFixture,
   loadFixture,
   type Outcome,
} from "../../../src/test_helpers/conformance/package_runner";

const DEALS =
   "id,owner,amount,is_deleted\n" +
   "1,a,10,false\n2,a,20,true\n3,b,5,false\n4,b,7,false\n";

const BASE = `source: deals is duckdb.table('deals.csv') extend {
  where: not is_deleted
  measure: deal_count is count()
}
`;

const QUERY =
   "run: open_deals -> { group_by: owner; aggregate: n is count(), total is amount.sum(); order_by: owner }";

/** The live answer: the base's `where:` drops deal 2. */
const ROWS = '[{"owner":"a","n":1,"total":10},{"owner":"b","n":2,"total":12}]';

const SHAPES: { label: string; stored: string }[] = [
   {
      label: "a #(materialize) extension",
      stored: `import "deals.malloy"

#(materialize) name="open_deals"
source: open_deals is deals extend { dimension: big is amount > 6 }
`,
   },
   {
      label: "a #@ persist query source",
      stored: `import "deals.malloy"

#@ persist name="open_deals"
source: open_deals is deals -> { group_by: id, owner, amount }
`,
   },
];

const VIA = ["stored.malloy", "ext.malloy"];

function answer(o: Outcome | undefined): { servedBy: string; rows: string } {
   if (!o) return { servedBy: "missing", rows: "" };
   if ("error" in o) return { servedBy: "error", rows: o.error };
   return {
      servedBy: o.answer.via ?? o.answer.servedBy,
      rows: JSON.stringify(o.answer.rows, (_k, v: unknown) =>
         typeof v === "bigint" ? Number(v) : v,
      ),
   };
}

for (const shape of SHAPES) {
   describe(`S4: ${shape.label} over a base whose plain where: is in another file`, () => {
      let restore: () => void;
      let loader: Awaited<ReturnType<typeof loadFixture>> | undefined;
      let fixture: LoadedFixture | undefined;
      const live: Record<string, Outcome> = {};
      const served: Record<string, Outcome> = {};

      beforeAll(async () => {
         restore = installSettings({
            env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
         }).restore;
         await freshLoadPool();
         loader = await loadFixture({
            "deals.malloy": BASE,
            "stored.malloy": shape.stored,
            "ext.malloy": `import "stored.malloy"\n`,
            "deals.csv": DEALS,
         });
         for (const model of VIA) live[model] = await loader.run(model, QUERY);
         fixture = await loader.bind();
         for (const model of VIA) {
            served[model] = await fixture.run(model, QUERY);
         }
      }, 600_000);

      afterAll(async () => {
         await fixture?.dispose();
         await loader?.dispose().catch(() => {});
         restore?.();
         await freshLoadPool();
      });

      it("built the table", () => {
         expect(fixture?.buildErrors).toEqual({});
         expect(fixture?.built.map((b) => b.name)).toEqual(["open_deals"]);
      });

      for (const model of VIA) {
         it(`is served from its table through ${model}, filtered as live`, () => {
            expect(answer(live[model])).toEqual({
               servedBy: "live",
               rows: ROWS,
            });
            expect(answer(served[model])).toEqual({
               servedBy: "served_copy",
               rows: ROWS,
            });
         });
      }

      it("raises no served-copy problem", () => {
         const warnings = (
            (fixture?.pkg.getPackageMetadata().warnings ?? []) as readonly {
               code?: string;
            }[]
         ).filter((w) => w.code === "served-copy-not-served");
         expect(warnings).toEqual([]);
      });
   });
}

/**
 * gtme's own shape: the base's `where:` reads a dimension computed THROUGH a
 * join (`is_deleted is deal_activity.delete_event_count > 0`), the filtered
 * source is stored in a `storage=` destination, and a second file stores an
 * extension of it. `#@ persist` refuses a filtered pass-through at load, so
 * both are `#(materialize)`. On DuckDB and DuckLake (Parquet) lakes alike,
 * every query answers with the filtered rows and none is refused as "Access
 * denied". (On a DuckDB lake today the chain answers live,
 * `fields_not_carried`, and the extension's build is refused
 * `upstream_table_unusable` ("'is_deleted' is not defined"); this guards
 * the access denial, not where the answer comes from.)
 */
const EVENTS = "deal_id,delete_event_count\n1,0\n2,1\n3,0\n4,0\n";
const RAW_DEALS = "id,owner,amount\n1,a,10\n2,a,20\n3,b,5\n4,b,7\n";
const JOINED_DEALS = `source: deal_activity is duckdb.table('data/events.csv') extend {
  primary_key: deal_id
}

source: deals_including_deleted is duckdb.table('data/deals.csv') extend {
  join_one: deal_activity on id = deal_activity.deal_id
  dimension: is_deleted is deal_activity.delete_event_count > 0
}

#(materialize) name="gtm_deals" storage=lake
source: deals is deals_including_deleted extend { where: not is_deleted }
`;
const FUNNEL = `import "deals.malloy"

#(materialize) name="gtm_funnel_deals" storage=lake
source: funnel_deals is deals extend { dimension: big is amount > 6 }
`;
const JOINED_QUERIES: [string, string][] = [
   ["deals.malloy", "deals"],
   ["funnel.malloy", "deals"],
   ["funnel.malloy", "funnel_deals"],
];
const JOINED_ROWS = '[{"owner":"a","n":1},{"owner":"b","n":2}]';

for (const lake of ["duckdb", "parquet"] as const) {
   describe(`S4: a storage= chain over a cross-file where: on a join-computed dimension (${lake} lake)`, () => {
      let restore: () => void;
      let loader: Awaited<ReturnType<typeof loadFixture>> | undefined;
      let fixture: LoadedFixture | undefined;
      const answers: Record<string, ReturnType<typeof answer>> = {};

      beforeAll(async () => {
         restore = installSettings({
            env: {
               PERSIST_STORAGE_MODE: "on",
               PUBLISHER_MATERIALIZE: "true",
               // The Parquet lake's warehouse attaches with setupSQL.
               PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true",
            },
         }).restore;
         await freshLoadPool();
         loader = await loadFixture(
            {
               "deals.malloy": JOINED_DEALS,
               "funnel.malloy": FUNNEL,
               "data/deals.csv": RAW_DEALS,
               "data/events.csv": EVENTS,
            },
            { lake },
         );
         for (const [model, source] of JOINED_QUERIES) {
            const q = `run: ${source} -> { group_by: owner; aggregate: n is count(); order_by: owner }`;
            answers[`live ${model} ${source}`] = answer(
               await loader.run(model, q),
            );
         }
         fixture = await loader.bind();
         for (const [model, source] of JOINED_QUERIES) {
            const q = `run: ${source} -> { group_by: owner; aggregate: n is count(); order_by: owner }`;
            answers[`served ${model} ${source}`] = answer(
               await fixture.run(model, q),
            );
         }
      }, 600_000);

      afterAll(async () => {
         await fixture?.dispose();
         await loader?.dispose().catch(() => {});
         restore?.();
         await freshLoadPool();
      });

      it("built the filtered source's table", () => {
         expect(fixture?.buildErrors).toEqual({});
         expect(fixture?.built.map((b) => b.name)).toContain("deals");
      });

      for (const [model, source] of JOINED_QUERIES) {
         it(`answers ${source} through ${model} with the filtered rows, never an access denial`, () => {
            for (const side of ["live", "served"]) {
               const a = answers[`${side} ${model} ${source}`];
               expect(a.rows).not.toContain("Access denied");
               expect([side, a.servedBy === "error", a.rows]).toEqual([
                  side,
                  false,
                  JOINED_ROWS,
               ]);
            }
         });
      }
   });
}
