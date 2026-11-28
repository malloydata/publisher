// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * S4 (gtme, gtm-funnel 0.1.12–0.1.17): a stored source's inherited `where:`
 * denied as "Access denied" through the package's entry file once its table
 * is bound.
 *
 * `deals.malloy` stores `deals_including_deleted` (a query-shaped source over
 * one that reads `is_deleted` through a join) and declares
 * `deals is deals_including_deleted extend { where: not is_deleted }`;
 * `funnel.malloy` imports it and declares `funnel_deals is deals extend {
 * … }`; `index.malloy` imports both files. Queried through `index.malloy`,
 * the inherited-filter guard took `funnel_deals`'s own declaration from the
 * author's compile of `funnel.malloy` as the base its `where:` was declared
 * on (the served copy shifts each file's positions, so that twin looked like
 * one lacking the condition) and compared it against the re-rooted
 * `funnel_deals`: `is_deleted` is a computed field in one and a stored column
 * in the other, so every query was denied. Live, and through
 * `funnel.malloy`, it always answered.
 *
 * Guards the other side too: a source that rebinds `is_deleted` before the
 * inherited `where:` reads it (in a file of its own) is still denied, live
 * and served.
 *
 * Through a real package (plan, build, bind, the served copy, the governor),
 * on DuckDB.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   freshLoadPool,
   type LoadedFixture,
   loadFixture,
   type Outcome,
} from "../../../src/test_helpers/conformance/package_runner";

const EVENTS =
   "deal_id,event_type,stage_rank,is_durable\n1,x,1,true\n1,x,2,true\n2,clarify:delete,1,true\n3,x,0,true\n4,x,3,true\n";

const DEALS = `##! experimental.access_modifiers
source: deal_events is duckdb.table('events.csv')

source: deal_activity_live is deal_events -> {
  group_by: deal_id
  aggregate: delete_event_count is count() { where: event_type = 'clarify:delete' }
}

source: deal_progress is deal_events extend {
  join_one: deal_activity is deal_activity_live on deal_id = deal_activity.deal_id
} -> {
  group_by:
    deal_id
    is_deleted is deal_activity.delete_event_count > 0
  aggregate: max_stage_rank is max(stage_rank) { where: is_durable }
}

#(materialize) name="gtm_deals_including_deleted" storage=lake
source: deals_including_deleted is deal_progress
include {
  public: deal_id, is_deleted, max_stage_rank
}
extend {
  measure: deal_count is count()
}

source: deals is deals_including_deleted extend {
  where: not is_deleted
}
`;

const FUNNEL = `##! experimental.access_modifiers
import 'deals.malloy'

source: funnel_deals is deals extend {
  view: funnel is {
    where: max_stage_rank > 0
    group_by: stage_order is max_stage_rank
    aggregate: deal_count
    order_by: stage_order
  }
}
`;

const REBOUND_FILE = `##! experimental.access_modifiers
import 'deals.malloy'

source: rebound_deals is deals extend {
  except: is_deleted
  dimension: is_deleted is false
}
`;

const INDEX = `##! experimental.access_modifiers
import 'deals.malloy'
import 'funnel.malloy'
import 'rebound.malloy'

export { deals, funnel_deals, rebound_deals }
`;

const QUERIES: Record<string, string> = {
   "an ad hoc count": "run: funnel_deals -> { aggregate: n is count() }",
   "the funnel view": "run: funnel_deals -> funnel",
   "the base": "run: deals -> { aggregate: n is count() }",
};
const REBOUND = "run: rebound_deals -> { aggregate: n is count() }";
const MODELS = ["index.malloy", "funnel.malloy"];
const REBOUND_MODELS = ["index.malloy", "rebound.malloy"];

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

describe("S4: an inherited where: through an entry file that imports both the base's file and its importer", () => {
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
         "deals.malloy": DEALS,
         "funnel.malloy": FUNNEL,
         "index.malloy": INDEX,
         "rebound.malloy": REBOUND_FILE,
         "events.csv": EVENTS,
      });
      for (const model of MODELS) {
         for (const [name, q] of Object.entries(QUERIES)) {
            live[`${model} ${name}`] = await loader.run(model, q);
         }
      }
      for (const model of REBOUND_MODELS) {
         live[`${model} rebound`] = await loader.run(model, REBOUND);
      }
      fixture = await loader.bind();
      for (const model of MODELS) {
         for (const [name, q] of Object.entries(QUERIES)) {
            served[`${model} ${name}`] = await fixture.run(model, q);
         }
      }
      for (const model of REBOUND_MODELS) {
         served[`${model} rebound`] = await fixture.run(model, REBOUND);
      }
   }, 600_000);

   afterAll(async () => {
      await fixture?.dispose();
      await loader?.dispose().catch(() => {});
      restore?.();
      await freshLoadPool();
   });

   it("built the stored table", () => {
      expect(fixture?.buildErrors).toEqual({});
      expect(fixture?.built.map((b) => b.name)).toContain(
         "deals_including_deleted",
      );
   });

   for (const model of MODELS) {
      for (const name of Object.keys(QUERIES)) {
         it(`serves ${name} through ${model} from the table, filtered as live`, () => {
            const before = answer(live[`${model} ${name}`]);
            expect(before.servedBy).toBe("live");
            expect(answer(served[`${model} ${name}`])).toEqual({
               servedBy: "served_copy",
               rows: before.rows,
            });
         });
      }
   }

   for (const model of REBOUND_MODELS) {
      it(`still denies a source that rebinds the field the where: reads, through ${model}`, () => {
         for (const o of [
            live[`${model} rebound`],
            served[`${model} rebound`],
         ]) {
            const got = answer(o);
            expect(got.servedBy).toBe("error");
            expect(got.rows).toStartWith(
               'Access denied for source "rebound_deals": ',
            );
            expect(got.rows).toContain("reason: redefined_field");
         }
      });
   }
});
