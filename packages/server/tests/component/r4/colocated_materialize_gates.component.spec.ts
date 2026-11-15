// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Colocated `#(materialize)` past its own row-level gate under G3, with the
 * gated-sources switch off, adversarially, through the real package path
 * (conformance/package_harness.ts): the plan's eligibility, the colocated
 * bind, the router and `govern()`.
 *
 * G3 serves a colocated table past the entry point's own row-level gate
 * because the table never leaves the author's warehouse and the gate is
 * re-applied when it is read. These specs pin that the same holds for
 * `#(materialize)` as for v1 `#@ persist`, and no further:
 *
 * - every caller, interleaved, gets exactly live's rows (or live's denial),
 *   from the served copy, for a plain source, an extension, an extension
 *   table, a pair table, an inlined join, and a join from another source;
 * - the stored table holds every row, so no build bakes one caller's rows;
 * - what G3 does not admit stays refused and answers live: a `storage=`
 *   table, an inherited gate, a joined gate, and a given the build would
 *   substitute;
 * - removing the gate's re-proof denies the answer, and removing the
 *   re-applied gate makes the per-caller check fail.
 */

import { QueryMaterializer } from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import { DATA_FILES, RAW_MODEL } from "./conformance/data";
import {
   freshLoadPool,
   type LoadedFixture,
   loadFixture,
   type Outcome,
} from "./conformance/package_harness";
import { rowDifference, type Row } from "./harness";

// Each materialized source selects its own columns, so no two share a
// content address (a shared table is refused when any sharer is).
const MODEL = `##! experimental { givens }
import "raw.malloy"

given: REGIONS :: string[]
given: ROLE :: string
given: STATUS :: string is 'placed'

#(access_filter) region in $REGIONS
#(materialize) name="g3_gated"
source: gated is orders_raw -> {
  select: id, customer_id, region, status, amount
} extend {
  measure: total is amount.sum(), n is count()
}

source: gated_ext is gated extend {
  measure: avg_amount is amount.avg()
}

#(materialize) name="g3_ext_tbl"
source: gated_ext_tbl is gated extend { dimension: big is amount > 100 }

source: joiner is customers_raw extend {
  join_many: g is gated on g.customer_id = id
  measure: n_customers is count()
}

#(materialize) name="g3_join_tbl"
source: mat_joiner is customers_raw extend {
  join_many: g is gated on g.customer_id = id
  measure: n_customers is count()
}

#(authorize) $ROLE = 'admin'
#(materialize) name="g3_lock"
source: locked is orders_raw -> { select: id, region } extend {
  measure: n is count()
}

#(authorize) $ROLE = 'admin'
#(access_filter) region in $REGIONS
#(materialize) name="g3_lock_rows"
source: locked_rows is orders_raw -> { select: id, region, status } extend {
  measure: n is count()
}

#(access_filter) region in $REGIONS
#(materialize) name="g3_pairs" inline=["lines"]
source: gated_pairs is orders_raw -> {
  select: id, region, customer_id
} extend {
  primary_key: id
  join_many: lines is lines_raw on lines.order_id = id
  measure: n is count(), qty is lines.qty.sum()
}

#(access_filter) region in $REGIONS
#(materialize) name="g3_inline" inline=["c"]
source: gated_inline is orders_raw -> {
  select: id, region, customer_id, status
} extend {
  primary_key: id
  join_one: c is customers_raw on c.id = customer_id
  measure: n is count()
}

#(access_filter) region in $REGIONS
#(materialize) storage="lake"
source: gated_sto is orders_raw -> { select: id, region, amount } extend {
  measure: n is count()
}

#(access_filter) region in $REGIONS
#(materialize) name="g3_baked"
source: baked is orders_raw -> {
  where: status = $STATUS
  select: id, region, status, customer_id
} extend {
  measure: n is count()
}
`;

const FILES = { ...DATA_FILES, "raw.malloy": RAW_MODEL, "m.malloy": MODEL };

interface Case {
   name: string;
   malloy: string;
   /** The answer's route when a caller is answered: the served copy, or live. */
   via: "served_copy" | "live";
   /** Every answered row's `region` is one of the caller's REGIONS. */
   rowsInCallersRegions?: boolean;
}

const CASES: Case[] = [
   {
      name: "the gated source",
      malloy: "run: gated -> { group_by: region; aggregate: total, n }",
      via: "served_copy",
      rowsInCallersRegions: true,
   },
   {
      name: "an extension of it, its gate inherited",
      malloy:
         "run: gated_ext -> { group_by: region; aggregate: total, avg_amount }",
      via: "served_copy",
      rowsInCallersRegions: true,
   },
   {
      name: "a materialized extension of it, read through the parent's table",
      malloy: "run: gated_ext_tbl -> { group_by: region, big; aggregate: n }",
      via: "served_copy",
      rowsInCallersRegions: true,
   },
   {
      name: "a pair table under its own gate",
      malloy: "run: gated_pairs -> { group_by: region; aggregate: n, qty }",
      via: "served_copy",
      rowsInCallersRegions: true,
   },
   {
      name: "an inlined join_one under its own gate",
      malloy:
         "run: gated_inline -> { group_by: c.segment, region; aggregate: n }",
      via: "served_copy",
      rowsInCallersRegions: true,
   },
   {
      name: "a lock and a row filter together",
      malloy: "run: locked_rows -> { group_by: region; aggregate: n }",
      via: "served_copy",
      rowsInCallersRegions: true,
   },
   {
      name: "a lock alone",
      malloy: "run: locked -> { group_by: region; aggregate: n }",
      via: "served_copy",
   },
   // A source joining the gated one: Publisher does not enforce a joined
   // source's gate on the parent, live or served (the `join-bypass` warning),
   // so the served answer must be exactly live's, no more and no fewer rows.
   {
      name: "a source joining the gated one",
      malloy:
         "run: joiner -> { group_by: g.region; aggregate: n_customers, g.n }",
      via: "served_copy",
   },
   {
      name: "a materialized source joining the gated one, refused itself",
      malloy:
         "run: mat_joiner -> { group_by: g.region; aggregate: n_customers, g.n }",
      via: "served_copy",
   },
   {
      name: "a storage= table under its own gate, refused",
      malloy: "run: gated_sto -> { group_by: region; aggregate: n }",
      via: "live",
   },
   {
      name: "a gated source whose build reads a given, refused",
      malloy: "run: baked -> { group_by: region, status; aggregate: n }",
      via: "live",
   },
];

const NORTH = { REGIONS: ["north"], ROLE: "admin", STATUS: "placed" };
const EAST_WEST = { REGIONS: ["east", "west"], ROLE: "viewer" };
const NO_GROUP = { REGIONS: [] as string[], ROLE: "admin" };
const NOBODY = {};
/** Interleaved, then repeated, so a graft or cache shared across callers shows. */
const ORDER = [NORTH, EAST_WEST, NO_GROUP, NOBODY, EAST_WEST, NORTH, NOBODY];

type Givens = Record<string, string | string[]>;

/** Rows equal to live's, or the same denial, for one caller. */
function expectAsLive(
   kase: Case,
   givens: Givens,
   live: Outcome,
   served: Outcome,
): void {
   if ("error" in live) {
      expect("error" in served ? served.error : "answered").toBe(live.error);
      return;
   }
   if ("error" in served) throw new Error(served.error);
   expect(served.answer.via ?? served.answer.servedBy).toBe(kase.via);
   expect(rowDifference(live.answer.rows, served.answer.rows)).toBeUndefined();
   if (kase.rowsInCallersRegions) {
      for (const row of served.answer.rows) {
         expect(givens.REGIONS as string[]).toContain(row.region as string);
      }
   }
}

const G3_ONLY = {
   PERSIST_STORAGE_MODE: "on",
   PUBLISHER_MATERIALIZE: "true",
   PUBLISHER_MATERIALIZE_GATED_SOURCES: "false",
};

describe("colocated #(materialize) past its own row-level gate under G3 (gated sources off)", () => {
   let restore: () => void = () => {};
   let fixture: LoadedFixture;
   const live = new Map<string, Outcome>();
   const key = (kase: Case, givens: object) =>
      `${kase.name} ${JSON.stringify(givens)}`;

   beforeAll(async () => {
      restore = installSettings({ env: G3_ONLY }).restore;
      // The plan (and its eligibility) is computed in a load worker, which
      // reads its settings at spawn.
      await freshLoadPool();
      const loader = await loadFixture(FILES);
      for (const kase of CASES) {
         for (const givens of [NORTH, EAST_WEST, NO_GROUP, NOBODY]) {
            live.set(
               key(kase, givens),
               await loader.run("m.malloy", kase.malloy, givens as Givens),
            );
         }
      }
      fixture = await loader.bind();
   }, 120_000);
   afterAll(async () => {
      await fixture?.dispose();
      restore();
      await freshLoadPool();
   });

   it("builds and serves exactly the gates G3 admits, and refuses the rest", () => {
      expect(fixture.buildErrors).toEqual({});
      expect([...fixture.verdicts].sort()).toEqual(
         [
            "gated:serves",
            "gated_inline:serves",
            "gated_pairs:serves",
            "locked:serves",
            "locked_rows:serves",
            // An inherited gate, a joined gate, storage=, a given the build
            // would substitute: refused as before.
            "gated_ext_tbl:live:refused",
            "mat_joiner:live:refused",
            "gated_sto:live:refused",
            "baked:live:refused",
         ].sort(),
      );
      expect(
         fixture.warnings
            .filter((w) => w.code === "persist-source-refused")
            .map((w) => w.subject)
            .sort(),
      ).toEqual(["baked", "gated_ext_tbl", "gated_sto", "mat_joiner"]);
   });

   for (const kase of CASES) {
      it(`${kase.name}: every caller, interleaved, gets live's rows or live's denial`, async () => {
         const answered: Record<string, Row[]> = {};
         for (const givens of ORDER) {
            const served = await fixture.run(
               "m.malloy",
               kase.malloy,
               givens as Givens,
            );
            expectAsLive(
               kase,
               givens as Givens,
               live.get(key(kase, givens))!,
               served,
            );
            if (!("error" in served)) {
               const k = JSON.stringify(givens);
               // The same caller agrees with itself however calls interleave.
               if (answered[k]) {
                  expect(
                     rowDifference(answered[k], served.answer.rows),
                  ).toBeUndefined();
               }
               answered[k] = served.answer.rows;
            }
         }
         if (kase.rowsInCallersRegions) {
            // Two callers' gates differ, so their answers do.
            const regions = (g: object) =>
               (answered[JSON.stringify(g)] ?? []).map((r) => r.region).sort();
            expect(regions(NORTH)).not.toEqual(regions(EAST_WEST));
         }
      });
   }

   it("denies a caller the lock refuses exactly as live does", async () => {
      const malloy = "run: locked_rows -> { group_by: region; aggregate: n }";
      const l = live.get(key(CASES[5], EAST_WEST))!;
      expect("error" in l).toBe(true);
      const served = await fixture.run("m.malloy", malloy, EAST_WEST as Givens);
      expect("error" in served && served.error).toBe("error" in l && l.error);
   });

   it("never bakes a caller's rows into a table: each holds every row, the gate is applied at read", async () => {
      const warehouse = (await fixture.pkg.getMalloyConnection("duckdb")) as {
         runSQL(sql: string): Promise<{ rows: Record<string, unknown>[] }>;
      };
      const count = async (table: string) => {
         const { rows } = await warehouse.runSQL(
            `SELECT COUNT(*) AS n, COUNT(DISTINCT region) AS regions FROM "${table}"`,
         );
         return [Number(rows[0].n), Number(rows[0].regions)];
      };
      // 48 orders over four regions (conformance/data.ts), whoever asks.
      for (const table of [
         "g3_gated",
         "g3_inline",
         "g3_lock",
         "g3_lock_rows",
      ]) {
         expect(await count(table)).toEqual([48, 4]);
      }
      // The pair table: one row per order and line pair, still every region.
      expect((await count("g3_pairs"))[1]).toBe(4);
      // The build SQL names no caller's region: the gate is not in it.
      const plan = fixture.pkg.getBuildPlan()?.sources ?? {};
      const gated = Object.values(plan).find((s) => s.name === "gated");
      expect(gated?.sql).toBeDefined();
      expect(gated?.sql).not.toMatch(/north|south|east|west/i);
   });

   describe("each guard is load-bearing", () => {
      const governor = () =>
         (
            fixture.pkg.getModel("m.malloy") as unknown as {
               governor: Record<string, unknown>;
            }
         ).governor;

      /** Run `body` with `object[method]` replaced, restoring it after. */
      async function patched(
         object: Record<string, unknown>,
         method: string,
         replace: (original: (...args: unknown[]) => unknown) => unknown,
         body: () => Promise<void>,
      ): Promise<void> {
         const original = object[method] as (...args: unknown[]) => unknown;
         object[method] = replace(original.bind(object));
         try {
            await body();
         } finally {
            object[method] = original;
         }
      }

      const GATED = CASES[0];

      it("baseline: the caller's rows, from the served copy", async () => {
         const served = await fixture.run(
            "m.malloy",
            GATED.malloy,
            NORTH as Givens,
         );
         expectAsLive(
            GATED,
            NORTH as Givens,
            live.get(key(GATED, NORTH))!,
            served,
         );
      });

      it("with the gate's re-proof finding no gate, the served answer is denied, never answered", async () => {
         await patched(
            governor(),
            "probeEntryPointGates",
            () => async () => [],
            async () => {
               const served = await fixture.run(
                  "m.malloy",
                  GATED.malloy,
                  NORTH as Givens,
               );
               expect("error" in served).toBe(true);
            },
         );
      });

      it("with the re-applied gate removed, the per-caller check fails: the table holds other callers' rows", async () => {
         // As if neither the graft nor its proof ran: what reaches the
         // stored table is the query alone.
         await patched(
            governor(),
            "govern",
            () =>
               async (_request: unknown, compiled: { runnable: unknown }) => ({
                  runnable: compiled.runnable,
                  decision: { enforced: undefined, grafts: [] },
               }),
            async () => {
               const served = await fixture.run(
                  "m.malloy",
                  GATED.malloy,
                  NORTH as Givens,
               );
               if ("error" in served) throw new Error(served.error);
               expect(served.answer.via).toBe("served_copy");
               expect(() =>
                  expectAsLive(
                     GATED,
                     NORTH as Givens,
                     live.get(key(GATED, NORTH))!,
                     served,
                  ),
               ).toThrow();
               expect(
                  new Set(served.answer.rows.map((r) => r.region)).size,
               ).toBeGreaterThan(1);
            },
         );
      });
   });
});

describe("the same package with the gated-sources switch on, as before", () => {
   it("serves the same gates, and the storage= table too", async () => {
      const restore = installSettings({
         env: { ...G3_ONLY, PUBLISHER_MATERIALIZE_GATED_SOURCES: "true" },
      }).restore;
      await freshLoadPool();
      const loader = await loadFixture(FILES);
      try {
         const liveOut = await loader.run(
            "m.malloy",
            "run: gated_sto -> { group_by: region; aggregate: n }",
            NORTH as Givens,
         );
         const fixture = await loader.bind();
         expect([...fixture.verdicts].sort()).toEqual(
            [
               "gated:serves",
               "gated_inline:serves",
               "gated_pairs:serves",
               "locked:serves",
               "locked_rows:serves",
               "gated_sto:serves",
               "gated_ext_tbl:live:refused",
               "mat_joiner:live:refused",
               "baked:live:refused",
            ].sort(),
         );
         const served = await fixture.run(
            "m.malloy",
            "run: gated_sto -> { group_by: region; aggregate: n }",
            NORTH as Givens,
         );
         if ("error" in served || "error" in liveOut) throw new Error("failed");
         expect(served.answer.via).toBe("served_copy");
         expect(served.answer.servedFrom).toBe("storage");
         expect(
            rowDifference(liveOut.answer.rows, served.answer.rows),
         ).toBeUndefined();
      } finally {
         await loader.dispose();
         restore();
         await freshLoadPool();
      }
   }, 120_000);
});

describe("a served answer that names a storage= table beside a colocated one, spelled another way (G3, gated sources off)", () => {
   // A gated colocated table and an ungated storage= table, both bound in
   // the served copy. G3 admits an answer past the gate only when every
   // table it reads is colocated.
   const MIXED = `##! experimental { givens }
import "raw.malloy"

given: REGIONS :: string[]

#(materialize) name="g3w_cust" storage="lake"
source: cust_sto is customers_raw -> { select: id, segment } extend {
  primary_key: id
}

#(access_filter) region in $REGIONS
#(materialize) name="g3w_gated"
source: gated_mix is orders_raw -> {
  select: id, customer_id, region, amount
} extend {
  join_one: c is cust_sto on c.id = customer_id
  measure: n is count()
}
`;
   const CALLER: Givens = { REGIONS: ["north"] };
   const QUERY = "run: gated_mix -> { group_by: region; aggregate: n }";
   let restore: () => void = () => {};
   let fixture: LoadedFixture;
   let live: Outcome;

   beforeAll(async () => {
      restore = installSettings({ env: G3_ONLY }).restore;
      await freshLoadPool();
      const loader = await loadFixture({
         ...DATA_FILES,
         "raw.malloy": RAW_MODEL,
         "m.malloy": MIXED,
      });
      live = await loader.run("m.malloy", QUERY, CALLER);
      fixture = await loader.bind();
   }, 120_000);
   afterAll(async () => {
      await fixture?.dispose();
      restore();
      await freshLoadPool();
   });

   /** What the served copy answered with, each time it was asked. */
   type Served = {
      tablesRead: { tier: string }[];
      gateTablesRead?: { tier: string }[];
   };
   async function runRecording(respell: boolean): Promise<{
      outcome: Outcome;
      served: Served[];
   }> {
      const copy = (
         fixture.pkg.getModel("m.malloy") as unknown as {
            servedCopy: { loadQuery: (...args: unknown[]) => Promise<Served> };
         }
      ).servedCopy;
      const loadQuery = copy.loadQuery;
      const getSQL = QueryMaterializer.prototype.getSQL;
      const served: Served[] = [];
      copy.loadQuery = async (...args) => {
         const answer = await loadQuery.apply(copy, args);
         served.push(answer);
         return answer;
      };
      if (respell) {
         // The served copy's SQL also reads the storage table, in a spelling
         // its exact match does not find (other quoting and case).
         QueryMaterializer.prototype.getSQL = async function (
            this: QueryMaterializer,
            ...args: Parameters<typeof getSQL>
         ) {
            const sql = await getSQL.apply(this, args);
            return sql.replace(
               /FROM "g3w_gated" as base/,
               'FROM "g3w_gated" as base LEFT JOIN (SELECT 1 AS one FROM "lake".G3W_CUST LIMIT 0) AS g3w_probe ON TRUE',
            );
         };
      }
      try {
         const outcome = await fixture.run("m.malloy", QUERY, CALLER);
         return { outcome, served };
      } finally {
         copy.loadQuery = loadQuery;
         QueryMaterializer.prototype.getSQL = getSQL;
      }
   }

   it("binds both tables", () => {
      expect([...fixture.verdicts].sort()).toEqual([
         "cust_sto:serves",
         "gated_mix:serves",
      ]);
   });

   it("baseline: a colocated-only answer is served from the copy past its gate", async () => {
      const { outcome, served } = await runRecording(false);
      if ("error" in outcome || "error" in live) throw new Error("failed");
      expect(outcome.answer.via).toBe("served_copy");
      expect(
         rowDifference(live.answer.rows, outcome.answer.rows),
      ).toBeUndefined();
      expect(served.at(-1)?.gateTablesRead?.map((b) => b.tier)).toEqual([
         "colocated",
      ]);
   });

   it("judges the answer to read the storage table too, and answers live, not from the copy past its gate", async () => {
      const { outcome, served } = await runRecording(true);
      // The served copy's exact match finds the colocated table alone…
      expect(served.at(-1)?.tablesRead.map((b) => b.tier)).toEqual([
         "colocated",
      ]);
      // …and the gate judgement sees the storage binding beside it.
      expect(
         served
            .at(-1)
            ?.gateTablesRead?.map((b) => b.tier)
            .sort(),
      ).toEqual(["colocated", "storage"]);
      // So the router keeps it off G3: answered exactly as live.
      if ("error" in live) {
         expect("error" in outcome && outcome.error).toBe(live.error);
         return;
      }
      if ("error" in outcome) throw new Error(outcome.error);
      expect(outcome.answer.via ?? outcome.answer.servedBy).toBe("live");
      expect(
         rowDifference(live.answer.rows, outcome.answer.rows),
      ).toBeUndefined();
   });
});
