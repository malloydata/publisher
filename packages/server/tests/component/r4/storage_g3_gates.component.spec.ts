// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A `storage=` table past its source's own row-level gate under G3, with the
 * gated-sources switch off, adversarially, through the real package path
 * (conformance/package_harness.ts): the plan's eligibility, the storage bind,
 * the router and `govern()`. The `storage=` twin of
 * colocated_materialize_gates.component.spec.ts.
 *
 * A maintainer-approved exception made ahead of the question-14 review, for
 * consistency: colocated and `storage=` behave the same under a source's own
 * row-level gate (an `#(access_filter)` row filter, an `#(authorize)` lock,
 * the two together, or a lock inherited from a base), v1 `#@ persist` and
 * `#(materialize)` alike. These specs pin that, and no further:
 *
 * - every caller, interleaved, gets exactly live's rows (or live's denial)
 *   from the served copy, read from the destination;
 * - the stored table holds every row, and its build SQL no gate value, so no
 *   build bakes one caller's rows;
 * - the gate cannot be read around: a caller's `rename:` or `except:` of the
 *   gate column, a raw read of the destination table, and a caller join are
 *   answered exactly as live;
 * - what G3 does not admit stays refused and answers live on this tier as on
 *   colocated: an inherited row filter, a joined gate, a given the build
 *   would substitute;
 * - an answer reading a G3 table beside one that is not answers live;
 * - removing the re-applied gate makes the per-caller check fail, and so does
 *   judging an answer by the exact-match tables alone (no
 *   `tablesReadForGates`).
 */

import { QueryMaterializer } from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it, spyOn } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import { DATA_FILES, RAW_MODEL } from "./conformance/data";
import {
   freshLoadPool,
   type LoadedFixture,
   loadFixture,
   type Outcome,
} from "../../../src/test_helpers/conformance/package_runner";
import { rowDifference, type Row } from "./harness";

// Each stored source selects its own columns, so no two share a content
// address (a shared table is refused when any sharer is). Every `mz_` source
// is `#(materialize)`, every `v1_` source `#@ persist`, all `storage=`.
const MODEL = `##! experimental { givens persistence }
import "raw.malloy"

given: REGIONS :: string[]
given: ROLE :: string
given: STATUS :: string is 'placed'

#(access_filter) region in $REGIONS
#(materialize) name="g3s_mz" storage="lake"
source: mz_gated is orders_raw -> {
  select: id, customer_id, region, status, amount
} extend {
  measure: total is amount.sum(), n is count()
}

#(access_filter) region in $REGIONS
#@ persist name="g3s_v1" storage="lake"
source: v1_gated is orders_raw -> {
  select: id, customer_id, region, amount
} extend {
  measure: total is amount.sum(), n is count()
}

source: mz_ext is mz_gated extend {
  measure: avg_amount is amount.avg()
}

source: v1_ext is v1_gated extend {
  measure: avg_amount is amount.avg()
}

#(authorize) $ROLE = 'admin'
#(materialize) name="g3s_mz_lock" storage="lake"
source: mz_lock is orders_raw -> { select: id, region } extend {
  measure: n is count()
}

#(authorize) $ROLE = 'admin'
#@ persist name="g3s_v1_lock" storage="lake"
source: v1_lock is orders_raw -> { select: id, region, status } extend {
  measure: n is count()
}

#(authorize) $ROLE = 'admin'
#(access_filter) region in $REGIONS
#(materialize) name="g3s_mz_lr" storage="lake"
source: mz_lock_rows is orders_raw -> {
  select: id, region, status, customer_id
} extend {
  measure: n is count()
}

#(authorize) $ROLE = 'admin'
#(access_filter) region in $REGIONS
#@ persist name="g3s_v1_lr" storage="lake"
source: v1_lock_rows is orders_raw -> { select: id, region, customer_id } extend {
  measure: n is count()
}

#(authorize) $ROLE = 'admin'
source: locked_base is orders_raw extend {}

#(materialize) name="g3s_mz_inh_lock" storage="lake"
source: mz_inh_lock is locked_base -> { select: id, region, amount } extend {
  measure: n is count()
}

#@ persist name="g3s_v1_inh_lock" storage="lake"
source: v1_inh_lock is locked_base -> {
  select: id, region, status, amount
} extend {
  measure: n is count()
}

#(access_filter) region in $REGIONS
source: rows_base is orders_raw extend {}

#(materialize) name="g3s_mz_inh_rows" storage="lake"
source: mz_inh_rows is rows_base -> { select: id, region, discount } extend {
  measure: n is count()
}

#@ persist name="g3s_v1_inh_rows" storage="lake"
source: v1_inh_rows is rows_base -> {
  select: id, region, status, discount
} extend {
  measure: n is count()
}

#(materialize) name="g3s_mz_joiner" storage="lake"
source: mz_joiner is customers_raw extend {
  join_many: g is mz_gated on g.customer_id = id
  measure: n_customers is count()
}

#(access_filter) region in $REGIONS
#(materialize) name="g3s_mz_baked" storage="lake"
source: mz_baked is orders_raw -> {
  where: status = $STATUS
  select: id, region, status, discount, amount
} extend {
  measure: n is count()
}

#(access_filter) region in $REGIONS
#@ persist name="g3s_v1_baked" storage="lake"
source: v1_baked is orders_raw -> {
  where: status = $STATUS
  select: id, region, customer_id, discount
} extend {
  measure: n is count()
}

#(materialize) name="g3s_cust" storage="lake"
source: cust_sto is customers_raw -> { select: id, segment } extend {
  primary_key: id
}

#(access_filter) region in $REGIONS
#(materialize) name="g3s_mz_mix" storage="lake"
source: mz_mix is orders_raw -> {
  select: id, customer_id, region, amount, discount
} extend {
  join_one: c is cust_sto on c.id = customer_id
  measure: n is count()
}

#(access_filter) region in $REGIONS
#@ persist name="g3s_v1_mix" storage="lake"
source: v1_mix is orders_raw -> {
  select: id, customer_id, region, discount
} extend {
  join_one: c is cust_sto on c.id = customer_id
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

/** One case per annotation, the same query on each. */
function both(
   name: string,
   query: (prefix: "mz" | "v1") => string,
   rest: Omit<Case, "name" | "malloy">,
): Case[] {
   return (["mz", "v1"] as const).map((prefix) => ({
      name: `${prefix === "mz" ? "#(materialize)" : "#@ persist"} storage=: ${name}`,
      malloy: query(prefix),
      ...rest,
   }));
}

const SERVED = { via: "served_copy" as const };
const SERVED_ROWS = { via: "served_copy" as const, rowsInCallersRegions: true };
const LIVE = { via: "live" as const };

const CASES: Case[] = [
   ...both(
      "an #(access_filter) row filter",
      (p) => `run: ${p}_gated -> { group_by: region; aggregate: total, n }`,
      SERVED_ROWS,
   ),
   ...both(
      "an extension of it, its gate inherited",
      (p) =>
         `run: ${p}_ext -> { group_by: region; aggregate: total, avg_amount }`,
      SERVED_ROWS,
   ),
   ...both(
      "an #(authorize) lock",
      (p) => `run: ${p}_lock -> { group_by: region; aggregate: n }`,
      SERVED,
   ),
   ...both(
      "an #(authorize) lock with a row filter",
      (p) => `run: ${p}_lock_rows -> { group_by: region; aggregate: n }`,
      SERVED_ROWS,
   ),
   ...both(
      "an #(authorize) lock inherited from its base",
      (p) => `run: ${p}_inh_lock -> { group_by: region; aggregate: n }`,
      SERVED,
   ),
   ...both(
      "an answer that reads only the gated table, not the table it joins",
      (p) => `run: ${p}_mix -> { group_by: region; aggregate: n }`,
      SERVED_ROWS,
   ),
   // Refused on this tier as on colocated: built nowhere, answered live.
   ...both(
      "a row filter inherited from its base, refused",
      (p) => `run: ${p}_inh_rows -> { group_by: region; aggregate: n }`,
      LIVE,
   ),
   ...both(
      "a gated source whose build reads a given, refused",
      (p) => `run: ${p}_baked -> { group_by: region; aggregate: n }`,
      LIVE,
   ),
   // Refused itself; a query on it is answered live, as on colocated, but
   // not from the joined gated table either: that table is in the
   // destination, which the live source cannot join across connections
   // (`CROSS_CONNECTION`), so live it is, not a gate difference.
   {
      name: "#(materialize) storage=: a source joining a gated one, refused itself",
      malloy:
         "run: mz_joiner -> { group_by: g.region; aggregate: n_customers, g.n }",
      via: "live",
   },
   // A G3 table beside a table G3 did not admit (an ungated storage= one):
   // the answer stays live.
   ...both(
      "an answer reading the gated table and an ungated storage= table",
      (p) => `run: ${p}_mix -> { group_by: c.segment, region; aggregate: n }`,
      LIVE,
   ),
   // A caller join into a gated source: Publisher does not enforce a joined
   // source's gate on the parent, live or served (the `join-bypass`
   // warning), so the served answer must be exactly live's.
   ...both(
      "a caller join into it",
      (p) =>
         `run: customers_raw extend { join_many: g is ${p}_gated on g.customer_id = id } -> { group_by: g.region; aggregate: c is count() }`,
      LIVE,
   ),
];

/**
 * Reads around the gate: each must be answered exactly as live answers it,
 * which for every one is a refusal.
 */
const READ_AROUND: { name: string; malloy: string }[] = [
   ...(["mz", "v1"] as const).flatMap((p) => [
      {
         name: `a caller rename: of the gate column (${p})`,
         malloy: `run: ${p}_gated extend { rename: r is region } -> { group_by: r; aggregate: n }`,
      },
      {
         name: `a caller except: of the gate column (${p})`,
         malloy: `run: ${p}_gated extend { except: region } -> { aggregate: n }`,
      },
   ]),
   ...["duckdb", "lake"].flatMap((connection) =>
      ["g3s_mz", "g3s_v1"].flatMap((table) => [
         {
            name: `${connection}.table() on the destination table ${table}`,
            malloy: `run: ${connection}.table('${table}') -> { group_by: region; aggregate: n is count() }`,
         },
         {
            name: `${connection}.sql() on the destination table ${table}`,
            malloy: `run: ${connection}.sql("SELECT * FROM ${table}") -> { group_by: region; aggregate: n is count() }`,
         },
      ]),
   ),
];

const NORTH = { REGIONS: ["north"], ROLE: "admin" };
const EAST_WEST = { REGIONS: ["east", "west"], ROLE: "viewer" };
const NO_GROUP = { REGIONS: [] as string[], ROLE: "admin" };
const NOBODY = {};
const CALLERS = [NORTH, EAST_WEST, NO_GROUP, NOBODY];
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
   if (kase.via === "served_copy") {
      expect(served.answer.servedFrom).toBe("storage");
   }
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

describe("storage= past its own row-level gate under G3 (gated sources off)", () => {
   let restore: () => void = () => {};
   let loader: Awaited<ReturnType<typeof loadFixture>>;
   let fixture: LoadedFixture;
   const live = new Map<string, Outcome>();
   const key = (malloy: string, givens: object) =>
      `${malloy} ${JSON.stringify(givens)}`;

   beforeAll(async () => {
      restore = installSettings({ env: G3_ONLY }).restore;
      // The plan (and its eligibility) is computed in a load worker, which
      // reads its settings at spawn.
      await freshLoadPool();
      loader = await loadFixture(FILES);
      for (const malloy of [
         ...CASES.map((c) => c.malloy),
         ...READ_AROUND.map((c) => c.malloy),
      ]) {
         for (const givens of CALLERS) {
            live.set(
               key(malloy, givens),
               await loader.run("m.malloy", malloy, givens as Givens),
            );
         }
      }
      fixture = await loader.bind();
   }, 180_000);
   afterAll(async () => {
      await fixture?.dispose();
      restore();
      await freshLoadPool();
   });

   it("builds and serves exactly the gates G3 admits, and refuses the rest", () => {
      expect(fixture.buildErrors).toEqual({});
      expect([...fixture.verdicts].sort()).toEqual(
         [
            "mz_gated:serves",
            "v1_gated:serves",
            // A v1 extension inherits `#@ persist` and shares the table, its
            // gate grafted through the extension as on colocated.
            "v1_ext:serves",
            "mz_lock:serves",
            "v1_lock:serves",
            "mz_lock_rows:serves",
            "v1_lock_rows:serves",
            "mz_inh_lock:serves",
            "v1_inh_lock:serves",
            "cust_sto:serves",
            "mz_mix:serves",
            "v1_mix:serves",
            // An inherited row filter, a joined gate, a given the build would
            // substitute: refused as on colocated.
            "mz_inh_rows:live:refused",
            "v1_inh_rows:live:refused",
            "mz_joiner:live:refused",
            "mz_baked:live:refused",
            "v1_baked:live:refused",
         ].sort(),
      );
      expect(
         fixture.built
            .filter((b) => b.tier === "storage")
            .map((b) => b.name)
            .sort(),
      ).toEqual(
         [
            "mz_gated",
            "v1_gated",
            "mz_lock",
            "v1_lock",
            "mz_lock_rows",
            "v1_lock_rows",
            "mz_inh_lock",
            "v1_inh_lock",
            "cust_sto",
            "mz_mix",
            "v1_mix",
         ].sort(),
      );
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
               live.get(key(kase.malloy, givens))!,
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
            // Two callers' gates differ, so their answers do (or one is
            // denied).
            const regions = (g: object) =>
               (answered[JSON.stringify(g)] ?? []).map((r) => r.region).sort();
            expect(regions(NORTH)).not.toEqual(regions(EAST_WEST));
         }
      });
   }

   it("denies a caller a lock refuses exactly as live does, on both annotations", async () => {
      for (const malloy of [
         "run: mz_lock -> { group_by: region; aggregate: n }",
         "run: v1_lock -> { group_by: region; aggregate: n }",
         "run: mz_lock_rows -> { group_by: region; aggregate: n }",
         "run: v1_lock_rows -> { group_by: region; aggregate: n }",
         "run: mz_inh_lock -> { group_by: region; aggregate: n }",
         "run: v1_inh_lock -> { group_by: region; aggregate: n }",
      ]) {
         const l = live.get(key(malloy, EAST_WEST))!;
         expect("error" in l).toBe(true);
         const served = await fixture.run(
            "m.malloy",
            malloy,
            EAST_WEST as Givens,
         );
         expect("error" in served && served.error).toBe(
            "error" in l && l.error,
         );
      }
   });

   for (const read of READ_AROUND) {
      it(`answers ${read.name} exactly as live, for every caller`, async () => {
         for (const givens of CALLERS) {
            const l = live.get(key(read.malloy, givens))!;
            const served = await fixture.run(
               "m.malloy",
               read.malloy,
               givens as Givens,
            );
            // Every read-around is refused live, and so served.
            expect("error" in l).toBe(true);
            expect("error" in served && served.error).toBe(
               "error" in l && l.error,
            );
         }
      });
   }

   it("never bakes a caller's rows into a table: each holds every row, and no build names a gate value", async () => {
      const count = async (table: string) => {
         const { rows } = await loader.lake.runSQL(
            `SELECT COUNT(*) AS n, COUNT(DISTINCT region) AS regions FROM ${table}`,
         );
         return [Number(rows[0].n), Number(rows[0].regions)];
      };
      const plan = fixture.pkg.getBuildPlan()?.sources ?? {};
      const gated = [
         "mz_gated",
         "v1_gated",
         "mz_lock",
         "v1_lock",
         "mz_lock_rows",
         "v1_lock_rows",
         "mz_inh_lock",
         "v1_inh_lock",
      ];
      for (const name of gated) {
         const source = Object.values(plan).find((s) => s.name === name);
         expect(source?.sql).toBeDefined();
         // The build SQL names no caller's region nor role: the gate is not
         // in it.
         expect(source?.sql).not.toMatch(/north|south|east|west|admin/i);
         const table = fixture.tables[source!.sourceEntityId];
         expect(table).toBeDefined();
         // 48 orders over four regions (conformance/data.ts), whoever asks.
         expect(await count(table)).toEqual([48, 4]);
      }
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

      const GATED = CASES.filter((c) => /_gated -> /.test(c.malloy));

      it("baseline: each caller's rows, from the destination", async () => {
         for (const kase of GATED) {
            const served = await fixture.run(
               "m.malloy",
               kase.malloy,
               NORTH as Givens,
            );
            expectAsLive(
               kase,
               NORTH as Givens,
               live.get(key(kase.malloy, NORTH))!,
               served,
            );
         }
      });

      it("with the gate's re-proof finding no gate, the served answer is denied, never answered", async () => {
         await patched(
            governor(),
            "probeEntryPointGates",
            () => async () => [],
            async () => {
               for (const kase of GATED) {
                  const served = await fixture.run(
                     "m.malloy",
                     kase.malloy,
                     NORTH as Givens,
                  );
                  expect("error" in served).toBe(true);
               }
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
               for (const kase of GATED) {
                  const served = await fixture.run(
                     "m.malloy",
                     kase.malloy,
                     NORTH as Givens,
                  );
                  if ("error" in served) throw new Error(served.error);
                  expect(served.answer.via).toBe("served_copy");
                  expect(() =>
                     expectAsLive(
                        kase,
                        NORTH as Givens,
                        live.get(key(kase.malloy, NORTH))!,
                        served,
                     ),
                  ).toThrow();
                  expect(
                     new Set(served.answer.rows.map((r) => r.region)).size,
                  ).toBeGreaterThan(1);
               }
            },
         );
      });

      describe("an answer reading the gated table and an ungated storage= table spelled another way", () => {
         /** What the served copy answered with, each time it was asked. */
         type Served = {
            tablesRead: { sourceName: string }[];
            gateTablesRead?: { sourceName: string }[];
         };
         /**
          * Run `malloy` with the served copy's SQL also reading the ungated
          * `g3s_cust` table, quoted unlike its bound path, so the served
          * copy's exact match misses it. `dropJudged` drops the fail-closed
          * read (`tablesReadForGates`), as if it were removed.
          */
         async function respelled(
            malloy: string,
            dropJudged: boolean,
         ): Promise<{ outcome: Outcome; served: Served[] }> {
            const copy = (
               fixture.pkg.getModel("m.malloy") as unknown as {
                  servedCopy: {
                     loadQuery: (...args: unknown[]) => Promise<Served>;
                  };
               }
            ).servedCopy;
            const loadQuery = copy.loadQuery;
            const served: Served[] = [];
            copy.loadQuery = async (...args) => {
               const answer = await loadQuery.apply(copy, args);
               served.push(answer);
               return dropJudged
                  ? { ...answer, gateTablesRead: undefined }
                  : answer;
            };
            const getSQL = QueryMaterializer.prototype.getSQL;
            const spy = spyOn(
               QueryMaterializer.prototype,
               "getSQL",
            ).mockImplementation(async function (
               this: QueryMaterializer,
               ...args: Parameters<typeof getSQL>
            ) {
               const sql = await getSQL.apply(this, args);
               return sql.replace(
                  /( as base)/,
                  "$1 LEFT JOIN (SELECT 1 AS one FROM lake.G3S_CUST LIMIT 0) AS g3s_probe ON TRUE",
               );
            });
            try {
               const outcome = await fixture.run(
                  "m.malloy",
                  malloy,
                  NORTH as Givens,
               );
               return { outcome, served };
            } finally {
               copy.loadQuery = loadQuery;
               spy.mockRestore();
            }
         }

         for (const p of ["mz", "v1"] as const) {
            const malloy = `run: ${p}_mix -> { group_by: region; aggregate: n }`;
            it(`judges it to read both, and answers live (${p})`, async () => {
               const { outcome, served } = await respelled(malloy, false);
               const last = served.at(-1);
               expect(last?.tablesRead.map((b) => b.sourceName)).toEqual([
                  `${p}_mix`,
               ]);
               expect(
                  last?.gateTablesRead?.map((b) => b.sourceName).sort(),
               ).toEqual(["cust_sto", `${p}_mix`].sort());
               if ("error" in outcome) throw new Error(outcome.error);
               expect(outcome.answer.via ?? outcome.answer.servedBy).toBe(
                  "live",
               );
               const l = live.get(key(malloy, NORTH))!;
               if ("error" in l) throw new Error(l.error);
               expect(
                  rowDifference(l.answer.rows, outcome.answer.rows),
               ).toBeUndefined();
            });

            it(`without the fail-closed read, the per-caller check fails: it is served past the gate (${p})`, async () => {
               const { outcome } = await respelled(malloy, true);
               if ("error" in outcome) throw new Error(outcome.error);
               expect(() =>
                  expectAsLive(
                     { name: "mixed", malloy, via: "live" },
                     NORTH as Givens,
                     live.get(key(malloy, NORTH))!,
                     outcome,
                  ),
               ).toThrow();
               expect(outcome.answer.via).toBe("served_copy");
            });
         }
      });
   });
});

describe("the same package with the gated-sources switch on", () => {
   it("serves the same gates from the destination, and refuses the same", async () => {
      const restore = installSettings({
         env: { ...G3_ONLY, PUBLISHER_MATERIALIZE_GATED_SOURCES: "true" },
      }).restore;
      await freshLoadPool();
      const loader = await loadFixture(FILES);
      try {
         const malloy = "run: v1_gated -> { group_by: region; aggregate: n }";
         const liveOut = await loader.run("m.malloy", malloy, NORTH as Givens);
         const fixture = await loader.bind();
         expect(
            fixture.verdicts.filter((v) => v.includes(":live")).sort(),
         ).toEqual(
            [
               "mz_inh_rows:live:refused",
               "v1_inh_rows:live:refused",
               "mz_joiner:live:refused",
               "mz_baked:live:refused",
               "v1_baked:live:refused",
            ].sort(),
         );
         const served = await fixture.run("m.malloy", malloy, NORTH as Givens);
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
   }, 180_000);
});
