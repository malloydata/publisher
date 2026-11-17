// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A materialized table past its source's own row-level gate under G3, with
 * the gated-sources switch off, adversarially, through the real package path
 * (conformance/package_harness.ts): the plan's eligibility, the bind, the
 * router and `govern()`. Once per tier: a colocated `#(materialize)` table,
 * and a `storage=` table under `#(materialize)` and v1 `#@ persist` alike.
 *
 * G3 serves a table past the entry point's own row-level gate because the
 * gate is re-applied when it is read. A maintainer-approved exception made
 * ahead of the question-14 review, for consistency: colocated and `storage=`
 * behave the same under a source's own row-level gate (an `#(access_filter)`
 * row filter, an `#(authorize)` lock, the two together, or a lock inherited
 * from a base). These specs pin that, and no further:
 *
 * - every caller, interleaved, gets exactly live's rows (or live's denial),
 *   from the served copy;
 * - the stored table holds every row, and its build SQL no gate value, so no
 *   build bakes one caller's rows;
 * - the gate cannot be read around: a caller's `rename:` or `except:` of the
 *   gate column, a raw read of the destination table, and a caller join are
 *   answered exactly as live;
 * - what G3 does not admit stays refused and answers live: an inherited row
 *   filter, a joined gate, a given the build would substitute;
 * - an answer reading a G3 table beside one that is not answers live;
 * - removing the gate's re-proof denies the answer, removing the re-applied
 *   gate makes the per-caller check fail, and so does judging an answer by
 *   the exact-match tables alone (no `tablesReadForGates`).
 */

import { QueryMaterializer } from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it, spyOn } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import { DATA_FILES, RAW_MODEL } from "./conformance/data";
import { G3_COLOCATED_MODEL, G3_STORAGE_MODEL } from "./g3_models";
import {
   freshLoadPool,
   type LoadedFixture,
   loadFixture,
   type Outcome,
} from "../../../src/test_helpers/conformance/package_runner";
import { rowDifference, type Row } from "./harness";

interface Case {
   name: string;
   malloy: string;
   /** The answer's route when a caller is answered: the served copy, or live. */
   via: "served_copy" | "live";
   /** Every answered row's `region` is one of the caller's REGIONS. */
   rowsInCallersRegions?: boolean;
}

type Givens = Record<string, string | string[]>;
type Loader = Awaited<ReturnType<typeof loadFixture>>;

const SERVED = { via: "served_copy" as const };
const SERVED_ROWS = { via: "served_copy" as const, rowsInCallersRegions: true };
const LIVE = { via: "live" as const };

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

interface Tier {
   label: string;
   model: string;
   cases: Case[];
   /** Reads around the gate, each refused live and so served. */
   readAround: { name: string; malloy: string }[];
   /** The tier a served answer reports, where the spec pins one. */
   servedFrom?: string;
   verdicts: string[];
   /** Callers a lock refuses: answered with live's denial. */
   locked: string[];
   /** The cases the guard checks run. */
   guarded: (cases: Case[]) => Case[];
   /** The plan's own checks of what it built and refused. */
   checkPlan(fixture: LoadedFixture): void;
   /** Each stored table holds every row, whoever asks. */
   checkTables(fixture: LoadedFixture, loader: Loader): Promise<void>;
   /** With the switch on: the query read from storage and the verdicts. */
   switchOn: { malloy: string; liveOnly: boolean; verdicts: string[] };
   /** The caller in the north region; the colocated model also reads STATUS. */
   north: Givens;
}

const COLOCATED: Tier = {
   label: "colocated #(materialize)",
   model: G3_COLOCATED_MODEL,
   cases: [
      {
         name: "the gated source",
         malloy: "run: gated -> { group_by: region; aggregate: total, n }",
         ...SERVED_ROWS,
      },
      {
         name: "an extension of it, its gate inherited",
         malloy:
            "run: gated_ext -> { group_by: region; aggregate: total, avg_amount }",
         ...SERVED_ROWS,
      },
      {
         name: "a materialized extension of it, read through the parent's table",
         malloy:
            "run: gated_ext_tbl -> { group_by: region, big; aggregate: n }",
         ...SERVED_ROWS,
      },
      {
         name: "a pair table under its own gate",
         malloy: "run: gated_pairs -> { group_by: region; aggregate: n, qty }",
         ...SERVED_ROWS,
      },
      {
         name: "an inlined join_one under its own gate",
         malloy:
            "run: gated_inline -> { group_by: c.segment, region; aggregate: n }",
         ...SERVED_ROWS,
      },
      {
         name: "a lock and a row filter together",
         malloy: "run: locked_rows -> { group_by: region; aggregate: n }",
         ...SERVED_ROWS,
      },
      {
         name: "a lock alone",
         malloy: "run: locked -> { group_by: region; aggregate: n }",
         ...SERVED,
      },
      // A source joining the gated one: Publisher does not enforce a joined
      // source's gate on the parent, live or served (the `join-bypass`
      // warning), so the served answer must be exactly live's.
      {
         name: "a source joining the gated one",
         malloy:
            "run: joiner -> { group_by: g.region; aggregate: n_customers, g.n }",
         ...SERVED,
      },
      {
         name: "a materialized source joining the gated one, refused itself",
         malloy:
            "run: mat_joiner -> { group_by: g.region; aggregate: n_customers, g.n }",
         ...SERVED,
      },
      {
         name: "a storage= table under its own gate, admitted as colocated (G3)",
         malloy: "run: gated_sto -> { group_by: region; aggregate: n }",
         ...SERVED_ROWS,
      },
      {
         name: "a gated source whose build reads a given, refused",
         malloy: "run: baked -> { group_by: region, status; aggregate: n }",
         ...LIVE,
      },
   ],
   readAround: [],
   north: { REGIONS: ["north"], ROLE: "admin", STATUS: "placed" },
   verdicts: [
      "gated:serves",
      "gated_inline:serves",
      "gated_pairs:serves",
      "locked:serves",
      "locked_rows:serves",
      // G3 on the storage tier, on the same conditions.
      "gated_sto:serves",
      // An inherited gate, a joined gate, a given the build would
      // substitute: refused.
      "gated_ext_tbl:live:refused",
      "mat_joiner:live:refused",
      "baked:live:refused",
   ],
   locked: ["run: locked_rows -> { group_by: region; aggregate: n }"],
   guarded: (cases) => [cases[0]],
   checkPlan(fixture) {
      expect(
         fixture.warnings
            .filter((w) => w.code === "materialize-not-built")
            .map((w) => w.subject)
            .sort(),
      ).toEqual(["baked", "gated_ext_tbl", "mat_joiner"]);
   },
   async checkTables(fixture) {
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
   },
   switchOn: {
      malloy: "run: gated_sto -> { group_by: region; aggregate: n }",
      liveOnly: false,
      verdicts: [
         "gated:serves",
         "gated_inline:serves",
         "gated_pairs:serves",
         "locked:serves",
         "locked_rows:serves",
         "gated_sto:serves",
         "gated_ext_tbl:live:refused",
         "mat_joiner:live:refused",
         "baked:live:refused",
      ],
   },
};

const STORAGE_GATED = [
   "mz_gated",
   "v1_gated",
   "mz_lock",
   "v1_lock",
   "mz_lock_rows",
   "v1_lock_rows",
   "mz_inh_lock",
   "v1_inh_lock",
];

const STORAGE: Tier = {
   label: "storage=",
   model: G3_STORAGE_MODEL,
   cases: [
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
      // A G3 table beside an ungated storage= one: the ungated table holds
      // only ungated rows, so it is a G3 table too (g-mixed, pending the
      // question-14 review), and the answer is served. One not proven
      // ungated keeps it live (the respelled checks below).
      ...both(
         "an answer reading the gated table and an ungated storage= table",
         (p) =>
            `run: ${p}_mix -> { group_by: c.segment, region; aggregate: n }`,
         SERVED_ROWS,
      ),
      // A caller join into a gated source: Publisher does not enforce a
      // joined source's gate on the parent, live or served (the
      // `join-bypass` warning), so the served answer must be exactly live's.
      ...both(
         "a caller join into it",
         (p) =>
            `run: customers_raw extend { join_many: g is ${p}_gated on g.customer_id = id } -> { group_by: g.region; aggregate: c is count() }`,
         LIVE,
      ),
   ],
   readAround: [
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
   ],
   servedFrom: "storage",
   north: { REGIONS: ["north"], ROLE: "admin" },
   verdicts: [
      "mz_gated:serves",
      "v1_gated:serves",
      // A v1 extension inherits `#@ persist` and shares the table, its gate
      // grafted through the extension as on colocated.
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
   ],
   locked: STORAGE_GATED.filter((name) => /lock/.test(name)).map(
      (name) => `run: ${name} -> { group_by: region; aggregate: n }`,
   ),
   guarded: (cases) => cases.filter((c) => /_gated -> /.test(c.malloy)),
   checkPlan(fixture) {
      expect(
         fixture.built
            .filter((b) => b.tier === "storage")
            .map((b) => b.name)
            .sort(),
      ).toEqual([...STORAGE_GATED, "cust_sto", "mz_mix", "v1_mix"].sort());
   },
   async checkTables(fixture, loader) {
      const count = async (table: string) => {
         const { rows } = await loader.lake.runSQL(
            `SELECT COUNT(*) AS n, COUNT(DISTINCT region) AS regions FROM ${table}`,
         );
         return [Number(rows[0].n), Number(rows[0].regions)];
      };
      const plan = fixture.pkg.getBuildPlan()?.sources ?? {};
      for (const name of STORAGE_GATED) {
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
   },
   switchOn: {
      malloy: "run: v1_gated -> { group_by: region; aggregate: n }",
      liveOnly: true,
      verdicts: [
         "mz_inh_rows:live:refused",
         "v1_inh_rows:live:refused",
         "mz_joiner:live:refused",
         "mz_baked:live:refused",
         "v1_baked:live:refused",
      ],
   },
};

const EAST_WEST = { REGIONS: ["east", "west"], ROLE: "viewer" };
const NO_GROUP = { REGIONS: [] as string[], ROLE: "admin" };
const NOBODY = {};

const G3_ONLY = {
   PERSIST_STORAGE_MODE: "on",
   PUBLISHER_MATERIALIZE: "true",
   PUBLISHER_MATERIALIZE_GATED_SOURCES: "false",
};

const key = (malloy: string, givens: object) =>
   `${malloy} ${JSON.stringify(givens)}`;

/** Rows equal to live's, or the same denial, for one caller. */
function expectAsLive(
   tier: Pick<Tier, "servedFrom">,
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
   if (tier.servedFrom && kase.via === "served_copy") {
      expect(served.answer.servedFrom).toBe(tier.servedFrom);
   }
   expect(rowDifference(live.answer.rows, served.answer.rows)).toBeUndefined();
   if (kase.rowsInCallersRegions) {
      for (const row of served.answer.rows) {
         expect(givens.REGIONS as string[]).toContain(row.region as string);
      }
   }
}

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

const filesOf = (model: string) => ({
   ...DATA_FILES,
   "raw.malloy": RAW_MODEL,
   "m.malloy": model,
});

for (const tier of [COLOCATED, STORAGE]) {
   const NORTH = tier.north;
   const CALLERS = [NORTH, EAST_WEST, NO_GROUP, NOBODY];
   /** Interleaved, then repeated, so a graft or cache shared across callers shows. */
   const ORDER = [NORTH, EAST_WEST, NO_GROUP, NOBODY, EAST_WEST, NORTH, NOBODY];

   describe(`${tier.label} past its own row-level gate under G3 (gated sources off)`, () => {
      let restore: () => void = () => {};
      let loader: Loader;
      let fixture: LoadedFixture;
      const live = new Map<string, Outcome>();
      const governor = () =>
         (
            fixture.pkg.getModel("m.malloy") as unknown as {
               governor: Record<string, unknown>;
            }
         ).governor;

      beforeAll(async () => {
         restore = installSettings({ env: G3_ONLY }).restore;
         // The plan (and its eligibility) is computed in a load worker, which
         // reads its settings at spawn.
         await freshLoadPool();
         loader = await loadFixture(filesOf(tier.model));
         for (const malloy of [
            ...tier.cases.map((c) => c.malloy),
            ...tier.readAround.map((c) => c.malloy),
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
            [...tier.verdicts].sort(),
         );
         tier.checkPlan(fixture);
      });

      for (const kase of tier.cases) {
         it(`${kase.name}: every caller, interleaved, gets live's rows or live's denial`, async () => {
            const answered: Record<string, Row[]> = {};
            for (const givens of ORDER) {
               const served = await fixture.run(
                  "m.malloy",
                  kase.malloy,
                  givens as Givens,
               );
               expectAsLive(
                  tier,
                  kase,
                  givens as Givens,
                  live.get(key(kase.malloy, givens))!,
                  served,
               );
               if (!("error" in served)) {
                  const k = JSON.stringify(givens);
                  // The same caller agrees with itself however calls
                  // interleave.
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
                  (answered[JSON.stringify(g)] ?? [])
                     .map((r) => r.region)
                     .sort();
               expect(regions(NORTH)).not.toEqual(regions(EAST_WEST));
            }
         });
      }

      it("denies a caller a lock refuses exactly as live does", async () => {
         for (const malloy of tier.locked) {
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

      for (const read of tier.readAround) {
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
         await tier.checkTables(fixture, loader);
      });

      describe("each guard is load-bearing", () => {
         const guarded = tier.guarded(tier.cases);

         it("baseline: each caller's rows, from the served copy", async () => {
            for (const kase of guarded) {
               const served = await fixture.run(
                  "m.malloy",
                  kase.malloy,
                  NORTH as Givens,
               );
               expectAsLive(
                  tier,
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
                  for (const kase of guarded) {
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
                  async (
                     _request: unknown,
                     compiled: { runnable: unknown },
                  ) => ({
                     runnable: compiled.runnable,
                     decision: { enforced: undefined, grafts: [] },
                  }),
               async () => {
                  for (const kase of guarded) {
                     const served = await fixture.run(
                        "m.malloy",
                        kase.malloy,
                        NORTH as Givens,
                     );
                     if ("error" in served) throw new Error(served.error);
                     expect(served.answer.via).toBe("served_copy");
                     expect(() =>
                        expectAsLive(
                           tier,
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

         if (tier === STORAGE) {
            describe("an answer reading the gated table and an ungated storage= table spelled another way", () => {
               /** What the served copy answered with, each time it was asked. */
               type Served = {
                  tablesRead: { sourceName: string }[];
                  gateTablesRead?: { sourceName: string }[];
               };
               /** The judged tables with the ungated one's proof dropped. */
               const unproven = (answer: Served) =>
                  answer.gateTablesRead?.map((b) =>
                     b.sourceName === "cust_sto"
                        ? { ...b, holdsOnlyUngatedRows: false }
                        : b,
                  );
               /**
                * Run `malloy` with the served copy's SQL also reading the
                * ungated `g3s_cust` table, quoted unlike its bound path, so
                * the served copy's exact match misses it. `dropJudged` drops
                * the fail-closed read (`tablesReadForGates`), as if it were
                * removed.
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
                     // `g3s_cust` as a table the plan could not prove holds
                     // only ungated rows (g-mixed would admit it).
                     return dropJudged
                        ? { ...answer, gateTablesRead: undefined }
                        : { ...answer, gateTablesRead: unproven(answer) };
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
                           tier,
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
         }
      });
   });

   describe(`${tier.label}: the same package with the gated-sources switch on`, () => {
      it("serves the same gates, the storage= table among them, and refuses the same", async () => {
         const restore = installSettings({
            env: { ...G3_ONLY, PUBLISHER_MATERIALIZE_GATED_SOURCES: "true" },
         }).restore;
         await freshLoadPool();
         const loader = await loadFixture(filesOf(tier.model));
         try {
            const { malloy, liveOnly, verdicts } = tier.switchOn;
            const liveOut = await loader.run(
               "m.malloy",
               malloy,
               NORTH as Givens,
            );
            const fixture = await loader.bind();
            expect(
               fixture.verdicts
                  .filter((v) => !liveOnly || v.includes(":live"))
                  .sort(),
            ).toEqual([...verdicts].sort());
            const served = await fixture.run(
               "m.malloy",
               malloy,
               NORTH as Givens,
            );
            if ("error" in served || "error" in liveOut) {
               throw new Error("failed");
            }
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
}

describe("a served answer that names a storage= table beside a colocated one, spelled another way (G3, gated sources off)", () => {
   // A gated colocated table and an ungated storage= table, both bound in
   // the served copy. G3 admits an answer past the gate only when every
   // table it reads is a G3 table; the storage= one is held here as one the
   // plan could not prove holds only ungated rows.
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
      const loader = await loadFixture(filesOf(MIXED));
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
      const served: Served[] = [];
      copy.loadQuery = async (...args) => {
         const answer = await loadQuery.apply(copy, args);
         served.push(answer);
         // The storage table as one the plan could not prove holds only
         // ungated rows (g-mixed would admit `g3w_cust`).
         return {
            ...answer,
            gateTablesRead: answer.gateTablesRead?.map((b) => ({
               ...b,
               holdsOnlyUngatedRows: false,
            })),
         };
      };
      const getSQL = QueryMaterializer.prototype.getSQL;
      // The served copy's SQL also reads the storage table, quoted
      // differently from the bound `"lake"."g3w_cust"`. Only the quoting
      // defeats the served copy's exact match (sqlTableReads ignores case).
      const respelled = respell
         ? spyOn(QueryMaterializer.prototype, "getSQL").mockImplementation(
              async function (
                 this: QueryMaterializer,
                 ...args: Parameters<typeof getSQL>
              ) {
                 const sql = await getSQL.apply(this, args);
                 return sql.replace(
                    /FROM "g3w_gated" as base/,
                    'FROM "g3w_gated" as base LEFT JOIN (SELECT 1 AS one FROM "lake".g3w_cust LIMIT 0) AS g3w_probe ON TRUE',
                 );
              },
           )
         : undefined;
      try {
         const outcome = await fixture.run("m.malloy", QUERY, CALLER);
         return { outcome, served };
      } finally {
         copy.loadQuery = loadQuery;
         respelled?.mockRestore();
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
      if ("error" in live) throw new Error(live.error);
      if ("error" in outcome) throw new Error(outcome.error);
      expect(outcome.answer.via ?? outcome.answer.servedBy).toBe("live");
      expect(
         rowDifference(live.answer.rows, outcome.answer.rows),
      ).toBeUndefined();
   });
});

describe("a persisted view or query over a gated source, the same on both tiers", () => {
   // Its own leg reads it past the gate only with the gated-sources switch
   // on (the view leg; the query leg never). Off, neither tier builds the
   // view; on, the colocated view is built as before and the storage= view
   // stays refused. A persisted query that reaches a gate is never planned,
   // whatever the switch (persisted_query.component.spec.ts): answered live.
   const VIEWS = `##! experimental { givens persistence }
import "raw.malloy"

given: REGIONS :: string[]

#(access_filter) region in $REGIONS
source: g_orders is orders_raw extend {
  measure: n is count()
  #@ persist name="g3v_col"
  view: by_region_col is { group_by: region; aggregate: n }
  #@ persist name="g3v_sto" storage="lake"
  view: by_region_sto is { group_by: region, status; aggregate: n }
}

#@ persist name="g3q_col"
query: q_col is g_orders -> { group_by: region; aggregate: c is count() }
`;
   const QUERIES = [
      "run: g_orders -> by_region_col",
      "run: g_orders -> by_region_sto",
      "run: q_col",
   ];
   const CALLERS_V: Givens[] = [
      { REGIONS: ["north"] },
      { REGIONS: ["east", "west"] },
   ];

   for (const gated of [false, true]) {
      it(`gated sources ${gated ? "on" : "off"}: ${gated ? "the colocated view is built, the storage= view refused" : "no view is built on either tier"}, and every caller gets live's rows`, async () => {
         const restore = installSettings({
            env: {
               ...G3_ONLY,
               PUBLISHER_MATERIALIZE_GATED_SOURCES: String(gated),
            },
         }).restore;
         await freshLoadPool();
         const loader = await loadFixture({
            ...DATA_FILES,
            "raw.malloy": RAW_MODEL,
            "m.malloy": VIEWS,
         });
         try {
            const live = new Map<string, Outcome>();
            for (const q of QUERIES) {
               for (const [i, g] of CALLERS_V.entries()) {
                  live.set(`${q}#${i}`, await loader.run("m.malloy", q, g));
               }
            }
            const fixture = await loader.bind();
            const built = fixture.built.map((b) => b.name).sort();
            expect(built).toEqual(
               gated ? ["g_orders__by_region_col__view"] : [],
            );
            const refusals = (fixture.pkg.getBuildPlan()?.refusedSources ??
               {}) as Record<
               string,
               { name: string; tier: string; reason?: string }
            >;
            expect(
               Object.values(refusals)
                  .map((r) => `${r.name}:${r.tier}:${r.reason}`)
                  .sort(),
            ).toEqual(
               (gated
                  ? [
                       "g_orders__by_region_sto__view:storage:access_filter",
                       "q_col__query:colocated:access_filter",
                    ]
                  : [
                       "g_orders__by_region_col__view:colocated:access_filter",
                       "g_orders__by_region_sto__view:storage:access_filter",
                       "q_col__query:colocated:access_filter",
                    ]
               ).sort(),
            );
            for (const q of QUERIES) {
               for (const [i, g] of CALLERS_V.entries()) {
                  const l = live.get(`${q}#${i}`)!;
                  const served = await fixture.run("m.malloy", q, g);
                  if ("error" in l) throw new Error(l.error);
                  if ("error" in served) throw new Error(served.error);
                  expect(
                     rowDifference(l.answer.rows, served.answer.rows),
                  ).toBeUndefined();
                  // Only the colocated view, with the switch on, is read past
                  // the gate.
                  expect([q, served.answer.servedBy]).toEqual([
                     q,
                     gated && q.endsWith("by_region_col") ? "view" : "live",
                  ]);
               }
            }
         } finally {
            await loader.dispose();
            restore();
            await freshLoadPool();
         }
      }, 180_000);
   }
});
