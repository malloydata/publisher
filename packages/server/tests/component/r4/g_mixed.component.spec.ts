// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * g-mixed: a gated entry point over an UNGATED stored source, with the
 * gated-sources switch off, through the real package path
 * (conformance/package_runner.ts): the plan's eligibility, the bind, the
 * router and `govern()`.
 *
 *     #@ persist storage="lake"
 *     source: orders is orders_raw -> { … }            // ungated
 *     #(access_filter) region in $REGIONS
 *     source: regional is orders extend {}             // gated entry point
 *
 * A `storage=` table every source stored in it was proven to hold only
 * ungated rows (no gate beneath any of them, joins and derivations included)
 * is a G3 table, as a database table is: the router serves the gated entry
 * point from it, and `govern()` grafts and proves the entry point's gate on
 * the answer exactly as on live. Pending the question-14 review.
 *
 * The invariants, each pinned below for `#@ persist` and `#(materialize)`
 * alike:
 *
 * - every caller (an allowed region, a denied one, no regions, a NULL in the
 *   regions, no givens at all, a caller a lock refuses) gets exactly live's
 *   rows, or live's denial, interleaved and repeated;
 * - widening the query (an OR filter, `having:`, a pipeline, a join back to
 *   the ungated base, `all()`/`exclude()`, `count(distinct)`, top-N and
 *   `limit:`) still equals live;
 * - a join_many fan-out under the gate, and a gate over a joined field;
 * - a computed gate column;
 * - `#(authorize)` locks, plain and inherited, as on a database table;
 * - the access record's summary is live's (gates, decision, fields read, row
 *   count), its path names the served copy, and no record or path carries a
 *   gate value;
 * - removing the gate's re-proof denies, removing the re-applied gate fails
 *   the per-caller check, and an answer that also reads a `storage=` table
 *   not proven ungated answers live.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import type { GivenValue } from "@malloydata/malloy";
import { installSettings } from "../../../src/test_helpers/settings_env";
import { DATA_FILES, RAW_MODEL } from "./conformance/data";
import {
   freshLoadPool,
   type LoadedFixture,
   loadFixture,
   type Outcome,
} from "../../../src/test_helpers/conformance/package_runner";
import { unrecordedAudit } from "../../../src/service/access_record";
import { QueryPrivileges } from "../../../src/service/model_query_request";
import type { Package } from "../../../src/service/package";
import { rowDifference, type Row } from "./harness";

type Givens = Record<string, GivenValue>;
type Loader = Awaited<ReturnType<typeof loadFixture>>;

/**
 * Every `p_` name is declared twice: `v1_` under `#@ persist`, `mz_` under
 * `#(materialize)`, both `storage=`. Each stored source selects its own
 * columns, so no two share a content address. Only the stored sources are
 * ungated; every gate is on an entry point over them.
 */
function storedPair(
   p: "v1" | "mz",
   annotation: (table: string) => string,
): string {
   return `
${annotation(`gmx_${p}_orders`)}
source: ${p}_orders is orders_raw -> {
  select: id, customer_id, region, status, amount${p === "mz" ? ", discount" : ""}
} extend {
  primary_key: id
  measure: n is count(), total is amount.sum()
}

#(access_filter) region in $REGIONS
source: ${p}_regional is ${p}_orders extend {}

#(authorize) $ROLE = 'admin'
source: ${p}_locked is ${p}_orders extend {}

#(authorize) $ROLE = 'admin'
#(access_filter) region in $REGIONS
source: ${p}_locked_rows is ${p}_orders extend {}

#(authorize) $ROLE = 'admin'
source: ${p}_lock_base is ${p}_orders extend {}
source: ${p}_inh_lock is ${p}_lock_base extend {}

#(access_filter) lower_region in $REGIONS
source: ${p}_computed is ${p}_orders extend {
  dimension: lower_region is lower(region)
}

${annotation(`gmx_${p}_cust`)}
source: ${p}_cust is customers_raw -> {
  select: id, home_region${p === "mz" ? ", segment" : ""}
} extend {
  primary_key: id
  measure: nc is count()
}

#(access_filter) c.home_region in $REGIONS
source: ${p}_by_home is ${p}_orders extend {
  join_one: c is ${p}_cust on c.id = customer_id
}

#(access_filter) home_region in $REGIONS
source: ${p}_cust_orders is cust_sto extend {
  join_many: o is ${p}_orders on o.customer_id = id
}
`;
}

export const G_MIXED_MODEL = `##! experimental { givens persistence }
import "raw.malloy"

given: REGIONS :: string[]
given: ROLE :: string
given: ON_DAY :: timestamp

#(materialize) name="gmx_cust" storage="lake"
source: cust_sto is customers_raw -> { select: id, segment, home_region } extend {
  primary_key: id
  measure: nc is count()
}

#(materialize) name="gmx_mz_ev" storage="lake"
source: mz_ev is orders_raw -> {
  select: id, region, created_at
} extend {
  dimension: order_day is created_at.day
  measure: n is count()
}

#(access_filter) order_day = $ON_DAY
source: mz_ev_day is mz_ev extend {}

#(access_filter) order_day = $ON_DAY
source: mz_ev_day_la is mz_ev extend { timezone: 'America/Los_Angeles' }

#(materialize) name="gmx_mz_stage" storage="lake"
source: mz_stage is orders_raw -> {
  select: id, region, stage_day is created_at.day
} extend {
  measure: n is count()
}

#(access_filter) stage_day = $ON_DAY
source: mz_stage_day is mz_stage extend {}

// A stored source derived from a gated one: its rows are the gated
// source's, so it is never proven ungated (refused, as an inherited row
// filter is).
#(access_filter) region in $REGIONS
source: gated_src is orders_raw extend {}

#(materialize) name="gmx_mz_derived" storage="lake"
source: mz_derived is gated_src -> { select: id, region, status }
${storedPair("v1", (t) => `#@ persist name="${t}" storage="lake"`)}
${storedPair("mz", (t) => `#(materialize) name="${t}" storage="lake"`)}
`;

interface Case {
   name: string;
   malloy: string;
   /** Every answered row's region-valued column is one of the caller's. */
   regionColumn?: string;
   /** Answered live, not from the table (default: served). */
   live?: true;
}

/** The same case for both mechanisms. */
function both(
   name: string,
   query: (p: "v1" | "mz") => string,
   regionColumn?: string,
): Case[] {
   return (["v1", "mz"] as const).map((p) => ({
      name: `${p === "v1" ? "#@ persist" : "#(materialize)"}: ${name}`,
      malloy: query(p),
      ...(regionColumn ? { regionColumn } : {}),
   }));
}

const CASES: Case[] = [
   ...both(
      "the gated entry point",
      (p) => `run: ${p}_regional -> { group_by: region; aggregate: n, total }`,
      "region",
   ),
   ...both(
      "every row selected",
      (p) =>
         `run: ${p}_regional -> { select: id, region, amount; order_by: id }`,
      "region",
   ),
   // Widening attempts: each must still read only the caller's rows.
   ...both(
      "an OR filter reaching for other regions",
      (p) =>
         `run: ${p}_regional -> { where: region = 'north' or region = 'south' or region = null; group_by: region; aggregate: n }`,
      "region",
   ),
   ...both(
      "an OR true filter",
      (p) =>
         `run: ${p}_regional -> { where: region = 'other' or true; group_by: region; aggregate: n }`,
      "region",
   ),
   ...both(
      "a nest:",
      (p) =>
         `run: ${p}_regional -> { group_by: status; aggregate: n; nest: by_region is { group_by: region; aggregate: n; order_by: region } }`,
   ),
   // Rebinding the gate's column: refused live, so refused served.
   ...both(
      "a caller dimension shadowing the gate column",
      (p) =>
         `run: ${p}_regional extend { dimension: region is 'east' } -> { group_by: region; aggregate: n }`,
   ),
   ...both(
      "a having: clause",
      (p) =>
         `run: ${p}_regional -> { group_by: region; aggregate: n; having: n >= 0 }`,
      "region",
   ),
   ...both(
      "a pipeline",
      (p) =>
         `run: ${p}_regional -> { group_by: region, status; aggregate: n } -> { group_by: region; aggregate: k is n.sum(), groups is count() }`,
      "region",
   ),
   ...both(
      "a caller join back to the ungated base",
      (p) =>
         `run: ${p}_regional extend { join_many: b is ${p}_orders on b.customer_id = customer_id } -> { group_by: region; aggregate: n, bn is b.count(), bt is b.total }`,
      "region",
   ),
   ...both(
      "all() and exclude()",
      (p) =>
         `run: ${p}_regional -> { group_by: region; aggregate: n, all_n is all(n), ex is exclude(n, region) }`,
      "region",
   ),
   ...both(
      "count(distinct) with no group",
      (p) =>
         `run: ${p}_regional -> { aggregate: regions is count(region), customers is count(customer_id), n }`,
   ),
   ...both(
      "top-N with a limit",
      (p) =>
         `run: ${p}_regional -> { group_by: region; aggregate: n; order_by: n desc, region; limit: 1 }`,
      "region",
   ),
   ...both(
      "a computed gate column",
      (p) =>
         `run: ${p}_computed -> { group_by: lower_region; aggregate: n, total }`,
      "lower_region",
   ),
   ...both(
      "a gate over a joined field",
      (p) =>
         `run: ${p}_by_home -> { group_by: c.home_region; aggregate: n, total, c.nc }`,
      "home_region",
   ),
   ...both(
      "a join_many fan-out under the gate",
      (p) =>
         `run: ${p}_cust_orders -> { group_by: home_region; aggregate: nc, o.n, o.total }`,
      "home_region",
   ),
   ...both(
      "a lock alone",
      (p) => `run: ${p}_locked -> { group_by: region; aggregate: n }`,
   ),
   ...both(
      "a lock with a row filter",
      (p) => `run: ${p}_locked_rows -> { group_by: region; aggregate: n }`,
      "region",
   ),
   ...both(
      "a lock inherited from a base",
      (p) => `run: ${p}_inh_lock -> { group_by: region; aggregate: n }`,
   ),
];

// A gate over a day (question 14, F1). Over an `extend` dimension the served
// copy truncates the stored time at read, in the caller's zone as live does,
// so every query is served. Over a day a query stage stored at build, a
// query in another zone would compare another day: answered live (#1855).
CASES.push(
   {
      name: "a gate over a day dimension, unzoned",
      malloy: "run: mz_ev_day -> { group_by: region; aggregate: n }",
   },
   ...["Pacific/Kiritimati", "Etc/GMT+12"].flatMap((zone): Case[] => [
      {
         name: `a gate over a day dimension, read in ${zone}`,
         malloy: `run: mz_ev_day -> { timezone: '${zone}'; group_by: region; aggregate: n }`,
      },
      {
         name: `a gate over a stored day, read in ${zone}`,
         malloy: `run: mz_stage_day -> { timezone: '${zone}'; group_by: region; aggregate: n }`,
         live: true,
      },
   ]),
   {
      name: "a gate over a day dimension, on a source in its own time zone",
      malloy: "run: mz_ev_day_la -> { group_by: region; aggregate: n }",
   },
   {
      name: "a gate over a stored day, unzoned",
      malloy: "run: mz_stage_day -> { group_by: region; aggregate: n }",
   },
);

const NORTH: Givens = {
   REGIONS: ["north"],
   ROLE: "admin",
   ON_DAY: "2025-01-22 00:00:00",
};
const EAST_WEST: Givens = {
   REGIONS: ["east", "west"],
   ROLE: "viewer",
   ON_DAY: "2025-01-21 00:00:00",
};
const NOWHERE: Givens = { REGIONS: ["nowhere"], ROLE: "admin" };
const NO_REGIONS: Givens = { REGIONS: [], ROLE: "admin" };
const NULL_REGION: Givens = {
   REGIONS: [null as unknown as string],
   ROLE: "admin",
};
const NOBODY: Givens = {};
const CALLERS = [NORTH, EAST_WEST, NOWHERE, NO_REGIONS, NULL_REGION, NOBODY];
/** Interleaved, then repeated, so a graft or cache shared across callers shows. */
const ORDER = [
   NOWHERE,
   NORTH,
   EAST_WEST,
   NULL_REGION,
   NO_REGIONS,
   NOBODY,
   EAST_WEST,
   NORTH,
   NOBODY,
   NULL_REGION,
];

const G3_ONLY = {
   PERSIST_STORAGE_MODE: "on",
   PUBLISHER_MATERIALIZE: "true",
   PUBLISHER_MATERIALIZE_GATED_SOURCES: "false",
};

const key = (malloy: string, givens: object) =>
   `${malloy} ${JSON.stringify(givens)}`;

/** One query's answer with the access record the model wrote for it. */
interface Audited {
   outcome: Outcome;
   audit: ReturnType<typeof unrecordedAudit>;
}

async function runAudited(
   pkg: Package,
   malloy: string,
   givens: Givens,
): Promise<Audited> {
   const audit = unrecordedAudit();
   const model = pkg.getModel("m.malloy");
   if (!model) throw new Error("m.malloy did not load");
   try {
      const result = await model.runQuery(
         audit,
         { query: malloy, givens } as never,
         QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
      );
      return {
         audit,
         outcome: {
            answer: {
               rows: result.compactResult as unknown as Row[],
               fields: [],
               servedFrom: result.servedFrom ?? null,
               servedBy: result.path?.servedBy?.kind ?? "live",
               ...(result.path?.servedBy?.via
                  ? { via: result.path.servedBy.via }
                  : {}),
            },
         },
      };
   } catch (error) {
      return {
         audit,
         outcome: {
            error: error instanceof Error ? error.message : String(error),
         },
      };
   }
}

/** What an access record says of who read what, served or live alike. */
function summaryOf(audit: Audited["audit"]) {
   const s = audit.summary;
   if (!s) return undefined;
   return {
      source: s.source,
      gates: s.gates,
      decision: s.decision,
      givenNames: [...s.givenNames].sort(),
      gateGivenNames: [...s.gateGivenNames].sort(),
      fieldsRead: s.fieldsRead,
      rowCount: s.rowCount,
   };
}

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
   if (kase.live) {
      expect([kase.name, served.answer.via ?? "live"]).toEqual([
         kase.name,
         "live",
      ]);
   } else {
      expect([kase.name, served.answer.via]).toEqual([
         kase.name,
         "served_copy",
      ]);
      expect(served.answer.servedFrom).toBe("storage");
   }
   expect(rowDifference(live.answer.rows, served.answer.rows)).toBeUndefined();
   if (kase.regionColumn) {
      const allowed = (givens.REGIONS as string[] | undefined) ?? [];
      for (const row of served.answer.rows) {
         expect(allowed).toContain(row[kase.regionColumn] as string);
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

const FILES = {
   ...DATA_FILES,
   "raw.malloy": RAW_MODEL,
   "m.malloy": G_MIXED_MODEL,
};

/** Every value a region column holds, to look for in a record or a path. */
const REGION_VALUES = ["north", "south", "east", "west"];

describe("g-mixed: a gated entry point over an ungated storage= source (gated sources off)", () => {
   let restore: () => void = () => {};
   let loader: Loader;
   let fixture: LoadedFixture;
   const live = new Map<string, Audited>();
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
      loader = await loadFixture(FILES);
      for (const { malloy } of CASES) {
         for (const givens of CALLERS) {
            live.set(
               key(malloy, givens),
               await runAudited(loader.pkg, malloy, givens),
            );
         }
      }
      fixture = await loader.bind();
   }, 240_000);
   afterAll(async () => {
      await fixture?.dispose();
      restore();
      await freshLoadPool();
   });

   it("builds every ungated stored source, and serves each from its table", () => {
      expect(fixture.buildErrors).toEqual({});
      const stored = fixture.built.filter((b) => b.tier === "storage");
      expect(
         [
            "cust_sto",
            "mz_orders",
            "mz_cust",
            "mz_ev",
            "mz_stage",
            "v1_orders",
            "v1_cust",
         ].every((n) => stored.some((b) => b.name === n)),
      ).toBe(true);
      // Read only through gated entry points, each table still serves: it
      // holds only ungated rows, so the gate is grafted on the answer.
      expect(
         fixture.verdicts
            .filter((v) => /^(cust_sto|mz_orders|mz_cust|v1_cust):/.test(v))
            .sort(),
      ).toEqual([
         "cust_sto:serves",
         "mz_cust:serves",
         "mz_orders:serves",
         "v1_cust:serves",
      ]);
      // The one source the plan refused derives from a gated one. A v1
      // extension whose own gate reads through its join (`c.home_region`)
      // stores into its own table, whose gate columns cannot be listed, so
      // that table keeps no gate (#1855's rule): its queries answer live.
      // Every other gate is on an entry point the plan stores nothing for,
      // or a v1 extension's own (G3).
      expect(
         fixture.verdicts.filter((v) => v.includes(":live")).sort(),
      ).toEqual(["mz_derived:live:refused", "v1_by_home:live:row_level_gate"]);
      const plan = fixture.pkg.getBuildPlan()?.sources ?? {};
      const ungated = (
         fixture.pkg.materialization() as unknown as {
            plan: { sourceEligibility?: { ungated?: string[] } };
         }
      ).plan.sourceEligibility?.ungated;
      const names = (ungated ?? []).map((id) => plan[id]?.name).sort();
      expect(names).toContain("mz_orders");
      expect(names).not.toContain("mz_derived");
      // `v1_orders` is ungated itself, but its gated extensions share its
      // address, so its table is not marked (below).
      expect(names).toContain("v1_orders");
      expect(names).not.toContain("v1_regional");
   });

   it("a caller whose regions hold no rows gets an empty answer, never a denial", async () => {
      for (const p of ["v1", "mz"]) {
         const served = await runAudited(
            fixture.pkg,
            `run: ${p}_regional -> { group_by: region; aggregate: n }`,
            NOWHERE,
         );
         if ("error" in served.outcome) throw new Error(served.outcome.error);
         expect(served.outcome.answer.rows).toEqual([]);
         expect(served.outcome.answer.via).toBe("served_copy");
      }
   });

   it("marks each table that holds only ungated rows, and only those", () => {
      const bindings = (
         fixture.pkg.materialization() as unknown as {
            storageBindings: readonly {
               sourceName: string;
               holdsOnlyUngatedRows?: boolean;
               ownGateKept?: boolean;
            }[];
         }
      ).storageBindings;
      const flags = (name: string) => {
         const b = bindings.find((x) => x.sourceName === name);
         return `${name}:${b?.holdsOnlyUngatedRows === true ? "ungated" : ""}${b?.ownGateKept === true ? "kept" : ""}`;
      };
      expect(
         [
            "cust_sto",
            "mz_orders",
            "mz_cust",
            "v1_cust",
            // A v1 extension inherits `#@ persist` and shares its base's
            // table, so `v1_orders`' table holds a gated source's rows (its
            // gated extensions'): admitted past their own gate (G3) as
            // before, never proven ungated.
            "v1_orders",
            "v1_regional",
         ].map(flags),
      ).toEqual([
         "cust_sto:ungated",
         "mz_orders:ungated",
         "mz_cust:ungated",
         "v1_cust:ungated",
         "v1_orders:kept",
         "v1_regional:kept",
      ]);
   });

   for (const kase of CASES) {
      it(`${kase.name}: every caller, interleaved, gets live's rows or live's denial, and live's access record`, async () => {
         const answered: Record<string, Row[]> = {};
         for (const givens of ORDER) {
            const l = live.get(key(kase.malloy, givens))!;
            const served = await runAudited(fixture.pkg, kase.malloy, givens);
            expectAsLive(kase, givens, l.outcome, served.outcome);
            // The access record says what live's says of who read what…
            expect(summaryOf(served.audit)).toEqual(summaryOf(l.audit));
            // …its path names the served copy when it answered…
            if (!("error" in served.outcome) && !kase.live) {
               expect(served.audit.path?.servedBy.via).toBe("served_copy");
               expect(served.audit.servedFrom).toBe("storage");
            }
            // …and neither the record nor the path carries a gate value.
            const text = JSON.stringify({
               summary: served.audit.summary,
               path: served.audit.path,
            });
            for (const value of REGION_VALUES) {
               expect(text.includes(`"${value}"`)).toBe(false);
               expect(text.includes(`'${value}'`)).toBe(false);
            }
            if (!("error" in served.outcome)) {
               const k = JSON.stringify(givens);
               if (answered[k]) {
                  expect(
                     rowDifference(answered[k], served.outcome.answer.rows),
                  ).toBeUndefined();
               }
               answered[k] = served.outcome.answer.rows;
            }
         }
      });
   }

   it("a lock refuses a caller exactly as live does, on the plain and the inherited lock", async () => {
      for (const p of ["v1", "mz"]) {
         for (const name of ["locked", "locked_rows", "inh_lock"]) {
            const malloy = `run: ${p}_${name} -> { group_by: region; aggregate: n }`;
            const l = live.get(key(malloy, EAST_WEST))!;
            expect("error" in l.outcome).toBe(true);
            const served = await runAudited(fixture.pkg, malloy, EAST_WEST);
            expect("error" in served.outcome && served.outcome.error).toBe(
               "error" in l.outcome && l.outcome.error,
            );
         }
      }
   });

   it("an answer is never one caller's rows for another: two callers' regions differ", async () => {
      for (const p of ["v1", "mz"]) {
         const malloy = `run: ${p}_regional -> { group_by: region; aggregate: n }`;
         const north = await runAudited(fixture.pkg, malloy, NORTH);
         const eastWest = await runAudited(fixture.pkg, malloy, EAST_WEST);
         if ("error" in north.outcome || "error" in eastWest.outcome) {
            throw new Error("failed");
         }
         expect(north.outcome.answer.rows.map((r) => r.region)).toEqual([
            "north",
         ]);
         expect(
            eastWest.outcome.answer.rows.map((r) => r.region).sort(),
         ).toEqual(["east", "west"]);
      }
   });

   describe("each guard is load-bearing", () => {
      const guarded = CASES.filter((c) =>
         c.name.endsWith("the gated entry point"),
      );
      /**
       * The g-mixed answers alone: a `#(materialize)` entry point over its
       * ungated base's table, and the v1 one joining an ungated v1 table
       * (the v1 base itself shares its table with its gated extensions).
       */
      const ungatedOnly = CASES.filter(
         (c) =>
            c.malloy.startsWith("run: mz_regional -> { group_by") ||
            c.malloy.startsWith("run: v1_by_home"),
      );

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
                     NORTH,
                  );
                  expect("error" in served).toBe(true);
               }
            },
         );
      });

      it("with the re-applied gate removed, the per-caller check fails: the table holds other callers' rows", async () => {
         await patched(
            governor(),
            "govern",
            () =>
               async (_request: unknown, compiled: { runnable: unknown }) => ({
                  runnable: compiled.runnable,
                  decision: { enforced: undefined, grafts: [] },
               }),
            async () => {
               for (const kase of guarded) {
                  const served = await fixture.run(
                     "m.malloy",
                     kase.malloy,
                     NORTH,
                  );
                  if ("error" in served) throw new Error(served.error);
                  expect(served.answer.via).toBe("served_copy");
                  expect(
                     new Set(served.answer.rows.map((r) => r.region)).size,
                  ).toBeGreaterThan(1);
               }
            },
         );
      });

      it("an answer reading a storage= table not proven to hold only ungated rows answers live", async () => {
         type Served = {
            gateTablesRead?: { sourceName?: string }[];
         };
         const copy = (
            fixture.pkg.getModel("m.malloy") as unknown as {
               servedCopy: {
                  loadQuery: (...args: unknown[]) => Promise<Served>;
               };
            }
         ).servedCopy;
         const loadQuery = copy.loadQuery;
         // As if the plan could not prove the table ungated (a gate on a
         // source sharing its address, or a walk that failed).
         copy.loadQuery = async (...args) => {
            const answer = await loadQuery.apply(copy, args);
            return {
               ...answer,
               gateTablesRead: (answer.gateTablesRead ?? []).map((t) => ({
                  ...t,
                  holdsOnlyUngatedRows: false,
               })),
            };
         };
         try {
            expect(ungatedOnly.length).toBeGreaterThan(2);
            for (const kase of ungatedOnly) {
               const served = await fixture.run("m.malloy", kase.malloy, NORTH);
               if ("error" in served) throw new Error(served.error);
               expect(served.answer.via ?? served.answer.servedBy).toBe("live");
               const l = live.get(key(kase.malloy, NORTH))!;
               if ("error" in l.outcome) throw new Error(l.outcome.error);
               expect(
                  rowDifference(l.outcome.answer.rows, served.answer.rows),
               ).toBeUndefined();
            }
         } finally {
            copy.loadQuery = loadQuery;
         }
      });
   });
});
