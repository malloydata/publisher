// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for a rollup over a base whose every gate is an `#(authorize)` lock
 * reading no row field (coverage row g-inh-lock). Such a lock admits or
 * refuses the whole query, decided per request on the live entry point, so
 * pre-grouped rows cannot change its outcome. The rollup is built and read
 * past it on both tiers, with the gated-sources switch on and off, for a lock
 * the base inherits (at depth one and two, through an `extend` chain, from an
 * imported file), its own, its own replacing an inherited one, and a locked base re-opened by
 * `true`.
 *
 * Every caller equals live: an admitted caller gets live's rows from the
 * rollup; a refused caller and a caller missing the given are denied exactly
 * as live denies them. A query on an extension that adds a row filter of its
 * own over a `storage=` rollup is not a lock-only entry point, so with the
 * switch off it answers live; a base with an inherited lock and a row filter
 * is not lock-only either.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   DESTINATION,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
   type R4Run,
} from "./harness";

const seedSql = [
   `CREATE TABLE orders_raw AS SELECT * FROM (VALUES
      (1, 10.0, 'A', 'east'),
      (2, 20.0, 'A', 'west'),
      (3, 30.0, 'B', 'east'),
      (4,  5.0, 'B', 'west'),
      (5, 12.5, 'C', 'west'),
      (6,  7.5, 'C', 'east')
    ) AS t(id, amount, category, region)`,
];

function baseFile(): string {
   return `##! experimental { givens access_modifiers }
given: TEAM :: string

#(authorize) 'ops' = $TEAM
source: imported_locked is duckdb.table('orders_raw')
`;
}

function model(placement: string): string {
   const p = placement;
   return `##! experimental { givens access_modifiers }
import "base.malloy"
given: ROLE :: string
given: ZONE :: string
given: REGION :: string

// Ungated, so the companion is never empty.
source: open is duckdb.table('orders_raw') extend {
  #(rollup) grain=["category"]${p}
  measure: open_total is amount.sum()
}

#(authorize) 'admin' = $ROLE
source: locked is duckdb.table('orders_raw')

// Depth one: the lock is inherited.
source: inh1 is locked extend {
  #(rollup) grain=["category"]${p}
  measure: total is amount.sum()
  #(rollup) grain=["category"]${p}
  measure: inh1_n is count()
}

// Depth two, through an extend chain that adds a field.
source: mid is locked extend {
  dimension: cat_lower is lower(category)
}
source: inh2 is mid extend {
  #(rollup) grain=["category"]${p}
  measure: total is amount.sum()
  #(rollup) grain=["category"]${p}
  measure: inh2_n is count()
}

// From another file.
source: inh_import is imported_locked extend {
  #(rollup) grain=["category"]${p}
  measure: total is amount.sum()
  #(rollup) grain=["category"]${p}
  measure: imp_n is count()
}

// Its own lock.
#(authorize) 'admin' = $ROLE
source: own_lock is duckdb.table('orders_raw') extend {
  #(rollup) grain=["category"]${p}
  measure: total is amount.sum()
  #(rollup) grain=["category"]${p}
  measure: own_n is count()
}

// Its own lock on a source extending a locked one: it replaces the
// inherited lock.
#(authorize) 'eu' = $ZONE
source: stacked is locked extend {
  #(rollup) grain=["category"]${p}
  measure: total is amount.sum()
  #(rollup) grain=["category"]${p}
  measure: st_n is count()
}

// A locked base re-opened by true.
#(authorize) false
source: shut is duckdb.table('orders_raw')
#(authorize) true
source: reopened is shut extend {
  #(rollup) grain=["category"]${p}
  measure: total is amount.sum()
  #(rollup) grain=["category"]${p}
  measure: ro_n is count()
}

// Not lock-only: an inherited lock and a row filter.
#(access_filter) region = $REGION
source: lock_and_filter is locked extend {
  #(rollup) grain=["category", "region"]${p}
  measure: total is amount.sum()
  #(rollup) grain=["category", "region"]${p}
  measure: lf_n is count()
}

// An extension of a lock-only base that adds a row filter of its own: its
// entry point reads a column, so it is not a lock-only answer.
#(access_filter) region = $REGION
source: inh1_filtered is inh1 extend {}

// A lock beside a given-reading where: of the base's own, own and inherited.
// The router counts the where:'s column as a gate column, so on storage= no
// answer is lock-only: the plan agrees and builds no such rollup there.
given: CAT :: string
#(authorize) 'admin' = $ROLE
source: lw is duckdb.table('orders_raw') extend {
  where: category = $CAT
  #(rollup) grain=["category"]${p}
  measure: lw_total is amount.sum()
}
source: inh_w is locked extend {
  where: region = $REGION
  #(rollup) grain=["category", "region"]${p}
  measure: iw_total is amount.sum()
}

// The same over a field the rollup keeps: only the router's lock-only rule
// tells it from a lock-only entry point.
#(access_filter) category = $CAT
source: inh1_by_cat is inh1 extend {}
`;
}

type Givens = Record<string, string>;

interface LockCase {
   source: string;
   admitted: Givens[];
   refused: Givens[];
   missing: Givens[];
}

const ADMIN: Givens = { ROLE: "admin" };
const CASES: LockCase[] = [
   {
      source: "inh1",
      admitted: [ADMIN],
      refused: [{ ROLE: "viewer" }, { ROLE: "" }],
      missing: [{}],
   },
   {
      source: "inh2",
      admitted: [ADMIN],
      refused: [{ ROLE: "Admin" }],
      missing: [{}],
   },
   {
      source: "inh_import",
      admitted: [{ TEAM: "ops" }],
      refused: [{ TEAM: "dev" }],
      missing: [{}, ADMIN],
   },
   {
      source: "own_lock",
      admitted: [ADMIN],
      refused: [{ ROLE: "viewer" }],
      missing: [{}],
   },
   {
      source: "stacked",
      // Its own lock replaces the one it would inherit, as live reads it.
      admitted: [
         { ROLE: "admin", ZONE: "eu" },
         { ROLE: "viewer", ZONE: "eu" },
         { ZONE: "eu" },
      ],
      refused: [{ ROLE: "admin", ZONE: "us" }],
      missing: [ADMIN, {}],
   },
   { source: "reopened", admitted: [{}, ADMIN], refused: [], missing: [] },
];

const QUERIES = (s: string) => [
   `run: ${s} -> { group_by: category; aggregate: total }`,
   `run: ${s} -> { aggregate: total }`,
   `run: ${s} -> { where: category != 'C'; group_by: category; aggregate: total }`,
   `run: ${s} -> { group_by: category; aggregate: total } -> { aggregate: grand is total.sum() }`,
];

function expectRollup(run: R4Run): void {
   expect(run.servedBy?.kind).toBe("rollup");
   expect(rowDifference(run.live, run.served)).toBeUndefined();
   expect(run.served.length).toBeGreaterThan(0);
}

function expectLive(run: R4Run): void {
   expect(run.servedBy?.kind).not.toBe("rollup");
   expect(rowDifference(run.live, run.served)).toBeUndefined();
}

/** Both runs refuse, with the same error (a denial, or a missing given). */
async function expectDeniedAsLive(
   fixture: R4Fixture,
   malloy: string,
   givens: Givens,
): Promise<void> {
   const outcome = async () => {
      try {
         await fixture.runAsIs(malloy, givens);
         return "answered";
      } catch (e) {
         // A refusal is AccessDeniedError; a missing given a where: reads
         // is the compiler's error. Either way served must equal live.
         if (!(e instanceof Error)) throw e;
         return `${(e as Error).constructor.name}: ${(e as Error).message}`;
      }
   };
   fixture.setBindings([]);
   const live = await outcome();
   fixture.setBindings(fixture.bindings);
   const served = await outcome();
   expect(live).not.toBe("answered");
   expect(served).toBe(live);
}

for (const placement of ["database", "storage"] as const) {
   for (const gatedSources of [false, true]) {
      describe(`R4: lock-only gates on ${placement} rollups (gated sources ${gatedSources ? "on" : "off"})`, () => {
         let restore: () => void;
         let fixture: R4Fixture;
         beforeAll(async () => {
            restore = installSettings({
               env: {
                  PERSIST_STORAGE_MODE: "on",
                  PUBLISHER_MATERIALIZE: "true",
                  PUBLISHER_MATERIALIZE_GATED_SOURCES: String(gatedSources),
               },
            }).restore;
            fixture = await buildScenario({
               files: {
                  "m.malloy": model(
                     placement === "storage" ? ` storage="${DESTINATION}"` : "",
                  ),
                  "base.malloy": baseFile(),
               },
               seedSql,
               preaggregate: true,
            });
         });
         afterAll(async () => {
            restore();
            await disposeScenarios();
         });

         it("builds every lock-only base's rollups", () => {
            const refused = new Set(
               fixture.rollupRefusals.map((r) => r.sourceName),
            );
            for (const c of CASES) expect(refused.has(c.source)).toBe(false);
         });

         for (const c of CASES) {
            it(`answers an admitted caller of ${c.source} from the rollup, equal to live`, async () => {
               // Interleaved and repeated with the refusals below.
               for (const round of [1, 2]) {
                  for (const givens of c.admitted) {
                     for (const malloy of QUERIES(c.source)) {
                        expectRollup(
                           await fixture.run({
                              name: `${c.source} ${JSON.stringify(givens)} ${round}`,
                              malloy,
                              givens,
                           }),
                        );
                     }
                  }
                  for (const givens of [...c.refused, ...c.missing]) {
                     await expectDeniedAsLive(
                        fixture,
                        QUERIES(c.source)[0],
                        givens,
                     );
                  }
               }
            });
         }

         it("governs a served answer on the author's source, gates and decision equal to live's", async () => {
            const { unrecordedAudit } = await import(
               "../../../src/service/access_record"
            );
            const { QueryPrivileges } = await import(
               "../../../src/service/model_query_request"
            );
            const governed = async (malloy: string, givens: Givens) => {
               const audit = unrecordedAudit();
               const result = await fixture.model.runQuery(
                  audit,
                  { query: malloy, filterParams: {}, givens },
                  QueryPrivileges.of({ bypassFilters: true }),
               );
               return { audit, result };
            };
            for (const c of CASES) {
               const malloy = QUERIES(c.source)[0];
               fixture.setBindings([]);
               const live = await governed(malloy, c.admitted[0]);
               fixture.setBindings(fixture.bindings);
               const served = await governed(malloy, c.admitted[0]);
               expect(served.result.path?.servedBy?.kind).toBe("rollup");
               expect(served.audit.summary?.source).toBe(
                  live.audit.summary?.source,
               );
               expect(served.audit.summary?.decision).toBe(
                  live.audit.summary?.decision,
               );
               const sorted = (g?: {
                  authorize: string[];
                  accessFilter: string[];
               }) =>
                  g && {
                     authorize: [...new Set(g.authorize)].sort(),
                     accessFilter: [...new Set(g.accessFilter)].sort(),
                  };
               expect(sorted(served.audit.summary?.gates)).toEqual(
                  sorted(live.audit.summary?.gates),
               );
            }
         });

         it("builds a lock beside a given-reading where: as its gates allow, and answers every caller as live does", async () => {
            const refused = fixture.rollupRefusals.map((r) => r.sourceName);
            const database = placement === "database";
            // This harness binds no `storage=` table as plan-admitted
            // (`ownGateKept`), so with the switch off a `storage=` answer
            // past a column-reading gate is live here;
            // storage_rollup_gates.component.spec.ts takes the bind.
            const fromRollup = database || gatedSources;
            for (const [source, malloy, admitted, refusedCallers] of [
               [
                  "lw",
                  "run: lw -> { group_by: category; aggregate: lw_total }",
                  [
                     { ROLE: "admin", CAT: "A" },
                     { ROLE: "admin", CAT: "Z" },
                  ],
                  [{ ROLE: "viewer", CAT: "A" }, { CAT: "A" }, ADMIN],
               ],
               [
                  "inh_w",
                  "run: inh_w -> { group_by: category; aggregate: iw_total }",
                  [
                     { ROLE: "admin", REGION: "east" },
                     { ROLE: "admin", REGION: "north" },
                  ],
                  [{ ROLE: "viewer", REGION: "east" }, ADMIN],
               ],
            ] as [string, string, Givens[], Givens[]][]) {
               // `lw`'s lock is its own (G3 on either tier); `inh_w`'s is
               // inherited and, beside its `where:`, not lock-only on
               // `storage=`, so it is built there only with the switch on.
               expect(refused.includes(source)).toBe(
                  source === "inh_w" && !database && !gatedSources,
               );
               for (const givens of admitted) {
                  const run = await fixture.run({
                     name: `${source} ${JSON.stringify(givens)}`,
                     malloy,
                     givens,
                     expectLive: !fromRollup,
                  });
                  expect(rowDifference(run.live, run.served)).toBeUndefined();
                  if (fromRollup) expect(run.servedBy?.kind).toBe("rollup");
                  else expectLive(run);
               }
               for (const givens of refusedCallers) {
                  await expectDeniedAsLive(fixture, malloy, givens);
               }
            }
         });

         it("answers a lock with a row filter as live does", async () => {
            for (const givens of <Givens[]>[
               { ROLE: "admin", REGION: "east" },
               { ROLE: "admin", REGION: "west" },
               { ROLE: "admin", REGION: "north" },
            ]) {
               const run = await fixture.run({
                  name: `lock_and_filter ${givens.REGION}`,
                  malloy:
                     "run: lock_and_filter -> { group_by: category; aggregate: total }",
                  givens,
                  // With the switch on, a rollup on either tier keeps an
                  // inherited gate (row 1, and P3/P4 on `storage=`);
                  // with it off this base is not lock-only.
                  expectLive: !gatedSources,
               });
               expect(rowDifference(run.live, run.served)).toBeUndefined();
               if (!gatedSources) {
                  expectLive(run);
               }
            }
            for (const givens of <Givens[]>[
               { ROLE: "viewer", REGION: "east" },
               ADMIN,
            ]) {
               await expectDeniedAsLive(
                  fixture,
                  "run: lock_and_filter -> { group_by: category; aggregate: total }",
                  givens,
               );
            }
         });

         it("answers an extension adding a row filter over a lock-only base as live does", async () => {
            for (const region of ["east", "west", "north"]) {
               const run = await fixture.run({
                  name: `inh1_filtered ${region}`,
                  malloy:
                     "run: inh1_filtered -> { group_by: category; aggregate: total }",
                  givens: { ROLE: "admin", REGION: region },
                  expectLive: true,
               });
               // Its row filter reads `region`, which the rollup does not
               // keep: never answered from it, on any tier or switch.
               expectLive(run);
            }
            for (const givens of <Givens[]>[
               { ROLE: "viewer", REGION: "east" },
               { REGION: "east" },
               ADMIN,
            ]) {
               await expectDeniedAsLive(
                  fixture,
                  "run: inh1_filtered -> { group_by: category; aggregate: total }",
                  givens,
               );
            }
         });

         it("reads a rollup past an extension's row filter on a grain field when it keeps the gate", async () => {
            for (const cat of ["A", "B", "Z"]) {
               // Database always; `storage=` with the switch on. With it
               // off, a `storage=` answer is kept only for a table the bind
               // marks, which this harness does not run.
               const servedFromRollup =
                  placement === "database" || gatedSources;
               const run = await fixture.run({
                  name: `inh1_by_cat ${cat}`,
                  malloy:
                     "run: inh1_by_cat -> { group_by: category; aggregate: total }",
                  givens: { ROLE: "admin", CAT: cat },
                  expectLive: !servedFromRollup,
               });
               expect(rowDifference(run.live, run.served)).toBeUndefined();
               if (servedFromRollup) {
                  expect(run.servedBy?.kind).toBe("rollup");
               } else {
                  expectLive(run);
               }
            }
            for (const givens of <Givens[]>[
               { ROLE: "viewer", CAT: "A" },
               ADMIN,
            ]) {
               await expectDeniedAsLive(
                  fixture,
                  "run: inh1_by_cat -> { group_by: category; aggregate: total }",
                  givens,
               );
            }
         });
      });
   }
}

/**
 * The join-to-rollup leg (A7) over a stored parent table: a parent joining a
 * rollup's base, its own table stored, the rollup in the same tier. Ungated,
 * the join leg engages: database it answers from the rollup; on `storage=`
 * this harness cannot store the DuckDB parent in the destination, so it
 * declines `CROSS_CONNECTION`. Behind a lock, own or
 * inherited, the leg declines `ROW_LEVEL_GATE` before the gate policy is asked (its
 * variant redeclares the parent around the join and carries no gate), so
 * `gatesReadNoColumn` never widens it: every caller is answered as live is.
 */
const KIDS_SEED = [
   ...seedSql,
   `CREATE TABLE kids_raw AS SELECT * FROM (VALUES
      (1, 1, 2.0), (2, 1, 3.0), (3, 2, 4.0), (4, 3, 1.0), (5, 5, 6.0), (6, 6, 2.5)
    ) AS t(kid_id, pid, v)`,
];

/** The rollup leg's step in a path report. */
function rollupStep(
   path: unknown,
): { outcome?: string; reason?: string } | undefined {
   return (
      path as
         | {
              considered?: {
                 kind: string;
                 outcome?: string;
                 reason?: string;
              }[];
           }
         | undefined
   )?.considered?.find((c) => c.kind === "rollup");
}

function joinModel(rollup: string, persist: string): string {
   return `##! experimental { givens access_modifiers }
given: ROLE :: string

source: kids is duckdb.table('kids_raw') extend {
  primary_key: kid_id
  #(rollup) grain=["pid"]${rollup}
  measure: kid_amt is v.sum()
}

#@ persist${persist}
source: parent is duckdb.table('orders_raw') extend {
  primary_key: id
  join_many: kids on kids.pid = id
}

#(authorize) 'admin' = $ROLE
source: parent_locked is parent extend {}

source: parent_inherited is parent_locked extend {}
`;
}

for (const placement of ["database", "storage"] as const) {
   for (const gatedSources of [false, true]) {
      describe(`R4: the join-to-rollup leg over a stored parent behind a lock, ${placement} (gated sources ${gatedSources ? "on" : "off"})`, () => {
         let restore: () => void;
         let fixture: R4Fixture;
         beforeAll(async () => {
            restore = installSettings({
               env: {
                  PERSIST_STORAGE_MODE: "on",
                  PUBLISHER_MATERIALIZE: "true",
                  PUBLISHER_MATERIALIZE_GATED_SOURCES: String(gatedSources),
               },
            }).restore;
            const at =
               placement === "storage" ? ` storage="${DESTINATION}"` : "";
            fixture = await buildScenario({
               files: { "m.malloy": joinModel(at, at) },
               seedSql: KIDS_SEED,
               preaggregate: true,
            });
         });
         afterAll(async () => {
            restore();
            await disposeScenarios();
         });

         const query = (s: string) =>
            `run: ${s} -> { group_by: category; aggregate: kids.kid_amt }`;

         it("engages for the ungated parent, equal to live", async () => {
            const run = await fixture.run({
               name: "parent",
               malloy: query("parent"),
               expectLive: placement === "storage",
            });
            expect(rowDifference(run.live, run.served)).toBeUndefined();
            if (placement === "database") {
               expect(run.servedBy?.kind).toBe("rollup");
               return;
            }
            // A DuckDB parent is not stored in a destination here, so the
            // leg is tried and declines across connections.
            fixture.setBindings(fixture.bindings);
            const served = await fixture.runAsIs(query("parent"));
            expect(rollupStep(served.path)?.reason).toBe("CROSS_CONNECTION");
         });

         for (const source of ["parent_locked", "parent_inherited"]) {
            // A lock reads no field of the parent, so on the database tier
            // the join branch answers past it (g-mixed for rollups, #1854):
            // the variant redeclares the parent with its lock, which admits
            // or denies the caller as live does. In a destination the
            // parent is not stored here, so it declines as for `parent`.
            it(`answers every caller of ${source} as live does, from the rollup on the database tier`, async () => {
               const run = await fixture.run({
                  name: `${source} admin`,
                  malloy: query(source),
                  givens: ADMIN,
                  expectLive: placement === "storage",
               });
               expect(run.servedBy?.kind === "rollup").toBe(
                  placement === "database",
               );
               expect(rowDifference(run.live, run.served)).toBeUndefined();
               for (const givens of <Givens[]>[{ ROLE: "viewer" }, {}]) {
                  await expectDeniedAsLive(fixture, query(source), givens);
               }
            });
         }
      });
   }
}
