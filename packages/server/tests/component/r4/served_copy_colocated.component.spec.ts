// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for colocated `#(materialize)` (materialization v2, phase 3 pull request
 * 6): a source with no `storage=` is built into its own warehouse and the
 * served copy re-roots it on that table, read through the source's own
 * connection, so every served-copy feature applies to it as it does to a
 * `storage=` table: a stored computed dimension and timeframe, an inlined
 * join_one, a pair table, private and internal fields, a given-reading
 * `where:` re-applied per caller, and a row-level gate grafted and proven on
 * the served compile. Every answer equals live, on DuckDB and, when
 * `PUBLISHER_TEST_POSTGRES_URL` names one, on a real Postgres (the docker
 * `postgres:16` the gates run). A query the copy cannot answer exactly (a
 * parent-only query over a pair table) is answered live, and equals live too.
 */

import { PostgresConnection } from "@malloydata/db-postgres";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import { DATA_FILES, RAW_MODEL } from "./conformance/data";
import { freshLoadPool, loadFixture } from "./conformance/package_harness";
import {
   buildScenario,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
   type R4Scenario,
   type Row,
   expectPairServedVia,
} from "./harness";

/** Measures that count through the inlined path (see {@link expectPairServedVia}). */
const PATH_COUNTS: string[] = [];

const POSTGRES_URL = process.env.PUBLISHER_TEST_POSTGRES_URL;
const PG_SCHEMA = "r4_colocated";

/** The model, on connection `w` with its tables in `schema` (or none). */
function model(w: string, schema: string): string {
   const t = (name: string) => `${w}.table('${schema}${name}')`;
   return `##! experimental { givens access_modifiers }
given: ORG :: string is 'acme'
given: GROUPS :: string[]

source: carriers is ${t("carriers")} extend { primary_key: code }
source: segs is ${t("segs")} extend { primary_key: seg_id }

source: base is ${t("flights")} include { internal: org; public: * }

#(materialize) name="r4c_flights" inline=["carriers"]
source: flights is ${t("flights")} extend {
  primary_key: id
  join_one: carriers with carrier
  dimension:
    long_haul is distance > 500
    dep_month is dep.month
    booked_month is booked.month
  measure:
    total_distance is distance.sum()
    flight_count is count()
  view: by_carrier is { group_by: carriers.nickname; aggregate: total_distance }
}

#(materialize) name="r4c_hidden"
source: hidden is base extend {
  private dimension: secret is distance * 2
  measure: secret_total is secret.sum(), org_n is count(org)
}

#(materialize) name="r4c_pairs" inline=["segs"]
source: flight_segs is ${t("flights")} extend {
  primary_key: id
  join_many: segs on segs.flight_id = id
  measure:
    flight_count is count()
    seg_n is segs.count()
    seg_miles is segs.miles.sum()
}

#(materialize) name="r4c_org"
source: org_flights is ${t("flights")} extend {
  where: org = $ORG
  measure: total_distance is distance.sum()
}

#(materialize) name="r4c_carriers"
source: carriers_m is ${t("carriers")} extend {
  primary_key: code
  dimension: loud is upper(nickname)
}

// Not materialized: it reaches carriers_m only through its join (#6643),
// beside a join to a warehouse table nothing stores (#6983).
source: legs is ${t("flights")} extend {
  join_one: carriers_m with carrier
  join_one: segs_first is ${t("segs")} on segs_first.flight_id = id
  measure: n is count()
}

#(access_filter) org in $GROUPS
#(materialize) name="r4c_gated"
source: gated is base extend {
  measure: total_distance is distance.sum(), flight_count is count()
}
`;
}

/** Portable seed SQL, DuckDB and Postgres alike, with tables in `schema`. */
function seed(schema: string): string[] {
   return [
      `CREATE TABLE ${schema}flights AS SELECT * FROM (VALUES
         (1, 'AA', 'acme', 100.0, TIMESTAMP '2026-01-03 10:00:00', DATE '2025-12-30'),
         (2, 'AA', 'globex', 900.0, TIMESTAMP '2026-01-20 23:30:00', DATE '2026-01-02'),
         (3, 'UA', 'acme', 650.0, TIMESTAMP '2026-02-01 00:00:00', DATE '2026-01-31'),
         (4, 'DL', 'initech', 50.0, TIMESTAMP '2026-02-14 12:00:00', DATE '2026-02-01'),
         (5, 'UA', 'globex', 300.0, TIMESTAMP '2026-03-31 23:59:59', DATE '2026-03-15'),
         (6, 'XX', 'acme', 720.0, TIMESTAMP '2026-03-01 06:00:00', DATE '2026-02-28')
       ) AS t(id, carrier, org, distance, dep, booked)`,
      `CREATE TABLE ${schema}carriers AS SELECT * FROM (VALUES
         ('AA', 'American'), ('UA', 'United'), ('DL', 'Delta')
       ) AS t(code, nickname)`,
      `CREATE TABLE ${schema}segs AS SELECT * FROM (VALUES
         (1, 1, 40.0), (2, 1, 60.0), (3, 2, 900.0), (4, 3, 300.0),
         (5, 3, 350.0), (6, 5, 300.0), (7, 99, 10.0)
       ) AS t(seg_id, flight_id, miles)`,
   ];
}

/** Each query, the givens it runs under, and whether the served copy answers it. */
const QUERIES: {
   name: string;
   malloy: string;
   givens?: Record<string, unknown>;
   live?: true;
}[] = [
   { name: "the inlined join's view", malloy: "run: flights -> by_carrier" },
   {
      name: "a stored computed dimension and a stored timeframe",
      malloy:
         "run: flights -> { group_by: long_haul, dep_month; aggregate: total_distance, flight_count }",
   },
   {
      name: "a stored date truncation, grouped, filtered and re-truncated",
      malloy:
         "run: flights -> { where: booked_month >= @2026-01; group_by: booked_month, y is booked_month.year; aggregate: flight_count }",
   },
   {
      name: "a re-truncated stored timeframe, filtered by a literal",
      malloy:
         "run: flights -> { where: dep_month = @2026-02; group_by: q is dep_month.quarter; aggregate: flight_count }",
   },
   {
      name: "measures over a private and an internal stored column",
      malloy:
         "run: hidden -> { group_by: carrier; aggregate: secret_total, org_n }",
   },
   {
      name: "child measures from the pair table",
      malloy:
         "run: flight_segs -> { group_by: carrier; aggregate: seg_n, seg_miles }",
   },
   {
      name: "a materialized source reached only through a join, beside a warehouse join",
      malloy:
         "run: legs -> { group_by: carriers_m.loud; aggregate: n, m is segs_first.miles.sum() }",
   },
   {
      name: "a parent-only query, answered live past the pair table",
      malloy:
         "run: flight_segs -> { group_by: carrier; aggregate: flight_count }",
      live: true,
   },
   ...["acme", "globex"].map((ORG) => ({
      name: `a given term re-applied for ${ORG}`,
      malloy: "run: org_flights -> { aggregate: total_distance }",
      givens: { ORG },
   })),
   ...[["acme"], ["acme", "globex"], ["nobody"]].map((GROUPS) => ({
      name: `a row-level gate for ${GROUPS.join("+")}`,
      malloy:
         "run: gated -> { group_by: carrier; aggregate: total_distance, flight_count }",
      givens: { GROUPS },
   })),
];

/** Fields with a number's integer or decimal subtype left out. */
function withoutNumberSubtype(fields: { name: string; type: unknown }[]) {
   return JSON.parse(
      JSON.stringify(fields, (key, value) =>
         key === "subtype" ? undefined : value,
      ),
   );
}

const tiers: {
   label: string;
   scenario: () => Promise<R4Scenario & { cleanup?: () => Promise<void> }>;
}[] = [
   {
      label: "DuckDB",
      scenario: async () => ({
         files: { "m.malloy": model("duckdb", "") },
         seedSql: seed(""),
         materialize: true,
      }),
   },
   ...(POSTGRES_URL
      ? [
           {
              label: "Postgres",
              scenario: async () => {
                 const pg = new PostgresConnection({
                    name: "pg",
                    connectionString: POSTGRES_URL,
                 });
                 await pg.runSQL(`DROP SCHEMA IF EXISTS ${PG_SCHEMA} CASCADE`);
                 await pg.runSQL(`CREATE SCHEMA ${PG_SCHEMA}`);
                 return {
                    files: { "m.malloy": model("pg", `${PG_SCHEMA}.`) },
                    seedSql: seed(`${PG_SCHEMA}.`),
                    materialize: true,
                    warehouse: {
                       name: "pg",
                       connection: pg,
                       schema: PG_SCHEMA,
                    },
                    cleanup: async () => {
                       await pg.runSQL(
                          `DROP SCHEMA IF EXISTS ${PG_SCHEMA} CASCADE`,
                       );
                       await pg.close();
                    },
                 };
              },
           },
        ]
      : []),
];

for (const tier of tiers) {
   describe(`R4: colocated #(materialize) served from its own warehouse equals live (${tier.label})`, () => {
      let restore: () => void;
      let fixture: R4Fixture;
      let cleanup: (() => Promise<void>) | undefined;
      beforeAll(async () => {
         // The storage tier is off: a colocated table needs none.
         restore = installSettings({
            env: {
               PERSIST_STORAGE_MODE: "off",
               PUBLISHER_MATERIALIZE: "true",
               PUBLISHER_MATERIALIZE_GATED_SOURCES: "true",
            },
         }).restore;
         const scenario = await tier.scenario();
         cleanup = scenario.cleanup;
         fixture = await buildScenario(scenario);
      });
      afterAll(async () => {
         restore();
         await disposeScenarios();
         await cleanup?.();
      });

      it("binds each table colocated, keyed by its content address on its own connection", () => {
         const connection = tier.label === "DuckDB" ? "duckdb" : "pg";
         expect(
            fixture.bindings
               .map((b) => [
                  b.sourceName,
                  b.colocated,
                  b.destinationName,
                  b.virtualHandle === fixture.tableIds[b.sourceName],
               ])
               .sort(),
         ).toEqual(
            [
               "carriers_m",
               "flight_segs",
               "flights",
               "gated",
               "hidden",
               "org_flights",
            ].map((name) => [name, true, connection, true]),
         );
      });

      it("compiles the served copy with no problem", async () => {
         expect(await fixture.servedCopyProblems()).toEqual([]);
      });

      for (const query of QUERIES) {
         it(query.name, async () => {
            const run = await fixture.run({
               name: query.name,
               malloy: query.malloy,
               ...(query.givens ? { givens: query.givens as never } : {}),
            });
            expect(rowDifference(run.live, run.served)).toBeUndefined();
            // The result's fields too, a timeframe's included, but for a
            // number's subtype, which no stored column carries (known
            // limitations, row 12).
            expect(withoutNumberSubtype(run.servedFields)).toEqual(
               withoutNumberSubtype(run.liveFields),
            );
            if (query.live) {
               expect(run.servedVia).toBeUndefined();
               return;
            }
            expectPairServedVia(run, PATH_COUNTS);
            // A count through the pair table's join is answered live.
            if (run.servedVia === undefined) return;
            // The source's own warehouse: not a storage hit.
            expect(run.servedFrom).toBeNull();
            expect(run.servedBy?.kind).toBe("materialized");
         });
      }

      it("refuses a private or internal stored column to a caller, as live does", async () => {
         fixture.setBindings(fixture.bindings);
         for (const malloy of [
            "run: hidden -> { select: secret }",
            "run: hidden -> { select: org }",
         ]) {
            await expect(fixture.runAsIs(malloy)).rejects.toThrow();
         }
         fixture.setBindings([]);
         for (const malloy of [
            "run: hidden -> { select: secret }",
            "run: hidden -> { select: org }",
         ]) {
            await expect(fixture.runAsIs(malloy)).rejects.toThrow();
         }
      });

      it("reports the pair table a parent-only query skips, on its own tier", async () => {
         fixture.setBindings(fixture.bindings);
         const { path } = await fixture.runAsIs(
            "run: flight_segs -> { group_by: carrier; aggregate: flight_count }",
         );
         expect(path?.servedBy.kind).toBe("live");
         expect(path?.considered).toContainEqual(
            expect.objectContaining({
               tableId: fixture.tableIds.flight_segs,
               outcome: "skipped",
               reason: "COUNT_SEMANTICS",
            }),
         );
      });

      it("keeps a colocated table off every leg but the served copy", () => {
         fixture.setBindings(fixture.bindings);
         // The serve shape, the rollup and persisted-view legs hold none.
         expect(fixture.model.servedStorageSourceNames()).toEqual([]);
      });

      it("denies a gated query with no group, as live does", async () => {
         const malloy = "run: gated -> { aggregate: flight_count }";
         fixture.setBindings(fixture.bindings);
         const served = await fixture
            .runAsIs(malloy)
            .then(() => "answered")
            .catch((e: Error) => e.constructor.name);
         fixture.setBindings([]);
         const live = await fixture
            .runAsIs(malloy)
            .then(() => "answered")
            .catch((e: Error) => e.constructor.name);
         expect(served).toBe(live);
      });
   });
}

/**
 * Through the real package path (conformance/package_harness.ts): the plan's
 * eligibility, the colocated manifest bind, the router and `govern()`.
 */
describe("colocated #(materialize) through the package: gates, and v1 unchanged", () => {
   const GATED = `##! experimental { givens }
import "raw.malloy"

given: REGIONS :: string[]

#(access_filter) region in $REGIONS
#(materialize) name="gated_tbl"
source: gated is orders_raw extend {
  measure: total is amount.sum(), n is count()
}
`;
   const QUERY = "run: gated -> { group_by: status; aggregate: total, n }";
   const CALLERS = [["north"], ["north", "east"], ["nowhere"]];

   async function loaded(gatedSources: "true" | "false") {
      const restore = installSettings({
         env: {
            PERSIST_STORAGE_MODE: "off",
            PUBLISHER_MATERIALIZE: "true",
            PUBLISHER_MATERIALIZE_GATED_SOURCES: gatedSources,
         },
      }).restore;
      // The plan (and its eligibility) is computed in a load worker, which
      // reads its settings at spawn.
      await freshLoadPool();
      const loader = await loadFixture({
         ...DATA_FILES,
         "raw.malloy": RAW_MODEL,
         "m.malloy": GATED,
      });
      const live: Row[][] = [];
      for (const REGIONS of CALLERS) {
         const outcome = await loader.run("m.malloy", QUERY, { REGIONS });
         if ("error" in outcome) throw new Error(outcome.error);
         live.push(outcome.answer.rows);
      }
      return { fixture: await loader.bind(), live, restore };
   }

   afterAll(async () => {
      await freshLoadPool();
   });

   it("serves a gated colocated source from the served copy, the gate grafted, for each caller equal to live", async () => {
      const { fixture, live, restore } = await loaded("true");
      try {
         expect(fixture.built).toEqual([{ name: "gated", tier: "colocated" }]);
         expect(fixture.verdicts).toEqual(["gated:serves"]);
         // A colocated bind alone (no storage bind after it) reaches the
         // served copy: unbound, the query is live; bound again, served.
         const entries = fixture.pkg.getBuildManifestEntries() ?? {};
         fixture.pkg.bindColocatedServeManifest({});
         const unbound = await fixture.run("m.malloy", QUERY, {
            REGIONS: CALLERS[0],
         });
         if ("error" in unbound) throw new Error(unbound.error);
         expect(unbound.answer.servedBy).toBe("live");
         // Eligibility unknown (a plan that could not be re-derived) admits
         // nothing, even through a bind that records the manifest unfiltered
         // (a reload): the served copy's bind applies it itself.
         const materialization = (
            fixture.pkg as unknown as {
               materialization: {
                  colocatedSourceEligibility: unknown;
                  recordManifestBinding(e: Record<string, unknown>): void;
               };
            }
         ).materialization;
         const known = materialization.colocatedSourceEligibility;
         materialization.colocatedSourceEligibility = undefined;
         materialization.recordManifestBinding(entries);
         const model = fixture.pkg.getModel("m.malloy") as unknown as {
            servedCopy: { hasBindings(): boolean };
         };
         expect(model.servedCopy.hasBindings()).toBe(false);
         materialization.colocatedSourceEligibility = known;
         fixture.pkg.bindColocatedServeManifest(entries);
         expect(model.servedCopy.hasBindings()).toBe(true);
         for (const [i, REGIONS] of CALLERS.entries()) {
            const outcome = await fixture.run("m.malloy", QUERY, { REGIONS });
            if ("error" in outcome) throw new Error(outcome.error);
            expect(outcome.answer.via).toBe("served_copy");
            expect(rowDifference(live[i], outcome.answer.rows)).toBeUndefined();
         }
      } finally {
         await fixture.dispose();
         restore();
      }
   });

   it("with gated serving off, binds no gated colocated table, and answers live", async () => {
      const { fixture, live, restore } = await loaded("false");
      try {
         // Refused at the bind (the colocated tier's eligibility), so its
         // table is never read: a table that cannot be served is not served.
         expect(fixture.verdicts).toEqual(["gated:live:refused"]);
         const model = fixture.pkg.getModel("m.malloy") as unknown as {
            servedCopy: { hasBindings(): boolean };
         };
         expect(model.servedCopy.hasBindings()).toBe(false);
         for (const [i, REGIONS] of CALLERS.entries()) {
            const outcome = await fixture.run("m.malloy", QUERY, { REGIONS });
            if ("error" in outcome) throw new Error(outcome.error);
            expect(outcome.answer.servedBy).toBe("live");
            expect(rowDifference(live[i], outcome.answer.rows)).toBeUndefined();
         }
      } finally {
         await fixture.dispose();
         restore();
      }
   });

   it("binds a v1 colocated #@ persist on the served copy, which answers it (the v1 cutover, G1)", async () => {
      const restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "off", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      await freshLoadPool();
      const loader = await loadFixture({
         ...DATA_FILES,
         "raw.malloy": RAW_MODEL,
         "m.malloy": `##! experimental { persistence }
import "raw.malloy"

#@ persist name="v1_tbl"
source: v1 is orders_raw -> {
  group_by: region
  aggregate: total is amount.sum()
}
`,
      });

      const query = "run: v1 -> { aggregate: t is total.sum() }";
      const live = await loader.run("m.malloy", query);
      const fixture = await loader.bind();
      try {
         if ("error" in live) throw new Error(live.error);
         const served = await fixture.run("m.malloy", query);
         if ("error" in served) throw new Error(served.error);
         expect(served.answer.servedBy).toBe("materialized");
         expect(served.answer.via).toBe("served_copy");
         expect(
            rowDifference(live.answer.rows, served.answer.rows),
         ).toBeUndefined();
         // Bound on the served copy, not left to Malloy's build manifest.
         const model = fixture.pkg.getModel("m.malloy") as unknown as {
            servedCopy: { hasBindings(): boolean };
         };
         expect(model.servedCopy.hasBindings()).toBe(true);
      } finally {
         await fixture.dispose();
         restore();
      }
   });
});

describe("a storage= table while the storage tier does not serve", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: {
            PERSIST_STORAGE_MODE: "write-only",
            PUBLISHER_MATERIALIZE: "true",
         },
      }).restore;
      fixture = await buildScenario({
         files: {
            "m.malloy": `source: ev is duckdb.table('flights')

#(materialize) storage="lake"
source: stored is ev extend { measure: n is count() }

#(materialize) name="coloc_tbl"
source: coloc is ev extend { measure: n is count() }
`,
         },
         seedSql: seed(""),
         materialize: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("is not read by the served copy, which reads the colocated table and warns of nothing", async () => {
      expect(await fixture.servedCopyProblems()).toEqual([]);
      const stored = await fixture.run({
         name: "stored",
         malloy: "run: stored -> { aggregate: n }",
      });
      expect(stored.servedVia).toBeUndefined();
      expect(rowDifference(stored.live, stored.served)).toBeUndefined();
      const coloc = await fixture.run({
         name: "coloc",
         malloy: "run: coloc -> { aggregate: n }",
      });
      expect(coloc.servedVia).toBe("served_copy");
      expect(rowDifference(coloc.live, coloc.served)).toBeUndefined();
   });
});
