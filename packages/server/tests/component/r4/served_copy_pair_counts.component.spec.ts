// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for counts through a pair table (known limitation 40, narrowed). Since
 * Malloy 0.0.438 (malloydata/malloy#3208) live takes a `count()`, `sum` or
 * `avg` at a joined source over that source's own rows: a parent with no
 * child counts as none and sums nothing. A pair table holds a null-extended
 * row for each such parent, so the served copy re-declares each measure that
 * counts through the inlined path, or sums or averages a value not null on
 * that row (a constant, `??`, a column of the parent), over the rows where
 * the level's stored key is not null, as live's guard reads them. That holds
 * only where the key is unique and never null on a row of the level: a level
 * above the leaf is checked on every build, and the leaf by a check recorded
 * beside the table. Where it failed, or was not recorded, the copy declines.
 *
 * Every answer equals live on its rows and its fields' types, on DuckDB (a
 * `storage=` table and a colocated one) and, when
 * `PUBLISHER_TEST_POSTGRES_URL` names one, on a real Postgres (the docker
 * `postgres:16` the gates run).
 */

import { PostgresConnection } from "@malloydata/db-postgres";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { splitManifestEntries } from "../../../src/service/materialization/run/manifest_loader";
import { deriveServeBindings } from "../../../src/service/materialization/serve/materialization_serve_transform";
import type {
   KeyUniqueness,
   ManifestEntry,
} from "../../../src/service/materialization/types";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
   type R4Scenario,
} from "./harness";

const POSTGRES_URL = process.env.PUBLISHER_TEST_POSTGRES_URL;
const PG_SCHEMA = "r4_pair_counts";

/** The measures each pair source declares, over its inlined join `j`. */
const leafMeasures = (j: string) => `  measure:
    flight_count is count()
    total_distance is distance.sum()
    seg_n is ${j}.count()
    seg_ones is ${j}.sum(1)
    seg_miles_or_zero is ${j}.sum(${j}.miles ?? 0)
    seg_avg_one is ${j}.avg(1)
    segs_per_flight is seg_n / flight_count
    long_legs is ${j}.count() { where: ${j}.miles > 200 }
    long_seg_n is seg_n { where: ${j}.miles > 200 }
    seg_ids is count(${j}.seg_id)
    seg_tags is count(${j}.tag ?? 'none')
    seg_distance is ${j}.sum(distance)
    seg_miles is ${j}.miles.sum()
    seg_longest is ${j}.miles.max()
    count_and_floor is ${j}.count() + max(${j}.miles ?? 0)`;

/**
 * The model on connection `w`, its tables in `schema`, each materialized
 * source placed by `at` (`storage="lake"`, or a colocated `name=`).
 */
function model(
   w: string,
   schema: string,
   at: (name: string) => string,
): string {
   const t = (name: string) => `${w}.table('${schema}${name}')`;
   return `source: segs is ${t("segs")} extend {
  primary_key: seg_id
  dimension: miles_or_one is miles ?? 1
}
source: segs_nullkey is ${t("segs_nullkey")} extend { primary_key: seg_id }
source: segs_dupkey is ${t("segs_dupkey")} extend { primary_key: seg_id }
source: stops is ${t("stops")} extend { primary_key: stop_id }
source: legs is segs extend {
  join_many: stops on stops.seg_id = seg_id
}

#(materialize) ${at("r4pc_flights")} inline=["segs"]
source: flights is ${t("flights")} extend {
  primary_key: id
  join_many: segs on segs.flight_id = id
${leafMeasures("segs")}
  measure: seg_miles_or_one is segs.miles_or_one.sum()
  view: by_carrier is { group_by: carrier; aggregate: seg_n, seg_ones, long_legs }
  view: by_carrier_inline is { group_by: carrier; aggregate: n is segs.count() }
}

#(materialize) ${at("r4pc_hop")} inline=["segs.stops"]
source: flights_hop is ${t("flights")} extend {
  primary_key: id
  join_many: segs is legs on segs.flight_id = id
  measure:
    flight_count is count()
    seg_n is segs.count()
    seg_ones is segs.sum(1)
    seg_distance is segs.sum(distance)
    seg_miles is segs.miles.sum()
    stop_n is segs.stops.count()
    stop_ones is segs.stops.sum(1)
    stop_mins is segs.stops.mins.sum()
    stops_per_seg is stop_n / seg_n
}

#(materialize) ${at("r4pc_nullkey")} inline=["segs_nullkey"]
source: flights_nullkey is ${t("flights")} extend {
  primary_key: id
  join_many: segs_nullkey on segs_nullkey.flight_id = id
${leafMeasures("segs_nullkey")}
}

#(materialize) ${at("r4pc_dupkey")} inline=["segs_dupkey"]
source: flights_dupkey is ${t("flights")} extend {
  primary_key: id
  join_many: segs_dupkey on segs_dupkey.flight_id = id
${leafMeasures("segs_dupkey")}
}
`;
}

/**
 * Portable seed SQL, DuckDB and Postgres alike, with tables in `schema`.
 * Flights 4 and 7 have no segment (8 has one, with no stop); segment 99
 * pairs with no flight. `segs_nullkey` adds a segment of flight 2 whose key
 * is NULL; `segs_dupkey` holds segment 3 twice, the same row, under flight 2.
 */
function seed(schema: string): string[] {
   const segs = `(1, 1, 40.0, 'a'), (2, 1, 260.0, NULL), (3, 2, 900.0, 'b'),
         (4, 3, 300.0, 'a'), (5, 3, 350.0, 'c'), (6, 5, 120.0, NULL),
         (8, 6, 610.0, 'b'), (9, 8, 75.0, 'a'), (99, 99, 10.0, 'z')`;
   return [
      `CREATE TABLE ${schema}flights AS SELECT * FROM (VALUES
         (1, 'AA', 100.0), (2, 'AA', 900.0), (3, 'UA', 650.0),
         (4, 'DL', 50.0), (5, 'UA', 300.0), (6, 'XX', 720.0),
         (7, 'XX', 80.0), (8, 'DL', 210.0)
       ) AS t(id, carrier, distance)`,
      `CREATE TABLE ${schema}segs AS SELECT * FROM (VALUES ${segs}
       ) AS t(seg_id, flight_id, miles, tag)`,
      `CREATE TABLE ${schema}segs_nullkey AS SELECT * FROM (VALUES ${segs},
         (NULL, 2, 500.0, 'n')
       ) AS t(seg_id, flight_id, miles, tag)`,
      `CREATE TABLE ${schema}segs_dupkey AS SELECT * FROM (VALUES ${segs},
         (3, 2, 900.0, 'b')
       ) AS t(seg_id, flight_id, miles, tag)`,
      `CREATE TABLE ${schema}stops AS SELECT * FROM (VALUES
         (1, 1, 30), (2, 1, 45), (3, 3, 20), (4, 4, 15), (5, 4, 25),
         (6, 4, 35), (7, 6, 10), (8, 8, 50)
       ) AS t(stop_id, seg_id, mins)`,
   ];
}

/** Each query over `flights` that the served copy now answers. */
const SERVED_LEAF: [string, string][] = [
   ["a leaf count", "run: flights -> { group_by: carrier; aggregate: seg_n }"],
   [
      "a leaf count by parent, childless parents included",
      "run: flights -> { group_by: id; aggregate: seg_n, seg_ones }",
   ],
   ["sum(1)", "run: flights -> { group_by: carrier; aggregate: seg_ones }"],
   [
      "a `??` sum",
      "run: flights -> { group_by: carrier; aggregate: seg_miles_or_zero }",
   ],
   [
      "an avg of a constant",
      "run: flights -> { group_by: carrier; aggregate: seg_avg_one }",
   ],
   [
      "a calculated measure over a count",
      "run: flights -> { group_by: carrier; aggregate: segs_per_flight }",
   ],
   [
      "a filtered count, as declared and over a measure",
      "run: flights -> { group_by: carrier; aggregate: long_legs, long_seg_n }",
   ],
   [
      "a count filtered in the query",
      "run: flights -> { group_by: carrier; aggregate: c is seg_n { where: segs.tag = 'a' } }",
   ],
   [
      "a distinct count, of a column and of a `??`",
      "run: flights -> { group_by: carrier; aggregate: seg_ids, seg_tags, seg_n }",
   ],
   [
      "a parent column summed through the join",
      "run: flights -> { group_by: carrier; aggregate: seg_distance, seg_n }",
   ],
   [
      "parent and leaf measures together",
      "run: flights -> { group_by: carrier; aggregate: flight_count, total_distance, seg_n, seg_miles, seg_longest }",
   ],
   [
      "a nest of counts under parent measures",
      "run: flights -> { group_by: carrier; aggregate: flight_count; nest: by_tag is { group_by: segs.tag; aggregate: seg_n, seg_ones, seg_avg_one } }",
   ],
   [
      "a nest of parent measures beside a count",
      "run: flights -> { group_by: carrier; aggregate: seg_n; nest: by_id is { group_by: id; aggregate: flight_count, seg_miles_or_zero } }",
   ],
   [
      "counts grouped by a leaf field",
      "run: flights -> { group_by: segs.tag; aggregate: seg_n, seg_ones, seg_distance }",
   ],
   [
      "counts under a leaf filter",
      "run: flights -> { where: segs.miles > 100; group_by: carrier; aggregate: seg_n, seg_avg_one }",
   ],
   ["a view that reads counts", "run: flights -> by_carrier"],
   [
      "a measure summing a stored computed field, not null on a childless parent's row",
      "run: flights -> { group_by: carrier; aggregate: seg_miles_or_one }",
   ],
];

/** Each query over the path `segs.stops` that the served copy now answers. */
const SERVED_HOP: [string, string][] = [
   [
      "a flagged middle-level count",
      "run: flights_hop -> { group_by: carrier; aggregate: seg_n, seg_ones }",
   ],
   [
      "a flagged middle-level count by parent, childless parents included",
      "run: flights_hop -> { group_by: id; aggregate: seg_n, seg_distance }",
   ],
   [
      "a leaf count under a middle level",
      "run: flights_hop -> { group_by: carrier; aggregate: stop_n, stop_ones, stop_mins }",
   ],
   [
      "a middle count beside the leaf (live aggregates the middle by its key)",
      "run: flights_hop -> { group_by: carrier; aggregate: seg_n, seg_ones, stop_n, stops_per_seg }",
   ],
   [
      "a middle count grouped by the middle level, beside the leaf",
      "run: flights_hop -> { group_by: segs.tag; aggregate: seg_n, stop_n }",
   ],
   [
      "a nest of the leaf beside a middle count",
      "run: flights_hop -> { group_by: carrier; aggregate: seg_n; nest: s is { group_by: segs.seg_id; aggregate: stop_n, stop_mins } }",
   ],
];

/** Each query the served copy still declines over `flights`, and why. */
const DECLINED_LEAF: [string, string][] = [
   [
      "a sum of a stored computed field, not null on a childless parent's row, written in the query",
      "run: flights -> { group_by: carrier; aggregate: m is segs.miles_or_one.sum() }",
   ],
   [
      "a count written in the query",
      "run: flights -> { group_by: carrier; aggregate: n is segs.count() }",
   ],
   [
      "a sum of a constant written in the query",
      "run: flights -> { group_by: carrier; aggregate: n is segs.sum(1) }",
   ],
   [
      "a view that counts in an aggregate of its own",
      "run: flights -> by_carrier_inline",
   ],
   [
      "a measure that counts beside a `max` of a `??` (live reads the childless row for the max)",
      "run: flights -> { group_by: carrier; aggregate: count_and_floor }",
   ],
];

/**
 * The fields' types as served and live are held equal: a number's subtype,
 * which no stored column carries (row 12), left out, and each nest's
 * per-compile `reference_id` blanked.
 */
function withoutNumberSubtype(fields: { name: string; type: unknown }[]) {
   return JSON.parse(
      JSON.stringify(fields, (key, value) =>
         key === "subtype"
            ? undefined
            : typeof value === "string"
              ? value.replace(/reference_id = "[^"]*"/g, 'reference_id = ""')
              : value,
      ),
   );
}

type Tier = {
   label: string;
   storage: boolean;
   scenario: () => Promise<R4Scenario & { cleanup?: () => Promise<void> }>;
};

const tiers: Tier[] = [
   {
      label: "DuckDB, storage",
      storage: true,
      scenario: async () => ({
         files: { "m.malloy": model("duckdb", "", () => 'storage="lake"') },
         seedSql: seed(""),
         materialize: true,
      }),
   },
   {
      label: "DuckDB, colocated",
      storage: false,
      scenario: async () => ({
         files: {
            "m.malloy": model("duckdb", "", (name) => `name="${name}"`),
         },
         seedSql: seed(""),
         materialize: true,
      }),
   },
   ...(POSTGRES_URL
      ? [
           {
              label: "Postgres, colocated",
              storage: false,
              scenario: async () => {
                 const pg = new PostgresConnection({
                    name: "pg",
                    connectionString: POSTGRES_URL,
                 });
                 await pg.runSQL(`DROP SCHEMA IF EXISTS ${PG_SCHEMA} CASCADE`);
                 await pg.runSQL(`CREATE SCHEMA ${PG_SCHEMA}`);
                 return {
                    files: {
                       "m.malloy": model(
                          "pg",
                          `${PG_SCHEMA}.`,
                          (name) => `name="${name}"`,
                       ),
                    },
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
   describe(`R4: counts through a pair table, served over the leaf's key, equal live (${tier.label})`, () => {
      let restore: () => void;
      let fixture: R4Fixture;
      let cleanup: (() => Promise<void>) | undefined;
      beforeAll(async () => {
         restore = installSettings({
            env: {
               PERSIST_STORAGE_MODE: tier.storage ? "on" : "off",
               PUBLISHER_MATERIALIZE: "true",
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

      /** Run `malloy` and hold served to live: rows and fields' types. */
      const equalToLive = async (name: string, malloy: string) => {
         const run = await fixture.run({ name, malloy });
         expect(run.live.length).toBeGreaterThan(0);
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         expect(withoutNumberSubtype(run.servedFields)).toEqual(
            withoutNumberSubtype(run.liveFields),
         );
         return run;
      };

      /** The reasons the path report gives for `malloy` as the router sees it. */
      const reasonsFor = async (malloy: string) => {
         fixture.setBindings(fixture.bindings);
         const { path } = await fixture.runAsIs(malloy);
         return (path?.considered ?? []).map((c) => c.reason);
      };

      it("compiles the served copy with no problem", async () => {
         expect(await fixture.servedCopyProblems()).toEqual([]);
      });

      it("counts a childless parent as none, as live does", async () => {
         const run = await equalToLive(
            "childless",
            "run: flights -> { where: id = 4 | 7 | 1; group_by: id; aggregate: seg_n, seg_ones, seg_avg_one, seg_miles_or_zero, seg_distance }",
         );
         expect(run.servedVia).toBe("served_copy");
         expect(
            run.live
               .map((r) => [Number(r.id), Number(r.seg_n), Number(r.seg_ones)])
               .sort((a, b) => a[0] - b[0]),
         ).toEqual([
            [1, 2, 2],
            [4, 0, 0],
            [7, 0, 0],
         ]);
      });

      for (const [name, malloy] of [...SERVED_LEAF, ...SERVED_HOP]) {
         it(`serves ${name}`, async () => {
            const run = await equalToLive(name, malloy);
            expect(run.servedVia).toBe("served_copy");
         });
      }

      for (const [name, malloy] of DECLINED_LEAF) {
         it(`answers live: ${name}`, async () => {
            const run = await equalToLive(name, malloy);
            expect(run.servedVia).toBeUndefined();
            expect(await reasonsFor(malloy)).toContain("COUNT_SEMANTICS");
         });
      }

      // The leaf's key check failed: a NULL key, or a key held twice (two
      // equal segments, which the pair table's distinct rows store as one).
      // Every count, sum or average at the leaf is declined, a stored
      // column's sum too: live skips the NULL key's segment, and counts the
      // two equal segments twice. A `max` and a distinct count read both
      // live as stored, so they are served.
      for (const source of ["flights_nullkey", "flights_dupkey"]) {
         for (const measures of [
            "seg_n",
            "seg_ones",
            "seg_miles_or_zero",
            "seg_avg_one",
            "segs_per_flight",
            "long_legs",
            "seg_distance",
            "seg_miles",
         ]) {
            it(`answers live where the leaf's key check failed: ${source} ${measures}`, async () => {
               const malloy = `run: ${source} -> { group_by: carrier; aggregate: ${measures} }`;
               const run = await equalToLive(`${source} ${measures}`, malloy);
               expect(run.servedVia).toBeUndefined();
               expect(await reasonsFor(malloy)).toContain("COUNT_SEMANTICS");
            });
         }
         it(`still serves a max and a distinct count where the leaf's key check failed: ${source}`, async () => {
            const run = await equalToLive(
               source,
               `run: ${source} -> { group_by: carrier; aggregate: seg_longest, seg_ids, seg_tags }`,
            );
            expect(run.servedVia).toBe("served_copy");
         });
      }
   });
}

// With no leaf key check on record (a table a host's manifest binds, or a
// build that could not check), the counts stay declined.
describe("R4: counts through a pair table with no leaf key check on record", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: { "m.malloy": model("duckdb", "", () => 'storage="lake"') },
         seedSql: seed(""),
         materialize: true,
         pairLeafKeyChecks: false,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   // A stored column's sum included: without the check a NULL key or two
   // equal children cannot be ruled out.
   for (const [name, malloy] of [
      ...SERVED_LEAF,
      [
         "a stored column's sum",
         "run: flights -> { group_by: carrier; aggregate: seg_miles }",
      ],
   ]) {
      it(`answers live, equal to live: ${name}`, async () => {
         const run = await fixture.run({ name, malloy });
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         expect(run.servedVia).toBeUndefined();
      });
   }

   // A middle level's key is checked on every build (a failure fails it), so
   // its counts are served without a leaf record; the leaf's are not.
   it("serves a middle-level count, and declines the leaf's", async () => {
      const middle = await fixture.run({
         name: "middle",
         malloy:
            "run: flights_hop -> { group_by: carrier; aggregate: seg_n, seg_ones }",
      });
      expect(rowDifference(middle.live, middle.served)).toBeUndefined();
      expect(middle.servedVia).toBe("served_copy");
      const leaf = await fixture.run({
         name: "leaf",
         malloy: "run: flights_hop -> { group_by: carrier; aggregate: stop_n }",
      });
      expect(rowDifference(leaf.live, leaf.served)).toBeUndefined();
      expect(leaf.servedVia).toBeUndefined();
   });
});

// A host's manifest binds the table through the path a host bind takes
// (`splitManifestEntries`, then `deriveServeBindings`), which never reads a
// leaf key record from the host: even one claiming the leaf's key unique and
// never null, over a table whose leaf key is NULL on a row or held twice.
// Every leaf count, sum and average answers live, a stored column's sum
// included, and equals live.
describe("R4: a pair table a host's manifest binds, its leaf key claimed checked", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: { "m.malloy": model("duckdb", "", () => 'storage="lake"') },
         seedSql: seed(""),
         materialize: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   /** The source's bindings as a host's manifest, claiming its leaf checked, binds them. */
   const hostBound = (sourceName: string) => {
      const own = fixture.bindings.find(
         (b) => b.sourceName === sourceName && b.destinationName,
      );
      if (!own?.primaryKeys)
         throw new Error(`no checked binding: ${sourceName}`);
      const claimed: KeyUniqueness = {
         parentKeys: Object.fromEntries(
            Object.keys(own.primaryKeys).map((id) => [id, true]),
         ),
      };
      const entry = {
         sourceEntityId: own.virtualHandle,
         sourceName,
         physicalTableName: fixture.physicalTables[sourceName],
         connectionName: "duckdb",
         storageDestinationName: own.destinationName,
         schema: own.schema,
         realization: "COPY",
         rowCount: null,
         keyUniqueness: claimed,
      } as ManifestEntry;
      const { storageEntries } = splitManifestEntries(
         { [own.virtualHandle]: entry },
         "a host's manifest",
      );
      const host = deriveServeBindings(
         storageEntries,
         {},
         undefined,
         () => own.sourceIDs ?? [],
      );
      return {
         own,
         bindings: [...fixture.bindings.filter((b) => b !== own), ...host],
         host,
      };
   };

   for (const [source, measures] of [
      ["flights", "seg_n, seg_ones, seg_miles, seg_distance"],
      ["flights_nullkey", "seg_n, seg_miles, seg_distance"],
      ["flights_dupkey", "seg_n, seg_miles, seg_distance"],
   ] as const) {
      it(`answers ${source}'s leaf counts and sums live, equal to live`, async () => {
         const { own, bindings, host } = hostBound(source);
         expect(host).toHaveLength(1);
         expect(host[0].tablePath).toBe(own.tablePath);
         expect(host[0].primaryKeys).toBeUndefined();
         const malloy = `run: ${source} -> { group_by: carrier; aggregate: ${measures} }`;
         fixture.setBindings([]);
         const live = await fixture.runAsIs(malloy);
         fixture.setBindings(bindings);
         const served = await fixture.runAsIs(malloy);
         fixture.setBindings(fixture.bindings);
         expect(live.rows.length).toBeGreaterThan(0);
         expect(served.servedFrom).toBeNull();
         expect(rowDifference(live.rows, served.rows)).toBeUndefined();
         // The same table still serves what needs no leaf record.
         fixture.setBindings(bindings);
         const max = await fixture.runAsIs(
            `run: ${source} -> { group_by: carrier; aggregate: seg_longest, flight_count }`,
         );
         fixture.setBindings(fixture.bindings);
         expect(max.servedFrom).not.toBeNull();
      });
   }
});
