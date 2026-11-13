// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for an inlined join_one answered from its stored columns: the served
 * copy replaces `inline=["carriers"]`'s join with a record of the stored
 * `carriers__<field>` columns under the join's name, so `carriers.<field>`
 * reads the table with no join. `carriers` is not materialized, so on the
 * storage tier the join would cross connections (question 11): before this,
 * every query through it was answered live. What the record cannot answer (a
 * measure, view or join of the joined source; an aggregate at the join's
 * grain) is declined and answered live. Every answer equals live.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import type { ModelDef } from "@malloydata/malloy";
import { classificationFindingsBySource } from "../../../src/service/classification_findings";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   PackageMaterialization,
   type PackageMaterializationOwner,
} from "../../../src/service/materialization/package_materialization";
import {
   buildScenario,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
} from "./harness";

/** The package warnings the served copy raises for the scenario's model. */
function servedCopyWarningsOf(fixture: R4Fixture) {
   return new PackageMaterialization({
      models: () => new Map([["m.malloy", fixture.model]]),
   } as unknown as PackageMaterializationOwner).servedCopyWarnings();
}

const seedSql = [
   `CREATE TABLE alliances AS SELECT * FROM (VALUES
      ('one', 'oneworld'), ('star', 'Star Alliance')
    ) AS t(id, alliance_name)`,
   `CREATE TABLE carriers AS SELECT * FROM (VALUES
      ('AA', 'American', 'east', 10.0, DATE '2001-03-15', 'one'),
      ('UA', 'United', 'west', 20.0, DATE '1999-07-02', 'star'),
      ('DL', 'Delta', 'east', 30.0, DATE '2004-11-30', NULL)
    ) AS t(code, nickname, region, fee, since, alliance_id)`,
   // 'XX' matches no carrier, and flight 7 has none: their joined fields
   // are null, as live's LEFT JOIN reads them.
   `CREATE TABLE flights AS SELECT * FROM (VALUES
      (1, 'AA', 100.0), (2, 'AA', 250.0), (3, 'UA', 300.0),
      (4, 'DL', 50.0), (5, 'XX', 75.0), (6, 'UA', 120.0), (7, NULL, 10.0)
    ) AS t(id, carrier, distance)`,
];

// `carriers` is not materialized: it stays on the origin connection while
// `flights_wide` is stored in the destination.
const model = `source: alliances is duckdb.table('alliances') extend {
  primary_key: id
}

source: carriers is duckdb.table('carriers') extend {
  primary_key: code
  join_one: alliance is alliances with alliance_id
  dimension:
    loud_name is upper(nickname)
    since_month is since.month
  measure:
    carrier_count is count()
    total_fee is fee.sum()
  view: by_region is { group_by: region; aggregate: carrier_count }
}

#(materialize) storage="lake" inline=["carriers"]
source: flights_wide is duckdb.table('flights') extend {
  primary_key: id
  join_one: carriers with carrier
  dimension: carrier_region is carriers.region
  measure:
    flight_count is count()
    total_distance is distance.sum()
    carriers_flown is count(carriers.code)
    fee_per_flight is sum(carriers.fee)
    fee_of_carriers is carriers.fee.sum()
  view: by_carrier is {
    group_by: carriers.nickname
    aggregate: flight_count, total_distance
  }
}
`;

describe("R4: an inlined join_one served from its stored columns", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: { "m.malloy": model },
         seedSql,
         materialize: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   const served = [
      [
         "a joined dimension",
         "run: flights_wide -> { group_by: carriers.nickname; aggregate: flight_count, total_distance }",
      ],
      [
         "a dimension through the join, stored as the source's own",
         "run: flights_wide -> { group_by: carrier_region; aggregate: flight_count }",
      ],
      [
         "a joined source's computed dimension",
         "run: flights_wide -> { group_by: carriers.loud_name; aggregate: total_distance }",
      ],
      [
         "a joined source's timeframe",
         "run: flights_wide -> { group_by: carriers.since_month; aggregate: flight_count }",
      ],
      [
         "a filter on a joined field",
         "run: flights_wide -> { where: carriers.region = 'east'; aggregate: flight_count, total_distance }",
      ],
      [
         "unmatched rows, whose joined fields are null",
         "run: flights_wide -> { where: carriers.code = null; group_by: id, carriers.nickname, carriers.fee }",
      ],
      [
         "a row-grain aggregate of a joined field",
         "run: flights_wide -> { group_by: carrier_region; aggregate: fee_per_flight, carriers_flown }",
      ],
      [
         "a distinct count of a joined key",
         "run: flights_wide -> { aggregate: c is count(carriers.code) }",
      ],
      ["a view through the join", "run: flights_wide -> by_carrier"],
      [
         "a filtered row-grain aggregate of a joined field",
         "run: flights_wide -> { aggregate: f is sum(carriers.fee) { where: carriers.region = 'east' } }",
      ],
      [
         "a source-grain aggregate of a joined field",
         "run: flights_wide -> { aggregate: w is source.sum(carriers.fee) }",
      ],
      [
         "a max through the join, which no grain changes",
         "run: flights_wide -> { aggregate: m is carriers.fee.max() }",
      ],
      [
         "a window over joined groups",
         "run: flights_wide -> { group_by: carriers.nickname; aggregate: flight_count; calculate: r is rank() }",
      ],
      [
         "a later stage over joined fields",
         "run: flights_wide -> { group_by: carriers.region, carriers.fee } -> { aggregate: s is fee.sum() }",
      ],
      [
         "a nest through the join",
         "run: flights_wide -> { group_by: carriers.region; nest: by_carrier }",
      ],
   ] as const;
   for (const [name, malloy] of served) {
      it(`serves ${name} from the table`, async () => {
         const run = await fixture.run({ name, malloy });
         expect(run.servedVia).toBe("served_copy");
         expect(rowDifference(run.live, run.served)).toBeUndefined();
      });
   }

   it("leaves no join to cross connections, so warns of none", async () => {
      const problems = await fixture.servedCopyProblems();
      expect(problems).toEqual([]);
   });

   it("reports the table as answering in the path report", async () => {
      fixture.setBindings(fixture.bindings);
      const { path } = await fixture.runAsIs(
         "run: flights_wide -> { group_by: carriers.nickname; aggregate: flight_count }",
      );
      expect(path?.servedBy).toMatchObject({
         kind: "materialized",
         via: "served_copy",
      });
   });

   // Each equals live because the served copy declines it and live answers.
   const declined = [
      [
         "a measure of the joined source",
         "run: flights_wide -> { group_by: carrier_region; aggregate: carriers.carrier_count, carriers.total_fee }",
         "MISSING_FIELD",
      ],
      [
         "an aggregate at the join's grain",
         "run: flights_wide -> { group_by: carrier_region; aggregate: f is carriers.fee.sum() }",
         "COUNT_SEMANTICS",
      ],
      [
         "a count at the join's grain",
         "run: flights_wide -> { aggregate: c is carriers.count() }",
         "COUNT_SEMANTICS",
      ],
      [
         "an average at the join's grain",
         "run: flights_wide -> { aggregate: a is carriers.fee.avg() }",
         "COUNT_SEMANTICS",
      ],
      [
         "the source's own measure at the join's grain",
         "run: flights_wide -> { group_by: carrier_region; aggregate: fee_of_carriers }",
         "COUNT_SEMANTICS",
      ],
      [
         "a join-grain aggregate in a nest",
         "run: flights_wide -> { group_by: id; nest: n is { aggregate: c is carriers.count() } }",
         "COUNT_SEMANTICS",
      ],
      [
         "a join-grain aggregate in having:",
         "run: flights_wide -> { group_by: carrier_region; aggregate: flight_count; having: carriers.fee.sum() > 15 }",
         "COUNT_SEMANTICS",
      ],
      [
         "a filtered join-grain aggregate",
         "run: flights_wide -> { aggregate: f is carriers.fee.sum() { where: carriers.region = 'east' } }",
         "COUNT_SEMANTICS",
      ],
      [
         "a measure that reads a join-grain measure",
         "run: flights_wide -> { aggregate: r is fee_of_carriers / flight_count }",
         "COUNT_SEMANTICS",
      ],
      [
         "a join-grain aggregate a query's extend: declares",
         "run: flights_wide -> { extend: { measure: x is carriers.fee.sum() } aggregate: x }",
         "COUNT_SEMANTICS",
      ],
      [
         "a join-grain aggregate under all() and exclude()",
         "run: flights_wide -> { group_by: carriers.region, carrier_region; aggregate: t is all(carriers.fee.sum()), e is exclude(carriers.fee.sum(), carrier_region) }",
         "COUNT_SEMANTICS",
      ],
      [
         "a query in another time zone, over joined fields the build computed",
         "run: flights_wide -> { timezone: 'America/New_York'; group_by: carriers.since_month; aggregate: flight_count }",
         "TIME_ZONE",
      ],
      [
         "a join of the joined source",
         "run: flights_wide -> { group_by: carriers.alliance.alliance_name; aggregate: flight_count }",
         "MISSING_FIELD",
      ],
   ] as const;
   for (const [name, malloy, reason] of declined) {
      it(`answers ${name} live (${reason})`, async () => {
         const run = await fixture.run({ name, malloy });
         expect(run.servedVia).toBeUndefined();
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         fixture.setBindings(fixture.bindings);
         const { path } = await fixture.runAsIs(malloy);
         expect(path?.considered?.map((c) => c.reason)).toContain(reason);
      });
   }
});

// The source's own text reads the joined source past what the table stores
// (a join of the joined source), so the record cannot stand in for the join:
// the served copy keeps the join as written, which here crosses connections,
// so the queries through it are answered live and the rest read the table.
const keptModel = model.replace(
   "    fee_of_carriers is carriers.fee.sum()",
   "    fee_of_carriers is carriers.fee.sum()\n" +
      "    alliances_flown is count(carriers.alliance.alliance_name)",
);

describe("R4: an inlined join the record cannot stand in for is kept as written", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: { "m.malloy": keptModel },
         seedSql,
         materialize: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("answers a query through the join live, and the rest from the table", async () => {
      const through = await fixture.run({
         name: "through",
         malloy:
            "run: flights_wide -> { group_by: carriers.nickname; aggregate: flight_count }",
      });
      expect(through.servedVia).toBeUndefined();
      expect(rowDifference(through.live, through.served)).toBeUndefined();
      const not = await fixture.run({
         name: "not through",
         malloy:
            "run: flights_wide -> { group_by: carrier_region; aggregate: flight_count }",
      });
      expect(not.servedVia).toBe("served_copy");
      expect(rowDifference(not.live, not.served)).toBeUndefined();
      const problems = await fixture.servedCopyProblems();
      expect(problems.map((p) => [p.reason, p.join])).toEqual([
         ["inline_join_kept", "carriers"],
         ["cross_connection_join", "carriers"],
      ]);
      // Why, in the compiler's words: the join's join is not in the record.
      expect(problems[0].sourceName).toBe("flights_wide");
      expect(problems[0].detail).toContain("alliance");
      // The join is already inlined, so the cross-connection warning does not
      // advise inlining it.
      const cross = servedCopyWarningsOf(fixture).find((w) =>
         w.message?.includes("cannot reach"),
      );
      expect(cross?.message).not.toContain("inline the join");
      expect(cross?.message).toContain("inlined join");
   });
});

// The same fallback when nothing crosses connections: every source is stored
// in the one destination, so the join kept as written is answered from the
// tables and nothing else would say the inlined join is not taking effect.
const sameConnectionModel = keptModel
   .replace(
      "source: alliances is",
      '#(materialize) storage="lake"\nsource: alliances is',
   )
   .replace(
      "source: carriers is",
      '#(materialize) storage="lake"\nsource: carriers is',
   );

describe("R4: an inlined join kept as written on one connection is warned", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: { "m.malloy": sameConnectionModel },
         seedSql,
         materialize: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("raises served-copy-not-served naming the join, and still serves through it", async () => {
      const problems = await fixture.servedCopyProblems();
      expect(problems.map((p) => [p.reason, p.sourceName, p.join])).toEqual([
         ["inline_join_kept", "flights_wide", "carriers"],
      ]);
      expect(problems[0].detail).toContain("alliance");
      // The join reads the joined source's own table, so the copy answers.
      const through = await fixture.run({
         name: "through",
         malloy:
            "run: flights_wide -> { group_by: carriers.nickname; aggregate: flight_count, alliances_flown }",
      });
      expect(through.servedVia).toBe("served_copy");
      expect(rowDifference(through.live, through.served)).toBeUndefined();
   });

   it("words the package warning with the join and the compiler's reason", async () => {
      await fixture.servedCopyProblems();
      const warnings = servedCopyWarningsOf(fixture);
      const kept = warnings.filter((w) => w.message?.includes("inline="));
      expect(kept).toHaveLength(1);
      expect(kept[0]).toMatchObject({
         code: "served-copy-not-served",
         subject: "flights_wide",
         severity: "warn",
      });
      expect(kept[0].message).toContain("`carriers`");
      expect(kept[0].message).toContain("'alliance' is not defined");
      expect(kept[0].message).not.toContain("file:");
   });
});

// A classified joined source. Each joined field's notes go on its stored
// column in the `type:`, and the record's field reads its class from the
// column, so the served copy's #(carries) and coverage findings are the
// compile of record's and the source stays served. A record's field cannot
// carry an annotation onto a result, though, so a query that returns an
// annotated joined field as a column is answered live; every other query
// keeps the table.
const classifiedModel = (
   sinceClass: string,
   carries: boolean,
) => `##! experimental.access_modifiers
source: carriers is duckdb.table('carriers') include {
  #(classification) "none"
  public: code, region, fee, alliance_id${sinceClass ? `, since` : ""}
  #(classification) "pii"
  # currency
  public: nickname
${sinceClass ? "" : "  public: since\n"}} extend {
  primary_key: code
  // No annotation: inferred pii, from nickname.
  dimension: loud_name is upper(nickname)
}

${carries ? '#(carries) ["none", "pii"]\n' : ""}#(materialize) storage="lake" inline=["carriers"]
source: flights_wide is duckdb.table('flights') include {
  #(classification) "none"
  public: id, carrier, distance
} extend {
  primary_key: id
  join_one: carriers with carrier
  // Stored as the source's own column; inferred pii.
  dimension: nick is carriers.nickname
  #(classification) "none"
  measure: flight_count is count()
  view: by_nickname is { group_by: carriers.nickname; aggregate: flight_count }
  view: by_loud_name is { group_by: carriers.loud_name; aggregate: flight_count }
}
`;

/** The classification findings of `source` in a compiled model. */
const findingsOf = (modelDef: unknown, source: string) =>
   classificationFindingsBySource("m.malloy", modelDef as ModelDef).get(source);

describe("R4: a classified inlined join keeps its classes, and declines per query", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: { "m.malloy": classifiedModel("none", true) },
         seedSql,
         materialize: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("holds the served copy to the same #(carries) and coverage findings as live", async () => {
      expect(await fixture.servedCopyProblems()).toEqual([]);
      const live = findingsOf(fixture.model.getModelDef(), "flights_wide");
      const served = findingsOf(
         await (
            fixture.servedCopy() as unknown as {
               compiledModelDef(): Promise<unknown>;
            }
         ).compiledModelDef(),
         "flights_wide",
      );
      expect(live).toEqual({ leaks: [], coverage: [] });
      expect(served).toEqual(live);
   });

   const served = [
      [
         "a filter on a classified joined field",
         "run: flights_wide -> { where: carriers.nickname = 'American'; group_by: id; aggregate: flight_count }",
      ],
      [
         "a joined field under another name",
         "run: flights_wide -> { group_by: n is carriers.nickname; aggregate: flight_count }",
      ],
      [
         "a stored dimension through the join",
         "run: flights_wide -> { group_by: nick; aggregate: flight_count }",
      ],
      [
         "an unannotated joined field (its class inferred)",
         "run: flights_wide -> { group_by: carriers.loud_name; aggregate: flight_count }",
      ],
      [
         "a view of an unannotated joined field",
         "run: flights_wide -> by_loud_name",
      ],
      [
         "a row-grain aggregate of a classified joined field",
         "run: flights_wide -> { aggregate: c is count(carriers.nickname) }",
      ],
   ] as const;
   for (const [name, malloy] of served) {
      it(`serves ${name} from the table`, async () => {
         const run = await fixture.run({ name, malloy });
         expect(run.servedVia).toBe("served_copy");
         expect(rowDifference(run.live, run.served)).toBeUndefined();
      });
   }

   const declined = [
      [
         "a classified joined field as a column",
         "run: flights_wide -> { group_by: carriers.nickname; aggregate: flight_count }",
      ],
      [
         "a joined field classified none as a column",
         "run: flights_wide -> { group_by: carriers.region; aggregate: flight_count }",
      ],
      ["a view that returns one", "run: flights_wide -> by_nickname"],
      [
         "a nest of a view that returns one",
         "run: flights_wide -> { group_by: nick; nest: by_nickname }",
      ],
      [
         "a select of one",
         "run: flights_wide -> { select: id, carriers.nickname }",
      ],
   ] as const;
   for (const [name, malloy] of declined) {
      it(`answers ${name} live, with its annotations (MISSING_FIELD)`, async () => {
         const run = await fixture.run({ name, malloy });
         expect(run.servedVia).toBeUndefined();
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         fixture.setBindings(fixture.bindings);
         const { path } = await fixture.runAsIs(malloy);
         expect(path?.considered?.map((c) => c.reason)).toContain(
            "MISSING_FIELD",
         );
      });
   }
});

describe("R4: a partly classified inlined join reports live's coverage, and stays served", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      // `carriers.since` declares no class; no #(carries), so it loads.
      fixture = await buildScenario({
         files: { "m.malloy": classifiedModel("", false) },
         seedSql,
         materialize: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("finds the same unclassified field on both compiles", async () => {
      expect(await fixture.servedCopyProblems()).toEqual([]);
      const live = findingsOf(fixture.model.getModelDef(), "flights_wide");
      const servedDef = await (
         fixture.servedCopy() as unknown as {
            compiledModelDef(): Promise<unknown>;
         }
      ).compiledModelDef();
      expect(live?.coverage.join(" ")).toContain("carriers.since");
      expect(findingsOf(servedDef, "flights_wide")).toEqual(live);
      const run = await fixture.run({
         name: "since",
         malloy:
            "run: flights_wide -> { group_by: carriers.since; aggregate: flight_count }",
      });
      expect(run.servedVia).toBe("served_copy");
      expect(rowDifference(run.live, run.served)).toBeUndefined();
   });
});

// A row-level gate that reads the inlined join: it is grafted on the served
// copy's own compile after routing, where `carriers.region` reads the stored
// column, so the gate keeps its rows with no join, as live's does with one.
const gatedModel = `##! experimental.givens
given: REGIONS :: string[]
${model
   .replace(
      '#(materialize) storage="lake" inline=["carriers"]',
      '#(access_filter) carriers.region in $REGIONS\n#(materialize) storage="lake" inline=["carriers"]',
   )
   .replace(
      "  dimension: carrier_region is carriers.region",
      "  dimension: carrier_region is carriers.region\n  dimension: carrier_nickname is carriers.nickname",
   )}`;

describe("R4: a row-level gate on an inlined join's field", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: {
            PERSIST_STORAGE_MODE: "on",
            PUBLISHER_MATERIALIZE: "true",
            PUBLISHER_MATERIALIZE_GATED_SOURCES: "true",
         },
      }).restore;
      fixture = await buildScenario({
         files: { "m.malloy": gatedModel },
         seedSql,
         materialize: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   for (const regions of [["east"], ["east", "west"], []]) {
      it(`answers [${regions.join(", ")}] from the table, with the gate's rows only`, async () => {
         const run = await fixture.run({
            name: regions.join(","),
            malloy:
               "run: flights_wide -> { group_by: carrier_region, carrier_nickname; aggregate: flight_count, total_distance }",
            givens: { REGIONS: regions },
         });
         expect(run.servedVia).toBe("served_copy");
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         for (const row of run.served) {
            expect(regions).toContain(row.carrier_region as string);
         }
      });
   }

   it("answers a join-grain aggregate under the gate live, equal to live", async () => {
      const run = await fixture.run({
         name: "join grain",
         malloy: "run: flights_wide -> { aggregate: f is carriers.fee.sum() }",
         givens: { REGIONS: ["east"] },
      });
      expect(run.servedVia).toBeUndefined();
      expect(rowDifference(run.live, run.served)).toBeUndefined();
   });
});
