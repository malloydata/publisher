// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * One served-copy golden per reason the served copy leaves a source, or its
 * whole copy, unserved (`ServedCopyProblem["reason"]`, fourteen): the package
 * goldens record bind warnings, not each declined source's reason. Each case
 * is the smallest scenario that makes a real served copy report its reason,
 * end to end through the R4 harness; where no author input reaches a reason
 * today (a declaration with no location, a range past its file, a given term
 * the re-root lost), the case edits the compiled author model or a binding in
 * place, as a stand-in for the defect the reason guards against. The golden
 * holds each problem's source, reason and join, and, for a case whose model
 * was not edited, where one query on the declined source is answered from and
 * whether it equals live. These double as the bind label set the bind
 * counter counts. Regenerate with UPDATE_GOLDENS=1 (see golden_files.ts).
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
   type R4Scenario,
} from "../r4/harness";
import {
   expectGolden,
   removeGoldens,
   unclaimedGoldens,
   UPDATING,
} from "./golden_files";
import { Normalizer } from "./normalize";

const AREA = "served_copy_reasons";

type Reason =
   | "compile_failed"
   | "cross_connection_unbuilt"
   | "column_types"
   | "no_declaration"
   | "range_not_found"
   | "definition_not_parsed"
   | "pair_join_not_found"
   | "pair_not_compiled"
   | "fields_not_carried"
   | "read_terms_not_kept"
   | "cross_connection_join"
   | "inline_join_kept"
   | "reroot_not_compiled"
   | "classification_not_kept";

interface ReasonCase {
   /** The reasons the case must report, each at least once. */
   reasons: Reason[];
   name: string;
   scenario: R4Scenario;
   /** Edit a built binding or the compiled author model in place. */
   tamper?(fixture: R4Fixture): void;
   /** A query to route after the problems are read; not with a model edit. */
   query?: string;
}

/** The compiled author model's source definitions, by name (edited in place). */
function authorDefs(
   fixture: R4Fixture,
): Record<string, Record<string, unknown>> {
   return (
      fixture.model as unknown as {
         modelDef: { contents: Record<string, Record<string, unknown>> };
      }
   ).modelDef.contents;
}

/** Retype one stored column of one source's binding, in place. */
function retype(
   fixture: R4Fixture,
   source: string,
   column: string,
   type: string,
): void {
   const bound = fixture.bindings.filter((b) => b.sourceName === source);
   expect(bound.map((b) => b.sourceName)).toEqual([source]);
   (bound[0] as { schema: (typeof bound)[0]["schema"] }).schema =
      bound[0].schema.map((c) => (c.name === column ? { ...c, type } : c));
}

const EVENTS_SEED = [
   `CREATE TABLE events AS
      SELECT ts, CAST(row_number() OVER (ORDER BY ts) AS INTEGER) AS amount,
             CASE WHEN hour(ts) % 2 = 0 THEN 'acme' ELSE 'globex' END AS org
      FROM range(TIMESTAMP '2026-11-01 00:00:00', TIMESTAMP '2026-11-03 00:00:00', INTERVAL 1 HOUR) t(ts)`,
];

const FLIGHTS_SEED = [
   `CREATE TABLE carriers AS SELECT * FROM (VALUES
      ('AA', 'American', 'east'), ('UA', 'United', 'west'), ('DL', 'Delta', 'east')
    ) AS t(code, nickname, region)`,
   `CREATE TABLE flights AS SELECT * FROM (VALUES
      (1, 'AA', 100.0), (2, 'AA', 250.0), (3, 'UA', 300.0),
      (4, 'DL', 50.0), (5, 'XX', 75.0), (6, 'UA', 120.0)
    ) AS t(id, carrier, distance)`,
   `CREATE TABLE segments AS SELECT * FROM (VALUES
      (1, 1, 40.0), (2, 1, 60.0), (3, 2, 250.0), (4, 3, 100.0), (5, 3, 200.0)
    ) AS t(seg_id, flight_id, miles)`,
   `CREATE TABLE notes AS SELECT * FROM (VALUES
      (1, 'AA', 'ops'), (2, 'UA', 'fees')
    ) AS t(note_id, carrier, topic)`,
];

const ORDERS_SEED = [
   `CREATE TABLE orders AS SELECT * FROM (VALUES
      (1, 10, 'east', 10.5, TIMESTAMP '2026-01-01 10:00:00'),
      (2, 11, 'west', 20.0, TIMESTAMP '2026-02-02 11:00:00'),
      (3, 10, 'east', 5.5, TIMESTAMP '2026-03-03 12:00:00')
    ) AS t(id, org_id, region, amount, ordered_at)`,
];

const GIVEN_SEED = [
   `CREATE TABLE ev AS SELECT * FROM (VALUES
      (1, 'acme', 10.0, DATE '2026-01-01'),
      (2, 'acme', 20.0, DATE '2026-01-02'),
      (3, 'globex', 5.0, DATE '2026-01-01')
    ) AS t(id, org, amt, d)`,
];

/** A `#(materialize)` source over `events`, and a refinement of it. */
const MATERIALIZED_EVENTS = `source: events is duckdb.table('events')

#(materialize) storage="lake"
source: m is events extend {
  measure: n is count()
}
`;

/** A v1 `#@ persist` source over `events`. */
const PERSISTED_EVENTS = `##! experimental { persistence }
source: events is duckdb.table('events')

#@ persist name="p" storage="lake"
source: p is events -> { select: org, amount } extend {
  measure: total is amount.sum()
}
`;

const CASES: ReasonCase[] = [
   {
      reasons: ["compile_failed"],
      name: "a refinement the stored table no longer compiles",
      scenario: {
         files: {
            "m.malloy": `${MATERIALIZED_EVENTS}
source: x is m extend {
  measure: total is amount.sum()
}
`,
         },
         seedSql: EVENTS_SEED,
         materialize: true,
      },
      // `m`'s own text does not read `amount`, so its re-root compiles; the
      // refinement, kept as written, does not.
      tamper: (f) => retype(f, "m", "amount", "VARCHAR"),
      query: "run: m -> { group_by: org; aggregate: n }",
   },
   {
      reasons: ["cross_connection_unbuilt"],
      name: "a query statement through a join to an unbuilt source",
      scenario: {
         files: {
            "m.malloy": `source: segments is duckdb.table('segments') extend {
  primary_key: seg_id
  measure: seg_miles is miles.sum()
}

#(materialize) storage="lake"
source: flights is duckdb.table('flights') extend {
  primary_key: id
  join_many: segments on segments.flight_id = id
  measure: flight_count is count()
}

run: flights -> { aggregate: segments.seg_miles }
`,
         },
         seedSql: FLIGHTS_SEED,
         materialize: true,
      },
      query: "run: flights -> { aggregate: flight_count }",
   },
   {
      reasons: ["column_types"],
      name: "a column no Malloy type declares",
      scenario: {
         files: {
            "m.malloy": `##! experimental { persistence }
source: base is duckdb.table('ex')

#@ persist name="ex_stored" storage="lake"
source: s is base -> { select: x }
`,
         },
         seedSql: [
            "CREATE TABLE ex AS SELECT * FROM (VALUES (1, MAP {'k': 1}), (2, MAP {'k': 2})) AS t(id, x)",
         ],
      },
      query: "run: s -> { aggregate: n is count() }",
   },
   {
      reasons: ["no_declaration"],
      name: "a declaration with no location",
      scenario: {
         files: { "m.malloy": PERSISTED_EVENTS },
         seedSql: EVENTS_SEED,
      },
      tamper: (f) => {
         delete authorDefs(f)["p"].location;
      },
   },
   {
      reasons: ["range_not_found"],
      name: "a declaration past the end of its file",
      scenario: {
         files: { "m.malloy": PERSISTED_EVENTS },
         seedSql: EVENTS_SEED,
      },
      tamper: (f) => {
         const def = authorDefs(f)["p"];
         def.location = {
            ...(def.location as object),
            range: {
               start: { line: 9999, character: 0 },
               end: { line: 9999, character: 4 },
            },
         };
      },
   },
   {
      reasons: ["definition_not_parsed"],
      name: "a rename over a base that includes",
      scenario: {
         files: {
            "m.malloy": `##! experimental { access_modifiers }
import "base.malloy"

#(materialize) storage="lake"
source: renamed_hidden is cbase extend {
  rename: amt is amount
  measure: total is amt.sum()
}

#(materialize) storage="lake"
source: beside is duckdb.table('orders') extend {
  measure: total is amount.sum()
}
`,
            "base.malloy": `##! experimental { access_modifiers }
source: cbase is duckdb.table('orders') include { internal: org_id; public: * }
`,
         },
         seedSql: ORDERS_SEED,
         materialize: true,
      },
      query: "run: renamed_hidden -> { group_by: region; aggregate: total }",
   },
   {
      reasons: ["pair_join_not_found", "pair_not_compiled"],
      name: "pair tables the served copy cannot read flat",
      scenario: {
         files: {
            "m.malloy": `source: segments is duckdb.table('segments') extend {
  primary_key: seg_id
  measure: seg_miles is miles.sum()
}

#(materialize) storage="lake" inline=["segments"]
source: reads_child_measure is duckdb.table('flights') extend {
  primary_key: id
  join_many: segments on segments.flight_id = id
  measure:
    flight_count is count()
    via_child is segments.seg_miles
}

#(materialize) storage="lake" inline=["segments"]
source: shared_statement is duckdb.table('flights') extend {
  primary_key: id
  join_many:
    segments on segments.flight_id = id
    notes is duckdb.table('notes') on notes.carrier = carrier
  measure: seg_n is segments.count()
}
`,
         },
         seedSql: FLIGHTS_SEED,
         materialize: true,
      },
      query: "run: shared_statement -> { group_by: carrier; aggregate: seg_n }",
   },
   {
      reasons: ["fields_not_carried"],
      name: "fields inherited from a base the re-root replaces",
      scenario: {
         files: {
            "m.malloy": `import "kept.malloy"

#(materialize) storage="lake"
source: kept_west is kept extend {
  where: org = 'globex'
}

#(materialize) storage="lake"
source: beside is duckdb.table('ev') extend {
  measure: total is amt.sum()
}
`,
            "kept.malloy": `source: kept is duckdb.table('ev') extend {
  measure: grand is amt.sum()
}
`,
         },
         seedSql: GIVEN_SEED,
         materialize: true,
      },
      query: "run: kept_west -> { aggregate: grand }",
   },
   {
      reasons: ["read_terms_not_kept"],
      name: "a given term the re-root lost",
      scenario: {
         files: {
            "m.malloy": `##! experimental.givens
given: ORG :: string is 'acme'

source: ev is duckdb.table('ev')

#(materialize) storage="lake"
source: trailing_where is ev extend {
  where: org = $ORG
  measure: total is amt.sum()
}
`,
         },
         seedSql: GIVEN_SEED,
         materialize: true,
      },
      // As if the author's source applied a given-reading term the served
      // source does not: the re-root lost it.
      tamper: (f) => {
         const def = authorDefs(f)["trailing_where"];
         def.filterList = [
            ...((def.filterList as unknown[]) ?? []),
            {
               code: "org != $ORG",
               expressionType: "scalar",
               e: {
                  node: "=",
                  kids: {
                     left: { node: "field", path: ["org"] },
                     right: { node: "given", refName: "ORG" },
                  },
               },
            },
         ];
      },
   },
   {
      reasons: ["cross_connection_join"],
      name: "a join to a source stored nowhere",
      scenario: {
         files: {
            "m.malloy": `source: segments is duckdb.table('segments') extend {
  primary_key: seg_id
  measure: seg_miles is miles.sum()
}

#(materialize) storage="lake"
source: flights is duckdb.table('flights') extend {
  primary_key: id
  join_many: segments on segments.flight_id = id
  measure: flight_count is count()
}
`,
         },
         seedSql: FLIGHTS_SEED,
         materialize: true,
      },
      query: "run: flights -> { group_by: carrier; aggregate: flight_count }",
   },
   {
      reasons: ["inline_join_kept"],
      name: "an inlined join read past one hop",
      scenario: {
         files: {
            "m.malloy": `source: alliances is duckdb.table('alliances') extend {
  primary_key: id
}

#(materialize) storage="lake"
source: carriers is duckdb.table('carriers') extend {
  primary_key: code
  join_one: alliance is alliances with alliance_id
}

#(materialize) storage="lake" inline=["carriers"]
source: flights_wide is duckdb.table('flights') extend {
  primary_key: id
  join_one: carriers with carrier
  dimension: carrier_region is carriers.region
  measure:
    flight_count is count()
    alliances_flown is count(carriers.alliance.alliance_name)
}
`,
         },
         seedSql: [
            `CREATE TABLE alliances AS SELECT * FROM (VALUES
               ('one', 'oneworld'), ('star', 'Star Alliance')
             ) AS t(id, alliance_name)`,
            `CREATE TABLE carriers AS SELECT * FROM (VALUES
               ('AA', 'American', 'east', 'one'),
               ('UA', 'United', 'west', 'star'),
               ('DL', 'Delta', 'east', NULL)
             ) AS t(code, nickname, region, alliance_id)`,
            `CREATE TABLE flights AS SELECT * FROM (VALUES
               (1, 'AA', 100.0), (2, 'AA', 250.0), (3, 'UA', 300.0), (4, 'DL', 50.0)
             ) AS t(id, carrier, distance)`,
         ],
         materialize: true,
      },
      query: "run: flights_wide -> { group_by: carrier_region; aggregate: flight_count }",
   },
   {
      reasons: ["reroot_not_compiled"],
      name: "a re-root whose own text no longer compiles",
      scenario: {
         files: {
            "m.malloy": `source: events is duckdb.table('events')

#(materialize) storage="lake"
source: broken is events extend {
  measure: total is amount.sum()
}
`,
         },
         seedSql: EVENTS_SEED,
         materialize: true,
      },
      tamper: (f) => retype(f, "broken", "amount", "VARCHAR"),
      query: "run: broken -> { group_by: org; aggregate: total }",
   },
];

function goldenPath(c: ReasonCase): string {
   return `${AREA}/${c.name.replace(/[^a-z0-9]+/gi, "-").toLowerCase()}`;
}

const claimed = new Set<string>();
let restore: () => void;

beforeAll(() => {
   restore = installSettings({
      env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
   }).restore;
});

afterAll(async () => {
   restore();
   await disposeScenarios();
});

describe("served-copy reason goldens", () => {
   it("covers every reason", () => {
      // Every reason but `classification_not_kept`, which no model reaches
      // end to end now that the build view is gone (the copy never
      // classifies past the compile of record); it is pinned on real
      // compiles in served_copy_classification_ceiling.spec.ts.
      const covered = new Set(CASES.flatMap((c) => c.reasons));
      expect(covered.size).toBe(13);
      expect(covered.has("classification_not_kept")).toBe(false);
   });

   for (const c of CASES) {
      describe(`${c.reasons.join(", ")}: ${c.name}`, () => {
         let fixture: R4Fixture;
         beforeAll(async () => {
            fixture = await buildScenario(c.scenario);
            c.tamper?.(fixture);
         }, 180_000);

         it("matches its golden", async () => {
            const normalizer = new Normalizer().addRoot(fixture.packagePath);
            const problems = await fixture.servedCopyProblems();
            for (const reason of c.reasons) {
               expect(problems.map((p) => p.reason)).toContain(reason);
            }
            let query: unknown = null;
            if (c.query) {
               const run = await fixture.run({ name: "q", malloy: c.query });
               query = {
                  servedFrom: run.servedFrom,
                  equalToLive:
                     rowDifference(run.live, run.served) === undefined,
               };
            }
            const base = goldenPath(c);
            claimed.add(`${base}.serve.json`);
            expectGolden(
               `${base}.serve.json`,
               normalizer.json({
                  problems: problems.map((p) => ({
                     sourceName: p.sourceName ?? null,
                     reason: p.reason,
                     ...(p.join ? { join: p.join } : {}),
                  })),
                  query,
               }),
            );
         }, 180_000);
      });
   }

   it("leaves no golden of a case that is gone", () => {
      const stale = unclaimedGoldens(AREA, claimed);
      if (UPDATING) removeGoldens(stale);
      else expect(stale).toEqual([]);
   });
});
