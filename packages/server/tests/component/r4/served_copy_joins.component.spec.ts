// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for joins through the served copy (materialization v2, phase 3 PR 4):
 * reference joins (the default), an inlined join_one, a reference join the
 * re-root moves across connections (question 11), and a join across origin
 * connections between sources stored in one destination (#6982). Every answer
 * equals live, or, where live cannot answer, a hand-computed truth.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
} from "./harness";

const seedSql = [
   `CREATE TABLE carriers AS SELECT * FROM (VALUES
      ('AA', 'American', 'east'),
      ('UA', 'United', 'west'),
      ('DL', 'Delta', 'east')
    ) AS t(code, nickname, region)`,
   `CREATE TABLE flights AS SELECT * FROM (VALUES
      (1, 'AA', 100.0), (2, 'AA', 250.0), (3, 'UA', 300.0),
      (4, 'DL', 50.0), (5, 'XX', 75.0), (6, 'UA', 120.0)
    ) AS t(id, carrier, distance)`,
   `CREATE TABLE segments AS SELECT * FROM (VALUES
      (1, 1, 40.0), (2, 1, 60.0), (3, 2, 250.0), (4, 3, 100.0), (5, 3, 200.0)
    ) AS t(seg_id, flight_id, miles)`,
];

const model = `#(materialize) storage="lake"
source: carriers is duckdb.table('carriers') extend {
  primary_key: code
  dimension: loud_name is upper(nickname)
}

source: segments is duckdb.table('segments') extend {
  primary_key: seg_id
  measure: seg_miles is miles.sum()
}

#(materialize) storage="lake"
source: flights is duckdb.table('flights') extend {
  primary_key: id
  join_one: carriers with carrier
  join_many: segments on segments.flight_id = id
  dimension:
    long_haul is distance > 200
    carrier_label is concat(carrier, ' / ', carriers.nickname)
  measure:
    flight_count is count()
    total_distance is distance.sum()
  view: by_carrier is {
    group_by: carriers.nickname
    aggregate: flight_count, total_distance
  }
}

#(materialize) storage="lake" inline=["carriers"]
source: flights_wide is duckdb.table('flights') extend {
  primary_key: id
  join_one: carriers with carrier
  dimension: carrier_region is carriers.region
  measure: flight_count is count()
}
`;

describe("R4: joins through the served copy answer equal to live", () => {
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
         "a reference join_one to a materialized source",
         "run: flights -> { group_by: carriers.nickname; aggregate: flight_count, total_distance }",
      ],
      ["a view through the reference join", "run: flights -> by_carrier"],
      [
         "a dimension through a reference join, computed at read",
         "run: flights -> { group_by: carrier_label; aggregate: flight_count }",
      ],
      [
         "a joined source's computed dimension",
         "run: flights -> { group_by: carriers.loud_name; aggregate: total_distance }",
      ],
      [
         "a dimension through an inlined join_one",
         "run: flights_wide -> { group_by: carrier_region; aggregate: flight_count }",
      ],
      [
         "the inlined join itself",
         "run: flights_wide -> { group_by: carriers.nickname; aggregate: flight_count }",
      ],
   ] as const;
   for (const [name, malloy] of served) {
      it(name, async () => {
         const run = await fixture.run({ name, malloy });
         expect(run.servedVia).toBe("served_copy");
         expect(rowDifference(run.live, run.served)).toBeUndefined();
      });
   }

   // `segments` is not materialized, so it stays on the origin connection
   // while `flights` moves to the destination (question 11).
   it("leaves out a reference join the re-root moves across connections, and answers its queries live", async () => {
      const malloy =
         "run: flights -> { group_by: long_haul; aggregate: flight_count, segments.seg_miles }";
      const run = await fixture.run({ name: "join_many", malloy });
      expect(run.servedVia).toBeUndefined();
      expect(rowDifference(run.live, run.served)).toBeUndefined();
   });

   it("still serves the table for a query that does not use the left-out join", async () => {
      const run = await fixture.run({
         name: "no join",
         malloy:
            "run: flights -> { group_by: long_haul; aggregate: flight_count }",
      });
      expect(run.servedVia).toBe("served_copy");
      expect(rowDifference(run.live, run.served)).toBeUndefined();
   });

   it("names the left-out join in a package warning", async () => {
      const problems = await fixture.servedCopyProblems();
      expect(problems.map((p) => [p.sourceName, p.reason, p.join])).toEqual([
         ["flights", "cross_connection_join", "segments"],
      ]);
   });
});

// The same through a v1 `#@ persist storage=` source, whose table the path
// report places on the query's chain: the skipped table carries
// `CROSS_CONNECTION`.
const persistModel = `##! experimental { persistence }
source: segments is duckdb.table('segments') extend {
  primary_key: seg_id
  measure: seg_miles is miles.sum()
}

#@ persist name="fl" storage="lake"
source: fl is duckdb.table('flights') -> { select: * } extend {
  primary_key: id
  join_many: segments on segments.flight_id = id
  measure: flight_count is count()
}
`;

describe("R4: a reference join across connections in the path report", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: { "m.malloy": persistModel },
         seedSql,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("skips the table with reason CROSS_CONNECTION for a query through the join", async () => {
      const malloy =
         "run: fl -> { group_by: carrier; aggregate: flight_count, segments.seg_miles }";
      const run = await fixture.run({ name: "through", malloy });
      expect(run.servedVia).toBeUndefined();
      expect(rowDifference(run.live, run.served)).toBeUndefined();
      fixture.setBindings(fixture.bindings);
      const { path } = await fixture.runAsIs(malloy);
      expect(path?.considered?.map((c) => c.reason)).toContain(
         "CROSS_CONNECTION",
      );
   });

   it("serves a query that does not use the join from the table", async () => {
      const run = await fixture.run({
         name: "not through",
         malloy: "run: fl -> { group_by: carrier; aggregate: flight_count }",
      });
      expect(run.servedVia).toBe("served_copy");
      expect(rowDifference(run.live, run.served)).toBeUndefined();
   });
});

// ms2data/service#6982: two sources from different origin connections, each
// materialized into one destination, joined. The author's file does not
// compile live; it loads from its build view (the join left out), builds both
// sides from it, and the served copy answers the join on the destination.
const crossOrigin = {
   crm: [
      `CREATE TABLE accounts AS SELECT * FROM (VALUES (1, 'acme'), (2, 'globex'), (3, 'initech')) AS t(id, account_name)`,
   ],
   product: [
      `CREATE TABLE usage AS SELECT * FROM (VALUES (1, 5), (1, 7), (2, 1), (9, 4)) AS t(account_id, events)`,
   ],
};
const crossModel = `#(materialize) storage="lake"
source: accounts is crm.table('accounts') extend { primary_key: id }

#(materialize) storage="lake"
source: usage is product.table('usage')

source: account_usage is accounts extend {
  join_many: u is usage on u.account_id = id
  measure: total_events is u.events.sum()
  view: by_account is { group_by: account_name; aggregate: total_events }
}
`;
const byAccount = [
   { account_name: "acme", total_events: 12 },
   { account_name: "globex", total_events: 1 },
   { account_name: "initech", total_events: 0 },
];

describe("R4: a join across origin connections, stored in one destination (#6982)", () => {
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

   it("answers the join from the destination once both sides are built", async () => {
      const fixture = await buildScenario({
         files: { "m.malloy": crossModel },
         seedSql: [],
         origins: crossOrigin,
         materialize: true,
      });
      fixture.setBindings(fixture.bindings);
      // Only a source that declares #(materialize) is built: the extension
      // inherits its base's declaration and reads both tables through it.
      expect(fixture.bindings.map((b) => b.sourceName).sort()).toEqual([
         "accounts",
         "usage",
      ]);
      const view = await fixture.runAsIs("run: account_usage -> by_account");
      expect(view.servedFrom).toBe("storage");
      const rows = view.rows.map((r) => ({
         account_name: r.account_name,
         total_events: r.total_events,
      }));
      expect(rowDifference(byAccount, rows)).toBeUndefined();
      // A query on one side alone also reads the destination.
      const one = await fixture.runAsIs(
         "run: accounts -> { aggregate: n is count() }",
      );
      expect(one.rows).toEqual([{ n: 3 }]);
   });

   it("with one side unbuilt, refuses only the queries that reach it", async () => {
      const fixture = await buildScenario({
         files: { "m.malloy": crossModel },
         seedSql: [],
         origins: crossOrigin,
         materialize: true,
         unbuilt: ["usage"],
      });
      fixture.setBindings(fixture.bindings);
      expect(fixture.bindings.map((b) => b.sourceName).includes("usage")).toBe(
         false,
      );
      // Through the join: neither the served copy (the join left out) nor
      // the build view live can answer.
      await expect(
         fixture.runAsIs("run: account_usage -> by_account"),
      ).rejects.toThrow();
      // Not through it: answered.
      const kept = await fixture.runAsIs(
         "run: account_usage -> { group_by: account_name; aggregate: n is count() }",
      );
      expect(kept.rows).toHaveLength(3);
      const problems = await fixture.servedCopyProblems();
      expect(problems.map((p) => [p.reason, p.join])).toEqual([
         ["cross_connection_join", "u"],
      ]);
   });
});

// The load-time classification checks run on the build view (#6982), which
// has no `u`; the served copy, once both sides are built, answers through it.
// So they run on the served copy's own compile too, and a source whose served
// surface leaks past its #(carries), or publishes an unclassified field, is
// never answered from the copy.
/** `eventsClass` empty: `events` declares no class. */
const classifiedModel = (eventsClass: string, carries = true) => {
   const events = eventsClass
      ? `#(classification) "${eventsClass}"\n  public: events`
      : "public: events";
   const ceiling = carries ? '#(carries) ["none"]\n' : "";
   return `##! experimental.access_modifiers
#(materialize) storage="lake"
source: accounts is crm.table('accounts') include {
  #(classification) "none"
  public: id, account_name
} extend { primary_key: id }

#(materialize) storage="lake"
source: usage is product.table('usage') include {
  #(classification) "none"
  public: account_id
  ${events}
}

${ceiling}source: account_usage is accounts extend {
  join_many: u is usage on u.account_id = id
  #(classification) "none"
  measure: n is count()
  view: by_key is { aggregate: n }
}
`;
};

describe("R4: the served copy is held to the load's classification checks (#6982)", () => {
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

   it("declines a source whose served copy reaches a field past its #(carries)", async () => {
      const fixture = await buildScenario({
         files: { "m.malloy": classifiedModel("phi") },
         seedSql: [],
         origins: crossOrigin,
         materialize: true,
      });
      fixture.setBindings(fixture.bindings);
      const problems = (await fixture.servedCopyProblems()).filter(
         (p) => p.reason === "classification_not_kept",
      );
      expect(problems.map((p) => p.sourceName)).toEqual(["account_usage"]);
      expect(problems[0].leaks?.join(" ")).toContain("u.events");
      // Through the join: the copy declines, and live cannot answer.
      await expect(
         fixture.runAsIs("run: account_usage -> { group_by: u.events }"),
      ).rejects.toThrow();
      // Its other queries are not answered from the copy either.
      const kept = await fixture.runAsIs("run: account_usage -> by_key");
      expect(kept.servedFrom).not.toBe("storage");
      // A source the copy leaks nothing on still reads its table.
      const one = await fixture.runAsIs(
         "run: accounts -> { aggregate: c is count() }",
      );
      expect(one.servedFrom).toBe("storage");
   });

   it("serves a source the checks pass on, through the join and not", async () => {
      // Under #(carries), a clean copy keeps its source served.
      const carried = await buildScenario({
         files: { "m.malloy": classifiedModel("none") },
         seedSql: [],
         origins: crossOrigin,
         materialize: true,
      });
      carried.setBindings(carried.bindings);
      expect(
         (await carried.servedCopyProblems()).filter(
            (p) => p.reason === "classification_not_kept",
         ),
      ).toEqual([]);
      expect(
         (await carried.runAsIs("run: account_usage -> by_key")).servedFrom,
      ).toBe("storage");
      // Under #(carries), a clean query through the join is served too: the
      // per-request ceiling is read on the served copy's compile, the one
      // that answers, not on the build view, which has no `u`.
      const throughCarried = await carried.runAsIs(
         "run: account_usage -> { group_by: u.events; aggregate: n }",
      );
      expect(throughCarried.servedFrom).toBe("storage");
      expect(
         rowDifference(
            [
               { events: 1, n: 1 },
               { events: 5, n: 1 },
               { events: 7, n: 1 },
               { events: null, n: 1 },
            ],
            throughCarried.rows,
         ),
      ).toBeUndefined();
      // Fully classified, the copy answers through the join.
      const covered = await buildScenario({
         files: { "m.malloy": classifiedModel("none", false) },
         seedSql: [],
         origins: crossOrigin,
         materialize: true,
      });
      covered.setBindings(covered.bindings);
      expect(
         (await covered.servedCopyProblems()).filter(
            (p) => p.reason === "classification_not_kept",
         ),
      ).toEqual([]);
      const through = await covered.runAsIs(
         "run: account_usage -> { group_by: u.events; aggregate: n }",
      );
      expect(through.servedFrom).toBe("storage");
   });

   it("refuses a query reading a class past the ceiling, served and live", async () => {
      // `secrets` is stored beside the others and carries a class
      // account_usage's ceiling does not; a caller's own join reaches it.
      const withSecrets = (carries: boolean) =>
         `${classifiedModel("none", carries)}
#(materialize) storage="lake"
source: secrets is crm.table('accounts') include {
  #(classification) "none"
  public: id
  #(classification) "phi"
  public: account_name
}
`;
      const join =
         "run: account_usage extend { join_one: s is secrets on s.id = id } -> ";
      const live = `${join}{ group_by: s.account_name; aggregate: n }`;
      const served = `${join}{ group_by: s.account_name, u.events; aggregate: n }`;
      // Without the ceiling, the query through `u` is answered from the copy:
      // so the refusal below is the ceiling's, read on the served copy.
      const open = await buildScenario({
         files: { "m.malloy": withSecrets(false) },
         seedSql: [],
         origins: crossOrigin,
         materialize: true,
      });
      open.setBindings(open.bindings);
      expect((await open.runAsIs(served)).servedFrom).toBe("storage");
      const capped = await buildScenario({
         files: { "m.malloy": withSecrets(true) },
         seedSql: [],
         origins: crossOrigin,
         materialize: true,
      });
      capped.setBindings(capped.bindings);
      // Through the join (the build view cannot compile it): read on the
      // served copy, and refused.
      await expect(capped.runAsIs(served)).rejects.toThrow(
         /carries \[none\]; this query reads s\.account_name/,
      );
      // Not through it: live compiles, and the ceiling refuses it there.
      await expect(capped.runAsIs(live)).rejects.toThrow(
         /carries \[none\]; this query reads s\.account_name/,
      );
      // A clean query through the join still answers from the copy.
      expect(
         (
            await capped.runAsIs(
               `${join}{ group_by: s.id, u.events; aggregate: n }`,
            )
         ).servedFrom,
      ).toBe("storage");
   });

   it("holds a query to a ceiling declared only where the build view leaves it out", async () => {
      // `by_usage` reads the join, so the build view leaves it out, and its
      // #(carries) with it: the only ceiling in the model is the served
      // copy's.
      const fixture = await buildScenario({
         files: {
            "m.malloy": `${classifiedModel("none", false)}
#(materialize) storage="lake"
source: secrets is crm.table('accounts') include {
  #(classification) "none"
  public: id
  #(classification) "phi"
  public: account_name
}

#(carries) ["none"]
source: by_usage is account_usage -> {
  group_by: acct is u.account_id
  aggregate: n
}
`,
         },
         seedSql: [],
         origins: crossOrigin,
         materialize: true,
      });
      fixture.setBindings(fixture.bindings);
      const join =
         "run: by_usage extend { join_one: s is secrets on s.id = acct } -> ";
      expect(
         (
            await fixture.runAsIs(
               `${join}{ group_by: acct; aggregate: c is count() }`,
            )
         ).servedFrom,
      ).toBe("storage");
      await expect(
         fixture.runAsIs(`${join}{ group_by: s.account_name }`),
      ).rejects.toThrow(/carries \[none\]; this query reads s\.account_name/);
   });

   it("declines a source whose served copy publishes an unclassified field", async () => {
      const fixture = await buildScenario({
         files: {
            "m.malloy": classifiedModel("", false),
         },
         seedSql: [],
         origins: crossOrigin,
         materialize: true,
      });
      fixture.setBindings(fixture.bindings);
      // The unclassified field is not served through the join.
      await expect(
         fixture.runAsIs(
            "run: account_usage -> { group_by: u.events; aggregate: n }",
         ),
      ).rejects.toThrow();
      const problems = (await fixture.servedCopyProblems()).filter(
         (p) => p.reason === "classification_not_kept",
      );
      expect(problems.map((p) => p.sourceName)).toEqual(["account_usage"]);
      expect(problems[0].coverage?.join(" ")).toContain("u.events");
      const kept = await fixture.runAsIs("run: account_usage -> by_key");
      expect(kept.servedFrom).not.toBe("storage");
   });
});

// A served-copy answer that fails at run time is retried live
// (freshnessFallback=live), and the retry is governed again with live's
// #(carries) ceiling. Live was already read before the served copy ran
// whenever it compiles, so the retry's read refuses nothing new; these pin
// that it refuses nothing clean either, and that a retry live cannot compile
// fails as a compile error rather than answering.
describe("R4: a served-copy answer retried live", () => {
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

   const liveFallback = (fixture: R4Fixture) =>
      fixture.setBindings(
         fixture.bindings.map((b) => ({
            ...b,
            freshnessFallback: "live" as const,
         })),
      );

   it("answers a clean query live, and refuses one past the ceiling", async () => {
      const fixture = await buildScenario({
         files: {
            "m.malloy": `##! experimental.access_modifiers
#(materialize) storage="lake"
source: accounts is crm.table('accounts') include {
  #(classification) "none"
  public: id
  #(classification) "phi"
  public: account_name
} extend { primary_key: id }

#(carries) ["none"]
source: capped is accounts extend {
  #(classification) "none"
  measure: n is count()
}
`,
         },
         seedSql: [],
         origins: crossOrigin,
         materialize: true,
      });
      liveFallback(fixture);
      const clean = "run: capped -> { group_by: id; aggregate: n }";
      expect((await fixture.runAsIs(clean)).servedFrom).toBe("storage");
      // The stored table goes, so the served copy fails at run time.
      await fixture.storedSQL((table) => `DROP TABLE ${table("accounts")}`);
      const retried = await fixture.runAsIs(clean);
      expect(retried.servedFrom).toBe("live_fallback");
      expect(
         rowDifference(
            [
               { id: 1, n: 1 },
               { id: 2, n: 1 },
               { id: 3, n: 1 },
            ],
            retried.rows,
         ),
      ).toBeUndefined();
      await expect(
         fixture.runAsIs("run: capped -> { group_by: account_name }"),
      ).rejects.toThrow(/carries \[none\]; this query reads account_name/);
   });

   it("refuses the retry of a query through a left-out join, which live cannot compile", async () => {
      const fixture = await buildScenario({
         files: { "m.malloy": classifiedModel("none") },
         seedSql: [],
         origins: crossOrigin,
         materialize: true,
      });
      liveFallback(fixture);
      const through =
         "run: account_usage -> { group_by: u.events; aggregate: n }";
      expect((await fixture.runAsIs(through)).servedFrom).toBe("storage");
      await fixture.storedSQL((table) => `DROP TABLE ${table("usage")}`);
      await expect(fixture.runAsIs(through)).rejects.toThrow(
         /'u' is not defined/,
      );
   });
});
