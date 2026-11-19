// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// A v1 `#@ persist` whose BUILD reads the current time stores the rows as of
// the build and serves them until the next rebuild, so it is refused
// (`current_time`) and answered live. Each case pairs the verdict with the
// build SQL that justifies it: a refused source's build reads the clock, and
// an admitted one's does not (its `now` is in the extend block, applied at
// every read).
import type { FixedConnectionMap, PersistSource } from "@malloydata/malloy";
import { beforeAll, describe, expect, it } from "bun:test";
import { MaterializationEligibilityError } from "../../../errors";
import {
   compilePersistSources,
   duckdbTestConnections,
} from "../../../test_helpers/incremental_test_harness";
import { collectSourceEligibility } from "./eligibility_collect";
import { assertTierEligible } from "./materialization_eligibility";
import {
   buildReadsCurrentTime,
   currentTimeReadsOf,
   storedAsOfBuild,
} from "./persist_dynamic_terms";

let connections: FixedConnectionMap;

beforeAll(() => {
   ({ connections } = duckdbTestConnections());
});

const HEAD = `##! experimental.persistence
source: ev is duckdb.sql("""SELECT * FROM (VALUES (1, 10, TIMESTAMP '2001-01-01'), (2, 20, TIMESTAMP '2100-01-01')) AS t(id, amt, ts)""")
source: base_where is ev extend { where: ts < now }
source: base_dim is ev extend {
  dimension: past is ts < now
  view: past_view is { select: id, past }
  view: plain_view is { select: id, amt }
  view: recent is { where: ts < now; select: id, amt }
}
source: base_join is ev extend {
  join_one: j is ev extend { where: ts < now } on j.id = id
}
source: query_base is ev -> { where: ts < now; select: * }
source: query_base2 is query_base -> { select: * }
source: query_base2x is query_base2 extend { dimension: k is 1 }
source: query_dim is base_dim -> { select: id, amt, ts }
source: base_join_query is ev extend {
  join_one: jq is query_base on jq.id = id
}
source: base_join_query_deep is ev extend {
  join_one: jq is query_base2 on jq.id = id
}
source: base_join_query_plain is ev extend {
  join_one: jq is query_dim on jq.id = id
}
`;

async function persistSource(body: string): Promise<PersistSource> {
   const { sources } = await compilePersistSources(
      connections,
      `${HEAD}\n${body}`,
   );
   const source = sources["p"];
   expect(source).toBeDefined();
   return source;
}

/** Whether the build SQL reads the clock (DuckDB renders `now` as LOCALTIMESTAMP). */
async function buildReadsClock(source: PersistSource): Promise<boolean> {
   return /LOCALTIMESTAMP|CURRENT_TIMESTAMP|now\(\)/i.test(
      await source.getSQL(),
   );
}

const REFUSED: [string, string][] = [
   [
      "the persisted query's where:",
      "source: p is ev -> { where: ts < now; select: * }",
   ],
   [
      "a field the persisted query computes",
      "source: p is ev -> { select: id, past is ts < now }",
   ],
   [
      "a later stage of the persisted query",
      "source: p is ev -> { select: * } -> { where: ts < now; select: * }",
   ],
   [
      "the where: of the source the query reads",
      "source: p is base_where -> { select: * }",
   ],
   [
      "a dimension of the source the query reads",
      "source: p is base_dim -> { select: id, past }",
   ],
   [
      "a join of the source the query reads",
      "source: p is base_join -> { select: id, jamt is j.amt }",
   ],
   ["a view the persist runs (its where:)", "source: p is base_dim -> recent"],
   [
      "a view the persist runs (a dimension it selects)",
      "source: p is base_dim -> past_view",
   ],
   [
      "a query-shaped base it extends",
      "source: p is query_base extend { measure: t is amt.sum() }",
   ],
   [
      "a query-shaped base the persisted query reads",
      "source: p is query_base -> { select: id, amt }",
   ],
   [
      "a query-shaped base two levels up",
      "source: p is query_base2 -> { select: id, amt }",
   ],
   [
      "a query-shaped base two levels up, extended",
      "source: p is query_base2x extend { measure: t is amt.sum() }",
   ],
   [
      "an upstream #@ persist the chain inlines",
      `#@ persist name="up"
source: up is ev -> { where: ts < now; select: * }
#@ persist name="p"
source: p is up -> { select: id, amt }`,
   ],
   [
      "a join to a query-shaped source whose query reads it",
      "source: p is base_join_query -> { select: id, jamt is jq.amt }",
   ],
   [
      "a join to a query-shaped source two levels up",
      "source: p is base_join_query_deep -> { select: id, jamt is jq.amt }",
   ],
   [
      "a select: * over a source with a current-time dimension",
      "source: p is base_dim -> { select: * }",
   ],
];

const ADMITTED: [string, string][] = [
   [
      "a later stage whose field shares a name with a current-time dimension",
      "source: p is base_dim -> { group_by: id, past is ts > @2000-01-01 } -> { select: id, past }",
   ],
   [
      "a later stage reading an aggregate named like a current-time dimension",
      "source: p is base_dim -> { group_by: id; aggregate: past is count() } -> { select: id, past }",
   ],
   [
      "a join to a query-shaped source whose query does not read it",
      "source: p is base_join_query_plain -> { select: id, jamt is jq.amt }",
   ],
   [
      "a query-shaped base whose unread base dimension does",
      "source: p is query_dim -> { select: id, amt }",
   ],
   [
      "its own extend-block where:",
      "source: p is ev extend { where: ts < now }",
   ],
   [
      "its own dimension",
      "source: p is ev extend { dimension: past is ts < now }",
   ],
   [
      "its own measure",
      "source: p is ev extend { measure: np is count() { where: ts < now } }",
   ],
   [
      "a query-shaped source's own trailing extend",
      "source: p is ev -> { select: * } extend { where: ts < now }",
   ],
   [
      "a base dimension the query does not read",
      "source: p is base_dim -> { select: id, amt }",
   ],
   ["a base view that reads no clock", "source: p is base_dim -> plain_view"],
   ["no clock at all", "source: p is ev -> { select: * }"],
];

describe("buildReadsCurrentTime", () => {
   for (const [where, body] of REFUSED) {
      it(`refuses now in ${where}, which the build reads`, async () => {
         const source = await persistSource(
            body.startsWith("#@") ? body : `#@ persist name="p"\n${body}`,
         );
         expect(await buildReadsClock(source)).toBe(true);
         expect(buildReadsCurrentTime(source)).toBe(true);
      });
   }
   for (const [where, body] of ADMITTED) {
      it(`admits now in ${where}, which the build leaves to the read`, async () => {
         const source = await persistSource(
            body.startsWith("#@") ? body : `#@ persist name="p"\n${body}`,
         );
         expect(await buildReadsClock(source)).toBe(false);
         expect(buildReadsCurrentTime(source)).toBe(false);
      });
   }
});

/** The refusal assertTierEligible throws for `source`, or undefined. */
function refusalOf(
   source: PersistSource,
   storage: boolean,
   plan?: Parameters<typeof assertTierEligible>[2],
): MaterializationEligibilityError | undefined {
   try {
      // A closure computed and found empty, unless the case names one.
      assertTierEligible(source, storage, { accessReads: [], ...plan });
      return undefined;
   } catch (err) {
      expect(err).toBeInstanceOf(MaterializationEligibilityError);
      return err as MaterializationEligibilityError;
   }
}

describe("assertTierEligible: a source whose build reads now", () => {
   for (const [annotation, storage] of [
      [`#@ persist name="p"`, false],
      [`#@ persist name="p" storage=lake`, true],
   ] as const) {
      const tier = storage ? "storage=" : "database";

      it(`builds an ungated one on ${tier}, stored as of its build`, async () => {
         const source = await persistSource(
            `${annotation}\nsource: p is ev -> { where: ts < now; select: * }`,
         );
         expect(refusalOf(source, storage)).toBeUndefined();
         expect(storedAsOfBuild(source, "persist")).toBe(true);
         expect(currentTimeReadsOf(source)).toEqual(["`where: ts < now`"]);
      });

      it(`refuses an incremental one on ${tier} as current_time`, async () => {
         const source = await persistSource(
            `${annotation} refresh="incremental" watermark="ts"\nsource: p is ev -> { where: ts < now; select: * }`,
         );
         const refusal = refusalOf(source, storage);
         expect(refusal?.reason).toBe("current_time");
         expect(refusal?.message).toContain("refreshes incrementally");
      });

      it(`refuses one scoped by a given (an access input) on ${tier} as current_time`, async () => {
         const { sources } = await compilePersistSources(
            connections,
            `##! experimental { persistence givens }
given: ORG :: number is 1
${HEAD.replace("##! experimental.persistence\n", "")}
${annotation}
source: p is ev -> { where: ts < now; select: * } extend { where: id = $ORG }
`,
         );
         const refusal = refusalOf(sources["p"], storage);
         expect(refusal?.reason).toBe("current_time");
         expect(refusal?.message).toContain("feed an access decision");
      });

      it(`admits now in its own extend block on ${tier}, not stored as of its build`, async () => {
         const source = await persistSource(
            `${annotation}\nsource: p is ev extend { where: ts < now }`,
         );
         expect(refusalOf(source, storage)).toBeUndefined();
         expect(storedAsOfBuild(source, "persist")).toBe(false);
      });
   }

   it("refuses a gated one as current_time", async () => {
      const { sources } = await compilePersistSources(
         connections,
         `##! experimental { persistence givens }
given: ORG :: number is 1
${HEAD.replace("##! experimental.persistence\n", "")}
#(access_filter) id = $ORG
#@ persist name="p"
source: p is ev -> { where: ts < now; select: * } extend {}
`,
      );
      const p = sources["p"];
      // Its own row-level gate, which a database table keeps at read: only
      // the clock refuses it.
      const gate = { classification: "row_level" as const, attributed: true };
      const refusal = refusalOf(p, false, {
         tables: { [p.sourceID]: { kind: "persist", source: p, gate } },
      });
      expect(refusal?.reason).toBe("current_time");
      expect(refusal?.message).toContain("gate");
      const ungated = await persistSource(
         `#@ persist name="p"\nsource: p is ev -> { where: ts < now; select: * } extend {}`,
      );
      expect(
         refusalOf(ungated, false, {
            tables: {
               [ungated.sourceID]: { kind: "persist", source: ungated },
            },
         }),
      ).toBeUndefined();
   });

   it("refuses one an access decision reads (accessReads), and one when the closure is unknown", async () => {
      const source = await persistSource(
         `#@ persist name="p"\nsource: p is ev -> { where: ts < now; select: * }`,
      );
      const read = refusalOf(source, false, { accessReads: [source.sourceID] });
      expect(read?.reason).toBe("current_time");
      expect(read?.message).toContain("feed an access decision");
      expect(refusalOf(source, false, { accessReads: ["*"] })?.reason).toBe(
         "current_time",
      );
      expect(refusalOf(source, false, { accessReads: undefined })?.reason).toBe(
         "current_time",
      );
      expect(
         refusalOf(source, false, { accessReads: ["other@x"] }),
      ).toBeUndefined();
   });

   it("leaves a rollup and a #(materialize) table to their own planners", async () => {
      const source = await persistSource(
         `#@ persist name="p"\nsource: p is ev -> { where: ts < now; select: * }`,
      );
      expect(storedAsOfBuild(source, "materialize")).toBe(false);
      expect(storedAsOfBuild(source, "rollup")).toBe(false);
      expect(storedAsOfBuild(source, "view")).toBe(true);
   });
});

describe("collectSourceEligibility: an ungated source over now is eligible", () => {
   it("lists it as eligible, so its table is bound and served", async () => {
      const { sources } = await compilePersistSources(
         connections,
         `${HEAD}
#@ persist name="p" storage=lake
source: p is ev -> { where: ts < now; select: * }
#@ persist name="ok" storage=lake
source: ok is ev -> { select: * } extend { where: ts < now }
`,
      );
      const byID = Object.fromEntries(
         Object.values(sources).map((s) => [s.sourceID, s]),
      );
      const eligibility = collectSourceEligibility({
         tables: Object.fromEntries(
            Object.entries(byID).map(([id, source]) => [
               id,
               { kind: "persist" as const, source },
            ]),
         ),
         connectionDigests: {},
         addresses: {},
         accessReads: [],
      });
      expect([...eligibility.eligible].sort()).toEqual(
         [sources["p"].sourceID, sources["ok"].sourceID].sort(),
      );
   });
});

describe("a given that defaults to now", () => {
   it("is refused as reads_given when the persisted query reads it", async () => {
      const { sources } = await compilePersistSources(
         connections,
         `##! experimental { persistence givens }
given: CUTOFF :: timestamp is now
${HEAD.replace("##! experimental.persistence\n", "")}
#@ persist name="p"
source: p is ev -> { where: ts < $CUTOFF; select: * }
`,
      );
      let caught: unknown;
      try {
         assertTierEligible(sources["p"], false, undefined);
      } catch (err) {
         caught = err;
      }
      expect(caught).toBeInstanceOf(MaterializationEligibilityError);
      expect((caught as MaterializationEligibilityError).reason).toBe(
         "reads_given",
      );
   });
});

// A filter string over a date or timestamp anchored to the clock compiles to
// no `now` node, but its SQL reads the clock all the same: the same frozen
// window, so the same refusal. An absolute one reads no clock and builds.
const FILTER_HEAD = `##! experimental { persistence givens }
source: ev is duckdb.sql("""SELECT * FROM (VALUES (1, 10, TIMESTAMP '2001-01-01', DATE '2001-01-01', 'today'), (2, 20, TIMESTAMP '2100-01-01', DATE '2100-01-01', 'x')) AS t(id, amt, ts, d, s)""")
source: base_where is ev extend { where: ts ~ f'last 7 days' }
source: base_dim is ev extend { dimension: recent is d ~ f'this month' }
source: query_base is ev -> { where: d ~ f'today'; select: * }
`;

async function filterPersistSource(body: string): Promise<PersistSource> {
   const { sources } = await compilePersistSources(
      connections,
      `${FILTER_HEAD}\n#@ persist name="p"\n${body}`,
   );
   const source = sources["p"];
   expect(source).toBeDefined();
   return source;
}

/** Whether the build SQL reads the clock, as DuckDB renders a relative filter. */
async function buildReadsClockOrToday(source: PersistSource): Promise<boolean> {
   return /LOCALTIMESTAMP|CURRENT_TIMESTAMP|CURRENT_DATE|now\(\)/i.test(
      await source.getSQL(),
   );
}

const FILTER_REFUSED: [string, string][] = [
   [
      "`last 7 days`",
      "source: p is ev -> { where: ts ~ f'last 7 days'; select: * }",
   ],
   ["`today`", "source: p is ev -> { where: ts ~ f'today'; select: * }"],
   [
      "`this month`",
      "source: p is ev -> { where: d ~ f'this month'; select: * }",
   ],
   [
      "`7 days ago to now`",
      "source: p is ev -> { where: ts ~ f'7 days ago to now'; select: * }",
   ],
   ["`yesterday`", "source: p is ev -> { where: d ~ f'yesterday'; select: * }"],
   [
      "`before 3 days ago`",
      "source: p is ev -> { where: ts ~ f'before 3 days ago'; select: * }",
   ],
   [
      "`after now`",
      "source: p is ev -> { where: ts ~ f'after now'; select: * }",
   ],
   ["`last week`", "source: p is ev -> { where: d ~ f'last week'; select: * }"],
   [
      "`next 2 days`",
      "source: p is ev -> { where: ts ~ f'next 2 days'; select: * }",
   ],
   ["`monday`", "source: p is ev -> { where: d ~ f'monday'; select: * }"],
   [
      "a relative half of an `or`",
      "source: p is ev -> { where: d ~ f'2024 or last 7 days'; select: * }",
   ],
   [
      "a relative match in an `or` of two",
      "source: p is ev -> { where: ts ~ f'2024' or ts ~ f'last 7 days'; select: * }",
   ],
   [
      "a negated relative match",
      "source: p is ev -> { where: ts !~ f'last 7 days'; select: * }",
   ],
   [
      "a field the persisted query computes",
      "source: p is ev -> { select: id, recent is ts ~ f'last 7 days' }",
   ],
   [
      "a filtered measure in the persisted query",
      "source: p is ev -> { group_by: id; aggregate: n is count() { where: ts ~ f'last 7 days' } }",
   ],
   [
      "the where: of the source the query reads",
      "source: p is base_where -> { select: * }",
   ],
   [
      "a dimension of the source the query reads",
      "source: p is base_dim -> { select: id, recent }",
   ],
   [
      "a query-shaped base it extends",
      "source: p is query_base extend { measure: t is amt.sum() }",
   ],
];

const FILTER_ADMITTED: [string, string][] = [
   [
      "the absolute year `2024`",
      "source: p is ev -> { where: ts ~ f'2024'; select: * }",
   ],
   [
      "the absolute range `2024-01-01 to 2024-02-01`",
      "source: p is ev -> { where: d ~ f'2024-01-01 to 2024-02-01'; select: * }",
   ],
   [
      "`after 2024-01-01`",
      "source: p is ev -> { where: ts ~ f'after 2024-01-01'; select: * }",
   ],
   [
      "`2024-01 for 3 months`",
      "source: p is ev -> { where: d ~ f'2024-01 for 3 months'; select: * }",
   ],
   ["`null`", "source: p is ev -> { where: ts ~ f'null'; select: * }"],
   [
      "a string filter that spells a relative word",
      "source: p is ev -> { where: s ~ f'today'; select: * }",
   ],
   [
      "its own extend-block where:",
      "source: p is ev -> { select: * } extend { where: ts ~ f'last 7 days' }",
   ],
   [
      "its own dimension",
      "source: p is ev -> { select: * } extend { dimension: recent is d ~ f'today' }",
   ],
   [
      "its own measure",
      "source: p is ev -> { select: * } extend { measure: n7 is count() { where: ts ~ f'last 7 days' } }",
   ],
   [
      "a base dimension the query does not read",
      "source: p is base_dim -> { select: id, amt }",
   ],
];

describe("buildReadsCurrentTime: filter strings", () => {
   for (const [what, body] of FILTER_REFUSED) {
      it(`refuses ${what}, which the build reads`, async () => {
         const source = await filterPersistSource(body);
         expect(await buildReadsClockOrToday(source)).toBe(true);
         expect(buildReadsCurrentTime(source)).toBe(true);
      });
   }
   for (const [what, body] of FILTER_ADMITTED) {
      it(`admits ${what}`, async () => {
         const source = await filterPersistSource(body);
         expect(await buildReadsClockOrToday(source)).toBe(false);
         expect(buildReadsCurrentTime(source)).toBe(false);
      });
   }

   it("builds a relative filter on both tiers, stored as of its build, and names it", async () => {
      for (const storage of [false, true]) {
         const { sources } = await compilePersistSources(
            connections,
            `${FILTER_HEAD}
#@ persist name="p"${storage ? " storage=lake" : ""}
source: p is ev -> { where: ts ~ f'last 7 days'; select: * }
`,
         );
         expect(refusalOf(sources["p"], storage)).toBeUndefined();
         expect(storedAsOfBuild(sources["p"], "persist")).toBe(true);
         expect(currentTimeReadsOf(sources["p"])).toEqual([
            "`where: ts ~ f'last 7 days'`",
         ]);
      }
   });

   it("refuses a relative filter on an incremental source as current_time", async () => {
      const { sources } = await compilePersistSources(
         connections,
         `${FILTER_HEAD}
#@ persist name="p" refresh="incremental" watermark="ts"
source: p is ev -> { where: ts ~ f'last 7 days'; select: * }
`,
      );
      expect(refusalOf(sources["p"], false)?.reason).toBe("current_time");
   });

   it("names a field the persisted query computes", async () => {
      const source = await filterPersistSource(
         "source: p is ev -> { select: id, recent is ts ~ f'last 7 days' }",
      );
      expect(currentTimeReadsOf(source)).toEqual(["the field `recent`"]);
   });

   it("refuses a filter given that defaults to a relative window as reads_given", async () => {
      const { sources } = await compilePersistSources(
         connections,
         `##! experimental { persistence givens }
given: WINDOW :: filter<timestamp> is f'last 7 days'
${FILTER_HEAD.replace("##! experimental { persistence givens }\n", "")}
#@ persist name="p"
source: p is ev -> { where: ts ~ $WINDOW; select: * }
`,
      );
      let caught: unknown;
      try {
         assertTierEligible(sources["p"], false, undefined);
      } catch (err) {
         caught = err;
      }
      expect(caught).toBeInstanceOf(MaterializationEligibilityError);
      expect((caught as MaterializationEligibilityError).reason).toBe(
         "reads_given",
      );
   });
});
