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
import { buildReadsCurrentTime } from "./persist_dynamic_terms";

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

describe("assertTierEligible: the current_time refusal on both tiers", () => {
   for (const [annotation, storage] of [
      [`#@ persist name="p"`, false],
      [`#@ persist name="p" storage=lake`, true],
   ] as const) {
      it(`refuses ${storage ? "storage=" : "colocated"} with current_time`, async () => {
         const source = await persistSource(
            `${annotation}\nsource: p is ev -> { where: ts < now; select: * }`,
         );
         let caught: unknown;
         try {
            assertTierEligible(source, storage, undefined);
         } catch (err) {
            caught = err;
         }
         expect(caught).toBeInstanceOf(MaterializationEligibilityError);
         expect((caught as MaterializationEligibilityError).reason).toBe(
            "current_time",
         );
         expect((caught as Error).message).toContain("answered live");
      });

      it(`admits ${storage ? "storage=" : "colocated"} with now in its own extend block`, async () => {
         const source = await persistSource(
            `${annotation}\nsource: p is ev extend { where: ts < now }`,
         );
         expect(() =>
            assertTierEligible(source, storage, undefined),
         ).not.toThrow();
      });
   }
});

describe("collectSourceEligibility: a stored table is not served for it either", () => {
   it("lists the source as refused, so a table built before is not bound", async () => {
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
         sources: byID,
         connectionDigests: {},
      });
      const refusedID = sources["p"].sourceID;
      expect(eligibility.refused[refusedID]).toContain(
         "reads the current time",
      );
      expect(eligibility.eligible).toEqual([sources["ok"].sourceID]);
   });
});

describe("a given that defaults to now", () => {
   it("is refused as given_in_persisted_query when the persisted query reads it", async () => {
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
         "given_in_persisted_query",
      );
   });
});
