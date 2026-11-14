// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What the retired storage serve shape guarded, answered by the served copy
 * (v1 cutover 6g). The serve shape rebuilt each stored source from extracted
 * pieces on the destination alone, so it needed:
 *
 *  - `withholdUnreproducibleCallerScopedJoins`: a source joining a
 *    given-scoped source was withheld unless that target was bound on the
 *    same destination and the join public, since a re-emitted join could
 *    not otherwise apply the caller's scope;
 *  - `restrictMalloyConfigToConnections`: the shape resolved only the
 *    destinations its bindings named.
 *
 * The served copy is the author's own text with each stored source re-rooted,
 * so a join (to a stored source or not) is kept as written and the caller's
 * givens apply at read; its connections are the package's plus an alias for
 * each destination it reads (served_copy_config.spec.ts). This spec runs the
 * retired guard's own fixture through the real package path, in every
 * binding the guard distinguished (all bound, the grant table unbound, the
 * grant table on another destination), with a source gated by
 * `#(access_filter)` and one locked by `#(authorize)`, and with
 * `PUBLISHER_MATERIALIZE_GATED_SOURCES` off and on: two callers or more per
 * gate kind, each served answer equal to live.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   freshLoadPool,
   loadFixture,
   type LoadedFixture,
   type Outcome,
} from "./conformance/package_harness";
import { rowDifference } from "./harness";

const MODEL = `##! experimental { persistence givens access_modifiers }
given:
  ORG_ID :: number
  USER_ID :: number
  ROLE :: string

source: raw is duckdb.sql("""SELECT * FROM (VALUES
  (1, 10, 5.0), (1, 11, 7.0), (1, 12, 9.0), (2, 20, 3.0), (2, 21, 4.0)
) AS t(org_id, opp_id, amount)""")
source: grants_raw is duckdb.sql("""SELECT * FROM (VALUES
  (1, 7, 10), (1, 7, 11), (1, 8, 12), (2, 9, 20)
) AS g(org_id, user_id, opp_id)""")

#@ persist name="grants" storage="lake"
source: grants is grants_raw -> { select: * } extend {
  where: org_id = $ORG_ID and user_id = $USER_ID
}

#@ persist name="opps" storage="lake"
source: opps is raw -> { select: * } extend {
  where: org_id = $ORG_ID
  join_one: g is grants on opp_id = g.opp_id
  dimension: visible is g.opp_id is not null
  measure: total is amount.sum()
}

#@ persist name="opp_notes" storage="lake"
source: opp_notes is raw -> { where: amount > 0; select: * } extend {
  where: org_id = $ORG_ID
  join_one: o is opps on opp_id = o.opp_id
  measure: total is amount.sum()
}

#@ persist name="hidden_grants_opps" storage="lake"
source: hidden_grants_opps is raw -> { where: amount > 1; select: * } extend {
  where: org_id = $ORG_ID
  join_one: g is grants on opp_id = g.opp_id
  dimension: visible is g.opp_id is not null
  measure: total is amount.sum()
} include { private: g; public: * }

// A gated v1 storage= source is refused at plan (served live); its colocated
// form is read with the gate grafted and proven (contracts at read).
#(access_filter) org_id = $ORG_ID
#@ persist name="refused_opps" storage="lake"
source: refused_opps is raw -> { where: amount > 2; select: * } extend {
  measure: total is amount.sum()
}

#@ persist name="grants_c"
source: grants_c is grants_raw -> { where: user_id > 0; select: * } extend {
  where: org_id = $ORG_ID and user_id = $USER_ID
}

#(access_filter) org_id = $ORG_ID
#@ persist name="filtered_opps"
source: filtered_opps is raw -> { where: amount > 2; select: * } extend {
  join_one: g is grants_c on opp_id = g.opp_id
  dimension: visible is g.opp_id is not null
  measure: total is amount.sum()
}

#(authorize) $ROLE = 'admin'
#@ persist name="locked_opps"
source: locked_opps is raw -> { where: amount > 3; select: * } extend {
  measure: total is amount.sum()
}

#@ persist name="plain_opps"
source: plain_opps is raw -> { where: amount > 0.5; select: * } extend {
  join_one: l is locked_opps on opp_id = l.opp_id
  measure: total is amount.sum()
}
`;

type Givens = Record<string, string | number>;
const CALLERS: [string, Givens][] = [
   ["org 1 user 7", { ORG_ID: 1, USER_ID: 7 }],
   ["org 1 user 8", { ORG_ID: 1, USER_ID: 8 }],
   ["org 2 user 9", { ORG_ID: 2, USER_ID: 9 }],
];
const QUERIES: [string, string, Givens[]][] = [
   [
      "a given-scoped join",
      "run: opps -> { group_by: visible; aggregate: total; order_by: visible }",
      CALLERS.map(([, g]) => g),
   ],
   [
      "a join to a source joining a given-scoped one",
      "run: opp_notes -> { group_by: o.visible; aggregate: total; order_by: visible }",
      CALLERS.map(([, g]) => g),
   ],
   [
      "a private given-scoped join read through a public dimension",
      "run: hidden_grants_opps -> { group_by: visible; aggregate: total; order_by: visible }",
      CALLERS.map(([, g]) => g),
   ],
   [
      "an #(access_filter) source with a given-scoped join",
      "run: filtered_opps -> { group_by: visible; aggregate: total; order_by: visible }",
      CALLERS.map(([, g]) => g),
   ],
   [
      "a gated storage= source (refused at plan)",
      "run: refused_opps -> { group_by: org_id; aggregate: total; order_by: org_id }",
      CALLERS.map(([, g]) => g),
   ],
   [
      "an #(authorize) source",
      "run: locked_opps -> { aggregate: total }",
      [{ ROLE: "admin" }, { ROLE: "viewer" }, {}],
   ],
   [
      "a join to an #(authorize) source",
      "run: plain_opps -> { group_by: l.opp_id; aggregate: total; order_by: opp_id }",
      [{ ROLE: "admin" }, { ROLE: "viewer" }],
   ],
];

type Binding = "all bound" | "grants unbound" | "grants on another destination";
const BINDINGS: Binding[] = [
   "all bound",
   "grants unbound",
   "grants on another destination",
];

function entriesFor(
   stored: Record<string, Record<string, unknown>>,
   binding: Binding,
): Record<string, Record<string, unknown>> {
   const out: Record<string, Record<string, unknown>> = {};
   for (const [id, entry] of Object.entries(stored)) {
      if (entry.sourceName === "grants") {
         if (binding === "grants unbound") continue;
         if (binding === "grants on another destination") {
            out[id] = { ...entry, storageDestinationName: "other_lake" };
            continue;
         }
      }
      out[id] = entry;
   }
   return out;
}

const sameOutcome = (live: Outcome, served: Outcome): string | undefined => {
   if ("error" in live) {
      return "error" in served ? undefined : "live refused, served answered";
   }
   if ("error" in served) return `served failed: ${served.error}`;
   return rowDifference(live.answer.rows, served.answer.rows);
};

for (const gated of [false, true]) {
   describe(`retired serve-shape guards, on the served copy (gated sources ${gated ? "on" : "off"})`, () => {
      let restore: () => void = () => {};
      let loader: Awaited<ReturnType<typeof loadFixture>>;
      let fixture: LoadedFixture;
      const live = new Map<string, Outcome>();
      const servedBy = new Map<string, string>();

      beforeAll(async () => {
         restore = installSettings({
            env: {
               PERSIST_STORAGE_MODE: "on",
               PUBLISHER_MATERIALIZE: "true",
               PUBLISHER_MATERIALIZE_GATED_SOURCES: gated ? "true" : "false",
            },
         }).restore;
         await freshLoadPool();
         loader = await loadFixture({ "m.malloy": MODEL });
         for (const [name, q, callers] of QUERIES) {
            for (const [i, g] of callers.entries()) {
               live.set(`${name}#${i}`, await loader.run("m.malloy", q, g));
            }
         }
         fixture = await loader.bind();
      }, 300_000);
      afterAll(async () => {
         restore();
         await loader?.dispose();
         await freshLoadPool();
      });

      it("builds every source the fixture declares", () => {
         expect(fixture.buildErrors).toEqual({});
         expect(
            Object.values(fixture.stored)
               .map((e) => e.sourceName)
               .sort(),
         ).toContain("grants");
      });

      for (const binding of BINDINGS) {
         for (const [name, q, callers] of QUERIES) {
            it(`${binding}: ${name}, each caller as live`, async () => {
               await fixture.rebind();
               fixture.pkg.bindStorageServeBindings(
                  entriesFor(fixture.stored, binding) as never,
               );
               await fixture.pkg.getModel("m.malloy")?.prepareServedCopy();
               for (const [i, g] of callers.entries()) {
                  const l = live.get(`${name}#${i}`)!;
                  const out = await fixture.run("m.malloy", q, g);
                  expect({
                     caller: g,
                     difference: sameOutcome(l, out),
                  }).toEqual({ caller: g, difference: undefined });
                  if (!("error" in out)) {
                     servedBy.set(
                        `${binding}|${name}#${i}`,
                        out.answer.servedBy,
                     );
                  }
               }
            });
         }
      }

      it("serves from the tables, not only live, where they are bound", () => {
         // The equalities above are not equal only because everything fell
         // back to live: each gate kind and the given-scoped joins are read
         // from tables when every target is bound.
         for (const key of [
            "a given-scoped join#0",
            "a given-scoped join#2",
            "a join to a source joining a given-scoped one#1",
            "a private given-scoped join read through a public dimension#0",
            "an #(access_filter) source with a given-scoped join#0",
            "an #(access_filter) source with a given-scoped join#2",
            "an #(authorize) source#0",
            "a join to an #(authorize) source#0",
         ]) {
            expect([key, servedBy.get(`all bound|${key}`)]).toEqual([
               key,
               "materialized",
            ]);
         }
         expect(
            servedBy.get(
               "all bound|a gated storage= source (refused at plan)#0",
            ),
         ).toBe("live");
      });

      it("the callers' live answers differ, so one caller's rows could not pass for another's", () => {
         const a = live.get("a given-scoped join#0");
         const b = live.get("a given-scoped join#1");
         const c = live.get("a given-scoped join#2");
         if (!a || !b || !c || "error" in a || "error" in b || "error" in c)
            throw new Error("live failed");
         expect(rowDifference(a.answer.rows, b.answer.rows)).toBeDefined();
         expect(rowDifference(a.answer.rows, c.answer.rows)).toBeDefined();
      });
   });
}
