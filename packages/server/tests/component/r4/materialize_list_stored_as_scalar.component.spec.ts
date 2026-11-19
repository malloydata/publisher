// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4: a `#(materialize)` table that stores a declared list column (a
 * `string[]`) as a scalar (here VARCHAR, its text) is never read with its
 * stored type. Declared from that scalar, the served copy answered
 * `select: id, v` with the text `"[x, y]"` where live answers the list
 * `["x", "y"]`. The column is left out instead, as any column no
 * declaration reads back as live answers it: a query reading it answers
 * live, equal to live, and a query that does not is served from the table.
 *
 * The table is made as such a build left it: built, then the column altered
 * to VARCHAR in the destination, with the run that bound it carrying the
 * altered schema. A list stored as a list still serves from the table, and
 * a gated source is held to the same answers under each caller's gate.
 *
 * The same holds for a list of records stored as text, and for a v1
 * `#@ persist storage=` table. A source whose own field reads the list
 * (`len!number(v)`, which answered the text's length, 6, for live's 2)
 * cannot be served without it, so every query on it answers live.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import {
   installSettings,
   useMaterializationOn,
} from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
} from "./harness";

const SEED = [
   `CREATE TABLE ex AS SELECT * FROM (VALUES
      (1, 'acme', 1.5, ['x', 'y'], [{'a': 'p', 'n': 1}]),
      (2, 'acme', 2.5, ['y'], [{'a': 'r', 'n': 3}, {'a': 's', 'n': 4}]),
      (3, 'globex', 3.5, ['x', 'z', 'w'], [{'a': 'p', 'n': 6}]),
      (4, 'globex', 4.5, []::VARCHAR[], [{'a': 'q', 'n': 2}])) AS t(id, org, amount, v, lr)`,
];

const MODEL = `#(materialize) storage="lake"
source: scalar_v is duckdb.table('ex') extend {
  primary_key: id
  measure: n is count()
}

#(materialize) storage="lake"
source: list_v is duckdb.table('ex') extend {
  primary_key: id
  measure: n is count()
}

#(materialize) storage="lake"
source: records_lr is duckdb.table('ex') extend {
  primary_key: id
  measure: n is count()
}

#(materialize) storage="lake"
source: computed_v is duckdb.table('ex') extend {
  primary_key: id
  measure: n is count()
  dimension: vlen is len!number(v)
}
`;

/** As a build that stored the list as its text left it, bound so. */
async function storeAsText(
   fixture: R4Fixture,
   source: string,
   column = "v",
): Promise<void> {
   await fixture.storedSQL(
      (t) => `ALTER TABLE ${t(source)} ALTER ${column} TYPE VARCHAR`,
   );
   const bound = fixture.bindings.filter((b) => b.sourceName === source);
   expect(bound.length).toBeGreaterThan(0);
   for (const b of bound) {
      expect(b.schema.find((c) => c.name === column)?.type).not.toBe("VARCHAR");
      b.schema = b.schema.map((c) =>
         c.name === column ? { ...c, type: "VARCHAR" } : c,
      );
   }
}

/** Queries that read the list: answered live where it is stored as text. */
const READS_LIST = (source: string) => [
   `run: ${source} -> { select: id, v; order_by: id }`,
   `run: ${source} -> { aggregate: m is count(v.each) }`,
   `run: ${source} -> { group_by: e is v.each; aggregate: c is count(); order_by: e }`,
   `run: ${source} -> { where: v.each = 'x'; aggregate: c is count() }`,
];

/** Queries that do not read the list: served from the table either way. */
const READS_NO_LIST = (source: string) => [
   `run: ${source} -> { aggregate: n }`,
   `run: ${source} -> { group_by: org; aggregate: n, total is amount.sum(); order_by: org }`,
   `run: ${source} -> { select: id, amount; order_by: id }`,
];

describe("a #(materialize) storage= list column stored as a scalar", () => {
   let fixture: R4Fixture;
   useMaterializationOn();
   beforeAll(async () => {
      fixture = await buildScenario({
         files: { "m.malloy": MODEL },
         seedSql: SEED,
         materialize: true,
      });
      await storeAsText(fixture, "scalar_v");
      await storeAsText(fixture, "records_lr", "lr");
      await storeAsText(fixture, "computed_v");
   }, 120_000);
   afterAll(async () => {
      await disposeScenarios();
   });

   it("answers the list as live does, not as its stored text", async () => {
      const malloy = "run: scalar_v -> { select: id, v; order_by: id }";
      const run = await fixture.run({ name: malloy, malloy, expectLive: true });
      expect(run.live[0].v).toEqual(["x", "y"]);
      expect(rowDifference(run.live, run.served)).toBeUndefined();
      expect(run.servedFrom).not.toBe("storage");
   });

   it.each(READS_LIST("scalar_v"))(
      "answers a query reading it live, equal to live: %s",
      async (malloy) => {
         const run = await fixture.run({
            name: malloy,
            malloy,
            expectLive: true,
         });
         expect(run.live.length).toBeGreaterThan(0);
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         expect(run.servedFrom).not.toBe("storage");
      },
   );

   it.each(READS_NO_LIST("scalar_v"))(
      "serves a query that does not read it from the table, equal to live: %s",
      async (malloy) => {
         const run = await fixture.run({ name: malloy, malloy });
         expect(run.live.length).toBeGreaterThan(0);
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         expect(run.servedFrom).toBe("storage");
      },
   );

   it.each([...READS_LIST("list_v"), ...READS_NO_LIST("list_v")])(
      "serves a list stored as a list from the table, equal to live: %s",
      async (malloy) => {
         const run = await fixture.run({ name: malloy, malloy });
         expect(run.live.length).toBeGreaterThan(0);
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         expect(run.servedFrom).toBe("storage");
      },
   );

   it.each([
      "run: records_lr -> { select: id, lr; order_by: id }",
      "run: records_lr -> { group_by: lr.a; aggregate: t is lr.n.sum(); order_by: a }",
   ])(
      "answers a list of records stored as text live, equal to live: %s",
      async (malloy) => {
         const run = await fixture.run({
            name: malloy,
            malloy,
            expectLive: true,
         });
         expect(run.live.length).toBeGreaterThan(0);
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         expect(run.servedFrom).not.toBe("storage");
      },
   );

   it("serves a query not reading a list of records stored as text from the table", async () => {
      const malloy =
         "run: records_lr -> { group_by: org; aggregate: n; order_by: org }";
      const run = await fixture.run({ name: malloy, malloy });
      expect(rowDifference(run.live, run.served)).toBeUndefined();
      expect(run.servedFrom).toBe("storage");
   });

   it.each([
      "run: computed_v -> { select: id, vlen; order_by: id }",
      "run: computed_v -> { aggregate: n }",
   ])(
      "answers a source whose own field reads the list live, with live's number: %s",
      async (malloy) => {
         const run = await fixture.run({
            name: malloy,
            malloy,
            expectLive: true,
         });
         expect(run.live.length).toBeGreaterThan(0);
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         expect(run.servedFrom).not.toBe("storage");
      },
   );

   it("computes a field over the list as live does, not over its text", async () => {
      const malloy = "run: computed_v -> { select: id, vlen; order_by: id }";
      const run = await fixture.run({ name: malloy, malloy, expectLive: true });
      expect(run.served[0]).toEqual({ id: 1, vlen: 2 });
   });
});

const GATED_MODEL = `##! experimental { givens }
given: ORG :: string

#(access_filter) org = $ORG
#(materialize) storage="lake"
source: gated is duckdb.table('ex') extend {
  primary_key: id
  measure: n is count()
}
`;

describe.each([true, false])(
   "a list stored as a scalar, gated (gated serving %p)",
   (gatedServing) => {
      let fixture: R4Fixture;
      let restore: () => void;
      beforeAll(async () => {
         restore = installSettings({
            env: {
               PERSIST_STORAGE_MODE: "on",
               PUBLISHER_MATERIALIZE: "true",
               ...(gatedServing
                  ? { PUBLISHER_MATERIALIZE_GATED_SOURCES: "true" }
                  : {}),
            },
         }).restore;
         fixture = await buildScenario({
            files: { "m.malloy": GATED_MODEL },
            seedSql: SEED,
            materialize: true,
         });
         await storeAsText(fixture, "gated");
      }, 120_000);
      afterAll(async () => {
         restore();
         await disposeScenarios();
      });

      it("answers each caller's query reading the list live, under its gate", async () => {
         const malloy = "run: gated -> { select: id, v; order_by: id }";
         for (const org of ["acme", "globex"]) {
            const run = await fixture.run({
               name: org,
               malloy,
               givens: { ORG: org },
               expectLive: true,
            });
            expect(run.live.length).toBe(2);
            expect(rowDifference(run.live, run.served)).toBeUndefined();
            expect(run.servedFrom).not.toBe("storage");
         }
      });

      it("scopes each caller to its own rows on a query not reading it", async () => {
         for (const [org, n] of [
            ["acme", 2],
            ["globex", 2],
         ] as const) {
            const run = await fixture.run({
               name: org,
               malloy: "run: gated -> { aggregate: n, total is amount.sum() }",
               givens: { ORG: org },
               expectLive: !gatedServing,
            });
            expect(rowDifference(run.live, run.served)).toBeUndefined();
            expect(run.served).toEqual([{ n, total: org === "acme" ? 4 : 8 }]);
            expect(run.servedFrom).toBe(gatedServing ? "storage" : null);
         }
      });
   },
);

const V1_MODEL = `##! experimental.persistence
source: ex_base is duckdb.table('ex')

#@ persist name="p_text" storage="lake"
source: p_text is ex_base -> { select: id, org, amount, v }

#@ persist name="p_list" storage="lake"
source: p_list is ex_base -> { select: id, org, amount, v }
`;

describe("a v1 #@ persist storage= list column stored as a scalar", () => {
   let fixture: R4Fixture;
   useMaterializationOn();
   beforeAll(async () => {
      fixture = await buildScenario({
         files: { "m.malloy": V1_MODEL },
         seedSql: SEED,
      });
      await storeAsText(fixture, "p_text");
   }, 120_000);
   afterAll(async () => {
      await disposeScenarios();
   });

   it.each(READS_LIST("p_text"))(
      "answers a query reading it live, equal to live: %s",
      async (malloy) => {
         const run = await fixture.run({
            name: malloy,
            malloy,
            expectLive: true,
         });
         expect(run.live.length).toBeGreaterThan(0);
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         expect(run.servedFrom).not.toBe("storage");
      },
   );

   it.each([
      ...READS_LIST("p_list"),
      "run: p_text -> { select: id, amount; order_by: id }",
   ])(
      "serves a list stored as a list, and a query not reading the text, from the table: %s",
      async (malloy) => {
         const run = await fixture.run({ name: malloy, malloy });
         expect(run.live.length).toBeGreaterThan(0);
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         expect(run.servedFrom).toBe("storage");
      },
   );
});
