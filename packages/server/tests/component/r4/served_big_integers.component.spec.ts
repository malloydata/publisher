// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for integers past 2^53 (known limitation 12): a stored table's `type:`
 * can declare a column only as a plain `number`, which Malloy reads through a
 * JavaScript number, so a stored read gave 9007199254740993 as
 * 9007199254740992 where live, on DuckDB, keeps every digit. Each stored
 * serving path now reads a column whose author field Malloy types as a
 * `bigint` number through its native type (`::"HUGEINT"`), so it stays served
 * from the table and comes back exact: the served copy over `#@ persist
 * storage=` and over `#(materialize)`, `storage=` and colocated, and the
 * storage serve shape. A column no cast reads exactly is left out per query.
 *
 * On Postgres live itself rounds (its rows come back through JSON): served
 * equals that, never more precise. Run with `PUBLISHER_TEST_POSTGRES_URL`.
 */

import { PostgresConnection } from "@malloydata/db-postgres";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { strictRowDifference } from "../../../src/test_helpers/conformance/answer_difference";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   freshLoadPool,
   loadFixture,
   type Outcome,
   withSwitch,
} from "./conformance/package_harness";
import {
   buildScenario,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
   type R4Scenario,
   type Row,
} from "./harness";

const POSTGRES_URL = process.env.PUBLISHER_TEST_POSTGRES_URL;
const PG_SCHEMA = "r4_big_integers";

/** 2^53 + 1, the first integer a JavaScript number cannot hold. */
const ABOVE = 9007199254740993n;
/** 2^127 - 1, HUGEINT's largest value. */
const HUGE_MAX = 170141183460469231731687303715884105727n;

/** The seed, DuckDB (with HUGEINT columns) or Postgres (BIGINT only). */
function seed(schema: string, duckdb: boolean): string[] {
   const big = (v: bigint) => `CAST('${v}' AS BIGINT)`;
   const huge = (v: bigint) => `CAST('${v}' AS HUGEINT)`;
   const rows: [bigint, string, number, bigint, bigint, bigint][] = [
      [ABOVE, "a", 1, 10n ** 30n, HUGE_MAX, 1n],
      [ABOVE - 2n, "a", 2, 1n, 1n, 2n],
      [-ABOVE, "b", 3, -(10n ** 30n) - 7n, -5n, 1n],
   ];
   return [
      `CREATE TABLE ${schema}big AS SELECT * FROM (VALUES
         ${rows
            .map(
               ([id, g, small, h, hmax, pid]) =>
                  `(${big(id)}, '${g}', ${small}, ${big(pid)}${duckdb ? `, ${huge(h)}, ${huge(hmax)}` : ""})`,
            )
            .join(",\n         ")}
       ) AS t(id, g, small, pid${duckdb ? ", h, hmax" : ""})`,
      `CREATE TABLE ${schema}par AS SELECT * FROM (VALUES
         (${big(1n)}, ${big(ABOVE + 2n)}), (${big(2n)}, ${big(-ABOVE)})
       ) AS t(pid, pbig)`,
   ];
}

/** Fields with a number's integer or decimal subtype left out. */
function withoutNumberSubtype(fields: { name: string; type: unknown }[]) {
   return JSON.parse(
      JSON.stringify(fields, (key, value) =>
         key === "subtype" ? undefined : value,
      ),
   );
}

/** The values of `column`, as exact decimal strings. */
function exact(rows: Row[], column: string): string[] {
   return rows.map((r) => String(r[column]));
}

interface Case {
   name: string;
   malloy: string;
   /** An exact column and the values live must give, in order. */
   expect?: { column: string; values: string[] };
   /** Answered from the table (default) or not. */
   live?: true;
   /** The DuckDB tiers only (HUGEINT columns). */
   duckdbOnly?: true;
}

const DUCKDB_CASES: Case[] = [
   {
      name: "a query selecting the big column, served exact",
      malloy: "run: s -> { select: id, g; order_by: id }",
      expect: {
         column: "id",
         values: [`${-ABOVE}`, `${ABOVE - 2n}`, `${ABOVE}`],
      },
   },
   {
      name: "a query not reading it",
      malloy:
         "run: s -> { group_by: g; aggregate: sm is small.sum(), n is count(); order_by: g }",
   },
   {
      name: "a group-by on it",
      malloy:
         "run: s -> { group_by: id; aggregate: n is count(); order_by: id }",
      expect: {
         column: "id",
         values: [`${-ABOVE}`, `${ABOVE - 2n}`, `${ABOVE}`],
      },
   },
   {
      name: "a filter on it, by a literal past 2^53",
      malloy: `run: s -> { where: id = ${ABOVE}; select: id, g }`,
      expect: { column: "id", values: [`${ABOVE}`] },
   },
   {
      name: "a sum and a max over it",
      malloy:
         "run: s -> { group_by: g; aggregate: tot is id.sum(), top is id.max(); order_by: g }",
      expect: { column: "tot", values: [`${2n * ABOVE - 2n}`, `${-ABOVE}`] },
   },
   {
      name: "HUGEINT values, selected and summed",
      malloy:
         "run: s -> { group_by: g; aggregate: th is h.sum(), top is hmax.max(); order_by: g }",
      expect: { column: "top", values: [`${HUGE_MAX}`, "-5"] },
      duckdbOnly: true,
   },
];

/** The cases for a model whose source also carries the author's own fields. */
const AUTHOR_FIELD_CASES: Case[] = [
   {
      name: "a measure the source declares over the big column",
      malloy: "run: s -> { group_by: g; aggregate: id_total; order_by: g }",
      expect: {
         column: "id_total",
         values: [`${2n * ABOVE - 2n}`, `${-ABOVE}`],
      },
   },
   {
      name: "a dimension the source computes from it",
      malloy: "run: s -> { select: id_next; order_by: id_next }",
      expect: {
         column: "id_next",
         values: [`${-ABOVE + 1n}`, `${ABOVE - 1n}`, `${ABOVE + 1n}`],
      },
   },
];

const JOIN_CASES: Case[] = [
   {
      name: "a big column of an inlined join_one",
      malloy: "run: s -> { select: id, par.pbig; order_by: id }",
      expect: {
         column: "pbig",
         values: [`${ABOVE + 2n}`, `${-ABOVE}`, `${ABOVE + 2n}`],
      },
   },
];

/** One model per path, over connection `w` with its tables in `schema`. */
function models(w: string, schema: string) {
   const t = (name: string) => `${w}.table('${schema}${name}')`;
   const own = `primary_key: id
  dimension: id_next is id + 1
  measure: id_total is id.sum()
  view: by_g is { group_by: g; aggregate: id_total }`;
   return {
      persist: `##! experimental { persistence }
source: base is ${t("big")}

#@ persist name="sp" storage="lake"
source: s is base -> { select: * } extend {
  ${own}
}
`,
      materialize: (
         storage: string,
      ) => `source: par is ${t("par")} extend { primary_key: pid }

#(materialize) ${storage} inline=["par"]
source: s is ${t("big")} extend {
  ${own}
  join_one: par with pid
}
`,
   };
}

interface Tier {
   label: string;
   env: Record<string, string>;
   cases: Case[];
   /** Postgres: live rounds past 2^53, and served must equal that. */
   rounds?: true;
   scenario: () => Promise<R4Scenario & { cleanup?: () => Promise<void> }>;
}

const duckdbCases = [...DUCKDB_CASES, ...AUTHOR_FIELD_CASES];

const tiers: Tier[] = [
   {
      label: "#@ persist storage=, served copy",
      env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      cases: duckdbCases,
      scenario: async () => ({
         files: { "m.malloy": models("duckdb", "").persist },
         seedSql: seed("", true),
      }),
   },
   {
      label: "#(materialize) storage=",
      env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      cases: [...duckdbCases, ...JOIN_CASES],
      scenario: async () => ({
         files: {
            "m.malloy": models("duckdb", "").materialize('storage="lake"'),
         },
         seedSql: seed("", true),
         materialize: true,
      }),
   },
   {
      label: "colocated #(materialize), DuckDB",
      env: { PERSIST_STORAGE_MODE: "off", PUBLISHER_MATERIALIZE: "true" },
      cases: [...duckdbCases, ...JOIN_CASES],
      scenario: async () => ({
         files: {
            "m.malloy": models("duckdb", "").materialize('name="r4_bigc"'),
         },
         seedSql: seed("", true),
         materialize: true,
      }),
   },
   ...(POSTGRES_URL
      ? [
           {
              label: "colocated #(materialize), Postgres",
              env: {
                 PERSIST_STORAGE_MODE: "off",
                 PUBLISHER_MATERIALIZE: "true",
              },
              cases: [...duckdbCases, ...JOIN_CASES].filter(
                 (c) => !c.duckdbOnly,
              ),
              rounds: true as const,
              scenario: async () => {
                 const pg = new PostgresConnection({
                    name: "pg",
                    connectionString: POSTGRES_URL,
                 });
                 await pg.runSQL(`DROP SCHEMA IF EXISTS ${PG_SCHEMA} CASCADE`);
                 await pg.runSQL(`CREATE SCHEMA ${PG_SCHEMA}`);
                 return {
                    files: {
                       "m.malloy": models("pg", `${PG_SCHEMA}.`).materialize(
                          'name="r4_bigpg"',
                       ),
                    },
                    seedSql: seed(`${PG_SCHEMA}.`, false),
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
   describe(`R4: integers past 2^53 served from storage equal live (${tier.label})`, () => {
      let restore: () => void;
      let fixture: R4Fixture;
      let cleanup: (() => Promise<void>) | undefined;
      beforeAll(async () => {
         restore = installSettings({ env: tier.env }).restore;
         const scenario = await tier.scenario();
         cleanup = scenario.cleanup;
         fixture = await buildScenario(scenario);
      });
      afterAll(async () => {
         restore();
         await disposeScenarios();
         await cleanup?.();
      });

      it("compiles the served copy with no problem", async () => {
         expect(await fixture.servedCopyProblems()).toEqual([]);
      });

      for (const c of tier.cases) {
         it(c.name, async () => {
            const run = await fixture.run({ name: c.name, malloy: c.malloy });
            expect(rowDifference(run.live, run.served)).toBeUndefined();
            if (c.expect) {
               const values = tier.rounds
                  ? // Live rounds through JSON on Postgres: the stored read
                    // must round the same way, no more precise.
                    c.expect.values.map((v) =>
                       BigInt(Number(BigInt(v))).toString(),
                    )
                  : c.expect.values;
               expect(exact(run.live, c.expect.column)).toEqual(values);
               expect(exact(run.served, c.expect.column)).toEqual(values);
            }
            // The fields too, the bigint subtype included (the cast gives
            // the field live's own type); other numbers' subtypes are known
            // limitation 12's metadata difference.
            expect(withoutNumberSubtype(run.servedFields)).toEqual(
               withoutNumberSubtype(run.liveFields),
            );
            const bigintFields = (fields: { name: string; type: unknown }[]) =>
               fields
                  .filter((f) => JSON.stringify(f.type).includes('"bigint"'))
                  .map((f) => f.name);
            expect(bigintFields(run.servedFields)).toEqual(
               bigintFields(run.liveFields),
            );
            if (c.live) {
               expect(run.servedVia).toBeUndefined();
               return;
            }
            expect(run.servedVia).toBe("served_copy");
         });
      }
   });
}

/**
 * A big-integer column no cast reads exactly (here, a table whose captured
 * schema says the column holds a DOUBLE, as a float the warehouse returned
 * would): left out per query, so a query reading it answers live and exact,
 * every other query still reads the table, and a source whose own text names
 * it is left as written.
 */
{
   describe("R4: a big-integer column no cast reads exactly is left out per query (served copy)", () => {
      let restore: () => void;
      let fixture: R4Fixture;
      beforeAll(async () => {
         restore = installSettings({
            env: {
               PERSIST_STORAGE_MODE: "on",
               PUBLISHER_MATERIALIZE: "true",
            },
         }).restore;
         fixture = await buildScenario({
            files: {
               "m.malloy": `##! experimental { persistence }
source: base is duckdb.table('big')

#@ persist name="plain" storage="lake"
source: plain is base -> { select: * }
${`
#@ persist name="sp" storage="lake"
source: s is base -> { select: * } extend {
  measure: id_total is id.sum()
}
`}`,
            },
            seedSql: seed("", true),
         });
         // The captured schema of a table whose `id` lost its digits.
         for (const b of fixture.bindings) {
            b.schema = b.schema.map((c) =>
               c.name === "id" ? { ...c, type: "DOUBLE" } : c,
            );
         }
      });
      afterAll(async () => {
         restore();
         await disposeScenarios();
      });

      const answer = async (malloy: string) => {
         fixture.setBindings([]);
         const live = await fixture.runAsIs(malloy);
         fixture.setBindings(fixture.bindings);
         const served = await fixture.runAsIs(malloy);
         return { live, served };
      };

      it("answers a query reading the column live, and exact", async () => {
         const { live, served } = await answer(
            "run: plain -> { select: id; order_by: id }",
         );
         expect(served.path?.servedBy.kind).toBe("live");
         expect(exact(served.rows, "id")).toEqual([
            `${-ABOVE}`,
            `${ABOVE - 2n}`,
            `${ABOVE}`,
         ]);
         expect(rowDifference(live.rows, served.rows)).toBeUndefined();
      });

      it("still serves a query that does not read it", async () => {
         const { live, served } = await answer(
            "run: plain -> { group_by: g; aggregate: n is count(), sm is small.sum() }",
         );
         expect(served.servedFrom).toBe("storage");
         expect(rowDifference(live.rows, served.rows)).toBeUndefined();
      });

      it("leaves a source whose own text names it as written, and answers live", async () => {
         const { live, served } = await answer(
            "run: s -> { group_by: g; aggregate: n is count(), id_total }",
         );
         expect(served.path?.servedBy.kind).toBe("live");
         expect(rowDifference(live.rows, served.rows)).toBeUndefined();
         expect(
            (await fixture.servedCopyProblems()).map((p) => [
               p.sourceName,
               p.reason,
            ]),
         ).toEqual([["s", "column_types"]]);
      });
   });
}

/**
 * The v1 cutover's colocated tables (`#@ persist` with no `storage=`, under
 * the v1 cutover): the served copy reads them in the source's
 * own warehouse, and a big-integer column through the warehouse's own bigint
 * type, so it stays served and exact.
 */
describe("R4: integers past 2^53 served from storage equal live (v1 colocated #@ persist, the v1 cutover)", () => {
   const values = seed("", true)[0].replace(/^CREATE TABLE big AS /, "");
   const model = `##! experimental { persistence }
source: base is duckdb.sql("""${values}""")

#@ persist name="r4_big_v1"
source: s is base -> { select: * } extend {
  primary_key: id
  dimension: id_next is id + 1
  measure: id_total is id.sum()
}
`;
   let restore: () => void;
   let loader: Awaited<ReturnType<typeof loadFixture>>;
   let fixture: Awaited<ReturnType<(typeof loader)["bind"]>>;
   const live = new Map<string, Outcome>();
   beforeAll(async () => {
      await freshLoadPool();
      restore = withSwitch("cutover");
      loader = await loadFixture({ "m.malloy": model });
      for (const c of duckdbCases) {
         live.set(c.name, await loader.run("m.malloy", c.malloy));
      }
      fixture = await loader.bind();
   }, 300_000);
   afterAll(async () => {
      restore();
      await loader.dispose();
      await freshLoadPool();
   });

   it("binds the table colocated", () => {
      expect(fixture.built).toEqual([{ name: "s", tier: "colocated" }]);
   });

   for (const c of duckdbCases) {
      it(c.name, async () => {
         const l = live.get(c.name);
         const s = await fixture.run("m.malloy", c.malloy);
         if (!l || "error" in l)
            throw new Error(`live errored: ${l && l.error}`);
         if ("error" in s) throw new Error(`served errored: ${s.error}`);
         expect(s.answer.via).toBe("served_copy");
         expect(s.answer.servedBy).not.toBe("live");
         expect(
            strictRowDifference(l.answer.rows, s.answer.rows),
         ).toBeUndefined();
         if (c.expect) {
            expect(exact(l.answer.rows, c.expect.column)).toEqual(
               c.expect.values,
            );
            expect(exact(s.answer.rows, c.expect.column)).toEqual(
               c.expect.values,
            );
         }
         const bigintFields = (fields: { name: string; type: unknown }[]) =>
            fields
               .filter((f) => JSON.stringify(f.type).includes('"bigint"'))
               .map((f) => f.name);
         expect(bigintFields(s.answer.fields)).toEqual(
            bigintFields(l.answer.fields),
         );
      });
   }
});

/** 2^127, one past HUGEINT's largest value. */
const PAST_HUGEINT = 2n ** 127n;
/** A BIGINT whose sum over two rows is past 2^63 (BIGINT's range). */
const HALF_PAST_INT64 = 6000000000000000001n;

/**
 * A colocated column wider than any type Malloy names for a `bigint` number
 * (review of #1714): read through a cast to exactly the type its build
 * captured from the warehouse's catalog, or, with no such cast, answered
 * live. Never narrowed: a served read that fails where live answers is a
 * wrong answer too.
 *
 * - DuckDB: a UHUGEINT past 2^127 - 1 is read as UHUGEINT, not HUGEINT.
 * - Postgres: a sum of BIGINTs is stored as `numeric`, which Malloy reads as
 *   a float. Casting it back to BIGINT failed with "bigint out of range" past
 *   2^63; it is now left out, so a query reading it answers live (equal to
 *   live, with live's own representation), and every other query still reads
 *   the table.
 */
const narrowingTiers: {
   label: string;
   scenario: () => Promise<R4Scenario & { cleanup?: () => Promise<void> }>;
   cases: (Case & { expectLive?: true })[];
}[] = [
   {
      label: "DuckDB, a UHUGEINT past 2^127 - 1",
      scenario: async () => ({
         files: {
            "m.malloy": `#(materialize) name="r4_bigu"
source: s is duckdb.table('wide') extend {
  measure: u_max is u.max()
}
`,
         },
         seedSql: [
            `CREATE TABLE wide AS SELECT * FROM (VALUES
               (CAST('${PAST_HUGEINT}' AS UHUGEINT), 'a'),
               (CAST('${2n ** 128n - 1n}' AS UHUGEINT), 'a'),
               (CAST('1' AS UHUGEINT), 'b')
             ) AS t(u, g)`,
         ],
         materialize: true,
      }),
      cases: [
         {
            name: "selects it, exact",
            malloy: "run: s -> { select: u, g; order_by: u }",
            expect: {
               column: "u",
               values: ["1", `${PAST_HUGEINT}`, `${2n ** 128n - 1n}`],
            },
         },
         {
            name: "groups and filters by it",
            malloy: `run: s -> { where: u >= ${PAST_HUGEINT}; group_by: u; order_by: u }`,
            expect: {
               column: "u",
               values: [`${PAST_HUGEINT}`, `${2n ** 128n - 1n}`],
            },
         },
         {
            name: "a max over it",
            malloy: "run: s -> { group_by: g; aggregate: u_max; order_by: g }",
            expect: { column: "u_max", values: [`${2n ** 128n - 1n}`, "1"] },
         },
      ],
   },
   ...(POSTGRES_URL
      ? [
           {
              label: "Postgres, a sum of BIGINTs past 2^63 stored as numeric",
              scenario: async () => {
                 const schema = "r4_big_sum";
                 const pg = new PostgresConnection({
                    name: "pg",
                    connectionString: POSTGRES_URL,
                 });
                 await pg.runSQL(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);
                 await pg.runSQL(`CREATE SCHEMA ${schema}`);
                 return {
                    files: {
                       "m.malloy": `#(materialize) name="r4_bigsum"
source: s is pg.table('${schema}.v') -> {
  group_by: g
  aggregate: tot is v.sum(), n is count()
}
`,
                    },
                    seedSql: [
                       `CREATE TABLE ${schema}.v AS SELECT * FROM (VALUES
                          (CAST('${HALF_PAST_INT64}' AS BIGINT), 'a'),
                          (CAST('${HALF_PAST_INT64}' AS BIGINT), 'a'),
                          (CAST('5' AS BIGINT), 'b')
                        ) AS t(v, g)`,
                    ],
                    materialize: true,
                    warehouse: { name: "pg", connection: pg, schema },
                    cleanup: async () => {
                       await pg.runSQL(
                          `DROP SCHEMA IF EXISTS ${schema} CASCADE`,
                       );
                       await pg.close();
                    },
                 };
              },
              cases: [
                 {
                    name: "a query reading the sum answers live, equal to live",
                    malloy: "run: s -> { select: g, tot; order_by: g }",
                    expect: {
                       column: "tot",
                       values: [`${2n * HALF_PAST_INT64}`, "5"].map((v) =>
                          BigInt(Number(BigInt(v))).toString(),
                       ),
                    },
                    expectLive: true as const,
                 },
                 {
                    name: "a query not reading it still reads the table",
                    malloy:
                       "run: s -> { group_by: g; aggregate: c is n.sum(); order_by: g }",
                 },
              ],
           },
        ]
      : []),
];

for (const tier of narrowingTiers) {
   describe(`R4: a colocated big integer wider than Malloy's bigint type is never narrowed (${tier.label})`, () => {
      let restore: () => void;
      let fixture: R4Fixture;
      let cleanup: (() => Promise<void>) | undefined;
      beforeAll(async () => {
         restore = installSettings({
            env: { PERSIST_STORAGE_MODE: "off", PUBLISHER_MATERIALIZE: "true" },
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

      for (const c of tier.cases) {
         it(c.name, async () => {
            const run = await fixture.run({
               name: c.name,
               malloy: c.malloy,
               ...(c.expectLive ? { expectLive: true } : {}),
            });
            expect(rowDifference(run.live, run.served)).toBeUndefined();
            if (c.expect) {
               expect(exact(run.live, c.expect.column)).toEqual(
                  c.expect.values,
               );
               expect(exact(run.served, c.expect.column)).toEqual(
                  c.expect.values,
               );
            }
            // The same representation as live: a bigint stays a bigint.
            const kinds = (rows: Row[]) =>
               rows.map((r) =>
                  Object.fromEntries(
                     Object.entries(r).map(([k, v]) => [k, typeof v]),
                  ),
               );
            expect(kinds(run.served)).toEqual(kinds(run.live));
            if (c.expectLive) {
               expect(run.servedVia).toBeUndefined();
            } else {
               expect(run.servedVia).toBe("served_copy");
            }
         });
      }
   });
}
