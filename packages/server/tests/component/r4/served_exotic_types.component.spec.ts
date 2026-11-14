// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for a `storage=` table holding a column Malloy's basic types do not
 * cover: a list, a struct, a MAP, BLOB, INTERVAL, ENUM, UNION, JSON, or a list
 * of structs holding a list. The build-time gate used to declare every such
 * column as `json`, which Malloy refuses in a `type:`, so the table was
 * refused (422 `not_duckdb_portable`) and never stored.
 *
 * Now each column follows the served copy's one declaration rule
 * (`storedMalloyType`): a list or struct is declared as Malloy's array or
 * record type and reads back as live does; anything else is left out of the
 * declaration, so a query reading it is answered live, and every query that
 * does not read it is served from the table. A table with no column left
 * to declare is built too, and the served copy declines its source at bind,
 * so every query reading it is answered live.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
} from "./harness";

interface ExoticType {
   label: string;
   /** A DuckDB expression of the column's type, per row id (1, 2, 3). */
   value: (id: number) => string;
   /** Statements the seed needs first (an ENUM's type). */
   prelude?: string[];
   /**
    * Declared in the served copy's `type:` (a list or a struct), so a query
    * reading it is served; else left out, and that query is answered live.
    */
   declared: boolean;
   /** More queries reading the column, each served or live as `declared`. */
   reads?: string[];
}

const TYPES: ExoticType[] = [
   {
      label: "INTEGER[]",
      value: (i) => `[${i}, ${i * 10}]::INTEGER[]`,
      declared: true,
      reads: [
         "run: s -> { group_by: g; aggregate: t is x.each.sum(), n is x.count(), c is count(); order_by: g }",
         "run: s -> { where: x.each > 15; select: id; order_by: id }",
      ],
   },
   {
      label: "STRUCT",
      value: (i) => `{'a': ${i}, 'b': 'v${i}'}`,
      declared: true,
      reads: [
         "run: s -> { where: x.a = 2; select: id, b is x.b; order_by: id }",
      ],
   },
   {
      label: "a list of structs holding a list",
      value: (i) =>
         `[{'k': 'k${i}', 'v': [${i}, ${i + 1}]}, {'k': 'z', 'v': []::INTEGER[]}]`,
      declared: true,
      reads: [
         // Sums and counts through the repeated record (known limitation
         // 41's shape, over a table rather than a nest of a query).
         "run: s -> { group_by: x.k; aggregate: n is x.count(), t is x.v.each.sum(), c is count(), a is amount.sum(); order_by: k }",
         "run: s -> { group_by: g; nest: by_k is { group_by: x.k; aggregate: c is count() }; order_by: g }",
      ],
   },
   {
      // NULL lists, NULL elements, empty lists, a NULL struct, and field
      // names that need quoting or are reserved words.
      label: "a struct with quoted and reserved field names, NULLs and empty lists",
      value: (i) =>
         i === 2
            ? 'NULL::STRUCT("select" INTEGER, "my field" VARCHAR, xs INTEGER[])'
            : `{'select': ${i === 3 ? "NULL" : i}, 'my field': ${i === 3 ? "NULL" : "'f'"}, 'xs': ${i === 3 ? "[]::INTEGER[]" : "[1, NULL]::INTEGER[]"}}`,
      declared: true,
      reads: [
         "run: s -> { where: x.`select` = 1; select: id, f is x.`my field`; order_by: id }",
         // (An aggregate through `x.xs` fails live on DuckDB in Malloy
         // 0.0.438: "Could not find key __distinct_key in struct".)
         "run: s -> { select: id, xs is x.xs; order_by: id }",
      ],
   },
   {
      // A zoned timestamp live types as `timestamptz`, in a list: declared
      // as one, so the field reads back as live's.
      label: "TIMESTAMPTZ[]",
      value: (i) =>
         `[TIMESTAMPTZ '2024-0${i}-02 03:04:05+00', NULL]::TIMESTAMPTZ[]`,
      declared: true,
      reads: [
         "run: s -> { timezone: 'America/Los_Angeles'; group_by: d is x.each.day; order_by: d }",
      ],
   },
   {
      // Integers past 2^53 in a list, or in a struct: live (DuckDB) keeps
      // every digit and a declared `number` would round them, and no cast
      // reads a nested value exactly, so each is left out.
      label: "BIGINT[] past 2^53",
      value: (i) => `[9007199254740992::BIGINT + ${i}, NULL]::BIGINT[]`,
      declared: false,
      reads: ["run: s -> { aggregate: m is x.each.max() }"],
   },
   {
      label: "STRUCT with a BIGINT past 2^53",
      value: (i) => `{'n': 9007199254740992::BIGINT + ${i}, 's': 'v'}`,
      declared: false,
      reads: ["run: s -> { select: id, n is x.n; order_by: id }"],
   },
   {
      label: "MAP",
      value: (i) => `MAP {'k${i}': ${i}}`,
      declared: false,
   },
   {
      label: "BLOB",
      value: (i) => `'\\x0${i}'::BLOB`,
      declared: false,
   },
   {
      label: "INTERVAL",
      value: (i) => `INTERVAL ${i} DAY`,
      declared: false,
   },
   {
      label: "ENUM",
      prelude: ["CREATE TYPE r4_mood AS ENUM ('calm', 'busy')"],
      value: (i) => `'${i === 2 ? "busy" : "calm"}'::r4_mood`,
      declared: false,
   },
   {
      label: "UNION",
      value: (i) =>
         i === 2
            ? `union_value(s := 'two')::UNION(n INTEGER, s VARCHAR)`
            : `union_value(n := ${i})::UNION(n INTEGER, s VARCHAR)`,
      declared: false,
   },
   {
      // A scalar live types as `sql native` that the destination stores as
      // a type the `type:` could only declare `string`: left out, decided
      // from live's field type.
      label: "UUID",
      value: (i) => `'${i}1111111-2222-3333-4444-555555555555'::UUID`,
      declared: false,
      reads: [
         "run: s -> { group_by: x; aggregate: n is count(); order_by: x }",
         "run: s -> { where: x != null; aggregate: n is count() }",
      ],
   },
   {
      label: "JSON",
      value: (i) => `'{"n": ${i}}'::JSON`,
      declared: false,
   },
];

/** Fields with a number's integer or decimal subtype left out. */
function withoutNumberSubtype(fields: { name: string; type: unknown }[]) {
   return JSON.parse(
      JSON.stringify(fields, (key, value) =>
         key === "subtype" ? undefined : value,
      ),
   );
}

/**
 * {@link withoutNumberSubtype}, and without a nest's per-compile field
 * annotations (each run's own `reference_id`).
 */
function comparableFields(fields: { name: string; type: unknown }[]) {
   return JSON.parse(
      JSON.stringify(fields, (key, value) =>
         key === "subtype" || key === "annotations" ? undefined : value,
      ),
   );
}

function seed(t: ExoticType): string[] {
   const rows = [1, 2, 3]
      .map((i) => `(${i}, '${i === 2 ? "b" : "a"}', ${i}.5, ${t.value(i)})`)
      .join(", ");
   return [
      ...(t.prelude ?? []),
      `CREATE TABLE ex AS SELECT * FROM (VALUES ${rows}) AS t(id, g, amount, x)`,
   ];
}

const MODEL = `##! experimental { persistence }
source: base is duckdb.table('ex')

#@ persist name="ex_stored" storage="lake"
source: s is base -> { select: * }
`;

for (const t of TYPES) {
   describe(`R4: a storage= table with a ${t.label} column builds and serves equal to live`, () => {
      let restore: () => void;
      let fixture: R4Fixture;
      beforeAll(async () => {
         restore = installSettings({
            env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
         }).restore;
         fixture = await buildScenario({
            files: { "m.malloy": MODEL },
            seedSql: seed(t),
         });
      });
      afterAll(async () => {
         restore();
         await disposeScenarios();
      });

      it("compiles the served copy with no problem", async () => {
         expect(await fixture.servedCopyProblems()).toEqual([]);
      });

      it("serves a query that does not read the column", async () => {
         const run = await fixture.run({
            name: "not reading it",
            malloy:
               "run: s -> { group_by: g; aggregate: n is count(), total is amount.sum(); order_by: g }",
         });
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         expect(run.servedVia).toBe("served_copy");
      });

      it(
         t.declared
            ? "serves a query reading the column, equal to live"
            : "answers a query reading the column live, and equal",
         async () => {
            const run = await fixture.run({
               name: "reading it",
               malloy: "run: s -> { select: id, x; order_by: id }",
               expectLive: !t.declared,
            });
            expect(run.live.length).toBe(3);
            expect(rowDifference(run.live, run.served)).toBeUndefined();
            // A number's integer subtype is known limitation 12's metadata
            // difference; every other part of each field, nested ones too,
            // is live's.
            expect(withoutNumberSubtype(run.servedFields)).toEqual(
               withoutNumberSubtype(run.liveFields),
            );
            if (t.declared) {
               expect(run.servedVia).toBe("served_copy");
            } else {
               expect(run.servedVia).toBeUndefined();
            }
         },
      );

      for (const malloy of [
         "run: s -> { select: *; order_by: id }",
         ...(t.reads ?? []),
      ]) {
         it(`${t.declared ? "serves" : "answers live"}, equal to live: ${malloy}`, async () => {
            const run = await fixture.run({
               name: malloy,
               malloy,
               expectLive: !t.declared,
            });
            expect(run.live.length).toBeGreaterThan(0);
            expect(rowDifference(run.live, run.served)).toBeUndefined();
            expect(comparableFields(run.servedFields)).toEqual(
               comparableFields(run.liveFields),
            );
            expect(run.servedVia).toBe(
               t.declared ? "served_copy" : (undefined as unknown as string),
            );
         });
      }
   });
}

/**
 * A warehouse column Malloy types as `sql native` that the destination stores
 * as a list or struct (a Postgres composite type, read by DuckDB as a STRUCT):
 * declaring it as Malloy's record type would serve it in another shape than
 * live's, so it is left out, and a query reading it is answered live. Here
 * the column is a BLOB live, and the captured schema says what such a
 * destination would report.
 */
describe("R4: a stored list or struct whose field live types as sql native is left out per query", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   const blob = TYPES.find((t) => t.label === "BLOB") as ExoticType;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: { "m.malloy": MODEL },
         seedSql: seed(blob),
      });
      for (const b of fixture.bindings) {
         b.schema = b.schema.map((c) =>
            c.name === "x" ? { ...c, type: "STRUCT(a INTEGER)" } : c,
         );
      }
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("answers a query reading it live, and equal", async () => {
      const run = await fixture.run({
         name: "reading it",
         malloy: "run: s -> { select: id, x; order_by: id }",
         expectLive: true,
      });
      expect(run.live.length).toBe(3);
      expect(rowDifference(run.live, run.served)).toBeUndefined();
      expect(run.servedVia).toBeUndefined();
   });

   it("still serves a query that does not read it", async () => {
      const run = await fixture.run({
         name: "not reading it",
         malloy:
            "run: s -> { group_by: g; aggregate: n is count(), total is amount.sum(); order_by: g }",
      });
      expect(rowDifference(run.live, run.served)).toBeUndefined();
      expect(run.servedVia).toBe("served_copy");
   });
});

/**
 * The same guard one level down: a struct whose field live types as `sql
 * native` (here a BLOB, captured as what a destination storing it as text
 * would report) is left out, though the struct itself is a record live.
 */
describe("R4: a stored struct holding a field live types as sql native is left out per query", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: { "m.malloy": MODEL },
         seedSql: seed({
            label: "STRUCT holding a BLOB",
            value: (i) => `{'a': ${i}, 'b': '\\x0${i}'::BLOB}`,
            declared: false,
         }),
      });
      for (const b of fixture.bindings) {
         b.schema = b.schema.map((c) =>
            c.name === "x" ? { ...c, type: "STRUCT(a INTEGER, b VARCHAR)" } : c,
         );
      }
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("answers a query reading it live, and equal", async () => {
      const run = await fixture.run({
         name: "reading it",
         malloy: "run: s -> { select: id, x; order_by: id }",
         expectLive: true,
      });
      expect(run.live.length).toBe(3);
      expect(rowDifference(run.live, run.served)).toBeUndefined();
      expect(run.servedVia).toBeUndefined();
   });

   it("still serves a query that does not read it", async () => {
      const run = await fixture.run({
         name: "not reading it",
         malloy:
            "run: s -> { group_by: g; aggregate: n is count(), total is amount.sum(); order_by: g }",
      });
      expect(rowDifference(run.live, run.served)).toBeUndefined();
      expect(run.servedVia).toBe("served_copy");
   });
});

/**
 * The same rules for a `#(materialize) storage=` table, whose served copy
 * declares the plan's columns: a UUID live types as `sql native` is left out
 * per query, and a zoned timestamp is declared `timestamptz`, as live types
 * it.
 */
describe("R4: a #(materialize) storage= table with a UUID and a zoned timestamp", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: {
            "m.malloy": `#(materialize) storage="lake"
source: s is duckdb.table('ex') extend { primary_key: id }
`,
         },
         seedSql: [
            `CREATE TABLE ex AS SELECT * FROM (VALUES
               (1, 'a', 1.5, '11111111-2222-3333-4444-555555555555'::UUID, TIMESTAMPTZ '2024-01-02 03:04:05+00'),
               (2, 'b', 2.5, NULL, NULL),
               (3, 'a', 3.5, '31111111-2222-3333-4444-555555555555'::UUID, TIMESTAMPTZ '2024-06-30 23:00:00+00')
             ) AS t(id, g, amount, x, seen)`,
         ],
         materialize: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("compiles the served copy with no problem", async () => {
      expect(await fixture.servedCopyProblems()).toEqual([]);
   });

   it.each([
      "run: s -> { select: id, x; order_by: id }",
      "run: s -> { select: *; order_by: id }",
      "run: s -> { group_by: x; aggregate: n is count(); order_by: x }",
   ])("answers live, equal to live: %s", async (malloy) => {
      const run = await fixture.run({ name: malloy, malloy, expectLive: true });
      expect(run.live.length).toBe(3);
      expect(rowDifference(run.live, run.served)).toBeUndefined();
      expect(comparableFields(run.servedFields)).toEqual(
         comparableFields(run.liveFields),
      );
      expect(run.servedVia).toBeUndefined();
   });

   it.each([
      "run: s -> { group_by: g; aggregate: n is count(), total is amount.sum(); order_by: g }",
      "run: s -> { select: id, seen; order_by: id }",
      "run: s -> { timezone: 'America/Los_Angeles'; group_by: d is seen.day; aggregate: n is count(); order_by: d }",
   ])("serves, equal to live: %s", async (malloy) => {
      const run = await fixture.run({ name: malloy, malloy });
      expect(rowDifference(run.live, run.served)).toBeUndefined();
      expect(comparableFields(run.servedFields)).toEqual(
         comparableFields(run.liveFields),
      );
      expect(run.servedVia).toBe("served_copy");
   });
});

/**
 * A field live types as `sql native` reached through an inlined join: a
 * join_one's record, or a pair table's (`inline=` on a join_many). No stored
 * declaration reads it back as live does, so the source is left to its
 * fallback and every query answers live, equal to live.
 */
describe.each([
   {
      label: "an inlined join_one",
      model: `source: par is duckdb.table('par') extend { primary_key: pid }

#(materialize) storage="lake" inline=["par"]
source: s is duckdb.table('ex') extend {
  primary_key: id
  join_one: par with pid
}
`,
   },
   {
      label: "a pair table",
      model: `source: par is duckdb.table('par') extend { primary_key: pid }

#(materialize) storage="lake" inline=["par"]
source: s is duckdb.table('ex') extend {
  primary_key: id
  join_many: par on par.pid = pid
}
`,
   },
])(
   "R4: a UUID reached through $label of a #(materialize) storage= table",
   ({ model }) => {
      let restore: () => void;
      let fixture: R4Fixture;
      beforeAll(async () => {
         restore = installSettings({
            env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
         }).restore;
         fixture = await buildScenario({
            files: { "m.malloy": model },
            seedSql: [
               `CREATE TABLE ex AS SELECT * FROM (VALUES
                  (1, 'a', 10), (2, 'b', 20), (3, 'a', 10)) AS t(id, g, pid)`,
               `CREATE TABLE par AS SELECT * FROM (VALUES
                  (10, '11111111-2222-3333-4444-555555555555'::UUID),
                  (20, '21111111-2222-3333-4444-555555555555'::UUID)) AS t(pid, u)`,
            ],
            materialize: true,
         });
      });
      afterAll(async () => {
         restore();
         await disposeScenarios();
      });

      it.each([
         "run: s -> { group_by: id, u is par.u; order_by: id }",
         "run: s -> { group_by: par.u; aggregate: n is count(); order_by: u }",
      ])("answers live, equal to live: %s", async (malloy) => {
         const run = await fixture.run({
            name: malloy,
            malloy,
            expectLive: true,
         });
         expect(run.live.length).toBeGreaterThan(0);
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         expect(comparableFields(run.servedFields)).toEqual(
            comparableFields(run.liveFields),
         );
         expect(run.servedVia).toBeUndefined();
      });
   },
);

describe("R4: a storage= table whose every column is left out builds, and its source answers live", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
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
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("is bound, and the served copy declines the source for its column types", async () => {
      const [binding] = fixture.bindings;
      expect(binding.schema.map((c) => c.name)).toEqual(["x"]);
      expect(
         (await fixture.servedCopyProblems()).map((p) => [
            p.sourceName,
            p.reason,
         ]),
      ).toEqual([["s", "column_types"]]);
   });

   it("answers a query over it live, and equal", async () => {
      const run = await fixture.run({
         name: "reading it",
         malloy: "run: s -> { aggregate: n is count() }",
         expectLive: true,
      });
      expect(run.live).toEqual([{ n: 2 }]);
      expect(rowDifference(run.live, run.served)).toBeUndefined();
   });
});
