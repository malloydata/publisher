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
 * does not read it is served from the table. Each table passes the real gate
 * over its captured schema.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { assertStorageServeShapeCompiles } from "../../../src/service/materialization/run/materialization_build_session";
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
}

const TYPES: ExoticType[] = [
   {
      label: "INTEGER[]",
      value: (i) => `[${i}, ${i * 10}]::INTEGER[]`,
      declared: true,
   },
   {
      label: "STRUCT",
      value: (i) => `{'a': ${i}, 'b': 'v${i}'}`,
      declared: true,
   },
   {
      label: "a list of structs holding a list",
      value: (i) =>
         `[{'k': 'k${i}', 'v': [${i}, ${i + 1}]}, {'k': 'z', 'v': []::INTEGER[]}]`,
      declared: true,
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

      it("passes the build-time gate over its captured schema", async () => {
         const [binding] = fixture.bindings;
         expect(binding.schema.map((c) => c.name)).toContain("x");
         await expect(
            assertStorageServeShapeCompiles({
               destinationName: "lake",
               sourceName: "s",
               virtualHandle: binding.virtualHandle,
               physicalTableName: "ex_stored",
               schema: binding.schema,
            }),
         ).resolves.toBeUndefined();
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
