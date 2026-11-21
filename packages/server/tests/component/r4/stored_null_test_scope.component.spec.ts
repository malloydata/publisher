// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A null test of a string column (`account_id is not null`, `is null`)
 * compares no string, so a `storage=` table re-applying it is served, equal
 * to live: in a `where:` of the caller's own `extend` (the row scope), and in
 * the query's own `where:` on a column under a nondeterministic collation,
 * where equality is not byte equality. Before, both answered live ("does not
 * compare as its source does"). Anything else on the string is still judged:
 * an equality on the nondeterministic column, an ordering under `en_US`, and
 * a null test of a function of the column (`nullif`) each answer live, equal
 * to live.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import {
   type Answer,
   openPostgresLake,
   POSTGRES_URL,
   type PostgresLake,
} from "./postgres_lake";

const SEED = [
   `CREATE COLLATION ci_l2 (provider = icu, locale = 'und-u-ks-level2', deterministic = false)`,
   `CREATE TABLE accounts (account_id text, label text COLLATE ci_l2, seats int)`,
   `INSERT INTO accounts VALUES
      ('a1', 'Acme', 3), ('A1', 'acme', 1), (NULL, 'globex', 2),
      ('b2', NULL, 4), ('', 'Initech', 5), (NULL, NULL, 6)`,
];

const MODEL = `##! experimental.persistence

#@ persist name="t_accounts" storage="lake"
source: accounts is warehouse.table('public.accounts') -> { select: * }
`;

interface Case {
   name: string;
   query: string;
   served: boolean;
}

const total = "aggregate: n is count(), s is seats.sum()";

const CASES: Case[] = [
   {
      name: "a scope's is not null",
      query: `run: accounts extend { where: account_id is not null } -> { ${total} }`,
      served: true,
   },
   {
      name: "a scope's is null",
      query: `run: accounts extend { where: account_id is null } -> { ${total} }`,
      served: true,
   },
   {
      name: "a scope's null test of the nondeterministic column",
      query: `run: accounts extend { where: label is not null } -> { ${total} }`,
      served: true,
   },
   {
      name: "a filter's null test of the nondeterministic column",
      query: `run: accounts -> { where: label is null; ${total} }`,
      served: true,
   },
   {
      name: "a filtered measure's null test of the nondeterministic column",
      query: "run: accounts -> { aggregate: n is count() { where: label is not null } }",
      served: true,
   },
   {
      name: "a filter's equality on the nondeterministic column",
      query: `run: accounts -> { where: label = 'acme'; ${total} }`,
      served: false,
   },
   {
      name: "a scope's ordering under en_US",
      query: `run: accounts extend { where: account_id > 'B' } -> { ${total} }`,
      served: false,
   },
   {
      name: "a scope's null test of a function of the column",
      query: `run: accounts extend { where: nullif(label, 'ACME') is null } -> { ${total} }`,
      served: false,
   },
];

describe.skipIf(!POSTGRES_URL)(
   "a null test of a string column over a storage= table (Postgres into DuckLake)",
   () => {
      let lake: PostgresLake;
      const live: Record<string, Answer> = {};
      const served: Record<string, Answer> = {};

      beforeAll(async () => {
         lake = await openPostgresLake("nul", { "m.malloy": MODEL }, SEED);
         for (const c of CASES)
            live[c.name] = await lake.ask("m.malloy", c.query);
         await lake.build();
         for (const c of CASES) {
            served[c.name] = await lake.ask("m.malloy", c.query);
         }
      }, 300_000);

      afterAll(async () => {
         await lake?.close();
      });

      it("answers live as the data says, before any build", () => {
         expect(live["a scope's is not null"].rows).toEqual([
            { n: "4", s: "13" },
         ]);
         expect(live["a scope's is null"].rows).toEqual([{ n: "2", s: "8" }]);
         // Case-blind equality matches both spellings, and nullif nulls both.
         expect(
            live["a filter's equality on the nondeterministic column"].rows,
         ).toEqual([{ n: "2", s: "4" }]);
         expect(
            live["a scope's null test of a function of the column"].rows,
         ).toEqual([{ n: "4", s: "14" }]);
      });

      for (const c of CASES) {
         it(`${c.name}: answers equal to live, ${c.served ? "from its stored table" : "live"}`, () => {
            expect(served[c.name].rows).toEqual(live[c.name].rows);
            expect(served[c.name].kind, served[c.name].detail).toBe(
               c.served ? "materialized" : "live",
            );
         });
      }
   },
);
