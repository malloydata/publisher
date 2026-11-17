// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   type QueryMaterializer,
   SingleConnectionRuntime,
} from "@malloydata/malloy";
import { afterAll, describe, expect, it } from "bun:test";
import { assertSameCompositeMember } from "./composite_member";

const connection = new DuckDBConnection("duckdb", ":memory:");
const runtime = new SingleConnectionRuntime({ connection });
afterAll(async () => {
   await connection.close();
});

const BASE = `##! experimental { composite_sources }
source: y is duckdb.sql("SELECT 1 AS org, 'a' AS cat, DATE '2020-01-01' AS d")
`;

/**
 * Two compiles of one model's text: `live` resolves each composite to its
 * first member; `routed` declares the first member without `cat`, so Malloy
 * resolves the same query to the second, as a served copy can when it
 * redeclares a stored column.
 */
function compiles(body: (first: string) => string, query: string) {
   const live = runtime
      .loadModel(BASE + body("y extend { where: org = 1 }"))
      .loadQuery(query);
   const routed = runtime
      .loadModel(BASE + body("y extend { where: org = 1; except: cat }"))
      .loadQuery(query);
   return { live, routed };
}

async function declines(
   author: QueryMaterializer,
   served: QueryMaterializer,
): Promise<boolean> {
   try {
      await assertSameCompositeMember(author, served);
      return false;
   } catch {
      return true;
   }
}

describe("assertSameCompositeMember", () => {
   it("declines a query whose own source resolves to another member", async () => {
      const { live, routed } = compiles(
         (first) => `source: c is compose(${first}, y)\n`,
         "run: c -> { group_by: cat }",
      );
      expect(await declines(live, routed)).toBe(true);
      expect(await declines(live, live)).toBe(false);
   });

   it("declines a composite reached through a join that resolves to another member", async () => {
      const { live, routed } = compiles(
         (first) =>
            `source: c is compose(${first}, y)\nsource: p is y extend { join_one: c on d = c.d }\n`,
         "run: p -> { group_by: c.cat }",
      );
      expect(await declines(live, routed)).toBe(true);
      expect(await declines(live, live)).toBe(false);
   });

   it("declines a composite inside a query-shaped source that resolved to another member", async () => {
      // The query reading `qs` resolves no composite of its own: the member
      // was chosen when `qs` compiled, and is read from its own query.
      const { live, routed } = compiles(
         (first) => `source: qs is compose(${first}, y) -> { select: cat }\n`,
         "run: qs -> { select: cat }",
      );
      expect(await declines(live, routed)).toBe(true);
      expect(await declines(live, live)).toBe(false);
   });

   it("keeps a query that reaches no composite, or only a join it does not use", async () => {
      const { live } = compiles(
         (first) =>
            `source: c is compose(${first}, y)\nsource: p is y extend { join_one: c on d = c.d }\n`,
         "run: p -> { group_by: org }",
      );
      expect(await declines(live, live)).toBe(false);
      const plain = runtime
         .loadModel(BASE)
         .loadQuery("run: y -> { group_by: cat }");
      expect(await declines(plain, plain)).toBe(false);
   });
});
