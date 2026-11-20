// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Real-compiler contract for the plan's `reads`: the IR shapes `sourceReads`
// walks (`table`, `query_source`, a stage's `activeJoins`, `sql_select`) are
// pinned against the compiler, so a Malloy change to any of them fails here
// rather than leave a host skipping a rebuild it needed.
import { DuckDBConnection } from "@malloydata/db-duckdb";
import type { PersistSource } from "@malloydata/malloy";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
} from "@malloydata/malloy";
import { beforeAll, describe, expect, it } from "bun:test";
import { plannedSources } from "../../materialization/plan/build_plan";
import { sourceReads } from "./source_reads";

const ROOT = "file:///reads/";
let connections: FixedConnectionMap;

beforeAll(async () => {
   const lake = new DuckDBConnection("lake", ":memory:");
   await lake.runSQL(
      "CREATE TABLE orders AS SELECT 1 AS id, 10 AS amount, 1 AS cid",
   );
   await lake.runSQL("CREATE TABLE customers AS SELECT 1 AS id, 'a' AS name");
   await lake.runSQL("CREATE TABLE regions AS SELECT 1 AS id, 'w' AS label");
   connections = new FixedConnectionMap(new Map([["lake", lake]]), "lake");
});

const BASE = `##! experimental.persistence
source: customers is lake.table('customers')
source: regions is lake.table('regions')
source: orders is lake.table('orders') extend {
   join_one: c is customers on cid = c.id
   join_one: r is regions on cid = r.id
}
`;

/** The planned persist source named `name` in `model`. */
async function persisted(model: string, name: string): Promise<PersistSource> {
   const urlReader = new InMemoryURLReader(
      new Map([[`${ROOT}m.malloy`, model]]),
   );
   const runtime = new Runtime({ urlReader, connections });
   const compiled = await runtime
      .loadModel(new URL(`${ROOT}m.malloy`), { importBaseURL: new URL(ROOT) })
      .getModel();
   const found = plannedSources(await runtime.getBuildTargets(compiled)).find(
      (s) => s.name === name,
   );
   expect(found).toBeDefined();
   return found!;
}

const read = (table: string) => ({ connectionName: "lake", table });

describe("sourceReads against the real compiler", () => {
   it("lists the table a query over a table source reads", async () => {
      const s = await persisted(
         `${BASE}#@ persist name="daily"
source: daily is orders -> { group_by: id; aggregate: total is amount.sum() }`,
         "daily",
      );
      expect(sourceReads(s)).toEqual([read("orders")]);
   });

   it("lists the table a query reads straight from connection.table()", async () => {
      const s = await persisted(
         `##! experimental.persistence
#@ persist name="raw"
source: raw is lake.table('orders') -> { select: id, amount }`,
         "raw",
      );
      expect(sourceReads(s)).toEqual([read("orders")]);
   });

   it("lists a join the build uses, and not one it leaves out", async () => {
      const s = await persisted(
         `${BASE}#@ persist name="by_customer"
source: by_customer is orders -> {
   group_by: c.name
   aggregate: total is amount.sum()
}`,
         "by_customer",
      );
      // `r` is declared on orders and read by no field of the build.
      expect(sourceReads(s)).toEqual([read("customers"), read("orders")]);
   });

   it("lists a join a stage's where: reaches", async () => {
      const s = await persisted(
         `${BASE}#@ persist name="west"
source: west is orders -> { where: r.label = 'w'; select: id }`,
         "west",
      );
      expect(sourceReads(s)).toEqual([read("orders"), read("regions")]);
   });

   it("lists a join a stage declares in its own extend:", async () => {
      const s = await persisted(
         `${BASE}#@ persist name="named"
source: named is lake.table('orders') -> {
   extend: { join_one: k is customers on cid = k.id }
   group_by: k.name
}`,
         "named",
      );
      expect(sourceReads(s)).toEqual([read("customers"), read("orders")]);
   });

   it("lists what a persisted upstream reads, as the inlined sql does", async () => {
      const s = await persisted(
         `${BASE}#@ persist name="daily"
source: daily is orders -> { group_by: c.name; aggregate: total is amount.sum() }
#@ persist name="top"
source: top is daily -> { select: name; limit: 1 }`,
         "top",
      );
      expect(sourceReads(s)).toEqual([read("customers"), read("orders")]);
   });

   it("is unknown for an .sql() source: its text names tables Malloy does not parse", async () => {
      const s = await persisted(
         `##! experimental.persistence
#@ persist name="via_sql"
source: via_sql is lake.sql("""SELECT o.id FROM orders o JOIN customers c ON o.cid = c.id""")`,
         "via_sql",
      );
      expect(sourceReads(s)).toBeUndefined();
   });

   it("is unknown for a query over an .sql() source", async () => {
      const s = await persisted(
         `##! experimental.persistence
source: q is lake.sql("""SELECT * FROM orders""")
#@ persist name="over_sql"
source: over_sql is q -> { group_by: id }`,
         "over_sql",
      );
      expect(sourceReads(s)).toBeUndefined();
   });

   it("is unknown when a raw SQL expression may hold a subquery", async () => {
      const s = await persisted(
         `##! experimental.sql_functions
${BASE}#@ persist name="raw_expr"
source: raw_expr is orders -> {
   group_by: n is sql_number("(SELECT count(*) FROM regions)")
}`,
         "raw_expr",
      );
      expect(sourceReads(s)).toBeUndefined();
   });

   it("is not unknown for a built-in function's own SQL", async () => {
      const s = await persisted(
         `${BASE}#@ persist name="rounded"
source: rounded is orders -> { group_by: r2 is round(amount / 3, 1) }`,
         "rounded",
      );
      expect(sourceReads(s)).toEqual([read("orders")]);
   });

   it("is unknown for a definition it cannot read", () => {
      expect(sourceReads({ _sourceDef: undefined })).toBeUndefined();
      expect(sourceReads({ _sourceDef: { type: "virtual" } })).toBeUndefined();
      expect(
         sourceReads({
            _sourceDef: {
               type: "query_source",
               query: { structRef: "orders", pipeline: [] },
            },
         }),
      ).toBeUndefined();
   });
});
