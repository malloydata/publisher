// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * MCP `execute_query` against a grid report's own source, through the real
 * tool handler and a real loaded Package. A source the report declares on top
 * of a served source runs by name when the call is addressed to the report,
 * exactly as it does on the HTTP query route (query_boundary.spec.ts pins the
 * full rule there); one built on an unserved source is refused with a fix that
 * names that base.
 */
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import {
   PackageLoadPool,
   __setPackageLoadPoolForTests,
} from "../../package_load/package_load_pool";
import type { EnvironmentStore } from "../../service/environment_store";
import { Package } from "../../service/package";
import { registerExecuteQueryTool } from "./execute_query_tool";

const REPORT = "models/flights_editable.malloy";
const ORDERS_REPORT = "models/orders_report.malloy";
const ORIGINAL_ENV = process.env.PACKAGE_LOAD_WORKERS;

type Handler = (params: Record<string, unknown>) => Promise<{
   isError?: boolean;
   content: Array<{ resource?: { text: string } }>;
}>;

describe("execute_query on a grid report's own source", () => {
   let tempDir: string;
   let handler: Handler;
   let duckdb: { close: () => Promise<void> };

   beforeAll(async () => {
      process.env.PACKAGE_LOAD_WORKERS = "1";
      await __setPackageLoadPoolForTests(new PackageLoadPool(1));
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "publisher-mcp-rs-"));
      fs.writeFileSync(
         path.join(tempDir, "publisher.json"),
         JSON.stringify({ name: "pkg", description: "test package" }),
      );
      fs.writeFileSync(
         path.join(tempDir, "flights.malloy"),
         `source: flights is duckdb.sql("""
  select 'AA' as carrier_code, 10 as dep_delay
  union all select 'UA', 5
""") extend {
  measure: flight_count is count()
}`,
      );
      fs.writeFileSync(
         path.join(tempDir, "hidden.malloy"),
         `source: flights_raw is duckdb.sql("select 1 as id")`,
      );
      fs.writeFileSync(
         path.join(tempDir, "index.malloy"),
         `##! experimental.givens
import { flights } from "flights.malloy"
import "orders.malloy"
export { flights, orders }`,
      );
      // A served base with a row filter bound to the caller's GROUPS.
      fs.writeFileSync(
         path.join(tempDir, "orders.malloy"),
         `##! experimental.givens
given: GROUPS :: number[]

#(access_filter) org_id in $GROUPS
source: orders is duckdb.sql("""
  select 1 as id, 1 as org_id, 10 as amt
  union all select 2, 1, 20
  union all select 3, 2, 30
  union all select 4, 2, 40
""") extend {
  measure: c is count()
  measure: total is amt.sum()
}`,
      );
      fs.mkdirSync(path.join(tempDir, "models"));
      fs.writeFileSync(
         path.join(tempDir, REPORT),
         `## artifact { layout=grid title="Editable Charts" tiles=["r -> kpis_r"] }
import { flights } from '../flights.malloy'
import { flights_raw } from '../hidden.malloy'
source: r is flights extend {
  # big_value
  view: kpis_r is { aggregate: flight_count }
}
source: raw_r is flights_raw extend {
  view: rv is { aggregate: c is count() }
}`,
      );

      fs.writeFileSync(
         path.join(tempDir, ORDERS_REPORT),
         `##! experimental.givens
## artifact { layout=grid title="Orders" tiles=["mine -> by_org"] }
import '../orders.malloy'
source: mine is orders extend {
  where: org_id = 2 or org_id = 1
  view: by_org is { group_by: org_id; aggregate: c, total; order_by: org_id }
  view: wide is { where: org_id = 2 or org_id > 0; group_by: org_id; aggregate: c; order_by: org_id }
}`,
      );

      const { MalloyConfig, FixedConnectionMap } = await import(
         "@malloydata/malloy"
      );
      const { DuckDBConnection } = await import("@malloydata/db-duckdb");
      const connection = new DuckDBConnection("duckdb", ":memory:");
      duckdb = connection;
      const malloyConfig = new MalloyConfig({ connections: {} });
      malloyConfig.wrapConnections(
         () =>
            new FixedConnectionMap(new Map([["duckdb", connection]]), "duckdb"),
      );
      const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
      const store: Partial<EnvironmentStore> = {
         getEnvironment: async () =>
            ({
               assertCanAdmitQuery: () => undefined,
               getApiConnection: () => {
                  throw new Error("no connection config in this test");
               },
               getPackage: async () => pkg,
            }) as never,
      };
      const fakeServer = {
         tool: (_name: string, _desc: string, _shape: unknown, h: Handler) => {
            handler = h;
         },
      };
      registerExecuteQueryTool(fakeServer as never, store as EnvironmentStore);
   });

   afterAll(async () => {
      await duckdb.close();
      await __setPackageLoadPoolForTests(null);
      if (ORIGINAL_ENV === undefined) delete process.env.PACKAGE_LOAD_WORKERS;
      else process.env.PACKAGE_LOAD_WORKERS = ORIGINAL_ENV;
      fs.rmSync(tempDir, { recursive: true, force: true });
   });

   const call = (args: Record<string, unknown>) =>
      handler({
         environmentName: "env",
         packageName: "pkg",
         modelPath: REPORT,
         ...args,
      });
   const payload = (result: Awaited<ReturnType<Handler>>) =>
      JSON.parse(result.content[0].resource!.text);

   it("runs the report's source and view by name", async () => {
      const result = await call({ sourceName: "r", queryName: "kpis_r" });
      expect(result.isError).not.toBe(true);
      expect(payload(result).rows).toEqual([{ flight_count: 2 }]);
   });

   it("refuses a report source on an unserved base, naming the base", async () => {
      const result = await call({ sourceName: "raw_r", queryName: "rv" });
      expect(result.isError).toBe(true);
      const error = JSON.stringify(payload(result));
      expect(error).toContain(
         `Fix: name \\"flights_raw\\" in the export { ... } of \\"index.malloy\\"`,
      );
      expect(error).not.toContain(`name the source in the export`);
   });

   it("holds a report source over an #(access_filter) base to the caller's rows", async () => {
      const org1 = { GROUPS: [1] };
      const rows = async (args: Record<string, unknown>) => {
         const result = await call(args);
         expect(result.isError).not.toBe(true);
         return payload(result).rows;
      };
      // By name through the report, and the base itself: the same org-1 rows.
      expect(
         await rows({
            modelPath: ORDERS_REPORT,
            sourceName: "mine",
            queryName: "by_org",
            givens: org1,
         }),
      ).toEqual([{ org_id: 1, c: 2, total: 30 }]);
      expect(
         await rows({
            modelPath: "index.malloy",
            query: "run: orders -> { group_by: org_id; aggregate: c, total; order_by: org_id }",
            givens: org1,
         }),
      ).toEqual([{ org_id: 1, c: 2, total: 30 }]);
      // Neither a view's `where:` nor the caller's widens it.
      expect(
         await rows({
            modelPath: ORDERS_REPORT,
            sourceName: "mine",
            queryName: "wide",
            givens: org1,
         }),
      ).toEqual([{ org_id: 1, c: 2 }]);
      expect(
         await rows({
            modelPath: ORDERS_REPORT,
            query: "run: mine -> { where: org_id = 2 or true; group_by: org_id; aggregate: c; order_by: org_id }",
            givens: org1,
         }),
      ).toEqual([{ org_id: 1, c: 2 }]);
      // No givens: refused, as the base is.
      const base = await call({
         modelPath: "index.malloy",
         query: "run: orders -> { aggregate: c }",
      });
      const viaReport = await call({
         modelPath: ORDERS_REPORT,
         sourceName: "mine",
         queryName: "by_org",
      });
      expect(base.isError).toBe(true);
      expect(viaReport.isError).toBe(true);
      expect(payload(base).error).toContain(
         'Access denied for source "orders"',
      );
      expect(payload(viaReport).error).toContain(
         'Access denied for source "mine"',
      );
   });
});
