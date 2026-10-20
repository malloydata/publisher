// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Real-compiler contract for the same-catalog build's author-SQL check: the IR
// shapes it reads (`type: 'sql_select'`, `node: 'genericSQLExpr'`) are pinned
// against the compiler, so a Malloy change to either must fail here rather than
// let author SQL onto a read-write lake session.
import { DuckDBConnection } from "@malloydata/db-duckdb";
import type { PersistSource } from "@malloydata/malloy";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
} from "@malloydata/malloy";
import { beforeAll, describe, expect, it } from "bun:test";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { components } from "../api";
import { plannedSources } from "./build_plan";
import { carriesAuthorSql, isSameCatalog } from "./same_catalog_build";

type ApiConnection = components["schemas"]["Connection"];

const ROOT = "file:///same/";
let connections: FixedConnectionMap;
let csvPath = "";

beforeAll(async () => {
   const dir = mkdtempSync(join(tmpdir(), "same-catalog-"));
   const lake = new DuckDBConnection("lake", join(dir, "lake.duckdb"));
   await lake.runSQL("CREATE TABLE orders AS SELECT 1 AS id, 10 AS amount");
   const file = join(dir, "outside.csv");
   writeFileSync(file, "id,amount\n1,10\n");
   // Forward slashes: the path goes inside a Malloy string, where a Windows
   // backslash is an escape, and DuckDB reads either separator.
   csvPath = file.replaceAll("\\", "/");
   connections = new FixedConnectionMap(new Map([["lake", lake]]), "lake");
});

async function persisted(model: string): Promise<PersistSource> {
   const urlReader = new InMemoryURLReader(
      new Map([[`${ROOT}m.malloy`, model]]),
   );
   const runtime = new Runtime({ urlReader, connections });
   const compiled = await runtime
      .loadModel(new URL(`${ROOT}m.malloy`), { importBaseURL: new URL(ROOT) })
      .getModel();
   const sources = plannedSources(await runtime.getBuildTargets(compiled));
   expect(sources.length).toBe(1);
   return sources[0];
}

describe("carriesAuthorSql against the real compiler", () => {
   it("is false for a source Malloy writes all of", async () => {
      const s = await persisted(`##! experimental.persistence
source: orders is lake.table('orders')
#@ persist name="daily"
source: daily is orders -> { group_by: id; aggregate: total is amount.sum() }`);
      expect(carriesAuthorSql(s._sourceDef)).toBe(false);
   });

   it("is true for an .sql() source", async () => {
      const s = await persisted(`##! experimental.persistence
source: raw is lake.sql("SELECT * FROM orders")
#@ persist name="daily"
source: daily is raw -> { group_by: id }`);
      expect(carriesAuthorSql(s._sourceDef)).toBe(true);
   });

   it("is true for a sql_number() expression", async () => {
      const s = await persisted(`##! experimental { persistence sql_functions }
source: orders is lake.table('orders') extend {
   dimension: doubled is sql_number("\${amount} * 2")
}
#@ persist name="daily"
source: daily is orders -> { group_by: doubled }`);
      expect(carriesAuthorSql(s._sourceDef)).toBe(true);
   });

   it("is true through a join to an .sql() source", async () => {
      const s = await persisted(`##! experimental.persistence
source: extra is lake.sql("SELECT 1 AS id, 'x' AS tag")
source: orders is lake.table('orders') extend {
   join_one: extra on id = extra.id
}
#@ persist name="daily"
source: daily is orders -> { group_by: extra.tag }`);
      expect(carriesAuthorSql(s._sourceDef)).toBe(true);
   });

   it("is true for a table read from a file or URL, false for a catalog table path", async () => {
      const fromFile = await persisted(`##! experimental.persistence
source: orders is lake.table('${csvPath}')
#@ persist name="daily"
source: daily is orders -> { group_by: id }`);
      expect(carriesAuthorSql(fromFile._sourceDef)).toBe(true);
      const qualified = await persisted(`##! experimental.persistence
source: orders is lake.table('main.orders')
#@ persist name="daily"
source: daily is orders -> { group_by: id }`);
      expect(carriesAuthorSql(qualified._sourceDef)).toBe(false);
   });

   it("fails closed on an unreadable definition", () => {
      expect(carriesAuthorSql(undefined)).toBe(true);
      expect(carriesAuthorSql("text")).toBe(true);
   });

   it("fails closed on a definition deeper than the walk", () => {
      let deep: Record<string, unknown> = {
         type: "table",
         tablePath: "orders",
      };
      for (let i = 0; i < 250; i++) deep = { inner: deep };
      expect(carriesAuthorSql(deep)).toBe(true);
   });
});

describe("isSameCatalog decides by catalog identity, never by name", () => {
   const lake = (
      name: string,
      over: {
         host?: string;
         port?: number;
         db?: string;
         schema?: string;
         bucket?: string;
      } = {},
   ): ApiConnection =>
      ({
         name,
         type: "ducklake",
         ducklakeConnection: {
            storage: { bucketUrl: over.bucket ?? "gs://b/ds_x" },
            catalog: {
               postgresConnection: {
                  host: over.host ?? "catalog",
                  port: over.port,
                  databaseName: over.db ?? "ducklake",
                  userName: "u",
               },
               metadataSchema: over.schema ?? "ds_x",
            },
         },
      }) as ApiConnection;

   it("is the same catalog under two names", () => {
      expect(isSameCatalog(lake("sales"), lake("analytics"))).toBe(true);
      expect(
         isSameCatalog(
            lake("a"),
            lake("a", { bucket: "gs://b/ds_x/", port: 5432 }),
         ),
      ).toBe(true);
   });

   it("is the same catalog when either config leaves the data path to the catalog", () => {
      const noPath = (name: string): ApiConnection => {
         const c = lake(name);
         delete c.ducklakeConnection!.storage;
         return c;
      };
      expect(isSameCatalog(noPath("sales"), lake("analytics"))).toBe(true);
      expect(isSameCatalog(lake("sales"), noPath("analytics"))).toBe(true);
      expect(isSameCatalog(noPath("sales"), noPath("analytics"))).toBe(true);
      expect(
         isSameCatalog(noPath("sales"), lake("other", { schema: "ds_y" })),
      ).toBe(false);
   });

   it("is not the same catalog under one name", () => {
      expect(
         isSameCatalog(lake("sales"), lake("sales", { schema: "ds_y" })),
      ).toBe(false);
      expect(
         isSameCatalog(lake("sales"), lake("sales", { bucket: "gs://b/ds_y" })),
      ).toBe(false);
      expect(
         isSameCatalog(lake("sales"), lake("sales", { host: "other" })),
      ).toBe(false);
      expect(isSameCatalog(lake("sales"), lake("sales", { port: 6543 }))).toBe(
         false,
      );
      expect(isSameCatalog(lake("sales"), lake("sales", { db: "other" }))).toBe(
         false,
      );
   });

   it("never matches a catalog given as a connectionString", () => {
      const byString = (name: string): ApiConnection => {
         const c = lake(name);
         c.ducklakeConnection!.catalog.postgresConnection = {
            connectionString: "postgresql://u@catalog:5432/ducklake",
         };
         return c;
      };
      expect(isSameCatalog(byString("sales"), lake("analytics"))).toBe(false);
      expect(isSameCatalog(byString("sales"), byString("analytics"))).toBe(
         false,
      );
   });

   it("is never true for a non-DuckLake side", () => {
      expect(
         isSameCatalog(
            { name: "sales", type: "postgres" } as ApiConnection,
            lake("sales"),
         ),
      ).toBe(false);
      expect(isSameCatalog(lake("sales"), undefined)).toBe(false);
   });
});
