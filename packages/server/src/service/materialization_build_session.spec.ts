import { describe, expect, it } from "bun:test";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { BadRequestError } from "../errors";
import type { components } from "../api";
import {
   assertStorageServeShapeCompiles,
   buildDownstreamIntoStorage,
   buildSourceIntoStorage,
   dropStorageTable,
   dropStorageTableSql,
   logicalViewSql,
   passthroughSourceType,
   wrapPassthrough,
} from "./materialization_build_session";
import {
   buildChainedStorageBuildModel,
   buildVirtualMap,
   type ServeBinding,
} from "./materialization_serve_transform";

type ApiConnection = components["schemas"]["Connection"];

describe("assertStorageServeShapeCompiles (build-time servability gate)", () => {
   it("passes for a well-formed captured schema (compiles the serve shape in DuckDB)", async () => {
      await expect(
         assertStorageServeShapeCompiles({
            destinationName: "lake",
            sourceName: "daily_orders",
            virtualHandle: "se_1",
            physicalTableName: "daily_orders__mabc123",
            schema: [
               { name: "order_date", type: "DATE" },
               { name: "order_count", type: "BIGINT" },
               { name: "total_amount", type: "DOUBLE" },
            ],
         }),
      ).resolves.toBeUndefined();
   });
});

describe("dropStorageTableSql", () => {
   it("drops the content-addressed table, catalog-qualified and quoted", () => {
      expect(dropStorageTableSql("lake", "daily__mabc123")).toBe(
         'DROP TABLE IF EXISTS "lake"."daily__mabc123"',
      );
   });
   it("qualifies a dotted schema.table name", () => {
      expect(dropStorageTableSql("lake", "analytics.daily__mabc123")).toBe(
         'DROP TABLE IF EXISTS "lake"."analytics"."daily__mabc123"',
      );
   });
});

describe("dropStorageTable gating (no session I/O before the gate)", () => {
   it("refuses a non-DuckDB-family destination before creating a session", async () => {
      await expect(
         dropStorageTable({
            destinationName: "bq_dest",
            destinationConnection: {
               name: "bq_dest",
               type: "bigquery",
            } as ApiConnection,
            physicalTableName: "t__mabc",
            environmentPath: "/tmp/env",
         }),
      ).rejects.toThrow(/must be one of/i);
   });
});

describe("logicalViewSql", () => {
   it("points the logical name at the physical table, catalog-qualified and quoted", () => {
      expect(logicalViewSql("lake", "daily", "daily__mabc123")).toBe(
         'CREATE OR REPLACE VIEW "lake"."daily" AS SELECT * FROM "lake"."daily__mabc123"',
      );
   });

   it("qualifies a dotted schema.table logical name", () => {
      expect(
         logicalViewSql("lake", "analytics.daily", "analytics.daily__mabc123"),
      ).toBe(
         'CREATE OR REPLACE VIEW "lake"."analytics"."daily" AS SELECT * FROM "lake"."analytics"."daily__mabc123"',
      );
   });
});

describe("wrapPassthrough", () => {
   const SQL = "SELECT a, b FROM t WHERE s = 'x'";

   it("bigquery: bigquery_query(handle, sql)", () => {
      expect(wrapPassthrough("bigquery", "my-project", SQL)).toBe(
         "SELECT * FROM bigquery_query('my-project', 'SELECT a, b FROM t WHERE s = ''x''')",
      );
   });

   it("postgres: postgres_query(handle, sql)", () => {
      expect(wrapPassthrough("postgres", "src_pg", SQL)).toBe(
         "SELECT * FROM postgres_query('src_pg', 'SELECT a, b FROM t WHERE s = ''x''')",
      );
   });

   it("snowflake: snowflake_query(sql, handle) — SQL FIRST", () => {
      expect(wrapPassthrough("snowflake", "secret_snowflake_src", SQL)).toBe(
         "SELECT * FROM snowflake_query('SELECT a, b FROM t WHERE s = ''x''', 'secret_snowflake_src')",
      );
   });

   it("escapes single quotes in both the handle and the inner SQL", () => {
      expect(wrapPassthrough("postgres", "a'b", "x'y")).toBe(
         "SELECT * FROM postgres_query('a''b', 'x''y')",
      );
   });
});

describe("passthroughSourceType", () => {
   it.each(["bigquery", "snowflake", "postgres"])("accepts %s", (type) => {
      expect(
         passthroughSourceType({ name: "s", type } as ApiConnection) as string,
      ).toBe(type);
   });

   it.each(["duckdb", "ducklake", "mysql", "trino"])(
      "refuses %s with a clean 422",
      (type) => {
         expect(() =>
            passthroughSourceType({ name: "s", type } as ApiConnection),
         ).toThrow(BadRequestError);
      },
   );
});

describe("buildSourceIntoStorage gating (no session I/O before the gates)", () => {
   const okSource = { name: "wh", type: "postgres" } as ApiConnection;

   it("refuses a non-DuckDB-family destination before creating a session", async () => {
      await expect(
         buildSourceIntoStorage({
            destinationName: "bq_dest",
            destinationConnection: {
               name: "bq_dest",
               type: "bigquery",
            } as ApiConnection,
            sourceConnection: okSource,
            buildSQL: "SELECT 1",
            physicalTableName: "t",
            environmentPath: "/tmp/env",
         }),
      ).rejects.toThrow(/must be one of/i);
   });

   it("refuses an unsupported source type before creating a session", async () => {
      await expect(
         buildSourceIntoStorage({
            destinationName: "lake",
            destinationConnection: {
               name: "lake",
               type: "duckdb",
            } as ApiConnection,
            sourceConnection: {
               name: "md",
               type: "motherduck",
            } as ApiConnection,
            buildSQL: "SELECT 1",
            physicalTableName: "t",
            environmentPath: "/tmp/env",
         }),
      ).rejects.toThrow(/query-passthrough build supports/i);
   });
});

// Skipped on Windows: these tests seed the parent table via one DuckDBConnection
// to a plain-DuckDB *file*, then buildDownstreamIntoStorage ATTACHes the SAME
// file in the same process. On Windows, DuckDB keeps an exclusive/cached handle
// on a file database even after close(), so the re-open fails ("file is already
// open in this process") — a same-process file-reopen limitation of the dev-only
// duckdb-file destination (production materializes into DuckLake, a Postgres
// catalog, with no file reopen). POSIX releases the handle promptly. The Tier-3
// rebind mechanism's cross-platform correctness is pinned in-memory (no file) by
// the "runs the downstream over the parent's stored rows" test in
// materialization_serve_transform.spec.ts.
describe.skipIf(process.platform === "win32")(
   "buildDownstreamIntoStorage (Tier 3 stack-on-parent, real DuckDB)",
   () => {
      const parent: ServeBinding = {
         sourceName: "daily_orders",
         connectionName: "lake",
         virtualHandle: "daily_h",
         tablePath: "lake.daily_orders__mabc",
         schema: [
            { name: "order_date", type: "DATE" },
            { name: "total", type: "DOUBLE" },
         ],
      };
      const downstreamDefText =
         "monthly_orders is daily_orders -> {\n" +
         "  group_by: order_month is order_date.month\n" +
         "  aggregate: monthly_total is total.sum()\n}";

      /** Seed a plain-DuckDB "lake" file with the parent's content-addressed table. */
      async function seedLake(dir: string): Promise<void> {
         const seed = new DuckDBConnection("lake", join(dir, "lake.duckdb"));
         try {
            await seed.runSQL(
               'CREATE TABLE "daily_orders__mabc" AS ' +
                  "SELECT CAST('2026-01-01' AS DATE) AS order_date, 150.0 AS total " +
                  "UNION ALL SELECT CAST('2026-01-02' AS DATE), 225.0 " +
                  "UNION ALL SELECT CAST('2026-02-01' AS DATE), 99.0",
            );
         } finally {
            await seed.close();
         }
      }

      it("compiles the rebind model, runs the downstream over the parent, and captures the rolled-up schema", async () => {
         // Drives the full build-session path against a real plain-DuckDB file
         // destination: attach read-write → compile the transient rebind model →
         // getSQL over the rebound parent → CTAS into the lake → DESCRIBE. A clean
         // return proves the downstream's SQL compiled AND executed against the
         // attached parent table, and the captured schema is exactly the roll-up's
         // output columns (the parent's raw columns are gone — it aggregated them).
         // Row VALUES are pinned in-memory in the serve-transform spec (a file
         // reopened by a second DuckDB instance is not reliably visible under load;
         // production serves via a transactional catalog, not a file reopen).
         const dir = mkdtempSync(join(tmpdir(), "t3-build-"));
         try {
            await seedLake(dir);
            const result = await buildDownstreamIntoStorage({
               destinationName: "lake",
               destinationConnection: {
                  name: "lake",
                  type: "duckdb",
               } as ApiConnection,
               transientModel: buildChainedStorageBuildModel({
                  upstreams: [parent],
                  downstreamName: "monthly_orders",
                  downstreamDefText,
                  destinationName: "lake",
               }),
               downstreamName: "monthly_orders",
               virtualMap: buildVirtualMap([parent]),
               physicalTableName: "monthly_orders__mdef456",
               logicalViewName: "monthly_orders",
               environmentPath: dir,
            });

            expect(result.storageConnectionName).toBe("lake");
            expect(result.schema.map((c) => c.name).sort()).toEqual([
               "monthly_total",
               "order_month",
            ]);
         } finally {
            rmSync(dir, { recursive: true, force: true });
         }
      });

      it("throws (⇒ caller falls back) when the downstream references a source the rebind model does not provide", async () => {
         const dir = mkdtempSync(join(tmpdir(), "t3-miss-"));
         try {
            await seedLake(dir);
            await expect(
               buildDownstreamIntoStorage({
                  destinationName: "lake",
                  destinationConnection: {
                     name: "lake",
                     type: "duckdb",
                  } as ApiConnection,
                  // The downstream reads `weekly_orders`, which is NOT rebound here —
                  // the transient model can't compile, mirroring the mixed-engine /
                  // uncarried-parent case that must fall back to recompute-from-raw.
                  transientModel: buildChainedStorageBuildModel({
                     upstreams: [parent],
                     downstreamName: "monthly_orders",
                     downstreamDefText:
                        "monthly_orders is weekly_orders -> { group_by: x is 1 }",
                     destinationName: "lake",
                  }),
                  downstreamName: "monthly_orders",
                  virtualMap: buildVirtualMap([parent]),
                  physicalTableName: "monthly_orders__mdef456",
                  environmentPath: dir,
               }),
            ).rejects.toThrow();
         } finally {
            rmSync(dir, { recursive: true, force: true });
         }
      });
   },
);
