// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { BuildCancelledError } from "../domain/build_cancelled";
import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import { DuckDBInstance } from "@duckdb/node-api";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { spawn, spawnSync } from "node:child_process";
import {
   existsSync,
   mkdirSync,
   mkdtempSync,
   readdirSync,
   rmSync,
   utimesSync,
   writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import {
   BadRequestError,
   MaterializationEligibilityError,
} from "../../../errors";
import { storageDestinationRoot } from "../../connection_config";
import { logger } from "../../../logger";
import { installDuckDBInstanceSharing } from "../../duckdb_instance_sharing";
import type { components } from "../../../api";
import {
   buildDownstreamIntoStorage,
   buildSourceIntoStorage,
   type BuildSessionDeps,
   createIsolatedBuildSession,
   type IsolatedSessionConnection,
   createTableAndDescribe,
   inTransaction,
   interruptOnAbort,
   partitionKeySql,
   partitionOrderSql,
   passthroughSourceType,
   removeStaleBuildDirs,
   wrapPassthrough,
   writePartitionedTable,
   oneFileModel,
   assertSingleSelect,
} from "./materialization_build_session";
import { applyDeltaScript } from "./incremental_apply";
import { sessionRunner } from "./incremental_storage";
import { dropStorageTable, dropStorageTableSql } from "./storage_admin";
import { identifier } from "../../malloy_text";
import { declarableColumns } from "../stored_column_types";
import { buildVirtualMap } from "../serve/bindings";
import { type Binding } from "../domain/binding";

type ApiConnection = components["schemas"]["Connection"];

/**
 * A one-file model over stored parents, for the build session's own specs:
 * each parent declared as a virtual source of its stored table (its
 * declarable columns only), then the downstream as a persist source.
 */
function buildChainedStorageBuildModel(params: {
   upstreams: Binding[];
   downstreamName: string;
   downstreamDefText: string;
   destinationName: string;
   documentFlags?: string[];
}): string {
   const parents = params.upstreams.map((b) => {
      const type = identifier(`${b.sourceName}__shape`);
      const fields = declarableColumns(b.schema)
         .declared.map((c) => `   ${identifier(c.name)}::${c.malloyType}`)
         .join(",\n");
      return (
         `type: ${type} is {\n${fields}\n}\n` +
         `source: ${identifier(b.sourceName)} is ${identifier(b.destinationName)}.virtual('${b.virtualHandle}')::${type}`
      );
   });
   return [
      ...(params.documentFlags ?? []),
      "##! experimental { persistence virtual_source }",
      ...parents,
      `#@ persist storage="${params.destinationName}"`,
      `source: ${params.downstreamDefText}`,
      "",
   ].join("\n");
}

describe("createIsolatedBuildSession (per-build instance isolation)", () => {
   // Multi-tenant safety regression guard. @malloydata/db-duckdb pools :memory:
   // DuckDB instances in a process-global cache keyed by a share key that
   // EXCLUDES the connection name, so build sessions built with identical config
   // would otherwise be pooled onto ONE shared instance and collide on their
   // transient ATTACH aliases (cross-tenant read/write). createIsolatedBuildSession
   // gives each session a unique workingDirectory so it lands its OWN instance.
   // If a library change ever re-pools them (drops workingDirectory from the
   // share key), this test fails LOUD rather than letting a silent cross-tenant
   // leak ship. No warehouse needed: a plain table proves catalog isolation.
   it("gives each session its own catalog (a table in one is invisible to another)", async () => {
      const a = createIsolatedBuildSession("build_lake");
      const b = createIsolatedBuildSession("build_lake");
      try {
         await a.session.runSQL("CREATE TABLE isolated_probe(x INT)");
         await a.session.runSQL("INSERT INTO isolated_probe VALUES (1)");
         // b must NOT see a's table — separate instances, separate catalogs.
         await expect(
            b.session.runSQL("SELECT * FROM isolated_probe"),
         ).rejects.toThrow();
      } finally {
         await a.dispose();
         await b.dispose();
      }
   });

   it("stays in-memory (dispose leaves no primary DB file) and is idempotent", async () => {
      const { session, dispose } = createIsolatedBuildSession("build_lake");
      await session.runSQL("CREATE TABLE t(x INT)");
      await dispose();
      // Second dispose must not throw (idempotent teardown).
      await dispose();
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

   it("says a DuckLake source is refused because it is a different catalog", () => {
      expect(() =>
         passthroughSourceType({
            name: "s",
            type: "ducklake",
         } as ApiConnection),
      ).toThrow("into a storage destination that is a different catalog");
   });
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

describe("buildSourceIntoStorage closes a federated source's tunnel", () => {
   // The federation tests prove `federatePostgres` RETURNS a close; this proves
   // the build session CALLS it, on both outcomes. A stubbed federate hands back
   // a spy close and a stubbed read supplies the SELECT the CTAS wraps, so the
   // whole session path runs against a real plain-DuckDB file destination with
   // no live warehouse.
   const source = { name: "wh", type: "postgres" } as ApiConnection;
   function harness(read: BuildSessionDeps["read"]) {
      let closed = 0;
      const deps: BuildSessionDeps = {
         federate: async () => ({
            handle: "wh",
            sourceType: "postgres" as const,
            close: async () => {
               closed += 1;
            },
         }),
         read,
         numericColumns: async () => ({ columns: [], numeric: [] }),
      };
      const dir = mkdtempSync(join(tmpdir(), "tunnel-close-"));
      mkdirSync(storageDestinationRoot(dir), { recursive: true });
      const params = {
         destinationName: "lake",
         destinationConnection: {
            name: "lake",
            type: "duckdb",
         } as ApiConnection,
         sourceConnection: source,
         buildSQL: "SELECT 1",
         physicalTableName: "t",
         environmentPath: dir,
         deps,
      };
      return { params, dir, closed: () => closed };
   }

   it("on a clean build, after the session is disposed", async () => {
      const h = harness(async () => ({
         selectSQL: "SELECT 1 AS x",
         jobId: null,
         cost: null,
      }));
      try {
         const result = await buildSourceIntoStorage(h.params);
         expect(result.schema.map((c) => c.name)).toEqual(["x"]);
         expect(h.closed()).toBe(1);
      } finally {
         rmSync(h.dir, { recursive: true, force: true });
      }
   });

   it("P5-10: refuses the CTAS of a run cancelled while the session was idle", async () => {
      // The abort lands during the read (the session idle meanwhile), so no
      // statement is in flight to interrupt: the next one must be refused.
      const run = new AbortController();
      const h = harness(async () => {
         run.abort();
         return { selectSQL: "SELECT 1 AS x", jobId: null, cost: null };
      });
      try {
         await expect(
            buildSourceIntoStorage({ ...h.params, signal: run.signal }),
         ).rejects.toBeInstanceOf(BuildCancelledError);
         expect(h.closed()).toBe(1);
      } finally {
         rmSync(h.dir, { recursive: true, force: true });
      }
   });

   it("P5-10: starts nothing for a run already cancelled", async () => {
      const run = new AbortController();
      run.abort();
      let read = 0;
      const h = harness(async () => {
         read += 1;
         return { selectSQL: "SELECT 1 AS x", jobId: null, cost: null };
      });
      try {
         await expect(
            buildSourceIntoStorage({ ...h.params, signal: run.signal }),
         ).rejects.toBeInstanceOf(BuildCancelledError);
         expect(read).toBe(0);
      } finally {
         rmSync(h.dir, { recursive: true, force: true });
      }
   });

   it("when the read throws, and the build's own error is the one raised", async () => {
      const h = harness(async () => {
         throw new Error("warehouse boom");
      });
      try {
         await expect(buildSourceIntoStorage(h.params)).rejects.toThrow(
            "warehouse boom",
         );
         expect(h.closed()).toBe(1);
      } finally {
         rmSync(h.dir, { recursive: true, force: true });
      }
   });
});

describe("buildSourceIntoStorage hands the warehouse its SQL verbatim", () => {
   // What is executed there, attributed there, and addressed by is the
   // compiled SQL and nothing else.
   function harness() {
      const received: string[] = [];
      const deps: BuildSessionDeps = {
         federate: async () => ({
            handle: "wh",
            sourceType: "postgres" as const,
            close: async () => {},
         }),
         read: async (_session, _type, _handle, buildSQL) => {
            received.push(buildSQL);
            return { selectSQL: "SELECT 1 AS org_id", jobId: null, cost: null };
         },
         numericColumns: async () => ({ columns: [], numeric: [] }),
      };
      const dir = mkdtempSync(join(tmpdir(), "build-verbatim-"));
      mkdirSync(storageDestinationRoot(dir), { recursive: true });
      const params = {
         destinationName: "lake",
         destinationConnection: {
            name: "lake",
            type: "duckdb",
         } as ApiConnection,
         sourceConnection: { name: "wh", type: "postgres" } as ApiConnection,
         buildSQL: "SELECT 1 AS org_id",
         physicalTableName: "t",
         environmentPath: dir,
         deps,
      };
      return { params, dir, received };
   }

   it("runs the compiled SQL unchanged", async () => {
      const h = harness();
      try {
         await buildSourceIntoStorage(h.params);
         expect(h.received).toEqual(["SELECT 1 AS org_id"]);
      } finally {
         rmSync(h.dir, { recursive: true, force: true });
      }
   });
});

// Skipped on Windows: these tests seed the parent table via one DuckDBConnection
// to a plain-DuckDB *file*, then buildDownstreamIntoStorage ATTACHes the SAME
// file in the same process. On Windows, DuckDB keeps an exclusive/cached handle
// on a file database even after close(), so the re-open fails ("file is already
// open in this process") — a same-process file-reopen limitation of the dev-only
// duckdb-file destination (production materializes into DuckLake, a Postgres
// catalog, with no file reopen). POSIX releases the handle promptly. The
// stack-on-parent rebind mechanism's cross-platform correctness is pinned in-memory (no file) by
// the "runs the downstream over the parent's stored rows" test in
// chained_storage_build.spec.ts.
describe.skipIf(process.platform === "win32")(
   "buildDownstreamIntoStorage (stack-on-parent, real DuckDB)",
   () => {
      const parent: Binding = {
         kind: "persist",
         tier: "storage",
         sourceName: "daily_orders",
         destinationName: "lake",
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

      /**
       * Run `sql` against the plain-DuckDB "lake" file, at the path the build
       * attaches: a destination's files live under the destinations root, not
       * directly in the environment directory, so they cannot collide with a
       * connection of the same name.
       *
       * Through node-api with both handles closed synchronously, not through
       * db-duckdb: its `close()` only drops a refcount, so the instance lives on
       * until GC finalizes its connection, and its shutdown checkpoint then
       * writes its own stale catalog over whatever a later instance committed
       * to the same file (a schema created after the seed vanished under CI load).
       */
      async function writeLake(dir: string, sql: string): Promise<void> {
         const root = storageDestinationRoot(dir);
         mkdirSync(root, { recursive: true });
         const instance = await DuckDBInstance.create(
            join(root, "lake.duckdb"),
         );
         const connection = await instance.connect();
         try {
            await connection.run(sql);
         } finally {
            connection.closeSync();
            instance.closeSync();
         }
      }

      /** Seed the lake with the parent's content-addressed table. */
      function seedLake(dir: string): Promise<void> {
         return writeLake(
            dir,
            'CREATE TABLE "daily_orders__mabc" AS ' +
               "SELECT CAST('2026-01-01' AS DATE) AS order_date, 150.0 AS total " +
               "UNION ALL SELECT CAST('2026-01-02' AS DATE), 225.0 " +
               "UNION ALL SELECT CAST('2026-02-01' AS DATE), 99.0",
         );
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
               model: oneFileModel(
                  buildChainedStorageBuildModel({
                     upstreams: [parent],
                     downstreamName: "monthly_orders",
                     downstreamDefText,
                     destinationName: "lake",
                  }),
               ),
               downstreamName: "monthly_orders",
               virtualMap: buildVirtualMap([parent]),
               physicalTableName: "monthly_orders__mdef456",
               environmentPath: dir,
            });

            expect(result.storageDestinationName).toBe("lake");
            expect(result.schema.map((c) => c.name).sort()).toEqual([
               "monthly_total",
               "order_month",
            ]);
            // It read the parent's stored rows, so no warehouse was charged and
            // the entry reports the reuse.
            expect(result.readCost).toBeNull();
            expect(result.upstreamReuse).toBe("reused");
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
                  model: oneFileModel(
                     buildChainedStorageBuildModel({
                        upstreams: [parent],
                        downstreamName: "monthly_orders",
                        downstreamDefText:
                           "monthly_orders is weekly_orders -> { group_by: x is 1 }",
                        destinationName: "lake",
                     }),
                  ),
                  downstreamName: "monthly_orders",
                  virtualMap: buildVirtualMap([parent]),
                  physicalTableName: "monthly_orders__mdef456",
                  environmentPath: dir,
               }),
               // The shape failure the caller falls back on, by type.
            ).rejects.toBeInstanceOf(MaterializationEligibilityError);
         } finally {
            rmSync(dir, { recursive: true, force: true });
         }
      });

      it("refuses a downstream Malloy cannot plan over its parents, naming it, so the caller falls back", async () => {
         const dir = mkdtempSync(join(tmpdir(), "t3-unplannable-"));
         try {
            await seedLake(dir);
            const error = await buildDownstreamIntoStorage({
               destinationName: "lake",
               destinationConnection: {
                  name: "lake",
                  type: "duckdb",
               } as ApiConnection,
               model: oneFileModel(
                  buildChainedStorageBuildModel({
                     upstreams: [parent],
                     downstreamName: "monthly_orders",
                     downstreamDefText,
                     destinationName: "lake",
                  }),
               ),
               downstreamName: "monthly_orders",
               // No table for the rebound parent, so its SQL cannot be written.
               virtualMap: new Map(),
               physicalTableName: "monthly_orders__mdef456",
               environmentPath: dir,
            }).catch((err: unknown) => err);
            expect(error).toBeInstanceOf(MaterializationEligibilityError);
            expect((error as Error).message).toContain(
               "could not plan 'monthly_orders' over the rebound parents",
            );
         } finally {
            rmSync(dir, { recursive: true, force: true });
         }
      });

      it("refuses a downstream carrying author SQL as ineligible", async () => {
         const dir = mkdtempSync(join(tmpdir(), "t3-sql-"));
         try {
            await seedLake(dir);
            const error = await buildDownstreamIntoStorage({
               destinationName: "lake",
               destinationConnection: {
                  name: "lake",
                  type: "duckdb",
               } as ApiConnection,
               model: oneFileModel(
                  buildChainedStorageBuildModel({
                     upstreams: [parent],
                     downstreamName: "monthly_orders",
                     downstreamDefText:
                        "monthly_orders is daily_orders extend {\n" +
                        '  dimension: doubled is sql_number("${total} * 2")\n' +
                        "} -> { group_by: doubled }",
                     destinationName: "lake",
                     documentFlags: ["##! experimental.sql_functions"],
                  }),
               ),
               downstreamName: "monthly_orders",
               virtualMap: buildVirtualMap([parent]),
               physicalTableName: "monthly_orders__mdef456",
               environmentPath: dir,
            }).then(
               () => undefined,
               (e: unknown) => e,
            );
            expect(error).toBeInstanceOf(MaterializationEligibilityError);
            // Refused by the author-SQL check, not by a compile failure.
            expect(String(error)).toMatch(/carries SQL its author wrote/);
         } finally {
            rmSync(dir, { recursive: true, force: true });
         }
      });

      it("refuses a destinationSchema the catalog does not have, and builds into one it has", async () => {
         const dir = mkdtempSync(join(tmpdir(), "t3-schema-"));
         try {
            await seedLake(dir);
            await expect(
               buildDownstreamIntoStorage({
                  destinationName: "lake",
                  destinationConnection: {
                     name: "lake",
                     type: "duckdb",
                     ducklakeConnection: { destinationSchema: "materialzed" },
                  } as ApiConnection,
                  model: oneFileModel(
                     buildChainedStorageBuildModel({
                        upstreams: [parent],
                        downstreamName: "monthly_orders",
                        downstreamDefText,
                        destinationName: "lake",
                     }),
                  ),
                  downstreamName: "monthly_orders",
                  virtualMap: buildVirtualMap([parent]),
                  physicalTableName: "monthly_orders__mdef456",
                  environmentPath: dir,
               }),
            ).rejects.toThrow(
               "Storage destination 'lake' declares destinationSchema 'materialzed', but its catalog has no schema by that name.",
            );

            await writeLake(dir, "CREATE SCHEMA Materialized");
            const built = await buildDownstreamIntoStorage({
               destinationName: "lake",
               destinationConnection: {
                  name: "lake",
                  type: "duckdb",
                  ducklakeConnection: { destinationSchema: "materialized" },
               } as ApiConnection,
               model: oneFileModel(
                  buildChainedStorageBuildModel({
                     upstreams: [parent],
                     downstreamName: "monthly_orders",
                     downstreamDefText,
                     destinationName: "lake",
                  }),
               ),
               downstreamName: "monthly_orders",
               virtualMap: buildVirtualMap([parent]),
               physicalTableName: "monthly_orders__mdef456",
               environmentPath: dir,
            });
            expect(built.schema.length).toBe(2);
         } finally {
            rmSync(dir, { recursive: true, force: true });
         }
      });

      // ms2data/service#6642: a chained build casts to the DOWNSTREAM's own
      // declared types, as a recompute would. DuckDB answers `date + 1 day`
      // with a timestamp, which Malloy declares a date.
      it("stores the downstream's columns as the types the downstream declares", async () => {
         const dir = mkdtempSync(join(tmpdir(), "t3-types-"));
         try {
            await seedLake(dir);
            const result = await buildDownstreamIntoStorage({
               destinationName: "lake",
               destinationConnection: {
                  name: "lake",
                  type: "duckdb",
               } as ApiConnection,
               model: oneFileModel(
                  buildChainedStorageBuildModel({
                     upstreams: [parent],
                     downstreamName: "next_days",
                     downstreamDefText:
                        "next_days is daily_orders -> {\n" +
                        "  group_by: next_day is order_date + 1 day\n" +
                        "  aggregate: t is total.sum()\n}",
                     destinationName: "lake",
                  }),
               ),
               downstreamName: "next_days",
               virtualMap: buildVirtualMap([parent]),
               physicalTableName: "next_days__m1",
               environmentPath: dir,
            });
            expect(result.schema.find((c) => c.name === "next_day")?.type).toBe(
               "DATE",
            );
         } finally {
            rmSync(dir, { recursive: true, force: true });
         }
      });

      it("raises an infrastructure failure as itself, not as a shape refusal the caller would fall back on", async () => {
         // The model compiles, but the parent's stored table is not in the lake:
         // the CTAS fails against the destination, and recomputing from raw would
         // not help, so this must not be a MaterializationEligibilityError.
         const dir = mkdtempSync(join(tmpdir(), "t3-infra-"));
         try {
            mkdirSync(storageDestinationRoot(dir), { recursive: true });
            const error = await buildDownstreamIntoStorage({
               destinationName: "lake",
               destinationConnection: {
                  name: "lake",
                  type: "duckdb",
               } as ApiConnection,
               model: oneFileModel(
                  buildChainedStorageBuildModel({
                     upstreams: [parent],
                     downstreamName: "monthly_orders",
                     downstreamDefText,
                     destinationName: "lake",
                  }),
               ),
               downstreamName: "monthly_orders",
               virtualMap: buildVirtualMap([parent]),
               physicalTableName: "monthly_orders__mdef456",
               environmentPath: dir,
            }).then(
               () => undefined,
               (e: unknown) => e,
            );
            expect(error).toBeInstanceOf(Error);
            expect(error).not.toBeInstanceOf(MaterializationEligibilityError);
            expect(String(error)).toMatch(/daily_orders__mabc/);
         } finally {
            rmSync(dir, { recursive: true, force: true });
         }
      });

      // The parents' `type:` follows the served copy's declaration rule: a
      // list or struct is declared as Malloy's array or record type, and a
      // column nothing declares faithfully (a MAP here) is left out. Before,
      // every such column was declared `json`, which Malloy refuses in a
      // `type:`, so no chained build over such a parent could lift.
      describe("over a parent with list, struct and MAP columns", () => {
         const exotic: Binding = {
            ...parent,
            sourceName: "tagged_orders",
            virtualHandle: "tagged_h",
            tablePath: "lake.tagged_orders__mabc",
            schema: [
               { name: "order_date", type: "DATE" },
               { name: "total", type: "DOUBLE" },
               { name: "tags", type: "INTEGER[]" },
               { name: "who", type: "STRUCT(a INTEGER, b VARCHAR)" },
               { name: "attrs", type: "MAP(VARCHAR, INTEGER)" },
            ],
         };

         function seedExotic(dir: string): Promise<void> {
            return writeLake(
               dir,
               'CREATE TABLE "tagged_orders__mabc" AS ' +
                  "SELECT CAST('2026-01-01' AS DATE) AS order_date, 150.0 AS total, " +
                  "[1, 2]::INTEGER[] AS tags, {'a': 1, 'b': 'x'} AS who, " +
                  "MAP {'k': 1} AS attrs " +
                  "UNION ALL SELECT CAST('2026-02-01' AS DATE), 99.0, " +
                  "[3]::INTEGER[], {'a': 2, 'b': 'y'}, MAP {'k': 2}",
            );
         }

         const build = (dir: string, name: string, defText: string) =>
            buildDownstreamIntoStorage({
               destinationName: "lake",
               destinationConnection: {
                  name: "lake",
                  type: "duckdb",
               } as ApiConnection,
               model: oneFileModel(
                  buildChainedStorageBuildModel({
                     upstreams: [exotic],
                     downstreamName: name,
                     downstreamDefText: defText,
                     destinationName: "lake",
                  }),
               ),
               downstreamName: name,
               virtualMap: buildVirtualMap([exotic]),
               physicalTableName: `${name}__m1`,
               environmentPath: dir,
            });

         it("lifts a downstream over the parent's scalar columns", async () => {
            const dir = mkdtempSync(join(tmpdir(), "t3-exotic-"));
            try {
               await seedExotic(dir);
               const result = await build(
                  dir,
                  "monthly_tagged",
                  "monthly_tagged is tagged_orders -> {\n" +
                     "  group_by: order_month is order_date.month\n" +
                     "  aggregate: monthly_total is total.sum()\n}",
               );
               expect(result.upstreamReuse).toBe("reused");
               expect(result.schema.map((c) => c.name).sort()).toEqual([
                  "monthly_total",
                  "order_month",
               ]);
            } finally {
               rmSync(dir, { recursive: true, force: true });
            }
         });

         it("lifts a downstream reading the list and struct columns", async () => {
            const dir = mkdtempSync(join(tmpdir(), "t3-exotic-nested-"));
            try {
               await seedExotic(dir);
               const result = await build(
                  dir,
                  "tag_totals",
                  "tag_totals is tagged_orders -> {\n" +
                     "  group_by: order_date, wa is who.a\n" +
                     "  aggregate: tag_total is tags.each.sum()\n}",
               );
               expect(result.upstreamReuse).toBe("reused");
               expect(result.schema.map((c) => c.name).sort()).toEqual([
                  "order_date",
                  "tag_total",
                  "wa",
               ]);
            } finally {
               rmSync(dir, { recursive: true, force: true });
            }
         });

         it("refuses as ineligible a downstream reading the left-out column", async () => {
            const dir = mkdtempSync(join(tmpdir(), "t3-exotic-map-"));
            try {
               await seedExotic(dir);
               const error = await build(
                  dir,
                  "attr_rows",
                  "attr_rows is tagged_orders -> { select: order_date, attrs }",
               ).then(
                  () => undefined,
                  (e: unknown) => e,
               );
               expect(error).toBeInstanceOf(MaterializationEligibilityError);
            } finally {
               rmSync(dir, { recursive: true, force: true });
            }
         });
      });
   },
);

describe("createTableAndDescribe (schema read-back window)", () => {
   /** A session that CTASs fine and fails the read-back, recording every stmt. */
   const sessionThatFailsDescribe = (opts: { dropFails?: boolean } = {}) => {
      const issued: string[] = [];
      const session = {
         runSQL: async (sql: string) => {
            issued.push(sql);
            if (/^DESCRIBE/i.test(sql.trim())) throw new Error("describe boom");
            if (/^DROP TABLE/i.test(sql.trim()) && opts.dropFails) {
               throw new Error("drop boom");
            }
            return { rows: [] };
         },
      } as unknown as IsolatedSessionConnection;
      return { session, issued };
   };

   it("rolls back, not drops, when the schema read-back fails", async () => {
      // The physical name is stable across generations, so a DROP after the
      // CREATE OR REPLACE would delete the table the previous manifest still
      // serves. The create and read-back run in one transaction instead: a
      // failed read-back rolls back to the previous generation (or, on a first
      // build, to no table), which no reclaim has to find.
      const { session, issued } = sessionThatFailsDescribe();

      await expect(
         createTableAndDescribe(session, `"lake"."t"`, "SELECT 1"),
      ).rejects.toThrow(/describe boom/);

      expect(issued[0]).toMatch(/^BEGIN/i);
      expect(issued.some((s) => /^CREATE OR REPLACE TABLE/i.test(s))).toBe(
         true,
      );
      expect(issued.some((s) => /^ROLLBACK/i.test(s))).toBe(true);
      expect(issued.some((s) => /^DROP TABLE/i.test(s))).toBe(false);
   });

   it("surfaces the read-back failure, not a failure of the cleanup drop", async () => {
      // The DESCRIBE error is the diagnosis; a drop that also fails must not
      // replace it (the leak is then logged and still surfaced by the warning).
      const { session } = sessionThatFailsDescribe({ dropFails: true });

      await expect(
         createTableAndDescribe(session, `"lake"."t"`, "SELECT 1"),
      ).rejects.toThrow(/describe boom/);
   });

   it("issues no drop on the happy path", async () => {
      const issued: string[] = [];
      const session = {
         runSQL: async (sql: string) => {
            issued.push(sql);
            return {
               rows: [{ column_name: "a", column_type: "VARCHAR" }],
            };
         },
      } as unknown as IsolatedSessionConnection;

      const schema = await createTableAndDescribe(
         session,
         `"lake"."t"`,
         "SELECT 1",
      );

      expect(schema).toEqual([{ name: "a", type: "VARCHAR" }]);
      expect(issued.some((s) => /^DROP TABLE/i.test(s))).toBe(false);
   });
});

describe("partitionKeySql", () => {
   it.each([
      [{ column: "region", transform: "identity" }, `"region"`],
      [{ column: "day", transform: "year" }, `year("day")`],
      [{ column: "day", transform: "month" }, `month("day")`],
      [{ column: "ts", transform: "day" }, `day("ts")`],
      [{ column: "ts", transform: "hour" }, `hour("ts")`],
      [{ column: "sku", transform: "bucket(16)" }, `bucket(16, "sku")`],
      [{ column: "Mixed Case", transform: "identity" }, `"Mixed Case"`],
   ])("renders %j as %s", (key, sql) => {
      expect(partitionKeySql(key)).toBe(sql);
   });

   it.each(["upper", "bucket(x)", "bucket(16", "truncate(4)", ""])(
      "refuses the transform %j rather than writing another layout",
      (transform) => {
         expect(() => partitionKeySql({ column: "c", transform })).toThrow(
            BadRequestError,
         );
      },
   );
});

describe("partitionOrderSql", () => {
   it.each([
      [[["region", "identity"]], [`"region"`]],
      [
         [
            ["day", "year"],
            ["category", "identity"],
         ],
         [`year("day")`, `"category"`, `"day"`],
      ],
      [
         [
            ["ts", "month"],
            ["ts", "day"],
         ],
         [`month("ts")`, `day("ts")`, `"ts"`],
      ],
      // No scalar `bucket` exists, so a bucket key orders by its column only.
      [
         [
            ["sku", "bucket(16)"],
            ["region", "identity"],
         ],
         [`"region"`, `"sku"`],
      ],
   ] as [string[][], string[]][])("orders %j by %j", (keys, terms) => {
      expect(
         partitionOrderSql(
            keys.map(([column, transform]) => ({ column, transform })),
         ),
      ).toEqual(terms);
   });
});

describe("writePartitionedTable", () => {
   const recordingSession = () => {
      const issued: string[] = [];
      const session = {
         runSQL: async (sql: string) => {
            issued.push(sql.trim());
            return { rows: [] };
         },
      } as unknown as IsolatedSessionConnection;
      return { session, issued };
   };

   it("creates empty, lays out, then inserts in partition order, in one transaction", async () => {
      const { session, issued } = recordingSession();

      await writePartitionedTable(session, {
         quotedTablePath: `"lake"."main"."events"`,
         selectSQL: `SELECT * FROM "src"."main"."events"`,
         keys: [
            { column: "day", transform: "year" },
            { column: "category", transform: "identity" },
            { column: "day", transform: "month" },
         ],
         createStatement: "CREATE TABLE",
      });

      const transaction = issued.slice(issued.indexOf("BEGIN TRANSACTION"));
      expect(transaction[1]).toBe(
         `CREATE TABLE "lake"."main"."events" AS (SELECT * FROM "src"."main"."events") WITH NO DATA`,
      );
      expect(transaction[2]).toBe(
         `ALTER TABLE "lake"."main"."events" SET PARTITIONED BY (year("day"), "category", month("day"))`,
      );
      // Ordered by the keys as DuckLake partitions on them, then by the
      // transformed column, so a year's categories are not interleaved.
      expect(transaction[3]).toBe(
         `INSERT INTO "lake"."main"."events" (SELECT * FROM (\nSELECT * FROM "src"."main"."events"\n) ` +
            `AS partitioned_build ORDER BY year("day"), "category", month("day"), "day")`,
      );
      expect(transaction[4]).toBe("COMMIT");
      // The bounds are set before the transaction and threads restored after.
      expect(issued).toContain("SET partitioned_write_flush_threshold = 8192");
      expect(issued).toContain("SET threads = 1");
      expect(issued[issued.length - 1]).toBe("RESET threads");
   });
});

/**
 * A plain-DuckDB file destination named `lake` in a fresh environment
 * directory, and a postgres source whose federation and read are stubbed, so
 * the whole build session (attach, CTAS, DESCRIBE, dispose) runs for real with
 * no live warehouse. The read hands the CTAS `selectSQL` and records the SQL
 * it was asked to run.
 */
function fileLakeBuild(selectSQL: string) {
   const reads: string[] = [];
   const deps: BuildSessionDeps = {
      federate: async () => ({
         handle: "wh",
         sourceType: "postgres" as const,
         close: async () => {},
      }),
      read: async (_session, _type, _handle, buildSQL) => {
         reads.push(buildSQL);
         return { selectSQL, jobId: null, cost: null };
      },
      // The stubbed warehouse holds no numeric column to read as text.
      numericColumns: async () => ({ columns: [], numeric: [] }),
   };
   const dir = mkdtempSync(join(tmpdir(), "storage-build-"));
   mkdirSync(storageDestinationRoot(dir), { recursive: true });
   const params = {
      destinationName: "lake",
      destinationConnection: { name: "lake", type: "duckdb" } as ApiConnection,
      sourceConnection: { name: "wh", type: "postgres" } as ApiConnection,
      buildSQL: "SELECT * FROM orders",
      physicalTableName: "orders__mabc",
      environmentPath: dir,
      deps,
   };
   return {
      params,
      reads,
      cleanup: () => rmSync(dir, { recursive: true, force: true }),
   };
}

describe("buildSourceIntoStorage (happy path, real DuckDB file destination)", () => {
   const SELECT =
      "SELECT 1 AS id, 'a' AS category UNION ALL SELECT 2, 'b' UNION ALL SELECT 3, 'b'";

   it("writes the read's rows into the destination table and reports the destination and schema", async () => {
      const lake = fileLakeBuild(SELECT);
      // The rows are read back on the build's OWN session (through the afterSeed
      // hook, which runs after the CTAS), because the attach does not outlive
      // the call.
      let rows: unknown[] = [];
      let seenPath = "";
      try {
         const result = await buildSourceIntoStorage({
            ...lake.params,
            incremental: {
               plan: async () => undefined, // "rebuild it"
               afterSeed: async ({ session, quotedTablePath }) => {
                  seenPath = quotedTablePath;
                  rows = (
                     await session.runSQL(
                        `SELECT id, category FROM ${quotedTablePath} ORDER BY id`,
                     )
                  ).rows;
                  return { malloyType: "number", value: "3" };
               },
            },
         });

         expect(lake.reads).toEqual(["SELECT * FROM orders"]);
         expect(seenPath).toBe('"lake"."orders__mabc"');
         expect(rows).toEqual([
            { id: 1, category: "a" },
            { id: 2, category: "b" },
            { id: 3, category: "b" },
         ]);
         expect(result.storageDestinationName).toBe("lake");
         expect(result.schema.map((c) => c.name)).toEqual(["id", "category"]);
         // Counted in the destination, for table stats.
         expect(result.rowCount).toBe(3);
         // A postgres read reports no cost; null is "not reported", never free.
         expect(result.readCost).toBeNull();
         // A rebuild reports where the new snapshot reaches, and no refresh.
         expect(result.seededThrough).toEqual({
            malloyType: "number",
            value: "3",
         });
         expect(result.refresh).toBeUndefined();
      } finally {
         lake.cleanup();
      }
   });

   // ms2data/service#6642: the warehouse's types, cast in DuckDB around the
   // passthrough, so the warehouse still runs exactly the build SQL.
   it("stores each column as the type Malloy declares where one DuckDB type is faithful", async () => {
      const lake = fileLakeBuild(
         "SELECT TIMESTAMP '2026-02-01 00:00:00' AS order_month, 1 AS is_open, 2.5 AS amount",
      );
      try {
         const result = await buildSourceIntoStorage({
            ...lake.params,
            declaredColumns: [
               { name: "order_month", type: "date" },
               { name: "is_open", type: "boolean" },
               { name: "amount", type: "number" },
            ],
         });
         expect(lake.reads).toEqual(["SELECT * FROM orders"]);
         expect(result.schema).toEqual([
            { name: "order_month", type: "DATE" },
            { name: "is_open", type: "BOOLEAN" },
            { name: "amount", type: "DECIMAL(2,1)" },
         ]);
      } finally {
         lake.cleanup();
      }
   });

   // A cast that cannot convert a value must not fail a build that used to
   // succeed: it stores what the warehouse returned, never TRY_CAST's NULLs.
   it("stores the warehouse's types when a declared-type cast cannot convert a value", async () => {
      const lake = fileLakeBuild("SELECT 'open' AS is_open, 1 AS n");
      try {
         const result = await buildSourceIntoStorage({
            ...lake.params,
            declaredColumns: [{ name: "is_open", type: "boolean" }],
         });
         expect(result.schema).toEqual([
            { name: "is_open", type: "VARCHAR" },
            { name: "n", type: "INTEGER" },
         ]);
      } finally {
         lake.cleanup();
      }
   });

   // Under a query time zone a date is not cast at build: an instant's date in
   // the build session's UTC can be the day before the zone's.
   it("does not cast a date at build when the source has a query time zone", async () => {
      const lake = fileLakeBuild(
         "SELECT TIMESTAMPTZ '2024-03-04 15:00:00+00' AS d, 1 AS is_open",
      );
      try {
         const result = await buildSourceIntoStorage({
            ...lake.params,
            declaredColumns: [
               { name: "d", type: "date" },
               { name: "is_open", type: "boolean" },
            ],
            castOptions: { queryTimezone: "Asia/Tokyo" },
         });
         expect(result.schema).toEqual([
            { name: "d", type: "TIMESTAMP WITH TIME ZONE" },
            { name: "is_open", type: "BOOLEAN" },
         ]);
      } finally {
         lake.cleanup();
      }
   });

   it("returns the plan's in-place refresh, with the table's schema, and issues no full read", async () => {
      const lake = fileLakeBuild(SELECT);
      let afterSeedRan = false;
      try {
         const result = await buildSourceIntoStorage({
            ...lake.params,
            incremental: {
               plan: async ({ session, quotedTablePath }) => {
                  // Stands in for a table an earlier build seeded and a delta
                  // just advanced.
                  await session.runSQL(
                     `CREATE TABLE ${quotedTablePath} AS SELECT 7 AS id`,
                  );
                  return {
                     refresh: "delta",
                     durationMs: 5,
                     coveredThrough: { malloyType: "number", value: "7" },
                     readCost: null,
                  };
               },
               afterSeed: async () => {
                  afterSeedRan = true;
                  return undefined;
               },
            },
         });

         expect(lake.reads).toEqual([]);
         expect(afterSeedRan).toBe(false);
         expect(result.refresh).toMatchObject({
            refresh: "delta",
            coveredThrough: { malloyType: "number", value: "7" },
         });
         expect(result.schema.map((c) => c.name)).toEqual(["id"]);
         expect(result.seededThrough).toBeUndefined();
      } finally {
         lake.cleanup();
      }
   });
});

// P3-06..09: a Snowflake build session that does not hold Malloy's TIMEZONE
// and WEEK_START could store other day and week buckets than live answers, so
// the build fails rather than store them.
describe("buildSourceIntoStorage refuses a Snowflake session without Malloy's settings", () => {
   it("P3-09: fails the build when WEEK_START does not read back as 7", async () => {
      const original = DuckDBConnection.prototype.runSQL;
      const created: string[] = [];
      const runSQL = spyOn(
         DuckDBConnection.prototype,
         "runSQL",
      ).mockImplementation(async function (
         this: DuckDBConnection,
         sql: string,
      ) {
         const shown = /SHOW PARAMETERS LIKE ''(\w+)''/.exec(sql);
         if (shown) {
            const value = { TIMEZONE: "UTC", WEEK_START: "1" }[shown[1]];
            return { rows: [{ KEY: shown[1], VALUE: value }] } as never;
         }
         if (sql.includes("CREATE OR REPLACE TABLE")) created.push(sql);
         if (
            sql.includes("snowflake_query") ||
            sql.includes("snowflake_execute")
         ) {
            return { rows: [] } as never;
         }
         return original.call(this, sql);
      });
      const dir = mkdtempSync(join(tmpdir(), "snowflake-settings-"));
      mkdirSync(storageDestinationRoot(dir), { recursive: true });
      try {
         await expect(
            buildSourceIntoStorage({
               destinationName: "lake",
               destinationConnection: {
                  name: "lake",
                  type: "duckdb",
               } as ApiConnection,
               sourceConnection: {
                  name: "sf",
                  type: "snowflake",
               } as ApiConnection,
               buildSQL: "SELECT 1",
               physicalTableName: "t",
               environmentPath: dir,
               deps: {
                  federate: async () => ({
                     handle: "secret_sf",
                     sourceType: "snowflake" as const,
                     close: async () => {},
                  }),
                  read: async () => ({
                     selectSQL: "SELECT 1 AS x",
                     jobId: null,
                     cost: null,
                  }),
               },
            }),
         ).rejects.toThrow(/WEEK_START = 1, not Malloy's 7/);
         expect(created).toEqual([]);
      } finally {
         runSQL.mockRestore();
         rmSync(dir, { recursive: true, force: true });
      }
   });
});

describe("buildSourceIntoStorage clears a Snowflake session's query tag", () => {
   // The tag is best-effort, so the session may arrive already tagged by
   // someone else; the cleanup has to run whenever the source is Snowflake.
   it("issues ALTER SESSION UNSET QUERY_TAG against the federated handle", async () => {
      const executed: string[] = [];
      const original = DuckDBConnection.prototype.runSQL;
      const runSQL = spyOn(
         DuckDBConnection.prototype,
         "runSQL",
      ).mockImplementation(async function (
         this: DuckDBConnection,
         sql: string,
      ) {
         executed.push(sql);
         // The session check reads Malloy's settings back, as held.
         const shown = /SHOW PARAMETERS LIKE ''(\w+)''/.exec(sql);
         if (shown) {
            const value = { TIMEZONE: "UTC", WEEK_START: "7" }[shown[1]];
            return { rows: [{ key: shown[1], value }] } as never;
         }
         // The passthrough statements need the Snowflake extension; every
         // other statement runs for real so the build completes.
         if (
            sql.includes("snowflake_query") ||
            sql.includes("snowflake_execute")
         ) {
            throw new Error("no snowflake extension in this test");
         }
         return original.call(this, sql);
      });
      const dir = mkdtempSync(join(tmpdir(), "snowflake-tag-"));
      mkdirSync(storageDestinationRoot(dir), { recursive: true });
      try {
         await buildSourceIntoStorage({
            destinationName: "lake",
            destinationConnection: {
               name: "lake",
               type: "duckdb",
            } as ApiConnection,
            sourceConnection: {
               name: "sf",
               type: "snowflake",
            } as ApiConnection,
            buildSQL: "SELECT 1",
            physicalTableName: "t",
            environmentPath: dir,
            deps: {
               federate: async () => ({
                  handle: "secret_sf",
                  sourceType: "snowflake" as const,
                  close: async () => {},
               }),
               read: async () => ({
                  selectSQL: "SELECT 1 AS x",
                  jobId: null,
                  cost: null,
               }),
            },
         });
         expect(
            executed.some(
               (sql) =>
                  sql.includes("ALTER SESSION UNSET QUERY_TAG") &&
                  sql.includes("secret_sf"),
            ),
         ).toBe(true);
         // P3-06..09: Malloy's live session settings, issued on the
         // federated handle before the build's read.
         const parity = [
            "QUOTED_IDENTIFIERS_IGNORE_CASE = FALSE",
            "TIMEZONE = ''UTC''",
            "WEEK_START = 7",
         ].map((p) =>
            executed.findIndex(
               (sql) => sql.includes(p) && sql.includes("secret_sf"),
            ),
         );
         expect(parity.every((i) => i >= 0)).toBe(true);
         const create = executed.findIndex((sql) =>
            sql.includes("CREATE OR REPLACE TABLE"),
         );
         expect(Math.max(...parity)).toBeLessThan(create);
      } finally {
         runSQL.mockRestore();
         rmSync(dir, { recursive: true, force: true });
      }
   });
});

describe("removeStaleBuildDirs", () => {
   it("removes build directories of this pid older than the process and keeps everything else", () => {
      const root = mkdtempSync(join(tmpdir(), "stale-sweep-"));
      const startedAt = Date.now();
      const at = (dir: string, offsetMs: number) => {
         const t = new Date(startedAt + offsetMs);
         utimesSync(dir, t, t);
      };
      const own = `malloy-build-${process.pid}-`;
      try {
         for (const name of [`${own}old`, `${own}new`, "other-old"]) {
            mkdirSync(join(root, name));
         }
         writeFileSync(join(root, `${own}file`), "x");
         at(join(root, `${own}old`), -60_000);
         at(join(root, `${own}new`), 60_000);
         at(join(root, "other-old"), -60_000);

         expect(removeStaleBuildDirs(root, startedAt)).toBe(1);

         expect(existsSync(join(root, `${own}old`))).toBe(false);
         expect(existsSync(join(root, `${own}new`))).toBe(true);
         expect(existsSync(join(root, "other-old"))).toBe(true);
         expect(existsSync(join(root, `${own}file`))).toBe(true);
      } finally {
         rmSync(root, { recursive: true, force: true });
      }
   });

   it("removes nothing when there is nothing stale", () => {
      const root = mkdtempSync(join(tmpdir(), "stale-sweep-"));
      try {
         expect(removeStaleBuildDirs(root, Date.now())).toBe(0);
      } finally {
         rmSync(root, { recursive: true, force: true });
      }
   });

   describe("a directory named for its process", () => {
      const startedAt = Date.now();
      const ownPid = process.pid;
      let root: string;
      const dir = (name: string, offsetMs: number) => {
         mkdirSync(join(root, name));
         const t = new Date(startedAt + offsetMs);
         utimesSync(join(root, name), t, t);
      };
      const left = () => readdirSync(root).sort();
      beforeEach(() => {
         root = mkdtempSync(join(tmpdir(), "stale-sweep-pid-"));
      });
      afterEach(() => rmSync(root, { recursive: true, force: true }));

      it("names the process that created it", () => {
         const { dispose, workDir } = createIsolatedBuildSession("pid_name");
         try {
            expect(basename(workDir)).toMatch(
               new RegExp(`^malloy-build-${process.pid}-`),
            );
         } finally {
            void dispose();
         }
      });

      it("removes one whose process is gone, however recent, and keeps one whose process is alive, however old", async () => {
         const live = spawn(process.execPath, [
            "-e",
            "setTimeout(() => {}, 60000)",
         ]);
         const dead = spawnSync(process.execPath, ["--version"]);
         try {
            dir(`malloy-build-${dead.pid}-x`, 60_000);
            dir(`malloy-build-${live.pid}-x`, -60_000);
            expect(removeStaleBuildDirs(root, startedAt)).toBe(1);
            expect(left()).toEqual([`malloy-build-${live.pid}-x`]);
         } finally {
            live.kill();
         }
      });

      it("removes one of its own pid only when it predates the process: a restarted container reuses pid 1", () => {
         dir(`malloy-build-${ownPid}-old`, -60_000);
         dir(`malloy-build-${ownPid}-live`, 60_000);
         expect(removeStaleBuildDirs(root, startedAt)).toBe(1);
         expect(left()).toEqual([`malloy-build-${ownPid}-live`]);
      });

      it("leaves one with no pid in its name alone, however old: an older publisher may share the temp root", () => {
         dir("malloy-build-abc123", -60_000);
         dir("malloy-build-7x", 60_000);
         expect(removeStaleBuildDirs(root, startedAt)).toBe(0);
         expect(left()).toEqual(["malloy-build-7x", "malloy-build-abc123"]);
      });
   });

   it("does not throw when the temp root cannot be listed", () => {
      expect(
         removeStaleBuildDirs(join(tmpdir(), "no-such-root-for-sweep")),
      ).toBe(0);
   });
});

describe("a build that also reads another destination", () => {
   // Its data path is in the session's allowlist, which DuckDB cannot make
   // read-only: only one SELECT from the model reaches the session.
   const other = "/tmp/other-destination/data";

   it("refuses a COPY ... TO into the other destination's path, alone or after a SELECT", async () => {
      for (const sql of [
         `COPY (SELECT 1 AS x) TO '${other}/x.parquet'`,
         `SELECT 1 AS x; COPY (SELECT 1 AS x) TO '${other}/x.parquet'`,
         `SELECT 1 AS x -- hack: split on this\nCOPY (SELECT 1) TO '${other}/x.parquet'`,
         `EXPORT DATABASE '${other}'`,
      ]) {
         await expect(assertSingleSelect(sql)).rejects.toThrow(
            /runs one SELECT from its model/,
         );
      }
   });

   it("runs one SELECT", async () => {
      await assertSingleSelect(
         "WITH t AS (SELECT 1 AS x) SELECT x FROM t ORDER BY x",
      );
   });
});

describe("IsolatedSessionConnection", () => {
   const dirs: string[] = [];
   afterEach(() => {
      for (const dir of dirs.splice(0)) {
         rmSync(dir, { recursive: true, force: true });
      }
   });

   const tempDir = () => {
      const dir = mkdtempSync(join(tmpdir(), "isolated-session-"));
      dirs.push(dir);
      return dir;
   };

   /** The lake's catalog and data live in `dir`, so a later session attaches the same tables. */
   async function attachLake(
      session: IsolatedSessionConnection,
      dir: string,
   ): Promise<void> {
      await session.runSQL("INSTALL ducklake");
      await session.runSQL("LOAD ducklake");
      await session.runSQL(
         `ATTACH 'ducklake:${join(dir, "catalog.ducklake")}' AS lake ` +
            `(DATA_PATH '${join(dir, "data")}/', ENCRYPTED true, DATA_INLINING_ROW_LIMIT 0)`,
      );
   }

   /** A lake with `lake.t(id NOT NULL)` holding 1, 2, 3, and the session that made it. */
   async function seededLake() {
      const dir = tempDir();
      const made = createIsolatedBuildSession("isolated_lake");
      await attachLake(made.session, dir);
      await made.session.runSQL("CREATE TABLE lake.t(id INTEGER NOT NULL)");
      await made.session.runSQL("INSERT INTO lake.t VALUES (1), (2), (3)");
      return { dir, ...made };
   }

   const count = async (session: IsolatedSessionConnection) =>
      Number(
         (
            (await session.runSQL("SELECT count(*) AS n FROM lake.t"))
               .rows[0] as { n: unknown }
         ).n,
      );

   /** What a fresh session on the same lake counts. */
   async function countInFreshSession(dir: string): Promise<number> {
      const fresh = createIsolatedBuildSession("isolated_fresh");
      try {
         await attachLake(fresh.session, dir);
         return await count(fresh.session);
      } finally {
         await fresh.dispose();
      }
   }

   const errorOf = (p: Promise<unknown>) =>
      p.then(
         () => undefined,
         (e: unknown) => e as Error,
      );

   it("opens its instance with invalidation off, keeping the thread cap and lockdown Publisher adds", async () => {
      installDuckDBInstanceSharing();
      const { session, dispose } = createIsolatedBuildSession("isolated_opts");
      try {
         const setting = async (name: string) =>
            String(
               (
                  (
                     await session.runSQL(
                        `SELECT current_setting('${name}') AS v`,
                     )
                  ).rows[0] as { v: unknown }
               ).v,
            );
         expect(await setting("disable_database_invalidation")).toBe("true");
         // The cap the spec preload (duckdb_test_threads.ts) puts on every instance.
         expect(await setting("threads")).toBe(
            process.env.PUBLISHER_TEST_DUCKDB_THREADS ?? "2",
         );
         expect(await setting("secret_directory")).toContain(
            "no-persistent-secrets",
         );
      } finally {
         await dispose();
      }
   });

   it("fails a fatal error's transaction and survives it: the process lives, a swallowed error cannot commit", async () => {
      const child = spawnSync(
         process.execPath,
         [
            join(
               import.meta.dir,
               "../../../test_helpers/fatal_session_fixture.ts",
            ),
         ],
         {
            cwd: join(import.meta.dir, "../../../.."),
            encoding: "utf8",
            timeout: 120_000,
         },
      );
      // Exit 134 is the abort of DuckDB 1.5's destructor, which the session contains.
      expect({ status: child.status, stderr: child.stderr }).toMatchObject({
         status: 0,
      });
      const line = child.stdout
         .split("\n")
         .find((l) => l.startsWith("RESULT "));
      type Scenario = {
         secondReadError: string;
         transaction: string;
         insertAfterFatal?: string;
         later: { message: string | null; causeIsInternal: boolean };
         disposed: boolean;
      };
      const result = JSON.parse(line!.slice("RESULT ".length)) as {
         propagated: Scenario;
         swallowed: Scenario;
         unchanged: number;
      };
      const { propagated, swallowed } = result;
      // The canary: when the second read stops failing, DuckDB has fixed duckdb#24311 (v2.0.0).
      expect(
         propagated.secondReadError,
         "The second read of a cached encrypted Parquet file no longer fails: DuckDB fixed duckdb#24311 (v2.0.0), so this trigger is gone. Point this containment test at another fatal trigger, and revisit IsolatedSessionConnection's invalidation override with the DuckDB 2.0 upgrade (the metadata cache stays off regardless: it measured no request savings)",
      ).toContain("INTERNAL Error");
      expect(propagated.transaction).toContain("INTERNAL Error");
      expect(propagated.later.message).toContain("fatal DuckDB error");
      expect(propagated.later.causeIsInternal).toBe(true);
      expect(propagated.disposed).toBe(true);
      expect(swallowed.insertAfterFatal).toContain("fatal DuckDB error");
      expect(swallowed.transaction).toContain("fatal DuckDB error");
      expect(swallowed.disposed).toBe(true);
      // Neither run's insert committed.
      expect(result.unchanged).toBe(3);
   }, 130_000);

   it("refuses the COMMIT after an ordinary error the body swallowed", async () => {
      const { dir, session, dispose } = await seededLake();
      try {
         const error = await errorOf(
            inTransaction(session, "lake.t", async () => {
               await session.runSQL("INSERT INTO lake.t VALUES (4)");
               await errorOf(
                  session.runSQL("INSERT INTO lake.t VALUES (NULL)"),
               );
            }),
         );
         expect(error?.message).toContain("transaction was aborted");
         expect(error?.cause).toBeInstanceOf(Error);
         expect((error!.cause as Error).message).toContain("NOT NULL");
         expect(await count(session)).toBe(3);
      } finally {
         await dispose();
      }
      expect(await countInFreshSession(dir)).toBe(3);
   });

   it("refuses the COMMIT after an ordinary error in a script sent as one statement, and a rollback restores the table", async () => {
      const { dir, session, dispose } = await seededLake();
      try {
         const script = await errorOf(
            session.runSQL(
               "BEGIN; INSERT INTO lake.t VALUES (4); INSERT INTO lake.t VALUES (NULL); COMMIT",
            ),
         );
         expect(script?.message).toContain("NOT NULL");
         expect((await errorOf(session.runSQL("COMMIT")))?.message).toContain(
            "transaction was aborted",
         );
         await session.runSQL("ROLLBACK");
         expect(await count(session)).toBe(3);
      } finally {
         await dispose();
      }
      expect(await countInFreshSession(dir)).toBe(3);
   });

   it("keeps running after a delta script that failed before its BEGIN, and after a failed autocommit statement", async () => {
      const { session, dispose } = await seededLake();
      try {
         await expect(
            applyDeltaScript(sessionRunner(session), "duckdb", [
               "CREATE TEMP TABLE stage AS SELECT CAST('x' AS INTEGER) AS id",
               "BEGIN TRANSACTION",
               "INSERT INTO lake.t SELECT id FROM stage",
               "COMMIT",
            ]),
         ).rejects.toThrow("Conversion Error");
         await session.runSQL("INSERT INTO lake.t VALUES (4)");
         expect(
            (await errorOf(session.runSQL("INSERT INTO lake.t VALUES (NULL)")))
               ?.message,
         ).toContain("NOT NULL");
         await session.runSQL("INSERT INTO lake.t VALUES (5)");
         expect(await count(session)).toBe(5);
      } finally {
         await dispose();
      }
   });

   it("latches on a BEGIN inside a transaction, until a rollback", async () => {
      const { session, dispose } = await seededLake();
      try {
         await session.runSQL("BEGIN TRANSACTION");
         expect((await errorOf(session.runSQL("BEGIN")))?.message).toContain(
            "within a transaction",
         );
         expect(
            (await errorOf(session.runSQL("INSERT INTO lake.t VALUES (4)")))
               ?.message,
         ).toContain("transaction was aborted");
         await session.runSQL("ROLLBACK");
         await session.runSQL("INSERT INTO lake.t VALUES (4)");
         expect(await count(session)).toBe(4);
      } finally {
         await dispose();
      }
   });

   it.each([
      // Not a rollback to the session: refused like any statement.
      ["ROLLBACK WORK WORK", "transaction was aborted"],
      ["ROLLBACK TRANSACTION WORK", "transaction was aborted"],
      ["ROLLBACK; ROLLBACK", "transaction was aborted"],
      // Shaped like one, but DuckDB refuses to parse it, so nothing rolled back.
      ["ROLLBACK /* unterminated", "Parser Error"],
   ])(
      "keeps an aborted transaction latched through %j",
      async (sql, message) => {
         const { dir, session, dispose } = await seededLake();
         try {
            await session.runSQL("BEGIN TRANSACTION");
            await session.runSQL("INSERT INTO lake.t VALUES (4)");
            await errorOf(session.runSQL("INSERT INTO lake.t VALUES (NULL)"));
            expect((await errorOf(session.runSQL(sql)))?.message).toContain(
               message,
            );
            expect(
               (await errorOf(session.runSQL("COMMIT")))?.message,
            ).toContain("transaction was aborted");
            await session.runSQL("ROLLBACK");
            expect(await count(session)).toBe(3);
         } finally {
            await dispose();
         }
         expect(await countInFreshSession(dir)).toBe(3);
      },
   );

   it("runs the statements queued behind one that was interrupted, and one issued after them", async () => {
      const { session, dispose } = createIsolatedBuildSession("late_row");
      try {
         const long = errorOf(
            session.runSQL(
               "SELECT count(*) FROM range(1000000000000) a WHERE a.range % 7 = 3",
            ),
         );
         const queued = session.runSQL("SELECT 1 AS one");
         // A statement-scoped interrupt, as the late-row check's, not a cancel.
         setTimeout(() => session.interrupt(), 200);
         expect((await queued).rows).toEqual([{ one: 1 }]);
         // Issued while the failed statement's probe is still pending.
         const next = await errorOf(session.runSQL("SELECT 2 AS two"));
         expect(next).toBeUndefined();
         expect((await long)?.message).toMatch(/interrupt/i);
         expect((await session.runSQL("SELECT 3 AS three")).rows).toEqual([
            { three: 3 },
         ]);
      } finally {
         await dispose();
      }
   }, 30_000);

   it("refuses a COMMIT queued behind a write that failed", async () => {
      const { dir, session, dispose } = await seededLake();
      try {
         await session.runSQL("BEGIN TRANSACTION");
         await session.runSQL("INSERT INTO lake.t VALUES (4)");
         const failed = errorOf(
            session.runSQL("INSERT INTO lake.t VALUES (NULL)"),
         );
         const commit = errorOf(session.runSQL("COMMIT"));
         expect((await failed)?.message).toContain("NOT NULL");
         expect((await commit)?.message).toContain("transaction was aborted");
         await session.runSQL("ROLLBACK");
         expect(await count(session)).toBe(3);
      } finally {
         await dispose();
      }
      expect(await countInFreshSession(dir)).toBe(3);
   });

   it("is not interrupted while it asks DuckDB whether a transaction is open", async () => {
      const { session, dispose } = await seededLake();
      try {
         const probe = Object.getPrototypeOf(session) as {
            transactionIsOpen(): Promise<boolean>;
         };
         const original = probe.transactionIsOpen;
         probe.transactionIsOpen = function (this: typeof session) {
            const asked = original.call(this);
            // An abandoned statement's abort lands while the probe runs.
            this.interrupt();
            return asked;
         };
         try {
            await errorOf(session.runSQL("INSERT INTO lake.t VALUES (NULL)"));
         } finally {
            probe.transactionIsOpen = original;
         }
         await session.runSQL("INSERT INTO lake.t VALUES (4)");
         expect(await count(session)).toBe(4);
      } finally {
         await dispose();
      }
   });

   it.each([
      ["a quoted value", "SELECT CAST('INTERNAL Error: x' AS INTEGER)"],
      [
         "a quoted value holding an apostrophe",
         "SELECT CAST('a''INTERNAL Error: x' AS INTEGER)",
      ],
      ["a message from error()", "SELECT error('INTERNAL Error: x')"],
   ])("does not read %s as a fatal DuckDB error", async (_label, sql) => {
      const { session, dispose } =
         createIsolatedBuildSession("isolated_quoted");
      try {
         const error = await errorOf(session.runSQL(sql));
         expect(error?.message).toContain("INTERNAL Error: x");
         expect(session.fatal).toBeUndefined();
         expect((await session.runSQL("SELECT 1 AS one")).rows).toEqual([
            { one: 1 },
         ]);
      } finally {
         await dispose();
      }
   });

   it.each([
      ["ROLLBACK"],
      ["rollback transaction"],
      ["ROLLBACK WORK"],
      ["ABORT"],
      ["abort work"],
      ["-- end it\n/* now */ Rollback ;"],
   ])(
      "lets %j clear an aborted transaction, and only a rollback does",
      async (rollback) => {
         const { session, dispose } = await seededLake();
         try {
            await session.runSQL("BEGIN TRANSACTION");
            await errorOf(session.runSQL("INSERT INTO lake.t VALUES (NULL)"));
            for (const sql of ["SELECT 1", "COMMIT", "ROLLBACK TO x"]) {
               expect((await errorOf(session.runSQL(sql)))?.message).toContain(
                  "transaction was aborted",
               );
            }
            await session.runSQL(rollback);
            expect(await count(session)).toBe(3);
         } finally {
            await dispose();
         }
      },
   );

   it("keeps an error DuckDB does not abort a transaction on, as a Catalog Error, from aborting it", async () => {
      const { dir, session, dispose } = await seededLake();
      try {
         await inTransaction(session, "lake.t", async () => {
            const probe = await errorOf(
               session.runSQL("SELECT * FROM lake.missing"),
            );
            expect(probe?.message).toMatch(/^Catalog Error:/);
            await session.runSQL("INSERT INTO lake.t VALUES (4)");
         });
         expect(await count(session)).toBe(4);
      } finally {
         await dispose();
      }
      expect(await countInFreshSession(dir)).toBe(4);
   });

   it("rolls back on dispose a transaction its owner left open, before it closes the session", async () => {
      const { dir, session, dispose } = await seededLake();
      const raw = spyOn(session, "runRawSQL");
      const close = spyOn(session, "close");
      try {
         await session.runSQL("BEGIN TRANSACTION");
         await session.runSQL("INSERT INTO lake.t VALUES (4)");
         await dispose();
         const rolledBack = raw.mock.calls.findIndex(
            ([sql]) => sql === "ROLLBACK",
         );
         expect(rolledBack).toBeGreaterThanOrEqual(0);
         expect(raw.mock.invocationCallOrder[rolledBack]).toBeLessThan(
            close.mock.invocationCallOrder[0],
         );
      } finally {
         raw.mockRestore();
         close.mockRestore();
      }
      expect(await countInFreshSession(dir)).toBe(3);
   });

   it("refuses a stream once the session has ended", async () => {
      const { session, dispose } = createIsolatedBuildSession("stream_refused");
      try {
         await errorOf(session.runSQL("SELECT CAST('x' AS INTEGER)"));
         session.cancel();
         const drain = async () => {
            for await (const _row of session.runSQLStream("SELECT 1 AS one")) {
               // Refused before any row.
            }
         };
         expect(await errorOf(drain())).toBeInstanceOf(BuildCancelledError);
      } finally {
         await dispose();
      }
   });

   it("rolls a cancelled build back in place, without a failed-rollback warning", async () => {
      const { dir, session, dispose } = await seededLake();
      const warn = spyOn(logger, "warn").mockImplementation(() => logger);
      try {
         const run = new AbortController();
         const stop = interruptOnAbort(session, run.signal);
         const error = await errorOf(
            inTransaction(session, "lake.t", async () => {
               await session.runSQL("INSERT INTO lake.t VALUES (4)");
               run.abort();
               await session.runSQL("INSERT INTO lake.t VALUES (5)");
            }),
         );
         expect(error).toBeInstanceOf(BuildCancelledError);
         expect(await errorOf(session.runRawSQL("SELECT 1"))).toBeInstanceOf(
            BuildCancelledError,
         );
         stop();
         expect(
            warn.mock.calls.filter(([message]) =>
               String(message).includes("Failed to roll back"),
            ),
         ).toEqual([]);
      } finally {
         warn.mockRestore();
         await dispose();
      }
      expect(await countInFreshSession(dir)).toBe(3);
   });
});
