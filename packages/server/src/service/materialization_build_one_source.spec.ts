// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Manifest } from "@malloydata/malloy";
import { beforeEach, describe, expect, it } from "bun:test";
import * as sinon from "sinon";
import { BuildInstruction } from "../storage/DatabaseInterface";
import { fakeSource } from "../test_helpers/materialization_test_fixtures";
import {
   createMocks,
   withQueryMetadataOn,
} from "../test_helpers/materialization_service_harness";

/**
 * buildOneSource: the colocated CTAS/swap, the SQL it issues and the metadata
 * it carries, and the upstream reuse it reports.
 */

describe("buildOneSource", () => {
   let ctx: ReturnType<typeof createMocks>;
   withQueryMetadataOn();
   beforeEach(() => {
      ctx = createMocks();
   });

   function callBuildOneSource(
      connection: { runSQL: sinon.SinonStub },
      physicalTableName: string,
      dialectName?: string,
      manifest: Manifest = new Manifest(),
      metadata?: {
         source?: Record<string, string>;
         package?: Record<string, string>;
         connection?: Record<string, string>;
         context?: Record<string, string | undefined>;
      },
   ): Promise<{ sourceEntityId: string; physicalTableName: string }> {
      const source = fakeSource({
         name: "orders",
         sourceEntityId: "abcdef1234567890",
         sql: "SELECT * FROM t",
         dialectName,
         queryMetadata: metadata?.source,
      });
      const instruction: BuildInstruction = {
         sourceEntityId: "abcdef1234567890",
         materializedTableId: "mt-1",
         physicalTableName,
         realization: "COPY",
      };
      return (
         ctx.service as unknown as {
            buildOneSource: (
               s: unknown,
               i: BuildInstruction,
               c: unknown,
               d: Record<string, string>,
               m: Manifest,
               env: unknown,
               built: Record<string, unknown>,
               buildMetadata?: unknown,
            ) => Promise<{ sourceEntityId: string; physicalTableName: string }>;
         }
      ).buildOneSource(
         source,
         instruction,
         connection,
         { duckdb: "dig" },
         manifest,
         {
            getApiConnection: () => {
               if (metadata?.connection) {
                  return { queryMetadata: metadata.connection };
               }
               throw new Error("no connection config in this colocated test");
            },
            getEnvironmentPath: () => "/tmp/env",
         },
         {}, // builtEntries — colocated build with no prior storage upstreams
         metadata
            ? {
                 packageMaterialization: metadata.package
                    ? {
                         schedule: null,
                         freshness: null,
                         queryMetadata: metadata.package,
                      }
                    : null,
                 context: metadata.context ?? { queryClass: "materialize" },
              }
            : undefined,
      );
   }

   it("tags every statement of the build with the resolved metadata", async () => {
      // One unit of work: the warehouse's query history has to show the staging
      // drop, the CTAS, the swap and the rename as the same build.
      const runSQL = sinon.stub().resolves();
      await callBuildOneSource({ runSQL }, "orders_v1", undefined, undefined, {
         connection: { deployment: "eu" },
         package: { team: "finance", tier: "bronze" },
         source: { tier: "gold" },
         context: { queryClass: "materialize", runId: "run-7" },
      });

      const bags = runSQL.getCalls().map((c) => c.args[1]?.queryMetadata);
      expect(bags).toHaveLength(4);
      for (const bag of bags) {
         expect(bag).toEqual({
            deployment: "eu",
            team: "finance",
            // The source's own declaration wins over the package's.
            tier: "gold",
            class: "materialize",
            run_id: "run-7",
            source: "orders",
         });
      }
   });

   it("issues untagged statements when nothing declares metadata", async () => {
      // No metadata context (an internal caller) means the statements are exactly
      // what they were before this existed.
      const runSQL = sinon.stub().resolves();
      await callBuildOneSource({ runSQL }, "orders_v1");
      for (const call of runSQL.getCalls()) {
         expect(call.args[1]?.queryMetadata).toBeUndefined();
      }
   });

   it("tags the cleanup drop after a failed build", async () => {
      // The cleanup statement belongs to the same build, and it is the one an
      // operator goes looking for after a failure.
      const runSQL = sinon.stub();
      runSQL.onCall(0).resolves();
      runSQL.onCall(1).rejects(new Error("create boom"));
      runSQL.onCall(2).resolves();
      await expect(
         callBuildOneSource({ runSQL }, "orders_v1", undefined, undefined, {
            source: { team: "finance" },
            context: { queryClass: "materialize" },
         }),
      ).rejects.toThrow("create boom");
      expect(runSQL.lastCall.args[1]?.queryMetadata).toEqual({
         team: "finance",
         class: "materialize",
         source: "orders",
      });
   });

   it("stages, swaps, and renames in a crash-safe order", async () => {
      const runSQL = sinon.stub().resolves();
      const entry = await callBuildOneSource({ runSQL }, "orders_v1");

      const sql = runSQL.getCalls().map((c) => c.args[0] as string);
      expect(sql).toEqual([
         'DROP TABLE IF EXISTS "orders_v1_abcdef123456"',
         'CREATE TABLE "orders_v1_abcdef123456" AS (SELECT * FROM t)',
         'DROP TABLE IF EXISTS "orders_v1"',
         'ALTER TABLE "orders_v1_abcdef123456" RENAME TO "orders_v1"',
      ]);
      expect(entry.physicalTableName).toBe("orders_v1");
      expect(entry.sourceEntityId).toBe("abcdef1234567890");
   });

   it("qualifies the rename target on a session-relative dialect", async () => {
      // The swap assertions above all use a 1-part name, where the bare and
      // qualified targets are textually identical and cannot discriminate. On a
      // dialect that resolves a bare target against the session, a container-
      // qualified name must reach the RENAME as the same path the CREATE used,
      // or the finished table lands in the session's container instead -- a green
      // build that wrote to the wrong schema.
      const runSQL = sinon.stub().resolves();
      const entry = await callBuildOneSource(
         { runSQL },
         "MY_SCHEMA.orders_v1",
         "snowflake",
      );

      const sql = runSQL.getCalls().map((c) => c.args[0] as string);
      expect(sql).toEqual([
         'DROP TABLE IF EXISTS "MY_SCHEMA"."orders_v1_abcdef123456"',
         'CREATE TABLE "MY_SCHEMA"."orders_v1_abcdef123456" AS (SELECT * FROM t)',
         'DROP TABLE IF EXISTS "MY_SCHEMA"."orders_v1"',
         'ALTER TABLE "MY_SCHEMA"."orders_v1_abcdef123456" RENAME TO "MY_SCHEMA"."orders_v1"',
      ]);
      // The manifest keeps the logical name the control plane sent.
      expect(entry.physicalTableName).toBe("MY_SCHEMA.orders_v1");
   });

   it("keeps the rename target bare on a container-relative dialect", async () => {
      // The other direction of the same wiring: BigQuery rejects a qualified
      // rename target, so a container-qualified name must still rename bare.
      const runSQL = sinon.stub().resolves();
      await callBuildOneSource(
         { runSQL },
         "mydataset.orders_v1",
         "standardsql",
      );

      const sql = runSQL.getCalls().map((c) => c.args[0] as string);
      expect(sql[3]).toBe(
         "ALTER TABLE `mydataset`.`orders_v1_abcdef123456` RENAME TO `orders_v1`",
      );
   });

   it("drops the staging table and rethrows when the build SQL fails", async () => {
      const runSQL = sinon.stub();
      runSQL.onCall(0).resolves(); // initial staging drop
      runSQL.onCall(1).rejects(new Error("create boom")); // CREATE TABLE AS
      runSQL.onCall(2).resolves(); // cleanup staging drop
      await expect(callBuildOneSource({ runSQL }, "orders_v1")).rejects.toThrow(
         "create boom",
      );
      expect(runSQL.lastCall.args[0]).toBe(
         'DROP TABLE IF EXISTS "orders_v1_abcdef123456"',
      );
   });

   it("rethrows the original build error when staging cleanup also fails", async () => {
      const runSQL = sinon.stub();
      runSQL.onCall(0).resolves(); // initial staging drop
      runSQL.onCall(1).rejects(new Error("create boom")); // CREATE TABLE AS
      runSQL.onCall(2).rejects(new Error("cleanup boom")); // cleanup drop fails
      // The cleanup failure is swallowed (logged as a physical leak); the
      // original build error is what propagates, so the run is classified by its
      // real cause rather than the cleanup noise.
      await expect(callBuildOneSource({ runSQL }, "orders_v1")).rejects.toThrow(
         "create boom",
      );
      expect(runSQL.callCount).toBe(3);
      expect(runSQL.lastCall.args[0]).toBe(
         'DROP TABLE IF EXISTS "orders_v1_abcdef123456"',
      );
   });

   it("drops staging when the build is cancelled mid-run (no _staging orphan)", async () => {
      // buildOneSource is cancellation-agnostic: a cancel surfaces as a thrown
      // build SQL and takes the same staging-cleanup path as a failure, so a
      // cancelled build leaves no orphaned _staging table behind.
      const runSQL = sinon.stub();
      runSQL.onCall(0).resolves(); // initial staging drop
      runSQL.onCall(1).rejects(new Error("Build cancelled")); // aborted mid-CREATE
      runSQL.onCall(2).resolves(); // cleanup staging drop
      await expect(callBuildOneSource({ runSQL }, "orders_v1")).rejects.toThrow(
         "Build cancelled",
      );
      expect(runSQL.lastCall.args[0]).toBe(
         'DROP TABLE IF EXISTS "orders_v1_abcdef123456"',
      );
   });

   it("quotes each segment of a container path for a backtick dialect", async () => {
      const runSQL = sinon.stub().resolves();
      // Container path (dataset.table) on a backtick dialect (BigQuery): each
      // segment is quoted independently, and the rename targets the bare name.
      const entry = await callBuildOneSource(
         { runSQL },
         "ds.orders_v1",
         "standardsql",
      );

      const sql = runSQL.getCalls().map((c) => c.args[0] as string);
      expect(sql).toEqual([
         "DROP TABLE IF EXISTS `ds`.`orders_v1_abcdef123456`",
         "CREATE TABLE `ds`.`orders_v1_abcdef123456` AS (SELECT * FROM t)",
         "DROP TABLE IF EXISTS `ds`.`orders_v1`",
         "ALTER TABLE `ds`.`orders_v1_abcdef123456` RENAME TO `orders_v1`",
      ]);
      expect(entry.physicalTableName).toBe("ds.orders_v1");
   });

   it("colocated build excludes a storage upstream from its warehouse manifest (mixed chain)", async () => {
      // A colocated downstream that reads a storage-materialized
      // upstream must NOT get the lake table name in its warehouse build SQL —
      // the warehouse can't resolve it. The storage upstream is excluded (⇒
      // inlined/recomputed); a colocated upstream is kept. The build SQL is
      // the first render; the second is the permissive full-manifest render
      // the reuse classification compares it with.
      const runSQL = sinon.stub().resolves();
      type SeenManifest = {
         entries?: Record<string, { tableName?: string }>;
         strict?: boolean;
      };
      const seenManifests: (SeenManifest | undefined)[] = [];
      const down = fakeSource({
         name: "down",
         sourceEntityId: "downdowndowndown",
         sql: "SELECT 1",
         onGetSQL: (o) => {
            seenManifests.push(
               (o as { buildManifest?: SeenManifest }).buildManifest,
            );
         },
      });
      const manifest = new Manifest();
      manifest.update("up_storage", { tableName: "daily__mabc" });
      manifest.update("up_pathc", { tableName: '"orders_v1"' });
      const builtEntries = {
         up_storage: {
            sourceEntityId: "up_storage",
            physicalTableName: "daily__mabc",
            storageDestinationName: "lake",
         },
         up_pathc: {
            sourceEntityId: "up_pathc",
            physicalTableName: "orders_v1",
            connectionName: "duckdb",
         },
      };
      await (
         ctx.service as unknown as {
            buildOneSource: (...a: unknown[]) => Promise<unknown>;
         }
      ).buildOneSource(
         down,
         {
            sourceEntityId: "downdowndowndown",
            materializedTableId: "mt",
            physicalTableName: "down_v1",
            realization: "COPY",
         }, // no destination ⇒ colocated
         { runSQL },
         { duckdb: "dig" },
         manifest,
         {
            getApiConnection: () => {
               throw new Error("unused in the colocated build");
            },
            getEnvironmentPath: () => "/tmp/env",
         },
         builtEntries,
      );
      const [buildRender, compareRender] = seenManifests;
      expect(buildRender?.entries?.up_storage).toBeUndefined();
      expect(buildRender?.entries?.up_pathc?.tableName).toBe('"orders_v1"');
      expect(compareRender?.entries?.up_storage?.tableName).toBe("daily__mabc");
      expect(compareRender?.strict).toBe(false);
   });

   it("records the QUOTED just-built name in the build manifest (matches the CREATE)", async () => {
      // The build manifest feeds a downstream persist's FROM verbatim, so the
      // recorded name must be the SAME quoted path the CREATE used — else a
      // downstream built later in this run misses the case-preserved table on a
      // case-folding engine. buildManifest also passes through Malloy's canonical
      // path validation (requireCanonicalTablePathAnyDialect), so this proves the
      // quoted form is accepted, not just produced.
      const runSQL = sinon.stub().resolves();
      const manifest = new Manifest();
      await callBuildOneSource(
         { runSQL },
         "schema.orders_base",
         "snowflake",
         manifest,
      );
      expect(manifest.buildManifest.entries["abcdef1234567890"].tableName).toBe(
         '"schema"."orders_base"',
      );
      // The logical (unquoted) name is what the CREATE was told to produce, and
      // is exactly what quoteTablePath quoted for the DDL — read mirrors write.
      const created = runSQL
         .getCalls()
         .map((c) => c.args[0] as string)
         .find((s) => s.startsWith("CREATE TABLE"))!;
      expect(created).toContain('"schema"."orders_base_abcdef123456"');
   });
});

describe("buildOneSource reports upstreamReuse from the manifest it substituted from", () => {
   let ctx: ReturnType<typeof createMocks>;
   withQueryMetadataOn();
   beforeEach(() => {
      ctx = createMocks();
   });

   // The author model: `rollup` (the source being built) reads `daily`, a
   // persist source over a table.
   const modelDef = {
      contents: {
         orders: { sourceID: "orders@m", type: "table" },
         daily: {
            sourceID: "daily@m",
            type: "query_source",
            persistent: true,
            query: { structRef: "orders@m" },
         },
         rollup: {
            sourceID: "rollup@m",
            type: "query_source",
            persistent: true,
            query: { structRef: "daily@m" },
         },
      },
   };
   const planFacts = {
      persistSourceIds: new Set(["daily@m", "rollup@m"]),
      aliasesBySourceName: {},
      addressBySourceId: { "daily@m": "addr-daily", "rollup@m": "addr-rollup" },
      compilerKeyBySourceId: {
         "daily@m": "addr-daily",
         "rollup@m": "addr-rollup",
      },
   };

   async function build(
      manifest: Manifest,
      builtEntries: Record<string, unknown>,
      onGetSQL?: (sqlOpts: unknown) => void,
   ) {
      const source = fakeSource({
         name: "rollup",
         sourceEntityId: "addr-rollup",
         sql: "SELECT * FROM t",
         modelDef,
         onGetSQL,
      });
      const instruction: BuildInstruction = {
         sourceEntityId: "addr-rollup",
         materializedTableId: "mt-1",
         physicalTableName: "rollup_mz",
         realization: "COPY",
      };
      return (
         ctx.service as unknown as {
            buildOneSource: (...args: unknown[]) => Promise<{
               upstreamReuse?: string;
               upstreamRecomputeReason?: string;
            }>;
         }
      ).buildOneSource(
         source,
         instruction,
         { runSQL: sinon.stub().resolves(undefined) },
         { duckdb: "dig" },
         manifest,
         {
            getApiConnection: () => ({}),
            getEnvironmentPath: () => "/tmp/env",
         },
         builtEntries,
         undefined,
         undefined,
         undefined,
         planFacts,
      );
   }

   it("reused when the upstream's address is in the manifest, even as a thin reference with no name", async () => {
      const manifest = new Manifest();
      manifest.update("addr-daily", { tableName: "daily__g1" });
      const entry = await build(manifest, {
         "addr-daily": {
            sourceEntityId: "addr-daily",
            physicalTableName: "daily__g1",
            connectionName: "duckdb",
         },
      });
      expect(entry.upstreamReuse).toBe("reused");
      expect(entry.upstreamRecomputeReason).toBeUndefined();
   });

   it("recomputed, with the reason, when the upstream is in no manifest and the compiler's strict render misses it", async () => {
      const entry = await build(new Manifest(), {}, (sqlOpts) => {
         const m = (
            sqlOpts as {
               buildManifest?: {
                  strict?: boolean;
                  entries?: Record<string, unknown>;
               };
            }
         ).buildManifest;
         if (m?.strict === true && !("addr-daily" in (m.entries ?? {}))) {
            throw Object.assign(
               new Error(
                  "Persisted source 'daily@m' not found in manifest (buildId: addr-daily); strict manifest mode forbids fallback to live compilation.",
               ),
               { code: "runtime-manifest-strict-miss" },
            );
         }
      });
      expect(entry.upstreamReuse).toBe("recomputed");
      expect(entry.upstreamRecomputeReason).toMatch(
         /'daily' is not in this build's manifest/,
      );
   });
});
