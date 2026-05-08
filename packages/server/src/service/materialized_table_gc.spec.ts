import type { Connection } from "@malloydata/malloy";
import { beforeEach, describe, expect, it } from "bun:test";
import * as sinon from "sinon";
import { ManifestEntry } from "../storage/DatabaseInterface";
import { ManifestService } from "./manifest_service";
import { stagingSuffix } from "./materialization_service";
import { dropManifestEntries, liveTableKey } from "./materialized_table_gc";

function makeEntry(overrides: Partial<ManifestEntry> = {}): ManifestEntry {
   return {
      id: "entry-1",
      environmentId: "proj-1",
      packageName: "pkg",
      buildId: "abcdef1234567890abcdef1234567890",
      tableName: "my_table",
      sourceName: "my_source",
      connectionName: "conn",
      createdAt: new Date(),
      updatedAt: new Date(),
      ...overrides,
   };
}

interface MockConnection {
   dialectName: string;
   runSQL: sinon.SinonStub;
}

function makeConnection(dialectName = "duckdb"): MockConnection {
   return {
      dialectName,
      runSQL: sinon.stub().resolves(),
   };
}

interface TestCtx {
   connections: Map<string, Connection>;
   manifestService: sinon.SinonStubbedInstance<ManifestService>;
   conn: MockConnection;
}

function makeCtx(dialectName = "duckdb"): TestCtx {
   const conn = makeConnection(dialectName);
   const connections = new Map<string, Connection>();
   connections.set("conn", conn as unknown as Connection);
   const manifestService = {
      deleteEntry: sinon.stub().resolves(),
      listEntries: sinon.stub().resolves([]),
      getManifest: sinon.stub().resolves({ entries: {}, strict: false }),
      writeEntry: sinon.stub().resolves(),
      reloadManifest: sinon.stub().resolves({ entries: {}, strict: false }),
   } as unknown as sinon.SinonStubbedInstance<ManifestService>;
   return { connections, manifestService, conn };
}

describe("dropManifestEntries", () => {
   let ctx: TestCtx;

   beforeEach(() => {
      ctx = makeCtx();
   });

   it("deletes the manifest row first, then drops the target and staging companion", async () => {
      const entry = makeEntry();
      const deleteEntry = ctx.manifestService.deleteEntry as sinon.SinonStub;

      const result = await dropManifestEntries([entry], {
         connections: ctx.connections,
         manifestService: ctx.manifestService,
         environmentId: "proj-1",
      });

      expect(result.dropped).toHaveLength(1);
      expect(result.errors).toHaveLength(0);
      expect(result.dropped[0].tableName).toBe("my_table");
      expect(result.dropped[0].stagingTableName).toBe(
         `my_table${stagingSuffix(entry.buildId)}`,
      );

      expect(deleteEntry.calledOnceWith("proj-1", entry.id)).toBe(true);

      expect(ctx.conn.runSQL.callCount).toBe(2);
      const targetSql = ctx.conn.runSQL.getCall(0).args[0];
      const stagingSql = ctx.conn.runSQL.getCall(1).args[0];
      expect(targetSql).toMatch(/^DROP TABLE IF EXISTS /);
      expect(targetSql).toContain("my_table");
      expect(stagingSql).toMatch(/^DROP TABLE IF EXISTS /);
      expect(stagingSql).toContain(`my_table${stagingSuffix(entry.buildId)}`);

      // Ordering: manifest row must be deleted before any physical DROP, so
      // we never leave a manifest row pointing at a missing table.
      expect(
         deleteEntry.getCall(0).calledBefore(ctx.conn.runSQL.getCall(0)),
      ).toBe(true);
   });

   it("records an error and skips physical DROPs when manifest delete fails", async () => {
      const entry = makeEntry();
      const deleteEntry = ctx.manifestService.deleteEntry as sinon.SinonStub;
      deleteEntry.rejects(new Error("manifest store down"));

      const result = await dropManifestEntries([entry], {
         connections: ctx.connections,
         manifestService: ctx.manifestService,
         environmentId: "proj-1",
      });

      expect(result.dropped).toHaveLength(0);
      expect(result.errors).toHaveLength(1);
      expect(result.errors[0].error).toContain("manifest store down");
      // No physical DROP must fire — we still need the table so the source
      // remains queryable while GC retries on a later pass.
      expect(ctx.conn.runSQL.called).toBe(false);
   });

   it("still counts as dropped when the target DROP fails after manifest delete (orphan warned, not retried)", async () => {
      const entry = makeEntry();
      ctx.conn.runSQL
         .onFirstCall()
         .rejects(new Error("permission denied on relation my_table"));

      const result = await dropManifestEntries([entry], {
         connections: ctx.connections,
         manifestService: ctx.manifestService,
         environmentId: "proj-1",
      });

      // Manifest row is gone → entry is authoritatively dropped. The
      // physical target becomes an orphan (logged as a warning); we do NOT
      // push to errors because there is nothing the caller can retry —
      // the manifest row no longer exists to drive another GC pass.
      expect(result.dropped).toHaveLength(1);
      expect(result.errors).toHaveLength(0);
      expect(
         (ctx.manifestService.deleteEntry as sinon.SinonStub).calledOnce,
      ).toBe(true);
      // Staging DROP still attempted for completeness.
      expect(ctx.conn.runSQL.callCount).toBe(2);
   });

   it("still counts as dropped when only the staging DROP fails (best-effort)", async () => {
      const entry = makeEntry();
      ctx.conn.runSQL.onFirstCall().resolves();
      ctx.conn.runSQL.onSecondCall().rejects(new Error("staging locked"));

      const result = await dropManifestEntries([entry], {
         connections: ctx.connections,
         manifestService: ctx.manifestService,
         environmentId: "proj-1",
      });

      expect(result.dropped).toHaveLength(1);
      expect(result.errors).toHaveLength(0);
      expect(
         (ctx.manifestService.deleteEntry as sinon.SinonStub).calledOnce,
      ).toBe(true);
   });

   it("skips DROP and records an error when the connection is missing", async () => {
      const entry = makeEntry({ connectionName: "ghost" });

      const result = await dropManifestEntries([entry], {
         connections: ctx.connections,
         manifestService: ctx.manifestService,
         environmentId: "proj-1",
      });

      expect(result.dropped).toHaveLength(0);
      expect(result.errors).toHaveLength(1);
      expect(result.errors[0].error).toMatch(/Connection 'ghost'/);
      expect(ctx.conn.runSQL.called).toBe(false);
      expect((ctx.manifestService.deleteEntry as sinon.SinonStub).called).toBe(
         false,
      );
   });

   it("skips DROP and records an error when the dialect is unknown", async () => {
      const ctx2 = makeCtx("martian-sql");
      const entry = makeEntry();

      const result = await dropManifestEntries([entry], {
         connections: ctx2.connections,
         manifestService: ctx2.manifestService,
         environmentId: "proj-1",
      });

      expect(result.dropped).toHaveLength(0);
      expect(result.errors).toHaveLength(1);
      expect(result.errors[0].error).toMatch(/martian-sql/);
      expect(ctx2.conn.runSQL.called).toBe(false);
   });

   it("dryRun lists what would drop without issuing SQL or deleting rows", async () => {
      const entries = [
         makeEntry(),
         makeEntry({ id: "entry-2", tableName: "schema.other" }),
      ];

      const result = await dropManifestEntries(entries, {
         connections: ctx.connections,
         manifestService: ctx.manifestService,
         environmentId: "proj-1",
         dryRun: true,
      });

      expect(result.dropped).toHaveLength(2);
      expect(result.errors).toHaveLength(0);
      expect(ctx.conn.runSQL.called).toBe(false);
      expect((ctx.manifestService.deleteEntry as sinon.SinonStub).called).toBe(
         false,
      );
   });

   it("isolates per-entry failures — other entries still drop", async () => {
      const good = makeEntry({ id: "good", tableName: "good_table" });
      const bad = makeEntry({
         id: "bad",
         tableName: "bad_table",
         connectionName: "ghost",
      });

      const result = await dropManifestEntries([bad, good], {
         connections: ctx.connections,
         manifestService: ctx.manifestService,
         environmentId: "proj-1",
      });

      expect(result.dropped).toHaveLength(1);
      expect(result.dropped[0].tableName).toBe("good_table");
      expect(result.errors).toHaveLength(1);
      expect(result.errors[0].tableName).toBe("bad_table");
   });

   // When a source is rebuilt with new SQL it gets a new BuildID but keeps
   // the same tableName. Without `liveTables`, GC would drop the stale
   // (old BuildID) manifest row and then issue `DROP TABLE IF EXISTS
   // <tableName>` — obliterating the fresh physical target produced by
   // the rebuild. `liveTables` short-circuits the physical DROP when
   // another active entry still claims the same (connection, tableName).
   describe("liveTables (rebuild safety)", () => {
      it("skips the target DROP when another active entry claims the same (connection, tableName)", async () => {
         const staleEntry = makeEntry({
            id: "stale",
            buildId: "old_buildid_000000000000000000000",
            tableName: "my_table",
            connectionName: "conn",
         });
         const live = new Set<string>([liveTableKey("conn", "my_table")]);

         const result = await dropManifestEntries([staleEntry], {
            connections: ctx.connections,
            manifestService: ctx.manifestService,
            environmentId: "proj-1",
            liveTables: live,
         });

         expect(result.dropped).toHaveLength(1);
         expect(result.dropped[0].targetDropSkipped).toBe(true);
         expect(result.errors).toHaveLength(0);

         expect(
            (ctx.manifestService.deleteEntry as sinon.SinonStub).calledOnce,
         ).toBe(true);

         // The target DROP must NOT fire; only the staging best-effort drop
         // should go through.
         expect(ctx.conn.runSQL.callCount).toBe(1);
         const only = ctx.conn.runSQL.getCall(0).args[0];
         expect(only).toMatch(/^DROP TABLE IF EXISTS /);
         expect(only).toContain(`my_table${stagingSuffix(staleEntry.buildId)}`);
         expect(only).not.toMatch(/my_table\b(?!_)/);
      });

      it("still drops the target when liveTables claims a different connection", async () => {
         const staleEntry = makeEntry({ tableName: "my_table" });
         // Different connection with the same tableName is not the same
         // physical table; the DROP must proceed.
         const live = new Set<string>([liveTableKey("other_conn", "my_table")]);

         const result = await dropManifestEntries([staleEntry], {
            connections: ctx.connections,
            manifestService: ctx.manifestService,
            environmentId: "proj-1",
            liveTables: live,
         });

         expect(result.dropped).toHaveLength(1);
         expect(result.dropped[0].targetDropSkipped).toBeUndefined();
         expect(ctx.conn.runSQL.callCount).toBe(2);
      });

      it("dryRun reports would-skip via targetDropSkipped", async () => {
         const staleEntry = makeEntry();
         const live = new Set<string>([liveTableKey("conn", "my_table")]);

         const result = await dropManifestEntries([staleEntry], {
            connections: ctx.connections,
            manifestService: ctx.manifestService,
            environmentId: "proj-1",
            dryRun: true,
            liveTables: live,
         });

         expect(result.dropped).toHaveLength(1);
         expect(result.dropped[0].targetDropSkipped).toBe(true);
         expect(ctx.conn.runSQL.called).toBe(false);
         expect(
            (ctx.manifestService.deleteEntry as sinon.SinonStub).called,
         ).toBe(false);
      });
   });

   describe("forceDeleteRowOnMissingConnection (teardown)", () => {
      it("deletes the manifest row without any DROP when the connection is gone", async () => {
         const entry = makeEntry({ connectionName: "ghost" });

         const result = await dropManifestEntries([entry], {
            connections: ctx.connections,
            manifestService: ctx.manifestService,
            environmentId: "proj-1",
            forceDeleteRowOnMissingConnection: true,
         });

         expect(result.dropped).toHaveLength(1);
         expect(result.dropped[0].targetDropSkipped).toBe(true);
         expect(result.errors).toHaveLength(0);
         expect(
            (ctx.manifestService.deleteEntry as sinon.SinonStub).calledOnce,
         ).toBe(true);
         expect(ctx.conn.runSQL.called).toBe(false);
      });

      it("records an error if the manifest-row delete itself fails", async () => {
         const entry = makeEntry({ connectionName: "ghost" });
         (ctx.manifestService.deleteEntry as sinon.SinonStub).rejects(
            new Error("manifest store down"),
         );

         const result = await dropManifestEntries([entry], {
            connections: ctx.connections,
            manifestService: ctx.manifestService,
            environmentId: "proj-1",
            forceDeleteRowOnMissingConnection: true,
         });

         expect(result.dropped).toHaveLength(0);
         expect(result.errors).toHaveLength(1);
         expect(result.errors[0].error).toContain("manifest store down");
      });

      it("falls back to the error path in dryRun (keeps dryRun side-effect-free)", async () => {
         const entry = makeEntry({ connectionName: "ghost" });

         const result = await dropManifestEntries([entry], {
            connections: ctx.connections,
            manifestService: ctx.manifestService,
            environmentId: "proj-1",
            forceDeleteRowOnMissingConnection: true,
            dryRun: true,
         });

         // In dryRun the row is NOT deleted; surface the same error signal
         // so operators see the connection gap they'd hit on a live run.
         expect(result.dropped).toHaveLength(0);
         expect(result.errors).toHaveLength(1);
         expect(result.errors[0].error).toMatch(/ghost/);
         expect(
            (ctx.manifestService.deleteEntry as sinon.SinonStub).called,
         ).toBe(false);
      });
   });
});

describe("stagingSuffix", () => {
   it("uses a fixed-length prefix of the BuildID", () => {
      const suffix = stagingSuffix(
         "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
      );
      // 12 hex chars + the "_" prefix = 13 chars total.
      expect(suffix).toBe("_0123456789ab");
      expect(suffix.length).toBe(13);
   });
});
