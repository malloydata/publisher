import { beforeEach, describe, expect, it } from "bun:test";
import * as sinon from "sinon";
import { ManifestEntry, ResourceRepository } from "../DatabaseInterface";
import { DuckDBManifestStore } from "./DuckDBManifestStore";

function makeEntry(overrides: Partial<ManifestEntry> = {}): ManifestEntry {
   return {
      id: "entry-1",
      projectId: "proj-1",
      packageName: "pkg",
      buildId: "build-abc",
      tableName: "my_table",
      sourceName: "my_source",
      connectionName: "duckdb",
      createdAt: new Date("2026-04-03"),
      updatedAt: new Date("2026-04-03"),
      ...overrides,
   };
}

function createMocks() {
   const sandbox = sinon.createSandbox();

   const repository: sinon.SinonStubbedInstance<
      Pick<
         ResourceRepository,
         | "listManifestEntries"
         | "upsertManifestEntry"
         | "getManifestEntryBySourceName"
         | "deleteManifestEntry"
      >
   > = {
      listManifestEntries: sandbox.stub(),
      upsertManifestEntry: sandbox.stub(),
      getManifestEntryBySourceName: sandbox.stub(),
      deleteManifestEntry: sandbox.stub(),
   };

   const store = new DuckDBManifestStore(
      repository as unknown as ResourceRepository,
   );

   return { sandbox, repository, store };
}

describe("DuckDBManifestStore", () => {
   let ctx: ReturnType<typeof createMocks>;

   beforeEach(() => {
      ctx = createMocks();
   });

   describe("getManifest", () => {
      it("should assemble a BuildManifest from repository entries", async () => {
         ctx.repository.listManifestEntries.resolves([
            makeEntry({ buildId: "b1", tableName: "tbl_a" }),
            makeEntry({ buildId: "b2", tableName: "tbl_b" }),
         ]);

         const manifest = await ctx.store.getManifest("proj-1", "pkg");

         expect(manifest.strict).toBe(false);
         expect(manifest.entries).toEqual({
            b1: { tableName: "tbl_a" },
            b2: { tableName: "tbl_b" },
         });
      });

      it("should return empty entries when no rows exist", async () => {
         ctx.repository.listManifestEntries.resolves([]);

         const manifest = await ctx.store.getManifest("proj-1", "pkg");

         expect(manifest.strict).toBe(false);
         expect(manifest.entries).toEqual({});
      });
   });

   describe("writeEntry", () => {
      it("should upsert an entry with all fields", async () => {
         ctx.repository.upsertManifestEntry.resolves(makeEntry());

         await ctx.store.writeEntry("proj-1", "pkg", "build-abc", {
            tableName: "tbl",
            sourceName: "src",
            connectionName: "conn",
         });

         expect(ctx.repository.upsertManifestEntry.calledOnce).toBe(true);
         const arg = ctx.repository.upsertManifestEntry.firstCall.args[0];
         expect(arg).toEqual({
            projectId: "proj-1",
            packageName: "pkg",
            buildId: "build-abc",
            tableName: "tbl",
            sourceName: "src",
            connectionName: "conn",
         });
      });

      it("should default sourceName and connectionName to null when omitted", async () => {
         ctx.repository.upsertManifestEntry.resolves(makeEntry());

         await ctx.store.writeEntry("proj-1", "pkg", "build-abc", {
            tableName: "tbl",
         });

         const arg = ctx.repository.upsertManifestEntry.firstCall.args[0];
         expect(arg.sourceName).toBeNull();
         expect(arg.connectionName).toBeNull();
      });
   });

   describe("getEntryBySourceName", () => {
      it("should delegate to repository", async () => {
         const entry = makeEntry();
         ctx.repository.getManifestEntryBySourceName.resolves(entry);

         const result = await ctx.store.getEntryBySourceName(
            "proj-1",
            "pkg",
            "my_source",
         );

         expect(result).toEqual(entry);
         expect(ctx.repository.getManifestEntryBySourceName.calledOnce).toBe(
            true,
         );
      });

      it("should return null when no entry exists", async () => {
         ctx.repository.getManifestEntryBySourceName.resolves(null);

         const result = await ctx.store.getEntryBySourceName(
            "proj-1",
            "pkg",
            "missing",
         );

         expect(result).toBeNull();
      });
   });

   describe("deleteEntry", () => {
      it("should delegate to repository", async () => {
         ctx.repository.deleteManifestEntry.resolves();

         await ctx.store.deleteEntry("entry-1");

         expect(ctx.repository.deleteManifestEntry.calledOnce).toBe(true);
         expect(ctx.repository.deleteManifestEntry.firstCall.args[0]).toBe(
            "entry-1",
         );
      });
   });
});
