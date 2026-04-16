import { beforeEach, describe, expect, it } from "bun:test";
import * as sinon from "sinon";
import {
   BuildManifest,
   ManifestEntry,
   ManifestStore,
   ResourceRepository,
} from "../storage/DatabaseInterface";
import { ManifestService } from "./manifest_service";
import { ProjectStore } from "./project_store";

function makeEntry(overrides: Partial<ManifestEntry> = {}): ManifestEntry {
   return {
      id: "entry-1",
      projectId: "proj-1",
      packageName: "pkg",
      buildId: "build-abc",
      tableName: "my_table",
      sourceName: "my_source",
      connectionName: "duckdb",
      createdAt: new Date("2025-01-01"),
      updatedAt: new Date("2025-01-01"),
      ...overrides,
   };
}

function createMocks() {
   const sandbox = sinon.createSandbox();

   const manifestStore: sinon.SinonStubbedInstance<ManifestStore> = {
      getManifest: sandbox.stub(),
      writeEntry: sandbox.stub(),
      deleteEntry: sandbox.stub(),
      listEntries: sandbox.stub(),
   };

   const repository = {
      listManifestEntries: sandbox.stub(),
   } as unknown as sinon.SinonStubbedInstance<ResourceRepository>;

   const reloadAllModels = sandbox.stub().resolves();

   const pkg = {
      reloadAllModels,
   };

   const project = {
      getPackage: sandbox.stub().resolves(pkg),
   };

   const projectStore = {
      storageManager: {
         getManifestStore: (_projectId?: string) => manifestStore,
         getRepository: () => repository,
      },
      getProject: sandbox.stub().resolves(project),
   } as unknown as ProjectStore;

   const service = new ManifestService(projectStore);

   return {
      sandbox,
      manifestStore,
      repository,
      projectStore,
      project,
      pkg,
      reloadAllModels,
      service,
   };
}

describe("ManifestService", () => {
   let ctx: ReturnType<typeof createMocks>;

   beforeEach(() => {
      ctx = createMocks();
   });

   describe("getManifest", () => {
      it("should delegate to the manifest store", async () => {
         const manifest: BuildManifest = {
            entries: { "build-abc": { tableName: "tbl" } },
            strict: false,
         };
         ctx.manifestStore.getManifest.resolves(manifest);

         const result = await ctx.service.getManifest("proj-1", "pkg");

         expect(result).toEqual(manifest);
         expect(ctx.manifestStore.getManifest.calledWith("proj-1", "pkg")).toBe(
            true,
         );
      });
   });

   describe("writeEntry", () => {
      it("should delegate to the manifest store", async () => {
         ctx.manifestStore.writeEntry.resolves();

         await ctx.service.writeEntry(
            "proj-1",
            "pkg",
            "build-abc",
            "tbl",
            "src",
            "duckdb",
         );

         expect(ctx.manifestStore.writeEntry.calledOnce).toBe(true);
         const args = ctx.manifestStore.writeEntry.firstCall.args;
         expect(args[0]).toBe("proj-1");
         expect(args[1]).toBe("pkg");
         expect(args[2]).toBe("build-abc");
         expect(args[3]).toBe("tbl");
         expect(args[4]).toBe("src");
         expect(args[5]).toBe("duckdb");
      });
   });

   describe("deleteEntry", () => {
      it("should delegate to the manifest store", async () => {
         ctx.manifestStore.deleteEntry.resolves();

         await ctx.service.deleteEntry("proj-1", "entry-1");

         expect(ctx.manifestStore.deleteEntry.calledOnce).toBe(true);
         expect(ctx.manifestStore.deleteEntry.firstCall.args[0]).toBe(
            "entry-1",
         );
      });
   });

   describe("reloadManifest", () => {
      it("should get manifest, reload it onto the package, and return it", async () => {
         const manifest: BuildManifest = {
            entries: { "build-abc": { tableName: "tbl" } },
            strict: false,
         };
         ctx.manifestStore.getManifest.resolves(manifest);

         const result = await ctx.service.reloadManifest(
            "proj-1",
            "pkg",
            "my-project",
         );

         expect(result).toEqual(manifest);
         expect(
            (ctx.projectStore.getProject as sinon.SinonStub).calledWith(
               "my-project",
               false,
            ),
         ).toBe(true);
         expect(ctx.project.getPackage.calledWith("pkg", false)).toBe(true);
         expect(ctx.reloadAllModels.calledWith(manifest.entries)).toBe(true);
      });

      it("should return an empty manifest when no entries exist", async () => {
         const emptyManifest: BuildManifest = {
            entries: {},
            strict: false,
         };
         ctx.manifestStore.getManifest.resolves(emptyManifest);

         const result = await ctx.service.reloadManifest(
            "proj-1",
            "pkg",
            "my-project",
         );

         expect(result.entries).toEqual({});
         expect(ctx.reloadAllModels.calledWith({})).toBe(true);
      });
   });

   describe("listEntries", () => {
      it("should return entries from the manifest store", async () => {
         const entries = [
            makeEntry(),
            makeEntry({ id: "entry-2", buildId: "build-def" }),
         ];
         ctx.manifestStore.listEntries.resolves(entries);

         const result = await ctx.service.listEntries("proj-1", "pkg");

         expect(result).toEqual(entries);
         expect(ctx.manifestStore.listEntries.calledWith("proj-1", "pkg")).toBe(
            true,
         );
      });

      it("should return empty array when no entries exist", async () => {
         ctx.manifestStore.listEntries.resolves([]);

         const result = await ctx.service.listEntries("proj-1", "pkg");

         expect(result).toEqual([]);
      });
   });
});
