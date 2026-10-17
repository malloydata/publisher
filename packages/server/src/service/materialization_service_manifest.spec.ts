// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { beforeEach, describe, expect, it } from "bun:test";
import * as sinon from "sinon";
import {
   MaterializationStatus,
   MaterializationUpdate,
} from "../storage/DatabaseInterface";
import { makeMaterialization } from "../test_helpers/materialization_test_fixtures";
import { createMocks } from "../test_helpers/materialization_service_harness";
import type { MaterializationService } from "./materialization_service";

/**
 * How a run's manifest is committed, loaded into the package models, and read
 * back as the previous run's entries.
 */

/** The private manifest methods these tests drive. */
function manifestMethods(service: MaterializationService): {
   autoLoadManifest: (
      env: unknown,
      pkg: string,
      entries: Record<string, unknown>,
   ) => Promise<void>;
   commitManifest: (
      id: string,
      entries: Record<string, unknown>,
      failures: Record<string, unknown>,
      metadata: Record<string, unknown>,
   ) => Promise<void>;
   getMostRecentManifestEntries: (
      envId: string,
      pkg: string,
      excludeId: string,
   ) => Promise<Record<string, unknown>>;
} {
   return service as unknown as ReturnType<typeof manifestMethods>;
}

let ctx: ReturnType<typeof createMocks>;
beforeEach(() => {
   ctx = createMocks();
});

describe("autoLoadManifest", () => {
   it("loads only entries with a physical table into the package models", async () => {
      const reloadAllModelsForPackage = sinon.stub().resolves();
      const entries = {
         b1: {
            sourceEntityId: "b1",
            physicalTableName: "orders_v1",
            connectionName: "duckdb",
         },
         b2: { sourceEntityId: "b2", physicalTableName: undefined },
      };

      await manifestMethods(ctx.service).autoLoadManifest(
         { reloadAllModelsForPackage },
         "pkg",
         entries,
      );

      expect(reloadAllModelsForPackage.calledOnce).toBe(true);
      // connectionName is carried alongside tableName so the bind step
      // (Package.quoteBoundTableNames) can quote the path for its dialect.
      expect(reloadAllModelsForPackage.firstCall.args).toEqual([
         "pkg",
         { b1: { tableName: "orders_v1", connectionName: "duckdb" } },
      ]);
   });

   it("does not bind a failed source mirrored into entries", async () => {
      // During the `ManifestEntry.error` deprecation window a failed source is
      // also written into `entries`, under the physical name it was headed for.
      // Auto-run names are stable, so that name is the previous generation's
      // table: binding it would serve stale rows as though this run built them.
      const reload = sinon.stub().resolves();
      const bindStorage = sinon.stub().resolves();
      const environment = {
         reloadAllModelsForPackage: reload,
         bindPackageStorageServeBindings: bindStorage,
      };

      await manifestMethods(ctx.service).autoLoadManifest(environment, "pkg", {
         ok: {
            sourceEntityId: "ok",
            sourceName: "healthy",
            physicalTableName: "ok_v1",
         },
         bad: {
            sourceEntityId: "bad",
            sourceName: "broken",
            physicalTableName: "bad",
            error: "Permission denied while writing to dataset analytics",
         },
      });

      expect(Object.keys(reload.firstCall.args[1])).toEqual(["ok"]);
      expect(Object.keys(bindStorage.firstCall.args[1])).toEqual(["ok"]);
   });

   it("swallows a reload failure (best-effort; run already succeeded)", async () => {
      const reloadAllModelsForPackage = sinon
         .stub()
         .rejects(new Error("reload boom"));
      await expect(
         manifestMethods(ctx.service).autoLoadManifest(
            { reloadAllModelsForPackage },
            "pkg",
            { b1: { sourceEntityId: "b1", physicalTableName: "t" } },
         ),
      ).resolves.toBeUndefined();
   });
});

describe("commitManifest", () => {
   beforeEach(() => {
      // commitManifest makes TWO transitions (PENDING -> MANIFEST_ROWS_READY ->
      // MANIFEST_FILE_READY) and each validates against the row's CURRENT status,
      // so the read has to advance as the writes land or the second hop is
      // rejected as invalid.
      let status: MaterializationStatus = "PENDING";
      ctx.repository.getMaterializationById.callsFake(async () =>
         makeMaterialization({ status }),
      );
      ctx.repository.updateMaterialization.callsFake(
         async (_id: string, update: MaterializationUpdate) => {
            if (update.status) status = update.status;
            return makeMaterialization({ status });
         },
      );
   });

   /** The manifest the run PERSISTED, read off the repository write. */
   function persistedManifest(): { entries?: unknown; failures?: unknown } {
      const call = ctx.repository.updateMaterialization
         .getCalls()
         .find((c) => c.args[1]?.manifest);
      return (call?.args[1] as { manifest: Record<string, unknown> }).manifest;
   }

   const healthy = {
      ok: {
         sourceEntityId: "ok",
         sourceName: "healthy",
         physicalTableName: "ok_v1",
      },
   };

   it("persists a failed source's reason on the manifest the control plane reads", async () => {
      // The reason only reaches a consumer if it is written into the stored
      // manifest. Everything upstream of this can be correct while the field is
      // dropped on the way to the store, which would leave a consumer inferring a
      // partial failure from an absent entry -- exactly the state this replaces.
      await manifestMethods(ctx.service).commitManifest(
         "mat-1",
         healthy,
         {
            bad: {
               sourceEntityId: "bad",
               sourceName: "broken",
               physicalTableName: "bad_v1",
               reason: "Permission denied while writing to dataset analytics",
            },
         },
         { mode: "auto" },
      );

      const manifest = persistedManifest();
      expect(Object.keys(manifest.entries as object)).toEqual(["ok"]);
      expect(
         manifest.failures,
         "the persisted manifest must carry the failure, or the reason never " +
            "reaches a consumer",
      ).toMatchObject({
         bad: {
            reason: "Permission denied while writing to dataset analytics",
         },
      });
   });

   it("omits the failures key entirely on a run where every source built", async () => {
      // A manifest recording no failures and one recording an empty set state the
      // same fact, and the absent key is the shape every manifest written before
      // this change has. Writing `{}` instead would make a clean run's manifest
      // differ from its own history for no gain.
      await manifestMethods(ctx.service).commitManifest(
         "mat-1",
         healthy,
         {},
         { mode: "auto" },
      );

      expect(persistedManifest()).not.toHaveProperty("failures");
   });
});

describe("getMostRecentManifestEntries", () => {
   it("returns entries from the most recent successful run, excluding the in-flight one", async () => {
      const entries = {
         b1: {
            sourceEntityId: "b1",
            physicalTableName: "orders_v1",
            connectionName: "duckdb",
         },
      };
      ctx.repository.listMaterializations.resolves([
         makeMaterialization({
            id: "in-flight",
            status: "MANIFEST_ROWS_READY",
         }),
         makeMaterialization({
            id: "old-success",
            status: "MANIFEST_FILE_READY",
            manifest: { builtAt: "t", strict: false, entries },
         }),
      ]);

      const result = await manifestMethods(
         ctx.service,
      ).getMostRecentManifestEntries("env-1", "pkg", "in-flight");

      expect(result).toEqual(entries);
   });

   it("returns {} when the only successful run is the excluded one", async () => {
      ctx.repository.listMaterializations.resolves([
         makeMaterialization({
            id: "self",
            status: "MANIFEST_FILE_READY",
            manifest: {
               builtAt: "t",
               strict: false,
               entries: {
                  b1: { sourceEntityId: "b1", physicalTableName: "t1" },
               },
            },
         }),
      ]);

      const result = await manifestMethods(
         ctx.service,
      ).getMostRecentManifestEntries("env-1", "pkg", "self");

      expect(result).toEqual({});
   });

   it("does not return a legacy failed entry from a persisted manifest", async () => {
      // This is the read boundary for reuse (skip-if-unchanged) and for reference
      // resolution. A manifest written by 0.0.245-0.0.246 records a failed source
      // among its entries, and returning one has two distinct consequences: it is
      // carried forward as a reused table, which retires the source from every
      // later run so a transient warehouse error is never retried; and it seeds a
      // downstream FROM against a table that was never created. Delete this test
      // with the field it tolerates.
      ctx.repository.listMaterializations.resolves([
         makeMaterialization({
            id: "mat-prior",
            status: "MANIFEST_FILE_READY",
            manifest: {
               entries: {
                  healthy: {
                     sourceEntityId: "healthy",
                     sourceName: "good",
                     physicalTableName: "good_v1",
                     connectionName: "duckdb",
                  },
                  legacyFailed: {
                     sourceEntityId: "legacyFailed",
                     sourceName: "broken",
                     physicalTableName: "broken_v1_prior_generation",
                     connectionName: "duckdb",
                     error: "Permission denied while writing to dataset analytics",
                  },
               },
            },
         }),
      ]);

      const entries = await manifestMethods(
         ctx.service,
      ).getMostRecentManifestEntries("env-1", "pkg", "mat-current");

      expect(Object.keys(entries)).toEqual(["healthy"]);
   });
});
