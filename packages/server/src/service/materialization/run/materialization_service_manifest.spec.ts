// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { beforeEach, describe, expect, it, spyOn } from "bun:test";
import * as metrics from "../../../materialization_metrics";
import { MaterializationUpdate } from "../../../storage/DatabaseInterface";
import { MaterializationStatus } from "../domain/types";
import { makeMaterialization } from "../../../test_helpers/materialization_test_fixtures";
import { createMocks } from "../../../test_helpers/materialization_service_harness";
import type { MaterializationService } from "./materialization_service";
import { rebindsOwnVersionAfter } from "./manifest_committer";

/**
 * How a run's manifest is committed, loaded into the package models, and read
 * back as the previous run's entries.
 */

/** The manifest steps these tests drive, typed loosely for their fakes. */
function manifestMethods(service: MaterializationService): {
   autoLoadManifest: (
      env: unknown,
      pkg: string,
      entries: Record<string, unknown>,
      versionId?: string,
      rollupKeys?: unknown,
      finish?: (served: Served) => Promise<void>,
   ) => Promise<void>;
   commitManifest: (
      id: string,
      entries: Record<string, unknown>,
      failures: Record<string, unknown>,
      metadata: Record<string, unknown>,
      serve?: (finish: (served: Served) => Promise<void>) => Promise<void>,
      serveTimeoutMs?: number,
   ) => Promise<void>;
} {
   return service.collaborators as unknown as ReturnType<
      typeof manifestMethods
   >;
}

/** The committer's skip-if-unchanged read, which the service keeps private. */
function mostRecentEntries(
   service: MaterializationService,
): (
   envId: string,
   pkg: string,
   excludeId: string,
) => Promise<Record<string, unknown>> {
   const { committer } = service as unknown as {
      committer: {
         mostRecentEntries: (
            envId: string,
            pkg: string,
            excludeId: string,
         ) => Promise<Record<string, unknown>>;
      };
   };
   return committer.mostRecentEntries.bind(committer);
}

interface Served {
   metadata: Record<string, unknown>;
   error?: string;
}

let ctx: ReturnType<typeof createMocks>;
beforeEach(() => {
   ctx = createMocks();
});

/**
 * An environment whose load reports `outcome`, recording what it was asked to
 * load and running the commit as the real one does (under its lock, after the
 * load).
 */
function loader(
   outcome:
      | { loaded: true }
      | { loaded: false; step: "reload" | "bind"; error: string } = {
      loaded: true,
   },
) {
   const calls: {
      packageName: string;
      colocated: Record<string, unknown>;
      storage: Record<string, unknown>;
   }[] = [];
   return {
      calls,
      env: {
         loadRunManifest: async (params: {
            packageName: string;
            colocated: Record<string, unknown>;
            storage: Record<string, unknown>;
            commit: (o: typeof outcome) => Promise<void>;
         }) => {
            calls.push(params);
            await params.commit(outcome);
            return outcome;
         },
      },
   };
}

describe("autoLoadManifest", () => {
   it("loads only entries with a physical table into the package models", async () => {
      const { env, calls } = loader();
      await manifestMethods(ctx.service).autoLoadManifest(env, "pkg", {
         b1: {
            sourceEntityId: "b1",
            physicalTableName: "orders_v1",
            connectionName: "duckdb",
         },
         b2: { sourceEntityId: "b2", physicalTableName: undefined },
      });
      expect(calls).toHaveLength(1);
      // connectionName is carried alongside tableName so the bind step
      // (Package.quoteBoundTableNames) can quote the path for its dialect.
      expect(calls[0].packageName).toBe("pkg");
      expect(calls[0].colocated).toEqual({
         b1: { tableName: "orders_v1", connectionName: "duckdb" },
      });
   });

   it("does not bind a failed source mirrored into entries", async () => {
      // During the `ManifestEntry.error` deprecation window a failed source is
      // also written into `entries`, under the physical name it was headed for.
      // Auto-run names are stable, so that name is the previous generation's
      // table: binding it would serve stale rows as though this run built them.
      const { env, calls } = loader();
      await manifestMethods(ctx.service).autoLoadManifest(env, "pkg", {
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
      expect(Object.keys(calls[0].colocated)).toEqual(["ok"]);
      expect(Object.keys(calls[0].storage)).toEqual(["ok"]);
   });

   it("finishes the run with the load that took", async () => {
      const finished: Served[] = [];
      await manifestMethods(ctx.service).autoLoadManifest(
         loader().env,
         "pkg",
         { b1: { sourceEntityId: "b1", physicalTableName: "t" } },
         undefined,
         undefined,
         async (served) => {
            finished.push(served);
         },
      );
      expect(finished).toEqual([{ metadata: { autoLoad: "loaded" } }]);
   });

   it.each(["reload", "bind"] as const)(
      "finishes the run with the %s step that failed, and its error (the tables were built)",
      async (step) => {
         const finished: Served[] = [];
         await manifestMethods(ctx.service).autoLoadManifest(
            loader({ loaded: false, step, error: "boom" }).env,
            "pkg",
            { b1: { sourceEntityId: "b1", physicalTableName: "t" } },
            undefined,
            undefined,
            async (served) => {
               finished.push(served);
            },
         );
         expect(finished).toHaveLength(1);
         expect(finished[0].metadata).toEqual({
            autoLoad: "failed",
            autoLoadStep: step,
            autoLoadError: "boom",
         });
         expect(finished[0].error).toContain(
            `Built, but not served (${step}): boom`,
         );
      },
   );
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

   function writes(): {
      status?: string;
      manifest?: unknown;
      metadata?: Record<string, unknown>;
      error?: string;
   }[] {
      return ctx.repository.updateMaterialization
         .getCalls()
         .map((c) => c.args[1] as never);
   }

   it("serves before it reports MANIFEST_FILE_READY, with the manifest recorded, and records how the load went", async () => {
      const seen: {
         status?: string;
         manifest?: unknown;
         metadata?: unknown;
      }[] = [];
      await manifestMethods(ctx.service).commitManifest(
         "mat-1",
         healthy,
         {},
         { mode: "auto" },
         async (finish) => {
            const w = writes();
            seen.push(w[w.length - 1]);
            await finish({ metadata: { autoLoad: "loaded" } });
         },
      );
      // While serving: built and recorded, not yet done; the metadata is not
      // rewritten, so the run's table claims hold through the load.
      expect(seen).toHaveLength(1);
      expect(seen[0].status).toBe("MANIFEST_ROWS_READY");
      expect(seen[0].manifest).toMatchObject({ entries: healthy });
      expect(seen[0].metadata).toBeUndefined();
      // Then done, once, with the load's outcome on the metadata.
      const final = writes().filter((w) => w.status === "MANIFEST_FILE_READY");
      expect(final).toHaveLength(1);
      expect(final[0].metadata).toEqual({ mode: "auto", autoLoad: "loaded" });
   });

   it("keeps a superseded-table mark another run left on the record during the load", async () => {
      // Another run marks this one while it loads.
      let status: MaterializationStatus = "PENDING";
      ctx.repository.getMaterializationById.callsFake(async () =>
         makeMaterialization({
            status,
            metadata:
               status === "MANIFEST_ROWS_READY"
                  ? { mode: "auto", supersededTables: ["ok_v1"] }
                  : { mode: "auto" },
         }),
      );
      ctx.repository.updateMaterialization.callsFake(
         async (_id: string, update: MaterializationUpdate) => {
            if (update.status) status = update.status;
            return makeMaterialization({ status });
         },
      );
      await manifestMethods(ctx.service).commitManifest(
         "mat-1",
         healthy,
         {},
         { mode: "auto" },
         async (finish) => finish({ metadata: { autoLoad: "loaded" } }),
      );
      const final = writes().find((w) => w.status === "MANIFEST_FILE_READY");
      expect(final?.metadata).toEqual({
         mode: "auto",
         supersededTables: ["ok_v1"],
         autoLoad: "loaded",
      });
   });

   it("settles a load that throws as not served, with the record's error", async () => {
      await manifestMethods(ctx.service).commitManifest(
         "mat-1",
         healthy,
         {},
         { mode: "auto" },
         async () => {
            throw new Error("the lock broke");
         },
      );
      const final = writes().find((w) => w.status === "MANIFEST_FILE_READY");
      expect(final?.metadata).toMatchObject({
         autoLoad: "failed",
         autoLoadError: "the lock broke",
      });
      expect(final?.error).toContain("Built, but not served");
   });

   it("rethrows a final write that fails, so the run settles FAILED rather than staying MANIFEST_ROWS_READY", async () => {
      let status = "PENDING" as MaterializationStatus;
      ctx.repository.getMaterializationById.callsFake(async () =>
         makeMaterialization({ status }),
      );
      ctx.repository.updateMaterialization.callsFake(
         async (_id: string, update: MaterializationUpdate) => {
            if (update.status === "MANIFEST_FILE_READY") {
               throw new Error("disk full");
            }
            if (update.status) status = update.status;
            return makeMaterialization({ status });
         },
      );
      // Through the load's own finish, and with no load at all.
      for (const serve of [
         async (finish: (s: Served) => Promise<void>) =>
            finish({ metadata: { autoLoad: "loaded" } }),
         undefined,
      ]) {
         status = "PENDING";
         await expect(
            manifestMethods(ctx.service).commitManifest(
               "mat-1",
               healthy,
               {},
               { mode: "auto" },
               serve,
            ),
         ).rejects.toThrow("disk full");
         // Left for the run's catch, which moves it to FAILED.
         expect(status as string).toBe("MANIFEST_ROWS_READY");
      }
   });

   it("counts a load's outcome once: a timed-out load's late success is not counted", async () => {
      const counted = spyOn(metrics, "recordAutoLoadOutcome");
      try {
         let lateFinish: ((s: Served) => Promise<void>) | undefined;
         await manifestMethods(ctx.service).commitManifest(
            "mat-1",
            healthy,
            {},
            { mode: "auto" },
            (finish) => {
               lateFinish = finish;
               return new Promise<void>(() => {});
            },
            20,
         );
         await lateFinish?.({ metadata: { autoLoad: "loaded" } });
         expect(counted.mock.calls).toEqual([["failure"]]);
      } finally {
         counted.mockRestore();
      }
   });

   it("settles a load that hangs as timed out, and ignores its late finish", async () => {
      let lateFinish: ((s: Served) => Promise<void>) | undefined;
      await manifestMethods(ctx.service).commitManifest(
         "mat-1",
         healthy,
         {},
         { mode: "auto" },
         (finish) => {
            lateFinish = finish;
            return new Promise<void>(() => {});
         },
         20,
      );
      const finals = () =>
         writes().filter((w) => w.status === "MANIFEST_FILE_READY");
      expect(finals()).toHaveLength(1);
      expect(finals()[0].metadata).toMatchObject({
         autoLoad: "failed",
         autoLoadStep: "timeout",
      });
      await lateFinish?.({ metadata: { autoLoad: "loaded" } });
      expect(finals()).toHaveLength(1);
   });
});

describe("rebindsOwnVersionAfter", () => {
   it("rebinds after a load that took, failed or was not run, but not after one that timed out", () => {
      expect(rebindsOwnVersionAfter({ autoLoad: "loaded" })).toBe(true);
      expect(
         rebindsOwnVersionAfter({ autoLoad: "failed", autoLoadStep: "reload" }),
      ).toBe(true);
      expect(rebindsOwnVersionAfter({})).toBe(true);
      expect(rebindsOwnVersionAfter(undefined)).toBe(true);
      expect(
         rebindsOwnVersionAfter({
            autoLoad: "failed",
            autoLoadStep: "timeout",
         }),
      ).toBe(false);
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

      const result = await mostRecentEntries(ctx.service)(
         "env-1",
         "pkg",
         "in-flight",
      );

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

      const result = await mostRecentEntries(ctx.service)(
         "env-1",
         "pkg",
         "self",
      );

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

      const entries = await mostRecentEntries(ctx.service)(
         "env-1",
         "pkg",
         "mat-current",
      );

      expect(Object.keys(entries)).toEqual(["healthy"]);
   });
});
