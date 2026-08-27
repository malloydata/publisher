// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { Connection as MalloyConnection } from "@malloydata/malloy";
import { Manifest } from "@malloydata/malloy";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as sinon from "sinon";
import {
   MaterializationEligibilityError,
   BadRequestError,
   ConnectionNotFoundError,
   DestinationNotFoundError,
   EnvironmentNotFoundError,
   InvalidStateTransitionError,
   MaterializationConflictError,
   MaterializationNotFoundError,
   internalErrorToHttpError,
} from "../errors";
import {
   BuildInstruction,
   LedgerEntry,
   MaterializationStatus,
   MaterializationUpdate,
   ResourceRepository,
} from "../storage/DatabaseInterface";
import { indexCallerLedger } from "./incremental_build";
import { DuplicateActiveMaterializationError } from "../storage/duckdb/MaterializationRepository";
import { EnvironmentStore } from "./environment_store";
import {
   compiledWith,
   fakeSource,
   makeBuildPlan,
   makeInstruction,
   makeMaterialization,
} from "./materialization_test_fixtures";
import {
   manifestExcludingStorage,
   MaterializationService,
   redactConnectionSecrets,
   stagingSuffix,
} from "./materialization_service";
import { logger } from "../logger";
import { resetMaterializationTelemetryForTesting } from "../materialization_metrics";
import {
   isReclaimableStorageTable,
   tallySources,
} from "./materialization_service";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "../test_helpers/metrics_harness";

type MockRepo = sinon.SinonStubbedInstance<ResourceRepository>;

/**
 * Turn per-query metadata on for one test and put the environment back after.
 * The feature ships dark, so a test that asserts what a statement carries has
 * to enable it — and saying so per test keeps the default honest here: a test
 * that forgets is testing the shipped default, which is "nothing attached".
 */
function withQueryMetadataOn(): void {
   const original = process.env.PUBLISHER_QUERY_METADATA;
   beforeEach(() => {
      process.env.PUBLISHER_QUERY_METADATA = "on";
   });
   afterEach(() => {
      if (original === undefined) delete process.env.PUBLISHER_QUERY_METADATA;
      else process.env.PUBLISHER_QUERY_METADATA = original;
   });
}

describe("redactConnectionSecrets", () => {
   it("strips credential values but keeps the message legible", () => {
      const source = {
         name: "orders_pg",
         type: "postgres",
         postgresConnection: {
            host: "db.internal",
            userName: "svc",
            password: "sup3r-secret-pw",
         },
      };
      const msg =
         "IO Error: could not connect password=sup3r-secret-pw to db.internal";
      const out = redactConnectionSecrets(msg, source);
      expect(out).not.toContain("sup3r-secret-pw");
      expect(out).toContain("could not connect");
      expect(out).toContain("db.internal"); // host isn't a secret — stays
   });

   it("passes a credential-free error (e.g. schema not found) through verbatim", () => {
      const source = {
         name: "lake",
         type: "ducklake",
         ducklakeConnection: {
            catalog: { postgresConnection: { password: "catpw123" } },
         },
      };
      const msg =
         "Catalog Error: Schema 'analytics' not found in DuckLake 'lake'";
      expect(redactConnectionSecrets(msg, source)).toBe(msg);
   });

   it("redacts a secret in its SQL-escaped form (single quotes doubled)", () => {
      // A DuckDB error echoes the offending statement, in which a secret with a
      // single quote appears escaped (`'` -> `''`), so the raw value won't match.
      const source = {
         name: "pg",
         type: "postgres",
         postgresConnection: { password: "pa'ss'word" },
      };
      const msg =
         "Parser Error: syntax error near 'password=pa''ss''word host=h'";
      const out = redactConnectionSecrets(msg, source);
      expect(out).not.toContain("pa''ss''word");
      expect(out).toContain("***");
   });

   it("still redacts when the connection graph contains a cycle", () => {
      // The callers pass a LIVE connection, whose driver/pool internals can hold
      // a back-reference. An unguarded walk recurses until the stack is
      // exhausted, and the resulting RangeError replaces the build error being
      // redacted -- so every failure on such a connection reports only
      // "Maximum call stack size exceeded".
      const source: Record<string, unknown> = {
         name: "sf",
         type: "snowflake",
         snowflakeConnection: { password: "sup3rsecret" },
      };
      source.pool = { owner: source };

      const out = redactConnectionSecrets(
         "Object 'x' already exists (password=sup3rsecret)",
         source,
      );
      expect(out).not.toContain("sup3rsecret");
      expect(out).toContain("***");
      expect(out).toContain("already exists");
   });

   // Stubbed at the seam because the leak needs an ATTACH (not CTAS) failure:
   // bad catalog creds fast-fail at connection validation, so no black-box test
   // can reach this branch.
   it("redacts an INFRA chained-build failure, and does not call it a refusal", async () => {
      const sandbox = sinon.createSandbox();
      try {
         const destinationConnection = {
            name: "lake",
            type: "ducklake",
            ducklakeConnection: {
               catalog: {
                  postgresConnection: {
                     host: "catalog.internal",
                     userName: "lake_rw",
                     password: "c4talog-pw",
                  },
               },
            },
         };
         const sourceConnection = {
            name: "orders_pg",
            type: "postgres",
            postgresConnection: { host: "db.internal", password: "src-pw-99" },
         };

         // A failed RW ATTACH echoes the statement, connstring included.
         sandbox
            .stub(
               MaterializationService.prototype as unknown as Record<
                  string,
                  unknown
               >,
               "buildDownstreamViaParents" as never,
            )
            .rejects(
               new Error(
                  "IO Error: failed to ATTACH 'ducklake:postgres:host=catalog.internal " +
                     "user=lake_rw password=c4talog-pw' (db lake)",
               ),
            );

         const { service } = createMocks();
         const environment = buildEnvironmentStub({
            connections: { orders_pg: sourceConnection },
            destinations: { lake: destinationConnection },
         });

         const call = (
            service as unknown as {
               buildOneSourceIntoStorage: (...a: unknown[]) => Promise<unknown>;
            }
         ).buildOneSourceIntoStorage({
            persistSource: {
               name: "orders_by_month",
               connectionName: "orders_pg",
            },
            instruction: {
               sourceEntityId: "sid-1",
               physicalTableName: "mz_orders_by_month",
               destination: "lake",
            },
            // strict: throw instead of falling through to recompute-from-raw.
            manifest: { strict: true, update: () => {} },
            environment,
            publicBuildSQL: "SELECT 1",
            buildSQL: "SELECT 1",
            builtEntries: {},
            dependsOnStorageUpstream: true, // take the chained path
         });

         // A failed ATTACH is infrastructure, not a shape limit: it must NOT
         // present as the strict-upstreams refusal (that message means "we could
         // have recomputed from raw but you forbade it", which is not what
         // happened here).
         await expect(call).rejects.toThrow(
            /Failed to materialize chained source/i,
         );
         await expect(call).rejects.not.toThrow(/strict upstreams forbid/i);
         const message = await call.then(
            () => "",
            (e: unknown) => (e instanceof Error ? e.message : String(e)),
         );
         expect(message).not.toContain("c4talog-pw");
         expect(message).toContain("***");
         // Redacted but still legible.
         expect(message).toContain("failed to ATTACH");
         expect(message).toContain("catalog.internal");
         expect(message).toContain("orders_by_month");
      } finally {
         sandbox.restore();
      }
   });
});

function createMocks() {
   const sandbox = sinon.createSandbox();

   const repository = {
      getEnvironmentByName: sandbox.stub(),
      listMaterializations: sandbox.stub(),
      getMaterializationById: sandbox.stub(),
      getActiveMaterialization: sandbox.stub(),
      createMaterialization: sandbox.stub(),
      updateMaterialization: sandbox.stub(),
      deleteMaterialization: sandbox.stub(),
   } as unknown as MockRepo;

   const storageManager = { getRepository: () => repository };

   const environmentStore = {
      storageManager,
      getEnvironment: sandbox.stub(),
   } as unknown as EnvironmentStore;

   // Default: environment resolves and yields a package whose build plan is
   // empty (no persist sources). Individual tests override getBuildPlan to
   // exercise the orchestrated path. getModelPaths()=>[] keeps the background
   // build from touching the Malloy runtime.
   repository.getEnvironmentByName.resolves({
      id: "env-1",
      name: "my-env",
      path: "/test",
      createdAt: new Date(),
      updatedAt: new Date(),
   });
   setPackage(environmentStore, { getBuildPlan: () => null });

   const service = new MaterializationService(environmentStore);

   return { sandbox, repository, environmentStore, service };
}

/**
 * A stub of the environment surface the build path depends on, with the two
 * lookups kept as disjoint as the real Environment keeps them: connection names
 * resolve only through `getApiConnection` and destination names only through
 * `getStorageDestination`, and asking either for the other's name fails
 * the way the real one does.
 *
 * That disjointness is the point. A stub that answered both lookups from one map
 * would let a build resolve its destination through the connection list and the
 * test would never notice, which is exactly the confusion these tests exist to
 * catch.
 */
function buildEnvironmentStub(opts: {
   connections?: Record<string, Record<string, unknown>>;
   destinations?: Record<string, Record<string, unknown>>;
   environmentPath?: string;
}) {
   const connections = opts.connections ?? {};
   const destinations = opts.destinations ?? {};
   return {
      getApiConnection: sinon.stub().callsFake((name: string) => {
         const connection = connections[name];
         if (!connection) {
            throw new ConnectionNotFoundError(`Connection ${name} not found`);
         }
         return connection;
      }),
      getStorageDestination: sinon.stub().callsFake((name: string) => {
         const destination = destinations[name];
         if (!destination) {
            throw new DestinationNotFoundError(
               `Storage destination ${name} not found`,
            );
         }
         return destination;
      }),
      getEnvironmentPath: () => opts.environmentPath ?? "/tmp/env",
   };
}

/** Point environmentStore.getEnvironment at a package with the given overrides. */
function setPackage(
   environmentStore: EnvironmentStore,
   pkgOverrides: Record<string, unknown>,
): void {
   const pkg = {
      getBuildPlan: () => null,
      getModelPaths: () => [],
      getPackagePath: () => "/test",
      getMalloyConfig: () => ({}),
      getMalloyConnection: async () => ({}),
      // No bound manifestLocation by default, so seedFromBoundManifest no-ops.
      getPackageMetadata: () => ({}),
      ...pkgOverrides,
   };
   (environmentStore.getEnvironment as sinon.SinonStub).resolves({
      getPackage: sinon.stub().resolves(pkg),
      withPackageLock: async (_name: string, fn: () => Promise<unknown>) =>
         fn(),
      reloadAllModelsForPackage: sinon.stub().resolves(),
   });
}

describe("MaterializationService", () => {
   let ctx: ReturnType<typeof createMocks>;

   beforeEach(() => {
      ctx = createMocks();
   });

   describe("resolveEnvironmentId", () => {
      it("throws EnvironmentNotFoundError when the environment is missing", async () => {
         ctx.repository.getEnvironmentByName.resolves(null);
         await expect(
            ctx.service.listMaterializations("unknown", "pkg"),
         ).rejects.toThrow(EnvironmentNotFoundError);
      });
   });

   describe("deleteMaterialization drop of a storage= table", () => {
      it("routes a storage entry to a destination-aware RW drop (best-effort, delete still completes)", async () => {
         // getApiConnection returns a non-storage type so the RW drop refuses
         // at its destination gate (pre-session), exercising the routing + the
         // best-effort failure handling without needing a live DuckDB attach.
         const environment = buildEnvironmentStub({
            destinations: { lake: { name: "lake", type: "postgres" } },
            environmentPath: "/test",
         });
         const getStorageDestination = environment.getStorageDestination;
         (ctx.environmentStore.getEnvironment as sinon.SinonStub).resolves({
            getPackage: sinon
               .stub()
               .resolves({ getMalloyConnection: async () => ({}) }),
            ...environment,
         });
         ctx.repository.getMaterializationById.resolves(
            makeMaterialization({
               status: "MANIFEST_FILE_READY",
               manifest: {
                  entries: {
                     se1: {
                        sourceEntityId: "se1",
                        sourceName: "daily",
                        physicalTableName: "daily__mabc123",
                        connectionName: "wh",
                        storageDestinationName: "lake",
                     },
                  },
                  // eslint-disable-next-line @typescript-eslint/no-explicit-any
               } as any,
            }),
         );

         await ctx.service.deleteMaterialization("my-env", "pkg", "mat-1", {
            dropTables: true,
         });

         // Routed to the storage path (resolved the destination connection for
         // an RW drop, not the old skip), and the delete completed despite the
         // drop's best-effort failure.
         expect(getStorageDestination.calledWith("lake")).toBe(true);
         expect(
            (
               ctx.repository.deleteMaterialization as sinon.SinonStub
            ).calledWith("mat-1"),
         ).toBe(true);
      });

      it("skips the drop when another MANIFEST_FILE_READY run still references the table (MED-3)", async () => {
         // name=-verbatim: generations of a source share one physical name, so
         // dropping a superseded record's table must NOT take out the table a
         // remaining run still serves.
         const environment = buildEnvironmentStub({
            destinations: { lake: { name: "lake", type: "duckdb" } },
            environmentPath: "/test",
         });
         const getStorageDestination = environment.getStorageDestination;
         (ctx.environmentStore.getEnvironment as sinon.SinonStub).resolves({
            getPackage: sinon
               .stub()
               .resolves({ getMalloyConnection: async () => ({}) }),
            ...environment,
         });
         const entries = {
            se1: {
               sourceEntityId: "se1",
               sourceName: "daily",
               physicalTableName: "daily", // shared logical name
               connectionName: "wh",
               storageDestinationName: "lake",
            },
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
         } as any;
         ctx.repository.getMaterializationById.resolves(
            makeMaterialization({
               id: "mat-1",
               status: "MANIFEST_FILE_READY",
               manifest: { entries },
            }),
         );
         // Another remaining successful run references the SAME lake.daily table.
         ctx.repository.listMaterializations.resolves([
            makeMaterialization({ id: "mat-1", manifest: { entries } }),
            makeMaterialization({
               id: "mat-2",
               status: "MANIFEST_FILE_READY",
               manifest: { entries },
            }),
         ]);

         await ctx.service.deleteMaterialization("my-env", "pkg", "mat-1", {
            dropTables: true,
         });

         // The drop was skipped (never resolved the destination), and the record
         // was still deleted.
         expect(getStorageDestination.called).toBe(false);
         expect(
            (
               ctx.repository.deleteMaterialization as sinon.SinonStub
            ).calledWith("mat-1"),
         ).toBe(true);
      });
   });

   describe("deleteMaterialization rebinds serve bindings (both tiers)", () => {
      const storageEntry = {
         se1: {
            sourceEntityId: "se1",
            sourceName: "daily",
            physicalTableName: "daily__mabc",
            connectionName: "wh",
            storageDestinationName: "lake",
            schema: [{ name: "a", type: "BIGINT" }],
         },
      };
      // A colocated entry carries NO storageDestinationName.
      const colocatedEntry = {
         ce1: {
            sourceEntityId: "ce1",
            sourceName: "daily",
            physicalTableName: "daily__cabc",
            connectionName: "wh",
         },
      };

      function setupEnv(): {
         storageSpy: sinon.SinonStub;
         colocatedSpy: sinon.SinonStub;
      } {
         const storageSpy = sinon.stub().resolves();
         const colocatedSpy = sinon.stub().resolves();
         (ctx.environmentStore.getEnvironment as sinon.SinonStub).resolves({
            getPackage: sinon
               .stub()
               .resolves({ getMalloyConnection: async () => ({}) }),
            getApiConnection: () => ({ name: "lake", type: "duckdb" }),
            getEnvironmentPath: () => "/test",
            bindPackageStorageServeBindings: storageSpy,
            bindPackageColocatedServeManifest: colocatedSpy,
         });
         ctx.repository.getMaterializationById.resolves(
            makeMaterialization({
               status: "MANIFEST_FILE_READY",
               // eslint-disable-next-line @typescript-eslint/no-explicit-any
               manifest: { entries: {} } as any,
            }),
         );
         return { storageSpy, colocatedSpy };
      }

      it("re-derives storage bindings from the next-latest materialization (on)", async () => {
         process.env.PERSIST_STORAGE_MODE = "on";
         try {
            const { storageSpy, colocatedSpy } = setupEnv();
            // The latest REMAINING successful run carries a storage entry.
            ctx.repository.listMaterializations.resolves([
               makeMaterialization({
                  id: "m-prev",
                  status: "MANIFEST_FILE_READY",
                  // eslint-disable-next-line @typescript-eslint/no-explicit-any
                  manifest: { entries: storageEntry } as any,
               }),
            ]);

            await ctx.service.deleteMaterialization("my-env", "pkg", "mat-1");

            expect(storageSpy.calledOnce).toBe(true);
            expect(Object.keys(storageSpy.firstCall.args[1])).toContain("se1");
            // Colocated is re-derived too (with no colocated entries ⇒ cleared).
            expect(colocatedSpy.calledOnce).toBe(true);
            expect(colocatedSpy.firstCall.args[1]).toEqual({});
         } finally {
            delete process.env.PERSIST_STORAGE_MODE;
         }
      });

      it("re-derives colocated bindings regardless of tier mode (off)", async () => {
         // PERSIST_STORAGE_MODE unset ⇒ off. Colocated is the v0 path, not gated
         // by the storage kill switch, so it is still re-derived; storage is not.
         const { storageSpy, colocatedSpy } = setupEnv();
         ctx.repository.listMaterializations.resolves([
            makeMaterialization({
               id: "m-prev",
               status: "MANIFEST_FILE_READY",
               // eslint-disable-next-line @typescript-eslint/no-explicit-any
               manifest: { entries: colocatedEntry } as any,
            }),
         ]);

         await ctx.service.deleteMaterialization("my-env", "pkg", "mat-1");

         expect(colocatedSpy.calledOnce).toBe(true);
         expect(Object.keys(colocatedSpy.firstCall.args[1])).toContain("ce1");
         // Storage rebind is skipped when the tier is off.
         expect(storageSpy.called).toBe(false);
      });

      it("clears both tiers' bindings when no successful materialization remains (on)", async () => {
         process.env.PERSIST_STORAGE_MODE = "on";
         try {
            const { storageSpy, colocatedSpy } = setupEnv();
            ctx.repository.listMaterializations.resolves([]); // none remain

            await ctx.service.deleteMaterialization("my-env", "pkg", "mat-1");

            expect(storageSpy.calledOnce).toBe(true);
            expect(storageSpy.firstCall.args[1]).toEqual({}); // empty ⇒ serve live
            expect(colocatedSpy.calledOnce).toBe(true);
            expect(colocatedSpy.firstCall.args[1]).toEqual({});
         } finally {
            delete process.env.PERSIST_STORAGE_MODE;
         }
      });

      it("skips only the storage rebind when the tier is off (dark-ship)", async () => {
         const { storageSpy, colocatedSpy } = setupEnv(); // off
         ctx.repository.listMaterializations.resolves([]); // none remain
         await ctx.service.deleteMaterialization("my-env", "pkg", "mat-1");
         // Storage is skipped when off; colocated still re-derives (to clear).
         expect(storageSpy.called).toBe(false);
         expect(colocatedSpy.calledOnce).toBe(true);
         expect(colocatedSpy.firstCall.args[1]).toEqual({});
      });
   });

   describe("queries", () => {
      it("lists materializations for a package", async () => {
         const rows = [
            makeMaterialization(),
            makeMaterialization({ id: "m2" }),
         ];
         ctx.repository.listMaterializations.resolves(rows);
         expect(
            await ctx.service.listMaterializations("my-env", "pkg"),
         ).toEqual(rows);
      });

      it("returns a specific materialization", async () => {
         const m = makeMaterialization();
         ctx.repository.getMaterializationById.resolves(m);
         expect(
            await ctx.service.getMaterialization("my-env", "pkg", "mat-1"),
         ).toEqual(m);
      });

      it("throws when the materialization is missing", async () => {
         ctx.repository.getMaterializationById.resolves(null);
         await expect(
            ctx.service.getMaterialization("my-env", "pkg", "nope"),
         ).rejects.toThrow(MaterializationNotFoundError);
      });

      it("throws when the materialization belongs to another package", async () => {
         ctx.repository.getMaterializationById.resolves(
            makeMaterialization({ packageName: "other" }),
         );
         await expect(
            ctx.service.getMaterialization("my-env", "pkg", "mat-1"),
         ).rejects.toThrow(MaterializationNotFoundError);
      });
   });

   describe("createMaterialization (auto-run)", () => {
      it("creates a PENDING record and persists options with mode=auto", async () => {
         ctx.repository.getActiveMaterialization.resolves(null);
         const pending = makeMaterialization({ status: "PENDING" });
         ctx.repository.createMaterialization.resolves(pending);

         const result = await ctx.service.createMaterialization(
            "my-env",
            "pkg",
            {
               forceRefresh: true,
               sourceNames: ["orders"],
            },
         );

         expect(result.status).toBe("PENDING");
         expect(ctx.repository.createMaterialization.firstCall.args).toEqual([
            "env-1",
            "pkg",
            "PENDING",
            {
               forceRefresh: true,
               // Recorded alongside forceRefresh, not folded into it: the history
               // has to show whether an incremental source rebuilt or advanced.
               reseed: false,
               sourceNames: ["orders"],
               mode: "auto",
               trigger: "ON_DEMAND",
            },
         ]);
      });

      it("defaults to auto mode in metadata", async () => {
         ctx.repository.getActiveMaterialization.resolves(null);
         ctx.repository.createMaterialization.resolves(
            makeMaterialization({ status: "PENDING" }),
         );

         await ctx.service.createMaterialization("my-env", "pkg", {});

         expect(ctx.repository.createMaterialization.firstCall.args[3]).toEqual(
            {
               forceRefresh: false,
               reseed: false,
               sourceNames: null,
               mode: "auto",
               trigger: "ON_DEMAND",
            },
         );
      });

      it("records trigger=SCHEDULER when the scheduler fires the run", async () => {
         ctx.repository.getActiveMaterialization.resolves(null);
         ctx.repository.createMaterialization.resolves(
            makeMaterialization({ status: "PENDING" }),
         );

         await ctx.service.createMaterialization("my-env", "pkg", {
            forceRefresh: true,
            trigger: "SCHEDULER",
         });

         expect(
            ctx.repository.createMaterialization.firstCall.args[3],
         ).toMatchObject({ mode: "auto", trigger: "SCHEDULER" });
      });

      it("rejects when an active materialization already exists", async () => {
         ctx.repository.getActiveMaterialization.resolves(
            makeMaterialization({
               id: "existing",
               status: "MANIFEST_ROWS_READY",
            }),
         );
         await expect(
            ctx.service.createMaterialization("my-env", "pkg"),
         ).rejects.toThrow(MaterializationConflictError);
      });

      it("translates a lost create race into a conflict", async () => {
         ctx.repository.getActiveMaterialization
            .onFirstCall()
            .resolves(null)
            .onSecondCall()
            .resolves(makeMaterialization({ id: "winner" }));
         ctx.repository.createMaterialization.rejects(
            new DuplicateActiveMaterializationError("env-1", "pkg"),
         );
         await expect(
            ctx.service.createMaterialization("my-env", "pkg"),
         ).rejects.toThrow(/winner/);
      });
   });

   describe("createMaterialization (orchestrated)", () => {
      it("creates a PENDING record with mode=orchestrated for valid instructions", async () => {
         setPackage(ctx.environmentStore, {
            getBuildPlan: () => makeBuildPlan(),
         });
         ctx.repository.getActiveMaterialization.resolves(null);
         ctx.repository.createMaterialization.resolves(
            makeMaterialization({ status: "PENDING" }),
         );

         const result = await ctx.service.createMaterialization(
            "my-env",
            "pkg",
            { buildInstructions: [makeInstruction()] },
         );

         expect(result.status).toBe("PENDING");
         expect(
            ctx.repository.createMaterialization.firstCall.args[3],
         ).toMatchObject({ mode: "orchestrated" });
      });

      it("takes the ledger from the caller only when the caller is instructing", async () => {
         setPackage(ctx.environmentStore, {
            getBuildPlan: () => makeBuildPlan(),
         });
         ctx.repository.getActiveMaterialization.resolves(null);
         ctx.repository.createMaterialization.resolves(
            makeMaterialization({ status: "PENDING" }),
         );
         const runBuild = sinon.stub().resolves();
         (ctx.service as unknown as { runBuild: sinon.SinonStub }).runBuild =
            runBuild;

         // An EMPTY ledger is meaningful — "I own the ledger and nothing is
         // recorded" — and must survive to the run rather than degrade to
         // "use the local store".
         await ctx.service.createMaterialization("my-env", "pkg", {
            buildInstructions: [makeInstruction()],
            ledger: [],
         });
         expect(runBuild.firstCall.args[3].ledger).toEqual([]);

         // An auto-run has no caller to take a ledger from, so it is clamped
         // rather than trusted: taking one would replace the local store the
         // auto-run depends on.
         await ctx.service.createMaterialization("my-env", "pkg", {
            ledger: [],
         });
         expect(runBuild.secondCall.args[3].ledger).toBeUndefined();
      });

      it("rejects instructions referencing an unknown sourceEntityId before creating", async () => {
         setPackage(ctx.environmentStore, {
            getBuildPlan: () => makeBuildPlan(),
         });
         await expect(
            ctx.service.createMaterialization("my-env", "pkg", {
               buildInstructions: [
                  makeInstruction({ sourceEntityId: "ghost" }),
               ],
            }),
         ).rejects.toThrow(BadRequestError);
         expect(ctx.repository.createMaterialization.called).toBe(false);
      });

      it("rejects SNAPSHOT realization", async () => {
         setPackage(ctx.environmentStore, {
            getBuildPlan: () => makeBuildPlan(),
         });
         await expect(
            ctx.service.createMaterialization("my-env", "pkg", {
               buildInstructions: [
                  makeInstruction({ realization: "SNAPSHOT" }),
               ],
            }),
         ).rejects.toThrow(BadRequestError);
      });

      it("rejects buildInstructions when the package has no persist sources", async () => {
         setPackage(ctx.environmentStore, { getBuildPlan: () => null });
         await expect(
            ctx.service.createMaterialization("my-env", "pkg", {
               buildInstructions: [makeInstruction()],
            }),
         ).rejects.toThrow(BadRequestError);
      });

      // A supplied ledger is validated at CREATE time, per the API contract
      // that an invalid entry is a 400 rather than a quiet rebuild — and the
      // sourceEntityId case is checked here precisely because it fires in
      // ordinary operation (a publish or rollback moves the address), where a
      // synchronous, actionable rejection beats a failed background run.
      describe("with a supplied ledger", () => {
         /** A plan whose one source declares incremental refresh. */
         const incrementalPlan = () =>
            makeBuildPlan({
               sources: {
                  "orders@m.malloy": {
                     name: "orders",
                     sourceID: "orders@m.malloy",
                     connectionName: "duckdb",
                     dialect: "duckdb",
                     sourceEntityId: "build-orders",
                     sql: "SELECT 1",
                     columns: [],
                     refresh: "incremental",
                  },
               },
            });

         /** A ledger entry matching {@link makeInstruction}'s target table. */
         const entry = (overrides: Partial<LedgerEntry> = {}): LedgerEntry => ({
            connectionName: "duckdb",
            physicalTableName: '"orders_v1"',
            coveredThrough: "2024-06-20",
            coveredThroughType: "date",
            watermark: "order_date",
            strategy: "range_replace",
            sourceEntityId: "build-orders",
            ...overrides,
         });

         function creating(options: {
            ledger: LedgerEntry[];
            reseed?: boolean;
            instruction?: BuildInstruction;
            plan?: () => ReturnType<typeof makeBuildPlan>;
         }) {
            setPackage(ctx.environmentStore, {
               getBuildPlan: options.plan ?? incrementalPlan,
            });
            ctx.repository.getActiveMaterialization.resolves(null);
            ctx.repository.createMaterialization.resolves(
               makeMaterialization({ status: "PENDING" }),
            );
            const runBuild = sinon.stub().resolves();
            (ctx.service as unknown as { runBuild: sinon.SinonStub }).runBuild =
               runBuild;
            return ctx.service.createMaterialization("my-env", "pkg", {
               buildInstructions: [options.instruction ?? makeInstruction()],
               ledger: options.ledger,
               ...(options.reseed === undefined
                  ? {}
                  : { reseed: options.reseed }),
            });
         }

         it("accepts an entry that matches the plan", async () => {
            const created = await creating({ ledger: [entry()] });
            expect(created.status).toBe("PENDING");
         });

         it("rejects an entry naming a table this run does not instruct", async () => {
            await expect(
               creating({
                  ledger: [entry({ physicalTableName: '"other_table"' })],
               }),
            ).rejects.toThrow(/do not build/);
            expect(ctx.repository.createMaterialization.called).toBe(false);
         });

         it("seeds, rather than refusing, when an older caller omits the destination", async () => {
            // A caller predating `storageDestinationName` echoes the entry
            // without one. The table it names IS built by this run, just into a
            // destination the entry cannot describe, so the entry is stale rather
            // than wrong: the run proceeds and the source seeds, because the
            // index the build reads keys on the destination too and finds no
            // boundary there.
            const created = await creating({
               ledger: [entry({ storageDestinationName: undefined })],
               instruction: makeInstruction({ destination: "lake" }),
            });
            expect(created.status).toBe("PENDING");
         });

         it("rejects an entry naming a destination this run does not write", async () => {
            // Not the stale case above: the entry names a destination, and it is
            // not the one instructed. Couriering that boundary would measure one
            // table's coverage onto another, so it is a caller error.
            await expect(
               creating({
                  ledger: [
                     entry({ storageDestinationName: "some_other_lake" }),
                  ],
                  instruction: makeInstruction({ destination: "lake" }),
               }),
            ).rejects.toThrow(/do not build/);
            expect(ctx.repository.createMaterialization.called).toBe(false);
         });

         it("rejects an entry measured under a different source definition", async () => {
            // The publish/rollback case: the caller echoed faithfully, but the
            // instructing version's address moved. The message says what to do.
            await expect(
               creating({
                  ledger: [entry({ sourceEntityId: "an-older-address" })],
               }),
            ).rejects.toThrow(/publish or rollback|reseed/);
         });

         it("rejects an entry for a source that is not incremental", async () => {
            await expect(
               creating({ ledger: [entry()], plan: () => makeBuildPlan() }),
            ).rejects.toThrow(/refresh="incremental"/);
         });

         it("rejects a ledger naming the same table twice", async () => {
            await expect(
               creating({ ledger: [entry(), entry()] }),
            ).rejects.toThrow(/more than once/);
         });

         it("ignores an invalid entry for a source being reseeded", async () => {
            // `reseed` is the documented recovery from the rejections above, so
            // it must not itself trip them — run-level or per-instruction.
            const stale = entry({ sourceEntityId: "an-older-address" });
            expect(
               (await creating({ ledger: [stale], reseed: true })).status,
            ).toBe("PENDING");
            expect(
               (
                  await creating({
                     ledger: [stale],
                     instruction: makeInstruction({ reseed: true }),
                  })
               ).status,
            ).toBe("PENDING");
         });
      });
   });

   describe("stopMaterialization", () => {
      const cancellable = ["PENDING", "MANIFEST_ROWS_READY"] as const;

      for (const status of cancellable) {
         it(`cancels a ${status} materialization`, async () => {
            ctx.repository.getMaterializationById.resolves(
               makeMaterialization({ status }),
            );
            ctx.repository.updateMaterialization.resolves(
               makeMaterialization({ status: "CANCELLED" }),
            );
            const result = await ctx.service.stopMaterialization(
               "my-env",
               "pkg",
               "mat-1",
            );
            expect(result.status).toBe("CANCELLED");
         });
      }

      const terminal = ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"] as const;
      for (const status of terminal) {
         it(`rejects stopping a ${status} materialization`, async () => {
            ctx.repository.getMaterializationById.resolves(
               makeMaterialization({ status }),
            );
            await expect(
               ctx.service.stopMaterialization("my-env", "pkg", "mat-1"),
            ).rejects.toThrow(InvalidStateTransitionError);
         });
      }
   });

   describe("deleteMaterialization", () => {
      const terminal = ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"] as const;
      for (const status of terminal) {
         it(`deletes a ${status} materialization`, async () => {
            ctx.repository.getMaterializationById.resolves(
               makeMaterialization({ status }),
            );
            await ctx.service.deleteMaterialization("my-env", "pkg", "mat-1");
            expect(
               ctx.repository.deleteMaterialization.calledOnceWith("mat-1"),
            ).toBe(true);
         });
      }

      const active = ["PENDING", "MANIFEST_ROWS_READY"] as const;
      for (const status of active) {
         it(`rejects deleting a ${status} materialization`, async () => {
            ctx.repository.getMaterializationById.resolves(
               makeMaterialization({ status }),
            );
            await expect(
               ctx.service.deleteMaterialization("my-env", "pkg", "mat-1"),
            ).rejects.toThrow(InvalidStateTransitionError);
            expect(ctx.repository.deleteMaterialization.called).toBe(false);
         });
      }

      it("does not drop tables by default (record-only delete)", async () => {
         const runSQL = sinon.stub().resolves();
         const getMalloyConnection = sinon.stub().resolves({ runSQL });
         (ctx.environmentStore.getEnvironment as sinon.SinonStub).resolves({
            getPackage: sinon.stub().resolves({ getMalloyConnection }),
            withPackageLock: async (_n: string, fn: () => Promise<unknown>) =>
               fn(),
         });
         ctx.repository.getMaterializationById.resolves(
            makeMaterialization({
               status: "MANIFEST_FILE_READY",
               manifest: {
                  builtAt: new Date().toISOString(),
                  strict: false,
                  entries: {
                     b1: {
                        sourceEntityId: "b1",
                        physicalTableName: "orders_mz",
                        connectionName: "duckdb",
                     },
                  },
               },
            }),
         );

         await ctx.service.deleteMaterialization("my-env", "pkg", "mat-1");

         expect(getMalloyConnection.called).toBe(false);
         expect(runSQL.called).toBe(false);
         expect(
            ctx.repository.deleteMaterialization.calledOnceWith("mat-1"),
         ).toBe(true);
      });

      it("drops manifest tables (and staging) when dropTables is set", async () => {
         const runSQL = sinon.stub().resolves();
         const getMalloyConnection = sinon
            .stub()
            .resolves({ runSQL, dialectName: "duckdb" });
         (ctx.environmentStore.getEnvironment as sinon.SinonStub).resolves({
            getPackage: sinon.stub().resolves({ getMalloyConnection }),
            withPackageLock: async (_n: string, fn: () => Promise<unknown>) =>
               fn(),
         });
         ctx.repository.getMaterializationById.resolves(
            makeMaterialization({
               status: "MANIFEST_FILE_READY",
               manifest: {
                  builtAt: new Date().toISOString(),
                  strict: false,
                  entries: {
                     b1: {
                        sourceEntityId: "b1",
                        physicalTableName: "orders_mz",
                        connectionName: "duckdb",
                     },
                  },
               },
            }),
         );

         await ctx.service.deleteMaterialization("my-env", "pkg", "mat-1", {
            dropTables: true,
         });

         expect(getMalloyConnection.calledOnceWith("duckdb")).toBe(true);
         const dropped = runSQL.getCalls().map((c) => c.args[0] as string);
         expect(dropped).toContain('DROP TABLE IF EXISTS "orders_mz"');
         expect(dropped.some((s) => s.includes('"orders_mz_'))).toBe(true);
         expect(
            ctx.repository.deleteMaterialization.calledOnceWith("mat-1"),
         ).toBe(true);
      });

      it("dialect-quotes the drop DDL from the live connection (backtick + container path)", async () => {
         const runSQL = sinon.stub().resolves();
         const getMalloyConnection = sinon
            .stub()
            .resolves({ runSQL, dialectName: "standardsql" });
         (ctx.environmentStore.getEnvironment as sinon.SinonStub).resolves({
            getPackage: sinon.stub().resolves({ getMalloyConnection }),
            withPackageLock: async (_n: string, fn: () => Promise<unknown>) =>
               fn(),
         });
         ctx.repository.getMaterializationById.resolves(
            makeMaterialization({
               status: "MANIFEST_FILE_READY",
               manifest: {
                  builtAt: new Date().toISOString(),
                  strict: false,
                  entries: {
                     b1: {
                        sourceEntityId: "b1",
                        // Container path on a hyphenated BigQuery project: each
                        // segment must be backtick-quoted independently.
                        physicalTableName: "my-proj.ds.engaged",
                        connectionName: "bq",
                     },
                  },
               },
            }),
         );

         await ctx.service.deleteMaterialization("my-env", "pkg", "mat-1", {
            dropTables: true,
         });

         const dropped = runSQL.getCalls().map((c) => c.args[0] as string);
         expect(dropped).toContain(
            "DROP TABLE IF EXISTS `my-proj`.`ds`.`engaged`",
         );
      });

      it("still deletes the record when a table drop fails", async () => {
         const runSQL = sinon.stub().rejects(new Error("boom"));
         const getMalloyConnection = sinon.stub().resolves({ runSQL });
         (ctx.environmentStore.getEnvironment as sinon.SinonStub).resolves({
            getPackage: sinon.stub().resolves({ getMalloyConnection }),
            withPackageLock: async (_n: string, fn: () => Promise<unknown>) =>
               fn(),
         });
         ctx.repository.getMaterializationById.resolves(
            makeMaterialization({
               status: "FAILED",
               manifest: {
                  builtAt: new Date().toISOString(),
                  strict: false,
                  entries: {
                     b1: {
                        sourceEntityId: "b1",
                        physicalTableName: "orders_mz",
                        connectionName: "duckdb",
                     },
                  },
               },
            }),
         );

         await ctx.service.deleteMaterialization("my-env", "pkg", "mat-1", {
            dropTables: true,
         });

         expect(
            ctx.repository.deleteMaterialization.calledOnceWith("mat-1"),
         ).toBe(true);
      });
   });
});

describe("deleteMaterialization telemetry", () => {
   let ctx: ReturnType<typeof createMocks>;
   let harness: MetricsHarness;
   withQueryMetadataOn();

   beforeEach(async () => {
      ctx = createMocks();
      harness = await startMetricsHarness();
      // Drop cached instruments so recordDropTables re-binds to this provider.
      resetMaterializationTelemetryForTesting();
   });

   afterEach(async () => {
      resetMaterializationTelemetryForTesting();
      await harness.shutdown();
   });

   const DROP_COUNTER = "publisher_materialization_drop_tables_total";

   // A terminal materialization whose manifest names one physical table, so
   // dropMaterializedTables issues exactly one DROP — the emission under test.
   function dropTablesFixture(runSQL: sinon.SinonStub) {
      const getMalloyConnection = sinon
         .stub()
         .resolves({ runSQL, dialectName: "duckdb" });
      (ctx.environmentStore.getEnvironment as sinon.SinonStub).resolves({
         getPackage: sinon.stub().resolves({ getMalloyConnection }),
         withPackageLock: async (_n: string, fn: () => Promise<unknown>) =>
            fn(),
      });
      ctx.repository.getMaterializationById.resolves(
         makeMaterialization({
            status: "MANIFEST_FILE_READY",
            manifest: {
               builtAt: new Date().toISOString(),
               strict: false,
               entries: {
                  b1: {
                     sourceEntityId: "b1",
                     physicalTableName: "orders_mz",
                     connectionName: "duckdb",
                  },
               },
            },
         }),
      );
   }

   it("meters a successful physical-table drop as outcome=success", async () => {
      dropTablesFixture(sinon.stub().resolves());

      await ctx.service.deleteMaterialization("my-env", "pkg", "mat-1", {
         dropTables: true,
      });

      expect(
         await harness.collectCounter(DROP_COUNTER, { outcome: "success" }),
      ).toBe(1);
      expect(
         await harness.collectCounter(DROP_COUNTER, { outcome: "failure" }),
      ).toBe(0);
   });

   it("tags the retiring drops as ops work", async () => {
      // A drop is warehouse work someone accounts for later, and it is the one
      // statement an operator goes looking for after a table disappears.
      const runSQL = sinon.stub().resolves();
      dropTablesFixture(runSQL);

      await ctx.service.deleteMaterialization("my-env", "pkg", "mat-1", {
         dropTables: true,
      });

      expect(runSQL.callCount).toBeGreaterThan(0);
      for (const call of runSQL.getCalls()) {
         expect(call.args[1]?.queryMetadata).toMatchObject({
            class: "ops",
            environment: "my-env",
            package: "pkg",
            run_id: "mat-1",
         });
      }
   });

   it("meters a failed drop as outcome=failure and still deletes the record", async () => {
      dropTablesFixture(sinon.stub().rejects(new Error("drop boom")));

      // The drop is best-effort: a failure is metered but never surfaces to the
      // caller, and the record is still removed.
      await ctx.service.deleteMaterialization("my-env", "pkg", "mat-1", {
         dropTables: true,
      });

      expect(
         await harness.collectCounter(DROP_COUNTER, { outcome: "failure" }),
      ).toBe(1);
      expect(ctx.repository.deleteMaterialization.calledOnceWith("mat-1")).toBe(
         true,
      );
   });
});

describe("stagingSuffix", () => {
   it("derives a short, stable suffix from the sourceEntityId", () => {
      expect(stagingSuffix("abcdef1234567890")).toBe("_abcdef123456");
   });
});

describe("manifestExcludingStorage (chained-storage inline)", () => {
   it("drops storage entries, keeps colocated entries with the manifest's quoting, preserves strict", () => {
      // The source manifest is already dialect-quoted by the seed loop (#904);
      // manifestExcludingStorage reuses that quoting for the kept entries.
      const manifest = new Manifest();
      manifest.strict = true;
      manifest.update("wh_up", { tableName: '"wh_up_v1"' });
      manifest.update("lake_up", { tableName: '"lake"."lake_up__mabc"' });
      const built: Record<string, unknown> = {
         wh_up: {
            sourceEntityId: "wh_up",
            physicalTableName: "wh_up_v1",
            connectionName: "duckdb",
            // no storageDestinationName -> colocated, keep it
         },
         lake_up: {
            sourceEntityId: "lake_up",
            physicalTableName: "lake_up__mabc",
            connectionName: "wh",
            storageDestinationName: "lake", // storage -> exclude it
         },
      };
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const reduced = manifestExcludingStorage(manifest, built as any) as any;
      expect(Object.keys(reduced.entries)).toEqual(["wh_up"]);
      // Kept with the manifest's dialect-quoted name, not the raw physical name.
      expect(reduced.entries.wh_up.tableName).toBe('"wh_up_v1"');
      expect(reduced.entries.lake_up).toBeUndefined();
      expect(reduced.strict).toBe(true);
   });
});

describe("autoLoadManifest", () => {
   let ctx: ReturnType<typeof createMocks>;
   beforeEach(() => {
      ctx = createMocks();
   });

   it("binds every entry it is given, since a failed source is not one", () => {
      // This is the path that rewrites a query's FROM. It binds what `entries`
      // holds without asking whether each one built, which is only safe because
      // a failed source is reported in `failures` and never reaches here -- so
      // the coverage that matters is the producer's split (below), not a filter
      // on this side.
      const reload = sinon.stub().resolves();
      const environment = {
         reloadAllModelsForPackage: reload,
         bindPackageStorageServeBindings: sinon.stub().resolves(),
      };

      void (
         ctx.service as unknown as {
            autoLoadManifest: (
               env: unknown,
               pkg: string,
               entries: Record<string, unknown>,
            ) => Promise<void>;
         }
      ).autoLoadManifest(environment, "pkg", {
         ok: {
            sourceEntityId: "ok",
            sourceName: "healthy",
            physicalTableName: "ok_v1",
         },
      });

      const bound = reload.firstCall?.args[1] ?? {};
      expect(Object.keys(bound)).toContain("ok");
   });
});

describe("tallySources", () => {
   it("counts a failed source as failed, never as built", () => {
      // The counts are what an operator sees; the reason only exists inside the
      // manifest. A run that lost a source and still reported every source built
      // would leave the failure invisible outside the manifest JSON.
      const tally = tallySources(
         {
            ok: {
               sourceEntityId: "ok",
               sourceName: "healthy",
               physicalTableName: "ok_v1",
            },
         },
         {
            bad: {
               sourceEntityId: "bad",
               sourceName: "broken",
               physicalTableName: "bad_v1",
               reason: "Permission denied while writing to dataset analytics",
            },
         },
         {},
      );

      expect(tally.sourcesFailed).toBe(1);
      expect(tally.sourcesBuilt, "a failed source is not a built one").toBe(1);
   });

   it("does not count a carried source as built", () => {
      // A reused table was not built by this run. Counting it as built would
      // report work the run never did.
      const carried = {
         reused: {
            sourceEntityId: "reused",
            sourceName: "prior",
            physicalTableName: "prior_v1",
         },
      };
      const tally = tallySources(
         {
            ...carried,
            fresh: {
               sourceEntityId: "fresh",
               sourceName: "new",
               physicalTableName: "new_v1",
            },
         },
         {},
         carried,
      );

      expect(tally.sourcesBuilt).toBe(1);
      expect(tally.sourcesReused).toBe(1);
      expect(tally.sourcesFailed).toBe(0);
   });
});

describe("commitManifest", () => {
   let ctx: ReturnType<typeof createMocks>;
   beforeEach(() => {
      ctx = createMocks();
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

   type CommitFn = (
      id: string,
      entries: Record<string, unknown>,
      failures: Record<string, unknown>,
      metadata: Record<string, unknown>,
   ) => Promise<void>;

   /** The manifest the run PERSISTED, read off the repository write. */
   function persistedManifest(): { entries?: unknown; failures?: unknown } {
      const call = ctx.repository.updateMaterialization
         .getCalls()
         .find((c) => c.args[1]?.manifest);
      return (call?.args[1] as { manifest: Record<string, unknown> }).manifest;
   }

   it("persists a failed source's reason on the manifest the control plane reads", async () => {
      // The reason only reaches a consumer if it is written into the stored
      // manifest. Everything upstream of this can be correct while the field is
      // dropped on the way to the store, which would leave a consumer inferring a
      // partial failure from an absent entry -- exactly the state this replaces.
      await (
         ctx.service as unknown as { commitManifest: CommitFn }
      ).commitManifest(
         "mat-1",
         {
            ok: {
               sourceEntityId: "ok",
               sourceName: "healthy",
               physicalTableName: "ok_v1",
            },
         },
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
      await (
         ctx.service as unknown as { commitManifest: CommitFn }
      ).commitManifest(
         "mat-1",
         {
            ok: {
               sourceEntityId: "ok",
               sourceName: "healthy",
               physicalTableName: "ok_v1",
            },
         },
         {},
         { mode: "auto" },
      );

      expect(persistedManifest()).not.toHaveProperty("failures");
   });
});

describe("getMostRecentManifestEntries (legacy tolerance)", () => {
   let ctx: ReturnType<typeof createMocks>;
   beforeEach(() => {
      ctx = createMocks();
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

      const entries = await (
         ctx.service as unknown as {
            getMostRecentManifestEntries: (
               envId: string,
               pkg: string,
               excludeId: string,
            ) => Promise<Record<string, unknown>>;
         }
      ).getMostRecentManifestEntries("env-1", "pkg", "mat-current");

      expect(Object.keys(entries)).toEqual(["healthy"]);
   });
});

describe("deriveSelfInstructions", () => {
   let ctx: ReturnType<typeof createMocks>;
   beforeEach(() => {
      ctx = createMocks();
   });

   it("rebuilds a source the prior run failed on, and reuses the one it built", () => {
      // Reuse reads a prior manifest's `entries`, and a source that failed is not
      // in it -- so the source has nothing to carry and is instructed to build
      // again. That is what keeps a warehouse error that clears on its own (an
      // expired grant, a dataset created a minute later) from retiring the source
      // from every later run. Asserted through the shape a real prior manifest
      // has, so it holds for the reason the code holds rather than by construction
      // of the fixture: `s2` is absent from `entries` because it failed.
      const compiled = compiledWith(
         {
            s1: fakeSource({ name: "s1", sourceEntityId: "b1aaaaaaaaaaaaaa" }),
            s2: fakeSource({ name: "s2", sourceEntityId: "b2bbbbbbbbbbbbbb" }),
         },
         [["s1", "s2"]],
      );
      const priorEntries = {
         b1aaaaaaaaaaaaaa: {
            sourceEntityId: "b1aaaaaaaaaaaaaa",
            physicalTableName: "s1_prev",
            connectionName: "duckdb",
         },
      };

      const { instructions, carried } = (
         ctx.service as unknown as {
            deriveSelfInstructions: (
               c: unknown,
               n: string[] | undefined,
               p: unknown,
            ) => { instructions: BuildInstruction[]; carried: unknown };
         }
      ).deriveSelfInstructions(compiled, undefined, priorEntries);

      const carriedMap = carried as Record<string, unknown>;
      expect(
         carriedMap["b2bbbbbbbbbbbbbb"],
         "a source the prior run failed on must not be carried forward",
      ).toBeUndefined();
      expect(
         instructions.map((i) => i.sourceEntityId),
         "a source the prior run failed on must be instructed to build again",
      ).toContain("b2bbbbbbbbbbbbbb");
      // The healthy one is still reused, so this is not simply refusing to carry
      // anything.
      expect(carriedMap["b1aaaaaaaaaaaaaa"]).toBeDefined();
   });

   it("carries forward unchanged sourceEntityIds and builds the rest (deduping repeats)", () => {
      const compiled = compiledWith(
         {
            s1: fakeSource({ name: "s1", sourceEntityId: "b1aaaaaaaaaaaaaa" }),
            s2: fakeSource({ name: "s2", sourceEntityId: "b2bbbbbbbbbbbbbb" }),
         },
         [["s1", "s2"], ["s2"]],
      );
      const priorEntries = {
         b1aaaaaaaaaaaaaa: {
            sourceEntityId: "b1aaaaaaaaaaaaaa",
            physicalTableName: "s1_prev",
            connectionName: "duckdb",
         },
      };

      const { instructions, carried } = (
         ctx.service as unknown as {
            deriveSelfInstructions: (
               c: unknown,
               n: string[] | undefined,
               p: unknown,
            ) => { instructions: BuildInstruction[]; carried: unknown };
         }
      ).deriveSelfInstructions(compiled, undefined, priorEntries);

      expect(instructions).toHaveLength(1);
      expect(instructions[0].sourceEntityId).toBe("b2bbbbbbbbbbbbbb");
      expect(instructions[0].physicalTableName).toBe("s2");
      expect(instructions[0].realization).toBe("COPY");
      expect(instructions[0].materializedTableId).toBe("local-b2bbbbbbbbbb");
      expect(
         (carried as Record<string, unknown>)["b1aaaaaaaaaaaaaa"],
      ).toBeDefined();
   });

   it("self-assigns the name= verbatim for a storage= source, exactly as a colocated build", () => {
      process.env.PERSIST_STORAGE_MODE = "on";
      try {
         const compiled = compiledWith(
            {
               lake_src: fakeSource({
                  name: "lake_src",
                  sourceEntityId: "aa11aa11aa11aa11aa11",
                  annotationFields: { storage: "lake", name: "daily" },
               }),
               wh_src: fakeSource({
                  name: "wh_src",
                  sourceEntityId: "bb22bb22bb22bb22bb22",
               }),
            },
            [["lake_src", "wh_src"]],
         );
         const { instructions } = (
            ctx.service as unknown as {
               deriveSelfInstructions: (
                  c: unknown,
                  n: string[] | undefined,
                  p: unknown,
               ) => { instructions: BuildInstruction[] };
            }
         ).deriveSelfInstructions(compiled, undefined, {});
         const byId = Object.fromEntries(
            instructions.map((i) => [i.sourceEntityId, i]),
         );
         const lake = byId["aa11aa11aa11aa11aa11"];
         const wh = byId["bb22bb22bb22bb22bb22"];
         // storage= → the `name=` value verbatim in the destination (no
         // content-hash decoration); only the destination differs from a colocated build.
         expect(lake.destination).toBe("lake");
         expect(lake.physicalTableName).toBe("daily");
         // colocated → the plain logical (source) name, no destination.
         expect(wh.destination).toBeUndefined();
         expect(wh.physicalTableName).toBe("wh_src");
      } finally {
         delete process.env.PERSIST_STORAGE_MODE;
      }
   });

   /** deriveSelfInstructions with defaults, for the row-level-authorize tests below. */
   function derive(compiled: unknown): { instructions: BuildInstruction[] } {
      return (
         ctx.service as unknown as {
            deriveSelfInstructions: (
               c: unknown,
               n: string[] | undefined,
               p: unknown,
            ) => { instructions: BuildInstruction[] };
         }
      ).deriveSelfInstructions(compiled, undefined, {});
   }

   /** A colocated (no `storage=`) fakeSource carrying an #(authorize) gate. */
   const authorizeGatedColocated = fakeSource({
      name: "s1",
      sourceEntityId: "c1c1c1c1c1c1c1c1",
      sourceDef: { blockNotes: ["#(authorize) true"] },
   });

   describe("colocated #(authorize) gate", () => {
      afterEach(() => {
         delete process.env.PERSIST_STORAGE_MODE;
      });

      it("refuses a colocated authorize-gated source with no compile-time gate outcome (fail-closed default)", () => {
         // No `sourceGateOutcomes` supplied — the relaxation below requires a
         // PROVEN row_level+attributed outcome, so an unclassified gate keeps
         // refusing exactly as it always has.
         const compiled = compiledWith({ s1: authorizeGatedColocated }, [
            ["s1"],
         ]);
         expect(() => derive(compiled)).toThrow(
            MaterializationEligibilityError,
         );
         expect(() => derive(compiled)).toThrow(/authorize/i);
      });

      // A `row_level` + `attributed` compile-time gate outcome proves the
      // entry point's own gate is a row filter and nothing else is reachable
      // beneath it, so colocated serving grafts exactly what a live query
      // would — see `assertColocatedPersistNotAuthorizeGated`'s doc.
      it("admits a colocated authorize-gated source whose compile-time outcome is row_level and attributed", () => {
         const compiled = compiledWith(
            { s1: authorizeGatedColocated },
            [["s1"]],
            new Map(),
            { s1: { classification: "row_level", attributed: true } },
         );
         expect(() => derive(compiled)).not.toThrow();
      });

      it("still refuses when the compile-time outcome is row_level but NOT attributed (a join-only gate outside identity reach)", () => {
         const compiled = compiledWith(
            { s1: authorizeGatedColocated },
            [["s1"]],
            new Map(),
            { s1: { classification: "row_level", attributed: false } },
         );
         expect(() => derive(compiled)).toThrow(
            MaterializationEligibilityError,
         );
      });

      it("still refuses when the compile-time outcome is rejected", () => {
         const compiled = compiledWith(
            { s1: authorizeGatedColocated },
            [["s1"]],
            new Map(),
            { s1: { classification: "rejected", attributed: true } },
         );
         expect(() => derive(compiled)).toThrow(
            MaterializationEligibilityError,
         );
      });

      it("leaves an ungated colocated source unaffected", () => {
         const compiled = compiledWith(
            {
               s1: fakeSource({
                  name: "s1",
                  sourceEntityId: "d1d1d1d1d1d1d1d1",
               }),
            },
            [["s1"]],
         );
         expect(() => derive(compiled)).not.toThrow();
      });

      it("still refuses a storage= authorize-gated source via the existing path, unchanged", () => {
         process.env.PERSIST_STORAGE_MODE = "on";
         const storageGated = fakeSource({
            name: "s1",
            sourceEntityId: "f1f1f1f1f1f1f1f1",
            annotationFields: { storage: "lake" },
            sourceDef: { blockNotes: ["#(authorize) true"] },
         });
         const compiled = compiledWith({ s1: storageGated }, [["s1"]]);
         // Same message as assertMaterializationEligible's storage-destination
         // refusal (no double-refusal, no changed message from the colocated path).
         expect(() => derive(compiled)).toThrow(
            /cannot be materialized into a storage destination/,
         );
      });
   });

   // An incremental source is the one case where an unchanged content address
   // does NOT mean there is nothing to do: its data moves while its SQL stays
   // put. Carrying it forward here would strand the delta path behind a check it
   // can never pass, so it is exempt — but only when the delta path could
   // actually act on the declaration.
   const INCREMENTAL_DECLARATION = {
      refresh: "incremental",
      incremental: true,
      declaredWatermark: true,
      declaredMergeKey: false,
      watermark: {
         name: "order_date",
         kind: "dimension" as const,
         malloyType: "date",
      },
      watermarkOrderable: true,
      mergeKeys: [],
      watermarkInMergeKeys: false,
      strategy: "range_replace" as const,
      malformed: [],
      unknownKeys: [],
      calculateFields: [],
      outputColumns: ["order_date", "revenue"],
   };

   /** deriveSelfInstructions with an incremental context for source `s1`. */
   function selfInstructions(
      compiled: unknown,
      priorEntries: unknown,
   ): { instructions: BuildInstruction[]; carried: Record<string, unknown> } {
      return (
         ctx.service as unknown as {
            deriveSelfInstructions: (
               c: unknown,
               n: string[] | undefined,
               p: unknown,
               i: unknown,
            ) => {
               instructions: BuildInstruction[];
               carried: Record<string, unknown>;
            };
         }
      ).deriveSelfInstructions(compiled, undefined, priorEntries, {
         environmentId: "env-1",
         packageName: "pkg",
         materializationId: "run-1",
         forceRefresh: false,
         now: new Date("2024-07-01T00:00:00Z"),
         declarations: { s1: INCREMENTAL_DECLARATION },
         materializers: {},
         ledger: {},
      });
   }

   it("does NOT carry an incremental source forward, even at an unchanged address", () => {
      const compiled = compiledWith(
         {
            s1: fakeSource({
               name: "s1",
               sourceEntityId: "b1aaaaaaaaaaaaaa",
               connectionName: "wh",
               dialectName: "postgres",
               annotationFields: {
                  refresh: "incremental",
                  watermark: "order_date",
               },
            }),
         },
         [["s1"]],
      );
      const priorEntries = {
         b1aaaaaaaaaaaaaa: {
            sourceEntityId: "b1aaaaaaaaaaaaaa",
            physicalTableName: "s1",
            connectionName: "wh",
         },
      };

      const { instructions, carried } = selfInstructions(
         compiled,
         priorEntries,
      );

      // Instructed, so the build reaches the dispatch and the ledger decides.
      expect(instructions).toHaveLength(1);
      expect(instructions[0].sourceEntityId).toBe("b1aaaaaaaaaaaaaa");
      expect(Object.keys(carried)).toHaveLength(0);
   });

   it("still carries forward an incremental source the delta path cannot act on", () => {
      // Same declaration on DuckDB, which is off the incremental dialect
      // allowlist. The delta will not run, so exempting it would rebuild the
      // table in full on every run in the name of an incremental refresh that
      // never happens.
      const compiled = compiledWith(
         {
            s1: fakeSource({
               name: "s1",
               sourceEntityId: "b1aaaaaaaaaaaaaa",
               annotationFields: {
                  refresh: "incremental",
                  watermark: "order_date",
               },
            }),
         },
         [["s1"]],
      );
      const priorEntries = {
         b1aaaaaaaaaaaaaa: {
            sourceEntityId: "b1aaaaaaaaaaaaaa",
            physicalTableName: "s1",
            connectionName: "duckdb",
         },
      };

      const { instructions, carried } = selfInstructions(
         compiled,
         priorEntries,
      );

      expect(instructions).toHaveLength(0);
      expect(carried["b1aaaaaaaaaaaaaa"]).toBeDefined();
   });

   it("honors the sourceNames filter (excluded sources are neither built nor carried)", () => {
      const compiled = compiledWith(
         {
            s1: fakeSource({ name: "s1", sourceEntityId: "b1aaaaaaaaaaaaaa" }),
            s2: fakeSource({ name: "s2", sourceEntityId: "b2bbbbbbbbbbbbbb" }),
         },
         [["s1", "s2"]],
      );
      const { instructions, carried } = (
         ctx.service as unknown as {
            deriveSelfInstructions: (
               c: unknown,
               n: string[] | undefined,
               p: unknown,
            ) => { instructions: BuildInstruction[]; carried: unknown };
         }
      ).deriveSelfInstructions(compiled, ["s2"], {});

      expect(instructions).toHaveLength(1);
      expect(instructions[0].sourceEntityId).toBe("b2bbbbbbbbbbbbbb");
      expect(Object.keys(carried as Record<string, unknown>)).toHaveLength(0);
   });
});

describe("getMostRecentManifestEntries (skip-if-unchanged)", () => {
   let ctx: ReturnType<typeof createMocks>;
   beforeEach(() => {
      ctx = createMocks();
   });

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

      const result = await (
         ctx.service as unknown as {
            getMostRecentManifestEntries: (
               e: string,
               p: string,
               x: string,
            ) => Promise<unknown>;
         }
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

      const result = await (
         ctx.service as unknown as {
            getMostRecentManifestEntries: (
               e: string,
               p: string,
               x: string,
            ) => Promise<unknown>;
         }
      ).getMostRecentManifestEntries("env-1", "pkg", "self");

      expect(result).toEqual({});
   });
});

describe("stopMaterialization (in-flight)", () => {
   let ctx: ReturnType<typeof createMocks>;
   beforeEach(() => {
      ctx = createMocks();
   });

   it("aborts a running build cooperatively without forcing a transition", async () => {
      ctx.repository.getMaterializationById.resolves(
         makeMaterialization({ status: "MANIFEST_ROWS_READY" }),
      );
      const controller = new AbortController();
      (
         ctx.service as unknown as {
            runningAbortControllers: Map<string, AbortController>;
         }
      ).runningAbortControllers.set("mat-1", controller);

      const result = await ctx.service.stopMaterialization(
         "my-env",
         "pkg",
         "mat-1",
      );

      expect(controller.signal.aborted).toBe(true);
      // The background run records the terminal state; stop must not also write.
      expect(ctx.repository.updateMaterialization.called).toBe(false);
      expect(result.status).toBe("MANIFEST_ROWS_READY");
   });
});

describe("executeInstructedBuild", () => {
   let ctx: ReturnType<typeof createMocks>;
   beforeEach(() => {
      ctx = createMocks();
   });

   type ExecuteResult = {
      entries: Record<string, { physicalTableName?: string }>;
      failures: Record<
         string,
         { reason?: string; physicalTableName?: string; sourceName?: string }
      >;
   };

   function callExecute(
      compiled: unknown,
      instructions: BuildInstruction[],
      seed: Record<string, unknown>,
      apiConnections?: Record<string, unknown>,
   ): Promise<ExecuteResult> {
      // A stub BuildEnvironment. The `storage=` branch reads it, and so does the
      // per-source failure path, which redacts against the connection's CONFIG.
      // Left unsupplied it throws, as the real one does for an unknown name --
      // which is the fallback these tests want to exercise.
      const environment = {
         getApiConnection: (name: string) => {
            const config = apiConnections?.[name];
            if (!config) {
               throw new Error("no connection config in this test");
            }
            return config;
         },
         getEnvironmentPath: () => "/tmp/env",
      };
      return (
         ctx.service as unknown as {
            executeInstructedBuild: (
               c: unknown,
               env: unknown,
               i: BuildInstruction[],
               s: Record<string, unknown>,
               sig: AbortSignal,
            ) => Promise<ExecuteResult>;
         }
      ).executeInstructedBuild(
         compiled,
         environment,
         instructions,
         seed,
         new AbortController().signal,
      );
   }

   it("builds instructed sources in dependency order and seeds carried entries", async () => {
      const runSQL = sinon.stub().resolves();
      const connection = { runSQL } as unknown as MalloyConnection;
      const s1 = fakeSource({ name: "s1", sourceEntityId: "b1aaaaaaaaaaaaaa" });
      const s2 = fakeSource({ name: "s2", sourceEntityId: "b2bbbbbbbbbbbbbb" });
      const compiled = compiledWith(
         { s1, s2 },
         [["s1"], ["s2"]],
         new Map([["duckdb", connection]]),
      );

      const { entries } = await callExecute(
         compiled,
         [
            {
               sourceEntityId: "b1aaaaaaaaaaaaaa",
               materializedTableId: "mt-1",
               physicalTableName: "s1_v1",
               realization: "COPY",
            },
            {
               sourceEntityId: "b2bbbbbbbbbbbbbb",
               materializedTableId: "mt-2",
               physicalTableName: "s2_v1",
               realization: "COPY",
            },
         ],
         {
            carried0: {
               sourceEntityId: "carried0",
               physicalTableName: "carried_tbl",
               connectionName: "duckdb",
            },
         },
      );

      // Both instructed sources built, plus the carried seed entry retained.
      expect(entries["b1aaaaaaaaaaaaaa"].physicalTableName).toBe("s1_v1");
      expect(entries["b2bbbbbbbbbbbbbb"].physicalTableName).toBe("s2_v1");
      expect(entries["carried0"].physicalTableName).toBe("carried_tbl");
   });

   it("does not seed a downstream build from a seed with no table name", async () => {
      // The seed feeds a downstream persist's FROM through the in-memory build
      // Manifest, so a seed that names no table would compile a dependent source
      // against a table that does not exist. A source that FAILED cannot arrive
      // here -- seeds come from a prior manifest's `entries`, which holds only
      // sources that built -- so what remains to guard is a malformed seed.
      const runSQL = sinon.stub().resolves();
      const connection = { runSQL } as unknown as MalloyConnection;
      const only = fakeSource({
         name: "only",
         sourceEntityId: "eonlyaaaaaaaaaa",
      });
      const compiled = {
         graphs: [
            {
               connectionName: "duckdb",
               nodes: [[{ sourceID: "only", dependsOn: [] }]],
            },
         ],
         sources: { only },
         connectionDigests: { duckdb: "dig" },
         connections: new Map([["duckdb", connection]]),
      };

      const updates: string[] = [];
      const manifestSpy = sinon
         .stub(Manifest.prototype, "update")
         .callsFake(function (this: unknown, key: string) {
            updates.push(key);
         });
      try {
         await callExecute(
            compiled,
            [
               {
                  sourceEntityId: "eonlyaaaaaaaaaa",
                  materializedTableId: "mt-o",
                  physicalTableName: "only_v1",
                  realization: "COPY",
               },
            ],
            {
               seedNameless: {
                  sourceEntityId: "seedNameless",
                  sourceName: "upstream",
               },
               seedOk: {
                  sourceEntityId: "seedOk",
                  sourceName: "goodUpstream",
                  physicalTableName: "good_upstream_v1",
                  connectionName: "duckdb",
               },
            },
         );
      } finally {
         manifestSpy.restore();
      }

      expect(
         updates,
         "a healthy seed is still bound, so the guard is not refusing everything",
      ).toContain("seedOk");
      expect(
         updates,
         "a seed with no table name must not be bound into the build manifest",
      ).not.toContain("seedNameless");
   });

   it("keeps healthy sources when one of several fails, and records why it failed", async () => {
      // A run that builds several sources and loses one is a partial failure, not
      // a total one: the sources that did build are usable, and the one that did
      // not carries the reason it gave. Dropping the whole build instead makes a
      // package with one bad source indistinguishable from a package that is
      // entirely broken, and discards tables that were already written.
      const runSQL = sinon.stub().callsFake(async (sql: string) => {
         if (String(sql).includes("bad_v1")) {
            throw new Error(
               "Permission denied while writing to dataset analytics",
            );
         }
      });
      const connection = { runSQL } as unknown as MalloyConnection;
      const good = fakeSource({
         name: "good",
         sourceEntityId: "bgoodaaaaaaaaaa",
      });
      const bad = fakeSource({
         name: "bad",
         sourceEntityId: "bbadbbbbbbbbbbb",
      });
      const compiled = {
         graphs: [
            {
               connectionName: "duckdb",
               nodes: [
                  [{ sourceID: "good", dependsOn: [] }],
                  [{ sourceID: "bad", dependsOn: [] }],
               ],
            },
         ],
         sources: { good, bad },
         connectionDigests: { duckdb: "dig" },
         connections: new Map([["duckdb", connection]]),
      };

      const { entries, failures } = await callExecute(
         compiled,
         [
            {
               sourceEntityId: "bgoodaaaaaaaaaa",
               materializedTableId: "mt-g",
               physicalTableName: "good_v1",
               realization: "COPY",
            },
            {
               sourceEntityId: "bbadbbbbbbbbbbb",
               materializedTableId: "mt-b",
               physicalTableName: "bad_v1",
               realization: "COPY",
            },
         ],
         {},
      );

      expect(
         entries["bgoodaaaaaaaaaa"]?.physicalTableName,
         "a source that built must still be usable when a sibling failed",
      ).toBe("good_v1");

      const failed = failures["bbadbbbbbbbbbbb"];
      expect(
         failed,
         "a failed source must be reported in failures, carrying its reason",
      ).toBeDefined();
      expect(failed?.reason).toContain(
         "Permission denied while writing to dataset analytics",
      );
      expect(
         failed?.physicalTableName,
         "the failure names the table the source was headed for",
      ).toBe("bad_v1");

      // Mirrored into `entries` carrying `error`, for the deprecation window only.
      // A consumer built against 0.0.245-0.0.246 reads the failure from there, and
      // would otherwise see this source as merely ABSENT -- indistinguishable from
      // a source nobody asked about, which is the fail-dangerous reading. Delete
      // this assertion with the field.
      const mirrored = entries["bbadbbbbbbbbbbb"] as { error?: string };
      expect(
         mirrored?.error,
         "the deprecated entry must carry the same reason while it exists",
      ).toContain("Permission denied while writing to dataset analytics");
   });

   it("redacts a failed source's reason before recording it", async () => {
      // The reason is the warehouse's own text and travels to a consumer that
      // persists and renders it. A connection error commonly echoes the
      // credentials it was handed, so the reason a failed source carries has to
      // be scrubbed the same way the run-level message is.
      const secret = "super-secret-token-value";
      const runSQL = sinon.stub().callsFake(async (sql: string) => {
         if (String(sql).includes("bad_v1")) {
            throw new Error(`auth failed using ${secret} against analytics`);
         }
      });
      // Deliberately carries NO secret: the secret is declared in the config
      // below, so this test fails if the redaction walks the live connection
      // instead of the config.
      const connection = {
         runSQL,
         toString: () => secret,
         name: "duckdb",
      } as unknown as MalloyConnection;
      const good = fakeSource({
         name: "good",
         sourceEntityId: "cgoodaaaaaaaaaa",
      });
      const bad = fakeSource({
         name: "bad",
         sourceEntityId: "cbadbbbbbbbbbbb",
      });
      const compiled = {
         graphs: [
            {
               connectionName: "duckdb",
               nodes: [
                  [{ sourceID: "good", dependsOn: [] }],
                  [{ sourceID: "bad", dependsOn: [] }],
               ],
            },
         ],
         sources: { good, bad },
         connectionDigests: { duckdb: "dig" },
         connections: new Map([["duckdb", connection]]),
      };

      const { failures } = await callExecute(
         compiled,
         [
            {
               sourceEntityId: "cgoodaaaaaaaaaa",
               materializedTableId: "mt-g",
               physicalTableName: "good_v1",
               realization: "COPY",
            },
            {
               sourceEntityId: "cbadbbbbbbbbbbb",
               materializedTableId: "mt-b",
               physicalTableName: "bad_v1",
               realization: "COPY",
            },
         ],
         {},
         // The redaction reads the connection's CONFIG, which is where a declared
         // secret lives.
         {
            duckdb: {
               name: "duckdb",
               type: "postgres",
               postgresConnection: { password: secret },
            },
         },
      );

      expect(
         failures["cbadbbbbbbbbbbb"]?.reason,
         "a per-source reason must not echo the connection's secrets",
      ).not.toContain(secret);
      expect(
         failures["cbadbbbbbbbbbbb"]?.reason,
         "and it must still say what went wrong",
      ).toContain("auth failed");
   });

   it("still redacts a failed source's reason when the config is unavailable", async () => {
      // getApiConnection throws for a connection the environment does not hold.
      // Falling back to the unredacted message would reintroduce the leak the
      // test above defends, so the fallback is the connection-free redactor --
      // which is what catches a DSN echoed by a connect failure.
      const dsn = "postgres://admin:hunter2@db.internal:5432/analytics";
      const runSQL = sinon.stub().callsFake(async (sql: string) => {
         if (String(sql).includes("bad_v1")) {
            throw new Error(`connect failed: ${dsn}`);
         }
      });
      const connection = { runSQL } as unknown as MalloyConnection;
      // A surviving source, so this is a PARTIAL failure: a run that loses every
      // source throws instead of recording per-source reasons.
      const good = fakeSource({
         name: "good",
         sourceEntityId: "cgoodaaaaaaaaaa",
      });
      const bad = fakeSource({
         name: "bad",
         sourceEntityId: "cbadbbbbbbbbbbb",
      });
      const compiled = {
         graphs: [
            {
               connectionName: "duckdb",
               nodes: [
                  [{ sourceID: "good", dependsOn: [] }],
                  [{ sourceID: "bad", dependsOn: [] }],
               ],
            },
         ],
         sources: { good, bad },
         connectionDigests: { duckdb: "dig" },
         connections: new Map([["duckdb", connection]]),
      };

      // No apiConnections argument: getApiConnection throws.
      const { failures } = await callExecute(
         compiled,
         [
            {
               sourceEntityId: "cgoodaaaaaaaaaa",
               materializedTableId: "mt-g",
               physicalTableName: "good_v1",
               realization: "COPY",
            },
            {
               sourceEntityId: "cbadbbbbbbbbbbb",
               materializedTableId: "mt-b",
               physicalTableName: "bad_v1",
               realization: "COPY",
            },
         ],
         {},
      );

      const reason = failures["cbadbbbbbbbbbbb"]?.reason ?? "";
      expect(reason, "the run must not fail outright").not.toBe("");
      expect(reason, "the DSN's password must not survive").not.toContain(
         "hunter2",
      );
      expect(reason, "and it must still say what went wrong").toContain(
         "connect failed",
      );
   });

   it("keeps a failed source's entry out of the storage tables it reclaims", async () => {
      // Reclaim exists for a build that records nothing, because manifest-driven
      // collection only drops names a manifest carries. A partial failure now
      // does record its sources, so the tables that built stay reachable and must
      // not be dropped underneath the manifest that names them.
      const runSQL = sinon.stub().callsFake(async (sql: string) => {
         if (String(sql).includes("bad_v1")) {
            throw new Error("write rejected");
         }
      });
      const connection = { runSQL } as unknown as MalloyConnection;
      const good = fakeSource({
         name: "good",
         sourceEntityId: "dgoodaaaaaaaaaa",
      });
      const bad = fakeSource({
         name: "bad",
         sourceEntityId: "dbadbbbbbbbbbbb",
      });
      const compiled = {
         graphs: [
            {
               connectionName: "duckdb",
               nodes: [
                  [{ sourceID: "good", dependsOn: [] }],
                  [{ sourceID: "bad", dependsOn: [] }],
               ],
            },
         ],
         sources: { good, bad },
         connectionDigests: { duckdb: "dig" },
         connections: new Map([["duckdb", connection]]),
      };

      const { entries, failures } = await callExecute(
         compiled,
         [
            {
               sourceEntityId: "dgoodaaaaaaaaaa",
               materializedTableId: "mt-g",
               physicalTableName: "good_v1",
               realization: "COPY",
            },
            {
               sourceEntityId: "dbadbbbbbbbbbbb",
               materializedTableId: "mt-b",
               physicalTableName: "bad_v1",
               realization: "COPY",
            },
         ],
         {},
      );

      // The healthy table is named by the manifest this build returns, so
      // manifest-driven collection can reach it: the build did not abandon it.
      expect(entries["dgoodaaaaaaaaaa"]?.physicalTableName).toBe("good_v1");
      // And the failed source is reported rather than dropped from the result, so
      // a consumer sees which one failed instead of an entry that is simply absent.
      const failed = failures["dbadbbbbbbbbbbb"];
      expect(failed?.sourceName).toBe("bad");
      expect(failed?.reason).toContain("write rejected");
      // The build returned normally, so the run is a partial success rather than a
      // total failure -- that is what keeps the healthy table's manifest committed
      // and therefore reachable, instead of reclaimed as an orphan.
      expect(Object.keys(entries).sort()).toEqual([
         "dbadbbbbbbbbbbb",
         "dgoodaaaaaaaaaa",
      ]);
      expect(Object.keys(failures)).toEqual(["dbadbbbbbbbbbbb"]);
   });

   it("overwrites a seeded entry when the same source is instructed and fails", async () => {
      // A source can be BOTH seeded and instructed: a reference manifest applies no
      // exclusion for instructed sources, and the bound-manifest seed excludes on
      // the caller's `instruction.sourceEntityId` while a failure keys on the
      // content address the publisher computes -- which the build treats as an
      // opaque caller-assigned value, so the two can differ by design.
      //
      // The seed names the PRIOR generation's table, which for a stable auto-run
      // name is a table that really exists. Leaving it in `entries` beside the
      // failure would let a serve binding resolve it and serve last generation's
      // data as though this run had produced it -- the failure mode that is worse
      // than a missing table, because nothing reports it.
      // A healthy sibling keeps this a PARTIAL failure: an all-failed build throws
      // rather than returning a manifest, so the overlap would never be observable.
      const runSQL = sinon.stub().callsFake(async (sql: string) => {
         if (String(sql).includes("bad_v2")) throw new Error("boom");
      });
      const connection = { runSQL } as unknown as MalloyConnection;
      const bad = fakeSource({
         name: "bad",
         sourceEntityId: "bsameaaaaaaaaaa",
      });
      const good = fakeSource({
         name: "good",
         sourceEntityId: "bfineaaaaaaaaaa",
      });
      const compiled = compiledWith(
         { bad, good },
         [["bad", "good"]],
         new Map([["duckdb", connection]]),
      );

      const { entries, failures } = await callExecute(
         compiled,
         [
            {
               sourceEntityId: "bsameaaaaaaaaaa",
               materializedTableId: "mt-b",
               physicalTableName: "bad_v2",
               realization: "COPY",
            },
            {
               sourceEntityId: "bfineaaaaaaaaaa",
               materializedTableId: "mt-g",
               physicalTableName: "good_v1",
               realization: "COPY",
            },
         ],
         {
            // Same source, seeded with the generation a previous run built.
            bsameaaaaaaaaaa: {
               sourceEntityId: "bsameaaaaaaaaaa",
               sourceName: "bad",
               physicalTableName: "bad_v1_prior_generation",
               connectionName: "duckdb",
            },
         },
      );

      expect(failures["bsameaaaaaaaaaa"]?.reason).toContain("boom");
      const entry = entries["bsameaaaaaaaaaa"] as {
         error?: string;
         physicalTableName?: string;
      };
      expect(
         entry?.error,
         "the seeded entry must be replaced by the failure, not left beside it",
      ).toContain("boom");
      expect(
         entry?.physicalTableName,
         "the prior generation's table name must not survive on the entry",
      ).not.toBe("bad_v1_prior_generation");
   });

   it("fails the build when every source fails", async () => {
      // The complement of the partial case, and the reason it is safe: a build
      // whose every source failed produced nothing, so it must not report itself
      // as a build that succeeded with errors attached.
      const runSQL = sinon.stub().rejects(new Error("warehouse unreachable"));
      const connection = { runSQL } as unknown as MalloyConnection;
      const only = fakeSource({
         name: "only",
         sourceEntityId: "bonlyaaaaaaaaaa",
      });
      const compiled = {
         graphs: [
            {
               connectionName: "duckdb",
               nodes: [[{ sourceID: "only", dependsOn: [] }]],
            },
         ],
         sources: { only },
         connectionDigests: { duckdb: "dig" },
         connections: new Map([["duckdb", connection]]),
      };

      await expect(
         callExecute(
            compiled,
            [
               {
                  sourceEntityId: "bonlyaaaaaaaaaa",
                  materializedTableId: "mt-o",
                  physicalTableName: "only_v1",
                  realization: "COPY",
               },
            ],
            {},
         ),
      ).rejects.toThrow(/warehouse unreachable/);
   });

   it("builds an intermediate persist source nested under a root node", async () => {
      // Mirrors malloy getBuildPlan(): only the terminal `root` is a graph
      // node; its persist dependency `mid` is nested in dependsOn (not a node).
      // Before the iterGraphSources fix, `mid` was silently never built even
      // though the caller instructed it. Both must now build, `mid` first.
      const runSQL = sinon.stub().resolves();
      const connection = { runSQL } as unknown as MalloyConnection;
      const root = fakeSource({
         name: "root",
         sourceEntityId: "brootaaaaaaaaaa",
      });
      const mid = fakeSource({
         name: "mid",
         sourceEntityId: "bmidbbbbbbbbbbb",
      });
      const compiled = {
         graphs: [
            {
               connectionName: "duckdb",
               nodes: [
                  [
                     {
                        sourceID: "root",
                        dependsOn: [{ sourceID: "mid", dependsOn: [] }],
                     },
                  ],
               ],
            },
         ],
         sources: { root, mid },
         connectionDigests: { duckdb: "dig" },
         connections: new Map([["duckdb", connection]]),
      };

      const { entries } = await callExecute(
         compiled,
         [
            {
               sourceEntityId: "brootaaaaaaaaaa",
               materializedTableId: "mt-r",
               physicalTableName: "root_v1",
               realization: "COPY",
            },
            {
               sourceEntityId: "bmidbbbbbbbbbbb",
               materializedTableId: "mt-m",
               physicalTableName: "mid_v1",
               realization: "COPY",
            },
         ],
         {},
      );

      expect(entries["bmidbbbbbbbbbbb"].physicalTableName).toBe("mid_v1");
      expect(entries["brootaaaaaaaaaa"].physicalTableName).toBe("root_v1");
      // The dependency's CREATE precedes its dependent's (dependency order).
      const creates = runSQL
         .getCalls()
         .map((c) => c.args[0] as string)
         .filter((s) => s.startsWith("CREATE TABLE"));
      const midIdx = creates.findIndex((s) => s.includes("mid_v1"));
      const rootIdx = creates.findIndex((s) => s.includes("root_v1"));
      expect(midIdx).toBeGreaterThanOrEqual(0);
      expect(rootIdx).toBeGreaterThan(midIdx);
   });

   it("seeds a downstream build with the QUOTED upstream reference (case-folding dialect)", async () => {
      // A carried/seeded upstream (built in a prior run or unit, e.g. via
      // referenceManifest) must reach the downstream's SQL generation as the
      // SAME quoted path the builder CREATEd it with; otherwise the downstream
      // CREATE misses the case-preserved table on a case-folding engine
      // (Snowflake). Capture the buildManifest Malloy is handed for the
      // downstream to prove the seed was quoted for its connection's dialect
      // before it becomes a FROM.
      let seen:
         | {
              buildManifest?: {
                 entries?: Record<string, { tableName?: string }>;
              };
           }
         | undefined;
      const runSQL = sinon.stub().resolves();
      const connection = {
         runSQL,
         dialectName: "snowflake",
      } as unknown as MalloyConnection;
      const down = fakeSource({
         name: "down",
         sourceEntityId: "bdownaaaaaaaaaa",
         dialectName: "snowflake",
         onGetSQL: (o) => {
            seen = o as typeof seen;
         },
      });
      const compiled = compiledWith(
         { down },
         [["down"]],
         new Map([["duckdb", connection]]),
      );

      await callExecute(
         compiled,
         [
            {
               sourceEntityId: "bdownaaaaaaaaaa",
               materializedTableId: "mt-d",
               physicalTableName: "down_v1",
               realization: "COPY",
            },
         ],
         {
            up: {
               sourceEntityId: "up",
               physicalTableName: "schema.orders_base",
               connectionName: "duckdb",
            },
         },
      );

      expect(seen?.buildManifest?.entries?.up?.tableName).toBe(
         '"schema"."orders_base"',
      );
   });

   it("seeds unquoted when the upstream's connection is absent from the build (older CP / cross-connection)", async () => {
      // No connectionName on the seed (an older control plane) -> bind verbatim,
      // exactly the pre-change behavior. Non-case-folding engines are unaffected;
      // a case-folding engine self-signals by failing the downstream CREATE.
      let seen:
         | {
              buildManifest?: {
                 entries?: Record<string, { tableName?: string }>;
              };
           }
         | undefined;
      const runSQL = sinon.stub().resolves();
      const connection = {
         runSQL,
         dialectName: "snowflake",
      } as unknown as MalloyConnection;
      const down = fakeSource({
         name: "down",
         sourceEntityId: "bdownaaaaaaaaaa",
         dialectName: "snowflake",
         onGetSQL: (o) => {
            seen = o as typeof seen;
         },
      });
      const compiled = compiledWith(
         { down },
         [["down"]],
         new Map([["duckdb", connection]]),
      );

      await callExecute(
         compiled,
         [
            {
               sourceEntityId: "bdownaaaaaaaaaa",
               materializedTableId: "mt-d",
               physicalTableName: "down_v1",
               realization: "COPY",
            },
         ],
         { up: { sourceEntityId: "up", physicalTableName: "orders_base" } },
      );

      expect(seen?.buildManifest?.entries?.up?.tableName).toBe("orders_base");
   });

   it("throws when an instructed graph's connection is missing", async () => {
      const s1 = fakeSource({ name: "s1", sourceEntityId: "b1aaaaaaaaaaaaaa" });
      const compiled = compiledWith({ s1 }, [["s1"]], new Map());

      await expect(
         callExecute(
            compiled,
            [
               {
                  sourceEntityId: "b1aaaaaaaaaaaaaa",
                  materializedTableId: "mt-1",
                  physicalTableName: "s1_v1",
                  realization: "COPY",
               },
            ],
            {},
         ),
      ).rejects.toThrow(BadRequestError);
   });

   describe("a refused sibling with no compile-time gate outcome", () => {
      // Colocated (no `storage=`) authorize-gated, and `compiledWith`'s default
      // `sourceGateOutcomes` is undefined — the fail-closed default the
      // colocated relaxation never admits, so this refuses unconditionally
      // (see the "colocated #(authorize) gate" describe above).
      function refusedColocated(sourceEntityId: string) {
         return fakeSource({
            name: "refused",
            sourceEntityId,
            sourceDef: { blockNotes: ["#(authorize) true"] },
         });
      }

      it("is skipped, and the run completes, when the caller never instructed it", async () => {
         // Before the reorder, the eligibility assert ran unconditionally on
         // EVERY persist source in the graph — including one with no
         // instruction at all — so this refused, uninstructed sibling threw
         // and aborted the whole run, taking `ok`'s build down with it.
         const runSQL = sinon.stub().resolves();
         const connection = { runSQL } as unknown as MalloyConnection;
         const ok = fakeSource({
            name: "ok",
            sourceEntityId: "b0k0k0k0k0k0k0k0",
         });
         const refused = refusedColocated("bref1bref1bref1b");
         const compiled = compiledWith(
            { ok, refused },
            [["ok"], ["refused"]],
            new Map([["duckdb", connection]]),
         );

         const { entries, failures } = await callExecute(
            compiled,
            [
               {
                  sourceEntityId: "b0k0k0k0k0k0k0k0",
                  materializedTableId: "mt-ok",
                  physicalTableName: "ok_v1",
                  realization: "COPY",
               },
               // No instruction for "refused" at all.
            ],
            {},
         );

         expect(entries["b0k0k0k0k0k0k0k0"].physicalTableName).toBe("ok_v1");
         // Skipped, not failed: the caller never asked for it, so it is
         // absent from both collections rather than reported as a failure.
         expect(entries["bref1bref1bref1b"]).toBeUndefined();
         expect(failures["bref1bref1bref1b"]).toBeUndefined();
      });

      it("still 422s when the caller DOES instruct the refused source", async () => {
         const runSQL = sinon.stub().resolves();
         const connection = { runSQL } as unknown as MalloyConnection;
         const ok = fakeSource({
            name: "ok",
            sourceEntityId: "b0k0k0k0k0k0k0k0",
         });
         const refused = refusedColocated("bref1bref1bref1b");
         const compiled = compiledWith(
            { ok, refused },
            [["ok"], ["refused"]],
            new Map([["duckdb", connection]]),
         );

         await expect(
            callExecute(
               compiled,
               [
                  {
                     sourceEntityId: "b0k0k0k0k0k0k0k0",
                     materializedTableId: "mt-ok",
                     physicalTableName: "ok_v1",
                     realization: "COPY",
                  },
                  {
                     sourceEntityId: "bref1bref1bref1b",
                     materializedTableId: "mt-ref",
                     physicalTableName: "refused_v1",
                     realization: "COPY",
                  },
               ],
               {},
            ),
         ).rejects.toThrow(MaterializationEligibilityError);
      });
   });
});

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
      // inlined/recomputed); a colocated upstream is kept.
      const runSQL = sinon.stub().resolves();
      let seenManifest:
         | { entries?: Record<string, { tableName?: string }> }
         | undefined;
      const down = fakeSource({
         name: "down",
         sourceEntityId: "downdowndowndown",
         sql: "SELECT 1",
         onGetSQL: (o) => {
            seenManifest = (o as { buildManifest?: typeof seenManifest })
               .buildManifest;
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
      expect(seenManifest?.entries?.up_storage).toBeUndefined();
      expect(seenManifest?.entries?.up_pathc?.tableName).toBe('"orders_v1"');
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

describe("buildOneSourceIntoStorage (chained-build fallback ladder)", () => {
   let ctx: ReturnType<typeof createMocks>;
   beforeEach(() => {
      ctx = createMocks();
   });

   function callInto(opts: {
      strict: boolean;
      stackOnParent: "ok" | "throw" | "infra";
   }): Promise<{
      physicalTableName: string;
      storageDestinationName?: string;
   }> {
      const source = fakeSource({
         name: "monthly",
         sourceEntityId: "abcdef1234567890",
         annotationFields: { storage: "lake" },
      });
      const instruction: BuildInstruction = {
         sourceEntityId: "abcdef1234567890",
         materializedTableId: "mt",
         physicalTableName: "monthly__mabc",
         realization: "COPY",
         destination: "lake",
      };
      const manifest = new Manifest();
      manifest.strict = opts.strict;
      const environment = buildEnvironmentStub({
         connections: { duckdb: { name: "duckdb", type: "duckdb" } },
         destinations: { lake: { name: "lake", type: "duckdb" } },
      });
      const svc = ctx.service as unknown as {
         buildDownstreamViaParents: unknown;
         buildOneSourceIntoStorage: (p: {
            persistSource: unknown;
            instruction: BuildInstruction;
            manifest: Manifest;
            environment: unknown;
            publicBuildSQL: string;
            buildSQL: string;
            builtEntries: Record<string, unknown>;
            dependsOnStorageUpstream: boolean;
         }) => Promise<{
            physicalTableName: string;
            storageDestinationName?: string;
         }>;
      };
      // Stub the stack-on-parent seam so the test exercises the LADDER, not the build.
      svc.buildDownstreamViaParents =
         opts.stackOnParent === "ok"
            ? sinon.stub().resolves({
                 storageDestinationName: "lake",
                 schema: [
                    { name: "order_month", type: "DATE" },
                    { name: "monthly_total", type: "DOUBLE" },
                 ],
              })
            : opts.stackOnParent === "infra"
              ? // An outage, not a modelling limit: a plain Error, as a failed
                // ATTACH or CTAS would throw.
                sinon.stub().rejects(new Error("IO Error: destination is down"))
              : // A shape limit: the downstream cannot be expressed over its
                // rebound parents.
                sinon.stub().rejects(
                   new MaterializationEligibilityError({
                      message: "uncarried parent",
                   }),
                );
      return svc.buildOneSourceIntoStorage({
         persistSource: source,
         instruction,
         manifest,
         environment,
         publicBuildSQL: "SELECT should_not_run",
         buildSQL: "SELECT should_not_run",
         builtEntries: {
            up: {
               sourceEntityId: "up",
               sourceName: "daily",
               physicalTableName: "daily__mabc",
               storageDestinationName: "lake",
               schema: [{ name: "order_date", type: "DATE" }],
            },
         },
         dependsOnStorageUpstream: true,
      });
   }

   it("stacks on the parent: builds by reading it and returns the storage entry", async () => {
      const entry = await callInto({ strict: false, stackOnParent: "ok" });
      expect(entry.storageDestinationName).toBe("lake");
      expect(entry.physicalTableName).toBe("monthly__mabc");
   });

   it("strict + cannot stack on the parent: refuses loudly instead of recomputing from raw", async () => {
      await expect(
         callInto({ strict: true, stackOnParent: "throw" }),
      ).rejects.toThrow(/strict upstreams forbid/i);
   });

   it("non-strict + an INFRA failure fails rather than recomputing from raw", async () => {
      // Recompute-from-raw writes to the same destination, so retrying an outage
      // there just fails again — and metering it as a fallback files the outage
      // under the same label as a legitimate shape miss. Only a shape failure is
      // a reason to try the other path.
      await expect(
         callInto({ strict: false, stackOnParent: "infra" }),
      ).rejects.toThrow(/Failed to materialize chained source/i);
   });

   it("non-strict + a SHAPE failure falls through to recompute-from-raw", async () => {
      // Only the parent-reuse seam is stubbed, so the recompute runs for real and
      // fails for its own reason (no destination file). That is the proof it was
      // reached: the two paths report differently — the chained path says
      // "chained source", the single-source recompute says "source". A shape
      // failure must reach the second; an infra failure must not.
      await expect(
         callInto({ strict: false, stackOnParent: "throw" }),
      ).rejects.toThrow(/Failed to materialize source 'monthly'/i);
   });
});

describe("buildOneSourceIntoStorage destination resolution", () => {
   let ctx: ReturnType<typeof createMocks>;
   beforeEach(() => {
      ctx = createMocks();
   });
   afterEach(() => ctx.sandbox.restore());

   const MANAGED_DESTINATION = {
      name: "shared",
      type: "ducklake",
      ducklakeConnection: {
         catalog: {
            postgresConnection: {
               host: "catalog.internal",
               password: "catalog-secret",
            },
         },
         storage: { bucketUrl: "gs://managed-tier/org_a" },
      },
   };

   /** The tenant's own warehouse, sharing a name with the destination above. */
   const TENANT_CONNECTION = {
      name: "shared",
      type: "postgres",
      postgresConnection: {
         host: "tenant.example.com",
         password: "tenant-secret",
      },
   };

   function callInto(environment: ReturnType<typeof buildEnvironmentStub>): {
      run: () => Promise<{ storageDestinationName?: string }>;
      write: sinon.SinonStub;
   } {
      // Stub the write seam so a resolved destination does not need a live
      // catalog: reaching it at all is what these tests are about.
      const write = sinon.stub().resolves({
         storageDestinationName: "shared",
         schema: [{ name: "order_month", type: "DATE" }],
      });
      (
         ctx.service as unknown as { buildDownstreamViaParents: unknown }
      ).buildDownstreamViaParents = write;

      const run = () =>
         (
            ctx.service as unknown as {
               buildOneSourceIntoStorage: (p: {
                  persistSource: unknown;
                  instruction: BuildInstruction;
                  manifest: unknown;
                  environment: unknown;
                  publicBuildSQL: string;
                  buildSQL: string;
                  builtEntries: Record<string, unknown>;
                  dependsOnStorageUpstream: boolean;
               }) => Promise<{ storageDestinationName?: string }>;
            }
         ).buildOneSourceIntoStorage({
            persistSource: fakeSource({
               name: "monthly",
               sourceEntityId: "abcdef1234567890",
               annotationFields: { storage: "shared" },
            }),
            instruction: {
               sourceEntityId: "abcdef1234567890",
               materializedTableId: "mt",
               physicalTableName: "monthly__mabc",
               realization: "COPY",
               destination: "shared",
            },
            manifest: new Manifest(),
            environment,
            publicBuildSQL: "SELECT should_not_run",
            buildSQL: "SELECT should_not_run",
            builtEntries: {
               up: {
                  sourceEntityId: "up",
                  sourceName: "daily",
                  physicalTableName: "daily__mabc",
                  storageDestinationName: "shared",
                  schema: [{ name: "order_date", type: "DATE" }],
               },
            },
            dependsOnStorageUpstream: true,
         });

      return { run, write };
   }

   it("materializes into the destination the destination list holds", async () => {
      const environment = buildEnvironmentStub({
         connections: { duckdb: { name: "duckdb", type: "duckdb" } },
         destinations: { shared: MANAGED_DESTINATION },
      });
      const { run, write } = callInto(environment);

      const entry = await run();

      expect(entry.storageDestinationName).toBe("shared");
      expect(environment.getStorageDestination.calledWith("shared")).toBe(true);
      // The config handed to the write is the destination's, by identity - not a
      // same-named connection's, and not a copy assembled from somewhere else.
      expect(write.firstCall.args[2]).toBe(MANAGED_DESTINATION);
      expect(environment.getApiConnection.calledWith("shared")).toBe(false);
   });

   it("refuses rather than writing into a same-named connection", async () => {
      // The hole this whole split exists to close: a `storage=` build naming a
      // destination that is not configured must fail, even when a connection of
      // that name would resolve, because that connection is the tenant's own
      // warehouse.
      const environment = buildEnvironmentStub({
         connections: {
            duckdb: { name: "duckdb", type: "duckdb" },
            shared: TENANT_CONNECTION,
         },
         destinations: {},
      });
      const { run, write } = callInto(environment);

      let error: unknown;
      try {
         await run();
      } catch (caught) {
         error = caught;
      }

      expect(error).toBeInstanceOf(DestinationNotFoundError);
      // Nothing was written, and the tenant's connection was never even resolved.
      expect(write.called).toBe(false);
      expect(environment.getApiConnection.calledWith("shared")).toBe(false);
      // Reported as a misconfigured target, not as a missing connection.
      expect(internalErrorToHttpError(error as Error).status).toBe(422);
   });
});

describe("runBuild (branch behavior)", () => {
   let ctx: ReturnType<typeof createMocks>;
   beforeEach(() => {
      ctx = createMocks();
   });

   // Exercise runBuild's orchestration in isolation: the build engine and
   // manifest commit are stubbed so the test asserts which instructions flow
   // through and whether the manifest is auto-loaded, not how a source is built.
   type RunBuildInternals = {
      executeInstructedBuild: sinon.SinonStub;
      commitManifest: sinon.SinonStub;
      autoLoadManifest: sinon.SinonStub;
      runBuild: (
         id: string,
         env: string,
         pkg: string,
         opts: {
            sourceNames: string[] | undefined;
            forceRefresh: boolean;
            buildInstructions: BuildInstruction[] | undefined;
            referenceManifest?:
               | {
                    sourceEntityId: string;
                    physicalTableName: string;
                    connectionName?: string;
                 }[]
               | undefined;
            strictUpstreams?: boolean | undefined;
         },
         signal: AbortSignal,
      ) => Promise<void>;
   };

   function stubEngine(): RunBuildInternals {
      const entries = {
         "build-orders": {
            sourceEntityId: "build-orders",
            physicalTableName: '"orders_v1"',
            connectionName: "duckdb",
         },
      };
      const svc = ctx.service as unknown as RunBuildInternals;
      svc.executeInstructedBuild = sinon
         .stub()
         .resolves({ entries, failures: {} });
      svc.commitManifest = sinon.stub().resolves();
      svc.autoLoadManifest = sinon.stub().resolves();
      return svc;
   }

   it("orchestrated: builds caller instructions and does not auto-load", async () => {
      const svc = stubEngine();
      const instructions = [makeInstruction()];

      await svc.runBuild(
         "mat-1",
         "my-env",
         "pkg",
         {
            sourceNames: undefined,
            forceRefresh: false,
            buildInstructions: instructions,
         },
         new AbortController().signal,
      );

      expect(svc.executeInstructedBuild.calledOnce).toBe(true);
      // Caller instructions pass straight through; nothing is carried/reused.
      // (arg[1] is the environment; instructions are arg[2], seed entries arg[3].)
      expect(svc.executeInstructedBuild.firstCall.args[2]).toEqual(
         instructions,
      );
      expect(svc.executeInstructedBuild.firstCall.args[3]).toEqual({});
      // The manifest is committed with both collections: entries (arg 1) and
      // failures (arg 2), metadata last. Pinned positionally because these are
      // sinon args -- asserting the metadata alone would keep passing if a later
      // signature change fed the wrong collection into the persisted manifest.
      expect(svc.commitManifest.firstCall.args[2]).toEqual({});
      expect(svc.commitManifest.firstCall.args[3]).toMatchObject({
         mode: "orchestrated",
         sourcesBuilt: 1,
         sourcesReused: 0,
      });
      // Orchestrated leaves distribution to the caller.
      expect(svc.autoLoadManifest.called).toBe(false);
   });

   it("orchestrated: seeds the build from referenceManifest and honors strictUpstreams", async () => {
      const svc = stubEngine();
      const instructions = [makeInstruction()];

      await svc.runBuild(
         "mat-1",
         "my-env",
         "pkg",
         {
            sourceNames: undefined,
            forceRefresh: false,
            buildInstructions: instructions,
            referenceManifest: [
               {
                  sourceEntityId: "up-1",
                  physicalTableName: "upstream_tbl",
                  connectionName: "sf",
               },
            ],
            strictUpstreams: true,
         },
         new AbortController().signal,
      );

      // The reference manifest becomes the seed (carried) entries so a
      // downstream build resolves its upstream to the existing physical table.
      // arg[1] is environment (the storage build session), so the seed
      // (carried) entries are arg[3]. connectionName is carried through so the
      // seed can be quoted for the upstream's dialect (see quoteSeedTablePath).
      expect(svc.executeInstructedBuild.firstCall.args[3]).toEqual({
         "up-1": {
            sourceEntityId: "up-1",
            physicalTableName: "upstream_tbl",
            connectionName: "sf",
         },
      });
      // strictUpstreams flows through as the strict flag (6th arg).
      expect(svc.executeInstructedBuild.firstCall.args[5]).toBe(true);
      // Reused upstreams are counted as carried, not built.
      expect(svc.commitManifest.firstCall.args[3]).toMatchObject({
         mode: "orchestrated",
         sourcesBuilt: 1,
         sourcesReused: 1,
      });
   });

   it("auto-run: derives instructions and auto-loads the manifest", async () => {
      const svc = stubEngine();

      await svc.runBuild(
         "mat-1",
         "my-env",
         "pkg",
         {
            sourceNames: undefined,
            forceRefresh: true,
            buildInstructions: undefined,
         },
         new AbortController().signal,
      );

      expect(svc.commitManifest.firstCall.args[3]).toMatchObject({
         mode: "auto",
      });
      // Auto-run owns distribution: it loads the fresh manifest into the models.
      expect(svc.autoLoadManifest.calledOnce).toBe(true);
   });

   // What the incremental context is told to do comes from `reseed` ALONE.
   // forceRefresh is a different question — whether skip-if-unchanged is
   // defeated — and an incremental source is exempt from that anyway, so it has
   // nothing to say about how one is built.
   async function contextArgsFor(
      opts: Partial<{
         forceRefresh: boolean;
         reseed: boolean;
         trigger: "ON_DEMAND" | "SCHEDULER";
         buildInstructions: unknown[];
         ledger: unknown[];
      }>,
   ): Promise<{ forceRefresh: boolean; ledger?: unknown[] }> {
      const svc = stubEngine();
      const contextSpy = sinon.stub().returns(undefined);
      (
         ctx.service as unknown as { incrementalRunContext: sinon.SinonStub }
      ).incrementalRunContext = contextSpy;

      await svc.runBuild(
         "mat-1",
         "my-env",
         "pkg",
         {
            sourceNames: undefined,
            forceRefresh: false,
            reseed: false,
            buildInstructions: undefined,
            trigger: "ON_DEMAND",
            ...opts,
         } as unknown as Parameters<typeof svc.runBuild>[3],
         new AbortController().signal,
      );

      expect(contextSpy.calledOnce).toBe(true);
      return contextSpy.firstCall.args[1] as {
         forceRefresh: boolean;
         ledger?: unknown[];
      };
   }

   it("hands the run the caller's ledger, or none, verbatim", async () => {
      // Absent means "read the local store", for every caller that predates the
      // field and any mid-upgrade fleet — which is what makes it additive.
      expect((await contextArgsFor({})).ledger).toBeUndefined();
      expect(
         (await contextArgsFor({ buildInstructions: [{}] })).ledger,
      ).toBeUndefined();
      // Present — even empty — means the caller owns it for this run.
      expect(
         (await contextArgsFor({ buildInstructions: [{}], ledger: [] })).ledger,
      ).toEqual([]);
   });

   it("NEVER reads a force as a re-seed, whoever asked", async () => {
      // Including an ON_DEMAND API caller, which is the case that used to
      // re-seed. A force says "do not reuse an unchanged table"; for an
      // incremental source, which is never reused, that is not a request at all.
      for (const trigger of ["ON_DEMAND", "SCHEDULER"] as const) {
         expect(
            await contextArgsFor({ forceRefresh: true, trigger }),
         ).toMatchObject({ forceRefresh: false });
      }
      // And the same for an orchestrated run, where no carry-forward even runs.
      expect(
         await contextArgsFor({
            forceRefresh: true,
            buildInstructions: [{}],
         }),
      ).toMatchObject({ forceRefresh: false });
   });

   it("reads `reseed` as the re-seed, whatever the force says", async () => {
      expect(await contextArgsFor({ reseed: true })).toMatchObject({
         forceRefresh: true,
      });
      // Not conditioned on the trigger: a scheduled run that explicitly asks to
      // rebuild gets a rebuild.
      expect(
         await contextArgsFor({ reseed: true, trigger: "SCHEDULER" }),
      ).toMatchObject({ forceRefresh: true });
      expect(
         await contextArgsFor({ reseed: false, forceRefresh: true }),
      ).toMatchObject({ forceRefresh: false });
   });
});

describe("autoLoadManifest", () => {
   let ctx: ReturnType<typeof createMocks>;
   beforeEach(() => {
      ctx = createMocks();
   });

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

      await (
         ctx.service as unknown as {
            autoLoadManifest: (
               env: { reloadAllModelsForPackage: sinon.SinonStub },
               pkg: string,
               entries: Record<string, unknown>,
            ) => Promise<void>;
         }
      ).autoLoadManifest({ reloadAllModelsForPackage }, "pkg", entries);

      expect(reloadAllModelsForPackage.calledOnce).toBe(true);
      // connectionName is carried alongside tableName so the bind step
      // (Package.quoteBoundTableNames) can quote the path for its dialect.
      expect(reloadAllModelsForPackage.firstCall.args).toEqual([
         "pkg",
         { b1: { tableName: "orders_v1", connectionName: "duckdb" } },
      ]);
   });

   it("swallows a reload failure (best-effort; run already succeeded)", async () => {
      const reloadAllModelsForPackage = sinon
         .stub()
         .rejects(new Error("reload boom"));
      await expect(
         (
            ctx.service as unknown as {
               autoLoadManifest: (
                  env: { reloadAllModelsForPackage: sinon.SinonStub },
                  pkg: string,
                  entries: Record<string, unknown>,
               ) => Promise<void>;
            }
         ).autoLoadManifest({ reloadAllModelsForPackage }, "pkg", {
            b1: { sourceEntityId: "b1", physicalTableName: "t" },
         }),
      ).resolves.toBeUndefined();
   });
});

describe("runInBackground (terminal recording)", () => {
   let ctx: ReturnType<typeof createMocks>;
   beforeEach(() => {
      ctx = createMocks();
   });

   const flush = () => new Promise((r) => setTimeout(r, 20));

   function background(): {
      runInBackground: (
         id: string,
         run: (signal: AbortSignal) => Promise<void>,
      ) => void;
      runningAbortControllers: Map<string, AbortController>;
   } {
      return ctx.service as unknown as {
         runInBackground: (
            id: string,
            run: (signal: AbortSignal) => Promise<void>,
         ) => void;
         runningAbortControllers: Map<string, AbortController>;
      };
   }

   it("records FAILED with the error message when the run rejects", async () => {
      background().runInBackground("bg-1", async () => {
         throw new Error("boom");
      });
      await flush();

      expect(ctx.repository.updateMaterialization.calledOnce).toBe(true);
      expect(ctx.repository.updateMaterialization.firstCall.args[0]).toBe(
         "bg-1",
      );
      expect(
         ctx.repository.updateMaterialization.firstCall.args[1],
      ).toMatchObject({ status: "FAILED", error: "boom" });
   });

   it("records CANCELLED when the run aborts cooperatively", async () => {
      const bg = background();
      bg.runInBackground("bg-2", async () => {
         bg.runningAbortControllers.get("bg-2")!.abort();
         throw new Error("boom");
      });
      await flush();

      expect(
         ctx.repository.updateMaterialization.firstCall.args[1],
      ).toMatchObject({ status: "CANCELLED", error: "Cancelled" });
   });

   it("clears the abort controller once the run settles", async () => {
      const bg = background();
      bg.runInBackground("bg-3", async () => {});
      await flush();
      expect(bg.runningAbortControllers.has("bg-3")).toBe(false);
   });

   it("logs the failure with its stack when the run rejects", async () => {
      const errorStub = sinon.stub(logger, "error");
      try {
         background().runInBackground("bg-4", async () => {
            throw new Error("boom");
         });
         await flush();

         expect(errorStub.calledOnce).toBe(true);
         const [message, meta] = errorStub.firstCall.args as unknown as [
            string,
            { materializationId: string; error: string; stack?: string },
         ];
         expect(message).toBe("Materialization run failed");
         expect(meta.materializationId).toBe("bg-4");
         expect(meta.error).toBe("boom");
         // The stack is the half that locates the throw; a message alone cannot.
         expect(meta.stack).toContain("Error: boom");
      } finally {
         errorStub.restore();
      }
   });

   it("logs a cancellation as info, not as a failure", async () => {
      const errorStub = sinon.stub(logger, "error");
      const infoStub = sinon.stub(logger, "info");
      try {
         const bg = background();
         bg.runInBackground("bg-5", async () => {
            bg.runningAbortControllers.get("bg-5")!.abort();
            throw new Error("boom");
         });
         await flush();

         expect(errorStub.called).toBe(false);
         expect(infoStub.calledOnce).toBe(true);
         const [message, meta] = infoStub.firstCall.args as unknown as [
            string,
            { materializationId: string; error: string },
         ];
         expect(message).toBe("Materialization run cancelled");
         // An abort that races a genuine failure records "Cancelled", so this is
         // the only place the real error is reported.
         expect(meta.error).toBe("boom");
      } finally {
         errorStub.restore();
         infoStub.restore();
      }
   });

   it("redacts a DSN from the run-level message it logs and records", async () => {
      // A whole-run throw has no connection to redact against, but a connect
      // failure while resolving the package's connections echoes the DSN.
      const errorStub = sinon.stub(logger, "error");
      try {
         background().runInBackground("bg-6", async () => {
            throw new Error(
               "connect failed: postgres://admin:hunter2@db.internal:5432/analytics",
            );
         });
         await flush();

         const [, meta] = errorStub.firstCall.args as unknown as [
            string,
            { error: string },
         ];
         expect(meta.error).not.toContain("hunter2");
         expect(meta.error).toContain("connect failed");

         const recorded = ctx.repository.updateMaterialization.firstCall
            .args[1] as { error?: string };
         expect(
            recorded.error,
            "the persisted record must not carry it either",
         ).not.toContain("hunter2");
      } finally {
         errorStub.restore();
      }
   });
});

describe("transition (state machine)", () => {
   let ctx: ReturnType<typeof createMocks>;
   beforeEach(() => {
      ctx = createMocks();
   });

   function transition(): (
      id: string,
      next: MaterializationStatus,
   ) => Promise<unknown> {
      return (
         ctx.service as unknown as {
            transition: (
               id: string,
               next: MaterializationStatus,
            ) => Promise<unknown>;
         }
      ).transition.bind(ctx.service);
   }

   it("throws MaterializationNotFoundError when the record is gone", async () => {
      ctx.repository.getMaterializationById.resolves(undefined);
      await expect(
         transition()("mat-1", "MANIFEST_ROWS_READY"),
      ).rejects.toThrow(MaterializationNotFoundError);
   });

   it("rejects a transition the state machine disallows", async () => {
      ctx.repository.getMaterializationById.resolves(
         makeMaterialization({ status: "MANIFEST_FILE_READY" }),
      );
      // MANIFEST_FILE_READY is terminal; nothing follows it.
      await expect(transition()("mat-1", "PENDING")).rejects.toThrow(
         InvalidStateTransitionError,
      );
      expect(ctx.repository.updateMaterialization.called).toBe(false);
   });

   it("persists a valid transition", async () => {
      ctx.repository.getMaterializationById.resolves(
         makeMaterialization({ status: "PENDING" }),
      );
      ctx.repository.updateMaterialization.resolves(
         makeMaterialization({ status: "MANIFEST_ROWS_READY" }),
      );
      await transition()("mat-1", "MANIFEST_ROWS_READY");
      expect(
         ctx.repository.updateMaterialization.calledOnceWith("mat-1", {
            status: "MANIFEST_ROWS_READY",
         }),
      ).toBe(true);
   });
});

// The only path here that issues a DROP against a customer's warehouse, so its
// guards get assertions rather than a trace. Scenario 64 covers it end to end,
// but hammer does not run in CI — and the failure mode is silent data loss, not
// a broken build.
describe("isReclaimableStorageTable", () => {
   type Entry = Parameters<typeof isReclaimableStorageTable>[0];
   const entry = (over: Partial<Entry>) =>
      ({
         sourceEntityId: "eid",
         physicalTableName: "daily_v1",
         ...over,
      }) as Entry;

   it("reclaims a stored table from a source that is not refreshed incrementally", () => {
      // A fresh generational name nothing else knows about: exactly what the
      // reclaim exists for.
      expect(
         isReclaimableStorageTable(entry({ storageDestinationName: "lake" })),
      ).toBe(true);
   });

   it("never reclaims a table an incremental source owns, whatever this run did", () => {
      // Its name is stable across runs by contract, so it is not a fresh
      // generation — a prior manifest may bind it, and a refresh writes it in
      // place either way: a delta as DML, a re-seed as CREATE OR REPLACE. A later
      // source failing the run must not take the source off its stored table, and
      // `full` is as exposed as `delta` here.
      for (const refresh of ["delta", "none", "full"] as const) {
         expect(
            isReclaimableStorageTable(
               entry({ storageDestinationName: "lake", refresh }),
            ),
         ).toBe(false);
      }
   });

   it("ignores a colocated entry, which this sweep does not own", () => {
      expect(isReclaimableStorageTable(entry({}))).toBe(false);
      expect(isReclaimableStorageTable(entry({ refresh: "delta" }))).toBe(
         false,
      );
   });
});

describe("reclaimStorageTablesFromFailedRun", () => {
   let ctx: ReturnType<typeof createMocks>;
   let infoLog: sinon.SinonStub;
   let warnLog: sinon.SinonStub;

   beforeEach(() => {
      ctx = createMocks();
      infoLog = ctx.sandbox.stub(logger, "info");
      warnLog = ctx.sandbox.stub(logger, "warn");
   });
   afterEach(() => ctx.sandbox.restore());

   const entry = (table: string) =>
      ({
         sourceEntityId: `eid-${table}`,
         sourceName: "daily",
         physicalTableName: table,
         storageDestinationName: "lake",
      }) as unknown as Parameters<
         MaterializationService["reclaimStorageTablesFromFailedRun"]
      >[0][number];

   /** A destination whose drop would fail loudly if one were ever attempted. */
   const environment = buildEnvironmentStub({
      destinations: { lake: { name: "lake", type: "duckdb" } },
      environmentPath: "/test",
   }) as unknown as Parameters<
      MaterializationService["reclaimStorageTablesFromFailedRun"]
   >[1];

   const reclaim = (
      built: ReturnType<typeof entry>[],
      others: unknown[],
   ): Promise<void> => {
      (ctx.repository.listMaterializations as sinon.SinonStub).resolves(others);
      return (
         ctx.service as unknown as {
            reclaimStorageTablesFromFailedRun: (
               b: unknown,
               e: unknown,
               o: unknown,
            ) => Promise<void>;
         }
      ).reclaimStorageTablesFromFailedRun(built, environment, {
         environmentId: "env-1",
         packageName: "pkg",
      });
   };

   const said = (stub: sinon.SinonStub, needle: RegExp) =>
      stub.getCalls().some((c) => needle.test(String(c.args[0])));

   it("never drops a table a live manifest still serves", async () => {
      // The carried-forward case: a previous successful run's manifest names the
      // same destination + table. Dropping it would delete a table that is being
      // served right now.
      await reclaim(
         [entry("daily_v1")],
         [
            {
               status: "MANIFEST_FILE_READY",
               manifest: {
                  entries: {
                     other: {
                        storageDestinationName: "lake",
                        physicalTableName: "daily_v1",
                     },
                  },
               },
            },
         ],
      );

      expect(said(infoLog, /Keeping a table from a failed run/)).toBe(true);
      expect(said(infoLog, /Reclaimed a table/)).toBe(false);
      expect(said(warnLog, /Failed to reclaim/)).toBe(false);
   });

   it("does attempt the drop when nothing references the table", async () => {
      // The inverse, so the test above is known to be the still-referenced check
      // doing the work rather than the reclaim being inert. No real destination
      // exists here, so the attempt surfaces as the drop-failure warning.
      await reclaim([entry("daily_v2")], []);

      expect(said(infoLog, /Keeping a table from a failed run/)).toBe(false);
      expect(
         said(warnLog, /Failed to reclaim/) ||
            said(infoLog, /Reclaimed a table/),
      ).toBe(true);
   });

   it("skips a table whose destination is no longer configured and keeps sweeping", async () => {
      // A destination can be un-registered between the build and the reclaim.
      // Aborting the sweep there would leave every table after it stranded too,
      // so the entry is skipped and the rest are still attempted.
      const withoutDestinations = buildEnvironmentStub({
         destinations: {},
         environmentPath: "/test",
      }) as unknown as Parameters<
         MaterializationService["reclaimStorageTablesFromFailedRun"]
      >[1];
      (ctx.repository.listMaterializations as sinon.SinonStub).resolves([]);

      await (
         ctx.service as unknown as {
            reclaimStorageTablesFromFailedRun: (
               b: unknown,
               e: unknown,
               o: unknown,
            ) => Promise<void>;
         }
      ).reclaimStorageTablesFromFailedRun(
         [entry("daily_v4"), entry("daily_v5")],
         withoutDestinations,
         { environmentId: "env-1", packageName: "pkg" },
      );

      expect(said(warnLog, /destination is no longer configured/)).toBe(true);
      // Both entries were considered, not just the one that tripped the skip.
      expect(
         warnLog
            .getCalls()
            .filter((c) => /no longer configured/.test(String(c.args[0]))),
      ).toHaveLength(2);
      // And the sweep did not report itself as having blown up.
      expect(said(warnLog, /did not complete/)).toBe(false);
   });

   it("ignores a superseded run's entries when deciding", async () => {
      // Only MANIFEST_FILE_READY runs count as live. A superseded run naming the
      // same table must not pin it forever.
      await reclaim(
         [entry("daily_v3")],
         [
            {
               status: "SUPERSEDED",
               manifest: {
                  entries: {
                     old: {
                        storageDestinationName: "lake",
                        physicalTableName: "daily_v3",
                     },
                  },
               },
            },
         ],
      );

      expect(said(infoLog, /Keeping a table from a failed run/)).toBe(false);
   });
});

// ── Incremental dispatch inside buildOneSource ────────────────────────────
//
// The planner's decisions are unit-tested against fakes in
// incremental_apply.spec.ts, and the DML's semantics are executed in
// incremental_dml_semantics.spec.ts. What is left, and what these cover, is the
// WIRING: that a delta replaces the CTAS rather than joining it, that a seed is
// still byte-for-byte the old build, and that the boundary is written and cleared
// at the right moments.
describe("buildOneSource: incremental refresh", () => {
   let ctx: ReturnType<typeof createMocks>;
   beforeEach(() => {
      ctx = createMocks();
   });

   const DECLARATION = {
      refresh: "incremental",
      incremental: true,
      declaredWatermark: true,
      declaredMergeKey: false,
      watermark: {
         name: "order_date",
         kind: "dimension" as const,
         malloyType: "date",
      },
      watermarkOrderable: true,
      mergeKeys: [],
      watermarkInMergeKeys: false,
      strategy: "range_replace" as const,
      malformed: [],
      unknownKeys: [],
      calculateFields: [],
      outputColumns: ["order_date", "revenue"],
   };

   const LEDGER_ROW = {
      environmentId: "env-1",
      packageName: "pkg",
      sourceEntityId: "abcdef1234567890",
      coveredThroughValue: "2024-06-01",
      coveredThroughType: "date",
      watermarkDimension: "order_date",
      mergeKeyDimensions: [] as string[],
      derivedStrategy: "range_replace" as const,
      physicalTableName: "orders_v1",
      connectionName: "wh",
      advancedByMaterializationId: "run-0",
      advancedAt: new Date("2024-06-01T00:00:00Z"),
      createdAt: new Date("2024-05-01T00:00:00Z"),
   };

   /** A runSQL stub that answers the probes and records every statement. */
   function probeAwareRunSQL(nonEmpty = true): sinon.SinonStub {
      const stub = sinon.stub();
      stub.callsFake(async (sql: string) => {
         if (sql.includes("information_schema")) {
            return {
               rows: [
                  { column_name: "order_date" },
                  { column_name: "revenue" },
               ],
            };
         }
         if (sql.includes("LIMIT 1")) {
            return { rows: nonEmpty ? [{ present: 1 }] : [] };
         }
         if (sql.includes("watermark_max")) {
            return { rows: [{ watermark_max: "2024-06-20" }] };
         }
         return { rows: [] };
      });
      return stub;
   }

   function incrementalContext(overrides: {
      ledgerEntry?: typeof LEDGER_ROW | null;
      forceRefresh?: boolean;
      /**
       * The caller-supplied ledger. Present (even `[]`) puts the run in
       * caller-ledger mode, replacing the local store; absent reads the store.
       */
      ledger?: LedgerEntry[];
      now?: Date;
   }) {
      const upsert = sinon.stub().resolves();
      const remove = sinon.stub().resolves();
      const read = sinon
         .stub()
         .resolves(
            overrides.ledgerEntry === undefined
               ? LEDGER_ROW
               : overrides.ledgerEntry,
         );
      const context = {
         environmentId: "env-1",
         packageName: "pkg",
         materializationId: "run-1",
         forceRefresh: overrides.forceRefresh ?? false,
         now: overrides.now ?? new Date("2024-07-01T00:00:00Z"),
         declarations: { orders: DECLARATION },
         ...(overrides.ledger !== undefined
            ? { callerLedger: indexCallerLedger(overrides.ledger) }
            : {}),
         ledger: {
            getIncrementalLedgerEntry: read,
            upsertIncrementalLedgerEntry: upsert,
            deleteIncrementalLedgerEntry: remove,
         },
      };
      return { context, upsert, remove, read };
   }

   function callBuildOneSource(params: {
      runSQL: sinon.SinonStub;
      context?: unknown;
      annotationFields?: Record<string, string>;
      manifest?: Manifest;
      dialectName?: string;
      reseed?: boolean;
   }): Promise<{
      sourceEntityId: string;
      physicalTableName: string;
      refresh?: string;
      ledger?: LedgerEntry;
   }> {
      const source = fakeSource({
         name: "orders",
         sourceEntityId: "abcdef1234567890",
         sql: "SELECT * FROM t",
         connectionName: "wh",
         dialectName: params.dialectName ?? "postgres",
         annotationFields: params.annotationFields,
         // What the delta's DML names, and what the seed's CTAS would have given
         // the table its columns from.
         columns: ["order_date", "revenue"],
      });
      const instruction = {
         sourceEntityId: "abcdef1234567890",
         materializedTableId: "mt-1",
         physicalTableName: "orders_v1",
         realization: "COPY",
         ...(params.reseed === undefined ? {} : { reseed: params.reseed }),
      } as BuildInstruction;
      return (
         ctx.service as unknown as {
            buildOneSource: (...args: unknown[]) => Promise<{
               sourceEntityId: string;
               physicalTableName: string;
               refresh?: string;
               ledger?: LedgerEntry;
            }>;
         }
      ).buildOneSource(
         source,
         instruction,
         { runSQL: params.runSQL },
         { wh: "dig" },
         params.manifest ?? new Manifest(),
         {
            getApiConnection: () => {
               throw new Error("no connection config in this test");
            },
            getEnvironmentPath: () => "/tmp/env",
         },
         {},
         undefined,
         params.context,
         // The content address, which is the ledger's key.
         "abcdef1234567890",
      );
   }

   it("applies the delta INSTEAD of the CTAS, and advances the boundary", async () => {
      const runSQL = probeAwareRunSQL();
      const { context, upsert } = incrementalContext({});
      const entry = await callBuildOneSource({ runSQL, context });

      const statements = runSQL.getCalls().map((c) => c.args[0] as string);
      // The delta went out as ONE call: the DELETE and the INSERT have to commit
      // together.
      const script = statements.find((s) => s.startsWith("BEGIN;"));
      expect(script).toBeDefined();
      expect(script).toContain(`DELETE FROM "orders_v1" WHERE "order_date" >=`);
      expect(script).toContain(
         `INSERT INTO "orders_v1" ("order_date", "revenue")`,
      );
      // The INSERT reads the SEED's own SQL, filtered. Not a compiled query's
      // SQL, which on Postgres would arrive wrapped in row_to_json and make this
      // INSERT fail on the first column it names — see deltaSelect.
      expect(script).toContain(`FROM (SELECT * FROM (SELECT * FROM t) AS __d`);
      expect(script).toContain("COMMIT;");
      // And nothing rebuilt the table.
      expect(statements.some((s) => s.startsWith("CREATE TABLE"))).toBe(false);
      expect(statements.some((s) => s.includes("RENAME TO"))).toBe(false);

      expect(upsert.calledOnce).toBe(true);
      expect(upsert.firstCall.args[0]).toMatchObject({
         sourceEntityId: "abcdef1234567890",
         // The run's snapshot, for a date watermark.
         coveredThroughValue: "2024-07-01",
         coveredThroughType: "date",
         watermarkDimension: "order_date",
         derivedStrategy: "range_replace",
         physicalTableName: "orders_v1",
         advancedByMaterializationId: "run-1",
      });
      // The entry still names the same table: a delta advances in place.
      expect(entry.physicalTableName).toBe("orders_v1");
      // And it reports the coverage it reached, so an orchestrating caller can
      // verify the delta advanced instead of inferring it from the run's
      // outcome. The boundary is reported ONLY here, alongside the watermark and
      // address it was measured under, which is what makes it comparable.
      expect(entry.ledger).toEqual({
         connectionName: "wh",
         physicalTableName: "orders_v1",
         coveredThrough: "2024-07-01",
         coveredThroughType: "date",
         watermark: "order_date",
         strategy: "range_replace",
         sourceEntityId: "abcdef1234567890",
      });
   });

   it("re-seeds the ONE source whose instruction says so", async () => {
      // Per-source, so a host can rebuild one while the rest advance by delta.
      // forceRefresh is never how this is asked for, in any mode.
      const runSQL = probeAwareRunSQL();
      const { context, upsert } = incrementalContext({});
      const entry = await callBuildOneSource({ runSQL, context, reseed: true });

      const statements = runSQL.getCalls().map((c) => c.args[0] as string);
      expect(statements.some((s) => s.startsWith("CREATE TABLE"))).toBe(true);
      expect(statements.some((s) => s.startsWith("BEGIN;"))).toBe(false);
      // A rebuild still leaves a boundary behind — probed from the table it just
      // wrote — so the NEXT refresh is a delta again rather than another rebuild.
      expect(upsert.calledOnce).toBe(true);
      expect(entry.ledger?.coveredThrough).toBe("2024-07-01");
   });

   it("reports the boundary a SEED reached on the manifest entry", async () => {
      // Same field as the delta case, so a caller reads coverage one way whatever
      // the step did.
      const runSQL = probeAwareRunSQL();
      const { context } = incrementalContext({ ledgerEntry: null });
      const entry = await callBuildOneSource({ runSQL, context });

      const statements = runSQL.getCalls().map((c) => c.args[0] as string);
      expect(statements.some((s) => s.startsWith("CREATE TABLE"))).toBe(true);
      expect(entry.ledger?.coveredThrough).toBe("2024-07-01");
      expect(entry.ledger?.coveredThroughType).toBe("date");
   });

   it("carries the Snowflake delta as ONE scripting block, not a script", async () => {
      // Snowflake's driver executes exactly one statement per call, each on a
      // possibly different pooled session — a BEGIN;…;COMMIT; script is refused
      // and a statement-per-call loop would autocommit the DELETE alone. The
      // whole transaction has to arrive inside a single Snowflake Scripting
      // statement.
      const runSQL = probeAwareRunSQL();
      const { context, upsert } = incrementalContext({});
      await callBuildOneSource({ runSQL, context, dialectName: "snowflake" });

      const statements = runSQL.getCalls().map((c) => c.args[0] as string);
      const block = statements.find((s) =>
         s.startsWith("EXECUTE IMMEDIATE $$"),
      );
      expect(block).toBeDefined();
      expect(block).toContain(`DELETE FROM "orders_v1" WHERE "order_date" >=`);
      expect(block).toContain(
         `INSERT INTO "orders_v1" ("order_date", "revenue")`,
      );
      expect(statements.some((s) => s.startsWith("BEGIN"))).toBe(false);
      expect(statements.some((s) => s.startsWith("CREATE TABLE"))).toBe(false);
      expect(upsert.calledOnce).toBe(true);
   });

   it("skips when nothing advanced: no DML, no ledger write, entry still emitted", async () => {
      // The boundary already sits at the run's snapshot, so there is no range to
      // compute. The table keeps serving as-is — but the manifest entry must
      // still appear, or a downstream source built later in this run would
      // recompute this upstream from raw.
      const runSQL = probeAwareRunSQL();
      const { context, upsert, remove } = incrementalContext({
         ledgerEntry: { ...LEDGER_ROW, coveredThroughValue: "2024-07-01" },
      });
      const manifest = new Manifest();
      const entry = await callBuildOneSource({ runSQL, context, manifest });

      const statements = runSQL.getCalls().map((c) => c.args[0] as string);
      expect(statements.some((s) => s.startsWith("BEGIN;"))).toBe(false);
      expect(statements.some((s) => s.startsWith("CREATE TABLE"))).toBe(false);
      expect(upsert.called).toBe(false);
      expect(remove.called).toBe(false);
      expect(entry.physicalTableName).toBe("orders_v1");
      // Downstream resolution: the QUOTED serving table is in the build manifest.
      expect(
         manifest.buildManifest.entries["abcdef1234567890"]?.tableName,
      ).toBe('"orders_v1"');
      // A skip reports the boundary that STAYS in force, so a caller does not
      // read a gap where coverage is simply unchanged.
      expect(entry.ledger?.coveredThrough).toBe("2024-07-01");
   });

   it("fails the build and leaves the boundary alone when the delta's DML fails", async () => {
      // The crash-safety property: an unadvanced boundary makes the next run
      // recompute the same (idempotent) range. Advancing it past a failed write
      // would skip those rows forever.
      const runSQL = probeAwareRunSQL();
      runSQL
         .withArgs(sinon.match((sql: string) => sql.startsWith("BEGIN;")))
         .rejects(new Error("deadlock detected"));
      const { context, upsert } = incrementalContext({});

      await expect(callBuildOneSource({ runSQL, context })).rejects.toThrow(
         "deadlock detected",
      );
      expect(upsert.called).toBe(false);
      // The session was rolled back so a pooled connection is not returned
      // inside an aborted transaction.
      const statements = runSQL.getCalls().map((c) => c.args[0] as string);
      expect(statements).toContain("ROLLBACK");
   });

   it("rebuilds and re-records the boundary when none is recorded", async () => {
      const runSQL = probeAwareRunSQL();
      const { context, upsert, remove } = incrementalContext({
         ledgerEntry: null,
      });
      await callBuildOneSource({ runSQL, context });

      const statements = runSQL.getCalls().map((c) => c.args[0] as string);
      // The full build, unchanged.
      expect(statements).toContain(
         'CREATE TABLE "orders_v1_abcdef123456" AS (SELECT * FROM t)',
      );
      expect(statements).toContain(
         'ALTER TABLE "orders_v1_abcdef123456" RENAME TO "orders_v1"',
      );
      // Cleared before the rebuild starts, then set from the table it wrote.
      expect(remove.calledOnce).toBe(true);
      expect(upsert.calledOnce).toBe(true);
      expect(upsert.firstCall.args[0]).toMatchObject({
         coveredThroughValue: "2024-07-01",
      });
   });

   it("routes forceRefresh to a full rebuild and resets the boundary", async () => {
      const runSQL = probeAwareRunSQL();
      const { context, remove } = incrementalContext({ forceRefresh: true });
      await callBuildOneSource({ runSQL, context });

      const statements = runSQL.getCalls().map((c) => c.args[0] as string);
      expect(statements.some((s) => s.startsWith("CREATE TABLE"))).toBe(true);
      expect(statements.some((s) => s.startsWith("BEGIN;"))).toBe(false);
      expect(remove.calledOnce).toBe(true);
   });

   it("records nothing when the rebuilt table has no watermark values", async () => {
      // An empty seed sets no boundary on purpose: one taken from an empty table
      // would exclude every historical row that arrives later.
      const runSQL = sinon.stub().callsFake(async (sql: string) => {
         if (sql.includes("watermark_max")) {
            return { rows: [{ watermark_max: null }] };
         }
         return { rows: [] };
      });
      const { context, upsert } = incrementalContext({ ledgerEntry: null });
      await callBuildOneSource({ runSQL, context });
      expect(upsert.called).toBe(false);
   });

   it("leaves a non-incremental source's build exactly as it was", async () => {
      // The context is present (another source in the package declared it), so
      // this proves the branch is keyed on the SOURCE, not on the run.
      const runSQL = sinon.stub().resolves({ rows: [] });
      const { context, upsert, remove } = incrementalContext({});
      context.declarations = {} as typeof context.declarations;
      await callBuildOneSource({ runSQL, context });

      const statements = runSQL.getCalls().map((c) => c.args[0] as string);
      expect(statements).toEqual([
         'DROP TABLE IF EXISTS "orders_v1_abcdef123456"',
         'CREATE TABLE "orders_v1_abcdef123456" AS (SELECT * FROM t)',
         'DROP TABLE IF EXISTS "orders_v1"',
         'ALTER TABLE "orders_v1_abcdef123456" RENAME TO "orders_v1"',
      ]);
      expect(upsert.called).toBe(false);
      expect(remove.called).toBe(false);
   });

   it("leaves the build alone on a dialect the delta path does not support", async () => {
      // Same declaration, DuckDB instead of Postgres: no probe, no delta, no
      // ledger write — the gate rejects this at publish, and a package that
      // predates the gate simply builds full.
      const runSQL = sinon.stub().resolves({ rows: [] });
      const { context, upsert } = incrementalContext({});
      const source = fakeSource({
         name: "orders",
         sourceEntityId: "abcdef1234567890",
         sql: "SELECT * FROM t",
         dialectName: "duckdb",
      });
      await (
         ctx.service as unknown as {
            buildOneSource: (...args: unknown[]) => Promise<unknown>;
         }
      ).buildOneSource(
         source,
         {
            sourceEntityId: "abcdef1234567890",
            materializedTableId: "mt-1",
            physicalTableName: "orders_v1",
            realization: "COPY",
         } as BuildInstruction,
         { runSQL },
         { duckdb: "dig" },
         new Manifest(),
         {
            getApiConnection: () => {
               throw new Error("no connection config in this test");
            },
            getEnvironmentPath: () => "/tmp/env",
         },
         {},
         undefined,
         context,
         "abcdef1234567890",
      );
      expect(runSQL.getCalls().map((c) => c.args[0])).toHaveLength(4);
      expect(upsert.called).toBe(false);
   });

   it("reports what an incremental refresh DID: delta, full or none", async () => {
      // Otherwise indistinguishable from the entry, and a caller cannot infer it
      // from what it instructed: every fallback answers success and rebuilds.
      const delta = await callBuildOneSource({
         runSQL: probeAwareRunSQL(),
         context: incrementalContext({}).context,
      });
      expect(delta.refresh).toBe("delta");

      const rebuilt = await callBuildOneSource({
         runSQL: probeAwareRunSQL(),
         context: incrementalContext({ ledgerEntry: null }).context,
      });
      expect(rebuilt.refresh).toBe("full");

      // A skip applied nothing and SAYS so, rather than leaving it to an absent
      // field: its unchanged boundary already says the table stands where it did.
      const skipped = await callBuildOneSource({
         runSQL: probeAwareRunSQL(),
         context: incrementalContext({
            ledgerEntry: { ...LEDGER_ROW, coveredThroughValue: "2024-07-01" },
         }).context,
      });
      expect(skipped.refresh).toBe("none");

      // Which leaves absence with exactly one meaning: this source is not
      // refreshed incrementally at all.
      const { context } = incrementalContext({});
      context.declarations = {} as typeof context.declarations;
      const ordinary = await callBuildOneSource({
         runSQL: probeAwareRunSQL(),
         context,
      });
      expect(ordinary.refresh).toBeUndefined();
   });

   // ── caller-supplied ledger ──────────────────────────────────────────────
   //
   // The same dispatch, reading the boundary off `buildInstructions.ledger`
   // instead of the local store. What these pin: the boundary reaches the same
   // planner through the same checks, the local store is neither read nor
   // written on any path, a missing entry seeds exactly like a missing row —
   // and, unlike a stored row, an entry that no longer describes its source
   // FAILS the run, per the API contract that an invalid input is an error.
   describe("with a caller-supplied ledger", () => {
      /** The publisher's own last report (`ManifestEntry.ledger`), echoed back. */
      const ENTRY: LedgerEntry = {
         connectionName: "wh",
         physicalTableName: "orders_v1",
         coveredThrough: "2024-06-20",
         coveredThroughType: "date",
         watermark: "order_date",
         strategy: "range_replace",
         sourceEntityId: "abcdef1234567890",
      };

      it("advances from the caller's entry while a DIFFERENT local row sits unread", async () => {
         // The local row says 2024-06-01 and is deliberately wrong: this is the
         // whole point of the mode, since the row a given worker holds may be a
         // generation stale or absent entirely.
         const runSQL = probeAwareRunSQL();
         const { context, upsert, remove, read } = incrementalContext({
            ledger: [ENTRY],
         });
         const entry = await callBuildOneSource({ runSQL, context });

         const script = runSQL
            .getCalls()
            .map((c) => c.args[0] as string)
            .find((s) => s.startsWith("BEGIN;"));
         expect(script).toContain(`>= DATE '2024-06-20'`);
         expect(script).not.toContain("2024-06-01");
         expect(read.called).toBe(false);
         // Not written either, on either path: a row left behind here is a stale
         // landmine for a later run in the other mode.
         expect(upsert.called).toBe(false);
         expect(remove.called).toBe(false);
         // The advance travels back on the entry's `ledger`, which is the
         // caller's own next input — verbatim.
         expect(entry.refresh).toBe("delta");
         expect(entry.ledger).toEqual({
            ...ENTRY,
            coveredThrough: "2024-07-01",
         });
      });

      it("round-trips: store run 1's `ledger`, return it, run 2 deltas from it", async () => {
         const first = await callBuildOneSource({
            runSQL: probeAwareRunSQL(),
            // An empty ledger, which is what an orchestrator sends the first
            // time: it owns the ledger and has nothing recorded yet.
            context: incrementalContext({ ledger: [] }).context,
         });
         expect(first.refresh).toBe("full");
         // The seed reports the whole entry to store — the caller's entire
         // protocol is `entries[*].ledger` back as `buildInstructions.ledger`.
         expect(first.ledger).toBeDefined();
         expect(first.ledger!.coveredThrough).toBe("2024-07-01");

         const runSQL = probeAwareRunSQL();
         const second = await callBuildOneSource({
            runSQL,
            context: incrementalContext({
               ledger: [first.ledger!],
               now: new Date("2024-07-02T00:00:00Z"),
            }).context,
         });
         expect(second.refresh).toBe("delta");
         const script = runSQL
            .getCalls()
            .map((c) => c.args[0] as string)
            .find((s) => s.startsWith("BEGIN;"));
         expect(script).toContain(`>= DATE '2024-07-01'`);
         expect(script).toContain(`< DATE '2024-07-02'`);
         expect(second.ledger!.coveredThrough).toBe("2024-07-02");
      });

      it("recomputes an idempotent overlap when the entry lags the table", async () => {
         // A stale-but-lagging entry (the caller echoed an older report) is
         // SAFE, not an error and not a rebuild: the half-open range is
         // idempotent, so the delta recomputes rows the table already holds and
         // replaces them with themselves. This is the same tolerance a lagging
         // local row has always had.
         const runSQL = probeAwareRunSQL();
         const { context } = incrementalContext({
            ledger: [{ ...ENTRY, coveredThrough: "2024-06-10" }],
         });
         const entry = await callBuildOneSource({ runSQL, context });
         expect(entry.refresh).toBe("delta");
         const script = runSQL
            .getCalls()
            .map((c) => c.args[0] as string)
            .find((s) => s.startsWith("BEGIN;"));
         expect(script).toContain(`>= DATE '2024-06-10'`);
      });

      // These still rebuild rather than erroring: none of them is a defect in
      // the caller's INPUT. A missing entry means "nothing recorded" (a first
      // build), and `reseed` is the documented escape hatch that ignores the
      // entry. Both report the path they took.
      const fallbacks: [
         string,
         {
            ledger: LedgerEntry[];
            reseed?: boolean;
         },
      ][] = [
         ["no entry for this table (an empty ledger)", { ledger: [] }],
         ["reseed, which ignores the entry", { ledger: [ENTRY], reseed: true }],
      ];

      for (const [what, params] of fallbacks) {
         it(`rebuilds in full, truthfully, given ${what}`, async () => {
            const runSQL = probeAwareRunSQL();
            const { context, upsert, remove } = incrementalContext({
               ledger: params.ledger,
            });
            const entry = await callBuildOneSource({
               runSQL,
               context,
               ...(params.reseed === undefined
                  ? {}
                  : { reseed: params.reseed }),
            });

            const statements = runSQL
               .getCalls()
               .map((c) => c.args[0] as string);
            expect(statements.some((s) => s.startsWith("CREATE TABLE"))).toBe(
               true,
            );
            expect(statements.some((s) => s.startsWith("BEGIN;"))).toBe(false);
            expect(entry.refresh).toBe("full");
            // A rebuild still reports the entry to store, so the caller's next
            // run has a boundary to send.
            expect(entry.ledger?.coveredThrough).toBe("2024-07-01");
            expect(upsert.called).toBe(false);
            expect(remove.called).toBe(false);
         });
      }

      // An entry that no longer describes the source it names is the caller's
      // error, and the API contract makes it one. Only a mismatch create-time
      // validation cannot see reaches this throw — one the compiled declaration
      // alone reveals — but the unit pins the whole family.
      const invalid: [string, LedgerEntry][] = [
         ["measures another watermark", { ...ENTRY, watermark: "shipped_at" }],
         [
            "was measured under another watermark type",
            { ...ENTRY, coveredThroughType: "timestamp" },
         ],
         ["was advanced by another strategy", { ...ENTRY, strategy: "merge" }],
         [
            "carries a different row identity",
            { ...ENTRY, mergeKeys: ["order_id"] },
         ],
         [
            "was measured under different SQL",
            { ...ENTRY, sourceEntityId: "some-older-address" },
         ],
      ];

      for (const [what, entry] of invalid) {
         it(`fails the run when the supplied entry ${what}`, async () => {
            const runSQL = probeAwareRunSQL();
            const { context, upsert, remove } = incrementalContext({
               ledger: [entry],
            });
            await expect(
               callBuildOneSource({ runSQL, context }),
            ).rejects.toThrow(/different definition of this source/);
            // Nothing was built and nothing was written: the run fails before
            // any DML, so the caller can fix the input and re-send.
            const statements = runSQL
               .getCalls()
               .map((c) => c.args[0] as string);
            expect(statements.some((s) => s.startsWith("CREATE TABLE"))).toBe(
               false,
            );
            expect(statements.some((s) => s.startsWith("BEGIN;"))).toBe(false);
            expect(upsert.called).toBe(false);
            expect(remove.called).toBe(false);
         });
      }

      it("still seeds on a fact about the TABLE, exactly as local mode does", async () => {
         // The entry is fine; the table is empty. That is not the caller's
         // input being wrong, so it degrades the same way a local row does.
         const runSQL = probeAwareRunSQL(false);
         const { context, upsert } = incrementalContext({ ledger: [ENTRY] });
         const entry = await callBuildOneSource({ runSQL, context });
         expect(entry.refresh).toBe("full");
         expect(upsert.called).toBe(false);
      });
   });
});
