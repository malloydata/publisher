// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { beforeEach, describe, expect, it } from "bun:test";
import * as sinon from "sinon";
import {
   BadRequestError,
   EnvironmentNotFoundError,
   InvalidStateTransitionError,
   MaterializationConflictError,
   MaterializationNotFoundError,
} from "../errors";
import {
   BuildInstruction,
   LedgerEntry,
   MaterializationStatus,
} from "../storage/DatabaseInterface";
import { DuplicateActiveMaterializationError } from "../storage/duckdb/MaterializationRepository";
import {
   makeBuildPlan,
   makeInstruction,
   makeMaterialization,
} from "../test_helpers/materialization_test_fixtures";
import {
   createMocks,
   setPackage,
} from "../test_helpers/materialization_service_harness";
import { logger } from "../logger";

/**
 * The run lifecycle of MaterializationService: create (auto-run and
 * orchestrated), queries, stop, the runBuild branches, background terminal
 * recording, and the state machine's transitions.
 */

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
         .resolves({ entries, failures: {}, sourcesRefused: 0 });
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
      expect(svc.commitManifest.firstCall.args[3]).not.toHaveProperty(
         "sourcesRefused",
      );
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

   it("records auto-run's refused sources in the run metadata, not on the manifest", async () => {
      // A refused source binds no table, and the manifest is what a
      // strict-schema caller parses, so the list rides the free-form metadata.
      const svc = stubEngine();
      const refused = {
         gated: {
            name: "gated",
            sourceID: "gated",
            tier: "storage",
            reason: "given",
            message: "Source 'gated' cannot be materialized",
         },
      };
      (
         svc as unknown as { deriveSelfInstructions: sinon.SinonStub }
      ).deriveSelfInstructions = sinon.stub().returns({
         instructions: [makeInstruction()],
         carried: {},
         refused,
      });

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
         sourcesBuilt: 1,
         sourcesRefused: 1,
         refusedSources: refused,
      });
      // Named in the error of a run whose every instructed source failed.
      expect(svc.executeInstructedBuild.firstCall.args[9]).toEqual([
         refused.gated,
      ]);
   });

   it("counts an orchestrated refusal in the metadata, and lists it only in failures", async () => {
      const svc = stubEngine();
      svc.executeInstructedBuild.resolves({
         entries: {
            "build-orders": {
               sourceEntityId: "build-orders",
               physicalTableName: '"orders_v1"',
               connectionName: "duckdb",
            },
         },
         failures: {
            gated: {
               sourceEntityId: "gated",
               sourceName: "gated",
               reason: "Source 'gated' cannot be materialized",
            },
         },
         sourcesRefused: 1,
      });

      await svc.runBuild(
         "mat-1",
         "my-env",
         "pkg",
         {
            sourceNames: undefined,
            forceRefresh: false,
            buildInstructions: [makeInstruction()],
         },
         new AbortController().signal,
      );

      expect(svc.commitManifest.firstCall.args[2]).toHaveProperty("gated");
      expect(svc.commitManifest.firstCall.args[3]).toMatchObject({
         sourcesRefused: 1,
      });
      expect(svc.commitManifest.firstCall.args[3]).not.toHaveProperty(
         "refusedSources",
      );
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
