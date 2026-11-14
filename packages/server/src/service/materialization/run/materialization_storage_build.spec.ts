// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Manifest } from "@malloydata/malloy";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as sinon from "sinon";
import {
   MaterializationEligibilityError,
   ChainedShapeNotCarriedError,
   ChainedUpstreamMissingError,
   DestinationNotFoundError,
   internalErrorToHttpError,
} from "../../../errors";
import { BuildInstruction } from "../domain/types";
import { fakeSource } from "../../../test_helpers/materialization_test_fixtures";
import {
   buildEnvironmentStub,
   createMocks,
} from "../../../test_helpers/materialization_service_harness";
import { MaterializationService } from "./materialization_service";
import { resetMaterializationTelemetryForTesting } from "../../../materialization_metrics";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "../../../test_helpers/metrics_harness";

/**
 * buildOneSourceIntoStorage: the chained-build fallback ladder, destination
 * resolution, and redaction of an infrastructure failure on the chained path.
 */

describe("buildOneSourceIntoStorage (failure redaction)", () => {
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

describe("buildOneSourceIntoStorage (chained-build fallback ladder)", () => {
   let ctx: ReturnType<typeof createMocks>;
   beforeEach(() => {
      ctx = createMocks();
   });

   function callInto(opts: {
      strict: boolean;
      /**
       * What the stack-on-parent seam does: builds (`ok`); finds the downstream
       * REACHES the warehouse, so no build over the parents exists (`raw`);
       * could not CARRY a shape that reads only stored parents (`uncarried`);
       * finds a persisted upstream MISSING from the destination; or fails on
       * infrastructure.
       */
      stackOnParent:
         | "ok"
         | "raw"
         | "uncarried"
         | "missing"
         | "infra"
         | "wrongTier"
         | "wrongTierAndMissing";
      /** The host instructed the run (orchestrated). */
      hostInstructed?: boolean;
      /** The host's `warehouseReach` (ms2data/service#6756). */
      warehouseReach?: "recompute" | "refuse";
   }): Promise<{
      physicalTableName: string;
      storageDestinationName?: string;
      upstreamReuse?: string;
      upstreamRecomputeReason?: string;
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
            planFacts?: unknown;
         }) => Promise<{
            physicalTableName: string;
            storageDestinationName?: string;
            upstreamReuse?: string;
            upstreamRecomputeReason?: string;
         }>;
      };
      // Stub the stack-on-parent seam so the test exercises the LADDER, not the build.
      const seam: Record<typeof opts.stackOnParent, sinon.SinonStub> = {
         ok: sinon.stub().resolves({
            storageDestinationName: "lake",
            schema: [
               { name: "order_month", type: "DATE" },
               { name: "monthly_total", type: "DOUBLE" },
            ],
            upstreamReuse: "reused",
         }),
         // An outage, not a modelling limit: a plain Error, as a failed ATTACH
         // or CTAS would throw.
         infra: sinon
            .stub()
            .rejects(new Error("IO Error: destination is down")),
         // The downstream reads the warehouse through a non-persisted source:
         // nothing stored stands in for that, so no build over the parents exists.
         raw: sinon.stub().rejects(
            new MaterializationEligibilityError({
               message:
                  "'monthly' reads the source warehouse (regions) through 'daily_regional'",
            }),
         ),
         // Every upstream is stored and present, yet the build could not carry
         // the shape over them: a limit of the carrying, not of the source.
         uncarried: sinon
            .stub()
            .rejects(
               new ChainedShapeNotCarriedError(
                  "'monthly' reads only stored upstreams ('daily') but could not be built over them: Reference to undefined object 'r'",
               ),
            ),
         // The parent's entry was supplied at the wrong tier (#7087): an
         // unlabeled reference for a parent declared `storage=`.
         wrongTier: sinon
            .stub()
            .rejects(
               new ChainedUpstreamMissingError(
                  ["daily"],
                  "'daily' is declared in storage destination 'lake', but its manifest entry is a warehouse table, or a reference that does not say where it lives: supplied at the wrong tier, so this build does not use it",
               ),
            ),
         // Two upstreams: one supplied at the wrong tier, one simply not
         // materialized here. Each carries its own reason.
         wrongTierAndMissing: sinon.stub().rejects(
            new ChainedUpstreamMissingError(
               ["daily", "weekly"],
               "persisted upstream of 'monthly': both — the build cannot stack on it",
               [
                  {
                     name: "daily",
                     detail:
                        "'daily' is declared in storage destination 'lake', but its manifest entry is a warehouse table, or a reference that does not say where it lives: supplied at the wrong tier, so this build does not use it",
                  },
                  {
                     name: "weekly",
                     detail:
                        "'weekly' is not materialized in destination 'lake' for this build",
                  },
               ],
            ),
         ),
         // A dispatch miss: a persisted upstream this build cannot see.
         missing: sinon
            .stub()
            .rejects(
               new ChainedUpstreamMissingError(
                  ["daily"],
                  "persisted upstream 'daily' of 'monthly' is not materialized in destination 'lake' for this build",
               ),
            ),
      };
      svc.buildDownstreamViaParents = seam[opts.stackOnParent];
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
         ...(opts.hostInstructed || opts.warehouseReach
            ? {
                 planFacts: {
                    persistSourceIds: new Set<string>(),
                    aliasesBySourceName: {},
                    addressBySourceId: {},
                    compilerKeyBySourceId: {},
                    hostInstructed: true,
                    ...(opts.warehouseReach
                       ? { warehouseReach: opts.warehouseReach }
                       : {}),
                 },
              }
            : {}),
      });
   }

   it("stacks on the parent: builds by reading it and the entry says so", async () => {
      const entry = await callInto({ strict: false, stackOnParent: "ok" });
      expect(entry.storageDestinationName).toBe("lake");
      expect(entry.physicalTableName).toBe("monthly__mabc");
      expect(entry.upstreamReuse).toBe("reused");
      expect(entry.upstreamRecomputeReason).toBeUndefined();
   });

   it("strict + a persisted upstream the build cannot see: refuses, naming it, instead of recomputing from raw", async () => {
      // The orchestrator meant to pin that upstream; recomputing it here would
      // rebuild a table it did not ask for. The refusal carries the upstream's
      // name so the dispatch, not the model, is what gets fixed.
      const call = callInto({ strict: true, stackOnParent: "missing" });
      await expect(call).rejects.toThrow(
         /strict upstreams forbid.*persisted upstream 'daily'/is,
      );
      // Typed, so the run can attribute it to an upstream that failed.
      await expect(call).rejects.toBeInstanceOf(ChainedUpstreamMissingError);
   });

   it("strict + a downstream that reaches the warehouse falls through to recompute-from-raw", async () => {
      // No build over the parents exists for this source, so the recompute is
      // the only build there is and strict permits it. Only the parent-reuse
      // seam is stubbed, so the recompute runs for real and fails for its own
      // reason (no destination file) — the proof it was reached, since the two
      // paths report differently: "chained source" from the stack-on-parent
      // path, "source" from the single-source recompute.
      await expect(
         callInto({ strict: true, stackOnParent: "raw" }),
      ).rejects.toThrow(/Failed to materialize source 'monthly'/i);
   });

   it("strict + a shape over stored upstreams the build could not carry: refuses rather than recomputing them", async () => {
      // Every upstream was handed to the build; recomputing them would rebuild
      // pinned tables because of a limit in the carrying, which is not the
      // source's doing and not a licence strict grants.
      const call = callInto({ strict: true, stackOnParent: "uncarried" });
      await expect(call).rejects.toThrow(
         /strict upstreams forbid.*could not be built over them/is,
      );
      // Typed, so the run classifies it deterministic.
      await expect(call).rejects.toBeInstanceOf(ChainedShapeNotCarriedError);
   });

   it("non-strict + a persisted upstream the build cannot see, or an uncarried shape, falls through to recompute-from-raw", async () => {
      for (const stackOnParent of ["missing", "uncarried"] as const) {
         await expect(
            callInto({ strict: false, stackOnParent }),
         ).rejects.toThrow(/Failed to materialize source 'monthly'/i);
      }
   });

   // Decided 2026-11-12: a host-instructed build refuses a host-supplied
   // upstream entry at the wrong tier, strict or not, instead of recomputing.
   it("host-instructed + a parent supplied at the wrong tier: refuses, strict or not, and never recomputes", async () => {
      const { HostSuppliedWrongTierError } = await import(
         "./materialization_run_support"
      );
      for (const strict of [false, true]) {
         const call = callInto({
            strict,
            stackOnParent: "wrongTier",
            hostInstructed: true,
         });
         await expect(call).rejects.toBeInstanceOf(HostSuppliedWrongTierError);
         await expect(call).rejects.toThrow(
            /the host supplied an upstream entry.*declared in storage destination 'lake'/is,
         );
      }
   });

   it("host-instructed: names only the upstream supplied at the wrong tier, each with its own reason", async () => {
      const { HostSuppliedWrongTierError } = await import(
         "./materialization_run_support"
      );
      const err = await callInto({
         strict: false,
         stackOnParent: "wrongTierAndMissing",
         hostInstructed: true,
      }).then(
         () => undefined,
         (e: unknown) => e,
      );
      expect(err).toBeInstanceOf(HostSuppliedWrongTierError);
      const message = (err as Error).message;
      expect((err as { missing: string[] }).missing).toEqual(["daily"]);
      expect(message).toContain("'daily' is declared in storage destination");
      expect(message).not.toContain("'weekly'");
      // One reason per upstream, not the whole joined message repeated.
      expect(message.split("supplied at the wrong tier").length).toBe(2);
   });

   it("self-instructed + a parent supplied at the wrong tier still recomputes when not strict", async () => {
      // The recompute runs for real and fails for its own reason (no
      // destination file): the single-source path's message proves it ran.
      await expect(
         callInto({ strict: false, stackOnParent: "wrongTier" }),
      ).rejects.toThrow(/Failed to materialize source 'monthly'/i);
   });

   it("an INFRA failure fails rather than recomputing from raw, strict or not", async () => {
      // Recompute-from-raw writes to the same destination, so retrying an outage
      // there just fails again — and metering it as a fallback files the outage
      // under the same label as a legitimate shape miss. Only a shape failure is
      // a reason to try the other path.
      for (const strict of [false, true]) {
         await expect(
            callInto({ strict, stackOnParent: "infra" }),
         ).rejects.toThrow(/Failed to materialize chained source/i);
      }
   });

   // ms2data/service#6756: the orchestrator can refuse the one recompute
   // strict permits.
   it("strict + warehouseReach refuse + a downstream that reaches the warehouse: refuses with the reason, never recomputing", async () => {
      const { WarehouseRecomputeRefusedError } = await import(
         "../../../errors"
      );
      const call = callInto({
         strict: true,
         stackOnParent: "raw",
         warehouseReach: "refuse",
      });
      await expect(call).rejects.toBeInstanceOf(WarehouseRecomputeRefusedError);
      // The reason the entry would have carried, and the knob that refused it.
      await expect(call).rejects.toThrow(
         /warehouseReach: refuse.*reads the source warehouse \(regions\) through 'daily_regional'/is,
      );
   });

   it("warehouseReach recompute is today's behaviour: strict recomputes a downstream that reaches the warehouse", async () => {
      await expect(
         callInto({
            strict: true,
            stackOnParent: "raw",
            warehouseReach: "recompute",
         }),
      ).rejects.toThrow(/Failed to materialize source 'monthly'/i);
   });

   it("a non-strict build ignores warehouseReach refuse, as it ignores strictUpstreams", async () => {
      await expect(
         callInto({
            strict: false,
            stackOnParent: "raw",
            warehouseReach: "refuse",
         }),
      ).rejects.toThrow(/Failed to materialize source 'monthly'/i);
   });

   it("warehouseReach refuse changes no other rung: a missing upstream and an uncarried shape still refuse as before", async () => {
      const missing = callInto({
         strict: true,
         stackOnParent: "missing",
         warehouseReach: "refuse",
      });
      await expect(missing).rejects.toBeInstanceOf(ChainedUpstreamMissingError);
      const uncarried = callInto({
         strict: true,
         stackOnParent: "uncarried",
         warehouseReach: "refuse",
      });
      await expect(uncarried).rejects.toBeInstanceOf(
         ChainedShapeNotCarriedError,
      );
   });

   it("non-strict + a downstream that reaches the warehouse falls through to recompute-from-raw", async () => {
      await expect(
         callInto({ strict: false, stackOnParent: "raw" }),
      ).rejects.toThrow(/Failed to materialize source 'monthly'/i);
   });

   describe("telemetry", () => {
      let harness: MetricsHarness;
      beforeEach(async () => {
         harness = await startMetricsHarness();
         resetMaterializationTelemetryForTesting();
      });
      afterEach(async () => {
         resetMaterializationTelemetryForTesting();
         await harness.shutdown();
      });
      const COUNTER = "publisher_storage_chained_build_total";

      it("meters each rung under its own outcome", async () => {
         await callInto({ strict: false, stackOnParent: "ok" });
         await callInto({ strict: true, stackOnParent: "raw" }).catch(
            () => undefined,
         );
         await callInto({ strict: false, stackOnParent: "raw" }).catch(
            () => undefined,
         );
         await callInto({ strict: true, stackOnParent: "missing" }).catch(
            () => undefined,
         );
         await callInto({ strict: true, stackOnParent: "uncarried" }).catch(
            () => undefined,
         );
         await callInto({ strict: true, stackOnParent: "infra" }).catch(
            () => undefined,
         );
         // The refused recompute is a strict refusal, not a fallback.
         await callInto({
            strict: true,
            stackOnParent: "raw",
            warehouseReach: "refuse",
         }).catch(() => undefined);
         for (const [outcome, n] of [
            ["parent_reuse", 1],
            ["strict_shape_fallback", 1],
            ["inline_fallback", 1],
            ["strict_refused", 3],
            ["infra_failure", 1],
         ] as const) {
            expect(await harness.collectCounter(COUNTER, { outcome })).toBe(n);
         }
      });
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
