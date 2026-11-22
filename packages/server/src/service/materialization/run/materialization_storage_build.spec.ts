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
import { StorageBuildStrategy } from "./storage_build_strategy";
import { resetMaterializationTelemetryForTesting } from "../../../materialization_metrics";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "../../../test_helpers/metrics_harness";
import { fakeContractReads } from "../../../test_helpers/contract_fixture";

// Builds over fake connections, which answer no catalog query.
fakeContractReads();

/**
 * The storage tier's build: the chained-build fallback ladder, destination
 * resolution, and redaction of an infrastructure failure on the chained path.
 */

describe("the storage tier's build (failure redaction)", () => {
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
         const service = new StorageBuildStrategy({
            buildDownstreamViaParents: sandbox
               .stub()
               .rejects(
                  new Error(
                     "IO Error: failed to ATTACH 'ducklake:postgres:host=catalog.internal " +
                        "user=lake_rw password=c4talog-pw' (db lake)",
                  ),
               ),
         });
         const environment = buildEnvironmentStub({
            connections: { orders_pg: sourceConnection },
            destinations: { lake: destinationConnection },
         });

         const call = (
            service as unknown as {
               build: (...a: unknown[]) => Promise<unknown>;
            }
         ).build({
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
            storedBuildSQL: "SELECT 1",
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

describe("the storage tier's build (chained-build fallback ladder)", () => {
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
         | "wrongTierAndMissing"
         | "withUndeclarable"
         | "collation";
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
         connections: { duckdb: { name: "duckdb", type: "postgres" } },
         destinations: { lake: { name: "lake", type: "duckdb" } },
      });
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
         // Builds a table with two columns no Malloy type declares.
         withUndeclarable: sinon.stub().resolves({
            storageDestinationName: "lake",
            schema: [
               { name: "order_month", type: "DATE" },
               { name: "tags", type: "MAP(VARCHAR, INTEGER)" },
               { name: "span", type: "INTERVAL" },
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
         // Over the parents the build would compare a string byte-exact that
         // its source compares under a collation (buildDownstreamViaParents):
         // no build over them gives live's rows, as for `raw`.
         collation: sinon.stub().rejects(
            new MaterializationEligibilityError({
               message:
                  "'monthly' compares a string over its stored upstreams ('daily') that the destination, byte-exact, would compare otherwise than its source does: `region` compares under a unknown collation",
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
      const svc = new StorageBuildStrategy({
         buildDownstreamViaParents: seam[opts.stackOnParent],
      }) as unknown as {
         build: (p: {
            persistSource: unknown;
            instruction: BuildInstruction;
            manifest: Manifest;
            environment: unknown;
            storedBuildSQL: string;
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
      return svc.build({
         persistSource: source,
         instruction,
         manifest,
         environment,
         storedBuildSQL: "SELECT should_not_run",
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

   // Always strict: a chained build is built over its stored upstreams or
   // refused, never by recomputing them from raw, whatever the run asks.
   it("a persisted upstream the build cannot see: refuses, naming it, strict or not", async () => {
      for (const strict of [true, false]) {
         const call = callInto({ strict, stackOnParent: "missing" });
         await expect(call).rejects.toThrow(
            /never built by recomputing its upstreams from raw.*persisted upstream 'daily'/is,
         );
         // Typed, so the run can attribute it to an upstream that failed.
         await expect(call).rejects.toBeInstanceOf(ChainedUpstreamMissingError);
      }
   });

   it("a downstream that reaches the warehouse through a non-persisted source: refused, never recomputed, strict or not", async () => {
      for (const opts of [
         { strict: true },
         { strict: false },
         { strict: true, warehouseReach: "recompute" as const },
         { strict: true, warehouseReach: "refuse" as const },
      ]) {
         const call = callInto({ ...opts, stackOnParent: "raw" });
         await expect(call).rejects.toBeInstanceOf(ChainedShapeNotCarriedError);
         await expect(call).rejects.toThrow(
            /never built by recomputing its upstreams from raw.*reads the source warehouse \(regions\) through 'daily_regional'/is,
         );
      }
   });

   it("a shape over stored upstreams the build could not carry: refused, strict or not", async () => {
      for (const strict of [true, false]) {
         const call = callInto({ strict, stackOnParent: "uncarried" });
         await expect(call).rejects.toThrow(/could not be built over them/i);
         // Typed, so the run classifies it deterministic.
         await expect(call).rejects.toBeInstanceOf(ChainedShapeNotCarriedError);
      }
   });

   // Decided 2026-11-12: a host-instructed build refuses a host-supplied
   // upstream entry at the wrong tier, strict or not, instead of recomputing.
   it("host-instructed + a parent supplied at the wrong tier: refuses, strict or not, and never recomputes", async () => {
      const { HostSuppliedWrongTierError } = await import("./build_upstreams");
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
      const { HostSuppliedWrongTierError } = await import("./build_upstreams");
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

   it("self-instructed + a parent supplied at the wrong tier: refused, never recomputed", async () => {
      await expect(
         callInto({ strict: false, stackOnParent: "wrongTier" }),
      ).rejects.toBeInstanceOf(ChainedUpstreamMissingError);
   });

   it("an INFRA failure fails rather than recomputing from raw, strict or not", async () => {
      // Recompute-from-raw writes to the same destination, so retrying an outage
      // there just fails again — and metering it as a fallback files the outage
      // under the same label as a legitimate shape miss.
      for (const strict of [false, true]) {
         await expect(
            callInto({ strict, stackOnParent: "infra" }),
         ).rejects.toThrow(/Failed to materialize chained source/i);
      }
   });

   it("a downstream whose strings its source compares otherwise: refused, never recomputed, strict or not", async () => {
      for (const opts of [{ strict: true }, { strict: false }]) {
         const call = callInto({ ...opts, stackOnParent: "collation" });
         await expect(call).rejects.toBeInstanceOf(ChainedShapeNotCarriedError);
         await expect(call).rejects.toThrow(
            /compares under a unknown collation/i,
         );
      }
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
         for (const stackOnParent of ["raw", "missing", "uncarried"] as const) {
            await callInto({ strict: false, stackOnParent }).catch(
               () => undefined,
            );
         }
         await callInto({ strict: true, stackOnParent: "infra" }).catch(
            () => undefined,
         );
         for (const [outcome, n] of [
            ["parent_reuse", 1],
            // Retained, no longer produced: nothing recomputes from raw.
            ["strict_shape_fallback", 0],
            ["inline_fallback", 0],
            ["strict_refused", 3],
            ["infra_failure", 1],
         ] as const) {
            expect(await harness.collectCounter(COUNTER, { outcome })).toBe(n);
         }
      });

      it("counts each column the table holds that no type declares, once per build", async () => {
         const result = await callInto({
            strict: false,
            stackOnParent: "withUndeclarable",
         });
         // Built, not refused: the served copy leaves the columns out at bind.
         expect(result.storageDestinationName).toBe("lake");
         expect(
            await harness.collectCounter(
               "publisher_storage_serve_shape_type_fallback_total",
               { kind: "unrecognized" },
            ),
         ).toBe(2);
      });
   });
});

describe("the storage tier's build destination resolution", () => {
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
      const service = new StorageBuildStrategy({
         buildDownstreamViaParents: write,
      });

      const run = () =>
         (
            service as unknown as {
               build: (p: {
                  persistSource: unknown;
                  instruction: BuildInstruction;
                  manifest: unknown;
                  environment: unknown;
                  storedBuildSQL: string;
                  buildSQL: string;
                  builtEntries: Record<string, unknown>;
                  dependsOnStorageUpstream: boolean;
               }) => Promise<{ storageDestinationName?: string }>;
            }
         ).build({
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
            storedBuildSQL: "SELECT should_not_run",
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
         connections: { duckdb: { name: "duckdb", type: "postgres" } },
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
            duckdb: { name: "duckdb", type: "postgres" },
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

   // The plan's gate refuses a source the build cannot read; one that reaches
   // the build anyway is refused alone, never failing the run with a missing
   // connection.
   for (const [what, connections] of [
      ["the package's sandbox, which the environment does not hold", {}],
      [
         "an environment DuckDB connection",
         { duckdb: { name: "duckdb", type: "duckdb" } },
      ],
   ] as const) {
      it(`refuses a source on ${what}, writing nothing`, async () => {
         const environment = buildEnvironmentStub({
            connections,
            destinations: { shared: MANAGED_DESTINATION },
         });
         const { run, write } = callInto(environment);

         let error: unknown;
         try {
            await run();
         } catch (caught) {
            error = caught;
         }

         expect(error).toBeInstanceOf(MaterializationEligibilityError);
         expect((error as MaterializationEligibilityError).reason).toBe(
            "storage_source_unsupported",
         );
         expect((error as Error).message).toContain("connection 'duckdb'");
         expect(write.called).toBe(false);
      });
   }

   // A DuckLake source builds only into its own catalog: another catalog is
   // refused alone, with the same reason, rather than failing the run.
   const catalogAt = (host: string) => ({
      catalog: {
         postgresConnection: { host, port: 5432, databaseName: "lake" },
      },
      storage: { bucketUrl: "gs://managed-tier/org_a" },
   });
   const DESTINATION_AT_A = {
      name: "shared",
      type: "ducklake",
      ducklakeConnection: catalogAt("catalog-a.internal"),
   };

   it("refuses a DuckLake source on another catalog, writing nothing", async () => {
      const environment = buildEnvironmentStub({
         connections: {
            duckdb: {
               name: "duckdb",
               type: "ducklake",
               ducklakeConnection: catalogAt("catalog-b.internal"),
            },
         },
         destinations: { shared: DESTINATION_AT_A },
      });
      const { run, write } = callInto(environment);

      let error: unknown;
      try {
         await run();
      } catch (caught) {
         error = caught;
      }

      expect(error).toBeInstanceOf(MaterializationEligibilityError);
      expect((error as MaterializationEligibilityError).reason).toBe(
         "storage_source_unsupported",
      );
      expect((error as Error).message).toContain(
         "a DuckLake catalog other than the destination's",
      );
      expect(write.called).toBe(false);
   });

   it("admits a DuckLake source on the destination's own catalog", async () => {
      const environment = buildEnvironmentStub({
         connections: {
            duckdb: {
               name: "duckdb",
               type: "ducklake",
               ducklakeConnection: catalogAt("catalog-a.internal"),
            },
         },
         destinations: { shared: DESTINATION_AT_A },
      });
      const { run, write } = callInto(environment);

      await run();

      expect(write.called).toBe(true);
   });
});
