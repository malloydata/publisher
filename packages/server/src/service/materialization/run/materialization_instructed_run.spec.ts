// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { Connection as MalloyConnection } from "@malloydata/malloy";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { BuildRunFailedError } from "./build_failure_class";
import { Manifest } from "@malloydata/malloy";
import { beforeEach, describe, expect, it } from "bun:test";
import * as sinon from "sinon";
import {
   MaterializationEligibilityError,
   BadRequestError,
} from "../../../errors";
import { BuildInstruction } from "../domain/types";
import {
   compiledWith,
   fakeSource,
} from "../../../test_helpers/materialization_test_fixtures";
import { createMocks } from "../../../test_helpers/materialization_service_harness";
import { logger } from "../../../logger";
import { resetMaterializationTelemetryForTesting } from "../../../materialization_metrics";
import { startMetricsHarness } from "../../../test_helpers/metrics_harness";
import { installSettings } from "../../../test_helpers/settings_env";

/**
 * Instructed builds (executeInstructedBuild): resolving each instruction,
 * table collisions and dedupe, destination checks, and per-source failure
 * records.
 */

describe("executeInstructedBuild", () => {
   let ctx: ReturnType<typeof createMocks>;
   beforeEach(() => {
      ctx = createMocks();
   });

   type ExecuteResult = {
      entries: Record<
         string,
         { physicalTableName?: string; sourceName?: string }
      >;
      failures: Record<
         string,
         { reason?: string; physicalTableName?: string; sourceName?: string }
      >;
      sourcesRefused: number;
   };

   function callExecute(
      compiled: unknown,
      instructions: BuildInstruction[],
      seed: Record<string, unknown>,
      apiConnections?: Record<string, unknown>,
      strict = false,
      // The host instructed the run (orchestrated).
      hostInstructed = false,
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
               strict: boolean,
               ...rest: unknown[]
            ) => Promise<ExecuteResult>;
         }
      ).executeInstructedBuild(
         compiled,
         environment,
         instructions,
         seed,
         new AbortController().signal,
         strict,
         undefined,
         undefined,
         undefined,
         [],
         undefined,
         hostInstructed,
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
         builds: [{ connectionName: "duckdb", order: ["only"], dependsOn: {} }],
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
         builds: [
            { connectionName: "duckdb", order: ["good", "bad"], dependsOn: {} },
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

   it("refuses a host-instructed rollup over a stored base with its reason, not a chained-build error (#6293)", async () => {
      const runSQL = sinon.stub().resolves();
      const orders = fakeSource({
         name: "orders",
         sourceEntityId: "b0r0r0r0r0r0r0r0",
         annotationFields: { storage: "lake" },
      });
      const rollup = fakeSource({
         name: "orders__preagg__category",
         sourceEntityId: "brollrollrollroll",
         annotationFields: { storage: "lake" },
      });
      const compiled = {
         builds: [
            {
               connectionName: "duckdb",
               order: ["orders", "orders__preagg__category"],
               dependsOn: {},
            },
         ],
         sources: { orders, orders__preagg__category: rollup },
         connectionDigests: { duckdb: "dig" },
         connections: new Map([
            ["duckdb", { runSQL } as unknown as MalloyConnection],
         ]),
         preaggregatePlans: {
            orders__preagg__category: {
               baseSourceName: "orders",
               baseSourceID: "orders",
               grainDimensions: ["category"],
               measures: [],
            },
         },
      };
      // Only the rollup instructed: refused, so the run has nothing to build.
      const err = await callExecute(
         compiled,
         [
            {
               sourceEntityId: "brollrollrollroll",
               materializedTableId: "mt-r",
               physicalTableName: "r_v1",
               realization: "COPY",
            },
         ],
         {},
      ).then(
         () => undefined,
         (e: unknown) => e as Error,
      );
      expect(err?.message).toContain("cannot build or serve yet");
      expect(runSQL.called).toBe(false);
   });

   describe("classifying a failure as retryable", () => {
      function instruction(id: string, table: string): BuildInstruction {
         return {
            sourceEntityId: id,
            materializedTableId: `mt-${table}`,
            physicalTableName: table,
            realization: "COPY",
         };
      }
      const strictMiss = () =>
         Object.assign(
            new Error("Persisted source 'up' not found in manifest"),
            { code: "runtime-manifest-strict-miss" },
         );

      async function runChain(upstreamError: string, thrown?: Error) {
         const runSQL = sinon.stub().callsFake(async (sql: string) => {
            if (String(sql).includes("up_v1")) {
               throw thrown ?? new Error(upstreamError);
            }
         });
         const connection = { runSQL } as unknown as MalloyConnection;
         const ok = fakeSource({
            name: "ok",
            sourceEntityId: "b0k0k0k0k0k0k0k0",
         });
         const up = fakeSource({
            name: "up",
            sourceEntityId: "bupupupupupupupu",
         });
         // A strict colocated build of a source whose upstream is not in the
         // manifest: Malloy refuses to render it. The option-free render that
         // computes its content address still succeeds.
         const down = fakeSource({
            name: "down",
            sourceEntityId: "bdowndowndowndow",
            onGetSQL: (opts) => {
               if (opts) throw strictMiss();
            },
         });
         const compiled = {
            builds: [
               {
                  connectionName: "duckdb",
                  order: ["ok", "up", "down"],
                  dependsOn: { down: ["up"] },
               },
            ],
            sources: { ok, up, down },
            connectionDigests: { duckdb: "dig" },
            connections: new Map([["duckdb", connection]]),
         };
         const result = await callExecute(
            compiled,
            [
               instruction("b0k0k0k0k0k0k0k0", "ok_v1"),
               instruction("bupupupupupupupu", "up_v1"),
               instruction("bdowndowndowndow", "down_v1"),
            ],
            {},
         );
         return result.failures as Record<
            string,
            { reason?: string; retryable?: boolean }
         >;
      }

      it("marks a dependent of a deterministically failed upstream deterministic, naming the upstream's failure", async () => {
         const failures = await runChain(
            'Parser Error: syntax error at or near "FORM"',
         );
         expect(failures["bupupupupupupupu"].retryable).toBe(false);
         const down = failures["bdowndowndowndow"];
         expect(
            down.retryable,
            "retrying cannot help a source whose upstream fails the same way every time",
         ).toBe(false);
         expect(down.reason).toContain(
            `upstream 'up' failed in this run: Parser Error: syntax error at or near "FORM"`,
         );
      });

      it("keeps a dependent of a transiently failed upstream retryable", async () => {
         const failures = await runChain("connect ECONNREFUSED 10.0.0.1:5432");
         expect(failures["bupupupupupupupu"].retryable).toBe(true);
         expect(failures["bdowndowndowndow"].retryable).toBe(true);
      });

      // #6847's shape: a package whose only source fails builds nothing, so
      // the run throws instead of committing a manifest with `failures`.
      async function runOnly(error: string): Promise<unknown> {
         const runSQL = sinon.stub().callsFake(async () => {
            throw new Error(error);
         });
         const only = fakeSource({
            name: "only",
            sourceEntityId: "b0n0n0n0n0n0n0n0",
         });
         return callExecute(
            {
               builds: [
                  { connectionName: "duckdb", order: ["only"], dependsOn: {} },
               ],
               sources: { only },
               connectionDigests: { duckdb: "dig" },
               connections: new Map([
                  ["duckdb", { runSQL } as unknown as MalloyConnection],
               ]),
            },
            [instruction("b0n0n0n0n0n0n0n0", "only_v1")],
            {},
         ).then(
            () => undefined,
            (err: unknown) => err,
         );
      }

      it("carries the run's answer on the error when every source failed", async () => {
         const deterministic = await runOnly(
            'Parser Error: syntax error at or near "FORM"',
         );
         expect(deterministic).toBeInstanceOf(BuildRunFailedError);
         expect((deterministic as BuildRunFailedError).retryable).toBe(false);
         expect((deterministic as Error).message).toContain("FORM");
         const memory = await runOnly(
            "Out of Memory Error: could not allocate block of size 256.0 KiB",
         );
         expect((memory as BuildRunFailedError).retryable).toBe(true);
         const odd = await runOnly("something odd happened");
         expect((odd as BuildRunFailedError).retryable).toBeUndefined();
      });

      // ms2data/service#6756: the instructions refused the only build there
      // is, so the same instructions refuse it again.
      it("marks a source refused under warehouseReach refuse deterministic, and its dependent too", async () => {
         const { WarehouseRecomputeRefusedError } = await import(
            "../../../errors"
         );
         const refused = new WarehouseRecomputeRefusedError(
            "up",
            "lake",
            "'up' reads the source warehouse (regions) through 'via'",
         );
         const failures = await runChain(refused.message, refused);
         expect(failures["bupupupupupupupu"].retryable).toBe(false);
         expect(failures["bupupupupupupupu"].reason).toContain(
            "warehouseReach: refuse",
         );
         expect(failures["bdowndowndowndow"].retryable).toBe(false);
      });

      it("omits the field when the failure is not recognized", async () => {
         const failures = await runChain("something odd happened");
         expect("retryable" in failures["bupupupupupupupu"]).toBe(false);
         expect("retryable" in failures["bdowndowndowndow"]).toBe(false);
      });
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
         builds: [
            { connectionName: "duckdb", order: ["good", "bad"], dependsOn: {} },
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
      // A warehouse failure may clear on its own, so it is not marked permanent.
      expect(failures["cbadbbbbbbbbbbb"]).not.toHaveProperty("refused");
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
         builds: [
            { connectionName: "duckdb", order: ["good", "bad"], dependsOn: {} },
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
         builds: [
            { connectionName: "duckdb", order: ["good", "bad"], dependsOn: {} },
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
         builds: [{ connectionName: "duckdb", order: ["only"], dependsOn: {} }],
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

   it("builds every instructed source of a build, dependencies first", async () => {
      // `root` reads `mid`; both are instructed, so both build, `mid` first.
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
         builds: [
            { connectionName: "duckdb", order: ["mid", "root"], dependsOn: {} },
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

   /** The CREATE TABLE statements a run issued, in order. */
   function createsFrom(runSQL: sinon.SinonStub): string[] {
      return runSQL
         .getCalls()
         .map((c) => c.args[0] as string)
         .filter((s) => s.startsWith("CREATE TABLE"));
   }

   it("builds one table once when several sources share its content address", async () => {
      // `#@ persist` is inherited and `extend` does not change a source's
      // materialization SQL, so a base and its extension are two names for ONE
      // table: same content address, same instruction. Iterating per source built
      // it once per name.
      const runSQL = sinon.stub().resolves();
      const connection = { runSQL } as unknown as MalloyConnection;
      const base = fakeSource({
         name: "base",
         sourceEntityId: "bsharedaaaaaaaa",
      });
      const ext = fakeSource({
         name: "ext",
         sourceEntityId: "bsharedaaaaaaaa",
      });
      // Malloy's walk reaches an extension through its base, so the base comes
      // first among the sources on their shared table.
      const compiled = {
         builds: [
            { connectionName: "duckdb", order: ["base", "ext"], dependsOn: {} },
         ],
         sources: { base, ext },
         connectionDigests: { duckdb: "dig" },
         connections: new Map([["duckdb", connection]]),
      };

      const { entries } = await callExecute(
         compiled,
         [
            {
               sourceEntityId: "bsharedaaaaaaaa",
               materializedTableId: "mt-1",
               physicalTableName: "rollup_v1",
               realization: "COPY",
            },
         ],
         {},
      );

      expect(
         createsFrom(runSQL).filter((s) => s.includes("rollup_v1")),
      ).toHaveLength(1);
      expect(entries["bsharedaaaaaaaa"].physicalTableName).toBe("rollup_v1");
      // The entry is attributed to the source that DECLARED the persist, not to
      // the extension that inherited it. This decides the `storage=` serve path:
      // deriveServeBindings emits one binding per entry keyed on
      // `entry.sourceName`, so whoever owns the entry is the source that routes to
      // the table — and the other serves LIVE. Before the dedupe, each source
      // overwrote the entry in turn, so the winner was whoever built LAST and the
      // declared source could silently lose its own routing.
      expect(entries["bsharedaaaaaaaa"].sourceName).toBe("base");
   });

   it("does not collapse one source across two connections", async () => {
      // The target key's connection half is the BUILD's, because `buildOneSource`
      // writes through the connection resolved from `build.connectionName`. One
      // declaration under two builds' connections is two tables, in two
      // warehouses, that must not dedupe onto one key. Keying on the SOURCE's
      // connection would collapse them and write only the first.
      const runDuck = sinon.stub().resolves();
      const runPg = sinon.stub().resolves();
      const shared = fakeSource({
         name: "shared",
         sourceEntityId: "bsharedddddddddd",
      });
      const compiled = {
         builds: [
            { connectionName: "duckdb", order: ["shared"], dependsOn: {} },
            { connectionName: "postgres", order: ["shared"], dependsOn: {} },
         ],
         sources: { shared },
         connectionDigests: { duckdb: "dig", postgres: "dig2" },
         connections: new Map([
            ["duckdb", { runSQL: runDuck } as unknown as MalloyConnection],
            ["postgres", { runSQL: runPg } as unknown as MalloyConnection],
         ]),
      };

      await callExecute(
         compiled,
         [
            {
               sourceEntityId: "bsharedddddddddd",
               materializedTableId: "mt-x",
               physicalTableName: "shared_v1",
               realization: "COPY",
            },
         ],
         {},
      );

      expect(createsFrom(runDuck)).toHaveLength(1);
      expect(createsFrom(runPg)).toHaveLength(1);
   });

   it("builds one table once when two models' builds both reach it", async () => {
      // A source declared in one model and consumed in another appears in BOTH
      // models' builds, and the package plan concatenates them, so the artifact
      // has to be deduplicated across builds rather than within one.
      const runSQL = sinon.stub().resolves();
      const connection = { runSQL } as unknown as MalloyConnection;
      const shared = fakeSource({
         name: "shared",
         sourceEntityId: "bsharedbbbbbbbb",
      });
      const build = {
         connectionName: "duckdb",
         order: ["shared"],
         dependsOn: {},
      };
      const compiled = {
         builds: [build, build],
         sources: { shared },
         connectionDigests: { duckdb: "dig" },
         connections: new Map([["duckdb", connection]]),
      };

      await callExecute(
         compiled,
         [
            {
               sourceEntityId: "bsharedbbbbbbbb",
               materializedTableId: "mt-1",
               physicalTableName: "shared_v1",
               realization: "COPY",
            },
         ],
         {},
      );

      expect(
         createsFrom(runSQL).filter((s) => s.includes("shared_v1")),
      ).toHaveLength(1);
   });

   /**
    * Two definitions that materialize into ONE physical table: different content
    * addresses, one instructed `physicalTableName`. The pair a host assigns rather
    * than the model declares, which is the case `persistenceCollisionWarnings`
    * cannot see at load.
    */
   function collidingBuild(runSQL: sinon.SinonStub) {
      const connection = { runSQL } as unknown as MalloyConnection;
      const a = fakeSource({
         name: "a",
         sourceEntityId: "baaaaaaaaaaaaaa",
         sql: "SELECT 1",
      });
      const b = fakeSource({
         name: "b",
         sourceEntityId: "bbbbbbbbbbbbbbb",
         sql: "SELECT 2",
      });
      return callExecute(
         compiledWith(
            { a, b },
            [["a", "b"]],
            new Map([["duckdb", connection]]),
         ),
         [
            {
               sourceEntityId: "baaaaaaaaaaaaaa",
               materializedTableId: "mt-a",
               physicalTableName: "rollup",
               realization: "COPY",
            },
            {
               sourceEntityId: "bbbbbbbbbbbbbbb",
               materializedTableId: "mt-b",
               physicalTableName: "rollup",
               realization: "COPY",
            },
         ],
         {},
      );
   }

   it("warns, and still builds, when two definitions collide on one table and PERSIST_COLLISION_ENFORCE=false", async () => {
      // The opt-out, matching the load/publish collision gate: a deployment
      // that keeps a package with a latent collision keeps its rebuilds.
      const warn = sinon.stub(logger, "warn");
      const { restore } = installSettings({
         env: { PERSIST_COLLISION_ENFORCE: "false" },
      });
      try {
         const runSQL = sinon.stub().resolves();
         await collidingBuild(runSQL);

         expect(
            createsFrom(runSQL).filter((s) => s.includes("rollup")),
         ).toHaveLength(2);
         expect(
            warn
               .getCalls()
               .some((c) =>
                  String(c.args[0]).includes(
                     "Two definitions are materializing into one table",
                  ),
               ),
         ).toBe(true);
      } finally {
         restore();
         warn.restore();
      }
   });

   it("builds only the last of several same-address instructions with no sourceID", async () => {
      // `sourceID` is optional on the wire, and without it the address index is the
      // only route back to an instruction — it holds one per address, so the LAST
      // instruction wins and the earlier names are never built. Nothing can
      // attribute them: an address maps to every one of them equally.
      //
      // Pinned because the sibling test stamps a sourceID on both instructions and
      // therefore proves the opposite behaviour. The condition is still reported —
      // the shared-address counter fires either way.
      const warn = sinon.stub(logger, "warn");
      try {
         const runSQL = sinon.stub().resolves();
         const connection = { runSQL } as unknown as MalloyConnection;
         const base = fakeSource({
            name: "base",
            sourceEntityId: "bsharedddddddddd",
         });
         const ext = fakeSource({
            name: "ext",
            sourceEntityId: "bsharedddddddddd",
         });

         await callExecute(
            compiledWith(
               { base, ext },
               [["base", "ext"]],
               new Map([["duckdb", connection]]),
            ),
            [
               {
                  sourceEntityId: "bsharedddddddddd",
                  materializedTableId: "mt-g0",
                  physicalTableName: "rollup__g000",
                  realization: "COPY",
               },
               {
                  sourceEntityId: "bsharedddddddddd",
                  materializedTableId: "mt-g1",
                  physicalTableName: "rollup__g001",
                  realization: "COPY",
               },
            ],
            {},
         );

         const creates = createsFrom(runSQL);
         expect(creates.filter((c) => c.includes("rollup__g001"))).toHaveLength(
            1,
         );
         expect(creates.filter((c) => c.includes("rollup__g000"))).toHaveLength(
            0,
         );
         expect(
            warn
               .getCalls()
               .some((c) =>
                  String(c.args[0]).includes(
                     "One content address was instructed to build more than one table",
                  ),
               ),
         ).toBe(true);
      } finally {
         warn.restore();
      }
   });

   it("meters a table collision apart from a per-source-naming host", async () => {
      // Two conditions, two counters, on purpose. A host minting a table per source
      // is wasteful but correct; two definitions sharing one table answers a query
      // from another source's data. Only the second is worth paging on, so it must
      // not be indistinguishable from the first.
      const harness = await startMetricsHarness();
      resetMaterializationTelemetryForTesting();
      const warn = sinon.stub(logger, "warn");
      const { restore } = installSettings({
         env: { PERSIST_COLLISION_ENFORCE: "false" },
      });
      try {
         await collidingBuild(sinon.stub().resolves());
         expect(
            await harness.collectCounter(
               "publisher_materialization_table_collision_total",
               {},
            ),
         ).toBe(1);
         expect(
            await harness.collectCounter(
               "publisher_materialization_shared_address_instructions_total",
               {},
            ),
         ).toBe(0);
      } finally {
         restore();
         warn.restore();
         resetMaterializationTelemetryForTesting();
         await harness.shutdown();
      }
   });

   it("refuses that collision by default (PERSIST_COLLISION_ENFORCE), before any write", async () => {
      // The refusal is the whole point of the flag, so it has to land before the
      // first CTAS: a mid-loop throw would leave the pair's first table already
      // replaced, and nothing puts the overwritten rows back — the failure path
      // reclaims storage tables only, and a reclaim cannot restore data.
      const { restore } = installSettings({
         env: { PERSIST_COLLISION_ENFORCE: "true" },
      });
      const runSQL = sinon.stub().resolves();
      try {
         const promise = collidingBuild(runSQL);
         await expect(promise).rejects.toThrow(
            /both materialize into table 'rollup'/,
         );
         await expect(promise).rejects.toBeInstanceOf(
            MaterializationEligibilityError,
         );
         expect(createsFrom(runSQL)).toHaveLength(0);
      } finally {
         restore();
      }
   });

   it("builds both tables, and warns, when one address is instructed twice", async () => {
      // A host that mints a physical name per SOURCE produces this for an
      // ordinary package. The publisher cannot resolve it — declining either
      // table leaves the host an anchor for something never written — so it
      // reports and builds both, exactly as before.
      const warn = sinon.stub(logger, "warn");
      try {
         const runSQL = sinon.stub().resolves();
         const connection = { runSQL } as unknown as MalloyConnection;
         const base = fakeSource({
            name: "base",
            sourceEntityId: "bsharedcccccccc",
         });
         const ext = fakeSource({
            name: "ext",
            sourceEntityId: "bsharedcccccccc",
         });
         const compiled = compiledWith(
            { base, ext },
            [["base", "ext"]],
            new Map([["duckdb", connection]]),
         );

         await callExecute(
            compiled,
            [
               {
                  sourceEntityId: "bsharedcccccccc",
                  sourceID: "base",
                  materializedTableId: "mt-g0",
                  physicalTableName: "rollup__g000",
                  realization: "COPY",
               },
               {
                  sourceEntityId: "bsharedcccccccc",
                  sourceID: "ext",
                  materializedTableId: "mt-g1",
                  physicalTableName: "rollup__g001",
                  realization: "COPY",
               },
            ],
            {},
         );

         const creates = createsFrom(runSQL);
         expect(creates.filter((s) => s.includes("rollup__g000"))).toHaveLength(
            1,
         );
         expect(creates.filter((s) => s.includes("rollup__g001"))).toHaveLength(
            1,
         );
         expect(
            warn
               .getCalls()
               .some((c) =>
                  String(c.args[0]).includes(
                     "One content address was instructed to build more than one table",
                  ),
               ),
         ).toBe(true);
      } finally {
         warn.restore();
      }
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

   // ms2data/service#7087: a source moved to `storage=` keeps its content
   // address, and a stored table's reference names the WAREHOUSE connection, so
   // a reference to it (or to the table from before the move) read as a
   // warehouse table, and its bare name was rendered into warehouse SQL.
   describe("an upstream declared `storage=` is never a warehouse table", () => {
      async function downstreamSees(opts: {
         upStorage?: string;
         seed: Record<string, unknown>;
         mode?: string;
         strict?: boolean;
         hostInstructed?: boolean;
         /** The downstream does not read `up` (default: it does). */
         downIgnoresUp?: boolean;
         /** Another upstream nothing supplied; a strict render reports it first. */
         alsoMissing?: boolean;
      }) {
         // Every render Malloy is handed. The FIRST is the build SQL; later ones
         // only classify the build's upstreams and are never executed.
         const renders: {
            buildManifest?: {
               entries?: Record<string, { tableName?: string }>;
            };
         }[] = [];
         const connection = {
            runSQL: sinon.stub().resolves(),
            dialectName: "duckdb",
         } as unknown as MalloyConnection;
         const up = fakeSource({
            name: "up",
            sourceEntityId: "up",
            ...(opts.upStorage
               ? { annotationFields: { storage: opts.upStorage } }
               : {}),
         });
         const down = fakeSource({
            name: "down",
            sourceEntityId: "bdownaaaaaaaaaa",
            onGetSQL: (o) => {
               renders.push(o as (typeof renders)[number]);
               // Malloy's strict render refuses an upstream it cannot find.
               const m = (
                  o as {
                     buildManifest?: {
                        strict?: boolean;
                        entries?: Record<string, unknown>;
                     };
                  }
               ).buildManifest;
               if (opts.downIgnoresUp) return undefined;
               if (m?.strict && opts.alsoMissing) {
                  throw Object.assign(
                     new Error(
                        "Persisted source 'other' not found in manifest",
                     ),
                     { code: "runtime-manifest-strict-miss" },
                  );
               }
               if (m?.strict && !m.entries?.up) {
                  throw Object.assign(
                     new Error("Persisted source 'up' not found in manifest"),
                     { code: "runtime-manifest-strict-miss" },
                  );
               }
               // As Malloy does: with up's entry the SQL reads its table.
               if (m?.entries?.up) return "SELECT * FROM up_table";
            },
         });
         const compiled = compiledWith(
            { up, down },
            [["up"], ["down"]],
            new Map([["duckdb", connection]]),
         );
         const { restore } = installSettings({
            env: { PERSIST_STORAGE_MODE: opts.mode ?? "on" },
         });
         try {
            const result = await callExecute(
               compiled,
               [
                  {
                     sourceEntityId: "bdownaaaaaaaaaa",
                     materializedTableId: "mt-d",
                     physicalTableName: "down_v1",
                     realization: "COPY",
                  },
               ],
               opts.seed,
               undefined,
               opts.strict ?? false,
               opts.hostInstructed ?? false,
            );
            return { seen: renders[0], result };
         } finally {
            restore();
         }
      }

      it("does not render a stored upstream's unlabeled reference into warehouse SQL", async () => {
         // The reference the control plane sends today: the stored table's
         // name on the warehouse connection, no destination.
         const { seen, result } = await downstreamSees({
            upStorage: "lake",
            seed: {
               up: {
                  sourceEntityId: "up",
                  physicalTableName: "up__g004__e9c3ad40",
                  connectionName: "duckdb",
               },
            },
         });
         expect(seen?.buildManifest?.entries?.up).toBeUndefined();
         // Still carried in the run's manifest; only the warehouse render skips it.
         expect(result.entries.up?.physicalTableName).toBe(
            "up__g004__e9c3ad40",
         );
      });

      it("does not render the warehouse table from before the upstream moved to storage", async () => {
         const { seen } = await downstreamSees({
            upStorage: "lake",
            seed: {
               up: {
                  sourceEntityId: "up",
                  physicalTableName: "analytics.up_v1",
                  connectionName: "duckdb",
               },
            },
         });
         expect(seen?.buildManifest?.entries?.up).toBeUndefined();
      });

      const oldWarehouseTable = {
         up: {
            sourceEntityId: "up",
            physicalTableName: "analytics.up_v1",
            connectionName: "duckdb",
         },
      };

      it("reads every upstream as a warehouse table while storage mode is off", async () => {
         const { seen } = await downstreamSees({
            upStorage: "lake",
            mode: "off",
            seed: oldWarehouseTable,
         });
         expect(seen?.buildManifest?.entries?.up?.tableName).toBe(
            '"analytics"."up_v1"',
         );
      });

      it("reads a storage= upstream as stored under write-only, so its warehouse table is not rendered", async () => {
         const { seen } = await downstreamSees({
            upStorage: "lake",
            mode: "write-only",
            seed: oldWarehouseTable,
         });
         expect(seen?.buildManifest?.entries?.up).toBeUndefined();
      });

      it("refuses a strict colocated build whose upstream was supplied at the wrong tier, naming both tiers", async () => {
         // The only instructed source fails, so the run itself fails, with it.
         const reason = await downstreamSees({
            upStorage: "lake",
            seed: oldWarehouseTable,
            strict: true,
         }).then(
            () => "",
            (err: unknown) =>
               err instanceof Error ? err.message : String(err),
         );
         expect(reason).toContain(
            "'up' is declared in storage destination 'lake'",
         );
         expect(reason).toContain("supplied at the wrong tier");
         expect(reason).toContain("strict upstreams forbid");
         expect(reason).not.toContain("not found in manifest");
      });

      // Decided 2026-11-12: a host that hands a build another tier's table
      // (two workers with separate stores sharing one config) is a case the
      // host must prevent, so a host-instructed build refuses, strict or not,
      // rather than recomputing the upstream across that boundary.
      const reasonOf = (p: Promise<unknown>) =>
         p.then(
            () => "",
            (err: unknown) =>
               err instanceof Error ? err.message : String(err),
         );

      const unlabeledUp = {
         up: {
            sourceEntityId: "up",
            physicalTableName: "up__g004__e9c3ad40",
            connectionName: "duckdb",
         },
      };

      it("refuses the host-supplied wrong-tier upstream even when a strict render reports another miss first", async () => {
         const reason = await reasonOf(
            downstreamSees({
               upStorage: "lake",
               hostInstructed: true,
               strict: true,
               alsoMissing: true,
               seed: unlabeledUp,
            }),
         );
         expect(reason).toContain("the host supplied an upstream entry");
         expect(reason).toContain(
            "'up' is declared in storage destination 'lake'",
         );
      });

      it("does not refuse a host-instructed build for a wrong-tier entry it does not read", async () => {
         const reason = await reasonOf(
            downstreamSees({
               upStorage: "lake",
               hostInstructed: true,
               downIgnoresUp: true,
               seed: unlabeledUp,
            }),
         );
         expect(reason).not.toContain("the host supplied an upstream entry");
      });

      it("refuses a host-instructed build whose upstream's unlabeled reference is at the wrong tier, not strict", async () => {
         const reason = await reasonOf(
            downstreamSees({
               upStorage: "lake",
               hostInstructed: true,
               seed: {
                  up: {
                     sourceEntityId: "up",
                     physicalTableName: "up__g004__e9c3ad40",
                     connectionName: "duckdb",
                  },
               },
            }),
         );
         expect(reason).toContain("the host supplied an upstream entry");
         expect(reason).toContain(
            "'up' is declared in storage destination 'lake'",
         );
         expect(reason).toContain("a reference that does not say where");
         expect(reason).not.toContain("not found in manifest");
      });

      it("refuses a host-instructed strict build with the host-supplied message", async () => {
         const reason = await reasonOf(
            downstreamSees({
               upStorage: "lake",
               hostInstructed: true,
               strict: true,
               seed: oldWarehouseTable,
            }),
         );
         expect(reason).toContain("the host supplied an upstream entry");
         expect(reason).not.toContain("strict upstreams forbid");
      });

      it("classifies the host-supplied refusal as deterministic", async () => {
         const { classifyBuildError } = await import("./build_failure_class");
         const { HostSuppliedWrongTierError } = await import(
            "./materialization_run_support"
         );
         expect(
            classifyBuildError(
               new HostSuppliedWrongTierError("down", [
                  { name: "up", detail: "x" },
               ]),
            ),
         ).toBe(false);
      });

      it("still recomputes in a self-instructed build, not strict", async () => {
         const { seen } = await downstreamSees({
            upStorage: "lake",
            seed: oldWarehouseTable,
         });
         expect(seen?.buildManifest?.entries?.up).toBeUndefined();
      });

      it("treats an entry built into the source's own database as current in a host-instructed build", async () => {
         // No `storage=`, and a destination that is the source's own
         // connection: the same database, so the entry is current and the host
         // build refuses nothing (#1619's review fix).
         const { result } = await downstreamSees({
            hostInstructed: true,
            seed: {
               up: {
                  sourceEntityId: "up",
                  physicalTableName: "analytics.up_v1",
                  connectionName: "duckdb",
                  storageDestinationName: "duckdb",
               },
            },
         });
         expect(result.entries.bdownaaaaaaaaaa).toBeDefined();
         expect(Object.keys(result.failures ?? {})).toEqual([]);
      });

      it("still renders a colocated upstream's warehouse table", async () => {
         const { seen } = await downstreamSees({
            seed: {
               up: {
                  sourceEntityId: "up",
                  physicalTableName: "analytics.up_v1",
                  connectionName: "duckdb",
               },
            },
         });
         expect(seen?.buildManifest?.entries?.up?.tableName).toBe(
            '"analytics"."up_v1"',
         );
      });
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

   it("throws when an instructed build's connection is missing", async () => {
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
         // EVERY persist source in the build — including one with no
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

      it("fails only itself, reported in failures, and its siblings still build, when the caller DOES instruct it", async () => {
         const runSQL = sinon.stub().resolves();
         const connection = { runSQL } as unknown as MalloyConnection;
         const ok = fakeSource({
            name: "ok",
            sourceEntityId: "b0k0k0k0k0k0k0k0",
         });
         const refused = refusedColocated("bref1bref1bref1b");
         const compiled = compiledWith(
            { ok, refused },
            [["refused"], ["ok"]],
            new Map([["duckdb", connection]]),
         );

         const result = await callExecute(
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
         );

         expect(result.entries["b0k0k0k0k0k0k0k0"].physicalTableName).toBe(
            "ok_v1",
         );
         // Reported where the caller resolves failures, under the address it
         // dispatched and with the gate's own message: an absent entry would
         // read as "built" to a caller that asked for this table.
         expect(result.failures["bref1bref1bref1b"]).toMatchObject({
            sourceName: "refused",
            physicalTableName: "refused_v1",
            materializedTableId: "mt-ref",
            // Permanent until the model changes, so a caller need not retry it.
            refused: true,
            retryable: false,
         });
         expect(result.failures["bref1bref1bref1b"].reason).toMatch(
            /authorize/i,
         );
         expect(result.sourcesRefused).toBe(1);
         expect(
            runSQL
               .getCalls()
               .some((c) => String(c.args[0]).includes("refused_v1")),
            "a refused source must not be written",
         ).toBe(false);
      });

      // ms2data/service#7078, colocated: a base and its `extend` share one
      // content address, and a colocated table is served by address (the bind
      // refuses the whole address when any source on it is refused). So a
      // refused extension refuses the address in both run orders: nothing is
      // built that could not serve, and the outcome no longer depends on order.
      for (const [label, order] of [
         ["the eligible source comes first", [["ok"], ["base"], ["ext"]]],
         ["the refused source comes first", [["ok"], ["ext"], ["base"]]],
      ] as const) {
         it(`refuses a colocated address a refused sibling shares when ${label}, building nothing on it`, async () => {
            const runSQL = sinon.stub().resolves();
            const connection = { runSQL } as unknown as MalloyConnection;
            const ok = fakeSource({
               name: "ok",
               sourceEntityId: "b0k0k0k0k0k0k0k0",
            });
            const base = fakeSource({
               name: "base",
               sourceEntityId: "bshared7078aaaaa",
            });
            const ext = fakeSource({
               name: "ext",
               sourceEntityId: "bshared7078aaaaa",
               sourceDef: { blockNotes: ["#(authorize) true"] },
            });
            const compiled = compiledWith(
               { ok, base, ext },
               order.map((o) => [...o]),
               new Map([["duckdb", connection]]),
            );
            const result = (await callExecute(
               compiled,
               [
                  {
                     sourceEntityId: "b0k0k0k0k0k0k0k0",
                     materializedTableId: "mt-ok",
                     physicalTableName: "ok_v1",
                     realization: "COPY",
                  },
                  {
                     sourceEntityId: "bshared7078aaaaa",
                     materializedTableId: "mt-shared",
                     physicalTableName: "shared_v1",
                     realization: "COPY",
                  },
               ],
               {},
            )) as ExecuteResult & {
               refusalsInFailures: number;
               refusedBesideBuilt: Record<string, unknown>;
            };
            expect(result.entries["b0k0k0k0k0k0k0k0"].physicalTableName).toBe(
               "ok_v1",
            );
            expect(result.failures["bshared7078aaaaa"]).toMatchObject({
               refused: true,
               retryable: false,
               physicalTableName: "shared_v1",
               // The gated source, by its own reason, in both orders: never
               // the ungated base with a borrowed reason.
               sourceName: "ext",
            });
            expect(result.failures["bshared7078aaaaa"].reason).not.toContain(
               "is not built",
            );
            expect(result.refusalsInFailures).toBe(1);
            expect(result.sourcesRefused).toBe(2);
            expect(result.refusedBesideBuilt).toEqual({});
            expect(
               createsFrom(runSQL).some((sql) => sql.includes("shared_v1")),
               "nothing is built on a refused colocated address",
            ).toBe(false);
         });
      }

      // Before #7078's fix a base and its gated extension built the base and
      // left failed entries; the run was not failed. With the address now
      // refused whole, nothing is built on it, and the run must still not be
      // failed as one whose every source was refused: the base's own gate
      // admits it.
      for (const [label, order] of [
         ["the base comes first", [["base"], ["ext"]]],
         ["the extension comes first", [["ext"], ["base"]]],
      ] as const) {
         it(`does not fail a run whose only sources are a colocated base and its refused extension when ${label}`, async () => {
            const runSQL = sinon.stub().resolves();
            const connection = { runSQL } as unknown as MalloyConnection;
            const base = fakeSource({
               name: "base",
               sourceEntityId: "bpair7078aaaaaaa",
            });
            const ext = fakeSource({
               name: "ext",
               sourceEntityId: "bpair7078aaaaaaa",
               sourceDef: { blockNotes: ["#(authorize) true"] },
            });
            const compiled = compiledWith(
               { base, ext },
               order.map((o) => [...o]),
               new Map([["duckdb", connection]]),
            );
            const result = (await callExecute(
               compiled,
               [
                  {
                     sourceEntityId: "bpair7078aaaaaaa",
                     materializedTableId: "mt-pair",
                     physicalTableName: "pair_v1",
                     realization: "COPY",
                  },
               ],
               {},
            )) as ExecuteResult & { refusalsInFailures: number };
            expect(result.failures["bpair7078aaaaaaa"]).toMatchObject({
               refused: true,
               sourceName: "ext",
            });
            expect(result.entries["bpair7078aaaaaaa"]).toMatchObject({
               sourceName: "ext",
            });
            expect(result.refusalsInFailures).toBe(1);
            expect(runSQL.called).toBe(false);
         });
      }

      it("builds the eligible table and nothing on a refused colocated address, on a real DuckDB connection", async () => {
         // No stubbed build: the real colocated build runs against DuckDB.
         const duckdb = new DuckDBConnection("duckdb");
         try {
            const ok = fakeSource({
               name: "ok",
               sourceEntityId: "b0k0k0k0k0k0k0k0",
               sql: "SELECT 1 AS n",
            });
            const base = fakeSource({
               name: "base",
               sourceEntityId: "breal7078aaaaaaa",
               sql: "SELECT 2 AS n",
            });
            const ext = fakeSource({
               name: "ext",
               sourceEntityId: "breal7078aaaaaaa",
               sql: "SELECT 2 AS n",
               sourceDef: { blockNotes: ["#(authorize) true"] },
            });
            const compiled = compiledWith(
               { ok, base, ext },
               [["ok"], ["base"], ["ext"]],
               new Map([["duckdb", duckdb as unknown as MalloyConnection]]),
            );
            const result = await callExecute(
               compiled,
               [
                  {
                     sourceEntityId: "b0k0k0k0k0k0k0k0",
                     materializedTableId: "mt-ok",
                     physicalTableName: "real_ok_v1",
                     realization: "COPY",
                  },
                  {
                     sourceEntityId: "breal7078aaaaaaa",
                     materializedTableId: "mt-real",
                     physicalTableName: "real_shared_v1",
                     realization: "COPY",
                  },
               ],
               {},
            );
            expect(result.failures["breal7078aaaaaaa"]).toMatchObject({
               sourceName: "ext",
            });
            const tables = (
               await duckdb.runSQL(
                  "SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'real_%'",
               )
            ).rows.map((r) =>
               String((r as { table_name: unknown }).table_name),
            );
            expect(tables).toContain("real_ok_v1");
            expect(tables).not.toContain("real_shared_v1");
         } finally {
            await duckdb.close();
         }
      });

      it("never fails a colocated run on an unrelated source whose gate throws", async () => {
         // The colocated sibling check gates only the sources on the address
         // it is asked about. An unrelated planned source whose gate walk
         // throws something other than a refusal used to fail every colocated
         // run in the package.
         const runSQL = sinon.stub().resolves();
         const connection = { runSQL } as unknown as MalloyConnection;
         const ok = fakeSource({
            name: "ok",
            sourceEntityId: "b0k0k0k0k0k0k0k0",
         });
         const odd = fakeSource({
            name: "odd",
            sourceEntityId: "boddoddoddoddodd",
         });
         Object.defineProperty(odd, "annotations", {
            get() {
               throw new TypeError("the gate walk broke");
            },
         });
         const compiled = compiledWith(
            { ok, odd },
            [["ok"], ["odd"]],
            new Map([["duckdb", connection]]),
         );
         const result = await callExecute(
            compiled,
            [
               {
                  sourceEntityId: "b0k0k0k0k0k0k0k0",
                  materializedTableId: "mt-ok",
                  physicalTableName: "ok_v1",
                  realization: "COPY",
               },
            ],
            {},
         );
         expect(result.entries["b0k0k0k0k0k0k0k0"].physicalTableName).toBe(
            "ok_v1",
         );
         expect(result.failures).toEqual({});
      });

      it("holds back a colocated base whose extension auto-run already refused", async () => {
         // Auto-run refused the extension (deriveSelfInstructions) and cleared
         // the base; the base's instruction still names the shared address.
         // The extension is not refused twice, and the base is not built onto
         // a table the bind would refuse to serve.
         const runSQL = sinon.stub().resolves();
         const connection = { runSQL } as unknown as MalloyConnection;
         const ok = fakeSource({
            name: "ok",
            sourceEntityId: "b0k0k0k0k0k0k0k0",
         });
         const base = fakeSource({
            name: "base",
            sourceEntityId: "bauto7078aaaaaaa",
         });
         const ext = fakeSource({
            name: "ext",
            sourceEntityId: "bauto7078aaaaaaa",
            sourceDef: { blockNotes: ["#(authorize) true"] },
         });
         const compiled = compiledWith(
            { ok, base, ext },
            [["ok"], ["base"], ["ext"]],
            new Map([["duckdb", connection]]),
         );
         const environment = {
            getApiConnection: () => {
               throw new Error("no connection config in this test");
            },
            getEnvironmentPath: () => "/tmp/env",
         };
         const service = ctx.service as unknown as {
            executeInstructedBuild: (
               ...args: unknown[]
            ) => Promise<ExecuteResult & { refusalsInFailures: number }>;
         };
         const result = await service.executeInstructedBuild(
            compiled,
            environment,
            [
               {
                  sourceEntityId: "b0k0k0k0k0k0k0k0",
                  materializedTableId: "mt-ok",
                  physicalTableName: "ok_v1",
                  realization: "COPY",
               },
               {
                  sourceEntityId: "bauto7078aaaaaaa",
                  materializedTableId: "mt-auto",
                  physicalTableName: "auto_v1",
                  realization: "COPY",
               },
            ],
            {},
            new AbortController().signal,
            false,
            undefined,
            undefined,
            undefined,
            [
               {
                  name: "ext",
                  sourceID: "ext",
                  tier: "colocated",
                  reason: "authorize",
                  message: "Source 'ext' cannot be materialized",
               },
            ],
            new Map([
               ["ok", "colocated"],
               ["base", "colocated"],
            ]),
         );
         expect(result.entries["b0k0k0k0k0k0k0k0"].physicalTableName).toBe(
            "ok_v1",
         );
         expect(result.failures["bauto7078aaaaaaa"]).toMatchObject({
            refused: true,
            sourceName: "base",
         });
         expect(result.failures["bauto7078aaaaaaa"].reason).toContain("ext");
         // Only the base's hold-back is counted here; auto-run counted ext.
         expect(result.sourcesRefused).toBe(1);
         expect(
            createsFrom(runSQL).some((sql) => sql.includes("auto_v1")),
            "nothing is built on a refused colocated address",
         ).toBe(false);
      });

      it("still fails an address only refused sources reach", async () => {
         const runSQL = sinon.stub().resolves();
         const connection = { runSQL } as unknown as MalloyConnection;
         const ok = fakeSource({
            name: "ok",
            sourceEntityId: "b0k0k0k0k0k0k0k0",
         });
         const a = refusedColocated("bonlyrefusedaaaa");
         const b = fakeSource({
            name: "refused_too",
            sourceEntityId: "bonlyrefusedaaaa",
            sourceDef: { blockNotes: ["#(authorize) true"] },
         });
         const compiled = compiledWith(
            { ok, refused: a, refused_too: b },
            [["ok"], ["refused", "refused_too"]],
            new Map([["duckdb", connection]]),
         );
         const result = (await callExecute(
            compiled,
            [
               {
                  sourceEntityId: "b0k0k0k0k0k0k0k0",
                  materializedTableId: "mt-ok",
                  physicalTableName: "ok_v1",
                  realization: "COPY",
               },
               {
                  sourceEntityId: "bonlyrefusedaaaa",
                  materializedTableId: "mt-ref",
                  physicalTableName: "refused_v1",
                  realization: "COPY",
               },
            ],
            {},
         )) as ExecuteResult & { refusalsInFailures: number };
         expect(result.failures["bonlyrefusedaaaa"]).toMatchObject({
            refused: true,
            retryable: false,
            physicalTableName: "refused_v1",
         });
         // Once per address in failures, though both sources are refused.
         expect(result.refusalsInFailures).toBe(1);
         expect(result.sourcesRefused).toBe(2);
      });

      it("still 422s when every instructed source is refused", async () => {
         const runSQL = sinon.stub().resolves();
         const connection = { runSQL } as unknown as MalloyConnection;
         const refused = refusedColocated("bref1bref1bref1b");
         const compiled = compiledWith(
            { refused },
            [["refused"]],
            new Map([["duckdb", connection]]),
         );

         await expect(
            callExecute(
               compiled,
               [
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
         expect(runSQL.called).toBe(false);
      });
   });
});
