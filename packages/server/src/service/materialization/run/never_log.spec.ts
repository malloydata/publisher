// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Manifest } from "@malloydata/malloy";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as sinon from "sinon";
import { logger } from "../../../logger";
import { BuildInstruction, LedgerEntry } from "../domain/types";
import { indexCallerLedger } from "./incremental_build";
import {
   compiledWith,
   fakeSource,
} from "../../../test_helpers/materialization_test_fixtures";
import { createMocks } from "../../../test_helpers/materialization_service_harness";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import type { Connection as MalloyConnection } from "@malloydata/malloy";
import { createTableWithDeclaredTypes } from "./materialization_build_session";
import { stackFrames } from "./run_lifecycle";
import { errorCodeOf } from "./error_code";

// The never-log rule (docs/materialization-v2-plan.md, "Step 4
// specification"): no log call, at any level, carries a value from the data
// (a watermark boundary, a row value) or SQL. Each case plants a sentinel and
// scans every logger call made.
const RECORDED = "1987-03-14";
const SNAPSHOT = "2031-11-29";

describe("incremental refresh logs never carry a watermark value", () => {
   let ctx: ReturnType<typeof createMocks>;
   let logged: unknown[][];
   let stubs: sinon.SinonStub[];
   beforeEach(() => {
      ctx = createMocks();
      logged = [];
      stubs = (["debug", "info", "warn", "error"] as const).map((level) =>
         sinon.stub(logger, level).callsFake(((...args: unknown[]) => {
            logged.push([level, ...args]);
            return logger;
         }) as never),
      );
   });
   afterEach(() => stubs.forEach((s) => s.restore()));

   /** Every logger call so far, and that none carries a planted boundary. */
   function expectNoBoundaryLogged(): void {
      expect(logged.length).toBeGreaterThan(0);
      for (const call of logged) {
         const text = JSON.stringify(call);
         expect(text).not.toContain(RECORDED);
         expect(text).not.toContain(SNAPSHOT);
      }
   }

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
      coveredThroughValue: RECORDED,
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
   function probeAwareRunSQL(
      nonEmpty = true,
      watermarkMax: unknown = SNAPSHOT,
   ): sinon.SinonStub {
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
            return { rows: [{ watermark_max: watermarkMax }] };
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
         now: overrides.now ?? new Date(`${SNAPSHOT}T00:00:00Z`),
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

   it("logs an applied delta without its range", async () => {
      const { context, upsert } = incrementalContext({});
      await callBuildOneSource({ runSQL: probeAwareRunSQL(), context });
      expect(upsert.calledOnce).toBe(true);
      expect(logged.some((c) => c[1] === "Applied an incremental delta")).toBe(
         true,
      );
      expectNoBoundaryLogged();
   });

   it("logs a skipped refresh by its reason code, not its boundary", async () => {
      const { context } = incrementalContext({
         ledgerEntry: { ...LEDGER_ROW, coveredThroughValue: SNAPSHOT },
      });
      await callBuildOneSource({ runSQL: probeAwareRunSQL(), context });
      const skip = logged.find(
         (c) => c[1] === "Skipping an incremental source's refresh",
      );
      expect(skip?.[2]).toMatchObject({ reasonCode: "not_advanced" });
      expectNoBoundaryLogged();
   });

   it("logs a seed over an emptied table by its reason code, not its boundary", async () => {
      const { context } = incrementalContext({});
      await callBuildOneSource({ runSQL: probeAwareRunSQL(false), context });
      const seed = logged.find(
         (c) => c[1] === "Rebuilding an incremental source in full",
      );
      expect(seed?.[2]).toMatchObject({ reasonCode: "table_emptied" });
      expectNoBoundaryLogged();
   });

   it("logs an unreadable boundary after a seed by its code, not the value it could not read", async () => {
      const { context } = incrementalContext({ ledgerEntry: null });
      // A MAX(watermark) the date parser refuses: its error quotes the value.
      await callBuildOneSource({
         runSQL: probeAwareRunSQL(true, `x${RECORDED}x`),
         context,
      });
      const unread = logged.find(
         (c) =>
            typeof c[1] === "string" &&
            c[1].startsWith("Could not read a covered_through boundary"),
      );
      expect(unread).toBeDefined();
      expectNoBoundaryLogged();
   });
});

// A driver error that quotes the build's SQL and a row value.
const ROW_VALUE = "SENTINEL_ROW_VALUE_7f3a";
const DRIVER_MESSAGE = `Conversion Error: INSERT INTO t SELECT '${ROW_VALUE}'`;

describe("build failure logs never carry the driver's message", () => {
   let logged: unknown[][];
   let stubs: sinon.SinonStub[];
   beforeEach(() => {
      logged = [];
      stubs = (["debug", "info", "warn", "error"] as const).map((level) =>
         sinon.stub(logger, level).callsFake(((...args: unknown[]) => {
            logged.push([level, ...args]);
            return logger;
         }) as never),
      );
   });
   afterEach(() => stubs.forEach((s) => s.restore()));

   function expectNothingQuoted(): void {
      expect(logged.length).toBeGreaterThan(0);
      for (const call of logged) {
         const text = JSON.stringify(call);
         expect(text).not.toContain(ROW_VALUE);
         expect(text).not.toContain("INSERT INTO");
      }
   }

   it("logs a source's failure by its code; the message stays on the record", async () => {
      const ctx = createMocks();
      const connection = {
         runSQL: sinon.stub().resolves(),
      } as unknown as MalloyConnection;
      const compiled = compiledWith(
         {
            s1: fakeSource({ name: "s1", sourceEntityId: "a1aaaaaaaaaaaaaa" }),
            s2: fakeSource({ name: "s2", sourceEntityId: "a2bbbbbbbbbbbbbb" }),
         },
         [["s1"], ["s2"]],
         new Map([["duckdb", connection]]),
      );
      (ctx.service as unknown as { buildOneSource: unknown }).buildOneSource =
         sinon.stub().callsFake(async (_s, i: BuildInstruction) => {
            if (i.sourceEntityId === "a1aaaaaaaaaaaaaa") {
               throw Object.assign(new Error(DRIVER_MESSAGE), {
                  code: "22018",
               });
            }
            return {
               sourceEntityId: i.sourceEntityId,
               physicalTableName: "t2",
               connectionName: "duckdb",
            };
         });
      const result = (await (
         ctx.service as unknown as {
            executeInstructedBuild: (...args: unknown[]) => Promise<unknown>;
         }
      ).executeInstructedBuild(
         compiled,
         {
            getApiConnection: () => {
               throw new Error("no config");
            },
            getEnvironmentPath: () => "/tmp/env",
         },
         [
            {
               sourceEntityId: "a1aaaaaaaaaaaaaa",
               materializedTableId: "mt-1",
               physicalTableName: "t1",
               realization: "COPY",
            },
            {
               sourceEntityId: "a2bbbbbbbbbbbbbb",
               materializedTableId: "mt-2",
               physicalTableName: "t2",
               realization: "COPY",
            },
         ],
         {},
         new AbortController().signal,
         false,
      )) as { failures: Record<string, { reason: string }> };

      expect(result.failures["a1aaaaaaaaaaaaaa"].reason).toContain(ROW_VALUE);
      const failed = logged.find(
         (c) => c[1] === "materialization.build.failed",
      );
      expect(failed?.[2]).toMatchObject({ code: "22018" });
      expectNothingQuoted();
   });

   it("logs a whole-run failure by its code and stack frames; the message stays on the record", async () => {
      const ctx = createMocks();
      (
         ctx.service as unknown as {
            runInBackground: (
               id: string,
               run: (signal: AbortSignal) => Promise<void>,
            ) => void;
         }
      ).runInBackground("bg-1", async () => {
         throw new Error(DRIVER_MESSAGE);
      });
      await new Promise((r) => setTimeout(r, 20));

      expect(
         ctx.repository.updateMaterialization.firstCall.args[1],
      ).toMatchObject({ status: "FAILED", error: DRIVER_MESSAGE });
      const failed = logged.find((c) => c[1] === "Materialization run failed");
      expect(failed?.[2]).toMatchObject({ code: "Error" });
      expect((failed?.[2] as { stack?: string }).stack).toContain("at ");
      expectNothingQuoted();
   });

   it("logs a declared-type cast that refused a value by its code", async () => {
      const conn = new DuckDBConnection("never_log_cast", ":memory:");
      try {
         const schema = await createTableWithDeclaredTypes(
            conn,
            "t",
            `SELECT 'x${ROW_VALUE}' AS flag`,
            { columns: [{ name: "flag", type: "boolean" }] },
            [],
         );
         // The cast refused the value, so the warehouse's type is stored.
         expect(schema).toEqual([{ name: "flag", type: "VARCHAR" }]);
      } finally {
         await conn.close();
      }
      expect(
         logged.some(
            (c) =>
               typeof c[1] === "string" &&
               c[1].startsWith("A declared-type cast could not convert"),
         ),
      ).toBe(true);
      for (const call of logged) {
         expect(JSON.stringify(call)).not.toContain(ROW_VALUE);
      }
   });
});

describe("errorCodeOf", () => {
   it("reads a wrapped driver error's code, never a message", () => {
      const driver = Object.assign(new Error(DRIVER_MESSAGE), {
         code: "42P01",
      });
      expect(errorCodeOf(new Error("wrapped", { cause: driver }))).toBe(
         "42P01",
      );
      expect(errorCodeOf(new TypeError(DRIVER_MESSAGE))).toBe("TypeError");
      const odd = Object.assign(new Error("m"), { code: DRIVER_MESSAGE });
      expect(errorCodeOf(odd)).toBe("Error");
      expect(errorCodeOf(DRIVER_MESSAGE)).toBe("Error");
      expect(stackFrames(new Error(DRIVER_MESSAGE))).not.toContain(ROW_VALUE);
   });
});

describe("stackFrames", () => {
   // A row value can hold anything, a stored stack trace included, and a cast
   // error quotes it whole: its lines must not pass for frames.
   const framed = `could not convert '${ROW_VALUE}\n    at ${ROW_VALUE} (x.ts:1:1)' to INTEGER`;

   it("drops a frame-shaped line of the message", () => {
      const frames = stackFrames(new Error(framed));
      expect(frames).not.toContain(ROW_VALUE);
      expect(frames).toContain("never_log.spec.ts");
   });

   it("drops a subclass's message, named or not", () => {
      class DriverError extends Error {
         override name = "DriverError";
      }
      expect(stackFrames(new DriverError(framed))).not.toContain(ROW_VALUE);
      expect(stackFrames(new Error(""))).toContain("never_log.spec.ts");
   });

   it("logs no frames when the stack no longer holds the message", () => {
      // A stack read before the message changed keeps the old message, which
      // nothing can tell from frames.
      const err = new Error(framed);
      void err.stack;
      err.message = "rewritten";
      expect(stackFrames(err)).toBeUndefined();
      // Cut short, it is still found in the stack, but not as the header.
      const cut = new Error(framed);
      void cut.stack;
      cut.message = framed.slice(0, 10);
      expect(stackFrames(cut)).toBeUndefined();
   });
});
