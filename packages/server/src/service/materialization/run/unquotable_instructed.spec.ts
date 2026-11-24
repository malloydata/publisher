// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A host-supplied table name its dialect cannot quote, in an instructed run:
 * a seeded upstream's is not bound (warned), and a source instructed to build
 * one is refused alone, permanently, before any warehouse work. The rest of
 * the run goes on.
 */

import type { Connection as MalloyConnection } from "@malloydata/malloy";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as sinon from "sinon";
import { logger } from "../../../logger";
import { createMocks } from "../../../test_helpers/materialization_service_harness";
import {
   fakeSource,
   persistTables,
} from "../../../test_helpers/materialization_test_fixtures";
import type { BuildInstruction } from "../domain/types";
import { fakeContractReads } from "../../../test_helpers/contract_fixture";

// Builds over fake connections, which answer no catalog query.
fakeContractReads();

let warn: sinon.SinonStub;
let quiet: sinon.SinonStub[] = [];
beforeEach(() => {
   warn = sinon.stub(logger, "warn");
   quiet = [sinon.stub(logger, "info"), sinon.stub(logger, "error")];
});
afterEach(() => {
   warn.restore();
   for (const stub of quiet) stub.restore();
});

type RunResult = {
   entries: Record<string, { error?: string }>;
   failures?: Record<
      string,
      { sourceName: string; refused?: boolean; retryable?: boolean }
   >;
};

function execute(
   compiled: unknown,
   instructions: BuildInstruction[],
   seed: Record<string, unknown> = {},
): Promise<RunResult> {
   const ctx = createMocks();
   const environment = {
      getApiConnection: () => {
         throw new Error("no connection config in this test");
      },
      getEnvironmentName: () => "env",
      getEnvironmentPath: () => "/tmp/env",
   };
   return (
      ctx.service.collaborators as unknown as {
         executeInstructedBuild: (...args: unknown[]) => Promise<RunResult>;
      }
   ).executeInstructedBuild({
      compiled,
      environment,
      instructions,
      seedEntries: seed,
      signal: new AbortController().signal,
      priorRefusals: [],
      hostInstructed: true,
   });
}

const instruction = (
   sourceEntityId: string,
   physicalTableName: string,
): BuildInstruction => ({
   sourceEntityId,
   materializedTableId: `mt-${sourceEntityId}`,
   physicalTableName,
   realization: "COPY",
});

function bigQuery(statements: string[]): MalloyConnection {
   return {
      dialectName: "standardsql",
      runSQL: async (sql: string) => {
         statements.push(sql);
         return { rows: [] };
      },
   } as unknown as MalloyConnection;
}

describe("an unquotable host-supplied name in an instructed run", () => {
   it("does not bind a seeded upstream's, warns, and runs on", async () => {
      const statements: string[] = [];
      const down = fakeSource({
         name: "down",
         annotationFields: { name: "ds.down" },
         sourceEntityId: "bdown1",
         connectionName: "bq",
         dialectName: "standardsql",
      });
      await execute(
         {
            builds: [{ connectionName: "bq", order: ["down"], dependsOn: {} }],
            tables: persistTables({ down }),
            connectionDigests: { bq: "dig" },
            connections: new Map([["bq", bigQuery(statements)]]),
         },
         [instruction("bdown1", "ds.down_v1")],
         {
            up: {
               sourceEntityId: "up",
               physicalTableName: "ds.a\nb",
               connectionName: "bq",
            },
         },
      );
      expect(
         warn
            .getCalls()
            .some((c) =>
               String(c.args[0]).includes("cannot be quoted for its dialect"),
            ),
      ).toBe(true);
      // The run still built its own source.
      expect(statements.some((s) => s.includes("`ds`.`down_v1"))).toBe(true);
   });

   it("refuses a source instructed to build one, alone and permanently, with no warehouse work", async () => {
      const statements: string[] = [];
      const bad = fakeSource({
         name: "bad",
         annotationFields: { name: "ds.bad" },
         sourceEntityId: "bbad1",
         connectionName: "bq",
         dialectName: "standardsql",
      });
      const good = fakeSource({
         name: "good",
         annotationFields: { name: "ds.good" },
         sourceEntityId: "bgood1",
         connectionName: "bq",
         dialectName: "standardsql",
      });
      const result = await execute(
         {
            builds: [
               {
                  connectionName: "bq",
                  order: ["bad", "good"],
                  dependsOn: {},
               },
            ],
            tables: persistTables({ bad, good }),
            connectionDigests: { bq: "dig" },
            connections: new Map([["bq", bigQuery(statements)]]),
         },
         [instruction("bbad1", "ds.a\nb"), instruction("bgood1", "ds.good_v1")],
      );
      expect(statements.some((s) => s.includes("a\nb"))).toBe(false);
      expect(statements.some((s) => s.includes("`ds`.`good_v1"))).toBe(true);
      expect(result.failures?.bbad1).toMatchObject({
         sourceName: "bad",
         refused: true,
         retryable: false,
      });
      expect(result.failures?.bgood1).toBeUndefined();
   });
});
