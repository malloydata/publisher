// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   afterAll,
   afterEach,
   beforeAll,
   beforeEach,
   describe,
   expect,
   it,
   spyOn,
} from "bun:test";
import type { components } from "../../api";
import { ConnectionController } from "../../controller/connection.controller";
import { installSettings } from "../../test_helpers/settings_env";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "../../test_helpers/metrics_harness";
import { resetInstrumentsForTesting } from "../../telemetry";
import * as connectionModule from "../connection";
import { ConnectionService } from "../connection_service";
import { destinationOverlap } from "../destination_overlap";
import type { ConnectionAdmin } from "../environment_ports";
import type { EnvironmentStore } from "../environment_store";

/**
 * The per-type decisions the connection controller, connection_service.ts
 * and destination_overlap.ts read from the registry: the type a connection
 * operation is counted under, whether a test refuses an unsafe name, whether
 * raw SQL is held to a single SELECT, which release a deleted connection
 * schedules, and whether one connection's config overlaps a destination's.
 * Every connection type, a prototype name and an unknown one.
 */

type ApiConnection = components["schemas"]["Connection"];

const TYPES = [
   "postgres",
   "bigquery",
   "snowflake",
   "trino",
   "databricks",
   "mysql",
   "sqlserver",
   "duckdb",
   "motherduck",
   "ducklake",
   "publisher",
   "constructor",
   "nope",
];

/** The types whose rules differ from a plain warehouse's. */
const UNSAFE_NAME_REFUSED = new Set(["duckdb", "ducklake"]);
const SELECT_ONLY = new Set(["duckdb", "motherduck", "ducklake"]);
const RELEASE: Record<string, string> = {
   duckdb: "deleteDuckDBConnection(c)",
   ducklake: "deleteDuckLakeConnection(c)",
};
const UNREGISTERED = new Set(["constructor", "nope"]);

function message(error: unknown): unknown {
   return {
      error: error instanceof Error ? error.message : String(error),
      name: error instanceof Error ? error.constructor.name : typeof error,
   };
}

async function outcome(run: () => Promise<unknown>): Promise<unknown> {
   try {
      return { value: (await run()) ?? null };
   } catch (error) {
      return message(error);
   }
}

/** The label a refused create of `type` is counted under. */
async function countedType(
   harness: MetricsHarness,
   type: string,
): Promise<string[]> {
   const controller = new ConnectionController({} as EnvironmentStore);
   await controller
      .addConnection("env", "c", { type } as ApiConnection)
      .catch(() => {});
   const labels: string[] = [];
   for (const label of [type, "unknown"]) {
      const n = await harness.collectCounter(
         "publisher_connection_operations_total",
         { operation: "create", type: label, outcome: "refused" },
      );
      if (n > 0) labels.push(label);
   }
   return labels;
}

/** A connection test of `type` under a name no file may take. */
async function unsafeNameTest(type: string): Promise<unknown> {
   const controller = new ConnectionController({} as EnvironmentStore);
   return outcome(() =>
      controller.testConnectionConfiguration({
         name: "../escape",
         type,
      } as ApiConnection),
   );
}

/** Raw SQL that is no SELECT, on a connection of `type`. */
async function nonSelectSql(type: string): Promise<unknown> {
   const environment = {
      getApiConnection: (name: string) => ({ name, type }),
      getMalloyConnection: async () => {
         throw new Error("lookup reached");
      },
   };
   const controller = new ConnectionController({
      getEnvironment: async () => environment,
   } as unknown as EnvironmentStore);
   return outcome(() =>
      controller.getConnectionSqlSource("env", "c", "DROP TABLE t"),
   );
}

/** The release a deleted connection of `type` schedules, once run. */
async function deleteRelease(type: string): Promise<unknown> {
   const deleted = { name: "c", type } as ApiConnection;
   const calls: string[] = [];
   let release: (() => Promise<void>) | undefined;
   const environment = {
      listApiConnections: () => [deleted],
      getApiConnection: () => deleted,
      updateConnections: (
         _config: unknown,
         _connections: unknown,
         after?: () => Promise<void>,
      ) => {
         release = after;
      },
      deleteDuckDBConnection: async (name: string) => {
         calls.push(`deleteDuckDBConnection(${name})`);
      },
      deleteDuckLakeConnection: async (name: string) => {
         calls.push(`deleteDuckLakeConnection(${name})`);
      },
      metadata: { location: "/env" },
   };
   const repository = {
      getEnvironmentByName: async () => ({ id: "e" }),
      getConnectionByName: async () => ({ id: "c1", name: "c", type }),
      deleteConnection: async () => {},
   };
   const service = new ConnectionService({
      finishedInitialization: Promise.resolve(),
      publisherConfigIsFrozen: false,
      getEnvironment: async () => environment,
      storageManager: { getRepository: () => repository },
   } as unknown as ConnectionAdmin);
   const result = await outcome(() => service.deleteConnection("env", "c"));
   if (release) await release();
   return { result, release: release !== undefined, calls };
}

const LAKE = {
   catalog: {
      postgresConnection: { host: "Cat", port: 5432, databaseName: "m" },
      metadataSchema: "s",
   },
   storage: { bucketUrl: "s3://b/lake/" },
};
const DESTINATIONS: Record<string, unknown> = {
   "same catalog": LAKE,
   "nested path": {
      catalog: { postgresConnection: { databaseName: "other" } },
      storage: { bucketUrl: "s3://b/lake/sub" },
   },
   apart: {
      catalog: { postgresConnection: { databaseName: "other" } },
      storage: { bucketUrl: "s3://c/" },
   },
};

let restoreSettings: () => void;
beforeAll(() => {
   restoreSettings = installSettings().restore;
});
afterAll(() => restoreSettings());

describe("the per-type routing reads the registry", () => {
   let harness: MetricsHarness;
   let testSpy: ReturnType<typeof spyOn>;
   beforeEach(async () => {
      harness = await startMetricsHarness();
      resetInstrumentsForTesting();
      testSpy = spyOn(
         connectionModule,
         "testConnectionConfig",
      ).mockImplementation(async () => ({ status: "ok" }) as never);
   });
   afterEach(async () => {
      testSpy.mockRestore();
      await harness.shutdown();
      resetInstrumentsForTesting();
   });

   it("counts, tests, holds raw SQL and releases each type by its rule", async () => {
      for (const type of TYPES) {
         resetInstrumentsForTesting();
         expect([type, await countedType(harness, type)]).toEqual([
            type,
            [UNREGISTERED.has(type) ? "unknown" : type],
         ]);
         expect([type, await unsafeNameTest(type)]).toEqual([
            type,
            UNSAFE_NAME_REFUSED.has(type)
               ? {
                    error: 'Invalid package name: must be 1-255 characters of letters, digits, "-", "_", or "." and must not start with "."',
                    name: "BadRequestError",
                 }
               : { value: { status: "ok" } },
         ]);
         expect([type, await nonSelectSql(type)]).toEqual([
            type,
            SELECT_ONLY.has(type)
               ? {
                    error: "'c' is a locked-down DuckDB connection: raw SQL on it runs only as a single SELECT, and it is not a SELECT.",
                    name: "BadRequestError",
                 }
               : { error: "lookup reached", name: "Error" },
         ]);
         const released = Object.hasOwn(RELEASE, type);
         expect([type, await deleteRelease(type)]).toEqual([
            type,
            {
               result: { value: null },
               release: released,
               calls: released ? [RELEASE[type]] : [],
            },
         ]);
      }
   }, 120_000);

   it("finds an overlap only between two DuckLake connections", () => {
      const overlap = (
         type: string,
         destinationType: string,
         config: unknown,
      ) =>
         destinationOverlap(
            { name: "c", type, ducklakeConnection: LAKE } as ApiConnection,
            {
               name: "d",
               type: destinationType,
               ducklakeConnection: config,
            } as ApiConnection,
         );
      expect(
         overlap("ducklake", "ducklake", DESTINATIONS["same catalog"]),
      ).toBe("it names the same DuckLake catalog");
      expect(overlap("ducklake", "ducklake", DESTINATIONS["nested path"])).toBe(
         "its data path overlaps the destination's",
      );
      expect(
         overlap("ducklake", "ducklake", DESTINATIONS.apart),
      ).toBeUndefined();
      for (const type of TYPES.filter((t) => t !== "ducklake")) {
         expect(overlap(type, "ducklake", LAKE)).toBeUndefined();
         expect(overlap("ducklake", type, LAKE)).toBeUndefined();
      }
   });
});
