// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterEach, describe, expect, it, spyOn } from "bun:test";
import { hostname } from "node:os";
import { logger } from "../logger";
import { installSettings } from "../test_helpers/settings_env";
import { prepareAndRun } from "./query_execution";
import { buildQueryMetadataInput } from "./query_metadata";
import {
   emitStatementUsage,
   meterStatement,
   responseBytes,
   usageCatalog,
   withStatementUsage,
} from "./statement_usage";

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

function captureUsage() {
   const spy = spyOn(logger, "info");
   const records = () =>
      (spy.mock.calls as unknown as [unknown, Record<string, unknown>][])
         .filter((c) => c[0] === "statement_usage")
         .map((c) => c[1]);
   return { spy, records };
}

describe("statement usage records", () => {
   let restore: (() => void) | undefined;
   afterEach(() => restore?.());

   it("emits the stable shape a collector maps", () => {
      const settings = installSettings({
         env: { PUBLISHER_DUCKDB_THREADS: "3" },
      });
      const { spy, records } = captureUsage();
      restore = () => {
         spy.mockRestore();
         settings.restore();
      };
      emitStatementUsage({
         environment: "acme___prod",
         connection: "sales",
         catalog: "sales_catalog",
         surface: "serve",
         package: "orders",
         durationMs: 12.6,
         rows: 3,
         status: "ok",
      });
      // The key names are the contract: a collector reads them by name.
      expect(records()).toEqual([
         {
            event: "statement_usage",
            statement_id: expect.stringMatching(UUID),
            environment: "acme___prod",
            connection: "sales",
            catalog: "sales_catalog",
            surface: "serve",
            package: "orders",
            duration_ms: 13,
            rows: 3,
            status: "ok",
            threads: 3,
            worker: hostname(),
         },
      ]);
   });

   it("leaves out what a statement does not have", () => {
      const settings = installSettings({
         env: { PUBLISHER_DUCKDB_THREADS: "off" },
      });
      const { spy, records } = captureUsage();
      restore = () => {
         spy.mockRestore();
         settings.restore();
      };
      emitStatementUsage({
         environment: "",
         connection: "warehouse",
         surface: "sql",
         durationMs: 1,
         status: "error",
      });
      expect(Object.keys(records()[0]).sort()).toEqual([
         "connection",
         "duration_ms",
         "environment",
         "event",
         "statement_id",
         "status",
         "surface",
         "worker",
      ]);
   });

   it("leaves threads out when the thread cap cannot be installed, whatever the setting says", () => {
      const settings = installSettings({
         env: { PUBLISHER_DUCKDB_THREADS: "3" },
      });
      // What a db-duckdb release that reshaped its internals looks like:
      // duckdb_instance_sharing.ts then skips the cap, and an instance opens
      // one thread per core.
      const proto = DuckDBConnection.prototype as unknown as Record<
         string,
         unknown
      >;
      const setupOnce = proto["setupOnce"];
      const { spy, records } = captureUsage();
      restore = () => {
         proto["setupOnce"] = setupOnce;
         spy.mockRestore();
         settings.restore();
      };
      const usage = {
         environment: "acme___prod",
         connection: "sales",
         surface: "serve" as const,
         durationMs: 1,
         status: "ok" as const,
      };

      emitStatementUsage(usage);
      proto["setupOnce"] = undefined;
      emitStatementUsage(usage);

      expect(records()[0].threads).toBe(3);
      expect(records()[1]).not.toHaveProperty("threads");
   });

   it("gives every record a statement_id of its own", () => {
      const { spy, records } = captureUsage();
      restore = () => spy.mockRestore();
      const usage = {
         environment: "e",
         connection: "c",
         surface: "serve",
         durationMs: 1,
         status: "ok",
      } as const;
      emitStatementUsage(usage);
      emitStatementUsage(usage);
      const ids = records().map((r) => r.statement_id);
      expect(ids).toHaveLength(2);
      expect(ids[0]).not.toBe(ids[1]);
   });

   it("records a failure and rethrows it unchanged", async () => {
      const { spy, records } = captureUsage();
      restore = () => spy.mockRestore();
      const boom = new Error("boom");
      await expect(
         withStatementUsage(
            { environment: "e", connection: "c", surface: "load" },
            async () => {
               throw boom;
            },
         ),
      ).rejects.toBe(boom);
      expect(records()[0].status).toBe("error");
   });

   it("returns the result when reading its row count throws, and records no rows", async () => {
      const { spy, records } = captureUsage();
      restore = () => spy.mockRestore();
      const result = await withStatementUsage(
         { environment: "e", connection: "c", surface: "serve" },
         async () => "answer",
         () => {
            throw new Error("no rowCount here");
         },
      );
      expect(result).toBe("answer");
      expect(records()).toHaveLength(1);
      expect(records()[0].status).toBe("ok");
      expect(records()[0].rows).toBeUndefined();
   });
});

describe("usageCatalog", () => {
   it("is a DuckLake connection's metadata schema, and nothing for any other", () => {
      expect(
         usageCatalog({
            ducklakeConnection: { catalog: { metadataSchema: "db_1a2b" } },
         }),
      ).toBe("db_1a2b");
      expect(usageCatalog({ ducklakeConnection: { catalog: {} } })).toBe(
         undefined,
      );
      expect(usageCatalog({ ducklakeConnection: {} })).toBe(undefined);
      expect(usageCatalog({})).toBe(undefined);
      expect(usageCatalog(undefined)).toBe(undefined);
   });
});

describe("which connections are metered", () => {
   const input = (envImpl: object) =>
      buildQueryMetadataInput({
         environmentName: "acme___prod",
         environment: envImpl as never,
         pkg: { getDeclaredQueryMetadata: () => null },
         noLayersLogMessage: "x",
      });

   it("meters a managed connection and a storage destination, nothing else", () => {
      const metered = input({
         getApiConnection: (name: string) => {
            if (name === "sales") return { managed: true };
            if (name === "warehouse") return {};
            throw new Error("not a connection");
         },
         getStorageDestination: (name: string) => {
            if (name !== "lake") throw new Error("not a destination");
            return {};
         },
      }).meteredConnection!;
      expect(metered("sales")).toBe(true);
      expect(metered("lake")).toBe(true);
      expect(metered("warehouse")).toBe(false);
      expect(metered("unknown")).toBe(false);
   });

   it("names the catalog of a managed connection and of a storage destination", () => {
      const ducklake = (metadataSchema: string) => ({
         ducklakeConnection: { catalog: { metadataSchema } },
      });
      const catalog = input({
         getApiConnection: (name: string) => {
            if (name === "sales") return { managed: true, ...ducklake("db_a") };
            // A customer's own DuckLake connection is not metered, so its
            // catalog is not the record's.
            if (name === "warehouse") return ducklake("db_w");
            throw new Error("not a connection");
         },
         getStorageDestination: (name: string) => {
            if (name !== "lake") throw new Error("not a destination");
            return ducklake("db_b");
         },
      }).meteredCatalog!;
      expect(catalog("sales")).toBe("db_a");
      expect(catalog("lake")).toBe("db_b");
      expect(catalog("warehouse")).toBe(undefined);
      expect(catalog("unknown")).toBe(undefined);
   });
});

describe("bytes_out and a record that waits for its response", () => {
   let restore: (() => void) | undefined;
   afterEach(() => restore?.());
   const base = {
      environment: "acme___prod",
      connection: "sales",
      surface: "serve" as const,
   };

   it("counts a response's UTF-8 bytes, not its characters", () => {
      expect(responseBytes('{"a":"x"}')).toBe(9);
      expect(responseBytes('{"a":"é"}')).toBe(10);
   });

   it("emits bytes_out, rounded, when a response size is given, and leaves it out when none is", () => {
      const { spy, records } = captureUsage();
      restore = () => spy.mockRestore();
      emitStatementUsage({
         ...base,
         durationMs: 1,
         bytesOut: 10.4,
         status: "ok",
      });
      emitStatementUsage({ ...base, durationMs: 1, status: "ok" });
      expect(records()[0].bytes_out).toBe(10);
      expect(records()[1]).not.toHaveProperty("bytes_out");
   });

   it("a statement that answered emits one record, with the size its caller gives, once", async () => {
      const { spy, records } = captureUsage();
      restore = () => spy.mockRestore();
      const { result, usage } = await meterStatement(
         base,
         async () => ({ rows: [1, 2, 3] }),
         (r) => r.rows.length,
      );
      expect(result.rows).toHaveLength(3);
      expect(records()).toEqual([]);
      usage.emit(responseBytes(JSON.stringify(result)));
      usage.emit();
      expect(records()).toHaveLength(1);
      expect(records()[0]).toMatchObject({
         rows: 3,
         bytes_out: 16,
         status: "ok",
      });
   });

   it("the duration is the statement's, not the time until its record is emitted", async () => {
      const { spy, records } = captureUsage();
      restore = () => spy.mockRestore();
      const { usage } = await meterStatement(base, async () => ({}));
      await new Promise((resolve) => setTimeout(resolve, 60));
      usage.emit(5);
      expect(records()[0].duration_ms as number).toBeLessThan(50);
   });

   it("a statement that fails emits its record at once, with no bytes_out, and rethrows", async () => {
      const { spy, records } = captureUsage();
      restore = () => spy.mockRestore();
      await expect(
         meterStatement(base, async () => {
            throw new Error("boom");
         }),
      ).rejects.toThrow("boom");
      expect(records()).toHaveLength(1);
      expect(records()[0]).toMatchObject({ status: "error" });
      expect(records()[0]).not.toHaveProperty("bytes_out");
   });

   it("withStatementUsage emits at once, with no bytes_out: a build, load or copy answers no rows", async () => {
      const { spy, records } = captureUsage();
      restore = () => spy.mockRestore();
      await withStatementUsage({ ...base, surface: "build" }, async () => 1);
      expect(records()).toHaveLength(1);
      expect(records()[0]).not.toHaveProperty("bytes_out");
   });
});

describe("prepareAndRun emits usage only for a metered connection", () => {
   const runnable = (connectionName: string) =>
      ({
         getPreparedResult: async () => ({
            connectionName,
            resultExplore: { limit: undefined },
         }),
         run: async () => ({ data: { rowCount: 2 } }),
      }) as never;

   for (const [name, expected] of [
      ["sales", 1],
      ["warehouse", 0],
   ] as const) {
      it(`${name}: ${expected} record`, async () => {
         const { spy, records } = captureUsage();
         try {
            const run = await prepareAndRun(
               runnable(name),
               {},
               {
                  abortSignal: undefined,
                  maxRows: 10,
                  queryMetadata: () => undefined,
                  usage: {
                     metered: (n) => n === "sales",
                     catalog: (n) => `db_${n}`,
                     environment: "e",
                     package: "p",
                  },
               },
            );
            // The record waits for the response's size: nothing yet.
            expect(records()).toEqual([]);
            expect(run.usage !== undefined).toBe(expected === 1);
            run.usage?.emit(42);
            run.usage?.emit(7);
            expect(records().length).toBe(expected);
            if (expected) {
               expect(records()[0]).toMatchObject({
                  connection: "sales",
                  catalog: "db_sales",
                  surface: "serve",
                  rows: 2,
                  bytes_out: 42,
               });
            }
         } finally {
            spy.mockRestore();
         }
      });
   }

   it("a metering check that throws never fails the query", async () => {
      const result = await prepareAndRun(
         runnable("sales"),
         {},
         {
            abortSignal: undefined,
            maxRows: 10,
            queryMetadata: () => undefined,
            usage: {
               metered: () => {
                  throw new Error("lookup failed");
               },
               environment: "e",
            },
         },
      );
      expect(result.result).toEqual({ data: { rowCount: 2 } } as never);
   });

   it("a catalog lookup that throws still records the statement", async () => {
      const { spy, records } = captureUsage();
      try {
         const run = await prepareAndRun(
            runnable("sales"),
            {},
            {
               abortSignal: undefined,
               maxRows: 10,
               queryMetadata: () => undefined,
               usage: {
                  metered: () => true,
                  catalog: () => {
                     throw new Error("lookup failed");
                  },
                  environment: "e",
               },
            },
         );
         run.usage?.emit();
         expect(records()).toHaveLength(1);
         expect(records()[0].catalog).toBeUndefined();
      } finally {
         spy.mockRestore();
      }
   });
});
