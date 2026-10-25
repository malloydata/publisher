// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, describe, expect, it, spyOn } from "bun:test";
import { logger } from "../logger";
import { prepareAndRun } from "./query_execution";
import { buildQueryMetadataInput } from "./query_metadata";
import { emitStatementUsage, withStatementUsage } from "./statement_usage";

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
      const { spy, records } = captureUsage();
      restore = () => spy.mockRestore();
      emitStatementUsage({
         environment: "acme___prod",
         connection: "sales",
         surface: "serve",
         package: "orders",
         durationMs: 12.6,
         rows: 3,
         status: "ok",
      });
      expect(records()).toEqual([
         {
            event: "statement_usage",
            environment: "acme___prod",
            connection: "sales",
            surface: "serve",
            package: "orders",
            duration_ms: 13,
            rows: 3,
            status: "ok",
         },
      ]);
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
            await prepareAndRun(
               runnable(name),
               {},
               {
                  abortSignal: undefined,
                  maxRows: 10,
                  queryMetadata: () => undefined,
                  usage: {
                     metered: (n) => n === "sales",
                     environment: "e",
                     package: "p",
                  },
               },
            );
            expect(records().length).toBe(expected);
            if (expected) {
               expect(records()[0]).toMatchObject({
                  connection: "sales",
                  surface: "serve",
                  rows: 2,
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
});
