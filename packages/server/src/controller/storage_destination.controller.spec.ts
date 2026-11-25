// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { DuckDBInstance } from "@duckdb/node-api";
import { mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { components } from "../api";
import {
   BadRequestError,
   DestinationNotFoundError,
   internalErrorToHttpError,
   StorageDestinationNotFoundError,
} from "../errors";
import { storageDestinationRoot } from "../service/connection_config";
import { StorageDestinationController } from "./storage_destination.controller";
import { fakeOf } from "../test_helpers/store_fakes";
import type { PackageLookup } from "../service/environment_ports";

type ApiConnection = components["schemas"]["Connection"];

describe("StorageDestinationController", () => {
   let envPath: string;
   let controller: StorageDestinationController;
   let declared: ApiConnection;

   beforeEach(() => {
      declared = { name: "lake", type: "duckdb" } as ApiConnection;
      envPath = mkdtempSync(join(tmpdir(), "dest-controller-"));
      mkdirSync(storageDestinationRoot(envPath), { recursive: true });
      const environment = {
         getEnvironmentName: () => "env",
         getEnvironmentPath: () => envPath,
         getStorageDestination: (name: string): ApiConnection => {
            if (name !== "lake") {
               throw new DestinationNotFoundError(
                  `Storage destination ${name} not found`,
               );
            }
            return declared;
         },
      };
      const store = fakeOf<PackageLookup>({
         getEnvironment: async () => environment,
      });
      controller = new StorageDestinationController(store);
   });
   afterEach(() => rmSync(envPath, { recursive: true, force: true }));

   async function seed(sql: string): Promise<void> {
      const instance = await DuckDBInstance.create(
         join(storageDestinationRoot(envPath), "lake.duckdb"),
      );
      const connection = await instance.connect();
      try {
         await connection.run(sql);
      } finally {
         connection.closeSync();
         instance.closeSync();
      }
   }

   it("lists and drops through the named schema", async () => {
      await seed(
         "CREATE TABLE daily__g001 AS SELECT 1 AS x; " +
            "CREATE SCHEMA analytics; " +
            "CREATE TABLE analytics.daily__g001 AS SELECT 1 AS x;",
      );
      expect(await controller.listTables("env", "lake", "main")).toEqual([
         { name: "daily__g001" },
      ]);

      // The schema qualifies the name, so the same table name in `main` is left alone.
      await controller.dropTable("env", "lake", "analytics", "daily__g001");
      expect(await controller.listTables("env", "lake", "analytics")).toEqual(
         [],
      );
      expect(await controller.listTables("env", "lake", "main")).toEqual([
         { name: "daily__g001" },
      ]);
   });

   describe("a destination that builds into a schema other than main", () => {
      beforeEach(async () => {
         declared = {
            name: "lake",
            type: "duckdb",
            ducklakeConnection: { destinationSchema: "materialized" },
         } as ApiConnection;
         await seed(
            "CREATE TABLE orders AS SELECT 1 AS x; " +
               "CREATE SCHEMA materialized; " +
               "CREATE TABLE materialized.orders AS SELECT 1 AS x;",
         );
      });

      it("drops from the schema the path names", async () => {
         await controller.dropTable("env", "lake", "materialized", "orders");
         expect(
            await controller.listTables("env", "lake", "materialized"),
         ).toEqual([]);
         expect(await controller.listTables("env", "lake", "main")).toEqual([
            { name: "orders" },
         ]);
      });

      it("refuses a drop in main, which a caller of an earlier release meant as the build schema, naming the schema to use", async () => {
         for (const schema of ["main", "Main"]) {
            const error = await controller
               .dropTable("env", "lake", schema, "orders")
               .catch((e) => e);
            expect(error).toBeInstanceOf(BadRequestError);
            expect(internalErrorToHttpError(error).status).toBe(400);
            expect(error.message).toContain("'materialized'");
            expect(error.message).toContain("DELETE /catalogs/");
         }
         expect(await controller.listTables("env", "lake", "main")).toEqual([
            { name: "orders" },
         ]);
         expect(
            await controller.listTables("env", "lake", "materialized"),
         ).toEqual([{ name: "orders" }]);
      });
   });

   it("drops in main on a destination that builds into main, however it spells it", async () => {
      for (const destinationSchema of [undefined, "main", "Main"]) {
         declared = {
            name: "lake",
            type: "duckdb",
            ...(destinationSchema
               ? { ducklakeConnection: { destinationSchema } }
               : {}),
         } as ApiConnection;
         for (const schema of ["main", "Main"]) {
            await seed("CREATE OR REPLACE TABLE orders AS SELECT 1 AS x;");
            await controller.dropTable("env", "lake", schema, "orders");
            expect(await controller.listTables("env", "lake", "main")).toEqual(
               [],
            );
         }
      }
   });

   it("answers an unknown destination as a 404, not the build path's 422", async () => {
      const error = await controller
         .listTables("env", "warehouse", "main")
         .catch((e) => e);
      expect(error).toBeInstanceOf(StorageDestinationNotFoundError);
      expect(internalErrorToHttpError(error).status).toBe(404);
   });

   // Every identifier reaches DDL, so anything that could quote out of an
   // identifier or address another schema is refused before any session opens.
   for (const [label, call] of [
      ["table", () => controller.dropTable("env", "lake", "main", 'x"; DROP')],
      [
         "dotted table",
         () => controller.dropTable("env", "lake", "main", "a.b"),
      ],
      ["schema", () => controller.listTables("env", "lake", "main.x")],
      ["destination", () => controller.listTables("env", "la ke", "main")],
   ] as const) {
      it(`refuses an invalid ${label} name with 400`, async () => {
         const error = await call().catch((e) => e);
         expect(error).toBeInstanceOf(BadRequestError);
         expect(internalErrorToHttpError(error).status).toBe(400);
      });
   }

   it("requires both cutoffs as date-times before touching the destination", async () => {
      for (const body of [
         {},
         { snapshotsOlderThan: "2026-01-01T00:00:00Z" },
         {
            snapshotsOlderThan: "yesterday",
            filesOlderThan: "2026-01-01T00:00:00Z",
         },
         { snapshotsOlderThan: 1, filesOlderThan: "2026-01-01T00:00:00Z" },
      ]) {
         const error = await controller
            .createFileCleanup("env", "warehouse", body)
            .catch((e) => e);
         // 400, not 404: the body is checked before the destination is resolved.
         expect(error).toBeInstanceOf(BadRequestError);
      }
   });
});
