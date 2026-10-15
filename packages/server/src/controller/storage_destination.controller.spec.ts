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
import type { EnvironmentStore } from "../service/environment_store";
import { StorageDestinationController } from "./storage_destination.controller";

type ApiConnection = components["schemas"]["Connection"];

describe("StorageDestinationController", () => {
   let envPath: string;
   let controller: StorageDestinationController;

   beforeEach(() => {
      envPath = mkdtempSync(join(tmpdir(), "dest-controller-"));
      mkdirSync(storageDestinationRoot(envPath), { recursive: true });
      const environment = {
         getEnvironmentPath: () => envPath,
         getStorageDestination: (name: string): ApiConnection => {
            if (name !== "lake") {
               throw new DestinationNotFoundError(
                  `Storage destination ${name} not found`,
               );
            }
            return { name: "lake", type: "duckdb" } as ApiConnection;
         },
      };
      const store = {
         getEnvironment: async () => environment,
      } as unknown as EnvironmentStore;
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

      // A non-`main` schema qualifies the name, so the same table name in `main`
      // is left alone.
      await controller.dropTable("env", "lake", "analytics", "daily__g001");
      expect(await controller.listTables("env", "lake", "analytics")).toEqual(
         [],
      );
      expect(await controller.listTables("env", "lake", "main")).toEqual([
         { name: "daily__g001" },
      ]);
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
