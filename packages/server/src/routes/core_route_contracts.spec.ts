// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Status/body contracts of core routes over the real route table and error
 * mapping, with the real controller wherever its own logic is under test and
 * a stub only where it would reach storage or a database.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import request from "supertest";
import { ConnectionController } from "../controller/connection.controller";
import { ReportController } from "../controller/report.controller";
import type { EnvironmentStore } from "../service/environment_store";
import {
   startClosingListener,
   type ClosingListener,
} from "../test_helpers/closing_listener";
import { API_PREFIX, buildRouteApp } from "../test_helpers/route_app";
import { installSettings } from "../test_helpers/settings_env";

describe("PUT …/models/{path} (report write)", () => {
   it("answers 403 under frozenConfig", async () => {
      const reportController = new ReportController({
         publisherConfigIsFrozen: true,
      } as unknown as EnvironmentStore);
      const res = await request(buildRouteApp({ reportController }))
         .put(`${API_PREFIX}/environments/env/packages/pkg/models/r.malloy`)
         .send({ source: "## artifact { layout=grid }\n" });
      expect(res.status).toBe(403);
      expect(res.body.message).toContain("frozenConfig");
   });
});

describe("POST /connections/test", () => {
   const SECRET = "hunter2-route-secret";
   let unreachable: ClosingListener;
   let restoreSettings: () => void;
   beforeAll(async () => {
      unreachable = await startClosingListener();
      // A DuckDB connection needs setupSQL or an attachment to be valid at all.
      restoreSettings = installSettings({
         env: { PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true" },
      }).restore;
   });
   afterAll(async () => {
      await unreachable.close();
      restoreSettings();
   });

   const test = (body: unknown) =>
      request(
         buildRouteApp({
            connectionController: new ConnectionController(
               {} as unknown as EnvironmentStore,
            ),
         }),
      )
         .post(`${API_PREFIX}/connections/test`)
         .send(body as object);

   const DUCKDB = {
      name: "probe",
      type: "duckdb",
      duckdbConnection: { setupSQL: "SELECT 1" },
   };

   it.each([
      ["bare", DUCKDB],
      ["wrapped in { config }", { config: DUCKDB }],
   ])("answers a valid DuckDB config (%s) with ok", async (_, body) => {
      const res = await test(body);
      expect(res.status).toBe(200);
      expect(res.body.status).toBe("ok");
   });

   it.each([
      ["an empty body", {}],
      ["a missing type", { name: "probe" }],
      ["an unsafe duckdb name", { name: "../evil", type: "duckdb" }],
   ])("answers %s with 400", async (_, body) => {
      expect((await test(body)).status).toBe(400);
   });

   it("answers a connection that fails as failed, without echoing its secret", async () => {
      const res = await test({
         name: "probe_pg",
         type: "postgres",
         postgresConnection: {
            host: "127.0.0.1",
            port: unreachable.port,
            userName: "alice",
            password: SECRET,
            databaseName: "db",
         },
      });
      expect(res.status).toBe(200);
      expect(res.body.status).toBe("failed");
      expect(res.body.errorMessage).toBeTruthy();
      expect(res.text).not.toContain(SECRET);
   }, 30_000);
});

describe("GET /status", () => {
   const STATUS = {
      operationalState: "serving",
      version: "9.9.9",
      environments: [{ name: "env" }],
   };
   const app = (storageReady: boolean, calls: unknown[] = []) =>
      buildRouteApp({
         environmentStore: {
            getStatus: async (options: unknown) => {
               calls.push(options);
               return STATUS;
            },
            storageManager: { isInitialized: () => storageReady },
         } as unknown as EnvironmentStore,
         themeStore: { get: async () => ({ defaultMode: "dark" }) },
      });

   it.each([
      ["before storage is ready, an empty theme", false, {}],
      ["once storage is ready, the saved theme", true, { defaultMode: "dark" }],
   ])("carries %s", async (_, ready, theme) => {
      const res = await request(app(ready)).get(`${API_PREFIX}/status`);
      expect(res.status).toBe(200);
      expect(res.body).toEqual({ ...STATUS, theme });
   });

   it("lists loading packages only for includeLoading=true", async () => {
      const calls: unknown[] = [];
      for (const q of ["", "?includeLoading=true", "?includeLoading=1"]) {
         await request(app(false, calls)).get(`${API_PREFIX}/status${q}`);
      }
      expect(calls).toEqual([
         { includeLoading: false },
         { includeLoading: true },
         { includeLoading: false },
      ]);
   });
});
