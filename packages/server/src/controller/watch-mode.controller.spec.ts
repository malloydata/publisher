// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The deprecated main-API watch-mode routes: their `{ error }` refusal body,
 * kept until the routes are removed (the admin API serves watch mode now).
 */

import { afterAll, describe, expect, it } from "bun:test";
import fs from "fs";
import os from "os";
import path from "path";
import request from "supertest";
import type { EnvironmentStore } from "../service/environment_store";
import { API_PREFIX, buildRouteApp } from "../test_helpers/route_app";
import { WatchModeController } from "./watch-mode.controller";

const root = fs.mkdtempSync(path.join(os.tmpdir(), "watch-mode-spec-"));
afterAll(() => fs.rmSync(root, { recursive: true, force: true }));

function setup() {
   const controller = new WatchModeController({
      serverRootPath: root,
   } as unknown as EnvironmentStore);
   return {
      controller,
      app: buildRouteApp({ watchModeController: controller }),
   };
}

describe("deprecated watch-mode routes", () => {
   it("report the status, marked deprecated", async () => {
      const res = await request(setup().app).get(
         `${API_PREFIX}/watch-mode/status`,
      );
      expect(res.status).toBe(200);
      expect(res.headers.deprecation).toBe("true");
      expect(res.body).toEqual({
         enabled: false,
         watchingPath: "",
         environmentName: "",
      });
   });

   it("refuse an environment that is not configured with 404 { error }", async () => {
      const res = await request(setup().app)
         .post(`${API_PREFIX}/watch-mode/start`)
         .send({ environmentName: "nowhere" });
      expect(res.status).toBe(404);
      expect(res.body).toEqual({
         error: "Environment nowhere not found or has no packages",
      });
   });

   it("refuse an unsafe name with 400 { error }", async () => {
      const res = await request(setup().app)
         .post(`${API_PREFIX}/watch-mode/start`)
         .send({ environmentName: "../up" });
      expect(res.status).toBe(400);
      expect(Object.keys(res.body)).toEqual(["error"]);
   });

   it("stop idempotently", async () => {
      const res = await request(setup().app).post(
         `${API_PREFIX}/watch-mode/stop`,
      );
      expect(res.status).toBe(200);
   });
});
