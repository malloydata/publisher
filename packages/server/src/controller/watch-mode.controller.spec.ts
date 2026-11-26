// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The deprecated main-API watch-mode routes: their `{ error }` refusal body,
 * kept until the routes are removed (the admin API serves watch mode now).
 */

import { afterAll, describe, expect, it, spyOn } from "bun:test";
import fs from "fs";
import os from "os";
import path from "path";
import request from "supertest";
import { logger } from "../logger";
import { linesMentioning } from "../test_helpers/log_lines";
import { API_PREFIX, buildRouteApp } from "../test_helpers/route_app";
import {
   ignoredByWatch,
   WatchModeController,
   watchedPackageOf,
} from "./watch-mode.controller";
import { fakeOf } from "../test_helpers/store_fakes";
import type { PackageLookup } from "../service/environment_ports";

const root = fs.mkdtempSync(path.join(os.tmpdir(), "watch-mode-spec-"));
afterAll(() => fs.rmSync(root, { recursive: true, force: true }));

function setup() {
   const controller = new WatchModeController(
      fakeOf<PackageLookup & { serverRootPath: string }>({
         serverRootPath: root,
      }),
   );
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

   it("answer a stop that fails with 500 { error } rather than hang", async () => {
      const { controller, app } = setup();
      controller.watcher = {
         close: async () => {
            throw new Error("watch-stop-marker");
         },
      } as unknown as WatchModeController["watcher"];
      const error = spyOn(logger, "error");
      try {
         const res = await request(app)
            .post(`${API_PREFIX}/watch-mode/stop`)
            .timeout(5000);
         expect(res.status).toBe(500);
         expect(Object.keys(res.body)).toEqual(["error"]);
         expect(res.text).not.toContain("watch-stop-marker");
         expect(linesMentioning(error, "watch-stop-marker")).toHaveLength(1);
      } finally {
         error.mockRestore();
      }
   });

   it("log a refused start once, as a refusal", async () => {
      const warn = spyOn(logger, "warn");
      const error = spyOn(logger, "error");
      try {
         await request(setup().app)
            .post(`${API_PREFIX}/watch-mode/start`)
            .send({ environmentName: "watch-refusal-marker" });
         expect(linesMentioning(warn, "watch-refusal-marker")).toHaveLength(1);
         expect(linesMentioning(error, "watch-refusal-marker")).toEqual([]);
      } finally {
         warn.mockRestore();
         error.mockRestore();
      }
   });
});

describe("which package a watched file belongs to", () => {
   const env = path.join(root, "env");
   const at = (...segments: string[]) => path.join(env, ...segments);

   it("is the first folder under the environment", () => {
      expect(watchedPackageOf(env, at("sales", "model.malloy"))).toBe("sales");
      expect(watchedPackageOf(env, at("sales", "sub", "a.malloy"))).toBe(
         "sales",
      );
   });

   it("is none for a file at the environment root, or outside it", () => {
      expect(watchedPackageOf(env, at("model.malloy"))).toBeNull();
      expect(
         watchedPackageOf(env, path.join(root, "x", "a.malloy")),
      ).toBeNull();
   });

   it("is none under the server's own directories, which an install writes and removes", () => {
      for (const dir of [
         ".retired",
         ".staging",
         ".legacy",
         ".install-records",
         ".storage-destinations",
      ]) {
         expect(
            watchedPackageOf(env, at(dir, "sales-1234", "model.malloy")),
         ).toBeNull();
      }
   });

   it("does not watch a dot-directory at the environment root at all", () => {
      const dir = { isFile: () => false } as fs.Stats;
      const file = { isFile: () => true } as fs.Stats;
      expect(ignoredByWatch(env, at(".retired"))).toBe(true);
      expect(ignoredByWatch(env, at(".retired"), dir)).toBe(true);
      expect(ignoredByWatch(env, at("sales"), dir)).toBe(false);
      // Inside a package, the extension decides, as before.
      expect(ignoredByWatch(env, at("sales", "model.malloy"), file)).toBe(
         false,
      );
      expect(ignoredByWatch(env, at("sales", "notes.bin"), file)).toBe(true);
   });
});

describe("WatchModeController with a real watcher", () => {
   it("reloads the package of a changed model, and never recompiles the server's own .retired folder", async () => {
      const envDir = fs.mkdtempSync(path.join(root, "env-"));
      fs.mkdirSync(path.join(envDir, "sales"));
      fs.mkdirSync(path.join(envDir, ".retired", "x"), { recursive: true });
      const reloaded: string[] = [];
      const environment = {
         getEnvironmentPath: () => envDir,
         getPackage: async (name: unknown) => {
            reloaded.push(String(name));
            return {};
         },
      };
      const controller = new WatchModeController(
         fakeOf<PackageLookup & { serverRootPath: string }>({
            serverRootPath: root,
            getEnvironment: async () => environment,
         }),
      );
      const error = spyOn(logger, "error").mockImplementation(() => logger);
      try {
         await controller.ensureWatching("retired-wiring-marker");
         await new Promise<void>((resolve) =>
            controller.watcher!.once("ready", () => resolve()),
         );
         // The retired file first: were it watched, its event would be
         // delivered before the real package's.
         fs.writeFileSync(
            path.join(envDir, ".retired", "x", "m.malloy"),
            "source: a is duckdb.table('a')\n",
         );
         fs.writeFileSync(
            path.join(envDir, "sales", "m.malloy"),
            "source: a is duckdb.table('a')\n",
         );
         const deadline = Date.now() + 10_000;
         while (reloaded.length === 0 && Date.now() < deadline) {
            await new Promise((resolve) => setTimeout(resolve, 25));
         }
         // Let any event for the retired file settle, bounded.
         await new Promise((resolve) => setTimeout(resolve, 300));
         expect(reloaded).toEqual(["sales"]);
         // A watched .retired would reach reloadPackage, which refuses the
         // name and logs the failure.
         expect(linesMentioning(error, "retired-wiring-marker")).toEqual([]);
      } finally {
         error.mockRestore();
         await controller.watcher?.close();
      }
   });
});
