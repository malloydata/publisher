// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * The browser runtime (`packages/server/src/runtime/publisher.js`, served at
 * GET /sdk/publisher.js) hands a page what a query's response says about its
 * answer through `opts.meta`, and reads the package's model and build plan
 * at the page's own version. Evaluated in a vm sandbox standing in for
 * `window`, against the real HTTP server, as sdk_givens does.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import path from "path";
import { fileURLToPath } from "url";
import vm from "vm";
import { type RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ENV_NAME = "sdk-meta-env";
const PKG = "query-givens";
const MODEL = "model.malloy";

type Publisher = {
   query: (
      m: string,
      q: string | undefined,
      o: Record<string, unknown>,
   ) => Promise<unknown[]>;
   model: (
      m: string,
      o?: Record<string, unknown>,
   ) => Promise<Record<string, unknown>>;
   packageResource: (
      n: string,
      o?: Record<string, unknown>,
   ) => Promise<unknown>;
};

describe("Publisher runtime: answer metadata and package reads (sandbox-eval HTTP E2E)", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;
   let publisher: Publisher;

   beforeAll(async () => {
      env = await startRestE2E();
      baseUrl = env.baseUrl;
      const fixtureDir = path.resolve(__dirname, "../../fixtures/query-givens");
      const createRes = await fetch(`${baseUrl}/api/v0/environments`, {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            name: ENV_NAME,
            packages: [{ name: PKG, location: fixtureDir }],
            connections: [],
         }),
      });
      if (!createRes.ok)
         throw new Error(
            `Failed to create test environment (${createRes.status}): ${await createRes.text()}`,
         );
      const deadline = Date.now() + 30_000;
      while (Date.now() < deadline) {
         const res = await fetch(
            `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${PKG}`,
         );
         if (res.ok) break;
         await new Promise((r) => setTimeout(r, 250));
      }
      const sdkSource = await (
         await fetch(`${baseUrl}/sdk/publisher.js`)
      ).text();
      const sandbox: Record<string, unknown> = {
         fetch,
         location: {
            pathname: `/environments/${ENV_NAME}/packages/${PKG}/`,
            origin: baseUrl,
         },
         console,
      };
      sandbox.self = sandbox;
      sandbox.top = sandbox;
      sandbox.window = sandbox;
      vm.runInContext(sdkSource, vm.createContext(sandbox), {
         filename: "publisher.js",
      });
      publisher = (sandbox.window as { Publisher: Publisher }).Publisher;
   });

   afterAll(async () => {
      await fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}`, {
         method: "DELETE",
      }).catch(() => {});
      await env?.stop();
   });

   it("fills opts.meta with what the response says about the answer, and still returns the rows", async () => {
      const meta: Record<string, unknown> = {};
      const rows = await publisher.query(
         MODEL,
         "run: orders -> { aggregate: order_count }",
         { meta },
      );
      expect(rows.length).toBe(1);
      expect(Object.keys(meta).sort()).toEqual([
         "executionTimeMs",
         "path",
         "servedFrom",
         "versionId",
      ]);
      expect(typeof meta.executionTimeMs).toBe("number");
   });

   it("leaves a query without meta as it was", async () => {
      const rows = await publisher.query(
         MODEL,
         "run: orders -> { aggregate: order_count }",
         {},
      );
      expect(rows.length).toBe(1);
   });

   it("reads the model's sources and the package's build plan", async () => {
      const model = await publisher.model(MODEL);
      expect(JSON.stringify(model)).toContain("orders");
      const plan = await publisher.packageResource("build-plan");
      expect(plan).toBeDefined();
   });

   it("refuses a package resource it does not know", async () => {
      await expect(publisher.packageResource("../models")).rejects.toThrow(
         /not one of/,
      );
   });
});
