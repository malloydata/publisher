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
import { installVariables } from "../../../src/test_helpers/settings_env";
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
   let sdkSource: string;

   // The runtime in a sandbox standing in for a page at `search`, recording every URL it fetches.
   function page(search = "") {
      const urls: string[] = [];
      const sandbox: Record<string, unknown> = {
         fetch: (url: string, init?: RequestInit) => {
            urls.push(String(url));
            return fetch(url, init);
         },
         location: {
            pathname: `/environments/${ENV_NAME}/packages/${PKG}/`,
            search,
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
      return {
         publisher: (sandbox.window as { Publisher: Publisher }).Publisher,
         urls,
      };
   }

   let restoreVersioning: (() => void) | undefined;

   beforeAll(async () => {
      // On, so the fixture publishes as the version its publisher.json names (1.0.0) and a page can be pinned to it.
      restoreVersioning = installVariables({
         PUBLISHER_PACKAGE_VERSIONING: "on",
      });
      env = await startRestE2E();
      baseUrl = env.baseUrl;
      const fixtureDir = path.resolve(__dirname, "../../fixtures/query-givens");
      const createRes = await fetch(`${baseUrl}/api/v0/environments`, {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            name: ENV_NAME,
            packages: [],
            connections: [],
         }),
      });
      if (!createRes.ok)
         throw new Error(
            `Failed to create test environment (${createRes.status}): ${await createRes.text()}`,
         );
      // Published, so the package holds the version its publisher.json names.
      const publishRes = await fetch(
         `${baseUrl}/api/v0/environments/${ENV_NAME}/packages`,
         {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ name: PKG, location: fixtureDir }),
         },
      );
      if (!publishRes.ok)
         throw new Error(
            `Failed to publish ${PKG} (${publishRes.status}): ${await publishRes.text()}`,
         );
      const deadline = Date.now() + 30_000;
      while (Date.now() < deadline) {
         const res = await fetch(
            `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${PKG}`,
         );
         if (res.ok) break;
         await new Promise((r) => setTimeout(r, 250));
      }
      sdkSource = await (await fetch(`${baseUrl}/sdk/publisher.js`)).text();
      publisher = page().publisher;
   });

   afterAll(async () => {
      await fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}`, {
         method: "DELETE",
      }).catch(() => {});
      await env?.stop();
      restoreVersioning?.();
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
      const plan = (await publisher.packageResource("build-plan")) as {
         graphs: unknown[];
         sources: unknown;
      };
      expect(Array.isArray(plan.graphs)).toBe(true);
      expect(plan.sources).toBeDefined();
   });

   it("reads the model and the build plan at the version a pinned page was opened at", async () => {
      const pinned = page("?versionId=1.0.0");
      const meta: Record<string, unknown> = {};
      await pinned.publisher.query(
         MODEL,
         "run: orders -> { aggregate: order_count }",
         { meta },
      );
      expect(meta.versionId).toBe("1.0.0");
      await pinned.publisher.model(MODEL);
      await pinned.publisher.packageResource("build-plan");
      const reads = pinned.urls.filter((u) =>
         /\/models\/model\.malloy$|\/models\/model\.malloy\?|\/build-plan/.test(
            u,
         ),
      );
      expect(reads.length).toBe(2);
      for (const u of reads) expect(u).toContain("versionId=1.0.0");
   });

   it("refuses a package resource it does not know", async () => {
      await expect(publisher.packageResource("../models")).rejects.toThrow(
         /not one of/,
      );
   });
});
