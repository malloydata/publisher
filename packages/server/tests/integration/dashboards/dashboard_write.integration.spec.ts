// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * `PUT …/models/dashboards/{slug}.malloy` over real HTTP.
 *
 * The write path had unit coverage at the controller (against a stubbed
 * environment) and at the service (against a real directory), and a browser
 * test that drives the builder. Nothing exercised the endpoint itself: the
 * route, the status codes, the JSON bodies, and the fact that a written file
 * is actually being served afterwards. Those are the contract external callers
 * and the skills depend on, and all of them were assertions nobody had made.
 *
 * Runs against a copy of the dashboards fixture, because these tests write to
 * the package and the fixture in the repository is not the place for that.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { createHash } from "crypto";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { fileURLToPath } from "url";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const FIXTURE = path.resolve(__dirname, "../../fixtures/dashboards-test");
const ENV_NAME = "dashboard-write-env";
const PKG = "dashboards-test";

const hashOf = (text: string) =>
   createHash("sha256").update(text, "utf8").digest("hex");

/** A dashboard that compiles against the fixture's `orders` model. */
const dashboardSource = (title: string) => `##! experimental.givens
import { orders } from '../orders.malloy'

#" Written by the write-path integration test.
# artifact { title="${title}" } dashboard {columns=12}
query: written is orders -> {
   aggregate:
      # label="Orders"
      # colspan=12
      order_count
}
`;

describe("PUT model source: dashboards", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;
   let location: string;
   /** The hash of the text currently on disk, as the last write reported it. */
   let currentHash: string;

   const modelsUrl = (modelPath: string) =>
      `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${PKG}/models/${modelPath}`;

   /** A dashboard's title as the package currently serves it. */
   const titleOf = async (name: string): Promise<string> => {
      const res = await fetch(
         `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${PKG}/dashboards/${name}`,
      );
      expect(res.status).toBe(200);
      return ((await res.json()) as { title?: string }).title ?? "";
   };

   const put = (modelPath: string, body: unknown) =>
      fetch(modelsUrl(modelPath), {
         method: "PUT",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify(body),
      });

   beforeAll(async () => {
      env = await startRestE2E();
      baseUrl = env.baseUrl;
      location = await fs.mkdtemp(
         path.join(os.tmpdir(), "publisher-write-e2e-"),
      );
      await fs.cp(FIXTURE, location, { recursive: true });

      const created = await fetch(`${baseUrl}/api/v0/environments`, {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            name: ENV_NAME,
            packages: [{ name: PKG, location }],
            connections: [],
         }),
      });
      if (!created.ok) {
         throw new Error(
            `Failed to create test environment (${created.status}): ${await created.text()}`,
         );
      }
      const deadline = Date.now() + 30_000;
      while (Date.now() < deadline) {
         try {
            const res = await fetch(
               `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${PKG}`,
            );
            if (res.ok) break;
         } catch {
            // not ready yet
         }
         await new Promise((r) => setTimeout(r, 500));
      }
   });

   afterAll(async () => {
      if (baseUrl) {
         await fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}`, {
            method: "DELETE",
         }).catch(() => undefined);
      }
      await env?.stop();
      env = null;
      if (location) await fs.rm(location, { recursive: true, force: true });
   });

   it("creates a new dashboard with 201, and serves it immediately", async () => {
      const source = dashboardSource("Created");
      const res = await put("dashboards/created.malloy", { source });
      expect(res.status).toBe(201);

      const body = (await res.json()) as {
         resource: string;
         path: string;
         contentHash: string;
         created: boolean;
      };
      expect(body.created).toBe(true);
      expect(body.path).toBe("dashboards/created.malloy");
      expect(body.contentHash).toBe(hashOf(source));
      currentHash = body.contentHash;
      expect(body.resource).toBe(
         `/api/v0/environments/${ENV_NAME}/packages/${PKG}/models/dashboards/created.malloy`,
      );

      // The point of reloading in place: the dashboard is discoverable and
      // running without anyone restarting or reloading the package.
      const listed = await fetch(
         `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${PKG}/dashboards`,
      );
      expect(listed.status).toBe(200);
      const names = ((await listed.json()) as Array<{ name?: string }>).map(
         (d) => d.name,
      );
      expect(names).toContain("created");
   });

   it("refuses to create over an existing file with 409, and changes nothing", async () => {
      const res = await put("dashboards/created.malloy", {
         source: dashboardSource("Should not land"),
      });
      expect(res.status).toBe(409);
      expect((await res.text()).toLowerCase()).toContain("already exists");
      expect(await titleOf("created")).toBe("Created");
   });

   it("replaces with 200 when the hash matches what the caller opened", async () => {
      // The hash the create handed back IS what the caller opened: a client
      // saves, keeps the hash, and saves again without re-reading.
      const next = dashboardSource("Replaced");
      const res = await put("dashboards/created.malloy", {
         source: next,
         expectedHash: currentHash,
      });
      expect(res.status).toBe(200);
      const body = (await res.json()) as {
         created: boolean;
         contentHash: string;
      };
      expect(body.created).toBe(false);
      expect(body.contentHash).toBe(hashOf(next));
      currentHash = body.contentHash;

      expect(await titleOf("created")).toBe("Replaced");
   });

   it("refuses a stale hash with 409, without merging", async () => {
      const res = await put("dashboards/created.malloy", {
         source: dashboardSource("From a stale read"),
         expectedHash: hashOf("text that was never on disk"),
      });
      expect(res.status).toBe(409);
      expect((await res.text()).toLowerCase()).toContain("changed");
      // Still the text the previous test wrote.
      expect(await titleOf("created")).toBe("Replaced");
   });

   it("refuses source that does not compile with 400, naming where", async () => {
      const res = await put("dashboards/broken.malloy", {
         source: `##! experimental.givens
import { orders } from '../orders.malloy'

# artifact { title="Broken" } dashboard {columns=12}
query: broken is orders -> { aggregate: no_such_measure }
`,
      });
      expect(res.status).toBe(400);
      const text = await res.text();
      expect(text).toContain("does not compile");
      // A caller fixing this needs a coordinate, not just a complaint.
      expect(text).toMatch(/line \d+:\d+/);

      // Nothing was written, so the package has no such dashboard to serve.
      const listed = await fetch(
         `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${PKG}/dashboards`,
      );
      const names = ((await listed.json()) as Array<{ name?: string }>).map(
         (d) => d.name,
      );
      expect(names).not.toContain("broken");
   });

   it("writes only dashboard files, refusing anything else with 400", async () => {
      for (const badPath of [
         "orders.malloy",
         "dashboards/nested/deep.malloy",
         "dashboards/notebook.malloynb",
      ]) {
         const res = await put(badPath, { source: dashboardSource("Nope") });
         expect(res.status).toBe(400);
      }
   });

   it("refuses a body with no source, with 400", async () => {
      const res = await put("dashboards/nobody.malloy", { notSource: "x" });
      expect(res.status).toBe(400);
      expect(await res.text()).toContain("source");
   });

   it("404s for a package that does not exist", async () => {
      const res = await fetch(
         `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/no-such-package/models/dashboards/x.malloy`,
         {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ source: dashboardSource("Nope") }),
         },
      );
      expect(res.status).toBe(404);
   });
});
