/// <reference types="bun-types" />

/**
 * Proves the browser SDK (`packages/server/src/runtime/publisher.js`, served
 * at GET /sdk/publisher.js) actually puts `givens` on the wire: fetch the
 * served source, evaluate it in a sandboxed vm context standing in for
 * `window` (self === top, so the in-iframe/live-reload branches are
 * inert no-ops), and call `window.Publisher.query(...)` with a `givens`
 * override against the real HTTP server. Approach used: sandbox-eval via
 * Node's `vm` module — evaluating the actual runtime rather than just
 * grepping its source proves the SDK's fetch body really carries `givens`
 * end-to-end.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import path from "path";
import { fileURLToPath } from "url";
import vm from "vm";
import { type RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ENV_NAME = "sdk-givens-env";
const PKG = "query-givens";
const MODEL = "model.malloy";

type Row = Record<string, unknown>;

describe("Publisher SDK forwards givens (sandbox-eval HTTP E2E)", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;

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
      if (!createRes.ok) {
         throw new Error(
            `Failed to create test environment (${createRes.status}): ${await createRes.text()}`,
         );
      }
      const deadline = Date.now() + 30_000;
      while (Date.now() < deadline) {
         const res = await fetch(
            `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${PKG}`,
         );
         if (res.ok) break;
         await new Promise((r) => setTimeout(r, 250));
      }
   });

   afterAll(async () => {
      await fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}`, {
         method: "DELETE",
      }).catch(() => {});
      await env?.stop();
   });

   it("Publisher.query() with a givens override reaches the server and retargets rows", async () => {
      const sdkRes = await fetch(`${baseUrl}/sdk/publisher.js`);
      expect(sdkRes.status).toBe(200);
      const sdkSource = await sdkRes.text();

      // Sandbox stands in for `window`. self === top so the runtime's
      // in-iframe (postMessage resize) and live-reload (EventSource) branches
      // are no-ops; document/EventSource are omitted since that code path
      // never runs for a non-embedded page.
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

      const context = vm.createContext(sandbox);
      vm.runInContext(sdkSource, context, { filename: "publisher.js" });

      const publisher = (sandbox.window as { Publisher: unknown })
         .Publisher as {
         query: (
            modelPath: string,
            malloyQuery: string | undefined,
            opts: Record<string, unknown>,
         ) => Promise<Row[]>;
      };
      expect(publisher).toBeDefined();

      const rows = await publisher.query(MODEL, undefined, {
         sourceName: "orders",
         queryName: "by_given_region",
         givens: { target_region: "EU" },
      });

      expect(rows.length).toBe(1);
      expect(Number(rows[0].order_count)).toBe(3);
   });
});
