// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import path from "path";
import { fileURLToPath } from "url";
import { type RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ENV_NAME = "notebook-givens-scope-env";
const PKG = "notebook-givens-scope";
const NOTEBOOK = "nb.malloynb";

/** The aggregate count out of a notebook cell's nested result encoding. */
function cellCount(body: { result?: string }): number {
   const parsed = JSON.parse(body.result ?? "{}") as {
      data?: {
         array_value?: Array<{
            record_value?: Array<{ number_value?: number }>;
         }>;
      };
   };
   const rows = parsed.data?.array_value ?? [];
   return rows[0]?.record_value?.[0]?.number_value ?? -1;
}

describe("notebook cell given scope (HTTP E2E)", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;

   beforeAll(async () => {
      env = await startRestE2E();
      baseUrl = env.baseUrl;
      const fixtureDir = path.resolve(
         __dirname,
         "../../fixtures/notebook-givens-scope",
      );
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

   const runCell = (cellIndex: number, givens: Record<string, unknown>) =>
      fetch(
         `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${PKG}/notebooks/${NOTEBOOK}/cells/${cellIndex}?givens=${encodeURIComponent(
            JSON.stringify(givens),
         )}`,
      );

   it("pre-import cell runs when handed a given it hasn't declared yet", async () => {
      const res = await runCell(0, { GROUPS: [1] });
      expect(res.status).toBe(200);
      const body = (await res.json()) as { result?: string };
      expect(cellCount(body)).toBe(4);
   });

   it("post-import cell filters by the given it declares", async () => {
      const res = await runCell(1, { GROUPS: [1] });
      expect(res.status).toBe(200);
      const body = (await res.json()) as { result?: string };
      expect(cellCount(body)).toBe(2);
   });

   it("a typo'd given still 400s on the pre-import cell", async () => {
      const res = await runCell(0, { NOtaGiven: 1 });
      expect(res.status).toBe(400);
   });
});
