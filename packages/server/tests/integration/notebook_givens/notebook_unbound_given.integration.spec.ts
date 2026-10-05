// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import path from "path";
import { fileURLToPath } from "url";
import { type RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ENV_NAME = "notebook-unbound-given-env";
const PKG = "notebook-unbound-given";
const NOTEBOOK = "notebooks/nb.malloy";

/** The aggregate count out of a notebook cell's nested result encoding. */
function cellCount(body: { result?: string }): number {
   const parsed = JSON.parse(body.result ?? "{}") as {
      data?: {
         array_value?: Array<{
            record_value?: Array<{ number_value?: number }>;
         }>;
      };
   };
   return parsed.data?.array_value?.[0]?.record_value?.[0]?.number_value ?? -1;
}

describe("a notebook cell reading a given with no default (HTTP E2E)", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;
   const pkgUrl = (sub: string) =>
      `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${PKG}${sub}`;

   beforeAll(async () => {
      env = await startRestE2E();
      baseUrl = env.baseUrl;
      const fixtureDir = path.resolve(
         __dirname,
         "../../fixtures/notebook-unbound-given",
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
      let loaded = false;
      while (Date.now() < deadline) {
         if ((await fetch(pkgUrl(""))).ok) {
            loaded = true;
            break;
         }
         await new Promise((r) => setTimeout(r, 250));
      }
      if (!loaded) throw new Error(`package ${PKG} never loaded within 30s`);
   });

   afterAll(async () => {
      await fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}`, {
         method: "DELETE",
      }).catch(() => {});
      await env?.stop();
   });

   async function queryCellIndex(): Promise<number> {
      const res = await fetch(pkgUrl(`/notebooks/${NOTEBOOK}`));
      expect(res.status).toBe(200);
      const body = (await res.json()) as {
         notebookCells?: Array<{ kind?: string }>;
      };
      const index = (body.notebookCells ?? []).findIndex(
         (c) => c.kind === "query",
      );
      expect(index).toBeGreaterThanOrEqual(0);
      return index;
   }

   const runCell = (index: number, givens?: Record<string, unknown>) =>
      fetch(
         pkgUrl(
            `/notebooks/${NOTEBOOK}/cells/${index}${
               givens
                  ? `?givens=${encodeURIComponent(JSON.stringify(givens))}`
                  : ""
            }`,
         ),
      );

   it("serves the notebook and its cell", async () => {
      await queryCellIndex();
   });

   it("runs the cell when the given is supplied", async () => {
      const res = await runCell(await queryCellIndex(), { ORG: 1 });
      expect(res.status).toBe(200);
      expect(cellCount((await res.json()) as { result?: string })).toBe(2);
   });

   it("refuses the cell when the given is not supplied", async () => {
      const res = await runCell(await queryCellIndex());
      expect(res.status).toBe(400);
   });
});
