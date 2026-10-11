// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// DEPRECATED FEATURE COVERAGE — delete together with
// `.malloynb` notebooks (see RELEASE_NOTES deprecations). Every test in
// this file is about that deprecated feature.

/// <reference types="bun-types" />

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import {
   fixture,
   startPackageEnv,
   type PackageEnv,
} from "../../harness/reports";

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
   let env: PackageEnv;

   beforeAll(async () => {
      env = await startPackageEnv("notebook-givens-scope-env", {
         [PKG]: fixture("notebook-givens-scope"),
      });
   }, 300_000);

   afterAll(() => env?.stop());

   const runCell = (cellIndex: number, givens: Record<string, unknown>) =>
      fetch(
         env.api(
            `/notebooks/${NOTEBOOK}/cells/${cellIndex}?givens=${encodeURIComponent(
               JSON.stringify(givens),
            )}`,
         ),
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
