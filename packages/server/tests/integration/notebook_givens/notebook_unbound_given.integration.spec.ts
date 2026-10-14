// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import {
   fixture,
   startPackageEnv,
   type PackageEnv,
} from "../../harness/reports";

const PKG = "notebook-unbound-given";
const NOTEBOOK = "notebooks/nb.malloy";

/** The aggregate count out of a query result's nested encoding. */
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

describe("a linear report tile reading a given with no default (HTTP E2E)", () => {
   let env: PackageEnv;
   const pkgUrl = (sub: string) => env.api(sub);

   beforeAll(async () => {
      env = await startPackageEnv("notebook-unbound-given-env", {
         [PKG]: fixture(PKG),
      });
   }, 300_000);

   afterAll(() => env?.stop());

   /** The report's one query tile, off `GET …/reports/{path}`, as the query a viewer runs. */
   async function queryCellText(): Promise<string> {
      const res = await fetch(pkgUrl(`/reports/${NOTEBOOK}`));
      expect(res.status).toBe(200);
      const body = (await res.json()) as {
         tiles?: Array<{ kind?: string; query?: string }>;
      };
      const tile = (body.tiles ?? []).find((t) => t.kind === "query");
      expect(tile?.query).toBeDefined();
      return `run: ${tile?.query ?? ""}`;
   }

   /** Run the tile the way a viewer does: through the query route. */
   const runCell = (text: string, givens?: Record<string, unknown>) =>
      env.query(NOTEBOOK, { query: text, givens });

   it("serves the report and its tile", async () => {
      await queryCellText();
   });

   it("runs the tile when the given is supplied", async () => {
      const res = await runCell(await queryCellText(), { ORG: 1 });
      expect(res.status).toBe(200);
      expect(cellCount((await res.json()) as { result?: string })).toBe(2);
   });

   it("refuses the tile when the given is not supplied", async () => {
      const res = await runCell(await queryCellText());
      expect(res.status).toBe(400);
   });
});
