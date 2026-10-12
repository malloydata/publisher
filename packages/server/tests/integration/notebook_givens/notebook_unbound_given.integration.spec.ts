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
   let env: PackageEnv;
   const pkgUrl = (sub: string) => env.api(sub);

   beforeAll(async () => {
      env = await startPackageEnv("notebook-unbound-given-env", {
         [PKG]: fixture(PKG),
      });
   }, 300_000);

   afterAll(() => env?.stop());

   /** The report's one query cell, off `GET …/reports/{path}`. */
   async function queryCellText(): Promise<string> {
      const res = await fetch(pkgUrl(`/reports/${NOTEBOOK}`));
      expect(res.status).toBe(200);
      const body = (await res.json()) as {
         notebook?: { notebookCells?: Array<{ kind?: string; text?: string }> };
      };
      const cell = (body.notebook?.notebookCells ?? []).find(
         (c) => c.kind === "query",
      );
      expect(cell?.text).toBeDefined();
      return cell?.text ?? "";
   }

   /** Run the cell the way a viewer does: its text through the query route. */
   const runCell = (text: string, givens?: Record<string, unknown>) =>
      env.query(NOTEBOOK, { query: text, givens });

   it("serves the notebook and its cell", async () => {
      await queryCellText();
   });

   it("runs the cell when the given is supplied", async () => {
      const res = await runCell(await queryCellText(), { ORG: 1 });
      expect(res.status).toBe(200);
      expect(cellCount((await res.json()) as { result?: string })).toBe(2);
   });

   it("refuses the cell when the given is not supplied", async () => {
      const res = await runCell(await queryCellText());
      expect(res.status).toBe(400);
   });
});
