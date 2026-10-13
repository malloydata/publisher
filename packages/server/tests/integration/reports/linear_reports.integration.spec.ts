// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * A linear report written as `run:` cells gets the same parameter contract as
 * a grid report, over real HTTP against `tests/fixtures/reports-linear`: the
 * control contract on its sources' givens, `autorun`, a starting given that
 * survives being sent back, a date given in the bare form the shared codec
 * sends, and the title chain in the listing. Its cells run through
 * `POST …/models/{path}/query`, the cell's `text` as the query.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import {
   fixture,
   startPackageEnv,
   type PackageEnv,
} from "../../harness/reports";

const PKG = "reports-linear";

interface GivenSpec {
   name?: string;
   type?: string;
   label?: string;
   control?: string;
   rangeMin?: number;
   rangeMax?: number;
   suggest?: { query?: string; source?: string; dimension?: string };
}

interface Cell {
   type?: string;
   kind?: string;
   text?: string;
}

interface LinearReport {
   path: string;
   layout?: string;
   title?: string;
   description?: string;
   notebook?: {
      autorun?: boolean;
      startingGivens?: Record<string, string>;
      sources?: { givens?: GivenSpec[] }[];
      notebookCells?: Cell[];
   };
}

describe("Linear reports written as run: cells (E2E)", () => {
   let env: PackageEnv;
   const report = (reportPath: string) =>
      env.getJson<LinearReport>(env.api(`/reports/${reportPath}`));
   /** The text of a report's one query cell, the way a viewer would run it. */
   const queryCellText = async (reportPath: string): Promise<string> => {
      const cells = (await report(reportPath)).notebook?.notebookCells ?? [];
      const queries = cells.filter((c) => c.kind === "query");
      expect(queries).toHaveLength(1);
      return queries[0].text ?? "";
   };
   const runCell = async (reportPath: string, since: string) =>
      env.query(reportPath, {
         query: await queryCellText(reportPath),
         givens: { SINCE: since },
         compactJson: true,
      });

   beforeAll(async () => {
      env = await startPackageEnv("reports-linear-env", {
         [PKG]: fixture(PKG),
      });
   }, 300_000);

   afterAll(() => env?.stop());

   it("carries each given's control contract on the report's sources", async () => {
      const body = await report("orders-since.malloy");
      expect(body.layout).toBe("linear");
      const givens = new Map(
         (body.notebook?.sources ?? [])
            .flatMap((source) => source.givens ?? [])
            .map((given) => [given.name, given]),
      );
      // The same presentation a grid report's manifest reports, because it is
      // read off the declaration rather than off either layout.
      expect(givens.get("BRAND")).toMatchObject({
         label: "Brand",
         control: "select",
         suggest: { source: "orders", dimension: "brand" },
      });
      expect(givens.get("MIN_AMOUNT")).toMatchObject({
         rangeMin: 0,
         rangeMax: 500,
      });
      expect(givens.get("SINCE")).toMatchObject({
         type: "date",
         label: "Ordered since",
      });
   });

   it("reports autorun=false from the artifact tag, and true by default", async () => {
      expect((await report("orders-since.malloy")).notebook?.autorun).toBe(
         false,
      );
      expect((await report("brands.malloy")).notebook?.autorun).toBe(true);
   });

   // Both layouts encode a Date through the SDK's `givensToRequest`, and
   // nothing in the SDK can check that the server reads what it sends, so the
   // wire form is pinned from this side: the three time types take three
   // spellings and each rejects the other two.
   it("reads a date given in the bare form the shared codec sends", async () => {
      const countSince = async (since: string) => {
         const res = await runCell("orders-since.malloy", since);
         expect(res.status).toBe(200);
         const rows = JSON.parse(
            ((await res.json()) as { result: string }).result,
         ) as { order_count?: number }[];
         return Number(rows[0]?.order_count);
      };
      // Six orders in the fixture; two are ordered on or after 2024-03-01.
      expect(await countSince("2024-01-01")).toBe(6);
      expect(await countSince("2024-03-01")).toBe(2);
   });

   it("publishes a date starting given the query endpoint will accept", async () => {
      const body = await report("orders-start.malloy");
      expect(body.notebook?.startingGivens?.SINCE).toBe("2024-03-01");
      const run = await runCell(
         "orders-start.malloy",
         body.notebook?.startingGivens?.SINCE ?? "",
      );
      expect(run.status).toBe(200);
   });

   it("rejects a full ISO timestamp for a date given", async () => {
      // The reason `givensToRequest` needs the declared type at all: a blanket
      // toISOString() lands here, not on a result.
      const res = await runCell(
         "orders-since.malloy",
         "2024-03-01T00:00:00.000Z",
      );
      expect(res.status).toBe(400);
      const body = (await res.json()) as { message?: string };
      expect(body.message).toContain("YYYY-MM-DD");
   });

   describe("titles in the package listing", () => {
      // Compared as served: a title or description never carries a `\r`,
      // whatever line endings the checkout gave the fixture
      // (`service/report_listing_crlf.spec.ts`).
      const listed = async (reportPath: string) =>
         (await env.getJson<LinearReport[]>(env.api("/reports"))).find(
            (r) => r.path === reportPath,
         );

      it("prefers an explicit title= over everything else", async () => {
         expect(await listed("orders-since.malloy")).toMatchObject({
            title: "Orders in a window",
            description: "Order counts from a date the reader picks.",
         });
      });

      it("falls back to the first markdown heading, so an untitled report still has one", async () => {
         const brands = await listed("brands.malloy");
         expect(brands?.title).toBe("Brands");
         // Nothing to describe it: the heading is a title, not a doc comment.
         expect(brands?.description).toBeUndefined();
      });

      it("takes the doc comment's first line ahead of the heading, and the rest as the description", async () => {
         // A title is one line and a description is not, and the description
         // must not repeat the line the title already took.
         const carriers = await listed("carriers.malloy");
         expect(carriers).toMatchObject({
            title: "Carrier volumes",
            description: "Shipments per carrier, refreshed nightly.",
         });
         expect(carriers?.title ?? "").not.toContain("\n");
      });
   });
});
