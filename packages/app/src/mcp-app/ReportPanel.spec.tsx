// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The report panel on a legacy `.malloynb`, as `show_report` opens one: the
 * notebook's cells, the panel's own actions beside them, and each cell's
 * query sent back through `report_api` on the notebook's path.
 */
import { render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it } from "bun:test";
import type { ReportApiCall, ReportApiAnswer } from "./reportApiAdapter";
import { ReportPanel } from "./ReportPanel";

const PATH = "notebooks/old.malloynb";

const NOTEBOOK_MANIFEST = {
   path: PATH,
   name: "old",
   title: "Old",
   layout: "linear",
   editable: false,
   notebook: {
      type: "notebook",
      format: "malloynb",
      notebookCells: [
         { type: "markdown", text: "Legacy prose" },
         {
            type: "code",
            text: "import { orders } from './orders.malloy'",
            // What makes the cell's "Data sources" button appear, where the
            // server offers the Explorer.
            newSources: [JSON.stringify({ name: "orders" })],
         },
         {
            type: "code",
            text: "run: orders -> by_month",
            runQuery: "run: orders -> by_month",
         },
      ],
      sources: [{ name: "orders" }],
   },
};

/** `report_api` as the server answers it; a query never settles, so the cell stays loading. */
function fakeReportApi() {
   const calls: ReportApiCall[] = [];
   const callReportApi = (call: ReportApiCall): Promise<ReportApiAnswer> => {
      calls.push(call);
      switch (call.operation) {
         case "status":
            return Promise.resolve({
               status: 200,
               data: { frozenConfig: false, modelPage: "viewer", theme: {} },
            });
         case "listReports":
            return Promise.resolve({
               status: 200,
               data: [
                  { path: PATH, name: "old", title: "Old", layout: "linear" },
               ],
            });
         case "getReport":
            return Promise.resolve({ status: 200, data: NOTEBOOK_MANIFEST });
         case "query":
            return new Promise<ReportApiAnswer>(() => {});
      }
   };
   return { calls, callReportApi };
}

describe("ReportPanel on a .malloynb", () => {
   it("draws the cells with the panel's Open in Publisher, and runs a cell through report_api on the notebook's path", async () => {
      const { calls, callReportApi } = fakeReportApi();
      render(
         <ReportPanel
            target={{
               environmentName: "examples",
               packageName: "storefront",
               path: PATH,
               givens: {},
            }}
            origin="http://localhost:4000"
            callReportApi={callReportApi}
            openLink={() => {}}
         />,
      );
      await screen.findByText("Legacy prose");
      expect(
         await screen.findByRole("button", { name: "Open in Publisher" }),
      ).toBeDefined();
      await waitFor(() =>
         expect(calls.find((c) => c.operation === "query")).toMatchObject({
            operation: "query",
            environmentName: "examples",
            packageName: "storefront",
            path: PATH,
            body: { query: "run: orders -> by_month" },
         }),
      );
   });

   it("offers no Data sources button on an import cell under the panel's viewer status", async () => {
      const { calls, callReportApi } = fakeReportApi();
      render(
         <ReportPanel
            target={{
               environmentName: "examples",
               packageName: "storefront",
               path: PATH,
               givens: {},
            }}
            origin="http://localhost:4000"
            callReportApi={callReportApi}
            openLink={() => {}}
         />,
      );
      await screen.findByText("Legacy prose");
      // The cells have run, so the status read has answered.
      await waitFor(() =>
         expect(calls.some((c) => c.operation === "query")).toBe(true),
      );
      expect(screen.queryByRole("button", { name: "Data sources" })).toBeNull();
   });
});
