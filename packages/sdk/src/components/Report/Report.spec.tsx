// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `Report` renders a report of either layout from `GET …/reports/{path}`: a
 * grid as a dashboard of carded tiles, a linear report as bare tiles top to
 * bottom, and a report written as `run:` cells cell by cell, through the
 * reports resource's own cell route.
 */
import { beforeEach, describe, expect, it, mock } from "bun:test";
import { render, screen, waitFor } from "@testing-library/react";
import {
   cacheKeys,
   clearCache,
   mockServerProvider,
   pending,
   serverWrapper,
} from "../../../test/serverProvider";
import type { ReportManifest } from "../../client";

let manifest: ReportManifest = {};
const getReport = mock(
   (
      _environmentName: string,
      _packageName: string,
      _path: string,
      _versionId?: string,
      _options?: unknown,
   ) => Promise.resolve({ data: manifest }),
);
const executeReportCell = mock(
   (
      _environmentName: string,
      _packageName: string,
      _path: string,
      _cellIndex: number,
      _versionId?: string,
      _givens?: string,
      _options?: unknown,
   ): Promise<never> => pending(),
);
const executeNotebookCell = mock((): Promise<never> => pending());
const getNotebook = mock((): Promise<never> => pending());
const executeQueryModel = mock(
   (
      _environmentName: string,
      _packageName: string,
      _modelPath: string,
      _request: unknown,
   ): Promise<never> => pending(),
);

mockServerProvider({
   reports: { getReport, executeReportCell },
   notebooks: { getNotebook, executeNotebookCell },
   models: { executeQueryModel, getModel: () => pending() },
});

// Imported after the stub is registered: a static import would hoist above it.
const { Report } = await import("./Report");

const URI = "publisher://environments/env/packages/pkg";

const TILES: ReportManifest = {
   path: "sales/q3.malloy",
   name: "q3",
   title: "Third quarter",
   tiles: [{ kind: "query", query: "orders -> by_month" }],
};

beforeEach(() => {
   clearCache();
   for (const fn of [
      getReport,
      executeReportCell,
      executeNotebookCell,
      getNotebook,
      executeQueryModel,
   ])
      fn.mockClear();
});

describe("the manifest fetch", () => {
   it("reads the report by its path, and keys the read on every value it is built from", async () => {
      manifest = { ...TILES, layout: "grid" };
      render(
         <Report resourceUri={`${URI}?versionId=v2`} path="sales/q3.malloy" />,
         {
            wrapper: serverWrapper,
         },
      );

      await waitFor(() => expect(getReport).toHaveBeenCalled());
      expect(getReport.mock.calls[0].slice(0, 4)).toEqual([
         "env",
         "pkg",
         "sales/q3.malloy",
         "v2",
      ]);
      expect(cacheKeys("report")).toEqual([
         '["report","env","pkg","v2","sales/q3.malloy","http://localhost/api/v0"]',
      ]);
   });

   it("keys an unversioned read with an empty version slot", async () => {
      manifest = { ...TILES, layout: "grid" };
      render(<Report resourceUri={URI} path="sales/q3.malloy" />, {
         wrapper: serverWrapper,
      });

      await waitFor(() => expect(getReport).toHaveBeenCalled());
      expect(getReport.mock.calls[0][3]).toBeUndefined();
      expect(cacheKeys("report")).toEqual([
         '["report","env","pkg",null,"sales/q3.malloy","http://localhost/api/v0"]',
      ]);
   });

   it("says so, rather than throwing, for a URI that names no package", () => {
      render(
         <Report resourceUri="publisher://environments/env" path="x.malloy" />,
         {
            wrapper: serverWrapper,
         },
      );

      expect(
         screen.getByText(/must name an environment and a package/),
      ).toBeTruthy();
      expect(getReport).not.toHaveBeenCalled();
   });
});

describe("rendering by layout", () => {
   it("draws a grid report's tiles as cards, run against the report's path", async () => {
      manifest = { ...TILES, layout: "grid" };
      const { container } = render(
         <Report resourceUri={URI} path="sales/q3.malloy" />,
         { wrapper: serverWrapper },
      );

      expect(await screen.findByText("Third quarter")).toBeTruthy();
      await waitFor(() => expect(executeQueryModel).toHaveBeenCalled());
      expect(executeQueryModel.mock.calls[0][2]).toBe("sales/q3.malloy");
      expect(container.querySelector('[data-chrome="card"]')).not.toBeNull();
      expect(container.querySelector('[data-chrome="none"]')).toBeNull();
   });

   it("draws a linear report's tiles bare, top to bottom", async () => {
      manifest = { ...TILES, layout: "linear" };
      const { container } = render(
         <Report resourceUri={URI} path="sales/q3.malloy" />,
         { wrapper: serverWrapper },
      );

      expect(await screen.findByText("Third quarter")).toBeTruthy();
      await waitFor(() => expect(executeQueryModel).toHaveBeenCalled());
      expect(executeQueryModel.mock.calls[0][2]).toBe("sales/q3.malloy");
      await waitFor(() =>
         expect(container.querySelector('[data-chrome="none"]')).not.toBeNull(),
      );
      expect(container.querySelector('[data-chrome="card"]')).toBeNull();
   });

   it("runs a run:-cell report's cells through the reports resource, never the notebooks one", async () => {
      manifest = {
         path: "review.malloy",
         name: "review",
         layout: "linear",
         editable: false,
         notebook: {
            format: "malloy",
            notebookCells: [
               { type: "markdown", kind: "markdown", text: "Cell prose" },
               {
                  type: "code",
                  kind: "query",
                  text: "run: orders -> by_month",
               },
            ],
         },
      } as ReportManifest;
      render(
         <Report resourceUri={`${URI}?versionId=v4`} path="review.malloy" />,
         {
            wrapper: serverWrapper,
         },
      );

      expect(await screen.findByText("Cell prose")).toBeTruthy();
      await waitFor(() => expect(executeReportCell).toHaveBeenCalled());
      expect(
         executeReportCell.mock.calls.map((call) => call.slice(0, 5)),
      ).toEqual([["env", "pkg", "review.malloy", 1, "v4"]]);
      expect(executeNotebookCell).not.toHaveBeenCalled();
      // The cells came with the manifest; nothing reads them again.
      expect(getNotebook).not.toHaveBeenCalled();
   });
});
