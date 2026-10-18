// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `Report` renders a report of either layout from `GET …/reports/{path}`: a
 * grid as a dashboard of carded tiles, a linear report as bare tiles top to
 * bottom, and a report written as `run:` cells cell by cell, through the
 * governed query endpoint. It runs nothing through the deprecated notebooks
 * routes: a `.malloynb` is the deprecated `Notebook`'s to render.
 */
import { beforeEach, describe, expect, it, mock } from "bun:test";
import { act, render, screen, waitFor } from "@testing-library/react";
import {
   cacheKeys,
   clearCache,
   mockServerProvider,
   pending,
   serverWrapper,
} from "../../../test/serverProvider";
import type { ReportManifest } from "../../client";
import { globalQueryClient } from "../../utils/queryClient";

let manifest = {} as ReportManifest;
const getReport = mock(
   (
      _environmentName: string,
      _packageName: string,
      _path: string,
      _versionId?: string,
      _options?: unknown,
   ) => Promise.resolve({ data: manifest }),
);
const executeNotebookCell = mock(
   (
      _environmentName: string,
      _packageName: string,
      _path: string,
      _cellIndex: number,
      _versionId?: string,
      _filterParams?: string,
      _bypassFilters?: boolean,
      _givens?: string,
      _options?: unknown,
   ): Promise<never> => pending(),
);
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
   reports: { getReport },
   notebooks: { getNotebook, executeNotebookCell },
   models: { executeQueryModel, getModel: () => pending() },
});

// Imported after the stub is registered: a static import would hoist above it.
const { Report } = await import("./Report");

const URI = "publisher://environments/env/packages/pkg";

const TILES = {
   path: "sales/q3.malloy",
   name: "q3",
   title: "Third quarter",
   tiles: [{ kind: "query", query: "orders -> by_month" }],
} as ReportManifest;

beforeEach(() => {
   clearCache();
   for (const fn of [
      getReport,
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

   it("asks again on every mount unless the host says otherwise", async () => {
      manifest = { ...TILES, layout: "grid" };
      const first = render(
         <Report resourceUri={URI} path="sales/q3.malloy" />,
         {
            wrapper: serverWrapper,
         },
      );
      await waitFor(() => expect(getReport).toHaveBeenCalledTimes(1));
      first.unmount();
      render(<Report resourceUri={URI} path="sales/q3.malloy" />, {
         wrapper: serverWrapper,
      });
      await waitFor(() => expect(getReport).toHaveBeenCalledTimes(2));
   });

   it("leaves the read's staleness to the host's default for the report key", async () => {
      // A host that reads the manifest itself first, then mounts the report
      // over the same entry, keeps that entry fresh with one default.
      globalQueryClient.setQueryDefaults(["report"], { staleTime: Infinity });
      try {
         manifest = { ...TILES, layout: "grid" };
         const first = render(
            <Report resourceUri={URI} path="sales/q3.malloy" />,
            { wrapper: serverWrapper },
         );
         await waitFor(() => expect(getReport).toHaveBeenCalledTimes(1));
         first.unmount();
         render(<Report resourceUri={URI} path="sales/q3.malloy" />, {
            wrapper: serverWrapper,
         });
         await screen.findAllByText("Third quarter");
         expect(getReport).toHaveBeenCalledTimes(1);
      } finally {
         globalQueryClient.setQueryDefaults(["report"], {});
      }
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

   it("keeps a rendered report when a refetch fails", async () => {
      // The manifest stays in the cache when a background refetch fails, and
      // the reader was looking at it. Swapping it for an error card threw it
      // away over a request the reader never made, and it was not a failed
      // load, so no `report.load_failed` goes to the host.
      manifest = { ...TILES, layout: "grid" };
      const onEvent = mock((_event: { type: string }) => {});
      render(
         <Report resourceUri={URI} path="sales/q3.malloy" onEvent={onEvent} />,
         { wrapper: serverWrapper },
      );
      expect(await screen.findByText("Third quarter")).toBeTruthy();

      getReport.mockImplementation(() =>
         Promise.reject(new Error("Network Error")),
      );
      try {
         await act(() =>
            globalQueryClient.refetchQueries({ queryKey: ["report"] }),
         );

         expect(
            await screen.findByText(/Could not refresh this report/),
         ).toBeTruthy();
         expect(screen.getByText(/Network Error/)).toBeTruthy();
         expect(screen.getByText("Third quarter")).toBeTruthy();
         expect(getReport).toHaveBeenCalledTimes(2);
         expect(onEvent.mock.calls.map(([e]) => e.type)).toEqual([
            "report.opened",
         ]);
      } finally {
         getReport.mockImplementation(() =>
            Promise.resolve({ data: manifest }),
         );
      }
   });
});

describe("the host's actions", () => {
   it("draws them on the title's line, not on a row of their own above it", async () => {
      manifest = { ...TILES, layout: "linear" };
      render(
         <Report
            resourceUri={URI}
            path="sales/q3.malloy"
            actions={<button type="button">Download</button>}
         />,
         { wrapper: serverWrapper },
      );

      const title = await screen.findByText("Third quarter");
      const action = screen.getByRole("button", { name: "Download" });
      // The title's own row holds both.
      expect(title.parentElement?.contains(action)).toBe(true);
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

   it("draws a linear report's query tiles in cards, as a grid does", async () => {
      manifest = { ...TILES, layout: "linear" };
      const { container } = render(
         <Report resourceUri={URI} path="sales/q3.malloy" />,
         { wrapper: serverWrapper },
      );

      expect(await screen.findByText("Third quarter")).toBeTruthy();
      await waitFor(() => expect(executeQueryModel).toHaveBeenCalled());
      expect(executeQueryModel.mock.calls[0][2]).toBe("sales/q3.malloy");
      await waitFor(() =>
         expect(container.querySelector('[data-chrome="card"]')).not.toBeNull(),
      );
   });

   it("runs no cells: a manifest carrying `run:` cells draws no cells", async () => {
      manifest = {
         path: "review.malloy",
         name: "review",
         layout: "linear",
         title: "Review",
         editable: false,
         notebook: {
            format: "malloy",
            notebookCells: [
               { type: "markdown", kind: "markdown", text: "Cell prose" },
               { type: "code", kind: "query", text: "run: orders -> by_month" },
            ],
         },
      } as ReportManifest;
      render(<Report resourceUri={URI} path="review.malloy" />, {
         wrapper: serverWrapper,
      });

      expect(await screen.findByText("Review")).toBeTruthy();
      expect(screen.queryByText("Cell prose")).toBeNull();
      expect(executeQueryModel).not.toHaveBeenCalled();
   });

   // DEPRECATED FEATURE COVERAGE — delete together with
   // `.malloynb` notebooks (see RELEASE_NOTES deprecations).
   describe("deprecated: .malloynb notebooks", () => {
      it("says a .malloynb is Notebook's to render, and runs nothing", async () => {
         manifest = {
            path: "legacy.malloynb",
            name: "legacy",
            layout: "linear",
            editable: false,
            notebook: {
               format: "malloynb",
               notebookCells: [
                  { type: "markdown", text: "Legacy prose" },
                  { type: "code", text: "run: orders -> by_month" },
               ],
            },
         } as ReportManifest;
         render(<Report resourceUri={URI} path="legacy.malloynb" />, {
            wrapper: serverWrapper,
         });

         expect(
            await screen.findByText(/render it with Notebook/),
         ).toBeTruthy();
         expect(screen.queryByText("Legacy prose")).toBeNull();
         expect(executeNotebookCell).not.toHaveBeenCalled();
         expect(executeQueryModel).not.toHaveBeenCalled();
      });
   });
});
