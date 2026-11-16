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
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
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

// Ported from the removed `Dashboard` wrapper's spec: a version that reaches
// the manifest read but not what the manifest drives lets two versions of one
// report share cache entries, and one silently serves the other's data.
describe("the version reaches what the manifest drives", () => {
   const VERSIONED: ReportManifest = {
      path: "ops.malloy",
      name: "ops",
      title: "Ops",
      layout: "grid",
      tiles: [{ kind: "query", query: "orders -> overview" }],
      givens: [
         { name: "REGION", type: "string" },
         {
            name: "TIER",
            control: "select",
            suggest: { source: "orders", dimension: "tier" },
         },
      ],
   } as ReportManifest;

   const reportAt = (versionId: string) => (
      <Report resourceUri={`${URI}?versionId=${versionId}`} path="ops.malloy" />
   );

   /** The latest tile request, told apart from a suggest by its `givens`. */
   const tileRequest = () =>
      executeQueryModel.mock.calls
         .map((call) => call[3])
         .filter(
            (request) =>
               typeof request === "object" &&
               request !== null &&
               "givens" in request,
         )
         .at(-1) as
         | { versionId?: string; givens?: Record<string, string> }
         | undefined;

   beforeEach(() => {
      manifest = VERSIONED;
   });

   it("runs the tile's query against the same version", async () => {
      render(reportAt("v2"), { wrapper: serverWrapper });

      await waitFor(() => expect(cacheKeys("queryResult").length).toBe(1));
      expect(tileRequest()?.versionId).toBe("v2");
      expect(cacheKeys("queryResult")[0]).toContain('"v2"');
   });

   it("runs every tile of a composite report against the same version", async () => {
      manifest = {
         ...VERSIONED,
         tiles: [
            { kind: "query", query: "orders -> by_month" },
            { kind: "query", query: "orders -> by_region" },
         ],
      } as ReportManifest;
      render(reportAt("v2"), { wrapper: serverWrapper });

      await waitFor(() => expect(cacheKeys("queryResult").length).toBe(2));
      for (const key of cacheKeys("queryResult")) expect(key).toContain('"v2"');
   });

   it("runs the control's suggest query against the same version", async () => {
      render(reportAt("v2"), { wrapper: serverWrapper });

      await waitFor(() => expect(cacheKeys("givenSuggest").length).toBe(1));
      expect(cacheKeys("givenSuggest")[0]).toContain('"v2"');
   });

   it("does not carry one version's applied values into another", async () => {
      // `documentKey` is what tells the givens state the document changed.
      // Mounted with no `givens` prop, which is the case the key governs. v2
      // is read first so that switching back to it is served from the cache:
      // the view stays mounted across the switch, and only the key can reset
      // the values (a fresh read would remount it and reset them anyway).
      const { rerender } = render(reportAt("v2"), { wrapper: serverWrapper });
      await waitFor(() => expect(cacheKeys("queryResult").length).toBe(1));
      rerender(reportAt("v1"));
      await waitFor(() => expect(cacheKeys("queryResult").length).toBe(2));

      // The control sits in its filter button's popover.
      fireEvent.click(await screen.findByRole("button", { name: /^REGION: / }));
      fireEvent.change(await screen.findByLabelText("REGION"), {
         target: { value: "CA" },
      });
      await waitFor(() =>
         expect(tileRequest()?.givens).toEqual({ REGION: "CA" }),
      );

      rerender(reportAt("v2"));

      await waitFor(() =>
         expect(
            // The popover is still open, so the bar behind it is aria-hidden.
            screen.getByRole("button", { name: /^REGION: /, hidden: true })
               .textContent,
         ).not.toContain("CA"),
      );
      const v2 = cacheKeys("queryResult").filter((key) => key.includes('"v2"'));
      expect(v2.length).toBeGreaterThan(0);
      for (const key of v2) expect(key).not.toContain("CA");
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
