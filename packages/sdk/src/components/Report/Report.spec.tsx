// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `Report` renders a report of either layout from `GET …/reports/{path}`: a
 * grid as a dashboard of carded tiles, a linear report as bare tiles top to
 * bottom, and a report written as `run:` cells cell by cell, through the
 * governed query endpoint. A `.malloynb`'s code cells run through the query
 * endpoint too.
 */
import { beforeEach, describe, expect, it, mock, spyOn } from "bun:test";
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
   for (const fn of [getReport, executeQueryModel]) fn.mockClear();
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
      const LEGACY = {
         path: "notebooks/legacy.malloynb",
         name: "legacy",
         layout: "linear",
         editable: false,
         notebook: {
            format: "malloynb",
            notebookCells: [
               { type: "markdown", text: "Legacy prose" },
               {
                  type: "code",
                  text: 'import "../orders.malloy"\nsource: big is orders extend { where: amount > 100 }',
               },
               {
                  type: "code",
                  text: '##! experimental.givens\nimport "../orders.malloy"\n# bar_chart\nrun: orders -> by_month',
               },
               {
                  type: "code",
                  text: "run: big -> { aggregate: n is count() }",
               },
            ],
            sources: [
               { name: "orders", givens: [{ name: "REGION", type: "string" }] },
            ],
         },
      } as ReportManifest;

      /** The query requests the cells sent, in the order they were sent. */
      const cellRequests = () =>
         executeQueryModel.mock.calls.map(
            ([environmentName, packageName, modelPath, request]) => ({
               environmentName,
               packageName,
               modelPath,
               request,
            }),
         );

      beforeEach(() => {
         manifest = LEGACY;
      });

      it("draws its markdown and its code cells, each code cell with its result", async () => {
         executeQueryModel.mockImplementation(
            () => Promise.resolve({ data: { result: "{}" } }) as Promise<never>,
         );
         try {
            render(
               <Report resourceUri={URI} path="notebooks/legacy.malloynb" />,
               { wrapper: serverWrapper },
            );

            expect(await screen.findByText("Legacy prose")).toBeTruthy();
            // One result tile, with its code behind a button, per cell that runs.
            await waitFor(() =>
               expect(
                  screen.getAllByRole("button", { name: "Malloy code" }),
               ).toHaveLength(2),
            );
         } finally {
            executeQueryModel.mockImplementation(() => pending());
         }
      });

      it("runs each code cell's run: through the query endpoint, against the notebook's path, with the givens and the version", async () => {
         render(
            <Report
               resourceUri={`${URI}?versionId=v2`}
               path="notebooks/legacy.malloynb"
               givens={{ REGION: "West" }}
            />,
            { wrapper: serverWrapper },
         );

         await waitFor(() =>
            expect(executeQueryModel).toHaveBeenCalledTimes(2),
         );
         const sent = cellRequests();
         // The declaration-only cell is not sent; the others send their run:,
         // render tag included, without their imports and flags.
         expect(
            sent.map((call) => (call.request as { query: string }).query),
         ).toEqual([
            "# bar_chart\nrun: orders -> by_month",
            "run: big -> { aggregate: n is count() }",
         ]);
         for (const call of sent) {
            expect(call).toEqual({
               environmentName: "env",
               packageName: "pkg",
               modelPath: "notebooks/legacy.malloynb",
               request: {
                  query: expect.any(String),
                  versionId: "v2",
                  givens: { REGION: "West" },
               },
            });
         }
      });

      it("sends no givens when none is set", async () => {
         render(<Report resourceUri={URI} path="notebooks/legacy.malloynb" />, {
            wrapper: serverWrapper,
         });

         await waitFor(() =>
            expect(executeQueryModel).toHaveBeenCalledTimes(2),
         );
         for (const { request } of cellRequests()) {
            expect(request).toEqual({
               query: expect.any(String),
               versionId: undefined,
            });
         }
      });

      it("draws the host's actions, on a row of their own above the cells", async () => {
         render(
            <Report
               resourceUri={URI}
               path="notebooks/legacy.malloynb"
               actions={<button type="button">Download</button>}
            />,
            { wrapper: serverWrapper },
         );

         const prose = await screen.findByText("Legacy prose");
         const action = screen.getByRole("button", { name: "Download" });
         // Above the notebook's first cell, which is its title.
         expect(
            action.compareDocumentPosition(prose) &
               Node.DOCUMENT_POSITION_FOLLOWING,
         ).toBeTruthy();
      });

      it("runs the cells with a ## givens starting value, and keeps it out of what it reports to the host", async () => {
         manifest = {
            ...LEGACY,
            notebook: {
               ...LEGACY.notebook,
               startingGivens: { REGION: "East" },
            },
         } as ReportManifest;
         const onGivensChange = mock(() => {});
         render(
            <Report
               resourceUri={URI}
               path="notebooks/legacy.malloynb"
               onGivensChange={onGivensChange}
            />,
            { wrapper: serverWrapper },
         );

         await waitFor(() =>
            expect(executeQueryModel).toHaveBeenCalledTimes(2),
         );
         for (const { request } of cellRequests()) {
            expect(request).toMatchObject({ givens: { REGION: "East" } });
         }
         // `Report` mounts the cells only once the manifest has arrived, so
         // the starting value is the controls' first state and is never
         // reported as a change for the host to write into its URL.
         expect(onGivensChange).not.toHaveBeenCalled();
      });

      it("shows a cell's failure in that cell, and still runs the others", async () => {
         const quiet = spyOn(console, "error").mockImplementation(() => {});
         executeQueryModel.mockImplementation(((
            _environmentName: string,
            _packageName: string,
            _modelPath: string,
            request: unknown,
         ) =>
            (request as { query: string }).query.startsWith("run: big")
               ? Promise.reject(
                    Object.assign(
                       new Error("Request failed with status code 404"),
                       {
                          response: {
                             data: {
                                message: "Query target is not queryable.",
                             },
                          },
                       },
                    ),
                 )
               : Promise.resolve({ data: { result: "{}" } })) as never);
         try {
            render(
               <Report resourceUri={URI} path="notebooks/legacy.malloynb" />,
               { wrapper: serverWrapper },
            );

            expect(
               await screen.findByText("Query target is not queryable."),
            ).toBeTruthy();
            expect(
               screen.getAllByText("This cell could not be run."),
            ).toHaveLength(1);
            await waitFor(() =>
               expect(
                  screen.getAllByRole("button", { name: "Malloy code" }),
               ).toHaveLength(1),
            );
         } finally {
            executeQueryModel.mockImplementation(() => pending());
            quiet.mockRestore();
         }
      });
   });
});
