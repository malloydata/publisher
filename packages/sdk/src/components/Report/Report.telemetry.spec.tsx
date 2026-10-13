// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What `Report` tells its host: one `report.opened` or `report.load_failed`
 * per report shown, whatever its layout, and a `report.query_failed` for each
 * tile or cell whose query failed, every event stamped with the report's path
 * and layout.
 */
import { afterEach, beforeEach, describe, expect, it, mock } from "bun:test";
import { render, screen, waitFor } from "@testing-library/react";
import {
   clearCache,
   mockServerProvider,
   pending,
   serverWrapper,
} from "../../../test/serverProvider";
import type { ReportManifest } from "../../client";
import type { ReportEvent } from "./telemetry";

let manifest = {} as ReportManifest;
let reportFailure: unknown;
const getReport = mock(() =>
   reportFailure === undefined
      ? Promise.resolve({ data: manifest })
      : Promise.reject(reportFailure),
);
let queryFailure: unknown;
const executeQueryModel = mock(
   (
      _environmentName: string,
      _packageName: string,
      _modelPath: string,
      _request: unknown,
   ): Promise<never> =>
      queryFailure === undefined ? pending() : Promise.reject(queryFailure),
);

mockServerProvider({
   reports: { getReport },
   models: { executeQueryModel, getModel: () => pending() },
});

const { Report } = await import("./Report");

const URI = "publisher://environments/env/packages/pkg";

const GRID = {
   path: "sales/q3.malloy",
   name: "q3",
   title: "Third quarter",
   layout: "grid",
   tiles: [
      { kind: "query", query: "orders -> by_month" },
      { kind: "text", name: "note", markdown: "A note" },
   ],
} as ReportManifest;

const CELLS = {
   path: "review.malloy",
   name: "review",
   layout: "linear",
   editable: false,
   notebook: {
      format: "malloy",
      notebookCells: [
         { type: "markdown", kind: "markdown", text: "Cell prose" },
         { type: "code", kind: "query", text: "run: orders -> by_month" },
      ],
   },
} as ReportManifest;

const typesOf = (calls: [ReportEvent][]) => calls.map(([event]) => event.type);

const errorSpy = mock((..._args: unknown[]) => {});
const realError = console.error;

beforeEach(() => {
   clearCache();
   reportFailure = undefined;
   queryFailure = undefined;
   getReport.mockClear();
   executeQueryModel.mockClear();
   errorSpy.mockClear();
   console.error = errorSpy;
});

afterEach(() => {
   console.error = realError;
});

describe("opening a report", () => {
   it("reports one open of a grid report, with its path, layout and tile count", async () => {
      manifest = GRID;
      const onEvent = mock((_event: ReportEvent) => {});
      const { rerender } = render(
         <Report resourceUri={URI} path="sales/q3.malloy" onEvent={onEvent} />,
         { wrapper: serverWrapper },
      );
      expect(await screen.findByText("Third quarter")).toBeTruthy();
      await waitFor(() => expect(onEvent).toHaveBeenCalled());
      expect(onEvent.mock.calls[0][0]).toMatchObject({
         type: "report.opened",
         path: "sales/q3.malloy",
         layout: "grid",
         surface: "view",
         from: "package",
         tiles: 2,
      });
      expect(
         typeof (onEvent.mock.calls[0][0] as { durationMs: number }).durationMs,
      ).toBe("number");

      // A host handing a new handler each render is not a new open.
      const next = mock((_event: ReportEvent) => {});
      rerender(
         <Report resourceUri={URI} path="sales/q3.malloy" onEvent={next} />,
      );
      await new Promise((resolve) => setTimeout(resolve, 20));
      expect(typesOf(onEvent.mock.calls)).toEqual(["report.opened"]);
      expect(typesOf(next.mock.calls)).not.toContain("report.opened");
   });

   it("reports a linear report's open with its layout", async () => {
      manifest = { ...GRID, layout: "linear" };
      const onEvent = mock((_event: ReportEvent) => {});
      render(
         <Report resourceUri={URI} path="sales/q3.malloy" onEvent={onEvent} />,
         { wrapper: serverWrapper },
      );
      await waitFor(() => expect(onEvent).toHaveBeenCalled());
      expect(onEvent.mock.calls[0][0]).toMatchObject({
         type: "report.opened",
         layout: "linear",
         tiles: 2,
      });
   });

   it("reports a failed read as load_failed with a category, never the message, and no open", async () => {
      reportFailure = Object.assign(
         new Error("no such report: secret.malloy"),
         {
            status: 404,
         },
      );
      const onEvent = mock((_event: ReportEvent) => {});
      render(
         <Report resourceUri={URI} path="secret.malloy" onEvent={onEvent} />,
         { wrapper: serverWrapper },
      );
      await waitFor(() => expect(onEvent).toHaveBeenCalled());
      expect(onEvent.mock.calls.map(([event]) => event)).toEqual([
         {
            type: "report.load_failed",
            path: "secret.malloy",
            surface: "view",
            category: "not_found",
         },
      ]);
   });

   it("reports a URI that names no package as an invalid load", async () => {
      const onEvent = mock((_event: ReportEvent) => {});
      render(
         <Report
            resourceUri="publisher://environments/env"
            path="x.malloy"
            onEvent={onEvent}
         />,
         { wrapper: serverWrapper },
      );
      await waitFor(() => expect(onEvent).toHaveBeenCalledTimes(1));
      expect(onEvent.mock.calls[0][0]).toMatchObject({
         type: "report.load_failed",
         category: "invalid",
      });
   });

   it("keeps rendering when the host's sink throws", async () => {
      manifest = GRID;
      render(
         <Report
            resourceUri={URI}
            path="sales/q3.malloy"
            onEvent={() => {
               throw new Error("sink down");
            }}
         />,
         { wrapper: serverWrapper },
      );
      expect(await screen.findByText("Third quarter")).toBeTruthy();
   });

   // DEPRECATED FEATURE COVERAGE — delete together with
   // `.malloynb` notebooks (see RELEASE_NOTES deprecations).
   describe("deprecated: .malloynb notebooks", () => {
      it("reports a .malloynb as unsupported rather than opened", async () => {
         manifest = {
            ...CELLS,
            path: "legacy.malloynb",
            notebook: { ...CELLS.notebook, format: "malloynb" },
         } as ReportManifest;
         const onEvent = mock((_event: ReportEvent) => {});
         render(
            <Report
               resourceUri={URI}
               path="legacy.malloynb"
               onEvent={onEvent}
            />,
            { wrapper: serverWrapper },
         );
         await waitFor(() => expect(onEvent).toHaveBeenCalled());
         expect(typesOf(onEvent.mock.calls)).toEqual(["report.load_failed"]);
         expect(onEvent.mock.calls[0][0]).toMatchObject({
            category: "unsupported",
         });
      });
   });
});

describe("a failed query", () => {
   it("reports a grid tile's failure once, by position and category", async () => {
      manifest = GRID;
      queryFailure = { response: { status: 500 } };
      const onEvent = mock((_event: ReportEvent) => {});
      render(
         <Report resourceUri={URI} path="sales/q3.malloy" onEvent={onEvent} />,
         { wrapper: serverWrapper },
      );
      await waitFor(() =>
         expect(typesOf(onEvent.mock.calls)).toContain("report.query_failed"),
      );
      await new Promise((resolve) => setTimeout(resolve, 20));
      const failures = onEvent.mock.calls
         .map(([event]) => event)
         .filter((event) => event.type === "report.query_failed");
      expect(failures).toEqual([
         {
            type: "report.query_failed",
            path: "sales/q3.malloy",
            layout: "grid",
            tile: 0,
            category: "server",
         },
      ]);
   });
});
