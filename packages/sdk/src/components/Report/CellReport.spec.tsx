// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What a report written as `run:` cells shows when something fails: a cell
 * the server refuses says why in that cell and leaves the others running, and
 * cells that could not be read at all say so in place of the cells. Rendering
 * the happy path is `Report.spec`'s.
 */
import {
   afterEach,
   beforeEach,
   describe,
   expect,
   it,
   mock,
   spyOn,
} from "bun:test";
import { render, screen, waitFor } from "@testing-library/react";
import {
   clearCache,
   mockServerProvider,
   serverWrapper,
} from "../../../test/serverProvider";
import type { RawNotebook } from "../../client";

let answer: (query: string) => Promise<unknown> = () =>
   Promise.resolve({ data: { result: undefined } });
const executeQueryModel = mock(
   (
      _environmentName: string,
      _packageName: string,
      _modelPath: string,
      request: { query?: string },
   ) => answer(request.query ?? ""),
);

mockServerProvider({
   models: { executeQueryModel, getModel: () => new Promise(() => {}) },
});

// Imported after the stub is registered: a static import would hoist above it.
const { CellReport, CellRunnerView } = await import("./CellReport");

const URI = "publisher://environments/env/packages/pkg/models/review.malloy";

const cells = (...queries: string[]): RawNotebook =>
   ({
      format: "malloy",
      notebookCells: [
         { type: "markdown", kind: "markdown", text: "Intro prose" },
         ...queries.map((text) => ({ type: "code", kind: "query", text })),
      ],
   }) as RawNotebook;

/** A rejection shaped like axios's, carrying the server's `{message}`. */
const refused = (message: string) =>
   Object.assign(new Error("Request failed with status code 400"), {
      response: { data: { message } },
   });

// A failed cell is logged as well as shown; the log is not under test.
let quiet: { mockRestore(): void } | undefined;
beforeEach(() => {
   clearCache();
   executeQueryModel.mockClear();
   quiet = spyOn(console, "error").mockImplementation(() => {});
});
afterEach(() => quiet?.mockRestore());

describe("a cell that fails", () => {
   it.each([
      [
         "the server's own message",
         refused("filter 'state' is required"),
         "filter 'state' is required",
      ],
      [
         "the error's message when the server sent none",
         new Error("Network Error"),
         "Network Error",
      ],
      [
         "a fixed line when nothing says why",
         "boom",
         "The server did not say why.",
      ],
   ])("says why with %s, in that cell", async (_name, error, shown) => {
      answer = () => Promise.reject(error);
      render(<CellReport resourceUri={URI} notebook={cells("run: a -> v")} />, {
         wrapper: serverWrapper,
      });

      expect(
         await screen.findByText("This cell could not be run."),
      ).toBeTruthy();
      expect(screen.getByText(shown)).toBeTruthy();
      expect(screen.getByText("Intro prose")).toBeTruthy();
   });

   it("still runs and shows the other cells", async () => {
      answer = (query) =>
         query === "run: bad -> v"
            ? Promise.reject(refused("no such source"))
            : Promise.resolve({ data: { result: undefined } });
      render(
         <CellReport
            resourceUri={URI}
            notebook={cells("run: bad -> v", "run: good -> v")}
         />,
         { wrapper: serverWrapper },
      );

      expect(await screen.findByText("no such source")).toBeTruthy();
      expect(executeQueryModel.mock.calls.map((call) => call[3])).toEqual([
         expect.objectContaining({ query: "run: bad -> v" }),
         expect.objectContaining({ query: "run: good -> v" }),
      ]);
      expect(screen.getAllByText("This cell could not be run.")).toHaveLength(
         1,
      );
   });
});

describe("cells that could not be read", () => {
   const view = (error: { status: number; message: string }) =>
      render(
         <CellRunnerView
            resourceUri={URI}
            notebook={undefined}
            error={{
               ...error,
               name: "ApiError",
               data: { code: error.status, message: error.message },
            }}
            runCell={() => Promise.resolve({})}
         />,
         { wrapper: serverWrapper },
      );

   it("names the report that does not exist", () => {
      view({ status: 404, message: "Not found" });
      expect(screen.getByText("env > pkg > review.malloy")).toBeTruthy();
      expect(screen.getByText(/not found\./)).toBeTruthy();
   });

   it("shows any other failure with the report it was reading", async () => {
      view({ status: 500, message: "The package did not load" });
      await waitFor(() =>
         expect(screen.getByText("env > pkg > review.malloy")).toBeTruthy(),
      );
      expect(screen.getByText("The package did not load")).toBeTruthy();
      expect(screen.queryByText(/not found\./)).toBeNull();
   });
});
