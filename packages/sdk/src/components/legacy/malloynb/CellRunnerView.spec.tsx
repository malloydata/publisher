// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// DEPRECATED FEATURE COVERAGE — delete together with
// `.malloynb` notebooks (see RELEASE_NOTES deprecations).

/**
 * What a `.malloynb` shows when something fails: a cell the server refuses
 * says why in that cell and leaves the others running, and cells that could
 * not be read at all say so in place of the cells.
 */
import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import { render, screen, waitFor } from "@testing-library/react";
import {
   clearCache,
   mockServerProvider,
   serverWrapper,
} from "../../../../test/serverProvider";
import type { RawNotebook } from "../../../client";
import type { CellRunner } from "./CellRunnerView";

// Cells run through the runner each test hands in, never the server.
mockServerProvider({ models: { getModel: () => new Promise(() => {}) } });

// Imported after the stub is registered: a static import would hoist above it.
const { CellRunnerView } = await import("./CellRunnerView");

const URI = "publisher://environments/env/packages/pkg/models/review.malloynb";

const cells = (...queries: string[]): RawNotebook => ({
   format: "malloynb",
   notebookCells: [
      { type: "markdown", text: "Intro prose" },
      ...queries.map((text) => ({ type: "code" as const, text })),
   ],
});

/** A rejection shaped like axios's, carrying the server's `{message}`. */
const refused = (message: string) =>
   Object.assign(new Error("Request failed with status code 400"), {
      response: { data: { message } },
   });

/** A runner that answers each cell's code with `answer`, and records the order it was asked in. */
function runner(answer: (code: string) => Promise<unknown>) {
   const asked: number[] = [];
   const runCell: CellRunner = ({ cell, index }) => {
      asked.push(index);
      return answer(cell.text ?? "") as ReturnType<CellRunner>;
   };
   return { asked, runCell };
}

// A failed cell is logged as well as shown; the log is not under test.
let quiet: { mockRestore(): void } | undefined;
beforeEach(() => {
   clearCache();
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
      const { runCell } = runner(() => Promise.reject(error));
      render(
         <CellRunnerView
            resourceUri={URI}
            notebook={cells("run: a -> v")}
            runCell={runCell}
         />,
         { wrapper: serverWrapper },
      );

      expect(
         await screen.findByText("This cell could not be run."),
      ).toBeTruthy();
      expect(screen.getByText(shown)).toBeTruthy();
      expect(screen.getByText("Intro prose")).toBeTruthy();
   });

   it("still runs and shows the other cells", async () => {
      const { asked, runCell } = runner((code) =>
         code === "run: bad -> v"
            ? Promise.reject(refused("no such source"))
            : Promise.resolve({}),
      );
      render(
         <CellRunnerView
            resourceUri={URI}
            notebook={cells("run: bad -> v", "run: good -> v")}
            runCell={runCell}
         />,
         { wrapper: serverWrapper },
      );

      expect(await screen.findByText("no such source")).toBeTruthy();
      // The markdown cell is index 0 and never runs.
      expect(asked).toEqual([1, 2]);
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

   it("names the notebook that does not exist", () => {
      view({ status: 404, message: "Not found" });
      expect(screen.getByText("env > pkg > review.malloynb")).toBeTruthy();
      expect(screen.getByText(/not found\./)).toBeTruthy();
   });

   it("shows any other failure with the notebook it was reading", async () => {
      view({ status: 500, message: "The package did not load" });
      await waitFor(() =>
         expect(screen.getByText("env > pkg > review.malloynb")).toBeTruthy(),
      );
      expect(screen.getByText("The package did not load")).toBeTruthy();
      expect(screen.queryByText(/not found\./)).toBeNull();
   });
});
