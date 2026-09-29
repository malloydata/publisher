// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { beforeEach, expect, it, mock } from "bun:test";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import {
   clearCache,
   mockServerProvider,
   pending,
   serverWrapper,
} from "../../../test/serverProvider";
import type { RawNotebook } from "../../client";

const SERVED = {
   format: "malloy",
   notebookCells: [
      {
         type: "code",
         kind: "definition",
         text: 'import { orders } from "../orders.malloy"',
      },
      { type: "markdown", kind: "markdown", text: "Served prose" },
      {
         type: "code",
         kind: "definition",
         text: "source: recent is orders extend { where: year = 2025 }",
      },
      {
         type: "code",
         kind: "query",
         text: '#" Revenue by month\n# bar_chart\nrun: orders -> by_month',
      },
   ],
} as RawNotebook;

let current: RawNotebook = SERVED;
const getNotebook = mock(() => Promise.resolve({ data: current }));
const executeNotebookCell = mock(
   (
      _env: string,
      _pkg: string,
      _path: string,
      _index: number,
   ): Promise<never> => pending(),
);

mockServerProvider({
   notebooks: { getNotebook, executeNotebookCell },
   models: { executeQueryModel: () => pending() },
});

const { default: Notebook } = await import("./Notebook");

const URI =
   "publisher://environments/env/packages/pkg/models/notebooks/ops.malloy";

beforeEach(() => {
   current = SERVED;
   clearCache();
   getNotebook.mockClear();
   executeNotebookCell.mockClear();
});

it("runs only the query cell of a served notebook", async () => {
   render(<Notebook resourceUri={URI} />, { wrapper: serverWrapper });

   await waitFor(() => expect(executeNotebookCell).toHaveBeenCalled());
   expect(executeNotebookCell.mock.calls.map((call) => call[3])).toEqual([3]);
});

it("shows a query cell's caption above its result", async () => {
   render(<Notebook resourceUri={URI} />, { wrapper: serverWrapper });

   expect(await screen.findByText("Served prose")).toBeTruthy();
   expect(await screen.findByText("Revenue by month")).toBeTruthy();
});

it("folds a definition cell to a one-line summary and expands it to the code", async () => {
   const { container } = render(<Notebook resourceUri={URI} />, {
      wrapper: serverWrapper,
   });

   const toggle = await screen.findByRole("button", {
      name: /source: recent/,
   });
   expect(toggle.getAttribute("aria-expanded")).toBe("false");
   expect(container.textContent).not.toContain("year = 2025");

   fireEvent.click(toggle);

   expect(toggle.getAttribute("aria-expanded")).toBe("true");
   await waitFor(() => expect(container.textContent).toContain("year = 2025"));
   const region = container.querySelector(
      `[id="${toggle.getAttribute("aria-controls")}"]`,
   );
   expect(region?.textContent).toContain("year = 2025");
});

it("puts the copy-link icon on the first markdown cell, not on a leading definition", async () => {
   render(<Notebook resourceUri={URI} />, { wrapper: serverWrapper });

   await screen.findByText("Served prose");
   expect(screen.getAllByTestId("LinkOutlinedIcon")).toHaveLength(1);
});

it("keeps the copy-link icon off a .malloynb that opens with a code cell", async () => {
   current = {
      format: "malloynb",
      notebookCells: [
         { type: "code", text: "import { orders } from '../orders.malloy'" },
         { type: "markdown", text: "Legacy prose" },
      ],
   } as RawNotebook;
   render(<Notebook resourceUri={URI} />, { wrapper: serverWrapper });

   await screen.findByText("Legacy prose");
   expect(screen.queryAllByTestId("LinkOutlinedIcon")).toHaveLength(0);
});
