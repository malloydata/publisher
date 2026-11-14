// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { beforeEach, describe, expect, it, mock } from "bun:test";
import { render, screen, waitFor } from "@testing-library/react";
import {
   clearCache,
   mockServerProvider,
   pending,
   serverWrapper,
} from "../../../../test/serverProvider";
import type { RawNotebook } from "../../../client";

const SERVED = {
   format: "malloynb",
   notebookCells: [
      { type: "markdown", text: "Served prose" },
      { type: "code", text: 'import { orders } from "../orders.malloy"' },
      { type: "code", text: "run: orders -> by_month" },
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
   "publisher://environments/env/packages/pkg/models/notebooks/ops.malloynb";

beforeEach(() => {
   current = SERVED;
   clearCache();
   getNotebook.mockClear();
   executeNotebookCell.mockClear();
});

it("runs every code cell of a .malloynb", async () => {
   render(<Notebook resourceUri={URI} />, { wrapper: serverWrapper });

   await waitFor(() =>
      expect(executeNotebookCell.mock.calls.map((call) => call[3])).toEqual([
         1, 2,
      ]),
   );
});

it("puts the copy-link icon on a leading markdown cell", async () => {
   render(<Notebook resourceUri={URI} />, { wrapper: serverWrapper });

   await screen.findByText("Served prose");
   expect(screen.getAllByTestId("LinkOutlinedIcon")).toHaveLength(1);
});

// DEPRECATED FEATURE COVERAGE — delete together with
// `.malloynb` notebooks (see RELEASE_NOTES deprecations).
describe("deprecated: .malloynb notebooks", () => {
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
});
