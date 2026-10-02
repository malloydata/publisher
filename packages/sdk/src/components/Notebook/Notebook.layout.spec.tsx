// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { beforeEach, expect, it, mock } from "bun:test";
import { render, screen, waitFor } from "@testing-library/react";
import {
   clearCache,
   mockServerProvider,
   pending,
   serverWrapper,
} from "../../../test/serverProvider";
import type { RawNotebook } from "../../client";

const LAYOUT = {
   format: "malloy",
   notebookCells: [{ type: "markdown", kind: "markdown", text: "Intro" }],
   dashboard: {
      name: "tour",
      kind: "notebook",
      path: "notebooks/tour.malloy",
      dashboardColumns: 1,
      tiles: [
         { kind: "text", name: "intro", markdown: "Layout **intro**" },
         { kind: "query", query: "orders -> by_month", label: "Monthly" },
      ],
   },
} as RawNotebook;

const getNotebook = mock(() => Promise.resolve({ data: LAYOUT }));
const executeNotebookCell = mock(() => pending());
const executeQueryModel = mock(
   (_env: string, _pkg: string, _path: string, _request: unknown) => pending(),
);

mockServerProvider({
   notebooks: { getNotebook, executeNotebookCell },
   models: { executeQueryModel, getModel: () => pending() },
});

const { default: Notebook } = await import("./Notebook");

beforeEach(() => {
   clearCache();
   executeNotebookCell.mockClear();
   executeQueryModel.mockClear();
});

it("renders a layout notebook as a bare dashboard grid", async () => {
   const { container } = render(
      <Notebook resourceUri="publisher://environments/env/packages/pkg/models/notebooks/tour.malloy" />,
      { wrapper: serverWrapper },
   );

   expect((await screen.findByText("intro")).tagName).toBe("STRONG");
   expect(container.querySelector('[data-chrome="card"]')).toBeNull();
   // Tiles run through the model query endpoint, not the cell endpoint.
   await waitFor(() => expect(executeQueryModel).toHaveBeenCalled());
   expect(executeQueryModel.mock.calls[0]?.[2]).toBe("notebooks/tour.malloy");
   expect(executeNotebookCell).not.toHaveBeenCalled();
});
