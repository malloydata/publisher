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

const TITLED = {
   ...LAYOUT,
   dashboard: {
      ...LAYOUT.dashboard,
      title: "Tour title",
      description: "Tour **description**",
   },
} as RawNotebook;

let served: RawNotebook = LAYOUT;
const getNotebook = mock(() => Promise.resolve({ data: served }));
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
   served = LAYOUT;
   clearCache();
   executeNotebookCell.mockClear();
   executeQueryModel.mockClear();
});

it("renders a layout notebook as a grid report draws its tiles", async () => {
   const { container } = render(
      <Notebook resourceUri="publisher://environments/env/packages/pkg/models/notebooks/tour.malloy" />,
      { wrapper: serverWrapper },
   );

   expect((await screen.findByText("intro")).tagName).toBe("STRONG");
   // Text as prose, a query tile in a card: the same look as a grid.
   expect(
      (await screen.findByText("intro"))
         .closest("[data-chrome]")
         ?.getAttribute("data-chrome"),
   ).toBe("none");
   await waitFor(() =>
      expect(container.querySelector('[data-chrome="card"]')).not.toBeNull(),
   );
   // Tiles run through the model query endpoint, not the cell endpoint.
   await waitFor(() => expect(executeQueryModel).toHaveBeenCalled());
   expect(executeQueryModel.mock.calls[0]?.[2]).toBe("notebooks/tour.malloy");
   expect(executeNotebookCell).not.toHaveBeenCalled();
});

it("shows a layout notebook's title and description above its tiles", async () => {
   served = TITLED;
   render(
      <Notebook resourceUri="publisher://environments/env/packages/pkg/models/notebooks/tour.malloy" />,
      { wrapper: serverWrapper },
   );

   expect(
      (await screen.findByRole("heading", { name: "Tour title" })).tagName,
      // The page's main heading: the report title is the page.
   ).toBe("H1");
   expect((await screen.findByText("description")).tagName).toBe("STRONG");
});

it("titles an untitled layout notebook with its name, as a grid report does", async () => {
   render(
      <Notebook resourceUri="publisher://environments/env/packages/pkg/models/notebooks/tour.malloy" />,
      { wrapper: serverWrapper },
   );

   await screen.findByText("intro");
   expect(screen.getByRole("heading", { name: "tour" })).toBeDefined();
});
