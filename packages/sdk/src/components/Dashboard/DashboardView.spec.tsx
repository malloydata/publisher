// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { beforeEach, describe, expect, it, mock } from "bun:test";
import { render, screen, waitFor } from "@testing-library/react";
import {
   clearCache,
   mockServerProvider,
   pending,
   serverWrapper,
} from "../../../test/serverProvider";
import type { DashboardManifest } from "../../client";

const executeQueryModel = mock(
   (
      _environmentName: string,
      _packageName: string,
      _modelPath: string,
      _request: unknown,
   ) => pending(),
);

mockServerProvider({
   models: { executeQueryModel, getModel: () => pending() },
});

const { DashboardView } = await import("./DashboardView");

const base: DashboardManifest = {
   name: "ops",
   path: "dashboards/ops.malloy",
   dashboardColumns: 2,
   givens: [{ name: "REGION", type: "string", label: "Region" }],
   tiles: [
      { kind: "text", name: "intro", markdown: "Hello **reader**" },
      {
         kind: "query",
         query: "orders -> by_month",
         givenNames: ["REGION"],
         label: "Monthly",
      },
      // No `kind`: servers before text tiles omitted it.
      { query: "orders -> by_year", givenNames: [] },
   ],
} as DashboardManifest;

const view = (chrome?: "card" | "none") => (
   <DashboardView
      manifest={base}
      environmentName="env"
      packageName="pkg"
      documentName="ops"
      chrome={chrome}
   />
);

beforeEach(() => {
   clearCache();
   executeQueryModel.mockReset();
   executeQueryModel.mockImplementation(() => pending());
});

describe("DashboardView tiles", () => {
   it("renders a text tile's markdown with no heading and runs no query for it", async () => {
      render(view(), { wrapper: serverWrapper });

      const strong = await screen.findByText("reader");
      expect(strong.tagName).toBe("STRONG");
      // Two query tiles run; the text tile adds none.
      await waitFor(() => expect(executeQueryModel).toHaveBeenCalledTimes(2));
      const text = strong.closest("[data-chrome]");
      expect(text?.textContent).toBe("Hello reader");
   });

   it("treats a tile with no kind as a query", async () => {
      render(view(), { wrapper: serverWrapper });

      await waitFor(() => expect(executeQueryModel).toHaveBeenCalledTimes(2));
      const queries = executeQueryModel.mock.calls.map(
         (call) => (call[3] as { query?: string }).query,
      );
      expect(queries).toContain("run: orders -> by_year");
   });

   it("tags each query tile with the filters that apply to it", async () => {
      render(view(), { wrapper: serverWrapper });

      const tags = await screen.findAllByTestId("tile-filter-tag");
      // The by_year tile names no givens, so only the monthly tile is tagged.
      expect(tags.map((tag) => tag.textContent)).toEqual(["Region"]);
   });
});

describe("DashboardView chrome", () => {
   it("draws cards by default", async () => {
      const { container } = render(view(), { wrapper: serverWrapper });
      await screen.findByText("reader");
      expect(
         container.querySelectorAll('[data-chrome="card"]').length,
      ).toBeGreaterThan(0);
      expect(container.querySelector('[data-chrome="none"]')).toBeNull();
   });

   it("draws no cards and no title block with chrome none", async () => {
      const { container } = render(view("none"), { wrapper: serverWrapper });
      await screen.findByText("reader");
      expect(container.querySelector('[data-chrome="card"]')).toBeNull();
      expect(container.querySelectorAll('[data-chrome="none"]').length).toBe(3);
      expect(screen.queryByText("ops")).toBeNull();
   });
});
