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

   it("warns only on a tile that ignores some of the page's filters", async () => {
      const manifest = {
         ...base,
         givens: [
            { name: "REGION", type: "string", label: "Region" },
            { name: "BRAND", type: "string", label: "Brand" },
         ],
         tiles: [
            { kind: "text", name: "intro", markdown: "Hello **reader**" },
            {
               kind: "query",
               query: "orders -> all_apply",
               givenNames: ["BRAND", "REGION"],
            },
            {
               kind: "query",
               query: "orders -> one_missing",
               givenNames: ["REGION"],
            },
            // Unresolved: it runs with the whole row, so nothing is ignored.
            { kind: "query", query: "orders -> unresolved" },
         ],
      } as DashboardManifest;
      render(
         <DashboardView
            manifest={manifest}
            environmentName="env"
            packageName="pkg"
            documentName="ops"
         />,
         { wrapper: serverWrapper },
      );

      const tags = await screen.findAllByTestId("tile-filter-tag");
      expect(tags.map((tag) => tag.textContent)).toEqual([
         "Doesn't respond to Brand",
      ]);
      expect(tags[0].closest("[data-chrome]")?.textContent).toContain(
         "One missing",
      );
   });

   it("puts no warning on a single-query dashboard", async () => {
      render(
         <DashboardView
            manifest={
               {
                  name: "ops",
                  path: "dashboards/ops.malloy",
                  query: "overview",
                  givens: [{ name: "REGION", type: "string", label: "Region" }],
               } as DashboardManifest
            }
            environmentName="env"
            packageName="pkg"
            documentName="ops"
         />,
         { wrapper: serverWrapper },
      );
      await waitFor(() => expect(executeQueryModel).toHaveBeenCalled());
      expect(screen.queryByTestId("tile-filter-tag")).toBeNull();
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
