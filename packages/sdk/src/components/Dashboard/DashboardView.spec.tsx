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

describe("DashboardView gate givens", () => {
   // ORG is read only by the source's `#(authorize)` gate: the tile names it,
   // the control row does not declare it, and the host supplies it.
   const gated = {
      name: "ops",
      path: "dashboards/ops.malloy",
      givens: [{ name: "REGION", type: "string", label: "Region" }],
      tiles: [
         {
            kind: "query",
            query: "orders -> by_month",
            givenNames: ["ORG", "REGION"],
         },
         { kind: "query", query: "orders -> by_year", givenNames: ["REGION"] },
      ],
   } as DashboardManifest;

   const tileGivens = (query: string) =>
      executeQueryModel.mock.calls
         .map((call) => call[3] as { query?: string; givens?: unknown })
         .filter((request) => request.query === `run: ${query}`)
         .at(-1)?.givens;

   it("sends a host-supplied gate given to the tiles that name it, with no control for it", async () => {
      render(
         <DashboardView
            manifest={gated}
            environmentName="env"
            packageName="pkg"
            documentName="ops"
            givens={{ ORG: "1", REGION: "CA" }}
         />,
         { wrapper: serverWrapper },
      );

      await waitFor(() =>
         expect(tileGivens("orders -> by_month")).toEqual({
            ORG: "1",
            REGION: "CA",
         }),
      );
      expect(tileGivens("orders -> by_year")).toEqual({ REGION: "CA" });
      expect(screen.queryByText("ORG")).toBeNull();
   });
});

describe("DashboardView in text-source mode", () => {
   const document = {
      ...base,
      givens: [
         { name: "REGION", type: "string", label: "Region" },
         { name: "TENANT", type: "string", label: "Tenant" },
         { name: "ORG", type: "string", label: "Organization", secure: true },
      ],
      tiles: [
         { kind: "query", query: "a -> by_month", givenNames: ["REGION"] },
         { kind: "query", query: "gated -> total", restricted: true },
      ],
   } as DashboardManifest;

   const mountText = () =>
      render(
         <DashboardView
            manifest={document}
            environmentName="env"
            packageName="pkg"
            documentName="ops"
            preamble={"source: a is orders"}
            runModelPath="models/orders.malloy"
            hiddenGivens={["TENANT"]}
         />,
         { wrapper: serverWrapper },
      );

   it("runs each tile as the definitions plus one run: against the model, and nothing for a restricted tile", async () => {
      mountText();

      await waitFor(() => expect(executeQueryModel).toHaveBeenCalledTimes(1));
      const [, , modelPath, request] = executeQueryModel.mock.calls[0];
      expect(modelPath).toBe("models/orders.malloy");
      expect((request as { query?: string }).query).toBe(
         "source: a is orders\n\nrun: a -> by_month",
      );
      expect(
         await screen.findByText("You don't have access to this data"),
      ).toBeDefined();
   });

   it("shows no control for a host-hidden or #(secure) given", async () => {
      mountText();

      expect((await screen.findAllByText("Region")).length).toBeGreaterThan(0);
      expect(screen.queryByText("Tenant")).toBeNull();
      expect(screen.queryByText("Organization")).toBeNull();
   });

   it("answers a 403 on a tile with the access notice, where an ordinary view shows the error", async () => {
      executeQueryModel.mockImplementation(() =>
         Promise.reject({
            response: { status: 403, data: { code: 403, message: "denied" } },
         }),
      );
      const text = mountText();
      await waitFor(() =>
         expect(
            screen.getAllByText("You don't have access to this data").length,
         ).toBe(2),
      );
      expect(screen.queryByText("denied")).toBeNull();
      text.unmount();

      render(view(), { wrapper: serverWrapper });
      expect((await screen.findAllByText("denied")).length).toBeGreaterThan(0);
   });
});

it("leaves a #(secure) given's control alone outside text-source mode", async () => {
   render(
      <DashboardView
         manifest={
            {
               name: "ops",
               path: "dashboards/ops.malloy",
               givens: [
                  {
                     name: "ORG",
                     type: "string",
                     label: "Organization",
                     secure: true,
                  },
               ],
               tiles: [{ kind: "query", query: "a -> v", givenNames: ["ORG"] }],
            } as DashboardManifest
         }
         environmentName="env"
         packageName="pkg"
         documentName="ops"
      />,
      { wrapper: serverWrapper },
   );
   expect((await screen.findAllByText("Organization")).length).toBeGreaterThan(
      0,
   );
});
