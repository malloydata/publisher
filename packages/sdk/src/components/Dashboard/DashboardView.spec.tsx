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
});

describe("DashboardView chrome", () => {
   // A linear report and a grid draw the same page: query tiles in cards, text
   // tiles as bare prose, and the title block. `chrome` is accepted and ignored.
   for (const chrome of [undefined, "card", "none"] as const) {
      it(`draws query tiles as cards and text as prose, with chrome ${chrome ?? "unset"}`, async () => {
         const { container } = render(view(chrome), {
            wrapper: serverWrapper,
         });
         const strong = await screen.findByText("reader");
         expect(
            strong.closest("[data-chrome]")?.getAttribute("data-chrome"),
         ).toBe("none");
         expect(
            container.querySelectorAll('[data-chrome="card"]'),
         ).toHaveLength(2);
         expect(screen.getByRole("heading", { name: "ops" })).toBeDefined();
      });
   }
});

describe("DashboardView header", () => {
   it("draws a dashboard's description as bare page prose, not a card", async () => {
      render(
         <DashboardView
            manifest={{ ...base, description: "About **this**" }}
            environmentName="env"
            packageName="pkg"
            documentName="ops"
         />,
         { wrapper: serverWrapper },
      );
      const strong = await screen.findByText("this");
      expect(strong.closest("[data-chrome]")?.getAttribute("data-chrome")).toBe(
         "none",
      );
   });
});

describe("DashboardView layout marker", () => {
   it("marks the page with its layout, and a report that failed to load too", () => {
      const { container, unmount } = render(
         <DashboardView
            manifest={{ ...base, layout: "linear" }}
            environmentName="env"
            packageName="pkg"
            documentName="ops"
         />,
         { wrapper: serverWrapper },
      );
      expect(container.querySelector('[data-layout="linear"]')).not.toBeNull();
      unmount();

      const failed = render(
         <DashboardView
            manifest={{ ...base, layout: "grid", error: "did not compile" }}
            environmentName="env"
            packageName="pkg"
            documentName="ops"
         />,
         { wrapper: serverWrapper },
      );
      expect(
         failed.container.querySelector('[data-layout="grid"]'),
      ).not.toBeNull();
   });
});

describe("DashboardView control row", () => {
   it("draws no sticky row when the document declares no controls", async () => {
      const { container } = render(
         <DashboardView
            manifest={{ ...base, givens: [] }}
            environmentName="env"
            packageName="pkg"
            documentName="ops"
         />,
         { wrapper: serverWrapper },
      );
      await screen.findByText("reader");
      const sticky = Array.from(container.querySelectorAll("div")).filter(
         (el) => getComputedStyle(el).position === "sticky",
      );
      expect(sticky).toHaveLength(0);
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

   it("sends a list-valued host given to the tiles that name it", async () => {
      render(
         <DashboardView
            manifest={gated}
            environmentName="env"
            packageName="pkg"
            documentName="ops"
            givens={{ ORG: ["1", "2"], REGION: "CA" }}
         />,
         { wrapper: serverWrapper },
      );

      await waitFor(() =>
         expect(tileGivens("orders -> by_month")).toEqual({
            ORG: ["1", "2"],
            REGION: "CA",
         }),
      );
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
