// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { render, screen, within } from "@testing-library/react";
import { beforeEach, describe, expect, it, mock } from "bun:test";
import type { DashboardManifest } from "../../client";
import {
   clearCache,
   mockServerProvider,
   pending,
   serverWrapper,
} from "../../../test/serverProvider";

/**
 * A tile the builder shows read-only keeps the title the reader gives it. Its
 * `# label` sits on a declaration the builder does not read (a named query, or
 * a view in the model), so the served manifest's label is the one to show.
 */
const packageFile = `## artifact { title="Flights" tiles=["by_carrier", "flights -> by_month"] }
import { flights } from "../flights.malloy"

# label="Flights by airline"
# bar_chart
query: by_carrier is flights -> { group_by: carrier; aggregate: flight_count }`;

const served: Partial<DashboardManifest> = {
   tiles: [
      {
         kind: "query",
         query: "by_carrier",
         label: "Flights by airline",
         givenNames: [],
      },
      {
         kind: "query",
         query: "flights -> by_month",
         label: "Flights each month",
         givenNames: [],
      },
   ],
};

mockServerProvider(
   {
      models: {
         getModel: mock((_env: string, _pkg: string, path: string) =>
            Promise.resolve({
               data:
                  path === "dashboards/flights.malloy"
                     ? { modelPath: path, sourceText: packageFile }
                     : { modelPath: path, sources: [], sourceInfos: [] },
            }),
         ),
         executeQueryModel: mock(() => pending()),
         listModels: mock(() => Promise.resolve({ data: [] })),
      },
      reports: {
         getReport: mock(() =>
            Promise.resolve({
               data: { path: "dashboards/flights.malloy", ...served },
            }),
         ),
         listReports: mock(() => Promise.resolve({ data: [] })),
      },
   },
   { mutable: true },
);

const { ReportEditor } = await import("./ReportEditor");

beforeEach(() => clearCache());

describe("a read-only tile's title in the builder", () => {
   it("is the served label, as the reader shows it, not one derived from the name", async () => {
      render(
         <ReportEditor
            environmentName="env"
            packageName="pkg"
            dashboardName="flights"
         />,
         { wrapper: serverWrapper },
      );
      const named = await screen.findByLabelText("Tile by_carrier");
      expect(
         await within(named).findByText("Flights by airline"),
      ).toBeDefined();
      expect(within(named).queryByText("By carrier")).toBeNull();
      // A view in the model is read-only too, and titled the same way.
      const inherited = screen.getByLabelText("Tile by_month");
      expect(within(inherited).getByText("Flights each month")).toBeDefined();
   });
});
