// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, mock } from "bun:test";
import {
   clearCache,
   mockServerProvider,
   pending,
   serverWrapper,
} from "../../../test/serverProvider";

/**
 * Every name the builder gives a tile, its aria labels included, is the title
 * the reader shows: the tile's own label, else the view's `# label`, else a
 * name derived from the view. Here the served manifest titles neither tile, as
 * a manifest from before the server read a query's inherited tags did, so the
 * view's `# label` has to come from the package's models.
 */
const packageFile = `## artifact { title="Flights" tiles=["by_carrier_tile", "flights -> by_month + { where: carrier = 'AA' }"] }
import { flights } from "../flights.malloy"

# viz=bar
query: by_carrier_tile is flights -> by_carrier + { where: carrier = 'UA' }`;

mockServerProvider(
   {
      models: {
         getModel: mock((_env: string, _pkg: string, path: string) =>
            Promise.resolve({
               data:
                  path === "dashboards/flights.malloy"
                     ? { modelPath: path, sourceText: packageFile }
                     : {
                          modelPath: path,
                          modelInfo: JSON.stringify({
                             entries: [{ kind: "source", name: "flights" }],
                          }),
                          sources: [
                             {
                                name: "flights",
                                views: [
                                   {
                                      name: "by_carrier",
                                      annotations: [
                                         '# label="Flights by airline"\n',
                                      ],
                                   },
                                   {
                                      name: "by_month",
                                      annotations: [
                                         '# label="Flights each month"\n',
                                      ],
                                   },
                                ],
                             },
                          ],
                          sourceInfos: [],
                       },
            }),
         ),
         executeQueryModel: mock(() => pending()),
         listModels: mock(() =>
            Promise.resolve({ data: [{ path: "flights.malloy" }] }),
         ),
      },
      reports: {
         getReport: mock(() =>
            Promise.resolve({
               data: {
                  path: "dashboards/flights.malloy",
                  givens: [],
                  tiles: [
                     {
                        kind: "query",
                        query: "by_carrier_tile",
                        givenNames: [],
                     },
                     {
                        kind: "query",
                        query: "flights -> by_month + { where: carrier = 'AA' }",
                        givenNames: [],
                     },
                  ],
               },
            }),
         ),
         listReports: mock(() => Promise.resolve({ data: [] })),
      },
   },
   { mutable: true },
);

const { ReportEditor } = await import("./ReportEditor");

beforeEach(() => clearCache());

describe("a tile's name in the builder", () => {
   const mount = () =>
      render(
         <ReportEditor
            environmentName="env"
            packageName="pkg"
            dashboardName="flights"
         />,
         { wrapper: serverWrapper },
      );

   it("is the view's # label for a named query, aria labels included", async () => {
      mount();
      expect(
         await screen.findByLabelText("Tile Flights by airline"),
      ).toBeDefined();
      expect(
         screen.getByLabelText("Settings for Flights by airline"),
      ).toBeDefined();
      expect(
         screen.queryByLabelText("Settings for by_carrier_tile"),
      ).toBeNull();
   });

   it("is the view's # label for a refined entry, not the refinement's text", async () => {
      mount();
      expect(
         await screen.findByLabelText("Tile Flights each month"),
      ).toBeDefined();
      expect(
         screen.getByLabelText("Settings for Flights each month"),
      ).toBeDefined();
      expect(screen.queryByText(/by_month \+ \{/)).toBeNull();
   });
});
