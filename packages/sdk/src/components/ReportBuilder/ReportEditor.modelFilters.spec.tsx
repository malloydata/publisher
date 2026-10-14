// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A filter a tile reads through the model (the source's own `where:`) shows in
 * the builder's row as it does in the reader's, settable, but not removable or
 * configured: the builder never edits the model.
 */
import { fireEvent, render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, mock } from "bun:test";
import type { DashboardManifest } from "../../client";
import {
   clearCache,
   mockServerProvider,
   pending,
   serverWrapper,
} from "../../../test/serverProvider";

let packageFile = "";
let served: Partial<DashboardManifest> = {};

const MANUFACTURER = {
   name: "manufacturer_filter",
   label: "Manufacturer",
   type: "filter<string>",
};
const SUBJECT = {
   name: "subject_filter",
   label: "Subject",
   type: "filter<string>",
};
const SINCE = { name: "SINCE", label: "Since", type: "date" };

const getModel = mock((_env: string, _pkg: string, path: string) =>
   Promise.resolve({
      data:
         path === "README.malloy"
            ? { path, sourceText: packageFile }
            : { path, sources: [], sourceInfos: [] },
   }),
);
const getReport = mock(() =>
   Promise.resolve({ data: { path: "README.malloy", tiles: [], ...served } }),
);
mockServerProvider(
   {
      models: {
         getModel,
         executeQueryModel: mock(() => pending()),
         listModels: mock(() => Promise.resolve({ data: [] })),
      },
      reports: {
         getReport,
         listReports: mock(() => Promise.resolve({ data: [] })),
      },
   },
   { mutable: true },
);

const { ReportEditor } = await import("./ReportEditor");

beforeEach(() => clearCache());

const open = () =>
   render(
      <ReportEditor
         resourceUri="publisher://environments/env/packages/pkg"
         path="README.malloy"
      />,
      { wrapper: serverWrapper },
   );

/** The filter buttons in the row, by their names as a reader hears them. */
const row = async () =>
   (
      await screen.findAllByRole("button", {
         name: /^(Manufacturer|Subject|Since): /,
      })
   ).map((button) => button.getAttribute("aria-label")?.split(":")[0]);

describe("filters read through the model", () => {
   it("are in the builder's row as in the reader's, with no remove and no settings", async () => {
      packageFile = `## artifact { title="Recalls" tiles=["recalls_tiles -> by_year", "recalls_tiles -> by_maker"] } dashboard { columns=12 }
import "auto_recalls.malloy"

source: recalls_tiles is recalls extend {
  view: by_year is by_year_view
  view: by_maker is by_maker_view
}`;
      served = {
         givens: [MANUFACTURER, SUBJECT],
         tiles: [
            {
               kind: "query",
               query: "recalls_tiles -> by_year",
               givenNames: ["manufacturer_filter"],
            },
            {
               kind: "query",
               query: "recalls_tiles -> by_maker",
               givenNames: ["manufacturer_filter", "subject_filter"],
            },
         ],
      };
      open();
      expect(await row()).toEqual(["Manufacturer", "Subject"]);
      expect(
         screen.queryByRole("button", {
            name: "Remove filter manufacturer_filter",
         }),
      ).toBeNull();

      fireEvent.click(screen.getByRole("button", { name: /^Manufacturer: / }));
      const popover = await screen.findByRole("dialog", {
         name: "Manufacturer filter",
      });
      expect(popover.textContent).toContain(
         "Set in the model auto_recalls.malloy. Edit the model to change it.",
      );
      expect(
         screen.queryByRole("button", {
            name: "Edit filter manufacturer_filter",
         }),
      ).toBeNull();
   });

   it("come after this file's own bound filters, which stay removable", async () => {
      packageFile = `## artifact { title="Recalls" tiles=["recalls_tiles -> by_year"] } dashboard { columns=12 }
import "auto_recalls.malloy"

#(control) label="Since"
given: SINCE :: date is @2020-01-01

source: recalls_tiles is recalls extend {
  view: by_year is by_year_view + { where: recall_date >= $SINCE }
}`;
      served = {
         givens: [SINCE, MANUFACTURER],
         tiles: [
            {
               kind: "query",
               query: "recalls_tiles -> by_year",
               givenNames: ["SINCE", "manufacturer_filter"],
            },
         ],
      };
      open();
      expect(await row()).toEqual(["Since", "Manufacturer"]);
      expect(
         screen.getByRole("button", { name: "Remove filter SINCE" }),
      ).toBeTruthy();
      expect(
         screen.queryByRole("button", {
            name: "Remove filter manufacturer_filter",
         }),
      ).toBeNull();
   });
});
