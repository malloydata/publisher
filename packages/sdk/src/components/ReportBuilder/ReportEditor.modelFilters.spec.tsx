// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A filter a tile reads through the model (the source's own `where:`) shows in
 * the builder's row as it does in the reader's, settable, but not removable or
 * configured: the builder never edits the model.
 */
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
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

const executeQueryModel = mock(
   (_env: string, _pkg: string, _path: string, _body: { query?: string }) =>
      pending(),
);
const getModel = mock((_env: string, _pkg: string, path: string) =>
   Promise.resolve({
      data:
         path === "README.malloy"
            ? { path, sourceText: packageFile }
            : path === "reports/recalls.malloy"
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
         executeQueryModel,
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

beforeEach(() => {
   clearCache();
   executeQueryModel.mockClear();
});

const open = (
   extra: { onOpenModel?: (modelPath: string) => void } = {},
   path = "README.malloy",
) =>
   render(
      <ReportEditor
         resourceUri="publisher://environments/env/packages/pkg"
         path={path}
         {...extra}
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

   it("says a filter this report's own source applies is the file's, not the model's", async () => {
      packageFile = `## artifact { title="Recalls" tiles=["recalls_tiles -> by_year"] } dashboard { columns=12 }
import "auto_recalls.malloy"

#(control) label="Since"
given: SINCE :: date is @2020-01-01

source: recalls_tiles is recalls extend {
  where: recall_date >= $SINCE
  view: by_year is by_year_view
}`;
      served = {
         givens: [SINCE],
         tiles: [
            {
               kind: "query",
               query: "recalls_tiles -> by_year",
               givenNames: ["SINCE"],
            },
         ],
      };
      open();
      expect(await row()).toEqual(["Since"]);
      fireEvent.click(screen.getByRole("button", { name: /^Since: / }));
      const popover = await screen.findByRole("dialog", {
         name: "Since filter",
      });
      expect(popover.textContent).not.toContain("Set in the model");
      expect(popover.textContent).toContain("recalls_tiles");
   });
});

describe("what a report says about a filter it cannot change", () => {
   const RECALLS = `## artifact { title="Recalls" tiles=["recalls_tiles -> by_year"] } dashboard { columns=12 }
import "auto_recalls.malloy"

source: recalls_tiles is recalls extend {
  view: by_year is by_year_view
}`;
   const READS_MANUFACTURER = {
      givens: [MANUFACTURER],
      startingGivens: { manufacturer_filter: "Ford" },
      tiles: [
         {
            kind: "query" as const,
            query: "recalls_tiles -> by_year",
            givenNames: ["manufacturer_filter"],
         },
      ],
   };

   it("can open the report with the model filter's current value", async () => {
      packageFile = RECALLS;
      served = READS_MANUFACTURER;
      open();
      fireEvent.click(
         await screen.findByRole("button", { name: /^Manufacturer: / }),
      );
      fireEvent.click(
         await screen.findByRole("button", {
            name: "Open the report with this value",
         }),
      );
      // The report now holds it: the popover says so, and Undo takes it back.
      expect(
         await screen.findByText("The report opens with this value."),
      ).toBeDefined();
      expect(
         (
            screen.getByRole("button", {
               name: "Undo",
               hidden: true,
            }) as HTMLButtonElement
         ).disabled,
      ).toBe(false);
   });

   it("opens the model by its package path, from a report in a folder", async () => {
      packageFile = RECALLS.replace(
         'import "auto_recalls.malloy"',
         'import "../auto_recalls.malloy"',
      );
      served = READS_MANUFACTURER;
      const onOpenModel = mock((_path: string) => {});
      open({ onOpenModel }, "reports/recalls.malloy");
      fireEvent.click(
         await screen.findByRole("button", { name: /^Manufacturer: / }),
      );
      fireEvent.click(
         await screen.findByRole("button", { name: "Open the model" }),
      );
      expect(onOpenModel).toHaveBeenCalledWith("auto_recalls.malloy");
   });
});

describe("a required filter the model reads, in the builder", () => {
   it("runs no tile that reads it and asks for it, as the reader does", async () => {
      packageFile = `## artifact { title="Recalls" tiles=["recalls_tiles -> by_year", "recalls_tiles -> everything"] } dashboard { columns=12 }
import "auto_recalls.malloy"

source: recalls_tiles is recalls extend {
  view: by_year is by_year_view
  view: everything is everything_view
}`;
      served = {
         givens: [
            {
               name: "RECALL_YEAR",
               label: "Recall Year",
               type: "number",
               required: true,
            },
         ],
         tiles: [
            {
               kind: "query",
               query: "recalls_tiles -> by_year",
               givenNames: ["RECALL_YEAR"],
            },
            {
               kind: "query",
               query: "recalls_tiles -> everything",
               givenNames: [],
            },
         ],
      };
      open();
      expect(
         await screen.findByText(
            "Pick a value for Recall Year to see this report.",
         ),
      ).toBeDefined();
      expect(screen.getByText("Waiting for Recall Year.")).toBeDefined();
      await waitFor(() => expect(executeQueryModel).toHaveBeenCalled());
      const queries = executeQueryModel.mock.calls.map((call) =>
         JSON.stringify(call[3]),
      );
      expect(queries.some((q) => q.includes("everything"))).toBe(true);
      expect(queries.some((q) => q.includes("by_year"))).toBe(false);
   });
});
