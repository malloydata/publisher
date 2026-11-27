// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   cleanup,
   fireEvent,
   render,
   screen,
   waitFor,
} from "@testing-library/react";
import { afterEach, describe, expect, it, mock } from "bun:test";
import { ReportBuilder } from "./ReportBuilder";
import { openDocument } from "./testing/fixtures";
import type { ReportEvent } from "../Report/telemetry";
import { editInline, viewOption } from "./testing/inline";

const NOTEBOOK = `## artifact { layout=linear title="Review" tiles=[intro { kind=text }, "a -> by_cat"] }
import "../data_app.malloy"

##|(markdown) intro
Hello.
|##

source: a is scoped_orders extend {
  view: by_cat is by_category
}
`;

const WIDE = NOTEBOOK.replace(
   "  view: by_cat",
   "  # colspan=6\n  view: by_cat",
);

const DASHBOARD = `## artifact { title="Storefront" tiles=["a -> by_cat"] } dashboard { columns=12 }
import "../data_app.malloy"

source: a is scoped_orders extend {
  # colspan=12
  view: by_cat is by_category
}
`;

const catalog = {
   sources: [
      {
         name: "scoped_orders",
         modelPath: "data_app.malloy",
         views: [{ name: "sales_by_state" }],
         givens: [],
         fields: [],
      },
   ],
};

const button = (name: string) =>
   screen.getByRole("button", { name, hidden: true });

afterEach(cleanup);

const mountText = async (
   source: string,
   options: {
      onSave?: (source: string) => void;
      onEvent?: (event: ReportEvent) => void;
      withCatalog?: boolean;
   } = {},
) =>
   render(
      <ReportBuilder
         source={source}
         document={await openDocument(source)}
         {...(options.withCatalog ? { catalog } : {})}
         {...(options.onSave ? { onSave: options.onSave } : {})}
         {...(options.onEvent ? { onEvent: options.onEvent } : {})}
      />,
   );

describe("ReportBuilder: a notebook is one column", () => {
   it("draws its tiles as a grid does: query tiles in cards, text and description as prose", async () => {
      // The layouts differ in arrangement only, not in how a tile is drawn.
      await mountText(NOTEBOOK);
      const tile = (name: string) =>
         screen
            .getByLabelText(`Tile ${name}`)
            .querySelector("[data-chrome]")
            ?.getAttribute("data-chrome");
      expect(tile("intro")).toBe("none");
      expect(tile("By cat")).toBe("card");
      expect(
         screen
            .getByRole("group", { name: "Description" })
            .querySelector("[data-chrome]")
            ?.getAttribute("data-chrome"),
      ).toBe("none");
   });

   it("offers no width: no resize edge, no width presets", async () => {
      await mountText(NOTEBOOK);
      expect(
         screen.queryByRole("separator", { name: /^Width of / }),
      ).toBeNull();
      // A text tile has no menu; a query tile's offers no width.
      expect(screen.queryByLabelText("Settings for intro")).toBeNull();
      fireEvent.click(screen.getByLabelText("Settings for By cat"));
      expect(screen.queryByRole("button", { name: "Width ½" })).toBeNull();
   });

   it("does not nudge a width with the arrow keys", async () => {
      // A width left in the file by hand is not the builder's to rewrite.
      await mountText(WIDE, { onSave: () => {} });
      fireEvent.click(screen.getByLabelText("Tile By cat"));
      fireEvent.keyDown(window, { key: "ArrowLeft" });
      fireEvent.keyDown(window, { key: "ArrowRight" });
      expect(button("Saved")).toBeDefined();
   });

   it("adds a tile without a width, and never writes columns", async () => {
      let written: string | undefined;
      await mountText(NOTEBOOK, {
         withCatalog: true,
         onSave: (source) => {
            written = source;
         },
      });
      fireEvent.click(button("Add tile"));
      expect(screen.queryByLabelText("Tile width")).toBeNull();
      fireEvent.click(viewOption("sales_by_state"));
      fireEvent.click(screen.getByRole("button", { name: "Add tile" }));
      fireEvent.click(button("Save"));
      await waitFor(() => expect(written).toBeDefined());
      expect(written).toContain('"scoped_orders -> sales_by_state"');
      expect(written).not.toContain("query:");
      expect(written).not.toContain("colspan");
      expect(written).not.toContain("columns");
   });

   it("describes the Add tile dialog the same way for either kind", async () => {
      await mountText(NOTEBOOK, { withCatalog: true });
      fireEvent.click(button("Add tile"));
      expect(
         screen.getByText(
            "Pick what the tile shows from the model. You can change its title and chart now or later.",
         ),
      ).toBeDefined();
      cleanup();

      await mountText(DASHBOARD, { withCatalog: true });
      fireEvent.click(button("Add tile"));
      expect(
         screen.getByText(
            "Pick what the tile shows from the model. You can change its title and chart now or later.",
         ),
      ).toBeDefined();
   });

   it("inserts a tile between two tiles from the + on the edge between them", async () => {
      let written: string | undefined;
      await mountText(NOTEBOOK, {
         withCatalog: true,
         onSave: (source) => {
            written = source;
         },
      });
      fireEvent.click(screen.getByLabelText("Insert tile after intro"));
      fireEvent.click(screen.getByRole("button", { name: "Add text instead" }));
      fireEvent.click(button("Save"));
      await waitFor(() => expect(written).toBeDefined());
      expect(written).toContain(
         'tiles=[intro { kind=text }, text_1 { kind=text }, "a -> by_cat"]',
      );
   });

   it("adds a tile at the end from the section-level +", async () => {
      let written: string | undefined;
      await mountText(NOTEBOOK, {
         withCatalog: true,
         onSave: (source) => {
            written = source;
         },
      });
      fireEvent.click(screen.getByLabelText("Insert tile after intro"));
      fireEvent.keyDown(screen.getByRole("dialog"), { key: "Escape" });
      fireEvent.click(screen.getByLabelText("Add tile at the end"));
      fireEvent.click(screen.getByRole("button", { name: "Add text instead" }));
      fireEvent.click(button("Save"));
      await waitFor(() => expect(written).toBeDefined());
      expect(written).toContain(
         'tiles=[intro { kind=text }, "a -> by_cat", text_1 { kind=text }]',
      );
   });

   it("offers no insert + in a dashboard, where position is a grid cell", async () => {
      await mountText(DASHBOARD, { withCatalog: true });
      expect(screen.queryByLabelText(/^Insert tile after/)).toBeNull();
      expect(screen.queryByLabelText("Add tile at the end")).toBeNull();
   });

   it("reports a save with the cell count, the path and the linear layout", async () => {
      const onEvent = mock((_event: ReportEvent) => {});
      await mountText(NOTEBOOK, { onSave: () => {}, onEvent });
      editInline("By cat", "Tile title", "Categories");
      fireEvent.click(button("Save"));
      await waitFor(() => expect(onEvent).toHaveBeenCalledTimes(1));
      expect(onEvent.mock.calls[0][0]).toMatchObject({
         type: "report.saved",
         layout: "linear",
         tiles: 2,
         structural: false,
         where: "package",
      });
      expect(onEvent.mock.calls[0][0]).not.toHaveProperty("converted");
   });
});

describe("ReportBuilder: a document's kind", () => {
   it("is fixed once created: there is no switch between dashboard and notebook", async () => {
      await mountText(DASHBOARD, { onSave: () => {} });
      expect(screen.queryByRole("button", { name: "Settings" })).toBeNull();
      expect(screen.queryByLabelText("Show as")).toBeNull();
   });
});
