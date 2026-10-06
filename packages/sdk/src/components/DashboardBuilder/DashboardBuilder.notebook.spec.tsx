// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   act,
   cleanup,
   fireEvent,
   render,
   screen,
   waitFor,
} from "@testing-library/react";
import { afterEach, describe, expect, it, mock } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import { DashboardBuilder } from "./DashboardBuilder";
import { readForEditor } from "./readForEditor";
import { openDocument } from "./testing/fixtures";
import type { BuilderEvent } from "./telemetry";
import { editInline } from "./testing/inline";

const NOTEBOOK = `## artifact { kind=notebook title="Review" tiles=[intro { kind=text }, "a -> by_cat"] }
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

const REPO = path.resolve(import.meta.dir, "../../../../..");
const LEGACY = fs
   .readFileSync(
      path.join(REPO, "examples/storefront/notebooks/category-review.malloy"),
      "utf8",
   )
   .replace(/\r\n/g, "\n");

const button = (name: string) =>
   screen.getByRole("button", { name, hidden: true });

afterEach(cleanup);

const mountText = async (
   source: string,
   options: {
      onSave?: (source: string) => void;
      onEvent?: (event: BuilderEvent) => void;
      withCatalog?: boolean;
   } = {},
) =>
   render(
      <DashboardBuilder
         source={source}
         document={await openDocument(source)}
         {...(options.withCatalog ? { catalog } : {})}
         {...(options.onSave ? { onSave: options.onSave } : {})}
         {...(options.onEvent ? { onEvent: options.onEvent } : {})}
      />,
   );

describe("DashboardBuilder: a notebook is one column", () => {
   it("offers no width: no resize edge, no width presets", async () => {
      await mountText(NOTEBOOK);
      expect(
         screen.queryByRole("separator", { name: /^Width of / }),
      ).toBeNull();
      fireEvent.click(screen.getByLabelText("Settings for intro"));
      expect(screen.queryByRole("button", { name: "Width ½" })).toBeNull();
      fireEvent.click(screen.getByLabelText("Settings for by_cat"));
      expect(screen.queryByRole("button", { name: "Width ½" })).toBeNull();
   });

   it("does not nudge a width with the arrow keys", async () => {
      // A width left in the file by hand is not the builder's to rewrite.
      await mountText(WIDE, { onSave: () => {} });
      fireEvent.click(screen.getByLabelText("Tile by_cat"));
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
      fireEvent.click(screen.getByLabelText("View sales_by_state"));
      fireEvent.click(screen.getByRole("button", { name: "Add tile" }));
      fireEvent.click(button("Save"));
      await waitFor(() => expect(written).toBeDefined());
      expect(written).toContain("view: sales_by_state_tile is sales_by_state");
      expect(written).not.toContain("colspan");
      expect(written).not.toContain("columns");
   });

   it("names the document's own kind in the Add tile dialog", async () => {
      await mountText(NOTEBOOK, { withCatalog: true });
      fireEvent.click(button("Add tile"));
      expect(
         screen.getByText(
            "A tile shows one view of one source this notebook imports.",
         ),
      ).toBeDefined();
      cleanup();

      await mountText(DASHBOARD, { withCatalog: true });
      fireEvent.click(button("Add tile"));
      expect(
         screen.getByText(
            "A tile shows one view of one source this dashboard imports.",
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
      fireEvent.click(screen.getByRole("button", { name: "Text" }));
      fireEvent.click(screen.getByRole("button", { name: "Add text" }));
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
      fireEvent.click(screen.getByRole("button", { name: "Text" }));
      fireEvent.click(screen.getByRole("button", { name: "Add text" }));
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

   it("reports notebook events, with the cell count", async () => {
      const onEvent = mock((_event: BuilderEvent) => {});
      await mountText(NOTEBOOK, { onSave: () => {}, onEvent });
      editInline("by_cat", "Tile title", "Categories");
      fireEvent.click(button("Save"));
      await waitFor(() => expect(onEvent).toHaveBeenCalledTimes(1));
      expect(onEvent.mock.calls[0][0]).toMatchObject({
         type: "notebook.saved",
         cells: 2,
         structural: false,
         converted: false,
         where: "package",
      });
   });
});

describe("DashboardBuilder: a document's kind", () => {
   it("is fixed once created: there is no switch between dashboard and notebook", async () => {
      await mountText(DASHBOARD, { onSave: () => {} });
      expect(screen.queryByRole("button", { name: "Settings" })).toBeNull();
      expect(screen.queryByLabelText("Show as")).toBeNull();
   });
});

describe("DashboardBuilder: a notebook in the cell format", () => {
   const open = async (
      options: {
         onSave?: (source: string) => void;
         onEvent?: (event: BuilderEvent) => void;
         onDirtyChange?: (dirty: boolean) => void;
      } = {},
   ) => {
      const result = await readForEditor(LEGACY);
      if (result.ok === false) throw new Error(result.reason);
      if (!result.conversion) throw new Error("expected a conversion");
      render(
         <DashboardBuilder
            source={LEGACY}
            document={result.document}
            conversion={result.conversion}
            {...options}
         />,
      );
      return result.conversion;
   };

   it("opens unsaved, says what Save does, and is not edited", async () => {
      const onDirtyChange = mock((_dirty: boolean) => {});
      await open({ onSave: () => {}, onDirtyChange });
      expect(
         screen.getByText(
            /This notebook is in the cell format\. Saving rewrites it as a layout notebook, and a named query run once becomes that tile's view\./,
         ),
      ).toBeDefined();
      expect(button("Save")).toBeDefined();
      // Each tile says why it has no preview yet, beside the view it will run.
      expect(
         screen.getAllByText("Preview appears after you Save"),
      ).toHaveLength(3);
      expect(
         screen.getByText("order_items_tiles → revenue_by_month"),
      ).toBeDefined();
      // No edit has been made, so a host guarding navigation is told so.
      expect(onDirtyChange).toHaveBeenLastCalledWith(false);
   });

   it("asks before Save converts the file, and writes it once confirmed", async () => {
      const writes: string[] = [];
      await open({ onSave: (source) => void writes.push(source) });
      fireEvent.click(button("Save"));
      expect(
         await screen.findByText(
            /Saving rewrites this notebook in the tile layout/,
         ),
      ).toBeDefined();
      expect(writes).toHaveLength(0);
      fireEvent.click(button("Convert and save"));
      await waitFor(() => expect(writes).toHaveLength(1));
   });

   it("writes nothing when the conversion is cancelled", async () => {
      const writes: string[] = [];
      await open({ onSave: (source) => void writes.push(source) });
      fireEvent.click(button("Save"));
      fireEvent.click(await screen.findByRole("button", { name: "Cancel" }));
      await act(async () => {});
      expect(writes).toHaveLength(0);
      expect(button("Save")).toBeDefined();
   });

   it("saves the conversion exactly as converted", async () => {
      const writes: string[] = [];
      const onEvent = mock((_event: BuilderEvent) => {});
      const conversion = await open({
         onSave: (source) => void writes.push(source),
         onEvent,
      });
      fireEvent.click(button("Save"));
      fireEvent.click(
         await screen.findByRole("button", { name: "Convert and save" }),
      );
      await waitFor(() => expect(writes).toHaveLength(1));
      // Nothing was edited, so the file written is the conversion itself.
      expect(writes[0]).toBe(conversion.to);
      expect(onEvent.mock.calls[0][0]).toMatchObject({
         type: "notebook.saved",
         converted: true,
      });
      expect(
         screen.queryByText(/This notebook is in the cell format/),
      ).toBeNull();
   });
});
