// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   fireEvent,
   render,
   screen,
   waitFor,
   within,
} from "@testing-library/react";
import { describe, expect, it } from "bun:test";
import { chartLineText } from "./chartLine";
import { DashboardBuilder } from "./DashboardBuilder";
import { openDocument } from "./testing/fixtures";
import { closeMenu, editInline } from "./testing/inline";

const SOURCE = `## artifact { title="Storefront" tiles=["a -> by_cat", "a -> by_brand"] } dashboard { columns=12 }
import { scoped_orders } from "../data_app.malloy"

source: a is scoped_orders extend {
  # colspan=6
  view: by_cat is by_category

  # colspan=6
  # bar_chart { size=spark }
  view: by_brand is by_brand_view
}`;

const catalog = {
   sources: [
      {
         name: "scoped_orders",
         modelPath: "data_app.malloy",
         views: [
            { name: "by_category", chart: "shape_map" },
            { name: "by_brand_view" },
            { name: "kpis", aggregateOnly: true },
            { name: "table_view" },
            { name: "odd view" },
         ],
         givens: [],
         fields: [],
      },
   ],
};

const mount = async (onSave?: (source: string) => void) => {
   const document = await openDocument(SOURCE);
   return render(
      <DashboardBuilder
         source={SOURCE}
         document={document}
         catalog={catalog}
         {...(onSave ? { onSave } : {})}
      />,
   );
};

const chartPicker = (name: string) =>
   screen.getByRole("combobox", { name: `Viz type, ${name}`, hidden: true });
const optionsOf = (name: string) => {
   fireEvent.mouseDown(chartPicker(name));
   const menu = screen.getAllByRole("listbox", { hidden: true }).at(-1)!;
   return within(menu).getAllByRole("option", { hidden: true });
};
const enabledNames = (name: string) =>
   optionsOf(name)
      .filter((o) => o.getAttribute("aria-disabled") !== "true")
      .map((o) => o.textContent);
const choose = (name: string, option: string) =>
   fireEvent.click(
      optionsOf(name).find((o) => o.textContent === option) as HTMLElement,
   );

describe("DashboardBuilder: charts", () => {
   it("sets a tile's chart from its menu and writes the one line", async () => {
      let written: string | undefined;
      await mount((source) => {
         written = source;
      });
      fireEvent.click(screen.getByLabelText("Settings for by_cat"));
      const names = enabledNames("a by_cat");
      // The wrapper's view `by_category` carries a shape map, so it is enabled; the rest are listed disabled.
      expect(names).toEqual([
         "From the view",
         "Table",
         "Line",
         "Bar",
         "Scatter",
         "Shape map",
      ]);
      choose("a by_cat", "Line");
      closeMenu();
      fireEvent.click(
         screen.getByRole("button", { name: "Save changes", hidden: true }),
      );
      await waitFor(() => expect(written).toBeDefined());
      expect(written).toContain(
         `  ${chartLineText("line_chart", ["shape_map"])}\n  view: by_cat is by_category`,
      );
   });

   it("treats picking a chart and then Default as no edit on a tile with no chart line", async () => {
      let written: string | undefined;
      await mount((source) => {
         written = source;
      });
      fireEvent.click(screen.getByLabelText("Settings for by_cat"));
      choose("a by_cat", "Bar");
      choose("a by_cat", "From the view");
      closeMenu();
      const save = screen.queryByRole("button", {
         name: "Save changes",
         hidden: true,
      });
      expect(save === null || (save as HTMLButtonElement).disabled).toBe(true);
      expect(written).toBeUndefined();
   });

   it("shows a line the builder does not model as locked, and keeps it", async () => {
      let written: string | undefined;
      await mount((source) => {
         written = source;
      });
      fireEvent.click(screen.getByLabelText("Settings for by_brand"));
      expect(
         screen
            .getByRole("combobox", {
               name: "Viz type, a by_brand",
               hidden: true,
            })
            .getAttribute("aria-disabled"),
      ).toBe("true");
      expect(screen.getByText(/is not one the builder models/)).toBeDefined();
      closeMenu();
      editInline("by_brand", "Tile title", "Brands");
      fireEvent.click(
         screen.getByRole("button", { name: "Save changes", hidden: true }),
      );
      await waitFor(() => expect(written).toBeDefined());
      expect(written).toContain("  # bar_chart { size=spark }\n");
      expect(written).toContain('# label="Brands"');
   });

   it("disables the picker on a tile the model owns, with the reason", async () => {
      const source = `## artifact { title="T" tiles=["orders -> by_brand"] }\nimport { orders } from '../orders.malloy'`;
      const document = await openDocument(source);
      render(<DashboardBuilder source={source} document={document} />);
      fireEvent.click(screen.getByLabelText("Settings for by_brand"));
      expect(
         screen
            .getByRole("combobox", {
               name: "Viz type, orders by_brand",
               hidden: true,
            })
            .getAttribute("aria-disabled"),
      ).toBe("true");
      expect(
         screen.getByText(/so its chart is set in the model/),
      ).toBeDefined();
   });

   it("adds a tile with a chart, offering big value only for an aggregate-only view", async () => {
      let written: string | undefined;
      await mount((source) => {
         written = source;
      });
      fireEvent.click(
         screen.getByRole("button", { name: "Add tile", hidden: true }),
      );
      fireEvent.click(screen.getByLabelText("View table_view"));
      expect(enabledNames("new tile")).not.toContain("Big value");
      fireEvent.click(
         within(
            screen.getAllByRole("listbox", { hidden: true }).at(-1)!,
         ).getByRole("option", { name: "From the view", hidden: true }),
      );
      fireEvent.click(screen.getByLabelText("View kpis"));
      expect(enabledNames("new tile")).toContain("Big value");
      choose("new tile", "Big value");
      fireEvent.click(screen.getByRole("button", { name: "Add tile" }));
      fireEvent.click(
         screen.getByRole("button", { name: "Save changes", hidden: true }),
      );
      await waitFor(() => expect(written).toBeDefined());
      expect(written).toContain(
         `  ${chartLineText("big_value", [])}\n  view: kpis_tile is kpis`,
      );
   });

   it("says why a view the writer cannot name is not addable", async () => {
      await mount();
      fireEvent.click(
         screen.getByRole("button", { name: "Add tile", hidden: true }),
      );
      fireEvent.click(screen.getByLabelText("View odd view"));
      expect(
         screen.getByText(/"odd view" is not a plain Malloy name/),
      ).toBeDefined();
      expect(
         (screen.getByRole("button", { name: "Add tile" }) as HTMLButtonElement)
            .disabled,
      ).toBe(true);
   });
});
