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
import { ReportBuilder } from "./ReportBuilder";
import { openDocument } from "./testing/fixtures";
import { closeMenu, editInline, viewOption } from "./testing/inline";

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
      <ReportBuilder
         source={SOURCE}
         document={document}
         catalog={catalog}
         {...(onSave ? { onSave } : {})}
      />,
   );
};

/** The open tile menu's Chart submenu: its choices, by their label. */
const primary = (item: HTMLElement) =>
   item.querySelector(".MuiListItemText-primary")?.textContent;
const chartItems = () => {
   fireEvent.click(
      screen.getByRole("menuitem", { name: /^Chart/, hidden: true }),
   );
   const menu = screen.getByRole("menu", { name: "Chart", hidden: true });
   return within(menu).getAllByRole("menuitemradio", { hidden: true });
};
const enabledCharts = () =>
   chartItems()
      .filter((o) => o.getAttribute("aria-disabled") !== "true")
      .map(primary);
const pickChart = (option: string) =>
   fireEvent.click(
      chartItems().find((o) => primary(o) === option) as HTMLElement,
   );
const chartItem = () =>
   screen.getByRole("menuitem", { name: /^Chart/, hidden: true });

describe("ReportBuilder: charts", () => {
   it("sets a tile's chart from its menu and writes the one line", async () => {
      let written: string | undefined;
      await mount((source) => {
         written = source;
      });
      fireEvent.click(screen.getByLabelText("Settings for by_cat"));
      const names = enabledCharts();
      // The wrapper's view `by_category` carries a shape map, so it is enabled; the rest are listed disabled.
      expect(names).toEqual([
         "Default",
         "Table",
         "Line",
         "Bar",
         "Scatter",
         "Shape map",
      ]);
      pickChart("Line");
      closeMenu();
      fireEvent.click(
         screen.getByRole("button", { name: "Save", hidden: true }),
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
      pickChart("Bar");
      pickChart("Default");
      closeMenu();
      const save = screen.queryByRole("button", {
         name: "Save",
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
      expect(chartItem().getAttribute("aria-disabled")).toBe("true");
      expect(screen.getByText(/is not one the builder models/)).toBeDefined();
      closeMenu();
      editInline("By brand", "Tile title", "Brands");
      fireEvent.click(
         screen.getByRole("button", { name: "Save", hidden: true }),
      );
      await waitFor(() => expect(written).toBeDefined());
      expect(written).toContain("  # bar_chart { size=spark }\n");
      expect(written).toContain('# label="Brands"');
   });

   it("disables the picker on a tile the model owns, with the reason", async () => {
      const source = `## artifact { title="T" tiles=["orders -> by_brand"] }\nimport { orders } from '../orders.malloy'`;
      const document = await openDocument(source);
      render(<ReportBuilder source={source} document={document} />);
      fireEvent.click(screen.getByLabelText("Settings for by_brand"));
      expect(chartItem().getAttribute("aria-disabled")).toBe("true");
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
      // The chart choices are only the ones the picked view can take.
      const chartChoicesShown = () =>
         within(screen.getByRole("group", { name: "Chart", hidden: true }))
            .getAllByRole("button", { hidden: true })
            .map((b) => b.textContent);
      fireEvent.click(viewOption("table_view"));
      expect(chartChoicesShown()).not.toContain("Big value");
      fireEvent.click(viewOption("kpis"));
      expect(chartChoicesShown()).toContain("Big value");
      fireEvent.click(
         within(
            screen.getByRole("group", { name: "Chart", hidden: true }),
         ).getByRole("button", { name: "Big value", hidden: true }),
      );
      fireEvent.click(screen.getByRole("button", { name: "Add tile" }));
      fireEvent.click(
         screen.getByRole("button", { name: "Save", hidden: true }),
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
      fireEvent.click(viewOption("odd view"));
      expect(
         screen.getByText(/"odd view" is not a plain Malloy name/),
      ).toBeDefined();
      expect(
         (screen.getByRole("button", { name: "Add tile" }) as HTMLButtonElement)
            .disabled,
      ).toBe(true);
   });
});

describe("ReportBuilder: charts on a view the report declares itself", () => {
   // A tile of big numbers whose body is written in the report: in no model
   // the catalog reads, so its shape comes from the report's own compile.
   const INLINE = `## artifact { title="Overview" tiles=["a -> kpis"] }
import { scoped_orders } from "../data_app.malloy"

source: a is scoped_orders extend {
  # label="Key figures"
  view: kpis is { aggregate: revenue, order_count }
}`;
   const withOwnViews = {
      ...catalog,
      documentViews: new Map([
         [
            "a",
            new Map([
               [
                  "kpis",
                  {
                     name: "kpis",
                     aggregateOnly: true,
                     shape: { groupBys: 0, totals: 2 },
                  },
               ],
            ]),
         ],
      ]),
   };

   it("offers big numbers and greys out every chart that needs an x-axis, before and after a switch", async () => {
      const document = await openDocument(INLINE);
      render(
         <ReportBuilder
            source={INLINE}
            document={document}
            catalog={withOwnViews}
         />,
      );
      fireEvent.click(screen.getByLabelText("Settings for Key figures"));
      expect(enabledCharts()).toEqual([
         "Default",
         "Table",
         "Big value",
         "Scatter",
      ]);
      const line = chartItems().find((o) => primary(o) === "Line");
      expect(line?.textContent).toContain(
         "This view has no field for the x-axis",
      );
      pickChart("Table");
      pickChart("Default");
      expect(enabledCharts()).toEqual([
         "Default",
         "Table",
         "Big value",
         "Scatter",
      ]);
   });

   it("says the shape is unknown, rather than what the view lacks, when no model reported it", async () => {
      const document = await openDocument(INLINE);
      render(
         <ReportBuilder
            source={INLINE}
            document={document}
            catalog={catalog}
         />,
      );
      fireEvent.click(screen.getByLabelText("Settings for Key figures"));
      const big = chartItems().find((o) => primary(o) === "Big value");
      expect(big?.textContent).toContain(
         "Not in the model's list, so its shape is unknown",
      );
   });
});

describe("ReportBuilder: a chart's color", () => {
   const colorItem = () =>
      screen.getByRole("menuitem", { name: /^Color/, hidden: true });
   const swatches = () => {
      fireEvent.click(colorItem());
      const menu = screen.getByRole("menu", { name: "Color", hidden: true });
      return within(menu).getAllByRole("menuitemradio", { hidden: true });
   };

   it("sets a bar chart's color from the theme's series and writes the one line", async () => {
      let written: string | undefined;
      await mount((source) => {
         written = source;
      });
      fireEvent.click(screen.getByLabelText("Settings for by_cat"));
      pickChart("Bar");
      const choices = swatches();
      // The theme's own colors first, then any other.
      expect(primary(choices[0])).toBe("Theme");
      const second = choices[2];
      const color = primary(second) as string;
      expect(color).toMatch(/^#[0-9a-f]{6}$/);
      fireEvent.click(second);
      closeMenu();
      fireEvent.click(
         screen.getByRole("button", { name: "Save", hidden: true }),
      );
      await waitFor(() => expect(written).toBeDefined());
      expect(written).toContain(`  # theme.palette.series=["${color}"]\n`);
      // And reopening the file reads it back.
      const reopened = await openDocument(written as string);
      const tile = reopened.tiles.find((t) => t.name === "by_cat");
      expect(tile && "color" in tile ? tile.color : undefined).toBe(color);
   });

   it("opens the platform's picker from the keyboard, where focus is on the item and not its input", async () => {
      await mount();
      fireEvent.click(screen.getByLabelText("Settings for by_cat"));
      pickChart("Bar");
      const other = swatches().at(-1) as HTMLElement;
      expect(primary(other)).toBe("Other color…");
      const input = within(other).getByLabelText("Other color");
      let opened = 0;
      input.addEventListener("click", () => opened++);
      fireEvent.keyDown(other, { key: "Enter" });
      fireEvent.keyDown(other, { key: " " });
      expect(opened).toBe(2);
   });

   // The row's swatch follows its value ("Theme", "#7f9862"); spaced like the
   // swatches in the Color menu it read as "Theme▢", touching the text.
   it("spaces the Color row's swatch from its value", async () => {
      await mount();
      fireEvent.click(screen.getByLabelText("Settings for by_cat"));
      pickChart("Bar");
      const swatch = colorItem().querySelector("[data-swatch]") as HTMLElement;
      expect(swatch.dataset.swatch).toBe("trailing");
      const style = getComputedStyle(swatch);
      expect(parseFloat(style.marginLeft)).toBeGreaterThan(0);
      expect(parseFloat(style.marginRight) || 0).toBe(0);
   });

   it("offers no color on a table, which draws no series", async () => {
      await mount();
      fireEvent.click(screen.getByLabelText("Settings for by_cat"));
      pickChart("Table");
      expect(colorItem().getAttribute("aria-disabled")).toBe("true");
      expect(screen.getByText(/has a color to set/)).toBeDefined();
   });
});
