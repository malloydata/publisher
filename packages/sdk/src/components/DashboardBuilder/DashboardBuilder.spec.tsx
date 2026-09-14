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
import { DashboardBuilder } from "./DashboardBuilder";
import type { DashboardDocument } from "./document";
import { readDashboardDocument, readFailed } from "./readDocument";

const SOURCE = `## artifact { title="Storefront" tiles=["a -> by_cat", "a -> by_brand"] } dashboard { columns=12 }
import "../data_app.malloy"

source: a is scoped_orders extend {
  // Kept, because a splice never rewrites what it did not change.
  # colspan=6
  # label="By category"
  view: by_cat is by_category

  # colspan=6
  view: by_brand is by_brand_view
}`;

const openDocument = async (source = SOURCE): Promise<DashboardDocument> => {
   const result = await readDashboardDocument(source);
   if (readFailed(result)) throw new Error(result.reason);
   return result.document;
};

const mount = async (onSave?: (source: string) => Promise<void> | void) => {
   const document = await openDocument();
   return render(
      <DashboardBuilder
         source={SOURCE}
         document={document}
         {...(onSave ? { onSave } : {})}
      />,
   );
};

const tile = (name: string) => screen.getByLabelText(`Tile ${name}`);
const selectTile = (name: string) => fireEvent.click(tile(name));
const field = (label: string) => screen.getByLabelText(label);
const button = (name: string) => screen.getByRole("button", { name });

/**
 * What the grid actually gives a tile.
 *
 * The layout is responsive (one column on a phone, the grid above `md`), so
 * `grid-column` lands in a media rule rather than on the element — reading
 * `style.gridColumn` would report nothing and pass whatever the component did.
 */
const gridColumnOf = (name: string): string => {
   const box = tile(name).parentElement;
   const className = Array.from(box?.classList ?? []).find((each) =>
      each.startsWith("css-"),
   );
   const declarations: string[] = [];
   const collect = (rules: CSSRuleList | undefined) => {
      for (const rule of Array.from(rules ?? [])) {
         if ("cssRules" in rule) collect((rule as CSSGroupingRule).cssRules);
         const text = rule.cssText;
         if (className && text.startsWith(`.${className} `))
            declarations.push(text);
      }
   };
   for (const sheet of Array.from(document.styleSheets))
      collect(sheet.cssRules);
   return declarations.join(" ");
};

describe("DashboardBuilder", () => {
   it("shows a tile per entry, titled as a reader would see it", async () => {
      await mount();
      // The labelled tile shows its label; the unlabelled one falls back to the
      // view name, which is what the dashboard itself does.
      expect(within(tile("by_cat")).getByText("By category")).toBeDefined();
      expect(within(tile("by_brand")).getByText("by_brand")).toBeDefined();
      expect(screen.getByText("Storefront")).toBeDefined();
   });

   // The point of taking the rule from `Dashboard` rather than restating it:
   // what you arrange is what a reader sees.
   it("lays tiles out on the dashboard's own grid rule", async () => {
      await mount();
      expect(gridColumnOf("by_cat")).toContain("grid-column: span 6");
   });

   it("edits a tile's label and shows it immediately", async () => {
      await mount();
      selectTile("by_cat");
      fireEvent.change(field("Label"), { target: { value: "Categories" } });
      expect(within(tile("by_cat")).getByText("Categories")).toBeDefined();
   });

   it("resizes a tile and moves it on the grid", async () => {
      await mount();
      selectTile("by_cat");
      fireEvent.change(field("Width (of 12)"), { target: { value: "4" } });
      expect(gridColumnOf("by_cat")).toContain("grid-column: span 4");
   });

   it("starts a new row when asked, which the grid has to express", async () => {
      await mount();
      selectTile("by_brand");
      fireEvent.click(field("Start a new row"));
      // An explicit start line, not just a span: that is what pushes it down.
      // (CSSOM gives the shorthand back without the spaces around the slash.)
      expect(gridColumnOf("by_brand")).toContain("grid-column: 1/span 6");
   });

   it("does not offer controls for a tile the model owns", async () => {
      const source = `## artifact { title="T" tiles=["orders -> by_brand"] }\nimport { orders } from '../orders.malloy'`;
      const document = await openDocument(source);
      render(<DashboardBuilder source={source} document={document} />);
      selectTile("by_brand");
      expect(screen.queryByLabelText("Label")).toBeNull();
      expect(screen.getByRole("alert").textContent).toContain(
         "declared on orders rather than in this dashboard",
      );
   });
});

describe("DashboardBuilder: history", () => {
   it("enables undo only once there is something to undo", async () => {
      await mount();
      expect(button("Undo")).toHaveProperty("disabled", true);
      selectTile("by_cat");
      fireEvent.change(field("Label"), { target: { value: "Categories" } });
      expect(button("Undo")).toHaveProperty("disabled", false);
   });

   it("takes a change back and puts it forward again", async () => {
      await mount();
      selectTile("by_cat");
      fireEvent.change(field("Label"), { target: { value: "Categories" } });

      fireEvent.click(button("Undo"));
      expect(within(tile("by_cat")).getByText("By category")).toBeDefined();

      fireEvent.click(button("Redo"));
      expect(within(tile("by_cat")).getByText("Categories")).toBeDefined();
   });
});

describe("DashboardBuilder: saving", () => {
   it("offers no save button when there is nowhere to save", async () => {
      await mount();
      expect(screen.queryByRole("button", { name: /Save|Saved/ })).toBeNull();
   });

   it("writes the change into the file, comment and all", async () => {
      let written: string | undefined;
      await mount((source) => {
         written = source;
      });

      selectTile("by_cat");
      fireEvent.change(field("Label"), { target: { value: "Categories" } });
      fireEvent.click(button("Save changes"));

      await waitFor(() => expect(written).toBeDefined());
      expect(written).toContain('# label="Categories"');
      expect(written).toContain(
         "  // Kept, because a splice never rewrites what it did not change.",
      );
      await waitFor(() => expect(button("Saved")).toBeDefined());
   });

   // The rule the writer rests on, shown through the surface: the work stays on
   // screen and the reason is visible.
   it("keeps the edit on screen when a save is refused", async () => {
      await mount(() => {
         throw new Error("disk full");
      });
      selectTile("by_cat");
      fireEvent.change(field("Label"), { target: { value: "Categories" } });
      fireEvent.click(button("Save changes"));

      await waitFor(() =>
         expect(screen.getByRole("alert").textContent).toContain("disk full"),
      );
      expect(within(tile("by_cat")).getByText("Categories")).toBeDefined();
      expect(button("Save changes")).toBeDefined();
   });
});
