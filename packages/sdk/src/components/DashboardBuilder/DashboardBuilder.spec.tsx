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
  view: by_cat is by_category + { where: cat ~ $CATEGORY }

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

/**
 * Select a tile AND open its settings.
 *
 * A tile's presentation is edited from a popover on the tile itself rather than
 * a panel under the page, so reaching those controls means opening it. Width
 * and position are not in here at all — they are dragged.
 */
const selectTile = (name: string) => fireEvent.click(tile(name));

/**
 * Retitle a tile from its menu.
 *
 * The one on-tile edit a test can drive by clicking. Width and position are
 * dragged, which needs real element geometry that jsdom does not provide, a
 * tile's row is set by dragging it into a gap, and filters are configured
 * only from the strip under the header.
 */
const retitle = (title: string, next: string) => {
   fireEvent.click(screen.getByLabelText(`Settings for ${title}`));
   const field = screen.getByLabelText("Tile title");
   fireEvent.change(field, { target: { value: next } });
   fireEvent.keyDown(field, { key: "Escape" });
};
const button = (name: string) => screen.getByRole("button", { name });

/**
 * Every style rule the grid puts on a tile's item — its column, and the
 * minimum that lets it narrow.
 *
 * The layout is responsive (one column on a phone, the grid above `md`), so
 * `grid-column` lands in a media rule rather than on the element — reading
 * `style.gridColumn` would report nothing and pass whatever the component did.
 */
const itemStyleOf = (name: string): string => {
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
      expect(itemStyleOf("by_cat")).toContain("grid-column: span 6");
   });

   // Narrowing a tile has to narrow it. A grid item's minimum width is its
   // content's, and a chart's content is an SVG as wide as the tile WAS — so
   // without an explicit zero the tile could grow but never shrink, and the
   // renderer never redrew for a size change it never saw.
   it("lets a tile narrow below whatever it holds", async () => {
      await mount();
      expect(itemStyleOf("by_cat")).toContain("min-width: 0;");
   });

   // Filters are configured in ONE place, the strip: a control's window is
   // where a tile is bound or unbound. Nothing on the tile edits a binding.
   it("unbinds a tile from the control's window, and nowhere else", async () => {
      let written: string | undefined;
      await mount((source) => {
         written = source;
      });
      expect(
         within(tile("by_cat")).queryByLabelText(/^Remove filter /),
      ).toBeNull();

      fireEvent.click(screen.getByLabelText("Edit filter CATEGORY"));
      fireEvent.click(screen.getByLabelText("Filter By category"));
      fireEvent.click(screen.getByRole("button", { name: "Apply" }));
      fireEvent.click(
         screen.getByRole("button", { name: "Save changes", hidden: true }),
      );
      await waitFor(() => expect(written).toBeDefined());
      expect(written).toContain("view: by_cat is by_category\n");
      expect(written).not.toContain("$CATEGORY");
   });

   it("offers no resize for a tile the model owns", async () => {
      const source = `## artifact { title="T" tiles=["orders -> by_brand"] }\nimport { orders } from '../orders.malloy'`;
      const document = await openDocument(source);
      render(<DashboardBuilder source={source} document={document} />);
      selectTile("by_brand");
      // Its tags live on the model's source, which the builder does not write,
      // so it gets no resize handle — offering one would offer a drag the
      // writer then refuses. It keeps its grip: order is this file's own
      // `tiles=[…]` array, which it owns for every tile.
      expect(within(tile("by_brand")).queryByLabelText(/^Resize /)).toBeNull();
      expect(within(tile("by_brand")).getByLabelText(/^Move /)).toBeDefined();
   });

   // `renderTile` hands over the WHOLE tile, card and heading included, because
   // a real `DashboardTile` draws both. Filling a card of ours would show a card
   // in a card under two titles, which is the opposite of the point.
   it("hands the whole tile to renderTile, adding no heading of its own", async () => {
      const document = await openDocument();
      render(
         <DashboardBuilder
            source={SOURCE}
            document={document}
            renderTile={(each) => <article>a real {each.name}</article>}
         />,
      );
      expect(within(tile("by_cat")).getByText(/a real by_cat/)).toBeDefined();
      // The label is the caller's to draw now, so the surface does not also
      // print it, and the placeholder is gone.
      expect(screen.queryByText("By category")).toBeNull();
      expect(screen.queryByText(/a → by_cat/)).toBeNull();
   });

   // Selecting is a thing you do while arranging, so it must not move what you
   // are arranging: an outline is drawn outside the box and takes no space.
   it("marks selection without resizing the tile", async () => {
      const document = await openDocument();
      render(
         <DashboardBuilder
            source={SOURCE}
            document={document}
            renderTile={(each) => <article>a real {each.name}</article>}
         />,
      );
      const before = itemStyleOf("by_cat");
      selectTile("by_cat");
      expect(tile("by_cat").getAttribute("aria-current")).toBe("true");
      expect(itemStyleOf("by_cat")).toBe(before);
   });

   // The whole card is the drag target, so a press anywhere on it is how a
   // move begins — and, below the drag threshold, how a tile is selected. The
   // drag itself needs geometry jsdom does not have; the press it can drive.
   it("selects a tile from a press anywhere on it, not only the grip", async () => {
      await mount();
      fireEvent.pointerDown(tile("by_brand"), { button: 0 });
      expect(tile("by_brand").getAttribute("aria-current")).toBe("true");
      // Released without travelling: a click, so nothing was reordered and the
      // page is left as it was.
      fireEvent.pointerUp(window);
      expect(tile("by_brand").getAttribute("aria-current")).toBe("true");
   });

   it("ignores a press with any button but the primary", async () => {
      await mount();
      fireEvent.pointerDown(tile("by_brand"), { button: 2 });
      expect(tile("by_brand").getAttribute("aria-current")).toBe("false");
   });
});

describe("DashboardBuilder: the dashboard's filters", () => {
   // The convention, end to end through the surface: a filter added here is a
   // declaration in THIS file plus a binding on each ticked tile, and the save
   // writes exactly that.
   it("adds a filter declared in this dashboard and writes it to the file", async () => {
      let written: string | undefined;
      await mount((source) => {
         written = source;
      });
      // The file binds CATEGORY without declaring it, so that chip is the
      // model's: removable here too, which takes it off every tile.
      expect(screen.getByLabelText("Edit filter CATEGORY")).toBeDefined();
      expect(screen.getByLabelText("Remove control CATEGORY")).toBeDefined();

      fireEvent.click(button("Add filter"));
      fireEvent.change(screen.getByLabelText("Field to filter"), {
         target: { value: "brand" },
      });
      fireEvent.change(screen.getByLabelText("Control label"), {
         target: { value: "Brand" },
      });
      fireEvent.click(
         screen.getByRole("button", { name: "Add filter", hidden: false }),
      );

      // Declared here, so its chip can be removed; and bound on both tiles.
      expect(screen.getByLabelText("Edit filter BRAND")).toBeDefined();
      expect(screen.getByLabelText("Remove control BRAND")).toBeDefined();

      // The window's exit transition hides the rest of the page from assistive
      // tech until it ends, and the test runner never ends it; the header's
      // button has to be reached through that.
      fireEvent.click(
         screen.getByRole("button", { name: "Save changes", hidden: true }),
      );
      await waitFor(() => expect(written).toBeDefined());
      expect(written).toContain(
         '# label="Brand" control=select suggest { source=scoped_orders dimension=brand }\n' +
            "given: BRAND :: filter<string> is f''",
      );
      expect(written).toContain(
         "view: by_cat is by_category + { where: cat ~ $CATEGORY, where: brand ~ $BRAND }",
      );
      expect(written).toContain(
         "view: by_brand is by_brand_view + { where: brand ~ $BRAND }",
      );
   });

   it("binds a given the model offers, comparing the way its type needs", async () => {
      let written: string | undefined;
      const document = await openDocument();
      render(
         <DashboardBuilder
            source={SOURCE}
            document={document}
            givens={[
               {
                  name: "SINCE",
                  type: "date",
                  label: "Since",
                  field: "created_at",
               },
            ]}
            onSave={(source) => {
               written = source;
            }}
         />,
      );
      fireEvent.click(button("Add filter"));
      fireEvent.click(screen.getByRole("button", { name: "From the model" }));
      fireEvent.click(
         screen.getByRole("button", { name: "Add filter", hidden: false }),
      );

      fireEvent.click(
         screen.getByRole("button", { name: "Save changes", hidden: true }),
      );
      await waitFor(() => expect(written).toBeDefined());
      // A `date` is a value, not a filter expression: `>=`, and no declaration
      // of ours, since the model's is the one that binds.
      expect(written).toContain("where: created_at >= $SINCE");
      expect(written).not.toContain("given: SINCE");
   });

   it("takes a control it declared off the dashboard, bindings and all", async () => {
      let written: string | undefined;
      const source = SOURCE.replace(
         'import "../data_app.malloy"',
         'import "../data_app.malloy"\n\n# label="Category"\ngiven: CATEGORY :: filter<string> is f\'\'',
      );
      const document = await openDocument(source);
      render(
         <DashboardBuilder
            source={source}
            document={document}
            onSave={(next) => {
               written = next;
            }}
         />,
      );
      fireEvent.click(screen.getByLabelText("Remove control CATEGORY"));
      expect(screen.queryByLabelText("Edit filter CATEGORY")).toBeNull();

      // One history entry for the whole removal.
      fireEvent.click(button("Undo"));
      expect(screen.getByLabelText("Edit filter CATEGORY")).toBeDefined();
      fireEvent.click(button("Redo"));

      fireEvent.click(button("Save changes"));
      await waitFor(() => expect(written).toBeDefined());
      expect(written).not.toContain("CATEGORY");
      expect(written).toContain("view: by_cat is by_category\n");
   });
});

describe("DashboardBuilder: a control the model declares", () => {
   // Its declaration is not ours to delete, but a control is a given some tile
   // binds, so taking it off the dashboard is unbinding every tile.
   it("can still be taken off the dashboard", async () => {
      let written: string | undefined;
      await mount((source) => {
         written = source;
      });
      fireEvent.click(screen.getByLabelText("Remove control CATEGORY"));
      expect(screen.queryByLabelText("Edit filter CATEGORY")).toBeNull();
      fireEvent.click(button("Save changes"));
      await waitFor(() => expect(written).toBeDefined());
      expect(written).toContain("view: by_cat is by_category\n");
      expect(written).not.toContain("$CATEGORY");
   });
});

describe("DashboardBuilder: a tile's own settings", () => {
   it("retitles a tile from its menu, as one history entry", async () => {
      await mount();
      fireEvent.click(screen.getByLabelText("Settings for By category"));
      fireEvent.change(screen.getByLabelText("Tile title"), {
         target: { value: "Revenue by category" },
      });
      fireEvent.change(screen.getByLabelText("Tile subtitle"), {
         target: { value: "Net of returns" },
      });
      // Closing commits, once.
      fireEvent.keyDown(screen.getByLabelText("Tile title"), { key: "Escape" });
      expect(
         within(tile("by_cat")).getByText("Revenue by category"),
      ).toBeDefined();
      expect(within(tile("by_cat")).getByText("Net of returns")).toBeDefined();
      fireEvent.click(button("Undo"));
      expect(within(tile("by_cat")).getByText("By category")).toBeDefined();
      expect(within(tile("by_cat")).queryByText("Net of returns")).toBeNull();
   });

   it("offers no title to a tile the model owns", async () => {
      const source = `## artifact { title="T" tiles=["orders -> by_brand"] }\nimport { orders } from '../orders.malloy'`;
      const document = await openDocument(source);
      render(<DashboardBuilder source={source} document={document} />);
      fireEvent.click(screen.getByLabelText("Settings for by_brand"));
      expect(screen.queryByLabelText("Tile title")).toBeNull();
      expect(screen.getByText(/Declared on its source/)).toBeDefined();
   });
});

describe("DashboardBuilder: keyboard", () => {
   it("undoes and redoes from the keyboard, and never from inside a text field", async () => {
      await mount();
      retitle("By category", "Renamed");
      expect(within(tile("by_cat")).getByText("Renamed")).toBeDefined();

      fireEvent.keyDown(window, { key: "z", ctrlKey: true, metaKey: true });
      expect(within(tile("by_cat")).getByText("By category")).toBeDefined();
      fireEvent.keyDown(window, {
         key: "z",
         ctrlKey: true,
         metaKey: true,
         shiftKey: true,
      });
      expect(within(tile("by_cat")).getByText("Renamed")).toBeDefined();

      // Typing in a field is typing, not editing the document.
      fireEvent.click(screen.getByLabelText("Settings for Renamed"));
      const field = screen.getByLabelText("Tile title");
      fireEvent.keyDown(field, { key: "z", ctrlKey: true, metaKey: true });
      expect(within(tile("by_cat")).getByText("Renamed")).toBeDefined();
   });

   it("nudges the selected tile's width with the arrow keys", async () => {
      await mount();
      selectTile("by_cat");
      fireEvent.keyDown(window, { key: "ArrowLeft" });
      expect(itemStyleOf("by_cat")).toContain("grid-column: span 5");
      fireEvent.keyDown(window, { key: "ArrowRight" });
      fireEvent.keyDown(window, { key: "ArrowRight" });
      expect(itemStyleOf("by_cat")).toContain("grid-column: span 7");
   });
});

describe("DashboardBuilder: history", () => {
   it("enables undo only once there is something to undo", async () => {
      await mount();
      expect(button("Undo")).toHaveProperty("disabled", true);
      retitle("By category", "Renamed");
      expect(button("Undo")).toHaveProperty("disabled", false);
   });

   it("takes a change back and puts it forward again", async () => {
      await mount();
      retitle("By category", "Renamed");
      expect(within(tile("by_cat")).getByText("Renamed")).toBeDefined();

      fireEvent.click(button("Undo"));
      expect(within(tile("by_cat")).getByText("By category")).toBeDefined();

      fireEvent.click(button("Redo"));
      expect(within(tile("by_cat")).getByText("Renamed")).toBeDefined();
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

      retitle("By category", "Renamed");
      fireEvent.click(button("Save changes"));

      await waitFor(() => expect(written).toBeDefined());
      // The tag is rewritten in place; the declaration is otherwise untouched,
      // and so is the comment in the block.
      expect(written).toContain('# label="Renamed"');
      expect(written).not.toContain('# label="By category"');
      expect(written).toContain(
         "  view: by_cat is by_category + { where: cat ~ $CATEGORY }",
      );
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
      retitle("By category", "Renamed");
      fireEvent.click(button("Save changes"));

      await waitFor(() =>
         expect(screen.getByRole("alert").textContent).toContain("disk full"),
      );
      // The edit is still on screen and still unsaved.
      expect(within(tile("by_cat")).getByText("Renamed")).toBeDefined();
      expect(button("Save changes")).toBeDefined();
   });
});
