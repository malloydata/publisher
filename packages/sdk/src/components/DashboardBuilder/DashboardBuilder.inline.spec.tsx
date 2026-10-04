// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   cleanup,
   fireEvent,
   render,
   screen,
   waitFor,
   within,
} from "@testing-library/react";
import { afterEach, describe, expect, it, mock } from "bun:test";
import { DashboardBuilder } from "./DashboardBuilder";
import { openDocument } from "./testing/fixtures";
import { editInline } from "./testing/inline";
import { TEXT_TILE_PLACEHOLDER } from "./TextTileBody";

const SOURCE = `## artifact { title="Storefront" tiles=[intro { kind=text colspan=12 }, "a -> by_cat", "a -> by_brand"] } dashboard { columns=12 }
import "../data_app.malloy"

##|(markdown) intro
Read [the docs](https://example.com/docs).
|##

source: a is scoped_orders extend {
  # colspan=6
  # label="By category"
  view: by_cat is by_category

  # colspan=6
  view: by_brand is by_category
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

const mount = async (
   options: {
      onSave?: (source: string) => void;
      savesTo?: "package" | "browser" | "host";
      saveLabel?: string;
   } = {},
) => {
   const document = await openDocument(SOURCE);
   return render(
      <DashboardBuilder
         source={SOURCE}
         document={document}
         catalog={catalog}
         {...(options.onSave ? { onSave: options.onSave } : {})}
         {...(options.savesTo ? { savesTo: options.savesTo } : {})}
         {...(options.saveLabel ? { saveLabel: options.saveLabel } : {})}
      />,
   );
};

const tile = (name: string) => screen.getByLabelText(`Tile ${name}`);
const button = (name: string) =>
   screen.getByRole("button", { name, hidden: true });
const undo = () => fireEvent.click(button("Undo"));

afterEach(cleanup);

describe("click-to-edit text", () => {
   it("opens on a click, commits on Enter, and is one undo step", async () => {
      await mount();
      editInline("By category", "Tile title", "Revenue");
      expect(within(tile("by_cat")).getByText("Revenue")).toBeDefined();
      undo();
      expect(within(tile("by_cat")).getByText("By category")).toBeDefined();
      expect(button("Undo")).toHaveProperty("disabled", true);
   });

   it("puts the old text back on Escape, with no history entry", async () => {
      await mount();
      editInline("By category", "Tile title", "Discarded", "Escape");
      expect(within(tile("by_cat")).getByText("By category")).toBeDefined();
      expect(screen.queryByLabelText("Tile title")).toBeNull();
      expect(button("Undo")).toHaveProperty("disabled", true);
   });

   it("commits when focus leaves the field", async () => {
      await mount();
      fireEvent.click(screen.getByText("By category"));
      const field = screen.getByLabelText("Tile title");
      fireEvent.change(field, { target: { value: "Left behind" } });
      fireEvent.blur(field);
      expect(within(tile("by_cat")).getByText("Left behind")).toBeDefined();
   });

   it("adds no history entry for text that did not change", async () => {
      await mount();
      editInline("By category", "Tile title", "By category");
      expect(button("Undo")).toHaveProperty("disabled", true);
   });

   it("opens from the keyboard: the display is a tab stop that Enter and Space open", async () => {
      await mount();
      const display = screen.getByText("By category");
      expect(display.tagName).toBe("BUTTON");
      expect(display.tabIndex).toBe(0);
      fireEvent.keyDown(display, { key: "Enter" });
      expect(screen.getByLabelText("Tile title")).toBeDefined();
      fireEvent.keyDown(screen.getByLabelText("Tile title"), {
         key: "Escape",
      });
      fireEvent.keyDown(screen.getByText("By category"), { key: " " });
      expect(screen.getByLabelText("Tile title")).toBeDefined();
   });

   it("shows a placeholder for a missing subtitle and an emptied title, and removes the tag when emptied", async () => {
      await mount();
      expect(within(tile("by_cat")).getByText("Add a subtitle")).toBeDefined();
      // An unlabelled tile shows the name it falls back to.
      expect(within(tile("by_brand")).getByText("by_brand")).toBeDefined();
      editInline("By category", "Tile title", "");
      expect(within(tile("by_cat")).getByText("by_cat")).toBeDefined();
   });

   it("edits the page's title and description in place", async () => {
      await mount();
      editInline("Storefront", "Dashboard title", "Storefront, weekly");
      expect(screen.getByText("Storefront, weekly")).toBeDefined();
      fireEvent.click(screen.getByText("Add a description"));
      const field = screen.getByLabelText("Markdown");
      fireEvent.change(field, { target: { value: "What sold." } });
      fireEvent.keyDown(field, { key: "Escape" });
      expect(screen.getByText("What sold.")).toBeDefined();
      undo();
      expect(screen.getByText("Add a description")).toBeDefined();
      undo();
      expect(screen.getByText("Storefront")).toBeDefined();
   });

   it("has no title or subtitle fields in the tile menu, and none for the page in Settings", async () => {
      await mount();
      fireEvent.click(screen.getByLabelText("Settings for By category"));
      expect(screen.queryByLabelText("Tile title")).toBeNull();
      expect(screen.queryByLabelText("Tile subtitle")).toBeNull();
      expect(button("Drill-through…")).toBeDefined();
      fireEvent.keyDown(button("Remove tile"), { key: "Escape" });
      fireEvent.click(button("Settings"));
      expect(screen.queryByLabelText("Dashboard title")).toBeNull();
      expect(screen.queryByLabelText("Dashboard description")).toBeNull();
      expect(screen.getByLabelText("Grid width")).toBeDefined();
   });
});

describe("click-to-edit markdown in a text tile", () => {
   it("opens on a click and commits on Done, as one undo step", async () => {
      await mount();
      fireEvent.click(within(tile("intro")).getByText(/Read/));
      const field = screen.getByLabelText("Markdown") as HTMLTextAreaElement;
      expect(field.selectionStart).toBe(field.value.length);
      fireEvent.change(field, { target: { value: "# New heading" } });
      fireEvent.click(screen.getByRole("button", { name: "Done" }));
      expect(
         within(tile("intro")).getByRole("heading", { name: "New heading" }),
      ).toBeDefined();
      undo();
      expect(within(tile("intro")).getByText(/Read/)).toBeDefined();
   });

   it("keeps its editor open on a draft the writer would refuse, and says why", async () => {
      await mount();
      fireEvent.click(within(tile("intro")).getByText(/Read/));
      const field = screen.getByLabelText("Markdown");
      fireEvent.change(field, { target: { value: "a\n|## b" } });
      expect(screen.getByText(/close the text early/)).toBeDefined();
      expect(
         screen.getByRole("button", { name: "Done" }).hasAttribute("disabled"),
      ).toBe(true);
      // Escape is Done, which holds.
      fireEvent.keyDown(field, { key: "Escape" });
      expect(screen.getByLabelText("Markdown")).toBeDefined();
      fireEvent.change(field, { target: { value: "fine" } });
      fireEvent.keyDown(field, { key: "Escape" });
      expect(screen.queryByLabelText("Markdown")).toBeNull();
      expect(within(tile("intro")).getByText("fine")).toBeDefined();
   });

   it("Cancel drops the draft", async () => {
      await mount();
      fireEvent.click(within(tile("intro")).getByText(/Read/));
      fireEvent.change(screen.getByLabelText("Markdown"), {
         target: { value: "dropped" },
      });
      fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
      expect(within(tile("intro")).queryByText("dropped")).toBeNull();
      expect(button("Undo")).toHaveProperty("disabled", true);
   });

   it("opens from the keyboard, and leaves a link inert", async () => {
      await mount();
      const display = within(tile("intro")).getByRole("button", {
         name: /Read/,
      });
      expect(display.tabIndex).toBe(0);
      // A link neither navigates nor opens the editor.
      const link = within(tile("intro")).getByRole("link", {
         name: "the docs",
      });
      expect(fireEvent.click(link)).toBe(false);
      expect(screen.queryByLabelText("Markdown")).toBeNull();
      fireEvent.keyDown(display, { key: "Enter" });
      expect(screen.getByLabelText("Markdown")).toBeDefined();
   });

   it("shows the hint in an empty tile, and the edit survives a save", async () => {
      let written: string | undefined;
      await mount({
         onSave: (source) => {
            written = source;
         },
      });
      fireEvent.click(button("Add tile"));
      fireEvent.click(screen.getByRole("button", { name: "Text" }));
      fireEvent.click(screen.getByRole("button", { name: "Add text" }));
      fireEvent.click(within(tile("text_1")).getByText(TEXT_TILE_PLACEHOLDER));
      const field = screen.getByLabelText("Markdown");
      expect(field.getAttribute("placeholder")).toBe(TEXT_TILE_PLACEHOLDER);
      fireEvent.change(field, { target: { value: "Typed in place" } });
      fireEvent.keyDown(field, { key: "Escape" });

      fireEvent.click(button("Save changes"));
      await waitFor(() => expect(written).toBeDefined());
      expect(written).toContain("##|(markdown) text_1\nTyped in place\n|##");
   });
});

describe("the save shortcut inside a markdown field", () => {
   it("saves the open draft, not the text before it", async () => {
      let written: string | undefined;
      await mount({
         onSave: (source) => {
            written = source;
         },
      });
      fireEvent.click(
         within(tile("intro")).getByRole("button", { name: /Read/ }),
      );
      const field = screen.getByLabelText("Markdown");
      field.focus();
      fireEvent.change(field, { target: { value: "Drafted, never closed" } });
      fireEvent.keyDown(field, { key: "s", ctrlKey: true, metaKey: true });
      await waitFor(() => expect(written).toBeDefined());
      expect(written).toContain(
         "##|(markdown) intro\nDrafted, never closed\n|##",
      );
   });
});

describe("undo and redo point at the tile they changed", () => {
   it("scrolls to it and lights it briefly, for the buttons and the keyboard", async () => {
      const scrolled = mock((_options?: unknown) => {});
      const original = HTMLElement.prototype.scrollIntoView;
      HTMLElement.prototype.scrollIntoView = scrolled;
      try {
         await mount();
         editInline("By category", "Tile title", "Revenue");
         expect(tile("by_cat").hasAttribute("data-flash")).toBe(false);
         undo();
         expect(tile("by_cat").getAttribute("data-flash")).toBe("true");
         expect(scrolled).toHaveBeenCalledTimes(1);
         expect(tile("by_brand").hasAttribute("data-flash")).toBe(false);

         fireEvent.click(button("Redo"));
         expect(scrolled).toHaveBeenCalledTimes(2);
         fireEvent.keyDown(window, {
            key: "z",
            ctrlKey: true,
            metaKey: true,
         });
         expect(scrolled).toHaveBeenCalledTimes(3);
         await waitFor(() =>
            expect(tile("by_cat").hasAttribute("data-flash")).toBe(true),
         );
      } finally {
         HTMLElement.prototype.scrollIntoView = original;
      }
   });

   it("copes with a browser that cannot scroll", async () => {
      await mount();
      editInline("By category", "Tile title", "Revenue");
      undo();
      expect(tile("by_cat").getAttribute("data-flash")).toBe("true");
   });
});

describe("the save target", () => {
   it("is a caption under Save, not only a tooltip", async () => {
      await mount({ onSave: () => {} });
      expect(screen.getByText("Saves to the package file")).toBeDefined();
   });

   it("is the workspace's own words when it supplies them", async () => {
      await mount({
         onSave: () => {},
         savesTo: "host",
         saveLabel: "Saved to this draft",
      });
      expect(screen.getByText("Saved to this draft")).toBeDefined();
      expect(screen.queryByText(/embedded in/)).toBeNull();
   });

   it("is a generic line, without the word host, when the workspace says nothing", async () => {
      await mount({ onSave: () => {}, savesTo: "host" });
      expect(
         screen.getByText("Saves to the app this is embedded in"),
      ).toBeDefined();
      expect(screen.queryByText(/host/i)).toBeNull();
   });

   it("is absent when there is nowhere to save", async () => {
      await mount();
      expect(screen.queryByText("Saves to the package file")).toBeNull();
   });
});
