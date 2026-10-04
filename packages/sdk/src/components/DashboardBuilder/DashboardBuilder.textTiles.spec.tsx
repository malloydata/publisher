// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   act,
   fireEvent,
   render,
   renderHook,
   screen,
   waitFor,
   within,
} from "@testing-library/react";
import { describe, expect, it } from "bun:test";
import { DashboardBuilder } from "./DashboardBuilder";
import { isTextTile } from "./document";
import { openDocument } from "./testing/fixtures";
import { TEXT_TILE_PLACEHOLDER } from "./TextTileBody";
import { useDashboardEditor } from "./useDashboardEditor";

const SOURCE = `## artifact { title="Storefront" tiles=[intro { kind=text colspan=12 }, "a -> by_cat", empty { kind=text }] } dashboard { columns=12 }
import "../data_app.malloy"

##|(markdown) intro
# Welcome

Read **this** first, then [the docs](https://example.com/docs).
|##

##|(markdown) empty
|##

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

const tile = (name: string) => screen.getByLabelText(`Tile ${name}`);
const button = (name: string) =>
   screen.getByRole("button", { name, hidden: true });

describe("DashboardBuilder: text tiles", () => {
   it("draws a text tile's markdown, and a hint while it is empty", async () => {
      await mount();
      expect(
         within(tile("intro")).getByRole("heading", { name: "Welcome" }),
      ).toBeDefined();
      expect(within(tile("intro")).getByText("this")).toBeDefined();
      expect(
         within(tile("empty")).getByText(TEXT_TILE_PLACEHOLDER),
      ).toBeDefined();
      expect(
         within(tile("intro")).queryByText(TEXT_TILE_PLACEHOLDER),
      ).toBeNull();
   });

   it("does not follow a link inside a tile being arranged", async () => {
      await mount();
      const link = within(tile("intro")).getByRole("link", {
         name: "the docs",
      });
      // fireEvent returns false when the default action was prevented.
      expect(fireEvent.click(link)).toBe(false);
   });

   it("adds an empty text tile from the Add dialog, and saves its block and entry", async () => {
      let written: string | undefined;
      await mount((source) => {
         written = source;
      });
      fireEvent.click(button("Add tile"));
      fireEvent.click(screen.getByRole("button", { name: "Text" }));
      fireEvent.click(screen.getByRole("button", { name: "Add text" }));
      expect(
         within(tile("text_1")).getByText(TEXT_TILE_PLACEHOLDER),
      ).toBeDefined();

      fireEvent.click(button("Save changes"));
      await waitFor(() => expect(written).toBeDefined());
      expect(written).toContain(
         'tiles=[intro { kind=text colspan=12 }, "a -> by_cat", empty { kind=text }, text_1 { kind=text colspan=12 }]',
      );
      expect(written).toContain("##|(markdown) text_1\n|##");
      // What was there is untouched.
      expect(written).toContain("##|(markdown) intro\n# Welcome");
   });

   it("names each new text tile for the first free number", async () => {
      await mount();
      for (const expected of ["text_1", "text_2"]) {
         fireEvent.click(button("Add tile"));
         fireEvent.click(screen.getByRole("button", { name: "Text" }));
         fireEvent.click(screen.getByRole("button", { name: "Add text" }));
         expect(tile(expected)).toBeDefined();
      }
   });

   it("sets a text tile's width from its menu, which offers no title or chart", async () => {
      let written: string | undefined;
      await mount((source) => {
         written = source;
      });
      fireEvent.click(screen.getByLabelText("Settings for intro"));
      expect(screen.queryByLabelText("Tile title")).toBeNull();
      expect(screen.queryByText("Drill-through…")).toBeNull();
      fireEvent.click(screen.getByRole("button", { name: "Width ½" }));
      fireEvent.keyDown(screen.getByRole("button", { name: "Width ½" }), {
         key: "Escape",
      });

      fireEvent.click(button("Save changes"));
      await waitFor(() => expect(written).toBeDefined());
      expect(written).toContain("intro { kind=text colspan=6 }");
   });

   it("removes a text tile from its menu, block and entry together", async () => {
      let written: string | undefined;
      await mount((source) => {
         written = source;
      });
      fireEvent.click(screen.getByLabelText("Settings for intro"));
      fireEvent.click(screen.getByRole("button", { name: "Remove tile" }));
      expect(screen.queryByLabelText("Tile intro")).toBeNull();

      fireEvent.click(button("Save changes"));
      await waitFor(() => expect(written).toBeDefined());
      expect(written).toContain('tiles=["a -> by_cat", empty { kind=text }]');
      expect(written).not.toContain("##|(markdown) intro");
      expect(written).toContain("##|(markdown) empty");
   });

   it("edits a text tile's markdown through the document, and writes the block", async () => {
      const document = await openDocument(SOURCE);
      const view = renderHook(() =>
         useDashboardEditor({ source: SOURCE, document, onSave: () => {} }),
      );
      act(() =>
         view.result.current.update((draft) => {
            const intro = draft.tiles.find(isTextTile);
            if (intro) intro.markdown = "# Welcome back";
         }),
      );
      let outcome: { ok: boolean } | undefined;
      await act(async () => {
         outcome = await view.result.current.save();
      });
      expect(outcome?.ok).toBe(true);
      expect(view.result.current.source).toContain(
         "##|(markdown) intro\n# Welcome back\n|##",
      );
      expect(view.result.current.source).not.toContain("Read **this** first");
   });
});
