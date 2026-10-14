// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, mock } from "bun:test";
import { AddTileDialog } from "./AddTileDialog";
import { openDocument } from "./testing/fixtures";

const SOURCE = `## artifact { title="T" tiles=["a -> x"] } dashboard { columns=12 }
import { empty, stocked } from "../m.malloy"

source: a is empty extend {
  view: x is vx
}`;

const catalog = {
   sources: [
      {
         name: "empty",
         modelPath: "m.malloy",
         views: [],
         givens: [],
         fields: [],
      },
      {
         name: "stocked",
         modelPath: "m.malloy",
         views: [{ name: "totals" }],
         givens: [],
         fields: [],
      },
   ],
};

const mount = async (
   onAdd: (tile: unknown) => void = () => {},
   onAddText: () => void = () => {},
) =>
   render(
      <AddTileDialog
         open
         document={await openDocument(SOURCE)}
         catalog={catalog}
         columns={12}
         onClose={() => {}}
         onAdd={onAdd}
         onAddText={onAddText}
      />,
   );

describe("AddTileDialog", () => {
   it("opens on the first view, and names a source with no views rather than offering it", async () => {
      await mount();
      const option = screen.getByRole("option", { name: "Totals (totals)" });
      expect(option.getAttribute("aria-selected")).toBe("true");
      expect(screen.getByText(/with no views to show: empty/)).toBeDefined();
   });

   it("adds the picked view, with the title typed for it", async () => {
      const onAdd = mock((_tile: unknown) => {});
      await mount(onAdd);
      fireEvent.change(screen.getByLabelText("Tile title"), {
         target: { value: "All totals" },
      });
      fireEvent.click(screen.getByRole("button", { name: "Add tile" }));
      expect(onAdd).toHaveBeenCalledTimes(1);
      expect(onAdd.mock.calls[0]?.[0]).toMatchObject({
         base: "stocked",
         view: "totals",
         label: "All totals",
      });
   });

   it("searches, and Enter adds what the search left picked", async () => {
      const onAdd = mock((_tile: unknown) => {});
      await mount(onAdd);
      const search = screen.getByRole("combobox", { name: "Search views" });
      fireEvent.change(search, { target: { value: "nothing like it" } });
      expect(screen.getByText(/No view matches/)).toBeDefined();
      fireEvent.change(search, { target: { value: "tot" } });
      fireEvent.keyDown(search, { key: "Enter" });
      expect(onAdd).toHaveBeenCalledTimes(1);
   });

   it("adds a text tile instead, without a view", async () => {
      const onAdd = mock((_tile: unknown) => {});
      const onAddText = mock(() => {});
      await mount(onAdd, onAddText);
      fireEvent.click(screen.getByRole("button", { name: "Add text instead" }));
      expect(onAddText).toHaveBeenCalledTimes(1);
      expect(onAdd).not.toHaveBeenCalled();
   });

   it("Enter takes the top match when a search hides the pick", async () => {
      const onAdd = mock((_tile: unknown) => {});
      render(
         <AddTileDialog
            open
            document={await openDocument(SOURCE)}
            catalog={{
               sources: [
                  {
                     name: "stocked",
                     modelPath: "m.malloy",
                     views: [{ name: "totals" }, { name: "by_brand" }],
                     givens: [],
                     fields: [],
                  },
               ],
            }}
            columns={12}
            onClose={() => {}}
            onAdd={onAdd}
            onAddText={() => {}}
         />,
      );
      // Opens on `totals`; the search hides it.
      const search = screen.getByRole("combobox", { name: "Search views" });
      fireEvent.change(search, { target: { value: "brand" } });
      fireEvent.keyDown(search, { key: "Enter" });
      expect(onAdd.mock.calls[0]?.[0]).toMatchObject({ view: "by_brand" });
   });
});
