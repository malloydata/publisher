// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   act,
   cleanup,
   fireEvent,
   render,
   screen,
   waitFor,
   within,
} from "@testing-library/react";
import { afterEach, describe, expect, it, mock } from "bun:test";
import { DashboardBuilder } from "./DashboardBuilder";
import type { SaveContext } from "./useDocumentEditor";
import { openDocument } from "./testing/fixtures";
import { editInline } from "./testing/inline";

const SOURCE = `## artifact { title="Storefront" tiles=["a -> by_cat", "a -> by_brand"] } dashboard { columns=12 }
import "../data_app.malloy"

source: a is scoped_orders extend {
  # colspan=6
  # label="By category"
  view: by_cat is by_category

  # colspan=6
  view: by_brand is by_brand_view
}`;

type Write = { source: string; context: SaveContext<unknown> };

const mount = async (
   onSave: (source: string, context: SaveContext<unknown>) => Promise<void>,
) => {
   const document = await openDocument(SOURCE);
   return render(
      <DashboardBuilder source={SOURCE} document={document} onSave={onSave} />,
   );
};

const writes: Write[] = [];
const recording = (fail?: () => Error | undefined) =>
   mock(async (source: string, context: SaveContext<unknown>) => {
      if (context.purpose === "undo") {
         const failure = fail?.();
         if (failure) throw failure;
      }
      writes.push({ source, context });
   });

const button = (name: string) =>
   screen.getByRole("button", { name, hidden: true });
const noButton = (name: string) =>
   screen.queryByRole("button", { name, hidden: true });

const retitle = (next: string) => {
   editInline("By category", "Tile title", next);
};
const removeTile = () => {
   fireEvent.click(screen.getByLabelText("Settings for By category"));
   fireEvent.click(screen.getByRole("button", { name: "Remove tile" }));
};

/** The notice, which is the status region that offers Undo save. */
const notice = async () =>
   (
      await screen.findByRole("button", { name: "Undo save", hidden: true })
   ).closest('[role="status"]') as HTMLElement;

const press = (key: string, init: KeyboardEventInit = {}) =>
   fireEvent.keyDown(window, { key, ctrlKey: true, metaKey: true, ...init });

afterEach(() => {
   cleanup();
   writes.length = 0;
});

describe("the save notice", () => {
   it("appears after a save with what moved, View change and Undo save", async () => {
      await mount(recording());
      removeTile();
      fireEvent.click(button("Save changes"));
      const region = await notice();
      expect(region.textContent).toContain("Removed 1 tile");
      within(region).getByRole("button", { name: "View change", hidden: true });
      // The toolbar's own Undo keeps its name; the notice never says a bare "Undo".
      expect(
         within(region).queryByRole("button", { name: "Undo", hidden: true }),
      ).toBeNull();
      expect(writes).toHaveLength(1);
   });

   it("withdraws on the first edit after the save", async () => {
      await mount(recording());
      retitle("Renamed");
      fireEvent.click(button("Save changes"));
      await notice();
      editInline("Renamed", "Tile title", "Again");
      await waitFor(() => expect(noButton("Undo save")).toBeNull());
   });

   it("stays through Escape, and Cmd-Z steps the editor back without writing the file", async () => {
      await mount(recording());
      retitle("Renamed");
      fireEvent.click(button("Save changes"));
      await notice();

      fireEvent.keyDown(window, { key: "Escape" });
      expect(noButton("Undo save")).not.toBeNull();
      expect(writes).toHaveLength(1);

      press("z");
      // The shortcut is the editor's undo: it withdraws the offer, and never undoes the save.
      await waitFor(() => expect(noButton("Undo save")).toBeNull());
      expect(writes).toHaveLength(1);
   });

   it("opens View change as a read-only viewer and pauses the shortcuts under it", async () => {
      await mount(recording());
      retitle("Renamed");
      fireEvent.click(button("Save changes"));
      await notice();
      fireEvent.click(button("View change"));
      const diff = (await screen.findByLabelText("File changes")).closest(
         '[role="dialog"]',
      ) as HTMLElement;
      expect(diff.textContent).toContain("Renamed");
      expect(
         within(diff).queryByRole("button", {
            name: "Save this",
            hidden: true,
         }),
      ).toBeNull();
      expect(
         within(diff).queryByRole("button", {
            name: "Keep editing",
            hidden: true,
         }),
      ).toBeNull();

      press("z");
      expect(noButton("Undo save")).not.toBeNull();

      fireEvent.click(
         within(diff).getByRole("button", { name: "Close", hidden: true }),
      );
      press("z");
      await waitFor(() => expect(noButton("Undo save")).toBeNull());
   });

   it("Undo save writes the file back, restores the unsaved edit with its history, and says so", async () => {
      await mount(recording());
      retitle("Renamed");
      fireEvent.click(button("Save changes"));
      fireEvent.click(
         await screen.findByRole("button", { name: "Undo save", hidden: true }),
      );
      await waitFor(() => expect(writes).toHaveLength(2));
      expect(writes[1].source).toBe(SOURCE);
      expect(writes[1].context.purpose).toBe("undo");

      const status = await screen.findByText(
         "Save undone. Your edits are back and unsaved.",
      );
      expect(status.closest('[role="status"]')).not.toBeNull();
      expect(noButton("Undo save")).toBeNull();
      // Dirty again, with the edit still on screen and its undo step intact.
      await waitFor(() => expect(button("Save changes")).toBeDefined());
      expect(screen.getByLabelText("Settings for Renamed")).toBeDefined();
      expect(button("Undo").hasAttribute("disabled")).toBe(false);
      await waitFor(() =>
         expect(document.activeElement).toBe(button("Save changes")),
      );

      fireEvent.click(button("Undo"));
      expect(screen.getByLabelText("Settings for By category")).toBeDefined();
   });

   it("keeps the notice and shows the failure when the undo is refused", async () => {
      await mount(recording(() => new Error("the file changed")));
      retitle("Renamed");
      fireEvent.click(button("Save changes"));
      fireEvent.click(
         await screen.findByRole("button", { name: "Undo save", hidden: true }),
      );
      expect(
         await screen.findByText(/Could not undo the save: the file changed/),
      ).toBeDefined();
      expect(noButton("Undo save")).not.toBeNull();
      expect(writes).toHaveLength(1);
      await act(async () => {});
   });
});
