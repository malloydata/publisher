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
import { DashboardBuilder } from "./DashboardBuilder";
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

const mount = async (
   options: {
      onExit?: () => void;
      onSave?: (source: string) => Promise<void> | void;
      onDirtyChange?: (dirty: boolean) => void;
   } = {},
) => {
   const document = await openDocument(SOURCE);
   return render(
      <DashboardBuilder
         source={SOURCE}
         document={document}
         {...(options.onExit ? { onExit: options.onExit } : {})}
         {...(options.onSave ? { onSave: options.onSave } : {})}
         {...(options.onDirtyChange
            ? { onDirtyChange: options.onDirtyChange }
            : {})}
      />,
   );
};

const button = (name: string) =>
   screen.getByRole("button", { name, hidden: true });

const retitle = () => {
   editInline("By category", "Tile title", "Renamed");
};

const shortcut = (key: string) =>
   fireEvent.keyDown(window, { key, ctrlKey: true, metaKey: true });

const dialogs = () => screen.getAllByRole("dialog", { hidden: true });

const settleTick = () => act(async () => {});

afterEach(cleanup);

describe("DashboardBuilder: leaving", () => {
   it("renders no Close without an onExit", async () => {
      await mount();
      expect(screen.queryByRole("button", { name: "Close" })).toBeNull();
   });

   it("exits at once when nothing is unsaved", async () => {
      const onExit = mock(() => {});
      await mount({ onExit });
      fireEvent.click(button("Close"));
      expect(onExit).toHaveBeenCalledTimes(1);
   });

   it("draws Save and no Close when it can save", async () => {
      await mount({ onExit: () => {}, onSave: async () => {} });
      expect(button("Saved").hasAttribute("disabled")).toBe(true);
      retitle();
      expect(button("Save")).toBeDefined();
      expect(screen.queryByRole("button", { name: "Close" })).toBeNull();
   });

   it("asks first when there are edits and nowhere to save, and Keep editing stays", async () => {
      const onExit = mock(() => {});
      await mount({ onExit });
      retitle();
      fireEvent.click(button("Close"));
      expect(screen.getByRole("dialog")).toBeDefined();
      fireEvent.click(button("Keep editing"));
      expect(onExit).not.toHaveBeenCalled();
   });

   it("Discard changes exits", async () => {
      const onExit = mock(() => {});
      await mount({ onExit });
      retitle();
      fireEvent.click(button("Close"));
      fireEvent.click(button("Discard changes"));
      expect(onExit).toHaveBeenCalledTimes(1);
   });

   it("Save saves in place and stays in the builder", async () => {
      const onExit = mock(() => {});
      const onSave = mock(async () => {});
      await mount({ onExit, onSave });
      retitle();
      fireEvent.click(button("Save"));
      await waitFor(() => expect(button("Saved")).toBeDefined());
      expect(onSave).toHaveBeenCalledTimes(1);
      expect(onExit).not.toHaveBeenCalled();
   });

   it("stays open when the save fails", async () => {
      const onExit = mock(() => {});
      const onSave = mock(async () => {
         throw new Error("nope");
      });
      await mount({ onExit, onSave });
      retitle();
      fireEvent.click(button("Save"));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      await screen.findByText(/nope/);
      expect(onExit).not.toHaveBeenCalled();
   });

   it("offers no Save and exit when the builder cannot save", async () => {
      await mount({ onExit: () => {} });
      retitle();
      fireEvent.click(button("Close"));
      expect(
         screen.queryByRole("button", { name: "Save and exit" }),
      ).toBeNull();
      expect(button("Discard changes")).toBeDefined();
   });

   it("reports clean when it unmounts dirty", async () => {
      const onDirtyChange = mock((_dirty: boolean) => {});
      const view = await mount({ onDirtyChange, onSave: async () => {} });
      retitle();
      expect(onDirtyChange.mock.calls.at(-1)?.[0]).toBe(true);
      view.unmount();
      expect(onDirtyChange.mock.calls.at(-1)?.[0]).toBe(false);
   });
});

const removeTile = () => {
   fireEvent.click(screen.getByLabelText("Settings for By category"));
   fireEvent.click(screen.getByRole("button", { name: "Delete" }));
};

describe("DashboardBuilder: leaving with a structural edit", () => {
   it("ignores undo and redo shortcuts while the exit dialog is open", async () => {
      await mount({ onExit: () => {} });
      removeTile();
      fireEvent.click(button("Close"));
      shortcut("z");
      await settleTick();
      expect(dialogs()).toHaveLength(1);
      expect(screen.queryByLabelText("Settings for By category")).toBeNull();
   });

   it("Save writes a structural edit at once, once, and stays open", async () => {
      const onExit = mock(() => {});
      const onSave = mock(async () => {});
      await mount({ onExit, onSave });
      removeTile();
      fireEvent.click(button("Save"));
      await waitFor(() => expect(button("Saved")).toBeDefined());
      expect(onSave).toHaveBeenCalledTimes(1);
      expect(onExit).not.toHaveBeenCalled();
   });

   it("holds the button through a save in flight, so a second press cannot write twice", async () => {
      let finish = () => {};
      const onSave = mock(
         () => new Promise<void>((resolve) => (finish = resolve)),
      );
      await mount({ onExit: () => {}, onSave });
      retitle();
      shortcut("s");
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      expect(button("Saving…").hasAttribute("disabled")).toBe(true);
      fireEvent.click(button("Saving…"));
      await act(async () => finish());
      await screen.findByRole("button", { name: "Saved" });
      expect(onSave).toHaveBeenCalledTimes(1);
   });
});
