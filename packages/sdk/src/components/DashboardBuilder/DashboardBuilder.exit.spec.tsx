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
      onSave?: (source: string) => Promise<void> | void;
      onDirtyChange?: (dirty: boolean) => void;
      onExit?: () => void;
   } = {},
) => {
   const document = await openDocument(SOURCE);
   return render(
      <DashboardBuilder
         source={SOURCE}
         document={document}
         {...(options.onSave ? { onSave: options.onSave } : {})}
         {...(options.onExit ? { onExit: options.onExit } : {})}
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

afterEach(cleanup);

describe("DashboardBuilder: leaving", () => {
   it("draws Save and no Close when it can save", async () => {
      await mount({ onSave: async () => {} });
      expect(button("Saved").hasAttribute("disabled")).toBe(true);
      retitle();
      expect(button("Save")).toBeDefined();
      expect(screen.queryByRole("button", { name: "Close" })).toBeNull();
   });

   it("Save saves in place and stays in the builder", async () => {
      const onSave = mock(async () => {});
      await mount({ onSave });
      retitle();
      fireEvent.click(button("Save"));
      await waitFor(() => expect(button("Saved")).toBeDefined());
      expect(onSave).toHaveBeenCalledTimes(1);
   });

   it("stays open when the save fails", async () => {
      const onSave = mock(async () => {
         throw new Error("nope");
      });
      await mount({ onSave });
      retitle();
      fireEvent.click(button("Save"));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      await screen.findByText(/nope/);
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
   it("Save writes a structural edit at once, once, and stays open", async () => {
      const onSave = mock(async () => {});
      await mount({ onSave });
      removeTile();
      fireEvent.click(button("Save"));
      await waitFor(() => expect(button("Saved")).toBeDefined());
      expect(onSave).toHaveBeenCalledTimes(1);
   });

   it("holds the button through a save in flight, so a second press cannot write twice", async () => {
      let finish = () => {};
      const onSave = mock(
         () => new Promise<void>((resolve) => (finish = resolve)),
      );
      await mount({ onSave });
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

describe("DashboardBuilder: a host's opt-in Close", () => {
   it("draws no Close unless the host passes onExit", async () => {
      await mount({ onSave: async () => {} });
      expect(screen.queryByRole("button", { name: "Close" })).toBeNull();
   });

   it("draws Close beside Save, and leaves at once when nothing is unsaved", async () => {
      const onExit = mock(() => {});
      await mount({ onSave: async () => {}, onExit });
      expect(button("Saved")).toBeDefined();
      fireEvent.click(button("Close"));
      expect(onExit).toHaveBeenCalledTimes(1);
   });

   it("asks first when there are edits, and Keep editing stays", async () => {
      const onExit = mock(() => {});
      await mount({ onSave: async () => {}, onExit });
      retitle();
      fireEvent.click(button("Close"));
      fireEvent.click(
         await screen.findByRole("button", { name: "Keep editing" }),
      );
      expect(onExit).not.toHaveBeenCalled();
   });

   it("Discard changes leaves without saving", async () => {
      const onExit = mock(() => {});
      const onSave = mock(async () => {});
      await mount({ onSave, onExit });
      retitle();
      fireEvent.click(button("Close"));
      fireEvent.click(
         await screen.findByRole("button", { name: "Discard changes" }),
      );
      expect(onExit).toHaveBeenCalledTimes(1);
      expect(onSave).not.toHaveBeenCalled();
   });

   it("Save and exit saves, then leaves", async () => {
      const onExit = mock(() => {});
      const onSave = mock(async () => {});
      await mount({ onSave, onExit });
      retitle();
      fireEvent.click(button("Close"));
      fireEvent.click(
         await screen.findByRole("button", { name: "Save and exit" }),
      );
      await waitFor(() => expect(onExit).toHaveBeenCalledTimes(1));
      expect(onSave).toHaveBeenCalledTimes(1);
   });
});
