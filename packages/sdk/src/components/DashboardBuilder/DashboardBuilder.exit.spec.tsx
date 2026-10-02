// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   cleanup,
   fireEvent,
   render,
   screen,
   waitFor,
} from "@testing-library/react";
import { afterEach, describe, expect, it, mock } from "bun:test";
import { DashboardBuilder } from "./DashboardBuilder";
import { openDocument } from "./testing/fixtures";

const SOURCE = `## artifact { title="Storefront" tiles=["a -> by_cat"] } dashboard { columns=12 }
import "../data_app.malloy"

source: a is scoped_orders extend {
  # colspan=6
  # label="By category"
  view: by_cat is by_category
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
   fireEvent.click(screen.getByLabelText("Settings for By category"));
   const field = screen.getByLabelText("Tile title");
   fireEvent.change(field, { target: { value: "Renamed" } });
   fireEvent.keyDown(field, { key: "Escape" });
};

afterEach(cleanup);

describe("DashboardBuilder: leaving", () => {
   it("renders no Done editing without an onExit", async () => {
      await mount();
      expect(screen.queryByRole("button", { name: "Done editing" })).toBeNull();
   });

   it("exits at once when nothing is unsaved", async () => {
      const onExit = mock(() => {});
      await mount({ onExit });
      fireEvent.click(button("Done editing"));
      expect(onExit).toHaveBeenCalledTimes(1);
   });

   it("asks first when there are edits, and Keep editing stays", async () => {
      const onExit = mock(() => {});
      await mount({ onExit, onSave: async () => {} });
      retitle();
      fireEvent.click(button("Done editing"));
      expect(screen.getByRole("dialog")).toBeDefined();
      fireEvent.click(button("Keep editing"));
      expect(onExit).not.toHaveBeenCalled();
   });

   it("Discard changes exits without saving", async () => {
      const onExit = mock(() => {});
      const onSave = mock(async () => {});
      await mount({ onExit, onSave });
      retitle();
      fireEvent.click(button("Done editing"));
      fireEvent.click(button("Discard changes"));
      expect(onExit).toHaveBeenCalledTimes(1);
      expect(onSave).not.toHaveBeenCalled();
   });

   it("Save and exit saves, then exits", async () => {
      const onExit = mock(() => {});
      const onSave = mock(async () => {});
      await mount({ onExit, onSave });
      retitle();
      fireEvent.click(button("Done editing"));
      fireEvent.click(button("Save and exit"));
      await waitFor(() => expect(onExit).toHaveBeenCalledTimes(1));
      expect(onSave).toHaveBeenCalledTimes(1);
   });

   it("stays open when the save fails", async () => {
      const onExit = mock(() => {});
      const onSave = mock(async () => {
         throw new Error("nope");
      });
      await mount({ onExit, onSave });
      retitle();
      fireEvent.click(button("Done editing"));
      fireEvent.click(button("Save and exit"));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      await screen.findByText(/nope/);
      expect(onExit).not.toHaveBeenCalled();
   });

   it("offers no Save and exit when the builder cannot save", async () => {
      await mount({ onExit: () => {} });
      retitle();
      fireEvent.click(button("Done editing"));
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
