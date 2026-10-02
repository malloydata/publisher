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
  # colspan=12
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

const openDescription = () => {
   fireEvent.click(screen.getByText("Add a description"));
   return screen.getByLabelText("Markdown");
};

const type = (field: HTMLElement, value: string) =>
   fireEvent.change(field, { target: { value } });

afterEach(cleanup);

describe("DashboardBuilder: an open inline draft", () => {
   it("asks before exiting with a typed, uncommitted markdown draft", async () => {
      const onExit = mock(() => {});
      await mount({ onExit, onSave: async () => {} });
      type(openDescription(), "Half-typed");
      fireEvent.click(button("Close"));
      expect(screen.getByRole("dialog")).toBeDefined();
      expect(onExit).not.toHaveBeenCalled();
   });

   it("exits at once when the open markdown field is unchanged", async () => {
      const onExit = mock(() => {});
      await mount({ onExit, onSave: async () => {} });
      openDescription();
      fireEvent.click(button("Close"));
      expect(onExit).toHaveBeenCalledTimes(1);
   });

   it("asks when the markdown draft is one the writer would refuse", async () => {
      const onExit = mock(() => {});
      await mount({ onExit, onSave: async () => {} });
      type(openDescription(), "# not ok\n|##");
      expect(screen.getByText(/would close the text early/)).toBeDefined();
      fireEvent.click(button("Close"));
      expect(screen.getByRole("dialog")).toBeDefined();
      expect(onExit).not.toHaveBeenCalled();
   });

   it("does not ask after the draft is committed and saved", async () => {
      const onExit = mock(() => {});
      const onSave = mock(async (_source: string) => {});
      await mount({ onExit, onSave });
      type(openDescription(), "Committed");
      fireEvent.click(button("Done"));
      fireEvent.click(button("Save changes"));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      await waitFor(() => expect(button("Saved")).toBeDefined());
      fireEvent.click(button("Close"));
      expect(onExit).toHaveBeenCalledTimes(1);
   });

   it("does not ask after Cancel drops the draft", async () => {
      const onExit = mock(() => {});
      await mount({ onExit, onSave: async () => {} });
      type(openDescription(), "Dropped");
      fireEvent.click(button("Cancel"));
      fireEvent.click(button("Close"));
      expect(onExit).toHaveBeenCalledTimes(1);
   });

   it("reports the draft through onDirtyChange, and clears it on Cancel", async () => {
      const onDirtyChange = mock((_dirty: boolean) => {});
      await mount({ onDirtyChange, onSave: async () => {} });
      expect(onDirtyChange.mock.calls.at(-1)?.[0]).toBe(false);
      type(openDescription(), "Half-typed");
      expect(onDirtyChange.mock.calls.at(-1)?.[0]).toBe(true);
      fireEvent.click(button("Cancel"));
      expect(onDirtyChange.mock.calls.at(-1)?.[0]).toBe(false);
   });

   it("Save and exit commits the open markdown draft into the saved file", async () => {
      const onExit = mock(() => {});
      const onSave = mock(async (_source: string) => {});
      await mount({ onExit, onSave });
      type(openDescription(), "Kept words");
      fireEvent.click(button("Close"));
      fireEvent.click(button("Save and exit"));
      await waitFor(() => expect(onExit).toHaveBeenCalledTimes(1));
      expect(onSave).toHaveBeenCalledTimes(1);
      expect(onSave.mock.calls[0]?.[0]).toContain("Kept words");
   });

   it("asks before exiting with a typed, uncommitted title", async () => {
      const onExit = mock(() => {});
      await mount({ onExit, onSave: async () => {} });
      fireEvent.click(screen.getByText("By category"));
      type(screen.getByLabelText("Tile title"), "Renamed");
      fireEvent.click(button("Close"));
      expect(screen.getByRole("dialog")).toBeDefined();
      expect(onExit).not.toHaveBeenCalled();
   });
});
