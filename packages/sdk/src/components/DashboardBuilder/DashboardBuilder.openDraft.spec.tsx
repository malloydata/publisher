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
      onSave?: (source: string) => Promise<void> | void;
      onDirtyChange?: (dirty: boolean) => void;
   } = {},
) => {
   const document = await openDocument(SOURCE);
   return render(
      <DashboardBuilder
         source={SOURCE}
         document={document}
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
   it("is clean again once the committed draft is saved, and stays open", async () => {
      const onDirtyChange = mock((_dirty: boolean) => {});
      const onSave = mock(async (_source: string) => {});
      await mount({ onSave, onDirtyChange });
      type(openDescription(), "Committed");
      fireEvent.click(button("Done"));
      fireEvent.click(button("Save"));
      await waitFor(() => expect(button("Saved")).toBeDefined());
      expect(onSave).toHaveBeenCalledTimes(1);
      expect(onDirtyChange.mock.calls.at(-1)?.[0]).toBe(false);
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

   // Outside act and with only microtasks drained, so work React defers to a later task does not count; a host's leave guard reads this flag in the very next task.
   it("has told the host the draft is gone by the end of the Cancel click", async () => {
      const onDirtyChange = mock((_dirty: boolean) => {});
      await mount({ onDirtyChange, onSave: async () => {} });
      type(openDescription(), "Dropped");
      expect(onDirtyChange.mock.calls.at(-1)?.[0]).toBe(true);

      const env = globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean };
      env.IS_REACT_ACT_ENVIRONMENT = false;
      try {
         button("Cancel").click();
         await new Promise<void>((resolve) => queueMicrotask(resolve));
         expect(onDirtyChange.mock.calls.at(-1)?.[0]).toBe(false);
      } finally {
         env.IS_REACT_ACT_ENVIRONMENT = true;
      }
   });

   it("Save commits the open markdown draft into the saved file, and stays open", async () => {
      const onSave = mock(async (_source: string) => {});
      await mount({ onSave });
      type(openDescription(), "Kept words");
      fireEvent.click(button("Save"));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      expect(onSave.mock.calls[0]?.[0]).toContain("Kept words");
   });
});
