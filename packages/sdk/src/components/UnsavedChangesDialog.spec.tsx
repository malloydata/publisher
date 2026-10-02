// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, mock } from "bun:test";
import { UnsavedChangesDialog } from "./UnsavedChangesDialog";

afterEach(cleanup);

const mount = (props: { open?: boolean; canSave?: boolean } = {}) => {
   const handlers = {
      onKeepEditing: mock(() => {}),
      onDiscard: mock(() => {}),
      onSaveAndExit: mock(() => {}),
   };
   render(
      <UnsavedChangesDialog
         open={props.open ?? true}
         canSave={props.canSave ?? true}
         {...handlers}
      />,
   );
   return handlers;
};

describe("UnsavedChangesDialog", () => {
   it("renders nothing while closed", () => {
      mount({ open: false });
      expect(screen.queryByRole("dialog")).toBeNull();
   });

   it("offers all three ways out when it can save", () => {
      const h = mount();
      fireEvent.click(screen.getByRole("button", { name: "Keep editing" }));
      fireEvent.click(screen.getByRole("button", { name: "Discard changes" }));
      fireEvent.click(screen.getByRole("button", { name: "Save and exit" }));
      expect(h.onKeepEditing).toHaveBeenCalledTimes(1);
      expect(h.onDiscard).toHaveBeenCalledTimes(1);
      expect(h.onSaveAndExit).toHaveBeenCalledTimes(1);
   });

   it("leaves out Save and exit where nothing can be written", () => {
      mount({ canSave: false });
      expect(
         screen.queryByRole("button", { name: "Save and exit" }),
      ).toBeNull();
      expect(
         screen.getByRole("button", { name: "Discard changes" }),
      ).toBeTruthy();
   });

   it("treats Escape as Keep editing", () => {
      const h = mount();
      fireEvent.keyDown(screen.getByRole("dialog"), { key: "Escape" });
      expect(h.onKeepEditing).toHaveBeenCalledTimes(1);
      expect(h.onDiscard).not.toHaveBeenCalled();
   });
});
