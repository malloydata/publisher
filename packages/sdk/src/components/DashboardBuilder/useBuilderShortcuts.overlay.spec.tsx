// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, mock } from "bun:test";
import { isMac, useBuilderShortcuts } from "./useBuilderShortcuts";

const mod = isMac ? { metaKey: true } : { ctrlKey: true };

function Harness({ onSave }: { onSave: () => void }) {
   useBuilderShortcuts({
      undo: () => {},
      redo: () => {},
      save: onSave,
      escape: () => {},
   });
   return <input aria-label="Field" />;
}

afterEach(() => {
   cleanup();
   // The hosts are attached by hand, and a leaked role=dialog would show up in other specs.
   document.body.replaceChildren();
});

/** Presses the save chord on `tag` placed inside `host`, which is attached to the page. */
const pressIn = (host: HTMLElement, tag: "input" | "button") => {
   const onSave = mock(() => {});
   render(<Harness onSave={onSave} />);
   const inner = document.createElement(tag);
   host.appendChild(inner);
   document.body.appendChild(host);
   inner.focus();
   fireEvent.keyDown(inner, { key: "s", ...mod });
   return onSave;
};

describe("useBuilderShortcuts: save inside a dialog or popover", () => {
   it("does not save from a field inside a dialog, whose draft commits on close", () => {
      const host = document.createElement("div");
      host.setAttribute("role", "dialog");
      expect(pressIn(host, "input")).not.toHaveBeenCalled();
   });

   it("does not save from a field inside a popover", () => {
      const host = document.createElement("div");
      host.className = "MuiPopover-root";
      expect(pressIn(host, "input")).not.toHaveBeenCalled();
   });

   it("does not save from outside a text field while a dialog holds focus", () => {
      const host = document.createElement("div");
      host.className = "MuiDialog-root";
      expect(pressIn(host, "button")).not.toHaveBeenCalled();
   });

   it("still saves from a plain inline field", () => {
      const onSave = mock(() => {});
      render(<Harness onSave={onSave} />);
      const field = screen.getByLabelText("Field");
      field.focus();
      fireEvent.keyDown(field, { key: "s", ...mod });
      expect(onSave).toHaveBeenCalledTimes(1);
   });
});

describe("useBuilderShortcuts: undo and redo while a window is open", () => {
   function UndoHarness({ onUndo }: { onUndo: () => void }) {
      useBuilderShortcuts({
         undo: onUndo,
         redo: onUndo,
         escape: () => {},
      });
      return null;
   }

   it("does not undo an edit behind a dialog", () => {
      const onUndo = mock(() => {});
      render(<UndoHarness onUndo={onUndo} />);
      const host = document.createElement("div");
      host.setAttribute("role", "dialog");
      const button = document.createElement("button");
      host.appendChild(button);
      document.body.appendChild(host);
      button.focus();
      fireEvent.keyDown(button, { key: "z", ...mod });
      fireEvent.keyDown(button, { key: "z", shiftKey: true, ...mod });
      expect(onUndo).not.toHaveBeenCalled();
   });

   it("undoes from the page", () => {
      const onUndo = mock(() => {});
      render(<UndoHarness onUndo={onUndo} />);
      fireEvent.keyDown(document.body, { key: "z", ...mod });
      expect(onUndo).toHaveBeenCalledTimes(1);
   });
});
