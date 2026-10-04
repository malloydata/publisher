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
