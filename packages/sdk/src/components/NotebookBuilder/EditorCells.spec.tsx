// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, mock } from "bun:test";
import { useState } from "react";
import {
   mockServerProvider,
   serverWrapper,
} from "../../../test/serverProvider";

mockServerProvider({});

const { MarkdownCell } = await import("./EditorCells");

const links = {
   environmentName: "env",
   packageName: "pkg",
   sourcePath: "n.malloy",
};

/** A host for the cell that keeps its markdown and open state, as the builder does. */
function Host({
   initial,
   onDirty,
   onCommitted,
}: {
   initial: string;
   onDirty?: (dirty: boolean) => void;
   onCommitted?: (next: string) => void;
}) {
   const [markdown, setMarkdown] = useState(initial);
   const [editing, setEditing] = useState(false);
   return (
      <MarkdownCell
         markdown={markdown}
         editing={editing}
         links={links}
         onEdit={() => setEditing(true)}
         onCommit={(next) => {
            setMarkdown(next);
            onCommitted?.(next);
         }}
         onClose={() => setEditing(false)}
         {...(onDirty ? { onDraftDirtyChange: onDirty } : {})}
      />
   );
}

const open = (
   initial = "Hello",
   props: Partial<Parameters<typeof Host>[0]> = {},
) => {
   const view = render(<Host initial={initial} {...props} />, {
      wrapper: serverWrapper,
   });
   fireEvent.click(screen.getByRole("button", { name: "Edit text" }));
   return view;
};
const field = () => screen.getByLabelText("Markdown") as HTMLTextAreaElement;
const type = (text: string) =>
   fireEvent.change(field(), { target: { value: text } });
const press = (name: string) =>
   fireEvent.click(screen.getByRole("button", { name }));

describe("MarkdownCell", () => {
   it("opens with the caret at the end of the text", () => {
      open("Hello");
      expect(field().selectionStart).toBe(5);
      expect(field().selectionEnd).toBe(5);
   });

   it("Cancel drops the draft and closes without committing", () => {
      const onCommitted = mock((_next: string) => {});
      open("Hello", { onCommitted });
      type("Hello, changed");
      press("Cancel");
      expect(onCommitted).not.toHaveBeenCalled();
      expect(screen.queryByLabelText("Markdown")).toBeNull();
      expect(screen.getByText("Hello")).toBeDefined();
   });

   it("keeps focus in the field when Cancel is pressed, so blur cannot commit first", () => {
      open();
      const cancel = screen.getByRole("button", { name: "Cancel" });
      // fireEvent returns false when the handler called preventDefault.
      expect(fireEvent.mouseDown(cancel)).toBe(false);
   });

   it("does not commit on a blur that moves focus to its own Cancel", () => {
      const onCommitted = mock((_next: string) => {});
      open("Hello", { onCommitted });
      type("Hello, changed");
      fireEvent.blur(field(), {
         relatedTarget: screen.getByRole("button", { name: "Cancel" }),
      });
      expect(onCommitted).not.toHaveBeenCalled();
      expect(field()).toBeDefined();
   });

   it("still commits when focus leaves for anywhere else", () => {
      const onCommitted = mock((_next: string) => {});
      open("Hello", { onCommitted });
      type("Hello, changed");
      fireEvent.blur(field());
      expect(onCommitted).toHaveBeenCalledWith("Hello, changed");
   });

   it("Cmd or Ctrl+Enter is Done", () => {
      const onCommitted = mock((_next: string) => {});
      open("Hello", { onCommitted });
      type("Hello, changed");
      fireEvent.keyDown(field(), { key: "Enter", ctrlKey: true });
      expect(onCommitted).toHaveBeenCalledWith("Hello, changed");
      expect(screen.queryByLabelText("Markdown")).toBeNull();
   });

   it("says why a draft the writer would refuse is refused, and Done and Cmd+Enter hold", () => {
      open("Hello");
      type("a\n|## b");
      expect(screen.getByText(/close the text early/)).toBeDefined();
      expect(
         screen.getByRole("button", { name: "Done" }).hasAttribute("disabled"),
      ).toBe(true);
      fireEvent.keyDown(field(), { key: "Enter", metaKey: true });
      expect(field()).toBeDefined();
      type("");
      expect(screen.getByText(/remove the cell instead/)).toBeDefined();
      type("fine");
      expect(screen.queryByText(/remove the cell instead/)).toBeNull();
   });

   it("lets an untouched empty cell close without complaint", () => {
      open("");
      expect(screen.queryByText(/remove the cell instead/)).toBeNull();
      press("Done");
      expect(screen.queryByLabelText("Markdown")).toBeNull();
   });

   it("commits on blur even when the draft is invalid; the writer refuses at Save", () => {
      const onCommitted = mock((_next: string) => {});
      open("Hello", { onCommitted });
      type("a\n|## b");
      fireEvent.blur(field());
      expect(onCommitted).toHaveBeenCalledWith("a\n|## b");
   });

   it("reports an open draft that differs from the text, and clears when it closes", () => {
      const onDirty = mock((_dirty: boolean) => {});
      open("Hello", { onDirty });
      expect(onDirty.mock.calls.at(-1)?.[0]).not.toBe(true);
      type("Hello!");
      expect(onDirty.mock.calls.at(-1)?.[0]).toBe(true);
      type("Hello");
      expect(onDirty.mock.calls.at(-1)?.[0]).toBe(false);
      type("Hello!");
      press("Cancel");
      expect(onDirty.mock.calls.at(-1)?.[0]).toBe(false);
   });

   it("reports clean when it unmounts mid-edit", () => {
      const onDirty = mock((_dirty: boolean) => {});
      const view = open("Hello", { onDirty });
      type("Hello!");
      view.unmount();
      expect(onDirty.mock.calls.at(-1)?.[0]).toBe(false);
   });

   it("stays silent when it mounts clean, so it cannot clear another cell's dirty draft", () => {
      const onDirty = mock((_dirty: boolean) => {});
      const view = render(<Host initial="Hello" onDirty={onDirty} />, {
         wrapper: serverWrapper,
      });
      expect(onDirty).not.toHaveBeenCalled();
      view.unmount();
      expect(onDirty).not.toHaveBeenCalled();
   });
});
