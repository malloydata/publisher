// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, mock } from "bun:test";
import { useState } from "react";
import { isMac, useBuilderShortcuts } from "./useBuilderShortcuts";

const mod = isMac ? { metaKey: true } : { ctrlKey: true };

// Save reads the rendered state, as a builder's save reads its render closure.
function Harness({
   onSave,
   withSave = true,
}: {
   onSave: (seen: string) => void;
   withSave?: boolean;
}) {
   const [value, setValue] = useState("");
   const [committed, setCommitted] = useState("");
   useBuilderShortcuts({
      undo: () => {},
      redo: () => {},
      ...(withSave ? { save: () => onSave(committed) } : {}),
      escape: () => {},
   });
   return (
      <input
         aria-label="Field"
         value={value}
         onChange={(event) => setValue(event.target.value)}
         onBlur={() => setCommitted(value)}
      />
   );
}

afterEach(cleanup);

describe("useBuilderShortcuts: save inside a text field", () => {
   it("leaves the field, then saves with what the blur committed", () => {
      const onSave = mock((_seen: string) => {});
      render(<Harness onSave={onSave} />);
      const field = screen.getByLabelText("Field");
      field.focus();
      fireEvent.change(field, { target: { value: "typed" } });
      fireEvent.keyDown(field, { key: "s", ...mod });
      expect(onSave).toHaveBeenCalledTimes(1);
      expect(onSave.mock.calls[0][0]).toBe("typed");
   });

   it("leaves the field alone when there is nothing to save to", () => {
      const onSave = mock((_seen: string) => {});
      render(<Harness onSave={onSave} withSave={false} />);
      const field = screen.getByLabelText("Field");
      field.focus();
      fireEvent.keyDown(field, { key: "s", ...mod });
      expect(onSave).not.toHaveBeenCalled();
      expect(document.activeElement).toBe(field);
   });
});
