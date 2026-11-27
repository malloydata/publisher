// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { act, cleanup, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { PLACEMENT_NOTE, usePlacementNote } from "./PlacementNote";

afterEach(cleanup);
beforeEach(() => localStorage.clear());

function mount() {
   const ref = {} as { warn: () => void };
   function Host() {
      const { warn, note } = usePlacementNote();
      ref.warn = warn;
      return <>{note}</>;
   }
   render(<Host />);
   return ref;
}

describe("the note before a grid's first move places every tile", () => {
   it("says what the move will do, once per browser", () => {
      const first = mount();
      expect(screen.queryByText(PLACEMENT_NOTE)).toBeNull();
      act(() => first.warn());
      expect(screen.getByText(PLACEMENT_NOTE)).toBeDefined();
      cleanup();
      const again = mount();
      act(() => again.warn());
      expect(screen.queryByText(PLACEMENT_NOTE)).toBeNull();
   });
});
