// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { NarrowEditGate } from "./NarrowEditGate";

const stubWidth = (narrow: boolean) => {
   window.matchMedia = ((query: string) => ({
      matches: narrow && query.includes("max-width"),
      media: query,
      addEventListener: () => {},
      removeEventListener: () => {},
      addListener: () => {},
      removeListener: () => {},
      onchange: null,
      dispatchEvent: () => false,
   })) as unknown as typeof window.matchMedia;
};

// The stub is global and the preloaded DOM is shared across spec files.
const original = window.matchMedia;
beforeEach(() => {
   window.matchMedia = original;
});
afterEach(() => {
   cleanup();
   window.matchMedia = original;
});

describe("NarrowEditGate", () => {
   it("renders the editor at once on a wide screen", () => {
      stubWidth(false);
      render(
         <NarrowEditGate>
            <p>the editor</p>
         </NarrowEditGate>,
      );
      expect(screen.getByText("the editor")).toBeDefined();
      expect(screen.queryByText(/works best on a larger screen/)).toBeNull();
   });

   it("holds the editor back on a narrow screen until Edit anyway", () => {
      stubWidth(true);
      render(
         <NarrowEditGate>
            <p>the editor</p>
         </NarrowEditGate>,
      );
      expect(screen.queryByText("the editor")).toBeNull();
      expect(
         screen.getByText("Editing works best on a larger screen"),
      ).toBeDefined();
      fireEvent.click(screen.getByRole("button", { name: "Edit anyway" }));
      expect(screen.getByText("the editor")).toBeDefined();
   });

   it("decides once, so widening the window does not swap the editor away", () => {
      stubWidth(false);
      const view = render(
         <NarrowEditGate>
            <p>the editor</p>
         </NarrowEditGate>,
      );
      stubWidth(true);
      view.rerender(
         <NarrowEditGate>
            <p>the editor</p>
         </NarrowEditGate>,
      );
      expect(screen.getByText("the editor")).toBeDefined();
   });
});
