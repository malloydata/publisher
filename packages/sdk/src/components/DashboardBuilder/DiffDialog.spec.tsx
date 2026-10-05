// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "bun:test";
import { DiffDialog } from "./DiffDialog";

describe("DiffDialog", () => {
   it("marks a removed line with its sign and no strikethrough", () => {
      render(
         <DiffDialog
            open
            before={"keep\nold line\n"}
            after={"keep\nnew line\n"}
            onClose={() => {}}
         />,
      );
      const removed = screen.getByText(/old line/);
      expect(removed.textContent).toContain("− old line");
      expect(getComputedStyle(removed).textDecorationLine).not.toBe(
         "line-through",
      );
      expect(screen.getByText(/new line/).textContent).toContain("+ new line");
   });
});
