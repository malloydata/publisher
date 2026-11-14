// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { resolveTheme } from "../../theme/resolveTheme";
import { selectionSx } from "./TileFrame";

const theme = resolveTheme([], "light");
const ringInset = (sx: ReturnType<typeof selectionSx>) =>
   (sx as { "&::before": { inset: string } })["&::before"].inset;

describe("selectionSx: a bare ring", () => {
   it("stands off its prose by one distance all round", () => {
      expect(
         ringInset(
            selectionSx(theme, { selected: true, bare: true, ringOffset: 12 }),
         ),
      ).toBe("-12px");
   });

   it("stands off by one distance across and another above and below", () => {
      // The description's: wide across, close above and below, so its halo
      // stays inside the gaps to the title and the filter row.
      expect(
         ringInset(
            selectionSx(theme, {
               selected: true,
               bare: true,
               ringOffset: { x: 12, y: 6 },
            }),
         ),
      ).toBe("-6px -12px");
   });
});
