// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { tileGridColumn } from "./Dashboard";

describe("tileGridColumn", () => {
   it("spans one column when the view carries no colspan", () => {
      expect(tileGridColumn({}, 12)).toBe("span 1");
   });

   it("spans the colspan the view asked for", () => {
      expect(tileGridColumn({ colspan: 8 }, 12)).toBe("span 8");
   });

   // Clamped rather than allowed to overflow the grid, matching how
   // @malloydata/render clamps the same tag on a `# dashboard` nest child. The
   // whole point of reading the tag off the view is that one view lays out the
   // same either way, which a different overflow rule would break.
   it("clamps a colspan wider than the grid", () => {
      expect(tileGridColumn({ colspan: 20 }, 4)).toBe("span 4");
   });

   // An explicit start line is what pushes the tile onto a fresh row; `span N`
   // alone would let it flow into whatever columns are left beside its sibling.
   it("starts a new row for a break", () => {
      expect(tileGridColumn({ colspan: 6, break: true }, 12)).toBe(
         "1 / span 6",
      );
   });

   it("breaks with no colspan too", () => {
      expect(tileGridColumn({ break: true }, 12)).toBe("1 / span 1");
   });
});
