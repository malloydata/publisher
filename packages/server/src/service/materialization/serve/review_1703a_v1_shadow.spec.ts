// Review probes (PR #1703): the shadow comparison must not call two different
// answers equal. Not for merge.
import { describe, expect, it } from "bun:test";
import { shadowRowDifference } from "./v1_shadow";

describe("shadowRowDifference: false equals", () => {
   it("tells two different strings of digits apart", () => {
      // A zip or account code; a cast that round-trips through a number
      // drops the leading zeros.
      expect(shadowRowDifference([{ z: "007" }], [{ z: "7" }])).toBeDefined();
   });
   it("compares integers exactly, not within a float tolerance", () => {
      expect(
         shadowRowDifference(
            [{ n: 1_000_000_000_000 }],
            [{ n: 1_000_000_000_500 }],
         ),
      ).toBeDefined();
   });
   it("tells two bigints above 2^53 apart", () => {
      expect(
         shadowRowDifference(
            [{ n: 9007199254740993n }],
            [{ n: 9007199254740992n }],
         ),
      ).toBeDefined();
   });
});
