import { describe, expect, it } from "bun:test";
import { MALLOY_ACCENT } from "../styles";
import { CONTENT_TINT } from "./ContentTypeIcon";

/** Relative luminance, WCAG 2.x definition. */
function luminance(hex: string): number {
   const channels = [1, 3, 5].map(
      (i) => parseInt(hex.slice(i, i + 2), 16) / 255,
   );
   const [r, g, b] = channels.map((c) =>
      c <= 0.03928 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4,
   );
   return 0.2126 * r + 0.7152 * g + 0.0722 * b;
}

function contrastWithWhite(hex: string): number {
   return (1 + 0.05) / (luminance(hex) + 0.05);
}

describe("CONTENT_TINT", () => {
   /**
    * The whole point of the map. Before the package row derived its color from
    * its type, four of the six rows were handed the same teal by four separate
    * call sites, so color distinguished two kinds out of six. A repeat here is
    * that regression arriving again, and nothing renders differently enough for
    * a person to notice it in review.
    */
   it("gives every content type its own color", () => {
      const values = Object.values(CONTENT_TINT);
      expect(new Set(values).size).toBe(values.length);
   });
});

describe("MALLOY_ACCENT", () => {
   /**
    * These sit behind a white glyph, so each has to clear 3:1 against white,
    * WCAG's minimum for a graphical object. Asserted rather than written down,
    * because the failure is invisible: a lighter accent looks fine in a
    * screenshot and the icon simply stops being readable.
    *
    * Scoped to the accents deliberately. `MALLOY_BRAND` is taken from the logo
    * and its teal is 2.5:1, below this bar; that is a brand decision and a
    * pre-existing one, so asserting it here would fail on day one.
    */
   it.each(Object.entries(MALLOY_ACCENT))(
      "%s clears 3:1 against white",
      (_name, hex) => {
         expect(contrastWithWhite(hex)).toBeGreaterThanOrEqual(3);
      },
   );
});
