// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { PALETTE } from "../styles";
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

describe("CONTENT_TINT contrast", () => {
   /**
    * Every tint fills a 32px backplate behind an 18px white glyph, so it has to
    * clear 3:1
    * against white, WCAG's minimum for a graphical object. Asserted rather than
    * written down, because the failure is invisible: a lighter colour looks fine
    * in a screenshot and the icon simply stops being readable.
    *
    * Over `CONTENT_TINT` rather than over `MALLOY_ACCENT`, because the tints are
    * what actually get painted. Asserting the accent palette alone left three of
    * the six painted values unchecked.
    */
   it.each(Object.entries(CONTENT_TINT))(
      "%s clears 3:1 against white",
      (_type, hex) => {
         expect(contrastWithWhite(hex)).toBeGreaterThanOrEqual(3);
      },
   );
});

describe("PALETTE", () => {
   // Every hue holds the bar, whether or not it is assigned to a content type
   // yet — so picking one for a new plate is always safe, and there is no
   // exception list to grandfather anything into. There used to be one: the
   // logo's teal sat at 2.5:1 and painted a row whose glyph did not meet the
   // standard.
   it.each(Object.entries(PALETTE))(
      "%s clears 3:1 against white",
      (_name, hex) => {
         expect(contrastWithWhite(hex)).toBeGreaterThanOrEqual(3);
      },
   );

   // Two kinds of thing painted the same colour is the failure the tints exist
   // to prevent, so the palette they are drawn from must not repeat itself.
   it("has no duplicate hues", () => {
      const values = Object.values(PALETTE);
      expect(new Set(values).size).toBe(values.length);
   });
});
