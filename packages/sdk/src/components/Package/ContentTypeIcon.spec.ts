// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { MALLOY_ACCENT } from "../styles";
import { CONTENT_TINT, type ContentType } from "./ContentTypeIcon";

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

/**
 * Content types whose colour predates this bar. `report` is backed by
 * `MALLOY_BRAND.teal` at 2.5:1, which is below it; that is inherited from the
 * logo and is a brand decision rather than a component one.
 *
 * Keyed by content TYPE rather than by colour, which is the whole point. A new
 * type handed teal is not grandfathered and fails, and fixing teal one day
 * leaves this entry merely unused rather than turning a correct fix into a red
 * suite.
 */
const GRANDFATHERED: ContentType[] = ["report"];

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
   it.each(
      Object.entries(CONTENT_TINT).filter(
         ([type]) => !GRANDFATHERED.includes(type as ContentType),
      ),
   )("%s clears 3:1 against white", (_type, hex) => {
      expect(contrastWithWhite(hex)).toBeGreaterThanOrEqual(3);
   });

   // Guards the exception list against drift: a grandfathered name that no
   // longer matches a `ContentType` fails here rather than silently exempting
   // nothing. It does NOT catch the list being emptied, which passes
   // vacuously; that case is caught instead by the contrast assertion for the
   // type that stops being skipped, and an empty list is the desired end state
   // anyway.
   it("grandfathers only types that exist", () => {
      for (const type of GRANDFATHERED) {
         expect(CONTENT_TINT).toHaveProperty(type);
      }
   });
});

describe("MALLOY_ACCENT", () => {
   // The accents were chosen against this bar, so they hold it even before any
   // of them is assigned to a content type.
   it.each(Object.entries(MALLOY_ACCENT))(
      "%s clears 3:1 against white",
      (_name, hex) => {
         expect(contrastWithWhite(hex)).toBeGreaterThanOrEqual(3);
      },
   );
});
