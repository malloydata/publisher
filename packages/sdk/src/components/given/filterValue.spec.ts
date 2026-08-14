import { StringFilterExpression } from "@malloydata/malloy-filter";
import { describe, expect, it } from "bun:test";
import {
   decodeAtLeast,
   decodeFilterList,
   encodeAtLeast,
   encodeFilterList,
   filterInnerType,
   isFilterType,
   isPlainFilterList,
} from "./filterValue";

/**
 * Values that are ordinary to a person and syntax to Malloy. Every one of these
 * was a live corruption before the encoder delegated to the filter package:
 * `-Outerwear` ran as a negation over the whole catalogue, `%` matched every
 * row, `null` hit the null operator, and `Ben & Jerry, Inc` filtered for two
 * brands, which is the exact case this module's docstring exists to prevent.
 */
const HAZARDS = [
   "Outerwear",
   "-Outerwear",
   "%",
   "100%",
   "%off%",
   "_",
   "a_b",
   "null",
   "NULL",
   "empty",
   "none",
   "Ben & Jerry, Inc",
   "a;b",
   "a|b",
   "(parens)",
   "back\\slash",
   "Levi's",
   ' say "hi" ',
   " padded ",
   "-",
];

/**
 * Read the filter back with Malloy's own parser and return the values it will
 * actually match on, or undefined if it is not a plain "is one of" clause.
 *
 * This is the assertion that matters. Checking `decode(encode(v))` only proves
 * the two halves of this module agree with each other, which the previous
 * quoting scheme also managed while agreeing with nothing else.
 */
function valuesMalloyWillMatch(filter: string): string[] | undefined {
   const { parsed } = StringFilterExpression.parse(filter);
   if (parsed === null) return undefined;
   if (parsed.operator !== "=" || parsed.not === true) return undefined;
   return parsed.values;
}

describe("filter type helpers", () => {
   it("recognizes the filter family and its inner type", () => {
      expect(isFilterType("filter<string>")).toBe(true);
      expect(isFilterType("string")).toBe(false);
      expect(isFilterType(undefined)).toBe(false);
      expect(filterInnerType("filter<timestamp>")).toBe("timestamp");
      expect(filterInnerType("number")).toBe(undefined);
   });
});

describe("encodeFilterList, against the real Malloy filter parser", () => {
   it("makes every hazardous value match itself and nothing else", () => {
      for (const value of HAZARDS) {
         expect(valuesMalloyWillMatch(encodeFilterList([value]))).toEqual([
            value,
         ]);
      }
   });

   it("keeps a multi-value selection intact whatever the values are", () => {
      for (const first of HAZARDS) {
         for (const second of HAZARDS) {
            expect(
               valuesMalloyWillMatch(encodeFilterList([first, second])),
            ).toEqual([first, second]);
         }
      }
   });

   it("joins plain values the way a string filter reads them", () => {
      expect(encodeFilterList(["us-east", "us-west"])).toBe(
         "us\\-east, us\\-west",
      );
      expect(valuesMalloyWillMatch("us\\-east, us\\-west")).toEqual([
         "us-east",
         "us-west",
      ]);
   });

   it("is empty for no selection, which is the natural All", () => {
      expect(encodeFilterList([])).toBe("");
   });

   it("drops an empty value rather than inventing a spelling for it", () => {
      // Malloy's `empty` operator means null-or-empty, a wider question than
      // "exactly the empty string", so there is nothing faithful to emit. An
      // all-empty selection therefore encodes to the empty filter, i.e. All.
      expect(encodeFilterList([""])).toBe("");
      expect(valuesMalloyWillMatch(encodeFilterList(["a", "", "b"]))).toEqual([
         "a",
         "b",
      ]);
   });
});

describe("a value that is entirely whitespace", () => {
   it("is refused rather than encoded, because encoding it matches everything", () => {
      // `unparse` emits the whitespace, `parse` returns a NULL clause for it with
      // an empty log rather than an error, and Malloy compiles a null clause to
      // the SQL constant `true`. Encoding it would silently select every row,
      // which is the widening this module exists to prevent.
      for (const raw of ["\t", "\n", "\u00a0", "\u2003", "   "]) {
         expect(encodeFilterList([raw])).toBe("");
      }
   });

   it("drops such a value from a list without dropping the rest", () => {
      expect(encodeFilterList(["Nike", "\u00a0", "Loft"])).toBe(
         encodeFilterList(["Nike", "Loft"]),
      );
   });

   it("still keeps whitespace INSIDE a value", () => {
      // The rule is "no non-whitespace character at all", not "no whitespace".
      const encoded = encodeFilterList(["  Nike  "]);
      expect(encoded).not.toBe("");
      expect(decodeFilterList(encoded)).toEqual(["  Nike  "]);
   });

   it("pins the codepoints that reach the null-clause path", () => {
      // 22 of them, and U+00A0 is ordinary in pasted or scraped data.
      const reaching: string[] = [];
      for (let cp = 1; cp < 0x3000; cp++) {
         const ch = String.fromCodePoint(cp);
         if (ch.trim() === "") reaching.push(ch);
      }
      for (const ch of reaching) expect(encodeFilterList([ch])).toBe("");
   });
});

describe("decodeFilterList", () => {
   it("round-trips the list form in either argument order", () => {
      // Order matters: the previous decoder passed only when the value needing
      // escaping came first, and returned a leading space otherwise.
      for (const values of [
         ["us-east", "us-west"],
         ["Levi's"],
         ["Ben & Jerry, Inc", "Nike"],
         ["Nike", "Ben & Jerry, Inc"],
         ['say "hi"'],
         ["back\\slash"],
         [" padded "],
         ["-Outerwear", "%", "null"],
      ]) {
         expect(decodeFilterList(encodeFilterList(values))).toEqual(values);
      }
   });

   it("round-trips every pair drawn from the hazard set", () => {
      for (const first of HAZARDS) {
         for (const second of HAZARDS) {
            const values = [first, second];
            expect(decodeFilterList(encodeFilterList(values))).toEqual(values);
         }
      }
   });

   it("reads an empty filter as no selection", () => {
      expect(decodeFilterList("")).toEqual([]);
      expect(decodeFilterList("   ")).toEqual([]);
   });

   it("survives leading and trailing spaces, which the escaper does cover", () => {
      for (const value of ["a b", " padded", "padded ", " padded "]) {
         expect(valuesMalloyWillMatch(encodeFilterList([value]))).toEqual([
            value,
         ]);
         expect(decodeFilterList(encodeFilterList([value]))).toEqual([value]);
      }
   });

   it("loses a trailing tab, and narrows rather than widens when it does", () => {
      // Documented in the module docstring: the escaper covers the space
      // character but not the tab, and the parser trims trailing whitespace.
      // Pinned so a change on either side is visible rather than silent. The
      // result is a filter for a DIFFERENT single value, never a broader one,
      // which is why it is documented rather than worked around.
      expect(valuesMalloyWillMatch(encodeFilterList(["a\t"]))).toEqual(["a"]);
      // A tab that is not trailing is unaffected.
      expect(valuesMalloyWillMatch(encodeFilterList(["a\tb"]))).toEqual([
         "a\tb",
      ]);
      // LEADING too, which the docstring used to claim only of trailing.
      expect(valuesMalloyWillMatch(encodeFilterList(["\ta"]))).toEqual(["a"]);
      // A space in either position IS escaped, so it survives. This is the
      // difference the docstring turns on, so it is pinned next to it.
      expect(valuesMalloyWillMatch(encodeFilterList([" a "]))).toEqual([" a "]);
   });

   it("does not drop a zero-width space, which is where `trim()` stops", () => {
      // The all-whitespace refusal tests with JavaScript's `trim()`, which
      // treats NBSP and the ideographic space as whitespace but not U+200B. So
      // a zero-width space is carried as an ordinary value. Benign, since it
      // filters for something useless rather than for everything, and pinned
      // because the boundary is worth knowing rather than rediscovering.
      expect(encodeFilterList(["West", "\u00a0"])).toBe("West");
      expect(encodeFilterList(["West", "\u3000"])).toBe("West");
      expect(encodeFilterList(["West", "\u200b"])).toBe("West, \u200b");
   });

   it("fails a newline outright rather than filtering by the wrong thing", () => {
      // The grammar excludes a bare newline from a match string, so this is
      // unparseable and the query errors where the reader can see it. Escaping
      // it would parse but come back carrying a stray backslash, because the
      // library's `unescape` is /\\(.)/g and `.` does not match a newline.
      expect(valuesMalloyWillMatch(encodeFilterList(["a\nb"]))).toBeUndefined();
   });

   it("keeps a filter it cannot represent in one piece", () => {
      // A control that cannot express these should preserve them, not split or
      // reinterpret them into something that means something else.
      for (const filter of [
         "-Nike",
         "%foo%",
         "null",
         "empty",
         "a; b",
         "a | b",
      ]) {
         expect(decodeFilterList(filter)).toEqual([filter]);
      }
   });
});

describe("encodeAtLeast", () => {
   it("round-trips a slider bound", () => {
      expect(encodeAtLeast(100)).toBe(">= 100");
      expect(decodeAtLeast(">= 100")).toBe(100);
      expect(decodeAtLeast(">=2.5")).toBe(2.5);
      expect(decodeAtLeast(">= -5")).toBe(-5);
   });

   it("reads spellings the parser accepts but a regex would not", () => {
      // The reason this delegates instead of matching `/^>=\s*(-?\d+...)$/`:
      // both of these are valid to `@malloydata/malloy-filter` and a regex of
      // that shape reads neither, so the slider would silently show no lower
      // bound for a filter that has one.
      expect(decodeAtLeast(">= 1e3")).toBe(1000);
      expect(decodeAtLeast(">= .5")).toBe(0.5);
      expect(decodeAtLeast("  >= -7.25  ")).toBe(-7.25);
   });

   it("survives input the parser cannot read at all", () => {
      // `parse` reports failure as a null `parsed` rather than by throwing, so
      // an unguarded read of `.operator` is a crash, not a fallback.
      for (const bad of ["((", "a,,b", ">=", "-", "\\"]) {
         expect(decodeAtLeast(bad)).toBe(undefined);
      }
   });

   it("declines to read a filter that is not a single lower bound", () => {
      // The slider then leaves the value alone rather than showing a position
      // that misrepresents it.
      expect(decodeAtLeast("100 to 500")).toBe(undefined);
      expect(decodeAtLeast("> 100")).toBe(undefined);
      expect(decodeAtLeast(">= 1, 2")).toBe(undefined);
      expect(decodeAtLeast("null")).toBe(undefined);
      expect(decodeAtLeast("")).toBe(undefined);
   });
});

describe("isPlainFilterList", () => {
   it("is true for what a picker can represent and re-encode safely", () => {
      expect(isPlainFilterList(undefined)).toBe(true);
      expect(isPlainFilterList("")).toBe(true);
      expect(isPlainFilterList("   ")).toBe(true);
      expect(isPlainFilterList("Nike")).toBe(true);
      expect(isPlainFilterList(encodeFilterList(["Nike", "Loft"]))).toBe(true);
   });

   it("is false for a filter the picker would silently rewrite", () => {
      // Each of these decodes to one opaque entry, and re-encoding that entry
      // escapes it into a literal: inverting a negation, or turning a wildcard
      // into a search for a percent sign. A caller that sees false should show
      // a text box instead.
      for (const filter of [
         "-Nike",
         "%foo%",
         "null",
         "empty",
         "a; b",
         "a | b",
      ]) {
         expect(isPlainFilterList(filter)).toBe(false);
      }
   });

   it("names the exact hazard: a negation re-encodes as a literal", () => {
      const chips = decodeFilterList("-Nike");
      expect(chips).toEqual(["-Nike"]);
      // This is what the picker WOULD do, and why it must not.
      expect(encodeFilterList(chips)).toBe("\\-Nike");
      expect(isPlainFilterList("-Nike")).toBe(false);
   });
});
