// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Coverage for the storefront data app's one pure module.
//
//   bun run test:examples
//   node --test examples/storefront/tests/format.test.mjs
//
// Named, not globbed: `node --test` on a glob that matches nothing exits 0 and
// reports "tests 0", so the globbed form passes while running none of this.
//
// The filter cases below are the point of this file. Every one of them is a
// value a hand-rolled encoder turns into something Malloy matches differently
// from what the reader picked, and every such failure is silent: the page
// renders a table either way.
//
// They run against the VENDORED bundle, which is what the page loads, so they
// prove the page and these expectations agree. They cannot prove the vendored
// copy is current: after a `@malloydata/*` bump the page and these tests would
// share the same stale grammar and agree with each other while the server
// parsed with the new one, which is the silent mismatch this module exists to
// prevent. `upgrade-malloy.sh` does not regenerate the bundle, so nothing else
// closes that either. The drift test at the end of this file does.
import { test } from "node:test";
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { readFile } from "node:fs/promises";
import { StringFilterExpression } from "../public/vendor/malloy-filter.js";
import {
   decodeAtLeast,
   decodeFilterList,
   encodeAtLeast,
   encodeFilterList,
   encodeFilterValue,
   formatValue,
   isPlainFilterList,
   readFilterForPicker,
   monthLabel,
   monthOfYearLabel,
   plural,
   readGivenDefault,
} from "../public/app/format.js";

// Built rather than typed, so no editor or tool can quietly normalise them.
const NBSP = String.fromCharCode(0xa0);
const TAB = String.fromCharCode(9);
const NEWLINE = String.fromCharCode(10);
const BACKSLASH = String.fromCharCode(92);

/** What Malloy will actually match, which is the only question that matters. */
function valuesMalloyWillMatch(filter) {
   const { parsed, log } = StringFilterExpression.parse(filter);
   assert.equal(log?.length ?? 0, 0, `filter did not parse: ${filter}`);
   assert.notEqual(parsed, null, `filter is a null clause: ${filter}`);
   assert.equal(parsed.operator, "=", `not a plain equality: ${filter}`);
   assert.notEqual(parsed.not, true, `filter is negated: ${filter}`);
   return parsed.values;
}

// Each of these means something other than itself in the filter grammar. The
// note on each is what it turns into when it is written out unescaped, or (for
// the quote cases) wrapped in double quotes, which the string grammar has no
// notion of. All measured against this same parser.
const HOSTILE = [
   ["-Outerwear", "is a negation: everything EXCEPT Outerwear"],
   ["%", "is a match-anything pattern, so every row comes back"],
   ["50% off", "is a wildcard match"],
   ["a_b", "is a single-character wildcard match"],
   ["Ben & Jerry, Inc", "splits into two brands on the comma"],
   ['The "Best" Brand', "keeps the quote characters inside the matched value"],
   [`AC${BACKSLASH}DC`, "escapes the character after the backslash"],
   ["null", "is the null operator"],
   ["empty", "is the empty operator"],
   ["a;b", "is a conjunction"],
   ["(x)", "is a grouping"],
   [" Nike", "loses the leading space"],
];

test("a value Malloy would read as syntax survives as a literal", () => {
   for (const [value, wasWrongBecause] of HOSTILE) {
      const filter = encodeFilterValue(value);
      assert.notEqual(filter, "", `${value}: refused, and it ${wasWrongBecause}`);
      assert.deepEqual(
         valuesMalloyWillMatch(filter),
         [value],
         `${value}: it ${wasWrongBecause}`,
      );
   }
});

test("several values become one 'any of these' filter", () => {
   const filter = encodeFilterList(["Nike", "Ben & Jerry, Inc", "-Outerwear"]);
   assert.deepEqual(valuesMalloyWillMatch(filter), [
      "Nike",
      "Ben & Jerry, Inc",
      "-Outerwear",
   ]);
});

test("a value that cannot round-trip is dropped, not approximated", () => {
   // Measured, not assumed. Each of these parses to a null clause, which Malloy
   // compiles to the SQL constant `true` and so matches every row. Encoding one
   // would widen the filter silently, which is the failure this module exists
   // to prevent. A non-breaking space is ordinary in pasted and scraped data.
   for (const value of ["", NBSP, TAB, NBSP + NBSP]) {
      assert.equal(
         encodeFilterValue(value),
         "",
         `${JSON.stringify(value)} should be refused`,
      );
   }
   // A newline makes the whole expression unparseable rather than mismatched.
   assert.equal(encodeFilterValue(`a${NEWLINE}b`), "");
   // A trailing tab comes back without the tab, so it is a different value.
   assert.equal(encodeFilterValue(`a${TAB}`), "");
   // One unusable value does not take the good ones with it.
   assert.deepEqual(
      valuesMalloyWillMatch(encodeFilterList(["Nike", NBSP, "Loft"])),
      ["Nike", "Loft"],
   );
});

test("ordinary spaces are carried, where a trim() guard would drop them", () => {
   // This is why the guard asks the grammar instead of predicting. A space
   // escapes and round-trips; the non-breaking space above does not. A
   // `value.trim() === ""` test cannot tell those two apart, and it is wrong
   // about this one: " " is a value some column really holds.
   assert.deepEqual(valuesMalloyWillMatch(encodeFilterValue(" ")), [" "]);
   assert.deepEqual(valuesMalloyWillMatch(encodeFilterValue("  ")), ["  "]);
});

test("no usable values is the empty filter, which means All", () => {
   assert.equal(encodeFilterList([]), "");
   assert.equal(encodeFilterList([NBSP, ""]), "");
});

test("non-strings a control can produce", () => {
   assert.equal(encodeFilterValue(null), "");
   assert.equal(encodeFilterValue(undefined), "");
   assert.equal(encodeFilterValue(Number.NaN), "");
   assert.equal(encodeFilterValue(Number.POSITIVE_INFINITY), "");
   assert.equal(encodeFilterValue(new Date("nope")), "");
   // What Malloy matches, not what the string looks like: a number and a date
   // go through the same escaping as a picked string, because the interesting
   // ones do not print as themselves.
   assert.deepEqual(valuesMalloyWillMatch(encodeFilterValue(true)), ["true"]);
   assert.deepEqual(valuesMalloyWillMatch(encodeFilterValue(42)), ["42"]);
   assert.deepEqual(
      valuesMalloyWillMatch(encodeFilterValue(new Date("2024-03-05T00:00:00Z"))),
      ["2024-03-05"],
   );
});

test("a negative number is a value, not a negation", () => {
   // `-5` written out as itself parses as "not 5", so a numeric control that
   // can reach a negative number would silently return the complement of the
   // rows the reader asked for. This is the string case's failure in the one
   // place it is easy to assume cannot have it.
   for (const n of [-5, -0.5]) {
      assert.deepEqual(valuesMalloyWillMatch(encodeFilterValue(n)), [String(n)]);
   }
});

test("a picker's selection survives the trip through the URL", () => {
   for (const values of [
      ["Nike"],
      ["Nike", "Loft"],
      ["Ben & Jerry, Inc", "-Outerwear", "50% off"],
   ]) {
      assert.deepEqual(decodeFilterList(encodeFilterList(values)), values);
   }
});

test("a filter a picker cannot represent is preserved, not mangled", () => {
   // `-Nike` means "everything except Nike". Re-encoding it as a literal would
   // turn it into a search for those five characters, so the picker has to know
   // it cannot show this one and hand it to a text box instead.
   assert.equal(isPlainFilterList("-Nike"), false);
   assert.deepEqual(decodeFilterList("-Nike"), ["-Nike"]);
   assert.equal(isPlainFilterList("Nike, Loft"), true);
   assert.equal(isPlainFilterList(""), true);
   assert.equal(isPlainFilterList(undefined), true);
});

test("a picker reads values and representability in one call", () => {
   // The two answers are returned together on purpose. Asking for the values
   // and forgetting to ask whether they can be represented is what shipped the
   // bug twice, once in each picker: `-Nike` means everything EXCEPT Nike, and
   // re-encoding it as a literal turns it into a search for five characters.
   assert.deepEqual(readFilterForPicker("-Nike"), {
      values: ["-Nike"],
      opaque: true,
   });
   assert.deepEqual(readFilterForPicker("Nike, Loft"), {
      values: ["Nike", "Loft"],
      opaque: false,
   });
   assert.deepEqual(readFilterForPicker(""), { values: [], opaque: false });
   assert.deepEqual(readFilterForPicker(undefined), {
      values: [],
      opaque: false,
   });
   // A wildcard is equally unrepresentable, and equally dangerous re-encoded.
   assert.equal(readFilterForPicker("%foo%").opaque, true);
});

test("what a picker must do with an opaque filter, as data", () => {
   // This models the decision the picker makes, in encoder terms: an opaque
   // filter contributes NOTHING to a new selection, because extending it
   // re-encodes it. It does NOT pin the picker. Reverting `controls.js` to
   // appending leaves every assertion here green, which was measured rather
   // than assumed. `controls.test.mjs` is what fails in that case.
   const { values, opaque } = readFilterForPicker("-Nike");
   const base = opaque ? [] : values;
   assert.deepEqual(base, [], "an opaque filter must be replaced, not extended");
   assert.deepEqual(
      decodeFilterList(encodeFilterList([...base, "Levi's"])),
      ["Levi's"],
      "so the next pick stands alone",
   );
   // And the shape that shipped: extending it produces the literal search.
   assert.deepEqual(
      decodeFilterList(encodeFilterList([...values, "Levi's"])),
      ["-Nike", "Levi's"],
      "extending re-encodes the hand-written filter as a literal",
   );
});

test("a slider's lower bound, both ways", () => {
   assert.equal(decodeAtLeast(encodeAtLeast(50)), 50);
   assert.equal(decodeAtLeast(encodeAtLeast(0.5)), 0.5);
   assert.equal(encodeAtLeast(0), "");
   assert.equal(encodeAtLeast(-5), "");
   assert.equal(encodeAtLeast("nope"), "");
});

test("a NEGATED lower bound is not a lower bound", () => {
   // `not >= 100` parses to the same operator and value as `>= 100` with a
   // `not` flag, so a reader that ignores the flag draws the slider at $100
   // over data filtered to BELOW $100: the bound inverted, and the control
   // stating the opposite of what is in force.
   assert.equal(decodeAtLeast("not >= 100"), 0);
   assert.equal(decodeAtLeast(">= 100"), 100);
});

test("a filter that is not a lower bound reads as no bound", () => {
   // The regex this replaced returned 2 for `<= 2`, 9 for `!= 9`, and 5 for
   // `>= .5`: a bound the filter never expressed, or ten times the real one.
   for (const filter of ["<= 2", "> 7", "!= 9", ">= 50 | >= 100", ""]) {
      assert.equal(decodeAtLeast(filter), 0, filter);
   }
   assert.equal(decodeAtLeast(">= 1e3"), 1000);
   assert.equal(decodeAtLeast(">= .5"), 0.5);
});

test("a given's default literal, unwrapped for a control", () => {
   assert.equal(readGivenDefault("f''"), "");
   assert.equal(readGivenDefault("f'Nike'"), "Nike");
   assert.equal(readGivenDefault("@2023-01-01"), "2023-01-01");
   assert.equal(readGivenDefault(undefined), "");
});

// The expectations below are written in en-US, because `formatValue` formats in
// the READER's locale, which is the right behaviour for a page and the wrong
// thing to hardcode in an assertion. Measured: under de-DE the same call gives
// `1.235 $` rather than `$1,235`. Stated and skipped rather than left to fail
// with a confusing diff, and CI runs en-US so it does run there.
const EN_US = new Intl.NumberFormat()
   .resolvedOptions()
   .locale.startsWith("en-US");

test("cells format the way their field's tags ask", {
   skip: EN_US
      ? false
      : "written for en-US; this run formats in another locale",
}, () => {
   assert.equal(formatValue(null), "—");
   assert.equal(formatValue(undefined), "—");
   assert.equal(formatValue(1234.5, "currency"), "$1,235");
   assert.equal(formatValue(0.125, "percent"), "12.5%");
   // Five digits, because the assertion has to be able to FAIL: a 3-digit
   // number is ungrouped by every locale, so `formatValue(884, "id")` passes
   // identically with the `id` branch deleted. Mutation-checked.
   assert.equal(formatValue(88884, "id"), "88884");
   assert.notEqual(formatValue(88884, "integer"), "88884");
   // The model asks for `#,##0.0` on orders-per-customer, so a value that
   // rounds to a whole number still shows its decimal place. Dropping it made
   // a deliberately-one-decimal KPI render as "3" and read like a count.
   assert.equal(formatValue(2.975, "decimal"), "3.0");
   assert.equal(formatValue(10.695, "decimal"), "10.7");
   assert.equal(formatValue("2024-03-01T00:00:00.000Z", "month"), "Mar 2024");
   assert.equal(formatValue("Nike"), "Nike");
   // An untagged column still gets thousands separators, because Malloy hands
   // a large integer back as a string. `Number` says 0 for an empty string and
   // 1 for `true` though, so those two must not take that path.
   assert.equal(formatValue("1234567"), "1,234,567");
   assert.equal(formatValue(""), "");
   assert.equal(formatValue(true), "true");
});

test("month labels", () => {
   assert.equal(monthLabel("2024-03-01T00:00:00.000Z"), "Mar 2024");
   assert.equal(monthLabel("not a date"), "not a date");
   assert.equal(monthOfYearLabel(3), "Mar");
   assert.equal(monthOfYearLabel(13), "13");
});

test("pluralizing a control's All option", () => {
   assert.equal(plural("category"), "categories");
   assert.equal(plural("brand"), "brands");
   assert.equal(plural("box"), "boxes");
});

// The guard the header refers to. Everything above compares the page against the
// bundle it loads; this compares that bundle against the package it was built
// from, which is the only comparison a stale bundle cannot satisfy by agreeing
// with itself.
//
// Shaped like the repo's other pinned-version checks (`validate-duckdb-sync`,
// `validate-pg-sync`) rather than inventing a mechanism: read the version the
// generator stamped into the banner and require it to equal the installed one.
test("the vendored grammar is the one currently installed", async () => {
   const require = createRequire(import.meta.url);
   const installed = require("@malloydata/malloy-filter/package.json").version;
   const bundle = await readFile(
      new URL("../public/vendor/malloy-filter.js", import.meta.url),
      "utf8",
   );
   const stamped = /@malloydata\/malloy-filter@([^\s(]+)/.exec(
      bundle.slice(0, 600),
   )?.[1];
   assert.ok(stamped, "the banner records which version was bundled");
   assert.equal(
      stamped,
      installed,
      `vendored ${stamped} against installed ${installed}: run \`bun run vendor:malloy-filter\` and commit the result`,
   );
});
