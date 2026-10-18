// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { blankQuoted } from "./blankQuoted";

describe("blankQuoted", () => {
   it("blanks each quoted value, escapes included", () => {
      expect(blankQuoted('# label="bar_chart" line_chart')).toBe(
         '# label="" line_chart',
      );
      expect(blankQuoted('# a="x \\" y" b="z"')).toBe('# a="" b=""');
      expect(blankQuoted('# a="x\\\\" bar_chart')).toBe('# a="" bar_chart');
      expect(blankQuoted("# bar_chart")).toBe("# bar_chart");
      expect(blankQuoted('""')).toBe('""');
   });

   it("leaves an unclosed quote and the rest of the line as they are", () => {
      expect(blankQuoted('# a="x" b="bar_chart')).toBe('# a="" b="bar_chart');
      expect(blankQuoted('# "\\"')).toBe('# "\\"');
   });

   // The regex it replaces, for lines it reads in linear time.
   const QUOTED = /"(?:[^"\\]|\\.)*"/g;
   it("agrees with the regex it replaces on every short line of quotes, escapes and words", () => {
      const alphabet = ['"', "\\", "a", " "];
      let checked = 0;
      for (let length = 0; length <= 7; length++) {
         const total = alphabet.length ** length;
         for (let n = 0; n < total; n++) {
            let text = "";
            for (let k = 0, rest = n; k < length; k++) {
               text += alphabet[rest % alphabet.length];
               rest = Math.floor(rest / alphabet.length);
            }
            expect(blankQuoted(text)).toBe(text.replace(QUOTED, '""'));
            checked++;
         }
      }
      expect(checked).toBe(21845);
   });

   it("reads a long unclosed line of escaped quotes in linear time", () => {
      // `"` then many `\"`: the shape that made the regex quadratic.
      const hostile = `# "${'\\"'.repeat(200_000)}`;
      const started = performance.now();
      expect(blankQuoted(hostile)).toBe(hostile);
      expect(performance.now() - started).toBeLessThan(250);
   });
});
