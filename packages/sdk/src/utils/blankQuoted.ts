// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `text` with every double-quoted string, escapes included, replaced by `""`,
 * so a tag line can be split into words without a quoted value reading as a
 * tag name: `# label="bar_chart" line_chart` -> `# label="" line_chart`.
 *
 * A quote that never closes is left as it is, along with the rest of the text.
 *
 * A scanner rather than `/"(?:[^"\\]|\\.)*"/g`: annotation text comes from the
 * model, and that regex retries from every quote of an unclosed string, so a
 * line like `"\"\"\"…` took time quadratic in its length. This one reads each
 * character once. It can stop at the first unclosed quote because every quote
 * after it was read as an escaped character, so a scan from any of them sees
 * the same characters and cannot close either.
 */
export function blankQuoted(text: string): string {
   let out = "";
   let from = 0;
   for (;;) {
      const open = text.indexOf('"', from);
      if (open < 0) break;
      let at = open + 1;
      while (at < text.length && text[at] !== '"')
         at += text[at] === "\\" ? 2 : 1;
      if (at >= text.length) break;
      out += `${text.slice(from, open)}""`;
      from = at + 1;
   }
   return out + text.slice(from);
}
