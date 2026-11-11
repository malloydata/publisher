// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What the viewer's scans of a model file's text share: the givens a source
 * reads, the order an `export { … }` lists, the source a derived one builds on.
 * They read the text with regular expressions rather than the parser (which
 * loads lazily), so each first masks what is not code.
 */

/**
 * The text with every comment and the contents of every string literal blanked
 * to spaces, line breaks kept, so a scan sees only code: a `$GIVEN`, a brace or
 * a keyword inside a comment or a string is not one. Offsets and line numbers
 * are unchanged.
 *
 * Comments are `//` and `--` to the line's end, and `/* … *\/`. Strings are
 * `'…'` and `"…"` (a backslash escapes the next character; a string ends at the
 * line's end, as Malloy's do), with any prefix (`f'…'`, `r'…'`) left as code,
 * and `"""…"""` across lines. A quoted identifier (`` `order items` ``) is code
 * and is kept.
 */
export function maskCommentsAndStrings(text: string): string {
   let out = "";
   let i = 0;
   const blank = (from: number, to: number) => {
      out += text.slice(from, to).replace(/[^\n]/g, " ");
      i = to;
   };
   while (i < text.length) {
      const c = text[i];
      const two = text.slice(i, i + 2);
      if (two === "//" || two === "--") {
         const end = text.indexOf("\n", i);
         blank(i, end === -1 ? text.length : end);
      } else if (two === "/*") {
         const end = text.indexOf("*/", i + 2);
         blank(i, end === -1 ? text.length : end + 2);
      } else if (text.startsWith('"""', i)) {
         const end = text.indexOf('"""', i + 3);
         const close = end === -1 ? text.length : end;
         out += '"""';
         i += 3;
         blank(i, close);
         if (end !== -1) {
            out += '"""';
            i += 3;
         }
      } else if (c === "'" || c === '"') {
         out += c;
         let j = i + 1;
         while (j < text.length && text[j] !== c && text[j] !== "\n")
            j += text[j] === "\\" && text[j + 1] !== "\n" ? 2 : 1;
         blank(i + 1, Math.min(j, text.length));
         if (text[i] === c) {
            out += c;
            i += 1;
         }
      } else if (c === "`") {
         let j = i + 1;
         while (j < text.length && text[j] !== "`" && text[j] !== "\n")
            j += text[j] === "\\" ? 2 : 1;
         const end = Math.min(j + 1, text.length);
         out += text.slice(i, end);
         i = end;
      } else {
         out += c;
         i += 1;
      }
   }
   return out;
}

/** A string matched literally inside a `RegExp`. */
export function escapeRegExp(value: string): string {
   return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
