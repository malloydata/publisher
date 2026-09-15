// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A line diff, for showing an author what a structural save will do to the
 * file before it does it.
 *
 * Adding or removing a tile moves a declaration and the comment block above
 * it, and the file cannot say whether that comment belonged to the tile, the
 * row, or the page. The writer makes a defensible choice (a tile's `#` tags
 * go with it; `//` comments stay), and this is what turns that choice from a
 * silent one into one the author sees and approves.
 *
 * Longest-common-subsequence over lines: exact, and a dashboard file is a few
 * hundred lines, so the quadratic table is small. Past `LIMIT` lines a side,
 * the diff degrades to "everything changed" rather than stalling the page.
 */
export type DiffLine =
   | { kind: "same"; text: string }
   | { kind: "add"; text: string }
   | { kind: "del"; text: string };

const LIMIT = 4000;

export function lineDiff(before: string, after: string): DiffLine[] {
   const a = before.split("\n");
   const b = after.split("\n");
   if (a.length > LIMIT || b.length > LIMIT) {
      return [
         ...a.map((text): DiffLine => ({ kind: "del", text })),
         ...b.map((text): DiffLine => ({ kind: "add", text })),
      ];
   }
   // lcs[i][j] = length of the LCS of a[i..] and b[j..].
   const lcs: Uint32Array[] = Array.from(
      { length: a.length + 1 },
      () => new Uint32Array(b.length + 1),
   );
   for (let i = a.length - 1; i >= 0; i--)
      for (let j = b.length - 1; j >= 0; j--)
         lcs[i][j] =
            a[i] === b[j]
               ? lcs[i + 1][j + 1] + 1
               : Math.max(lcs[i + 1][j], lcs[i][j + 1]);
   const out: DiffLine[] = [];
   let i = 0;
   let j = 0;
   while (i < a.length && j < b.length) {
      if (a[i] === b[j]) {
         out.push({ kind: "same", text: a[i] });
         i++;
         j++;
      } else if (lcs[i + 1][j] >= lcs[i][j + 1]) {
         out.push({ kind: "del", text: a[i++] });
      } else {
         out.push({ kind: "add", text: b[j++] });
      }
   }
   while (i < a.length) out.push({ kind: "del", text: a[i++] });
   while (j < b.length) out.push({ kind: "add", text: b[j++] });
   return out;
}

/**
 * The diff with unchanged stretches folded, keeping `context` lines around
 * every change — what a review tool shows, and all a reader needs to judge a
 * save. A fold is one entry saying how many lines it hides.
 */
export type DiffHunkLine = DiffLine | { kind: "fold"; count: number };

export function foldUnchanged(lines: DiffLine[], context = 3): DiffHunkLine[] {
   const keep = new Array<boolean>(lines.length).fill(false);
   lines.forEach((line, index) => {
      if (line.kind === "same") return;
      for (
         let k = Math.max(0, index - context);
         k <= Math.min(lines.length - 1, index + context);
         k++
      )
         keep[k] = true;
   });
   const out: DiffHunkLine[] = [];
   let folded = 0;
   lines.forEach((line, index) => {
      if (keep[index]) {
         if (folded > 0) out.push({ kind: "fold", count: folded });
         folded = 0;
         out.push(line);
      } else folded++;
   });
   if (folded > 0) out.push({ kind: "fold", count: folded });
   return out;
}
