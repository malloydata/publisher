// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Where a SKILL.md's leading frontmatter block closes, and which newline
 * style it uses. Tolerates both LF and CRLF: an upstream sync or a Windows
 * checkout can produce either, and a parser that only recognized LF (`---\n`)
 * classified every skill on a Windows checkout as having no frontmatter block
 * at all, since git there checks the files out as CRLF.
 *
 * `index` is the absolute offset of the newline that precedes the closing
 * `---`, matching where the caller wants to splice a new line in before it.
 * Returns null when there is no leading frontmatter block.
 */
export function locateFrontmatterClose(
   text: string,
): { index: number; newline: string } | null {
   const open = /^---\r?\n/.exec(text);
   if (!open) return null;
   const close = /\r?\n---/.exec(text.slice(open[0].length));
   if (!close) return null;
   return {
      index: open[0].length + close.index,
      newline: open[0].endsWith("\r\n") ? "\r\n" : "\n",
   };
}
