// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The materialization rewriters' name and position helpers over the
 * statement tokens of the Malloy lexer core (`service/malloy_lexer_core.ts`).
 *
 * Pure.
 */

import type { Token } from "../../malloy_lexer_core";

/** Whether `t` can name a definition: a word or a quoted identifier. */
export function isName(t: Token | undefined): t is Token {
   return !!t && (t.kind === "word" || t.text.startsWith("`"));
}

/** The character offset of a zero-based line/character position, or -1. */
export function offsetOf(
   text: string,
   pos: { line: number; character: number },
): number {
   let offset = 0;
   let line = 0;
   while (line < pos.line) {
      const nl = text.indexOf("\n", offset);
      if (nl < 0) return -1;
      offset = nl + 1;
      line++;
   }
   const lineEnd = text.indexOf("\n", offset);
   const length = (lineEnd < 0 ? text.length : lineEnd) - offset;
   if (pos.character > length) return -1;
   return offset + pos.character;
}
