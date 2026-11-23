// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The statement lexer the materialization rewriters read the author's files
 * with: the re-root rewriter (serve/served_copy.ts), the served copy's
 * leaving out of joins it moves across connections (cross_connection_text.ts)
 * and the text readers here. The statement tokens (brackets and their depth,
 * which is all a statement-level edit needs) live in the one Malloy lexer
 * core (`service/malloy_lexer_core.ts`), which discovery reads source
 * annotations with too; this module adds the rewriters' name and position
 * helpers. It never parses Malloy; a lexer that cannot read the text returns
 * undefined.
 *
 * Pure.
 */

import type { Token } from "../../malloy_lexer_core";

export {
   lexAll,
   statementHead,
   statementStarts,
   type Token,
} from "../../malloy_lexer_core";

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
