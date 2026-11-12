// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Where a top-level source declaration's name ends and its trailing
 * `extend { … }` run begins, read with the materialization lexer
 * (`malloy_lexer.ts`).
 *
 * Shared by the two writers of Malloy text over an author's file: the re-root
 * rewriter (serve/served_copy.ts), which keeps a declaration's trailing
 * `extend` run and replaces its base, and the build-file generator's build
 * copy (plan/build_file_generator.ts), which may take a given-reading
 * `where:` out of the build only where that run holds it. One reader, so the
 * two never disagree about which text the served copy keeps.
 */

import { lexAll, type Token } from "./malloy_lexer";

/**
 * Where the name ends (`is` begins) and where the trailing run of top-level
 * `extend { … }` blocks begins, if any. Undefined when no top-level `is`
 * follows the name.
 */
export function declarationSpans(declaration: string):
   | {
        nameEnd: number;
        extendStart?: number;
        queryShaped: boolean;
        /**
         * The base is a connection's table or SQL (`conn.table('…')`,
         * `conn.sql("…")`) and nothing else: no source of the model's is
         * under it.
         */
        connectionRooted: boolean;
     }
   | undefined {
   const tokens = topLevelTokens(declaration);
   if (!tokens) return undefined;
   const isIndex = tokens.findIndex(
      (t) => t.kind === "word" && t.text === "is",
   );
   if (isIndex <= 0) return undefined;
   const nameEnd = tokens[isIndex].start;
   // Walk back from the end over `extend` + block pairs.
   let extendStart: number | undefined;
   let j = tokens.length - 1;
   while (j > isIndex + 1) {
      const close = tokens[j];
      if (close.kind !== "close") break;
      // The block's opening brace is the matching top-level "open" before it.
      const open = tokens[j - 1];
      const kw = tokens[j - 2];
      if (
         open?.kind !== "open" ||
         kw?.kind !== "word" ||
         kw.text !== "extend" ||
         j - 2 <= isIndex
      ) {
         break;
      }
      extendStart = kw.start;
      j -= 3;
   }
   // A query-shaped base has a top-level `->` before its trailing extends.
   const baseEnd = extendStart ?? declaration.length;
   const queryShaped = tokens.some(
      (t, k) =>
         k > isIndex &&
         t.text === "-" &&
         t.end <= baseEnd &&
         tokens[k + 1]?.text === ">" &&
         tokens[k + 1].start === t.end,
   );
   // `<connection> . table|sql ( … )`: the parenthesised argument is not a
   // top-level token, so exactly three tokens stand between `is` and the run.
   const base = tokens.filter(
      (t, k) => k > isIndex && t.end <= baseEnd && t.kind !== "close",
   );
   const connectionRooted =
      !queryShaped &&
      base.length === 3 &&
      base[0].kind === "word" &&
      base[1].text === "." &&
      base[1].start === base[0].end &&
      base[2].kind === "word" &&
      (base[2].text === "table" || base[2].text === "sql");
   return {
      nameEnd,
      ...(extendStart !== undefined ? { extendStart } : {}),
      queryShaped,
      connectionRooted,
   };
}

/**
 * The tokens at brace depth 0: words, quoted text, and each top-level `{ … }`
 * block as an "open" token at its `{` and a "close" token at its `}` (the
 * block's inside is skipped). Undefined when the text does not lex.
 */
export function topLevelTokens(s: string): Token[] | undefined {
   const all = lexAll(s);
   if (!all) return undefined;
   return all.filter(
      (t) =>
         t.depth === 0 &&
         ((t.kind !== "open" && t.kind !== "close") ||
            t.text === "{" ||
            t.text === "}"),
   );
}
