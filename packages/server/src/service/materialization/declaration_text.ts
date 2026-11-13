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

/** One edit to a text: `[start, end)` replaced by `insert`. */
export interface TextEdit {
   start: number;
   end: number;
   insert: string;
}

/** The access labels a declaration's build copy lifts. */
const LIFTED_LABELS: ReadonlySet<string> = new Set(["private", "internal"]);

/**
 * The edits that lift a source declaration's own access modifiers, so a query
 * over the source can select the fields they hide (P3: the build stores the
 * source's private and internal fields). Offsets are into `declaration`.
 *
 * - A statement's label (`private dimension:`, `internal rename:`) is deleted.
 * - An `include { … }` block's labels (`private: org_id`, `internal: *`)
 *   become `public`, and a block with no wildcard gains a `public: *` item,
 *   since `include` makes every field it does not list private.
 *
 * `except:` is left alone: an excluded field is never stored. Only the
 * declaration's own text is edited, so a field private on a source it extends
 * (ancestor-private) stays private, and `include { public: * }` cannot lift it
 * either (Malloy keeps an inaccessible field inaccessible). Undefined when the
 * text does not lex. The inserted text is Malloy's own keywords, no name.
 */
export function accessLiftEdits(declaration: string): TextEdit[] | undefined {
   const tokens = lexAll(declaration);
   if (!tokens) return undefined;
   const edits: TextEdit[] = [];
   for (let k = 0; k < tokens.length; k++) {
      const t = tokens[k];
      if (t.kind !== "word" || !LIFTED_LABELS.has(t.text)) continue;
      const next = tokens[k + 1];
      const after = tokens[k + 2];
      if (next?.text === ":" && next.start === t.end && after?.text !== ":") {
         // An include item's label: `private: org_id`.
         edits.push({ start: t.start, end: t.end, insert: "public" });
      } else if (
         next?.kind === "word" &&
         after?.text === ":" &&
         after.start === next.end &&
         tokens[k + 3]?.text !== ":"
      ) {
         // A statement's label: `private dimension: …`.
         edits.push({ start: t.start, end: next.start, insert: "" });
      }
   }
   for (let k = 0; k + 1 < tokens.length; k++) {
      const t = tokens[k];
      const open = tokens[k + 1];
      if (t.kind !== "word" || t.text !== "include") continue;
      if (open.kind !== "open" || open.text !== "{") continue;
      const close = tokens.find(
         (c, j) => j > k + 1 && c.kind === "close" && c.depth === open.depth,
      );
      if (!close || close.text !== "}") return undefined;
      const wildcard = tokens.some(
         (c) =>
            c.start > open.start &&
            c.end <= close.start &&
            c.depth === open.depth + 1 &&
            c.text === "*",
      );
      if (!wildcard) {
         edits.push({
            start: close.start,
            end: close.start,
            insert: "\n  public: *\n",
         });
      }
   }
   return edits;
}

/** `text` with `edits` applied; undefined when two of them overlap. */
export function applyEdits(
   text: string,
   edits: readonly TextEdit[],
): string | undefined {
   const ordered = [...edits].sort(
      (a, b) => b.start - a.start || b.end - a.end,
   );
   for (let i = 1; i < ordered.length; i++) {
      // Sorted from the end: each edit must end at or before the one after it.
      if (ordered[i].end > ordered[i - 1].start) return undefined;
   }
   let out = text;
   for (const e of ordered) {
      out = out.slice(0, e.start) + e.insert + out.slice(e.end);
   }
   return out;
}
