// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The Malloy text that stands an inlined join_one's stored columns in for the
 * join (materialization v2, P5 with #1665): the join's definition taken out of
 * a run of `extend { … }` blocks, and in its place each stored
 * `<join>__<field>` column renamed internally and a dimension of the join's
 * name, a record of the joined source's fields over those columns. A hidden
 * join's renames and record carry its modifier.
 *
 * One emitter for both readers of a stored table: the served copy, which
 * reads an inlined join from its source's own table (`serve/served_copy.ts`),
 * and a `storage=` extension table's build over its parent's stored table
 * (`plan/build_file_generator.ts`, `storedChainText`), which reads the
 * parent's inlined join the same way. A leaf, so both import it.
 */

import { doubleQuotedString, identifier } from "../malloy_text";
import { lexAll, isName } from "./malloy_lexer";
import { definitionCutAt } from "./plan/cross_connection_joins";

/** The timeframes a stored truncation can carry, the only ones emitted. */
export const TIMEFRAMES: ReadonlySet<string> = new Set([
   "second",
   "minute",
   "hour",
   "day",
   "week",
   "month",
   "quarter",
   "year",
]);

/** A stored column that holds a joined source's field, as the record reads it. */
interface InlinedJoinColumnText {
   /** The field on the joined source. */
   name: string;
   /** The stored column (`<join>__<field>`). */
   column: string;
   /** The timeframe the joined field truncates to, restored on the record. */
   timeframe?: string;
   /**
    * The native SQL type a big-integer column is read through (`HUGEINT`), so
    * the record's field is a `bigint` number that keeps every digit (known
    * limitation 12).
    */
   nativeCast?: string;
}

/** One inlined join, as {@link inlinedJoinStatements} writes its record. */
export interface InlinedJoinText {
   join: string;
   columns: readonly InlinedJoinColumnText[];
   /** The join's modifier on the author's source, when it is not public. */
   access?: "private" | "internal";
}

/** A statement's access label, as Malloy text: empty for a public one. */
function accessLabel(access: "private" | "internal" | undefined): string {
   switch (access) {
      case "private":
         return "private ";
      case "internal":
         return "internal ";
      default:
         return "";
   }
}

/**
 * The modifier a stored column's internal name is given: `private` when the
 * field it stores is hidden, so no caller reaches it by any path (a caller's
 * own `extend` reads an `internal` field), and `internal` for a public one,
 * whose value a caller may read as the field anyway.
 *
 * A private name is readable only in the block that defines it, or in the
 * block just after an `include` that hides it, so what reads it (the
 * redefined field, the record) is always written in the re-root's first
 * block; a public field's definition may be rewritten in place in a later
 * block, which is why its stored column stays `internal`.
 */
export function renameAccess(c: {
   access?: "private" | "internal";
}): "private" | "internal" {
   return c.access ? "private" : "internal";
}

/** A stored column the re-root renames to an internal name. */
export interface GeneratedRename {
   /** The internal name it is read under. */
   internal: string;
   /** The stored column's name. */
   column: string;
   access: "private" | "internal";
}

/**
 * `renames` as labelled `rename:` statements, to open a block. Malloy refuses
 * a rename in the block just after an `include` (it then cannot find the old
 * name to set its access on), so a re-root that writes an include renames
 * ahead of it instead ({@link renameBlock}).
 */
export function labelledRenames(renames: readonly GeneratedRename[]): string {
   let out = "";
   for (const access of ["internal", "private"] as const) {
      const lines = renames
         .filter((r) => r.access === access)
         .map((r) => `    ${identifier(r.internal)} is ${identifier(r.column)}`)
         .join("\n");
      if (lines) out += `\n  ${accessLabel(access)}rename:\n${lines}`;
   }
   return out;
}

/**
 * `renames` as one unlabelled `extend { rename: … }` block, written ahead of
 * an `include` that then hides each internal name under its access; empty
 * when there is none.
 */
export function renameBlock(renames: readonly GeneratedRename[]): string {
   if (renames.length === 0) return "";
   const lines = renames
      .map((r) => `    ${identifier(r.internal)} is ${identifier(r.column)}`)
      .join("\n");
   return ` extend {\n  rename:\n${lines}\n}`;
}

/**
 * The internal name each inlined join's stored `<join>__<field>` column is
 * renamed to (none of `reserved` and no stored column's name), under
 * {@link renameAccess} of its join.
 */
export function inlinedJoinRenames(
   inlined: readonly InlinedJoinText[],
   reserved: readonly string[],
): GeneratedRename[] {
   const taken = new Set([
      ...reserved,
      ...inlined.flatMap((j) => j.columns.map((c) => c.column)),
   ]);
   const out: GeneratedRename[] = [];
   for (const j of inlined) {
      for (const c of j.columns) {
         let internal = `${c.column}__stored`;
         for (let n = 2; taken.has(internal); n++) {
            internal = `${c.column}__stored_${n}`;
         }
         taken.add(internal);
         out.push({ internal, column: c.column, access: renameAccess(j) });
      }
   }
   return out;
}

/**
 * The statements that replace inlined joins: per join a dimension of the
 * join's name, a record of the joined source's fields over their renamed
 * columns ({@link inlinedJoinRenames}), a timeframe field truncated again as
 * the source's own are, and with `withRenames` the renames themselves first
 * ({@link labelledRenames}). A hidden join's record carries its modifier
 * ({@link InlinedJoinText.access}).
 */
export function inlinedJoinStatements(
   inlined: readonly InlinedJoinText[],
   renames: readonly GeneratedRename[],
   withRenames: boolean,
): string {
   const internalOf = new Map(renames.map((r) => [r.column, r.internal]));
   let out = withRenames ? labelledRenames(renames) : "";
   for (const access of [undefined, "internal", "private"] as const) {
      const records = recordsOf(
         inlined.filter((j) => j.access === access),
         internalOf,
      );
      if (records) {
         out += `\n  ${accessLabel(access)}dimension:\n${records}`;
      }
   }
   return out;
}

/** The record dimensions of `inlined`, each over its renamed columns. */
function recordsOf(
   inlined: readonly InlinedJoinText[],
   internalOf: ReadonlyMap<string, string>,
): string {
   // malloy-text: names through identifier, a timeframe from TIMEFRAMES and a cast through doubleQuotedString
   return inlined
      .map((j) => {
         const fields = j.columns
            .map((c) => {
               const internal = identifier(internalOf.get(c.column) as string);
               const read = c.nativeCast
                  ? `${internal}::${doubleQuotedString(c.nativeCast)}`
                  : c.timeframe && TIMEFRAMES.has(c.timeframe)
                    ? `${internal}.${c.timeframe}`
                    : internal;
               return `      ${identifier(c.name)} is ${read}`;
            })
            .join(",\n");
         return `    ${identifier(j.join)} is {\n${fields}\n    }`;
      })
      .join("\n");
}

/**
 * A run of `extend { … }` blocks with the join_one named `join` taken out:
 * its definition, with the annotation lines written on it, or its whole
 * statement when it is the statement's only definition. The text unchanged
 * when it declares no such join (one inherited from a base the re-root
 * replaces is not in it); undefined when the text does not lex, or the name
 * is defined there by another kind of join.
 */
export function dropJoinDefinition(
   text: string,
   join: string,
): string | undefined {
   const tokens = lexAll(text);
   if (!tokens) return undefined;
   const nameOf = (t: { text: string }) =>
      t.text.startsWith("`") ? t.text.slice(1, -1) : t.text;
   for (const t of tokens) {
      if (!isName(t) || nameOf(t) !== join) continue;
      const cut = definitionCutAt(text, t.start);
      if (!cut || cut.left.name !== join) continue;
      if (!cut.left.keyword.startsWith("join_")) continue;
      if (cut.left.keyword !== "join_one") return undefined;
      // An access label written on the statement goes with it when this is
      // its only definition, and stays on the others when it is not; the
      // record carries the join's own modifier, read off the compiled source
      // ({@link InlinedJoin.access}), not this text.
      return text.slice(0, cut.start) + text.slice(cut.end);
   }
   return text;
}

/**
 * `statements` placed just inside the first `extend {` of a run of extend
 * blocks, or as a new block when there is none. Undefined when the text does
 * not lex.
 */
export function intoFirstExtend(
   extend: string,
   // malloy-text: timeframeStatements' text, every name through identifier
   statements: string,
): string | undefined {
   if (extend === "") return `extend {${statements}\n}`;
   const tokens = lexAll(extend);
   if (!tokens) return undefined;
   // A run that carries the author's `include { … }` (a v1 re-root under the
   // cutover keeps it, `keepsIncludes`): the statements go in the first
   // extend block after the last one (or a block of their own when none
   // follows), never inside an include's braces, and never ahead of an
   // include whose `public: *` would make the internal names they rename to
   // selectable again.
   const include = tokens.findLastIndex(
      (t) => t.depth === 0 && t.kind === "word" && t.text === "include",
   );
   let from = 0;
   if (include >= 0) {
      const close = tokens.findIndex(
         (t, i) => i > include && t.depth === 0 && t.kind === "close",
      );
      if (close < 0) return undefined;
      from = close + 1;
      if (
         !tokens.slice(from).some((t) => t.kind === "open" && t.text === "{")
      ) {
         const head = extend.slice(0, tokens[close].end);
         return head + " " + intoFirstExtend("", statements);
      }
   }
   const open = tokens
      .slice(from)
      .find((t) => t.kind === "open" && t.text === "{");
   if (!open) return undefined;
   const rest = extend.slice(open.end);
   // On its own line, so the author's first statement keeps its own.
   const sep = /^[ \t]*\n/.test(rest) ? "" : "\n";
   return extend.slice(0, open.end) + statements + sep + rest;
}
