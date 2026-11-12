// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The re-root rewriter: the served copy of an author's model file
 * (materialization v2, phase 3).
 *
 * The served copy IS the author's file, served in memory in place of the file
 * on disk, with each persisted source that has a stored table re-rooted on it:
 *
 *    source: daily is ev -> { group_by: r; aggregate: total is amt.sum() }
 *       extend { dimension: big is total > 1 }
 *
 * becomes
 *
 *    source: daily is <alias>.virtual('<handle>')::<type>
 *       extend { dimension: big is total > 1 }
 *
 * under its own name, so everything the author built on it (extensions in the
 * same file or in an importing file, joins to it, views, named queries) is the
 * author's own text, unchanged, and now reads the stored table. Only the base
 * of the declaration changes: the trailing `extend { … }` blocks are kept byte
 * for byte, annotations before `source:` are untouched (gates and docs stay),
 * and nothing else in the file moves.
 *
 * Text in, text out: no compiler internals are edited. The declaration's
 * location comes from the compiled model (a top-level source's range starts
 * after `source: ` and covers `<name> is <definition>`), and every emitted name
 * and string goes through `malloy_text.ts`.
 *
 * `<alias>` is a connection name only the server knows (see
 * served_copy_config.ts), so an author's file can never name a storage
 * destination: the served copy reaches the destination only through
 * `virtual()` handles the server mapped.
 */

import { identifier, stringLiteral } from "../../malloy_text";
import { duckdbTypeToMalloy } from "./materialization_serve_transform";

/** A zero-based Malloy range, as a compiled source's `location.range` carries. */
export interface ServedCopyRange {
   start: { line: number; character: number };
   end: { line: number; character: number };
}

/** One persisted source to re-root on its stored table. */
export interface Reroot {
   /** The file that declares the source, as its compiled `location.url`. */
   url: string;
   /** The declaration's range in that file (`<name> is <definition>`). */
   range: ServedCopyRange;
   /** The source's name in its declaring file. */
   sourceName: string;
   /** The connection name the served copy reaches the destination by. */
   connectionAlias: string;
   /** The virtualMap handle for the stored table. */
   virtualHandle: string;
   /** The `type:` the virtual source is declared as; unique in the file. */
   typeName: string;
   /**
    * The stored table's public columns, under the author's spelling, each with
    * the annotation lines its field carries (`#(doc) "…"`, `#(classification)
    * …`), written on the field in the `type:` so they read back on it.
    */
   columns: readonly {
      name: string;
      type: string;
      notes?: readonly string[];
   }[];
   /**
    * For a `#(materialize)` re-root: the computed dimensions its table stores
    * under their own names. Its table is the whole source (`src -> { select:
    * … }`, its `where:` applied), so any declaration shape re-roots, and each
    * of these definitions is dropped from the source's own `extend` blocks so
    * the stored column is the field. Absent for a `#@ persist` re-root.
    */
   storedDimensions?: readonly string[];
}

/** Why a declaration could not be re-rooted; the source then serves live. */
export interface RerootRefusal {
   sourceName: string;
   url: string;
   /**
    * `not_query_shaped`: the declaration's base is not a query (`x -> { … }`).
    * A source declared by refinement alone (`deals is all_deals extend {
    * where: … }`) stores its root's rows, and the serve side re-applies the
    * whole chain's filters, so re-rooting it would drop its base's `where:`.
    * It is left as written; it still inherits a re-rooted base by name.
    */
   reason: "range_not_found" | "definition_not_parsed" | "not_query_shaped";
}

/** The served copy of the files that declare re-rooted sources. */
export interface ServedCopy {
   /** File URL → its served text. Files with nothing re-rooted are absent. */
   files: Map<string, string>;
   /** Re-roots that did not apply. */
   refused: RerootRefusal[];
}

/** The compiler flag every served copy needs for `virtual()` sources. */
export const SERVED_COPY_FLAG = "##! experimental { virtual_source }";

/**
 * Produce the served copy of every file that declares a source in `reroots`.
 * `fileText` reads a file by URL (the author's bytes, as the compile read
 * them). A file whose text is unavailable refuses all its re-roots.
 */
export function buildServedCopy(
   reroots: readonly Reroot[],
   fileText: (url: string) => string | undefined,
): ServedCopy {
   const byFile = new Map<string, Reroot[]>();
   for (const r of reroots) {
      const list = byFile.get(r.url) ?? [];
      list.push(r);
      byFile.set(r.url, list);
   }
   const files = new Map<string, string>();
   const refused: RerootRefusal[] = [];
   for (const [url, list] of byFile) {
      const text = fileText(url);
      if (text === undefined) {
         for (const r of list) {
            refused.push({
               sourceName: r.sourceName,
               url,
               reason: "range_not_found",
            });
         }
         continue;
      }
      const applied: Reroot[] = [];
      let out = text;
      // Splice from the last declaration back, so earlier offsets stay valid.
      const ordered = [...list].sort(
         (a, b) =>
            offsetOf(text, b.range.start) - offsetOf(text, a.range.start),
      );
      for (const r of ordered) {
         const start = offsetOf(out, r.range.start);
         const end = offsetOf(out, r.range.end);
         if (start < 0 || end < 0 || end < start) {
            refused.push({
               sourceName: r.sourceName,
               url,
               reason: "range_not_found",
            });
            continue;
         }
         const declaration = out.slice(start, end);
         const spans = declarationSpans(declaration);
         if (spans && !spans.queryShaped && !r.storedDimensions) {
            refused.push({
               sourceName: r.sourceName,
               url,
               reason: "not_query_shaped",
            });
            continue;
         }
         const replaced = rerootDeclaration(declaration, r);
         if (replaced === undefined) {
            refused.push({
               sourceName: r.sourceName,
               url,
               reason: "definition_not_parsed",
            });
            continue;
         }
         out = out.slice(0, start) + replaced + out.slice(end);
         applied.push(r);
      }
      if (applied.length === 0) continue;
      files.set(url, withTypes(out, applied));
   }
   return { files, refused };
}

/**
 * The re-rooted declaration text for one source: `<name> is <virtual>` plus the
 * author's trailing `extend { … }` blocks, verbatim. Undefined when the
 * declaration text does not parse as `<name> is <definition>`.
 */
export function rerootDeclaration(
   declaration: string,
   r: Pick<
      Reroot,
      | "sourceName"
      | "connectionAlias"
      | "virtualHandle"
      | "typeName"
      | "storedDimensions"
   >,
): string | undefined {
   const spans = declarationSpans(declaration);
   if (!spans) return undefined;
   const virtual =
      `${identifier(r.connectionAlias)}.virtual(${stringLiteral(r.virtualHandle)})` +
      `::${identifier(r.typeName)}`;
   const name = declaration.slice(0, spans.nameEnd).trimEnd();
   let extend =
      spans.extendStart === undefined
         ? ""
         : declaration.slice(spans.extendStart).trimEnd();
   if (r.storedDimensions && r.storedDimensions.length > 0) {
      // A dimension the source inherits is not in its own text: the base the
      // re-root replaces defined it, and the stored column stands in alone.
      const kept = dropDimensions(extend, new Set(r.storedDimensions));
      if (!kept) return undefined;
      extend = kept.text;
   }
   return `${name} is ${virtual}${extend ? ` ${extend}` : ""}`;
}

/** Indexes in `tokens` of a word followed by `:` (not `::`) at its own depth. */
function statementStarts(tokens: readonly Token[]): number[] {
   const starts: number[] = [];
   for (let k = 0; k + 1 < tokens.length; k++) {
      const t = tokens[k];
      if (
         t.kind === "word" &&
         tokens[k + 1].text === ":" &&
         tokens[k + 1].start === t.end &&
         tokens[k + 2]?.text !== ":"
      ) {
         starts.push(k);
      }
   }
   return starts;
}

/** Whether `t` can name a definition: a word or a quoted identifier. */
function isName(t: Token | undefined): t is Token {
   return !!t && (t.kind === "word" || t.text.startsWith("`"));
}

/**
 * In a run of `extend { … }` blocks, delete the `dimension:` definition of
 * each name in `names`, with the annotations written on it, so the stored
 * column of that name IS the field (its annotations travel on the `type:`).
 * A statement left with no definition is deleted whole, with its own
 * annotations; everything else stays byte for byte. Returns the names it
 * found, or undefined when the text does not lex.
 */
export function dropDimensions(
   text: string,
   names: ReadonlySet<string>,
): { text: string; dropped: Set<string> } | undefined {
   const tokens = lexAll(text);
   if (!tokens) return undefined;
   const dropped = new Set<string>();
   const cuts: { start: number; end: number }[] = [];
   // A block's statements are its depth-1 tokens; its closing brace is not one.
   const level = tokens.filter((t) => t.depth === 1 && t.kind !== "close");
   const closes = tokens.filter((t) => t.kind === "close" && t.depth === 0);
   const starts = statementStarts(level);
   for (let si = 0; si < starts.length; si++) {
      const kw = level[starts[si]];
      if (kw.text !== "dimension") continue;
      const colon = level[starts[si] + 1];
      const nextStart =
         si + 1 < starts.length ? level[starts[si + 1]].start : Infinity;
      const blockEnd =
         closes.find((t) => t.start > kw.start)?.start ?? Infinity;
      const stmtEnd = Math.min(nextStart, blockEnd);
      const body = level.filter(
         (t) => t.start > colon.start && t.start < stmtEnd,
      );
      // A definition is `<name> is …` opening the statement, after a comma,
      // or on a new line after the previous definition.
      const defIdx: number[] = [];
      for (let k = 0; k + 1 < body.length; k++) {
         if (!isName(body[k]) || body[k + 1].text !== "is") continue;
         if (
            k === 0 ||
            body[k - 1].text === "," ||
            text.slice(body[k - 1].end, body[k].start).includes("\n")
         ) {
            defIdx.push(k);
         }
      }
      const defs = defIdx.map((k, d) => {
         const nameTok = body[k];
         const exprFrom = body[k + 1].end;
         const exprTo =
            d + 1 < defIdx.length ? body[defIdx[d + 1]].start : stmtEnd;
         const inExpr = tokens.filter(
            (t) => t.start >= exprFrom && t.end <= exprTo,
         );
         const comma =
            inExpr[inExpr.length - 1]?.text === "," &&
            inExpr[inExpr.length - 1].depth === 1
               ? inExpr.pop()
               : undefined;
         return {
            name: nameTok.text.startsWith("`")
               ? nameTok.text.slice(1, -1)
               : nameTok.text,
            exprEnd: inExpr[inExpr.length - 1]?.end,
            afterComma: comma?.end,
         };
      });
      if (defs.some((d) => d.exprEnd === undefined)) return undefined;
      const drop = defs.map((d) => names.has(d.name));
      if (!drop.some(Boolean)) continue;
      for (const [d, def] of defs.entries()) if (drop[d]) dropped.add(def.name);
      if (drop.every(Boolean)) {
         // The whole statement, from the end of whatever precedes it (so its
         // own annotation lines go too).
         const before = tokens.filter((t) => t.end <= kw.start);
         const from = before[before.length - 1]?.end ?? 0;
         cuts.push({
            start: from,
            end: defs[defs.length - 1].exprEnd as number,
         });
         continue;
      }
      // Each dropped definition goes from the end of the definition kept
      // before it (taking the comma between them), or, with none kept yet,
      // from the colon through its own trailing comma.
      let anchor = colon.end;
      let keptBefore = false;
      for (const [d, def] of defs.entries()) {
         const exprEnd = def.exprEnd as number;
         if (!drop[d]) {
            keptBefore = true;
            anchor = exprEnd;
            continue;
         }
         const end = keptBefore ? exprEnd : (def.afterComma ?? exprEnd);
         cuts.push({ start: anchor, end });
         anchor = end;
      }
   }
   let out = text;
   for (const c of cuts.sort((a, b) => b.start - a.start)) {
      out = out.slice(0, c.start) + out.slice(c.end);
   }
   return { text: out, dropped };
}

/**
 * The base a declaration (at `range` in `text`) extends, when it is a bare
 * source name (`<name> is <base> extend { … }` or `<name> is <base>`), else
 * undefined.
 */
export function declaredBaseName(
   text: string,
   range: ServedCopyRange,
): string | undefined {
   const start = offsetOf(text, range.start);
   const end = offsetOf(text, range.end);
   if (start < 0 || end < start) return undefined;
   const declaration = text.slice(start, end);
   const tokens = topLevelTokens(declaration);
   const spans = declarationSpans(declaration);
   if (!tokens || !spans) return undefined;
   const baseEnd = spans.extendStart ?? declaration.length;
   const base = tokens.filter(
      (t) => t.start > spans.nameEnd + 1 && t.end <= baseEnd,
   );
   if (base.length !== 1) return undefined;
   const t = base[0];
   if (t.kind === "word") return t.text;
   return t.text.startsWith("`") ? t.text.slice(1, -1) : undefined;
}

/** The emitted `type:` declaration for one re-rooted source. */
export function typeDeclaration(
   r: Pick<Reroot, "typeName" | "columns">,
): string {
   const fields = r.columns
      .map(
         (c) =>
            (c.notes ?? []).map((note) => `   ${note.trimEnd()}\n`).join("") +
            `   ${identifier(c.name)}::${duckdbTypeToMalloy(c.type)}`,
      )
      .join(",\n");
   return `type: ${identifier(r.typeName)} is {\n${fields}\n}`;
}

/**
 * Insert the compiler flag and the `type:` declarations after the file's
 * leading model annotations (`##` lines), blank lines and comments, so no
 * statement's own annotations (`#@ persist`, `#(authorize)`) are separated
 * from it.
 */
function withTypes(text: string, applied: readonly Reroot[]): string {
   const lines = text.split("\n");
   let i = 0;
   while (i < lines.length) {
      const t = lines[i].trim();
      if (
         t === "" ||
         t.startsWith("##") ||
         t.startsWith("--") ||
         t.startsWith("//")
      ) {
         i++;
         continue;
      }
      break;
   }
   const types = applied.map(typeDeclaration).join("\n");
   const head = lines.slice(0, i).join("\n");
   const rest = lines.slice(i).join("\n");
   return `${head}${head ? "\n" : ""}${SERVED_COPY_FLAG}\n${types}\n\n${rest}`;
}

/** The character offset of a zero-based line/character position, or -1. */
function offsetOf(
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

/** One lexical token, at the bracket depth it sits at. */
interface Token {
   kind: "word" | "open" | "close" | "other";
   text: string;
   start: number;
   end: number;
   /** The brackets enclosing it; a bracket itself has the depth outside it. */
   depth: number;
}

/**
 * Where the name ends (`is` begins) and where the trailing run of top-level
 * `extend { … }` blocks begins, if any. Undefined when no top-level `is`
 * follows the name.
 */
export function declarationSpans(
   declaration: string,
): { nameEnd: number; extendStart?: number; queryShaped: boolean } | undefined {
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
   return {
      nameEnd,
      ...(extendStart !== undefined ? { extendStart } : {}),
      queryShaped,
   };
}

/**
 * The tokens at brace depth 0: words, quoted text, and each top-level `{ … }`
 * block as an "open" token at its `{` and a "close" token at its `}` (the
 * block's inside is skipped). Undefined when the text does not lex.
 */
function topLevelTokens(s: string): Token[] | undefined {
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

/**
 * Every token of `s` with its depth. Comments and `#` annotation lines are
 * skipped; a string or quoted identifier is one "other" token. Undefined when
 * brackets do not balance or a string or comment does not close.
 */
function lexAll(s: string): Token[] | undefined {
   const out: Token[] = [];
   let depth = 0;
   let i = 0;
   const n = s.length;
   const push = (kind: Token["kind"], start: number, end: number, d = depth) =>
      out.push({ kind, text: s.slice(start, end), start, end, depth: d });
   while (i < n) {
      const c = s[i];
      const next = s[i + 1];
      // Comments, and annotations (`#` to the end of the line).
      if (
         (c === "-" && next === "-") ||
         (c === "/" && next === "/") ||
         c === "#"
      ) {
         const nl = s.indexOf("\n", i);
         i = nl < 0 ? n : nl + 1;
         continue;
      }
      if (c === "/" && next === "*") {
         const close = s.indexOf("*/", i + 2);
         if (close < 0) return undefined;
         i = close + 2;
         continue;
      }
      // Strings and quoted identifiers.
      if (c === '"' && s.startsWith('"""', i)) {
         const close = s.indexOf('"""', i + 3);
         if (close < 0) return undefined;
         push("other", i, close + 3);
         i = close + 3;
         continue;
      }
      if (c === "'" || c === '"' || c === "`") {
         const end = quotedEnd(s, i, c);
         if (end < 0) return undefined;
         push("other", i, end);
         i = end;
         continue;
      }
      if (c === "{" || c === "(" || c === "[") {
         push("open", i, i + 1);
         depth++;
         i++;
         continue;
      }
      if (c === "}" || c === ")" || c === "]") {
         depth--;
         if (depth < 0) return undefined;
         push("close", i, i + 1);
         i++;
         continue;
      }
      if (/[A-Za-z_]/.test(c)) {
         let j = i + 1;
         while (j < n && /[A-Za-z0-9_]/.test(s[j])) j++;
         push("word", i, j);
         i = j;
         continue;
      }
      if (!/\s/.test(c)) push("other", i, i + 1);
      i++;
   }
   return depth === 0 ? out : undefined;
}

/** The index just past the closing `quote` of a string starting at `start`. */
function quotedEnd(s: string, start: number, quote: string): number {
   let i = start + 1;
   while (i < s.length) {
      const c = s[i];
      if (c === "\\") {
         i += 2;
         continue;
      }
      if (c === quote) return i + 1;
      i++;
   }
   return -1;
}
