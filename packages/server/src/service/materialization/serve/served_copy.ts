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

import { identifier, nowComparison, stringLiteral } from "../../malloy_text";
import type { MaterializeReadTimeTerm } from "../plan/build_file_generator";
import { duckdbTypeToMalloy } from "./materialization_serve_transform";
import { declarationSpans, topLevelTokens } from "../declaration_text";
import { isName, lexAll, offsetOf, statementStarts } from "../malloy_lexer";

export { declarationSpans };

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
   columns: readonly ServedColumn[];
   /**
    * For a `#(materialize)` re-root: the computed dimensions its table stores
    * under their own names. Its table is the whole source (`src -> { select:
    * … }`, its `where:` applied), so any declaration shape re-roots, and each
    * of these definitions is dropped from the source's own `extend` blocks so
    * the stored column is the field. Absent for a `#@ persist` re-root.
    */
   storedDimensions?: readonly string[];
   /**
    * Every field name the model declares, on any source. A stored timeframe
    * column is renamed internally (see {@link ServedColumn.timeframe}), and
    * the internal name must not collide with any of them: an extension sees an
    * `internal` field.
    */
   reservedNames?: readonly string[];
   /**
    * For a `#(materialize)` re-root: its plan's read-time terms, applied again
    * over the stored table at every read ({@link readTimeTermText}). A term
    * inherited from a base is not in the source's own text, so the re-root
    * writes every term right on the stored table (one the source's own
    * `extend` repeats is applied twice, which is harmless).
    */
   readTimeTerms?: readonly MaterializeReadTimeTerm[];
}

/** One stored column of a re-rooted source. */
export interface ServedColumn {
   name: string;
   /** The stored (warehouse) type. */
   type: string;
   /** The annotation lines its field carries, written on it again. */
   notes?: readonly string[];
   /**
    * The timeframe the author's field truncates to (`order_date.month`). A
    * `type:` field cannot carry one, so the stored column is renamed
    * internally and the field redefined as that column truncated again, which
    * is the identity on a truncated value and restores the field's timeframe:
    *
    *    internal rename: order_month__stored is order_month
    *    dimension: order_month is order_month__stored.month
    */
   timeframe?: string;
}

/** The timeframes a stored truncation can carry, the only ones emitted. */
const TIMEFRAMES: ReadonlySet<string> = new Set([
   "second",
   "minute",
   "hour",
   "day",
   "week",
   "month",
   "quarter",
   "year",
]);

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
      | "reservedNames"
      | "readTimeTerms"
   > & { columns?: Reroot["columns"] },
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
   // Each stored timeframe column, renamed internally so its field can be
   // redefined with the timeframe a `type:` cannot declare.
   const timeframed = timeframedColumns(r.columns ?? [], r.reservedNames ?? []);
   const stored = new Set(r.storedDimensions ?? []);
   const rewrite = new Map<string, string>();
   for (const c of timeframed) {
      // Only a `#(materialize)` computed dimension has a definition in the
      // source's own text to rewrite; a `#@ persist` query's output has none.
      if (stored.has(c.name)) rewrite.set(c.name, timeframeExpression(c));
   }
   let rewritten = new Set<string>();
   if (stored.size > 0) {
      // A dimension the source inherits is not in its own text: the base the
      // re-root replaces defined it, and the stored column stands in alone.
      const kept = rewriteDimensions(
         extend,
         new Set([...stored].filter((n) => !rewrite.has(n))),
         rewrite,
      );
      if (!kept) return undefined;
      extend = kept.text;
      rewritten = kept.rewritten;
   }
   if (timeframed.length > 0) {
      const opened = intoFirstExtend(
         extend,
         timeframeStatements(
            timeframed,
            timeframed.filter((c) => !rewritten.has(c.name)),
         ),
      );
      if (opened === undefined) return undefined;
      extend = opened;
   }
   const terms = (r.readTimeTerms ?? [])
      .map(readTimeTermText)
      .filter((t) => t !== undefined);
   const reread = terms.length ? ` extend { where: ${terms.join(", ")} }` : "";
   return `${name} is ${virtual}${reread}${extend ? ` ${extend}` : ""}`;
}

/**
 * The `where:` text the re-root writes over the stored table for one
 * read-time term, or undefined when the source's own text already carries it.
 * Exhaustive by `kind`, so a new kind of read-time term must say how it is
 * re-applied.
 */
function readTimeTermText(term: MaterializeReadTimeTerm): string | undefined {
   switch (term.kind) {
      case "current_time":
         // Written from the classified parts, never the author's text: a term
         // inherited from a base is re-applied here, outside its own file.
         return nowComparison(
            term.window.field,
            term.window.op,
            term.window.offset,
         );
      case "given":
         // Kept, not written: a given-reading term (P2) is in the
         // declaration's own trailing `extend`, which the re-root keeps as the
         // author wrote it (the build generator takes a term out only there).
         return;
   }
}

/** A timeframe column with the internal name its stored value is renamed to. */
interface Timeframed extends ServedColumn {
   timeframe: string;
   internalName: string;
}

/**
 * The columns that carry a timeframe, each with an internal name that is
 * none of `reserved` and no stored column's name.
 */
function timeframedColumns(
   columns: readonly ServedColumn[],
   reserved: readonly string[],
): Timeframed[] {
   const taken = new Set([...reserved, ...columns.map((c) => c.name)]);
   const out: Timeframed[] = [];
   for (const c of columns) {
      if (!c.timeframe || !TIMEFRAMES.has(c.timeframe)) continue;
      let internalName = `${c.name}__stored`;
      for (let n = 2; taken.has(internalName); n++) {
         internalName = `${c.name}__stored_${n}`;
      }
      taken.add(internalName);
      out.push({ ...c, timeframe: c.timeframe, internalName });
   }
   return out;
}

/** `<internal>.<timeframe>`: the stored value truncated again. */
function timeframeExpression(c: Timeframed): string {
   return `${identifier(c.internalName)}.${c.timeframe}`;
}

/**
 * The statements that open the first extend block: the internal renames, and
 * a `dimension:` for each timeframe field whose definition is not in the
 * source's own text (an inherited one, or a `#@ persist` query's output
 * column), with the field's annotations.
 */
function timeframeStatements(
   all: readonly Timeframed[],
   emitted: readonly Timeframed[],
): string {
   const renames = all
      .map((c) => `    ${identifier(c.internalName)} is ${identifier(c.name)}`)
      .join("\n");
   let out = `\n  internal rename:\n${renames}`;
   if (emitted.length > 0) {
      // malloy-text: the author's own notes, then names through identifier and a timeframe from TIMEFRAMES
      const dims = emitted
         .map(
            (c) =>
               (c.notes ?? []).map((n) => `    ${n.trimEnd()}\n`).join("") +
               `    ${identifier(c.name)} is ${timeframeExpression(c)}`,
         )
         .join("\n");
      out += `\n  dimension:\n${dims}`;
   }
   return out;
}

/**
 * `statements` placed just inside the first `extend {` of a run of extend
 * blocks, or as a new block when there is none. Undefined when the text does
 * not lex.
 */
function intoFirstExtend(
   extend: string,
   // malloy-text: timeframeStatements' text, every name through identifier
   statements: string,
): string | undefined {
   if (extend === "") return `extend {${statements}\n}`;
   const open = lexAll(extend)?.find(
      (t) => t.kind === "open" && t.text === "{",
   );
   if (!open) return undefined;
   const rest = extend.slice(open.end);
   // On its own line, so the author's first statement keeps its own.
   const sep = /^[ \t]*\n/.test(rest) ? "" : "\n";
   return extend.slice(0, open.end) + statements + sep + rest;
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
   const out = rewriteDimensions(text, names, new Map());
   return out && { text: out.text, dropped: out.dropped };
}

/**
 * {@link dropDimensions}, and also replace the expression of each definition
 * named in `rewrite` (`<name> is <expression>`) with the given text, keeping
 * its name, its annotations and its place. Returns the names it dropped and
 * the names it rewrote.
 */
export function rewriteDimensions(
   text: string,
   names: ReadonlySet<string>,
   rewrite: ReadonlyMap<string, string>,
): { text: string; dropped: Set<string>; rewritten: Set<string> } | undefined {
   const tokens = lexAll(text);
   if (!tokens) return undefined;
   const dropped = new Set<string>();
   const rewritten = new Set<string>();
   const cuts: { start: number; end: number; insert?: string }[] = [];
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
            exprFrom,
            exprEnd: inExpr[inExpr.length - 1]?.end,
            afterComma: comma?.end,
         };
      });
      if (defs.some((d) => d.exprEnd === undefined)) return undefined;
      for (const def of defs) {
         const expression = rewrite.get(def.name);
         if (expression === undefined) continue;
         rewritten.add(def.name);
         cuts.push({
            start: def.exprFrom,
            end: def.exprEnd as number,
            insert: ` ${expression}`,
         });
      }
      const drop = defs.map((d) => names.has(d.name) && !rewrite.has(d.name));
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
      out = out.slice(0, c.start) + (c.insert ?? "") + out.slice(c.end);
   }
   return { text: out, dropped, rewritten };
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
   // malloy-text: the author's own notes, then names through identifier and a Malloy type name
   const fields = r.columns
      .map(
         (c) =>
            // A timeframe column's notes go on its redefined field instead.
            (c.timeframe && TIMEFRAMES.has(c.timeframe) ? [] : (c.notes ?? []))
               .map((note) => `   ${note.trimEnd()}\n`)
               .join("") +
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
