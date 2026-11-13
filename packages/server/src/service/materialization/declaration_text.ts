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

import { identifier } from "../malloy_text";
import {
   isName,
   lexAll,
   offsetOf,
   statementHead,
   statementStarts,
   type Token,
} from "./malloy_lexer";

/** A zero-based Malloy range, as a compiled source's `location.range` carries. */
interface DeclarationRange {
   start: { line: number; character: number };
   end: { line: number; character: number };
}

/**
 * Where the name ends (`is` begins) and where the trailing run of top-level
 * `extend { … }` blocks begins, if any. Undefined when no top-level `is`
 * follows the name.
 */
export function declarationSpans(
   declaration: string,
   options: {
      /**
       * Walk back over trailing `include { … }` blocks too, so the run is
       * everything after the base (a v1 query-shaped source's re-root under
       * the cutover keeps its author's `include`, review F3).
       */
      throughIncludes?: boolean;
   } = {},
):
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
         (kw.text !== "extend" &&
            !(options.throughIncludes && kw.text === "include")) ||
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
         si + 1 < starts.length
            ? statementHead(level, starts[si + 1]).start
            : Infinity;
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
         // own annotation lines go too), its access label included: a label
         // left behind would attach to the next statement, or end a block.
         const first = statementHead(level, starts[si]);
         const before = tokens.filter((t) => t.end <= first.start);
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
   range: DeclarationRange,
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

/** One definition of a `rename:` statement: `<name> is <from>`. */
export interface RenameDefinition {
   /** The name the field has after the statement. */
   name: string;
   /** The name it had before. */
   from: string;
}

/**
 * Every definition of a plain `rename:` statement in a run of `extend { … }`
 * blocks, in order. Undefined when the text does not lex, or a `rename:`
 * carries an access modifier or a definition that is not `<name> is <name>`:
 * a reader that cannot place every rename must not guess.
 */
export function renamesIn(text: string): RenameDefinition[] | undefined {
   const tokens = lexAll(text);
   if (!tokens) return undefined;
   const level = tokens.filter((t) => t.depth === 1 && t.kind !== "close");
   const closes = tokens.filter((t) => t.kind === "close" && t.depth === 0);
   const starts = statementStarts(level);
   const unquote = (t: Token) =>
      t.text.startsWith("`") ? t.text.slice(1, -1) : t.text;
   const out: RenameDefinition[] = [];
   for (let si = 0; si < starts.length; si++) {
      const kw = level[starts[si]];
      if (kw.text !== "rename") continue;
      const before = level[starts[si] - 1];
      if (
         before &&
         before.kind === "word" &&
         ["public", "internal", "private"].includes(before.text)
      ) {
         return undefined;
      }
      const colon = level[starts[si] + 1];
      const nextStart =
         si + 1 < starts.length
            ? statementHead(level, starts[si + 1]).start
            : Infinity;
      const blockEnd =
         closes.find((t) => t.start > kw.start)?.start ?? Infinity;
      const stmtEnd = Math.min(nextStart, blockEnd);
      const body = level.filter(
         (t) => t.start > colon.start && t.start < stmtEnd,
      );
      for (let k = 0; k < body.length; ) {
         const [name, is, from] = [body[k], body[k + 1], body[k + 2]];
         if (!isName(name) || is?.text !== "is" || !isName(from)) {
            return undefined;
         }
         out.push({ name: unquote(name), from: unquote(from) });
         k += 3;
         if (body[k]?.text === ",") k++;
      }
   }
   return out;
}

/**
 * The stored names {@link inverseRenameBlocks} gives an old name back, so no
 * statement placed after those blocks can read them under the stored name.
 */
export function inverselyRenamedColumns(
   text: string,
   stored: ReadonlySet<string>,
): Set<string> {
   const out = new Set<string>();
   const renames = renamesIn(text);
   if (!renames) return out;
   const live = new Set(stored);
   for (const r of [...renames].reverse()) {
      if (!live.has(r.name) || live.has(r.from)) continue;
      if (stored.has(r.name)) out.add(r.name);
      live.delete(r.name);
      live.add(r.from);
   }
   return out;
}

/**
 * The `extend { rename: … }` blocks that undo the renames in a run of `extend`
 * blocks, for a re-root onto a table that stores each renamed field under its
 * new name: written between the new base and the kept blocks, they give each
 * stored column back the name the kept blocks read it by before they rename
 * it again, so every definition in them reads the field as written. Undone
 * last rename first, and only for a field the table stores (`stored`); a
 * rename of any other field is the kept text's own. Each in a block of its
 * own, so they apply in order. Undefined when the renames cannot be read
 * ({@link renamesIn}).
 */
export function inverseRenameBlocks(
   text: string,
   stored: ReadonlySet<string>,
): string | undefined {
   const renames = renamesIn(text);
   if (!renames) return undefined;
   const live = new Set(stored);
   const blocks: string[] = [];
   for (const r of [...renames].reverse()) {
      if (!live.has(r.name) || live.has(r.from)) continue;
      blocks.push(
         ` extend { rename: ${identifier(r.from)} is ${identifier(r.name)} }`,
      );
      live.delete(r.name);
      live.add(r.from);
   }
   return blocks.join("");
}

/**
 * Where a model file's statements begin: past its leading model annotations
 * (`##` lines), blank lines and comments, so text inserted there separates no
 * statement from its own annotations (`#@ persist`, `#(authorize)`).
 */
export function afterModelHeader(text: string): number {
   const lines = text.split("\n");
   let offset = 0;
   for (const line of lines) {
      const t = line.trim();
      if (
         t === "" ||
         t.startsWith("##") ||
         t.startsWith("--") ||
         t.startsWith("//")
      ) {
         offset += line.length + 1;
         continue;
      }
      break;
   }
   return Math.min(offset, text.length);
}

/** The `##!` lines of an author file, in order, trimmed. */
export function documentFlagLines(text: string): string[] {
   // Block comments first, so a `##!` quoted inside one is not a flag.
   return text
      .replace(/\/\*[\s\S]*?\*\//g, "")
      .split("\n")
      .map((line) => line.trim())
      .filter((line) => line.startsWith("##!"));
}

/**
 * In a run of `extend { … }` blocks, delete each `except:` item of a
 * top-level block's own statement that `removable` accepts, and an `except:`
 * statement left with no item. For a
 * stored table that never held an excluded column (a v1 `storage=` table
 * stores public columns only), `except: region` names nothing the re-rooted
 * source has, which Malloy refuses; with nothing to exclude, deleting it
 * leaves the surface as it was. An item whose name appears anywhere else in
 * the text (a field the text itself defines) is kept: it may hide something.
 * Returns the names it deleted, or undefined when the text does not lex.
 */
export function dropExceptItems(
   text: string,
   removable: (name: string) => boolean,
): { text: string; dropped: string[] } | undefined {
   const tokens = lexAll(text);
   if (!tokens) return undefined;
   const nameOf = (t: { kind: string; text: string }): string | undefined =>
      t.kind === "word"
         ? t.text
         : t.text.startsWith("`") && t.text.endsWith("`") && t.text.length > 1
           ? t.text.slice(1, -1)
           : undefined;
   const exceptItems = new Set<number>();
   const statements: { keyword: number; items: number[]; end: number }[] = [];
   for (let k = 0; k + 1 < tokens.length; k++) {
      const t = tokens[k];
      const colon = tokens[k + 1];
      // Only a statement of a top-level block (`extend { … }`, `include
      // { … }`), which excludes from the re-rooted source itself. One nested
      // deeper (a join's own `extend { except: … }`) hides a field of the
      // joined source, which the table never stores either: deleting it
      // would expose that field through the join.
      if (t.kind !== "word" || t.text !== "except" || t.depth !== 1) continue;
      if (colon.text !== ":" || colon.start !== t.end) continue;
      const items: number[] = [];
      let i = k + 2;
      for (;;) {
         const item = tokens[i];
         if (!item || item.depth !== t.depth || nameOf(item) === undefined) {
            break;
         }
         items.push(i);
         exceptItems.add(i);
         if (tokens[i + 1]?.text !== "," || tokens[i + 1].depth !== t.depth) {
            break;
         }
         i += 2;
      }
      if (items.length === 0) continue;
      statements.push({ keyword: k, items, end: items[items.length - 1] });
   }
   const elsewhere = new Set<string>();
   tokens.forEach((t, k) => {
      const n = nameOf(t);
      if (n !== undefined && !exceptItems.has(k)) elsewhere.add(n);
   });
   const edits: TextEdit[] = [];
   const dropped: string[] = [];
   for (const st of statements) {
      const gone = st.items.filter((k) => {
         const n = nameOf(tokens[k])!;
         return !elsewhere.has(n) && removable(n);
      });
      if (gone.length === 0) continue;
      dropped.push(...gone.map((k) => nameOf(tokens[k])!));
      if (gone.length === st.items.length) {
         edits.push({
            start: tokens[st.keyword].start,
            end: tokens[st.end].end,
            insert: "",
         });
         continue;
      }
      const keep = st.items.filter((k) => !gone.includes(k));
      edits.push({
         start: tokens[st.items[0]].start,
         end: tokens[st.end].end,
         insert: keep.map((k) => tokens[k].text).join(", "),
      });
   }
   if (edits.length === 0) return { text, dropped };
   const out = applyEdits(text, edits);
   return out === undefined ? undefined : { text: out, dropped };
}
