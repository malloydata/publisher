// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { DocumentSymbol } from "@malloydata/malloy";
import { rangeText, type ParsedModelText } from "./modelTextParse";

/** The field kinds a source declares with `kind: name is …`. */
export type DeclaredKind = "view" | "measure" | "dimension";

/** One field's declaration as written in the model file. */
export interface TextDeclaration {
   kind: DeclaredKind;
   /**
    * The annotation lines directly above it, then the declaration from its name
    * (`name is …`) to its end — the shape of the entities artifact's `code`, so
    * the row's code display (viewDeclaration, stripDefinitionWrapper) reads both alike.
    */
   code: string;
}

/**
 * Every view, measure and dimension a source declares, read from the model
 * file with Malloy's parser: field name → its declaration.
 *
 * A host's entities can carry a field's code, but the Publisher's own reads
 * don't, so without them the rows would offer no code at all. The compiled
 * model carries the file's text, and this reads the declarations from it, as
 * `namedQueryCode` does for a named query.
 *
 * The parser gives each declaration's exact text but not its kind, so the kind
 * is the `dimension:` / `measure:` / `view:` keyword that owns the entry.
 * Fields the source inherits from the source it extends (`source: b is a
 * extend { … }`, `a` in the same file) are read from `a`'s declaration, the
 * source's own taking precedence. A name declared twice is left out rather than
 * guessed, and so is anything that is not a `name is …` definition under one of
 * those keywords (a `rename:`, a query source's `group_by: carrier`, a field
 * from another file): the row then says it has no code.
 */
export function fieldDeclarations(
   parsed: ParsedModelText | undefined,
   sourceName: string | undefined,
): ReadonlyMap<string, TextDeclaration> {
   const out = new Map<string, TextDeclaration>();
   if (!parsed || !sourceName) return out;
   const sources = new Map<string, DocumentSymbol>();
   for (const symbol of parsed.symbols)
      if (symbol.type === "explore") sources.set(unquote(symbol.name), symbol);
   const seen = new Set<string>();
   for (let name: string | undefined = sourceName; name && !seen.has(name); ) {
      seen.add(name);
      const source = sources.get(name);
      if (!source) break;
      for (const [field, decl] of declarationsIn(parsed.lines, source)) {
         if (!out.has(field)) out.set(field, decl);
      }
      name = baseOf(rangeText(parsed.lines, source.range));
   }
   return out;
}

const DECLARED_KIND = /^\s*(view|measure|dimension)\s*:\s*$/;
/** A definition, `name is …`, as against a reference (`carrier` in a query source's `group_by:`). */
const DEFINITION = /^`?([A-Za-z_]\w*)`?\s+is\b/;

function declarationsIn(
   lines: string[],
   source: DocumentSymbol,
): Map<string, TextDeclaration> {
   const found = new Map<string, TextDeclaration | null>();
   // The keyword that owns the entry: the last one between the previous entry
   // and this one, or the previous entry's when there is none between them
   // (`measure: a is count(), b is sum(x)`, or entries on lines of their own).
   let keyword: string | undefined;
   let from = source.range.start;
   for (const child of source.children) {
      const gap = rangeText(lines, { start: from, end: child.range.start });
      keyword = lastKeyword(gap) ?? keyword;
      from = child.range.end;
      const kind = declaredKind(child, keyword);
      if (!kind) continue;
      const decl = declarationOf(lines, child, kind);
      if (!decl) continue;
      const name = unquote(child.name);
      // Declared twice: neither is shown, rather than the wrong one.
      found.set(name, found.has(name) ? null : decl);
   }
   const out = new Map<string, TextDeclaration>();
   for (const [name, decl] of found) if (decl) out.set(name, decl);
   return out;
}

function declaredKind(
   symbol: DocumentSymbol,
   keyword: string | undefined,
): DeclaredKind | undefined {
   if (symbol.type === "query") return keyword === "view" ? "view" : undefined;
   if (
      symbol.type === "field" &&
      (keyword === "dimension" || keyword === "measure")
   )
      return keyword;
   return undefined;
}

/**
 * The entry's annotations then its text from its name. The parser's range
 * holds an annotation written on the entry itself (`# percent` above `a is
 * count()` inside a `measure:` block); one above the statement (`# currency`
 * above `dimension: carrier is …`) is outside it, so it is read from the lines
 * above.
 */
function declarationOf(
   lines: string[],
   symbol: DocumentSymbol,
   kind: DeclaredKind,
): TextDeclaration | undefined {
   const text = rangeText(lines, symbol.range).split("\n");
   const own: string[] = [];
   // The annotations it opens with; a comment between them is not part of the code shown.
   while (text.length > 1 && /^\s*(#|\/\/|--|$)/.test(text[0])) {
      const line = text.shift()!.trim();
      if (line.startsWith("#")) own.push(line);
   }
   text[0] = text[0].trimStart();
   const body = text.join("\n").trimEnd();
   const definition = DEFINITION.exec(body);
   if (!definition || definition[1] !== unquote(symbol.name)) return undefined;
   return {
      kind,
      code: [...annotationsAbove(lines, symbol.range.start), ...own, body].join(
         "\n",
      ),
   };
}

/**
 * The `#` lines directly above an entry that starts its line (after nothing,
 * or after its `view:` / `measure:` / `dimension:`), and — when it is the first
 * entry under a bare keyword line — those above that line too, since they apply
 * to every entry the statement declares. An entry later on a line (after a
 * comma, or a one-line source's `extend {`) has none.
 */
function annotationsAbove(
   lines: string[],
   start: { line: number; character: number },
): string[] {
   const before = (lines[start.line] ?? "").slice(0, start.character);
   const startsLine = before.trim() === "";
   if (!startsLine && !DECLARED_KIND.test(before)) return [];
   const collect = (from: number) => {
      const out: string[] = [];
      let i = from;
      while (i >= 0 && lines[i].trim().startsWith("#"))
         out.unshift(lines[i--].trim());
      return { out, next: i };
   };
   const own = collect(start.line - 1);
   if (startsLine && own.next >= 0 && DECLARED_KIND.test(lines[own.next])) {
      return [...collect(own.next - 1).out, ...own.out];
   }
   return own.out;
}

/** A statement keyword (`measure:`, `where:`), not a `::` cast. */
const KEYWORD = /(?<![\w:])([A-Za-z_]\w*)\s*:(?!:)/g;

/** The last statement keyword in some text, its strings, comments and annotations skipped. */
function lastKeyword(text: string): string | undefined {
   let last: string | undefined;
   for (const match of codeOnly(text).matchAll(KEYWORD)) last = match[1];
   return last;
}

/** The text with its strings, comments and annotations blanked. */
function codeOnly(text: string): string {
   let out = "";
   for (let i = 0; i < text.length; i++) {
      const c = text[i];
      if (text.startsWith('"""', i)) {
         const close = text.indexOf('"""', i + 3);
         const end = close === -1 ? text.length : close + 3;
         out += " ".repeat(end - i);
         i = end - 1;
      } else if (c === '"' || c === "'" || c === "`") {
         const close = closingQuote(text, i);
         out += " ".repeat(close - i + 1);
         i = close;
      } else if (
         c === "#" ||
         (c === "/" && text[i + 1] === "/") ||
         (c === "-" && text[i + 1] === "-")
      ) {
         const eol = text.indexOf("\n", i);
         const end = eol === -1 ? text.length : eol;
         out += " ".repeat(end - i);
         i = end - 1;
      } else {
         out += c;
      }
   }
   return out;
}

/** The index of the quote that closes the one at `open`, past escapes (or the text's end). */
function closingQuote(text: string, open: number): number {
   const quote = text[open];
   for (let i = open + 1; i < text.length; i++) {
      if (text[i] === "\\") i++;
      else if (text[i] === quote) return i;
   }
   return text.length;
}

/**
 * The source a source's declaration (`b is a extend { … }`) builds on, when
 * that passes `a`'s fields on: `extend`, or `b is a` alone. A query's (`b is a
 * -> { group_by: x }`) output columns are new fields that do not carry `a`'s
 * definitions.
 */
function baseOf(declaration: string): string | undefined {
   return /^`?[A-Za-z_]\w*`?\s+is\s+`?([A-Za-z_]\w*)`?\s*(?:extend\b|$)/.exec(
      declaration,
   )?.[1];
}

function unquote(name: string): string {
   return name.replace(/^`(.*)`$/, "$1");
}
