// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The ancestors whose trailing `extend { … }` runs a `#(materialize)` re-root
 * keeps: an extension table's bases, and a base whose given-reading terms or
 * dimensions the build leaves out.
 *
 * The served copy replaces a source's declaration base with its stored table,
 * so whatever a base it is built from declares is gone from the served source
 * unless the re-root writes that base's blocks again. It can for a chain of
 * plain extensions: `ext is mid extend { … }`, `mid is base extend { … }`,
 * `base is <anything> extend { … }`, in one file or across files one
 * imports. Each ancestor's trailing run is kept, root first, ahead of the
 * source's own, over the stored columns; a run that does not compile in the
 * source's file falls back as an extension table does.
 *
 * One walk for both sides. The build-file generator plans from the compiled
 * model (no text), and its build copy confirms the plan against the file's
 * text; the served copy walks again with the text. Both stop where the walk
 * stops here, so a term or dimension the build leaves out because a kept
 * block re-applies it is in the served copy's kept blocks.
 *
 * Pure.
 */

import { declarationSpans, declaredBaseName } from "./declaration_text";
import { lexAll } from "../../malloy_lexer_core";
import { offsetOf } from "./malloy_lexer";
import type { SourceMap } from "./source_map";
import {
   splitSourceID,
   type KeptAlias,
   type KeptAncestor,
   type KeptRange,
} from "../domain/table_plans";

/** The ancestors' walk is bounded, as every chain walk in the module is. */
const MAX_HOPS = 64;

/**
 * The renames of `sourceID` declared in `url`, transitively: each a rename hop
 * (`b2 is base`, which Malloy records as `referenceID`) to it or to one
 * already found. A rename adds nothing to its referent, so it has no block
 * to keep; the text check ({@link confirmedAncestors}) confirms each is a
 * bare rename.
 */
function plainRenamesOf(
   map: SourceMap,
   sourceID: string,
   url: string,
): KeptAlias[] {
   const out: KeptAlias[] = [];
   const reached = new Set([sourceID]);
   for (let grew = true, rounds = 0; grew && rounds < MAX_HOPS; rounds++) {
      grew = false;
      for (const node of map.nodes()) {
         const base = node.base;
         if (reached.has(node.sourceID) || base?.hop !== "rename") continue;
         if (base.sourceID === undefined || !reached.has(base.sourceID)) {
            continue;
         }
         const range = locatedIn(node.def, url);
         if (!range) continue;
         out.push({ sourceID: node.sourceID, range });
         reached.add(node.sourceID);
         grew = true;
      }
   }
   return out;
}

/** The name a sourceID was declared with (`name@url`). */
function declaredName(sourceID: string): string | undefined {
   return splitSourceID(sourceID)?.name;
}

/**
 * Whether the declaration at `child` names `ancestor` as its whole base, in
 * the text: by its own name, or by one of its renames, each of which the
 * text declares as a bare rename (`<alias> is <next>`, no `extend`), down to
 * the ancestor's name.
 */
function namesAncestor(
   text: string,
   child: KeptRange,
   ancestor: KeptAncestor,
): boolean {
   const aliases = new Map(
      (ancestor.aliases ?? []).map((a) => [declaredName(a.sourceID), a]),
   );
   let named = declaredBaseName(text, child);
   for (let hops = 0; named !== undefined && hops <= aliases.size; hops++) {
      if (named === declaredName(ancestor.sourceID)) return true;
      const alias = aliases.get(named);
      if (!alias) return false;
      const from = offsetOf(text, alias.range.start);
      const to = offsetOf(text, alias.range.end);
      if (from < 0 || to < from) return false;
      const spans = declarationSpans(text.slice(from, to));
      if (!spans || spans.extendStart !== undefined) return false;
      named = declaredBaseName(text, alias.range);
   }
   return false;
}

/** A compiled declaration's range when it is declared in `url`. */
function locatedIn(def: unknown, url: string): KeptRange | undefined {
   const location = (
      def as { location?: { url?: unknown; range?: KeptRange } } | undefined
   )?.location;
   return location?.url === url &&
      typeof location.range?.start?.line === "number" &&
      typeof location.range?.end?.line === "number"
      ? location.range
      : undefined;
}

/** A {@link KeptAncestor} declared in the file at `url`. */
interface LocatedAncestor extends KeptAncestor {
   url: string;
}

/**
 * The ancestors of `sourceID` whose blocks its re-root can keep, nearest
 * first, from the compiled model: up `extend` and rename hops, each in
 * whatever file declares it, so an extension declared in a file that imports
 * its base's file keeps that base's blocks too. A query-shaped
 * ancestor (`x is y -> { … } extend { … }`) is kept, and the walk stops
 * there: its trailing run is over its query's output, which is what its
 * extensions' tables store. Each ancestor's renames are the ones declared in
 * the file of the source naming it, where {@link confirmedAncestors} reads
 * them. Empty when the source extends nothing.
 */
export function keptAncestorChain(
   map: SourceMap,
   sourceID: string,
   url: string,
): LocatedAncestor[] {
   const out: LocatedAncestor[] = [];
   let node = map.node(sourceID);
   let childURL = url;
   for (let hops = 0; node && hops < MAX_HOPS; hops++) {
      const base = node.base;
      if (!base || base.hop === "query" || base.sourceID === undefined) break;
      const parent = map.node(base.sourceID);
      const parentURL = declaringURLOf(parent?.def);
      const range = parentURL ? locatedIn(parent?.def, parentURL) : undefined;
      if (!parent || !parentURL || !range) break;
      const aliases = plainRenamesOf(map, parent.sourceID, childURL);
      out.push({
         sourceID: parent.sourceID,
         url: parentURL,
         range,
         ...(aliases.length > 0 ? { aliases } : {}),
      });
      node = parent;
      childURL = parentURL;
   }
   return out;
}

/**
 * {@link keptAncestorChain}, and for a source the compiled model records no
 * base for, the base its declaration names in the text. Malloy records no
 * edge for an extension of a table-backed source, and the source map infers
 * one only when the extension holds every field of its base: an `except:`
 * drops one, so `no_region is orders extend { except: region }` gets no
 * edge, and a measure, view or join `orders` declares was lost from the
 * re-root (`fields_not_carried`). Its base is the source of the name
 * the declaration reads (`<name> is <base> extend …`), in its own file or
 * one it imports, when that is a table-backed source on the same table and
 * every field the source has that its own statement does not declare is
 * one of the base's. {@link confirmedAncestors} confirms the step against
 * the text as it does any other, and the chain continues from the base.
 */
export function keptAncestorChainOrDeclared(
   map: SourceMap,
   sourceID: string,
   url: string,
   fileText: (url: string) => string | undefined,
): LocatedAncestor[] {
   const chain = keptAncestorChain(map, sourceID, url);
   const node = map.node(sourceID);
   if (chain.length > 0 || !node || node.base !== undefined) return chain;
   const base = declaredTableBase(node.def, url, map, fileText);
   if (!base) return chain;
   return [base, ...keptAncestorChain(map, base.sourceID, base.url)];
}

/** A table-backed source's compiled shape, as the declared-base check reads it. */
interface TableDef {
   type?: unknown;
   connection?: unknown;
   tablePath?: unknown;
   fields?: unknown;
}

/** The declared base of a table-backed extension the model records none for. */
function declaredTableBase(
   def: unknown,
   url: string,
   map: SourceMap,
   fileText: (url: string) => string | undefined,
): LocatedAncestor | undefined {
   const own = def as TableDef | undefined;
   if (own?.type !== "table") return undefined;
   const text = fileText(url);
   const range = locatedIn(def, url);
   if (text === undefined || !range) return undefined;
   const name = declaredBaseName(text, range);
   if (name === undefined) return undefined;
   const parentID = [`${name}@${url}`, map.sourceIDOf(name)].find(
      (id): id is string => id !== undefined && map.node(id) !== undefined,
   );
   const parent = parentID ? map.node(parentID) : undefined;
   const base = parent?.def as TableDef | undefined;
   if (
      !parent ||
      base?.type !== "table" ||
      base.connection !== own.connection ||
      base.tablePath !== own.tablePath
   ) {
      return undefined;
   }
   const parentURL = declaringURLOf(parent.def);
   const parentRange = parentURL ? locatedIn(parent.def, parentURL) : undefined;
   if (!parentURL || !parentRange) return undefined;
   // What the source has and did not declare itself, the base has.
   const baseNames = new Set(fieldNames(base));
   const inherited = fieldList(own).filter((f) => !declaredIn(f, url, range));
   if (!inherited.every((f) => baseNames.has(fieldName(f) ?? ""))) {
      return undefined;
   }
   return { sourceID: parent.sourceID, url: parentURL, range: parentRange };
}

function fieldList(def: TableDef): unknown[] {
   return Array.isArray(def.fields) ? def.fields : [];
}

function fieldName(field: unknown): string | undefined {
   const f = field as { as?: unknown; name?: unknown } | undefined;
   const name = f?.as ?? f?.name;
   return typeof name === "string" ? name : undefined;
}

function fieldNames(def: TableDef): string[] {
   return fieldList(def).flatMap((f) => fieldName(f) ?? []);
}

/** Whether a compiled field was declared inside the statement at `range` in `url`. */
function declaredIn(field: unknown, url: string, range: KeptRange): boolean {
   const at = locatedIn(field, url);
   if (!at) return false;
   const before = (
      a: { line: number; character: number },
      b: { line: number; character: number },
   ) => a.line < b.line || (a.line === b.line && a.character <= b.character);
   return before(range.start, at.start) && before(at.end, range.end);
}

/**
 * The prefix of `chain` the files' text confirms, each with its trailing
 * run's text: each declaration names the next ancestor as its whole base
 * (`<name> is <ancestor> extend …`, the declared name read from its
 * sourceID), directly or through bare renames of it
 * ({@link KeptAncestor.aliases}), in the text of the file declaring it, and
 * the ancestor's declaration parses in its own file. `own` is the source's
 * own declaration range in `ownURL`. The walk stops at the first step the
 * text does not confirm, or a file it cannot read.
 */
export function confirmedAncestors(
   fileText: (url: string) => string | undefined,
   ownURL: string,
   own: KeptRange,
   chain: readonly LocatedAncestor[],
): (LocatedAncestor & { runText?: string })[] {
   const out: (LocatedAncestor & { runText?: string })[] = [];
   let child = own;
   let childURL = ownURL;
   for (const ancestor of chain) {
      const childText = fileText(childURL);
      const text = fileText(ancestor.url);
      if (childText === undefined || text === undefined) break;
      if (!namesAncestor(childText, child, ancestor)) break;
      const from = offsetOf(text, ancestor.range.start);
      const to = offsetOf(text, ancestor.range.end);
      if (from < 0 || to < from) break;
      const spans = declarationSpans(text.slice(from, to));
      if (!spans) break;
      const runText =
         spans.extendStart !== undefined
            ? text.slice(from + spans.extendStart, to).trimEnd()
            : undefined;
      // Across files, the run is compiled in the importing file: only where
      // each name it reads is provably the one its own file reads.
      if (
         ancestor.url !== childURL &&
         !bindsAlikeAcrossFiles(
            childText,
            childURL,
            text,
            ancestor.url,
            runText ?? "",
            fileText,
         )
      ) {
         break;
      }
      out.push({ ...ancestor, ...(runText !== undefined ? { runText } : {}) });
      child = ancestor.range;
      childURL = ancestor.url;
   }
   return out;
}

/**
 * Whether `runText`, written in the file at `ancestorURL`, reads every name
 * the same way compiled in the file at `childURL` (review).
 *
 * The guarantee is structural, from two conditions:
 *
 *  - the child imports the ancestor's file whole (`import "<file>"`), never
 *    by a selective import, which may leave a name out for the child to
 *    declare itself. A whole import brings every name the ancestor's file
 *    declares, and Malloy refuses a file that declares again a name it
 *    imported, so each such name reads the ancestor's own source;
 *  - no word of the run is a name the ancestor's file imports: a selective
 *    import's local names, and for a whole import every name the imported
 *    file declares or imports, through its own whole imports in turn. A name
 *    the ancestor's file imported need not reach the child through the whole
 *    import, so the child could bind it itself; a run that reads none reads
 *    only names the ancestor's file declares. An imported file that cannot
 *    be read or lexed proves nothing.
 *
 * A third check is defence in depth: no word of the run (a backticked name
 * included) is a name the child declares or imports selectively from
 * elsewhere.
 *
 * Unproven, the blocks are not carried and the source is served as before
 * (or answers live): a join in the run that bound to another source in the
 * child would read a table curation or a gate keeps from the caller.
 */
function bindsAlikeAcrossFiles(
   childText: string,
   childURL: string,
   ancestorText: string,
   ancestorURL: string,
   runText: string,
   fileText: (url: string) => string | undefined,
): boolean {
   const childImports = topLevelImports(childText, childURL);
   const ancestorImports = topLevelImports(ancestorText, ancestorURL);
   if (!childImports || !ancestorImports) return false;
   if (
      !childImports.some((i) => i.whole && sameFile(i.resolved, ancestorURL))
   ) {
      return false;
   }
   const runTokens = lexAll(runText);
   if (!runTokens) return false;
   const runWords = new Set(
      runTokens.flatMap((t) => (isName(t) ? [unquote(t.text)] : [])),
   );
   const imported = namesImported(ancestorImports, fileText);
   if (!imported) return false;
   for (const name of imported) if (runWords.has(name)) return false;
   const childNames = namesDeclaredOrImported(childText, childImports);
   if (!childNames) return false;
   for (const name of childNames) if (runWords.has(name)) return false;
   return true;
}

/**
 * Every name `imports` bring into a file: a selective import's local names,
 * and for a whole import each name the imported file declares or imports,
 * following its whole imports in turn (each file once). Undefined when an
 * imported file cannot be read or lexed.
 */
function namesImported(
   imports: readonly FileImport[],
   fileText: (url: string) => string | undefined,
): Set<string> | undefined {
   const names = new Set<string>();
   const seen = new Set<string>();
   const pending = [...imports];
   while (pending.length > 0) {
      const next = pending.pop()!;
      for (const name of next.names) names.add(name);
      if (!next.whole || seen.has(next.resolved)) continue;
      seen.add(next.resolved);
      const text = fileText(next.resolved);
      if (text === undefined) return undefined;
      const own = topLevelImports(text, next.resolved);
      const declared = own && namesDeclaredOrImported(text, own);
      if (!own || !declared) return undefined;
      for (const name of declared) names.add(name);
      pending.push(...own);
   }
   return names;
}

/** A top-level `import` of a file: its path resolved, and whether it is whole. */
interface FileImport {
   resolved: string;
   whole: boolean;
   /** A selective import's local names. */
   names: string[];
}

/** The file's top-level imports, or undefined when the text does not lex. */
function topLevelImports(text: string, url: string): FileImport[] | undefined {
   const tokens = lexAll(text);
   if (!tokens) return undefined;
   const out: FileImport[] = [];
   for (let k = 0; k < tokens.length; k++) {
      const t = tokens[k];
      if (t.kind !== "word" || t.text !== "import" || t.depth !== 0) continue;
      const names: string[] = [];
      let whole = true;
      let j = k + 1;
      if (tokens[j]?.text === "{") {
         whole = false;
         // `{ a, b is c }`: each local name is the word before `is`, or the
         // word alone.
         j++;
         let expectName = true;
         for (; j < tokens.length && tokens[j].text !== "}"; j++) {
            const w = tokens[j];
            if (w.text === ",") {
               expectName = true;
               continue;
            }
            if (expectName && isName(w)) {
               names.push(unquote(w.text));
               expectName = false;
            }
         }
         j++;
         if (tokens[j]?.text === "from") j++;
      }
      // The path is the statement's first string at the top level.
      while (j < tokens.length && !/^["']/.test(tokens[j].text)) j++;
      const literal = tokens[j];
      if (!literal || literal.depth !== 0) return undefined;
      const path = literal.text.slice(1, -1);
      if (path.includes("\\")) return undefined;
      let resolved: string;
      try {
         resolved = new URL(path, url).href;
      } catch {
         return undefined;
      }
      out.push({ resolved, whole, names });
   }
   return out;
}

/**
 * The names the file declares at top level (`source:`, `query:`, `given:`
 * statements, each item of one), and the local names its selective imports
 * bind. Undefined when the text does not lex.
 */
function namesDeclaredOrImported(
   text: string,
   imports: readonly FileImport[],
): Set<string> | undefined {
   const tokens = lexAll(text);
   if (!tokens) return undefined;
   const names = new Set(imports.flatMap((i) => i.names));
   const declaring = new Set(["source", "query", "given", "run"]);
   let inStatement = false;
   for (let k = 0; k < tokens.length; k++) {
      const t = tokens[k];
      if (t.depth !== 0) continue;
      if (
         t.kind === "word" &&
         declaring.has(t.text) &&
         tokens[k + 1]?.text === ":"
      ) {
         inStatement = true;
         k++;
         const next = tokens[k + 1];
         if (next && isName(next)) names.add(unquote(next.text));
         continue;
      }
      // A later item of the same statement: `, name is …`.
      if (
         inStatement &&
         t.text === "," &&
         tokens[k + 1] &&
         isName(tokens[k + 1])
      ) {
         names.add(unquote(tokens[k + 1].text));
         continue;
      }
      if (
         t.kind === "word" &&
         tokens[k + 1]?.text === ":" &&
         !declaring.has(t.text)
      ) {
         inStatement = false;
      }
   }
   return names;
}

/** A name: a word, or a backticked name (which the lexer leaves unclassified). */
function isName(t: { kind: string; text: string }): boolean {
   return (
      t.kind === "word" ||
      (t.text.length > 1 && t.text.startsWith("`") && t.text.endsWith("`"))
   );
}

function unquote(word: string): string {
   return word.startsWith("`") && word.endsWith("`") ? word.slice(1, -1) : word;
}

function sameFile(a: string, b: string): boolean {
   try {
      return new URL(a).href === new URL(b).href;
   } catch {
      return a === b;
   }
}

/** The file a compiled declaration was declared in. */
function declaringURLOf(def: unknown): string | undefined {
   const url = (def as { location?: { url?: unknown } } | undefined)?.location
      ?.url;
   return typeof url === "string" ? url : undefined;
}
