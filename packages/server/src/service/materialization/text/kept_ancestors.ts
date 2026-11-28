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
import { doubleQuotedString, identifier } from "../../malloy_text";
import { offsetOf } from "./malloy_lexer";
import { detachedNode, type SourceMap, type SourceNode } from "./source_map";
import {
   splitSourceID,
   type KeptAlias,
   type KeptAncestor,
   type KeptRange,
} from "../domain/table_plans";

/** The ancestors' walk is bounded, as every chain walk in the module is. */
const MAX_HOPS = 64;

/**
 * A declaration's definition from whichever package model holds it, by
 * sourceID: how the walk reaches an ancestor this model's compiled IR does
 * not hold (a base declared in a file the declaring file imports, which a
 * model importing the declaring file does not see, as Malloy imports are not
 * transitive).
 */
type DeclarationLookup = (sourceID: string) => unknown;

/** The node for `sourceID`: this model's, else one another model declares. */
function nodeIn(
   map: SourceMap,
   sourceID: string,
   elsewhere: DeclarationLookup | undefined,
): SourceNode | undefined {
   return map.node(sourceID) ?? detachedNode(elsewhere?.(sourceID));
}

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
   elsewhere?: DeclarationLookup,
): LocatedAncestor[] {
   const out: LocatedAncestor[] = [];
   let node = nodeIn(map, sourceID, elsewhere);
   let childURL = url;
   for (let hops = 0; node && hops < MAX_HOPS; hops++) {
      const base = node.base;
      if (!base || base.hop === "query" || base.sourceID === undefined) break;
      const parent = nodeIn(map, base.sourceID, elsewhere);
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
 * one only when the extension holds every field of its base and the base is
 * declared where the compiled model shows the order: an `except:` drops a
 * field, so `no_region is orders extend { except: region }` gets no edge, and
 * a model that imports the extension's file but not its base's cannot order
 * the two (`ModelDef.imports` holds the model's own imports only), so the
 * base of `trips_2015 is trips extend { … }`, declared in a file the
 * extension's file imports, gets none in a file importing that one. Without
 * the edge a measure, view or join the base declares was lost from the
 * re-root (`fields_not_carried`). Its base is the source of the name the
 * declaration reads (`<name> is <base> extend …`), resolved in the declaring
 * file's own scope ({@link declaredBaseID}), when that is a table-backed
 * source on the same table and every field the source has that its own
 * statement does not declare is one of the base's. Each ancestor the walk
 * ends on is read the same way, so a chain whose links are all such
 * extensions is followed to its root. {@link confirmedAncestors} confirms
 * each step against the text as it does any other.
 */
export function keptAncestorChainOrDeclared(
   map: SourceMap,
   sourceID: string,
   url: string,
   fileText: (url: string) => string | undefined,
   elsewhere?: DeclarationLookup,
): LocatedAncestor[] {
   const out: LocatedAncestor[] = [];
   const seen = new Set([sourceID]);
   let tail = { sourceID, url };
   for (let hops = 0; hops < MAX_HOPS; hops++) {
      const chain = keptAncestorChain(map, tail.sourceID, tail.url, elsewhere);
      out.push(...chain);
      tail = chain.at(-1) ?? tail;
      const node = nodeIn(map, tail.sourceID, elsewhere);
      // The walk stopped at a recorded hop it does not follow (a query, an
      // unresolved base): only a missing edge is read from the text.
      if (!node || node.base !== undefined) break;
      const base = declaredTableBase(node.def, tail.url, fileText, (id) =>
         nodeIn(map, id, elsewhere),
      );
      for (const a of chain) seen.add(a.sourceID);
      if (!base || seen.has(base.sourceID)) break;
      seen.add(base.sourceID);
      out.push(base);
      tail = base;
   }
   return out;
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
   fileText: (url: string) => string | undefined,
   nodeOf: (sourceID: string) => SourceNode | undefined,
): LocatedAncestor | undefined {
   const own = def as TableDef | undefined;
   if (own?.type !== "table") return undefined;
   const text = fileText(url);
   const range = locatedIn(def, url);
   if (text === undefined || !range) return undefined;
   const name = declaredBaseName(text, range);
   if (name === undefined) return undefined;
   const parentID = declaredBaseID(name, url, text, nodeOf);
   const parent = parentID ? nodeOf(parentID) : undefined;
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

/**
 * The sourceID `name` reads in the file at `url`, by that file's own scope,
 * never the compiled model's namespace (the entry file's: Malloy imports are
 * not transitive, and the entry may declare a namesake): a source the file
 * declares (`name@url`), or one a whole import brings (`name@<imported
 * file>`). A sourceID names the file that declares it, so a name an imported
 * file imported in turn is not found. Exactly one the package declares, or
 * undefined: a selective import (whose text {@link confirmedAncestors} would
 * not confirm either), or text that does not lex, answers live as before.
 */
function declaredBaseID(
   name: string,
   url: string,
   text: string,
   nodeOf: (sourceID: string) => SourceNode | undefined,
): string | undefined {
   const imports = topLevelImports(text, url);
   if (!imports) return undefined;
   const found = [
      `${name}@${url}`,
      ...imports.filter((i) => i.whole).map((i) => `${name}@${i.resolved}`),
   ].filter((id) => nodeOf(id) !== undefined);
   return found.length === 1 ? found[0] : undefined;
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
 *
 * Every kept run is compiled in the source's own file, so a run from
 * another file is kept only where each name it reads binds there as it does
 * in its own file ({@link keptRunImports}); a name the own file does not
 * bind at all is brought by a selective import the re-root adds
 * (`imports`, statements to write in the own file).
 */
export function confirmedAncestors(
   fileText: (url: string) => string | undefined,
   ownURL: string,
   own: KeptRange,
   chain: readonly LocatedAncestor[],
): (LocatedAncestor & { runText?: string; imports?: string[] })[] {
   const out: (LocatedAncestor & { runText?: string; imports?: string[] })[] =
      [];
   let child = own;
   let childURL = ownURL;
   const ownText = fileText(ownURL);
   for (const ancestor of chain) {
      const childText = fileText(childURL);
      const text = fileText(ancestor.url);
      if (
         ownText === undefined ||
         childText === undefined ||
         text === undefined
      ) {
         break;
      }
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
      let imports: string[] = [];
      if (ancestor.url !== ownURL) {
         const proven = keptRunImports(
            ownText,
            ownURL,
            text,
            ancestor.url,
            runText ?? "",
            fileText,
         );
         if (!proven) break;
         imports = proven;
      }
      out.push({
         ...ancestor,
         ...(runText !== undefined ? { runText } : {}),
         ...(imports.length > 0 ? { imports } : {}),
      });
      child = ancestor.range;
      childURL = ancestor.url;
   }
   return out;
}

/**
 * Whether `runText`, written in the file at `ancestorURL`, reads every name
 * the same way compiled in the file at `ownURL`, and the selective imports
 * that make it so; undefined when it is not proven (review).
 *
 * Each file's top-level names are read exactly ({@link fileBindings}): the
 * sources, queries and givens it declares, each a selective import binds,
 * and each one a whole import's file declares itself (Malloy imports are not
 * transitive, so a name that file imported in turn is not one of them). For
 * every word of the run (a backticked name included):
 *
 *  - a name the ancestor's file binds must bind in the own file to the same
 *    declaration (the same declaring file and declared name), or not at all,
 *    and then the re-root imports it there selectively from the file that
 *    declares it (`import { users } from "users.malloy"`, renamed as the
 *    ancestor's file renames it). The own file declares or imports no such
 *    name anywhere, so the import collides with nothing, and Malloy imports
 *    are not transitive, so no file importing the own file sees it;
 *  - any other word must bind to nothing in the own file either: a field
 *    name the own file happened to declare as a source is refused rather
 *    than reasoned about.
 *
 * A file that does not lex, or that a whole import names and cannot be read,
 * proves nothing. Unproven, the blocks are not carried and the source is
 * served as before (or answers live): a join in the run that bound to
 * another source in the own file would read a table curation or a gate
 * keeps from the caller.
 */
function keptRunImports(
   ownText: string,
   ownURL: string,
   ancestorText: string,
   ancestorURL: string,
   runText: string,
   fileText: (url: string) => string | undefined,
): string[] | undefined {
   const theirs = fileBindings(ancestorText, ancestorURL, fileText);
   const ours = fileBindings(ownText, ownURL, fileText);
   if (!theirs || !ours) return undefined;
   const runTokens = lexAll(runText);
   if (!runTokens) return undefined;
   // A `%{ … }` embed in a SQL string reads Malloy names the lexer folds into
   // the string's one token, so no check here can see them: a block that
   // embeds is not carried across files (it answers live, as before).
   if (runTokens.some((t) => !isName(t) && t.text.includes("%{"))) {
      return undefined;
   }
   const runWords = new Set(
      runTokens.flatMap((t) => (isName(t) ? [unquote(t.text)] : [])),
   );
   const needed = new Map<string, string[]>();
   for (const word of [...runWords].sort()) {
      const want = theirs.get(word);
      const have = ours.get(word);
      if (!want) {
         if (have) return undefined;
         continue;
      }
      if (have) {
         if (!sameFile(have.file, want.file) || have.name !== want.name) {
            return undefined;
         }
         continue;
      }
      const list = needed.get(want.file) ?? [];
      list.push(
         want.name === word
            ? identifier(word)
            : `${identifier(word)} is ${identifier(want.name)}`,
      );
      needed.set(want.file, list);
   }
   return [...needed].map(
      ([file, names]) =>
         // malloy-text: each name was written by identifier() above.
         `import { ${names.join(", ")} } from ${doubleQuotedString(relativeImportPath(ownURL, file))}`,
   );
}

/** The declaration a top-level name reads: its declaring file and declared name. */
interface Binding {
   file: string;
   name: string;
}

/**
 * Every top-level name the file at `url` binds, to the declaration it reads
 * ({@link keptRunImports}). Undefined when the file, or a file it imports
 * whole, cannot be read or lexed, or one name binds two ways.
 */
function fileBindings(
   text: string,
   url: string,
   fileText: (url: string) => string | undefined,
): Map<string, Binding> | undefined {
   const imports = topLevelImports(text, url);
   const declared = namesDeclaredOrImported(text, []);
   if (!imports || !declared) return undefined;
   const out = new Map<string, Binding>();
   const bind = (local: string, b: Binding): boolean => {
      const had = out.get(local);
      if (had && (!sameFile(had.file, b.file) || had.name !== b.name)) {
         return false;
      }
      out.set(local, b);
      return true;
   };
   for (const name of declared)
      if (!bind(name, { file: url, name })) return undefined;
   for (const i of imports) {
      if (!i.whole) {
         for (const p of i.pairs) {
            if (!bind(p.local, { file: i.resolved, name: p.original })) {
               return undefined;
            }
         }
         continue;
      }
      const imported = fileText(i.resolved);
      const own =
         imported === undefined
            ? undefined
            : namesDeclaredOrImported(imported, []);
      if (!own) return undefined;
      for (const name of own) {
         if (!bind(name, { file: i.resolved, name })) return undefined;
      }
   }
   return out;
}

/** `to` as an import path in the file at `from`: relative when both are files. */
function relativeImportPath(from: string, to: string): string {
   try {
      const a = new URL(from);
      const b = new URL(to);
      if (a.protocol !== b.protocol || a.host !== b.host) return b.href;
      const dir = a.pathname.split("/").slice(0, -1);
      const target = b.pathname.split("/");
      let k = 0;
      while (k < dir.length && k < target.length - 1 && dir[k] === target[k])
         k++;
      const up = dir.length - k;
      const rest = target.slice(k).join("/");
      return up === 0 ? rest : `${"../".repeat(up)}${rest}`;
   } catch {
      return to;
   }
}

/** A top-level `import` of a file: its path resolved, and whether it is whole. */
interface FileImport {
   resolved: string;
   whole: boolean;
   /** A selective import's local names. */
   names: string[];
   /** A selective import's names, each local and as its file declares it. */
   pairs: { local: string; original: string }[];
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
      const pairs: { local: string; original: string }[] = [];
      let whole = true;
      let j = k + 1;
      if (tokens[j]?.text === "{") {
         whole = false;
         // `{ a, b is c }`: each local name is the word before `is`, or the
         // word alone; the declared name is the word after `is`, or the same.
         j++;
         let expectName = true;
         let renaming = false;
         for (; j < tokens.length && tokens[j].text !== "}"; j++) {
            const w = tokens[j];
            if (w.text === ",") {
               expectName = true;
               renaming = false;
               continue;
            }
            if (expectName && isName(w)) {
               names.push(unquote(w.text));
               pairs.push({
                  local: unquote(w.text),
                  original: unquote(w.text),
               });
               expectName = false;
               continue;
            }
            if (w.kind === "word" && w.text === "is") {
               renaming = true;
               continue;
            }
            if (renaming && isName(w) && pairs.length > 0) {
               pairs[pairs.length - 1].original = unquote(w.text);
               renaming = false;
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
      out.push({ resolved, whole, names, pairs });
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
