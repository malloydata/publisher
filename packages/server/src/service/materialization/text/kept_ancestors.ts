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
 * its base's file keeps that base's blocks too (P2-22). A query-shaped
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
      out.push({
         ...ancestor,
         ...(spans.extendStart !== undefined
            ? { runText: text.slice(from + spans.extendStart, to).trimEnd() }
            : {}),
      });
      child = ancestor.range;
      childURL = ancestor.url;
   }
   return out;
}

/** The file a compiled declaration was declared in. */
function declaringURLOf(def: unknown): string | undefined {
   const url = (def as { location?: { url?: unknown } } | undefined)?.location
      ?.url;
   return typeof url === "string" ? url : undefined;
}
