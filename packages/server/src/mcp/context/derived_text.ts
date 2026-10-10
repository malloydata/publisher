// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The searchable text of one catalog node, built from the node and what is
 * around it rather than from its raw name and doc alone:
 *
 * - a field: its name in words, the "what it is" part of its doc, and the
 *   name and one-line summary of its source;
 * - a field of a join the catalog keeps as a field of its own (a raw table,
 *   or fields the join adds): its own doc first, then the path in words with
 *   the first sentence of the join's own doc ("Reached from orders through
 *   customer: ..."), then the line of the source the join is made from when
 *   that source is in the catalog;
 * - a field computed from other fields: the names and "what it is" of what it
 *   reads, so `net_sales is sales { where: is_fulfilled }` is found by
 *   "fulfilled" with no doc at all;
 * - a view: its doc and the fields it outputs;
 * - a source: its summary.
 *
 * An extension's line is its own doc, never a stored summary (which may have
 * been written from its parent's notes), and a separate "Extends <parent>"
 * line carries the parent's, so the parent's doc never reads as the
 * extension's.
 *
 * Rules in a doc ("exclude test accounts") are not here: they are kept for the
 * judge and never embedded (see doc_parts.ts). The text is capped at
 * DERIVED_TEXT_MAX_WORDS so a long doc cannot make one row dominate the cost;
 * a node's own doc comes first, so the cap cuts its context, not its doc.
 */

import { humanizeName } from "../tools/embedding_index";
import { nodeKeyOf, type Catalog, type CatalogNode } from "./catalog";

export const DERIVED_TEXT_MAX_WORDS = 120;

/** What the text of a node may draw on besides the catalog. */
export interface DerivedTextContext {
   catalog: Catalog;
   /** The "what it is" part of a node's doc; the whole doc when not split. */
   whatOf: (node: CatalogNode) => string;
   /** A source's one-line summary, by source key, when one is stored. */
   summaryOf: (sourceKey: string) => string | undefined;
}

/** A name in words, falling back to the raw name for a punctuation-only identifier. */
function words(name: string): string {
   return humanizeName(name) || name;
}

/** The first sentence of a text, or the text when it has none. */
export function firstSentence(text: string): string {
   const t = text.replace(/\s+/g, " ").trim();
   const end = t.search(/[.!?](\s|$)/);
   return end === -1 ? t : t.slice(0, end + 1);
}

function trimEnd(text: string): string {
   return text.trim().replace(/[.\s]+$/, "");
}

/** Cut a text to at most `max` words. */
export function capWords(text: string, max: number): string {
   const parts = text.split(/\s+/).filter(Boolean);
   return parts.length <= max ? parts.join(" ") : parts.slice(0, max).join(" ");
}

/**
 * The source's line: its summary, else the first sentence of its own doc. An
 * extension's is always its own doc.
 */
function sourceLine(
   ctx: DerivedTextContext,
   sourceKey: string,
): string | undefined {
   const source = ctx.catalog.sources.get(sourceKey);
   const summary = source?.extendsKey ? undefined : ctx.summaryOf(sourceKey);
   if (summary) return summary;
   if (!source) return undefined;
   const what = ctx.whatOf(source.node);
   return what ? firstSentence(what) : undefined;
}

/** `name: line`, or the name alone when there is no line. */
function labelled(name: string, line: string | undefined): string {
   return line ? `${name}: ${trimEnd(line)}` : name;
}

/** "Extends <parent>: <its line>" for an extension, else undefined. */
function extendsLine(
   ctx: DerivedTextContext,
   sourceKey: string,
): string | undefined {
   const parentKey = ctx.catalog.sources.get(sourceKey)?.extendsKey;
   const parent = parentKey ? ctx.catalog.sources.get(parentKey) : undefined;
   if (!parent) return undefined;
   return labelled(
      `Extends ${words(parent.name)}`,
      sourceLine(ctx, parent.key),
   );
}

/**
 * The join node a field of a join is reached through: the last hop of its
 * path, declared on its own source or on the catalog source the rest of the
 * path reaches. Undefined when that hop is on a raw table.
 */
function joinNodeOf(
   catalog: Catalog,
   node: CatalogNode,
): CatalogNode | undefined {
   const path = node.joinPath!.split(".");
   const via =
      path.length === 1
         ? node.sourceKey
         : catalog.joins
              .get(node.sourceKey)
              ?.find((h) => h.path.join(".") === path.slice(0, -1).join("."))
              ?.to;
   return via
      ? catalog.nodes.get(nodeKeyOf("join", via, path[path.length - 1]))
      : undefined;
}

export function derivedText(
   node: CatalogNode,
   ctx: DerivedTextContext,
): string {
   const parts: string[] = [];
   const what = trimEnd(ctx.whatOf(node));
   if (node.kind === "source") {
      const own = ctx.catalog.sources.get(node.sourceKey)?.extendsKey
         ? what
         : (ctx.summaryOf(node.sourceKey) ?? what);
      parts.push(labelled(words(node.name), own));
      const ext = extendsLine(ctx, node.sourceKey);
      if (ext) parts.push(ext);
      return finish(parts);
   }
   const leaf = node.joinPath
      ? node.name.slice(node.joinPath.length + 1)
      : node.name;
   parts.push(labelled(words(leaf), what));
   if (node.joinPath) {
      const join = joinNodeOf(ctx.catalog, node);
      const joinWhat = join ? trimEnd(ctx.whatOf(join)) : "";
      parts.push(
         labelled(
            `Reached from ${words(node.source)} through ${words(node.joinPath)}`,
            joinWhat ? firstSentence(joinWhat) : undefined,
         ),
      );
      const joined = node.joinedSourceKey
         ? ctx.catalog.sources.get(node.joinedSourceKey)
         : undefined;
      if (joined) {
         parts.push(
            labelled(`From ${words(joined.name)}`, sourceLine(ctx, joined.key)),
         );
      }
   }
   const deps = node.dependsOn
      .map((k) => ctx.catalog.nodes.get(k))
      .filter((n): n is CatalogNode => n !== undefined);
   if (deps.length > 0) {
      parts.push(
         `Computed from ${deps
            .map((d) => {
               const w = trimEnd(firstSentence(ctx.whatOf(d)));
               return w ? `${words(d.name)} (${w})` : words(d.name);
            })
            .join(", ")}`,
      );
   }
   const used = node.uses
      .map((k) => ctx.catalog.nodes.get(k))
      .filter((n): n is CatalogNode => n !== undefined);
   if (used.length > 0) {
      parts.push(`Uses ${used.map((u) => words(u.name)).join(", ")}`);
   }
   parts.push(
      labelled(`In ${words(node.source)}`, sourceLine(ctx, node.sourceKey)),
   );
   const ext = extendsLine(ctx, node.sourceKey);
   if (ext) parts.push(ext);
   return finish(parts);
}

function finish(parts: string[]): string {
   const text = parts
      .map(trimEnd)
      .filter(Boolean)
      .map((p) => `${p}.`)
      .join(" ");
   return capWords(text, DERIVED_TEXT_MAX_WORDS);
}
