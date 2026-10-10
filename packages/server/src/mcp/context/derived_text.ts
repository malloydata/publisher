// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The searchable text of one catalog node, built from the node and what is
 * around it rather than from its raw name and doc alone:
 *
 * - a field: its name in words, the "what it is" part of its doc, and the
 *   name and one-line summary of its source;
 * - a field of an inline-table join: the same, plus the path in words
 *   ("region, reached from orders through customer");
 * - a field computed from other fields: the names and "what it is" of what it
 *   reads, so `net_sales is sales { where: is_fulfilled }` is found by
 *   "fulfilled" with no doc at all;
 * - a view: its doc and the fields it outputs;
 * - a source: its summary.
 *
 * Rules in a doc ("exclude test accounts") are not here: they are kept for the
 * judge and never embedded (see doc_parts.ts). The text is capped at
 * DERIVED_TEXT_MAX_WORDS so a long doc cannot make one row dominate the cost.
 */

import { humanizeName } from "../tools/embedding_index";
import type { Catalog, CatalogNode } from "./catalog";

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

/** The source's line: its summary, else the first sentence of its own doc. */
function sourceLine(
   ctx: DerivedTextContext,
   sourceKey: string,
): string | undefined {
   const summary = ctx.summaryOf(sourceKey);
   if (summary) return summary;
   const source = ctx.catalog.sources.get(sourceKey);
   if (!source) return undefined;
   const what = ctx.whatOf(source.node);
   return what ? firstSentence(what) : undefined;
}

export function derivedText(
   node: CatalogNode,
   ctx: DerivedTextContext,
): string {
   const parts: string[] = [];
   const what = trimEnd(ctx.whatOf(node));
   if (node.kind === "source") {
      const line = ctx.summaryOf(node.sourceKey) ?? what;
      parts.push(
         line ? `${words(node.name)}: ${trimEnd(line)}` : words(node.name),
      );
      return finish(parts);
   }
   const leaf = node.joinPath
      ? node.name.slice(node.joinPath.length + 1)
      : node.name;
   let head = words(leaf);
   if (node.joinPath) {
      head += `, reached from ${words(node.source)} through ${words(node.joinPath)}`;
   }
   parts.push(what ? `${head}: ${what}` : head);
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
   const line = sourceLine(ctx, node.sourceKey);
   parts.push(
      line
         ? `In ${words(node.source)}: ${trimEnd(line)}`
         : `In ${words(node.source)}`,
   );
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
