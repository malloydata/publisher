// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { GivenInfo } from "./modelGivens";

/**
 * Which givens a piece of Malloy reads, found in its text.
 *
 * The compiled model lists a model's givens but not where each is used, so
 * this reads the definitions the page already has: a
 * field's or view's code, and the model file's text. A given is referenced as
 * `$NAME` and its name is a declared identifier, so a whole-name match against
 * the declared list is reliable; comments are skipped so a mention there is not
 * a use. Host-filled givens never reach here (the viewer leaves out its
 * `hostGivenNames`), so `$GROUPS` in a gate is not a reader's given.
 */
export function givensReadBy(
   code: string | undefined,
   givens: ReadonlyArray<GivenInfo>,
): GivenInfo[] {
   if (!code || givens.length === 0) return [];
   const text = withoutComments(code);
   return givens.filter((g) =>
      new RegExp(`\\$${escapeRegExp(g.name)}(?![A-Za-z0-9_])`).test(text),
   );
}

/**
 * The givens a named query reads when the model doesn't say which source it
 * runs against: those its declaration names, plus those of every source of
 * the model it names (`givensBySource`, what a run on that source gates on).
 * Undefined without its declaration's text, where every given is the only
 * safe answer.
 */
export function givensOfNamedQuery(
   code: string | undefined,
   givens: ReadonlyArray<GivenInfo>,
   givensBySource: ReadonlyMap<string, ReadonlyArray<GivenInfo>> | undefined,
): GivenInfo[] | undefined {
   if (!code) return undefined;
   const text = withoutComments(code);
   const read = new Set(givensReadBy(code, givens).map((g) => g.name));
   for (const [source, sourceGivens] of givensBySource ?? []) {
      const named = new RegExp(
         `(?<![A-Za-z0-9_$.])\`?${escapeRegExp(source)}\`?(?![A-Za-z0-9_])`,
      );
      if (named.test(text)) for (const g of sourceGivens) read.add(g.name);
   }
   return givens.filter((g) => read.has(g.name));
}

/**
 * The givens that matter on one source's page: those its own declaration reads
 * (and the sources it extends, derives from or joins, which its queries run
 * through — exported or not), plus the model-wide ones no source reads — a
 * given used only by a top-level query, or by nothing yet, belongs to every
 * page. Falls back to every given when the model's text is absent or the
 * source's declaration is not one this can find, so a page never hides a
 * control it might need.
 */
export function givensForSource(
   sourceText: string | undefined,
   sourceName: string,
   givens: GivenInfo[],
): GivenInfo[] {
   const walk = walkSource(sourceText, sourceName, givens);
   if (!walk) return [...givens];
   return givens.filter(
      (g) => walk.readHere.has(g.name) || !walk.readByAnySource.has(g.name),
   );
}

/**
 * The givens a source's page shows only because a source it builds on reads
 * them, where that source isn't one the page can open (the model doesn't
 * export it): given name → the unexported sources that read it. A reader sees
 * the control with no field on the page that uses it, so the section names
 * where the given comes from.
 */
export function givensInheritedBy(
   sourceText: string | undefined,
   sourceName: string,
   givens: GivenInfo[],
   knownSources: ReadonlySet<string>,
): Map<string, string[]> {
   const out = new Map<string, string[]>();
   const walk = walkSource(sourceText, sourceName, givens);
   if (!walk) return out;
   const readByShown = new Set<string>();
   for (const name of walk.reached) {
      if (name !== sourceName && !knownSources.has(name)) continue;
      for (const g of givensReadBy(walk.blocks.get(name), givens))
         readByShown.add(g.name);
   }
   for (const name of walk.reached) {
      if (name === sourceName || knownSources.has(name)) continue;
      for (const g of givensReadBy(walk.blocks.get(name), givens)) {
         if (readByShown.has(g.name)) continue;
         out.set(g.name, [...(out.get(g.name) ?? []), name]);
      }
   }
   return out;
}

/** Every source a source's queries run through, and the givens they read. */
function walkSource(
   sourceText: string | undefined,
   sourceName: string,
   givens: GivenInfo[],
) {
   if (!sourceText || givens.length === 0) return undefined;
   const blocks = sourceBlocks(withoutComments(sourceText));
   if (!blocks.has(sourceName)) return undefined;

   const readByAnySource = new Set<string>();
   for (const block of blocks.values()) {
      for (const g of givensReadBy(block, givens)) readByAnySource.add(g.name);
   }
   // Through every source the model declares, exported or not: a given an
   // unexported base reads is still one this source's queries need.
   const reached = new Set<string>();
   const visit = (name: string) => {
      if (reached.has(name)) return;
      reached.add(name);
      const block = blocks.get(name);
      if (!block) return;
      for (const upstream of upstreamSources(block, name, blocks))
         visit(upstream);
   };
   visit(sourceName);
   const readHere = new Set<string>();
   for (const name of reached) {
      for (const g of givensReadBy(blocks.get(name), givens))
         readHere.add(g.name);
   }
   return { blocks, reached, readHere, readByAnySource };
}

/** A top-level statement's first line: a declaration keyword or an annotation at column 0. */
const STATEMENT_START = /^(?:source|query|run|given|import)\b|^#/;

/** Source name → its declaration's text, from `source: NAME is` to the next top-level statement. Line-based, so it reads raw text as well as text with its comments blanked. */
function sourceBlocks(text: string): Map<string, string> {
   const blocks = new Map<string, string>();
   const lines = text.split("\n");
   let current: { name: string; lines: string[] } | undefined;
   const close = () => {
      if (current) blocks.set(current.name, current.lines.join("\n"));
      current = undefined;
   };
   for (const line of lines) {
      const head = /^source\s*:\s*`?([A-Za-z_]\w*)`?\s+is\b/.exec(line);
      if (head) {
         close();
         current = { name: head[1], lines: [line] };
      } else if (STATEMENT_START.test(line)) {
         close();
      } else if (current) {
         current.lines.push(line);
      }
   }
   close();
   return blocks;
}

/** The model's own sources a declaration builds on: its base, and its joins' targets. */
function upstreamSources(
   block: string,
   name: string,
   declared: ReadonlyMap<string, string>,
): string[] {
   const found: string[] = [];
   const base = new RegExp(
      `source\\s*:\\s*\`?${escapeRegExp(name)}\`?\\s+is\\s+\`?([A-Za-z_]\\w*)\`?\\s*(?:->|extend\\b|$)`,
      "m",
   ).exec(block);
   if (base) found.push(base[1]);
   const join =
      /join_\w+\s*:\s*`?([A-Za-z_]\w*)`?(?:\s+is\s+`?([A-Za-z_]\w*)`?)?/g;
   for (let m = join.exec(block); m; m = join.exec(block))
      found.push(m[2] ?? m[1]);
   return found.filter((s) => s !== name && declared.has(s));
}

/** The text with `//` and `--` comments blanked, so a mention there is not a use. */
function withoutComments(text: string): string {
   return text.replace(/(?:\/\/|--).*$/gm, "");
}

function escapeRegExp(value: string): string {
   return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
