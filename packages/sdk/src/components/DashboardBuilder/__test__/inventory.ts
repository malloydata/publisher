// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * ONE ORACLE for the whole writer: an edit changes the declaration it names,
 * and nothing else.
 *
 * Every defect this directory has shipped was a WRONGLY COMPUTED SPAN, and the
 * two gates in `spliceDocument` cannot see one: the syntax check only asks
 * whether the result parses, and the read-back check compares the projection,
 * which holds neither comments nor the tags the document does not model. Each
 * miss got a hand-written spec of its own, and the next sibling of the same
 * shape went out anyway.
 *
 * So this takes an inventory of a file from a FRESH PARSE, before and after,
 * and {@link diffInventories} says what moved. Never from the writer's own
 * spans: an oracle phrased in the numbers the writer computed inherits the
 * mistake and certifies it. And never "the bytes outside the planned edits are
 * unchanged", which is vacuous -- the writer applies exactly the edits it
 * planned, so it can never fail that.
 */

import { parseMalloy, parseRefused, type Span } from "../malloyTree";

/** Everything about a declaration that an edit elsewhere must not disturb. */
export interface Declaration {
   /**
    * The `#` lines that annotate it, in order, each MODELLED one reduced to
    * its key and each other one kept as written.
    *
    * Reduced rather than dropped, and held apart from `text` rather than left
    * in it, so that one question can be asked of a retag: did the SET of tag
    * lines change? Rewriting `# colspan=6` to `# colspan=3` is the edit; ending
    * up with both is the defect, and it is the defect a writer produces when it
    * cannot see a tag it is about to duplicate.
    */
   tags: string[];
   /** The comment lines above it, `#` and `##` lines excluded. */
   block: string[];
   /** Its statement, tag block off the front and child declarations excised. */
   text: string;
   /** The whitespace its first line of code begins with. */
   indent: string;
}

export interface Inventory {
   /**
    * Keyed `source:NAME`, `view:SOURCE.NAME`, `dimension:SOURCE.NAME`,
    * `given:NAME`, `import:PATH`.
    */
   declarations: Record<string, Declaration>;
   /**
    * The code on a line -> the comment trailing it.
    *
    * Per STATEMENT, not per declaration, which is the whole point of it: the
    * comment that slides lives on a line INSIDE a view body
    * (`aggregate: n is count() // keep with measure` becoming
    * `where: b ~ $B // keep with measure`). Asking a declaration for the
    * comment trailing its own first line looks at `view: kpis is {`, which
    * never had one. Keyed by the code rather than by the line number, so a
    * comment that lands on a different statement is a different key.
    */
   attached: Record<string, string>;
   /** Every comment in the file, in order, both of the lexer's token kinds. */
   comments: string[];
   /**
    * The statements no declaration covers and no gate models: `run:`, `query:`,
    * a bare `sql:`. Without these a `run:` sharing a removed tile's line
    * vanishes with nothing going red.
    *
    * `#` and `##` lines are NOT here. A `#` line belongs to the declaration
    * below it and is in that declaration's `tags`; a `##` line is the model
    * annotation surface that `planOrder` and `planSettings` own and rewrite on
    * purpose, so holding it here would paint every correct reorder red.
    */
   residue: string[];
}

/**
 * The tag keys the document models, across tiles, givens and drills -- the only
 * `#` lines the writer may rewrite.
 */
const MODELLED_TAG =
   /^#\s*(colspan|break|borderless|label|subtitle|description|control|suggest|range_min|range_max|drill)\b/;

/** A modelled tag reduced to its key; anything else kept as written. */
function maskTag(trimmed: string): string {
   const key = MODELLED_TAG.exec(trimmed);
   return key ? `#${key[1]}` : trimmed;
}

/** Whitespace flattened, so formatting is not mistaken for meaning. */
const flatten = (text: string) => text.replace(/\s+/g, " ").trim();

export async function inventory(source: string): Promise<Inventory> {
   const result = await parseMalloy(source);
   if (parseRefused(result))
      throw new Error(`the oracle needs a file that parses: ${result.reason}`);
   const parsed = result.parsed;
   const lines = source.split("\n");
   const lineOf = (offset: number) =>
      parsed.lineStarts.findLastIndex((at) => at <= offset);
   const covered: Span[] = [];
   const declarations: Record<string, Declaration> = {};

   // Every line any comment touches, taken from the lexer's raw spans.
   //
   // NOT `parsed.blockStart`, which is the same heuristic `blockAbove` walks:
   // an oracle that shares a locator with the code under test can be blind in
   // exactly the way that code is, and one of the defects this file exists for
   // was a walk that could not see a comment.
   const commentLines = new Set<number>();
   for (const at of parsed.comments)
      for (let l = lineOf(at.start); l <= lineOf(at.end - 1); l++)
         commentLines.add(l);

   /** The first line of the `#`/comment block above `line`. */
   const blockStart = (line: number): number => {
      let start = line;
      for (let i = line - 1; i >= 0; i--) {
         const text = lines[i].trim();
         if (commentLines.has(i)) {
            start = i;
            continue;
         }
         if (text === "") break;
         if (text.startsWith("#")) start = i;
         else break;
      }
      return start;
   };

   const record = (key: string, statement: Span, excise: Span[] = []) => {
      covered.push(statement);
      // The prelude is walked from `blockStart` DOWNWARD to the first line of
      // code, rather than upward from the declaration: a view's statement span
      // opens at its first `#` tag while a given's opens at its name, and
      // `view:\n  kpis is ...` puts the keyword on a line of its own. One walk
      // that stops at the first line which is neither blank, a `#` line nor a
      // comment lands in the right place for all of them.
      let code = lineOf(statement.start);
      const tags: string[] = [];
      const block: string[] = [];
      for (let i = blockStart(code); i < lines.length; i++) {
         const text = lines[i].trim();
         if (text === "") continue;
         // Inside a comment, where a line beginning `#` is prose.
         if (commentLines.has(i)) {
            block.push(text);
            continue;
         }
         // `##` is a MODEL annotation. It is dropped rather than walked past
         // because the walk above counts one as a `#`, and `## artifact {...}`
         // sits directly above `import` in every dashboard -- keeping it would
         // make the import's prelude differ on every correct tile removal.
         if (text.startsWith("##")) continue;
         if (text.startsWith("#")) tags.push(maskTag(text));
         else {
            code = i;
            break;
         }
      }
      // A declaration that is not the first thing on its line owns no prelude:
      // Malloy lets a second `view:` share a line, and the tags above belong to
      // the one that opens it.
      const start = Math.max(statement.start, parsed.lineStarts[code]);
      const shared = source.slice(parsed.lineStarts[code], start).trim() !== "";
      let text = "";
      let at = start;
      for (const child of [...excise].sort((a, b) => a.start - b.start)) {
         text += source.slice(at, Math.max(at, child.start));
         at = Math.max(at, child.end);
      }
      text += source.slice(at, Math.max(at, statement.end));
      declarations[key] = {
         tags: shared ? [] : tags,
         block: shared ? [] : block,
         text: flatten(text),
         indent: /^[ \t]*/.exec(lines[code])?.[0] ?? "",
      };
   };

   for (const at of parsed.imports) record(`import:${at.from}`, at.statement);
   for (const given of parsed.givens)
      record(`given:${given.name}`, given.statement);
   for (const declared of parsed.sources) {
      const children = [
         ...declared.views.map((v) => v.statement),
         ...declared.dimensions.map((d) => d.statement),
      ];
      // The source keeps its key: its base, its own `where:` and its
      // `extend { ... }` braces are worth watching. Its CHILDREN come out, or
      // every filter edit and every tile removal would report the enclosing
      // source as changed as well.
      record(`source:${declared.name}`, declared.statement, children);
      for (const view of declared.views)
         record(`view:${declared.name}.${view.name}`, view.statement);
      for (const dimension of declared.dimensions)
         record(
            `dimension:${declared.name}.${dimension.name}`,
            dimension.statement,
         );
   }

   const attached: Record<string, string> = {};
   for (const at of parsed.comments) {
      const before = source
         .slice(parsed.lineStarts[lineOf(at.start)], at.start)
         .trim();
      if (before === "") continue;
      let key = before;
      for (let n = 2; key in attached; n++) key = `${before} #${n}`;
      attached[key] = source.slice(at.start, at.end).trim();
   }

   const residue: string[] = [];
   lines.forEach((raw, i) => {
      const text = raw.trim();
      if (text === "" || text.startsWith("#") || commentLines.has(i)) return;
      const start = parsed.lineStarts[i];
      const end = start + raw.length;
      if (covered.some((at) => at.start < end && at.end > start)) return;
      residue.push(text);
   });

   return {
      declarations,
      attached,
      comments: parsed.comments.map((at) =>
         source.slice(at.start, at.end).trim(),
      ),
      residue,
   };
}

/**
 * What moved between two inventories, one list per field so a failure names the
 * thing that broke rather than dumping two files. `+` appeared, `-` vanished,
 * `~` changed.
 */
export interface InventoryDiff {
   declarations: string[];
   tags: string[];
   block: string[];
   indent: string[];
   attached: string[];
   comments: string[];
   residue: string[];
}

/** What every field must be when an edit disturbed nothing it did not name. */
export const NOTHING_MOVED: InventoryDiff = {
   declarations: [],
   tags: [],
   block: [],
   indent: [],
   attached: [],
   comments: [],
   residue: [],
};

function keyDiff<T>(
   before: Record<string, T>,
   after: Record<string, T>,
   same: (a: T, b: T) => boolean,
): string[] {
   const out: string[] = [];
   for (const key of Object.keys(before))
      if (!(key in after)) out.push(`-${key}`);
      else if (!same(before[key], after[key])) out.push(`~${key}`);
   for (const key of Object.keys(after))
      if (!(key in before)) out.push(`+${key}`);
   return out.sort();
}

/** A multiset difference, so two identical comments are two entries. */
function listDiff(before: string[], after: string[]): string[] {
   const remaining = [...after];
   const out: string[] = [];
   for (const entry of before) {
      const at = remaining.indexOf(entry);
      if (at < 0) out.push(`-${entry}`);
      else remaining.splice(at, 1);
   }
   for (const entry of remaining) out.push(`+${entry}`);
   // The same multiset in a different order: one comment moved past another,
   // which changes which code each of them sits beside.
   if (out.length === 0 && before.join("\v") !== after.join("\v"))
      out.push("~order");
   return out.sort();
}

export function diffInventories(
   before: Inventory,
   after: Inventory,
): InventoryDiff {
   const both = (key: string) =>
      key in before.declarations && key in after.declarations;
   const changed = (read: (d: Declaration) => string) =>
      Object.keys(before.declarations)
         .filter(
            (key) =>
               both(key) &&
               read(before.declarations[key]) !== read(after.declarations[key]),
         )
         .map((key) => `~${key}`)
         .sort();
   return {
      declarations: keyDiff(
         before.declarations,
         after.declarations,
         (a, b) => a.text === b.text,
      ),
      tags: changed((d) => d.tags.join("\v")),
      block: changed((d) => d.block.join("\v")),
      indent: changed((d) => d.indent),
      attached: keyDiff(before.attached, after.attached, (a, b) => a === b),
      comments: listDiff(before.comments, after.comments),
      residue: listDiff(before.residue, after.residue),
   };
}

/** The diff between two texts, which is how every sweep here asks. */
export async function whatMoved(
   before: string,
   after: string,
): Promise<InventoryDiff> {
   return diffInventories(await inventory(before), await inventory(after));
}
