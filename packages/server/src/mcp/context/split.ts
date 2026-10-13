// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * PROVISIONAL (`retrieval.fcj.splitBundles`): the items of a search_text that
 * is a list. Agents exploring a package often write one target for several
 * fields ("venue state, city, address, zip code or market"). One embedding
 * of the whole list lands between its items, so a field that matches one
 * item well can rank below fields that match the blend loosely, and the
 * judge never sees it. Each item is searched on its own as well; the whole
 * phrase is always searched too (engine.ts), so a wrong split adds
 * candidates but cannot remove one.
 *
 * The split is deterministic, no model call:
 *
 * 1. A question (a "?", or a first word such as "what", "how", "between",
 *    "for") is never a list, nor is a text with an item over MAX_ITEM_WORDS
 *    words: its commas set off clauses. Otherwise a phrase is a list only
 *    when it has a comma or a semicolon outside
 *    parentheses and outside an example tail ("such as ...", "e.g. ..."),
 *    or three items set apart by spaced slashes ("daypart / hour / day").
 *    "first and last name", "profit and loss", "buy/sell" are never split.
 * 2. Items are cut at those commas and semicolons. Inside an item, " and ",
 *    " or ", "&" and "/" cut further, but only when every part is three
 *    words or fewer and the joined words are not a phrase the package itself
 *    uses (a field name or doc that says "shipping and handling").
 *    "first and last name" becomes "first name", "last name".
 * 3. An item that starts with a modifier ("after discount", "not test",
 *    "all sizes") is not an item: it stays on the item before it. A
 *    description followed by its example values ("kind of account a
 *    merchant holds, retail or SaaS") is not split.
 * 4. The shared subject is carried onto every item: a prefix before a colon
 *    ("guest details: name, phone"), a trailing "of the ..." or "for the ..."
 *    on the last item ("state, city and zip of the venue"), or the leading
 *    word of a first item of two or three words when most other items are
 *    one word ("venue state, city, address").
 *
 * At most MAX_ITEMS items are returned; a list longer than that is still
 * searched whole.
 */

/** Most items one target is split into. */
export const MAX_ITEMS = 8;

/** Most words in one part of an item cut at "and" / "or" / "/". */
const MAX_PART_WORDS = 3;

/** Most words in one item of a list; a longer item means the text is a sentence, not a list. */
export const MAX_ITEM_WORDS = 8;

/** First words of a question or an instruction: a sentence whose commas set off clauses, not items. */
const SENTENCE_START =
   /^(what|which|who|whom|whose|how|when|where|why|is|are|was|were|do|does|did|can|could|should|would|will|has|have|had|between|for|in|across|during|since|from|excluding|including|among|of|on|at|by|after|before|if|given|assuming|compared)\b/i;

const EXAMPLE_TAIL =
   /\b(such as|e\.g\.|eg\.|for example|for instance|including|like)\b/i;

/** Words that start a qualifier of the item before, not an item of their own. */
const MODIFIER_START =
   /^(after|before|per|excluding|excl\.?|including|incl\.?|with|without|net of|less|minus|plus|within|based on|not|no|all|only|except|other than)\b/i;

/** A trailing shared subject: "of the venue", "for each customer". */
const SUFFIX = /^(.+?)\s+((?:of|for) (?:the|a|an|each|every|this|that) .+)$/i;

/** Leading words of a colon prefix that carry no subject. */
const FILLER =
   /^(?:(?:anything|everything|something|any|all|fields?|data|info(?:rmation)?|details?)\s+)?(?:(?:about|related to|on|for|of|regarding)\s+)?/i;

/** First words never carried onto the other items. */
const NOT_A_SUBJECT = new Set([
   "the",
   "a",
   "an",
   "any",
   "all",
   "each",
   "every",
   "total",
   "number",
   "count",
   "average",
   "avg",
   "sum",
   "top",
   "monthly",
   "daily",
   "weekly",
   "yearly",
   "annual",
   "net",
   "gross",
   "new",
   "old",
   "first",
   "last",
]);

function words(s: string): string[] {
   return s.trim().split(/\s+/).filter(Boolean);
}

/**
 * The items of `text`, each with its shared subject, or [] when it is not a
 * list (fewer than two items). `knownPhrase` says whether the package uses a
 * phrase as written ("shipping and handling" in a field name or doc); such a
 * phrase is never cut.
 */
export function splitBundle(
   text: string,
   knownPhrase: (phrase: string) => boolean = () => false,
): string[] {
   let body = text.trim().replace(/\s+/g, " ");
   // A question or a sentence is not a list: its commas set off clauses.
   if (body.includes("?") || SENTENCE_START.test(body)) return [];
   // Three or more items set apart by spaced slashes are a list too.
   if (!/[,;]/.test(body) && (body.match(/ \/ /g) ?? []).length >= 2) {
      body = body.replace(/ \/ /g, ", ");
   }
   if (!/[,;]/.test(body)) return [];

   // Protect parentheses and an example tail from every cut.
   const kept: string[] = [];
   const protect = (s: string) => {
      kept.push(s);
      return `\u0000${kept.length - 1}\u0000`;
   };
   body = body.replace(/\([^()]*\)/g, protect);
   const tail = EXAMPLE_TAIL.exec(body);
   if (tail) body = body.slice(0, tail.index) + protect(body.slice(tail.index));
   const restore = (s: string) =>
      s.replace(/\u0000(\d+)\u0000/g, (_, i) => kept[Number(i)]);
   if (!/[,;]/.test(body)) return [];

   // A prefix before a colon is the subject of every item.
   let prefix = "";
   const colon = body.indexOf(":");
   const firstComma = body.search(/[,;]/);
   if (colon > 0 && colon < firstComma) {
      prefix = body.slice(0, colon).trim().replace(FILLER, "").trim();
      body = body.slice(colon + 1).trim();
   }

   // Items at commas and semicolons; a modifier stays on the item before it.
   const items: string[] = [];
   for (const raw of body.split(/\s*[,;]\s*/)) {
      const item = raw.replace(/^(?:and|or|&)\s+/i, "").trim();
      if (!item) continue;
      if (items.length > 0 && MODIFIER_START.test(restore(item))) {
         items[items.length - 1] += `, ${item}`;
         continue;
      }
      items.push(item);
   }
   if (items.length === 0) return [];
   if (items.some((i) => words(restore(i)).length > MAX_ITEM_WORDS)) return [];
   // "kind of account a merchant holds, retail or SaaS": a description, then
   // its example values, is one field.
   if (
      items.length === 2 &&
      words(items[0]).length >= 4 &&
      words(items[1]).length <= 4 &&
      /\s(or|and)\s|\//i.test(items[1])
   ) {
      return [];
   }

   // A trailing "of the ..." on the last item belongs to every item, unless
   // an earlier item has its own.
   let suffix = "";
   const last = SUFFIX.exec(items[items.length - 1]);
   if (last && !items.slice(0, -1).some((i) => / (of|for) /i.test(i))) {
      items[items.length - 1] = last[1];
      suffix = last[2];
   }

   // "and" / "or" / "/" inside an item, when every part is short.
   const parts: string[] = [];
   for (const item of items) {
      const cut = item
         .split(/\s+(?:and|or|&)\s+|\s*\/\s*/i)
         .map((p) => p.trim())
         .filter(Boolean);
      if (
         cut.length < 2 ||
         cut.some((p) => words(p).length > MAX_PART_WORDS) ||
         knownPhrase(restore(item))
      ) {
         parts.push(item);
         continue;
      }
      // "first and last name": a one-word part shares the last part's head.
      const head = words(cut[cut.length - 1]).at(-1)!;
      const multi = words(cut[cut.length - 1]).length > 1;
      for (const p of cut) {
         parts.push(multi && words(p).length === 1 ? `${p} ${head}` : p);
      }
   }

   // The first item's leading word, when most other items are one word and
   // none of them has the first item's head noun.
   let lead = "";
   const first = words(parts[0]);
   if (!prefix && first.length >= 2 && first.length <= 3 && parts.length > 1) {
      const mod = first[0];
      const headNoun = first.at(-1)!.toLowerCase();
      const others = parts.slice(1);
      const single = others.filter((p) => words(p).length === 1).length;
      const sharesHead = others.some(
         (p) =>
            words(p).length > 1 && words(p).at(-1)!.toLowerCase() === headNoun,
      );
      if (
         !NOT_A_SUBJECT.has(mod.toLowerCase()) &&
         !sharesHead &&
         single * 2 >= others.length
      ) {
         lead = mod;
      }
   }

   const out: string[] = [];
   parts.forEach((p, i) => {
      const startsWithLead =
         lead && p.toLowerCase().startsWith(`${lead.toLowerCase()} `);
      const withLead = i > 0 && lead && !startsWithLead ? `${lead} ${p}` : p;
      const piece = restore(
         [prefix, withLead, suffix].filter((s) => s !== "").join(" "),
      );
      if (!out.includes(piece)) out.push(piece);
   });
   return out.length >= 2 ? out.slice(0, MAX_ITEMS) : [];
}

/**
 * Whether the package uses `phrase` as written, in a field or source name
 * (underscores read as spaces) or a doc: "shipping and handling" in a doc
 * means a field is about that one thing. Built once per catalog.
 */
export function phraseCheckerOf(texts: Iterable<string>) {
   const all = [...texts]
      .map((t) => t.toLowerCase().replace(/_/g, " ").replace(/\s+/g, " "))
      .join("\n");
   return (phrase: string): boolean => {
      const p = phrase.toLowerCase().replace(/\s+/g, " ").trim();
      if (!/\s(and|or|&)\s|\//.test(p)) return false;
      return all.includes(p);
   };
}
