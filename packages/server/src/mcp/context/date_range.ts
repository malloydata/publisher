// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * PROVISIONAL (`retrieval.fcj.dateRange`): the "date or time range" rule.
 *
 * When the request text names a date or time range (a year, a month name, a
 * quarter, "last", "since", "this", "past", "daily", "weekly", "YTD" and the
 * like), then for each source a returned measure sits on, up to two of that
 * source's date or time fields are added after the judge, unless a date or
 * time field of that source already came back.
 *
 * Why: a whole question ("What was our revenue in 2025?") is searched as one
 * text per target type, and its date field is often not in the find list at
 * all, so the judge cannot pick it. The query needs that date to filter on.
 * No model call; read only from the judge's picks and the "also likely"
 * fields. Each added field carries its reason, and the trace lists it under
 * `date_range`, so its effect can be told apart from the judge's.
 *
 * "Sits on" means the card the measure is returned on: its own source for a
 * field, the joining source for a picked route (the source the agent queries
 * from).
 */

import type { AlsoLikelyField } from "./also_likely";
import type { CatalogNode } from "./catalog";
import type { FcjIndex } from "./index_build";
import type { JudgeVerdict } from "./judge";
import { parseRouteKey } from "./routes";

/** Most date or time fields added per source. */
export const DATE_RANGE_FIELDS_PER_SOURCE = 2;

const MONTHS =
   "january|february|march|april|june|july|august|september|october|november|december";

/**
 * Whether a text names a date or time range: a year, a month name, a quarter,
 * or a word such as "last", "since", "this", "past", "daily", "weekly",
 * "YTD". "May" counts only beside a number or after "in" ("in May", "May
 * 2025"), since it is also a verb.
 */
export function namesDateOrTimeRange(text: string): boolean {
   const t = text.toLowerCase();
   return (
      /\b(19|20)\d{2}\b/.test(t) ||
      new RegExp(`\\b(${MONTHS})\\b`).test(t) ||
      /\b(in may|may \d{1,4})\b/.test(t) ||
      /\b(q[1-4]|quarters?|quarterly|last|since|this|past|previous|prior|daily|weekly|monthly|yearly|annually|ytd|mtd|qtd|year[- ]to[- ]date|month[- ]to[- ]date|yesterday|today|year[- ]over[- ]year|yoy|month[- ]over[- ]month)\b/.test(
         t,
      )
   );
}

const DATE_TYPES = new Set(["date", "timestamp", "timestamptz"]);

/**
 * A field name that ends as a date or a time grain: `order_date`,
 * `opened_month`, `performance_year`, `hour_time`. Not `_at`: `arrives_at`
 * can be a place. A `created_at` timestamp is caught by its type.
 */
const DATE_NAME = /(^|_)(date|month|week|quarter|year|day|time|period)$/i;

/** Names ending in `_date` or `_month` (or just `date`, `month`): the first choice. */
const PLAIN_DATE_NAME = /(^|_)(date|month)$/i;

/** Whether a node is a date or time field: a dimension of a date type, or one whose name reads as a date. */
export function isDateOrTimeField(node: CatalogNode): boolean {
   return (
      node.kind === "dimension" &&
      (DATE_TYPES.has(node.dataType ?? "") || DATE_NAME.test(node.name))
   );
}

/** The card source and node of a pick or "also likely" key. */
function placedOf(
   index: FcjIndex,
   key: string,
): { card: string; node: CatalogNode } | undefined {
   const route = parseRouteKey(key);
   const node = index.catalog.nodes.get(route?.node ?? key);
   if (!node) return undefined;
   return { card: route?.from ?? node.sourceKey, node };
}

/**
 * The date or time fields the rule adds, in the order measures were returned.
 * Empty when no caller target's text names a date or time range.
 */
export function dateRangeFields(args: {
   index: FcjIndex;
   /** The caller's target texts (a split list's items are not separate). */
   texts: readonly string[];
   verdicts: readonly JudgeVerdict[];
   alsoLikely: readonly AlsoLikelyField[];
}): AlsoLikelyField[] {
   const { index } = args;
   if (!args.texts.some(namesDateOrTimeRange)) return [];
   const returned = [
      ...args.verdicts.flatMap((v) => v.picks.flatMap((p) => p.keys)),
      ...args.alsoLikely.map((a) => a.key),
   ];
   const have = new Set<string>();
   const measureSources: Array<{ card: string; measure: string }> = [];
   const datedSources = new Set<string>();
   for (const key of returned) {
      const placed = placedOf(index, key);
      if (!placed) continue;
      have.add(placed.node.key);
      if (isDateOrTimeField(placed.node)) datedSources.add(placed.card);
      if (
         placed.node.kind === "measure" &&
         !measureSources.some((m) => m.card === placed.card)
      ) {
         measureSources.push({
            card: placed.card,
            measure: `${index.catalog.sources.get(placed.card)?.name ?? placed.card}.${parseRouteKey(key) ? `${parseRouteKey(key)!.path.join(".")}.` : ""}${placed.node.name}`,
         });
      }
   }
   const out: AlsoLikelyField[] = [];
   for (const { card, measure } of measureSources) {
      if (datedSources.has(card)) continue;
      const source = index.catalog.sources.get(card);
      if (!source) continue;
      const dates = source.fields
         .map((k) => index.catalog.nodes.get(k))
         .filter(
            (n): n is CatalogNode =>
               n !== undefined &&
               !n.joinPath &&
               isDateOrTimeField(n) &&
               !have.has(n.key),
         )
         .sort(
            (a, b) =>
               Number(PLAIN_DATE_NAME.test(b.name)) -
                  Number(PLAIN_DATE_NAME.test(a.name)) ||
               Number(DATE_TYPES.has(b.dataType ?? "")) -
                  Number(DATE_TYPES.has(a.dataType ?? "")) ||
               a.name.length - b.name.length ||
               (a.key < b.key ? -1 : a.key > b.key ? 1 : 0),
         )
         .slice(0, DATE_RANGE_FIELDS_PER_SOURCE);
      for (const d of dates) {
         have.add(d.key);
         out.push({
            key: d.key,
            id: d.id,
            rule: "date_range",
            of: measure,
            reason: `Added by the date or time range rule: the request names a date or time range, and the measure ${measure} came back with no date or time field of ${source.name}.`,
         });
      }
   }
   return out;
}
