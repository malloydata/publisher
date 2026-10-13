// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * PROVISIONAL (`retrieval.fcj.alsoLikely`): fields returned beside the judge's
 * picks, marked "also likely" so an agent can tell them apart. Two rules,
 * read only from what the request already has (no model call):
 *
 *   named  for each field name the judge wrote in a reason or a caveat that
 *          no pick carries, the best-ranked candidate of that name the judge
 *          was shown;
 *   twin   for each pick, up to `twins` fields with the same name and kind on
 *          other sources, best Stage 1 rank first, from every target's
 *          Stage 1 list (top perTarget) and the judge's input.
 *
 * The rule is fixed, so its effect can be checked against the same rule
 * applied to a saved trace:
 *
 * - "Rank" is a candidate's best rank in any Stage 1 list or in the judge's
 *   list (an extra, which has no rank, ranks 999); ties go to the id.
 * - Names and kinds are read from wire ids: the name is the id's last
 *   segment after any join path (`dimension:a:b.region` is `region`).
 * - "The same field" is read as the trace reads it: a field's own id, its
 *   routes, and its folded copies are one field. A
 *   twin is never the pick itself under another id; an added field is never
 *   one the response already has under any of its ids, and neither is a
 *   source that already has a card.
 * - Named fields come first, then twins, pick by pick in pick order.
 */

import type { CatalogNode } from "./catalog";
import type { ConnectResult } from "./connect";
import type { FindResult } from "./find";
import type { FcjIndex } from "./index_build";
import type { JudgeVerdict } from "./judge";
import { fieldKeyOf, parseRouteKey, routesOf } from "./routes";

/** The rank an extra (a structural addition with no rank) sorts at. */
const EXTRA_RANK = 999;

export interface AlsoLikelyField {
   /** Catalog key of the field. */
   key: string;
   /** Its wire id. */
   id: string;
   rule: "named" | "twin" | "date_range";
   /** For a twin: the id of the pick it shares a name with. For a date_range field: the measure it was added for. */
   of?: string;
   /** The sentence the response carries. */
   reason: string;
}

/** The name an id carries, past its kind, source and any join path. */
export function nameOfId(id: string): string {
   const last = id.split(":").at(-1) ?? id;
   return last.split(".").at(-1) ?? last;
}

function kindOfId(id: string): string {
   return id.split(":")[0];
}

function escapeRegExp(text: string): string {
   return text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/** Whether `words` (lowercase) names `name`, as written or with spaces for underscores. */
export function mentions(words: string, name: string): boolean {
   const lower = name.toLowerCase();
   const forms = [escapeRegExp(lower)];
   if (lower.includes("_")) forms.push(escapeRegExp(lower.replace(/_/g, " ")));
   return new RegExp(`(?<![a-z0-9_])(${forms.join("|")})(?![a-z0-9_])`).test(
      words,
   );
}

export function alsoLikelyFields(args: {
   index: FcjIndex;
   found: readonly FindResult[];
   connected: ConnectResult;
   verdicts: readonly JudgeVerdict[];
   /** Most twins per pick (`retrieval.fcj.alsoLikelyTwins`). */
   twins: number;
   /**
    * With `splitBundles`: a caveat or reason sentence that says something is
    * missing names no field to add ("no buyer domain field" must not add a
    * domain field of another entity).
    */
   ignoreAbsence?: boolean;
}): AlsoLikelyField[] {
   const { index, found, connected, verdicts } = args;
   const nodes = index.catalog.nodes;
   const keyOfId = new Map<string, string>();
   const best = new Map<string, number>();
   const same = new Map<string, Set<string>>();
   const inInput = new Set<string>();
   const see = (node: CatalogNode, rank: number, ids: string[]) => {
      if (!keyOfId.has(node.id)) keyOfId.set(node.id, node.key);
      best.set(node.id, Math.min(best.get(node.id) ?? EXTRA_RANK, rank));
      const all = new Set(ids);
      for (const i of all) {
         const set = same.get(i) ?? new Set<string>();
         for (const j of all) set.add(j);
         same.set(i, set);
      }
   };
   // Stage 1's lists, as the find trace shows them.
   for (const r of found) {
      r.candidates.forEach((c, i) => {
         const node = nodes.get(c.key);
         if (node)
            see(node, i + 1, [node.id, ...routesOf(index.catalog, node)]);
      });
   }
   // The judge's input, as the connect trace shows it.
   const connectIds = (node: CatalogNode) => {
      const rep = node.copyOf ? nodes.get(node.copyOf) : undefined;
      return [
         node.id,
         ...routesOf(index.catalog, node),
         ...(rep ? [rep.id, ...routesOf(index.catalog, rep)] : []),
      ];
   };
   for (const cluster of connected.clusters) {
      for (const list of cluster.candidates.values()) {
         for (const c of list) {
            const node = nodes.get(c.key);
            if (!node) continue;
            see(node, c.rank, connectIds(node));
            inInput.add(node.id);
         }
      }
      for (const e of cluster.extras) {
         const node = nodes.get(e.key);
         if (!node) continue;
         see(node, EXTRA_RANK, connectIds(node));
         inInput.add(node.id);
      }
   }
   const pool = [...best.entries()]
      .sort((a, b) => a[1] - b[1] || (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0))
      .map(([id]) => id);

   const pickIds: string[] = [];
   for (const v of verdicts) {
      for (const p of v.picks) {
         for (const k of p.keys) {
            // A picked route is a pick of its field: the field's own id is
            // then never added beside it (it is the other population).
            const id = nodes.get(fieldKeyOf(k))?.id;
            if (id && !pickIds.includes(id)) pickIds.push(id);
         }
      }
   }
   if (pickIds.length === 0) return [];
   const words = verdicts
      .flatMap((v) => [...v.caveats, ...v.picks.map((p) => p.reason ?? "")])
      .flatMap((text) =>
         args.ignoreAbsence
            ? sentencesOf(text).filter((s) => !isAbsenceSentence(s))
            : [text],
      )
      .join(" ")
      .toLowerCase();

   const additions: Array<Omit<AlsoLikelyField, "key">> = [];
   // Named: one field per name the judge wrote that no pick carries.
   const pickNames = new Set(pickIds.map(nameOfId));
   const named = new Set<string>();
   for (const id of pool) {
      const name = nameOfId(id);
      if (pickNames.has(name) || name.length < 2 || named.has(name)) continue;
      if (inInput.has(id) && mentions(words, name)) {
         named.add(name);
         additions.push({
            id,
            rule: "named",
            reason: `The judge's note names ${name}, and no pick carries that name.`,
         });
      }
   }
   // Twins: up to `twins` same-named fields of the same kind per pick.
   const pickFields = new Set<string>();
   for (const id of pickIds) {
      pickFields.add(id);
      for (const j of same.get(id) ?? []) pickFields.add(j);
   }
   if (args.twins > 0) {
      for (const pick of pickIds) {
         if (kindOfId(pick) === "source") continue;
         const name = nameOfId(pick);
         const kind = kindOfId(pick);
         const twins = pool
            .filter(
               (id) =>
                  nameOfId(id) === name &&
                  kindOfId(id) === kind &&
                  !pickFields.has(id),
            )
            .slice(0, args.twins);
         const pickNode = nodes.get(keyOfId.get(pick) ?? "");
         const label = pickNode ? `${pickNode.source}.${pickNode.name}` : pick;
         for (const id of twins) {
            additions.push({
               id,
               rule: "twin",
               of: pick,
               reason: `Same name and kind as the pick ${label}, on another source.`,
            });
         }
      }
   }

   // What the response already holds: every pick under any of its ids, and
   // the card each pick is returned on. That is the field's own source for a
   // field, and the joining source for a picked route (respond puts a route on
   // that card, and returns no card for the field's own source).
   const have = new Set<string>();
   const hold = (id: string) => {
      have.add(id);
      for (const j of same.get(id) ?? []) have.add(j);
   };
   for (const id of pickIds) hold(id);
   for (const v of verdicts) {
      for (const p of v.picks) {
         for (const k of p.keys) {
            const card = parseRouteKey(k)?.from ?? nodes.get(k)?.sourceKey;
            const source = card ? index.catalog.sources.get(card) : undefined;
            if (source) hold(source.node.id);
         }
      }
   }
   for (const cluster of connected.clusters) {
      if (cluster.peers) continue;
      const home = index.catalog.sources.get(cluster.home);
      if (home) hold(home.node.id);
   }
   const out: AlsoLikelyField[] = [];
   const tried = new Set<string>();
   for (const a of additions) {
      if (tried.has(a.id)) continue;
      tried.add(a.id);
      if (have.has(a.id)) continue;
      const key = keyOfId.get(a.id);
      if (!key) continue;
      out.push({ key, ...a });
      hold(a.id);
   }
   return out;
}

/** A note's sentences, cut at a full stop, question mark or semicolon. */
export function sentencesOf(text: string): string[] {
   return text
      .split(/(?<=[.;?!])\s+/)
      .map((s) => s.trim())
      .filter(Boolean);
}

/** Whether a sentence says something is missing or was not found. */
export function isAbsenceSentence(sentence: string): boolean {
   return /\b(not (available|found|present|included|provided|listed|exist\w*|contain\w*|have|has|include\w*)|no (\w+ ){0,3}(field|fields|data|column|columns|dimension|dimensions|measure|measures|name|names|domain)\b|does not|do not|doesn't|don't|cannot|can't|unavailable|missing|absent|lacks?|only an? (id|identifier))/i.test(
      sentence,
   );
}
