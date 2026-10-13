// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Stage 3, judge: one chat call per cluster (one call in all when there is one
 * cluster) picks, for each target, which of the cluster's candidates to
 * return, and names the caveats it applied. The same chat model the shipped
 * refine step uses.
 *
 * The call sees the targets verbatim and in order, whether this cluster
 * covers each, and for each target the cluster's candidates (and the
 * structural extras) with their kind, type, source, route and full `#(doc)`,
 * rules included. Docs are model text, sent today by refine under every
 * egress preset. A stored value is data: under any preset below `full` the
 * judge is told that a candidate matched the phrase's value and how, never
 * the stored string; authored expressions are not sent at all.
 *
 * In a list with `listRoutes` above 0, each candidate field is followed by up
 * to that many routes to it from the other listed sources, shortest first,
 * each a numbered option of its own. A picked route is the joining source's
 * rows, and respond puts it on that source's card under the route's id. Each
 * route line, and each joined copy's, quotes the first sentence of the
 * joining source's doc (and a joined copy's join doc), so the judge can see
 * which rows the route drops.
 *
 * A target with no pick in any cluster is returned as unresolved by name,
 * which is the agent's signal to ask or to search again.
 */

import {
   DEFAULT_JUDGE_INSTRUCTIONS,
   DEFAULT_JUDGE_LIST_INSTRUCTIONS,
   DEFAULT_JUDGE_LIST_ROUTES_INSTRUCTIONS,
   JUDGE_REPLY_FORMAT,
   JUDGE_SPLIT_RULES,
} from "../../prompts/judge";
import type { ChatModel } from "../../providers/types";
import { getEgressPreset } from "../../retrieval_config";
import { humanizeName } from "../tools/embedding_index";
import { runPooled } from "../tools/get_context_llm";
import type { CatalogNode } from "./catalog";
import type { Cluster } from "./connect";
import { firstSentence, joinNodeOf } from "./derived_text";
import {
   parseRouteKey,
   routeKey,
   routeOptions,
   routesFrom,
   type RouteOption,
} from "./routes";
import type { FindTarget } from "./find";
import type { FcjIndex } from "./index_build";

/** One numbered line the judge may pick. */
export interface JudgeOption {
   number: number;
   key: string;
   /** Targets it is listed under; an extra is listed under none. */
   targets: number[];
   extra?: string;
   /** A route to a listed field from another listed source; `key` is its routeKey. */
   route?: RouteOption;
}

export interface JudgePick {
   target: number;
   keys: string[];
   reason?: string;
   /** Route keys the judge picked and `dropRoutesAt` took out, for the trace. */
   droppedRoutes?: string[];
}

export interface JudgeVerdict {
   cluster: string;
   picks: JudgePick[];
   caveats: string[];
   /** Route options the judge was shown (`listRoutes`), for the trace. */
   routeOptions?: number;
}

/** Doc text the judge reads for one candidate; long docs are cut here, not dropped. */
const JUDGE_DOC_MAX_CHARS = 1_500;

/** Most join paths shown between two peers, and most routes shown per field. */
export const PATHS_SHOWN = 5;
export const ROUTES_SHOWN = 8;

/**
 * In routes mode (`joinedCopies: "routes"`), how the other listed sources
 * reach a field: `reachable from orders through customer.region or from
 * shipments through order.customer.region`, shortest first.
 */
export function routesLine(
   index: FcjIndex,
   node: CatalogNode,
   listed: ReadonlySet<string>,
): string | undefined {
   const routes = routesFrom(index.catalog, node, listed);
   if (routes.length === 0) return undefined;
   const nameOf = (s: string) => index.catalog.sources.get(s)?.name ?? s;
   const shown = routes
      .slice(0, ROUTES_SHOWN)
      .map((r) => `from ${nameOf(r.from)} through ${r.path.join(".")}`);
   const more = routes.length - shown.length;
   return `reachable ${shown.join(" or ")}${more > 0 ? ` (and ${more} more routes)` : ""}`;
}

/**
 * How a field is reached: from the home in a cluster. In a list, which has
 * no home, nothing; in routes mode, the other listed sources' routes to it.
 */
function routeOf(
   index: FcjIndex,
   cluster: Cluster,
   node: CatalogNode,
   routeCap: number,
): string | undefined {
   if (cluster.peers) {
      // With route options, a route is a numbered line of its own; a joined
      // copy (a field of a join whose source is not served, or a raw table)
      // is a route already, and says so.
      if (routeCap > 0) {
         if (!node.joinPath) return undefined;
         const rel = node.relationship ? `, join_${node.relationship}` : "";
         const notes = [
            holdsNote(index, node.sourceKey),
            joinNote(index, node),
         ].filter((n): n is string => n !== undefined);
         return `a route from ${node.source} through ${node.joinPath}${rel}: only the rows ${node.source} reaches${notes.length > 0 ? ` (${notes.join("; ")})` : ""}`;
      }
      if (index.catalog.joinedCopies !== "routes") return undefined;
      return routesLine(
         index,
         node,
         new Set(cluster.peers.map((p) => p.source)),
      );
   }
   const entry = cluster.reach.get(node.sourceKey);
   if (!entry || entry.hops === 0) return "own field";
   const home = index.catalog.sources.get(cluster.home)?.name ?? cluster.home;
   const paths = entry.paths
      .filter((p) => p.length > 0)
      .map((p) => `${home}.${p.join(".")}`);
   return `through ${paths.join(" or ")} (a left join from ${home})`;
}

/**
 * One route option, under its field: `[5] order_items.products.product_count
 * (route from order_items through products, join_one): only the products rows
 * order_items reaches`.
 */
function routeOptionLine(index: FcjIndex, option: JudgeOption): string {
   const r = option.route!;
   const node = index.catalog.nodes.get(r.node)!;
   const from = index.catalog.sources.get(r.from)?.name ?? r.from;
   const holds = holdsNote(index, r.from);
   return `[${option.number}] ${from}.${r.path.join(".")}.${node.name} (route from ${from} through ${r.path.join(".")}, join_${r.fanout}): only the ${node.source} rows ${from} reaches${holds ? ` (${holds})` : ""}`;
}

/** Longest quoted doc sentence a route line carries. */
const ROUTE_NOTE_MAX_CHARS = 200;

function quoted(text: string): string {
   const s = firstSentence(text);
   return JSON.stringify(
      s.length > ROUTE_NOTE_MAX_CHARS
         ? `${s.slice(0, ROUTE_NOTE_MAX_CHARS)}…`
         : s,
   );
}

/**
 * What a route's joining source holds, in its author's words: the first
 * sentence of its own doc (`orders: "One row per order."`). A summary is not
 * used, because it is written for search and can drop which rows are in.
 * Undefined when the source has no doc.
 */
function holdsNote(index: FcjIndex, sourceKey: string): string | undefined {
   const source = index.catalog.sources.get(sourceKey);
   const doc = source?.node.doc.trim();
   return source && doc ? `${source.name}: ${quoted(doc)}` : undefined;
}

/**
 * For a field of a join whose source is not listed (a joined copy): the
 * first sentence of the join's own doc, the only text that says what the
 * joined rows are. Undefined for a raw-table join or a join with no doc.
 */
function joinNote(index: FcjIndex, node: CatalogNode): string | undefined {
   const join = joinNodeOf(index.catalog, node);
   const doc = join?.doc.trim();
   return join && doc ? `join ${node.joinPath}: ${quoted(doc)}` : undefined;
}

function optionLine(
   index: FcjIndex,
   cluster: Cluster,
   option: JudgeOption,
   valueNote: string | undefined,
   routeCap = 0,
): string {
   const node = index.catalog.nodes.get(option.key)!;
   const kind = node.dataType ? `${node.kind}, ${node.dataType}` : node.kind;
   const doc = node.doc.replace(/\s+/g, " ").trim();
   const cut =
      doc.length > JUDGE_DOC_MAX_CHARS
         ? `${doc.slice(0, JUDGE_DOC_MAX_CHARS)}…`
         : doc;
   const route = routeOf(index, cluster, node, routeCap);
   const parts = [
      `[${option.number}] ${node.name} (${kind}; source ${node.source}${route ? `; ${route}` : ""})`,
      cut
         ? `: ${cut}`
         : `: (no doc; name in words: ${humanizeName(node.name)})`,
   ];
   if (valueNote) parts.push(` | ${valueNote}`);
   if (option.extra) parts.push(` | added because: ${option.extra}`);
   return parts.join("");
}

/** Number the cluster's candidates and extras once, so a node listed under two targets has one number. */
export function judgeOptions(
   cluster: Cluster,
   targets: readonly FindTarget[],
   /**
    * With `connect: "list"`: number each field's `cap` shortest routes from
    * the other listed sources right after the field (`listRoutes`).
    */
   routes?: { index: FcjIndex; cap: number },
): JudgeOption[] {
   const byKey = new Map<string, JudgeOption>();
   let n = 0;
   const cap = cluster.peers && routes ? routes.cap : 0;
   const peers = new Set((cluster.peers ?? []).map((p) => p.source));
   for (const t of targets) {
      for (const c of cluster.candidates.get(t.index) ?? []) {
         const prev = byKey.get(c.key);
         if (prev) {
            prev.targets.push(t.index);
            for (const o of byKey.values())
               if (o.route?.node === c.key) o.targets.push(t.index);
            continue;
         }
         byKey.set(c.key, { number: ++n, key: c.key, targets: [t.index] });
         if (cap === 0) continue;
         const node = routes!.index.catalog.nodes.get(c.key);
         if (!node) continue;
         for (const r of routeOptions(
            routes!.index.catalog,
            node,
            peers,
            cap,
         )) {
            const key = routeKey(r);
            if (byKey.has(key)) continue;
            byKey.set(key, {
               number: ++n,
               key,
               targets: [t.index],
               route: r,
            });
         }
      }
   }
   for (const e of cluster.extras) {
      if (byKey.has(e.key)) continue;
      byKey.set(e.key, {
         number: ++n,
         key: e.key,
         targets: [],
         extra: e.reason,
      });
   }
   return [...byKey.values()];
}

/** A source's one-line description, as the judge reads it. */
function sourceLine(index: FcjIndex, key: string): string {
   const source = index.catalog.sources.get(key);
   const summary = (index.summaries.get(key) ?? source?.node.doc ?? "")
      .replace(/\s+/g, " ")
      .trim();
   return `${source?.name ?? key}${summary ? ` - ${summary}` : ""}`;
}

/** The user message for one cluster. Targets are numbered from 1 in the caller's order. */
export function renderJudgePrompt(
   index: FcjIndex,
   cluster: Cluster,
   targets: readonly FindTarget[],
   options: readonly JudgeOption[],
   /** `listRoutes`: above 0, a list shows route options and marks joined copies as routes. */
   routeCap = 0,
): string {
   const full = getEgressPreset() === "full";
   const nameOf = (s: string) => index.catalog.sources.get(s)?.name ?? s;
   const lines: string[] = cluster.peers
      ? peerLines(index, cluster)
      : homeLines(index, cluster);
   const noteOf = (t: FindTarget, o: JudgeOption) => {
      const found = cluster.candidates
         .get(t.index)
         ?.find((c) => c.key === o.key)?.found;
      const values = found?.values;
      return values && values.length > 0
         ? full
            ? `stored values matching the target: ${values
                 .slice(0, 5)
                 .map((v) => JSON.stringify(v.value))
                 .join(", ")}`
            : `a stored value matches the target's value (${values[0].how})`
         : undefined;
   };
   lines.push("", "<targets>");
   targets.forEach((t, i) => {
      const covered = cluster.covers.includes(t.index);
      const parent =
         t.partOf !== undefined
            ? targets.findIndex((x) => x.index === t.partOf)
            : -1;
      lines.push(
         `Target ${i + 1} (${t.type}${parent >= 0 ? `; one item of the list in target ${parent + 1}` : ""}${covered ? "" : "; this group matched it weakly"}): ${JSON.stringify(t.text)}`,
      );
      const listed = options.filter((o) => o.targets.includes(t.index));
      if (listed.length === 0) lines.push("  (no candidates in this group)");
      if (!cluster.peers) {
         for (const o of listed)
            lines.push(`  ${optionLine(index, cluster, o, noteOf(t, o))}`);
         return;
      }
      // A list: the target's candidates in their own order, under the
      // source that owns them.
      const byKey = new Map(listed.map((o) => [o.key, o]));
      let owner: string | undefined;
      for (const c of cluster.candidates.get(t.index) ?? []) {
         const o = byKey.get(c.key);
         if (!o) continue;
         const source = index.catalog.nodes.get(c.key)!.sourceKey;
         if (source !== owner) {
            owner = source;
            lines.push(`  From ${nameOf(source)}:`);
         }
         lines.push(
            `    ${optionLine(index, cluster, o, noteOf(t, o), routeCap)}`,
         );
         for (const r of listed) {
            if (r.route?.node === c.key)
               lines.push(`        ${routeOptionLine(index, r)}`);
         }
      }
   });
   lines.push("</targets>");
   const extras = options.filter((o) => o.targets.length === 0);
   if (extras.length > 0) {
      lines.push("", "Also in this group:");
      for (const o of extras)
         lines.push(`  ${optionLine(index, cluster, o, undefined, routeCap)}`);
   }
   lines.push("", JUDGE_REPLY_FORMAT);
   return lines.join("\n");
}

/** A cluster's header: its home, the sources its joins reach, its twins and contenders. */
function homeLines(index: FcjIndex, cluster: Cluster): string[] {
   const nameOf = (s: string) => index.catalog.sources.get(s)?.name ?? s;
   const lines = [`Home source: ${sourceLine(index, cluster.home)}`];
   const reached = [...cluster.reach.values()].filter((r) => r.hops > 0);
   if (reached.length > 0) {
      lines.push(
         `Sources its joins reach: ${reached.map((r) => nameOf(r.source)).join(", ")}`,
      );
   }
   if (cluster.twins.length > 0) {
      lines.push(
         `Sources that give the same answer under another name: ${cluster.twins
            .map(nameOf)
            .join(", ")}`,
      );
   }
   if (cluster.contenders.length > 0) {
      lines.push(
         `Sources that match these targets almost as well, with other fields: ${cluster.contenders
            .map(nameOf)
            .join(
               ", ",
            )}. Pick from the one that fits the question; say which in "caveats" when it is a close call.`,
      );
   }
   return lines;
}

/**
 * A list's header: every source as a peer, in order of first appearance,
 * with its description and the other listed sources a query from it can use
 * through its joins, by join path.
 */
function peerLines(index: FcjIndex, cluster: Cluster): string[] {
   const nameOf = (s: string) => index.catalog.sources.get(s)?.name ?? s;
   const lines = ["Sources with candidates:"];
   for (const peer of cluster.peers ?? []) {
      const name = nameOf(peer.source);
      lines.push(`- ${sourceLine(index, peer.source)}`);
      lines.push(
         peer.reaches.length === 0
            ? `  A query from ${name} can use none of the other listed sources.`
            : `  A query from ${name} can also use: ${peer.reaches
                 .map((r) => {
                    const paths = r.paths.filter((p) => p.length > 0);
                    const more = paths.length - PATHS_SHOWN;
                    return `${nameOf(r.source)} (through ${paths
                       .slice(0, PATHS_SHOWN)
                       .map((p) => `${name}.${p.join(".")}`)
                       .join(
                          " or ",
                       )}${more > 0 ? ` or ${more} more paths` : ""})`;
                 })
                 .join(", ")}.`,
      );
   }
   return lines;
}

/** Check one reply against what was offered; the message says what to fix. */
export function validateVerdict(
   targets: readonly FindTarget[],
   options: readonly JudgeOption[],
) {
   const byNumber = new Map(options.map((o) => [o.number, o]));
   return (
      value: unknown,
   ): {
      picks: Array<{ target: number; keys: string[]; reason?: string }>;
      caveats: string[];
   } => {
      if (typeof value !== "object" || value === null || Array.isArray(value)) {
         throw new Error('expected {"picks": [...], "caveats": [...]}');
      }
      const v = value as { picks?: unknown; caveats?: unknown };
      if (!Array.isArray(v.picks)) throw new Error('"picks" must be a list');
      const bad: string[] = [];
      const picks: Array<{ target: number; keys: string[]; reason?: string }> =
         [];
      for (const raw of v.picks) {
         const p = raw as {
            target?: unknown;
            candidates?: unknown;
            reason?: unknown;
         };
         const tn = typeof p.target === "number" ? p.target : Number(p.target);
         if (!Number.isInteger(tn) || tn < 1 || tn > targets.length) {
            bad.push(
               `target ${JSON.stringify(p.target)} is not a target number (1 to ${targets.length})`,
            );
            continue;
         }
         const nums = Array.isArray(p.candidates) ? p.candidates : [];
         const keys: string[] = [];
         for (const x of nums) {
            const n = typeof x === "number" ? x : Number(x);
            const o = byNumber.get(n);
            if (!o) {
               bad.push(`candidate ${JSON.stringify(x)} was not offered`);
               continue;
            }
            if (!keys.includes(o.key)) keys.push(o.key);
         }
         picks.push({
            target: targets[tn - 1].index,
            keys,
            ...(typeof p.reason === "string" && p.reason.trim()
               ? { reason: p.reason.trim().slice(0, 300) }
               : {}),
         });
      }
      if (bad.length > 0) throw new Error(bad.join("; "));
      const caveats = Array.isArray(v.caveats)
         ? v.caveats
              .filter(
                 (c): c is string => typeof c === "string" && c.trim() !== "",
              )
              .map((c) => c.trim().slice(0, 300))
              .slice(0, 10)
         : [];
      return { picks, caveats };
   };
}

/**
 * The judge reply's output cap: a reason per target (120 tokens), the JSON around it (300), and room to list every
 * offered number once (4 tokens each, with its comma and space). A cap by targets alone cut long replies off
 * mid-list once a list carried route options: a reply ended without its closing bracket.
 */
export function judgeMaxTokens(targets: number, options: number): number {
   return 120 * targets + 300 + 4 * options;
}

/**
 * `dropRoutesAt`: when one target's picks hold `at` or more routes beside
 * their own fields (the field and a route to it both picked for that
 * target), drop those routes and keep the fields. A judge that lists a
 * field and then every route to it, target after target, multiplies the
 * response's size for the same fields. A route
 * picked in place of its field, and a target with fewer such routes, are
 * left alone. A route whose id is also a found candidate's own id (an
 * indexed joined copy under that id) is that candidate, not a route, and is
 * never dropped. `at` 0 keeps every pick. A dropped route another target
 * kept is still returned, under that target.
 */
export function dropBesideRoutes(
   verdicts: readonly JudgeVerdict[],
   at: number,
   /** The wire id of an option key (optionId). */
   idOf: (key: string) => string | undefined,
   /** Wire ids of every candidate the find stage returned. */
   foundIds: ReadonlySet<string>,
): JudgeVerdict[] {
   if (at <= 0) return [...verdicts];
   return verdicts.map((v) => ({
      ...v,
      picks: v.picks.map((p) => {
         const picked = new Set(p.keys);
         const beside = p.keys.filter((k) => {
            const route = parseRouteKey(k);
            if (!route || !picked.has(route.node)) return false;
            const id = idOf(k);
            return id === undefined || !foundIds.has(id);
         });
         if (beside.length < at) return p;
         const drop = new Set(beside);
         return {
            ...p,
            keys: p.keys.filter((k) => !drop.has(k)),
            droppedRoutes: beside,
         };
      }),
   }));
}

/** Judge every cluster, `concurrency` calls at a time. Throws on a failed call. */
export async function judge(args: {
   index: FcjIndex;
   clusters: readonly Cluster[];
   targets: readonly FindTarget[];
   chat: ChatModel;
   instructions?: string;
   concurrency: number;
   /** `listRoutes`: routes shown per field in a list (0, the default, shows none). */
   listRoutes?: number;
   /** `splitBundles`: add JUDGE_SPLIT_RULES to the instructions. */
   splitRules?: boolean;
}): Promise<JudgeVerdict[]> {
   return runPooled(args.clusters, args.concurrency, async (cluster) => {
      const cap = cluster.peers ? (args.listRoutes ?? 0) : 0;
      const options = judgeOptions(cluster, args.targets, {
         index: args.index,
         cap,
      });
      const base =
         args.instructions ??
         (cluster.peers
            ? cap > 0
               ? DEFAULT_JUDGE_LIST_ROUTES_INSTRUCTIONS
               : DEFAULT_JUDGE_LIST_INSTRUCTIONS
            : DEFAULT_JUDGE_INSTRUCTIONS);
      const reply = await args.chat.completeJson({
         system: args.splitRules ? `${base}\n\n${JUDGE_SPLIT_RULES}` : base,
         prompt: renderJudgePrompt(
            args.index,
            cluster,
            args.targets,
            options,
            cap,
         ),
         maxTokens: judgeMaxTokens(args.targets.length, options.length),
         validate: validateVerdict(args.targets, options),
      });
      const shown = options.filter((o) => o.route).length;
      return {
         cluster: cluster.home,
         ...reply.value,
         ...(cap > 0 ? { routeOptions: shown } : {}),
      };
   });
}
