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
 * A target with no pick in any cluster is returned as unresolved by name,
 * which is the agent's signal to ask or to search again.
 */

import {
   DEFAULT_JUDGE_INSTRUCTIONS,
   JUDGE_REPLY_FORMAT,
} from "../../prompts/judge";
import type { ChatModel } from "../../providers/types";
import { getEgressPreset } from "../../retrieval_config";
import { humanizeName } from "../tools/embedding_index";
import { runPooled } from "../tools/get_context_llm";
import type { CatalogNode } from "./catalog";
import type { Cluster } from "./connect";
import type { FindTarget } from "./find";
import type { FcjIndex } from "./index_build";

/** One numbered line the judge may pick. */
export interface JudgeOption {
   number: number;
   key: string;
   /** Targets it is listed under; an extra is listed under none. */
   targets: number[];
   extra?: string;
}

export interface JudgePick {
   target: number;
   keys: string[];
   reason?: string;
}

export interface JudgeVerdict {
   cluster: string;
   picks: JudgePick[];
   caveats: string[];
}

/** Doc text the judge reads for one candidate; long docs are cut here, not dropped. */
const JUDGE_DOC_MAX_CHARS = 1_500;

function routeOf(index: FcjIndex, cluster: Cluster, node: CatalogNode): string {
   const entry = cluster.reach.get(node.sourceKey);
   if (!entry || entry.hops === 0) return "own field";
   const home = index.catalog.sources.get(cluster.home)?.name ?? cluster.home;
   const paths = entry.paths
      .filter((p) => p.length > 0)
      .map((p) => `${home}.${p.join(".")}`);
   return `through ${paths.join(" or ")} (a left join from ${home})`;
}

function optionLine(
   index: FcjIndex,
   cluster: Cluster,
   option: JudgeOption,
   valueNote: string | undefined,
): string {
   const node = index.catalog.nodes.get(option.key)!;
   const kind = node.dataType ? `${node.kind}, ${node.dataType}` : node.kind;
   const doc = node.doc.replace(/\s+/g, " ").trim();
   const cut =
      doc.length > JUDGE_DOC_MAX_CHARS
         ? `${doc.slice(0, JUDGE_DOC_MAX_CHARS)}…`
         : doc;
   const parts = [
      `[${option.number}] ${node.name} (${kind}; source ${node.source}; ${routeOf(index, cluster, node)})`,
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
): JudgeOption[] {
   const byKey = new Map<string, JudgeOption>();
   let n = 0;
   for (const t of targets) {
      for (const c of cluster.candidates.get(t.index) ?? []) {
         const prev = byKey.get(c.key);
         if (prev) prev.targets.push(t.index);
         else byKey.set(c.key, { number: ++n, key: c.key, targets: [t.index] });
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

/** The user message for one cluster. Targets are numbered from 1 in the caller's order. */
export function renderJudgePrompt(
   index: FcjIndex,
   cluster: Cluster,
   targets: readonly FindTarget[],
   options: readonly JudgeOption[],
): string {
   const full = getEgressPreset() === "full";
   const home = index.catalog.sources.get(cluster.home);
   const summary = index.summaries.get(cluster.home) ?? home?.node.doc ?? "";
   const lines: string[] = [
      `Home source: ${home?.name ?? cluster.home}${summary ? ` - ${summary.replace(/\s+/g, " ").trim()}` : ""}`,
   ];
   const reached = [...cluster.reach.values()].filter((r) => r.hops > 0);
   if (reached.length > 0) {
      lines.push(
         `Sources its joins reach: ${reached
            .map((r) => index.catalog.sources.get(r.source)?.name ?? r.source)
            .join(", ")}`,
      );
   }
   if (cluster.twins.length > 0) {
      lines.push(
         `Sources that give the same answer under another name: ${cluster.twins
            .map((s) => index.catalog.sources.get(s)?.name ?? s)
            .join(", ")}`,
      );
   }
   lines.push("", "<targets>");
   targets.forEach((t, i) => {
      const covered = cluster.covers.includes(t.index);
      lines.push(
         `Target ${i + 1} (${t.type}${covered ? "" : "; this group matched it weakly"}): ${JSON.stringify(t.text)}`,
      );
      const listed = options.filter((o) => o.targets.includes(t.index));
      if (listed.length === 0) lines.push("  (no candidates in this group)");
      for (const o of listed) {
         const found = cluster.candidates
            .get(t.index)
            ?.find((c) => c.key === o.key)?.found;
         const values = found?.values;
         const note =
            values && values.length > 0
               ? full
                  ? `stored values matching the target: ${values
                       .slice(0, 5)
                       .map((v) => JSON.stringify(v.value))
                       .join(", ")}`
                  : `a stored value matches the target's value (${values[0].how})`
               : undefined;
         lines.push(`  ${optionLine(index, cluster, o, note)}`);
      }
   });
   lines.push("</targets>");
   const extras = options.filter((o) => o.targets.length === 0);
   if (extras.length > 0) {
      lines.push("", "Also in this group:");
      for (const o of extras)
         lines.push(`  ${optionLine(index, cluster, o, undefined)}`);
   }
   lines.push("", JUDGE_REPLY_FORMAT);
   return lines.join("\n");
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

/** Judge every cluster, `concurrency` calls at a time. Throws on a failed call. */
export async function judge(args: {
   index: FcjIndex;
   clusters: readonly Cluster[];
   targets: readonly FindTarget[];
   chat: ChatModel;
   instructions?: string;
   concurrency: number;
}): Promise<JudgeVerdict[]> {
   return runPooled(args.clusters, args.concurrency, async (cluster) => {
      const options = judgeOptions(cluster, args.targets);
      const reply = await args.chat.completeJson({
         system: args.instructions ?? DEFAULT_JUDGE_INSTRUCTIONS,
         prompt: renderJudgePrompt(args.index, cluster, args.targets, options),
         maxTokens: 120 * args.targets.length + 300,
         validate: validateVerdict(args.targets, options),
      });
      return { cluster: cluster.home, ...reply.value };
   });
}
