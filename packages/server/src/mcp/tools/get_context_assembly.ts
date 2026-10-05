// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Assembly: turn the ranked rows into source cards. This is one plain
 * function, not a pluggable stage, because it needs every target's rows at
 * once. Card stages run on its output; shapeCards in get_context_tool.ts
 * pages and serializes it.
 */

import type {
   CardDraft,
   CardState,
   PipelineContext,
   RankedState,
} from "./get_context_pipeline";
import { KEY_SEPARATOR, entityRowKey } from "./embedding_index";
import {
   enterJoin,
   matchesScope,
   sourceContextKey,
   type Entity,
   type JoinReach,
   type JoinTopology,
   type ResolvedRequest,
   type ResultEntity,
} from "./get_context_tool";

/**
 * A card's relevance after reading one more row: a source's own row sets it,
 * and any scored row raises it. One definition, used here and by
 * toSourceResults, so a card and its wire form cannot disagree.
 */
export function foldRelevance(
   current: number | undefined,
   row: ResultEntity,
): number | undefined {
   if (row.score === undefined) return current;
   // The source itself matched: its score belongs on the card outright.
   if (row.kind === "source") return row.score;
   // A source with no hit of its own still ranks by its best entity, so a
   // caller reading source relevance never sees a matched source at null.
   return current === undefined || row.score > current ? row.score : current;
}

/** Plain code-unit order, so the result does not depend on the host's locale. */
function compareText(a: string, b: string): number {
   return a < b ? -1 : a > b ? 1 : 0;
}

/**
 * The order of ranked rows: score descending, then source, name, kind and
 * model path. The score is the rounded one the response publishes, so two rows
 * that show the same relevance are listed in a fixed order, not in whatever
 * order the scan happened to return them. Shared by the semantic retriever
 * and by assembly, which adds joined copies and so has to sort again.
 */
export function compareRanked(a: ResultEntity, b: ResultEntity): number {
   return (
      (b.score ?? 0) - (a.score ?? 0) ||
      compareText(a.source ?? "", b.source ?? "") ||
      compareText(a.name, b.name) ||
      compareText(a.kind, b.kind) ||
      compareText(a.modelPath, b.modelPath)
   );
}

/** The kinds a joined copy exists for: a view or join is not reachable as `alias.name`. */
function isJoinable(kind: string): boolean {
   return kind === "dimension" || kind === "measure";
}

/** One root source that reaches a target source, and by which join path. */
interface Reacher {
   source: string;
   modelPath: string;
   reach: JoinReach;
}

const reachersCache = new WeakMap<JoinTopology, Map<string, Reacher[]>>();

/**
 * The topology turned around: for each target source, the roots that reach
 * it. The topology is keyed by root; assembly starts from a ranked target
 * field and asks "who can reach this?". A source is identified by the file
 * that defines it and its name, so two files that each define `cust` have
 * different reachers. Built once per topology.
 */
function reachersOf(topology: JoinTopology): Map<string, Reacher[]> {
   const cached = reachersCache.get(topology);
   if (cached) return cached;
   const byTarget = new Map<string, Reacher[]>();
   for (const [rootKey, reaches] of topology) {
      const at = rootKey.indexOf(KEY_SEPARATOR);
      const modelPath = rootKey.slice(0, at);
      const source = rootKey.slice(at + KEY_SEPARATOR.length);
      for (const reach of reaches) {
         const target = sourceContextKey(
            reach.targetModelPath,
            reach.targetSource,
         );
         const list = byTarget.get(target) ?? [];
         list.push({ source, modelPath, reach });
         byTarget.set(target, list);
      }
   }
   reachersCache.set(topology, byTarget);
   return byTarget;
}

/** Where a direct field reappears when reached through a join. */
interface JoinedCopy {
   source: string;
   modelPath: string;
   /** The join names that reach the field, dotted: the copy's `join_path`. */
   joinPath: string;
   /** The dotted display name, `joinPath.fieldName`. */
   name: string;
   hops: number;
   fanout: JoinReach["fanout"];
}

/**
 * Every place a direct dimension or measure also appears through a join: one
 * copy per root and join path, up to `maxDepth` joins. Same name, join path
 * and fan-out as the copies the index makes for the lexical path, so a caller
 * cannot tell which kind it got.
 */
function joinedCopiesOf(
   base: { kind: string; name: string; source?: string; modelPath: string },
   topology: JoinTopology,
   maxDepth: number,
): JoinedCopy[] {
   if (!isJoinable(base.kind) || base.source === undefined) return [];
   const target = sourceContextKey(base.modelPath, base.source);
   return (reachersOf(topology).get(target) ?? []).flatMap((reacher) => {
      const hops = reacher.reach.path.length;
      if (hops > maxDepth) return [];
      const joinPath = reacher.reach.path.join(".");
      return [
         {
            source: reacher.source,
            modelPath: reacher.modelPath,
            joinPath,
            name: `${joinPath}.${base.name}`,
            hops,
            fanout: reacher.reach.fanout,
         },
      ];
   });
}

/**
 * The vector-cache rows a scoped request can be answered from, when joined
 * copies are made at assembly. A field in scope may be a copy reached through
 * a join, and its score comes from the target field's row, so that row is
 * searched even though the target source itself is out of scope. Returns each
 * such row once.
 */
export function scopeKeysWithJoins(
   direct: Iterable<Entity>,
   topology: JoinTopology,
   request: ResolvedRequest,
   maxDepth: number,
): Array<{ kind: string; source: string; name: string }> {
   const seen = new Set<string>();
   const keys: Array<{ kind: string; source: string; name: string }> = [];
   for (const e of direct) {
      const inScope =
         matchesScope(e, request) ||
         joinedCopiesOf(e, topology, maxDepth).some((copy) =>
            matchesScope({ ...copy, kind: e.kind }, request),
         );
      if (!inScope) continue;
      const source = e.source ?? "";
      const key = entityRowKey(e.kind, source, e.name);
      if (seen.has(key)) continue;
      seen.add(key);
      keys.push({ kind: e.kind, source, name: e.name });
   }
   return keys;
}

const round4 = (x: number) => Math.round(x * 10_000) / 10_000;

function dampTargetScores(
   row: ResultEntity,
   factor: number,
): Map<number, number> | undefined {
   if (!row.targetScores) return undefined;
   return new Map(
      [...row.targetScores].map(([target, score]) => [
         target,
         round4(score * factor),
      ]),
   );
}

/**
 * Add the joined copies of the ranked direct fields, damped, and drop what
 * the request's scope excludes.
 *
 * A copy scores as its direct field's score times `damping ** (hops + 1)`, so
 * one join is 0.81 and two are 0.729; the direct field is not damped. The
 * same display name reached twice (the target source is resolvable from two
 * model files) keeps the higher score. A copy's per-target scores are damped
 * the same way, so `matched_targets` agrees with the entity's own relevance.
 */
function expandJoins(
   rows: ResultEntity[],
   ctx: PipelineContext,
): ResultEntity[] {
   const { request, pkgIndex, settings } = ctx;
   const damping = settings.joinDamping;
   const out: ResultEntity[] = [];
   const copies = new Map<string, ResultEntity>();
   // Rows the index already holds under the display name a copy would take: a
   // field the index kept (an inline-table join's field, reached again through
   // another join) can also be rebuilt here, and the two must not both appear.
   const ranked = new Set(
      rows.map((r) =>
         [r.modelPath, r.source ?? "", r.kind, r.name].join(KEY_SEPARATOR),
      ),
   );
   for (const row of rows) {
      if (matchesScope(row, request)) out.push(row);
      for (const place of joinedCopiesOf(
         row,
         pkgIndex.topology,
         settings.joinMaxDepth,
      )) {
         if (!matchesScope({ ...place, kind: row.kind }, request)) continue;
         // A row that is itself a dotted index row (`lines.total` on `cust`)
         // is reached through the root's join and then its own: the copy's path
         // is both, its fan-out the widest of the two (the same rule the index's
         // own copies use), and its hops count both.
         const path =
            row.joinPath === undefined
               ? place.joinPath
               : `${place.joinPath}.${row.joinPath}`;
         const fanout =
            row.joinPath === undefined
               ? place.fanout
               : enterJoin(
                    { name: row.joinPath, relationship: row.relationship },
                    { joinPath: place.joinPath, fanout: place.fanout },
                 ).fanout;
         const hops =
            place.hops +
            (row.joinPath === undefined ? 0 : row.joinPath.split(".").length);
         if (hops > settings.joinMaxDepth) continue;
         const factor = damping === null ? 1 : damping ** (hops + 1);
         const score =
            row.score === undefined ? undefined : round4(row.score * factor);
         const key = [place.modelPath, place.source, row.kind, place.name].join(
            KEY_SEPARATOR,
         );
         if (ranked.has(key)) continue;
         const seen = copies.get(key);
         if (seen) {
            if (score !== undefined && score > (seen.score ?? 0)) {
               seen.score = score;
               seen.targetScores = dampTargetScores(row, factor);
               seen.bestTarget = row.bestTarget;
            }
            continue;
         }
         const copy: ResultEntity = {
            kind: row.kind,
            name: place.name,
            source: place.source,
            environmentName: row.environmentName,
            packageName: row.packageName,
            modelPath: place.modelPath,
            doc: row.doc,
            relationship: fanout,
            joinPath: path,
            ...(row.dataType ? { dataType: row.dataType } : {}),
            ...(score !== undefined ? { score } : {}),
            ...(row.targetScores
               ? { targetScores: dampTargetScores(row, factor) }
               : {}),
            ...(row.bestTarget !== undefined
               ? { bestTarget: row.bestTarget }
               : {}),
            joinHops: hops,
         };
         copies.set(key, copy);
         out.push(copy);
      }
   }
   return out.sort(compareRanked);
}

/**
 * Group the ranked rows into one card per (model path, source), in the order
 * sources first appear, and cap the entities each card carries per search
 * target.
 *
 * Nothing is folded across sources: two sources exposing a same-named
 * measure are two different numbers, each nested under its own card. Cards
 * are not paged here; shapeCards keeps the first `limit` and sums the
 * entities dropped from those alone.
 */
export function assembleCards(
   state: RankedState,
   ctx: PipelineContext,
): CardState {
   // On the semantic path the card holds the source's own window plus the
   // dotted rows' window, so a dotted row the scan kept is not then cut here for
   // a higher-scored one. The lexical path has no separate window and keeps 10.
   const perSourcePerTarget =
      ctx.settings.entityWindow.perSourcePerTarget +
      (state.retrieval === "semantic"
         ? (ctx.settings.entityWindow.joinedPerSourcePerTarget ?? 0)
         : 0);
   const cards = new Map<string, CardDraft>();
   const perTarget = new Map<string, Map<number, number>>();
   // Joined copies are made here only for semantic rows, which search direct
   // fields alone. The lexical path's rows already include the index's copies.
   const rows =
      ctx.settings.joins === "assembly" && state.retrieval === "semantic"
         ? expandJoins(state.rows, ctx)
         : state.rows;
   for (const r of rows) {
      const source = r.source ?? "";
      const key = sourceContextKey(r.modelPath, source);
      let card = cards.get(key);
      if (!card) {
         card = {
            key,
            modelPath: r.modelPath,
            source,
            rows: [],
            entitiesDropped: 0,
         };
         cards.set(key, card);
         perTarget.set(key, new Map());
      }
      // A source row becomes the card itself, so it never spends a slot.
      if (r.kind !== "source") {
         // -1 buckets a row no target claims, so it shares one cap rather
         // than none. Both ranked paths set bestTarget, so on them this is
         // only a guard.
         const target = r.bestTarget ?? -1;
         const counts = perTarget.get(key) as Map<number, number>;
         const taken = counts.get(target) ?? 0;
         if (taken >= perSourcePerTarget) {
            card.entitiesDropped += 1;
            continue;
         }
         counts.set(target, taken + 1);
      }
      card.rows.push(r);
      card.relevance = foldRelevance(card.relevance, r);
   }
   // Rows the semantic scan's per-source window dropped never reach the loop
   // above, so the scan counted them. Every card of the source reports them,
   // as it would have counted its own copies of those rows.
   for (const card of cards.values()) {
      card.entitiesDropped += state.entitiesCutBySource?.get(card.source) ?? 0;
   }
   const { rows: _rows, ...rest } = state;
   return { ...rest, cards: Array.from(cards.values()) };
}
