// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { createHash } from "crypto";
import { E_ALREADY_LOCKED, Mutex, tryAcquire } from "async-mutex";
import { logger } from "../../logger";
import { DuckDBConnection } from "../../storage/duckdb/DuckDBConnection";
import type { Package } from "../../service/package";
import {
   EmbeddingProvider,
   EMBEDDING_BATCH_TIMEOUT_MS,
   EMBEDDING_QUERY_TIMEOUT_MS,
   MAX_EMBED_INPUT_CHARS,
   prepareEmbeddingInput,
} from "../../service/embedding_provider";

/**
 * Minimum cosine similarity for a semantic hit. Below this the entity is
 * dropped, so a query about something the package does not model returns
 * an empty result rather than the k least-unrelated entities. Agents are
 * taught to treat an empty result as "not in this package"; unfiltered
 * top-k would destroy that signal.
 *
 * The value now travels on the provider (`provider.minSimilarity`), because
 * cosine similarity is not calibrated across embedding models and the right
 * floor is a property of the model that produced the vectors. Operators set
 * it with `EMBEDDING_MIN_SIMILARITY`; the default is unchanged at 0.20, which
 * matches the hosted retrieval pipeline's min_score. Tune against
 * get_context_eval.ts.
 */
export { DEFAULT_EMBEDDING_MIN_SIMILARITY } from "../../config";
/**
 * Packages with more entities than this stay lexical: the first embed of
 * such a package would take minutes of provider calls and rate limits.
 * The bundled examples sit around a few hundred entities.
 *
 * Counted in ENTITIES, not rows. Faceting means a documented entity costs
 * more than one embedding (a name row plus its doc rows), so the ceiling on
 * first-sync provider calls is a small multiple of this number rather than
 * this number. The cap is deliberately still expressed in entities: it is
 * checked before facets are computed, and it is the figure an operator can
 * reason about from their model. Undocumented entities, which are the ones
 * that make a package large by accident, still cost exactly one row each.
 */
export const MAX_EMBEDDED_ENTITIES = 5_000;
/**
 * After a provider failure the semantic path short-circuits to lexical
 * for this long, so a down or misconfigured endpoint costs one timeout
 * per window, not one per call.
 */
export const PROVIDER_FAILURE_COOLDOWN_MS = 60_000;
/**
 * After a dims-mismatch purge, suppress further purges for this long. It
 * MUST exceed PROVIDER_FAILURE_COOLDOWN_MS. A dims-mismatch backoff arms
 * the cooldown, which then blocks every heal-reaching call for one
 * cooldown window; if the two windows were equal, the first call past the
 * cooldown would always find the purge guard expired too and re-purge,
 * re-embedding the whole package once per cooldown window forever. With a
 * longer window a durably dims-inconsistent provider (e.g. mid-migration
 * replicas serving different dims under one model name) is throttled to
 * at most one re-embed per this interval, staying lexical between, while
 * still re-adopting a genuinely-new stable dimensionality within the
 * window. Deliberately not "never re-purge": that would strand the cache
 * on the old dims if the provider later settles on a new one.
 */
export const HEAL_PURGE_SUPPRESSION_MS = 10 * PROVIDER_FAILURE_COOLDOWN_MS;

/** The subset of the tool's Entity shape the index needs. */
export interface EmbeddableEntity {
   kind: string;
   name: string;
   source: string | undefined;
   modelPath: string;
   // #(doc)-only text (see get_context_tool docOnlyText). Deliberately NOT
   // the response `doc`, which can fall back to raw annotation lines and
   // would leak #(authorize) predicates to the embedding provider.
   embedDoc: string;
}

export interface SemanticHit {
   kind: string;
   name: string;
   source: string | undefined;
   /** Best score across every target, which is what the hit is ranked on. */
   score: number;
   /**
    * Score per caller target index. One scan scores every target, so this
    * costs nothing to carry and is what fills the response's matched_targets:
    * without it a caller cannot tell WHICH of its targets found the entity.
    */
   targetScores: Map<number, number>;
}

export type SemanticUnavailableReason =
   | "cooldown"
   | "too-many-entities"
   | "indexing"
   | "error";

export type SemanticSearchResult =
   | {
        hits: SemanticHit[];
        /**
         * Entities whose best facet scored below the floor, under the same
         * scope as `hits`. Always read against {@link totalEntities}: every
         * entity in scope is either at-or-above the floor (and so eligible to
         * be a hit) or below it (and so counted here), so the two partition
         * the package and the count means nothing on its own.
         *
         * The ratio is the signal. A small fraction is a tight match. A large
         * fraction is a weak one. `belowCutoffCount === totalEntities` is the
         * true negative -- nothing cleared the floor -- and it necessarily
         * comes with `hits: []`.
         *
         * Note what this rules out: `belowCutoffCount: 0` alongside empty
         * hits cannot occur, because zero entities below the floor means
         * every entity was above it, which would have produced hits. Only an
         * empty package reaches 0-with-no-hits, so never wait on that
         * combination as a signal.
         */
        belowCutoffCount: number;
        /**
         * Entities weighed for this query, under the same scope as `hits`:
         * the denominator that makes `belowCutoffCount` interpretable.
         */
        totalEntities: number;
     }
   | { unavailable: SemanticUnavailableReason };

/**
 * Turn an identifier into the words a person would search with:
 * `dep_delay` -> "dep delay", `totalSales` -> "total sales". This is the
 * heart of closing the lexical token gap; the embedding model then maps
 * "departure delay" near "dep delay" where BM25 could not.
 */
export function humanizeName(name: string): string {
   return name
      .replace(/[_\-./]+/g, " ")
      .replace(/([a-z0-9])([A-Z])/g, "$1 $2")
      .toLowerCase()
      .replace(/\s+/g, " ")
      .trim();
}

/**
 * The text embedded per entity: humanized name plus its `#(doc)` text.
 * Deliberately minimal (no kind, no parent source); the recipe is
 * eval-tunable via get_context_eval.ts. A punctuation-only identifier
 * (`_` is legal Malloy) humanizes to nothing; fall back to the raw name
 * so the embedded text stays meaningful. prepareEmbeddingInput (which
 * both the hash and the request path apply) separately guarantees that
 * even a whitespace-only name never reaches the provider as an empty
 * input.
 *
 * Retained as the definition of an entity's DOC facet text; the name facet
 * is embedded separately. See entityFacets.
 */
export function embeddingText(entity: EmbeddableEntity): string {
   const name = humanizeName(entity.name) || entity.name;
   return entity.embedDoc ? `${name}: ${entity.embedDoc}` : name;
}

/** The facet holding an entity's own name, embedded free of doc text. */
export const NAME_FACET = "name";

/**
 * Target and hard ceiling for one doc chunk, in characters, and the most
 * chunks one doc may produce.
 *
 * The target is small enough that a single fact keeps a vector of its own,
 * and the cap bounds what one lavishly-documented entity can cost: at most
 * MAX_DOC_CHUNKS + 1 embeddings. A doc at or under CHUNK_MAX_CHARS stays a
 * single chunk, so short docs produce byte-identical rows to the unchunked
 * scheme and cost no re-embed when this ships.
 */
export const CHUNK_TARGET_CHARS = 300;
export const CHUNK_MAX_CHARS = 500;
export const MAX_DOC_CHUNKS = 8;
/**
 * Ceiling the adaptive packing may grow a chunk to. Sits below
 * MAX_EMBED_INPUT_CHARS with room for the `<name>: ` prefix entityFacets
 * adds, so packing alone never produces a chunk that has to be split.
 */
export const CHUNK_HARD_MAX_CHARS = 768;
/**
 * The most doc text one entity can contribute, across every chunk.
 *
 * A fixed per-entity embedding budget and complete coverage of an unbounded
 * doc cannot both hold. This names where coverage stops, so the loss is a
 * stated bound rather than an invisible cut downstream; syncPackageEmbeddings
 * warns whenever a doc exceeds it. Six thousand characters is far past any
 * doc observed in a real model, and everything under it is embedded whole.
 */
export const MAX_DOC_CHARS = MAX_DOC_CHUNKS * CHUNK_HARD_MAX_CHARS;

/** One embeddable unit of an entity: its name, or a chunk of its doc. */
export interface EntityFacet {
   facet: string;
   text: string;
}

/**
 * Split doc text into chunks that each keep their own embedding.
 *
 * A source doc is where modellers put grain caveats, population rules and
 * reporting conventions. Averaged into one vector across everything the doc
 * mentions, no single fact in it is close to anything, so those facts were
 * unreachable by their own wording.
 *
 * It also fixes a silent loss. prepareEmbeddingInput caps input at
 * MAX_EMBED_INPUT_CHARS, so before chunking, everything past that cap was
 * dropped with no signal — the tail of a long doc was not merely diluted, it
 * was never embedded at all.
 *
 * Splits on sentence boundaries and packs greedily toward a target that grows
 * with the doc, so MAX_DOC_CHUNKS chunks cover the whole of it. A doc short
 * enough for the plain CHUNK_TARGET_CHARS keeps that target, so ordinary docs
 * chunk exactly as they did and cost no re-embed.
 *
 * Whole sentences are preserved, never cut. A chunk too long to embed once
 * prefixed is split into further facets by entityFacets, so no text is lost
 * between here and the provider. An earlier form folded an unbounded
 * remainder into the last chunk and claimed nothing was dropped; the fold was
 * then cut at MAX_EMBED_INPUT_CHARS and the tail vanished anyway. Coverage
 * now stops at exactly one place, MAX_DOC_CHARS, and syncPackageEmbeddings
 * warns when a doc reaches it.
 */
export function chunkDoc(doc: string): string[] {
   const full = doc.replace(/\s+/g, " ").trim();
   if (!full) return [];
   const text =
      full.length > MAX_DOC_CHARS ? splitToFit(full, MAX_DOC_CHARS)[0] : full;
   if (text.length <= CHUNK_MAX_CHARS) return [text];

   // Grow the target so the doc fits in MAX_DOC_CHUNKS, and grow the ceiling
   // with it in the same proportion the two constants hold.
   const target = Math.min(
      Math.max(CHUNK_TARGET_CHARS, Math.ceil(text.length / MAX_DOC_CHUNKS)),
      CHUNK_HARD_MAX_CHARS,
   );
   const ceiling = Math.min(
      Math.max(
         CHUNK_MAX_CHARS,
         Math.round((target * CHUNK_MAX_CHARS) / CHUNK_TARGET_CHARS),
      ),
      CHUNK_HARD_MAX_CHARS,
   );

   const sentences = text.split(/(?<=[.!?])\s+/);
   const chunks: string[] = [];
   let current = "";
   for (const sentence of sentences) {
      if (!current) {
         current = sentence;
         continue;
      }
      const joined = `${current} ${sentence}`;
      // Pack until the target, but never push a chunk past the ceiling.
      if (
         joined.length <= target ||
         (current.length < target && joined.length <= ceiling)
      ) {
         current = joined;
         continue;
      }
      chunks.push(current);
      current = sentence;
   }
   if (current) chunks.push(current);

   // Uneven sentence lengths can still overshoot the chunk count, so fold the
   // tail as before. Nothing is truncated here: a chunk too long to embed
   // with its prefix is SPLIT into further facets by entityFacets, which is
   // the only place the prefix length is known.
   if (chunks.length > MAX_DOC_CHUNKS) {
      return [
         ...chunks.slice(0, MAX_DOC_CHUNKS - 1),
         chunks.slice(MAX_DOC_CHUNKS - 1).join(" "),
      ];
   }
   return chunks;
}

/**
 * Split an entity into the units that get their own embedding.
 *
 * One vector per entity meant a long `#(doc)` dominated the average and the
 * entity stopped matching the plain name of the concept it describes, so
 * documenting a field well made it harder to find - backwards for a tool
 * whose quality is meant to track the model's.
 *
 * Embedding the name on its own fixes that: the name facet matches the plain
 * name at full strength no matter how much documentation the entity carries,
 * and the doc facets still contribute their own vocabulary. Scoring takes the
 * best facet (see trySemanticSearch), so more documentation can only add
 * recall, never cost precision on the entity's own name.
 *
 * A long doc splits across `doc:N` facets so individual facts stay
 * retrievable by their own content (see chunkDoc). Each chunk is prefixed
 * with the entity's name, which anchors a bare fact to the thing it is about;
 * the chunks are short, so that prefix costs little dilution.
 */
export function entityFacets(entity: EmbeddableEntity): EntityFacet[] {
   const name = humanizeName(entity.name) || entity.name;
   const facets: EntityFacet[] = [{ facet: NAME_FACET, text: name }];
   // The prefix is only known here, so this is the only place that can tell
   // whether a chunk will fit the provider's input cap. A chunk that does not
   // fit is split across facets rather than cut: prepareEmbeddingInput used
   // to trim the overflow away with no signal, which is the loss chunking
   // exists to remove.
   const room = MAX_EMBED_INPUT_CHARS - name.length - 2;
   let i = 0;
   for (const chunk of chunkDoc(entity.embedDoc)) {
      for (const piece of splitToFit(chunk, room)) {
         facets.push({ facet: `doc:${i++}`, text: `${name}: ${piece}` });
      }
   }
   return facets;
}

/**
 * Break text into pieces of at most `max` characters, on word boundaries
 * where one is available. `max` at or below zero yields one piece, leaving
 * the provider's own cap as the backstop: an entity name longer than the
 * whole input budget has no room to give.
 */
function splitToFit(text: string, max: number): string[] {
   if (max <= 0 || text.length <= max) return [text];
   const pieces: string[] = [];
   let rest = text;
   while (rest.length > max) {
      const head = rest.slice(0, max);
      const lastSpace = head.lastIndexOf(" ");
      const cut = lastSpace > max * 0.6 ? lastSpace : max;
      pieces.push(rest.slice(0, cut).trimEnd());
      rest = rest.slice(cut).trimStart();
   }
   if (rest) pieces.push(rest);
   return pieces;
}

function contentHash(text: string): string {
   return createHash("sha256").update(text).digest("hex");
}

/** '' in the entity_source column encodes "no parent source". */
function sourceColumn(source: string | undefined): string {
   return source ?? "";
}

/**
 * Separator for the composite in-memory keys below.
 *
 * NUL, not `|`, and for the same reason metaKey uses it: a Malloy identifier
 * can be backtick-quoted, and the grammar's BQ_STRING excludes only quotes,
 * backslash and control characters. `|` is therefore legal inside a source or
 * field name, so joining on it let two different entities collide on one key
 * -- `{source: "a|b", name: "c"}` and `{source: "a", name: "b|c"}` -- and a
 * collision in the sync diff silently skips a delete or a re-embed. A control
 * character cannot appear in an identifier, so it cannot collide.
 */
const KEY_SEPARATOR = "\u0000";

/**
 * The stable key for one entity. Shared by the index dedup, the WeakMap-free
 * row identity, and the tool's result mapping so the format lives in exactly
 * one place. Pass "" for a sourceless entity.
 *
 * In-memory only: nothing persists this string, so its shape can change
 * without a migration. The database keys on the three columns directly.
 */
export function entityRowKey(
   kind: string,
   source: string,
   name: string,
): string {
   return [kind, source, name].join(KEY_SEPARATOR);
}

/**
 * The stable key for one embedding ROW: an entity plus the facet of it that
 * row holds. Keeping the format here means the diff, the upsert and the
 * delete cannot drift apart.
 */
export function facetRowKey(
   kind: string,
   source: string,
   name: string,
   facet: string,
): string {
   return entityRowKey(kind, source, name) + KEY_SEPARATOR + facet;
}

// Sync state, two layers.
//
// Per package NAME (`syncMeta`): a mutex serializing every read-diff-write
// section (sync AND the heal's purge) so a reload racing an in-flight sync
// cannot tear rows; a `generation` counter bumped by every purge, so a
// purge invalidates the memo of EVERY Package instance, not just the
// caller's (a reloaded instance's `done` memo must not survive a purge
// over a now-empty table); `lastPurgeAtMs`, which bounds how often the
// heal may purge (a backend serving inconsistent dimensionalities
// otherwise causes an unbounded purge / full-re-embed loop); and
// `failureAtMs`, the per-package provider cool-down. The cool-down is
// scoped per package, NOT global: a query timeout or dims-mismatch on
// one package must not force every other healthy, correctly-cached
// package to lexical for the window. If the endpoint is genuinely down,
// each package cools itself on its own first failed probe (one wasted
// probe per package per window, negligible at the entity counts a single
// Publisher serves).
//
// Per package INSTANCE (`syncState`, WeakMap): memoizes "this instance is
// synced" (reload swaps the instance, so entity-set staleness clears
// itself, same contract as the tool's lunr cache). A rejected sync
// promise is evicted so one transient failure is not permanent.
// `providerKey` records which model/dims request-config the sync used, so
// switching EMBEDDING_MODEL or EMBEDDING_DIMENSIONS re-syncs promptly.
interface PackageSyncMeta {
   mutex: Mutex;
   generation: number;
   lastPurgeAtMs: number;
   failureAtMs: number;
}
interface SyncState {
   done: boolean;
   providerKey: string;
   generation: number;
}
const syncState = new WeakMap<Package, SyncState>();
const syncMeta = new Map<string, PackageSyncMeta>();
// Every generation value ever issued is globally unique (drawn from this
// counter, never incremented locally). That makes deleting a syncMeta
// entry safe: a re-minted meta for the same name can never coincide with
// a generation some live memo recorded under the old meta, which would
// let that memo be trusted over a table the deletion just emptied.
let generationCounter = 0;
const oversizeWarned = new Set<string>();

// Effective timing windows. Mutable only so tests can drive real expiry
// deterministically with small sleeps (the cooldown-vs-suppression
// relationship is real-time and not otherwise unit-observable); production
// always uses the exported constants.
let cooldownMs = PROVIDER_FAILURE_COOLDOWN_MS;
let purgeSuppressionMs = HEAL_PURGE_SUPPRESSION_MS;

function metaKey(environmentName: string, packageName: string): string {
   return `${environmentName}\x00${packageName}`;
}

function metaFor(
   environmentName: string,
   packageName: string,
): PackageSyncMeta {
   const key = metaKey(environmentName, packageName);
   let meta = syncMeta.get(key);
   if (!meta) {
      meta = {
         mutex: new Mutex(),
         generation: ++generationCounter,
         lastPurgeAtMs: 0,
         failureAtMs: 0,
      };
      syncMeta.set(key, meta);
   }
   return meta;
}

function markProviderFailure(meta: PackageSyncMeta): void {
   meta.failureAtMs = Date.now();
}

function inCooldown(meta: PackageSyncMeta): boolean {
   return Date.now() - meta.failureAtMs < cooldownMs;
}

/** Test seam: forget cool-down, purge, timing, and oversize state. */
export function _resetEmbeddingIndexStateForTests(): void {
   oversizeWarned.clear();
   syncMeta.clear();
   cooldownMs = PROVIDER_FAILURE_COOLDOWN_MS;
   purgeSuppressionMs = HEAL_PURGE_SUPPRESSION_MS;
}

/** Test seam: shrink the timing windows to drive real expiry in tests. */
export function _setTimingForTests(t: {
   cooldownMs?: number;
   purgeSuppressionMs?: number;
}): void {
   if (t.cooldownMs !== undefined) cooldownMs = t.cooldownMs;
   if (t.purgeSuppressionMs !== undefined)
      purgeSuppressionMs = t.purgeSuppressionMs;
}

/** Test seam: clear the per-package cool-downs, keeping sync metas. */
export function _clearProviderCooldownForTests(): void {
   for (const meta of syncMeta.values()) {
      meta.failureAtMs = 0;
   }
}

/** Test seam: observe syncMeta growth (the churn-leak pin). */
export function _syncMetaSizeForTests(): number {
   return syncMeta.size;
}

/** Test seam: read a package's lastPurgeAtMs (the heal-terminal pin). */
export function _lastPurgeAtMsForTests(
   environmentName: string,
   packageName: string,
): number | undefined {
   return syncMeta.get(metaKey(environmentName, packageName))?.lastPurgeAtMs;
}

interface ExistingRow {
   entity_kind: string;
   entity_source: string;
   entity_name: string;
   facet: string;
   content_hash: string;
   embedding_model: string;
   dims: number;
}

/**
 * Remove a deleted package's cached embeddings so package churn does not
 * grow publisher.db forever. Called best-effort from the deletion paths;
 * a missed cleanup is inert (every read is scoped by environment and
 * package) and `--init` reclaims it. Runs under the package mutex so it
 * cannot tear an in-flight sync, and bumps the generation so any live
 * instance memo stops trusting its rows.
 */
export async function deletePackageEmbeddings(
   db: DuckDBConnection,
   environmentName: string,
   packageName: string,
): Promise<void> {
   const meta = metaFor(environmentName, packageName);
   await meta.mutex.runExclusive(async () => {
      // Same orphan guard as every other mutexed writer: a second delete
      // queued on the old meta must not run again (its map removal would
      // hit a re-minted meta's entry and orphan a live sync).
      if (syncMeta.get(metaKey(environmentName, packageName)) !== meta) {
         return;
      }
      await db.run(
         `DELETE FROM entity_embeddings
          WHERE environment_name = ? AND package_name = ?`,
         [environmentName, packageName],
      );
      meta.generation = ++generationCounter;
      // Removing the entry keeps package churn from growing the map for
      // the process lifetime; it is safe because generations are
      // globally unique (a re-minted meta can never match a memo issued
      // under this one). It MUST happen inside the mutexed section: the
      // mutex hands off to the next queued waiter the moment this
      // callback returns, before any outer continuation runs, and a
      // queued sync's orphan guard has to see the entry already gone or
      // it will re-embed rows for the deleted package.
      syncMeta.delete(metaKey(environmentName, packageName));
   });
}

/**
 * Remove a deleted environment's cached embeddings. Packages with live
 * sync state are cleaned under their mutex; a final environment-wide
 * sweep covers packages never queried in this process. The mutex gives
 * mutual exclusion, not ordering: a getContext call already in flight at
 * deletion time can cold-kick a sync that lands after the cleanup and
 * rewrites its package's rows. Accepted: such rows are inert (the
 * package is gone from every serving path), are reconciled if the name
 * returns, and are reclaimed by --init.
 */
export async function deleteEnvironmentEmbeddings(
   db: DuckDBConnection,
   environmentName: string,
): Promise<void> {
   const prefix = `${environmentName}\x00`;
   for (const [key] of syncMeta) {
      if (key.startsWith(prefix)) {
         await deletePackageEmbeddings(
            db,
            environmentName,
            key.slice(prefix.length),
         );
      }
   }
   await db.run(`DELETE FROM entity_embeddings WHERE environment_name = ?`, [
      environmentName,
   ]);
}

/**
 * Bring the entity_embeddings rows for one package in line with the
 * current entity set: embed new/changed entities (content-hash diff, so
 * unchanged entities never re-embed, across restarts too), upsert them,
 * and delete rows for entities that no longer exist. Runs under the
 * package-name mutex. Returns the package generation the sync ran under.
 * Throws on provider or storage failure; partial writes are safe because
 * the hash diff self-heals on the next sync.
 */
async function syncPackageEmbeddings(
   db: DuckDBConnection,
   provider: EmbeddingProvider,
   environmentName: string,
   packageName: string,
   entities: EmbeddableEntity[],
): Promise<number> {
   const meta = metaFor(environmentName, packageName);
   return meta.mutex.runExclusive(async () => {
      // The meta may have been orphaned while this sync waited on the
      // mutex (deletePackageEmbeddings removes the map entry, e.g. the
      // package was deleted with this call already in flight). A sync
      // under an orphaned meta is no longer serialized against syncs
      // under a re-minted meta for the same name, so it must not write.
      // Aborting is safe: the caller's memo records this orphaned
      // generation, which can never match a fresh meta's (generations
      // are globally unique), so the next call re-syncs under the fresh
      // meta.
      if (syncMeta.get(metaKey(environmentName, packageName)) !== meta) {
         logger.debug(
            "[MCP Tool getContext] Skipping embedding sync for a deleted package",
            { environmentName, packageName },
         );
         return meta.generation;
      }

      // Everything here runs under the package mutex: a purge (same
      // mutex) either finished before this sync started or starts after
      // it ends, so the generation moves mid-sync only via the bump at
      // the bottom of this function.
      const existingRows = await db.all<ExistingRow>(
         `SELECT entity_kind, entity_source, entity_name, facet, content_hash,
                 embedding_model, CAST(dims AS INTEGER) AS dims
          FROM entity_embeddings
          WHERE environment_name = ? AND package_name = ?`,
         [environmentName, packageName],
      );
      const existing = new Map(
         existingRows.map((r) => [
            facetRowKey(r.entity_kind, r.entity_source, r.entity_name, r.facet),
            r,
         ]),
      );

      // A doc past MAX_DOC_CHARS cannot be covered inside the per-entity
      // embedding budget, so its tail is not embedded and cannot be retrieved
      // by its own wording. Say so once per sync, naming the entity: this is
      // the one loss chunking does not remove, and it must not be silent.
      for (const entity of entities) {
         const docChars = entity.embedDoc.replace(/\s+/g, " ").trim().length;
         if (docChars > MAX_DOC_CHARS) {
            logger.warn(
               "[MCP Tool getContext] Documentation past the per-entity limit is not embedded",
               {
                  environmentName,
                  packageName,
                  entityKind: entity.kind,
                  entitySource: sourceColumn(entity.source),
                  entityName: entity.name,
                  docChars,
                  embeddedChars: MAX_DOC_CHARS,
               },
            );
         }
      }

      // One desired row per (entity, facet). Hashing per facet is what keeps
      // the diff cheap under faceting: editing a doc re-embeds that entity's
      // doc rows and leaves its name row alone.
      const desired = entities.flatMap((entity) =>
         entityFacets(entity).map(({ facet, text: raw }) => {
            const text = prepareEmbeddingInput(raw);
            return { entity, facet, text, hash: contentHash(text) };
         }),
      );
      const desiredKeys = new Set(
         desired.map((d) =>
            facetRowKey(
               d.entity.kind,
               sourceColumn(d.entity.source),
               d.entity.name,
               d.facet,
            ),
         ),
      );

      // A row is current when its text hash and model match; a model
      // switch re-embeds in place (upsert) rather than colliding on the
      // primary key. Dimensionality is deliberately NOT part of this
      // check: `dims` stores the ACTUAL response vector length, and a
      // provider that ignores the `dimensions` request parameter (e.g.
      // Ollama) would otherwise mismatch the configured value forever
      // and re-embed the whole package on every instance swap. A real
      // dims change is caught at query time by the stale-row heal.
      const toEmbed = desired.filter((d) => {
         const row = existing.get(
            facetRowKey(
               d.entity.kind,
               sourceColumn(d.entity.source),
               d.entity.name,
               d.facet,
            ),
         );
         if (!row) return true;
         if (row.content_hash !== d.hash) return true;
         if (row.embedding_model !== provider.model) return true;
         return false;
      });

      // A sync that CHANGED rows invalidates every other instance's
      // snapshot the same way a purge does: without the bump below, a
      // call blocked behind this sync on the mutex could return its
      // pre-sync cosine snapshot (possibly empty) marked semantic. The
      // bump runs in a finally: a write failure mid-loop (disk full)
      // has already changed rows, and those torn writes must invalidate
      // snapshots too, even though this sync rejects.
      let rowsChanged = false;
      let deleted = 0;
      try {
         if (toEmbed.length > 0) {
            const vectors = await provider.embedBatch(
               toEmbed.map((d) => d.text),
               EMBEDDING_BATCH_TIMEOUT_MS,
            );
            const now = new Date().toISOString();
            for (let i = 0; i < toEmbed.length; i++) {
               const d = toEmbed[i];
               const vector = vectors[i];
               await db.run(
                  `INSERT INTO entity_embeddings (
                     environment_name, package_name, entity_kind, entity_source,
                     entity_name, facet, model_path, content_hash, embedding_model,
                     dims, embedding, updated_at
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS FLOAT[]), ?)
                   ON CONFLICT (environment_name, package_name, entity_kind, entity_source, entity_name, facet)
                   DO UPDATE SET
                     model_path = EXCLUDED.model_path,
                     content_hash = EXCLUDED.content_hash,
                     embedding_model = EXCLUDED.embedding_model,
                     dims = EXCLUDED.dims,
                     embedding = EXCLUDED.embedding,
                     updated_at = EXCLUDED.updated_at`,
                  [
                     environmentName,
                     packageName,
                     d.entity.kind,
                     sourceColumn(d.entity.source),
                     d.entity.name,
                     d.facet,
                     d.entity.modelPath,
                     d.hash,
                     provider.model,
                     vector.length,
                     JSON.stringify(vector),
                     now,
                  ],
               );
               rowsChanged = true;
            }
         }

         for (const [rowKey, row] of existing) {
            if (!desiredKeys.has(rowKey)) {
               await db.run(
                  `DELETE FROM entity_embeddings
                   WHERE environment_name = ? AND package_name = ?
                     AND entity_kind = ? AND entity_source = ? AND entity_name = ?
                     AND facet = ?`,
                  [
                     environmentName,
                     packageName,
                     row.entity_kind,
                     row.entity_source,
                     row.entity_name,
                     row.facet,
                  ],
               );
               rowsChanged = true;
               deleted++;
            }
         }
      } finally {
         if (rowsChanged) {
            meta.generation = ++generationCounter;
         }
      }

      logger.debug("[MCP Tool getContext] Synced entity embeddings", {
         environmentName,
         packageName,
         entityCount: entities.length,
         embedded: toEmbed.length,
         deleted,
      });
      return meta.generation;
   });
}

/**
 * Semantic retrieval for tier 4 of malloy_getContext. Returns ranked
 * hits, or a reason the semantic path is unavailable so the caller can
 * fall back to lexical. Never throws.
 *
 * Cold-start contract: the first call for a Package instance kicks off
 * the embedding sync in the background and reports `indexing`, so no
 * call ever waits on a bulk embed; subsequent calls are semantic once the
 * sync lands.
 */
export async function trySemanticSearch(args: {
   db: DuckDBConnection;
   provider: EmbeddingProvider;
   pkg: Package;
   environmentName: string;
   packageName: string;
   entities: EmbeddableEntity[];
   /**
    * One entry per search target that carries text, in the caller's order.
    * All of them are embedded in ONE provider request and scored in ONE pass
    * over the vector cache; see the scan below.
    */
   queries: Array<{ targetIndex: number; text: string }>;
   limit: number;
   sourceName?: string;
}): Promise<SemanticSearchResult> {
   const {
      db,
      provider,
      pkg,
      environmentName,
      packageName,
      entities,
      queries,
      limit,
      sourceName,
   } = args;

   if (entities.length > MAX_EMBEDDED_ENTITIES) {
      const key = `${environmentName}\x00${packageName}`;
      if (!oversizeWarned.has(key)) {
         oversizeWarned.add(key);
         logger.warn(
            "[MCP Tool getContext] Package exceeds the semantic index entity cap; using lexical ranking",
            {
               environmentName,
               packageName,
               entityCount: entities.length,
               cap: MAX_EMBEDDED_ENTITIES,
            },
         );
      }
      return { unavailable: "too-many-entities" };
   }

   const providerKey = `${provider.model}\x00${provider.dimensions ?? ""}`;
   const meta = metaFor(environmentName, packageName);
   // Per-package cool-down: a recent provider failure for THIS package
   // (sync, query embed, or a dims-mismatch backoff) keeps it lexical for
   // the window without touching any other package.
   if (inCooldown(meta)) {
      return { unavailable: "cooldown" };
   }
   // Captured at entry: if a concurrent heal purges rows while this call
   // is searching, the generation moves and this call must not assert
   // anything about the (now-changed) table; see the re-check below.
   const entryGeneration = meta.generation;
   let state = syncState.get(pkg);
   if (
      !state ||
      state.providerKey !== providerKey ||
      // A purge bumped the generation after this instance synced: its
      // rows are gone, so a `done` memo must not be trusted.
      (state.done && state.generation !== meta.generation)
   ) {
      // No await between the get above and the set below: single-threaded
      // JS therefore guarantees concurrent calls cannot both kick a sync
      // for the same instance. (The per-name mutex still guards the
      // cross-instance reload race.)
      const tracked: SyncState = {
         done: false,
         providerKey,
         generation: meta.generation,
      };
      // The sync promise is deliberately not stored: nothing may ever
      // await it (cold starts answer lexically); completion is observed
      // through `done` and failure through the handler below.
      syncPackageEmbeddings(
         db,
         provider,
         environmentName,
         packageName,
         entities,
      ).then(
         (generation) => {
            tracked.generation = generation;
            tracked.done = true;
         },
         (error: unknown) => {
            if (syncState.get(pkg) === tracked) {
               syncState.delete(pkg);
            }
            markProviderFailure(meta);
            logger.warn(
               "[MCP Tool getContext] Embedding sync failed; semantic ranking cooling down",
               {
                  environmentName,
                  packageName,
                  error: error instanceof Error ? error.message : String(error),
               },
            );
         },
      );
      syncState.set(pkg, tracked);
      state = tracked;
   }
   if (!state.done) {
      return { unavailable: "indexing" };
   }

   let queryVectors: number[][];
   try {
      // ONE request for every target. embedBatch already batches to 512, so N
      // targets cost one round trip rather than N -- and the round trip is
      // what sits on the latency path of every semantic call.
      queryVectors = await provider.embedBatch(
         queries.map((q) => q.text),
         EMBEDDING_QUERY_TIMEOUT_MS,
      );
   } catch (error) {
      markProviderFailure(meta);
      const message = error instanceof Error ? error.message : String(error);
      logger.warn(
         "[MCP Tool getContext] Query embedding failed; falling back to lexical ranking",
         { environmentName, packageName, error: message },
      );
      return { unavailable: "error" };
   }

   try {
      // One statement, one pass over the vectors. The hits and the two
      // cutoff counts all derive from the same scored set, so they cannot
      // disagree, and the cosine work is done once rather than once per
      // query: at the cap that is 5,000 entities and up to nine facets each.
      //
      // An entity scores as its BEST-matching facet, not as an average over
      // them. A weighted name/doc blend would need a doc vector for every
      // entity, and roughly half of a real model's entities have no doc at
      // all, so any blend either penalises them or invents a neutral fill;
      // MAX is also the only composition that stays well-defined as a doc
      // splits into a variable number of chunks. The floor applies to that
      // max, so it keeps meaning "nothing here is even weakly related", and
      // can only ever be cleared by more facets, never blocked by them.
      //
      // MATERIALIZED pins the single evaluation rather than leaving it to the
      // optimiser to inline `scored` into both branches. The LEFT JOIN ON
      // TRUE is what keeps the counts in the case that matters most: with no
      // row above the floor, one row still comes back carrying the counts and
      // null hit columns, where an inner join would have dropped exactly the
      // true negative these counts exist to explain.
      // One statement, one pass over the vectors, however many targets the
      // caller sent. The hits and the two cutoff counts all derive from the
      // same scored set, so they cannot disagree, and the cosine work is done
      // once rather than once per target: at the cap that is 5,000 entities
      // and up to nine facets each, per target.
      //
      // An entity scores as its BEST-matching facet, not as an average over
      // them. A weighted name/doc blend would need a doc vector for every
      // entity, and roughly half of a real model's entities have no doc at
      // all, so any blend either penalises them or invents a neutral fill;
      // MAX is also the only composition that stays well-defined as a doc
      // splits into a variable number of chunks. The floor applies to that
      // max, so it keeps meaning "nothing here is even weakly related", and
      // can only ever be cleared by more facets, never blocked by them.
      //
      // Across TARGETS the composition is MAX as well, and that is sound for
      // the same reason it is not sound on the lexical path: cosine is an
      // absolute scale, so 0.7 from one target means what 0.7 from another
      // does. An entity clears the floor if ANY target clears it.
      //
      // MATERIALIZED pins the single evaluation rather than leaving it to the
      // optimiser to inline `scored` into three branches. The LEFT JOIN ON
      // TRUE is what keeps the counts in the case that matters most: with no
      // row above the floor, one row still comes back carrying the counts and
      // null hit columns, where an inner join would have dropped exactly the
      // true negative these counts exist to explain.
      const vectorValues = queryVectors
         .map((_, k) => `(${k}, CAST(? AS FLOAT[]))`)
         .join(", ");
      const scan = await db.all<{
         total: number;
         below: number;
         entity_kind: string | null;
         entity_source: string | null;
         entity_name: string | null;
         best: number | null;
         target_idx: number | null;
         score: number | null;
      }>(
         `WITH q(target_idx, vec) AS (VALUES ${vectorValues}),
         scored AS MATERIALIZED (
            SELECT entity_kind, entity_source, entity_name, q.target_idx,
                   MAX(list_cosine_similarity(embedding, q.vec)) AS score
            FROM entity_embeddings, q
            WHERE environment_name = ? AND package_name = ?
              AND embedding_model = ? AND dims = ?
              ${sourceName !== undefined ? "AND entity_source = ?" : ""}
            GROUP BY entity_kind, entity_source, entity_name, q.target_idx
         ),
         per_entity AS (
            SELECT entity_kind, entity_source, entity_name, MAX(score) AS best
            FROM scored
            GROUP BY entity_kind, entity_source, entity_name
         ),
         agg AS (
            SELECT CAST(COUNT(*) AS INTEGER) AS total,
                   CAST(COUNT(*) FILTER (WHERE best < ?) AS INTEGER) AS below
            FROM per_entity
         ),
         hits AS (
            SELECT entity_kind, entity_source, entity_name, best
            FROM per_entity
            WHERE best >= ?
            ORDER BY best DESC, entity_name
            LIMIT ?
         )
         SELECT agg.total, agg.below,
                h.entity_kind, h.entity_source, h.entity_name, h.best,
                s.target_idx, s.score
         FROM agg
         LEFT JOIN hits h ON TRUE
         LEFT JOIN scored s
           ON s.entity_kind = h.entity_kind
          AND s.entity_source = h.entity_source
          AND s.entity_name = h.entity_name
         ORDER BY h.best DESC, h.entity_name, s.target_idx`,
         [
            ...queryVectors.map((v) => JSON.stringify(v)),
            environmentName,
            packageName,
            provider.model,
            queryVectors[0].length,
            ...(sourceName !== undefined ? [sourceName] : []),
            provider.minSimilarity,
            provider.minSimilarity,
            limit,
         ],
      );

      // The scan returns one row per (hit entity, target), so fold them back
      // into one hit carrying its per-target scores. Order is preserved from
      // the SQL, which already sorted by best score then name.
      const byEntity = new Map<
         string,
         {
            entity_kind: string;
            entity_source: string;
            entity_name: string;
            score: number;
            targetScores: Map<number, number>;
         }
      >();
      for (const r of scan) {
         if (r.entity_name === null) continue;
         const key = `${r.entity_kind}\x00${r.entity_source}\x00${r.entity_name}`;
         let hit = byEntity.get(key);
         if (!hit) {
            hit = {
               entity_kind: r.entity_kind as string,
               entity_source: r.entity_source as string,
               entity_name: r.entity_name as string,
               score: r.best as number,
               targetScores: new Map<number, number>(),
            };
            byEntity.set(key, hit);
         }
         if (r.target_idx !== null && r.score !== null) {
            // Back to the caller's own target index: the SQL numbered them by
            // position in `queries`, which is not the caller's numbering.
            const targetIndex = queries[r.target_idx]?.targetIndex;
            if (targetIndex !== undefined) {
               hit.targetScores.set(
                  targetIndex,
                  Math.max(hit.targetScores.get(targetIndex) ?? 0, r.score),
               );
            }
         }
      }
      const rows = [...byEntity.values()];
      // How many entities were weighed, and how many fell below the floor.
      // Without them an empty result is indistinguishable from "this package
      // models nothing like that", and we watched analysts conclude the
      // latter from the former.
      //
      // Both counts, because one alone is not interpretable. Every entity in
      // scope is either at-or-above the floor (so it could be a hit) or below
      // it (so it is counted here), which means `below` is only meaningful
      // against `total`: 7-of-52 is a tight match, 47-of-52 is a stretch, and
      // 52-of-52 is the true negative. Reporting `below` on its own invites
      // reading a large number as "too diffuse, rephrase" at exactly the
      // moment it means the opposite.
      const cutoffCounts = scan[0];

      // A sync or heal holding the package mutex right now may be
      // rewriting rows underneath the query that just ran: that snapshot
      // is unreliable (it can be partial or empty mid-write) and must
      // not be served as semantic. Completed writers are caught by the
      // generation re-check below; this catches the in-flight ones.
      // Placed after the scan so an unreliable snapshot invalidates the hits
      // and the counts together, rather than reporting a number read
      // mid-write.
      if (meta.mutex.isLocked()) {
         return { unavailable: "indexing" };
      }

      // Invariant check, every call: no row may exist that does not
      // match the provider's current (model, dims). The sync diff cannot
      // see a dims change (row hashes still match), so stale-dims rows
      // are detected here. Detecting STALE rows directly, rather than
      // inferring from an empty result, also heals the mixed state where
      // a partial sync wrote some rows at the new dimensionality and the
      // rest are stranded at the old one (an empty-result trigger would
      // never fire there, silently hiding the stranded entities forever).
      const staleRows = await db.get<{ n: number }>(
         `SELECT CAST(COUNT(*) AS INTEGER) AS n FROM entity_embeddings
          WHERE environment_name = ? AND package_name = ?
            AND NOT (embedding_model = ? AND dims = ?)`,
         [environmentName, packageName, provider.model, queryVectors[0].length],
      );
      if ((staleRows?.n ?? 0) > 0) {
         // The check-and-purge runs under the package-name mutex so it
         // cannot interleave with a sync, and the generation bump
         // invalidates every instance's memo, not just this caller's.
         // Acquired WITHOUT waiting: a held mutex means a bulk embed or
         // another heal is mid-flight, and this call must answer as
         // indexing rather than queue minutes behind it (the docstring's
         // no-call-waits-on-a-bulk-embed contract).
         let outcome: "none" | "purged" | "backoff" | "busy";
         try {
            outcome = await tryAcquire(meta.mutex).runExclusive(async () => {
               // Same orphaned-meta guard as the sync: never write under
               // a meta that deletePackageEmbeddings removed.
               if (
                  syncMeta.get(metaKey(environmentName, packageName)) !== meta
               ) {
                  return "none";
               }
               const again = await db.get<{ n: number }>(
                  `SELECT CAST(COUNT(*) AS INTEGER) AS n FROM entity_embeddings
                   WHERE environment_name = ? AND package_name = ?
                     AND NOT (embedding_model = ? AND dims = ?)`,
                  [
                     environmentName,
                     packageName,
                     provider.model,
                     queryVectors[0].length,
                  ],
               );
               if ((again?.n ?? 0) === 0) return "none";
               // Backoff: at most one purge per suppression window. Within
               // it, a fresh mismatch means the endpoint is serving
               // inconsistent dimensionalities; re-purging would re-embed
               // the whole package, so cool down and stay lexical instead.
               // lastPurgeAtMs is NOT advanced here: the window is measured
               // from the last real PURGE, and because the suppression
               // window is longer than the cooldown (see
               // HEAL_PURGE_SUPPRESSION_MS), a call arriving after the
               // cooldown clears still lands inside the suppression window
               // and backs off, so a durably-inconsistent provider does
               // NOT re-purge once per cooldown window.
               const now = Date.now();
               if (now - meta.lastPurgeAtMs < purgeSuppressionMs) {
                  markProviderFailure(meta);
                  logger.warn(
                     "[MCP Tool getContext] Repeated embedding dimensionality mismatch; the endpoint looks inconsistent, cooling down",
                     {
                        environmentName,
                        packageName,
                        model: provider.model,
                        queryDims: queryVectors[0].length,
                     },
                  );
                  return "backoff";
               }
               logger.warn(
                  "[MCP Tool getContext] Cached embeddings do not match the provider's current model/dimensions; purging stale rows and re-syncing",
                  {
                     environmentName,
                     packageName,
                     model: provider.model,
                     queryDims: queryVectors[0].length,
                     staleRows: again?.n,
                  },
               );
               // Only the stale rows: current-dims rows stay, so the
               // follow-up sync re-embeds just what was stranded.
               await db.run(
                  `DELETE FROM entity_embeddings
                   WHERE environment_name = ? AND package_name = ?
                     AND NOT (embedding_model = ? AND dims = ?)`,
                  [
                     environmentName,
                     packageName,
                     provider.model,
                     queryVectors[0].length,
                  ],
               );
               meta.lastPurgeAtMs = now;
               meta.generation = ++generationCounter;
               return "purged";
            });
         } catch (error) {
            if (error !== E_ALREADY_LOCKED) throw error;
            outcome = "busy";
         }
         if (outcome === "purged" || outcome === "busy") {
            if (outcome === "purged") {
               syncState.delete(pkg);
            }
            return { unavailable: "indexing" };
         }
         if (outcome === "backoff") {
            return { unavailable: "cooldown" };
         }
      }

      // A purge moved the generation while this call was searching: the
      // rows snapshot above is unreliable (possibly empty because a
      // concurrent heal deleted mid-search), and an unreliable empty
      // result must never be served as semantic "nothing relevant here".
      // Answer as indexing (marked lexical); the next call is consistent.
      if (meta.generation !== entryGeneration) {
         return { unavailable: "indexing" };
      }

      return {
         hits: rows.map((row) => ({
            kind: row.entity_kind,
            source: row.entity_source === "" ? undefined : row.entity_source,
            name: row.entity_name,
            score: row.score,
            targetScores: row.targetScores,
         })),
         belowCutoffCount: cutoffCounts?.below ?? 0,
         totalEntities: cutoffCounts?.total ?? 0,
      };
   } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      logger.warn(
         "[MCP Tool getContext] Semantic search failed; falling back to lexical ranking",
         { environmentName, packageName, error: message },
      );
      return { unavailable: "error" };
   }
}

/** The identity fields getEmbeddingIndexStatus needs from a live entity. */
export interface IndexedEntity {
   kind: string;
   name: string;
   source: string | undefined;
}

/** What a package's semantic index is currently doing. */
export interface EmbeddingIndexStatus {
   status: "indexing" | "ready" | "cooldown" | "too-many-entities";
   /**
    * Rows cached for this package under the provider's CURRENT model, across
    * all entities and facets. Rows left by an earlier model are excluded,
    * because the search path excludes them too and a number that counted them
    * would not agree with `status`.
    */
   embeddedRows: number;
   /** Entities the package currently exposes to retrieval. */
   totalEntities: number;
   /** Current entities with at least one usable vector. */
   embeddedEntities: number;
   /** Most recent row write, absent when nothing is cached yet. */
   lastSyncedAt?: string;
}

/**
 * Report a package's embedding-index state for the REST Package resource.
 *
 * The state existed only as a debug log line ("Synced entity embeddings"),
 * so anything wanting to wait for a warm index — a harness measuring
 * retrieval, an operator checking an upgrade re-embedded — had to scrape the
 * server log. This reads the same state the search path uses.
 *
 * Derived, never authoritative: it takes no mutex and writes nothing, so
 * calling it cannot perturb or serialize behind a sync in flight.
 */
export async function getEmbeddingIndexStatus(
   db: DuckDBConnection,
   provider: EmbeddingProvider,
   environmentName: string,
   packageName: string,
   entities: IndexedEntity[],
): Promise<EmbeddingIndexStatus> {
   const entityCount = entities.length;
   // Scoped to the provider's current model, because the search path is
   // (see trySemanticSearch) and the stale-row heal purges anything else.
   // Counting rows this query would reject is what let an index report ready
   // while retrieval had nothing to serve.
   const scope = [environmentName, packageName, provider.model];
   const dimsClause = provider.dimensions !== undefined ? " AND dims = ?" : "";
   const dimsParam =
      provider.dimensions !== undefined ? [provider.dimensions] : [];

   const row = await db.get<{ n: number; last: string | null }>(
      `SELECT CAST(COUNT(*) AS INTEGER) AS n,
              CAST(MAX(updated_at) AS VARCHAR) AS last
       FROM entity_embeddings
       WHERE environment_name = ? AND package_name = ? AND embedding_model = ?${dimsClause}`,
      [...scope, ...dimsParam],
   );
   const embeddedRows = row?.n ?? 0;
   const lastSyncedAt = row?.last ?? undefined;

   // Which entities are covered, not just how many rows exist. A row count
   // alone cannot see an edit that removes two entities and adds two others:
   // the total is unchanged while the two new ones have no vector at all.
   const covered = await db.all<{
      entity_kind: string;
      entity_source: string;
      entity_name: string;
   }>(
      `SELECT DISTINCT entity_kind, entity_source, entity_name
       FROM entity_embeddings
       WHERE environment_name = ? AND package_name = ? AND embedding_model = ?${dimsClause}`,
      [...scope, ...dimsParam],
   );
   const coveredKeys = new Set(
      covered.map((r) =>
         entityRowKey(r.entity_kind, r.entity_source, r.entity_name),
      ),
   );
   const embeddedEntities = entities.filter((e) =>
      coveredKeys.has(entityRowKey(e.kind, sourceColumn(e.source), e.name)),
   ).length;

   const meta = syncMeta.get(metaKey(environmentName, packageName));
   const status: EmbeddingIndexStatus["status"] =
      entityCount > MAX_EMBEDDED_ENTITIES
         ? "too-many-entities"
         : meta && inCooldown(meta)
           ? "cooldown"
           : // Every current entity has a usable vector and nothing is
             // mid-write: the cache is serving what retrieval will read.
             embeddedEntities >= entityCount && !meta?.mutex.isLocked()
             ? "ready"
             : "indexing";

   return {
      status,
      embeddedRows,
      totalEntities: entityCount,
      embeddedEntities,
      ...(lastSyncedAt ? { lastSyncedAt } : {}),
   };
}
