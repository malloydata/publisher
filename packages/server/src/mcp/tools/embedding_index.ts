// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { createHash } from "crypto";
import type { EmbeddingModel } from "../../providers/types";
import { E_ALREADY_LOCKED, Mutex, tryAcquire } from "async-mutex";
import { logger } from "../../logger";
import { DuckDBConnection } from "../../storage/duckdb/DuckDBConnection";
import type { Package } from "../../service/package";
import {
   DEFAULT_EMBEDDING_RETRY,
   EmbeddingRetryPolicy,
   EMBEDDING_BATCH_TIMEOUT_MS,
   EMBEDDING_QUERY_TIMEOUT_MS,
   MAX_EMBED_BATCH_SIZE,
   MAX_EMBED_INPUT_CHARS,
   prepareEmbeddingInput,
} from "../../service/embedding_provider";
import { HttpRequestError, publicMessage } from "../../service/http_retry";
import { yieldToEventLoop } from "../../service/event_loop_yield";
import { DEFAULT_SEMANTIC_INDEX_MAX_ENTITIES } from "../../config";
import type { PackageRepresentation } from "../../service/package_retrieval";
import { embeddingSyncQueue } from "./embedding_sync_queue";
import { SyncRequestBudget } from "./get_context_llm";
import { indexSettingsOf, type IndexSettings } from "./index_settings";
import {
   PROVIDER_FAILURE_COOLDOWN_MS,
   providerKeyFor,
} from "./retrieval_shared";
import {
   KeyphraseStageError,
   countNeedingLlm,
   deleteEnvironmentKeyphrases,
   deletePackageKeyphrases,
   keyphraseInputsDigest,
   keyphraseKey,
   resolveKeyphrases,
   type KeyphraseProgress,
} from "./keyphrases";
import {
   SourceSummaryStageError,
   countSummarizableSources,
   deleteEnvironmentSourceSummaries,
   deletePackageSourceSummaries,
   resolveSourceSummaries,
   sourceSummaryInputsDigest,
   type SourceSummaryProgress,
} from "./source_summaries";

/**
 * Minimum cosine similarity for a semantic hit, so a query about something the
 * package does not model returns nothing rather than the k least-unrelated
 * entities. It travels on the provider (`provider.minSimilarity`) because
 * cosine similarity is not calibrated across embedding models.
 */
export { DEFAULT_EMBEDDING_MIN_SIMILARITY } from "../../config";

/**
 * The most entities a package may have and still be embedded
 * (`retrieval.indexing.maxEntities`; see DEFAULT_SEMANTIC_INDEX_MAX_ENTITIES).
 * Set once at startup by {@link setMaxEmbeddedEntities}.
 */
let maxEmbeddedEntities = DEFAULT_SEMANTIC_INDEX_MAX_ENTITIES;

export function getMaxEmbeddedEntities(): number {
   return maxEmbeddedEntities;
}

/** Set the entity cap. Called once at server start with the configured value. */
export function setMaxEmbeddedEntities(cap: number): void {
   maxEmbeddedEntities = cap;
}
/**
 * After a dims-mismatch purge, suppress further purges for this long. It MUST
 * exceed PROVIDER_FAILURE_COOLDOWN_MS: with equal windows the first call past
 * the cooldown would always find this guard expired and re-purge, re-embedding
 * the package once per cooldown window forever. Not "never re-purge", which
 * would strand the cache if the provider settles on a new dimensionality.
 */
export const HEAL_PURGE_SUPPRESSION_MS = 10 * PROVIDER_FAILURE_COOLDOWN_MS;

/** The subset of the tool's Entity shape the index needs. */
export interface EmbeddableEntity {
   kind: string;
   name: string;
   source: string | undefined;
   modelPath: string;
   // Doc text only (see package_entities docOnlyText).
   // Deliberately NOT the response `doc`, which can fall back to raw
   // annotation lines and would leak #(authorize) predicates to the
   // embedding provider.
   embedDoc: string;
   /** Malloy type of a dimension or measure; absent on other kinds. */
   dataType?: string;
   /** Authored expression text; sent to the LLM only under the `full` egress preset. */
   code?: string;
   /** Join cardinality, on `kind: "join"` entities only. */
   relationship?: string;
   /**
    * On a `join`: the source it joins to, when the compiled model names one.
    * Read by the source summary to nest a joined source's fields.
    */
   joinTarget?: string;
   /** The file that defines `joinTarget`: the name alone is not an identity. */
   joinTargetModelPath?: string;
   /** On a joined copy of a field: the join path that reaches it (`buyer.origin`). */
   joinPath?: string;
}

export interface SemanticHit {
   kind: string;
   name: string;
   source: string | undefined;
   /** Best score across every target, which is what the hit is ranked on. */
   score: number;
   /** Score per caller target index; fills the response's matched_targets. */
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
         * Entities in scope (source scope and claimable kinds) whose best
         * facet scored below the floor. Read against {@link totalEntities}:
         * the two partition the scope, so `belowCutoffCount === totalEntities`
         * (with `hits: []`) is the true negative, and 0 with no hits only
         * means an empty package.
         */
        belowCutoffCount: number;
        /** Entities weighed for this query, under the same scope as `hits`. */
        totalEntities: number;
        /**
         * Per source: entities (sources excluded) that cleared the floor but
         * were cut from `hits` by the per-source window. The window drops them
         * in SQL, so callers cannot count them from `hits`.
         */
        cutBySource: Map<string, number>;
     }
   | { unavailable: SemanticUnavailableReason };

/**
 * Turn an identifier into the words a person would search with:
 * `dep_delay` -> "dep delay", `totalSales` -> "total sales".
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
 * An entity's doc-facet text: humanized name plus its doc text (the name
 * facet is embedded separately; see entityFacets). A punctuation-only
 * identifier (`_` is legal Malloy) humanizes to nothing, so it falls back to
 * the raw name.
 */
export function embeddingText(entity: EmbeddableEntity): string {
   const name = humanizeName(entity.name) || entity.name;
   return entity.embedDoc ? `${name}: ${entity.embedDoc}` : name;
}

/** The facet holding an entity's own name, embedded free of doc text. */
export const NAME_FACET = "name";

/**
 * Target and hard ceiling for one doc chunk, in characters, and the most
 * chunks one doc may produce: small enough that a single fact keeps a vector
 * of its own, capped so one lavishly-documented entity costs at most
 * MAX_DOC_CHUNKS + 1 embeddings.
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
 * The most doc text one entity can contribute, across every chunk. A fixed
 * per-entity embedding budget cannot cover an unbounded doc; this makes the
 * cut a stated bound, and syncPackageEmbeddings warns when a doc exceeds it.
 */
export const MAX_DOC_CHARS = MAX_DOC_CHUNKS * CHUNK_HARD_MAX_CHARS;

/** One embeddable unit of an entity: its name, or a chunk of its doc. */
export interface EntityFacet {
   facet: string;
   text: string;
}

/**
 * Split doc text into chunks that each keep their own embedding, so a single
 * fact in a long source doc (a grain caveat, a population rule) stays
 * reachable by its own wording instead of being averaged away.
 *
 * Packs whole sentences greedily toward a target that grows with the doc, so
 * MAX_DOC_CHUNKS chunks cover all of it. A chunk too long to embed once
 * prefixed is split further by entityFacets, so coverage stops only at
 * MAX_DOC_CHARS.
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
   // tail. A fold too long to embed is split by entityFacets, not truncated.
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
 * The name is embedded on its own so it matches the plain concept name at
 * full strength however much documentation the entity carries; scoring takes
 * the best facet (see trySemanticSearch), so more documentation can only add
 * recall. A long doc splits across `doc:N` facets (see chunkDoc), each
 * prefixed with the entity's name to anchor a bare fact to its subject.
 */
export function entityFacets(entity: EmbeddableEntity): EntityFacet[] {
   const name = humanizeName(entity.name) || entity.name;
   const facets: EntityFacet[] = [{ facet: NAME_FACET, text: name }];
   // Only here is the prefix length known, so only here can a chunk be checked
   // against the provider's input cap. One that does not fit is split across
   // facets rather than cut.
   const room = MAX_EMBED_INPUT_CHARS - name.length - 2;
   let i = 0;
   for (const chunk of chunkDoc(entity.embedDoc)) {
      for (const piece of splitToFit(chunk, room)) {
         facets.push({ facet: `doc:${i++}`, text: `${name}: ${piece}` });
      }
   }
   return facets;
}

/** The only row an entity has under the `single` representation. */
export const SINGLE_FACET = "single";
/** An LLM-written (or short-description) search phrase, as an extra `facets` row. */
export const KEYPHRASE_FACET = "keyphrase";

/**
 * The rows one entity is embedded as, by representation.
 *
 * `facets`: the name, then the doc chunks, plus a `keyphrase` row when the
 * entity has one.
 *
 * `single`: ONE row whose text is the keyphrase, else the doc, else the
 * humanized name. The name is left out when there is a doc or keyphrase,
 * because mixing the identifier in dilutes the meaning. A long doc is cut at
 * the provider's input limit, not chunked.
 */
export function entityRows(
   entity: EmbeddableEntity,
   representation: PackageRepresentation,
   keyphrase?: string,
): EntityFacet[] {
   if (representation === "facets") {
      const rows = entityFacets(entity);
      if (keyphrase) rows.push({ facet: KEYPHRASE_FACET, text: keyphrase });
      return rows;
   }
   const doc = entity.embedDoc.replace(/\s+/g, " ").trim();
   const text = keyphrase || doc || humanizeName(entity.name) || entity.name;
   return [{ facet: SINGLE_FACET, text }];
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
 * Separator for the composite in-memory keys below. NUL, not `|`: a
 * backtick-quoted Malloy identifier may contain `|`, so two entities could
 * collide on one key and the sync diff would silently skip a delete or a
 * re-embed. A control character cannot appear in an identifier.
 */
export const KEY_SEPARATOR = "\u0000";

/**
 * The stable key for one entity, shared by the index dedup, row identity and
 * the tool's result mapping. Pass "" for a sourceless entity. In-memory only:
 * the database keys on the three columns directly.
 */
export function entityRowKey(
   kind: string,
   source: string,
   name: string,
): string {
   return [kind, source, name].join(KEY_SEPARATOR);
}

/**
 * One entry per (kind, source, name), which is the identity the vector cache
 * keys on.
 *
 * A source is queryable at every model path that resolves it, so the entity
 * list carries one entry PER PATH. The embedding rows have no model_path in
 * their key, so duplicates would embed identical text per path and inflate the
 * count checked against the cap, letting a package exceed it because a file
 * was added rather than because the model grew. The response fan-out maps hits
 * back onto the per-path entities itself (see get_context_tool). First wins.
 */
export function uniqueByEntityKey<
   T extends { kind: string; name: string; source: string | undefined },
>(entities: readonly T[]): T[] {
   const seen = new Set<string>();
   return entities.filter((e) => {
      const key = entityRowKey(e.kind, sourceColumn(e.source), e.name);
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
   });
}

/**
 * The stable key for one embedding ROW: an entity plus the facet it holds.
 * One format keeps the diff, the upsert and the delete from drifting apart.
 */
export function facetRowKey(
   kind: string,
   source: string,
   name: string,
   facet: string,
): string {
   return entityRowKey(kind, source, name) + KEY_SEPARATOR + facet;
}

/**
 * What decides an entity's rows: the package's representation and, once they
 * exist, its keyphrases by entity key. The readiness fingerprint is computed
 * with no keyphrases (they come from a store and a model, and the search path
 * must not wait on either); {@link IndexSettings.key} stands in for them.
 */
interface RowPlan {
   representation: PackageRepresentation;
   keyphrases?: ReadonlyMap<string, string>;
}

/** One desired embedding row: an entity's facet, its text, and that text's hash. */
interface DesiredFacet {
   entity: EmbeddableEntity;
   facet: string;
   text: string;
   hash: string;
}

/**
 * The full set of rows a package's entities SHOULD have, one per (entity,
 * facet), each with the hash of the exact text it embeds.
 *
 * The sync diffs the table against it and desiredFingerprint folds it into the
 * readiness test, so the two cannot disagree; if they did, a package would be
 * stranded as `indexing` or claim a warm index over stale rows. Hashing per
 * facet means editing a doc re-embeds only that entity's doc rows.
 */
function desiredFacets(
   entities: EmbeddableEntity[],
   plan: RowPlan,
): DesiredFacet[] {
   return entities.flatMap((entity) =>
      entityRows(
         entity,
         plan.representation,
         plan.keyphrases?.get(keyphraseKey(entity)),
      ).map(({ facet, text: raw }) => {
         const text = prepareEmbeddingInput(raw);
         return { entity, facet, text, hash: contentHash(text) };
      }),
   );
}

/**
 * Rows as the sync stores them when the embedding model wants text put before
 * indexed text (`retrieval.embedding.documentPrefix`). The hash is then taken
 * over prefix + text, which is what is sent; with no prefix the rows are
 * returned unchanged, so turning the feature off costs no re-embed.
 */
function withDocumentPrefix(
   desired: DesiredFacet[],
   prefix: string,
): DesiredFacet[] {
   if (prefix === "") return desired;
   return desired.map((d) => ({ ...d, hash: contentHash(prefix + d.text) }));
}

/**
 * The fingerprint's input: every desired row's key and content hash. Two
 * entity sets share a fingerprint exactly when a sync over either is a no-op
 * for the other, which lets a reloaded package keep the replaced instance's
 * index. Sorted before hashing so entity order cannot read as a change.
 */
function desiredRowStrings(desired: readonly DesiredFacet[]): string[] {
   return desired.map(
      (d) =>
         facetRowKey(
            d.entity.kind,
            sourceColumn(d.entity.source),
            d.entity.name,
            d.facet,
         ) +
         KEY_SEPARATOR +
         d.hash,
   );
}

function fingerprintOfRows(rows: string[]): string {
   rows.sort();
   return contentHash(rows.join("\n"));
}

/**
 * {@link desiredFacets} over a large entity list, yielding to the event loop
 * every FINGERPRINT_CHUNK_ENTITIES entities: the work is synchronous and
 * proportional to the package, and runs while the server answers questions.
 */
async function desiredFacetsChunked(
   entities: EmbeddableEntity[],
   plan: RowPlan,
): Promise<DesiredFacet[]> {
   const desired: DesiredFacet[] = [];
   for (let i = 0; i < entities.length; i += FINGERPRINT_CHUNK_ENTITIES) {
      desired.push(
         ...desiredFacets(
            entities.slice(i, i + FINGERPRINT_CHUNK_ENTITIES),
            plan,
         ),
      );
      if (i + FINGERPRINT_CHUNK_ENTITIES < entities.length) {
         await yieldToEventLoop();
      }
   }
   return desired;
}

/** {@link desiredFingerprint}, yielding while it builds the row strings. */
async function desiredFingerprintChunked(
   desired: readonly DesiredFacet[],
): Promise<string> {
   const rows: string[] = [];
   const chunk = FINGERPRINT_CHUNK_ENTITIES * 4;
   for (let i = 0; i < desired.length; i += chunk) {
      rows.push(...desiredRowStrings(desired.slice(i, i + chunk)));
      // Only between chunks: a small package never yields, so its search
      // cannot be overtaken by a sync finishing meanwhile (which would move
      // the captured generation and answer `indexing` once more).
      if (i + chunk < desired.length) await yieldToEventLoop();
   }
   return fingerprintOfRows(rows);
}

// Sync state, two layers.
//
// Per package NAME (`syncMeta`):
// - `mutex` serializes every read-diff-write section (sync AND the heal's
//   purge), so a reload racing an in-flight sync cannot tear rows.
// - `generation` moves on every purge, so a `synced` fact cannot survive a
//   purge over a now-empty table.
// - `lastPurgeAtMs` bounds how often the heal may purge; a backend serving
//   inconsistent dimensionalities would otherwise loop purge / re-embed.
// - `failureAtMs` is the provider cool-down, per package so one package's
//   timeout or dims-mismatch does not cool down every healthy package. A
//   provider-wide failure (network, timeout, 429, 5xx, rejected credentials)
//   also opens `providerBreaker`, so packages queued behind it fail fast
//   instead of each sending a request.
// - `synced` is keyed on CONTENT (the desired-row fingerprint), not Package
//   identity: every reload allocates a new instance, and a reload that changed
//   nothing must keep the warm index. `providerKey` records the model/dims
//   config, so switching EMBEDDING_MODEL or EMBEDDING_DIMENSIONS re-syncs.
//
// Per package INSTANCE (`syncState`, WeakMap): the in-flight marker only, so
// concurrent calls on one instance kick at most one sync. Cleared when the
// sync settles (resolved or rejected), so a later purge or retry can re-kick.
interface SyncedFact {
   /** {@link desiredFingerprint} of the entity set this sync covered. */
   fingerprint: string;
   providerKey: string;
   /** The generation the sync ran under; a purge invalidates the fact. */
   generation: number;
}
/** The sync stages that can fail on their own, named in the status and the log. */
export type SyncStage = "keyphrase" | "source_summary";

interface PackageSyncMeta {
   mutex: Mutex;
   generation: number;
   lastPurgeAtMs: number;
   failureAtMs: number;
   /** Why the provider last failed for this package. Read by the status. */
   lastError?: string;
   /** The sync stage `lastError` came from, when it was not the embedding call. */
   lastErrorStage?: SyncStage;
   /** Where the keyphrase step is, from the latest sync that ran it. */
   keyphraseProgress?: KeyphraseProgress;
   /** Where the source summary step is, from the latest sync that ran it. */
   sourceSummaryProgress?: SourceSummaryProgress;
   /** When the sync now running began, absent while none is running. */
   syncStartedAtMs?: number;
   /**
    * Set when the package's delete starts, before it has the mutex. A running
    * sync stops at its next request or retry instead of finishing first.
    */
   deleting?: boolean;
   /**
    * The content and settings of a sync that failed with a client error the
    * vendor will not change its answer to. While the package is still this
    * content under this model it is not synced again; see isClientError.
    */
   permanentFailure?: { fingerprint: string; providerKey: string };
   synced?: SyncedFact;
}
/** A sync in flight for one Package instance, keyed by what it covers. */
interface SyncState {
   fingerprint: string;
   providerKey: string;
}
const syncState = new WeakMap<Package, SyncState>();
/** Package instances whose load-time sync has been queued; see enqueuePackageSync. */
const loadQueued = new WeakSet<Package>();
const syncMeta = new Map<string, PackageSyncMeta>();
// Generations are drawn from one global counter, never incremented locally,
// so deleting a syncMeta entry is safe: a re-minted meta can never coincide
// with a generation recorded under the old one and trust a `synced` fact over
// a table the deletion just emptied.
let generationCounter = 0;
const oversizeWarned = new Set<string>();

// Mutable only so tests can drive real expiry with small sleeps; production
// uses the exported constants.
let cooldownMs = PROVIDER_FAILURE_COOLDOWN_MS;

/**
 * The longest one package's sync may hold the process-wide queue. Each
 * request has its own timeout and retries are bounded, but a slow provider
 * that keeps answering can stretch a large package indefinitely, and every
 * other package waits behind it. Checked between batches and retries.
 */
export const SYNC_JOB_DEADLINE_MS = 15 * 60_000;
let syncDeadlineMs = SYNC_JOB_DEADLINE_MS;
let purgeSuppressionMs = HEAL_PURGE_SUPPRESSION_MS;
// Mutable only so tests can inject a fake sleep and a small batch.
let syncRetryPolicy: EmbeddingRetryPolicy = DEFAULT_EMBEDDING_RETRY;
let syncBatchSize = MAX_EMBED_BATCH_SIZE;

function metaKey(environmentName: string, packageName: string): string {
   return `${environmentName}\x00${packageName}`;
}

/**
 * Thrown inside a sync to stop it because its package is being deleted. It is
 * not a provider failure, so the caller starts no cool-down and reports no
 * error.
 */
class SyncAbandonedError extends Error {
   constructor() {
      super("the package was deleted while its embedding sync was running");
      this.name = "SyncAbandonedError";
   }
}

/**
 * Thrown inside a sync that has run past its time limit. Batches already
 * written stay, so the next sync resumes where this one stopped.
 */
class SyncDeadlineError extends Error {
   constructor() {
      super(
         "The embedding sync ran past its time limit. Rows already saved are kept, and it resumes after the cooldown.",
      );
      this.name = "SyncDeadlineError";
   }
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

function markProviderFailure(
   meta: PackageSyncMeta,
   message: string,
   stage?: SyncStage,
): void {
   meta.failureAtMs = Date.now();
   meta.lastError = message;
   meta.lastErrorStage = stage;
}

/**
 * What `lastError` shows on the package status. A provider failure carries
 * its own public wording (see publicMessage), which names no endpoint; any
 * other failure gets fixed wording, since its text can name a file or a
 * statement. The full error goes to the log.
 */
function publicStatusMessage(error: unknown): string {
   if (
      error instanceof HttpRequestError ||
      error instanceof KeyphraseStageError ||
      error instanceof SourceSummaryStageError
   ) {
      // Stage error messages are written for the status; vendor text comes
      // through publicMessage.
      return publicMessage(error);
   }
   if (error instanceof SyncDeadlineError) return error.message;
   return "The embedding sync failed on an unusable response or a write error. See the server log.";
}

/** Whether `error` says the provider itself is down, not one package's input. */
function isProviderWideFailure(error: unknown): boolean {
   return (
      error instanceof HttpRequestError &&
      (error.retryable || error.status === 401 || error.status === 403)
   );
}

/**
 * Each provider's last provider-wide failure. While one is recent, queued
 * syncs record it as their cause and send nothing. Keyed by the provider
 * object (one endpoint), not by model name, so a different endpoint serving
 * the same model is not held back by this one's outage.
 */
let providerBreaker = new WeakMap<
   EmbeddingModel,
   { atMs: number; message: string }
>();

function openBreaker(provider: EmbeddingModel, message: string): void {
   providerBreaker.set(provider, { atMs: Date.now(), message });
}

function openBreakerFor(
   provider: EmbeddingModel,
): { atMs: number; message: string } | undefined {
   const open = providerBreaker.get(provider);
   if (!open) return undefined;
   if (Date.now() - open.atMs >= cooldownMs) {
      providerBreaker.delete(provider);
      return undefined;
   }
   return open;
}

function inCooldown(meta: PackageSyncMeta): boolean {
   return Date.now() - meta.failureAtMs < cooldownMs;
}

/**
 * Whether a failure is a 4xx that waiting does not fix: the vendor refused the
 * request itself (a prompt past the context window, a bad model name). 408 and
 * 429 can clear; 401 and 403 can be fixed by rotating a credential without the
 * package changing, so they keep their cool-down and retry. Looks through the
 * cause chain, because a failed LLM step wraps the HTTP failure.
 */
function isClientError(error: unknown): boolean {
   let cause: unknown = error;
   for (let depth = 0; cause instanceof Error && depth < 5; depth++) {
      if (cause instanceof HttpRequestError) {
         return (
            !cause.retryable &&
            cause.status !== undefined &&
            cause.status >= 400 &&
            cause.status < 500 &&
            cause.status !== 401 &&
            cause.status !== 403
         );
      }
      cause = cause.cause;
   }
   return false;
}

/** The last sync failed with a client error, and the package has not changed since. */
function isPermanentlyFailed(
   meta: PackageSyncMeta,
   fingerprint: string,
   providerKey: string,
): boolean {
   const failed = meta.permanentFailure;
   return (
      failed !== undefined &&
      failed.fingerprint === fingerprint &&
      failed.providerKey === providerKey
   );
}

/** What one pass over an entity set's desired rows yields. */
interface DesiredSummary {
   fingerprint: string;
   /** Rows the sync wants cached for this set: the progress denominator. */
   rows: number;
   /** Sources that get a summary; 0 when summaries are off. */
   summarySources: number;
}

/**
 * Summaries already computed (or being computed), keyed on the frozen entity
 * array they describe. The promise is cached, not the value, so two callers
 * that arrive during the first computation share it. See desiredSummaryFor.
 */
const fingerprintCache = new WeakMap<
   readonly EmbeddableEntity[],
   Map<string, Promise<DesiredSummary>>
>();

/** Entities hashed between two yields to the event loop. */
const FINGERPRINT_CHUNK_ENTITIES = 250;

/**
 * The fingerprint of an entity set: the value the sync records as
 * `meta.synced.fingerprint`. Deduped here, as the sync's input is, so the
 * caller may pass the per-path list.
 *
 * It costs one entityFacets pass plus a sha256 per facet (over 500ms for a
 * large, heavily documented package), computed in yielding chunks and cached,
 * because the search path and the status endpoint need it on every call.
 *
 * Cached on the ARRAY, and only a frozen one: get_context passes the same
 * frozen array per Package on every call. Keying on the Package would hand a
 * caller passing a SUBSET the full set's fingerprint, silently reading as
 * synced; an unfrozen array could change after caching.
 */
function desiredSummaryFor(
   entities: readonly EmbeddableEntity[],
   settings: IndexSettings,
): Promise<DesiredSummary> {
   const frozen = Object.isFrozen(entities);
   let byKey = frozen ? fingerprintCache.get(entities) : undefined;
   const cached = byKey?.get(settings.key);
   if (cached !== undefined) return cached;
   const computed = computeDesiredSummary(entities, settings);
   if (frozen) {
      if (!byKey) {
         byKey = new Map();
         fingerprintCache.set(entities, byKey);
      }
      byKey.set(settings.key, computed);
      // A failure must not be cached for the life of the array.
      const slot = byKey;
      computed.catch(() => slot.delete(settings.key));
   }
   return computed;
}

async function computeDesiredSummary(
   entities: readonly EmbeddableEntity[],
   settings: IndexSettings,
): Promise<DesiredSummary> {
   const unique = uniqueByEntityKey(entities);
   // No keyphrases here: see RowPlan.
   const plan: RowPlan = { representation: settings.representation };
   const desired = await desiredFacetsChunked(unique, plan);
   return {
      fingerprint: contentHash(
         settings.key +
            "\n" +
            // The fields only the keyphrase step reads (data type, code).
            keyphraseInputsDigest(unique, settings.keyphrases) +
            "\n" +
            // What the source summary step shows the model; empty when off.
            // Read from the per-model-file list, not the deduplicated one:
            // summaries are per file, and the sync and a request must see
            // the same list or a stored summary fails its check every time.
            sourceSummaryInputsDigest(entities, settings.sourceSummary) +
            "\n" +
            (await desiredFingerprintChunked(desired)),
      ),
      rows: desired.length,
      summarySources: settings.sourceSummary
         ? countSummarizableSources(entities)
         : 0,
   };
}

async function fingerprintFor(
   entities: readonly EmbeddableEntity[],
   settings: IndexSettings,
): Promise<string> {
   return (await desiredSummaryFor(entities, settings)).fingerprint;
}

/**
 * Whether a completed sync covers exactly this content under this provider
 * config and still stands: the fingerprint says the text matches,
 * `providerKey` that the configured model embedded it, and the generation
 * that no purge has emptied the table since.
 */
function isSynced(
   meta: PackageSyncMeta,
   fingerprint: string,
   providerKey: string,
): boolean {
   const synced = meta.synced;
   return (
      synced !== undefined &&
      synced.fingerprint === fingerprint &&
      synced.providerKey === providerKey &&
      synced.generation === meta.generation
   );
}

/** Test seam: forget cool-down, purge, timing, and oversize state. */
export function _resetEmbeddingIndexStateForTests(): void {
   oversizeWarned.clear();
   syncMeta.clear();
   cooldownMs = PROVIDER_FAILURE_COOLDOWN_MS;
   syncDeadlineMs = SYNC_JOB_DEADLINE_MS;
   providerBreaker = new WeakMap();
   purgeSuppressionMs = HEAL_PURGE_SUPPRESSION_MS;
   // Tests never wait out a backoff; retry specs install their own policy
   // through _setSyncRetryForTests.
   syncRetryPolicy = { ...DEFAULT_EMBEDDING_RETRY, sleep: async () => {} };
   syncBatchSize = MAX_EMBED_BATCH_SIZE;
   maxEmbeddedEntities = DEFAULT_SEMANTIC_INDEX_MAX_ENTITIES;
}

/** Test seam: shrink the timing windows to drive real expiry in tests. */
export function _setTimingForTests(t: {
   cooldownMs?: number;
   purgeSuppressionMs?: number;
   syncDeadlineMs?: number;
}): void {
   if (t.cooldownMs !== undefined) cooldownMs = t.cooldownMs;
   if (t.syncDeadlineMs !== undefined) syncDeadlineMs = t.syncDeadlineMs;
   if (t.purgeSuppressionMs !== undefined)
      purgeSuppressionMs = t.purgeSuppressionMs;
}

/** Test seam: the sync's retry policy and rows-per-request. */
export function _setSyncRetryForTests(t: {
   policy?: Partial<EmbeddingRetryPolicy>;
   batchSize?: number;
}): void {
   if (t.policy) syncRetryPolicy = { ...syncRetryPolicy, ...t.policy };
   if (t.batchSize !== undefined) syncBatchSize = t.batchSize;
}

/** Test seam: clear the per-package cool-downs, keeping sync metas. */
export function _clearProviderCooldownForTests(): void {
   for (const meta of syncMeta.values()) {
      meta.failureAtMs = 0;
   }
   providerBreaker = new WeakMap();
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
 * grow publisher.db forever. Best-effort: a missed cleanup is inert (every
 * read is scoped by environment and package) and `--init` reclaims it. Runs
 * under the package mutex and bumps the generation.
 *
 * It marks the package as deleting before waiting for the mutex, so a running
 * sync stops at its next request instead of finishing a minutes-long sync for
 * a package nobody can query.
 */
export async function deletePackageEmbeddings(
   db: DuckDBConnection,
   environmentName: string,
   packageName: string,
): Promise<void> {
   const meta = metaFor(environmentName, packageName);
   meta.deleting = true;
   try {
      await deleteUnderMutex(db, environmentName, packageName, meta);
   } catch (error) {
      // The rows are still there; leaving the flag set would make every
      // later sync for the package abandon itself.
      if (syncMeta.get(metaKey(environmentName, packageName)) === meta) {
         meta.deleting = false;
      }
      throw error;
   }
}

async function deleteUnderMutex(
   db: DuckDBConnection,
   environmentName: string,
   packageName: string,
   meta: PackageSyncMeta,
): Promise<void> {
   await meta.mutex.runExclusive(async () => {
      // Orphan guard: a second delete queued on the old meta must not run
      // (its map removal would hit a re-minted meta and orphan a live sync).
      if (syncMeta.get(metaKey(environmentName, packageName)) !== meta) {
         return;
      }
      await db.run(
         `DELETE FROM entity_embeddings
          WHERE environment_name = ? AND package_name = ?`,
         [environmentName, packageName],
      );
      await deletePackageKeyphrases(db, environmentName, packageName);
      await deletePackageSourceSummaries(db, environmentName, packageName);
      meta.generation = ++generationCounter;
      // Removing the entry keeps churn from growing the map; safe because
      // generations are globally unique. It MUST happen inside the mutexed
      // section: the mutex hands off to the next waiter as soon as this
      // callback returns, and a queued sync's orphan guard has to see the
      // entry gone or it will re-embed rows for the deleted package.
      syncMeta.delete(metaKey(environmentName, packageName));
   });
}

/**
 * Remove a deleted environment's cached embeddings: packages with live sync
 * state under their mutex, then an environment-wide sweep for the rest. The
 * mutex gives exclusion, not ordering, so a getContext call in flight at
 * deletion time can still write rows after the cleanup. Accepted: such rows
 * are inert, reconciled if the name returns, and reclaimed by --init.
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
   await deleteEnvironmentKeyphrases(db, environmentName);
   await deleteEnvironmentSourceSummaries(db, environmentName);
}

/**
 * Write one batch of embedded rows with a single multi-row INSERT, so the
 * batch lands whole or not at all. BEGIN/COMMIT around separate statements is
 * not an option on this shared connection: other callers' statements would
 * run inside it. Rows are unique by key, which a multi-row upsert requires.
 */
async function upsertEmbeddingRows(
   db: DuckDBConnection,
   environmentName: string,
   packageName: string,
   model: string,
   batch: DesiredFacet[],
   vectors: number[][],
): Promise<void> {
   const now = new Date().toISOString();
   const params: unknown[] = [];
   batch.forEach((d, i) => {
      params.push(
         environmentName,
         packageName,
         d.entity.kind,
         sourceColumn(d.entity.source),
         d.entity.name,
         d.facet,
         d.entity.modelPath,
         d.hash,
         model,
         vectors[i].length,
         JSON.stringify(vectors[i]),
         now,
      );
   });
   const row = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS FLOAT[]), ?)";
   await db.run(
      `INSERT INTO entity_embeddings (
         environment_name, package_name, entity_kind, entity_source,
         entity_name, facet, model_path, content_hash, embedding_model,
         dims, embedding, updated_at
       ) VALUES ${batch.map(() => row).join(", ")}
       ON CONFLICT (environment_name, package_name, entity_kind, entity_source, entity_name, facet)
       DO UPDATE SET
         model_path = EXCLUDED.model_path,
         content_hash = EXCLUDED.content_hash,
         embedding_model = EXCLUDED.embedding_model,
         dims = EXCLUDED.dims,
         embedding = EXCLUDED.embedding,
         updated_at = EXCLUDED.updated_at`,
      params,
   );
}

/**
 * Bring one package's entity_embeddings rows in line with the current entity
 * set: embed new/changed entities (content-hash diff, so unchanged entities
 * never re-embed, across restarts too), upsert them, and delete rows for
 * entities that are gone. Runs under the package-name mutex. Nothing awaits
 * this call, so completion is recorded only on `meta.synced` (see kickSync).
 * Throws on provider or storage failure; partial writes self-heal on the next
 * sync.
 *
 * `meta` is the one the caller scheduled under, not looked up here: a package
 * deleted while this waited in the queue has had its meta removed, and
 * comparing against a fresh lookup would write its rows back.
 */
async function syncPackageEmbeddings(
   db: DuckDBConnection,
   provider: EmbeddingModel,
   environmentName: string,
   packageName: string,
   entities: EmbeddableEntity[],
   /**
    * Every entity, one per model file. `entities` is deduplicated for the
    * embedding rows; source summaries are per model file and read this list.
    */
   allEntities: readonly EmbeddableEntity[],
   settings: IndexSettings,
   meta: PackageSyncMeta,
): Promise<void> {
   return meta.mutex.runExclusive(async () => {
      // The meta may have been orphaned while this waited on the mutex (the
      // package was deleted). A sync under an orphaned meta is no longer
      // serialized against one under a re-minted meta, so it must not write.
      // Aborting records nothing, so the next call re-syncs under the new meta.
      if (syncMeta.get(metaKey(environmentName, packageName)) !== meta) {
         logger.debug(
            "[MCP Tool getContext] Skipping embedding sync for a deleted package",
            { environmentName, packageName },
         );
         return;
      }

      // Under the package mutex, a purge either finished before this sync or
      // starts after it, so the generation moves mid-sync only via the bump
      // at the bottom.

      // Keyphrases first: they decide what text an entity embeds as. Each
      // batch is saved as it returns; a failure stops the sync before any
      // embedding is written, because a package whose keyphrases cannot be
      // produced is an error, never a quiet fallback to doc-or-name vectors.
      let keyphrases: Map<string, string> | undefined;
      // One request budget (in HTTP requests, so a JSON repair counts) for
      // both LLM steps; the summaries get what the keyphrases left.
      const budget = new SyncRequestBudget(
         settings.keyphrases?.maxCallsPerSync ??
            settings.sourceSummary?.maxCallsPerSync ??
            0,
      );
      const shouldStop = () => meta.deleting === true;
      if (settings.keyphrases) {
         meta.keyphraseProgress = {
            done: 0,
            total: countNeedingLlm(entities, settings.keyphrases.mode),
            capped: false,
         };
         const outcome = await resolveKeyphrases({
            db,
            environmentName,
            packageName,
            entities,
            settings: settings.keyphrases,
            budget,
            shouldStop,
            onProgress: (p) => {
               meta.keyphraseProgress = p;
            },
         });
         if (shouldStop()) throw new SyncAbandonedError();
         keyphrases = outcome.keyphrases;
      } else {
         meta.keyphraseProgress = undefined;
      }

      // Source summaries next, sharing the request budget. Saved as they
      // return; a failure stops the sync before any embedding is written.
      if (settings.sourceSummary) {
         meta.sourceSummaryProgress = {
            done: 0,
            total: countSummarizableSources(allEntities),
            capped: false,
         };
         await resolveSourceSummaries({
            db,
            environmentName,
            packageName,
            entities: allEntities,
            settings: settings.sourceSummary,
            budget,
            shouldStop,
            onProgress: (p) => {
               meta.sourceSummaryProgress = p;
            },
         });
         if (shouldStop()) throw new SyncAbandonedError();
      } else {
         meta.sourceSummaryProgress = undefined;
      }

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

      // A doc past MAX_DOC_CHARS has a tail that is not embedded. This is the
      // one loss chunking does not remove, so it must not be silent.
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

      // A row's hash covers the text actually sent, prefix included, so a
      // changed documentPrefix re-embeds every row through the ordinary diff.
      const documentPrefix = provider.documentPrefix ?? "";
      const desired = withDocumentPrefix(
         await desiredFacetsChunked(entities, {
            representation: settings.representation,
            keyphrases,
         }),
         documentPrefix,
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

      // A row is current when its text hash and model match; a model switch
      // re-embeds in place. Dimensionality is deliberately NOT checked:
      // `dims` stores the ACTUAL vector length, and a provider that ignores
      // the `dimensions` parameter (e.g. Ollama) would mismatch forever and
      // re-embed on every instance swap. A real dims change is caught at query
      // time by the stale-row heal. This is the CACHED rule; the READABLE rule
      // is set out above getEmbeddingIndexStatus.
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

      // A sync that CHANGED rows must bump the generation like a purge does,
      // or a call blocked behind it on the mutex could return its pre-sync
      // snapshot marked semantic. In a finally, because a write failure
      // mid-loop has already changed rows.
      let rowsChanged = false;
      let deleted = 0;
      try {
         // One request, then one write, per batch. Each batch is saved as it
         // returns, so a failure keeps earlier batches and the hash diff
         // resumes from there. Transient errors are retried inside embedBatch.
         //
         // A package being deleted stops the sync before each request and
         // write and around each retry wait, so the delete's cleanup (queued
         // on this mutex) does not wait out a whole sync.
         const abandoned = () => meta.deleting === true;
         const deadlineAtMs = Date.now() + syncDeadlineMs;
         const pastDeadline = () => Date.now() > deadlineAtMs;
         const retry: EmbeddingRetryPolicy = {
            ...syncRetryPolicy,
            sleep: async (ms) => {
               if (abandoned()) throw new SyncAbandonedError();
               if (pastDeadline()) throw new SyncDeadlineError();
               await syncRetryPolicy.sleep(ms);
               if (abandoned()) throw new SyncAbandonedError();
            },
         };
         for (let start = 0; start < toEmbed.length; start += syncBatchSize) {
            if (abandoned()) throw new SyncAbandonedError();
            if (pastDeadline()) throw new SyncDeadlineError();
            const batch = toEmbed.slice(start, start + syncBatchSize);
            const vectors = await provider.embedBatch(
               batch.map((d) => documentPrefix + d.text),
               EMBEDDING_BATCH_TIMEOUT_MS,
               retry,
            );
            if (abandoned()) throw new SyncAbandonedError();
            await upsertEmbeddingRows(
               db,
               environmentName,
               packageName,
               provider.model,
               batch,
               vectors,
            );
            rowsChanged = true;
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

      // Recorded inside the mutex after the generation settles, so the fact
      // and its rows are atomic with respect to a purge.
      meta.synced = {
         // Computed exactly as the readiness check computes it: unprefixed
         // rows plus the settings key (the prefix is part of providerKey),
         // over the same per-model-file list.
         fingerprint: await fingerprintFor(allEntities, settings),
         providerKey: providerKeyFor(provider),
         generation: meta.generation,
      };

      logger.debug("[MCP Tool getContext] Synced entity embeddings", {
         environmentName,
         packageName,
         entityCount: entities.length,
         embedded: toEmbed.length,
         deleted,
      });
   });
}

/**
 * Queue a sync for this content unless one is already queued or running for
 * this Package instance.
 *
 * The safety net behind {@link enqueuePackageSync}: it covers a sync never
 * queued or since invalidated (the embedding model changed, a purge emptied
 * the table, a failed sync's cooldown ended). A search is what notices, so a
 * search queues the retry, through the same process-wide queue.
 *
 * Nothing may ever await the promise: completion is observed through
 * `meta.synced`, and failure is handled in {@link runTrackedSync}.
 */
function kickSync(args: {
   db: DuckDBConnection;
   provider: EmbeddingModel;
   pkg: Package;
   environmentName: string;
   packageName: string;
   entities: EmbeddableEntity[];
   /** One per model file; see syncPackageEmbeddings. */
   allEntities: readonly EmbeddableEntity[];
   meta: PackageSyncMeta;
   fingerprint: string;
   providerKey: string;
}): void {
   const { pkg, fingerprint, providerKey } = args;

   const inFlight = syncState.get(pkg);
   if (
      inFlight &&
      inFlight.fingerprint === fingerprint &&
      inFlight.providerKey === providerKey
   ) {
      return;
   }
   // No await between the get above and the set below, so concurrent calls
   // cannot both kick a sync for one instance. (The per-name mutex guards the
   // cross-instance reload race.)
   const tracked: SyncState = { fingerprint, providerKey };
   syncState.set(pkg, tracked);

   void embeddingSyncQueue.enqueue(() => runTrackedSync(args, tracked));
}

/**
 * Run one sync to completion and settle its bookkeeping. Never throws: a
 * failure is recorded on the package's meta (which starts its cooldown) and
 * logged, because nothing awaits a sync and the queue behind it must go on.
 */
async function runTrackedSync(
   args: {
      db: DuckDBConnection;
      provider: EmbeddingModel;
      pkg: Package;
      environmentName: string;
      packageName: string;
      entities: EmbeddableEntity[];
      allEntities: readonly EmbeddableEntity[];
      meta: PackageSyncMeta;
   },
   tracked: SyncState,
): Promise<void> {
   const {
      db,
      provider,
      pkg,
      environmentName,
      packageName,
      entities,
      allEntities,
      meta,
   } = args;
   meta.syncStartedAtMs = Date.now();
   try {
      // The provider is already known to be down: record that and send nothing.
      const down = openBreakerFor(provider);
      if (down) {
         markProviderFailure(meta, down.message);
         meta.failureAtMs = down.atMs;
         return;
      }
      await syncPackageEmbeddings(
         db,
         provider,
         environmentName,
         packageName,
         entities,
         allEntities,
         indexSettingsOf(pkg),
         meta,
      );
      meta.lastError = undefined;
      meta.lastErrorStage = undefined;
      meta.permanentFailure = undefined;
   } catch (error: unknown) {
      if (error instanceof SyncAbandonedError) {
         // Not a provider failure: nothing to cool down or report.
         logger.debug(
            "[MCP Tool getContext] Stopped an embedding sync for a deleted package",
            { environmentName, packageName },
         );
         return;
      }
      const message = error instanceof Error ? error.message : String(error);
      const stage: SyncStage | undefined =
         error instanceof KeyphraseStageError
            ? "keyphrase"
            : error instanceof SourceSummaryStageError
              ? "source_summary"
              : undefined;
      // The log keeps the full message; the status never names the endpoint.
      const shown = publicStatusMessage(error);
      markProviderFailure(meta, shown, stage);
      if (isProviderWideFailure(error)) {
         openBreaker(provider, shown);
      }
      // The same request would fail the same way, so it is not retried after
      // the cool-down; a change to content or settings, or a restart, does.
      if (isClientError(error)) {
         meta.permanentFailure = {
            fingerprint: tracked.fingerprint,
            providerKey: tracked.providerKey,
         };
      }
      logger.warn(
         stage === "keyphrase"
            ? "[MCP Tool getContext] Keyphrase generation failed; semantic ranking cooling down"
            : stage === "source_summary"
              ? "[MCP Tool getContext] Source summary generation failed; semantic ranking cooling down"
              : "[MCP Tool getContext] Embedding sync failed; semantic ranking cooling down",
         { environmentName, packageName, stage, error: message },
      );
   } finally {
      meta.syncStartedAtMs = undefined;
      // Cleared on success too: the marker means "queued or in flight", and
      // leaving it would stop a later purge (which invalidates meta.synced
      // without changing content) from ever re-kicking this fingerprint.
      if (syncState.get(pkg) === tracked) {
         syncState.delete(pkg);
      }
   }
}

/**
 * Queue a sync for a package that has just loaded, so the index builds before
 * anyone asks a question.
 *
 * It only enqueues: a restart loads every package at once, and the shared
 * queue keeps them from hitting the provider together. Idempotent per Package
 * instance; a reload makes a new instance, queued afresh.
 *
 * `prepare` runs when the job reaches the front of the queue, because building
 * the entity list is slow and must not run on the load path. It returns
 * undefined when there is nothing to sync.
 */
export function enqueuePackageSync(args: {
   pkg: Package;
   environmentName: string;
   packageName: string;
   prepare: () => Promise<
      | {
           db: DuckDBConnection;
           provider: EmbeddingModel;
           entities: readonly EmbeddableEntity[];
        }
      | undefined
   >;
}): void {
   const { pkg, environmentName, packageName } = args;
   if (loadQueued.has(pkg)) return;
   loadQueued.add(pkg);
   // Take the meta now, not after `prepare`: a delete during `prepare` marks
   // THIS object and removes it from the map, and a later lookup would mint a
   // fresh one that knows nothing about the delete.
   const meta = metaFor(environmentName, packageName);
   void embeddingSyncQueue.enqueue(async () => {
      const prepared = await args.prepare();
      if (!prepared) return;
      if (
         meta.deleting ||
         syncMeta.get(metaKey(environmentName, packageName)) !== meta
      ) {
         return;
      }
      const { db, provider } = prepared;
      const entities = uniqueByEntityKey(prepared.entities);
      if (entities.length > maxEmbeddedEntities) return;
      const providerKey = providerKeyFor(provider);
      const fingerprint = await fingerprintFor(
         prepared.entities,
         indexSettingsOf(pkg),
      );
      // Already current, cooling down, failed permanently, or already queued
      // by a search: nothing to do.
      if (
         isSynced(meta, fingerprint, providerKey) ||
         inCooldown(meta) ||
         isPermanentlyFailed(meta, fingerprint, providerKey)
      ) {
         return;
      }
      const inFlight = syncState.get(pkg);
      if (
         inFlight &&
         inFlight.fingerprint === fingerprint &&
         inFlight.providerKey === providerKey
      ) {
         return;
      }
      const tracked: SyncState = { fingerprint, providerKey };
      syncState.set(pkg, tracked);
      // Run inline: this job IS the sync, so the queue stays serial.
      await runTrackedSync(
         {
            db,
            provider,
            pkg,
            environmentName,
            packageName,
            entities,
            allEntities: prepared.entities,
            meta,
         },
         tracked,
      );
   });
}

/** What {@link trySemanticSearch} is asked. */
export interface SemanticSearchArgs {
   db: DuckDBConnection;
   provider: EmbeddingModel;
   pkg: Package;
   environmentName: string;
   packageName: string;
   /**
    * Every entity the package exposes to retrieval. Pass the same frozen array
    * on every call, so its fingerprint is computed once (see fingerprintFor).
    */
   entities: readonly EmbeddableEntity[];
   /**
    * One entry per search target that carries text, in the caller's order,
    * all embedded in ONE provider request and scored in ONE scan.
    *
    * `kinds` (never empty) is the entity kinds the target may claim, applied
    * INSIDE the scan: windows are cut per target, so filtering afterwards
    * could empty a `measure` target whose nearest neighbours were dimensions.
    */
   queries: Array<{ targetIndex: number; text: string; kinds: string[] }>;
   /**
    * The most rows kept per source, per target, never one number across the
    * package, so a source that matches everything cannot crowd others out.
    */
   perSourceWindow: number;
   /**
    * The most DOTTED rows (`path.leaf` joined fields) kept per source, per
    * target, in a window of their own, so hundreds of inline-joined fields
    * cannot take a source's whole window. Omit for one shared window.
    */
   perSourceJoinedWindow?: number;
   sourceName?: string;
   /**
    * The (kind, source, name) triples the caller's scope admits, when it
    * narrows by something this cache cannot express (a `model_path`, or an
    * `entity_name`, whose rule exempts source rows). Omit for a source
    * drill-down or no scope.
    *
    * Applied INSIDE the scan so `belowCutoffCount` and `totalEntities`
    * describe the same set as `hits`; otherwise a pin that matched nothing
    * would read as "nothing cleared the floor". An EMPTY array admits nothing
    * and answers 0 of 0.
    */
   scopeKeys?: Array<{ kind: string; source: string; name: string }>;
}

/**
 * Semantic retrieval for tier 4 of get_context: ranked hits, or a reason the
 * semantic path is unavailable, which the caller turns into `indexing` or an
 * error (there is no lexical fallback while a provider is configured). Never
 * throws.
 *
 * Cold start: content with no completed sync kicks one in the background and
 * reports `indexing`, so no call waits on a bulk embed. A reload whose facet
 * texts hash the same keeps the warm index.
 *
 * Steps: admit, embed the query texts, scan the vector cache, then heal
 * stale-dimension rows before the scan's answer is trusted.
 */
export async function trySemanticSearch(
   args: SemanticSearchArgs,
): Promise<SemanticSearchResult> {
   const { environmentName, packageName } = args;
   // Unique by the key the rows use, before anything counts or embeds them.
   const entities = uniqueByEntityKey(args.entities);

   const admitted = await admitSearch(args, entities);
   if (!("meta" in admitted)) return admitted;
   const { meta, entryGeneration } = admitted;

   const queryVectors = await embedQueries(args, meta);
   if (!queryVectors) return { unavailable: "error" };

   try {
      const scanned = await scanVectorCache(args, queryVectors);

      // A sync or heal holding the mutex may be rewriting rows under the scan
      // just run, so the snapshot (possibly partial or empty) must not be
      // served as semantic. The generation re-check below catches completed
      // writers; this catches in-flight ones. After the scan, so hits and
      // counts are invalidated together.
      if (meta.mutex.isLocked()) {
         return { unavailable: "indexing" };
      }

      const healed = await healStaleDimensions(
         args,
         meta,
         queryVectors[0].length,
      );
      if (healed) return healed;

      // A purge moved the generation mid-search, so the snapshot may be
      // empty from a concurrent heal; an unreliable empty result must never
      // be served as semantic "nothing relevant here".
      if (meta.generation !== entryGeneration) {
         return { unavailable: "indexing" };
      }

      return scanned;
   } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      logger.warn("[MCP Tool getContext] Semantic search failed", {
         environmentName,
         packageName,
         error: message,
      });
      return { unavailable: "error" };
   }
}

/**
 * Whether the package has a current index this call may search. Answers the
 * call outright when it does not (over the entity cap, empty scope, cooling
 * down, permanently failed, or not yet synced, which kicks a sync); otherwise
 * returns the meta and the generation the search began under.
 */
async function admitSearch(
   args: SemanticSearchArgs,
   entities: EmbeddableEntity[],
): Promise<
   SemanticSearchResult | { meta: PackageSyncMeta; entryGeneration: number }
> {
   const { db, provider, pkg, environmentName, packageName, scopeKeys } = args;

   if (entities.length > maxEmbeddedEntities) {
      const key = `${environmentName}\x00${packageName}`;
      if (!oversizeWarned.has(key)) {
         oversizeWarned.add(key);
         logger.warn(
            "[MCP Tool getContext] Package exceeds the semantic index entity cap; semantic search is unavailable for it",
            {
               environmentName,
               packageName,
               entityCount: entities.length,
               cap: maxEmbeddedEntities,
            },
         );
      }
      return { unavailable: "too-many-entities" };
   }

   // A scope that admits nothing answers 0 of 0 here, before a provider call
   // whose result cannot be used.
   if (scopeKeys !== undefined && scopeKeys.length === 0) {
      return {
         hits: [],
         belowCutoffCount: 0,
         totalEntities: 0,
         cutBySource: new Map(),
      };
   }

   const providerKey = providerKeyFor(provider);
   const meta = metaFor(environmentName, packageName);
   // Per-package cool-down: a recent provider failure for THIS package makes
   // it unavailable without touching any other package.
   if (inCooldown(meta)) {
      return { unavailable: "cooldown" };
   }
   // Captured at entry: a concurrent purge moves the generation, and this
   // call must then not assert anything about the table.
   const entryGeneration = meta.generation;
   // The caller's array, not the deduped copy, so the fingerprint cache hits.
   const fingerprint = await fingerprintFor(
      args.entities,
      indexSettingsOf(pkg),
   );
   if (isPermanentlyFailed(meta, fingerprint, providerKey)) {
      return { unavailable: "cooldown" };
   }
   if (!isSynced(meta, fingerprint, providerKey)) {
      kickSync({
         db,
         provider,
         pkg,
         environmentName,
         packageName,
         entities,
         allEntities: args.entities,
         meta,
         fingerprint,
         providerKey,
      });
      return { unavailable: "indexing" };
   }
   return { meta, entryGeneration };
}

/**
 * One vector per query text, or undefined when the provider failed, which
 * starts the package's cool-down.
 */
async function embedQueries(
   args: SemanticSearchArgs,
   meta: PackageSyncMeta,
): Promise<number[][] | undefined> {
   const { provider, environmentName, packageName, queries } = args;
   try {
      // ONE request for every target: the round trip sits on the latency
      // path of every semantic call.
      return await provider.embedBatch(
         queries.map((q) => (provider.queryPrefix ?? "") + q.text),
         EMBEDDING_QUERY_TIMEOUT_MS,
      );
   } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      // The log keeps the full message; the status never names the endpoint.
      markProviderFailure(meta, publicStatusMessage(error));
      logger.warn(
         "[MCP Tool getContext] Query embedding failed; semantic search cooling down",
         { environmentName, packageName, error: message },
      );
      return undefined;
   }
}

/** The scan's ranked hits and cutoff counts, as the search result. */
async function scanVectorCache(
   args: SemanticSearchArgs,
   queryVectors: number[][],
): Promise<SemanticSearchResult> {
   const {
      db,
      provider,
      environmentName,
      packageName,
      queries,
      perSourceWindow,
      sourceName,
      scopeKeys,
   } = args;
   // One statement, one pass over the vectors however many targets: the hits
   // and both cutoff counts derive from the same scored set, so they cannot
   // disagree, and the cosine work is done once.
   //
   // An entity scores as its BEST-matching facet, not an average: about half
   // of a real model's entities have no doc, so any name/doc blend would
   // penalise them or invent a fill, and MAX stays well-defined as a doc
   // splits into a variable number of chunks. More facets can only clear the
   // floor, never block it. Across TARGETS it is MAX too, sound because cosine
   // is an absolute scale.
   //
   // MATERIALIZED pins the single evaluation of `scored`. The LEFT JOIN ON
   // TRUE keeps the counts when no row clears the floor, which an inner join
   // would drop: exactly the true negative the counts exist to explain.
   const vectorValues = queryVectors
      .map((_, k) => `(${k}, CAST(? AS FLOAT[]))`)
      .join(", ");
   // One row per (target, claimable kind), so the join is plain equality.
   const targetKinds = queries.flatMap((q, k) =>
      q.kinds.map((kind) => ({ k, kind })),
   );
   // Empty when the caller sent no scope, which drops the CTE and its join.
   const scopeValues = (scopeKeys ?? []).map(() => "(?, ?, ?)").join(", ");
   const kindValues = targetKinds.map(({ k }) => `(${k}, ?)`).join(", ");
   // Dotted rows get their own window when the caller sets one. The window is
   // a server-set number, validated here, so it is written into the
   // statement rather than bound.
   const joinedWindow = args.perSourceJoinedWindow;
   if (
      joinedWindow !== undefined &&
      (!Number.isInteger(joinedWindow) || joinedWindow < 0)
   ) {
      throw new Error(
         `Invalid perSourceJoinedWindow: expected a whole number of at least 0, got ${joinedWindow}. Fix: pass 3 or omit it.`,
      );
   }
   const dotted = "(CASE WHEN strpos(entity_name, '.') > 0 THEN 1 ELSE 0 END)";
   const joinedCol = joinedWindow === undefined ? "" : `${dotted} AS dotted,`;
   const joinedPartition = joinedWindow === undefined ? "" : `, ${dotted}`;
   const hitWhere =
      joinedWindow === undefined
         ? "h.rn <= ?"
         : `h.rn <= CASE WHEN h.dotted = 1 THEN ${joinedWindow} ELSE ? END`;
   const scan = await db.all<ScanRow>(
      `WITH q(target_idx, vec) AS (VALUES ${vectorValues}),
         qk(target_idx, kind) AS (VALUES ${kindValues}),${
            scopeValues
               ? `
         -- The caller's scope, as rows: model_path is not a column here and
         -- an entity_name scope exempts source rows, so neither can be
         -- written as a predicate. Joined in scored so the counts below
         -- describe the same set as the hits.
         scope(kind, source, name) AS (VALUES ${scopeValues}),`
               : ""
         }
         -- An entity is scored against a target only if that target may
         -- claim its kind. This is where the claim lives, not downstream:
         -- ranked_per_target cuts each target's window from these rows, so a
         -- kind filter applied after it would leave a window full of rows the
         -- target cannot return and nothing else. It also scopes the counts:
         -- an entity no target can claim is neither a hit nor below the
         -- floor, which is what keeps "no hits" and "below == total" the same
         -- fact.
         scored AS MATERIALIZED (
            SELECT entity_kind, entity_source, entity_name, q.target_idx,
                   MAX(list_cosine_similarity(embedding, q.vec)) AS score
            FROM entity_embeddings, q, qk
            WHERE qk.target_idx = q.target_idx AND qk.kind = entity_kind
              AND environment_name = ? AND package_name = ?
              AND embedding_model = ? AND dims = ?
              ${sourceName !== undefined ? "AND entity_source = ?" : ""}
              ${
                 scopeValues
                    ? `AND EXISTS (SELECT 1 FROM scope sc
                                   WHERE sc.kind = entity_kind
                                     AND sc.source = entity_source
                                     AND sc.name = entity_name)`
                    : ""
              }
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
         -- The window is PER TARGET AND PER SOURCE, never global. One shared
         -- LIMIT over the union lets the highest-scoring target fill it and
         -- crowd the others out entirely: measured against malloy-samples, a
         -- dimension target at 0.63 took every slot while the measure target's
         -- own best hit at 0.42 and the view target's at 0.53 vanished from
         -- the response -- absent, not merely ranked lower. That is the exact
         -- failure typed targets exist to prevent, so each target gets its own
         -- share. The same crowding happens between sources: one wide source
         -- that matches a target on every field would take the whole target
         -- window and leave the other sources with no card, so each source
         -- keeps its own best rows and the caller sees every source that
         -- cleared the floor.
         ranked_per_target AS (
            SELECT entity_kind, entity_source, entity_name, target_idx, score,
                   -- Score, then source, name and kind: the full identity, so
                   -- an exact tie (two sources declaring the same field name
                   -- embed identically) is cut the same way on every run
                   -- instead of in whatever order the scan produced the rows.
                   -- This decides which tied rows fit the window, not just
                   -- how they are listed.
                   ${joinedCol}
                   ROW_NUMBER() OVER (
                      PARTITION BY target_idx, entity_source${joinedPartition}
                      ORDER BY score DESC, entity_source, entity_name,
                               entity_kind
                   ) AS rn
            FROM scored
            WHERE score >= ?
         ),
         hits AS (
            SELECT DISTINCT h.entity_kind, h.entity_source, h.entity_name,
                   p.best
            FROM ranked_per_target h
            JOIN per_entity p
              ON p.entity_kind = h.entity_kind
             AND p.entity_source = h.entity_source
             AND p.entity_name = h.entity_name
            WHERE ${hitWhere}
         ),
         -- What the window left out, per source: entities that cleared the
         -- floor on some target, less the ones kept. Source rows do not spend
         -- a slot in assembly's per-source cap, so they are not counted.
         above_floor AS (
            SELECT entity_source, COUNT(*) AS n
            FROM per_entity
            WHERE best >= ? AND entity_kind <> 'source'
            GROUP BY entity_source
         ),
         kept AS (
            SELECT entity_source, COUNT(*) AS n
            FROM hits
            WHERE entity_kind <> 'source'
            GROUP BY entity_source
         )
         SELECT agg.total, agg.below,
                h.entity_kind, h.entity_source, h.entity_name, h.best,
                s.target_idx, s.score,
                CAST(a.n - COALESCE(k.n, 0) AS INTEGER) AS cut
         FROM agg
         LEFT JOIN hits h ON TRUE
         LEFT JOIN above_floor a ON a.entity_source = h.entity_source
         LEFT JOIN kept k ON k.entity_source = h.entity_source
         LEFT JOIN scored s
           ON s.entity_kind = h.entity_kind
          AND s.entity_source = h.entity_source
          AND s.entity_name = h.entity_name
         ORDER BY h.best DESC, h.entity_source, h.entity_name, h.entity_kind,
                  s.target_idx`,
      [
         ...queryVectors.map((v) => JSON.stringify(v)),
         ...targetKinds.map(({ kind }) => kind),
         // Positional: these sit where the `scope` CTE does.
         ...(scopeKeys ?? []).flatMap((k) => [k.kind, k.source, k.name]),
         environmentName,
         packageName,
         provider.model,
         queryVectors[0].length,
         ...(sourceName !== undefined ? [sourceName] : []),
         provider.minSimilarity,
         provider.minSimilarity,
         perSourceWindow,
         provider.minSimilarity,
      ],
   );

   return foldScanRows(scan, queries);
}

/** One row of the vector-cache scan: one (hit entity, target) pair. */
interface ScanRow {
   total: number;
   below: number;
   entity_kind: string | null;
   entity_source: string | null;
   entity_name: string | null;
   best: number | null;
   target_idx: number | null;
   score: number | null;
   cut: number | null;
}

/** The scan's rows as the search result: one hit per entity, with the counts. */
function foldScanRows(
   scan: ScanRow[],
   queries: SemanticSearchArgs["queries"],
): SemanticSearchResult {
   // Fold the one-row-per-(hit, target) scan into one hit per entity,
   // keeping the SQL's order.
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
   const cutBySource = new Map<string, number>();
   for (const r of scan) {
      if (r.entity_name === null) continue;
      if (r.cut !== null && r.cut > 0) {
         cutBySource.set(r.entity_source as string, r.cut);
      }
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
         // Back to the caller's target index; the SQL numbered by position.
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
   // Both counts, because without them an empty result is indistinguishable
   // from "this package models nothing like that", and `below` alone is not
   // interpretable: 7-of-52 is a tight match, 52-of-52 the true negative.
   const cutoffCounts = scan[0];
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
      cutBySource,
   };
}

/**
 * Detect and heal cached rows that do not match the provider's current
 * (model, dims). Returns the answer for this call when it found any, or
 * undefined when the cache is consistent and the scan may be served.
 */
async function healStaleDimensions(
   args: SemanticSearchArgs,
   meta: PackageSyncMeta,
   queryDims: number,
): Promise<SemanticSearchResult | undefined> {
   const { db, provider, environmentName, packageName } = args;
   // Invariant, every call: no row may mismatch the provider's current
   // (model, dims). The sync diff cannot see a dims change (hashes still
   // match). Detecting stale rows directly, rather than inferring from an
   // empty result, also heals a partial sync that left some rows stranded at
   // the old dimensionality.
   const staleRows = await db.get<{ n: number }>(
      `SELECT CAST(COUNT(*) AS INTEGER) AS n FROM entity_embeddings
          WHERE environment_name = ? AND package_name = ?
            AND NOT (embedding_model = ? AND dims = ?)`,
      [environmentName, packageName, provider.model, queryDims],
   );
   if ((staleRows?.n ?? 0) > 0) {
      // Under the package-name mutex so it cannot interleave with a sync; the
      // generation bump invalidates every instance's memo. Acquired WITHOUT
      // waiting: a held mutex means a bulk embed or heal is in flight, and no
      // call may wait on a bulk embed.
      let outcome: "none" | "purged" | "backoff" | "busy";
      try {
         outcome = await tryAcquire(meta.mutex).runExclusive(async () => {
            // Orphaned-meta guard, as in the sync.
            if (syncMeta.get(metaKey(environmentName, packageName)) !== meta) {
               return "none";
            }
            const again = await db.get<{ n: number }>(
               `SELECT CAST(COUNT(*) AS INTEGER) AS n FROM entity_embeddings
                   WHERE environment_name = ? AND package_name = ?
                     AND NOT (embedding_model = ? AND dims = ?)`,
               [environmentName, packageName, provider.model, queryDims],
            );
            if ((again?.n ?? 0) === 0) return "none";
            // Backoff: at most one purge per suppression window. A fresh
            // mismatch within it means the endpoint serves inconsistent
            // dimensionalities, so cool down instead of re-embedding.
            // lastPurgeAtMs is NOT advanced here, so the window stays
            // measured from the last real purge (see
            // HEAL_PURGE_SUPPRESSION_MS).
            const now = Date.now();
            if (now - meta.lastPurgeAtMs < purgeSuppressionMs) {
               markProviderFailure(
                  meta,
                  `The embedding endpoint returned vectors of inconsistent dimensions for model ${provider.model}`,
               );
               logger.warn(
                  "[MCP Tool getContext] Repeated embedding dimensionality mismatch; the endpoint looks inconsistent, cooling down",
                  {
                     environmentName,
                     packageName,
                     model: provider.model,
                     queryDims,
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
                  queryDims,
                  staleRows: again?.n,
               },
            );
            // Only the stale rows, so the re-sync embeds just what was stranded.
            await db.run(
               `DELETE FROM entity_embeddings
                   WHERE environment_name = ? AND package_name = ?
                     AND NOT (embedding_model = ? AND dims = ?)`,
               [environmentName, packageName, provider.model, queryDims],
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
         // The generation bump invalidated meta.synced, so the next call
         // re-kicks on its own.
         return { unavailable: "indexing" };
      }
      if (outcome === "backoff") {
         return { unavailable: "cooldown" };
      }
   }
   return undefined;
}

/** What a package's semantic index is currently doing. */
export interface EmbeddingIndexStatus {
   /**
    * - `lexical`: no embedding provider is configured. A mode, not a fallback.
    * - `indexing`: vectors are being built (or are queued to be).
    * - `ready`: the next question is ranked semantically.
    * - `error`: the index cannot serve now. `reason` and `lastError` say why.
    */
   status: "lexical" | "indexing" | "ready" | "error";
   /**
    * Why `status` is `error`: `cooldown` (the provider failed and the package
    * is waiting out the window before the next try) or `too-many-entities`
    * (over the cap, which no retry fixes).
    */
   reason?: "cooldown" | "too-many-entities";
   /**
    * The sync stage that failed, when `status` is `error` because of one:
    * `keyphrase` means the LLM could not write keyphrases and `source_summary`
    * that it could not write source summaries (each after retries), so
    * nothing was embedded. Absent when the embedding call itself failed.
    */
   stage?: SyncStage;
   lastError?: { message: string; retryAt?: string };
   /**
    * Entities whose keyphrase comes from the LLM, and how many have one.
    * Present only when keyphrases are on (the package asks for them and an LLM
    * is configured). `done` below `total` while `ready` means the per-sync
    * call limit stopped the step; the next sync continues.
    */
   keyphraseProgress?: { done: number; total: number; capped?: boolean };
   /**
    * Sources that get an LLM summary, and how many have a current one.
    * Present only when source summaries are on (the package does not turn them
    * off and an LLM is configured). `done` below `total` while `ready` means
    * the per-sync call limit stopped the step; the next sync continues.
    */
   sourceSummaryProgress?: { done: number; total: number; capped?: boolean };
   /** When the sync now running began. Absent when none is running. */
   startedAt?: string;
   /**
    * Rows the package wants cached: the denominator for `embeddedRows`. One
    * per entity name plus one per chunk of documentation.
    */
   totalRows: number;
   /**
    * Rows cached under the provider's CURRENT model, across all entities and
    * facets. Scoped by model only, NOT vector length, so after an unprobed
    * dims change it can count rows the heal will purge on the next search
    * (see the two row rules above getEmbeddingIndexStatus). Read `status` for
    * readiness; this says how much is cached.
    */
   embeddedRows: number;
   /** Entities the package currently exposes to retrieval. */
   totalEntities: number;
   /**
    * Current entities with at least one cached vector. Coverage by identity,
    * so it can equal `totalEntities` while `status` is `indexing` (every doc
    * rewritten, nothing renamed).
    */
   embeddedEntities: number;
   /** Most recent row write, absent when nothing is cached yet. */
   lastSyncedAt?: string;
}

/**
 * Report a package's embedding-index state for the REST Package resource.
 *
 * `ready` means the next question will be ranked semantically, so it is
 * decided by the same recorded sync the search path gates on, NOT by whether
 * rows cover the current entity names: rows survive restarts and reloads, so
 * name coverage would read `ready` while questions were still not semantic.
 *
 * Derived, never authoritative: it takes no mutex and writes nothing. Its CPU
 * cost is the content fingerprint, cached per loaded package, so polling is
 * cheap but not free.
 *
 * Two DIFFERENT row rules live in this file; do not collapse them:
 *
 *   CACHED (model + content hash, dims ignored): "would the sync rewrite this
 *   row?" Used by the sync diff and the counts below. `dims` records the
 *   length the provider RETURNED, and a provider that ignores the requested
 *   `dimensions` (Ollama) would otherwise re-embed on every reload and pin
 *   these counts at 0.
 *
 *   READABLE (model + dims == the query vector's length): "can the cosine scan
 *   use this row?" Used by trySemanticSearch and the stale-row heal. Only a
 *   real question knows that length, and this path must not embed anything.
 *
 * After an unprobed dims change, `status` is still correct (providerKeyFor
 * carries the configured dims) while the counts describe rows until a search
 * fires the heal.
 */
export async function getEmbeddingIndexStatus(
   db: DuckDBConnection,
   provider: EmbeddingModel,
   environmentName: string,
   packageName: string,
   allEntities: readonly EmbeddableEntity[],
   pkg?: Package,
): Promise<EmbeddingIndexStatus> {
   // Counted per cached entity, not per card; see uniqueByEntityKey.
   const entities = uniqueByEntityKey(allEntities);
   const entityCount = entities.length;
   // The CACHED rule: current model, any dims.
   const scope = [environmentName, packageName, provider.model];

   // `totalRows` counts only name and doc rows; a `keyphrase` row is an LLM
   // extra, excluded here so the numerator cannot pass the denominator.
   const row = await db.get<{ n: number; last: string | null }>(
      `SELECT CAST(COUNT(*) AS INTEGER) AS n,
              CAST(MAX(updated_at) AS VARCHAR) AS last
       FROM entity_embeddings
       WHERE environment_name = ? AND package_name = ? AND embedding_model = ?
         AND facet <> ?`,
      [...scope, KEYPHRASE_FACET],
   );
   const embeddedRows = row?.n ?? 0;
   const lastSyncedAt = row?.last ?? undefined;

   // Which entities are covered, not just how many rows: a count cannot see
   // an edit that removes two entities and adds two others.
   const covered = await db.all<{
      entity_kind: string;
      entity_source: string;
      entity_name: string;
   }>(
      `SELECT DISTINCT entity_kind, entity_source, entity_name
       FROM entity_embeddings
       WHERE environment_name = ? AND package_name = ? AND embedding_model = ?`,
      scope,
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
   const settings = indexSettingsOf(pkg);
   const summary = await desiredSummaryFor(allEntities, settings);

   let state: Pick<
      EmbeddingIndexStatus,
      "status" | "reason" | "lastError" | "stage"
   >;
   if (entityCount > maxEmbeddedEntities) {
      state = {
         status: "error",
         reason: "too-many-entities",
         lastError: { message: tooManyEntitiesMessage(entityCount) },
      };
   } else if (
      meta &&
      isPermanentlyFailed(meta, summary.fingerprint, providerKeyFor(provider))
   ) {
      // No retryAt: nothing is scheduled, and a question will not start one.
      state = {
         status: "error",
         reason: "cooldown",
         ...(meta.lastErrorStage ? { stage: meta.lastErrorStage } : {}),
         lastError: {
            message: `${meta.lastError ?? "The embedding provider failed"}. The vendor refused the request itself, so it will not be retried until the package or its retrieval settings change, or the server restarts.`,
         },
      };
   } else if (meta && inCooldown(meta)) {
      state = {
         status: "error",
         reason: "cooldown",
         ...(meta.lastErrorStage ? { stage: meta.lastErrorStage } : {}),
         lastError: {
            message: meta.lastError ?? "The embedding provider failed",
            retryAt: new Date(meta.failureAtMs + cooldownMs).toISOString(),
         },
      };
   } else if (
      // A sync covering exactly this content stands and nothing is mid-write.
      meta &&
      isSynced(meta, summary.fingerprint, providerKeyFor(provider)) &&
      !meta.mutex.isLocked()
   ) {
      state = { status: "ready" };
   } else {
      state = { status: "indexing" };
   }

   const keyphraseProgress = settings.keyphrases
      ? (meta?.keyphraseProgress ?? {
           done: 0,
           total: countNeedingLlm(
              uniqueByEntityKey(allEntities),
              settings.keyphrases.mode,
           ),
        })
      : undefined;
   const sourceSummaryProgress = settings.sourceSummary
      ? (meta?.sourceSummaryProgress ?? {
           done: 0,
           total: summary.summarySources,
        })
      : undefined;

   return {
      ...state,
      ...(meta?.syncStartedAtMs !== undefined && state.status === "indexing"
         ? { startedAt: new Date(meta.syncStartedAtMs).toISOString() }
         : {}),
      ...(keyphraseProgress ? { keyphraseProgress } : {}),
      ...(sourceSummaryProgress ? { sourceSummaryProgress } : {}),
      embeddedRows,
      totalRows: summary.rows,
      totalEntities: entityCount,
      embeddedEntities,
      ...(lastSyncedAt ? { lastSyncedAt } : {}),
   };
}

/**
 * Why a package over the entity cap is not embedded, with the count, the cap
 * and how to raise it. Shared by the status endpoint and get_context's error.
 */
export function tooManyEntitiesMessage(entityCount: number): string {
   const suggested = Math.max(entityCount, maxEmbeddedEntities * 4);
   return (
      `The package has ${entityCount} entities, over the semantic index cap of ${maxEmbeddedEntities}. ` +
      `Fix: set retrieval.indexing.maxEntities to ${suggested} or more in publisher.config.json ` +
      `(for example "retrieval": { "indexing": { "maxEntities": ${suggested} } }) and restart the server. ` +
      `A higher cap makes the first index take longer, in proportion to the number of entities.`
   );
}
