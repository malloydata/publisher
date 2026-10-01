// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { createHash } from "node:crypto";
import { logger } from "../logger";
import {
   applyEnrichmentOverlay,
   enrichmentOverlayVersion,
   entityRowKey,
   humanizeName,
   installEnrichmentOverlay,
   uniqueByEntityKey,
   type FacetExtras,
} from "../mcp/tools/embedding_index";
import type { EmbeddingProvider } from "../service/embedding_provider";
import { LlmError } from "../service/llm_provider";
import { LlmBudget, type LlmRunner } from "../service/llm_runner";
import type { DuckDBConnection } from "../storage/duckdb/DuckDBConnection";
import {
   egressSignature,
   keyphraseField,
   needsKeyphrase,
   summaryInput,
   type EnrichableEntity,
} from "./egress";
import {
   parseKeyphraseBatchReply,
   parseKeyphraseReply,
   parseSummaryReply,
} from "./llm_json";
import {
   buildKeyphraseBatchPrompt,
   buildKeyphrasePrompt,
   type KeyphraseField,
} from "./prompts/keyphrase";
import { REPAIR_NOTE } from "./prompts/refine";
import { buildSummaryPrompt } from "./prompts/summary";
import { resolveEgress, type RetrievalConfig } from "./retrieval_config";

/**
 * Index-time enrichment: an LLM writes a short keyphrase for each field whose
 * documentation is missing or long, and a summary for each source, and those
 * become extra embedded facets (`kw`, `sum`) beside the name and doc ones.
 *
 * Three properties shape it:
 *
 * - **Additive.** Scoring takes an entity's best facet, so a generated facet can
 *   only add recall. Nothing the model wrote replaces what an author wrote, and
 *   the generated text never appears in a response unless asked for.
 * - **Cached by what was sent.** A row is reused only while the hash of the
 *   exact, egress-filtered inputs, the model and the prompt version still
 *   matches, so the LLM is asked again when, and only when, something it saw
 *   changed. The text is stored, so what the model said is inspectable.
 * - **Bounded.** One sync spends at most `indexing.maxLlmCallsPerSync` calls
 *   and `indexing.deadlineMs`, summaries first, then the emptiest docs. What a
 *   limit cuts off is picked up by a later sync, so a big package converges
 *   over several runs instead of one that never ends.
 *
 * It runs outside the embedding mutex: a held mutex answers every query
 * lexically, and these calls take minutes.
 */

const SEP = "\u0000";
const KEYPHRASE = "keyphrase";
const SUMMARY = "source_summary";
/** Longest generated one-line summary kept, in characters. */
const MAX_KEYPHRASE_CHARS = 300;

export type EnrichmentState =
   | "pending"
   | "running"
   | "ready"
   | "partial"
   | "failed";

export interface EnrichmentStatus {
   status: EnrichmentState;
   /** Entities that qualify for enrichment under the current config. */
   eligible: number;
   /** Of those, how many have generated text that matches their inputs. */
   enriched: number;
   /** Attempted and failed, waiting out `retryAfterMs`. */
   failed: number;
   /** Not attempted because a limit was reached; picked up by a later run. */
   deferredByBudget: number;
   updatedAt?: string;
   error?: string;
}

interface CacheRow {
   entity_kind: string;
   entity_source: string;
   entity_name: string;
   enrichment: string;
   input_hash: string;
   status: string;
   text: string | null;
   text2: string | null;
   attempts: number;
   updated_ms: number;
}

interface Item {
   what: typeof KEYPHRASE | typeof SUMMARY;
   entity: EnrichableEntity;
   hash: string;
   /** keyphrase input */
   field?: KeyphraseField;
   /** summary input */
   summary?: ReturnType<typeof summaryInput>;
   attempts: number;
}

const rowKey = (e: EnrichableEntity, what: string) =>
   [entityRowKey(e.kind, e.source ?? "", e.name), what].join(SEP);

const pkgKey = (env: string, pkg: string) => `${env}${SEP}${pkg}`;

function sha(parts: string[]): string {
   return createHash("sha256").update(parts.join(SEP)).digest("hex");
}

export interface Models {
   keyphrase: string | undefined;
   summary: string | undefined;
}

export function enrichmentModels(
   config: RetrievalConfig,
   envModel: string | undefined,
): Models {
   const fallback = config.llm.model ?? envModel;
   return {
      keyphrase: config.llm.models.keyphrase ?? fallback,
      summary: config.llm.models.summary ?? fallback,
   };
}

/** Render a keyphrase into the text that gets embedded. */
export function renderKeyphrase(
   template: string,
   name: string,
   keyphrase: string,
): string {
   return template
      .replaceAll("{name}", () => humanizeName(name) || name)
      .replaceAll("{keyphrase}", () => keyphrase);
}

// ---------------------------------------------------------------------------
// Planning: what does the cache already cover, and what is left to ask?
// ---------------------------------------------------------------------------

interface Plan {
   /** Items with no usable cached answer, in the order they should be asked. */
   pending: Item[];
   /** Facet text for every item the cache answers, keyed by entity row key. */
   extras: Map<string, FacetExtras>;
   eligible: number;
   enriched: number;
   /** Failed recently enough that they are not retried yet. */
   waiting: number;
   /** Left for a later sync because `indexing.maxItemsPerPackage` is spent. */
   deferredByRows: number;
}

async function loadCache(
   db: DuckDBConnection,
   environmentName: string,
   packageName: string,
): Promise<Map<string, CacheRow>> {
   const rows = await db.all<CacheRow>(
      `SELECT entity_kind, entity_source, entity_name, enrichment, input_hash,
              status, text, text2, CAST(attempts AS INTEGER) AS attempts,
              CAST(epoch_ms(updated_at) AS DOUBLE) AS updated_ms
       FROM entity_enrichment
       WHERE environment_name = ? AND package_name = ?`,
      [environmentName, packageName],
   );
   return new Map(
      rows.map((r) => [
         [
            entityRowKey(r.entity_kind, r.entity_source, r.entity_name),
            r.enrichment,
         ].join(SEP),
         r,
      ]),
   );
}

export async function planEnrichment(args: {
   db: DuckDBConnection;
   environmentName: string;
   packageName: string;
   entities: readonly EnrichableEntity[];
   config: RetrievalConfig;
   models: Models;
   now?: number;
   /** Rows generated text may add to the index; undefined means no limit. */
   itemBudget?: number;
}): Promise<Plan> {
   const { config, models } = args;
   const cfg = config.enrichment;
   const classes = resolveEgress(config);
   const signature = egressSignature(classes);
   const now = args.now ?? Date.now();
   const entities = uniqueByEntityKey(args.entities);
   const cache = await loadCache(
      args.db,
      args.environmentName,
      args.packageName,
   );

   const items: Item[] = [];
   if (cfg.keyphrase.mode !== "never") {
      for (const e of entities) {
         if (
            !needsKeyphrase(
               e,
               cfg.keyphrase.mode,
               cfg.keyphrase.wordThreshold,
               cfg.keyphrase.viewWordThreshold,
            )
         ) {
            continue;
         }
         const field = keyphraseField(
            e,
            entities,
            classes,
            cfg.keyphrase.maxCodeChars,
         );
         items.push({
            what: KEYPHRASE,
            entity: e,
            field,
            hash: sha([
               KEYPHRASE,
               cfg.keyphrase.promptVersion,
               models.keyphrase ?? "",
               signature,
               JSON.stringify(field),
            ]),
            attempts: 0,
         });
      }
   }
   if (cfg.sourceSummary.enabled) {
      for (const e of entities) {
         if (e.kind !== "source") continue;
         const summary = summaryInput(e, entities, classes);
         items.push({
            what: SUMMARY,
            entity: e,
            summary,
            hash: sha([
               SUMMARY,
               cfg.sourceSummary.promptVersion,
               models.summary ?? "",
               signature,
               JSON.stringify(summary),
            ]),
            attempts: 0,
         });
      }
   }

   const pending: Item[] = [];
   const extras = new Map<string, FacetExtras>();
   let enriched = 0;
   let waiting = 0;
   for (const item of items) {
      const cached = cache.get(rowKey(item.entity, item.what));
      item.attempts = cached?.attempts ?? 0;
      if (cached && cached.input_hash === item.hash) {
         if (cached.status === "ok" && cached.text) {
            enriched++;
            const key = entityRowKey(
               item.entity.kind,
               item.entity.source ?? "",
               item.entity.name,
            );
            const slot = extras.get(key) ?? {};
            if (item.what === KEYPHRASE) {
               slot.kw = renderKeyphrase(
                  cfg.keyphrase.template,
                  item.entity.name,
                  cached.text,
               );
               slot.keyphrase = cached.text;
            } else {
               slot.sum = cached.text2
                  ? `${cached.text2} ${cached.text}`
                  : cached.text;
               slot.summary = cached.text;
               if (cached.text2) slot.oneLine = cached.text2;
            }
            extras.set(key, slot);
            continue;
         }
         if (
            cached.status === "failed" &&
            now - cached.updated_ms < cfg.retryAfterMs
         ) {
            waiting++;
            continue;
         }
      }
      pending.push(item);
   }

   // Summaries first (few, and they anchor a source), then the emptiest docs
   // (the ones the model can help most), then the longest, by name.
   const rank = (i: Item) =>
      i.what === SUMMARY ? 0 : i.entity.embedDoc.trim() === "" ? 1 : 2;
   pending.sort(
      (a, b) =>
         rank(a) - rank(b) ||
         b.entity.embedDoc.length - a.entity.embedDoc.length ||
         (a.entity.name < b.entity.name
            ? -1
            : a.entity.name > b.entity.name
              ? 1
              : 0),
   );

   // `indexing.maxItemsPerPackage` is a budget for every row this package
   // embeds. The entity's own facets come first and are not negotiable, so
   // what is left is what generated text may add: summaries, then keyphrases,
   // in the order above. Cached text over the limit is left out of the index
   // too, so lowering the limit takes effect without clearing the cache.
   let deferredByRows = 0;
   let fitted = extras;
   if (args.itemBudget !== undefined) {
      fitted = fitExtras(extras, args.itemBudget);
      // Cached answers the budget no longer has room for wait like new work.
      const trimmed = rowsOf(extras) - rowsOf(fitted);
      enriched -= trimmed;
      deferredByRows = trimmed;
      const room = Math.max(0, args.itemBudget - rowsOf(fitted));
      if (pending.length > room) {
         deferredByRows += pending.length - room;
         pending.length = room;
      }
   }
   return {
      pending,
      extras: fitted,
      eligible: items.length,
      enriched,
      waiting,
      deferredByRows,
   };
}

/** How many embedding rows these extras add: one per keyphrase, one per summary. */
function rowsOf(extras: ReadonlyMap<string, FacetExtras>): number {
   let n = 0;
   for (const v of extras.values())
      n += (v.kw !== undefined ? 1 : 0) + (v.sum !== undefined ? 1 : 0);
   return n;
}

/** The extras that fit in `limit` rows, summaries before keyphrases. */
function fitExtras(
   extras: ReadonlyMap<string, FacetExtras>,
   limit: number,
): Map<string, FacetExtras> {
   const out = new Map<string, FacetExtras>();
   let used = 0;
   for (const which of ["sum", "kw"] as const) {
      for (const [key, v] of extras) {
         if (v[which] === undefined || used >= limit) continue;
         used++;
         const slot = out.get(key) ?? {};
         if (which === "sum") {
            slot.sum = v.sum;
            slot.summary = v.summary;
            slot.oneLine = v.oneLine;
         } else {
            slot.kw = v.kw;
            slot.keyphrase = v.keyphrase;
         }
         out.set(key, slot);
      }
   }
   return out;
}

// ---------------------------------------------------------------------------
// Running
// ---------------------------------------------------------------------------

const statuses = new Map<string, EnrichmentStatus>();
const running = new Map<string, Promise<void>>();
/** Per Package instance: has it run, and did anything stay unfinished? */
let kicked = new WeakMap<object, { at: number; unfinished: boolean }>();
let hydrated = new WeakSet<object>();
/** What was last installed for a package, so an unchanged plan installs nothing. */
const installedSignature = new Map<string, string>();

export function getEnrichmentStatus(
   environmentName: string,
   packageName: string,
): EnrichmentStatus | undefined {
   return statuses.get(pkgKey(environmentName, packageName));
}

export function _resetEnrichmentStateForTests(): void {
   statuses.clear();
   running.clear();
   installedSignature.clear();
   // WeakMaps cannot be cleared; a test that reuses one Package object across
   // cases needs them forgotten.
   kicked = new WeakMap();
   hydrated = new WeakSet();
}

const signatureOf = (extras: Map<string, FacetExtras>): string =>
   JSON.stringify([...extras.entries()].sort((a, b) => (a[0] < b[0] ? -1 : 1)));

async function upsert(
   db: DuckDBConnection,
   environmentName: string,
   packageName: string,
   item: Item,
   result:
      | { ok: true; text: string; text2?: string }
      | { ok: false; error: string },
   model: string,
   promptVersion: string,
   classes: string,
): Promise<void> {
   await db.run(
      `INSERT INTO entity_enrichment (
         environment_name, package_name, entity_kind, entity_source, entity_name,
         enrichment, input_hash, llm_model, prompt_version, status, text, text2,
         egress_classes, attempts, last_error, updated_at
       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT (environment_name, package_name, entity_kind, entity_source, entity_name, enrichment)
       DO UPDATE SET
         input_hash = EXCLUDED.input_hash, llm_model = EXCLUDED.llm_model,
         prompt_version = EXCLUDED.prompt_version, status = EXCLUDED.status,
         text = EXCLUDED.text, text2 = EXCLUDED.text2,
         egress_classes = EXCLUDED.egress_classes, attempts = EXCLUDED.attempts,
         last_error = EXCLUDED.last_error, updated_at = EXCLUDED.updated_at`,
      [
         environmentName,
         packageName,
         item.entity.kind,
         item.entity.source ?? "",
         item.entity.name,
         item.what,
         item.hash,
         model,
         promptVersion,
         result.ok ? "ok" : "failed",
         result.ok ? result.text : null,
         result.ok ? (result.text2 ?? null) : null,
         classes,
         item.attempts + 1,
         result.ok ? null : result.error.slice(0, 500),
         new Date().toISOString(),
      ],
   );
}

export interface RunArgs {
   db: DuckDBConnection;
   provider: EmbeddingProvider;
   environmentName: string;
   packageName: string;
   entities: readonly EnrichableEntity[];
   config: RetrievalConfig;
   runner: LlmRunner;
   /** The environment's LLM_MODEL, the last fallback for a stage's model. */
   envModel?: string;
   /**
    * Rows generated text may add to the embedding index: `indexing.maxItemsPerPackage`
    * less the rows the entities' own facets take. Undefined means no limit.
    */
   itemBudget?: number;
}

/** One enrichment run. Resolves when the overlay is installed; never throws. */
export async function runEnrichment(args: RunArgs): Promise<EnrichmentStatus> {
   const { db, environmentName, packageName, config, runner } = args;
   const key = pkgKey(environmentName, packageName);
   const cfg = config.enrichment;
   const models = enrichmentModels(config, args.envModel);
   const classes = resolveEgress(config);
   const signature = egressSignature(classes);

   const setStatus = (s: EnrichmentStatus) => {
      statuses.set(key, { ...s, updatedAt: new Date().toISOString() });
      return statuses.get(key)!;
   };
   const prior = statuses.get(key);
   setStatus({
      status: "running",
      eligible: prior?.eligible ?? 0,
      enriched: prior?.enriched ?? 0,
      failed: prior?.failed ?? 0,
      deferredByBudget: prior?.deferredByBudget ?? 0,
   });

   try {
      if (!classes.names) {
         // A keyphrase or summary prompt is built around field and source
         // names; sending it with the names withheld would not be a smaller
         // prompt, it would be a different and useless one.
         throw new Error(
            "retrieval.egress.names is off, and generated text needs field and source names in its prompts: turn names on, or turn enrichment off",
         );
      }
      if (!models.keyphrase && cfg.keyphrase.mode !== "never") {
         throw new Error(
            "no model for keyphrase generation: set LLM_MODEL, retrieval.llm.model or retrieval.llm.models.keyphrase",
         );
      }
      const plan = await planEnrichment({ ...args, models });
      const budget = new LlmBudget(
         config.indexing.maxLlmCallsPerSync,
         config.indexing.deadlineMs,
      );

      let failed = plan.waiting;
      let deferred = plan.deferredByRows;
      const results = new Map<string, FacetExtras>(plan.extras);
      let enriched = plan.enriched;

      const record = async (
         item: Item,
         result:
            | { ok: true; text: string; text2?: string }
            | { ok: false; error: string },
      ) => {
         const model =
            (item.what === KEYPHRASE ? models.keyphrase : models.summary) ?? "";
         const version =
            item.what === KEYPHRASE
               ? cfg.keyphrase.promptVersion
               : cfg.sourceSummary.promptVersion;
         await upsert(
            db,
            environmentName,
            packageName,
            item,
            result,
            model,
            version,
            signature,
         );
         if (!result.ok) {
            failed++;
            return;
         }
         enriched++;
         const ek = entityRowKey(
            item.entity.kind,
            item.entity.source ?? "",
            item.entity.name,
         );
         const slot = results.get(ek) ?? {};
         if (item.what === KEYPHRASE) {
            slot.kw = renderKeyphrase(
               cfg.keyphrase.template,
               item.entity.name,
               result.text,
            );
            slot.keyphrase = result.text;
         } else {
            slot.sum = result.text2
               ? `${result.text2} ${result.text}`
               : result.text;
            slot.summary = result.text;
            if (result.text2) slot.oneLine = result.text2;
         }
         results.set(ek, slot);
      };

      /** A budget or breaker stop defers the work; anything else marks it failed. */
      const settle = async (items: Item[], error: unknown) => {
         const e =
            error instanceof LlmError
               ? error
               : new LlmError(
                    String((error as Error)?.message ?? error),
                    "network",
                    false,
                 );
         if (
            e.kind === "budget" ||
            e.kind === "breaker" ||
            e.kind === "aborted"
         ) {
            deferred += items.length;
            return;
         }
         for (const item of items)
            await record(item, { ok: false, error: e.message });
      };

      const ask = async (
         stage: "keyphrase" | "summary",
         model: string,
         prompt: { system: string; user: string },
      ): Promise<string> =>
         (
            await runner.complete(budget, {
               stage,
               model,
               system: prompt.system,
               user: prompt.user,
               useCache: false,
            })
         ).text;

      const jobs: Array<Promise<void>> = [];

      // Summaries: one call per source.
      for (const item of plan.pending.filter((i) => i.what === SUMMARY)) {
         jobs.push(
            (async () => {
               try {
                  const prompt = buildSummaryPrompt(item.summary!);
                  const model = models.summary!;
                  let parsed = parseSummaryReply(
                     await ask("summary", model, prompt),
                  );
                  if (!parsed) {
                     parsed = parseSummaryReply(
                        await ask("summary", model, {
                           system: prompt.system,
                           user: prompt.user + REPAIR_NOTE,
                        }),
                     );
                  }
                  if (!parsed)
                     throw new LlmError(
                        "the summary reply could not be read",
                        "malformed",
                        false,
                     );
                  await record(item, {
                     ok: true,
                     text: parsed.summary,
                     text2: parsed.oneLine,
                  });
               } catch (error) {
                  await settle([item], error);
               }
            })(),
         );
      }

      // Keyphrases: batched by source, so a batch shares its sibling context.
      const bySource = new Map<string, Item[]>();
      for (const item of plan.pending.filter((i) => i.what === KEYPHRASE)) {
         const src = item.entity.source ?? "";
         const at = bySource.get(src);
         if (at) at.push(item);
         else bySource.set(src, [item]);
      }
      const size = cfg.keyphrase.batchSize;
      for (const group of bySource.values()) {
         for (let i = 0; i < group.length; i += size) {
            const batch = group.slice(i, i + size);
            jobs.push(
               (async () => {
                  const model = models.keyphrase!;
                  try {
                     if (batch.length === 1 && size === 1) {
                        const prompt = buildKeyphrasePrompt(batch[0].field!);
                        let kp = parseKeyphraseReply(
                           await ask("keyphrase", model, prompt),
                        );
                        if (!kp) {
                           kp = parseKeyphraseReply(
                              await ask("keyphrase", model, {
                                 system: prompt.system,
                                 user: prompt.user + REPAIR_NOTE,
                              }),
                           );
                        }
                        if (!kp)
                           throw new LlmError(
                              "the keyphrase reply could not be read",
                              "malformed",
                              false,
                           );
                        await record(batch[0], {
                           ok: true,
                           text: kp.slice(0, MAX_KEYPHRASE_CHARS),
                        });
                        return;
                     }
                     const prompt = buildKeyphraseBatchPrompt(
                        batch.map((b) => b.field!),
                     );
                     let got = parseKeyphraseBatchReply(
                        await ask("keyphrase", model, prompt),
                        batch.length,
                     );
                     if (got.size === 0) {
                        got = parseKeyphraseBatchReply(
                           await ask("keyphrase", model, {
                              system: prompt.system,
                              user: prompt.user + REPAIR_NOTE,
                           }),
                           batch.length,
                        );
                     }
                     for (let n = 0; n < batch.length; n++) {
                        const kp = got.get(n);
                        await record(
                           batch[n],
                           kp
                              ? {
                                   ok: true,
                                   text: kp.slice(0, MAX_KEYPHRASE_CHARS),
                                }
                              : {
                                   ok: false,
                                   error: "the model returned no keyphrase for this field",
                                },
                        );
                     }
                  } catch (error) {
                     await settle(batch, error);
                  }
               })(),
            );
         }
      }
      await Promise.all(jobs);

      // Install what the cache and this run now cover.
      const sig = signatureOf(results);
      if (installedSignature.get(key) !== sig) {
         await applyEnrichmentOverlay({
            db,
            provider: args.provider,
            environmentName,
            packageName,
            entities: args.entities,
            extras: results,
         });
         installedSignature.set(key, sig);
      }

      const state: EnrichmentState =
         deferred > 0 || failed > 0 ? "partial" : "ready";
      logger.info("[MCP Tool getContext] Enrichment run finished", {
         environmentName,
         packageName,
         eligible: plan.eligible,
         enriched,
         failed,
         deferred,
         llmCalls: budget.usage.calls,
      });
      return setStatus({
         status: state,
         eligible: plan.eligible,
         enriched,
         failed,
         deferredByBudget: deferred,
      });
   } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      logger.warn(
         "[MCP Tool getContext] Enrichment failed; serving the base index",
         {
            environmentName,
            packageName,
            error: message,
         },
      );
      return setStatus({
         status: "failed",
         eligible: prior?.eligible ?? 0,
         enriched: prior?.enriched ?? 0,
         failed: prior?.failed ?? 0,
         deferredByBudget: prior?.deferredByBudget ?? 0,
         error: message.slice(0, 300),
      });
   }
}

/**
 * Start an enrichment run for this Package instance unless one is running or
 * has already finished it. Non-blocking: a query never waits on the LLM.
 * Unfinished work (a limit was hit, or a field failed) is retried once
 * `retryAfterMs` has passed, on the next call that reaches here.
 */
export function kickEnrichment(
   instance: object,
   args: RunArgs,
   now: number = Date.now(),
): void {
   const key = pkgKey(args.environmentName, args.packageName);
   if (running.has(key)) return;
   const last = kicked.get(instance);
   if (
      last &&
      (!last.unfinished || now - last.at < args.config.enrichment.retryAfterMs)
   ) {
      return;
   }
   if (!statuses.has(key)) {
      statuses.set(key, {
         status: "pending",
         eligible: 0,
         enriched: 0,
         failed: 0,
         deferredByBudget: 0,
      });
   }
   kicked.set(instance, { at: now, unfinished: false });
   const promise = runEnrichment(args)
      .then((status) => {
         kicked.set(instance, {
            at: Date.now(),
            unfinished:
               status.status === "partial" || status.status === "failed",
         });
      })
      .finally(() => running.delete(key));
   running.set(key, promise);
}

/** Test seam: wait for a run started by {@link kickEnrichment}. */
export async function _settleEnrichmentForTests(
   environmentName: string,
   packageName: string,
): Promise<void> {
   await running.get(pkgKey(environmentName, packageName));
}

/**
 * Install whatever the cache already answers, before the first sync of a
 * Package instance, without asking the LLM anything.
 *
 * After a restart the in-memory overlay is empty while the embedding rows for
 * the generated facets are still in the database. Without this, the first sync
 * would find those rows absent from the desired set, delete them, and the
 * enrichment run would embed them all over again. Hydrating first makes the
 * desired set already include them, so that sync finds them current.
 */
export async function hydrateEnrichment(
   instance: object,
   args: Omit<RunArgs, "runner" | "provider">,
): Promise<void> {
   if (hydrated.has(instance)) return;
   hydrated.add(instance);
   const key = pkgKey(args.environmentName, args.packageName);
   if (enrichmentOverlayVersion(args.environmentName, args.packageName) > 0)
      return;
   try {
      const models = enrichmentModels(args.config, args.envModel);
      const plan = await planEnrichment({ ...args, models });
      if (plan.extras.size === 0) return;
      installEnrichmentOverlay(
         args.environmentName,
         args.packageName,
         plan.extras,
      );
      installedSignature.set(key, signatureOf(plan.extras));
      statuses.set(key, {
         status:
            plan.pending.length === 0 && plan.deferredByRows === 0
               ? "ready"
               : "partial",
         eligible: plan.eligible,
         enriched: plan.enriched,
         failed: plan.waiting,
         deferredByBudget: plan.deferredByRows,
         updatedAt: new Date().toISOString(),
      });
   } catch (error) {
      // Best effort: without it the cost is one re-embed, never a wrong answer.
      logger.debug("[MCP Tool getContext] Enrichment hydrate skipped", {
         error: error instanceof Error ? error.message : String(error),
      });
   }
}
