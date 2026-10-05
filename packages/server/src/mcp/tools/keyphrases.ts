// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Index-time keyphrases: a short search phrase for each entity, written once
 * and stored, so a long or missing description does not decide how an entity
 * is found.
 *
 * The rule, for `retrieval.keyphrases: "auto"` (and only with an LLM
 * configured):
 *
 *   - a description of 1 to 8 words (1 to 12 for a view) IS the keyphrase, with
 *     no LLM call;
 *   - an empty description, or a longer one, gets an LLM-written keyphrase.
 *
 * `always` sends every entity to the LLM; `never` sends none.
 *
 * Generated keyphrases live in the `entity_keyphrases` table, keyed so that
 * one is regenerated only when the entity's inputs, the prompt text or the
 * model changed. Each batch is saved as it returns, so a failure part way
 * keeps the batches before it, and a restart resumes from there.
 *
 * What is sent to the LLM: name, kind, source, data type and `#(doc)` text,
 * and under the `full` egress preset also the field's code. Never an access
 * predicate: the doc text is `#(doc)`-only to begin with, and it is scrubbed
 * again here so a future change to that upstream filter cannot leak one.
 */

import { createHash } from "crypto";
import { logger } from "../../logger";
import {
   KEYPHRASE_TARGET_WORDS,
   renderKeyphraseUserPrompt,
   type KeyphrasePromptEntity,
} from "../../prompts/keyphrase";
import type { ChatModel } from "../../providers/types";
import { publicMessage } from "../../service/http_retry";
import type { EgressPreset } from "../../retrieval_config";
import type { KeyphraseMode } from "../../service/package_retrieval";
import type { DuckDBConnection } from "../../storage/duckdb/DuckDBConnection";
import type { EmbeddableEntity } from "./embedding_index";
import { SyncBudgetReached, SyncRequestBudget } from "./get_context_llm";

/** Entities per LLM call. */
export const KEYPHRASE_BATCH_SIZE = 10;
/** A description this short or shorter is its own keyphrase (words). */
export const SHORT_DESCRIPTION_MAX_WORDS = 8;
export const SHORT_DESCRIPTION_MAX_WORDS_VIEW = 12;
/** A reply longer than this is refused; the prompt asks for far fewer. */
const MAX_REPLY_WORDS = 2 * KEYPHRASE_TARGET_WORDS;
/** Code sent under the `full` preset is cut to this many characters. */
const MAX_CODE_CHARS = 500;

/** The keyphrase step failed. The sync reports it with the stage name `keyphrase`. */
export class KeyphraseStageError extends Error {
   readonly stage = "keyphrase" as const;
   constructor(
      message: string,
      cause?: unknown,
      /** The same failure worded for a caller: no endpoint. See publicMessage. */
      readonly publicMessage?: string,
   ) {
      super(message, { cause });
      this.name = "KeyphraseStageError";
   }
}

/** What the keyphrase step needs to run, resolved once per sync. */
export interface KeyphraseSettings {
   /** `auto` or `always`; `never` has no settings at all. */
   mode: Exclude<KeyphraseMode, "never">;
   chat: ChatModel;
   /** Stored with each keyphrase: `provider/model`. */
   modelId: string;
   /** The instructions in force: the package's prompt file or the built-in one. */
   instructions: string;
   /** See keyphrasePromptHash. */
   promptHash: string;
   egress: EgressPreset;
   concurrency: number;
   maxCallsPerSync: number;
}

/** Identity of an entity in `entity_keyphrases` and in the sync's keyphrase map. */
export function keyphraseKey(entity: {
   kind: string;
   source: string | undefined;
   name: string;
}): string {
   return JSON.stringify([entity.kind, entity.source ?? "", entity.name]);
}

function words(text: string): number {
   const trimmed = text.trim();
   return trimmed === "" ? 0 : trimmed.split(/\s+/).length;
}

/** The most words a description may have and still be its own keyphrase. */
export function shortDescriptionLimit(kind: string): number {
   return kind === "view"
      ? SHORT_DESCRIPTION_MAX_WORDS_VIEW
      : SHORT_DESCRIPTION_MAX_WORDS;
}

/**
 * Drop everything an access predicate could ride on. Doc text reaches here
 * already limited to `#(doc)` lines; this removes any other annotation that
 * got into it (a `#(access_filter)` or `#(authorize)` predicate, a persist
 * line) from its marker to the end of that line.
 */
export function scrubForEgress(text: string): string {
   return text
      .split(/\r?\n/)
      .map((line) => {
         const marker = /(^|\s)(#\((?!doc\))[^)]*\)|#@)/.exec(line);
         return marker ? line.slice(0, marker.index) : line;
      })
      .join(" ")
      .replace(/\s+/g, " ")
      .trim();
}

function description(entity: EmbeddableEntity): string {
   return scrubForEgress(entity.embedDoc);
}

/**
 * The keyphrase an entity gets with no LLM call: its own description, when
 * the mode is `auto` and the description is short enough.
 */
export function descriptionKeyphrase(
   entity: EmbeddableEntity,
   mode: Exclude<KeyphraseMode, "never">,
): string | undefined {
   if (mode !== "auto") return undefined;
   const doc = description(entity);
   const n = words(doc);
   return n >= 1 && n <= shortDescriptionLimit(entity.kind) ? doc : undefined;
}

/** Whether the entity's keyphrase must come from the LLM. */
export function needsLlmKeyphrase(
   entity: EmbeddableEntity,
   mode: Exclude<KeyphraseMode, "never">,
): boolean {
   return descriptionKeyphrase(entity, mode) === undefined;
}

function promptEntityOf(
   entity: EmbeddableEntity,
   egress: EgressPreset,
   id: string,
): KeyphrasePromptEntity {
   const doc = description(entity);
   return {
      id,
      kind: entity.kind,
      name: entity.name,
      ...(entity.source ? { source: entity.source } : {}),
      ...(entity.dataType ? { type: entity.dataType } : {}),
      ...(doc ? { description: doc } : {}),
      ...(egress === "full" && entity.code
         ? { code: entity.code.slice(0, MAX_CODE_CHARS) }
         : {}),
   };
}

/**
 * Hash of exactly what the LLM would be shown for this entity. Part of the
 * stored row's identity: a change to any field sent regenerates, a change to
 * anything not sent does not.
 */
export function keyphraseInputHash(
   entity: EmbeddableEntity,
   egress: EgressPreset,
): string {
   const { id: _id, ...fields } = promptEntityOf(entity, egress, "");
   return createHash("sha256").update(JSON.stringify(fields)).digest("hex");
}

/**
 * Extra inputs the readiness fingerprint must see. The fingerprint is built
 * from row texts, which carry name and doc but not the data type or code; with
 * keyphrases on, those also decide the stored keyphrase, so they are folded
 * in. Empty string when keyphrases are off.
 */
export function keyphraseInputsDigest(
   entities: readonly EmbeddableEntity[],
   settings: Pick<KeyphraseSettings, "egress"> | undefined,
): string {
   if (!settings) return "";
   const rows = entities.map(
      (e) =>
         keyphraseKey(e) + "\u0000" + keyphraseInputHash(e, settings.egress),
   );
   rows.sort();
   return createHash("sha256").update(rows.join("\n")).digest("hex");
}

/** Count of entities whose keyphrase comes from the LLM: the progress denominator. */
export function countNeedingLlm(
   entities: readonly EmbeddableEntity[],
   mode: Exclude<KeyphraseMode, "never">,
): number {
   return entities.filter((e) => needsLlmKeyphrase(e, mode)).length;
}

// ---------------------------------------------------------------------------
// Storage
// ---------------------------------------------------------------------------

interface StoredKeyphrase {
   entity_key: string;
   input_hash: string;
   prompt_hash: string;
   model: string;
   keyphrase: string;
}

async function loadStored(
   db: DuckDBConnection,
   environmentName: string,
   packageName: string,
): Promise<Map<string, StoredKeyphrase>> {
   const rows = await db.all<StoredKeyphrase>(
      `SELECT entity_key, input_hash, prompt_hash, model, keyphrase
       FROM entity_keyphrases
       WHERE environment_name = ? AND package_name = ?`,
      [environmentName, packageName],
   );
   return new Map(rows.map((r) => [r.entity_key, r]));
}

async function saveBatch(
   db: DuckDBConnection,
   environmentName: string,
   packageName: string,
   rows: Array<{
      key: string;
      inputHash: string;
      keyphrase: string;
   }>,
   promptHash: string,
   modelId: string,
): Promise<void> {
   const now = new Date().toISOString();
   const params: unknown[] = [];
   for (const r of rows) {
      params.push(
         environmentName,
         packageName,
         r.key,
         r.inputHash,
         promptHash,
         modelId,
         r.keyphrase,
         now,
      );
   }
   await db.run(
      `INSERT INTO entity_keyphrases (
         environment_name, package_name, entity_key, input_hash,
         prompt_hash, model, keyphrase, created_at
       ) VALUES ${rows.map(() => "(?, ?, ?, ?, ?, ?, ?, ?)").join(", ")}
       ON CONFLICT (environment_name, package_name, entity_key)
       DO UPDATE SET
         input_hash = EXCLUDED.input_hash,
         prompt_hash = EXCLUDED.prompt_hash,
         model = EXCLUDED.model,
         keyphrase = EXCLUDED.keyphrase,
         created_at = EXCLUDED.created_at`,
      params,
   );
}

async function deleteKeys(
   db: DuckDBConnection,
   environmentName: string,
   packageName: string,
   keys: string[],
): Promise<void> {
   for (let i = 0; i < keys.length; i += 200) {
      const chunk = keys.slice(i, i + 200);
      await db.run(
         `DELETE FROM entity_keyphrases
          WHERE environment_name = ? AND package_name = ?
            AND entity_key IN (${chunk.map(() => "?").join(", ")})`,
         [environmentName, packageName, ...chunk],
      );
   }
}

/** Remove a deleted package's keyphrases. */
export async function deletePackageKeyphrases(
   db: DuckDBConnection,
   environmentName: string,
   packageName: string,
): Promise<void> {
   await db.run(
      `DELETE FROM entity_keyphrases
       WHERE environment_name = ? AND package_name = ?`,
      [environmentName, packageName],
   );
}

/** Remove a deleted environment's keyphrases. */
export async function deleteEnvironmentKeyphrases(
   db: DuckDBConnection,
   environmentName: string,
): Promise<void> {
   await db.run(`DELETE FROM entity_keyphrases WHERE environment_name = ?`, [
      environmentName,
   ]);
}

// ---------------------------------------------------------------------------
// Generation
// ---------------------------------------------------------------------------

export interface KeyphraseProgress {
   /** Entities that need an LLM keyphrase and have one stored. */
   done: number;
   /** Entities that need an LLM keyphrase. */
   total: number;
   /** True when maxCallsPerSync stopped the step before every entity had one. */
   capped: boolean;
}

export interface KeyphraseOutcome {
   /** Keyphrase by {@link keyphraseKey}, for every entity that has one. */
   keyphrases: Map<string, string>;
   progress: KeyphraseProgress;
   /** Chat calls made, counted against the sync's `maxCallsPerSync`. */
   calls: number;
}

function cleanKeyphrase(raw: string): string {
   return raw
      .replace(/\s+/g, " ")
      .trim()
      .replace(/^["'`]+|["'`.]+$/g, "")
      .trim();
}

/** Validate a batch reply: one `{ "keyphrase": string }` per requested id. */
function validateReply(ids: readonly string[]) {
   return (value: unknown): Map<string, string> => {
      if (typeof value !== "object" || value === null || Array.isArray(value)) {
         throw new Error(
            'expected a JSON object that maps each id to {"keyphrase": "..."}',
         );
      }
      const reply = value as Record<string, unknown>;
      const missing: string[] = [];
      const bad: string[] = [];
      const out = new Map<string, string>();
      for (const id of ids) {
         const entry = reply[id];
         const raw =
            typeof entry === "object" && entry !== null
               ? (entry as { keyphrase?: unknown }).keyphrase
               : undefined;
         if (entry === undefined) {
            missing.push(id);
         } else if (typeof raw !== "string" || cleanKeyphrase(raw) === "") {
            bad.push(`${id} (needs a non-empty "keyphrase" string)`);
         } else if (words(cleanKeyphrase(raw)) > MAX_REPLY_WORDS) {
            bad.push(
               `${id} (more than ${MAX_REPLY_WORDS} words; aim for ${KEYPHRASE_TARGET_WORDS} or fewer)`,
            );
         } else {
            out.set(id, cleanKeyphrase(raw));
         }
      }
      if (missing.length > 0 || bad.length > 0) {
         throw new Error(
            [
               missing.length > 0 ? `missing ids: ${missing.join(", ")}` : "",
               bad.length > 0 ? `bad entries: ${bad.join("; ")}` : "",
            ]
               .filter(Boolean)
               .join(". "),
         );
      }
      return out;
   };
}

/**
 * Produce the keyphrase for every entity that gets one.
 *
 * Description keyphrases (the `auto` rule) are free. For the rest, a stored
 * keyphrase is reused when its input, prompt and model hashes all match;
 * anything else is generated in batches of {@link KEYPHRASE_BATCH_SIZE},
 * `concurrency` batches at a time, each saved as it returns. At most
 * `maxCallsPerSync` batches run; entities beyond that are left without a
 * keyphrase (they embed from their doc or name) and reported through
 * `progress.capped`. A batch that fails after the provider layer's retries
 * stops the step with a {@link KeyphraseStageError}: loud, never a silent
 * downgrade, and what was saved stays.
 */
export async function resolveKeyphrases(args: {
   db: DuckDBConnection;
   environmentName: string;
   packageName: string;
   /** Deduplicated entities, as the sync sees them. */
   entities: readonly EmbeddableEntity[];
   settings: KeyphraseSettings;
   onProgress?: (progress: KeyphraseProgress) => void;
   /**
    * The sync's request budget, shared with the steps after this one. Absent:
    * a fresh one of `maxCallsPerSync` requests.
    */
   budget?: SyncRequestBudget;
   /** True when the package is going away: no further request is sent. */
   shouldStop?: () => boolean;
}): Promise<KeyphraseOutcome> {
   const { db, environmentName, packageName, entities, settings, onProgress } =
      args;
   const budget =
      args.budget ?? new SyncRequestBudget(settings.maxCallsPerSync);
   const requestsBefore = budget.requests;
   const keyphrases = new Map<string, string>();
   const needing: EmbeddableEntity[] = [];
   for (const entity of entities) {
      const free = descriptionKeyphrase(entity, settings.mode);
      if (free !== undefined) keyphrases.set(keyphraseKey(entity), free);
      else needing.push(entity);
   }

   const stored = await loadStored(db, environmentName, packageName);
   // Rows of entities that left the package are removed; rows of entities
   // still here are kept even when the current mode does not use them, so
   // switching `auto` to `always` and back does not pay twice.
   const present = new Set(entities.map(keyphraseKey));
   const gone = [...stored.keys()].filter((k) => !present.has(k));
   if (gone.length > 0) {
      await deleteKeys(db, environmentName, packageName, gone);
   }

   const pending: Array<{
      entity: EmbeddableEntity;
      key: string;
      inputHash: string;
   }> = [];
   for (const entity of needing) {
      const key = keyphraseKey(entity);
      const inputHash = keyphraseInputHash(entity, settings.egress);
      const row = stored.get(key);
      if (
         row &&
         row.input_hash === inputHash &&
         row.prompt_hash === settings.promptHash &&
         row.model === settings.modelId
      ) {
         keyphrases.set(key, row.keyphrase);
      } else {
         pending.push({ entity, key, inputHash });
      }
   }

   const progress: KeyphraseProgress = {
      done: needing.length - pending.length,
      total: needing.length,
      capped: false,
   };
   onProgress?.({ ...progress });
   if (pending.length === 0) return { keyphrases, progress, calls: 0 };

   const batches: (typeof pending)[] = [];
   for (let i = 0; i < pending.length; i += KEYPHRASE_BATCH_SIZE) {
      batches.push(pending.slice(i, i + KEYPHRASE_BATCH_SIZE));
   }

   let failure: unknown;
   let next = 0;
   const worker = async () => {
      while (failure === undefined) {
         if (args.shouldStop?.()) return;
         const index = next++;
         if (index >= batches.length) return;
         const batch = batches[index];
         const ids = batch.map((_, i) => String(i + 1));
         try {
            const reply = await budget.spend((onRequest) =>
               settings.chat.completeJson({
                  system: settings.instructions,
                  prompt: renderKeyphraseUserPrompt(
                     batch.map((b, i) =>
                        promptEntityOf(b.entity, settings.egress, ids[i]),
                     ),
                  ),
                  maxTokens: 80 * batch.length + 100,
                  validate: validateReply(ids),
                  onRequest,
               }),
            );
            await saveBatch(
               db,
               environmentName,
               packageName,
               batch.map((b, i) => ({
                  key: b.key,
                  inputHash: b.inputHash,
                  keyphrase: reply.value.get(ids[i]) as string,
               })),
               settings.promptHash,
               settings.modelId,
            );
            batch.forEach((b, i) =>
               keyphrases.set(b.key, reply.value.get(ids[i]) as string),
            );
            progress.done += batch.length;
            onProgress?.({ ...progress });
         } catch (error) {
            // Out of budget: this batch and the rest are left for the next
            // sync, which resumes from what is stored. Not a failure.
            if (error instanceof SyncBudgetReached) return;
            failure ??= error;
         }
      }
   };
   await Promise.all(
      Array.from(
         {
            length: Math.min(Math.max(1, settings.concurrency), batches.length),
         },
         worker,
      ),
   );
   if (
      failure === undefined &&
      progress.done < progress.total &&
      !args.shouldStop?.()
   ) {
      progress.capped = true;
      onProgress?.({ ...progress });
      logger.warn(
         "[get_context] Keyphrase generation stopped at the per-sync call limit; the rest are left for the next sync",
         {
            environmentName,
            packageName,
            limit: settings.maxCallsPerSync,
            setting: "retrieval.llm.maxCallsPerSync",
            remainingEntities: progress.total - progress.done,
         },
      );
   }
   if (failure !== undefined) {
      const message =
         failure instanceof Error ? failure.message : String(failure);
      const after = `Keyphrase generation failed after ${progress.done} of ${progress.total} entities`;
      throw new KeyphraseStageError(
         `${after}: ${message}`,
         failure,
         `${after}: ${publicMessage(failure)}`,
      );
   }
   return { keyphrases, progress, calls: budget.requests - requestsBefore };
}
