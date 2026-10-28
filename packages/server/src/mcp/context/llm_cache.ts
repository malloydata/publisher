// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Index-time LLM output, cached by content. The engine's two index-time
 * steps (splitting docs into parts, writing questions) both send batches of
 * items to the chat model and keep what comes back, and both must not pay
 * again for an item whose input, prompt and model are unchanged. This is the
 * one place that does that: a stored row
 * is reused only while its `input_hash`, `prompt_hash` and `model` all match.
 *
 * A failed batch does not fail the build. The step stops sending (the
 * provider is down or refusing; more requests would only cost time), keeps
 * what it stored, and reports the failure, and the caller answers without the
 * missing output: the output only adds search text, and a catalog without it
 * still answers every question it could answer before.
 */

import { createHash } from "crypto";
import type { ChatModel } from "../../providers/types";
import type { DuckDBConnection } from "../../storage/duckdb/DuckDBConnection";
import { publicMessage } from "../../service/http_retry";
import {
   SyncBudgetReached,
   type SyncRequestBudget,
} from "../tools/get_context_llm";

export type LlmCacheTable = "doc_parts" | "node_questions";

export function sha256(text: string): string {
   return createHash("sha256").update(text).digest("hex");
}

/** One item a step may send. */
export interface CacheItem {
   /** Stable key of the item within the package. */
   key: string;
   /** Hash of everything the model is shown for this item. */
   inputHash: string;
   /** The item as one prompt line; numbered by the batch. */
   line: string;
}

export interface CachedRun<T> {
   /** Output by item key: stored rows reused plus what this run generated. */
   output: Map<string, T>;
   reused: number;
   generated: number;
   /** Items left without output: a failure, or the call budget ran out. */
   missing: number;
   /** The first batch failure, worded for a log or a status page. */
   failure?: string;
   /** True when the call budget stopped the step before the end. */
   capped: boolean;
}

interface StoredRow {
   item_key: string;
   input_hash: string;
   prompt_hash: string;
   model: string;
   output: string;
}

/**
 * Generate output for every item that has no current stored row, in batches,
 * `concurrency` batches at a time, saving each batch as it returns. Rows of
 * items no longer present are deleted.
 */
export async function generateCached<T>(args: {
   db: DuckDBConnection;
   table: LlmCacheTable;
   environmentName: string;
   packageName: string;
   items: readonly CacheItem[];
   chat: ChatModel;
   instructions: string;
   /** The user message for one batch, from its numbered lines. */
   render: (lines: readonly string[]) => string;
   /** Checks one batch reply and returns output per 1-based item number. */
   validate: (value: unknown, count: number) => Map<number, T>;
   batchSize: number;
   concurrency: number;
   maxTokensPerItem: number;
   budget: SyncRequestBudget;
   shouldStop?: () => boolean;
}): Promise<CachedRun<T>> {
   const { db, table, environmentName, packageName } = args;
   const promptHash = sha256(args.instructions);
   const model = `${args.chat.provider}:${args.chat.model}`;
   const rows = await db.all<StoredRow>(
      `SELECT item_key, input_hash, prompt_hash, model, output FROM ${table}
       WHERE environment_name = ? AND package_name = ?`,
      [environmentName, packageName],
   );
   const stored = new Map(rows.map((r) => [r.item_key, r]));
   const present = new Set(args.items.map((i) => i.key));
   for (const key of stored.keys()) {
      if (!present.has(key)) {
         await db.run(
            `DELETE FROM ${table} WHERE environment_name = ? AND package_name = ? AND item_key = ?`,
            [environmentName, packageName, key],
         );
      }
   }

   const output = new Map<string, T>();
   const pending: CacheItem[] = [];
   for (const item of args.items) {
      const row = stored.get(item.key);
      if (
         row &&
         row.input_hash === item.inputHash &&
         row.prompt_hash === promptHash &&
         row.model === model
      ) {
         output.set(item.key, JSON.parse(row.output) as T);
      } else {
         pending.push(item);
      }
   }
   const reused = output.size;
   const batches: CacheItem[][] = [];
   for (let i = 0; i < pending.length; i += args.batchSize) {
      batches.push(pending.slice(i, i + args.batchSize));
   }

   let failure: unknown;
   let capped = false;
   let generated = 0;
   let next = 0;
   const worker = async () => {
      while (failure === undefined && !capped) {
         if (args.shouldStop?.()) return;
         const index = next++;
         if (index >= batches.length) return;
         const batch = batches[index];
         try {
            const reply = await args.budget.spend((onRequest) =>
               args.chat.completeJson({
                  system: args.instructions,
                  prompt: args.render(batch.map((b) => b.line)),
                  maxTokens: args.maxTokensPerItem * batch.length + 100,
                  validate: (v) => args.validate(v, batch.length),
                  onRequest,
               }),
            );
            const now = new Date().toISOString();
            const params: unknown[] = [];
            const values: string[] = [];
            batch.forEach((item, i) => {
               const out = reply.value.get(i + 1);
               if (out === undefined) return;
               output.set(item.key, out);
               generated += 1;
               values.push("(?, ?, ?, ?, ?, ?, ?, ?)");
               params.push(
                  environmentName,
                  packageName,
                  item.key,
                  item.inputHash,
                  promptHash,
                  model,
                  JSON.stringify(out),
                  now,
               );
            });
            if (values.length > 0) {
               await db.run(
                  `INSERT INTO ${table} (environment_name, package_name, item_key, input_hash, prompt_hash, model, output, created_at)
                   VALUES ${values.join(", ")}
                   ON CONFLICT (environment_name, package_name, item_key) DO UPDATE SET
                     input_hash = EXCLUDED.input_hash, prompt_hash = EXCLUDED.prompt_hash,
                     model = EXCLUDED.model, output = EXCLUDED.output, created_at = EXCLUDED.created_at`,
                  params,
               );
            }
         } catch (error) {
            if (error instanceof SyncBudgetReached) {
               capped = true;
               return;
            }
            failure ??= error;
         }
      }
   };
   await Promise.all(
      Array.from(
         { length: Math.min(Math.max(1, args.concurrency), batches.length) },
         worker,
      ),
   );
   return {
      output,
      reused,
      generated,
      missing: args.items.length - output.size,
      ...(failure !== undefined ? { failure: publicMessage(failure) } : {}),
      capped,
   };
}

/** Item numbers 1..count that a reply object `{"results": [{"index": n, ...}]}` covers, with each entry. */
export function resultsByIndex(
   value: unknown,
   count: number,
): Map<number, Record<string, unknown>> {
   const results =
      typeof value === "object" && value !== null && !Array.isArray(value)
         ? (value as { results?: unknown }).results
         : value;
   if (!Array.isArray(results)) {
      throw new Error(
         'expected {"results": [{"index": <item number>, ...}, ...]}',
      );
   }
   const out = new Map<number, Record<string, unknown>>();
   for (const r of results) {
      if (typeof r !== "object" || r === null) continue;
      const index = (r as { index?: unknown }).index;
      if (
         typeof index === "number" &&
         Number.isInteger(index) &&
         index >= 1 &&
         index <= count
      ) {
         out.set(index, r as Record<string, unknown>);
      }
   }
   return out;
}

/** The strings of an array field, trimmed, empties dropped; [] when not an array. */
export function stringList(value: unknown, max: number): string[] {
   if (!Array.isArray(value)) return [];
   return value
      .filter((v): v is string => typeof v === "string")
      .map((v) => v.replace(/\s+/g, " ").trim())
      .filter(Boolean)
      .slice(0, max);
}
