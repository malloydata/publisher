// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What the request-time LLM stages share: a meter that counts one request's
 * chat calls and tokens, and the error a stage throws so that get_context can
 * answer with a message naming the stage.
 */

import type {
   ChatModel,
   JsonChatRequest,
   JsonChatResult,
} from "../../providers/types";

/** A stage failed after the provider layer's retries. get_context returns an error result. */
export class StageError extends Error {
   constructor(
      readonly stage: string,
      readonly reason: string,
      cause?: unknown,
   ) {
      super(`${stage}: ${reason}`, { cause });
      this.name = "StageError";
   }
}

/**
 * The array a model replied with. A root array is what the prompts ask for,
 * but a vendor's JSON mode only returns objects, so these shapes are accepted
 * as well, each seen from OpenAI's `json_object` mode:
 * - an object with exactly one array-valued key (`{"ratings": [...]}`);
 * - a single element returned bare (`{"index": 3, "score": "HIGH"}`), which
 *   is what a model does when it has one thing to say;
 * - an empty object, which is what it does when it has nothing to say.
 */
export function replyArray(value: unknown): unknown[] {
   if (Array.isArray(value)) return value;
   if (typeof value === "object" && value !== null) {
      const arrays = Object.values(value).filter(Array.isArray);
      if (arrays.length === 1) return arrays[0] as unknown[];
      if (arrays.length === 0) {
         if (Object.keys(value).length === 0) return [];
         if ("index" in value) return [value];
      }
   }
   throw new Error(
      'expected a JSON array, for example [{"index": 1, "score": ...}]',
   );
}

/**
 * Run `fn` over `items` with at most `concurrency` calls in flight, results in
 * item order. The first failure stops new calls from starting, waits for the
 * ones already running, and is thrown.
 */
export async function runPooled<T, R>(
   items: readonly T[],
   concurrency: number,
   fn: (item: T, index: number) => Promise<R>,
): Promise<R[]> {
   const out = new Array<R>(items.length);
   let next = 0;
   let failure: { error: unknown } | undefined;
   const worker = async () => {
      while (failure === undefined) {
         const index = next++;
         if (index >= items.length) return;
         try {
            out[index] = await fn(items[index], index);
         } catch (error) {
            failure ??= { error };
         }
      }
   };
   await Promise.all(
      Array.from(
         { length: Math.min(Math.max(1, concurrency), items.length) },
         worker,
      ),
   );
   if (failure) throw failure.error;
   return out;
}

export interface LlmUsage {
   calls: number;
   inputTokens: number;
   outputTokens: number;
}

/**
 * The request has already made as many chat calls as
 * `retrieval.llm.maxCallsPerRequest` allows, and a stage tried for one more.
 * The stage that hit it wraps this in a StageError, so the caller's message
 * names the stage.
 */
export class LlmCallLimitError extends Error {
   constructor(readonly limit: number) {
      super(
         `this request already made the ${limit} LLM ${limit === 1 ? "call" : "calls"} that retrieval.llm.maxCallsPerRequest allows, ` +
            `and it needs another. Fix: raise retrieval.llm.maxCallsPerRequest in publisher.config.json, ` +
            `or ask with fewer search targets.`,
      );
      this.name = "LlmCallLimitError";
   }
}

/**
 * Counts the chat calls and tokens of ONE get_context request, across every
 * stage, and refuses a call past `limit`. A "call" is one `completeJson` (or
 * `complete`); the provider layer's own retries and its single JSON repair
 * happen inside it and are not counted separately. The check runs before the
 * call, so the call that would pass the limit is never sent.
 */
export class LlmMeter {
   private used: LlmUsage = { calls: 0, inputTokens: 0, outputTokens: 0 };

   /** `limit` of null means no ceiling. */
   constructor(private readonly limit: number | null = null) {}

   snapshot(): LlmUsage {
      return { ...this.used };
   }

   /** Count one call, or throw if the limit is already reached. Synchronous, so concurrent callers cannot slip past it. */
   private admit(): void {
      if (this.limit !== null && this.used.calls >= this.limit) {
         throw new LlmCallLimitError(this.limit);
      }
      this.used.calls += 1;
   }

   private addUsage(usage: { inputTokens?: number; outputTokens?: number }) {
      this.used.inputTokens += usage.inputTokens ?? 0;
      this.used.outputTokens += usage.outputTokens ?? 0;
   }

   /** A chat model whose calls are counted, and limited, here. */
   wrap(chat: ChatModel): ChatModel {
      return {
         provider: chat.provider,
         model: chat.model,
         complete: async (req) => {
            this.admit();
            const result = await chat.complete(req);
            this.addUsage(result.usage);
            return result;
         },
         completeJson: async <T>(
            req: JsonChatRequest<T>,
         ): Promise<JsonChatResult<T>> => {
            this.admit();
            const result = await chat.completeJson(req);
            this.addUsage(result.usage);
            return result;
         },
      };
   }
}
