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
 * but a vendor's JSON mode only returns objects, so an object with exactly one
 * array-valued key (`{"ratings": [...]}`) is accepted as that array.
 */
export function replyArray(value: unknown): unknown[] {
   if (Array.isArray(value)) return value;
   if (typeof value === "object" && value !== null) {
      const arrays = Object.values(value).filter(Array.isArray);
      if (arrays.length === 1) return arrays[0] as unknown[];
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
 * Counts the chat calls and tokens of ONE get_context request, across every
 * stage. A "call" is one `completeJson`; the provider layer's own retries and
 * its single JSON repair happen inside it and are not counted separately.
 */
export class LlmMeter {
   private used: LlmUsage = { calls: 0, inputTokens: 0, outputTokens: 0 };

   snapshot(): LlmUsage {
      return { ...this.used };
   }

   /** A chat model whose calls are counted here. */
   wrap(chat: ChatModel): ChatModel {
      return {
         provider: chat.provider,
         model: chat.model,
         complete: (req) => chat.complete(req),
         completeJson: async <T>(
            req: JsonChatRequest<T>,
         ): Promise<JsonChatResult<T>> => {
            this.used.calls += 1;
            const result = await chat.completeJson(req);
            this.used.inputTokens += result.usage.inputTokens ?? 0;
            this.used.outputTokens += result.usage.outputTokens ?? 0;
            return result;
         },
      };
   }
}
