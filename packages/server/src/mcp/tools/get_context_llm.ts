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
