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
