// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What the request-time LLM stages share: a meter that counts one request's
 * chat calls and tokens, and the error a stage throws so that get_context can
 * answer with a message naming the stage.
 */

import type {
   ChatModel,
   ChatRequest,
   JsonChatRequest,
   JsonChatResult,
} from "../../providers/types";
import { DEFAULT_RETRY, type RetryPolicy } from "../../service/http_retry";

/**
 * How a chat call made while a person waits retries: once, after at most a
 * second, so one 429 cannot hold a get_context request for minutes (the index
 * sync keeps the model's longer ladder). A longer `Retry-After` fails at once.
 */
export const REQUEST_RETRY: RetryPolicy = {
   ...DEFAULT_RETRY,
   maxAttempts: 2,
   baseDelayMs: 250,
   maxDelayMs: 1_000,
   maxTotalDelayMs: 1_000,
};

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
 * The array a model replied with. The prompts ask for a root array, but a
 * vendor's JSON mode (e.g. OpenAI `json_object`) only returns objects, so also
 * accept: one array-valued key (`{"ratings": [...]}`), a single bare element
 * (`{"index": 3, ...}`), or `{}` for nothing.
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

/**
 * The index sync has sent as many chat requests as `retrieval.llm.maxCallsPerSync`
 * allows. The step that hit it stops and reports `capped`; it is not a failure.
 */
export class SyncBudgetReached extends Error {
   constructor(readonly limit: number) {
      super(`the sync already sent its ${limit} allowed LLM requests`);
      this.name = "SyncBudgetReached";
   }
}

/**
 * The chat requests one index sync may still send, shared by its LLM steps
 * (keyphrases, then source summaries). Counts HTTP requests, so retries and
 * JSON-repair re-asks each count. A model that reports no requests (a test
 * double) counts one per job.
 */
export class SyncRequestBudget {
   private used = 0;

   constructor(readonly limit: number) {}

   /** Requests sent so far. */
   get requests(): number {
      return this.used;
   }

   get exhausted(): boolean {
      return this.used >= this.limit;
   }

   /**
    * Run one LLM job. `fn` passes the `onRequest` it is given to the chat
    * call, which reports each request before it is sent; the report throws
    * {@link SyncBudgetReached} once the limit is spent, so a request past it
    * is never sent.
    */
   async spend<T>(fn: (onRequest: () => void) => Promise<T>): Promise<T> {
      if (this.exhausted) throw new SyncBudgetReached(this.limit);
      let reported = 0;
      try {
         return await fn(() => {
            if (this.exhausted) throw new SyncBudgetReached(this.limit);
            this.used += 1;
            reported += 1;
         });
      } finally {
         if (reported === 0) this.used += 1;
      }
   }
}

export interface LlmUsage {
   /** HTTP requests sent to the vendor, retries and JSON repairs included. */
   calls: number;
   inputTokens: number;
   outputTokens: number;
}

/**
 * The request has already sent as many chat requests as
 * `retrieval.llm.maxCallsPerRequest` allows, and a stage tried to send one
 * more. The stage that hit it wraps this in a StageError, so the caller's
 * message names the stage.
 */
export class LlmCallLimitError extends Error {
   constructor(readonly limit: number) {
      super(
         `this request already sent the ${limit} LLM ${limit === 1 ? "request" : "requests"} that retrieval.llm.maxCallsPerRequest allows, ` +
            `and it needs another. Fix: raise retrieval.llm.maxCallsPerRequest in publisher.config.json, ` +
            `or ask with fewer search targets.`,
      );
      this.name = "LlmCallLimitError";
   }
}

/**
 * Counts the chat requests and tokens of one get_context request across every
 * stage, and refuses a request past `limit`. A request is one HTTP call
 * (retries and JSON repairs included), checked before it is sent so the one
 * that would pass the limit never goes out. Wrapped models get
 * {@link REQUEST_RETRY}.
 */
export class LlmMeter {
   private used: LlmUsage = { calls: 0, inputTokens: 0, outputTokens: 0 };

   /** `limit` of null means no ceiling. */
   constructor(
      private readonly limit: number | null = null,
      private readonly requestRetry: RetryPolicy = REQUEST_RETRY,
   ) {}

   snapshot(): LlmUsage {
      return { ...this.used };
   }

   /** Requests this call may still send, or null when there is no ceiling. */
   remaining(): number | null {
      return this.limit === null
         ? null
         : Math.max(0, this.limit - this.used.calls);
   }

   /** Count one request, or throw if the limit is already reached. Synchronous, so concurrent callers cannot slip past it. */
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

   /** What a metered call adds to the caller's own request. */
   private metered(req: ChatRequest): Pick<ChatRequest, "retry" | "onRequest"> {
      return {
         retry: req.retry ?? this.requestRetry,
         onRequest: () => {
            req.onRequest?.();
            this.admit();
         },
      };
   }

   /** A chat model whose requests are counted, and limited, here. */
   wrap(chat: ChatModel): ChatModel {
      return {
         provider: chat.provider,
         model: chat.model,
         complete: async (req) => {
            const result = await chat.complete({
               ...req,
               ...this.metered(req),
            });
            this.addUsage(result.usage);
            return result;
         },
         completeJson: async <T>(
            req: JsonChatRequest<T>,
         ): Promise<JsonChatResult<T>> => {
            const result = await chat.completeJson({
               ...req,
               ...this.metered(req),
            });
            this.addUsage(result.usage);
            return result;
         },
      };
   }
}
