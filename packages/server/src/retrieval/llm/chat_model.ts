// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   type LlmFailureReason,
   recordLlmCall,
   recordLlmJsonRepair,
   recordLlmRetry,
   recordLlmTokens,
} from "../../llm_metrics";
import {
   DEFAULT_RETRY,
   HttpRequestError,
   type RetryPolicy,
   withRetry,
} from "../../service/http_retry";
import { httpFailureReason } from "./http";
import { FailureCooldown, ProviderCooldownError } from "./cooldown";
import { LlmJsonError, parseAndValidate } from "./json";
import type {
   ChatModel,
   ChatRequest,
   ChatResult,
   ChatUsage,
   JsonChatRequest,
   JsonChatResult,
   ProviderName,
} from "./types";

/** One HTTP attempt against a vendor. Retry, JSON and metrics live above it. */
export interface RawChatRequest {
   system?: string;
   prompt: string;
   maxTokens?: number;
   /** The model's declared maximum output; nothing an adapter adds may pass it. */
   maxOutputTokens?: number;
   /** Ask the vendor for JSON output where it has a mode for that. */
   json: boolean;
   signal: AbortSignal;
   timeoutMs: number;
}

export interface RawChat {
   send(req: RawChatRequest): Promise<ChatResult>;
}

/** A token count from a vendor reply, or undefined when it is not a finite number. */
export function tokenCount(value: unknown): number | undefined {
   return typeof value === "number" && Number.isFinite(value)
      ? value
      : undefined;
}

const JSON_ONLY_INSTRUCTION =
   "Reply with a single JSON value and nothing else: no prose, no code fences.";

function failureReason(error: unknown): LlmFailureReason {
   if (error instanceof ProviderCooldownError) return "cooldown";
   if (error instanceof HttpRequestError && error.status === 408) {
      return "timeout";
   }
   return httpFailureReason(error);
}

function addUsage(a: ChatUsage, b: ChatUsage): ChatUsage {
   const sum = (x?: number, y?: number) =>
      x === undefined && y === undefined ? undefined : (x ?? 0) + (y ?? 0);
   return {
      inputTokens: sum(a.inputTokens, b.inputTokens),
      outputTokens: sum(a.outputTokens, b.outputTokens),
   };
}

interface ChatModelOptions {
   maxOutputTokens?: number;
   timeoutMs: number;
   retry?: RetryPolicy;
   cooldown?: FailureCooldown;
}

/**
 * The part of a chat model every vendor shares: the per-call timeout, the
 * retry, the cooldown, JSON extraction with one repair re-ask, usage
 * accounting and metrics. A vendor adapter supplies only {@link RawChat}.
 */
export class ChatModelImpl implements ChatModel {
   readonly maxOutputTokens?: number;
   private readonly retry: RetryPolicy;
   private readonly cooldown: FailureCooldown;

   constructor(
      readonly provider: ProviderName,
      readonly model: string,
      private readonly raw: RawChat,
      private readonly options: ChatModelOptions,
   ) {
      this.maxOutputTokens = options.maxOutputTokens;
      this.retry = options.retry ?? DEFAULT_RETRY;
      this.cooldown = options.cooldown ?? new FailureCooldown(provider);
   }

   complete(req: ChatRequest): Promise<ChatResult> {
      return this.call(req, false);
   }

   async completeJson<T>(req: JsonChatRequest<T>): Promise<JsonChatResult<T>> {
      const first = await this.call(req, true);
      this.throwIfCutOff(first);
      let problem: string;
      try {
         return {
            value: parseAndValidate(first.text, req.validate),
            usage: first.usage,
         };
      } catch (error) {
         problem = (error as Error).message;
      }
      // One repair: show the model its own reply and what was wrong with it.
      recordLlmJsonRepair(this.provider);
      const second = await this.call(
         {
            ...req,
            prompt:
               `${req.prompt}\n\nYour previous reply was:\n${first.text}\n\n` +
               `It was rejected: ${problem}\n` +
               "Reply again with only the corrected JSON.",
         },
         true,
      );
      this.throwIfCutOff(second);
      const usage = addUsage(first.usage, second.usage);
      try {
         return {
            value: parseAndValidate(second.text, req.validate),
            usage,
            repaired: true,
         };
      } catch (error) {
         throw new LlmJsonError(
            `${this.provider} ${this.model} did not return usable JSON after one repair: ${(error as Error).message}`,
         );
      }
   }

   /**
    * A reply cut off at its output limit is never used, and not re-asked:
    * the repair would ask for the same JSON in the same room.
    */
   private throwIfCutOff(result: ChatResult): void {
      if (!result.truncated) return;
      throw new LlmJsonError(
         `${this.provider} ${this.model} stopped at a token limit; its JSON reply may be incomplete`,
      );
   }

   /** What a request may ask for: never more than the declared maximum. */
   private allowance(maxTokens?: number): number | undefined {
      const most = this.maxOutputTokens;
      return most === undefined ? maxTokens : Math.min(maxTokens ?? most, most);
   }

   private async call(req: ChatRequest, json: boolean): Promise<ChatResult> {
      const started = Date.now();
      // An error `onRequest` threw is a refusal by the caller, not a failure
      // of the vendor: it is not counted as one.
      let refusal: unknown;
      try {
         this.cooldown.check();
         const system = json
            ? [req.system, JSON_ONLY_INSTRUCTION].filter(Boolean).join("\n\n")
            : req.system;
         const result = await withRetry(
            () => {
               try {
                  req.onRequest?.();
               } catch (error) {
                  refusal = error;
                  throw error;
               }
               const timeout = AbortSignal.timeout(this.options.timeoutMs);
               return this.raw.send({
                  system,
                  prompt: req.prompt,
                  maxTokens: this.allowance(req.maxTokens),
                  maxOutputTokens: this.maxOutputTokens,
                  json,
                  timeoutMs: this.options.timeoutMs,
                  signal: req.signal
                     ? AbortSignal.any([req.signal, timeout])
                     : timeout,
               });
            },
            req.retry ?? this.retry,
            "Chat request",
            () => recordLlmRetry(this.provider, "chat"),
         );
         this.cooldown.success();
         recordLlmTokens(this.provider, result.usage);
         recordLlmCall(this.provider, "chat", Date.now() - started);
         return result;
      } catch (error) {
         // Only a failure that looks like an outage counts: 429, 408, 5xx, a
         // timeout or a network error. The cooldown is process-wide, so a 400,
         // a rejected key or a reply that is not usable JSON (one package's bad
         // prompt) must not stop every other package's LLM steps. A refused
         // call must not extend its own cooldown either.
         if (error === refusal) throw error;
         if (error instanceof HttpRequestError && error.retryable) {
            this.cooldown.failure();
         }
         recordLlmCall(
            this.provider,
            "chat",
            Date.now() - started,
            failureReason(error),
         );
         throw error;
      }
   }
}
