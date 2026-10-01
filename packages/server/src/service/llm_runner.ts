// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { RetrievalConfig } from "../retrieval/retrieval_config";
import {
   LlmError,
   getLlmProvider,
   type LlmProvider,
   type LlmResponse,
   type LlmStageName,
} from "./llm_provider";

/** Counters for the LLM work of one get_context call (or one sync). */
export interface LlmUsage {
   calls: number;
   cacheHits: number;
   failures: number;
   promptTokens: number;
   completionTokens: number;
   ms: number;
}

export const newUsage = (): LlmUsage => ({
   calls: 0,
   cacheHits: 0,
   failures: 0,
   promptTokens: 0,
   completionTokens: 0,
   ms: 0,
});

/**
 * What one get_context call (or one index sync) may spend: a wall-clock
 * deadline and a call count. Every attempt takes one call from it, and the
 * per-call timeout is clamped to whatever time is left, so a slow local model
 * degrades the response instead of stalling it.
 */
export class LlmBudget {
   readonly usage: LlmUsage = newUsage();
   private readonly deadline: number;
   private taken = 0;

   constructor(
      private readonly maxCalls: number,
      budgetMs: number,
      private readonly now: () => number = Date.now,
   ) {
      this.deadline = now() + budgetMs;
   }

   remainingMs(): number {
      return this.deadline - this.now();
   }

   callsLeft(): number {
      return this.maxCalls - this.taken;
   }

   /** Reserve one call; false when the budget is spent. */
   take(): boolean {
      if (this.taken >= this.maxCalls || this.remainingMs() <= 0) return false;
      this.taken++;
      return true;
   }
}

export interface RunnerSettings {
   timeoutMs: number;
   maxAttempts: number;
   backoffMs: number;
   concurrency: number;
   temperature: number;
   seed: number;
   jsonMode: "none" | "json_object";
   extraBody: Record<string, unknown>;
   breaker: { failures: number; cooldownMs: number };
   cacheMaxEntries: number;
}

export function runnerSettingsFrom(
   llm: RetrievalConfig["llm"],
): RunnerSettings {
   return {
      timeoutMs: llm.timeoutMs,
      maxAttempts: llm.maxAttempts,
      backoffMs: llm.backoffMs,
      concurrency: llm.concurrency,
      temperature: llm.temperature,
      seed: llm.seed,
      jsonMode: llm.jsonMode,
      extraBody: llm.extraBody,
      breaker: llm.breaker,
      cacheMaxEntries: llm.cache.maxEntries,
   };
}

export interface CompleteArgs {
   stage: LlmStageName;
   model: string;
   system?: string;
   user: string;
   maxTokens?: number;
   /**
    * Identifies the request for the result cache. Must cover everything that
    * changes the answer: model, prompt version and the candidate text. Omit to
    * skip the cache.
    */
   cacheKey?: string;
   /** Per-request switch (retrieval.llm.cache.enabled after any override). */
   useCache?: boolean;
}

export interface CompleteResult extends LlmResponse {
   cached: boolean;
}

export interface Clock {
   now(): number;
   sleep(ms: number): Promise<void>;
}

const realClock: Clock = {
   now: () => Date.now(),
   sleep: (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
};

/**
 * Wraps an {@link LlmProvider} with everything the retrieval stages share:
 * a process-wide concurrency cap, retry with backoff, a circuit breaker, an
 * LRU cache of results, and per-call accounting against an {@link LlmBudget}.
 * The provider stays a thin HTTP client, so a test double implements only
 * `complete()`.
 *
 * It never decides what a failure means for a response. It throws
 * {@link LlmError}; each stage catches it and keeps its prior order.
 */
export class LlmRunner {
   private inFlight = 0;
   private waiters: Array<() => void> = [];
   private consecutiveFailures = 0;
   private openUntil = 0;
   private cache = new Map<string, LlmResponse>();
   readonly totals = newUsage();

   constructor(
      private readonly provider: LlmProvider,
      private readonly settings: RunnerSettings,
      private readonly clock: Clock = realClock,
   ) {}

   get providerId(): string {
      return this.provider.id;
   }

   breakerOpen(): boolean {
      return (
         this.consecutiveFailures >= this.settings.breaker.failures &&
         this.clock.now() < this.openUntil
      );
   }

   async complete(
      budget: LlmBudget | null,
      args: CompleteArgs,
   ): Promise<CompleteResult> {
      const cacheable = args.useCache !== false && args.cacheKey !== undefined;
      if (cacheable) {
         const hit = this.cache.get(args.cacheKey!);
         if (hit) {
            // Re-insert so it becomes the most recently used.
            this.cache.delete(args.cacheKey!);
            this.cache.set(args.cacheKey!, hit);
            this.totals.cacheHits++;
            if (budget) budget.usage.cacheHits++;
            return { ...hit, cached: true };
         }
      }

      if (this.breakerOpen()) {
         throw new LlmError(
            `LLM circuit breaker is open after ${this.consecutiveFailures} consecutive failures; retrying after cooldown`,
            "breaker",
            false,
         );
      }

      let lastError: LlmError | undefined;
      for (let attempt = 1; attempt <= this.settings.maxAttempts; attempt++) {
         await this.acquire();
         // The slot is given back exactly once per attempt: before a backoff
         // sleep (so waiting does not starve other callers) or on the way out.
         let slotHeld = true;
         const giveBack = () => {
            if (slotHeld) {
               slotHeld = false;
               this.release();
            }
         };
         // The budget is taken AFTER the wait for a slot, not before. A batch of
         // jobs all start at once and queue here; checked before the queue, every
         // one of them passes at t=0 and the deadline never applies to the ones
         // that only get a slot seconds later.
         if (budget && !budget.take()) {
            giveBack();
            throw (
               lastError ??
               new LlmError(
                  "LLM budget for this request is spent",
                  "budget",
                  false,
               )
            );
         }
         const timeoutMs = budget
            ? Math.max(
                 1,
                 Math.min(this.settings.timeoutMs, budget.remainingMs()),
              )
            : this.settings.timeoutMs;
         const started = this.clock.now();
         try {
            const response = await this.provider.complete({
               stage: args.stage,
               model: args.model,
               system: args.system,
               user: args.user,
               temperature: this.settings.temperature,
               seed: this.settings.seed,
               maxTokens: args.maxTokens,
               jsonMode: this.settings.jsonMode,
               extraBody: this.settings.extraBody,
               timeoutMs,
            });
            this.record(budget, started, response);
            this.consecutiveFailures = 0;
            if (cacheable) this.remember(args.cacheKey!, response);
            return { ...response, cached: false };
         } catch (error) {
            let llmError =
               error instanceof LlmError
                  ? error
                  : new LlmError(
                       `LLM call failed: ${(error as Error)?.message ?? String(error)}`,
                       "network",
                       true,
                    );
            // A call cut short because the budget had little time left was
            // stopped by the budget, not by a slow endpoint. Reported as a
            // timeout it would count toward the circuit breaker and be marked
            // failed, when all it needs is another go with a fresh budget.
            if (
               llmError.kind === "timeout" &&
               timeoutMs < this.settings.timeoutMs
            ) {
               llmError = new LlmError(
                  `LLM call ran out of the time budget (${timeoutMs}ms left)`,
                  "budget",
                  false,
               );
            }
            this.totals.failures++;
            this.totals.calls++;
            this.totals.ms += this.clock.now() - started;
            if (budget) {
               budget.usage.failures++;
               budget.usage.calls++;
               budget.usage.ms += this.clock.now() - started;
            }
            lastError = llmError;
            if (!llmError.retryable || attempt === this.settings.maxAttempts) {
               break;
            }
            const delay =
               llmError.retryAfterMs ??
               this.settings.backoffMs * 2 ** (attempt - 1);
            // A delay the request cannot afford is a failure now, not a wait.
            if (budget && delay >= budget.remainingMs()) break;
            giveBack();
            await this.clock.sleep(delay);
            continue;
         } finally {
            giveBack();
         }
      }

      // Aborts and budget exhaustion say nothing about the endpoint's health.
      if (
         lastError &&
         lastError.kind !== "aborted" &&
         lastError.kind !== "budget"
      ) {
         this.consecutiveFailures++;
         if (this.consecutiveFailures >= this.settings.breaker.failures) {
            this.openUntil =
               this.clock.now() + this.settings.breaker.cooldownMs;
         }
      }
      throw lastError!;
   }

   private acquire(): Promise<void> {
      if (this.inFlight < this.settings.concurrency) {
         this.inFlight++;
         return Promise.resolve();
      }
      return new Promise<void>((resolve) => {
         this.waiters.push(() => {
            this.inFlight++;
            resolve();
         });
      });
   }

   private release(): void {
      this.inFlight--;
      const next = this.waiters.shift();
      if (next) next();
   }

   private record(
      budget: LlmBudget | null,
      started: number,
      response: LlmResponse,
   ): void {
      const ms = this.clock.now() - started;
      for (const u of budget ? [this.totals, budget.usage] : [this.totals]) {
         u.calls++;
         u.ms += ms;
         u.promptTokens += response.usage?.promptTokens ?? 0;
         u.completionTokens += response.usage?.completionTokens ?? 0;
      }
   }

   private remember(key: string, response: LlmResponse): void {
      if (this.settings.cacheMaxEntries <= 0) return;
      this.cache.set(key, response);
      while (this.cache.size > this.settings.cacheMaxEntries) {
         const oldest = this.cache.keys().next().value;
         if (oldest === undefined) break;
         this.cache.delete(oldest);
      }
   }
}

// One runner per distinct provider and settings, so the breaker, the cache
// and the concurrency cap are shared by every request (and by the index sync)
// instead of being rebuilt, and forgotten, per call.
let shared: { key: string; runner: LlmRunner } | null = null;

/**
 * The shared runner for `config`, or null when the LLM is unavailable: no
 * endpoint configured, or `retrieval.llm.enabled` is false. A per-request
 * override never reaches here: it cannot change these settings, only the
 * models and the cache switch, which are read per call.
 */
export function getLlmRunner(config: RetrievalConfig): LlmRunner | null {
   if (config.llm.enabled === false) return null;
   let provider: LlmProvider | null;
   try {
      provider = getLlmProvider();
   } catch {
      return null;
   }
   if (!provider) return null;
   const settings = runnerSettingsFrom(config.llm);
   const key = `${provider.id}\u0000${JSON.stringify(settings)}`;
   if (
      !shared ||
      shared.key !== key ||
      shared.runner["provider"] !== provider
   ) {
      shared = { key, runner: new LlmRunner(provider, settings) };
   }
   return shared.runner;
}

export function _resetLlmRunnerForTests(): void {
   shared = null;
}
