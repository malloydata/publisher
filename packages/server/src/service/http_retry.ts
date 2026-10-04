// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { logger } from "../logger";

/**
 * A failed HTTP request to a model vendor, carrying what a retry decision
 * needs. Shared by the embedding client and the chat adapters.
 */
export class HttpRequestError extends Error {
   constructor(
      message: string,
      /** HTTP status, absent for a network failure or timeout. */
      readonly status: number | undefined,
      /**
       * Whether trying the same request again can succeed: 429, 408, 5xx and
       * network failures or timeouts. Auth (401/403), other 4xx and a
       * malformed response will fail the same way every time.
       */
      readonly retryable: boolean,
      /** The server's `Retry-After`, in ms, when it sent a usable one. */
      readonly retryAfterMs?: number,
      /**
       * The same failure worded for a caller of the MCP tool: the status and the
       * vendor's own error message, without the endpoint (host, project path)
       * that `message` carries for the server log. Absent when `message` is
       * already safe to show.
       */
      readonly publicMessage?: string,
   ) {
      super(message);
      this.name = "HttpRequestError";
   }
}

/**
 * The wording of a failure that is safe to show an MCP caller or return from
 * the status API: the public wording of a vendor failure, and the message of
 * anything else. The server log keeps `error.message`, which names the
 * endpoint.
 */
export function publicMessage(error: unknown): string {
   if (error instanceof HttpRequestError && error.publicMessage) {
      return error.publicMessage;
   }
   return error instanceof Error ? error.message : String(error);
}

/**
 * A reply that arrived but cannot be used (the wrong number of vectors, no
 * text). Not retryable, because it would come back the same. The log message
 * names where it came from; the public one does not.
 */
export function malformedReply(
   what: string,
   where: string,
   detail: string,
): HttpRequestError {
   return new HttpRequestError(
      `${what} from ${where} malformed: ${detail}`,
      undefined,
      false,
      undefined,
      `${what} malformed: ${detail}`,
   );
}

/**
 * How a bulk call retries. A caller on a latency path passes no policy and
 * fails fast instead.
 *
 * The delay before retry n is `baseDelayMs * 2^(n-1)`, capped at
 * `maxDelayMs`, then spread over its upper half with jitter so that several
 * clients sharing one rate limit do not retry in step. A `Retry-After` longer
 * than that replaces it, but only up to `maxDelayMs`: a longer one is not
 * waited out. Both a single wait (`maxDelayMs`) and the sum of all waits
 * (`maxTotalDelayMs`) are bounded: when the next wait would exceed either, the
 * last error is thrown at once rather than waited out.
 */
export interface RetryPolicy {
   /** Total attempts including the first. */
   maxAttempts: number;
   baseDelayMs: number;
   maxDelayMs: number;
   maxTotalDelayMs: number;
   /** Injected so tests never wait in real time. */
   sleep: (ms: number) => Promise<void>;
   /** Uniform [0, 1); injected so jitter is deterministic in tests. */
   random: () => number;
}

export const DEFAULT_RETRY: RetryPolicy = {
   maxAttempts: 5,
   baseDelayMs: 1_000,
   maxDelayMs: 30_000,
   maxTotalDelayMs: 90_000,
   sleep: (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
   random: Math.random,
};

/** True for the statuses that can clear on their own. */
export function isRetryableStatus(status: number): boolean {
   return status === 429 || status === 408 || status >= 500;
}

/** `Retry-After` is either whole seconds or an HTTP date. */
export function parseRetryAfterMs(value: string | null): number | undefined {
   if (value === null) return undefined;
   const trimmed = value.trim();
   if (/^\d+$/.test(trimmed)) return Number(trimmed) * 1_000;
   const at = Date.parse(trimmed);
   return Number.isNaN(at) ? undefined : Math.max(0, at - Date.now());
}

/**
 * Run `attempt`, retrying a retryable {@link HttpRequestError} under
 * `policy`. Anything else, and the last error once the attempts or the wait
 * budget are used up, is thrown as it came. `label` names the call in the
 * retry log line ("Embedding request", "Chat request"); `onRetry` lets the
 * caller count retries.
 */
export async function withRetry<T>(
   attempt: () => Promise<T>,
   policy: RetryPolicy,
   label = "Request",
   onRetry?: () => void,
): Promise<T> {
   let waited = 0;
   for (let n = 1; ; n++) {
      try {
         return await attempt();
      } catch (error) {
         if (
            !(error instanceof HttpRequestError) ||
            !error.retryable ||
            n >= policy.maxAttempts
         ) {
            throw error;
         }
         const exponential = Math.min(
            policy.maxDelayMs,
            policy.baseDelayMs * 2 ** (n - 1),
         );
         const jittered = exponential / 2 + (policy.random() * exponential) / 2;
         const delay = Math.max(jittered, error.retryAfterMs ?? 0);
         if (
            delay > policy.maxDelayMs ||
            waited + delay > policy.maxTotalDelayMs
         ) {
            throw error;
         }
         logger.warn(`${label} failed; retrying`, {
            attempt: n,
            maxAttempts: policy.maxAttempts,
            retryInMs: Math.round(delay),
            status: error.status,
            error: error.message,
         });
         onRetry?.();
         waited += delay;
         await policy.sleep(delay);
      }
   }
}
