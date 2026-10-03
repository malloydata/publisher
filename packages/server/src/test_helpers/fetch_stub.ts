// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** Test helper: a scripted `fetch` that records every request it receives. */

import type { RetryPolicy } from "../service/http_retry";

export interface RecordedRequest {
   url: string;
   method: string | undefined;
   headers: Record<string, string>;
   // eslint-disable-next-line @typescript-eslint/no-explicit-any -- test helper: specs inspect each vendor's own request shape
   body: any;
}

export type Scripted = () => Response | Promise<Response>;

export function jsonResponse(
   body: unknown,
   init: { status?: number; headers?: Record<string, string> } = {},
): Response {
   return new Response(JSON.stringify(body), {
      status: init.status ?? 200,
      headers: { "Content-Type": "application/json", ...init.headers },
   });
}

/** Answers from `script`, one entry per request; the last entry repeats. */
export function stubFetch(script: Scripted[]) {
   const requests: RecordedRequest[] = [];
   const fn = (async (url: string | URL | Request, init?: RequestInit) => {
      const i = requests.length;
      requests.push({
         url: String(url),
         method: init?.method,
         headers: { ...(init?.headers as Record<string, string>) },
         body: init?.body ? JSON.parse(String(init.body)) : undefined,
      });
      return script[Math.min(i, script.length - 1)]();
   }) as unknown as typeof fetch;
   return { fetchFn: fn, requests };
}

/** Retry policy that never really waits and records each wait. */
export function instantRetry(sleeps: number[] = []): RetryPolicy {
   return {
      maxAttempts: 5,
      baseDelayMs: 1_000,
      maxDelayMs: 30_000,
      maxTotalDelayMs: 90_000,
      sleep: async (ms) => {
         sleeps.push(ms);
      },
      random: () => 0,
   };
}
