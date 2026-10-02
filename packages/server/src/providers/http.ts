// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   HttpRequestError,
   isRetryableStatus,
   parseRetryAfterMs,
} from "../service/http_retry";
import type { FetchFn } from "./types";

export interface PostJsonArgs {
   fetchFn: FetchFn;
   url: string;
   headers: Record<string, string>;
   body: unknown;
   signal: AbortSignal;
   /** For the timeout message only; the timeout itself is on `signal`. */
   timeoutMs: number;
   /** Secrets to scrub from any error text. Never logged or echoed. */
   secrets: (string | undefined)[];
   /** Names the setting to check when the vendor says 401/403. */
   authHint: string;
   /** "Chat request" or "Embedding request". */
   what: string;
}

/** The URL without its query string, so a key in a query never reaches a log. */
function safeUrl(url: string): string {
   return url.split("?")[0];
}

/**
 * POST a JSON body once and return the parsed JSON reply. Failures become an
 * {@link HttpRequestError} carrying whether a retry can help. Auth failures
 * drop the vendor's body entirely (it often reflects the credential) and the
 * rest has any literal occurrence of a secret scrubbed, then is cut to 200
 * characters.
 */
export async function postJson(args: PostJsonArgs): Promise<unknown> {
   const where = safeUrl(args.url);
   let response: Response;
   try {
      response = await args.fetchFn(args.url, {
         method: "POST",
         headers: { "Content-Type": "application/json", ...args.headers },
         body: JSON.stringify(args.body),
         signal: args.signal,
      });
   } catch (error) {
      const name = (error as Error)?.name;
      if (name === "TimeoutError") {
         throw new HttpRequestError(
            `${args.what} to ${where} failed: timed out after ${args.timeoutMs}ms`,
            undefined,
            true,
         );
      }
      if (name === "AbortError") {
         throw new HttpRequestError(
            `${args.what} to ${where} was cancelled`,
            undefined,
            false,
         );
      }
      throw new HttpRequestError(
         `${args.what} to ${where} failed: ${scrub((error as Error).message, args.secrets)}`,
         undefined,
         true,
      );
   }

   if (!response.ok) {
      let detail: string;
      if (response.status === 401 || response.status === 403) {
         detail = `authentication failed; check ${args.authHint}`;
      } else {
         const text = await response.text().catch(() => "");
         detail = scrub(text, args.secrets).slice(0, 200);
      }
      throw new HttpRequestError(
         `${args.what} to ${where} failed (${response.status}): ${detail}`,
         response.status,
         isRetryableStatus(response.status),
         parseRetryAfterMs(response.headers.get("retry-after")),
      );
   }

   try {
      return await response.json();
   } catch {
      throw new HttpRequestError(
         `${args.what} to ${where} returned a body that is not JSON`,
         response.status,
         false,
      );
   }
}

function scrub(text: string, secrets: (string | undefined)[]): string {
   let out = text;
   for (const secret of secrets) {
      if (secret) out = out.split(secret).join("[REDACTED]");
   }
   return out;
}
