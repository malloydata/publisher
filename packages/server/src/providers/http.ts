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
   /**
    * Whether the vendor's own error text may be shown to a caller. A vendor
    * whose messages name the account's resources (Vertex AI puts the project
    * and location in the text) sets this false: the caller then sees the
    * status and a fixed sentence, and the server log keeps the full text.
    */
   showVendorMessage?: boolean;
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
            undefined,
            `${args.what} failed: timed out after ${args.timeoutMs}ms`,
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
         undefined,
         `${args.what} failed: the endpoint could not be reached`,
      );
   }

   if (!response.ok) {
      let detail: string;
      let vendorMessage: string;
      if (response.status === 401 || response.status === 403) {
         detail = `authentication failed; check ${args.authHint}`;
         vendorMessage = detail;
      } else {
         const text = await response.text().catch(() => "");
         detail = scrub(text, args.secrets).slice(0, 200);
         vendorMessage =
            args.showVendorMessage === false
               ? "the vendor rejected the request; the server log has its message"
               : vendorErrorMessage(scrub(text, args.secrets));
      }
      throw new HttpRequestError(
         `${args.what} to ${where} failed (${response.status}): ${detail}`,
         response.status,
         isRetryableStatus(response.status),
         parseRetryAfterMs(response.headers.get("retry-after")),
         `${args.what} failed (${response.status}): ${vendorMessage}`,
      );
   }

   // Reading the body can fail after good headers: the timeout fires, or the
   // connection drops. That is the network, so a retry can help. Only text
   // that arrived whole and is not JSON will fail the same way again.
   let text: string;
   try {
      text = await response.text();
   } catch (error) {
      const name = (error as Error)?.name;
      if (name === "AbortError") {
         throw new HttpRequestError(
            `${args.what} to ${where} was cancelled`,
            undefined,
            false,
         );
      }
      const timedOut = name === "TimeoutError";
      throw new HttpRequestError(
         `${args.what} to ${where} failed while reading the reply: ${scrub((error as Error).message, args.secrets)}`,
         undefined,
         true,
         undefined,
         timedOut
            ? `${args.what} failed: timed out after ${args.timeoutMs}ms`
            : `${args.what} failed: the connection dropped while reading the reply`,
      );
   }
   try {
      return JSON.parse(text);
   } catch {
      throw new HttpRequestError(
         `${args.what} to ${where} returned a body that is not JSON`,
         response.status,
         false,
         undefined,
         `${args.what} failed: the reply is not JSON`,
      );
   }
}

/**
 * The vendor's own error message from an error body: `error.message`, `error`
 * as a string, or `message`, cut to 200 characters. A body that is not JSON is
 * what a vendor that does not answer in JSON said, so it is shown as text, also
 * cut. Nothing else of the body is passed on.
 */
function vendorErrorMessage(body: string): string {
   const none = "the vendor gave no error message";
   try {
      const parsed = JSON.parse(body) as {
         error?: { message?: unknown } | string;
         message?: unknown;
      } | null;
      const found =
         typeof parsed?.error === "string"
            ? parsed.error
            : (parsed?.error?.message ?? parsed?.message);
      return typeof found === "string" && found.trim() !== ""
         ? found.trim().slice(0, 200)
         : none;
   } catch {
      const text = body.trim().slice(0, 200);
      return text === "" ? none : text;
   }
}

function scrub(text: string, secrets: (string | undefined)[]): string {
   let out = text;
   for (const secret of secrets) {
      if (secret) out = out.split(secret).join("[REDACTED]");
   }
   return out;
}
