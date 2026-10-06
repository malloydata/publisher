// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { GoogleAuth } from "google-auth-library";
import { HttpRequestError } from "../service/http_retry";
import type { RawChat, RawChatRequest } from "./chat_model";
import { checkVectors, type EmbedChunkFn } from "./embedding_http";
import { bareModelName, geminiGenerateBody, parseGeminiReply } from "./google";
import { postJson } from "./http";
import type { ChatResult, FetchFn } from "./types";

/**
 * Instances per `:predict` embedding request for the text-embedding models,
 * which accept up to 250. The gemini-embedding models accept ONE input per
 * request and answer 400 to more, so {@link vertexEmbedMaxBatch} gives 1 for
 * them.
 */
export const VERTEX_EMBED_MAX_BATCH = 250;

/** The most inputs one `:predict` request may carry for `model`. */
export function vertexEmbedMaxBatch(model: string): number {
   return /^gemini-embedding/.test(bareModelName(model))
      ? 1
      : VERTEX_EMBED_MAX_BATCH;
}
export const VERTEX_SCOPE = "https://www.googleapis.com/auth/cloud-platform";

/** Resolves an access token from Application Default Credentials. */
export function adcAccessToken(): () => Promise<string> {
   const auth = new GoogleAuth({ scopes: [VERTEX_SCOPE] });
   return async () => {
      try {
         const token = await auth.getAccessToken();
         if (!token) throw new Error("no access token returned");
         return token;
      } catch (error) {
         // Never retried: missing credentials will not appear on their own.
         // The library's text can name a key file path from
         // GOOGLE_APPLICATION_CREDENTIALS, so it stays in the log message and
         // the caller sees only the fixed sentence.
         const fix =
            "Fix: run `gcloud auth application-default login`, or run with a service account.";
         throw new HttpRequestError(
            `Vertex AI needs Application Default Credentials: ${(error as Error).message}. ${fix}`,
            undefined,
            false,
            undefined,
            `Vertex AI needs Application Default Credentials. ${fix}`,
         );
      }
   };
}

/**
 * Wait for `pending`, but stop at once when `signal` aborts. Fetching an
 * access token can stall (a metadata server that does not answer), and the
 * request's own timeout only starts once the request is sent, so without this
 * a cancelled or timed-out call would sit waiting for the token.
 */
export function untilAborted<T>(
   pending: Promise<T>,
   signal: AbortSignal,
   what: string,
): Promise<T> {
   const abortError = () =>
      (signal.reason as Error | undefined)?.name === "TimeoutError"
         ? new HttpRequestError(
              `${what} timed out while getting an access token`,
              undefined,
              true,
              undefined,
              `${what} failed: timed out getting an access token`,
           )
         : new HttpRequestError(`${what} was cancelled`, undefined, false);
   if (signal.aborted) return Promise.reject(abortError());
   return new Promise<T>((resolve, reject) => {
      const onAbort = () => reject(abortError());
      signal.addEventListener("abort", onAbort, { once: true });
      pending.then(
         (value) => {
            signal.removeEventListener("abort", onAbort);
            resolve(value);
         },
         (error) => {
            signal.removeEventListener("abort", onAbort);
            reject(error);
         },
      );
   });
}

/** `https://{location}-aiplatform.googleapis.com`, or the global host. */
export function vertexHost(location: string): string {
   return location === "global"
      ? "https://aiplatform.googleapis.com"
      : `https://${location}-aiplatform.googleapis.com`;
}

function modelUrl(
   projectId: string,
   location: string,
   model: string,
   method: string,
): string {
   return (
      `${vertexHost(location)}/v1/projects/${projectId}/locations/${location}` +
      `/publishers/google/models/${bareModelName(model)}:${method}`
   );
}

/** Chat through Vertex AI's Gemini endpoint with a Bearer token. */
export class VertexChat implements RawChat {
   constructor(
      private readonly model: string,
      private readonly projectId: string,
      private readonly location: string,
      private readonly getAccessToken: () => Promise<string>,
      private readonly fetchFn: FetchFn,
   ) {}

   async send(req: RawChatRequest): Promise<ChatResult> {
      const url = modelUrl(
         this.projectId,
         this.location,
         this.model,
         "generateContent",
      );
      const token = await untilAborted(
         this.getAccessToken(),
         req.signal,
         "Chat request",
      );
      const reply = await postJson({
         fetchFn: this.fetchFn,
         url,
         headers: { Authorization: `Bearer ${token}` },
         body: geminiGenerateBody(req),
         signal: req.signal,
         timeoutMs: req.timeoutMs,
         secrets: [token],
         authHint:
            "Application Default Credentials and the project's Vertex AI access",
         what: "Chat request",
         showVendorMessage: false,
      });
      return parseGeminiReply(reply, url);
   }
}

/** One `:predict` request per chunk. */
export function vertexEmbedChunk(args: {
   model: string;
   projectId: string;
   location: string;
   dimensions?: number;
   getAccessToken: () => Promise<string>;
   fetchFn: FetchFn;
}): EmbedChunkFn {
   const url = modelUrl(args.projectId, args.location, args.model, "predict");
   return async (inputs, signal, timeoutMs) => {
      const token = await untilAborted(
         args.getAccessToken(),
         signal,
         "Embedding request",
      );
      const reply = (await postJson({
         fetchFn: args.fetchFn,
         url,
         headers: { Authorization: `Bearer ${token}` },
         body: {
            instances: inputs.map((content) => ({ content })),
            ...(args.dimensions !== undefined
               ? { parameters: { outputDimensionality: args.dimensions } }
               : {}),
         },
         signal,
         timeoutMs,
         secrets: [token],
         authHint:
            "Application Default Credentials and the project's Vertex AI access",
         what: "Embedding request",
         showVendorMessage: false,
      })) as { predictions?: { embeddings?: { values?: unknown } }[] };
      const predictions = reply?.predictions;
      return checkVectors(
         Array.isArray(predictions)
            ? predictions.map((p) => p?.embeddings?.values)
            : predictions,
         inputs.length,
         url,
      );
   };
}
