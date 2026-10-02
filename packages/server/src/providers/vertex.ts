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
 * Instances per `:predict` embedding request. To verify against current
 * vendor docs: Vertex has documented up to 250 instances per request for
 * its text embedding models, and some newer models accept fewer.
 */
export const VERTEX_EMBED_MAX_BATCH = 250;
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
         throw new HttpRequestError(
            `Vertex AI needs Application Default Credentials: ${(error as Error).message}. ` +
               "Fix: run `gcloud auth application-default login`, or run with a service account.",
            undefined,
            false,
         );
      }
   };
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
      const token = await this.getAccessToken();
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
      const token = await args.getAccessToken();
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
