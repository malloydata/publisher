// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { RawChat, RawChatRequest } from "./chat_model";
import { checkVectors, type EmbedChunkFn } from "./embedding_http";
import { malformedReply } from "../service/http_retry";
import { postJson } from "./http";
import type { ChatResult, FetchFn } from "./types";

export const GEMINI_BASE_URL = "https://generativelanguage.googleapis.com";
export const GEMINI_API_VERSION = "v1beta";
/**
 * Texts per `batchEmbedContents` request. To verify against current vendor
 * docs: the Gemini API has documented a limit of 100 requests per batch.
 */
export const GEMINI_EMBED_MAX_BATCH = 100;

/** `models/x` and `x` both name a model; the URL wants the bare name. */
export function bareModelName(model: string): string {
   return model.replace(/^models\//, "");
}

function count(value: unknown): number | undefined {
   return typeof value === "number" && Number.isFinite(value)
      ? value
      : undefined;
}

/** The request body Gemini and Vertex share for `generateContent`. */
export function geminiGenerateBody(req: RawChatRequest) {
   const generationConfig: Record<string, unknown> = { temperature: 0 };
   if (req.maxTokens !== undefined) {
      generationConfig.maxOutputTokens = req.maxTokens;
   }
   if (req.json) generationConfig.responseMimeType = "application/json";
   const body: Record<string, unknown> = {
      contents: [{ role: "user", parts: [{ text: req.prompt }] }],
      generationConfig,
   };
   if (req.system) {
      body.systemInstruction = { parts: [{ text: req.system }] };
   }
   return body;
}

interface GeminiReply {
   candidates?: { content?: { parts?: { text?: unknown }[] } }[];
   usageMetadata?: {
      promptTokenCount?: unknown;
      candidatesTokenCount?: unknown;
   };
}

/** Text and usage out of a `generateContent` reply. */
export function parseGeminiReply(reply: unknown, url: string): ChatResult {
   const r = reply as GeminiReply;
   const parts = r?.candidates?.[0]?.content?.parts;
   const text = Array.isArray(parts)
      ? parts.map((p) => (typeof p?.text === "string" ? p.text : "")).join("")
      : "";
   if (text === "") {
      throw malformedReply(
         "Chat response",
         url,
         "no candidates[0].content.parts text",
      );
   }
   return {
      text,
      usage: {
         inputTokens: count(r.usageMetadata?.promptTokenCount),
         outputTokens: count(r.usageMetadata?.candidatesTokenCount),
      },
   };
}

/** Chat through the Gemini API with an API key in `x-goog-api-key`. */
export class GeminiChat implements RawChat {
   constructor(
      private readonly model: string,
      private readonly baseUrl: string,
      private readonly apiKey: string,
      private readonly fetchFn: FetchFn,
   ) {}

   async send(req: RawChatRequest): Promise<ChatResult> {
      const url = `${this.baseUrl}/${GEMINI_API_VERSION}/models/${bareModelName(this.model)}:generateContent`;
      const reply = await postJson({
         fetchFn: this.fetchFn,
         url,
         headers: { "x-goog-api-key": this.apiKey },
         body: geminiGenerateBody(req),
         signal: req.signal,
         timeoutMs: req.timeoutMs,
         secrets: [this.apiKey],
         authHint: "LLM_API_KEY",
         what: "Chat request",
      });
      return parseGeminiReply(reply, url);
   }
}

/** One `batchEmbedContents` request per chunk, authenticated by API key. */
export function geminiEmbedChunk(args: {
   model: string;
   baseUrl: string;
   apiKey: string;
   dimensions?: number;
   fetchFn: FetchFn;
}): EmbedChunkFn {
   const name = bareModelName(args.model);
   const url = `${args.baseUrl}/${GEMINI_API_VERSION}/models/${name}:batchEmbedContents`;
   return async (inputs, signal, timeoutMs) => {
      const reply = (await postJson({
         fetchFn: args.fetchFn,
         url,
         headers: { "x-goog-api-key": args.apiKey },
         body: {
            requests: inputs.map((text) => ({
               model: `models/${name}`,
               content: { parts: [{ text }] },
               ...(args.dimensions !== undefined
                  ? { outputDimensionality: args.dimensions }
                  : {}),
            })),
         },
         signal,
         timeoutMs,
         secrets: [args.apiKey],
         authHint: "EMBEDDING_API_KEY",
         what: "Embedding request",
      })) as { embeddings?: { values?: unknown }[] };
      const embeddings = reply?.embeddings;
      return checkVectors(
         Array.isArray(embeddings)
            ? embeddings.map((e) => e?.values)
            : embeddings,
         inputs.length,
         url,
      );
   };
}
