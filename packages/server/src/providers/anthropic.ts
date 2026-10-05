// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { RawChat, RawChatRequest } from "./chat_model";
import { malformedReply } from "../service/http_retry";
import { postJson } from "./http";
import type { ChatResult, FetchFn } from "./types";

export const ANTHROPIC_BASE_URL = "https://api.anthropic.com";
export const ANTHROPIC_VERSION = "2023-06-01";
/** The Messages API requires max_tokens; this applies when the caller gives none. */
export const ANTHROPIC_DEFAULT_MAX_TOKENS = 1_024;

interface AnthropicReply {
   content?: { type?: string; text?: unknown }[];
   usage?: { input_tokens?: unknown; output_tokens?: unknown };
}

function count(value: unknown): number | undefined {
   return typeof value === "number" && Number.isFinite(value)
      ? value
      : undefined;
}

/**
 * Chat through the Anthropic Messages API. Chat only: Anthropic has no
 * embeddings endpoint. There is no JSON mode, so JSON requests rely on the
 * instruction the shared layer adds plus its validate-and-repair step.
 */
export class AnthropicChat implements RawChat {
   constructor(
      private readonly model: string,
      private readonly baseUrl: string,
      private readonly apiKey: string,
      private readonly fetchFn: FetchFn,
   ) {}

   async send(req: RawChatRequest): Promise<ChatResult> {
      const url = `${this.baseUrl}/v1/messages`;
      const body: Record<string, unknown> = {
         model: this.model,
         max_tokens: req.maxTokens ?? ANTHROPIC_DEFAULT_MAX_TOKENS,
         temperature: 0,
         messages: [{ role: "user", content: req.prompt }],
      };
      if (req.system) body.system = req.system;

      const reply = (await postJson({
         fetchFn: this.fetchFn,
         url,
         headers: {
            "x-api-key": this.apiKey,
            "anthropic-version": ANTHROPIC_VERSION,
         },
         body,
         signal: req.signal,
         timeoutMs: req.timeoutMs,
         secrets: [this.apiKey],
         authHint: "LLM_API_KEY",
         what: "Chat request",
      })) as AnthropicReply;

      const text = (reply?.content ?? [])
         .filter((b) => b?.type === "text" && typeof b.text === "string")
         .map((b) => b.text as string)
         .join("");
      if (!Array.isArray(reply?.content) || text === "") {
         throw malformedReply("Chat response", url, "no text content block");
      }
      return {
         text,
         usage: {
            inputTokens: count(reply.usage?.input_tokens),
            outputTokens: count(reply.usage?.output_tokens),
         },
      };
   }
}
