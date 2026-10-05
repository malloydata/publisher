// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { malformedReply } from "../service/http_retry";
import { postJson } from "./http";
import type { RawChat, RawChatRequest } from "./chat_model";
import type { ChatResult, FetchFn, ProviderName } from "./types";

export const OPENAI_BASE_URL = "https://api.openai.com/v1";
export const OLLAMA_BASE_URL = "http://localhost:11434/v1";

/** The default base URL for a provider that speaks this protocol, if it has one. */
export function defaultOpenAiBaseUrl(
   provider: ProviderName,
): string | undefined {
   if (provider === "openai") return OPENAI_BASE_URL;
   if (provider === "ollama") return OLLAMA_BASE_URL;
   return undefined;
}

/**
 * Whether OpenAI's own endpoint refuses a `temperature` other than the default
 * for this model. The reasoning families (o1, o3, o4 and the gpt-5 models)
 * answer 400 to `temperature: 0`; gpt-5 "chat" variants accept it. Applies to
 * OpenAI only: a compatible server (Ollama, vLLM, a gateway) takes the field
 * whatever the model is called.
 */
export function rejectsTemperature(
   provider: ProviderName,
   model: string,
): boolean {
   if (provider !== "openai") return false;
   const name = model.toLowerCase().replace(/^.*\//, "");
   if (/^gpt-5.*-chat/.test(name)) return false;
   return /^(o\d|gpt-5)/.test(name);
}

interface OpenAiChatReply {
   choices?: { message?: { content?: unknown } }[];
   usage?: { prompt_tokens?: unknown; completion_tokens?: unknown };
}

function count(value: unknown): number | undefined {
   return typeof value === "number" && Number.isFinite(value)
      ? value
      : undefined;
}

/**
 * Chat through an OpenAI-style `/chat/completions` endpoint: OpenAI itself,
 * Ollama's compatibility endpoint, and any server that copies the shape
 * (vLLM, Azure, a gateway). Temperature is 0 unless the model rejects it. `response_format` is sent only
 * when JSON was asked for. Ollama and some gateways need no key, so the
 * Authorization header is omitted when there is none.
 */
export class OpenAiCompatibleChat implements RawChat {
   constructor(
      private readonly provider: ProviderName,
      private readonly model: string,
      private readonly baseUrl: string,
      private readonly apiKey: string | undefined,
      private readonly fetchFn: FetchFn,
   ) {}

   async send(req: RawChatRequest): Promise<ChatResult> {
      const url = `${this.baseUrl}/chat/completions`;
      const messages: { role: string; content: string }[] = [];
      if (req.system) messages.push({ role: "system", content: req.system });
      messages.push({ role: "user", content: req.prompt });
      const body: Record<string, unknown> = {
         model: this.model,
         messages,
      };
      // Temperature 0 keeps ranking stable where the model allows it.
      if (!rejectsTemperature(this.provider, this.model)) body.temperature = 0;
      if (req.maxTokens !== undefined) {
         // OpenAI renamed the field; other servers still read max_tokens.
         body[
            this.provider === "openai" ? "max_completion_tokens" : "max_tokens"
         ] = req.maxTokens;
      }
      if (req.json) body.response_format = { type: "json_object" };

      const reply = (await postJson({
         fetchFn: this.fetchFn,
         url,
         headers: this.apiKey ? { Authorization: `Bearer ${this.apiKey}` } : {},
         body,
         signal: req.signal,
         timeoutMs: req.timeoutMs,
         secrets: [this.apiKey],
         authHint: "LLM_API_KEY",
         what: "Chat request",
      })) as OpenAiChatReply;

      const content = reply?.choices?.[0]?.message?.content;
      if (typeof content !== "string") {
         throw malformedReply(
            "Chat response",
            url,
            "no choices[0].message.content",
         );
      }
      return {
         text: content,
         usage: {
            inputTokens: count(reply.usage?.prompt_tokens),
            outputTokens: count(reply.usage?.completion_tokens),
         },
      };
   }
}
