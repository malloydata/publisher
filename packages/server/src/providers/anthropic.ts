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

/**
 * The model's version, from an id like `claude-haiku-5-5` or
 * `claude-opus-4-8`; null for an id in another shape (`claude-3-5-haiku-...`,
 * a gateway alias), which is treated as an older model.
 */
function claudeVersion(
   model: string,
): { line: string; major: number; minor: number } | null {
   const m = /^claude-([a-z]+)-(\d+)(?:-(\d{1,2}))?(?:$|[-@])/.exec(
      model.toLowerCase().replace(/^.*\//, ""),
   );
   if (!m) return null;
   return { line: m[1], major: Number(m[2]), minor: Number(m[3] ?? 0) };
}

/**
 * The sampling fields a Messages request carries for this model. Temperature
 * 0 keeps ranking stable where the model allows it; Claude 4.7 and later
 * (every 5.x model, Fable, Mythos) answer 400 "`temperature` is deprecated
 * for this model", so it is left out there. Haiku 5 and later think by
 * default, and the thinking counts against `max_tokens`: a caller that sizes
 * its cap for the reply alone (the judge) gets a reply cut off mid-JSON, so
 * thinking is turned off for them. Other current lines either take no
 * "disabled" (Opus 5.5, Sonnet 5.5) or think only when asked, and keep the
 * vendor default.
 */
export function anthropicSampling(model: string): Record<string, unknown> {
   const v = claudeVersion(model);
   if (!v) return { temperature: 0 };
   const modern =
      v.line === "fable" ||
      v.line === "mythos" ||
      v.major >= 5 ||
      (v.major === 4 && v.minor >= 7);
   if (!modern) return { temperature: 0 };
   if (v.line === "haiku" && v.major >= 5) {
      return { thinking: { type: "disabled" } };
   }
   return {};
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
         ...anthropicSampling(this.model),
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
