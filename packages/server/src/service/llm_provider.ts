// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { getLlmConfig, type LlmConfig } from "../config";

/**
 * Which retrieval stage is asking. Carried for metrics and traces only; the
 * provider never branches on it.
 */
export type LlmStageName =
   | "refine"
   | "rerank"
   | "keyphrase"
   | "summary"
   | "valueRefine";

export interface LlmRequest {
   stage: LlmStageName;
   model: string;
   system?: string;
   user: string;
   /** 0 by default: a stage that scores candidates should be repeatable. */
   temperature?: number;
   seed?: number;
   maxTokens?: number;
   /**
    * "json_object" asks the endpoint for JSON mode. Endpoints that reject it
    * (a 400 naming `response_format`) are retried once without it, and
    * remembered, so the caller need not know which server it is talking to.
    */
   jsonMode?: "none" | "json_object";
   /** Merged into the request body last (provider-specific parameters). */
   extraBody?: Record<string, unknown>;
   timeoutMs: number;
   signal?: AbortSignal;
}

export interface LlmResponse {
   text: string;
   model: string;
   finishReason?: string;
   usage?: { promptTokens?: number; completionTokens?: number };
   latencyMs: number;
}

export type LlmErrorKind =
   | "timeout"
   | "auth"
   | "rate_limit"
   | "http"
   | "network"
   | "malformed"
   | "aborted"
   | "breaker"
   | "budget";

/**
 * A failed LLM call. `retryable` says whether trying the same request again
 * could plausibly succeed; the runner owns the decision to do so. Messages
 * carry at most a short body excerpt and never a credential.
 */
export class LlmError extends Error {
   constructor(
      message: string,
      public readonly kind: LlmErrorKind,
      public readonly retryable: boolean,
      public readonly status?: number,
      public readonly retryAfterMs?: number,
   ) {
      super(message);
      this.name = "LlmError";
   }
}

/** The only thing the pipeline needs from a model server. */
export interface LlmProvider {
   readonly id: string;
   complete(request: LlmRequest): Promise<LlmResponse>;
}

type FetchFn = typeof fetch;

/** Reasoning models wrap their scratch work in <think>; it is not the answer. */
export function stripThinking(text: string): string {
   return text
      .replace(/<think>[\s\S]*?<\/think>/gi, "")
      .replace(/<think>[\s\S]*$/i, "")
      .trim();
}

function contentText(message: unknown): string {
   if (typeof message !== "object" || message === null) return "";
   const m = message as {
      content?: unknown;
      reasoning_content?: unknown;
   };
   const fromContent = (c: unknown): string => {
      if (typeof c === "string") return c;
      // Some servers return content as a list of {type:"text", text} parts.
      if (Array.isArray(c)) {
         return c
            .map((p) =>
               typeof p === "string"
                  ? p
                  : typeof (p as { text?: unknown })?.text === "string"
                    ? (p as { text: string }).text
                    : "",
            )
            .join("");
      }
      return "";
   };
   const content = stripThinking(fromContent(m.content));
   if (content) return content;
   // A thinking model that spent its whole budget thinking leaves content
   // empty; the reasoning field is the only text there is.
   return typeof m.reasoning_content === "string"
      ? stripThinking(m.reasoning_content)
      : "";
}

function parseRetryAfter(header: string | null): number | undefined {
   if (!header) return undefined;
   const seconds = Number(header);
   if (Number.isFinite(seconds) && seconds >= 0) {
      return Math.min(seconds * 1000, 60_000);
   }
   const date = Date.parse(header);
   if (Number.isFinite(date)) {
      return Math.max(0, Math.min(date - Date.now(), 60_000));
   }
   return undefined;
}

/**
 * Client for an OpenAI-compatible `POST {base}/chat/completions` endpoint
 * (OpenAI, Ollama, vLLM, LM Studio, and other servers that speak it), called
 * with global fetch and no provider SDK, the same way EmbeddingProvider calls
 * `/embeddings`. The key travels only in the Authorization header, is omitted
 * entirely when there is none (a local server), and is never logged.
 */
export class OpenAiCompatLlmProvider implements LlmProvider {
   readonly id: string;
   /** Set once an endpoint has rejected JSON mode, so later calls skip it. */
   private jsonModeRejected = false;

   constructor(
      private config: LlmConfig,
      private fetchFn: FetchFn = fetch,
   ) {
      this.id = new URL(config.baseUrl).host;
   }

   async complete(request: LlmRequest): Promise<LlmResponse> {
      const wantJson =
         request.jsonMode === "json_object" && !this.jsonModeRejected;
      try {
         return await this.send(request, wantJson);
      } catch (error) {
         if (
            wantJson &&
            error instanceof LlmError &&
            error.kind === "http" &&
            error.status === 400 &&
            /response_format|json_object|json mode/i.test(error.message)
         ) {
            this.jsonModeRejected = true;
            return this.send(request, false);
         }
         throw error;
      }
   }

   private async send(
      request: LlmRequest,
      jsonMode: boolean,
   ): Promise<LlmResponse> {
      const url = `${this.config.baseUrl}/chat/completions`;
      const messages: Array<{ role: string; content: string }> = [];
      if (request.system) {
         messages.push({ role: "system", content: request.system });
      }
      messages.push({ role: "user", content: request.user });

      const body: Record<string, unknown> = {
         model: request.model,
         messages,
         temperature: request.temperature ?? 0,
         stream: false,
      };
      if (request.seed !== undefined) body.seed = request.seed;
      if (request.maxTokens !== undefined) body.max_tokens = request.maxTokens;
      if (jsonMode) body.response_format = { type: "json_object" };
      Object.assign(body, request.extraBody);

      const headers: Record<string, string> = {
         "Content-Type": "application/json",
      };
      if (this.config.apiKey) {
         headers.Authorization = `Bearer ${this.config.apiKey}`;
      }

      const signals = [AbortSignal.timeout(request.timeoutMs)];
      if (request.signal) signals.push(request.signal);
      const started = Date.now();

      let response: Response;
      try {
         response = await this.fetchFn(url, {
            method: "POST",
            headers,
            body: JSON.stringify(body),
            signal: AbortSignal.any(signals),
         });
      } catch (error) {
         const name = (error as Error)?.name;
         if (request.signal?.aborted) {
            throw new LlmError(`LLM request to ${url} was aborted`, "aborted", false);
         }
         if (name === "TimeoutError" || name === "AbortError") {
            throw new LlmError(
               `LLM request to ${url} timed out after ${request.timeoutMs}ms`,
               "timeout",
               true,
            );
         }
         throw new LlmError(
            `LLM request to ${url} failed: ${(error as Error).message}`,
            "network",
            true,
         );
      }

      if (!response.ok) {
         throw await this.httpError(url, response);
      }

      let json: {
         choices?: Array<{ message?: unknown; finish_reason?: string }>;
         model?: string;
         usage?: { prompt_tokens?: number; completion_tokens?: number };
      };
      try {
         json = (await response.json()) as typeof json;
      } catch {
         throw new LlmError(
            `LLM response from ${url} was not JSON`,
            "malformed",
            true,
         );
      }
      const choice = json?.choices?.[0];
      const text = contentText(choice?.message);
      if (!choice || !text) {
         throw new LlmError(
            `LLM response from ${url} malformed: no message content` +
               (choice?.finish_reason ? ` (finish_reason ${choice.finish_reason})` : ""),
            "malformed",
            true,
         );
      }
      return {
         text,
         model: json.model ?? request.model,
         ...(choice.finish_reason ? { finishReason: choice.finish_reason } : {}),
         ...(json.usage
            ? {
                 usage: {
                    promptTokens: json.usage.prompt_tokens,
                    completionTokens: json.usage.completion_tokens,
                 },
              }
            : {}),
         latencyMs: Date.now() - started,
      };
   }

   private async httpError(url: string, response: Response): Promise<LlmError> {
      const status = response.status;
      if (status === 401 || status === 403) {
         // Auth-failure bodies commonly reflect the presented credential, and
         // this message is logged by callers. Drop them entirely.
         return new LlmError(
            `LLM request to ${url} failed (${status}): authentication failed; check LLM_API_KEY`,
            "auth",
            false,
            status,
         );
      }
      const bodyText = await response.text().catch(() => "");
      // split("") would shred the body, so an absent key skips the scrub.
      const scrubbed = (
         this.config.apiKey
            ? bodyText.split(this.config.apiKey).join("[REDACTED]")
            : bodyText
      ).slice(0, 200);
      if (status === 429) {
         return new LlmError(
            `LLM request to ${url} was rate limited (429): ${scrubbed}`,
            "rate_limit",
            true,
            status,
            parseRetryAfter(response.headers.get("retry-after")),
         );
      }
      return new LlmError(
         `LLM request to ${url} failed (${status}): ${scrubbed}`,
         "http",
         status >= 500,
         status,
      );
   }
}

// Cached on a config fingerprint, never on null, exactly like the embedding
// provider: a call after the environment changes always sees the current one.
let cached: { fingerprint: string; provider: LlmProvider } | null = null;
let testOverride: { provider: LlmProvider | null } | null = null;

/**
 * The process-wide provider for the current environment, or null when no LLM
 * endpoint is configured. Throws on a malformed LLM_API_BASE (see
 * getLlmConfig); callers on the request path catch and degrade.
 */
export function getLlmProvider(): LlmProvider | null {
   if (testOverride) return testOverride.provider;
   const config = getLlmConfig();
   if (!config) {
      cached = null;
      return null;
   }
   const fingerprint = [config.baseUrl, config.apiKey].join("\u0000");
   if (!cached || cached.fingerprint !== fingerprint) {
      cached = { fingerprint, provider: new OpenAiCompatLlmProvider(config) };
   }
   return cached.provider;
}

/** Test seam: force the provider (or null). Undo with _clear...(). */
export function _setLlmProviderForTests(provider: LlmProvider | null): void {
   testOverride = { provider };
}

export function _clearLlmProviderForTests(): void {
   testOverride = null;
   cached = null;
}
