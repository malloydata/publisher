// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Test support for the request-time LLM stages: a scripted chat model built on
 * the real provider layer (so JSON extraction, the single repair and the
 * usage counts are the production ones), and a semantic get_context handler
 * over a real DuckDB vector cache with a deterministic embedding stub.
 */

import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { DEFAULT_EMBEDDING_MIN_SIMILARITY } from "../config";
import { ChatModelImpl, type RawChat } from "../providers/chat_model";
import { _setChatModelForTests } from "../providers/active";
import type { ChatModel, LlmSettings } from "../providers/types";
import {
   EmbeddingProvider,
   _setEmbeddingProviderForTests,
} from "../service/embedding_provider";
import type { EnvironmentStore } from "../service/environment_store";
import type { RetryPolicy } from "../service/http_retry";
import { DuckDBConnection } from "../storage/duckdb/DuckDBConnection";
import { createEntityEmbeddingsTable } from "../storage/duckdb/schema";
import { _resetEmbeddingIndexStateForTests } from "../mcp/tools/embedding_index";
import { registerGetContextTool } from "../mcp/tools/get_context_tool";

/** A retry policy that never waits, so a failing call fails at once. */
export const NO_WAIT_RETRY: RetryPolicy = {
   maxAttempts: 2,
   baseDelayMs: 0,
   maxDelayMs: 0,
   maxTotalDelayMs: 0,
   sleep: async () => {},
   random: () => 0,
};

export interface ScriptedChat {
   model: ChatModel;
   /** Every prompt the model received, in arrival order (repair re-asks included). */
   prompts: string[];
   /** Most requests ever in flight at once. */
   maxInFlight: () => number;
}

/**
 * A chat model whose replies come from `reply(prompt, n)` (n counts from 1).
 * A thrown error is the vendor failing; a returned string is the model's text.
 */
export function scriptedChat(
   reply: (prompt: string, n: number) => string | Promise<string>,
   options: { delayMs?: number } = {},
): ScriptedChat {
   const prompts: string[] = [];
   let inFlight = 0;
   let peak = 0;
   const raw: RawChat = {
      async send(req) {
         prompts.push(req.prompt);
         inFlight += 1;
         peak = Math.max(peak, inFlight);
         try {
            if (options.delayMs) {
               await new Promise((r) => setTimeout(r, options.delayMs));
            }
            const text = await reply(req.prompt, prompts.length);
            return { text, usage: { inputTokens: 11, outputTokens: 5 } };
         } finally {
            inFlight -= 1;
         }
      },
   };
   return {
      model: new ChatModelImpl("openai-compatible", "scripted", raw, {
         timeoutMs: 5_000,
         retry: NO_WAIT_RETRY,
      }),
      prompts,
      maxInFlight: () => peak,
   };
}

/** The candidates (or sources) of a prompt, as [number, rest-of-line]. */
export function numberedLines(prompt: string): Array<[number, string]> {
   return [...prompt.matchAll(/^\[(\d+)\] (.*)$/gm)].map((m) => [
      Number(m[1]),
      m[2],
   ]);
}

const STOP = new Set(
   "a an the of to per one row is are for in on and or by that it its this with from".split(
      " ",
   ),
);

/** Lowercase words, identifiers split on `_`, without filler words. */
export function contentWords(text: string): Set<string> {
   return new Set(
      (
         text
            .toLowerCase()
            .replace(/_/g, " ")
            .match(/[a-z0-9]+/g) ?? []
      ).filter((w) => !STOP.has(w)),
   );
}

const overlap = (a: Set<string>, b: Set<string>) =>
   [...a].filter((w) => b.has(w)).length;

/**
 * A deterministic stand-in for an LLM that answers both stages by keyword
 * overlap with the text it is shown.
 *
 * Refine: a candidate sharing 2 or more words with the phrase is HIGH, one
 * word is MEDIUM, none is LOW. Rerank: a source sharing 3 or more words with
 * the question is 3, then 2, 1, 0 by overlap; listed best first.
 */
export function keywordReply(prompt: string): string {
   if (prompt.includes("Source search phrase:")) {
      // Source match: 2 or more shared words is HIGH, one is MEDIUM, none is
      // left out. Each candidate is two lines, so group them by blank line.
      const phrase = /Source search phrase:\n(.*)/.exec(prompt)![1];
      const words = contentWords(JSON.parse(phrase));
      const body = prompt
         .split("<candidates>\n")[1]
         .split("\n</candidates>")[0];
      return JSON.stringify(
         body.split("\n\n").flatMap((block) => {
            const index = Number(/^\[(\d+)\]/.exec(block)![1]);
            const n = overlap(
               words,
               contentWords(block.replace(/^\[\d+\]/, "")),
            );
            return n >= 2
               ? [{ index, score: "HIGH" }]
               : n === 1
                 ? [{ index, score: "MEDIUM" }]
                 : [];
         }),
      );
   }
   if (prompt.includes("<candidates>")) {
      const phrase = /Search phrase to rate the candidates against:\n(.*)/.exec(
         prompt,
      )![1];
      const words = contentWords(JSON.parse(phrase));
      return JSON.stringify(
         numberedLines(prompt).map(([index, line]) => {
            const n = overlap(words, contentWords(line));
            return {
               index,
               score: n >= 2 ? "HIGH" : n === 1 ? "MEDIUM" : "LOW",
            };
         }),
      );
   }
   const question = /Question the user is asking:\n(.*)/.exec(prompt)![1];
   const words = contentWords(question);
   const body = prompt.split("<sources>\n")[1].split("\n</sources>")[0];
   const scored = body.split("\n\n").map((block, i) => {
      const text = block.replace(/^\[\d+\] Source: [^\n]*\n/, "");
      const name = /^\[\d+\] Source: ([^,]+),/.exec(block)![1];
      const n = overlap(words, contentWords(`${name} ${text}`));
      return { index: i + 1, score: Math.min(3, n) };
   });
   scored.sort((a, b) => b.score - a.score || a.index - b.index);
   return JSON.stringify(scored);
}

/** Install `chat` as the process-wide chat model, with settings overrides. */
export function useChat(
   chat: ChatModel | null,
   settings: Partial<LlmSettings> = {},
): void {
   _setChatModelForTests(chat, settings);
}

// ---------------------------------------------------------------------------
// A semantic get_context handler
// ---------------------------------------------------------------------------

const BUCKETS = 64;

/** A deterministic bag-of-words embedding: texts sharing words are close. */
export function embedText(text: string): number[] {
   const v = new Array<number>(BUCKETS).fill(0);
   for (const word of text.toLowerCase().match(/[a-z0-9]+/g) ?? []) {
      let h = 2166136261;
      for (const ch of word) h = Math.imul(h ^ ch.charCodeAt(0), 16777619);
      v[(h >>> 0) % BUCKETS] += 1;
   }
   const norm = Math.sqrt(v.reduce((s, x) => s + x * x, 0)) || 1;
   return v.map((x) => x / norm);
}

export function stubEmbeddingProvider(): EmbeddingProvider {
   const fetchStub = (async (_url: RequestInfo | URL, init?: RequestInit) => {
      const body = JSON.parse(String(init?.body)) as { input: string[] };
      const data = body.input.map((text, index) => ({
         index,
         embedding: embedText(text),
      }));
      return new Response(JSON.stringify({ data }), { status: 200 });
   }) as typeof fetch;
   return new EmbeddingProvider(
      {
         apiKey: "test",
         model: "stub-model",
         baseUrl: "https://stub.example.com/v1",
         minSimilarity: DEFAULT_EMBEDDING_MIN_SIMILARITY,
      },
      fetchStub,
   );
}

export type Handler = (
   params: Record<string, unknown>,
   extra?: unknown,
) => Promise<{
   isError?: boolean;
   content: Array<{ resource?: { text: string } }>;
}>;

export interface SemanticHarness {
   db: DuckDBConnection;
   /** Reset the index state and install the stub embedding provider. */
   reset(): void;
   close(): Promise<void>;
   /** A get_context handler over `pkg`. */
   handlerFor(pkg: unknown): Handler;
}

export async function semanticHarness(): Promise<SemanticHarness> {
   const dir = fs.mkdtempSync(path.join(os.tmpdir(), "get-context-llm-"));
   const db = new DuckDBConnection(path.join(dir, "llm.db"));
   await db.initialize();
   await createEntityEmbeddingsTable(db);
   return {
      db,
      reset() {
         _resetEmbeddingIndexStateForTests();
         _setEmbeddingProviderForTests(stubEmbeddingProvider());
      },
      async close() {
         _setEmbeddingProviderForTests(null);
         await db.close();
         fs.rmSync(dir, { recursive: true, force: true });
      },
      handlerFor(pkg) {
         let handler: Handler | undefined;
         const store = {
            getEnvironment: async () => ({
               getPackage: async () => pkg,
               getStaleCompileErrors: () => new Map(),
            }),
            storageManager: { getDuckDbConnection: () => db },
         } as unknown as EnvironmentStore;
         registerGetContextTool(
            {
               tool: (name: string, _d: string, _s: unknown, h: Handler) => {
                  if (name === "get_context") handler = h;
               },
            } as never,
            store,
         );
         return handler as Handler;
      },
   };
}

/** Parsed JSON payload of one call. */
export async function callPayload(
   handler: Handler,
   params: Record<string, unknown>,
   extra?: unknown,
) {
   const result = await handler(params, extra);
   return {
      isError: result.isError === true,
      payload: JSON.parse(result.content[0].resource!.text),
   };
}

/** Ask until the background vector sync lands and the answer is semantic. */
export async function untilSemantic(
   handler: Handler,
   params: Record<string, unknown>,
   extra?: unknown,
) {
   for (let i = 0; i < 1_000; i++) {
      const out = await callPayload(handler, params, extra);
      if (out.payload.retrieval === "semantic" || out.isError) return out;
      await new Promise((resolve) => setTimeout(resolve, 5));
   }
   throw new Error("retrieval never became semantic");
}
