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
import { ChatModelImpl, type RawChat } from "../retrieval/llm/chat_model";
import type { ChatModel } from "../retrieval/llm/types";
import {
   EmbeddingProvider,
   _setEmbeddingProviderForTests,
} from "../retrieval/llm/embedding_provider";
import type { Environment } from "../service/environment";
import type { EnvironmentCatalog } from "../service/environment_ports";
import { RetrievalRegistry } from "../retrieval/registry";
import { RetrievalService } from "../retrieval/service";
import type { RetryPolicy } from "../service/http_retry";
import { DuckDBConnection } from "../storage/duckdb/DuckDBConnection";
import {
   createCatalogTables,
   createSourceSummariesTable,
} from "../storage/duckdb/schema";
import { registerGetContextTool } from "../mcp/tools/get_context_tool";
import { pollFor } from "./poll";

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
   /** The `maxTokens` of every request, in arrival order. */
   maxTokens: Array<number | undefined>;
   /** Most requests ever in flight at once. */
   maxInFlight: () => number;
}

/**
 * A chat model whose replies come from `reply(prompt, n)` (n counts from 1).
 * A thrown error is the vendor failing; a returned string is the model's text.
 * With `charsPerToken`, the model stops at its allowance as a real one does:
 * a reply longer than `maxTokens` tokens is cut there and reported as cut off.
 */
export function scriptedChat(
   reply: (prompt: string, n: number) => string | Promise<string>,
   options: {
      delayMs?: number;
      maxOutputTokens?: number;
      charsPerToken?: number;
   } = {},
): ScriptedChat {
   const prompts: string[] = [];
   const maxTokens: Array<number | undefined> = [];
   let inFlight = 0;
   let peak = 0;
   const raw: RawChat = {
      async send(req) {
         prompts.push(req.prompt);
         maxTokens.push(req.maxTokens);
         inFlight += 1;
         peak = Math.max(peak, inFlight);
         try {
            if (options.delayMs) {
               await new Promise((r) => setTimeout(r, options.delayMs));
            }
            const text = await reply(req.prompt, prompts.length);
            const room =
               options.charsPerToken && req.maxTokens !== undefined
                  ? req.maxTokens * options.charsPerToken
                  : Infinity;
            return {
               text: text.slice(0, room),
               usage: { inputTokens: 11, outputTokens: 5 },
               ...(text.length > room ? { truncated: true } : {}),
            };
         } finally {
            inFlight -= 1;
         }
      },
   };
   return {
      model: new ChatModelImpl("openai-compatible", "scripted", raw, {
         maxOutputTokens: options.maxOutputTokens,
         timeoutMs: 5_000,
         retry: NO_WAIT_RETRY,
      }),
      prompts,
      maxTokens,
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

/**
 * A deterministic stand-in for the source summary step: the summary lists the
 * field names it was shown in backticks, and the one-liner is the first
 * sentence of the documentation, or the exact undocumented line.
 */
export function summaryReply(prompt: string): string {
   const source = /^Source name: (.*)$/m.exec(prompt)![1];
   const doc = /^Source documentation:\n(.*)$/m.exec(prompt)![1];
   const fields = [...prompt.matchAll(/^\s*- ([^ :(]+)/gm)].map((m) => m[1]);
   const oneLine =
      doc === "No source docs."
         ? `The \`${source}\` source.`
         : (doc.split(". ")[0].replace(/\.$/, "") + ".").slice(0, 120);
   return JSON.stringify({
      summary: `${doc} Fields: ${fields.map((f) => `\`${f}\``).join(", ")}.`,
      one_line_summary: oneLine,
   });
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
   /**
    * The retrieval state every handler of this harness shares: its index
    * builds, held pages and chat model. A spec sets its seams (the chat
    * model, the value fetcher, the deadline) here.
    */
   registry: RetrievalRegistry;
   /** Reset the index state and install the stub embedding provider. */
   reset(): void;
   close(): Promise<void>;
   /** A get_context handler over `pkg`; `stale` maps a package name to its failed reload. */
   handlerFor(
      pkg: unknown,
      stale?: Map<string, { message: string; failedAt: string }>,
   ): Handler;
}

export async function semanticHarness(): Promise<SemanticHarness> {
   const dir = fs.mkdtempSync(path.join(os.tmpdir(), "get-context-llm-"));
   const db = new DuckDBConnection(path.join(dir, "llm.db"));
   await db.initialize();
   await createCatalogTables(db);
   await createSourceSummariesTable(db);
   const registry = new RetrievalRegistry();
   return {
      db,
      registry,
      reset() {
         _setEmbeddingProviderForTests(stubEmbeddingProvider());
      },
      async close() {
         _setEmbeddingProviderForTests(null);
         await db.close();
         fs.rmSync(dir, { recursive: true, force: true });
      },
      handlerFor(pkg, stale = new Map()) {
         let handler: Handler | undefined;
         const environment = {
            getPackage: async () => pkg,
            getStaleCompileErrors: () => stale,
         } as unknown as Environment;
         const store: EnvironmentCatalog = {
            getEnvironment: async () => environment,
            listEnvironments: async () => [],
         };
         registerGetContextTool(
            {
               registerTool: (name: string, _config: unknown, h: Handler) => {
                  if (name === "get_context") handler = h;
               },
            } as never,
            store,
            new RetrievalService(
               store,
               { getDuckDbConnection: () => db },
               registry,
            ),
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
   return pollFor(
      () => callPayload(handler, params, extra),
      (out) => out.payload.retrieval === "semantic" || out.isError,
      5_000,
   );
}
