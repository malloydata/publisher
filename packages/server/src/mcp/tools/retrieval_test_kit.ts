// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Shared scaffolding for the specs that drive get_context through its handler
// with a scripted embedding provider and a scripted LLM. Test-only: nothing in
// the server imports this.

import { DEFAULT_EMBEDDING_MIN_SIMILARITY } from "../../config";
import type { EnvironmentStore } from "../../service/environment_store";
import { EmbeddingProvider } from "../../service/embedding_provider";
import {
   LlmError,
   type LlmProvider,
   type LlmRequest,
} from "../../service/llm_provider";
import type { DuckDBConnection } from "../../storage/duckdb/DuckDBConnection";
import { registerGetContextTool } from "./get_context_tool";

export type Content = Array<{ type?: string; resource?: { text: string } }>;
export type Extra = { requestInfo?: { headers?: Record<string, string> } };
export type Result = { isError?: boolean; content: Content };
export type Handler = (
   params: Record<string, unknown>,
   extra?: Extra,
) => Promise<Result>;

export function captureHandler(store: Partial<EnvironmentStore>): Handler {
   const handlers = new Map<string, Handler>();
   registerGetContextTool(
      {
         tool: (name: string, _d: string, _s: unknown, h: Handler) => {
            handlers.set(name, h);
         },
      } as never,
      store as EnvironmentStore,
   );
   return handlers.get("get_context")!;
}

export const parse = (r: Result) => JSON.parse(r.content[0].resource!.text);

/** A store whose one package is `pkg`, backed by `db` for the vector cache. */
export function storeFor(
   pkg: unknown,
   db: DuckDBConnection,
): Partial<EnvironmentStore> {
   return {
      getEnvironment: async () =>
         ({
            getPackage: async () => pkg,
            getStaleCompileErrors: () => new Map(),
         }) as never,
      storageManager: { getDuckDbConnection: () => db } as never,
   };
}

/** An embedding provider over an explicit text -> vector map; unknown text throws. */
export function embeddingsFor(vectors: Record<string, number[]>): EmbeddingProvider {
   const fetchStub = (async (_u: RequestInfo | URL, init?: RequestInit) => {
      const body = JSON.parse(String(init?.body)) as { input: string[] };
      return new Response(
         JSON.stringify({
            data: body.input.map((text, index) => {
               const embedding = vectors[text];
               if (!embedding) throw new Error(`no stub vector for "${text}"`);
               return { index, embedding };
            }),
         }),
         { status: 200 },
      );
   }) as typeof fetch;
   return new EmbeddingProvider(
      {
         apiKey: "t",
         model: "stub",
         baseUrl: "https://stub.example.com/v1",
         minSimilarity: DEFAULT_EMBEDDING_MIN_SIMILARITY,
      },
      fetchStub,
   );
}

/** A fake LLM: `reply` sees each request and answers with text, or an error to throw. */
export function fakeLlm(
   reply: (req: LlmRequest, n: number) => string | LlmError,
   seen: LlmRequest[] = [],
): LlmProvider {
   return {
      id: "fake-llm",
      async complete(req) {
         seen.push(req);
         const out = reply(req, seen.length);
         if (out instanceof LlmError) throw out;
         return {
            text: out,
            model: req.model,
            usage: { promptTokens: 10, completionTokens: 5 },
            latencyMs: 1,
         };
      },
   };
}

/** Header set that turns every LLM stage off for one call. */
export const STAGES_OFF: Extra = {
   requestInfo: {
      headers: {
         "x-publisher-retrieval": JSON.stringify({
            refine: { enabled: false },
            rerank: { enabled: false },
         }),
      },
   },
};

/**
 * Wait until the embedding index has built (polling with the LLM stages off,
 * so the lexical answers given meanwhile spend no LLM calls and trip no
 * breaker), then make the one call under test. Needs
 * PUBLISHER_RETRIEVAL_OVERRIDES=1.
 */
export async function afterWarmup(
   handler: Handler,
   params: Record<string, unknown>,
   extra?: Extra,
): Promise<any> {
   for (let i = 0; i < 400; i++) {
      const warm = parse(await handler(params, STAGES_OFF));
      if (warm.retrieval === "semantic") return parse(await handler(params, extra));
      await new Promise((r) => setTimeout(r, 5));
   }
   throw new Error("retrieval never became semantic");
}

/** `[{index, score, reason}]` in the refine reply shape. */
export const rateReply = (items: Array<[number, string, string?]>) =>
   JSON.stringify(
      items.map(([index, score, reason]) => ({ index, score, reason: reason ?? "r" })),
   );

/** `[{index, score}]` in the rerank reply shape. */
export const rankReply = (items: Array<[number, number]>) =>
   JSON.stringify(items.map(([index, score]) => ({ source: "s", index, score })));

/** The index a source has in a rerank prompt. */
export function sourceIndex(req: LlmRequest, source: string): number {
   const m = req.user.match(new RegExp(`\\[(\\d+)\\] Source: ${source},`));
   if (!m) throw new Error(`source ${source} not in the rerank prompt`);
   return Number(m[1]);
}

/** The index an entity has in a refine prompt. */
export function entityIndex(req: LlmRequest, name: string): number {
   const m = req.user.match(new RegExp(`- \\[(\\d+)\\] ${name} \\(`));
   if (!m) throw new Error(`${name} not in the refine prompt`);
   return Number(m[1]);
}

export const entityNames = (payload: any): string[] =>
   payload.sources.flatMap((c: any) => (c.entities ?? []).map((e: any) => e.name));

export const sourceNames = (payload: any): string[] =>
   payload.sources.map((c: any) => c.source_info.resource_id.source);
