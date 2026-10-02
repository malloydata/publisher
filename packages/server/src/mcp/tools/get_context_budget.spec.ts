// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The response budget through the tool: a ranked answer on either path is cut
 * to whole cards that fit in 35,000 characters, while a listing is not.
 */

import {
   afterAll,
   afterEach,
   beforeAll,
   beforeEach,
   describe,
   expect,
   it,
} from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { DEFAULT_EMBEDDING_MIN_SIMILARITY } from "../../config";
import {
   EmbeddingProvider,
   _clearEmbeddingProviderForTests,
   _setEmbeddingProviderForTests,
} from "../../service/embedding_provider";
import { DuckDBConnection } from "../../storage/duckdb/DuckDBConnection";
import { createEntityEmbeddingsTable } from "../../storage/duckdb/schema";
import { _resetEmbeddingIndexStateForTests } from "./embedding_index";
import { embeddingSyncQueue } from "./embedding_sync_queue";
import { registerGetContextTool } from "./get_context_tool";

const BUDGET = 35_000;
const SOURCES = 120;
const FIELDS_PER_SOURCE = 3;
const FILLER = "These readings describe the device group in detail. ".repeat(8);

const field = (name: string) => ({
   kind: "dimension",
   name,
   annotations: [`#(doc) Metric readings, ${name}. ${FILLER}`],
});

/** Enough sources, each with a long doc and long field docs, to pass the budget. */
function bigPackage() {
   const model = {
      getSourceInfos: () =>
         Array.from({ length: SOURCES }, (_, i) => ({
            name: `group_${String(i).padStart(3, "0")}`,
            annotations: [`#(doc) Metric readings for group ${i}. ${FILLER}`],
            schema: {
               fields: Array.from({ length: FIELDS_PER_SOURCE }, (_, f) =>
                  field(`metric_${f}`),
               ),
            },
         })),
      getQueries: () => [],
   };
   return {
      listModels: async () => [{ path: "big.malloy" }],
      getModel: () => model,
   };
}

const envWith = (pkg: unknown) =>
   ({
      getPackage: async () => pkg,
      getStaleCompileErrors: () => new Map(),
   }) as never;

type Handler = (params: Record<string, unknown>) => Promise<{
   content: Array<{ resource?: { text: string } }>;
}>;

function handlerFor(store: Record<string, unknown>): Handler {
   let handler: Handler | undefined;
   registerGetContextTool(
      {
         tool: (name: string, _d: string, _s: unknown, h: Handler) => {
            if (name === "get_context") handler = h;
         },
      } as never,
      store as never,
   );
   if (!handler) throw new Error("get_context was not registered");
   return handler;
}

async function ask(
   handler: Handler,
   packageName: string,
   targets: unknown[],
   extra: Record<string, unknown> = {},
) {
   const result = await handler({
      search_targets: targets,
      scopes: [{ environment: "env", package: packageName }],
      limit: 150,
      ...extra,
   });
   const text = result.content[0].resource?.text as string;
   return { text, payload: JSON.parse(text) };
}

const query = [{ target_type: "dimension", search_text: "metric readings" }];

function expectFitsWholeCards(text: string, payload: Record<string, unknown>) {
   // The envelope and the cards together are under the budget.
   expect(text.length).toBeLessThanOrEqual(BUDGET);
   const sources = payload.sources as Array<{ entities?: unknown[] }>;
   expect(sources.length).toBeGreaterThanOrEqual(1);
   expect(sources.length).toBeLessThan(SOURCES);
   expect(payload.returned).toBe(sources.length);
   // total_available still counts the sources the budget removed.
   expect(payload.total_available).toBe(SOURCES);
   // Whole cards: none was trimmed inside.
   for (const card of sources) {
      expect(card.entities).toHaveLength(FIELDS_PER_SOURCE);
   }
   const dropped = SOURCES - sources.length;
   expect(payload.warnings).toEqual([
      `${dropped} further sources matched but were left out to keep the response under 35,000 characters. Narrow with scopes or a more specific question to see them.`,
   ]);
}

describe("get_context response budget: lexical", () => {
   beforeEach(() => {
      _setEmbeddingProviderForTests(null);
      _resetEmbeddingIndexStateForTests();
   });

   it("cuts a ranked answer to whole cards under the budget", async () => {
      const handler = handlerFor({
         getEnvironment: async () => envWith(bigPackage()),
      });
      const { text, payload } = await ask(handler, "lex", query);
      expectFitsWholeCards(text, payload);
   });

   it("leaves a listing alone", async () => {
      const handler = handlerFor({
         getEnvironment: async () => envWith(bigPackage()),
      });
      const { text, payload } = await ask(
         handler,
         "lex-listing",
         [{ target_type: "source" }],
         { limit: 150 },
      );
      expect(payload.returned).toBe(SOURCES);
      expect(text.length).toBeGreaterThan(BUDGET);
      expect(JSON.stringify(payload.warnings ?? [])).not.toContain("left out");
   });

   it("says nothing when the answer fits", async () => {
      const handler = handlerFor({
         getEnvironment: async () => envWith(bigPackage()),
      });
      const { payload } = await ask(handler, "lex-small", query, { limit: 2 });
      expect(payload.returned).toBe(2);
      // The page cut (2 of 120) is the only warning; the budget did not act.
      expect(payload.warnings).toHaveLength(1);
      expect(payload.warnings[0]).toContain("Raise limit");
   });
});

describe("get_context response budget: semantic", () => {
   let tempDir: string;
   let db: DuckDBConnection;

   beforeAll(async () => {
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "get-context-budget-"));
      db = new DuckDBConnection(path.join(tempDir, "budget.db"));
      await db.initialize();
      await createEntityEmbeddingsTable(db);
   });
   afterAll(async () => {
      _clearEmbeddingProviderForTests();
      await embeddingSyncQueue.idle();
      await db.close();
      fs.rmSync(tempDir, { recursive: true, force: true });
   });
   beforeEach(() => {
      _setEmbeddingProviderForTests(stubProvider());
      _resetEmbeddingIndexStateForTests();
   });
   afterEach(() => {
      _setEmbeddingProviderForTests(null);
   });

   it("cuts a ranked answer to whole cards under the budget", async () => {
      const handler = handlerFor({
         getEnvironment: async () => envWith(bigPackage()),
         storageManager: { getDuckDbConnection: () => db },
      });
      for (let i = 0; i < 2_000; i++) {
         const { text, payload } = await ask(handler, "sem", query);
         if (payload.retrieval === "semantic") {
            expectFitsWholeCards(text, payload);
            return;
         }
         await new Promise((resolve) => setTimeout(resolve, 5));
      }
      throw new Error("retrieval never became semantic");
   });
});

/** Every text embeds to the same direction, so every source clears the floor. */
function stubProvider(): EmbeddingProvider {
   const fetchStub = (async (_url: RequestInfo | URL, init?: RequestInit) => {
      const body = JSON.parse(String(init?.body)) as { input: string[] };
      const data = body.input.map((_, index) => ({
         index,
         embedding: [1, 0, 0, 0],
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
