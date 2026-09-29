// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Pins the full get_context response for a handful of scenarios, recorded
// before the LLM retrieval stages existed. With nothing about retrieval
// configured, the payload must stay byte-identical: no new keys, no changed
// values. Any diff here is a regression in the default path, not an update to
// bless.
//
// To re-record after a deliberate default-path change:
//    UPDATE_GET_CONTEXT_GOLDEN=1 bun test src/mcp/tools/get_context_payload_pin.spec.ts

import { afterAll, beforeAll, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { DEFAULT_EMBEDDING_MIN_SIMILARITY } from "../../config";
import type { EnvironmentStore } from "../../service/environment_store";
import {
   EmbeddingProvider,
   _clearEmbeddingProviderForTests,
   _setEmbeddingProviderForTests,
} from "../../service/embedding_provider";
import { DuckDBConnection } from "../../storage/duckdb/DuckDBConnection";
import { createEntityEmbeddingsTable } from "../../storage/duckdb/schema";
import { _resetEmbeddingIndexStateForTests } from "./embedding_index";
import { registerGetContextTool } from "./get_context_tool";

const GOLDEN_PATH = path.join(
   __dirname,
   "testdata",
   "get_context_payloads.golden.json",
);

type Content = Array<{ type?: string; resource?: { text: string } }>;
type Handler = (
   params: Record<string, unknown>,
   extra?: unknown,
) => Promise<{ isError?: boolean; content: Content }>;

function captureHandler(store: Partial<EnvironmentStore>): Handler {
   const handlers = new Map<string, Handler>();
   const fakeServer = {
      tool: (name: string, _d: string, _s: unknown, h: Handler) => {
         handlers.set(name, h);
      },
   };
   registerGetContextTool(fakeServer as never, store as EnvironmentStore);
   return handlers.get("get_context")!;
}

const parse = (r: { content: Content }) =>
   JSON.parse(r.content[0].resource!.text);

const model = {
   getSourceInfos: () => [
      {
         name: "orders",
         annotations: ["#(doc) One row per order."],
         schema: {
            fields: [
               {
                  kind: "measure",
                  name: "total_revenue",
                  annotations: ["#(doc) Sum of order revenue."],
               },
               {
                  kind: "dimension",
                  name: "status",
                  annotations: ["#(doc) Order status."],
               },
               { kind: "view", name: "by_month", annotations: [] },
            ],
         },
      },
      {
         name: "customers",
         annotations: ["#(doc) One row per customer."],
         schema: {
            fields: [
               {
                  kind: "dimension",
                  name: "state",
                  annotations: ["#(doc) Customer home state."],
               },
            ],
         },
      },
   ],
   getQueries: () => [],
};
const pkg = {
   listModels: async () => [{ path: "sales.malloy" }],
   getModel: () => model,
};
const envWith = () =>
   ({
      getPackage: async () => pkg,
      getStaleCompileErrors: () => new Map(),
   }) as never;

const scope = { environment: "pin", package: "sales" };

const VECTORS: Record<string, number[]> = {
   orders: [1, 0],
   "orders: One row per order.": [1, 0],
   "total revenue": [1, 0],
   "total revenue: Sum of order revenue.": [1, 0],
   status: [0, 1],
   "status: Order status.": [0, 1],
   "by month": [0, 1],
   customers: [0, 1],
   "customers: One row per customer.": [0, 1],
   state: [0, 1],
   "state: Customer home state.": [0, 1],
   "revenue by order": [1, 0],
   // Points away from every facet: nothing clears the floor.
   "seismic retrofit of bridge pilings": [-1, -1],
};

function stubProvider(): EmbeddingProvider {
   const fetchStub = (async (_u: RequestInfo | URL, init?: RequestInit) => {
      const body = JSON.parse(String(init?.body)) as { input: string[] };
      const data = body.input.map((text, index) => {
         const embedding = VECTORS[text];
         if (!embedding) throw new Error(`no stub vector for "${text}"`);
         return { index, embedding };
      });
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

describe("get_context payload pin (default path is byte-identical)", () => {
   let tempDir: string;
   let db: DuckDBConnection;
   const recorded: Record<string, unknown> = {};

   beforeAll(async () => {
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "get-context-pin-"));
      db = new DuckDBConnection(path.join(tempDir, "test.db"));
      await db.initialize();
      await createEntityEmbeddingsTable(db);
   });

   afterAll(async () => {
      await db.close();
      fs.rmSync(tempDir, { recursive: true, force: true });
      _clearEmbeddingProviderForTests();
      if (process.env.UPDATE_GET_CONTEXT_GOLDEN) {
         fs.mkdirSync(path.dirname(GOLDEN_PATH), { recursive: true });
         fs.writeFileSync(
            GOLDEN_PATH,
            JSON.stringify(recorded, null, 2) + "\n",
         );
      }
   });

   beforeEach(() => {
      _setEmbeddingProviderForTests(null);
      _resetEmbeddingIndexStateForTests();
   });

   const store = (): Partial<EnvironmentStore> => ({
      getEnvironment: async () => envWith(),
      storageManager: { getDuckDbConnection: () => db } as never,
   });

   const golden = (): Record<string, unknown> =>
      JSON.parse(fs.readFileSync(GOLDEN_PATH, "utf8"));

   function check(name: string, payload: unknown) {
      if (process.env.UPDATE_GET_CONTEXT_GOLDEN) {
         recorded[name] = payload;
         return;
      }
      expect(payload).toEqual(golden()[name]);
   }

   async function untilSemantic(
      handler: Handler,
      params: Record<string, unknown>,
   ) {
      for (let i = 0; i < 400; i++) {
         const payload = parse(await handler(params));
         if (payload.retrieval === "semantic") return payload;
         await new Promise((r) => setTimeout(r, 5));
      }
      throw new Error("never became semantic");
   }

   it("lexical ranked search", async () => {
      const handler = captureHandler(store());
      const payload = parse(
         await handler({
            search_targets: [
               { target_type: "measure", search_text: "total revenue" },
               { target_type: "dimension", search_text: "status" },
            ],
            scopes: [scope],
         }),
      );
      expect(payload.retrieval).toBeUndefined();
      check("lexical ranked", payload);
   });

   it("lexical source listing", async () => {
      const handler = captureHandler(store());
      const payload = parse(
         await handler({
            search_targets: [{ target_type: "source" }],
            scopes: [scope],
         }),
      );
      check("lexical listing", payload);
   });

   it("dimensional_value target is unsupported with a warning", async () => {
      const handler = captureHandler(store());
      const payload = parse(
         await handler({
            search_targets: [
               { target_type: "dimensional_value", search_text: "Premium" },
            ],
            scopes: [scope],
         }),
      );
      check("dimensional value unsupported", payload);
   });

   it("semantic ranked search", async () => {
      _setEmbeddingProviderForTests(stubProvider());
      const handler = captureHandler(store());
      const payload = await untilSemantic(handler, {
         search_targets: [
            { target_type: "measure", search_text: "revenue by order" },
         ],
         scopes: [scope],
      });
      check("semantic ranked", payload);
   });

   it("semantic true negative", async () => {
      _setEmbeddingProviderForTests(stubProvider());
      const handler = captureHandler(store());
      // Warm the index with a query that hits, then ask the unmodelled one.
      await untilSemantic(handler, {
         search_targets: [
            { target_type: "measure", search_text: "revenue by order" },
         ],
         scopes: [scope],
      });
      const payload = parse(
         await handler({
            search_targets: [
               {
                  target_type: "measure",
                  search_text: "seismic retrofit of bridge pilings",
               },
            ],
            scopes: [scope],
         }),
      );
      check("semantic true negative", payload);
   });
});
