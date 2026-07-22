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
import { docText, sanitize, registerGetContextTool } from "./get_context_tool";
import type { EnvironmentStore } from "../../service/environment_store";
import { DuckDBConnection } from "../../storage/duckdb/DuckDBConnection";
import { createEntityEmbeddingsTable } from "../../storage/duckdb/schema";
import {
   EmbeddingProvider,
   _clearEmbeddingProviderForTests,
   _setEmbeddingProviderForTests,
} from "../../service/embedding_provider";
import { _resetEmbeddingIndexStateForTests } from "./embedding_index";

// Pin the baseline: every test in this file runs unconfigured (lexical)
// unless it sets a provider itself, regardless of ambient EMBEDDING_* env
// vars in the developer's shell.
beforeEach(() => {
   _setEmbeddingProviderForTests(null);
   _resetEmbeddingIndexStateForTests();
});
afterAll(() => {
   _clearEmbeddingProviderForTests();
});

describe("get_context docText", () => {
   it("extracts #(doc) text from annotation lines", () => {
      expect(docText(["#(doc) Total revenue in USD."])).toBe(
         "Total revenue in USD.",
      );
   });

   it("accepts Annotation objects ({ value }) as well as raw strings", () => {
      expect(docText([{ value: "#(doc) Customer state." }])).toBe(
         "Customer state.",
      );
   });

   it("joins multiple #(doc) lines and collapses whitespace", () => {
      expect(docText(["#(doc) line one", "#(doc)   line   two"])).toBe(
         "line one line two",
      );
   });

   it("falls back to the raw lines when no #(doc) annotation is present", () => {
      expect(docText(["# bar_chart"])).toBe("# bar_chart");
   });

   it("returns an empty string for missing or empty annotations", () => {
      expect(docText()).toBe("");
      expect(docText([])).toBe("");
   });
});

describe("get_context sanitize", () => {
   it("strips lunr operator characters so a plain-English query never throws", () => {
      const cleaned = sanitize('revenue: +top ~10 "exact" -minus ^boost *wild');
      expect(cleaned).not.toMatch(/[~^:*+\-"]/);
   });

   it("keeps an ordinary query intact", () => {
      expect(sanitize("revenue by product category")).toBe(
         "revenue by product category",
      );
   });
});

// Capture the tool handler that registerGetContextTool passes to McpServer.tool,
// so each discovery tier can be exercised against a mocked EnvironmentStore.
type Handler = (params: Record<string, unknown>) => Promise<{
   isError?: boolean;
   content: Array<{ resource: { text: string } }>;
}>;

function captureHandler(store: Partial<EnvironmentStore>): Handler {
   let handler: Handler | undefined;
   const fakeServer = {
      tool: (_name: string, _desc: string, _shape: unknown, h: Handler) => {
         handler = h;
      },
   };
   registerGetContextTool(fakeServer as never, store as EnvironmentStore);
   if (!handler) throw new Error("handler was not registered");
   return handler;
}

function parse(result: { content: Array<{ resource: { text: string } }> }) {
   return JSON.parse(result.content[0].resource.text);
}

// A model with one source (order_items) carrying one dimension (state).
const mockModel = {
   getSourceInfos: () => [
      {
         name: "order_items",
         annotations: [],
         schema: {
            fields: [{ kind: "dimension", name: "state", annotations: [] }],
         },
      },
   ],
   getQueries: () => [],
};
const mockPackage = {
   listModels: async () => [{ path: "ecommerce.malloy" }],
   getModel: () => mockModel,
};

// A package with more than 10 sources, to prove the enumeration tier is not
// silently capped the way ranked retrieval is.
const manySourceNames = Array.from({ length: 12 }, (_, i) => `s${i}`);
const mockManySourceModel = {
   getSourceInfos: () =>
      manySourceNames.map((name) => ({
         name,
         annotations: [],
         schema: { fields: [] },
      })),
   getQueries: () => [],
};
const mockManySourcePackage = {
   listModels: async () => [{ path: "many.malloy" }],
   getModel: () => mockManySourceModel,
};

describe("get_context discovery tiers", () => {
   it("tier 1: no environment lists environments with their package names", async () => {
      const handler = captureHandler({
         listEnvironments: async () =>
            [
               {
                  name: "malloy-samples",
                  packages: [{ name: "ecommerce" }, { name: "imdb" }],
               },
            ] as never,
      });
      const { results } = parse(await handler({}));
      expect(results).toEqual([
         {
            kind: "environment",
            name: "malloy-samples",
            packages: ["ecommerce", "imdb"],
         },
      ]);
   });

   it("tier 2: environment without a package lists the packages", async () => {
      const handler = captureHandler({
         getEnvironment: async () =>
            ({
               listPackages: async () => [
                  { name: "ecommerce", description: "Ecommerce demo" },
               ],
            }) as never,
      });
      const { results } = parse(
         await handler({ environmentName: "malloy-samples" }),
      );
      expect(results).toEqual([
         {
            kind: "package",
            name: "ecommerce",
            description: "Ecommerce demo",
            environmentName: "malloy-samples",
         },
      ]);
   });

   it("tier 2: surfaces an unresolved environment as a tool error", async () => {
      const handler = captureHandler({
         getEnvironment: async () => {
            throw new Error("Environment 'nope' could not be resolved");
         },
      });
      const result = await handler({ environmentName: "nope" });
      expect(result.isError).toBe(true);
      const parsed = parse(result);
      expect(parsed.results).toEqual([]);
      expect(parsed.error).toContain("could not be resolved");
   });

   it("tier 3: package without a query lists only its sources", async () => {
      const handler = captureHandler({
         getEnvironment: async () =>
            ({ getPackage: async () => mockPackage }) as never,
      });
      const { results } = parse(
         await handler({
            environmentName: "malloy-samples",
            packageName: "ecommerce",
         }),
      );
      expect(results).toEqual([
         {
            kind: "source",
            name: "order_items",
            source: "order_items",
            environmentName: "malloy-samples",
            packageName: "ecommerce",
            modelPath: "ecommerce.malloy",
            doc: "",
         },
      ]);
   });

   it("tier 4: a query retrieves the matching entity", async () => {
      const handler = captureHandler({
         getEnvironment: async () =>
            ({ getPackage: async () => mockPackage }) as never,
      });
      const { results } = parse(
         await handler({
            environmentName: "malloy-samples",
            packageName: "ecommerce",
            query: "state",
         }),
      );
      expect(
         results.some(
            (r: { name: string; kind: string }) =>
               r.name === "state" && r.kind === "dimension",
         ),
      ).toBe(true);
   });

   it("tier 1: surfaces a listEnvironments failure as a tool error", async () => {
      const handler = captureHandler({
         listEnvironments: async () => {
            throw new Error("environment store not initialized");
         },
      });
      const result = await handler({});
      expect(result.isError).toBe(true);
      const parsed = parse(result);
      expect(parsed.results).toEqual([]);
      expect(parsed.error).toContain("not initialized");
   });

   it("tier 3: lists every source, not just the first 10", async () => {
      const handler = captureHandler({
         getEnvironment: async () =>
            ({ getPackage: async () => mockManySourcePackage }) as never,
      });
      const { results } = parse(
         await handler({ environmentName: "e", packageName: "p" }),
      );
      expect(results).toHaveLength(12);
   });

   it("tier 3: honors an explicit limit when given", async () => {
      const handler = captureHandler({
         getEnvironment: async () =>
            ({ getPackage: async () => mockManySourcePackage }) as never,
      });
      const { results } = parse(
         await handler({ environmentName: "e", packageName: "p", limit: 5 }),
      );
      expect(results).toHaveLength(5);
   });

   it("tier 4: without a provider the payload has no retrieval marker or scores", async () => {
      const handler = captureHandler({
         getEnvironment: async () =>
            ({ getPackage: async () => mockPackage }) as never,
      });
      const payload = parse(
         await handler({
            environmentName: "malloy-samples",
            packageName: "ecommerce",
            query: "state",
         }),
      );
      expect(payload.retrieval).toBeUndefined();
      expect(Object.keys(payload)).toEqual(["results"]);
      for (const r of payload.results) {
         expect(r.score).toBeUndefined();
      }
   });
});

describe("get_context semantic retrieval", () => {
   let tempDir: string;
   let db: DuckDBConnection;

   beforeAll(async () => {
      tempDir = fs.mkdtempSync(
         path.join(os.tmpdir(), "get-context-semantic-spec-"),
      );
      db = new DuckDBConnection(path.join(tempDir, "test.db"));
      await db.initialize();
      await createEntityEmbeddingsTable(db);
   });

   afterAll(async () => {
      // Close before removing: Windows refuses to delete a directory
      // holding DuckDB's open file handle.
      await db.close();
      fs.rmSync(tempDir, { recursive: true, force: true });
   });

   afterEach(() => {
      _setEmbeddingProviderForTests(null);
   });

   // Entity texts are humanize(name) [+ doc]: "order items" and "state".
   const VECTORS: Record<string, number[]> = {
      "order items": [0, 1],
      state: [1, 0],
      "where do customers live": [1, 0],
   };

   function stubProvider(options: { fail?: boolean } = {}): EmbeddingProvider {
      const fetchStub = (async (
         _url: RequestInfo | URL,
         init?: RequestInit,
      ) => {
         if (options.fail) return new Response("down", { status: 500 });
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
         },
         fetchStub,
      );
   }

   /** A store whose package is a fresh instance, backed by the temp DB. */
   function semanticStore(): Partial<EnvironmentStore> {
      const pkg = {
         listModels: async () => [{ path: "ecommerce.malloy" }],
         getModel: () => mockModel,
      };
      return {
         getEnvironment: async () => ({ getPackage: async () => pkg }) as never,
         storageManager: {
            getDuckDbConnection: () => db,
         } as never,
      };
   }

   async function callUntilSemantic(
      handler: Handler,
      params: Record<string, unknown>,
   ) {
      for (let i = 0; i < 200; i++) {
         const payload = parse(await handler(params));
         if (payload.retrieval === "semantic") return payload;
         await new Promise((resolve) => setTimeout(resolve, 5));
      }
      throw new Error("retrieval never became semantic");
   }

   it("ranks semantically once the index syncs, with scores and a marker", async () => {
      _setEmbeddingProviderForTests(stubProvider());
      const handler = captureHandler(semanticStore());
      const params = {
         environmentName: "specs",
         packageName: "semantic-pkg",
         query: "where do customers live",
      };

      // Cold start: the sync kicks off in the background and this call
      // answers lexically, marked as such.
      const first = parse(await handler(params));
      expect(first.retrieval).toBe("lexical");

      const payload = await callUntilSemantic(handler, params);
      expect(payload.results[0].name).toBe("state");
      expect(payload.results[0].kind).toBe("dimension");
      expect(payload.results[0].score).toBeCloseTo(1.0, 3);
      // "order items" is orthogonal to the query: below the similarity
      // floor, so it must not pad the results.
      expect(
         payload.results.some(
            (r: { name: string }) => r.name === "order_items",
         ),
      ).toBe(false);
   });

   it("falls back to lexical, marked, when the provider goes down after indexing", async () => {
      // Index healthily first, so this pins the query-embed failure
      // path, not just the cold start (which answers lexically anyway).
      _setEmbeddingProviderForTests(stubProvider());
      const handler = captureHandler(semanticStore());
      const params = {
         environmentName: "specs",
         packageName: "failing-pkg",
         query: "where do customers live",
      };
      await callUntilSemantic(handler, params);

      // Same model and config, but the endpoint now returns 500s: the
      // per-call query embed fails and the call degrades to marked
      // lexical with no scores.
      _setEmbeddingProviderForTests(stubProvider({ fail: true }));
      const payload = parse(await handler({ ...params, query: "state" }));
      expect(payload.retrieval).toBe("lexical");
      expect(
         payload.results.some((r: { name: string }) => r.name === "state"),
      ).toBe(true);
      for (const r of payload.results) {
         expect(r.score).toBeUndefined();
      }
   });

   it("degrades to lexical when the storage handle is unavailable", async () => {
      _setEmbeddingProviderForTests(stubProvider());
      const store = semanticStore();
      delete (store as { storageManager?: unknown }).storageManager;
      const handler = captureHandler(store);
      const payload = parse(
         await handler({
            environmentName: "specs",
            packageName: "no-storage-pkg",
            query: "state",
         }),
      );
      expect(payload.retrieval).toBe("lexical");
      expect(
         payload.results.some((r: { name: string }) => r.name === "state"),
      ).toBe(true);
   });
});
