// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

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
import {
   docOnlyText,
   docText,
   sanitize,
   registerGetContextTool,
} from "./get_context_tool";
import { embeddingText } from "./embedding_index";
import type { EnvironmentStore } from "../../service/environment_store";
import { PackageNotFoundError } from "../../errors";
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

describe("get_context docOnlyText (embedding-input safety)", () => {
   it("extracts #(doc) text like docText", () => {
      expect(docOnlyText(["#(doc) Total revenue."])).toBe("Total revenue.");
      expect(docOnlyText([{ value: "#(doc) Customer state." }])).toBe(
         "Customer state.",
      );
   });

   it("does NOT fall back to raw lines: no #(doc) yields empty", () => {
      // The security-critical difference from docText: predicate-bearing
      // annotations must never become embedding input.
      expect(docOnlyText(["# bar_chart"])).toBe("");
      expect(
         docOnlyText([
            "#(authorize) \"$ROLE = 'admin'\"",
            "#(malloy) drillable",
         ]),
      ).toBe("");
   });

   it("keeps only the #(doc) line when mixed with predicate annotations", () => {
      expect(
         docOnlyText([
            "#(authorize) \"$TENANT = 'acme'\"",
            "#(doc) Secured orders.",
         ]),
      ).toBe("Secured orders.");
   });

   it("an #(authorize)-only entity produces no embedding text beyond its name", () => {
      // End-to-end: what embeddingText actually sends for a governed,
      // undocumented entity is the humanized name only, never the predicate.
      const embedDoc = docOnlyText([
         "#(authorize) \"$ROLE = 'admin'\"",
         "#(authorize) \"$TENANT = 'acme' or $TENANT = 'globex'\"",
      ]);
      const text = embeddingText({
         kind: "source",
         name: "orders_secured",
         source: "orders_secured",
         modelPath: "m.malloy",
         embedDoc,
      });
      expect(text).toBe("orders secured");
      expect(text).not.toContain("authorize");
      expect(text).not.toContain("ROLE");
      expect(text).not.toContain("acme");
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
type Content = Array<{
   type?: string;
   text?: string;
   resource?: { text: string };
}>;

type Handler = (params: Record<string, unknown>) => Promise<{
   isError?: boolean;
   content: Content;
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

function parse(result: { content: Content }) {
   return JSON.parse(result.content[0].resource!.text);
}

function textBlock(result: { content: Content }) {
   return result.content.find((b) => b.type === "text")?.text;
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

/**
 * An environment double for the tiers that resolve a package.
 * getStaleCompileErrors is part of the real Environment interface and
 * getContext reads it to decide whether to attach the staleness note, so a
 * double that omits it exercises the lookup's failure path instead of the
 * behavior under test. Defaults to "nothing is stale".
 */
const envWith = (
   getPackage: () => Promise<unknown>,
   staleCompileErrors: Map<
      string,
      { message: string; failedAt: string }
   > = new Map(),
) =>
   ({
      getPackage,
      getStaleCompileErrors: () => staleCompileErrors,
   }) as never;

const mockTwoSourceModel = {
   getSourceInfos: () => [
      {
         name: "orders",
         annotations: [{ value: "#(doc) One row per order." }],
         schema: {
            fields: [
               { kind: "view", name: "by_month", annotations: [] },
               { kind: "dimension", name: "status", annotations: [] },
               { kind: "measure", name: "total_revenue", annotations: [] },
               // Structural: collectEntities skips joins, so a drill-down must
               // not start surfacing them just because the kind filter widened.
               { kind: "join", name: "customer", annotations: [] },
            ],
         },
      },
      {
         name: "customers",
         annotations: [],
         schema: {
            fields: [{ kind: "dimension", name: "state", annotations: [] }],
         },
      },
   ],
   getQueries: () => [
      { name: "top_orders", sourceName: "orders", annotations: [] },
   ],
};

const mockTwoSourcePackage = {
   listModels: async () => [{ path: "sales.malloy" }],
   getModel: () => mockTwoSourceModel,
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
               getFailedPackages: () => new Map(),
               getStaleCompileErrors: () => new Map(),
            }) as never,
      });
      const { results } = parse(
         await handler({ environmentName: "malloy-samples" }),
      );
      // No health markers on a healthy package: the entry is byte-identical
      // to what it was before staleness was reported at all.
      expect(results).toEqual([
         {
            kind: "package",
            name: "ecommerce",
            description: "Ecommerce demo",
            environmentName: "malloy-samples",
         },
      ]);
   });

   it("tier 2: lists a failed package with its load error instead of omitting it", async () => {
      // listPackages() drops packages that failed to load, which reads as
      // "does not exist" to an agent. The listing must carry them with an
      // error marker so a broken package is distinguishable from an absent one.
      const handler = captureHandler({
         getEnvironment: async () =>
            ({
               listPackages: async () => [{ name: "good" }],
               getFailedPackages: () =>
                  new Map([["broken", "Compile failed: unexpected token"]]),
               getStaleCompileErrors: () => new Map(),
            }) as never,
      });
      const { results } = parse(
         await handler({ environmentName: "malloy-samples" }),
      );
      expect(results).toHaveLength(2);
      expect(results[0]).toMatchObject({ kind: "package", name: "good" });
      expect(results[1]).toEqual({
         kind: "package",
         name: "broken",
         environmentName: "malloy-samples",
         error: "Compile failed: unexpected token",
      });
      // No stale marker: this package is not serving anything at all, which is
      // the distinction the marker exists to draw.
      expect("stale" in results[1]).toBe(false);
   });

   it("tier 2: marks a stale package, which listPackages reports as healthy", async () => {
      // A failed reload keeps the package SERVING, so it comes back from
      // listPackages looking exactly like a current one. Unmarked, an agent
      // queries it and gets numbers from the model compiled before the last
      // save, with nothing in the payload to say so.
      const handler = captureHandler({
         getEnvironment: async () =>
            ({
               listPackages: async () => [
                  { name: "current" },
                  { name: "stale-pkg" },
               ],
               getFailedPackages: () => new Map(),
               getStaleCompileErrors: () =>
                  new Map([
                     [
                        "stale-pkg",
                        {
                           message: "line 3: missing ')'",
                           failedAt: "2026-08-13T00:00:00.000Z",
                        },
                     ],
                  ]),
            }) as never,
      });
      const { results } = parse(
         await handler({ environmentName: "malloy-samples" }),
      );
      expect(results).toEqual([
         {
            kind: "package",
            name: "current",
            environmentName: "malloy-samples",
         },
         {
            kind: "package",
            name: "stale-pkg",
            environmentName: "malloy-samples",
            error: "line 3: missing ')'",
            stale: true,
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
      // Suggestions and a text block, same as this tool's three siblings. Both
      // were absent before: the payload carried a bare message, and a
      // text-only client saw nothing at all.
      expect(parsed.suggestions.length).toBeGreaterThan(0);
      expect(textBlock(result)).toContain("could not be resolved");
   });

   it("reports an unknown package as not-found, not as an internal fault", async () => {
      // Pins the classification, not just that an error came back. Before this
      // routed through classifyToolError every failure arrived as the raw
      // message with no remediation, so a typo'd package name gave the agent
      // nothing to act on.
      const handler = captureHandler({
         getEnvironment: async () =>
            ({
               getPackage: async () => {
                  throw new PackageNotFoundError("Package 'nope' not found");
               },
            }) as never,
      });
      const result = await handler({
         environmentName: "malloy-samples",
         packageName: "nope",
         query: "state",
      });
      expect(result.isError).toBe(true);
      const parsed = parse(result);
      expect(parsed.results).toEqual([]);
      expect(parsed.error).toContain("Resource not found");
      expect(parsed.error).toContain("nope");
      expect(textBlock(result)).toContain("Resource not found");
   });

   it("reports a non-Error throwable without inventing 'Unknown error'", async () => {
      // The old per-site `error instanceof Error ? error.message : "Unknown
      // error"` turned a thrown string into exactly the unhelpful text this
      // tool's callers reported. classifyToolError stringifies it instead.
      const handler = captureHandler({
         listEnvironments: async () => {
            throw "the store exploded";
         },
      });
      const result = await handler({});
      const parsed = parse(result);
      expect(parsed.error).toContain("the store exploded");
      expect(parsed.error).not.toContain("Unknown error");
      expect(textBlock(result)).toContain("the store exploded");
   });

   it("tier 3: package without a query lists only its sources", async () => {
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => mockPackage),
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

   it("tier 3: a populated listing of a current package carries no note", async () => {
      // Notes are for the ambiguous cases only; a healthy payload must stay
      // byte-identical to what it was before notes existed.
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => mockPackage),
      });
      const payload = parse(
         await handler({
            environmentName: "malloy-samples",
            packageName: "ecommerce",
         }),
      );
      expect("note" in payload).toBe(false);
   });

   it("tier 3: a stale package says its names predate the last save", async () => {
      // The index is the last model that compiled, so every name here is real
      // and every query against it succeeds. That is exactly the trap: without
      // the note the payload is indistinguishable from a current package.
      const handler = captureHandler({
         getEnvironment: async () =>
            envWith(
               async () => mockPackage,
               new Map([
                  [
                     "ecommerce",
                     {
                        message: "line 3: missing ')'",
                        failedAt: "2026-08-13T00:00:00.000Z",
                     },
                  ],
               ]),
            ),
      });
      const payload = parse(
         await handler({
            environmentName: "malloy-samples",
            packageName: "ecommerce",
         }),
      );
      expect(payload.results).toHaveLength(1);
      expect(payload.note).toContain("STALE");
      expect(payload.note).toContain("2026-08-13T00:00:00.000Z");
      expect(payload.note).toContain("malloy_reloadPackage");
   });

   it("tier 4: retrieval against a stale package carries the note too", async () => {
      // Tier 4 is the path that goes straight from a question to field names
      // to a query, so it is the one an agent is most likely to trust blind.
      const handler = captureHandler({
         getEnvironment: async () =>
            envWith(
               async () => mockPackage,
               new Map([
                  [
                     "ecommerce",
                     {
                        message: "line 3: missing ')'",
                        failedAt: "2026-08-13T00:00:00.000Z",
                     },
                  ],
               ]),
            ),
      });
      const payload = parse(
         await handler({
            environmentName: "malloy-samples",
            packageName: "ecommerce",
            query: "order items",
         }),
      );
      expect(payload.note).toContain("STALE");
   });

   it("tier 3: an empty listing says it is a curation gap, not an empty database", async () => {
      // A package that loaded but exposes nothing and a package with no data
      // produce the same results: []. The note is what tells an agent the
      // difference (a failed load never gets here: getPackage throws).
      const emptyPackage = {
         listModels: async () => [{ path: "importer.malloy" }],
         getModel: () => ({ getSourceInfos: () => [], getQueries: () => [] }),
      };
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => emptyPackage),
      });
      const payload = parse(
         await handler({
            environmentName: "malloy-samples",
            packageName: "curated-empty",
         }),
      );
      expect(payload.results).toEqual([]);
      expect(payload.note).toContain("curation gap");
      expect(payload.note).toContain("malloy_getStatus");
   });

   it("tier 4: a query retrieves the matching entity", async () => {
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => mockPackage),
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
      expect(parsed.suggestions.length).toBeGreaterThan(0);
      expect(textBlock(result)).toContain("not initialized");
   });

   it("tier 3: lists every source, not just the first 10", async () => {
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => mockManySourcePackage),
      });
      const { results } = parse(
         await handler({ environmentName: "e", packageName: "p" }),
      );
      expect(results).toHaveLength(12);
   });

   it("tier 3: honors an explicit limit when given", async () => {
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => mockManySourcePackage),
      });
      const { results } = parse(
         await handler({ environmentName: "e", packageName: "p", limit: 5 }),
      );
      expect(results).toHaveLength(5);
   });

   it("tier 4: without a provider the payload has no retrieval marker or scores", async () => {
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => mockPackage),
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

   it("tier 3 drill-down: sourceName lists that source's fields, views and queries", async () => {
      // The regression pin. This returned exactly one row — the source itself —
      // so an agent following the documented drill-down never saw a single
      // dimension or measure and had no way to learn a source's fields.
      const handler = captureHandler({
         getEnvironment: async () =>
            ({ getPackage: async () => mockTwoSourcePackage }) as never,
      });
      const { results } = parse(
         await handler({
            environmentName: "malloy-samples",
            packageName: "sales",
            sourceName: "orders",
         }),
      );
      // Order matters: the source leads, so its doc is the first thing read.
      expect(
         results.map((r: { kind: string; name: string }) => [r.kind, r.name]),
      ).toEqual([
         ["source", "orders"],
         ["view", "by_month"],
         ["dimension", "status"],
         ["measure", "total_revenue"],
         ["query", "top_orders"],
      ]);
      expect(results[0].doc).toBe("One row per order.");
   });

   it("tier 3 drill-down: excludes entities belonging to another source", async () => {
      const handler = captureHandler({
         getEnvironment: async () =>
            ({ getPackage: async () => mockTwoSourcePackage }) as never,
      });
      const { results } = parse(
         await handler({
            environmentName: "malloy-samples",
            packageName: "sales",
            sourceName: "customers",
         }),
      );
      expect(
         results.map((r: { kind: string; name: string }) => [r.kind, r.name]),
      ).toEqual([
         ["source", "customers"],
         ["dimension", "state"],
      ]);
   });

   it("tier 3 without sourceName still lists sources only, not their fields", async () => {
      // The widened filter must stay scoped to the drill-down: the plain
      // package listing is an overview and would be unreadable with every
      // field of every source in it.
      const handler = captureHandler({
         getEnvironment: async () =>
            ({ getPackage: async () => mockTwoSourcePackage }) as never,
      });
      const { results } = parse(
         await handler({
            environmentName: "malloy-samples",
            packageName: "sales",
         }),
      );
      expect(results.map((r: { name: string }) => r.name)).toEqual([
         "orders",
         "customers",
      ]);
   });

   it("tier 3 drill-down: an unknown sourceName returns no results, not everything", async () => {
      const handler = captureHandler({
         getEnvironment: async () =>
            ({ getPackage: async () => mockTwoSourcePackage }) as never,
      });
      const { results } = parse(
         await handler({
            environmentName: "malloy-samples",
            packageName: "sales",
            sourceName: "nope",
         }),
      );
      expect(results).toEqual([]);
   });

   it("tier 3 drill-down: limit caps the fields returned", async () => {
      const handler = captureHandler({
         getEnvironment: async () =>
            ({ getPackage: async () => mockTwoSourcePackage }) as never,
      });
      const { results } = parse(
         await handler({
            environmentName: "malloy-samples",
            packageName: "sales",
            sourceName: "orders",
            limit: 2,
         }),
      );
      expect(
         results.map((r: { kind: string; name: string }) => [r.kind, r.name]),
      ).toEqual([
         ["source", "orders"],
         ["view", "by_month"],
      ]);
   });

   it("omits the retrieval field entirely when no embedding provider is configured", async () => {
      // Pins what the tool description now promises: absent, not defaulted. A
      // caller that branches on `retrieval` must not see a value invented for it.
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
      expect("retrieval" in payload).toBe(false);
      expect(payload.results.every((r: object) => !("score" in r))).toBe(true);
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
         getEnvironment: async () => envWith(async () => pkg),
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

   it("degrades to lexical (never throws) when the real config is malformed", async () => {
      // Exercises the tool-path catch with REAL env parsing, not the
      // _setEmbeddingProviderForTests override: a malformed base makes
      // getEmbeddingProvider() throw, and tier 4 must swallow it and
      // answer marked lexical (embeddingConfigured() is still true).
      const saved = {
         key: process.env.EMBEDDING_API_KEY,
         base: process.env.EMBEDDING_API_BASE,
      };
      process.env.EMBEDDING_API_KEY = "k";
      process.env.EMBEDDING_API_BASE = "not a url";
      _clearEmbeddingProviderForTests();
      try {
         const handler = captureHandler(semanticStore());
         const payload = parse(
            await handler({
               environmentName: "specs",
               packageName: "malformed-cfg-pkg",
               query: "state",
            }),
         );
         expect(payload.retrieval).toBe("lexical");
         expect(
            payload.results.some((r: { name: string }) => r.name === "state"),
         ).toBe(true);
      } finally {
         if (saved.key === undefined) delete process.env.EMBEDDING_API_KEY;
         else process.env.EMBEDDING_API_KEY = saved.key;
         if (saved.base === undefined) delete process.env.EMBEDDING_API_BASE;
         else process.env.EMBEDDING_API_BASE = saved.base;
         _clearEmbeddingProviderForTests();
         _setEmbeddingProviderForTests(null);
      }
   });
});
