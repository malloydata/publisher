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
import { DEFAULT_EMBEDDING_MIN_SIMILARITY } from "../../config";
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

/**
 * The response's entities as one ranked list, un-nested from `sources[]`.
 *
 * The payload is source-centric (the hosted API's GetContextResponse
 * shape), but
 * most of what this file tests -- ranking order, sibling grouping, alias
 * collapse, the relevance floor -- is about the ranked ENTITY list that feeds
 * serialization. Walking the nesting inside each of those tests would bury the
 * behaviour under structure, so they read the list and the shape gets its own
 * tests below.
 *
 * Field names are the wire's, not the old flat payload's, so a test still
 * asserts on what actually ships. `source` is lifted from the containing
 * `resource_id` because an un-nested entity has lost it otherwise. Source
 * order then entity order reproduces the original ranking.
 */
interface SourceCardShape {
   source_info: {
      resource_id: { source: string };
      [key: string]: unknown;
   };
   entities?: Record<string, unknown>[];
}

function rankedEntities(payload: {
   sources?: {
      source_info: { resource_id: { source: string } };
      entities?: Record<string, unknown>[];
   }[];
}) {
   return (payload.sources ?? []).flatMap((s) =>
      (s.entities ?? []).map((e) => ({
         ...e,
         source: s.source_info.resource_id.source,
      })),
   ) as RankedEntity[];
}

/** One entity as the response ships it, tagged with the source it came from. */
interface RankedEntity {
   name: string;
   entity_type: string;
   source: string;
   entity_id?: string;
   description?: string;
   data_type?: string;
   relevance?: number;
   relationship?: string;
   aliases?: string[];
   also_in?: string[];
}

/** The sources a response returned, by name, in response order. */
function sourceNames(payload: {
   sources?: { source_info: { resource_id: { source: string } } }[];
}) {
   return (payload.sources ?? []).map((s) => s.source_info.resource_id.source);
}

function textBlock(result: { content: Content }) {
   return result.content.find((b) => b.type === "text")?.text;
}

// A model with one source (order_items) carrying one dimension (state) and one
// declared join (current_building). The join's own schema carries a field with
// a distinctive token so a no-recursion pin can assert it is never indexed.
const mockModel = {
   getSourceInfos: () => [
      {
         name: "order_items",
         annotations: ["#(doc) One row per product sold on an order."],
         schema: {
            fields: [
               { kind: "dimension", name: "state", annotations: [] },
               {
                  kind: "join",
                  name: "current_building",
                  relationship: "one",
                  annotations: ["#(doc) Building this asset sits in."],
                  schema: {
                     fields: [
                        {
                           kind: "dimension",
                           name: "building_name",
                           annotations: ["#(doc) Zzyzx joined-source field."],
                        },
                     ],
                  },
               },
            ],
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
// A source carrying a raw physical column beside its own documented alias,
// which is what a model that renames without hiding produces.
const mockAliasModel = {
   getSourceInfos: () => [
      {
         name: "fclt_building",
         annotations: [],
         schema: {
            fields: [
               { kind: "dimension", name: "SITE", annotations: [] },
               {
                  kind: "dimension",
                  name: "site",
                  annotations: ["#(doc) Campus site the building sits on."],
               },
               { kind: "dimension", name: "height", annotations: [] },
            ],
         },
      },
   ],
   getQueries: () => [],
};
const mockAliasPackage = {
   listModels: async () => [{ path: "b.malloy" }],
   getModel: () => mockAliasModel,
};

// Three parallel sources carrying the same concept, the shape that produced
// the largest measured failure class.
const SIBLING_SOURCES = ["fac_building", "fclt_building", "fclt_building_hist"];
const mockSiblingModel = {
   getSourceInfos: () =>
      SIBLING_SOURCES.map((name) => ({
         name,
         annotations: [],
         schema: {
            fields: [{ kind: "dimension", name: "site", annotations: [] }],
         },
      })),
   getQueries: () => [],
};
const mockSiblingPackage = {
   listModels: async () => [{ path: "sib.malloy" }],
   getModel: () => mockSiblingModel,
};

// A source whose doc is longer than SOURCE_DOC_MAX_CHARS, and one that
// declares no joins at all, to pin the truncation and the empty-joins signal.
const LONG_SOURCE_DOC = `Rooms in the facilities inventory. ${"Grain caveats and population rules go here. ".repeat(20)}`;
const mockLongDocModel = {
   getSourceInfos: () => [
      {
         name: "rooms",
         annotations: [`#(doc) ${LONG_SOURCE_DOC}`],
         schema: {
            fields: [
               { kind: "dimension", name: "room_key", annotations: [] },
               // Shares no token with the source's name or doc, so a query for
               // it reaches the source only as the field's parent -- which is
               // the case where the doc is carried rather than returned.
               { kind: "dimension", name: "occupancy_pct", annotations: [] },
            ],
         },
      },
   ],
   getQueries: () => [],
};
const mockLongDocPackage = {
   listModels: async () => [{ path: "rooms.malloy" }],
   getModel: () => mockLongDocModel,
};

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
               // A declared join is an entity of this source, so a drill-down
               // shows what the agent can traverse.
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
      const payload = parse(
         await handler({
            environmentName: "malloy-samples",
            packageName: "ecommerce",
         }),
      );
      // The whole listing payload, pinned: this is the response contract, in
      // the platform's shape. A listing has no relevance to report and no
      // entities under each source -- it is the catalog, not a search.
      expect(payload).toEqual({
         sources: [
            {
               source_info: {
                  resource_id: {
                     environment: "malloy-samples",
                     package: "ecommerce",
                     model_path: "ecommerce.malloy",
                     source: "order_items",
                  },
                  one_line_summary: "One row per product sold on an order.",
                  docs: "One row per product sold on an order.",
                  joins: [
                     {
                        name: "current_building",
                        relationship: "one",
                        doc: "Building this asset sits in.",
                     },
                  ],
               },
            },
         ],
         ranking: "prominence",
         total_available: 1,
         returned: 1,
      });
   });

   it("tier 3: a populated listing of a current package carries no warnings", async () => {
      // Warnings are for the ambiguous cases only; a healthy payload must stay
      // byte-identical to what it was before warnings existed.
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => mockPackage),
      });
      const payload = parse(
         await handler({
            environmentName: "malloy-samples",
            packageName: "ecommerce",
         }),
      );
      expect("warnings" in payload).toBe(false);
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
      expect(payload.sources).toHaveLength(1);
      expect(payload.warnings.join(" ")).toContain("STALE");
      expect(payload.warnings.join(" ")).toContain("2026-08-13T00:00:00.000Z");
      expect(payload.warnings.join(" ")).toContain("malloy_reloadPackage");
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
      expect(payload.warnings.join(" ")).toContain("STALE");
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
      expect(payload.sources).toEqual([]);
      expect(payload.warnings.join(" ")).toContain("curation gap");
      expect(payload.warnings.join(" ")).toContain("malloy_getStatus");
   });

   it("tier 4: a query retrieves the matching entity", async () => {
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => mockPackage),
      });
      const results = rankedEntities(
         parse(
            await handler({
               environmentName: "malloy-samples",
               packageName: "ecommerce",
               query: "state",
            }),
         ),
      );
      expect(
         results.some(
            (r) => r.name === "state" && r.entity_type === "dimension",
         ),
      ).toBe(true);
   });

   it("tier 4: retrieves a declared join, carrying its cardinality", async () => {
      // Joins were skipped by the collector, so an agent could not see that
      // the model declared one and concluded it had to bridge the tables
      // itself. The join's #(doc) was likewise unreachable.
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => mockPackage),
      });
      const results = rankedEntities(
         parse(
            await handler({
               environmentName: "malloy-samples",
               packageName: "ecommerce",
               query: "building",
            }),
         ),
      );
      const join = results.find((r) => r.name === "current_building");
      expect(join).toBeDefined();
      // `join` is not one of the hosted API's three entity types. Publisher keeps it
      // because an agent that cannot see a declared join concludes the model
      // has none and burns calls bridging the tables itself.
      expect(join!.entity_type).toBe("join");
      expect(join!.relationship).toBe("one");
      // A join nests under the source that DECLARES it, so a drill-down on
      // that source sees it.
      expect(join!.source).toBe("order_items");
      expect(join!.description).toBe("Building this asset sits in.");
      expect(join!.entity_id).toBe("join:order_items:current_building");
   });

   it("identifies every entity as kind:source:name, container included", async () => {
      // The response already carries kind, name and source, so this adds no
      // information -- it adds agreement. Consumers comparing entities across
      // two responses were each assembling the key themselves, and a
      // one-character disagreement reads as a total miss on a call that
      // returned the right thing.
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => mockPackage),
      });
      const listed = parse(
         await handler({
            environmentName: "malloy-samples",
            packageName: "ecommerce",
         }),
      );
      // A source's identity is its resource_id, which is how the shape
      // names a source; it needs no entity row and gets none.
      expect(listed.sources[0].source_info.resource_id).toEqual({
         environment: "malloy-samples",
         package: "ecommerce",
         model_path: "ecommerce.malloy",
         source: "order_items",
      });

      const results = rankedEntities(
         parse(
            await handler({
               environmentName: "malloy-samples",
               packageName: "ecommerce",
               query: "state",
            }),
         ),
      );
      const field = results.find((r) => r.name === "state");
      expect(field!.entity_id).toBe("dimension:order_items:state");
      // The invariant a caller splitting on ":" depends on, across every kind.
      // Shape before brevity: a form that dropped the middle segment for an
      // entity with no source would hand callers two id shapes to parse.
      for (const r of results) {
         expect(r.entity_id?.split(":")).toHaveLength(3);
         expect(r.entity_id).toBe(`${r.entity_type}:${r.source}:${r.name}`);
      }
   });

   it("tier 4: does not recurse into a join's own schema", async () => {
      // The joined source's fields are already indexed under that source.
      // Recursing would re-index every one of them once per join that
      // reaches it, which is the redundancy this tool can least afford.
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => mockPackage),
      });
      const payload = parse(
         await handler({
            environmentName: "malloy-samples",
            packageName: "ecommerce",
            query: "Zzyzx",
         }),
      );
      expect(payload.sources).toEqual([]);
   });

   it("tier 3: a package listing still returns only sources, not joins", async () => {
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => mockPackage),
      });
      const payload = parse(
         await handler({
            environmentName: "malloy-samples",
            packageName: "ecommerce",
         }),
      );
      // A listing is sources and nothing else: no entity rows at all, so a
      // join can never appear as one.
      expect(sourceNames(payload)).toEqual(["order_items"]);
      expect(rankedEntities(payload)).toEqual([]);
   });

   it("delivers the parent source's doc and joins alongside a field hit", async () => {
      // A field hit used to arrive with only its own doc: the source's grain
      // and population rules reached the agent only when the source itself
      // happened to rank for the same query, so guidance placement depended
      // on query phrasing rather than on where the modeller wrote it.
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
      // The hit itself is a field carrying only its own (empty) doc...
      const hit = rankedEntities(payload)[0];
      expect(hit.entity_type).toBe("dimension");
      expect(hit.description).toBeUndefined();
      // ...and its source's guidance arrives regardless, on the container the
      // field now nests inside, which is what makes placement independent of
      // how the question was phrased.
      expect(payload.sources[0].source_info).toEqual({
         resource_id: {
            environment: "malloy-samples",
            package: "ecommerce",
            model_path: "ecommerce.malloy",
            source: "order_items",
         },
         one_line_summary: "One row per product sold on an order.",
         docs: "One row per product sold on an order.",
         joins: [
            {
               name: "current_building",
               relationship: "one",
               doc: "Building this asset sits in.",
            },
         ],
      });
   });

   it("reports each source once, however many of its entities matched", async () => {
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => mockPackage),
      });
      const payload = parse(
         await handler({
            environmentName: "malloy-samples",
            packageName: "ecommerce",
            // Matches the source, the join, and (via the source doc) more.
            query: "order building state",
         }),
      );
      expect(rankedEntities(payload).length).toBeGreaterThan(1);
      expect(sourceNames(payload)).toEqual(["order_items"]);
   });

   it("truncates a long source doc in context but not in the result itself", async () => {
      // The context copy exists to deliver the caveat, not to reproduce the
      // model file; a source that is itself the hit still returns in full.
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => mockLongDocPackage),
      });
      const matched = parse(
         await handler({
            environmentName: "e",
            packageName: "p",
            query: "rooms",
         }),
      );
      // The source itself ranked, so its docs come back whole.
      expect(matched.sources[0].source_info.docs).toBe(LONG_SOURCE_DOC.trim());
      expect(matched.sources[0].source_info.docs.length).toBeGreaterThan(500);

      // Reached only as the parent of a field hit, the same doc is the
      // truncated context copy: enough to deliver the caveat, not the file.
      const viaField = parse(
         await handler({
            environmentName: "e",
            packageName: "p",
            query: "occupancy_pct",
         }),
      );
      const docs = viaField.sources[0].source_info.docs;
      expect(docs.length).toBeLessThanOrEqual(501);
      expect(docs.endsWith("…")).toBe(true);
      expect(docs).toStartWith("Rooms in the facilities inventory.");
   });

   it("reports an empty joins list for a source that declares none", async () => {
      // The authoritative negative: without it an agent cannot tell "no joins
      // declared" from "joins exist but were not returned", and we watched
      // agents spend queries probing for a relationship that was never there.
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => mockLongDocPackage),
      });
      const payload = parse(
         await handler({ environmentName: "e", packageName: "p" }),
      );
      expect(payload.sources[0].source_info.joins).toEqual([]);
   });

   it("returns sources as an empty array, never absent, when nothing matched", async () => {
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => mockPackage),
      });
      const payload = parse(
         await handler({
            environmentName: "malloy-samples",
            packageName: "ecommerce",
            query: "Zzyzx",
         }),
      );
      // `sources` is the result set now, not a context block beside one, so
      // it is always present -- required in the published schema. Absent and
      // empty would otherwise be two spellings of a true negative, and a
      // caller has to distinguish "nothing matched" from "old server".
      expect(payload.sources).toEqual([]);
      expect(payload.returned).toBe(0);
      expect(payload.total_available).toBe(0);
   });

   it("collapses a raw column into its documented alias, naming the raw one", async () => {
      // `SITE` and `site` are one physical column indexed twice, and both
      // competed for the same scarce slots: 34% of returned slots were a
      // concept the result set already held. The documented spelling wins,
      // because a modeller who wrote the #(doc) said which name they meant.
      const handler = captureHandler({
         getEnvironment: async () =>
            ({ getPackage: async () => mockAliasPackage }) as never,
      });
      const results = rankedEntities(
         parse(
            await handler({
               environmentName: "e",
               packageName: "p",
               query: "site",
            }),
         ),
      );
      const sites = results.filter((r) => r.name.toLowerCase() === "site");
      expect(sites).toHaveLength(1);
      expect(sites[0].name).toBe("site");
      expect(sites[0].description).toBe("Campus site the building sits on.");
      expect(sites[0].aliases).toEqual(["SITE"]);
   });

   it("leaves genuinely distinct fields in a source alone", async () => {
      const handler = captureHandler({
         getEnvironment: async () =>
            ({ getPackage: async () => mockAliasPackage }) as never,
      });
      const results = rankedEntities(
         parse(
            await handler({
               environmentName: "e",
               packageName: "p",
               query: "height",
            }),
         ),
      );
      const height = results.find((r) => r.name === "height");
      expect(height).toBeDefined();
      expect(height!.aliases).toBeUndefined();
   });

   it("never sees a field the model hid with an access modifier", async () => {
      // The modelling-side fix for the same redundancy, and the reason this
      // tool needs no opt-out: Malloy drops non-public fields before the
      // compiled SourceInfo exists, so `include { internal: SITE }` removes
      // the raw twin from retrieval outright. Pinned because the collapse
      // heuristic above would otherwise look like the only defence.
      const hiddenModel = {
         getSourceInfos: () => [
            {
               name: "fclt_building",
               annotations: [],
               schema: {
                  fields: [
                     {
                        kind: "dimension",
                        name: "site",
                        annotations: ["#(doc) Campus site."],
                     },
                  ],
               },
            },
         ],
         getQueries: () => [],
      };
      const handler = captureHandler({
         getEnvironment: async () =>
            ({
               getPackage: async () => ({
                  listModels: async () => [{ path: "h.malloy" }],
                  getModel: () => hiddenModel,
               }),
            }) as never,
      });
      const results = rankedEntities(
         parse(
            await handler({
               environmentName: "e",
               packageName: "p",
               query: "site",
            }),
         ),
      );
      expect(results.some((r) => r.name === "SITE")).toBe(false);
      expect(results[0].aliases).toBeUndefined();
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
      const payload = parse(
         await handler({ environmentName: "e", packageName: "p" }),
      );
      expect(payload.sources).toHaveLength(12);
      expect(payload.total_available).toBe(12);
   });

   it("tier 3: honors an explicit limit when given", async () => {
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => mockManySourcePackage),
      });
      const payload = parse(
         await handler({ environmentName: "e", packageName: "p", limit: 5 }),
      );
      expect(payload.sources).toHaveLength(5);
      // returned against total_available is how a caller sees it was capped.
      expect(payload.returned).toBe(5);
      expect(payload.total_available).toBe(12);
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
      // The shape is the shape with or without a provider: source cards and
      // their joins are metadata every caller benefits from. Only the marker
      // and the relevance scores are provider-only.
      expect(Object.keys(payload)).toEqual([
         "sources",
         "ranking",
         "total_available",
         "returned",
      ]);
      for (const r of rankedEntities(payload)) {
         expect(r.relevance).toBeUndefined();
      }
      expect(payload.sources[0].relevance).toBeUndefined();
   });

   it("tier 3 drill-down: sourceName lists that source's fields, views and queries", async () => {
      // The regression pin. This returned exactly one row — the source itself —
      // so an agent following the documented drill-down never saw a single
      // dimension or measure and had no way to learn a source's fields.
      const handler = captureHandler({
         getEnvironment: async () =>
            ({ getPackage: async () => mockTwoSourcePackage }) as never,
      });
      const payload = parse(
         await handler({
            environmentName: "malloy-samples",
            packageName: "sales",
            sourceName: "orders",
         }),
      );
      // One card: the source is the container, its fields nest inside it.
      expect(payload.sources).toHaveLength(1);
      expect(payload.sources[0].source_info.resource_id.source).toBe("orders");
      expect(payload.sources[0].source_info.docs).toBe("One row per order.");
      expect(
         payload.sources[0].entities.map(
            (e: { entity_type: string; name: string }) => [
               e.entity_type,
               e.name,
            ],
         ),
      ).toEqual([
         ["view", "by_month"],
         ["dimension", "status"],
         ["measure", "total_revenue"],
         // The declared join is an entity of this source too: a drill-down is
         // where an agent learns what it can traverse.
         ["join", "customer"],
         ["query", "top_orders"],
      ]);
      // The card is one source of one, however many entities nest in it.
      expect(payload.total_available).toBe(1);
      expect(payload.returned).toBe(1);
   });

   it("tier 3 drill-down: excludes entities belonging to another source", async () => {
      const handler = captureHandler({
         getEnvironment: async () =>
            ({ getPackage: async () => mockTwoSourcePackage }) as never,
      });
      const payload = parse(
         await handler({
            environmentName: "malloy-samples",
            packageName: "sales",
            sourceName: "customers",
         }),
      );
      expect(payload.sources).toHaveLength(1);
      expect(payload.sources[0].source_info.resource_id.source).toBe(
         "customers",
      );
      expect(
         payload.sources[0].entities.map(
            (e: { entity_type: string; name: string }) => [
               e.entity_type,
               e.name,
            ],
         ),
      ).toEqual([["dimension", "state"]]);
   });

   it("tier 3 without sourceName still lists sources only, not their fields", async () => {
      // The widened filter must stay scoped to the drill-down: the plain
      // package listing is an overview and would be unreadable with every
      // field of every source in it.
      const handler = captureHandler({
         getEnvironment: async () =>
            ({ getPackage: async () => mockTwoSourcePackage }) as never,
      });
      const payload = parse(
         await handler({
            environmentName: "malloy-samples",
            packageName: "sales",
         }),
      );
      expect(
         payload.sources.map(
            (c: { source_info: { resource_id: { source: string } } }) =>
               c.source_info.resource_id.source,
         ),
      ).toEqual(["orders", "customers"]);
      // An overview names sources, never their fields.
      expect(
         payload.sources.every(
            (c: { entities?: unknown[] }) => c.entities === undefined,
         ),
      ).toBe(true);
   });

   it("tier 3 drill-down: an unknown sourceName returns no results, not everything", async () => {
      const handler = captureHandler({
         getEnvironment: async () =>
            ({ getPackage: async () => mockTwoSourcePackage }) as never,
      });
      const payload = parse(
         await handler({
            environmentName: "malloy-samples",
            packageName: "sales",
            sourceName: "nope",
         }),
      );
      expect(payload.sources).toEqual([]);
      expect(payload.total_available).toBe(0);
   });

   it("tier 3 drill-down: limit caps the fields returned", async () => {
      const handler = captureHandler({
         getEnvironment: async () =>
            ({ getPackage: async () => mockTwoSourcePackage }) as never,
      });
      const payload = parse(
         await handler({
            environmentName: "malloy-samples",
            packageName: "sales",
            sourceName: "orders",
            limit: 2,
         }),
      );
      // The source is the card, not an entity, so the limit buys two FIELDS
      // rather than the source row plus one field.
      expect(
         payload.sources[0].entities.map(
            (e: { entity_type: string; name: string }) => [
               e.entity_type,
               e.name,
            ],
         ),
      ).toEqual([
         ["view", "by_month"],
         ["dimension", "status"],
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
      expect(rankedEntities(payload).every((e) => !("relevance" in e))).toBe(
         true,
      );
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

   const VECTORS: Record<string, number[]> = {
      // An entity's name and its doc embed as separate facets, so both need
      // a stub vector. A missing entry throws, which is what makes this map
      // double as a pin on exactly what gets embedded: the join's facets are
      // here because it is indexed, and the field inside its schema is
      // absent because it is not.
      "order items": [0, 1],
      "order items: One row per product sold on an order.": [0, 1],
      state: [1, 0],
      "current building": [0, 1],
      "current building: Building this asset sits in.": [0, 1],
      "where do customers live": [1, 0],
      "what building is this in": [0, 1],
      // Points away from both entity axes, so it scores about -0.707 against
      // every facet in the package and nothing clears the floor. This is the
      // true negative: the question is not modelled here at all.
      "seismic retrofit of bridge pilings": [-1, -1],
   };

   /** A provider over an explicit text -> vector map. Unknown text throws, */
   /** so the map pins exactly which facets get embedded. */
   function stubProviderFor(
      vectors: Record<string, number[]>,
      options: { fail?: boolean } = {},
   ): EmbeddingProvider {
      const fetchStub = (async (
         _url: RequestInfo | URL,
         init?: RequestInit,
      ) => {
         if (options.fail) return new Response("down", { status: 500 });
         const body = JSON.parse(String(init?.body)) as { input: string[] };
         const data = body.input.map((text, index) => {
            const embedding = vectors[text];
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

   function stubProvider(options: { fail?: boolean } = {}): EmbeddingProvider {
      return stubProviderFor(VECTORS, options);
   }

   /** A store over the given package, backed by the temp DB. */
   function semanticStoreFor(pkg: unknown): Partial<EnvironmentStore> {
      return {
         getEnvironment: async () => envWith(async () => pkg),
         storageManager: {
            getDuckDbConnection: () => db,
         } as never,
      };
   }

   /** A store whose package is a fresh instance, backed by the temp DB. */
   function semanticStore(): Partial<EnvironmentStore> {
      return semanticStoreFor({
         listModels: async () => [{ path: "ecommerce.malloy" }],
         getModel: () => mockModel,
      });
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
      const results = rankedEntities(payload);
      expect(results[0].name).toBe("state");
      expect(results[0].entity_type).toBe("dimension");
      expect(results[0].relevance).toBeCloseTo(1.0, 3);
      // "order items" is orthogonal to the query: below the similarity
      // floor, so it must not pad the results.
      expect(results.some((r) => r.name === "order_items")).toBe(false);
   });

   it("ranks a join semantically and writes it to the embedding index", async () => {
      _setEmbeddingProviderForTests(stubProvider());
      const handler = captureHandler(semanticStore());
      const payload = await callUntilSemantic(handler, {
         environmentName: "specs",
         packageName: "join-pkg",
         query: "what building is this in",
      });
      const join = rankedEntities(payload).find(
         (r) => r.name === "current_building",
      );
      expect(join).toBeDefined();
      expect(join!.entity_type).toBe("join");
      expect(join!.relationship).toBe("one");
      expect(join!.relevance).toBeCloseTo(1.0, 3);

      // The vector cache holds it under its own kind, so it survives a
      // restart and takes part in the incremental hash diff like any other
      // entity — including being faceted, so a join's doc cannot bury the
      // join's own name.
      const rows = await db.all<{ facet: string }>(
         `SELECT facet FROM entity_embeddings
          WHERE package_name = ? AND entity_kind = 'join' AND entity_name = ?
          ORDER BY facet`,
         ["join-pkg", "current_building"],
      );
      expect(rows.map((r) => r.facet)).toEqual(["doc:0", "name"]);
   });

   it("delivers a source's doc even when the source itself scores below the floor", async () => {
      // The §5 fix, on the path that produced it. "where do customers live"
      // is orthogonal to the order_items source entity, so the source is
      // dropped by MIN_SIMILARITY and, before this, its doc went with it —
      // the agent got a field with no idea of the source's grain. The doc now
      // rides on the response regardless of how the source itself scored.
      _setEmbeddingProviderForTests(stubProvider());
      const handler = captureHandler(semanticStore());
      const payload = await callUntilSemantic(handler, {
         environmentName: "specs",
         packageName: "context-pkg",
         query: "where do customers live",
      });
      // The source itself never cleared the floor, so it is in the response
      // only as the parent of a field that did. Its relevance is therefore
      // derived from its best entity, which is what the published schema
      // specifies when source-level matching is unavailable -- a matched
      // source is never reported at null for a caller ranking sources.
      expect(payload.sources[0].relevance).toBeCloseTo(1.0, 3);
      expect(payload.sources).toEqual([
         {
            relevance: 1,
            // ...and it is still the container its field hit nests in, which
            // is what carries the doc through regardless of the source's score.
            source_info: {
               resource_id: {
                  environment: "specs",
                  package: "context-pkg",
                  model_path: "ecommerce.malloy",
                  source: "order_items",
               },
               one_line_summary: "One row per product sold on an order.",
               docs: "One row per product sold on an order.",
               joins: [
                  {
                     name: "current_building",
                     relationship: "one",
                     doc: "Building this asset sits in.",
                  },
               ],
            },
            entities: [
               {
                  name: "state",
                  entity_type: "dimension",
                  entity_id: "dimension:order_items:state",
                  relevance: 1,
               },
            ],
         },
      ]);
   });

   it("says WHY a configured server answered lexically, and stops once semantic", async () => {
      // "lexical" alone is a dead end: an agent cannot tell a cold index,
      // which clears in seconds and is worth one retry, from a down provider,
      // which is not. Only the first is actionable, so only naming it helps.
      _setEmbeddingProviderForTests(stubProvider());
      const handler = captureHandler(semanticStore());
      const params = {
         environmentName: "specs",
         packageName: "reason-pkg",
         query: "where do customers live",
      };
      const first = parse(await handler(params));
      expect(first.retrieval).toBe("lexical");
      expect(first.retrieval_reason).toBe("indexing");

      const warm = await callUntilSemantic(handler, params);
      expect(warm).not.toHaveProperty("retrieval_reason");
   });

   it("reports a dead provider as provider-error, then as cooldown", async () => {
      // Two different remedies behind one "lexical": the first call learns the
      // endpoint is down, and every call in the window after it is being
      // short-circuited deliberately rather than re-probing.
      _setEmbeddingProviderForTests(stubProvider());
      const handler = captureHandler(semanticStore());
      const params = {
         environmentName: "specs",
         packageName: "dead-provider-pkg",
         query: "where do customers live",
      };
      await callUntilSemantic(handler, params);

      _setEmbeddingProviderForTests(stubProvider({ fail: true }));
      const failed = parse(await handler(params));
      expect(failed.retrieval).toBe("lexical");
      expect(failed.retrieval_reason).toBe("provider-error");

      const cooled = parse(await handler(params));
      expect(cooled.retrieval_reason).toBe("cooldown");
   });

   // A source that matches on its own terms becomes the CARD, not a row under
   // it, so returned-entities and matched-rows are different units. Pairing
   // them reported a cut that never happened. Both directions pinned here.
   const truncModel = {
      getSourceInfos: () => [
         {
            name: "orders",
            annotations: ["#(doc) Orders placed by customers."],
            schema: {
               fields: [
                  { kind: "dimension", name: "state", annotations: [] },
                  { kind: "dimension", name: "city", annotations: [] },
               ],
            },
         },
      ],
      getQueries: () => [],
   };
   const TRUNC_VECTORS: Record<string, number[]> = {
      orders: [1, 0],
      "orders: Orders placed by customers.": [1, 0],
      state: [1, 0],
      city: [1, 0],
      "where do customers live": [1, 0],
   };
   const truncStore = () =>
      semanticStoreFor({
         listModels: async () => [{ path: "orders.malloy" }],
         getModel: () => truncModel,
      });

   it("stays silent on the semantic path when a matched source fills no slot", async () => {
      _setEmbeddingProviderForTests(stubProviderFor(TRUNC_VECTORS));
      const handler = captureHandler(truncStore());
      const payload = await callUntilSemantic(handler, {
         environmentName: "specs",
         packageName: "trunc-pkg",
         query: "where do customers live",
         limit: 10,
      });

      // Three rows matched (the source and its two fields) and all three came
      // back; the source became the card, leaving two nested entities.
      expect(payload.sources).toHaveLength(1);
      expect(payload.sources[0].relevance).toBeCloseTo(1.0, 3);
      expect(rankedEntities(payload)).toHaveLength(2);
      expect("warnings" in payload).toBe(false);
   });

   it("still reports a real cut on the semantic path", async () => {
      _setEmbeddingProviderForTests(stubProviderFor(TRUNC_VECTORS));
      const handler = captureHandler(truncStore());
      const payload = await callUntilSemantic(handler, {
         environmentName: "specs",
         packageName: "trunc-cut-pkg",
         query: "where do customers live",
         limit: 1,
      });

      expect(payload.warnings.join(" ")).toContain("Returned 1 of 3");
      expect(payload.warnings.join(" ")).toContain("Raise limit");
   });

   it("reports below_cutoff_count on semantic responses, never on lexical ones", async () => {
      _setEmbeddingProviderForTests(stubProvider());
      const handler = captureHandler(semanticStore());
      const params = {
         environmentName: "specs",
         packageName: "cutoff-pkg",
         query: "where do customers live",
      };

      // Cold start answers lexically: there is no floor on that path, so
      // reporting a count would be meaningless.
      const first = parse(await handler(params));
      expect(first.retrieval).toBe("lexical");
      expect(first).not.toHaveProperty("below_cutoff_count");

      // order_items and its join are orthogonal to this query, so they are
      // dropped by the floor and counted rather than silently missing.
      const payload = await callUntilSemantic(handler, params);
      expect(rankedEntities(payload).map((r) => r.name)).toEqual(["state"]);
      expect(payload.below_cutoff_count).toBe(2);
      // The denominator ships with it. Without it the count is a bare number
      // an agent cannot scale: 2 rejected is a tight match out of 3 and a
      // catastrophe out of 200, and nothing else in the response says which.
      expect(payload.total_entities).toBe(3);
   });

   it("reports the true negative as below_cutoff_count === total_entities, not 0", async () => {
      // The contract this replaces said "0 with no results means nothing is
      // related". That state is unreachable: every entity in scope is either
      // at-or-above the floor (and could be a hit) or below it (and is
      // counted), so zero-below means every entity was above, which would
      // have produced hits. Measured against a real provider, an ecommerce
      // package asked about seismic retrofits returned 0 results with
      // below_cutoff_count equal to its full entity count -- which the old
      // wording told the agent to read as "too diffuse, rephrase", the exact
      // opposite of the truth. Pin the reachable shape so the description
      // cannot drift back.
      _setEmbeddingProviderForTests(stubProvider());
      const handler = captureHandler(semanticStore());
      const params = {
         environmentName: "specs",
         packageName: "cutoff-pkg",
         query: "seismic retrofit of bridge pilings",
      };

      const payload = await callUntilSemantic(handler, params);
      expect(payload.sources).toEqual([]);
      expect(payload.total_entities).toBeGreaterThan(0);
      expect(payload.below_cutoff_count).toBe(payload.total_entities);
   });

   it("collapses the same concept across sibling sources into one marked row", async () => {
      // The measured worst case: "site of the building" returned SITE at an
      // identical 0.96 from fac_building, fclt_building and
      // fclt_building_hist, presented as three peers with nothing to choose
      // between them. Agents picked arbitrarily, and choosing wrong between
      // sibling families was the largest single failure class. One row now,
      // naming the alternatives, so the ambiguity is stated rather than
      // inferred — and the freed slots go to different concepts.
      _setEmbeddingProviderForTests(
         stubProviderFor({
            site: [1, 0],
            "fac building": [0, 1],
            "fclt building": [0, 1],
            "fclt building hist": [0, 1],
            "site of the building": [1, 0],
         }),
      );
      const handler = captureHandler(semanticStoreFor(mockSiblingPackage));
      const payload = await callUntilSemantic(handler, {
         environmentName: "specs",
         packageName: "sibling-pkg",
         query: "site of the building",
      });
      const sites = rankedEntities(payload).filter((r) => r.name === "site");
      expect(sites).toHaveLength(1);
      expect(sites[0].also_in).toHaveLength(2);
      expect([sites[0].source, ...(sites[0].also_in ?? [])].sort()).toEqual(
         [...SIBLING_SOURCES].sort(),
      );
   });

   it("keeps siblings separate under a drill-down, where they cannot be duplicates", async () => {
      _setEmbeddingProviderForTests(
         stubProviderFor({
            site: [1, 0],
            "fac building": [0, 1],
            "fclt building": [0, 1],
            "fclt building hist": [0, 1],
            "site of the building": [1, 0],
         }),
      );
      const handler = captureHandler(semanticStoreFor(mockSiblingPackage));
      const payload = await callUntilSemantic(handler, {
         environmentName: "specs",
         packageName: "sibling-scoped-pkg",
         query: "site of the building",
         sourceName: "fclt_building",
      });
      const drilled = rankedEntities(payload);
      expect(drilled).toHaveLength(1);
      expect(drilled[0].source).toBe("fclt_building");
      expect(drilled[0].also_in).toBeUndefined();
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
      expect(rankedEntities(payload).some((r) => r.name === "state")).toBe(
         true,
      );
      for (const r of rankedEntities(payload)) {
         expect(r.relevance).toBeUndefined();
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
      expect(rankedEntities(payload).some((r) => r.name === "state")).toBe(
         true,
      );
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
         expect(rankedEntities(payload).some((r) => r.name === "state")).toBe(
            true,
         );
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

/**
 * The card fields that come from the compiled model rather than from its
 * schema: what governs querying a source, and what type a field carries.
 *
 * These exist because retrieval that names an entity without them sends an
 * agent to write a query it cannot run. A gated source looks like any other
 * until the denial arrives; a given the model defaults looks like a value the
 * agent has to invent; and a field's type decides whether it can be filtered
 * with a string or summed at all.
 */
describe("get_context source governance and field types", () => {
   const gatedModel = {
      getSourceInfos: () => [
         {
            name: "orders_secured",
            annotations: ["#(doc) Orders visible to this caller."],
            schema: {
               fields: [
                  { kind: "measure", name: "order_count", annotations: [] },
               ],
            },
         },
         {
            name: "sales",
            annotations: [],
            schema: {
               fields: [{ kind: "dimension", name: "region", annotations: [] }],
            },
         },
      ],
      getQueries: () => [],
      getSources: () => [
         {
            name: "orders_secured",
            givens: [
               { name: "ROLE", type: "string", default: "'analyst'" },
               { name: "TENANT", type: "string" },
            ],
            authorize: ["$ROLE = 'admin' or $TENANT = 'acme'"],
         },
         {
            name: "sales",
            givens: [{ name: "REGION", type: "filter<string>" }],
         },
      ],
   };
   const gatedPackage = {
      listModels: async () => [{ path: "governed.malloy" }],
      getModel: () => gatedModel,
   };

   it("reports a source's authorize gates and the givens they read", async () => {
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => gatedPackage),
      });
      const payload = parse(
         await handler({
            environmentName: "specs",
            packageName: "governed",
         }),
      );
      const gated = payload.sources.find(
         (c: SourceCardShape) =>
            c.source_info.resource_id.source === "orders_secured",
      );
      expect(gated.source_info.authorize).toEqual([
         {
            expression: "$ROLE = 'admin' or $TENANT = 'acme'",
            given_names: ["ROLE", "TENANT"],
         },
      ]);
      expect(gated.source_info.givens).toEqual([
         { name: "ROLE", type: "string", default: "'analyst'" },
         { name: "TENANT", type: "string" },
      ]);
   });

   it("omits authorize on an ungated source rather than sending it empty", async () => {
      // Absence is the contract on both sides, and an empty array would read
      // as "a gate with no expressions" to anything checking length.
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => gatedPackage),
      });
      const payload = parse(
         await handler({ environmentName: "specs", packageName: "governed" }),
      );
      const open = payload.sources.find(
         (c: SourceCardShape) => c.source_info.resource_id.source === "sales",
      );
      expect("authorize" in open.source_info).toBe(false);
      expect(open.source_info.givens).toEqual([
         { name: "REGION", type: "filter<string>" },
      ]);
   });

   it("omits both when the model exposes no compiled sources", async () => {
      // getSources() is read defensively: a model that does not implement it
      // must still list, not throw, and must not grow empty keys.
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => mockPackage),
      });
      const payload = parse(
         await handler({
            environmentName: "malloy-samples",
            packageName: "ecommerce",
         }),
      );
      expect("givens" in payload.sources[0].source_info).toBe(false);
      expect("authorize" in payload.sources[0].source_info).toBe(false);
   });

   it("carries a field's Malloy type, and only where a type exists", async () => {
      const typedModel = {
         getSourceInfos: () => [
            {
               name: "orders",
               annotations: [],
               schema: {
                  fields: [
                     {
                        kind: "dimension",
                        name: "status",
                        type: { kind: "string_type" },
                        annotations: [],
                     },
                     {
                        kind: "measure",
                        name: "revenue",
                        type: { kind: "number_type" },
                        annotations: [],
                     },
                     { kind: "view", name: "by_month", annotations: [] },
                  ],
               },
            },
         ],
         getQueries: () => [],
      };
      const handler = captureHandler({
         getEnvironment: async () =>
            envWith(async () => ({
               listModels: async () => [{ path: "typed.malloy" }],
               getModel: () => typedModel,
            })),
      });
      const payload = parse(
         await handler({
            environmentName: "specs",
            packageName: "typed",
            sourceName: "orders",
         }),
      );
      const byName = new Map(
         (payload.sources[0].entities as RankedEntity[]).map((e) => [
            e.name,
            e,
         ]),
      );
      expect(byName.get("status")?.data_type).toBe("string");
      expect(byName.get("revenue")?.data_type).toBe("number");
      // A view is a query, not a value, so it has no type to report.
      expect("data_type" in (byName.get("by_month") ?? {})).toBe(false);
   });

   it("summarizes a source in one line, cut at its first sentence", async () => {
      const wordy = `#(doc) One row per order. ${"Detail sentence. ".repeat(20)}`;
      const handler = captureHandler({
         getEnvironment: async () =>
            envWith(async () => ({
               listModels: async () => [{ path: "wordy.malloy" }],
               getModel: () => ({
                  getSourceInfos: () => [
                     {
                        name: "orders",
                        annotations: [wordy],
                        schema: { fields: [] },
                     },
                  ],
                  getQueries: () => [],
               }),
            })),
      });
      const payload = parse(
         await handler({ environmentName: "specs", packageName: "wordy" }),
      );
      const info = payload.sources[0].source_info;
      expect(info.one_line_summary).toBe("One row per order.");
      // The full text stays on the card; the summary is a label, not a
      // replacement for it.
      expect(info.docs.length).toBeGreaterThan(info.one_line_summary.length);
   });

   it("omits the summary for an undocumented source", async () => {
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => mockTwoSourcePackage),
      });
      const payload = parse(
         await handler({ environmentName: "specs", packageName: "sales" }),
      );
      const customers = payload.sources.find(
         (c: SourceCardShape) =>
            c.source_info.resource_id.source === "customers",
      );
      expect("one_line_summary" in customers.source_info).toBe(false);
   });
});

/**
 * What a capped response says about what it left out.
 *
 * A response cut to `limit` used to look identical to one that found exactly
 * that many entities, so an agent stopped at a partial answer believing it was
 * complete. The warning states the count and the remedy that works, which is
 * raising the limit or narrowing the question -- not "search more
 * specifically", which is advice about a stage that did not do the cutting.
 */
describe("get_context truncation reporting", () => {
   const wideModel = {
      getSourceInfos: () => [
         {
            name: "orders",
            annotations: [],
            schema: {
               fields: Array.from({ length: 12 }, (_, i) => ({
                  kind: "measure",
                  name: `revenue_${i}`,
                  // Documented so a lexical search matches all twelve:
                  // `revenue_0` is a single lunr token and the bare term
                  // would not reach it.
                  annotations: [`#(doc) Revenue measure ${i}.`],
               })),
            },
         },
      ],
      getQueries: () => [],
   };
   const widePackage = {
      listModels: async () => [{ path: "wide.malloy" }],
      getModel: () => wideModel,
   };

   it("says how many entities a capped drill-down left out", async () => {
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => widePackage),
      });
      const payload = parse(
         await handler({
            environmentName: "specs",
            packageName: "wide",
            sourceName: "orders",
            limit: 4,
         }),
      );
      expect(payload.sources[0].entities).toHaveLength(4);
      expect(payload.warnings.join(" ")).toContain("Returned 4 of 12");
      expect(payload.warnings.join(" ")).toContain("Raise limit");
   });

   it("says how many entities a capped search left out", async () => {
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => widePackage),
      });
      const payload = parse(
         await handler({
            environmentName: "specs",
            packageName: "wide",
            query: "revenue",
            limit: 3,
         }),
      );
      expect(rankedEntities(payload)).toHaveLength(3);
      expect(payload.warnings.join(" ")).toContain("Returned 3 of 12");
   });

   it("stays silent when nothing was cut", async () => {
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => widePackage),
      });
      const payload = parse(
         await handler({
            environmentName: "specs",
            packageName: "wide",
            sourceName: "orders",
            limit: 50,
         }),
      );
      expect("warnings" in payload).toBe(false);
   });
});

/**
 * Duplicate and hidden entities, the classes that decide what a result window
 * is actually worth. Each is a defect observed in a comparable retrieval
 * service; these pin whether Publisher shares it.
 */
describe("get_context duplicate and visibility handling", () => {
   const sharedSource = {
      name: "orders",
      annotations: ["#(doc) One row per order."],
      schema: {
         fields: [{ kind: "dimension", name: "status", annotations: [] }],
      },
   };

   it("indexes a re-exported source once, from the same model every time", async () => {
      // A package can expose one source from several models (an import, or a
      // model that extends another). Only the first is kept, so which model
      // that is has to be a property of the package rather than of the order
      // the filesystem happened to list it in.
      const pathsFor = async (models: string[]) => {
         const handler = captureHandler({
            getEnvironment: async () =>
               envWith(async () => ({
                  listModels: async () => models.map((path) => ({ path })),
                  getModel: () => ({
                     getSourceInfos: () => [sharedSource],
                     getQueries: () => [],
                  }),
               })),
         });
         const payload = parse(
            await handler({ environmentName: "specs", packageName: "dup" }),
         );
         expect(payload.sources).toHaveLength(1);
         return payload.sources[0].source_info.resource_id.model_path;
      };
      expect(await pathsFor(["a.malloy", "b.malloy"])).toBe("a.malloy");
      // Reversed listing, same answer: the choice does not follow the order.
      expect(await pathsFor(["b.malloy", "a.malloy"])).toBe("a.malloy");
   });

   it("never returns a join declared inside another join's target", async () => {
      // A joined source's own joins belong to that source, and it is indexed
      // under its own name. Surfacing them here would offer a path the caller
      // cannot write from this source, at a grain it did not ask for.
      const handler = captureHandler({
         getEnvironment: async () =>
            envWith(async () => ({
               listModels: async () => [{ path: "nested.malloy" }],
               getModel: () => ({
                  getSourceInfos: () => [
                     {
                        name: "order_items",
                        annotations: [],
                        schema: {
                           fields: [
                              {
                                 kind: "join",
                                 name: "products",
                                 relationship: "one",
                                 annotations: [],
                                 schema: {
                                    fields: [
                                       {
                                          kind: "join",
                                          name: "category",
                                          relationship: "one",
                                          annotations: [],
                                          schema: { fields: [] },
                                       },
                                    ],
                                 },
                              },
                           ],
                        },
                     },
                  ],
                  getQueries: () => [],
               }),
            })),
      });
      const payload = parse(
         await handler({
            environmentName: "specs",
            packageName: "nested",
            sourceName: "order_items",
         }),
      );
      const names = payload.sources[0].entities.map(
         (e: { name: string }) => e.name,
      );
      expect(names).toEqual(["products"]);
      // The card's join list is the source's own, and complete.
      expect(payload.sources[0].source_info.joins).toEqual([
         { name: "products", relationship: "one" },
      ]);
   });

   it("treats an underscore-prefixed name as an ordinary field", async () => {
      // Malloy has no naming convention for privacy; hiding a field is what
      // `include { internal: ... }` is for, and the compiler drops those
      // before this code sees them. Inventing a second rule here would hide a
      // field its author never asked to hide.
      const handler = captureHandler({
         getEnvironment: async () =>
            envWith(async () => ({
               listModels: async () => [{ path: "u.malloy" }],
               getModel: () => ({
                  getSourceInfos: () => [
                     {
                        name: "orders",
                        annotations: [],
                        schema: {
                           fields: [
                              {
                                 kind: "dimension",
                                 name: "_internal_key",
                                 annotations: [],
                              },
                           ],
                        },
                     },
                  ],
                  getQueries: () => [],
               }),
            })),
      });
      const payload = parse(
         await handler({
            environmentName: "specs",
            packageName: "u",
            sourceName: "orders",
         }),
      );
      expect(
         payload.sources[0].entities.map((e: { name: string }) => e.name),
      ).toEqual(["_internal_key"]);
   });

   it("states a source's doc once, not once per matching entity", async () => {
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => mockPackage),
      });
      const result = await handler({
         environmentName: "malloy-samples",
         packageName: "ecommerce",
         query: "order state",
      });
      const text = JSON.stringify(parse(result).sources);
      const doc = "One row per product sold on an order.";
      // Once as `docs`, once as the one-line summary derived from it, and
      // never repeated onto the entities that nest under the card.
      expect(text.split(doc).length - 1).toBe(2);
   });
});

/**
 * Duplicate collapsing on the LEXICAL path, which is what an unconfigured
 * server runs.
 *
 * A model whose sources extend a common base repeats every inherited field
 * verbatim, so the duplicates score identically and fill the window with one
 * concept. Measured on the bundled governed-analytics package, a search for
 * "total order amount" spent 2 of 6 slots that way. Collapsing is not a
 * property of the ranker that happens to be configured.
 */
describe("get_context lexical sibling collapse", () => {
   const inheritedField = (doc: string) => ({
      kind: "measure",
      name: "total_amount",
      annotations: [`#(doc) ${doc}`],
   });
   const siblingModel = (baseDoc: string, derivedDoc: string) => ({
      getSourceInfos: () => [
         {
            name: "sales",
            annotations: [],
            schema: { fields: [inheritedField(baseDoc)] },
         },
         {
            name: "sales_secured",
            annotations: [],
            schema: { fields: [inheritedField(derivedDoc)] },
         },
      ],
      getQueries: () => [],
   });
   const handlerFor = (model: unknown) =>
      captureHandler({
         getEnvironment: async () =>
            envWith(async () => ({
               listModels: async () => [{ path: "s.malloy" }],
               getModel: () => model,
            })),
      });

   it("collapses a field two sibling sources inherited unchanged", async () => {
      const doc = "Total order amount.";
      const payload = parse(
         await handlerFor(siblingModel(doc, doc))({
            environmentName: "specs",
            packageName: "sib",
            query: "total order amount",
         }),
      );
      const rows = rankedEntities(payload).filter(
         (e) => e.name === "total_amount",
      );
      expect(rows).toHaveLength(1);
      expect(rows[0].also_in).toEqual(["sales_secured"]);
   });

   it("keeps them apart when their docs say different things", async () => {
      // Different docs are different guidance, and the agent chooses between
      // them by reading both. Collapsing here would hide the one that says
      // the numbers are filtered.
      const payload = parse(
         await handlerFor(
            siblingModel(
               "Total order amount.",
               "Total order amount visible to this caller, after the access gate.",
            ),
         )({
            environmentName: "specs",
            packageName: "sib",
            query: "total order amount",
         }),
      );
      const rows = rankedEntities(payload).filter(
         (e) => e.name === "total_amount",
      );
      expect(rows).toHaveLength(2);
      expect(rows.every((r) => r.also_in === undefined)).toBe(true);
   });

   it("never collapses within a drill-down, where there are no siblings", async () => {
      const doc = "Total order amount.";
      const payload = parse(
         await handlerFor(siblingModel(doc, doc))({
            environmentName: "specs",
            packageName: "sib",
            query: "total order amount",
            sourceName: "sales",
         }),
      );
      expect(sourceNames(payload)).toEqual(["sales"]);
      expect(
         rankedEntities(payload).every((e) => e.also_in === undefined),
      ).toBe(true);
   });
});
