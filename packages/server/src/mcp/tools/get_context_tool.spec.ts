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
            fields: [{ kind: "dimension", name: "room_key", annotations: [] }],
         },
      },
   ],
   getQueries: () => [],
};
const mockLongDocPackage = {
   listModels: async () => [{ path: "rooms.malloy" }],
   getModel: () => mockLongDocModel,
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
            doc: "One row per product sold on an order.",
            joins: [
               {
                  name: "current_building",
                  relationship: "one",
                  doc: "Building this asset sits in.",
               },
            ],
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

   it("tier 4: retrieves a declared join, carrying its cardinality", async () => {
      // Joins were skipped by the collector, so an agent could not see that
      // the model declared one and concluded it had to bridge the tables
      // itself. The join's #(doc) was likewise unreachable.
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => mockPackage),
      });
      const { results } = parse(
         await handler({
            environmentName: "malloy-samples",
            packageName: "ecommerce",
            query: "building",
         }),
      );
      const join = results.find(
         (r: { name: string }) => r.name === "current_building",
      );
      expect(join).toBeDefined();
      expect(join.kind).toBe("join");
      expect(join.relationship).toBe("one");
      // `source` names the source that DECLARES the join, so a drill-down on
      // that source sees it.
      expect(join.source).toBe("order_items");
      expect(join.doc).toBe("Building this asset sits in.");
   });

   it("tier 4: does not recurse into a join's own schema", async () => {
      // The joined source's fields are already indexed under that source.
      // Recursing would re-index every one of them once per join that
      // reaches it, which is the redundancy this tool can least afford.
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => mockPackage),
      });
      const { results } = parse(
         await handler({
            environmentName: "malloy-samples",
            packageName: "ecommerce",
            query: "Zzyzx",
         }),
      );
      expect(results).toEqual([]);
   });

   it("tier 3: a package listing still returns only sources, not joins", async () => {
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => mockPackage),
      });
      const { results } = parse(
         await handler({
            environmentName: "malloy-samples",
            packageName: "ecommerce",
         }),
      );
      expect(results.map((r: { kind: string }) => r.kind)).toEqual(["source"]);
   });

   it("delivers the parent source's doc and joins alongside a field hit", async () => {
      // A field hit used to arrive with only its own doc: the source's grain
      // and population rules reached the agent only when the source itself
      // happened to rank for the same query, so guidance placement depended
      // on query phrasing rather than on where the modeller wrote it.
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => mockPackage),
      });
      const { results, sources } = parse(
         await handler({
            environmentName: "malloy-samples",
            packageName: "ecommerce",
            query: "state",
         }),
      );
      // The hit itself is a field, carrying only its own (empty) doc...
      expect(results[0].kind).toBe("dimension");
      expect(results[0].doc).toBe("");
      // ...and the source context arrives regardless.
      expect(sources).toEqual([
         {
            name: "order_items",
            modelPath: "ecommerce.malloy",
            doc: "One row per product sold on an order.",
            joins: [
               {
                  name: "current_building",
                  relationship: "one",
                  doc: "Building this asset sits in.",
               },
            ],
         },
      ]);
   });

   it("reports each source once, however many of its entities matched", async () => {
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => mockPackage),
      });
      const { results, sources } = parse(
         await handler({
            environmentName: "malloy-samples",
            packageName: "ecommerce",
            // Matches the source, the join, and (via the source doc) more.
            query: "order building state",
         }),
      );
      expect(results.length).toBeGreaterThan(1);
      expect(sources).toHaveLength(1);
      expect(sources[0].name).toBe("order_items");
   });

   it("truncates a long source doc in context but not in the result itself", async () => {
      // The context copy exists to deliver the caveat, not to reproduce the
      // model file; a source that is itself the hit still returns in full.
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => mockLongDocPackage),
      });
      const { results, sources } = parse(
         await handler({
            environmentName: "e",
            packageName: "p",
            query: "rooms",
         }),
      );
      const sourceHit = results.find(
         (r: { kind: string }) => r.kind === "source",
      );
      expect(sourceHit.doc).toBe(LONG_SOURCE_DOC.trim());
      expect(sourceHit.doc.length).toBeGreaterThan(500);

      expect(sources[0].doc.length).toBeLessThanOrEqual(501);
      expect(sources[0].doc.endsWith("…")).toBe(true);
      expect(sources[0].doc).toStartWith("Rooms in the facilities inventory.");
   });

   it("reports an empty joins list for a source that declares none", async () => {
      // The authoritative negative: without it an agent cannot tell "no joins
      // declared" from "joins exist but were not returned", and we watched
      // agents spend queries probing for a relationship that was never there.
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => mockLongDocPackage),
      });
      const { results } = parse(
         await handler({ environmentName: "e", packageName: "p" }),
      );
      expect(results[0].joins).toEqual([]);
   });

   it("omits the sources block entirely when nothing matched", async () => {
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
      expect(payload.results).toEqual([]);
      expect(payload).not.toHaveProperty("sources");
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
      const { results } = parse(
         await handler({
            environmentName: "e",
            packageName: "p",
            query: "site",
         }),
      );
      const sites = results.filter(
         (r: { name: string }) => r.name.toLowerCase() === "site",
      );
      expect(sites).toHaveLength(1);
      expect(sites[0].name).toBe("site");
      expect(sites[0].doc).toBe("Campus site the building sits on.");
      expect(sites[0].aliases).toEqual(["SITE"]);
   });

   it("leaves genuinely distinct fields in a source alone", async () => {
      const handler = captureHandler({
         getEnvironment: async () =>
            ({ getPackage: async () => mockAliasPackage }) as never,
      });
      const { results } = parse(
         await handler({
            environmentName: "e",
            packageName: "p",
            query: "height",
         }),
      );
      const height = results.find((r: { name: string }) => r.name === "height");
      expect(height).toBeDefined();
      expect(height.aliases).toBeUndefined();
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
      const { results } = parse(
         await handler({
            environmentName: "e",
            packageName: "p",
            query: "site",
         }),
      );
      expect(results.some((r: { name: string }) => r.name === "SITE")).toBe(
         false,
      );
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
      // `sources` is metadata every caller benefits from, so it rides on the
      // unconfigured payload too; the marker and scores stay provider-only.
      expect(Object.keys(payload)).toEqual(["results", "sources"]);
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

   it("ranks a join semantically and writes it to the embedding index", async () => {
      _setEmbeddingProviderForTests(stubProvider());
      const handler = captureHandler(semanticStore());
      const payload = await callUntilSemantic(handler, {
         environmentName: "specs",
         packageName: "join-pkg",
         query: "what building is this in",
      });
      const join = payload.results.find(
         (r: { name: string }) => r.name === "current_building",
      );
      expect(join).toBeDefined();
      expect(join.kind).toBe("join");
      expect(join.relationship).toBe("one");
      expect(join.score).toBeCloseTo(1.0, 3);

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
      expect(
         payload.results.some((r: { kind: string }) => r.kind === "source"),
      ).toBe(false);
      expect(payload.sources).toEqual([
         {
            name: "order_items",
            modelPath: "ecommerce.malloy",
            doc: "One row per product sold on an order.",
            joins: [
               {
                  name: "current_building",
                  relationship: "one",
                  doc: "Building this asset sits in.",
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
      expect(first.retrievalReason).toBe("indexing");

      const warm = await callUntilSemantic(handler, params);
      expect(warm).not.toHaveProperty("retrievalReason");
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
      expect(failed.retrievalReason).toBe("provider-error");

      const cooled = parse(await handler(params));
      expect(cooled.retrievalReason).toBe("cooldown");
   });

   it("reports belowCutoffCount on semantic responses, never on lexical ones", async () => {
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
      expect(first).not.toHaveProperty("belowCutoffCount");

      // order_items and its join are orthogonal to this query, so they are
      // dropped by the floor and counted rather than silently missing.
      const payload = await callUntilSemantic(handler, params);
      expect(payload.results.map((r: { name: string }) => r.name)).toEqual([
         "state",
      ]);
      expect(payload.belowCutoffCount).toBe(2);
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
      const sites = payload.results.filter(
         (r: { name: string }) => r.name === "site",
      );
      expect(sites).toHaveLength(1);
      expect(sites[0].alsoIn).toHaveLength(2);
      expect([sites[0].source, ...sites[0].alsoIn].sort()).toEqual(
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
      expect(payload.results).toHaveLength(1);
      expect(payload.results[0].source).toBe("fclt_building");
      expect(payload.results[0].alsoIn).toBeUndefined();
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

const mockTypedModel = {
   getSourceInfos: () => [
      {
         name: "order_items",
         annotations: ["#(doc) One row per product sold on an order."],
         schema: {
            fields: [
               { kind: "dimension", name: "state", annotations: [] },
               {
                  kind: "measure",
                  name: "sales",
                  annotations: ["#(doc) Total sales amount."],
               },
               {
                  kind: "view",
                  name: "by_category",
                  annotations: ["#(doc) Sales by product category."],
               },
               {
                  kind: "join",
                  name: "current_building",
                  relationship: "one",
                  annotations: ["#(doc) Building this asset sits in."],
               },
            ],
         },
      },
   ],
   getQueries: () => [
      {
         name: "top_orders",
         sourceName: "order_items",
         annotations: ["#(doc) Highest value orders."],
      },
   ],
   getSources: () => [
      {
         name: "order_items",
         givens: [{ name: "ROLE", type: "string" }],
         authorize: undefined,
      },
   ],
};
const mockTypedPackage = {
   listModels: async () => [{ path: "ecommerce.malloy" }],
   getModel: () => mockTypedModel,
};

describe("get_context typed contract", () => {
   it("returns a Credible-shaped envelope for source targets", async () => {
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => mockTypedPackage),
      });
      const payload = parse(
         await handler({
            search_targets: [
               { target_type: "source", search_text: "product sold" },
            ],
            scopes: [
               { environment: "malloy-samples", package: "ecommerce" },
            ],
         }),
      );
      expect(payload.ranking).toBe("relevance");
      expect(payload.sources[0].name).toBe("order_items");
      expect(payload.sources[0].resource_id).toEqual({
         environment: "malloy-samples",
         package: "ecommerce",
         model_path: "ecommerce.malloy",
         source: "order_items",
      });
      expect(payload.sources[0].joins).toHaveLength(1);
      expect(payload.targets[0].results[0].entityId).toBe(
         "source:order_items:order_items",
      );
      expect(payload.targets[0].results[0].rank).toBe(1);
      expect(payload.results).toBeUndefined();
   });

   it("maps named queries through the view target type", async () => {
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => mockTypedPackage),
      });
      const payload = parse(
         await handler({
            search_targets: [
               { target_type: "view", search_text: "highest value" },
            ],
            environmentName: "malloy-samples",
            packageName: "ecommerce",
         }),
      );
      const names = payload.targets[0].results.map((r: { name: string }) => r.name);
      expect(names).toContain("top_orders");
      expect(
         payload.targets[0].results.every(
            (r: { kind: string }) => r.kind === "view",
         ),
      ).toBe(true);
   });

   it("rejects mixing source targets with entity targets", async () => {
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => mockTypedPackage),
      });
      const result = await handler({
         search_targets: [
            { target_type: "source", search_text: "orders" },
            { target_type: "measure", search_text: "sales" },
         ],
         environmentName: "malloy-samples",
         packageName: "ecommerce",
      });
      expect(result.isError).toBe(true);
      expect(parse(result).error).toMatch(/Do not mix source targets/);
   });

   it("lists dimensions by prominence when search_text is null", async () => {
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => mockTypedPackage),
      });
      const payload = parse(
         await handler({
            search_targets: [{ target_type: "dimension" }],
            scopes: [
               {
                  environment: "malloy-samples",
                  package: "ecommerce",
                  source: "order_items",
               },
            ],
         }),
      );
      expect(payload.ranking).toBe("prominence");
      expect(payload.targets[0].results.map((r: { name: string }) => r.name)).toEqual([
         "state",
      ]);
      expect(payload.sources[0].entities.dimensions[0].entityId).toBe(
         "dimension:order_items:state",
      );
   });

   it("keeps the legacy query payload byte-compatible", async () => {
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
      expect(payload.results[0].kind).toBe("dimension");
      expect(payload.ranking).toBeUndefined();
      expect(payload.targets).toBeUndefined();
   });
});

