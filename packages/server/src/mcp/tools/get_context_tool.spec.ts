// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterAll, beforeEach, describe, expect, it } from "bun:test";
import { sanitizeLunrQuery as sanitize } from "./retrieval_shared";
import { entityId } from "./get_context_response";
import { registerGetContextTool } from "./get_context_tool";
import { registerListPackagesTool } from "./list_packages_tool";
import { docOnlyText, docText } from "./package_entities";
import { setRetrievalConfig } from "../../retrieval_config";
import type { EnvironmentStore } from "../../service/environment_store";
import { EnvironmentNotFoundError, PackageNotFoundError } from "../../errors";
import {
   _clearEmbeddingProviderForTests,
   _setEmbeddingProviderForTests,
} from "../../service/embedding_provider";

// Pin the baseline: every test in this file runs with no embedding provider,
// regardless of ambient EMBEDDING_* env vars in the developer's shell.
// Value search is off here: the dimensional_value tests in this file pin what a
// server with it turned off answers. The on-mode is get_context_values.spec.ts.
beforeEach(() => {
   _setEmbeddingProviderForTests(null);
   setRetrievalConfig({ dimensionValues: { enabled: false } });
});
afterAll(() => {
   _clearEmbeddingProviderForTests();
   setRetrievalConfig(undefined);
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
            "#(access_filter) \"$ROLE = 'admin'\"",
            "#(malloy) drillable",
         ]),
      ).toBe("");
   });

   it("keeps only the #(doc) line when mixed with predicate annotations", () => {
      expect(
         docOnlyText([
            "#(access_filter) \"$TENANT = 'acme'\"",
            "#(doc) Secured orders.",
         ]),
      ).toBe("Secured orders.");
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

/**
 * Two tools register from one call, so capture by NAME rather than by taking
 * whichever registered last. Defaults to the flat tool, which is what the bulk
 * of this file exercises; `captureConverged` takes the other.
 */
function captureNamed(
   store: Partial<EnvironmentStore>,
   wanted: string,
): Handler {
   const handlers = new Map<string, Handler>();
   const fakeServer = {
      tool: (name: string, _desc: string, _shape: unknown, h: Handler) => {
         handlers.set(name, h);
      },
   };
   registerGetContextTool(fakeServer as never, store as EnvironmentStore);
   const handler = handlers.get(wanted);
   if (!handler) throw new Error(`${wanted} was not registered`);
   return handler;
}

function captureHandler(store: Partial<EnvironmentStore>): Handler {
   return captureNamed(store, "get_context");
}

const captureConverged = captureHandler;

const EVERY_KIND = ["source", "dimension", "measure", "view", "join"] as const;

/**
 * One phrase against every kind. The flat request this tool replaced took a
 * single untyped `query` that ranged over all of them, so this is what those
 * cases mean once targets are typed. Ranking is unchanged: identical text
 * scores identically whichever target carries it, and each entity is claimed
 * by the one target whose kind it matches.
 */
const anyKind = (text: string) =>
   EVERY_KIND.map((target_type) => ({ target_type, search_text: text }));

/** Enumerate every kind, which is what a source-scoped listing returns. */
const everyKind = () => EVERY_KIND.map((target_type) => ({ target_type }));

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
   join_path?: string;
   matched_targets?: Array<{ search_text: string; relevance: number }>;
   aliases?: string[];
   code?: string;
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
// a distinctive token, so a search for it pins that a joined field is indexed
// under its dotted path rather than left unreachable.
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
   listSkills: () => [],
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
   listSkills: () => [],
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
      // Every package ships no skills unless a test says otherwise.
      getPackage: async () => ({
         listSkills: () => [],
         ...((await getPackage()) as object),
      }),
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
   // The compiled IR behind that schema. `site` is declared as `SITE`, which
   // is what a real `dimension: site is SITE` compiles to and the only thing
   // that proves the two are one column; `height` is an ordinary raw column
   // and references nothing.
   getModelDef: () => ({
      contents: {
         fclt_building: {
            name: "fclt_building",
            fields: [
               { name: "SITE" },
               {
                  name: "site",
                  code: "SITE",
                  e: { node: "field", path: ["SITE"] },
               },
               { name: "height" },
            ],
         },
      },
   }),
};
const mockAliasPackage = {
   listModels: async () => [{ path: "b.malloy" }],
   getModel: () => mockAliasModel,
   listSkills: () => [],
};

// Both carry an expression, and NEITHER is a bare reference to the other:
// `netSales` is computed with a filter. Under the old name-humanizing rule
// these folded unless their docs differed; under the reference rule they are
// simply two fields.
const mockDistinctSpellingDefs = {
   contents: {
      orders: {
         name: "orders",
         fields: [
            { name: "net_sales", code: "gross - returns" },
            {
               name: "netSales",
               code: "gross { where: status = 'complete' } - returns",
            },
         ],
      },
   },
};

// Three parallel sources carrying the same concept, the shape that produced
// the largest measured failure class.
// Two names that humanize identically but are NOT one column: a raw amount
// and a filtered measure. Neither is declared as the other, and that -- not
// their docs -- is what keeps them apart now.
const mockDistinctSpellingModel = {
   getSourceInfos: () => [
      {
         name: "orders",
         annotations: [],
         schema: {
            fields: [
               {
                  kind: "dimension",
                  name: "net_sales",
                  annotations: ["#(doc) Gross sales less returns, all orders."],
               },
               {
                  kind: "dimension",
                  name: "netSales",
                  annotations: [
                     "#(doc) Completed orders only, excludes trials.",
                  ],
               },
            ],
         },
      },
   ],
   getQueries: () => [],
   getModelDef: () => mockDistinctSpellingDefs,
};
const mockDistinctSpellingPackage = {
   listModels: async () => [{ path: "d.malloy" }],
   getModel: () => mockDistinctSpellingModel,
   listSkills: () => [],
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
   listSkills: () => [],
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
   listSkills: () => [],
};

describe("get_context discovery tiers", () => {
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
         search_targets: anyKind("state"),
         scopes: [{ environment: "malloy-samples", package: "nope" }],
      });
      expect(result.isError).toBe(true);
      const parsed = parse(result);
      expect(parsed.sources).toEqual([]);
      expect(parsed).not.toHaveProperty("results");
      expect(parsed.error).toContain("Resource not found");
      expect(parsed.error).toContain("nope");
      expect(textBlock(result)).toContain("Resource not found");
   });

   it("names an unknown environment and the ones that exist", async () => {
      const handler = captureHandler({
         getEnvironment: async () => {
            throw new EnvironmentNotFoundError(
               'Environment "analytics" could not be resolved to a path.',
               {
                  environmentName: "analytics",
                  availableEnvironments: ["default"],
               },
            );
         },
      });
      const result = await handler({
         search_targets: anyKind("state"),
         scopes: [{ environment: "analytics", package: "p" }],
      });
      expect(result.isError).toBe(true);
      expect(parse(result).error).toBe(
         "Environment 'analytics' not found. Available environments: default. Use a name from list_packages.",
      );
   });

   it("tier 3: package without a query lists only its sources", async () => {
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => mockPackage),
      });
      const payload = parse(
         await handler({
            search_targets: [{ target_type: "source" }],
            scopes: [{ environment: "malloy-samples", package: "ecommerce" }],
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

   it("names the guides a package ships, without their bodies", async () => {
      // The listing rides on every package-scoped response because the agent
      // that most needs it is the one that did not think to ask. Descriptions
      // only: bodies are what get_skill is for.
      const withSkills = {
         ...mockPackage,
         listSkills: () => [
            {
               name: "storefront-conventions",
               description: "What the measures actually count.",
               body: "LONG BODY",
            },
            {
               name: "storefront-conventions/margin",
               description: "Reference detail.",
               body: "REFERENCE BODY",
            },
         ],
      };
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => withSkills),
      });
      const payload = parse(
         await handler({
            search_targets: [{ target_type: "source" }],
            scopes: [{ environment: "malloy-samples", package: "ecommerce" }],
         }),
      );
      // The reference entry is addressed by its parent's pointer, so it is not
      // one of the things a caller chooses from.
      expect(payload.skills).toEqual([
         {
            name: "storefront-conventions",
            description: "What the measures actually count.",
         },
      ]);
      expect(JSON.stringify(payload)).not.toContain("LONG BODY");
      expect(typeof payload.skills_note).toBe("string");
   });

   it("bounds the total description text it attaches, keeping every name", async () => {
      const handler = captureHandler({
         getEnvironment: async () =>
            envWith(async () => ({
               ...mockPackage,
               listSkills: () =>
                  ["a", "b", "c"].map((name) => ({
                     name,
                     description: "x".repeat(3000),
                     body: "",
                  })),
            })),
      });
      const payload = parse(
         await handler({
            search_targets: [{ target_type: "source" }],
            scopes: [{ environment: "malloy-samples", package: "ecommerce" }],
         }),
      );
      const skills = payload.skills as Array<{
         name: string;
         description: string;
      }>;
      expect(skills.map((s) => s.name)).toEqual(["a", "b", "c"]);
      expect(skills.reduce((n, s) => n + s.description.length, 0)).toBe(4000);
   });

   it("carries no skills key for a package that ships none", async () => {
      // Most packages ship none, and an empty array is a key every consumer
      // would then have to test.
      const handler = captureHandler({
         getEnvironment: async () =>
            envWith(async () => ({ ...mockPackage, listSkills: () => [] })),
      });
      const payload = parse(
         await handler({
            search_targets: [{ target_type: "source" }],
            scopes: [{ environment: "malloy-samples", package: "ecommerce" }],
         }),
      );
      expect("skills" in payload).toBe(false);
      expect("skills_note" in payload).toBe(false);
   });

   it("tier 3: a populated listing of a current package carries no warnings", async () => {
      // Warnings are for the ambiguous cases only; a healthy payload must stay
      // byte-identical to what it was before warnings existed.
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => mockPackage),
      });
      const payload = parse(
         await handler({
            search_targets: [{ target_type: "source" }],
            scopes: [{ environment: "malloy-samples", package: "ecommerce" }],
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
            search_targets: [{ target_type: "source" }],
            scopes: [{ environment: "malloy-samples", package: "ecommerce" }],
         }),
      );
      expect(payload.sources).toHaveLength(1);
      expect(payload.warnings.join(" ")).toContain("STALE");
      expect(payload.warnings.join(" ")).toContain("2026-08-13T00:00:00.000Z");
      expect(payload.warnings.join(" ")).toContain("reload_package");
   });

   it("a ranked request against a stale package carries the note too", async () => {
      // A ranked request is the path that goes straight from a question to field names
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
            search_targets: anyKind("order items"),
            scopes: [{ environment: "malloy-samples", package: "ecommerce" }],
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
            search_targets: [{ target_type: "source" }],
            scopes: [
               { environment: "malloy-samples", package: "curated-empty" },
            ],
         }),
      );
      expect(payload.sources).toEqual([]);
      expect(payload.warnings.join(" ")).toContain("curation gap");
      expect(payload.warnings.join(" ")).toContain("get_status");
   });

   it("a scope naming a source that does not exist warns about nothing", async () => {
      // Found by hand-driving a live server. The truncation warning counted
      // its two sides in different units: `capped.length - 1` assumed the
      // scoped source's own row was present, so an unknown source produced
      // "Returned -1 of 0 matching entities" -- a negative count, advising a
      // remedy for a cut that never happened. A result that lost nothing must
      // say nothing, the way a question the model does not cover already does.
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => mockPackage),
      });
      const payload = parse(
         await handler({
            search_targets: everyKind(),
            scopes: [
               {
                  environment: "malloy-samples",
                  package: "ecommerce",
                  source: "not_a_source",
               },
            ],
         }),
      );
      expect(payload.sources).toEqual([]);
      expect(payload.total_available).toBe(0);
      expect(payload.returned).toBe(0);
      expect(payload.warnings).toBeUndefined();
   });

   it("an unsearchable target type is not reported as a curation gap", async () => {
      // `dimensional_value` selects no kinds, so the listing is empty for a
      // reason that has nothing to do with curation -- and the package here
      // exposes a source. Blaming `explores` / `export {}` contradicted the
      // unsupported-target warning printed beside it and sent an agent to
      // audit a healthy package.
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => mockPackage),
      });
      const payload = parse(
         await handler({
            search_targets: [
               { target_type: "dimensional_value", search_text: "California" },
            ],
            scopes: [{ environment: "malloy-samples", package: "ecommerce" }],
         }),
      );
      expect(payload.sources).toEqual([]);
      const warnings = (payload.warnings ?? []).join(" ");
      expect(warnings).toContain("No index for target_type dimensional_value");
      expect(warnings).not.toContain("curation gap");
   });

   it("a listing returns a dimension under its source", async () => {
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => mockPackage),
      });
      const results = rankedEntities(
         parse(
            await handler({
               search_targets: everyKind(),
               scopes: [
                  { environment: "malloy-samples", package: "ecommerce" },
               ],
            }),
         ),
      );
      expect(
         results.some(
            (r) => r.name === "state" && r.entity_type === "dimension",
         ),
      ).toBe(true);
   });

   it("lists a declared join, carrying its cardinality", async () => {
      // Joins were skipped by the collector, so an agent could not see that
      // the model declared one and concluded it had to bridge the tables
      // itself. The join's #(doc) was likewise unreachable.
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => mockPackage),
      });
      const results = rankedEntities(
         parse(
            await handler({
               search_targets: everyKind(),
               scopes: [
                  { environment: "malloy-samples", package: "ecommerce" },
               ],
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
            search_targets: [{ target_type: "source" }],
            scopes: [{ environment: "malloy-samples", package: "ecommerce" }],
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
               search_targets: everyKind(),
               scopes: [
                  { environment: "malloy-samples", package: "ecommerce" },
               ],
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

   it("keeps entity_id at three segments for a sourceless entity", () => {
      // The case the docstring calls out as already costly and no test
      // covered: a model-level named query declares no source, and a caller
      // splitting on ":" must still read [1] as the source.
      const id = entityId("query", undefined, "top_sellers");
      expect(id).toBe("query:top_sellers:top_sellers");
      expect(id.split(":")).toHaveLength(3);
      // A container is its own source, so a source self-references.
      expect(entityId("source", "orders", "orders")).toBe(
         "source:orders:orders",
      );
   });

   it("keeps entity_id at three segments when a name contains a colon", () => {
      // A backtick-quoted Malloy identifier may contain ":". Unescaped, the
      // id grew extra segments and a caller reading [1] as the source got a
      // fragment of the name instead.
      const id = entityId("dimension", "orders", "a:b");
      expect(id.split(":")).toHaveLength(3);
      expect(id.split(":").map(decodeURIComponent)).toEqual([
         "dimension",
         "orders",
         "a:b",
      ]);
      // Reversible, so "%" in a name round-trips too.
      expect(
         entityId("dimension", "orders", "100%:done")
            .split(":")
            .map(decodeURIComponent)[2],
      ).toBe("100%:done");
      // Ordinary names are untouched.
      expect(entityId("measure", "orders", "revenue")).toBe(
         "measure:orders:revenue",
      );
   });

   it("lists a joined field under its dotted path", async () => {
      // The path is what a query needs and what nothing else in the response
      // can supply: JoinInfo inlines the target's schema without naming the
      // target source, so an agent holding only the join entity cannot derive
      // `current_building.building_name` -- while the tool description tells
      // it to traverse a join exactly that way.
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => mockPackage),
      });
      const payload = parse(
         await handler({
            search_targets: everyKind(),
            scopes: [{ environment: "malloy-samples", package: "ecommerce" }],
         }),
      );
      const hit = rankedEntities(payload).find((r) =>
         r.name.includes("building_name"),
      );
      expect(hit).toBeDefined();
      // The dotted Malloy path, verbatim, not the bare field name.
      expect(hit!.name).toBe("current_building.building_name");
      expect(hit!.join_path).toBe("current_building");
      // The cardinality of the path that reaches it: `one` means aggregating
      // through it does not fan out.
      expect(hit!.relationship).toBe("one");
      // It nests under the source that DECLARES the traversal, which is the
      // card a caller would actually query from.
      expect(
         payload.sources.find(
            (c: { source_info: { resource_id: { source: string } } }) =>
               c.source_info.resource_id.source === "order_items",
         ),
      ).toBeDefined();
   });

   it("tier 3: a package listing still returns only sources, not joins", async () => {
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => mockPackage),
      });
      const payload = parse(
         await handler({
            search_targets: [{ target_type: "source" }],
            scopes: [{ environment: "malloy-samples", package: "ecommerce" }],
         }),
      );
      // A listing is sources and nothing else: no entity rows at all, so a
      // join can never appear as one.
      expect(sourceNames(payload)).toEqual(["order_items"]);
      expect(rankedEntities(payload)).toEqual([]);
   });

   it("delivers the parent source's doc and joins on the card a field is listed under", async () => {
      // A field hit used to arrive with only its own doc: the source's grain
      // and population rules reached the agent only when the source itself
      // happened to rank for the same query, so guidance placement depended
      // on query phrasing rather than on where the modeller wrote it.
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => mockPackage),
      });
      const payload = parse(
         await handler({
            search_targets: everyKind(),
            scopes: [{ environment: "malloy-samples", package: "ecommerce" }],
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

   it("reports each source once, however many of its entities are listed", async () => {
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => mockPackage),
      });
      const payload = parse(
         await handler({
            search_targets: everyKind(),
            scopes: [{ environment: "malloy-samples", package: "ecommerce" }],
         }),
      );
      expect(rankedEntities(payload).length).toBeGreaterThan(1);
      expect(sourceNames(payload)).toEqual(["order_items"]);
   });

   it("reports an empty joins list for a source that declares none", async () => {
      // The authoritative negative: without it an agent cannot tell "no joins
      // declared" from "joins exist but were not returned", and we watched
      // agents spend queries probing for a relationship that was never there.
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => mockLongDocPackage),
      });
      const payload = parse(
         await handler({
            search_targets: [{ target_type: "source" }],
            scopes: [{ environment: "e", package: "p" }],
         }),
      );
      expect(payload.sources[0].source_info.joins).toEqual([]);
   });

   it("returns sources as an empty array, never absent, when nothing matched", async () => {
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => mockPackage),
      });
      const payload = parse(
         await handler({
            search_targets: anyKind("Qwghlm"),
            scopes: [{ environment: "malloy-samples", package: "ecommerce" }],
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

   it("collapses a raw column into the alias declared as it", async () => {
      // `dimension: site is SITE` puts one physical column in the schema
      // twice, and both competed for the same scarce slots. The fold is
      // decided by the compiled expression -- `site` IS `SITE` -- not by the
      // two names looking alike, and the authored name is the survivor
      // because it is the one the modeller meant an agent to use.
      const handler = captureHandler({
         getEnvironment: async () =>
            ({ getPackage: async () => mockAliasPackage }) as never,
      });
      const results = rankedEntities(
         parse(
            await handler({
               search_targets: everyKind(),
               scopes: [{ environment: "e", package: "p" }],
            }),
         ),
      );
      const sites = results.filter((r) => r.name.toLowerCase() === "site");
      expect(sites).toHaveLength(1);
      expect(sites[0].name).toBe("site");
      expect(sites[0].description).toBe("Campus site the building sits on.");
      expect(sites[0].aliases).toEqual(["SITE"]);
   });

   it("does not fold a derivation that merely humanizes the same", async () => {
      // The false positive the old rule could produce and could not detect.
      // `site_code` humanizes to "site code" and `SITE_CODE` to the same, but
      // here the second is computed rather than declared as the first, so
      // there is no reference and nothing folds. Under name matching these
      // collapsed unless their docs happened to differ.
      const model = {
         getSourceInfos: () => [
            {
               name: "bldg",
               annotations: [],
               schema: {
                  fields: [
                     { kind: "dimension", name: "site_code", annotations: [] },
                     { kind: "dimension", name: "siteCode", annotations: [] },
                  ],
               },
            },
         ],
         getQueries: () => [],
         getModelDef: () => ({
            contents: {
               bldg: {
                  name: "bldg",
                  fields: [
                     { name: "site_code" },
                     { name: "siteCode", code: "upper(site_code)" },
                  ],
               },
            },
         }),
      };
      const handler = captureHandler({
         getEnvironment: async () =>
            envWith(async () => ({
               listModels: async () => [{ path: "b.malloy" }],
               getModel: () => model,
            })),
      });
      const names = rankedEntities(
         parse(
            await handler({
               search_targets: [{ target_type: "dimension" }],
               scopes: [{ environment: "e", package: "p", source: "bldg" }],
            }),
         ),
      ).map((r) => r.name);
      expect(names.sort()).toEqual(["siteCode", "site_code"]);
   });

   it("does not fold a reference that traverses a join", async () => {
      // `dimension: category is products.category` is a bare field reference
      // too, but it reaches a DIFFERENT column through a relationship that
      // may filter or fan out. Only a single-segment path names a sibling of
      // the same source. This is not hypothetical: order_items.category,
      // .brand and .region in the bundled storefront model are all exactly
      // this shape.
      const model = {
         getSourceInfos: () => [
            {
               name: "order_items",
               annotations: [],
               schema: {
                  fields: [
                     { kind: "dimension", name: "category", annotations: [] },
                  ],
               },
            },
            {
               name: "products",
               annotations: [],
               schema: {
                  fields: [
                     { kind: "dimension", name: "category", annotations: [] },
                  ],
               },
            },
         ],
         getQueries: () => [],
         getModelDef: () => ({
            contents: {
               order_items: {
                  name: "order_items",
                  fields: [
                     {
                        name: "category",
                        code: "products.category",
                        e: { node: "field", path: ["products", "category"] },
                     },
                  ],
               },
               products: {
                  name: "products",
                  fields: [{ name: "category" }],
               },
            },
         }),
      };
      const handler = captureHandler({
         getEnvironment: async () =>
            envWith(async () => ({
               listModels: async () => [{ path: "s.malloy" }],
               getModel: () => model,
            })),
      });
      const rows = rankedEntities(
         parse(
            await handler({
               search_targets: [{ target_type: "dimension" }],
               scopes: [{ environment: "e", package: "p" }],
            }),
         ),
      ).filter((r) => r.name === "category");
      expect(rows).toHaveLength(2);
      expect(rows.map((r) => r.source).sort()).toEqual([
         "order_items",
         "products",
      ]);
      expect(rows.every((r) => r.aliases === undefined)).toBe(true);
   });

   it("keeps two same-humanizing fields apart when neither declares the other", async () => {
      // `net_sales` and `netSales` humanize identically, so the old name-only
      // rule folded them unless their docs happened to differ -- it had no
      // way to see that one is a raw amount and the other a filtered measure.
      // Now the docs are beside the point: neither field's expression is a
      // reference to the other, so there is nothing to fold and the doc is
      // free to be whatever the modeller wrote.
      const handler = captureHandler({
         getEnvironment: async () =>
            ({ getPackage: async () => mockDistinctSpellingPackage }) as never,
      });
      // Listed via the drill-down, which enumerates every entity the source
      // still offers, so this asserts on what survived the collapse rather
      // than on what one query's tokens happened to match.
      const results = rankedEntities(
         parse(
            await handler({
               search_targets: everyKind(),
               scopes: [{ environment: "e", package: "p", source: "orders" }],
            }),
         ),
      );
      expect(results.map((r) => r.name).sort()).toEqual([
         "netSales",
         "net_sales",
      ]);
      expect(results.every((r) => r.aliases === undefined)).toBe(true);
      // Each keeps the doc that distinguishes it.
      expect(results.map((r) => r.description).sort()).toEqual([
         "Completed orders only, excludes trials.",
         "Gross sales less returns, all orders.",
      ]);
   });

   it("leaves genuinely distinct fields in a source alone", async () => {
      const handler = captureHandler({
         getEnvironment: async () =>
            ({ getPackage: async () => mockAliasPackage }) as never,
      });
      const results = rankedEntities(
         parse(
            await handler({
               search_targets: everyKind(),
               scopes: [{ environment: "e", package: "p" }],
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
                  listSkills: () => [],
               }),
            }) as never,
      });
      const results = rankedEntities(
         parse(
            await handler({
               search_targets: everyKind(),
               scopes: [{ environment: "e", package: "p" }],
            }),
         ),
      );
      expect(results.some((r) => r.name === "SITE")).toBe(false);
      expect(results[0].aliases).toBeUndefined();
   });

   it("tier 3: lists every source, not just the first 10", async () => {
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => mockManySourcePackage),
      });
      const payload = parse(
         await handler({
            search_targets: [{ target_type: "source" }],
            scopes: [{ environment: "e", package: "p" }],
         }),
      );
      expect(payload.sources).toHaveLength(12);
      expect(payload.total_available).toBe(12);
   });

   it("tier 3: honors an explicit limit when given", async () => {
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => mockManySourcePackage),
      });
      const payload = parse(
         await handler({
            search_targets: [{ target_type: "source" }],
            scopes: [{ environment: "e", package: "p" }],
            limit: 5,
         }),
      );
      expect(payload.sources).toHaveLength(5);
      // returned against total_available is how a caller sees it was capped.
      expect(payload.returned).toBe(5);
      expect(payload.total_available).toBe(12);
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
            search_targets: everyKind(),
            scopes: [
               {
                  environment: "malloy-samples",
                  package: "sales",
                  source: "orders",
               },
            ],
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
            search_targets: everyKind(),
            scopes: [
               {
                  environment: "malloy-samples",
                  package: "sales",
                  source: "customers",
               },
            ],
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
            search_targets: [{ target_type: "source" }],
            scopes: [{ environment: "malloy-samples", package: "sales" }],
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
            search_targets: everyKind(),
            scopes: [
               {
                  environment: "malloy-samples",
                  package: "sales",
                  source: "nope",
               },
            ],
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
            search_targets: everyKind(),
            scopes: [
               {
                  environment: "malloy-samples",
                  package: "sales",
                  source: "orders",
               },
            ],
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
            accessFilter: ["$ROLE = 'admin' or $TENANT = 'acme'"],
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

   it("reports a source's accessFilter gates and the givens they read", async () => {
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => gatedPackage),
      });
      const payload = parse(
         await handler({
            search_targets: [{ target_type: "source" }],
            scopes: [{ environment: "specs", package: "governed" }],
         }),
      );
      const gated = payload.sources.find(
         (c: SourceCardShape) =>
            c.source_info.resource_id.source === "orders_secured",
      );
      expect(gated.source_info.accessFilter).toEqual([
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

   it("omits accessFilter on an ungated source rather than sending it empty", async () => {
      // Absence is the contract on both sides, and an empty array would read
      // as "a gate with no expressions" to anything checking length.
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => gatedPackage),
      });
      const payload = parse(
         await handler({
            search_targets: [{ target_type: "source" }],
            scopes: [{ environment: "specs", package: "governed" }],
         }),
      );
      const open = payload.sources.find(
         (c: SourceCardShape) => c.source_info.resource_id.source === "sales",
      );
      expect("accessFilter" in open.source_info).toBe(false);
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
            search_targets: [{ target_type: "source" }],
            scopes: [{ environment: "malloy-samples", package: "ecommerce" }],
         }),
      );
      expect("givens" in payload.sources[0].source_info).toBe(false);
      expect("accessFilter" in payload.sources[0].source_info).toBe(false);
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
            search_targets: everyKind(),
            scopes: [
               { environment: "specs", package: "typed", source: "orders" },
            ],
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
         await handler({
            search_targets: [{ target_type: "source" }],
            scopes: [{ environment: "specs", package: "wordy" }],
         }),
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
         await handler({
            search_targets: [{ target_type: "source" }],
            scopes: [{ environment: "specs", package: "sales" }],
         }),
      );
      const customers = payload.sources.find(
         (c: SourceCardShape) =>
            c.source_info.resource_id.source === "customers",
      );
      expect("one_line_summary" in customers.source_info).toBe(false);
   });

   it("reports a source's filters and its locks, each under its own field", async () => {
      const model = {
         getSourceInfos: () => [
            {
               name: "finance_only",
               annotations: [],
               schema: { fields: [] },
            },
         ],
         getQueries: () => [],
         getSources: () => [
            {
               name: "finance_only",
               givens: [{ name: "ROLE", type: "string[]" }],
               accessFilter: ["region = $REGION"],
               authorize: ["'finance' in $ROLE"],
            },
         ],
      };
      const handler = captureHandler({
         getEnvironment: async () =>
            envWith(async () => ({
               listModels: async () => [{ path: "finance.malloy" }],
               getModel: () => model,
            })),
      });
      const payload = parse(
         await handler({
            search_targets: [{ target_type: "source" }],
            scopes: [{ environment: "specs", package: "finance" }],
         }),
      );
      const card = payload.sources[0];
      expect(card.source_info.accessFilter).toEqual([
         { expression: "region = $REGION", given_names: ["REGION"] },
      ]);
      expect(card.source_info.authorize).toEqual([
         { expression: "'finance' in $ROLE", given_names: ["ROLE"] },
      ]);
   });
});

/**
 * A source carrying an unconditional `false` on either route needs no
 * caller-supplied given to know nobody is admitted — that is
 * decidable without trusting anything the caller sent, unlike a real rule
 * (execute_query_tool.ts's `givens` are untrusted MCP-path input). So this
 * source is dropped from the results entirely rather than listed with a gate
 * an agent can only learn is unsatisfiable from a 403.
 */
describe("get_context accessFilter deny-all drop", () => {
   function packageWithSource(apiSource: {
      name: string;
      accessFilter?: string[];
      authorize?: string[];
   }) {
      const model = {
         getSourceInfos: () => [
            { name: apiSource.name, annotations: [], schema: { fields: [] } },
         ],
         getQueries: () => [],
         getSources: () => [apiSource],
      };
      return {
         listModels: async () => [{ path: "m.malloy" }],
         getModel: () => model,
      };
   }

   async function sourcesFor(apiSource: {
      name: string;
      accessFilter?: string[];
      authorize?: string[];
   }) {
      const handler = captureHandler({
         getEnvironment: async () =>
            envWith(async () => packageWithSource(apiSource)),
      });
      const payload = parse(
         await handler({
            search_targets: [{ target_type: "source" }],
            scopes: [{ environment: "specs", package: "deny-drop" }],
         }),
      );
      return payload.sources as SourceCardShape[];
   }

   it("drops a source gated by an unconditional `#(access_filter) false`", async () => {
      const sources = await sourcesFor({
         name: "locked",
         accessFilter: ["false"],
      });
      expect(sources).toEqual([]);
   });

   it("drops a source gated by an unconditional `#(authorize) false`", async () => {
      const sources = await sourcesFor({
         name: "locked",
         authorize: ["false"],
      });
      expect(sources).toEqual([]);
   });

   it("drops the case variant `#(authorize) FALSE` too — the wire can carry it uppercase", async () => {
      // The grammar parser lowercases only for its OWN comparison
      // (authorize_grammar.ts); the effective text it reports back can still
      // be the author's original casing/whitespace.
      const sources = await sourcesFor({
         name: "locked",
         authorize: [" FALSE "],
      });
      expect(sources).toEqual([]);
   });

   it("does NOT drop a source gated by a real (non-deny) rule", async () => {
      const sources = await sourcesFor({
         name: "gated",
         accessFilter: ["org_id in $GROUPS"],
      });
      expect(sources).toHaveLength(1);
      expect(sources[0].source_info.resource_id.source).toBe("gated");
      expect(sources[0].source_info.accessFilter).toEqual([
         { expression: "org_id in $GROUPS", given_names: ["GROUPS"] },
      ]);
   });

   it("does not drop an ungated source", async () => {
      const sources = await sourcesFor({ name: "open" });
      expect(sources).toHaveLength(1);
      expect("accessFilter" in sources[0].source_info).toBe(false);
   });

   // `isUnconditionalDenyAuthorize` compares against `"false"` only — `true`
   // is a real (if unconditional) gate, not a deny, so it is reported like
   // any other rule rather than dropped.
   it("does NOT drop a source gated by `#(authorize) true` — reports it like an ordinary gate", async () => {
      const sources = await sourcesFor({
         name: "reopened",
         authorize: ["true"],
      });
      expect(sources).toHaveLength(1);
      expect(sources[0].source_info.resource_id.source).toBe("reopened");
      expect(sources[0].source_info.authorize).toEqual([
         { expression: "true", given_names: [] },
      ]);
   });

   // Two models can expose a same-named source with different gates —
   // `droppedSources` must be keyed by (modelPath, sourceName), not the bare
   // name, or a deny-all in one model blacks out the open model's card too
   // (cardFor/governance/bySource collapse same-named sources into one card,
   // first-model-wins, but that is a SEPARATE, pre-existing identity rule —
   // the drop itself must not leak across models).
   it("a deny-all source in one model does not black out a same-named open source in another model", async () => {
      const lockedModel = {
         getSourceInfos: () => [
            { name: "shared", annotations: [], schema: { fields: [] } },
         ],
         getQueries: () => [],
         getSources: () => [{ name: "shared", accessFilter: ["false"] }],
      };
      const openModel = {
         getSourceInfos: () => [
            { name: "shared", annotations: [], schema: { fields: [] } },
         ],
         getQueries: () => [],
         getSources: () => [{ name: "shared" }],
      };
      const pkg = {
         // Sorted by path in collectEntities, so "a_locked" is processed
         // before "b_open" — the deny-all model comes first on purpose, to
         // pin that a later open occurrence is not blacked out by an
         // earlier drop.
         listModels: async () => [
            { path: "a_locked.malloy" },
            { path: "b_open.malloy" },
         ],
         getModel: (path: string) =>
            path === "a_locked.malloy" ? lockedModel : openModel,
      };
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => pkg),
      });
      const payload = parse(
         await handler({
            search_targets: [{ target_type: "source" }],
            scopes: [{ environment: "specs", package: "deny-drop" }],
         }),
      );
      const sources = payload.sources as SourceCardShape[];
      expect(sources).toHaveLength(1);
      expect(sources[0].source_info.resource_id.source).toBe("shared");
      expect(
         (sources[0].source_info.resource_id as { model_path?: string })
            .model_path,
      ).toBe("b_open.malloy");
      expect("accessFilter" in sources[0].source_info).toBe(false);
   });

   // A named query over a dropped source (`query: q is locked -> {...}`) is a
   // second route to the same name and shape: KINDS_BY_TARGET.view includes
   // "query", so a bare {target_type: "view"} enumeration would otherwise
   // resurrect a bare card via cardFor even though the source card itself was
   // dropped from the sourceInfos loop above.
   function packageWithQueriedSource(
      apiSource: {
         name: string;
         accessFilter?: string[];
         authorize?: string[];
      },
      query: { name: string; sourceName: string },
   ) {
      const model = {
         getSourceInfos: () => [
            { name: apiSource.name, annotations: [], schema: { fields: [] } },
         ],
         getQueries: () => [{ ...query, annotations: [] }],
         getSources: () => [apiSource],
      };
      return {
         listModels: async () => [{ path: "m.malloy" }],
         getModel: () => model,
      };
   }

   async function viewSourcesFor(
      apiSource: {
         name: string;
         accessFilter?: string[];
         authorize?: string[];
      },
      query: { name: string; sourceName: string },
   ) {
      const handler = captureHandler({
         getEnvironment: async () =>
            envWith(async () => packageWithQueriedSource(apiSource, query)),
      });
      const payload = parse(
         await handler({
            search_targets: [{ target_type: "view" }],
            scopes: [{ environment: "specs", package: "deny-drop" }],
         }),
      );
      return payload.sources as SourceCardShape[];
   }

   it("drops neither a query entity nor a source card for a query over an `#(access_filter) false` source", async () => {
      const sources = await viewSourcesFor(
         { name: "locked", accessFilter: ["false"] },
         { name: "q", sourceName: "locked" },
      );
      expect(sources).toEqual([]);
   });

   it("drops neither a query entity nor a source card for a query over an `#(authorize) false` source", async () => {
      const sources = await viewSourcesFor(
         { name: "locked", authorize: ["false"] },
         { name: "q", sourceName: "locked" },
      );
      expect(sources).toEqual([]);
   });

   it("still surfaces a query entity and its source card for a query over a non-deny gated source", async () => {
      const sources = await viewSourcesFor(
         { name: "gated", accessFilter: ["org_id in $GROUPS"] },
         { name: "q", sourceName: "gated" },
      );
      expect(sources).toHaveLength(1);
      expect(sources[0].source_info.resource_id.source).toBe("gated");
      expect(sources[0].entities?.map((e) => e.entity_type)).toContain("query");
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
            search_targets: everyKind(),
            scopes: [
               { environment: "specs", package: "wide", source: "orders" },
            ],
            limit: 4,
         }),
      );
      expect(payload.sources[0].entities).toHaveLength(4);
      expect(payload.warnings.join(" ")).toContain("Returned 4 of 12");
      expect(payload.warnings.join(" ")).toContain("Raise limit");
   });

   it("stays silent when nothing was cut", async () => {
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => widePackage),
      });
      const payload = parse(
         await handler({
            search_targets: everyKind(),
            scopes: [
               { environment: "specs", package: "wide", source: "orders" },
            ],
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

   it("reports a source under every model that resolves it", async () => {
      // A source is queryable at every model path that resolves it -- its own
      // file and every file importing it -- so each pairing is its own card
      // with its own resource_id, and a caller can query the one it prefers.
      //
      // This deliberately reverses an earlier decision to keep only the first
      // model. That rule made the OTHER valid paths unreachable, and picked
      // its survivor before `collectSourceInfos` existed, when a model also
      // claimed sources it could not resolve -- so "first sorted model" could
      // name a file the source did not compile in. With the namespace fixed,
      // every path here is a path that compiles, and returning them all
      // matches the comparable retrieval service, which keys its result set
      // the same way.
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
            await handler({
               search_targets: [{ target_type: "source" }],
               scopes: [{ environment: "specs", package: "dup" }],
            }),
         );
         return payload.sources.map(
            (s: { source_info: { resource_id: { model_path: string } } }) =>
               s.source_info.resource_id.model_path,
         );
      };
      expect(await pathsFor(["a.malloy", "b.malloy"])).toEqual([
         "a.malloy",
         "b.malloy",
      ]);
      // Reversed listing, same answer IN THE SAME ORDER. Sorting this side
      // too would have asserted only that the same set comes back, which is
      // true with no sort at all.
      expect(await pathsFor(["b.malloy", "a.malloy"])).toEqual([
         "a.malloy",
         "b.malloy",
      ]);
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
            search_targets: everyKind(),
            scopes: [
               {
                  environment: "specs",
                  package: "nested",
                  source: "order_items",
               },
            ],
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
            search_targets: everyKind(),
            scopes: [{ environment: "specs", package: "u", source: "orders" }],
         }),
      );
      expect(
         payload.sources[0].entities.map((e: { name: string }) => e.name),
      ).toEqual(["_internal_key"]);
   });

   it("states a source's doc once, not once per listed entity", async () => {
      const handler = captureHandler({
         getEnvironment: async () => envWith(async () => mockPackage),
      });
      const result = await handler({
         search_targets: everyKind(),
         scopes: [{ environment: "malloy-samples", package: "ecommerce" }],
      });
      const text = JSON.stringify(parse(result).sources);
      const doc = "One row per product sold on an order.";
      // Once as `docs`, once as the one-line summary derived from it, and
      // never repeated onto the entities that nest under the card.
      expect(text.split(doc).length - 1).toBe(2);
   });
});

/**
 * Same-named fields in sibling sources, on the LEXICAL path, which is what an
 * unconfigured server runs.
 *
 * A model whose sources extend a common base repeats every inherited field
 * verbatim -- same name, same inherited #(doc), same score. These used to
 * fold into one row, and the identical doc was read as evidence they were one
 * number. They are not: `sales_secured` is `sales` behind an access gate, and
 * the difference is stated on the SOURCE, which a field-doc comparison never
 * looks at. Both are returned, on either ranker.
 */
describe("get_context same-named fields in sibling sources", () => {
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

   it("returns both when two sources inherited the field unchanged", async () => {
      const doc = "Total order amount.";
      const payload = parse(
         await handlerFor(siblingModel(doc, doc))({
            search_targets: everyKind(),
            scopes: [{ environment: "specs", package: "sib" }],
         }),
      );
      const rows = rankedEntities(payload).filter(
         (e) => e.name === "total_amount",
      );
      expect(rows).toHaveLength(2);
      expect(rows.map((r) => r.source).sort()).toEqual([
         "sales",
         "sales_secured",
      ]);
   });

   it("returns both when their docs say different things", async () => {
      // Different docs are different guidance, and the agent chooses between
      // them by reading both.
      const payload = parse(
         await handlerFor(
            siblingModel(
               "Total order amount.",
               "Total order amount visible to this caller, after the access gate.",
            ),
         )({
            search_targets: everyKind(),
            scopes: [{ environment: "specs", package: "sib" }],
         }),
      );
      const rows = rankedEntities(payload).filter(
         (e) => e.name === "total_amount",
      );
      expect(rows).toHaveLength(2);
      expect(rows.map((r) => r.description).sort()).toEqual([
         "Total order amount visible to this caller, after the access gate.",
         "Total order amount.",
      ]);
   });

   it("narrows to the scoped source under a drill-down", async () => {
      const doc = "Total order amount.";
      const payload = parse(
         await handlerFor(siblingModel(doc, doc))({
            search_targets: everyKind(),
            scopes: [{ environment: "specs", package: "sib", source: "sales" }],
         }),
      );
      expect(sourceNames(payload)).toEqual(["sales"]);
   });
});

describe("get_context source documentation", () => {
   // `narrowed` is `base extend { where: ... }` -- a filtered source, so it is
   // a different population and the parent's description does not describe
   // it. Malloy's stable SourceInfo flattens the two into one array with the
   // INHERITED note first, which is the shape reproduced here.
   const OWN = "#(doc) Only completed lines.";
   const PARENT = "#(doc) Every order line.";
   const model = {
      getSourceInfos: () => [
         {
            name: "base",
            annotations: [PARENT],
            schema: {
               fields: [{ kind: "measure", name: "total", annotations: [] }],
            },
         },
         {
            // Flattened: parent first, then its own.
            name: "narrowed",
            annotations: [PARENT, OWN],
            schema: {
               fields: [{ kind: "measure", name: "total", annotations: [] }],
            },
         },
      ],
      getQueries: () => [],
      getModelDef: () => ({
         contents: {
            base: {
               name: "base",
               annotations: { blockNotes: [{ text: PARENT }] },
               fields: [{ name: "total" }],
            },
            narrowed: {
               name: "narrowed",
               annotations: {
                  blockNotes: [{ text: OWN }],
                  inherits: { blockNotes: [{ text: PARENT }] },
               },
               fields: [{ name: "total" }],
            },
         },
      }),
   };
   const handlerFor = (m: unknown) =>
      captureHandler({
         getEnvironment: async () =>
            envWith(async () => ({
               listModels: async () => [{ path: "m.malloy" }],
               getModel: () => m,
            })),
      });
   const cardsOf = (payload: {
      sources: Array<{
         source_info: {
            resource_id: { source: string };
            docs?: string;
            one_line_summary?: string;
         };
      }>;
   }) =>
      new Map(
         payload.sources.map((c) => [c.source_info.resource_id.source, c]),
      );

   it("reports an extended source's OWN doc, not the one it inherited", async () => {
      const cards = cardsOf(
         parse(
            await handlerFor(model)({
               search_targets: [{ target_type: "source" }],
               scopes: [{ environment: "e", package: "p" }],
            }),
         ),
      );
      expect(cards.get("base")?.source_info.docs).toBe("Every order line.");
      // The whole point: a filtered source does not borrow its parent's
      // description, and its summary is its own rather than a copy.
      expect(cards.get("narrowed")?.source_info.docs).toBe(
         "Only completed lines.",
      );
      expect(cards.get("narrowed")?.source_info.one_line_summary).not.toBe(
         cards.get("base")?.source_info.one_line_summary,
      );
   });

   it("falls back to the flattened annotations with no compiled model", async () => {
      // A spec stand-in or a model that failed to compile has no IR to read.
      // That must keep the previous behaviour rather than silently drop the
      // doc entirely.
      const { getModelDef: _omitted, ...withoutIr } = model;
      const cards = cardsOf(
         parse(
            await handlerFor(withoutIr)({
               search_targets: [{ target_type: "source" }],
               scopes: [{ environment: "e", package: "p" }],
            }),
         ),
      );
      expect(cards.get("narrowed")?.source_info.docs).toContain(
         "Only completed lines.",
      );
   });
});

describe("get_context include_code", () => {
   // A measure with an expression and a raw column without one, so the flag's
   // two outcomes are both observable: code present, and code meaningfully
   // ABSENT because the field is physical rather than derived.
   const model = {
      getSourceInfos: () => [
         {
            name: "orders",
            annotations: [],
            schema: {
               fields: [
                  { kind: "measure", name: "revenue", annotations: [] },
                  { kind: "dimension", name: "order_id", annotations: [] },
               ],
            },
         },
      ],
      getQueries: () => [],
      getModelDef: () => ({
         contents: {
            orders: {
               name: "orders",
               fields: [
                  { name: "revenue", code: "sale_price.sum()" },
                  { name: "order_id" },
               ],
            },
         },
      }),
   };
   const handler = () =>
      captureHandler({
         getEnvironment: async () =>
            envWith(async () => ({
               listModels: async () => [{ path: "o.malloy" }],
               getModel: () => model,
            })),
      });
   const request = (extra: Record<string, unknown>) => ({
      search_targets: [
         { target_type: "measure" },
         { target_type: "dimension" },
      ],
      scopes: [{ environment: "e", package: "p", source: "orders" }],
      ...extra,
   });

   it("omits code unless the caller asks", async () => {
      // Off by default: an expression is long, and a source's #(doc) is meant
      // to carry the meaning. A caller that wants the mechanics opts in.
      const rows = rankedEntities(parse(await handler()(request({}))));
      expect(rows.length).toBeGreaterThan(0);
      expect(rows.every((r) => r.code === undefined)).toBe(true);
   });

   it("returns each field's expression when include_code is set", async () => {
      const rows = rankedEntities(
         parse(await handler()(request({ include_code: true }))),
      );
      const revenue = rows.find((r) => r.name === "revenue");
      expect(revenue?.code).toBe("sale_price.sum()");
      // A physical column has no expression, so absence under the flag is the
      // fact that the field is raw -- not a gap in the response.
      const orderId = rows.find((r) => r.name === "order_id");
      expect(orderId).toBeDefined();
      expect(orderId?.code).toBeUndefined();
   });
});

describe("get_context converged request shape", () => {
   // A package with two sources so a target's kind filter is observable:
   // `orders` carries a measure and a dimension, `customers` only a dimension.
   const convergedModel = {
      getSourceInfos: () => [
         {
            name: "orders",
            annotations: ["#(doc) One row per order."],
            schema: {
               fields: [
                  {
                     kind: "measure",
                     name: "revenue",
                     annotations: ["#(doc) Total order revenue."],
                  },
                  {
                     kind: "dimension",
                     name: "category",
                     annotations: ["#(doc) Product category of the order."],
                  },
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
                     name: "category",
                     annotations: ["#(doc) Customer segment category."],
                  },
               ],
            },
         },
      ],
      getQueries: () => [],
   };
   const handler = () =>
      captureConverged({
         getEnvironment: async () =>
            envWith(async () => ({
               listModels: async () => [{ path: "m.malloy" }],
               getModel: () => convergedModel,
            })),
      });
   const scope = [{ environment: "specs", package: "shop" }];

   it("answers a multi-target question in ONE call, grouped by source", async () => {
      const payload = parse(
         await handler()({
            search_targets: [
               { target_type: "measure" },
               { target_type: "dimension" },
            ],
            scopes: scope,
         }),
      );
      // No source scope was given and none was needed: the response says which
      // sources hold these fields. That is the whole point of the shape.
      const names = rankedEntities(payload)
         .map((e) => `${e.source}.${e.name}`)
         .sort();
      expect(names).toContain("orders.revenue");
      expect(names).toContain("orders.category");
      expect(payload.ranking).toBe("prominence");
   });

   it("a target only claims the kinds it selects", async () => {
      const payload = parse(
         await handler()({
            search_targets: [{ target_type: "measure" }],
            scopes: scope,
         }),
      );
      // "order" matches the measure `revenue` ("Total order revenue.") AND
      // the dimension `category` ("Product category of the order.") by text,
      // but a measure target must surface only the measure: that filter is
      // what target_type buys over a single untyped query string. And it
      // must still return SOMETHING: `.every()` is vacuously true on an empty
      // list, so without the length check a filter that drops everything
      // would pass here. (The earlier text, "category", matched no measure
      // at all, so this test was passing on exactly that vacuity.)
      expect(rankedEntities(payload).length).toBeGreaterThan(0);
      expect(
         rankedEntities(payload).every((e) => e.entity_type === "measure"),
      ).toBe(true);
   });

   it("excludes a named query over an inline source, which has no card to live in", async () => {
      // `query: q is orders extend {...} -> ...` compiles with no source
      // name. The response nests every entity under a source card, so such a
      // query could match a view target, spend a slot in the per-source
      // window and count in total_available while never appearing in
      // `returned`. It is excluded before it can be counted anywhere.
      const payload = parse(
         await captureConverged({
            getEnvironment: async () =>
               envWith(async () => ({
                  listModels: async () => [{ path: "m.malloy" }],
                  getModel: () => ({
                     ...convergedModel,
                     getQueries: () => [
                        {
                           name: "revenue_by_category",
                           sourceName: undefined,
                           annotations: ["#(doc) Revenue by category."],
                        },
                        {
                           name: "revenue_rollup",
                           sourceName: "orders",
                           annotations: ["#(doc) Revenue rolled up."],
                        },
                     ],
                  }),
               })),
         })({
            search_targets: [{ target_type: "view" }],
            scopes: scope,
         }),
      );
      const names = rankedEntities(payload).map((e) => e.name);
      expect(names).toContain("revenue_rollup");
      expect(names).not.toContain("revenue_by_category");
      expect(payload.total_available).toBe(payload.returned);
   });

   it("enumerates instead of ranking when no target carries text", async () => {
      const payload = parse(
         await handler()({
            search_targets: [{ target_type: "source" }],
            scopes: scope,
         }),
      );
      expect(payload.ranking).toBe("prominence");
      expect(
         payload.sources
            .map(
               (c: { source_info: { resource_id: { source: string } } }) =>
                  c.source_info.resource_id.source,
            )
            .sort(),
      ).toEqual(["customers", "orders"]);
   });

   it("narrows to one source when the scope names it", async () => {
      const payload = parse(
         await handler()({
            search_targets: [
               { target_type: "dimension", search_text: "category" },
            ],
            scopes: [
               { environment: "specs", package: "shop", source: "customers" },
            ],
         }),
      );
      expect(
         rankedEntities(payload).every((e) => e.source === "customers"),
      ).toBe(true);
   });

   it("says so, with the remedy, for a target type it cannot search", async () => {
      const payload = parse(
         await handler()({
            search_targets: [
               { target_type: "dimensional_value", search_text: "premium" },
            ],
            scopes: scope,
         }),
      );
      // Publisher indexes the model, not the values in it. Silence would read
      // as "no such value"; this says which it is and what to do instead.
      const warning = (payload.warnings as string[]).join(" ");
      expect(warning).toContain("dimensional_value");
      expect(warning).toContain("execute_query");
   });
});

// DEPRECATED FEATURE COVERAGE — delete together with
// `#(filter)` (see RELEASE_NOTES deprecations).
describe("deprecated: #(filter) — get_context filter_params", () => {
   it("reports a source's #(filter) declarations, required ones included", async () => {
      // A required filter the caller cannot see is a query that fails the
      // moment they use the source. Publisher enforces #(filter) server-side
      // and exposes it over REST as filterParams; withholding it from the card
      // made the card unusable without a second, undocumented lookup.
      const model = {
         getSourceInfos: () => [
            { name: "sales", annotations: [], schema: { fields: [] } },
         ],
         getSources: () => [
            {
               name: "sales",
               filters: [
                  { name: "region", type: "in", dimension: "region_code" },
                  { name: "amount", type: "greater_than", required: true },
               ],
            },
         ],
         getQueries: () => [],
      };
      const handler = captureConverged({
         getEnvironment: async () =>
            envWith(async () => ({
               listModels: async () => [{ path: "s.malloy" }],
               getModel: () => model,
            })),
      });
      const payload = parse(
         await handler({
            search_targets: [{ target_type: "source" }],
            scopes: [{ environment: "specs", package: "shop" }],
         }),
      );
      const card = payload.sources[0];
      expect(card.source_info.filter_params).toEqual([
         { name: "region", type: "in", dimension: "region_code" },
         { name: "amount", type: "greater_than", required: true },
      ]);
   });

   it("omits filter_params entirely when the source declares none", async () => {
      // Absent, not empty: the published shape omits null fields, so a caller
      // reads absence as "declares none" on both surfaces.
      const model = {
         getSourceInfos: () => [
            { name: "plain", annotations: [], schema: { fields: [] } },
         ],
         getSources: () => [{ name: "plain" }],
         getQueries: () => [],
      };
      const handler = captureConverged({
         getEnvironment: async () =>
            envWith(async () => ({
               listModels: async () => [{ path: "s.malloy" }],
               getModel: () => model,
            })),
      });
      const payload = parse(
         await handler({
            search_targets: [{ target_type: "source" }],
            scopes: [{ environment: "specs", package: "shop" }],
         }),
      );
      expect(payload.sources[0].source_info).not.toHaveProperty(
         "filter_params",
      );
   });
});

describe("get_context catalog browse", () => {
   const documented = (n: number) => ({
      name: `src_${n}`,
      annotations: [
         `#(doc) Source number ${n}. A long paragraph of grain and population rules that a browse does not need in order to choose between cards.`,
      ],
      schema: { fields: [] },
   });
   const model = {
      getSourceInfos: () => [1, 2, 3, 4, 5].map(documented),
      getQueries: () => [],
   };
   const handler = () =>
      captureConverged({
         getEnvironment: async () =>
            envWith(async () => ({
               listModels: async () => [{ path: "s.malloy" }],
               getModel: () => model,
            })),
      });
   const scopes = [{ environment: "specs", package: "many" }];
   const browse = (extra: Record<string, unknown> = {}) => ({
      search_targets: [{ target_type: "source" }],
      scopes,
      ...extra,
   });
   const sourceNames = (payload: {
      sources: Array<{ source_info: { resource_id: { source: string } } }>;
   }) => payload.sources.map((c) => c.source_info.resource_id.source);

   it("keeps each source's full doc on a browse", async () => {
      // A deliberate divergence from the hosted thin catalog card, which drops
      // `docs` because its browse spans a whole workspace and can run to 150
      // sources. This one cannot: `scopes` requires a package, so a browse is
      // one package's sources -- a handful, whose docs carry the grain and
      // population rules an agent needs to pick between them. Withholding
      // there would cost the caveat and save almost nothing.
      const payload = parse(await handler()(browse()));
      const card = payload.sources[0].source_info;
      expect(card.docs).toContain("grain and population rules");
      expect(card.one_line_summary).toContain("Source number 1");
      // The joins list stays complete, so "empty means none declared" holds
      // on a browse as it does everywhere else.
      expect(card.joins).toEqual([]);
   });

   it("names paging, not the limit, as the remedy on a browse", async () => {
      // A listing has a resumable order, so the rest is one offset away.
      // Telling a caller to raise the limit sends them at the one control
      // that cannot reach source 151.
      const payload = parse(await handler()(browse({ limit: 2 })));
      const warning = payload.warnings.join(" ");
      expect(warning).toContain("Returned 2 of 5 sources in scope");
      expect(warning).toContain("next_offset");
      expect(warning).not.toContain("Raise limit");
   });

   it("reports the same source count in the envelope and the warning", async () => {
      // These are the same fact in the same unit, and they drifted: the
      // warning counted matched sources while `returned` counted CARDS, which
      // differ whenever a folded sibling's card rides along with the row that
      // names it.
      const payload = parse(await handler()(browse({ limit: 2 })));
      expect(payload.returned).toBe(payload.sources.length);
      expect(payload.warnings.join(" ")).toContain(
         `Returned ${payload.returned} of`,
      );
   });

   it("pages with offset and says where the next page starts", async () => {
      const first = parse(await handler()(browse({ limit: 2 })));
      expect(sourceNames(first)).toEqual(["src_1", "src_2"]);
      expect(first.next_offset).toBe(2);
      expect(first.total_available).toBe(5);

      const second = parse(
         await handler()(browse({ limit: 2, offset: first.next_offset })),
      );
      expect(sourceNames(second)).toEqual(["src_3", "src_4"]);
      expect(second.next_offset).toBe(4);

      const last = parse(await handler()(browse({ limit: 2, offset: 4 })));
      expect(sourceNames(last)).toEqual(["src_5"]);
      // Absent on the final page: present would promise a page that is not
      // there, and a caller walking next_offset would loop.
      expect(last).not.toHaveProperty("next_offset");
   });

   it("keeps the full doc when the caller scopes to one source", async () => {
      const payload = parse(
         await handler()({
            search_targets: [{ target_type: "source" }],
            scopes: [
               { environment: "specs", package: "many", source: "src_2" },
            ],
         }),
      );
      expect(payload.sources[0].source_info.docs).toContain(
         "grain and population rules",
      );
   });

   it("refuses an offset on a response with no resumable order", async () => {
      const payload = parse(
         await handler()({
            search_targets: [{ target_type: "measure" }],
            scopes,
            offset: 10,
         }),
      );
      // Silently dropping it would hand back page 1 to a caller who believed
      // they were reading page 2.
      expect(JSON.stringify(payload)).toContain("Invalid offset");
   });
});

describe("list_packages", () => {
   function listHandler(store: Partial<EnvironmentStore>): Handler {
      const handlers = new Map<string, Handler>();
      const fakeServer = {
         tool: (name: string, _d: string, _s: unknown, h: Handler) => {
            handlers.set(name, h);
         },
      };
      registerListPackagesTool(fakeServer as never, store as EnvironmentStore);
      const handler = handlers.get("list_packages");
      if (!handler) throw new Error("not registered");
      return handler;
   }
   const storeWith = (env: unknown): Partial<EnvironmentStore> => ({
      listEnvironments: async () => [{ name: "specs" }] as never,
      getEnvironment: async () => env as never,
   });

   it("lists each environment with its packages", async () => {
      const handler = listHandler(
         storeWith({
            listPackages: async () => [
               { name: "ecommerce", description: "Ecommerce demo" },
            ],
            getFailedPackages: () => new Map(),
            getStaleCompileErrors: () => new Map(),
         }),
      );
      const { environments } = parse(await handler({}));
      expect(environments).toEqual([
         {
            name: "specs",
            packages: [{ name: "ecommerce", description: "Ecommerce demo" }],
         },
      ]);
   });

   it("lists a failed package with its load error instead of omitting it", async () => {
      // listPackages() drops packages that failed to load, which reads as
      // "does not exist" to an agent. This is the only surface that draws the
      // distinction, so a broken package must arrive with its reason.
      const handler = listHandler(
         storeWith({
            listPackages: async () => [{ name: "good" }],
            getFailedPackages: () =>
               new Map([["broken", "Compile failed: unexpected token"]]),
            getStaleCompileErrors: () => new Map(),
         }),
      );
      const { environments } = parse(await handler({}));
      const packages = environments[0].packages;
      expect(packages).toHaveLength(2);
      expect(packages[0]).toMatchObject({ name: "good" });
      expect(packages[1]).toEqual({
         name: "broken",
         error: "Compile failed: unexpected token",
      });
      // No stale marker: this package is not serving anything at all, which is
      // the distinction the marker exists to draw.
      expect("stale" in packages[1]).toBe(false);
   });

   it("marks a stale package, which listPackages reports as healthy", async () => {
      // A failed reload keeps the package SERVING, so it comes back from
      // listPackages looking exactly like a current one. Unmarked, an agent
      // queries it and gets numbers from the model compiled before the last
      // save, with nothing to say so.
      const handler = listHandler(
         storeWith({
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
         }),
      );
      const { environments } = parse(await handler({}));
      expect(environments[0].packages).toEqual([
         { name: "current" },
         { name: "stale-pkg", error: "line 3: missing ')'", stale: true },
      ]);
   });

   it("keeps listing when one environment cannot be read, and says why", async () => {
      // The point of this tool is to say what IS there, so one unreachable
      // environment must not take the catalog down with it. But surviving the
      // failure is only half of it: an empty `packages` is also what a
      // genuinely empty environment looks like, so the reason has to come
      // back with it.
      const handler = listHandler({
         listEnvironments: async () => [{ name: "broken" }] as never,
         getEnvironment: async () => {
            throw new Error("Environment 'broken' could not be resolved");
         },
      });
      const { environments } = parse(await handler({}));
      expect(environments).toEqual([
         {
            name: "broken",
            error: "Environment 'broken' could not be resolved",
            packages: [],
         },
      ]);
   });

   it("leaves a genuinely empty environment without an error", async () => {
      // The other half of the pair above: `error` is what separates "the
      // listing failed" from "there is nothing here", so an environment that
      // really holds no packages must not carry one.
      const handler = listHandler(
         storeWith({
            listPackages: async () => [],
            getFailedPackages: () => new Map(),
            getStaleCompileErrors: () => new Map(),
         }),
      );
      const { environments } = parse(await handler({}));
      expect(environments).toEqual([{ name: "specs", packages: [] }]);
   });

   it("reports a non-Error thrown while listing one environment's packages", async () => {
      const handler = listHandler({
         listEnvironments: async () => [{ name: "broken" }] as never,
         getEnvironment: async () => {
            throw "the store exploded";
         },
      });
      const { environments } = parse(await handler({}));
      expect(environments[0].error).toBe("the store exploded");
   });

   it("reports a non-Error throwable without inventing 'Unknown error'", async () => {
      // The old per-site `error instanceof Error ? error.message : "Unknown
      // error"` turned a thrown string into exactly the unhelpful text this
      // tool's callers reported. classifyToolError stringifies it instead.
      const handler = listHandler({
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

   it("surfaces a total failure as a tool error, with suggestions", async () => {
      const handler = listHandler({
         listEnvironments: async () => {
            throw new Error("storage is unavailable");
         },
      });
      const result = await handler({});
      expect(result.isError).toBe(true);
      const parsed = parse(result);
      expect(parsed.error).toContain("storage is unavailable");
      expect(parsed.suggestions.length).toBeGreaterThan(0);
      expect(textBlock(result)).toContain("storage is unavailable");
      // The envelope is this tool's, not get_context's: the same empty
      // `environments` the success path answers with, and an error that
      // names the operation that failed.
      expect(parsed.environments).toEqual([]);
      expect(parsed).not.toHaveProperty("sources");
      expect(parsed).not.toHaveProperty("results");
      expect(parsed.error).toContain("listPackages");
      expect(parsed.error).not.toContain("getContext");
   });
});
