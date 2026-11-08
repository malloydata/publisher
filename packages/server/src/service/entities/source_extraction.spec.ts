// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { pathToFileURL } from "node:url";
import { afterEach, beforeEach, describe, test } from "bun:test";
import assert from "node:assert";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import type { Explore, Field } from "@malloydata/malloy";
import { Annotations, Runtime, InMemoryURLReader } from "@malloydata/malloy";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { makeDimIndexLookupKey } from "./dim_index_discovery";
import { compileEntryModel } from "./test_support";
import type { GateExpr } from "./access_gates";
import type { DimIndexFingerprintInputs } from "./dim_index_entity_id";
import type { Given as ApiGiven } from "./internal_types";
import {
   RequiredFilterNonPublicDimensionError,
   sourceExtraction,
   parseJoinCode,
} from "./source_extraction";

// A `file:` URL under an absolute temp root, so `fileURLToPath` accepts it on
// Windows too (where `file:///tmp/x` has no drive and is refused).
const TMP_URL = `${pathToFileURL(path.resolve("/tmp")).href}/`;
const tmpUrl = (name: string) => `${TMP_URL}${name}`;

/** A structDef carrying `lines` as block notes, where the query path reads `#(filter)`s. */
const filterStruct = (lines: string[]) => ({
   parameters: {},
   annotations: { blockNotes: lines.map((text) => ({ text })) },
});

describe("sourceExtraction.extractMetadataFromExplores", () => {
   const noTaglines = (): string[] => [];

   test("extracts docs from explores, and reports no source parameters", () => {
      const explore = {
         name: "orders",
         location: {
            url: tmpUrl("orders.malloy"),
         },
         getTaglines: noTaglines,
         structDef: {
            type: "table",
            annotations: {
               blockNotes: [{ text: "#(doc) Orders source" }],
            },
            parameters: {
               state_param: { type: "string", value: null },
            },
         },
      } as unknown as Explore;

      const metadata = sourceExtraction.extractMetadataFromExplores(
         [explore],
         "orders.malloy",
      );

      assert.strictEqual(metadata.length, 1);
      assert.deepStrictEqual(metadata[0], {
         modelFilePath: "orders.malloy",
         sourceName: "orders",
         metadata: "Orders source",
         metadataType: "DOCS",
         agentHidden: false,
      });
   });

   test("propagates source-level agent-hidden tag across imports", () => {
      // An imported source has its #(agent-hidden) annotation attached
      // directly (no `inherits` chain), so it should still be marked
      // hidden when its model file is processed by a different consumer.
      const explore = {
         name: "orders",
         location: { url: tmpUrl("imported.malloy") },
         // Real Malloy explores have getTaglines() that may include
         // inherited tags; we stub it as a no-op here to make sure the
         // source-level check is reading from structDef.annotation, not
         // from getTaglines().
         getTaglines: noTaglines,
         structDef: {
            type: "table",
            parameters: {},
            annotations: {
               blockNotes: [{ text: "#(agent-hidden)\n" }],
            },
         },
      } as unknown as Explore;

      const metadata = sourceExtraction.extractMetadataFromExplores(
         [explore],
         "consumer.malloy",
         false,
         tmpUrl("consumer.malloy"),
      );

      assert.strictEqual(metadata.length, 1);
      assert.strictEqual(metadata[0].metadataType, "AGENT_HIDDEN");
      assert.strictEqual(metadata[0].agentHidden, true);
   });

   test("source-level agent-hidden is read in every spelling Malloy routes", () => {
      // Same claim as the authorize tagline test below, asked of Malloy rather
      // than of a list: a source hidden with `#[agent-hidden]` must not come
      // back visible because the matcher only knew about `(`.
      const at = {
         url: tmpUrl("m.malloy"),
         range: {
            start: { line: 0, character: 0 },
            end: { line: 0, character: 0 },
         },
      };
      const brackets: [string, string][] = [
         ["(", ")"],
         ["[", "]"],
         ["{", "}"],
         ["<", ">"],
      ];

      let routed = 0;
      for (const block of ["", "|"]) {
         for (const [open, close] of brackets) {
            const text = `#${block}${open}agent-hidden${close}`;
            if (
               new Annotations({ notes: [{ text, at }] }).forRoute(
                  "agent-hidden",
               ).length === 0
            ) {
               continue;
            }
            routed++;
            const explore = {
               name: "orders",
               location: { url: tmpUrl("m.malloy") },
               getTaglines: noTaglines,
               structDef: {
                  type: "table",
                  parameters: {},
                  annotations: { blockNotes: [{ text: `${text}\n` }] },
               },
            } as unknown as Explore;

            const metadata = sourceExtraction.extractMetadataFromExplores(
               [explore],
               "m.malloy",
               false,
               tmpUrl("m.malloy"),
            );
            assert.strictEqual(
               metadata[0]?.agentHidden,
               true,
               `${text} routes to agent-hidden in Malloy but left the source visible`,
            );
         }
      }
      // Both block forms, four bracket pairs; guards against a vacuous pass.
      assert.strictEqual(routed, 8);
   });

   test("source-level agent-hidden does NOT propagate through extend", () => {
      // model1.malloy:
      //   #(agent-hidden)
      //   source: source_a_base is duckdb.table('x.parquet') extend { ... }
      //
      // model2.malloy:
      //   import "model1.malloy"
      //   source: source_a is source_a_base extend { ... }
      //
      // When we process model2, both source_a_base (re-exported) and
      // source_a appear in model.explores. Malloy attaches the base's
      // annotation directly to source_a_base, and source_a's annotation
      // points to the base's annotation via `inherits`.
      const baseAnnotation = {
         blockNotes: [{ text: "#(agent-hidden)\n" }],
      };
      const base = {
         name: "source_a_base",
         location: { url: tmpUrl("model1.malloy") },
         // getTaglines() walks `inherits`, but that's not relevant for
         // the base since the tag is direct. Returning the tag mirrors
         // what real Malloy would return.
         getTaglines: () => ["#(agent-hidden)\n"],
         structDef: {
            parameters: {},
            annotations: baseAnnotation,
         },
      } as unknown as Explore;
      const extending = {
         name: "source_a",
         location: { url: tmpUrl("model2.malloy") },
         // Real Malloy.getTaglines() would walk `inherits` and return
         // the base's #(agent-hidden) here. The fix is that we should
         // ignore that and only consider direct annotations.
         getTaglines: () => ["#(agent-hidden)\n"],
         structDef: {
            parameters: {},
            annotations: {
               inherits: baseAnnotation,
               // No direct blockNotes/notes carrying #(agent-hidden).
            },
         },
      } as unknown as Explore;

      const metadata = sourceExtraction.extractMetadataFromExplores(
         [base, extending],
         "model2.malloy",
         false,
         tmpUrl("model2.malloy"),
      );

      const baseHidden = metadata.find(
         (m) =>
            m.sourceName === "source_a_base" &&
            m.metadataType === "AGENT_HIDDEN",
      );
      assert.ok(baseHidden, "source_a_base should be agent-hidden");

      const extendingHidden = metadata.find(
         (m) =>
            m.sourceName === "source_a" && m.metadataType === "AGENT_HIDDEN",
      );
      assert.strictEqual(
         extendingHidden,
         undefined,
         "source_a (extending source_a_base) should NOT inherit agent-hidden",
      );
   });

   test("source-level agent-hidden does NOT leak when extending source has no own annotation", () => {
      // Real Malloy quirk: when an extending source declares NO annotation
      // of its own, Malloy does NOT build an `inherits` wrapper. Instead it
      // copies the base's annotation onto the extending source verbatim, so
      // the base's #(agent-hidden) shows up as a top-level blockNote. The
      // only signal that it is inherited is the note's *location*, which
      // points back at the base's declaration (far from this source).
      //
      // Layout (single file, mirroring real compiler output):
      //   line 1:  #(agent-hidden)
      //   line 2:  source: source_a_base is ... extend { ... }   (lines 2-4)
      //   ...
      //   line 11: source: source_c is source_a_base extend { } (lines 11-13)
      const url = "internal://internal.malloy";
      const hiddenNote = {
         text: "#(agent-hidden)\n",
         at: {
            url,
            range: {
               start: { line: 1, character: 0 },
               end: { line: 1, character: 16 },
            },
         },
      };
      const base = {
         name: "source_a_base",
         location: {
            url,
            range: {
               start: { line: 2, character: 8 },
               end: { line: 4, character: 1 },
            },
         },
         getTaglines: () => ["#(agent-hidden)\n"],
         structDef: {
            parameters: {},
            // Directly declared on the base: note sits on line 1, right
            // above the source at line 2.
            annotations: { blockNotes: [hiddenNote] },
         },
      } as unknown as Explore;
      const extending = {
         name: "source_c",
         location: {
            url,
            range: {
               start: { line: 11, character: 8 },
               end: { line: 13, character: 1 },
            },
         },
         getTaglines: () => ["#(agent-hidden)\n"],
         structDef: {
            parameters: {},
            // Copied verbatim from the base: same note object, whose
            // location (line 1) is nowhere near source_c (lines 11-13).
            annotations: { blockNotes: [hiddenNote] },
         },
      } as unknown as Explore;

      const metadata = sourceExtraction.extractMetadataFromExplores(
         [base, extending],
         "model.malloy",
         false,
         url,
      );

      const baseHidden = metadata.find(
         (m) =>
            m.sourceName === "source_a_base" &&
            m.metadataType === "AGENT_HIDDEN",
      );
      assert.ok(baseHidden, "source_a_base should be agent-hidden");

      const extendingHidden = metadata.find(
         (m) =>
            m.sourceName === "source_c" && m.metadataType === "AGENT_HIDDEN",
      );
      assert.strictEqual(
         extendingHidden,
         undefined,
         "source_c (extending source_a_base with no own annotation) should NOT inherit agent-hidden",
      );
   });

   test("agent-hidden is NOT leaked to a child defined immediately below the base", () => {
      // Adversarial layout where a naive line-gap check would fail: the
      // base is declared on the line directly between the #(agent-hidden)
      // note and the child source.
      //   line 0: #(agent-hidden)
      //   line 1: source: base is ... extend { }
      //   line 2: source: child is base extend { }
      const url = "internal://internal.malloy";
      const hiddenNote = {
         text: "#(agent-hidden)\n",
         at: {
            url,
            range: {
               start: { line: 0, character: 0 },
               end: { line: 0, character: 16 },
            },
         },
      };
      const base = {
         name: "base",
         location: {
            url,
            range: {
               start: { line: 1, character: 8 },
               end: { line: 1, character: 40 },
            },
         },
         getTaglines: () => ["#(agent-hidden)\n"],
         structDef: {
            parameters: {},
            annotations: { blockNotes: [hiddenNote] },
         },
      } as unknown as Explore;
      const child = {
         name: "child",
         location: {
            url,
            range: {
               start: { line: 2, character: 8 },
               end: { line: 2, character: 40 },
            },
         },
         getTaglines: () => ["#(agent-hidden)\n"],
         structDef: {
            parameters: {},
            annotations: { blockNotes: [hiddenNote] },
         },
      } as unknown as Explore;

      const metadata = sourceExtraction.extractMetadataFromExplores(
         [base, child],
         "model.malloy",
         false,
         url,
      );

      assert.ok(
         metadata.find(
            (m) => m.sourceName === "base" && m.metadataType === "AGENT_HIDDEN",
         ),
         "base should be agent-hidden",
      );
      assert.strictEqual(
         metadata.find(
            (m) =>
               m.sourceName === "child" && m.metadataType === "AGENT_HIDDEN",
         ),
         undefined,
         "child (defined immediately below base) should NOT inherit agent-hidden",
      );
   });

   test("stacked block annotations directly above a source are all treated as direct", () => {
      // line 0: #(agent-hidden)
      // line 1: #(doc) some doc
      // line 2: source: hidden_source is ... extend { }
      const url = "internal://internal.malloy";
      const agentHiddenNote = {
         text: "#(agent-hidden)\n",
         at: {
            url,
            range: {
               start: { line: 0, character: 0 },
               end: { line: 0, character: 16 },
            },
         },
      };
      const docNote = {
         text: "#(doc) some doc\n",
         at: {
            url,
            range: {
               start: { line: 1, character: 0 },
               end: { line: 1, character: 16 },
            },
         },
      };
      const explore = {
         name: "hidden_source",
         location: {
            url,
            range: {
               start: { line: 2, character: 8 },
               end: { line: 2, character: 40 },
            },
         },
         getTaglines: () => ["#(agent-hidden)\n", "#(doc) some doc\n"],
         structDef: {
            parameters: {},
            annotations: { blockNotes: [agentHiddenNote, docNote] },
         },
      } as unknown as Explore;

      const metadata = sourceExtraction.extractMetadataFromExplores(
         [explore],
         "model.malloy",
         false,
         url,
      );

      assert.ok(
         metadata.find(
            (m) =>
               m.sourceName === "hidden_source" &&
               m.metadataType === "AGENT_HIDDEN",
         ),
         "hidden_source should be agent-hidden from its own stacked annotation",
      );
      assert.ok(
         metadata.find(
            (m) =>
               m.sourceName === "hidden_source" &&
               m.metadataType === "DOCS" &&
               m.metadata === "some doc",
         ),
         "hidden_source should keep its own #(doc) annotation",
      );
   });

   test("model-file-level agent-hidden does NOT propagate through extend in another file", () => {
      // model1.malloy:
      //   ##(agent-hidden)
      //   source: source_a_base is duckdb.table('x.parquet') extend { ... }
      //
      // model2.malloy (no model-level tag):
      //   import "model1.malloy"
      //   source: source_a is source_a_base extend { ... }
      //
      // When processing model2.malloy: model.getTaglines() does not
      // include the model1 model-level tag, so
      // isAgentHiddenTagPresentOnModelFile=false. Neither source_a_base
      // (defined in model1) nor source_a (defined in model2) should be
      // marked hidden when viewed via model2.
      const base = {
         name: "source_a_base",
         location: { url: tmpUrl("model1.malloy") },
         getTaglines: noTaglines,
         structDef: {
            parameters: {},
            annotations: {
               // Note: the model-level ##(agent-hidden) lives on the
               // Model, not on the explore's structDef.annotation, so
               // there are no direct notes here.
            },
         },
      } as unknown as Explore;
      const extending = {
         name: "source_a",
         location: { url: tmpUrl("model2.malloy") },
         getTaglines: noTaglines,
         structDef: {
            parameters: {},
            annotations: {},
         },
      } as unknown as Explore;

      const metadata = sourceExtraction.extractMetadataFromExplores(
         [base, extending],
         "model2.malloy",
         false, // isModelFileAgentHidden = false (model2 has no ##(agent-hidden))
         tmpUrl("model2.malloy"),
      );

      const anyAgentHidden = metadata.find(
         (m) => m.metadataType === "AGENT_HIDDEN",
      );
      assert.strictEqual(
         anyAgentHidden,
         undefined,
         "no source should be agent-hidden when viewed from model2",
      );
   });

   test("emits FILTERS metadata for valid #(filter) annotations", () => {
      const explore = {
         name: "orders",
         location: { url: tmpUrl("orders.malloy") },
         getTaglines: () => [
            "#(filter) name=Region dimension=region type=in",
            '#(filter) name="Created After" dimension=created_at type=greater_than required',
            "#(filter) dimension=tenant_id type=equal implicit",
         ],
         allFields: [
            { name: "region" },
            { name: "created_at" },
            { name: "tenant_id" },
         ],
         structDef: filterStruct([
            "#(filter) name=Region dimension=region type=in",
            '#(filter) name="Created After" dimension=created_at type=greater_than required',
            "#(filter) dimension=tenant_id type=equal implicit",
         ]),
      } as unknown as Explore;

      const metadata = sourceExtraction.extractMetadataFromExplores(
         [explore],
         "orders.malloy",
      );

      const filtersEntry = metadata.find((m) => m.metadataType === "FILTERS");
      assert.ok(filtersEntry);
      assert.deepStrictEqual(filtersEntry.filters, [
         {
            name: "Region",
            dimension: "region",
            type: "in",
            implicit: false,
            required: false,
         },
         {
            name: "Created After",
            dimension: "created_at",
            type: "greater_than",
            implicit: false,
            required: true,
         },
         {
            name: "tenant_id",
            dimension: "tenant_id",
            type: "equal",
            implicit: true,
            required: false,
         },
      ]);
   });

   test("allows two required filters on distinct dimensions", () => {
      const explore = {
         name: "orders",
         location: { url: tmpUrl("orders.malloy") },
         getTaglines: () => [
            "#(filter) name=Tier dimension=tier type=equal required",
            "#(filter) name=Region dimension=region type=equal required",
         ],
         allFields: [{ name: "tier" }, { name: "region" }],
         structDef: filterStruct([
            "#(filter) name=Tier dimension=tier type=equal required",
            "#(filter) name=Region dimension=region type=equal required",
         ]),
      } as unknown as Explore;

      assert.doesNotThrow(() =>
         sourceExtraction.extractMetadataFromExplores(
            [explore],
            "orders.malloy",
         ),
      );
   });

   test("allows a duplicate dimension when only one filter is required", () => {
      const explore = {
         name: "orders",
         location: { url: tmpUrl("orders.malloy") },
         getTaglines: () => [
            "#(filter) name=Required dimension=tier type=equal required",
            "#(filter) name=Optional dimension=tier type=in",
         ],
         allFields: [{ name: "tier" }],
         structDef: filterStruct([
            "#(filter) name=Required dimension=tier type=equal required",
            "#(filter) name=Optional dimension=tier type=in",
         ]),
      } as unknown as Explore;

      assert.doesNotThrow(() =>
         sourceExtraction.extractMetadataFromExplores(
            [explore],
            "orders.malloy",
         ),
      );
   });

   test("parses every filter type and flag combination", () => {
      const explore = {
         name: "orders",
         location: { url: tmpUrl("orders.malloy") },
         getTaglines: () => [
            "#(filter) name=Eq dimension=d type=equal",
            "#(filter) name=Like_F dimension=d type=like",
            "#(filter) name=Less_F dimension=d type=less_than",
            "#(filter) name=Both dimension=d type=equal required implicit",
         ],
         allFields: [{ name: "d" }],
         structDef: filterStruct([
            "#(filter) name=Eq dimension=d type=equal",
            "#(filter) name=Like_F dimension=d type=like",
            "#(filter) name=Less_F dimension=d type=less_than",
            "#(filter) name=Both dimension=d type=equal required implicit",
         ]),
      } as unknown as Explore;

      const metadata = sourceExtraction.extractMetadataFromExplores(
         [explore],
         "orders.malloy",
      );

      const filters = metadata.find(
         (m) => m.metadataType === "FILTERS",
      )!.filters!;
      assert.strictEqual(filters.length, 4);
      assert.deepStrictEqual(
         filters.map((f) => [f.name, f.type, f.required, f.implicit]),
         [
            ["Eq", "equal", false, false],
            ["Like_F", "like", false, false],
            ["Less_F", "less_than", false, false],
            ["Both", "equal", true, true],
         ],
      );
   });

   test("drops filter referencing unknown dimension", () => {
      const explore = {
         name: "orders",
         location: { url: tmpUrl("orders.malloy") },
         getTaglines: () => [
            "#(filter) name=Region dimension=region type=in",
            "#(filter) name=Bogus dimension=does_not_exist type=equal",
         ],
         allFields: [{ name: "region" }],
         structDef: filterStruct([
            "#(filter) name=Region dimension=region type=in",
            "#(filter) name=Bogus dimension=does_not_exist type=equal",
         ]),
      } as unknown as Explore;

      const metadata = sourceExtraction.extractMetadataFromExplores(
         [explore],
         "orders.malloy",
      );

      const filtersEntry = metadata.find((m) => m.metadataType === "FILTERS");
      assert.ok(filtersEntry);
      assert.strictEqual(filtersEntry.filters!.length, 1);
      assert.strictEqual(filtersEntry.filters![0].name, "Region");
   });

   test("throws RequiredFilterMissingDimensionError when required filter has unknown dimension", () => {
      const explore = {
         name: "orders",
         location: { url: tmpUrl("orders.malloy") },
         getTaglines: () => [
            "#(filter) name=Tenant dimension=missing_dim type=equal required",
         ],
         allFields: [{ name: "region" }],
         structDef: filterStruct([
            "#(filter) name=Tenant dimension=missing_dim type=equal required",
         ]),
      } as unknown as Explore;

      assert.throws(
         () =>
            sourceExtraction.extractMetadataFromExplores(
               [explore],
               "orders.malloy",
            ),
         /required.*missing_dim/i,
      );
   });

   test("throws RequiredFilterNonPublicDimensionError when required filter references a private dimension", () => {
      const explore = {
         name: "orders",
         location: { url: tmpUrl("orders.malloy") },
         getTaglines: () => [
            "#(filter) name=Tier dimension=tier type=equal required",
         ],
         // `tier` exists but is private — usable inside the source's own
         // compile but not on the public surface a filtered query hits.
         allFields: [
            { name: "tier", fieldTypeDef: { accessModifier: "private" } },
         ],
         structDef: filterStruct([
            "#(filter) name=Tier dimension=tier type=equal required",
         ]),
      } as unknown as Explore;

      assert.throws(
         () =>
            sourceExtraction.extractMetadataFromExplores(
               [explore],
               "orders.malloy",
            ),
         (err: unknown) =>
            err instanceof RequiredFilterNonPublicDimensionError &&
            /non-public dimension 'tier'/.test((err as Error).message),
      );
   });

   test("drops optional filter referencing a non-public dimension but keeps valid ones", () => {
      const explore = {
         name: "orders",
         location: { url: tmpUrl("orders.malloy") },
         getTaglines: () => [
            "#(filter) name=Region dimension=region type=in",
            "#(filter) name=Internal dimension=secret type=equal",
         ],
         allFields: [
            { name: "region" },
            { name: "secret", fieldTypeDef: { accessModifier: "internal" } },
         ],
         structDef: filterStruct([
            "#(filter) name=Region dimension=region type=in",
            "#(filter) name=Internal dimension=secret type=equal",
         ]),
      } as unknown as Explore;

      const metadata = sourceExtraction.extractMetadataFromExplores(
         [explore],
         "orders.malloy",
      );

      const filtersEntry = metadata.find((m) => m.metadataType === "FILTERS");
      assert.ok(filtersEntry);
      assert.strictEqual(filtersEntry.filters!.length, 1);
      assert.strictEqual(filtersEntry.filters![0].name, "Region");
   });

   test("a malformed filter annotation leaves the source with no filters, as the query path reads it", () => {
      const explore = {
         name: "orders",
         location: { url: tmpUrl("orders.malloy") },
         getTaglines: () => [
            "#(filter) name=Region dimension=region type=in",
            "#(filter) name=Bad dimension=region type=not_a_real_type",
         ],
         allFields: [{ name: "region" }],
         structDef: filterStruct([
            "#(filter) name=Region dimension=region type=in",
            "#(filter) name=Bad dimension=region type=not_a_real_type",
         ]),
      } as unknown as Explore;

      const metadata = sourceExtraction.extractMetadataFromExplores(
         [explore],
         "orders.malloy",
      );

      assert.strictEqual(
         metadata.find((m) => m.metadataType === "FILTERS"),
         undefined,
      );
   });

   test("model-level agent-hidden applies only to sources defined in this model file", () => {
      const local = {
         name: "local_source",
         location: { url: tmpUrl("model.malloy") },
         getTaglines: noTaglines,
         structDef: { type: "table", parameters: {} },
      } as unknown as Explore;
      const imported = {
         name: "imported_source",
         location: { url: tmpUrl("other.malloy") },
         getTaglines: noTaglines,
         structDef: { type: "table", parameters: {} },
      } as unknown as Explore;

      const metadata = sourceExtraction.extractMetadataFromExplores(
         [local, imported],
         "model.malloy",
         true,
         tmpUrl("model.malloy"),
      );

      assert.strictEqual(metadata.length, 1);
      assert.strictEqual(metadata[0].sourceName, "local_source");
      assert.strictEqual(metadata[0].metadataType, "AGENT_HIDDEN");
      assert.strictEqual(metadata[0].agentHidden, true);
   });

   // -----------------------------------------------------------------
   // GIVENS emission.
   //
   // Duck-typed Given mock matches what `extractGivensFromModel` reads:
   // {name, type:{type, filterType?}, annotations.forRoute(undefined),
   // _internal.defaultText}. The annotations stub returns RoutedNote-like
   // objects with a `route` and `text` so the route-filter behaves the
   // same way it does against the real SDK class.
   // -----------------------------------------------------------------

   const mockGiven = (opts: {
      name: string;
      type: {
         type: string;
         filterType?: string;
         elementTypeDef?: { type: string };
      };
      defaultText?: string;
      annotationTexts?: { route: string; text: string; content?: string }[];
   }) => ({
      name: opts.name,
      type: opts.type,
      annotations: {
         // Honors the route filter like the real SDK (undefined → all);
         // exposes `content` (payload after the prefix) alongside `text`.
         forRoute: (route?: string) =>
            (opts.annotationTexts ?? [])
               .filter((a) => route === undefined || a.route === route)
               .map((a) => ({
                  route: a.route,
                  text: a.text,
                  content: a.content ?? "",
               })),
      },
      _internal: { defaultText: opts.defaultText },
   });

   const mockModel = (givens: ReturnType<typeof mockGiven>[]) =>
      ({
         givens: new Map(givens.map((g) => [g.name, g])),
      }) as unknown as Parameters<
         typeof sourceExtraction.extractGivensFromModel
      >[0];

   test("extractGivensFromModel renders types, defaults, and annotations", () => {
      const model = mockModel([
         mockGiven({
            name: "carrier",
            type: { type: "string" },
            defaultText: "'WN'",
            annotationTexts: [
               { route: "description", text: "#(description) IATA" },
            ],
         }),
         mockGiven({
            name: "after",
            type: { type: "timestamp" },
            defaultText: "@2003-01-01 00:00:00",
         }),
         mockGiven({
            name: "limit_n",
            type: { type: "number" },
         }),
         mockGiven({
            name: "tags",
            type: { type: "filter expression", filterType: "string" },
            defaultText: "f'WN'",
         }),
         // Array given: Malloy's raw type is `array` + elementTypeDef; must render
         // as `string[]` (the multi-value convention the trusted-attribute system keys on).
         mockGiven({
            name: "regions",
            type: { type: "array", elementTypeDef: { type: "string" } },
         }),
      ]);

      // As the wire carries it: the server's own given serializer.
      const out = JSON.parse(
         JSON.stringify(sourceExtraction.extractGivensFromModel(model)),
      );

      assert.deepStrictEqual(out, [
         {
            name: "carrier",
            type: "string",
            annotations: ["#(description) IATA"],
            default: "'WN'",
         },
         {
            name: "after",
            type: "timestamp",
            annotations: [],
            default: "@2003-01-01 00:00:00",
         },
         { name: "limit_n", type: "number", annotations: [], required: true },
         {
            name: "tags",
            type: "filter<string>",
            annotations: [],
            default: "f'WN'",
         },
         { name: "regions", type: "string[]", annotations: [], required: true },
      ]);
   });

   test("extractGivensFromModel filters reserved-route annotations", () => {
      const model = mockModel([
         mockGiven({
            name: "carrier",
            type: { type: "string" },
            annotationTexts: [
               { route: "", text: "## reserved" },
               { route: "#!", text: "##! pragma" },
               { route: "description", text: "#(description) ok" },
            ],
         }),
      ]);

      const [given] = sourceExtraction.extractGivensFromModel(model);
      assert.deepStrictEqual(given.annotations, ["#(description) ok"]);
   });

   test("extractGivensFromModel marks secure=true for #(secure)", () => {
      // The trust marker: a given carrying `#(secure)` (route "secure", presence
      // is the signal — no content match). Secure-given middleware treats these
      // as server-resolved. Unmarked givens carry no `secure` field.
      const model = mockModel([
         mockGiven({
            name: "tenant_id",
            type: { type: "number" },
            annotationTexts: [{ route: "secure", text: "#(secure)" }],
         }),
         mockGiven({ name: "report_date", type: { type: "date" } }),
      ]);

      const out = JSON.parse(
         JSON.stringify(sourceExtraction.extractGivensFromModel(model)),
      );

      // Required too: a host must supply a secure given that has no default.
      assert.deepStrictEqual(out, [
         {
            name: "tenant_id",
            type: "number",
            annotations: ["#(secure)"],
            secure: true,
            required: true,
         },
         { name: "report_date", type: "date", annotations: [], required: true },
      ]);
   });

   test("emits GIVENS metadata for every source reachable in this compile", () => {
      // When a model imports a source from another file, the entities
      // for the imported source are indexed under the importing model's
      // path. The GIVENS row must use the same key so a
      // (model file, source) lookup finds it.
      const local = {
         name: "spotlight_flights",
         location: { url: tmpUrl("model.malloy") },
         getTaglines: noTaglines,
         structDef: { parameters: {} },
      } as unknown as Explore;
      const imported = {
         name: "carriers",
         location: { url: tmpUrl("other.malloy") },
         getTaglines: noTaglines,
         structDef: { parameters: {} },
      } as unknown as Explore;
      const givens = [
         { name: "carrier", type: "string", default: "'WN'" },
         { name: "after", type: "timestamp", default: "@2003-01-01 00:00:00" },
      ];

      const metadata = sourceExtraction.extractMetadataFromExplores(
         [local, imported],
         "model.malloy",
         false,
         tmpUrl("model.malloy"),
         givens,
      );

      const givensRows = metadata.filter((m) => m.metadataType === "GIVENS");
      assert.strictEqual(givensRows.length, 2);
      const bySource = new Map(givensRows.map((r) => [r.sourceName, r]));
      const localRow = bySource.get("spotlight_flights")!;
      const importedRow = bySource.get("carriers")!;
      assert.strictEqual(localRow.modelFilePath, "model.malloy");
      assert.strictEqual(importedRow.modelFilePath, "model.malloy");
      assert.deepStrictEqual(localRow.givens, givens);
      assert.deepStrictEqual(importedRow.givens, givens);
   });

   test("emits no GIVENS rows when the model declares no givens", () => {
      const explore = {
         name: "orders",
         location: { url: tmpUrl("orders.malloy") },
         getTaglines: noTaglines,
         structDef: { parameters: {} },
      } as unknown as Explore;

      const metadata = sourceExtraction.extractMetadataFromExplores(
         [explore],
         "orders.malloy",
         false,
         tmpUrl("orders.malloy"),
         [],
      );

      assert.strictEqual(
         metadata.filter((m) => m.metadataType === "GIVENS").length,
         0,
      );
   });

   // -----------------------------------------------------------------
   // AUTHORIZE emission.
   //
   // File-level expressions come from `model.getTaglines()` (read by the
   // separate `extractAuthorizeFromModel` accessor); source-level come from
   // `gateExprsForStruct`/`buildAuthorizeForExplore` reading the explore's
   // own `structDef.annotations` raw. The duck-typed Model mocks below only
   // need `getTaglines` since the source-level path reads its own.
   // -----------------------------------------------------------------

   const mockAuthorizeModel = (taglines: string[]) =>
      ({
         getTaglines: () => taglines,
      }) as unknown as Parameters<
         typeof sourceExtraction.extractAuthorizeFromModel
      >[0];

   test("extractAuthorizeFromModel parses file-level expressions in declaration order", () => {
      const model = mockAuthorizeModel([
         "##(authorize) \"$ROLE = 'admin'\"",
         "##! experimental.givens", // non-authorize taglines pass through untouched
         "##(authorize) \"$REGION = 'us-west'\"",
      ]);

      const out = sourceExtraction.extractAuthorizeFromModel(
         model,
         "model.malloy",
      );

      assert.deepStrictEqual(out, [
         { route: "authorize", expr: "$ROLE = 'admin'" },
         { route: "authorize", expr: "$REGION = 'us-west'" },
      ]);
   });

   test("extractAuthorizeFromModel drops a malformed annotation but keeps valid ones", () => {
      // A `##(authorize)` whose body never closes its double quote must not
      // fail the whole file's indexing — skip it (with a warning) and keep
      // the valid gates, matching the filter-parsing best-effort contract.
      const model = mockAuthorizeModel([
         "##(authorize) \"$ROLE = 'admin'", // malformed: missing closing "
         "##(authorize) \"$REGION = 'us-west'\"",
      ]);
      const out = sourceExtraction.extractAuthorizeFromModel(
         model,
         "model.malloy",
      );
      assert.deepStrictEqual(out, [
         { route: "authorize", expr: "$REGION = 'us-west'" },
      ]);
   });

   test("#( authorize ) with inner whitespace is a near miss: not a gate to Malloy, refused at load, skipped here", () => {
      const model = mockAuthorizeModel(["##( authorize ) \"$ROLE = 'admin'\""]);
      const out = sourceExtraction.extractAuthorizeFromModel(
         model,
         "model.malloy",
      );
      // Malloy does not route this spelling as a gate, so it is
      // not one; the load refuses it (`assertNoAuthorizeNearMisses`).
      assert.deepStrictEqual(out, []);
   });

   test("an annotation whose name merely starts with 'authorize' is NOT matched", () => {
      const model = mockAuthorizeModel([
         "##(authorized-users) \"$ROLE = 'admin'\"",
      ]);
      const out = sourceExtraction.extractAuthorizeFromModel(
         model,
         "model.malloy",
      );
      assert.deepStrictEqual(out, []);
   });

   test("# (authorize) with whitespace between sigil and paren is a near miss: skipped, as Malloy routes it", () => {
      const model = mockAuthorizeModel(["# (authorize) \"$ROLE = 'admin'\""]);
      const out = sourceExtraction.extractAuthorizeFromModel(
         model,
         "model.malloy",
      );
      // Malloy does not route this spelling as a gate, so it is
      // not one; the load refuses it (`assertNoAuthorizeNearMisses`).
      assert.deepStrictEqual(out, []);
   });

   test("## (authorize) with whitespace between sigil and paren is a near miss: skipped, as Malloy routes it", () => {
      const model = mockAuthorizeModel(["## (authorize) \"$ROLE = 'admin'\""]);
      const out = sourceExtraction.extractAuthorizeFromModel(
         model,
         "model.malloy",
      );
      // Malloy does not route this spelling as a gate, so it is
      // not one; the load refuses it (`assertNoAuthorizeNearMisses`).
      assert.deepStrictEqual(out, []);
   });

   test("#(authorize_foo) is NOT matched (no over-match past the widened sigil-paren gap)", () => {
      const model = mockAuthorizeModel([
         "##(authorize_foo) \"$ROLE = 'admin'\"",
      ]);
      const out = sourceExtraction.extractAuthorizeFromModel(
         model,
         "model.malloy",
      );
      assert.deepStrictEqual(out, []);
   });

   test("#(access_filter) is matched — the row-filter route is read, tagged with its own route", () => {
      const model = mockAuthorizeModel([
         "##(access_filter) \"$ROLE = 'admin'\"",
      ]);
      const out = sourceExtraction.extractAuthorizeFromModel(
         model,
         "model.malloy",
      );
      assert.deepStrictEqual(out, [
         { route: "access_filter", expr: "$ROLE = 'admin'" },
      ]);
   });

   test("#( access_filter ) is a near miss on the row-filter route too: skipped", () => {
      const model = mockAuthorizeModel([
         "##( access_filter ) \"$ROLE = 'admin'\"",
      ]);
      const out = sourceExtraction.extractAuthorizeFromModel(
         model,
         "model.malloy",
      );
      // Malloy does not route this spelling as a gate, so it is
      // not one; the load refuses it (`assertNoAuthorizeNearMisses`).
      assert.deepStrictEqual(out, []);
   });

   test("#(access_filtered) is NOT matched (no over-match past the route word)", () => {
      const model = mockAuthorizeModel([
         "##(access_filtered) \"$ROLE = 'admin'\"",
      ]);
      const out = sourceExtraction.extractAuthorizeFromModel(
         model,
         "model.malloy",
      );
      assert.deepStrictEqual(out, []);
   });

   test("#(access-filter) is NOT matched — hyphenated spellings are refused at load, so not accepted here", () => {
      const model = mockAuthorizeModel([
         "##(access-filter) \"$ROLE = 'admin'\"",
      ]);
      const out = sourceExtraction.extractAuthorizeFromModel(
         model,
         "model.malloy",
      );
      assert.deepStrictEqual(out, []);
   });

   test("every spelling Malloy routes to a gate is read as one", () => {
      // Asserted against Malloy's own `Annotations.forRoute` rather than a list
      // kept here, because the claim is about the compiler: a hand-kept list
      // goes stale silently the moment the prefix grammar moves, and the stale
      // direction is the leaking one. Reading MORE than Malloy routes is safe
      // (publisher refuses those spellings at load, so such a source is never
      // served); reading LESS means publisher enforces a gate this file reports
      // as absent, and the source's dimension values go back into value search.
      const at = {
         url: tmpUrl("m.malloy"),
         range: {
            start: { line: 0, character: 0 },
            end: { line: 0, character: 0 },
         },
      };
      const brackets: [string, string][] = [
         ["(", ")"],
         ["[", "]"],
         ["{", "}"],
         ["<", ">"],
      ];

      let routed = 0;
      for (const route of ["authorize", "access_filter"]) {
         for (const sigil of ["#", "##"]) {
            for (const block of ["", "|"]) {
               for (const [open, close] of brackets) {
                  const text = `${sigil}${block}${open}${route}${close} "$ROLE = 'admin'"`;
                  const malloyRoutes =
                     new Annotations({ notes: [{ text, at }] }).forRoute(route)
                        .length > 0;
                  if (!malloyRoutes) continue;
                  routed++;
                  assert.deepStrictEqual(
                     sourceExtraction.extractAuthorizeFromModel(
                        mockAuthorizeModel([text]),
                        "model.malloy",
                     ),
                     [{ route, expr: "$ROLE = 'admin'" }],
                     `${text} routes to ${route} in Malloy but was not read as a gate`,
                  );
               }
            }
         }
      }
      // Without this the loop would pass vacuously if Malloy routed none of
      // them. Both routes, both sigils, both block forms, four bracket pairs.
      assert.strictEqual(routed, 32);
   });

   test("a mismatched bracket pair is NOT matched", () => {
      // Malloy does not route `#(authorize]`, so neither does this; accepting
      // it would report a gate on text no worker treats as one.
      for (const tag of [
         "##(authorize]",
         "##[access_filter)",
         "##<authorize}",
      ]) {
         const model = mockAuthorizeModel([`${tag} "$ROLE = 'admin'"`]);
         assert.deepStrictEqual(
            sourceExtraction.extractAuthorizeFromModel(model, "model.malloy"),
            [],
            `${tag} should not be read as a gate`,
         );
      }
   });

   test("the retired #(row_authorize) / #(source_authorize) spellings are NOT matched", () => {
      // Neither was ever in a publisher release, so a model carrying one fails
      // the load rather than reaching here; matching them would report a gate
      // on a package that cannot be served.
      for (const tag of ["row_authorize", "source_authorize"]) {
         const model = mockAuthorizeModel([`##(${tag}) "$ROLE = 'admin'"`]);
         assert.deepStrictEqual(
            sourceExtraction.extractAuthorizeFromModel(model, "model.malloy"),
            [],
         );
      }
   });

   test("scopes file-level AUTHORIZE to this file; source-level travels with imports", () => {
      // A locally-defined source combines the file-level `##(authorize)` with
      // its own `#(authorize)` expressions — publisher extracts the whole list
      // in this order, file-level first. An imported
      // source (location ≠ modelFileUrl) must NOT inherit the importing file's
      // file-level gate (same scoping as `agent-hidden`), but DOES keep its
      // own source-level gate, which is attached directly to the explore. This
      // pins both the ordering/given-name extraction and the file-vs-import
      // scoping.
      const local = {
         name: "orders",
         location: { url: tmpUrl("model.malloy") },
         getTaglines: noTaglines,
         structDef: {
            type: "table",
            annotations: {
               blockNotes: [
                  { text: "#(authorize) \"$ROLE = 'analyst'\"" },
                  {
                     text: "#(authorize) \"$TENANT in ['acme', 'globex']\"",
                  },
               ],
            },
            parameters: {},
         },
      } as unknown as Explore;
      const imported = {
         name: "carriers",
         location: { url: tmpUrl("other.malloy") },
         getTaglines: noTaglines,
         structDef: {
            type: "table",
            annotations: {
               blockNotes: [{ text: "#(authorize) \"$REGION = 'us-west'\"" }],
            },
            parameters: {},
         },
      } as unknown as Explore;

      const metadata = sourceExtraction.extractMetadataFromExplores(
         [local, imported],
         "model.malloy",
         false,
         tmpUrl("model.malloy"),
         [],
         [{ route: "authorize", expr: "$ROLE = 'admin'" }],
      );

      const authorizeRows = metadata.filter(
         (m) => m.metadataType === "AUTHORIZE",
      );
      assert.strictEqual(authorizeRows.length, 2);
      const bySource = new Map(authorizeRows.map((r) => [r.sourceName, r]));
      const orders = bySource.get("orders")!;
      assert.deepStrictEqual(orders.authorize, [
         {
            expression: "$ROLE = 'admin'",
            given_names: ["ROLE"],
            route: "authorize",
         },
         {
            expression: "$ROLE = 'analyst'",
            given_names: ["ROLE"],
            route: "authorize",
         },
         {
            expression: "$TENANT in ['acme', 'globex']",
            given_names: ["TENANT"],
            route: "authorize",
         },
      ]);
      // The imported source keeps only its own source-level gate — the
      // importing file's `$ROLE = 'admin'` file-level gate is NOT leaked onto it.
      const carriers = bySource.get("carriers")!;
      assert.deepStrictEqual(carriers.authorize, [
         {
            expression: "$REGION = 'us-west'",
            given_names: ["REGION"],
            route: "authorize",
         },
      ]);
   });

   test("does not leak a file-level gate onto an imported ungated source", () => {
      // The phantom-gate case from review: a file-level `##(authorize)` plus an
      // imported source with no annotation of its own must emit NO AUTHORIZE
      // row for that imported source (otherwise it is reported gated and
      // an agent needlessly avoids a source it may use). The locally-defined
      // source still carries the file-level gate. Mirrors the agent-hidden
      // imported-source scoping.
      const local = {
         name: "orders",
         location: { url: tmpUrl("model.malloy") },
         getTaglines: noTaglines,
         structDef: { type: "table", parameters: {} },
      } as unknown as Explore;
      const importedUngated = {
         name: "carriers",
         location: { url: tmpUrl("other.malloy") },
         getTaglines: noTaglines,
         structDef: { type: "table", parameters: {} },
      } as unknown as Explore;

      const metadata = sourceExtraction.extractMetadataFromExplores(
         [local, importedUngated],
         "model.malloy",
         false,
         tmpUrl("model.malloy"),
         [],
         [{ route: "authorize", expr: "$ROLE = 'admin'" }],
      );

      const authorizeRows = metadata.filter(
         (m) => m.metadataType === "AUTHORIZE",
      );
      assert.strictEqual(authorizeRows.length, 1);
      assert.strictEqual(authorizeRows[0]!.sourceName, "orders");
      assert.deepStrictEqual(authorizeRows[0]!.authorize, [
         {
            expression: "$ROLE = 'admin'",
            given_names: ["ROLE"],
            route: "authorize",
         },
      ]);
   });

   test("extracts lower/mixed-case AND all-caps given names (customer user attributes)", () => {
      // Customer user attributes (e.g. a LookML `user_attribute: brandName`) are
      // commonly lower/mixed-case; the registry must support them alongside ALL_CAPS.
      const orders = {
         name: "orders",
         location: { url: tmpUrl("model.malloy") },
         getTaglines: noTaglines,
         structDef: { type: "table", parameters: {} },
      } as unknown as Explore;

      const metadata = sourceExtraction.extractMetadataFromExplores(
         [orders],
         "model.malloy",
         false,
         tmpUrl("model.malloy"),
         [],
         [
            {
               route: "authorize",
               expr: "$brandName in ['acme'] or $REGION = 'us'",
            },
         ],
      );

      const authorizeRows = metadata.filter(
         (m) => m.metadataType === "AUTHORIZE",
      );
      assert.strictEqual(authorizeRows.length, 1);
      assert.deepStrictEqual(authorizeRows[0]!.authorize, [
         {
            expression: "$brandName in ['acme'] or $REGION = 'us'",
            given_names: ["brandName", "REGION"],
            route: "authorize",
         },
      ]);
   });

   test("emits no AUTHORIZE rows when no gate is in scope", () => {
      const explore = {
         name: "orders",
         location: { url: tmpUrl("orders.malloy") },
         getTaglines: noTaglines,
         structDef: { type: "table", parameters: {} },
      } as unknown as Explore;

      const metadata = sourceExtraction.extractMetadataFromExplores(
         [explore],
         "orders.malloy",
         false,
         tmpUrl("orders.malloy"),
         [],
         [],
      );

      assert.strictEqual(
         metadata.filter((m) => m.metadataType === "AUTHORIZE").length,
         0,
      );
   });

   test("a bare source-line #(authorize) expression is read verbatim, quotes and all", () => {
      const explore = {
         name: "orders",
         location: { url: tmpUrl("orders.malloy") },
         getTaglines: noTaglines,
         structDef: {
            type: "table",
            parameters: {},
            annotations: {
               blockNotes: [{ text: "#(authorize) 'analysts' in $GROUPS" }],
            },
         },
      } as unknown as Explore;

      const metadata = sourceExtraction.extractMetadataFromExplores(
         [explore],
         "orders.malloy",
         false,
         tmpUrl("orders.malloy"),
         [],
         [],
      );

      const row = metadata.find(
         (m) => m.sourceName === "orders" && m.metadataType === "AUTHORIZE",
      );
      assert.ok(row, "a bare #(authorize) must produce an AUTHORIZE row");
      assert.deepStrictEqual(row!.authorize, [
         {
            expression: "'analysts' in $GROUPS",
            given_names: ["GROUPS"],
            route: "authorize",
         },
      ]);
   });

   test("a bare #(authorize) with no payload still fails closed", () => {
      const explore = {
         name: "orders",
         location: { url: tmpUrl("orders.malloy") },
         getTaglines: noTaglines,
         structDef: {
            parameters: {},
            annotations: { blockNotes: [{ text: "#(authorize)" }] },
         },
      } as unknown as Explore;

      const metadata = sourceExtraction.extractMetadataFromExplores(
         [explore],
         "orders.malloy",
         false,
         tmpUrl("orders.malloy"),
         [],
         [],
      );

      const row = metadata.find(
         (m) => m.sourceName === "orders" && m.metadataType === "AUTHORIZE",
      );
      assert.ok(
         row,
         "an empty #(authorize) must still produce an AUTHORIZE row (deny)",
      );
      assert.deepStrictEqual(row!.authorize, [
         { expression: "false", given_names: [], route: "authorize" },
      ]);
   });

   test("a malformed #(authorize) on a source's own notes fails closed (deny), not skip", () => {
      const explore = {
         name: "orders",
         location: { url: tmpUrl("orders.malloy") },
         getTaglines: noTaglines,
         structDef: {
            parameters: {},
            annotations: {
               blockNotes: [{ text: "#(authorize) \"$ROLE = 'admin'" }], // missing closing "
            },
         },
      } as unknown as Explore;

      const metadata = sourceExtraction.extractMetadataFromExplores(
         [explore],
         "orders.malloy",
         false,
         tmpUrl("orders.malloy"),
         [],
         [{ route: "authorize", expr: "$REGION = 'us'" }],
      );

      const row = metadata.find(
         (m) => m.sourceName === "orders" && m.metadataType === "AUTHORIZE",
      );
      assert.ok(
         row,
         "a malformed own #(authorize) must still produce an AUTHORIZE row (deny)",
      );
      assert.deepStrictEqual(
         row!.authorize,
         [{ expression: "false", given_names: [], route: "authorize" }],
         "fails closed to a bare deny gate — the file-level gate is not prepended",
      );
   });

   test("an unmodelled struct.type (nest_source) fails closed (deny), not ungated", () => {
      // `gateExprsForStruct` only walks shapes it can prove are terminal or
      // resolvable (`table`/`composite`/`record`/`array`/`sql_select`/
      // `query_source`). `nest_source` isn't modeled at all, so reporting it
      // as [] (ungated) would silently drop a gate this walk can't see.
      const explore = {
         name: "nested_source",
         location: { url: tmpUrl("orders.malloy") },
         getTaglines: noTaglines,
         structDef: { type: "nest_source", parameters: {} },
      } as unknown as Explore;

      const metadata = sourceExtraction.extractMetadataFromExplores(
         [explore],
         "orders.malloy",
         false,
         tmpUrl("orders.malloy"),
         [],
         [],
      );

      const row = metadata.find(
         (m) =>
            m.sourceName === "nested_source" && m.metadataType === "AUTHORIZE",
      );
      assert.ok(
         row,
         "an unmodelled struct.type must still produce an AUTHORIZE row (deny)",
      );
      assert.deepStrictEqual(row!.authorize, [
         { expression: "false", given_names: [], route: "authorize" },
      ]);
   });

   test("a sql_select carrying an embedded query/source segment fails closed (deny)", () => {
      // A plain `duckdb.sql(...)` compiles with no query-bearing segment
      // (see the compile-backed test below), but `select.containsQueries`
      // can embed a query/source segment in `selectSegments` — that segment
      // can carry a gated source this walk doesn't descend into, so it must
      // deny rather than report [] (ungated).
      const explore = {
         name: "raw_sql_source",
         location: { url: tmpUrl("orders.malloy") },
         getTaglines: noTaglines,
         structDef: {
            type: "sql_select",
            parameters: {},
            selectSegments: [{ type: "query_source" }],
         },
      } as unknown as Explore;

      const metadata = sourceExtraction.extractMetadataFromExplores(
         [explore],
         "orders.malloy",
         false,
         tmpUrl("orders.malloy"),
         [],
         [],
      );

      const row = metadata.find(
         (m) =>
            m.sourceName === "raw_sql_source" && m.metadataType === "AUTHORIZE",
      );
      assert.ok(
         row,
         "a sql_select with an embedded segment must still produce an AUTHORIZE row (deny)",
      );
      assert.deepStrictEqual(row!.authorize, [
         { expression: "false", given_names: [], route: "authorize" },
      ]);
   });

   test("an `inherits` chain deeper than ANCESTOR_WALK_MAX_DEPTH fails closed (deny)", () => {
      // Build 33 own-annotated ancestors (none carrying #(authorize)) linked
      // via `inherits` — one level past the 32-level cap, so the walk stops
      // with a still-non-empty remainder and must fail closed. This pins a
      // DELIBERATE choice: on a depth-cap deny the caller drops the
      // file-level prefix entirely rather than OR it in — stricter,
      // and kept that way (see `ancestorGateExprs`'s doc).
      const chain: { blockNotes: { text: string }[]; inherits?: unknown }[] =
         [];
      for (let i = 0; i < 33; i++) {
         chain.push({ blockNotes: [{ text: `#(doc) "level ${i}"` }] });
      }
      for (let i = 0; i < chain.length - 1; i++) {
         chain[i]!.inherits = chain[i + 1];
      }
      const explore = {
         name: "child_src",
         location: { url: tmpUrl("model.malloy") },
         getTaglines: noTaglines,
         structDef: {
            parameters: {},
            annotations: { blockNotes: [], inherits: chain[0] },
         },
      } as unknown as Explore;

      const metadata = sourceExtraction.extractMetadataFromExplores(
         [explore],
         "model.malloy",
         false,
         tmpUrl("model.malloy"),
         [],
         [{ route: "authorize", expr: "$REGION = 'us'" }],
      );

      const row = metadata.find(
         (m) => m.sourceName === "child_src" && m.metadataType === "AUTHORIZE",
      );
      assert.ok(
         row,
         "a too-deep inherits chain must still produce an AUTHORIZE row (deny)",
      );
      assert.deepStrictEqual(row!.authorize, [
         { expression: "false", given_names: [], route: "authorize" },
      ]);
   });

   test("a gate exactly ANCESTOR_WALK_MAX_DEPTH ancestors deep is still found, not failed closed", () => {
      // 32 ancestors (indices 0..31), none gated except the LAST — the
      // farthest one the depth-capped loop (`depth < 32`, i.e. depth 0..31)
      // still visits. An off-by-one the other way would deny this and
      // over-restrict a source publisher would still gate correctly.
      const chain: { blockNotes: { text: string }[]; inherits?: unknown }[] =
         [];
      for (let i = 0; i < 32; i++) {
         chain.push({ blockNotes: [{ text: `#(doc) "level ${i}"` }] });
      }
      chain[31]!.blockNotes = [{ text: "#(authorize) \"$ROLE = 'admin'\"" }];
      for (let i = 0; i < chain.length - 1; i++) {
         chain[i]!.inherits = chain[i + 1];
      }
      const explore = {
         name: "child_src",
         location: { url: tmpUrl("model.malloy") },
         getTaglines: noTaglines,
         structDef: {
            type: "table",
            parameters: {},
            annotations: { blockNotes: [], inherits: chain[0] },
         },
      } as unknown as Explore;

      const metadata = sourceExtraction.extractMetadataFromExplores(
         [explore],
         "model.malloy",
         false,
         tmpUrl("model.malloy"),
         [],
         [{ route: "authorize", expr: "$REGION = 'us'" }],
      );

      const row = metadata.find(
         (m) => m.sourceName === "child_src" && m.metadataType === "AUTHORIZE",
      );
      assert.ok(
         row,
         "a gate exactly at the depth cap must still produce an AUTHORIZE row",
      );
      assert.deepStrictEqual(row!.authorize, [
         {
            expression: "$REGION = 'us'",
            given_names: ["REGION"],
            route: "authorize",
         },
         {
            expression: "$ROLE = 'admin'",
            given_names: ["ROLE"],
            route: "authorize",
         },
      ]);
   });
});

describe("sourceExtraction.extractMetadataFromExplores — join-aware authorize (compile-backed)", () => {
   // Real multi-file compiles, not fake Explore/Field stubs. A hand-built
   // stub can set `location: undefined` or a note without `at` — the exact
   // shapes `filterDirectlyDeclaredNotes` treats as "can't prove inherited,
   // keep it" — which would hide a child gate dropped at extraction for a
   // real, cross-file-located note. A real compile always populates both
   // `location` and `at`.
   let packageRoot: string;
   beforeEach(async () => {
      packageRoot = await mkdtemp(path.join(tmpdir(), "pjoin-"));
   });
   afterEach(async () => {
      await rm(packageRoot, { recursive: true, force: true });
   });

   async function compileMetadata(
      files: Record<string, string>,
      entryRelativePath: string,
      modelGivens: ApiGiven[] = [],
      modelAuthorize: GateExpr[] = [],
      nonQueryableSourceNames: Set<string> = new Set(),
   ) {
      const compiled = await compileEntryModel(
         packageRoot,
         files,
         entryRelativePath,
      );
      const entryAbs = path.join(packageRoot, entryRelativePath);
      const source = files[entryRelativePath]!;
      return sourceExtraction.extractMetadataFromExplores(
         compiled.explores as unknown as Explore[],
         entryRelativePath,
         false,
         new URL("file://" + entryAbs).href,
         modelGivens,
         modelAuthorize,
         nonQueryableSourceNames,
         source,
         packageRoot,
      );
   }

   test("deep-import named join emits an AUTHORIZE (+ GIVENS) row for the join-only source", async () => {
      // Two-hop import chain (entry → bridge → items): `order_items` is
      // never in `compiled.explores` for `entry.malloy` (Malloy's
      // transitive-import rule), so it's only reachable through
      // `bridge_model`'s NAMED join (no `extend`): the deep-import case. Malloy copies `order_items`'s own `#(authorize)`
      // onto the join field's structDef with the note's `at.url` pointing
      // at `items.malloy` (order_items's own file) while the join field's
      // `location.url` is `bridge.malloy` — a real cross-file, non-
      // contiguous note that `filterDirectlyDeclaredNotes` would drop.
      const entryPath = "entry.malloy";
      const files = {
         "sources/items.malloy":
            `#(authorize) "$TENANT_ID = '123'"\n` +
            `source: order_items is duckdb.table('t') extend { dimension: oid is id }\n`,
         "models/bridge.malloy":
            `import "../sources/items.malloy"\n` +
            `source: bridge_model is duckdb.table('t') extend {\n` +
            `  join_many: order_items on order_items.oid = id\n` +
            `}\n`,
         [entryPath]: `import "./models/bridge.malloy"\n`,
      };
      const modelGivens: ApiGiven[] = [{ name: "TENANT_ID", type: "string" }];
      const metadata = await compileMetadata(files, entryPath, modelGivens);

      const authorizeRows = metadata.filter(
         (m) => m.metadataType === "AUTHORIZE",
      );
      const orderItemsRow = authorizeRows.find(
         (r) => r.sourceName === "order_items",
      );
      assert.ok(
         orderItemsRow,
         "deep-import join-only source must get an AUTHORIZE row — got: " +
            JSON.stringify(authorizeRows),
      );
      assert.deepStrictEqual(orderItemsRow!.authorize, [
         {
            expression: "$TENANT_ID = '123'",
            given_names: ["TENANT_ID"],
            route: "authorize",
         },
      ]);
      // `original_source_path` doesn't resolve (order_items isn't in the
      // entry's sourceToModelPath map — it's not top-level either), so
      // the row falls back to the join entity's own model path, which is the
      // entry file.
      assert.strictEqual(orderItemsRow!.modelFilePath, entryPath);

      const givensRows = metadata.filter((m) => m.metadataType === "GIVENS");
      const orderItemsGivens = givensRows.find(
         (r) => r.sourceName === "order_items",
      );
      assert.ok(
         orderItemsGivens,
         "join-only source must also get a GIVENS row so a lookup of its givens isn't empty",
      );
      assert.deepStrictEqual(orderItemsGivens!.givens, modelGivens);
   });

   test("inline extend-join alias emits a dotted AUTHORIZE row", async () => {
      // `a is order_items extend { ... }` — parseJoinCode reports
      // `is_nested_index: true`. The gate is declared directly above the
      // join line (same file, contiguous to the join field's own
      // location) — the extend/inherits base-vs-own distinction still
      // applies here too.
      const files = {
         "items.malloy": `source: order_items is duckdb.table('t') extend { dimension: oid is id }\n`,
         "entry.malloy":
            `import "./items.malloy"\n` +
            `source: parent_src is duckdb.table('t') extend {\n` +
            `  #(authorize) "$REGION = 'us'"\n` +
            `  join_one: a is order_items extend { dimension: z is oid } on a.oid = id\n` +
            `}\n`,
      };
      const metadata = await compileMetadata(files, "entry.malloy");

      const authorizeRows = metadata.filter(
         (m) => m.metadataType === "AUTHORIZE",
      );
      const nestedRow = authorizeRows.find(
         (r) => r.sourceName === "parent_src.a",
      );
      assert.ok(
         nestedRow,
         "inline extend-join alias must get a dotted AUTHORIZE row",
      );
      assert.deepStrictEqual(nestedRow!.authorize, [
         {
            expression: "$REGION = 'us'",
            given_names: ["REGION"],
            route: "authorize",
         },
      ]);
      assert.strictEqual(nestedRow!.modelFilePath, "items.malloy");
   });

   test("direct-table join alias can carry its own #(authorize) and gets a dotted row (SHOULD-1)", async () => {
      // `x is duckdb.table(...)` has no separate named source
      // (`original_source_name: null`), but the gate declared directly
      // above the join line is the join field's OWN annotation (same
      // file, contiguous) — `get_join_link` keys ANY `is_nested_index`
      // target as dotted `${parent}.${alias}` regardless of
      // `original_source_name`, so this must not be skipped.
      const files = {
         "entry.malloy":
            `source: parent_src is duckdb.table('t') extend {\n` +
            `  #(authorize) "$ROLE = 'admin'"\n` +
            `  join_one: x is duckdb.table('t') on x.id = id\n` +
            `}\n`,
      };
      const metadata = await compileMetadata(files, "entry.malloy");

      const authorizeRows = metadata.filter(
         (m) => m.metadataType === "AUTHORIZE",
      );
      const directTableRow = authorizeRows.find(
         (r) => r.sourceName === "parent_src.x",
      );
      assert.ok(
         directTableRow,
         "direct-table join alias carrying its own gate must get a dotted AUTHORIZE row — got: " +
            JSON.stringify(authorizeRows),
      );
      assert.deepStrictEqual(directTableRow!.authorize, [
         {
            expression: "$ROLE = 'admin'",
            given_names: ["ROLE"],
            route: "authorize",
         },
      ]);
      assert.strictEqual(directTableRow!.modelFilePath, "entry.malloy");
   });

   test("same-file non-queryable child joined from a queryable root keeps the file-level gate (no clobbering duplicate)", async () => {
      // `child_src` is TOP-LEVEL in this same file (a `source:` declaration
      // of its own), but non-queryable, and joined from `parent_src` (the
      // queryable root) — `shouldRecurse`'s `targetIsNonQueryable &&
      // rootIsQueryable` branch forces the join pass to walk it even though
      // it's already reachable (and already gets its own correct row) via
      // the top-level loop. The join pass must neither drop the file-level
      // gate for a same-file source nor re-emit a second row for one the
      // top-level loop already reported: the base/shared source is declared
      // before the fact source that joins it, so a second, wrongly gated row
      // would be written last and win a consumer's last-write-wins fold by
      // (modelFilePath, sourceName).
      // `child_src` also carries its OWN source-level `#(authorize)` so a
      // wrong join-path row (file-level gate dropped, source-level kept)
      // would have non-empty content and be pushed, rather than being
      // swallowed by the `childAuthorize.length > 0` guard.
      const entryPath = "entry.malloy";
      const files = {
         [entryPath]:
            `#(authorize) "$REGION = 'us'"\n` +
            `source: child_src is duckdb.table('t') extend { dimension: oid is id }\n` +
            `source: parent_src is duckdb.table('t') extend {\n` +
            `  join_one: child_src on child_src.oid = id\n` +
            `}\n`,
      };
      const modelAuthorize: GateExpr[] = [
         { route: "authorize", expr: "$ROLE = 'admin'" },
      ];
      const metadata = await compileMetadata(
         files,
         entryPath,
         /* modelGivens */ [],
         modelAuthorize,
         /* nonQueryableSourceNames */ new Set(["child_src"]),
      );

      const childRows = metadata.filter(
         (m) => m.metadataType === "AUTHORIZE" && m.sourceName === "child_src",
      );
      assert.strictEqual(
         childRows.length,
         1,
         "expected exactly one AUTHORIZE row for the same-file non-queryable " +
            "child — got: " +
            JSON.stringify(childRows),
      );
      // File-level first, then source-level — matches
      // `buildAuthorizeForExplore`'s ordering.
      assert.deepStrictEqual(childRows[0]!.authorize, [
         {
            expression: "$ROLE = 'admin'",
            given_names: ["ROLE"],
            route: "authorize",
         },
         {
            expression: "$REGION = 'us'",
            given_names: ["REGION"],
            route: "authorize",
         },
      ]);
      assert.strictEqual(childRows[0]!.modelFilePath, entryPath);
   });

   test("REGRESSION PIN: extend-derived named-join target inherits its base's #(authorize) via getRawTaglines (current behavior — do not change without intent)", async () => {
      // `order_items is base_src extend {...}` copies `base_src`'s own
      // `#(authorize)` onto `order_items`'s structDef via Malloy's own
      // `extend` semantics. Reached through a NAMED join (no `extend` at the
      // join site, so `is_nested_index` is false), `childTaglines` uses
      // `getRawTaglines` — which applies no contiguity filtering — so the
      // inherited note surfaces here even though `order_items` declares no
      // gate of its own. The top-level case now also inherits an extend
      // base's gate (`gateExprsForStruct`), so this is no longer an
      // asymmetry with it; this test only pins the CURRENT behavior so a
      // future change to it is deliberate, not accidental.
      const entryPath = "entry.malloy";
      const files = {
         "sources/base.malloy":
            `#(authorize) "$TENANT_ID = '123'"\n` +
            `source: base_src is duckdb.table('t') extend { dimension: oid is id }\n`,
         "sources/items.malloy":
            `import "../sources/base.malloy"\n` +
            `source: order_items is base_src extend { dimension: z is oid }\n`,
         "models/bridge.malloy":
            `import "../sources/items.malloy"\n` +
            `source: bridge_model is duckdb.table('t') extend {\n` +
            `  join_many: order_items on order_items.oid = id\n` +
            `}\n`,
         [entryPath]: `import "./models/bridge.malloy"\n`,
      };
      const metadata = await compileMetadata(files, entryPath);

      const authorizeRows = metadata.filter(
         (m) => m.metadataType === "AUTHORIZE",
      );
      const orderItemsRow = authorizeRows.find(
         (r) => r.sourceName === "order_items",
      );
      assert.ok(
         orderItemsRow,
         "extend-derived named-join target must still get an AUTHORIZE row — got: " +
            JSON.stringify(authorizeRows),
      );
      assert.deepStrictEqual(orderItemsRow!.authorize, [
         {
            expression: "$TENANT_ID = '123'",
            given_names: ["TENANT_ID"],
            route: "authorize",
         },
      ]);
   });

   test("named-join target whose gate the base DEMOTED to `annotations.inherits` still gets an AUTHORIZE row", async () => {
      // The `REGRESSION PIN` above covers the shape where Malloy COPIES the
      // base's gate onto the target (target declares no annotation at all), so
      // raw own notes find it. Add ONE unrelated annotation to the target and
      // Malloy moves the base's annotations to `annotations.inherits` instead —
      // own notes then carry no gate, and before the `inherits` fallback in
      // `buildAuthorizeForExplore` this join-only source got NO row at all
      // while publisher 0.0.237 gates it (its walk is inheritance-aware).
      // `order_items` is reachable ONLY through the join here (the entry
      // selectively imports `bridge_model`), so the join path is the only
      // producer of its row.
      const entryPath = "entry.malloy";
      const files = {
         "sources/base.malloy":
            `#(authorize) "$TENANT_ID = '123'"\n` +
            `source: base_src is duckdb.table('t') extend { dimension: oid is id }\n`,
         "sources/items.malloy":
            `import "../sources/base.malloy"\n` +
            `#(doc) "order items, derived"\n` +
            `source: order_items is base_src extend { dimension: z is oid }\n`,
         "models/bridge.malloy":
            `import "../sources/items.malloy"\n` +
            `source: bridge_model is duckdb.table('t') extend {\n` +
            `  join_many: order_items on order_items.oid = id\n` +
            `}\n`,
         [entryPath]: `import { bridge_model } from "./models/bridge.malloy"\n`,
      };
      const metadata = await compileMetadata(files, entryPath);

      const authorizeRows = metadata.filter(
         (m) => m.metadataType === "AUTHORIZE",
      );
      const orderItemsRow = authorizeRows.find(
         (r) => r.sourceName === "order_items",
      );
      assert.ok(
         orderItemsRow,
         "a join-only target inheriting its gate via `inherits` must still get an AUTHORIZE row — got: " +
            JSON.stringify(authorizeRows),
      );
      assert.deepStrictEqual(orderItemsRow!.authorize, [
         {
            expression: "$TENANT_ID = '123'",
            given_names: ["TENANT_ID"],
            route: "authorize",
         },
      ]);
   });

   test("a join-only target's own malformed #(authorize), with no gated ancestor, denies rather than reporting ungated", async () => {
      // `order_items` declares its OWN malformed gate directly (not
      // inherited), and its base `base_src` carries no gate at all — so
      // `ownTaglines` is non-empty-but-all-malformed, not empty, and the
      // ancestor walk finds nothing either. The two cases must not be
      // conflated: falling back to the (empty) ancestor walk would report
      // `order_items` as fully ungated instead of denying. `order_items` is reachable ONLY through the join (the entry
      // selectively imports `bridge_model`), so this exercises the
      // `ownTaglines !== undefined` branch, not the entry-point one.
      const entryPath = "entry.malloy";
      const files = {
         "sources/base.malloy": `source: base_src is duckdb.table('t') extend { dimension: oid is id }\n`,
         "sources/items.malloy":
            `import "../sources/base.malloy"\n` +
            `#(authorize) "$ROLE = 'admin'\n` +
            `source: order_items is base_src extend { dimension: z is oid }\n`,
         "models/bridge.malloy":
            `import "../sources/items.malloy"\n` +
            `source: bridge_model is duckdb.table('t') extend {\n` +
            `  join_many: order_items on order_items.oid = id\n` +
            `}\n`,
         [entryPath]: `import { bridge_model } from "./models/bridge.malloy"\n`,
      };
      const metadata = await compileMetadata(files, entryPath);

      const authorizeRows = metadata.filter(
         (m) => m.metadataType === "AUTHORIZE",
      );
      const orderItemsRow = authorizeRows.find(
         (r) => r.sourceName === "order_items",
      );
      assert.ok(
         orderItemsRow,
         "a join-only target with a malformed own gate and no gated ancestor must deny, not report absent — got: " +
            JSON.stringify(authorizeRows),
      );
      assert.deepStrictEqual(orderItemsRow!.authorize, [
         { expression: "false", given_names: [], route: "authorize" },
      ]);
   });

   test("a join-only target's own gate — one valid tagline plus one malformed sibling — denies rather than reporting only the valid gate", async () => {
      // `order_items` declares TWO own `#(authorize)` taglines: one valid, one
      // malformed. The valid tagline must not mask the malformed one: the
      // source denies rather than reporting only the valid gate. `order_items` is reachable ONLY through the join
      // (the entry selectively imports `bridge_model`), so this exercises the
      // `ownTaglines !== undefined` branch, not the entry-point one.
      const entryPath = "entry.malloy";
      const files = {
         "sources/base.malloy": `source: base_src is duckdb.table('t') extend { dimension: oid is id }\n`,
         "sources/items.malloy":
            `import "../sources/base.malloy"\n` +
            `#(authorize) "$TENANT_ID = '123'"\n` +
            `#(authorize) "$ROLE = 'admin'\n` +
            `source: order_items is base_src extend { dimension: z is oid }\n`,
         "models/bridge.malloy":
            `import "../sources/items.malloy"\n` +
            `source: bridge_model is duckdb.table('t') extend {\n` +
            `  join_many: order_items on order_items.oid = id\n` +
            `}\n`,
         [entryPath]: `import { bridge_model } from "./models/bridge.malloy"\n`,
      };
      const metadata = await compileMetadata(files, entryPath);

      const authorizeRows = metadata.filter(
         (m) => m.metadataType === "AUTHORIZE",
      );
      const orderItemsRow = authorizeRows.find(
         (r) => r.sourceName === "order_items",
      );
      assert.ok(
         orderItemsRow,
         "a join-only target with one valid and one malformed own tagline must deny — got: " +
            JSON.stringify(authorizeRows),
      );
      assert.deepStrictEqual(orderItemsRow!.authorize, [
         { expression: "false", given_names: [], route: "authorize" },
      ]);
   });

   test("a join-only query_source over a locked base inherits the base's gate (not just the inherits chain)", async () => {
      // The query_source counterpart of the malformed test above. `laundered`'s
      // gate lives on `query.structRef`, NOT on `annotations.inherits`, so an
      // ancestor-only fallback finds nothing and emits no row at all — the
      // laundering closed at the entry point would reopen at the join site
      // because a join-only source has no second pass to catch it.
      const entryPath = "entry.malloy";
      const files = {
         "sources/base.malloy":
            `#(authorize) "false"\n` +
            `source: locked_src is duckdb.table('t') extend { dimension: secret is id }\n`,
         "sources/items.malloy":
            `import "../sources/base.malloy"\n` +
            `source: laundered is locked_src -> { select: secret }\n`,
         "models/bridge.malloy":
            `import "../sources/items.malloy"\n` +
            `source: bridge_model is duckdb.table('t') extend {\n` +
            `  join_many: laundered on laundered.secret = id\n` +
            `}\n`,
         [entryPath]: `import { bridge_model } from "./models/bridge.malloy"\n`,
      };
      const metadata = await compileMetadata(files, entryPath);

      const authorizeRows = metadata.filter(
         (m) => m.metadataType === "AUTHORIZE",
      );
      const launderedRow = authorizeRows.find(
         (r) => r.sourceName === "laundered",
      );
      assert.ok(
         launderedRow,
         "a join-only query_source over a locked base must inherit its gate, not report ungated — got: " +
            JSON.stringify(authorizeRows),
      );
      assert.deepStrictEqual(launderedRow!.authorize, [
         { expression: "false", given_names: [], route: "authorize" },
      ]);
   });

   test("a join-only bare composite with a gated member is gated (static-safe rule reaches the join site)", async () => {
      // The composite counterpart: the gate lives on `sources` (a member),
      // which an ancestor-only fallback never walks either.
      const entryPath = "entry.malloy";
      const files = {
         "sources/members.malloy":
            `##! experimental.composite_sources\n` +
            `source: mem_a is duckdb.table('t') extend { dimension: oid is id }\n` +
            `#(authorize) "$ROLE = 'admin'"\n` +
            `source: mem_b is duckdb.table('t') extend { dimension: oid is id }\n` +
            `source: composed_src is compose(mem_a, mem_b)\n`,
         "models/bridge.malloy":
            `import "../sources/members.malloy"\n` +
            `source: bridge_model is duckdb.table('t') extend {\n` +
            `  join_many: composed_src on composed_src.oid = id\n` +
            `}\n`,
         [entryPath]: `import { bridge_model } from "./models/bridge.malloy"\n`,
      };
      const metadata = await compileMetadata(files, entryPath);

      const authorizeRows = metadata.filter(
         (m) => m.metadataType === "AUTHORIZE",
      );
      const composedRow = authorizeRows.find(
         (r) => r.sourceName === "composed_src",
      );
      assert.ok(
         composedRow,
         "a join-only bare composite with a gated member must get an AUTHORIZE row — got: " +
            JSON.stringify(authorizeRows),
      );
      assert.deepStrictEqual(composedRow!.authorize, [
         {
            expression: "$ROLE = 'admin'",
            given_names: ["ROLE"],
            route: "authorize",
         },
      ]);
   });

   test("KNOWN GAP: an ANNOTATED inline extend-join of a gated base still reports ungated (needs the sourceRegistry walk)", async () => {
      // Not the `inherits` case the test above closes: Malloy's `join.ts`
      // REPLACES an annotated join's struct annotations outright — no
      // `inherits` link survives, only `sourceID`/`referenceID`. Recovering the
      // base's gate from those takes the `sourceRegistry` walk
      // (`resolveDeclaredSource` in ../gate_registry_walk.ts), which this
      // extraction does not use. Pinned so the residual is visible and a
      // future fix has a test to flip.
      const entryPath = "entry.malloy";
      const files = {
         "sources/base.malloy":
            `#(authorize) "$ROLE = 'admin'"\n` +
            `source: gated_base is duckdb.table('t') extend { dimension: oid is id }\n`,
         [entryPath]:
            `import "./sources/base.malloy"\n` +
            `source: parent_src is duckdb.table('t') extend {\n` +
            `  #(doc) "the join alias"\n` +
            `  join_one: j is gated_base extend { dimension: extra is oid } on j.oid = id\n` +
            `}\n`,
      };
      const metadata = await compileMetadata(files, entryPath);
      assert.strictEqual(
         metadata.find(
            (m) =>
               m.sourceName === "parent_src.j" &&
               m.metadataType === "AUTHORIZE",
         ),
         undefined,
      );
   });

   test("a plain raw-SQL source emits no AUTHORIZE row (over-denial regression)", async () => {
      // `duckdb.sql(...)` with no embedded query compiles to `sql_select`
      // with `selectSegments` absent — an earlier allowlist regression treated
      // every `sql_select` as an unmodelled shape and denied it, which would
      // fail this assertion (a bogus AUTHORIZE row for `x`).
      const entryPath = "entry.malloy";
      const files = {
         [entryPath]: `source: x is duckdb.sql("select 1 as id")\n`,
      };
      const metadata = await compileMetadata(files, entryPath);

      assert.strictEqual(
         metadata.find(
            (m) => m.sourceName === "x" && m.metadataType === "AUTHORIZE",
         ),
         undefined,
         "an ungated raw-SQL source must not get an AUTHORIZE row",
      );
   });

   test("an inline record dimension emits no AUTHORIZE deny row for its own nesting", async () => {
      // A record/array field (nested repeated record, inline record
      // dimension) is walked as an `isExploreField()` join candidate by
      // `extractJoinAuthorizeMetadata` — its structDef reaches
      // `gateExprsForStruct` with `type: "record"`. An earlier allowlist
      // regression treated that as unmodelled and denied it, which would
      // fail this assertion (a bogus AUTHORIZE row for the nested field).
      const entryPath = "entry.malloy";
      const files = {
         [entryPath]:
            `source: x is duckdb.table('t') extend {\n` +
            `  dimension: r is {a is 1}\n` +
            `}\n`,
      };
      const metadata = await compileMetadata(files, entryPath);

      const denyRows = metadata.filter(
         (m) =>
            m.metadataType === "AUTHORIZE" &&
            m.sourceName !== "x" &&
            JSON.stringify(m.authorize) ===
               JSON.stringify([
                  { expression: "false", given_names: [], route: "authorize" },
               ]),
      );
      assert.deepStrictEqual(
         denyRows,
         [],
         "an inline record dimension must not get a deny AUTHORIZE row for its own nesting — got: " +
            JSON.stringify(denyRows),
      );
   });
});

describe("sourceExtraction.extractMetadataFromExplores — entry-point #(authorize) inheritance through extend (compile-backed)", () => {
   let packageRoot: string;
   beforeEach(async () => {
      packageRoot = await mkdtemp(path.join(tmpdir(), "pextauth-"));
   });
   afterEach(async () => {
      await rm(packageRoot, { recursive: true, force: true });
   });

   async function compileMetadata(
      files: Record<string, string>,
      entryRelativePath: string,
   ) {
      const compiled = await compileEntryModel(
         packageRoot,
         files,
         entryRelativePath,
      );
      const entryAbs = path.join(packageRoot, entryRelativePath);
      const modelAuthorize = sourceExtraction.extractAuthorizeFromModel(
         compiled,
         entryRelativePath,
      );
      return sourceExtraction.extractMetadataFromExplores(
         compiled.explores as unknown as Explore[],
         entryRelativePath,
         false,
         new URL("file://" + entryAbs).href,
         [],
         modelAuthorize,
      );
   }

   const authorizeFor = (
      metadata: ReturnType<typeof sourceExtraction.extractMetadataFromExplores>,
      sourceName: string,
   ) =>
      metadata.find(
         (m) => m.sourceName === sourceName && m.metadataType === "AUTHORIZE",
      );

   test("child_src is base_src extend {} with no own annotation inherits the base's gate", async () => {
      const entryPath = "entry.malloy";
      const files = {
         [entryPath]:
            `#(authorize) "$ROLE = 'admin'"\n` +
            `source: base_src is duckdb.table('t') extend { dimension: oid is id }\n` +
            `source: child_src is base_src extend {}\n`,
      };
      const metadata = await compileMetadata(files, entryPath);
      const row = authorizeFor(metadata, "child_src");
      assert.ok(
         row,
         "extend-derived child with no own annotation must inherit the base's gate",
      );
      assert.deepStrictEqual(row!.authorize, [
         {
            expression: "$ROLE = 'admin'",
            given_names: ["ROLE"],
            route: "authorize",
         },
      ]);
   });

   test("child_src is base_src (plain alias, no extend block) inherits the base's gate", async () => {
      const entryPath = "entry.malloy";
      const files = {
         [entryPath]:
            `#(authorize) "$ROLE = 'admin'"\n` +
            `source: base_src is duckdb.table('t') extend { dimension: oid is id }\n` +
            `source: child_src is base_src\n`,
      };
      const metadata = await compileMetadata(files, entryPath);
      const row = authorizeFor(metadata, "child_src");
      assert.ok(row, "plain-alias child must inherit the base's gate");
      assert.deepStrictEqual(row!.authorize, [
         {
            expression: "$ROLE = 'admin'",
            given_names: ["ROLE"],
            route: "authorize",
         },
      ]);
   });

   test("child_src extending a gated base with an unrelated own annotation inherits via the `inherits` chain", async () => {
      const entryPath = "entry.malloy";
      const files = {
         "sources/base.malloy":
            `#(authorize) "$ROLE = 'admin'"\n` +
            `source: base_src is duckdb.table('t') extend { dimension: oid is id }\n`,
         [entryPath]:
            `import "./sources/base.malloy"\n` +
            `#(doc) "child's own description"\n` +
            `source: child_src is base_src extend { dimension: z is oid }\n`,
      };
      const metadata = await compileMetadata(files, entryPath);
      const row = authorizeFor(metadata, "child_src");
      assert.ok(
         row,
         "child with an unrelated own annotation must still inherit the base's gate via `inherits`",
      );
      assert.deepStrictEqual(row!.authorize, [
         {
            expression: "$ROLE = 'admin'",
            given_names: ["ROLE"],
            route: "authorize",
         },
      ]);
   });

   test("child_src declaring its own #(authorize) replaces the base's gate, not unioned with it", async () => {
      const entryPath = "entry.malloy";
      const files = {
         "sources/base.malloy":
            `#(authorize) "$ROLE = 'admin'"\n` +
            `source: base_src is duckdb.table('t') extend { dimension: oid is id }\n`,
         [entryPath]:
            `import "./sources/base.malloy"\n` +
            `#(authorize) "$ROLE = 'analyst'"\n` +
            `source: child_src is base_src extend { dimension: z is oid }\n`,
      };
      const metadata = await compileMetadata(files, entryPath);
      const row = authorizeFor(metadata, "child_src");
      assert.ok(
         row,
         "child declaring its own gate must still get an AUTHORIZE row",
      );
      assert.deepStrictEqual(
         row!.authorize,
         [
            {
               expression: "$ROLE = 'analyst'",
               given_names: ["ROLE"],
               route: "authorize",
            },
         ],
         "child's own gate must replace the base's, not union with it",
      );
   });

   test("child_src with no gate anywhere (own or ancestor) is ungated", async () => {
      const entryPath = "entry.malloy";
      const files = {
         [entryPath]:
            `source: base_src is duckdb.table('t') extend { dimension: oid is id }\n` +
            `source: child_src is base_src extend {}\n`,
      };
      const metadata = await compileMetadata(files, entryPath);
      assert.strictEqual(
         authorizeFor(metadata, "child_src"),
         undefined,
         "child with no gate anywhere must not get an AUTHORIZE row",
      );
   });

   test("nearest of two gated ancestors wins (base → mid → child, mid's gate applies, not base's)", async () => {
      const entryPath = "entry.malloy";
      const files = {
         [entryPath]:
            `#(authorize) "$ROLE = 'director'"\n` +
            `source: base_src is duckdb.table('t') extend { dimension: oid is id }\n` +
            `#(authorize) "$ROLE = 'manager'"\n` +
            `source: mid_src is base_src extend { dimension: z is oid }\n` +
            `#(doc) "child's own description"\n` +
            `source: child_src is mid_src extend { dimension: w is z }\n`,
      };
      const metadata = await compileMetadata(files, entryPath);
      const row = authorizeFor(metadata, "child_src");
      assert.ok(
         row,
         "child inheriting through a two-level ancestor chain must still get a gate",
      );
      assert.deepStrictEqual(
         row!.authorize,
         [
            {
               expression: "$ROLE = 'manager'",
               given_names: ["ROLE"],
               route: "authorize",
            },
         ],
         "the nearer ancestor (mid_src)'s gate must win over the farther one (base_src)",
      );
   });

   test("the file-level prefix is still prepended to an inherited (ancestor) gate", async () => {
      const entryPath = "entry.malloy";
      const files = {
         "sources/base.malloy":
            `#(authorize) "$ROLE = 'admin'"\n` +
            `source: base_src is duckdb.table('t') extend { dimension: oid is id }\n`,
         [entryPath]:
            `##(authorize) "$REGION = 'us'"\n` +
            `import "./sources/base.malloy"\n` +
            `#(doc) "child's own description"\n` +
            `source: child_src is base_src extend { dimension: z is oid }\n`,
      };
      const metadata = await compileMetadata(files, entryPath);
      const row = authorizeFor(metadata, "child_src");
      assert.ok(
         row,
         "child inheriting an ancestor gate must still carry the file-level gate too",
      );
      assert.deepStrictEqual(row!.authorize, [
         {
            expression: "$REGION = 'us'",
            given_names: ["REGION"],
            route: "authorize",
         },
         {
            expression: "$ROLE = 'admin'",
            given_names: ["ROLE"],
            route: "authorize",
         },
      ]);
   });

   test("parent_src that only JOINS a gated source (does not extend it) is ungated — joins are not walked", async () => {
      const entryPath = "entry.malloy";
      const files = {
         "sources/gated.malloy":
            `#(authorize) "$ROLE = 'admin'"\n` +
            `source: gated_src is duckdb.table('t') extend { dimension: oid is id }\n`,
         [entryPath]:
            `import "./sources/gated.malloy"\n` +
            `source: parent_src is duckdb.table('t') extend {\n` +
            `  join_one: gated_src on gated_src.oid = id\n` +
            `}\n`,
      };
      const metadata = await compileMetadata(files, entryPath);
      assert.strictEqual(
         authorizeFor(metadata, "parent_src"),
         undefined,
         "a source that only joins a gated source must remain ungated at its own entry-point row",
      );
   });

   test("a query_source naming a locked source (source: laundered is locked -> {...}) inherits the base's gate", async () => {
      const entryPath = "entry.malloy";
      const files = {
         [entryPath]:
            `#(authorize) "false"\n` +
            `source: locked_src is duckdb.table('t') extend { dimension: secret is id }\n` +
            `source: laundered is locked_src -> { select: secret }\n`,
      };
      const metadata = await compileMetadata(files, entryPath);
      const row = authorizeFor(metadata, "laundered");
      assert.ok(
         row,
         "a query_source derived from a locked source must inherit the base's gate, not launder it away",
      );
      assert.deepStrictEqual(row!.authorize, [
         { expression: "false", given_names: [], route: "authorize" },
      ]);
   });

   test("a chained query_source (double is laundered -> {...}) still inherits the original base's gate", async () => {
      const entryPath = "entry.malloy";
      const files = {
         [entryPath]:
            `#(authorize) "false"\n` +
            `source: locked_src is duckdb.table('t') extend { dimension: secret is id }\n` +
            `source: laundered is locked_src -> { select: secret }\n` +
            `source: double is laundered -> { select: secret }\n`,
      };
      const metadata = await compileMetadata(files, entryPath);
      const row = authorizeFor(metadata, "double");
      assert.ok(
         row,
         "a query_source chained two derivations deep must still inherit the gate",
      );
      assert.deepStrictEqual(row!.authorize, [
         { expression: "false", given_names: [], route: "authorize" },
      ]);
   });

   test("a query_source's own #(authorize) is reported with its base's lock, as the query path ANDs them", async () => {
      const entryPath = "entry.malloy";
      const files = {
         [entryPath]:
            `#(authorize) "false"\n` +
            `source: locked_src is duckdb.table('t') extend { dimension: secret is id }\n` +
            `#(authorize) "$ROLE = 'analyst'"\n` +
            `source: laundered is locked_src -> { select: secret }\n`,
      };
      const metadata = await compileMetadata(files, entryPath);
      const row = authorizeFor(metadata, "laundered");
      assert.ok(
         row,
         "a query_source declaring its own gate must still get an AUTHORIZE row",
      );
      assert.deepStrictEqual(row!.authorize, [
         {
            expression: "$ROLE = 'analyst'",
            given_names: ["ROLE"],
            route: "authorize",
         },
         { expression: "false", given_names: [], route: "authorize" },
      ]);
   });

   test("a long (40-hop) chain of ungated query_source derivations produces no AUTHORIZE rows", async () => {
      // `Model.collectEntryPointGates` (../model.ts) bounds this walk with an
      // identity `seen` set, not a numeric depth cap — a real compile's
      // `query_source` chain is always inline structRef objects (see the
      // string-structRef test below), so an ordinary ungated model must
      // never hit a cap and must never get a fabricated deny.
      const entryPath = "entry.malloy";
      let src = `source: q0 is duckdb.table('t') extend { dimension: v is id }\n`;
      for (let i = 1; i <= 40; i++) {
         src += `source: q${i} is q${i - 1} -> { select: v }\n`;
      }
      const metadata = await compileMetadata({ [entryPath]: src }, entryPath);
      assert.strictEqual(
         authorizeFor(metadata, "q40"),
         undefined,
         "a 40-hop chain of ungated derivations must not fabricate a deny",
      );
   });

   test("a gated base still propagates through a long (40-hop) chain of query_source derivations", async () => {
      const entryPath = "entry.malloy";
      let src =
         `#(authorize) "$ROLE = 'director'"\n` +
         `source: q0 is duckdb.table('t') extend { dimension: v is id }\n`;
      for (let i = 1; i <= 40; i++) {
         src += `source: q${i} is q${i - 1} -> { select: v }\n`;
      }
      const metadata = await compileMetadata({ [entryPath]: src }, entryPath);
      const row = authorizeFor(metadata, "q40");
      assert.ok(row, "the base's gate must still reach the 40th derivation");
      assert.deepStrictEqual(row!.authorize, [
         {
            expression: "$ROLE = 'director'",
            given_names: ["ROLE"],
            route: "authorize",
         },
      ]);
   });

   test("an unresolvable string structRef fails closed (deny)", () => {
      // A hand-built stub: every real compile observed produces an inline
      // structRef object, never a bare string, but the
      // type allows a string name resolved against
      // `modelDef.contents`. Exercise that branch directly against a name
      // absent from the model's own top-level sources.
      const explore = {
         name: "laundered",
         location: { url: tmpUrl("model.malloy") },
         getTaglines: () => [],
         structDef: {
            type: "query_source",
            parameters: {},
            annotations: undefined,
            query: { structRef: "nonexistent_source" },
         },
      } as unknown as Explore;

      const metadata = sourceExtraction.extractMetadataFromExplores(
         [explore],
         "model.malloy",
         false,
         tmpUrl("model.malloy"),
      );

      const row = metadata.find(
         (m) => m.sourceName === "laundered" && m.metadataType === "AUTHORIZE",
      );
      assert.ok(
         row,
         "an unresolvable string structRef must still produce an AUTHORIZE row (deny)",
      );
      assert.deepStrictEqual(row!.authorize, [
         { expression: "false", given_names: [], route: "authorize" },
      ]);
   });

   test("an own-gated query_source over a base absent from the model's sources is denied, not served on its own gate", () => {
      const explore = {
         name: "laundered",
         location: { url: tmpUrl("model.malloy") },
         getTaglines: () => [],
         structDef: {
            type: "query_source",
            parameters: {},
            // Own gates on BOTH routes, so neither route's walk reaches the base empty-handed.
            annotations: {
               blockNotes: [
                  { text: "#(access_filter) region = $REGION" },
                  { text: "#(authorize) true" },
               ],
            },
            query: { structRef: "deep_imported_base" },
         },
      } as unknown as Explore;

      const metadata = sourceExtraction.extractMetadataFromExplores(
         [explore],
         "model.malloy",
         false,
         tmpUrl("model.malloy"),
         [{ name: "REGION", type: "string" }],
      );

      const row = metadata.find(
         (m) => m.sourceName === "laundered" && m.metadataType === "AUTHORIZE",
      );
      assert.ok(
         row,
         "an own-gated source with an unresolvable base must still get an AUTHORIZE row",
      );
      assert.deepStrictEqual(row!.authorize, [
         { expression: "false", given_names: [], route: "authorize" },
      ]);
   });
});

describe("sourceExtraction.extractMetadataFromExplores — composite-source #(authorize) propagation (compile-backed)", () => {
   let packageRoot: string;
   beforeEach(async () => {
      packageRoot = await mkdtemp(path.join(tmpdir(), "pcomposite-"));
   });
   afterEach(async () => {
      await rm(packageRoot, { recursive: true, force: true });
   });

   async function compileMetadata(
      files: Record<string, string>,
      entryRelativePath: string,
   ) {
      const compiled = await compileEntryModel(
         packageRoot,
         files,
         entryRelativePath,
      );
      const entryAbs = path.join(packageRoot, entryRelativePath);
      return sourceExtraction.extractMetadataFromExplores(
         compiled.explores as unknown as Explore[],
         entryRelativePath,
         false,
         new URL("file://" + entryAbs).href,
      );
   }

   const authorizeFor = (
      metadata: ReturnType<typeof sourceExtraction.extractMetadataFromExplores>,
      sourceName: string,
   ) =>
      metadata.find(
         (m) => m.sourceName === sourceName && m.metadataType === "AUTHORIZE",
      );

   test("a query_source over a composite resolves to the gated member's branch (compositeResolvedSourceDef)", async () => {
      const entryPath = "entry.malloy";
      const files = {
         [entryPath]:
            `##! experimental.composite_sources\n` +
            `source: a is duckdb.table('t') extend { dimension: only_a is id }\n` +
            `#(authorize) "$ROLE = 'admin'"\n` +
            `source: b is duckdb.table('t') extend { dimension: only_b is id }\n` +
            `source: c is compose(a, b)\n` +
            // Selecting a field unique to `b` forces Malloy to resolve the
            // composite to `b`'s branch for this derivation.
            `source: qc is c -> { select: only_b }\n`,
      };
      const metadata = await compileMetadata(files, entryPath);
      const row = authorizeFor(metadata, "qc");
      assert.ok(
         row,
         "a query_source resolved to a gated composite member must get an AUTHORIZE row",
      );
      assert.deepStrictEqual(row!.authorize, [
         {
            expression: "$ROLE = 'admin'",
            given_names: ["ROLE"],
            route: "authorize",
         },
      ]);
   });

   test("a query_source over a composite resolved to an ungated member stays ungated", async () => {
      const entryPath = "entry.malloy";
      const files = {
         [entryPath]:
            `##! experimental.composite_sources\n` +
            `source: a is duckdb.table('t') extend { dimension: only_a is id }\n` +
            `#(authorize) "$ROLE = 'admin'"\n` +
            `source: b is duckdb.table('t') extend { dimension: only_b is id }\n` +
            `source: c is compose(a, b)\n` +
            `source: qc is c -> { select: only_a }\n`,
      };
      const metadata = await compileMetadata(files, entryPath);
      assert.strictEqual(
         authorizeFor(metadata, "qc"),
         undefined,
         "resolving to the ungated member must not fabricate a gate",
      );
   });

   test("a top-level composite with a gated member is gated (static-safe: any member gated ⇒ composite gated)", async () => {
      const entryPath = "entry.malloy";
      const files = {
         [entryPath]:
            `##! experimental.composite_sources\n` +
            `source: a is duckdb.table('t') extend { dimension: only_a is id }\n` +
            `#(authorize) "$ROLE = 'admin'"\n` +
            `source: b is duckdb.table('t') extend { dimension: only_b is id }\n` +
            `source: c is compose(a, b)\n`,
      };
      const metadata = await compileMetadata(files, entryPath);
      const row = authorizeFor(metadata, "c");
      assert.ok(
         row,
         "a bare composite with a gated member must get an AUTHORIZE row",
      );
      assert.deepStrictEqual(row!.authorize, [
         {
            expression: "$ROLE = 'admin'",
            given_names: ["ROLE"],
            route: "authorize",
         },
      ]);
   });

   test("a top-level composite with no gated member stays ungated", async () => {
      const entryPath = "entry.malloy";
      const files = {
         [entryPath]:
            `##! experimental.composite_sources\n` +
            `source: a is duckdb.table('t') extend { dimension: only_a is id }\n` +
            `source: b is duckdb.table('t') extend { dimension: only_b is id }\n` +
            `source: c is compose(a, b)\n`,
      };
      const metadata = await compileMetadata(files, entryPath);
      assert.strictEqual(
         authorizeFor(metadata, "c"),
         undefined,
         "a composite with no gated member must not be flagged (guard against blanket over-gating)",
      );
   });
});

describe("checkAuthorizeJoinBypass", () => {
   const check = sourceExtraction.checkAuthorizeJoinBypass;

   test("fires when the child carries a gate the parent doesn't", () => {
      const advisories = check(
         /* parentAuthorize */ [],
         /* childAuthorize */ [
            {
               expression: "$ROLE = 'admin'",
               given_names: ["ROLE"],
               route: "authorize",
            },
         ],
         "parent_source",
         "child_source",
      );
      assert.strictEqual(advisories.length, 1);
      assert.strictEqual(advisories[0]!.parentSourceName, "parent_source");
      assert.strictEqual(advisories[0]!.childSourceName, "child_source");
   });

   test("does not fire when the parent's gate set already covers the child's", () => {
      const gate = {
         expression: "$ROLE = 'admin'",
         given_names: ["ROLE"],
         route: "authorize" as const,
      };
      const advisories = check([gate], [gate], "parent_source", "child_source");
      assert.deepStrictEqual(advisories, []);
   });

   test("does not fire when the child carries no gate at all", () => {
      const advisories = check([], [], "parent_source", "child_source");
      assert.deepStrictEqual(advisories, []);
   });

   test("fires when the parent's gate set is a subset of the child's (partial overlap)", () => {
      // The child requires TENANT_ID that the parent does not, so reaching the
      // child through the join sheds that constraint. This is the case the
      // advisory exists to catch, and the one an OR-only reading passes
      // silently.
      const advisories = check(
         [
            {
               expression: "$ROLE = 'admin'",
               given_names: ["ROLE"],
               route: "authorize",
            },
         ],
         [
            {
               expression: "$ROLE = 'admin'",
               given_names: ["ROLE"],
               route: "authorize",
            },
            {
               expression: "$TENANT_ID = '1'",
               given_names: ["TENANT_ID"],
               route: "authorize",
            },
         ],
         "parent_source",
         "child_source",
      );
      assert.strictEqual(advisories.length, 1);
      assert.strictEqual(advisories[0]!.parentSourceName, "parent_source");
      assert.strictEqual(advisories[0]!.childSourceName, "child_source");
   });

   test("does not fire when the parent's gate set is a superset of the child's", () => {
      // The parent carries every gate the child does, plus one more, so the
      // join sheds no constraint the child imposes.
      const advisories = check(
         [
            {
               expression: "$TENANT = '1'",
               given_names: ["TENANT"],
               route: "authorize",
            },
            {
               expression: "$ROLE = 'admin'",
               given_names: ["ROLE_ADMIN"],
               route: "authorize",
            },
         ],
         [
            {
               expression: "$TENANT = '1'",
               given_names: ["TENANT"],
               route: "authorize",
            },
         ],
         "parent_source",
         "child_source",
      );
      assert.deepStrictEqual(advisories, []);
   });
});

// --------------------------------------------------------------------
// extractEntitiesFromExplores — dim-index-id resolver wiring.
//
// ``extract.ts`` computes a fingerprint for every ``#(index)``-tagged
// dimension and hands the ``${sourceName}::${fieldName}`` → fingerprint
// map down to the extraction, which stamps it onto the entity's
// ``dim_index_fingerprint``. These tests pin the wiring —
// they don't exercise Malloy compilation; they construct a fake
// Explore + Field and assert the extraction reads the map.
// --------------------------------------------------------------------

/**
 * Build a minimum-viable fake :class:`Field` that flows through
 * ``getEntityFromMalloyField`` → ``inferType`` → ``inferFieldDataType``
 * etc. without throwing. Returns a non-aggregate, non-explore, atomic
 * field (= a plain dimension), since that's the shape ``#(index)``
 * tags get applied to.
 */
function fakeDimensionField(name: string, taglines: string[] = []): Field {
   return {
      name,
      parentExplore: {
         // ``isFieldHidden`` walks the parent's tag tree; an empty
         // tagParse keeps the field non-hidden.
         tagParse: () => ({
            tag: {
               array: () => undefined,
               text: () => undefined,
            },
         }),
      },
      // No location → ``extractCodeFromEntity`` returns "" → the
      // ``is name`` detection in ``isMalloyDefined`` fails → type
      // resolves to ``Column`` instead of ``Dimension``. The extraction
      // reports the dim-index fingerprint regardless of which entity type
      // was inferred — the resolver wiring is the contract under test, not
      // the type inference.
      location: undefined,
      isAtomicField: () => true,
      isCalculation: () => false,
      isExploreField: () => false,
      type: { toString: () => "string" },
      getTaglines: () => taglines,
   } as unknown as Field;
}

describe("sourceExtraction.extractEntitiesFromExplores", () => {
   test("reports the fingerprint on a matching #(index) field, not on others", () => {
      const explore = {
         name: "orders",
         allFields: [
            // Has a fingerprint entry → gets dim_index_fingerprint.
            fakeDimensionField("country"),
            // No entry → no fingerprint, just a per-run uuid.
            fakeDimensionField("region"),
         ],
         sourceStructDef: { connection: "duckdb", dialect: "duckdb" },
         location: { url: tmpUrl("orders.malloy") },
         structDef: { parameters: {} },
      } as unknown as Explore;

      const dimIndexFingerprints = new Map<string, DimIndexFingerprintInputs>([
         [
            makeDimIndexLookupKey("orders", "country"),
            { sqlFingerprint: "abc123", requiredFilterDimensions: ["tier"] },
         ],
      ]);

      const entities = sourceExtraction.extractEntitiesFromExplores(
         [explore],
         "orders.malloy",
         "/tmp",
         /* docString */ "",
         dimIndexFingerprints,
      );

      const country = entities.find((e) => e.name === "country");
      const region = entities.find((e) => e.name === "region");
      assert.ok(country, "country entity must be present");
      assert.ok(region, "region entity must be present");
      // The #(index) field reports the fingerprint a host derives a stable id
      // from.
      assert.deepStrictEqual(country.dim_index_fingerprint, {
         sql_fingerprint: "abc123",
         required_filter_dimensions: ["tier"],
      });
      // Non-indexed dims carry no fingerprint.
      assert.strictEqual(region.dim_index_fingerprint, undefined);
   });

   test("reports no fingerprint when the map is empty (default arg)", () => {
      // Default-argument path: no resolver wired in at all, so no entity
      // carries a fingerprint.
      const explore = {
         name: "orders",
         allFields: [fakeDimensionField("country")],
         sourceStructDef: { connection: "duckdb", dialect: "duckdb" },
         location: { url: tmpUrl("orders.malloy") },
         structDef: { parameters: {} },
      } as unknown as Explore;

      const [country] = sourceExtraction.extractEntitiesFromExplores(
         [explore],
         "orders.malloy",
         "/tmp",
         /* docString */ "",
      );

      assert.ok(country);
      assert.strictEqual(country.dim_index_fingerprint, undefined);
   });

   test("simulates fingerprint-compile failure: missing entry → no fingerprint", () => {
      // ``buildDimIndexFingerprintLookup`` in ``extract.ts``
      // catches ``Malloy.compile`` failures on the per-dim fingerprint
      // fragment and SKIPS the candidate (logging a warning); the resulting
      // map simply omits that ``${source}::${field}`` key. The extraction's
      // report-or-skip contract — pinned here — guarantees the entity still
      // gets a valid (random) id for that run instead of crashing, and simply
      // carries no fingerprint (so a host has no stable id to derive).
      //
      // The observable boundary is "candidate is real but absent from the
      // map": one ``#(index)``-tagged dim makes it in; a second one doesn't
      // (= its fingerprint compile failed).
      const explore = {
         name: "orders",
         allFields: [
            fakeDimensionField("country"), // compiled OK → in map
            fakeDimensionField("city"), // simulated compile failure → not in map
         ],
         sourceStructDef: { connection: "duckdb", dialect: "duckdb" },
         location: { url: tmpUrl("orders.malloy") },
         structDef: { parameters: {} },
      } as unknown as Explore;

      const dimIndexFingerprints = new Map<string, DimIndexFingerprintInputs>([
         [
            makeDimIndexLookupKey("orders", "country"),
            { sqlFingerprint: "deadbeef", requiredFilterDimensions: [] },
         ],
      ]);

      const entities = sourceExtraction.extractEntitiesFromExplores(
         [explore],
         "orders.malloy",
         "/tmp",
         "",
         dimIndexFingerprints,
      );
      const country = entities.find((e) => e.name === "country");
      const city = entities.find((e) => e.name === "city");
      assert.ok(country && city);
      assert.deepStrictEqual(country.dim_index_fingerprint, {
         sql_fingerprint: "deadbeef",
         required_filter_dimensions: [],
      });
      // Candidate whose fingerprint compile failed: no fingerprint reported.
      assert.strictEqual(city.dim_index_fingerprint, undefined);
   });
});

describe("parseJoinCode", () => {
   // Realistic joinCode shapes: Malloy's ``location.range`` for a join
   // field starts at the *alias*, NOT at the ``join_one:`` /
   // ``join_many:`` keyword. Empirically verified against
   // ``Malloy.compile`` output — every joinCode string below matches
   // what ``extractCodeFromEntity`` actually produces in production.
   const noPaths = new Map<string, string>();

   test("plain `alias is source on …` → original source extracted, not nested", () => {
      const out = parseJoinCode(
         "cb_org is crunchbase on cb_lookup.crunchbase_uuid = cb_org.uuid",
         "cb_org",
         noPaths,
      );
      assert.strictEqual(out.original_source_name, "crunchbase");
      assert.strictEqual(out.is_nested_index, false);
   });

   test("`alias is duckdb.table(...)` → flagged as nested-index direct-table", () => {
      const out = parseJoinCode(
         "a is duckdb.table('s3://bucket/x.parquet') on x = a.x",
         "a",
         noPaths,
      );
      assert.strictEqual(out.original_source_name, null);
      assert.strictEqual(out.is_nested_index, true);
   });

   test("`alias is source extend { … }` → flagged as nested-index", () => {
      const out = parseJoinCode(
         "a is b extend { dimension: x is 1 } on x = a.x",
         "a",
         noPaths,
      );
      assert.strictEqual(out.original_source_name, "b");
      assert.strictEqual(out.is_nested_index, true);
   });

   test("`join_many` shape (no keyword in joinCode) — parses correctly", () => {
      const out = parseJoinCode(
         "event_appearances is crunchbase_event_appearances on uuid = event_appearances.participant_uuid",
         "event_appearances",
         noPaths,
      );
      assert.strictEqual(
         out.original_source_name,
         "crunchbase_event_appearances",
      );
      assert.strictEqual(
         out.is_nested_index,
         false,
         "plain `alias is source on ...` is NOT nested-index — no extend/include, no direct-table",
      );
   });

   test("`alias with key` (no `is` clause) → original_source_name falls back to fieldName", () => {
      const out = parseJoinCode("aircraft with tail_num", "aircraft", noPaths);
      assert.strictEqual(out.original_source_name, "aircraft");
      assert.strictEqual(out.is_nested_index, false);
   });

   test("empty joinCode → no-match branch", () => {
      const out = parseJoinCode("", "anything", noPaths);
      assert.strictEqual(out.is_nested_index, false);
      assert.strictEqual(out.original_source_name, "anything");
   });
});

// Compile-backed (real Malloy): source-level #(doc) must NOT be inherited by an
// extending source that declares no #(doc) of its own. Malloy copies a base's
// blockNotes verbatim onto such a child, so this exercises the location-filter
// in extractDocsFromExplore (via getDirectTaglines).
describe("extractMetadataFromExplores — source #(doc) inheritance (compile-backed)", () => {
   async function docsBySource(model: string, names: string[]) {
      const conn = new DuckDBConnection("duckdb");
      try {
         const runtime = new Runtime({
            urlReader: new InMemoryURLReader(new Map()),
            connection: conn,
         });
         await conn.runRawSQL("CREATE OR REPLACE TABLE t AS SELECT 1 as id");
         const compiled = await runtime.getModel(model);
         const explores = names.map((n) => compiled.getExploreByName(n));
         const md = sourceExtraction.extractMetadataFromExplores(
            explores as unknown as Explore[],
            "m.malloy",
         );
         const out: Record<string, string | undefined> = {};
         for (const n of names) {
            out[n] = md.find(
               (m) => m.metadataType === "DOCS" && m.sourceName === n,
            )?.metadata;
         }
         return out;
      } finally {
         await conn.close();
      }
   }

   test("child with no own #(doc) does NOT inherit the base's; own doc still works", async () => {
      const model = `
#(doc) Base source doc.
source: s_base is duckdb.table('t') extend { dimension: d is id }

source: s_child is s_base extend { dimension: d2 is id }

#(doc) Own child doc.
source: s_child_doc is s_base extend { dimension: d3 is id }`;
      const docs = await docsBySource(model, [
         "s_base",
         "s_child",
         "s_child_doc",
      ]);
      assert.strictEqual(docs.s_base, "Base source doc.");
      assert.strictEqual(docs.s_child, undefined); // no inheritance
      assert.strictEqual(docs.s_child_doc, "Own child doc.");
   });

   test("doc text is stored verbatim — no trailing period appended", async () => {
      // Deliberate departure from an earlier version, which appended a missing "." because
      // all notes were collapsed into ONE line (the period separated
      // sentences). Docs are now separate lines joined with \n, so
      // auto-appending "." would corrupt wrapped prose and list-style docs.
      const model = `
#(doc) First line without period
#(doc) second line continues
source: s_verbatim is duckdb.table('t') extend { dimension: d is id }`;
      const docs = await docsBySource(model, ["s_verbatim"]);
      assert.strictEqual(
         docs.s_verbatim,
         "First line without period\nsecond line continues",
      );
   });
});
