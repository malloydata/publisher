// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { GivenValue } from "../../hooks/givenValue";
import type { DashboardDocument, QueryTile } from "./document";
import { queryTile } from "./testing/fixtures";
import {
   givensReadThroughModel,
   previewGivens,
   previewTileQuery,
   tileExpressionKey,
} from "./preview";

const tile = (
   name: string,
   from: string,
   filters?: QueryTile["filters"],
): QueryTile => ({
   name,
   source: "overview",
   declaration: { kind: "reference", from },
   ...(filters ? { filters } : {}),
});

const document: DashboardDocument = {
   layout: "grid",
   title: "Overview",
   imports: [],
   sources: [{ name: "overview", base: "order_items" }],
   localGivens: [
      {
         name: "CATEGORY",
         type: "filter<string>",
         default: "f''",
         label: "Category",
         control: "select",
         suggest: { source: "products", dimension: "category" },
      },
      { name: "SINCE", type: "date", default: "@2023-01-01", label: "Since" },
      { name: "UNBOUND", type: "filter<string>", default: "f''" },
   ],
   tiles: [
      tile("kpis", "key_figures", [
         { field: "category", given: "CATEGORY" },
         { field: "created_at", given: "SINCE", op: ">=" },
      ]),
      tile("trend", "sales_by_month", [
         { field: "category", given: "CATEGORY" },
      ]),
      tile("plain", "by_state"),
   ],
};

describe("previewGivens", () => {
   it("shows the givens some tile binds, this file's first, in declaration order", () => {
      const specs = previewGivens(document, [
         { name: "REGION", type: "filter<string>" },
         { name: "CATEGORY", type: "filter<string>", label: "From the model" },
      ]);
      // UNBOUND is declared and bound by nothing, so it is not a control; the
      // model's REGION is bound by nothing either. The model's CATEGORY shares
      // a name with the local one, and the local declaration is the control.
      expect(specs.map((s) => s.name)).toEqual(["CATEGORY", "SINCE"]);
      expect(specs[0].label).toBe("Category");
      expect(specs[0].control).toBe("select");
      expect(specs[0].suggest).toEqual({
         source: "products",
         dimension: "category",
      });
   });

   it("shows a filter default as the server publishes it: unwrapped, and empty when empty", () => {
      const filtered: DashboardDocument = {
         ...document,
         localGivens: [
            { name: "EMPTY", type: "filter<string>", default: "f''" },
            { name: "STATE", type: "filter<string>", default: "f'WN'" },
         ],
         tiles: [
            tile("t", "v", [
               { field: "a", given: "EMPTY" },
               { field: "b", given: "STATE" },
            ]),
         ],
      };
      const [empty, state] = previewGivens(filtered, []);
      // As the server publishes it: "", which filters nothing (absent would
      // read as no default declared).
      expect(empty.default).toBe("");
      expect(state.default).toBe("WN");
      // The document keeps the literal the file is written from.
      expect(filtered.localGivens?.[1].default).toBe("f'WN'");
   });

   it("uses the model's own spec for a model given a tile binds", () => {
      const withModel: DashboardDocument = {
         ...document,
         localGivens: [],
         tiles: [tile("t", "v", [{ field: "brand", given: "BRAND" }])],
      };
      const spec = {
         name: "BRAND",
         type: "filter<string>",
         suggest: {
            query: "brand_suggest",
            dimension: "brand",
            givenNames: ["REGION"],
         },
      };
      expect(previewGivens(withModel, [spec])).toEqual([spec]);
   });
});

describe("givens read through the model", () => {
   // The auto_recalls shape: the report binds nothing; the model's source
   // filters itself by four givens in its own `where:`.
   const recalls: DashboardDocument = {
      layout: "grid",
      title: "Recalls",
      imports: [{ kind: "all", from: "auto_recalls.malloy" }],
      sources: [{ name: "recalls_tiles", base: "recalls" }],
      tiles: [
         { ...tile("by_year", "by_year"), source: "recalls_tiles" },
         { ...tile("by_maker", "by_maker"), source: "recalls_tiles" },
      ],
   };
   const MODEL = [
      { name: "manufacturer_filter", type: "filter<string>" },
      { name: "subject_filter", type: "filter<string>" },
      { name: "major_recall_filter", type: "boolean" },
      { name: "recall_date_filter", type: "filter<date>" },
   ];
   const served = new Map([
      [
         "recalls_tiles -> by_year",
         ["manufacturer_filter", "recall_date_filter", "major_recall_filter"],
      ],
      ["recalls_tiles -> by_maker", ["manufacturer_filter", "subject_filter"]],
   ]);

   it("shows a given only the model reads, in the manifest's order", () => {
      const reads = givensReadThroughModel(recalls, recalls, served);
      expect(
         previewGivens(recalls, MODEL, reads).map((spec) => spec.name),
      ).toEqual(MODEL.map((spec) => spec.name));
      // Without the reads, the row the builder drew before: empty.
      expect(previewGivens(recalls, MODEL)).toEqual([]);
   });

   it("leaves out what this file bound, so a removed binding takes its control", () => {
      const bound: DashboardDocument = {
         ...recalls,
         tiles: [
            {
               ...tile("by_year", "by_year", [
                  { field: "category", given: "CATEGORY" },
               ]),
               source: "recalls_tiles",
            },
         ],
      };
      const reads = givensReadThroughModel(
         bound,
         bound,
         new Map([
            ["recalls_tiles -> by_year", ["CATEGORY", "manufacturer_filter"]],
         ]),
      );
      expect([...reads]).toEqual(["manufacturer_filter"]);
   });

   it("puts this file's bound declarations first, then the model's", () => {
      const mixed: DashboardDocument = {
         ...recalls,
         localGivens: [{ name: "SINCE", type: "date", default: "@2020-01-01" }],
         tiles: [
            {
               ...tile("by_year", "by_year", [
                  { field: "recall_date", given: "SINCE", op: ">=" },
               ]),
               source: "recalls_tiles",
            },
         ],
      };
      const reads = givensReadThroughModel(mixed, mixed, served);
      expect(
         previewGivens(mixed, MODEL, reads).map((spec) => spec.name),
      ).toEqual([
         "SINCE",
         "manufacturer_filter",
         "major_recall_filter",
         "recall_date_filter",
      ]);
   });

   it("reads a new tile by a served tile of the same expression, and nothing otherwise", () => {
      const added: DashboardDocument = {
         ...recalls,
         tiles: [
            ...recalls.tiles,
            { ...tile("by_maker", "by_maker"), source: "recalls_tiles" },
            { ...tile("fresh", "fresh"), source: "recalls_tiles" },
         ],
      };
      expect(
         [
            ...givensReadThroughModel(added, { ...recalls, tiles: [] }, served),
         ].sort(),
      ).toEqual(
         [
            "major_recall_filter",
            "manufacturer_filter",
            "recall_date_filter",
            "subject_filter",
         ].sort(),
      );
      const onlyNew = { ...recalls, tiles: [added.tiles[3]!] };
      expect(givensReadThroughModel(onlyNew, recalls, served).size).toBe(0);
   });
});

describe("previewTileQuery", () => {
   const runnable = new Set(["CATEGORY", "SINCE"]);

   it("runs the view on the source the extension extends, with the document's bindings", () => {
      const q = previewTileQuery(document, queryTile(document, 0), runnable);
      expect(q.expression).toBe(
         "overview -> key_figures + { where: category ~ $CATEGORY, where: created_at >= $SINCE }",
      );
      expect(q.givenNames).toEqual(["CATEGORY", "SINCE"]);
   });

   it("carries the wrapper's chart line above the run, and nothing for the view's own", () => {
      const base = queryTile(document, 2);
      const withChart = (chart: QueryTile["chart"]) =>
         previewTileQuery(
            document,
            {
               ...base,
               chart,
               chartLines: ["# bar_chart { size=spark }"],
            } as QueryTile,
            runnable,
         ).annotation;
      expect(withChart("bar_chart")).toBe("# viz=bar");
      expect(withChart("none")).toBe("# viz=table");
      expect(withChart("custom")).toBe("# bar_chart { size=spark }");
      expect(withChart("default")).toBeUndefined();
      expect(withChart(undefined)).toBeUndefined();
   });

   it("clears the saved view's own chart line for an inline or opaque tile on Default", () => {
      // Every chart tag off and `viz` left alone: not Table's line, so a view
      // of totals still draws as big numbers, as it does with no line at all.
      const NONE =
         "# -line_chart -bar_chart -scatter_chart -shape_map -segment_map -combo_chart";
      // A colour is set so this reads the chart line alone; Theme's line is pinned below.
      const annotationOf = (
         declaration: QueryTile["declaration"],
         chart: QueryTile["chart"],
      ) =>
         previewTileQuery(
            document,
            {
               name: "x",
               source: "overview",
               declaration,
               chart,
               color: "#123456",
            },
            runnable,
         )
            .annotation?.split("\n")
            .filter((line) => line !== '# theme.palette.series=["#123456"]')
            .join("\n") || undefined;
      expect(annotationOf({ kind: "inline" }, "default")).toBe(NONE);
      for (const why of [
         "a `{ … } + { … }` compound refinement",
         "a `->` pipeline from a named view",
      ])
         expect(annotationOf({ kind: "opaque", why }, "default")).toBe(NONE);
      // These start from a named view, whose own chart the saved dashboard shows.
      for (const why of [
         "a chained refinement",
         "a parenthesized expression",
         "an unreadable refinement",
      ])
         expect(
            annotationOf({ kind: "opaque", why }, "default"),
         ).toBeUndefined();
      expect(annotationOf({ kind: "inline" }, undefined)).toBeUndefined();
      expect(
         annotationOf({ kind: "reference", from: "v" }, "default"),
      ).toBeUndefined();
   });

   it("takes the saved view's colour off an inline or opaque tile set to Theme", () => {
      // An inline tile runs its own saved view, which still carries the colour
      // it was saved with: without this line, picking Theme kept drawing that
      // colour while the menu showed the theme's. A reference tile runs its
      // base view, which never had the wrapper's colour, so it needs none.
      const RESET = "# -theme.palette.series";
      const annotationOf = (
         declaration: QueryTile["declaration"],
         color?: string,
      ) =>
         previewTileQuery(
            document,
            {
               name: "x",
               source: "overview",
               declaration,
               chart: "combo_chart",
               ...(color ? { color } : {}),
            },
            runnable,
         ).annotation?.split("\n");
      expect(annotationOf({ kind: "inline" })).toEqual(["# viz=combo", RESET]);
      expect(
         annotationOf({ kind: "opaque", why: "a chained refinement" })?.at(-1),
      ).toBe(RESET);
      for (const declaration of [
         { kind: "reference", from: "v" },
         { kind: "named", from: "v" },
         { kind: "inherited" },
      ] as const)
         expect(annotationOf(declaration)?.includes(RESET)).toBe(false);
      expect(annotationOf({ kind: "inline" }, "#7f9862")?.at(-1)).toBe(
         '# theme.palette.series=["#7f9862"]',
      );
   });

   it("sends a tile only the givens it binds", () => {
      // `trend` binds CATEGORY alone: SINCE moving must not re-run it, and
      // unbinding a tile is what takes a control's effect off it.
      const q = previewTileQuery(document, queryTile(document, 1), runnable);
      expect(q.givenNames).toEqual(["CATEGORY"]);
      const bare = previewTileQuery(document, queryTile(document, 2), runnable);
      expect(bare.expression).toBe("overview -> by_state");
      expect(bare.givenNames).toEqual([]);
   });

   it("writes the value of a given the server does not know yet as a literal", () => {
      // SINCE is declared here and not compiled on the server: it cannot be
      // sent by name, so its value goes into the query, and a new filter works
      // before the file is saved. Only CATEGORY travels with the request.
      const q = previewTileQuery(
         document,
         queryTile(document, 0),
         new Set(["CATEGORY"]),
         new Map<string, GivenValue>([
            ["CATEGORY", "Shoes"],
            ["SINCE", new Date("2024-01-31T00:00:00Z")],
         ]),
      );
      expect(q.expression).toBe(
         "overview -> key_figures + { where: category ~ $CATEGORY, where: created_at >= @2024-01-31 }",
      );
      expect(q.givenNames).toEqual(["CATEGORY"]);
   });

   it("applies an unsent given's declared default until a value is set, and skips one nobody declares", () => {
      // No value set: SINCE's default applies, as the server applies a
      // given's default once the report is saved; CATEGORY's default is an
      // empty filter, which filters nothing, so it adds no clause.
      const q = previewTileQuery(document, queryTile(document, 0), new Set());
      expect(q.expression).toBe(
         "overview -> key_figures + { where: created_at >= @2023-01-01 }",
      );
      expect(q.givenNames).toEqual([]);
      const stray: QueryTile = tile("t", "v", [
         { field: "x", given: "NOBODY" },
      ]);
      expect(
         previewTileQuery(document, stray, new Set(), new Map([["NOBODY", 1]]))
            .expression,
      ).toBe("overview -> v");
   });

   it("runs an inline tile the same as a reference, on its own name", () => {
      // An inline body still names a view of the extension — `view: x is {
      // … }` is as much a named view as `view: x is base_view` — so a
      // binding takes the same `source -> x + { where: … }` path a reference
      // tile does, rather than being sent unrefined.
      const inline: QueryTile = {
         name: "kpis",
         source: "overview",
         declaration: { kind: "inline" },
         filters: [{ field: "category", given: "CATEGORY" }],
      };
      const q = previewTileQuery(document, inline, runnable);
      expect(q.expression).toBe(
         "overview -> kpis + { where: category ~ $CATEGORY }",
      );
      expect(q.givenNames).toEqual(["CATEGORY"]);
   });

   it("runs a named-query tile by its name, sending the whole row", () => {
      const named: QueryTile = {
         name: "by_carrier",
         declaration: { kind: "query" },
      };
      expect(previewTileQuery(document, named, runnable)).toEqual({
         expression: "by_carrier",
         givenNames: undefined,
         reads: undefined,
      });
   });

   it("runs an inherited tile as the model has it, sending the whole row", () => {
      const inherited: QueryTile = {
         name: "by_brand",
         source: "orders",
         declaration: { kind: "inherited" },
      };
      expect(previewTileQuery(document, inherited, runnable)).toEqual({
         expression: "orders -> by_brand",
         givenNames: undefined,
         reads: undefined,
      });
   });

   it("runs a query of the report's own as its view, under its chart and with its bindings", () => {
      const named: QueryTile = {
         name: "by_brand_tile",
         source: "orders",
         declaration: { kind: "named", from: "by_brand" },
         chart: "line_chart",
         color: "#7f9862",
         filters: [{ field: "category", given: "CATEGORY" }],
      };
      expect(
         previewTileQuery(document, named, runnable, new Map(), []),
      ).toEqual({
         annotation: '# viz=line\n# theme.palette.series=["#7f9862"]',
         expression: "orders -> by_brand + { where: category ~ $CATEGORY }",
         givenNames: ["CATEGORY"],
         reads: ["CATEGORY"],
      });
   });
});

describe("previewTileQuery reads", () => {
   const runnable = new Set(["CATEGORY", "REGION"]);

   it("counts a literal-bound local given the server cannot be sent", () => {
      const q = previewTileQuery(document, queryTile(document, 0), runnable);
      expect(q.givenNames).toEqual(["CATEGORY"]);
      expect(q.reads).toEqual(["CATEGORY", "SINCE"]);
   });

   it("counts, and sends, what the extension's own where: reads", () => {
      const scoped: DashboardDocument = {
         ...document,
         sources: [
            { name: "overview", base: "order_items", scopedBy: ["REGION"] },
         ],
      };
      const q = previewTileQuery(scoped, queryTile(scoped, 2), runnable);
      expect(q.expression).toBe("overview -> by_state");
      expect(q.givenNames).toEqual(["REGION"]);
      expect(q.reads).toEqual(["REGION"]);
   });

   it("adds what the served tile reads, which the document cannot see, and sends the runnable part", () => {
      // A model source's own `where: region ~ $REGION`: the reader moves the
      // tile with REGION, so the builder must send it too.
      const q = previewTileQuery(
         document,
         queryTile(document, 2),
         runnable,
         new Map(),
         ["REGION", "BRAND"],
      );
      expect(q.reads).toEqual(["REGION", "BRAND"]);
      expect(q.givenNames).toEqual(["REGION"]);
   });

   it("says nothing for a tile the served file lacks and the document scopes by nothing", () => {
      // A tile not saved yet over a model-scoped source: no served entry, so
      // an empty reads would chip every control the model reads.
      expect(
         previewTileQuery(document, queryTile(document, 2), runnable).reads,
      ).toBeUndefined();
      expect(
         previewTileQuery(
            document,
            queryTile(document, 2),
            runnable,
            new Map(),
            [],
         ).reads,
      ).toEqual([]);
   });

   it("takes an inherited tile's reads from the served file alone", () => {
      const inherited: QueryTile = {
         name: "by_brand",
         source: "orders",
         declaration: { kind: "inherited" },
      };
      expect(
         previewTileQuery(document, inherited, runnable, new Map(), ["REGION"])
            .reads,
      ).toEqual(["REGION"]);
      expect(
         previewTileQuery(document, inherited, runnable).reads,
      ).toBeUndefined();
   });
});

describe("tileExpressionKey", () => {
   it("keys a tile expression as the server does", () => {
      expect(tileExpressionKey("orders->by_x")).toBe("orders -> by_x");
      expect(tileExpressionKey("  orders  ->   by_x ")).toBe("orders -> by_x");
   });
});

describe("previewTileQuery: a filter on a reserved field name", () => {
   it("back-quotes the field in the preview's where clause", () => {
      const q = previewTileQuery(
         document,
         tile("t", "v", [{ field: "date", given: "CATEGORY" }]),
         new Set(["CATEGORY"]),
      );
      expect(q.expression).toBe(
         "overview -> v + { where: `date` ~ $CATEGORY }",
      );
   });
});
