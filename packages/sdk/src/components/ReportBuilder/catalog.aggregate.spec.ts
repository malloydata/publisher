// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { CompiledModel } from "../../client";
import { buildCatalog, documentViewsOf } from "./catalog";

/**
 * `sourceInfos` as the compiler wrote it (modelDefToModelInfo on a DuckDB source with a measure `n`
 * and a dimension that is itself named `calculation`), trimmed to the views.
 */
const REAL = String.raw`{"name":"s","schema":{"fields":[{"kind":"view","name":"kpi","schema":{"fields":[{"kind":"dimension","name":"n","type":{"kind":"number_type","subtype":"integer"},"annotations":[{"value":"#(malloy) reference_id = \"7e2bec9a-47bb-45e5-828a-7fdd5ad7889d\" calculation drill_expression { kind = field_reference name = n }\n"}]}]}},{"kind":"view","name":"by_calc","schema":{"fields":[{"kind":"dimension","name":"calculation","type":{"kind":"number_type","subtype":"integer"},"annotations":[{"value":"#(malloy) reference_id = \"51c5aa79-dc3c-4e58-8ada-5babf3a9a08e\" drill_expression { kind = field_reference name = calculation code = calculation }\n"}]}]}},{"kind":"view","name":"by_calc_n","schema":{"fields":[{"kind":"dimension","name":"calculation","type":{"kind":"number_type","subtype":"integer"},"annotations":[{"value":"#(malloy) reference_id = \"e1336f24-53ff-41ff-aef8-d254e3fc87d6\" drill_expression { kind = field_reference name = calculation code = calculation }\n"}]},{"kind":"dimension","name":"n","type":{"kind":"number_type","subtype":"integer"},"annotations":[{"value":"#(malloy) reference_id = \"d9ea9d2e-c85f-48e8-80a2-f84d074028b9\" calculation drill_expression { kind = field_reference name = n }\n"}]}]}},{"kind":"view","name":"nst","schema":{"fields":[{"kind":"dimension","name":"a","type":{"kind":"number_type","subtype":"integer"},"annotations":[{"value":"#(malloy) reference_id = \"ce679402-230c-427d-a5bd-6d5faaba4334\" drill_expression { kind = field_reference name = a code = a }\n"}]},{"kind":"dimension","name":"x","type":{"kind":"array_type","element_type":{"kind":"record_type","fields":[{"name":"calculation","annotations":[{"value":"#(malloy) reference_id = \"76958db7-2cd8-43a1-8ae8-87e5b82fc44b\" drill_expression { kind = field_reference name = calculation path = [x] code = calculation }\n"}],"type":{"kind":"number_type","subtype":"integer"}}]}},"annotations":[{"value":"#(malloy) drillable\n"}]}]}}]}}`;

const MODEL = {
   modelPath: "notebooks/n.malloy",
   modelInfo: JSON.stringify({ entries: [{ kind: "source", name: "s" }] }),
   sources: [
      {
         name: "s",
         views: ["kpi", "by_calc", "by_calc_n", "nst"].map((name) => ({
            name,
         })),
      },
   ],
   sourceInfos: [REAL],
} as unknown as CompiledModel;

describe("buildCatalog, aggregate-only views, from real compiler output", () => {
   it("marks only the view whose every column is an aggregate", () => {
      const [source] = buildCatalog([MODEL]).sources;
      expect(
         Object.fromEntries(source.views.map((v) => [v.name, v.aggregateOnly])),
      ).toEqual({
         kpi: true,
         // A group-by on a column named `calculation` is not an aggregate.
         by_calc: undefined,
         by_calc_n: undefined,
         nst: undefined,
      });
   });

   it("says nothing when the model carries no source info", () => {
      const [source] = buildCatalog([
         { ...MODEL, sourceInfos: undefined } as CompiledModel,
      ]).sources;
      expect(source.views.every((v) => v.aggregateOnly === undefined)).toBe(
         true,
      );
   });
});

describe("documentViewsOf, the views a report file declares itself", () => {
   it("gives each view its shape, from the file's own compiled model", () => {
      const views = documentViewsOf(MODEL).get("s");
      expect(views?.get("kpi")).toEqual({
         name: "kpi",
         aggregateOnly: true,
         shape: { groupBys: 0, totals: 1 },
      });
      expect(views?.get("by_calc_n")?.shape).toEqual({
         groupBys: 1,
         totals: 1,
      });
      // A nest is neither an x-axis field nor a total.
      expect(views?.get("nst")?.shape).toEqual({ groupBys: 1, totals: 0 });
   });

   // A pick negates the chart its view declares. Without it, a bar picked on
   // a `# line_chart` view wrote `# -viz bar_chart` and the renderer was asked
   // for both charts at once, and drew an error.
   it("carries the chart and doc line each view declares, as the catalog does", () => {
      const withNotes = {
         ...MODEL,
         sources: [
            {
               name: "s",
               views: [
                  {
                     name: "by_calc_n",
                     annotations: ["#(doc) Orders by month\n", "# line_chart\n"],
                  },
                  { name: "kpi" },
               ],
            },
         ],
      } as unknown as CompiledModel;
      const views = documentViewsOf(withNotes).get("s");
      expect(views?.get("by_calc_n")).toEqual({
         name: "by_calc_n",
         description: "Orders by month",
         chart: "line_chart",
         shape: { groupBys: 1, totals: 1 },
      });
      expect(views?.get("kpi")?.chart).toBeUndefined();
      expect(views?.get("by_calc_n")).toEqual(
         buildCatalog([withNotes]).sources[0].views.find(
            (v) => v.name === "by_calc_n",
         ),
      );
   });
});

describe("buildCatalog, combo-chart views", () => {
   // Columns as the compiler annotates them: an aggregate carries
   // `calculation` in its `#(malloy)` note, a group-by does not.
   const column = (name: string, aggregate: boolean, array = false) => ({
      kind: "dimension",
      name,
      type: array
         ? {
              kind: "array_type",
              element_type: { kind: "record_type", fields: [] },
           }
         : { kind: "number_type" },
      annotations: [
         {
            value: `#(malloy) ${aggregate ? "calculation " : ""}drill_expression { kind = field_reference name = ${name} }\n`,
         },
      ],
   });
   const view = (name: string, columns: unknown[]) => ({
      kind: "view",
      name,
      schema: { fields: columns },
   });
   const VIEWS = [
      view("by_month", [
         column("month", false),
         column("orders", true),
         column("revenue", true),
      ]),
      view("one_total", [column("month", false), column("orders", true)]),
      view("three_totals", [
         column("month", false),
         column("a", true),
         column("b", true),
         column("c", true),
      ]),
      view("totals_only", [column("a", true), column("b", true)]),
      view("nested_only", [
         column("rows", false, true),
         column("a", true),
         column("b", true),
      ]),
   ];
   const COMBO = {
      ...MODEL,
      sources: [{ name: "s", views: VIEWS.map((v) => ({ name: v.name })) }],
      sourceInfos: [JSON.stringify({ name: "s", schema: { fields: VIEWS } })],
   } as unknown as CompiledModel;

   it("marks only a view with a group by and exactly two totals, the shape a bare # combo_chart draws", () => {
      const [source] = buildCatalog([COMBO]).sources;
      expect(
         Object.fromEntries(source.views.map((v) => [v.name, v.comboShape])),
      ).toEqual({
         by_month: true,
         one_total: undefined,
         three_totals: undefined,
         totals_only: undefined,
         nested_only: undefined,
      });
   });
});
