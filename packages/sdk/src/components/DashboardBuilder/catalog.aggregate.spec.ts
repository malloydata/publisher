// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { CompiledModel } from "../../client";
import { buildCatalog } from "./catalog";

/** Column notes as the compiler writes them: an aggregate output carries `calculation`, a group-by does not. */
const column = (name: string, aggregate: boolean, type = "number_type") => ({
   kind: "dimension",
   name,
   type: { kind: type },
   annotations: [
      {
         value: `#(malloy) reference_id = "x" ${aggregate ? "calculation " : ""}drill_expression { kind = field_reference name = ${name} }\n`,
      },
   ],
});

const view = (name: string, columns: unknown[]) => ({
   kind: "view",
   name,
   schema: { fields: columns },
});

const MODEL = {
   modelPath: "notebooks/n.malloy",
   sources: [
      {
         name: "s",
         views: [
            { name: "kpi" },
            { name: "grouped" },
            { name: "nested" },
            { name: "empty" },
         ],
      },
   ],
   sourceInfos: [
      JSON.stringify({
         name: "s",
         schema: {
            fields: [
               view("kpi", [column("n", true), column("total", true)]),
               view("grouped", [column("a", false), column("n", true)]),
               view("nested", [
                  column("n", true),
                  {
                     kind: "dimension",
                     name: "x",
                     type: { kind: "array_type" },
                     annotations: [{ value: "#(malloy) drillable\n" }],
                  },
               ]),
               view("empty", []),
            ],
         },
      }),
   ],
} as unknown as CompiledModel;

describe("buildCatalog, aggregate-only views", () => {
   it("marks a view whose every column is an aggregate, and no other", () => {
      const [source] = buildCatalog([MODEL]).sources;
      expect(
         Object.fromEntries(source.views.map((v) => [v.name, v.aggregateOnly])),
      ).toEqual({
         kpi: true,
         grouped: undefined,
         nested: undefined,
         empty: undefined,
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
