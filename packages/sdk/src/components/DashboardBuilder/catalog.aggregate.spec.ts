// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { CompiledModel } from "../../client";
import { buildCatalog } from "./catalog";

/**
 * `sourceInfos` as the compiler wrote it (modelDefToModelInfo on a DuckDB source with a measure `n`
 * and a dimension that is itself named `calculation`), trimmed to the views.
 */
const REAL = String.raw`{"name":"s","schema":{"fields":[{"kind":"view","name":"kpi","schema":{"fields":[{"kind":"dimension","name":"n","type":{"kind":"number_type","subtype":"integer"},"annotations":[{"value":"#(malloy) reference_id = \"7e2bec9a-47bb-45e5-828a-7fdd5ad7889d\" calculation drill_expression { kind = field_reference name = n }\n"}]}]}},{"kind":"view","name":"by_calc","schema":{"fields":[{"kind":"dimension","name":"calculation","type":{"kind":"number_type","subtype":"integer"},"annotations":[{"value":"#(malloy) reference_id = \"51c5aa79-dc3c-4e58-8ada-5babf3a9a08e\" drill_expression { kind = field_reference name = calculation code = calculation }\n"}]}]}},{"kind":"view","name":"by_calc_n","schema":{"fields":[{"kind":"dimension","name":"calculation","type":{"kind":"number_type","subtype":"integer"},"annotations":[{"value":"#(malloy) reference_id = \"e1336f24-53ff-41ff-aef8-d254e3fc87d6\" drill_expression { kind = field_reference name = calculation code = calculation }\n"}]},{"kind":"dimension","name":"n","type":{"kind":"number_type","subtype":"integer"},"annotations":[{"value":"#(malloy) reference_id = \"d9ea9d2e-c85f-48e8-80a2-f84d074028b9\" calculation drill_expression { kind = field_reference name = n }\n"}]}]}},{"kind":"view","name":"nst","schema":{"fields":[{"kind":"dimension","name":"a","type":{"kind":"number_type","subtype":"integer"},"annotations":[{"value":"#(malloy) reference_id = \"ce679402-230c-427d-a5bd-6d5faaba4334\" drill_expression { kind = field_reference name = a code = a }\n"}]},{"kind":"dimension","name":"x","type":{"kind":"array_type","element_type":{"kind":"record_type","fields":[{"name":"calculation","annotations":[{"value":"#(malloy) reference_id = \"76958db7-2cd8-43a1-8ae8-87e5b82fc44b\" drill_expression { kind = field_reference name = calculation path = [x] code = calculation }\n"}],"type":{"kind":"number_type","subtype":"integer"}}]}},"annotations":[{"value":"#(malloy) drillable\n"}]}]}}]}}`;

const MODEL = {
   modelPath: "notebooks/n.malloy",
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
