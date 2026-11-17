// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterAll, describe, expect, it } from "bun:test";
import type { components } from "../../../api";
import type { PackageAnnotationFinding } from "../../annotation_reader";
import { loadGateText } from "./load_gate_text";
import { tempBuildPlanPackages } from "../../../test_helpers/incremental_test_harness";
import { computePackageBuildPlan } from "./build_plan_compile";

type BuildPlan = components["schemas"]["BuildPlan"];

function planOf(
   sources: Record<
      string,
      { name: string; tableName?: string; address: string }
   >,
): BuildPlan {
   return {
      graphs: [],
      refusedSources: {},
      sources: Object.fromEntries(
         Object.entries(sources).map(([id, s]) => [
            id,
            {
               name: s.name,
               sourceID: id,
               connectionName: "duckdb",
               dialect: "duckdb",
               sourceEntityId: s.address,
               annotationFields: s.tableName ? { name: s.tableName } : {},
               modelPath: "m.malloy",
            },
         ]),
      ),
   } as unknown as BuildPlan;
}

const COLLISION: PackageAnnotationFinding = {
   route: "materialize",
   tier: "malformed",
   code: "materialization-table-collision",
   message: '#(materialize) on source "x" is invalid: its name= `t` is a table',
   subject: "x",
   model: "m.malloy",
};

describe("loadGateText", () => {
   it("names every invalid finding and author collision with its code, and nothing valid", () => {
      const text = loadGateText(
         {
            plan: planOf({
               "a@file:///m.malloy": {
                  name: "a",
                  tableName: "main.t",
                  address: "ba",
               },
               "b@file:///m.malloy": {
                  name: "b",
                  tableName: "t",
                  address: "bb",
               },
               "c@file:///m.malloy": { name: "c", address: "bc" },
            }),
            annotationFindings: [
               COLLISION,
               {
                  route: "@",
                  tier: "unservable",
                  code: "persist-not-built",
                  message: "valid, refused alone",
               },
            ],
            incrementalDeclarations: {},
         },
         { enforceCollisions: true, versionScoped: false },
      );
      const lines = text.split("\n");
      expect(lines[0]).toEndWith(". [materialization-table-collision]");
      expect(
         lines.filter((l) => l.includes('"a" (m.malloy), "b" (m.malloy)')),
      ).toHaveLength(1);
      expect(text).not.toContain("valid, refused alone");
      expect(text).not.toContain('"c"');
   });

   it("under the opt-out, refuses collisions alone rather than failing", () => {
      expect(
         loadGateText(
            {
               plan: planOf({
                  "a@file:///m.malloy": {
                     name: "a",
                     tableName: "t",
                     address: "ba",
                  },
                  "b@file:///m.malloy": {
                     name: "b",
                     tableName: "t",
                     address: "bb",
                  },
               }),
               annotationFindings: [COLLISION],
               incrementalDeclarations: {},
            },
            { enforceCollisions: false, versionScoped: false },
         ),
      ).toBe("");
   });
});

describe("loadGateText over files a run compiles", () => {
   // The model-level mistakes the load reads off its models: a run over the
   // same files must refuse them too.
   const packages = tempBuildPlanPackages();
   afterAll(() => packages.cleanup());
   const ROWS = `duckdb.sql("""SELECT 1 AS id, 'A' AS category, 10 AS amount""")`;
   const HEADER = "##! experimental { persistence composite_sources }\n";
   for (const [label, model, code] of [
      [
         "#@ preaggregate bogus=1",
         `${HEADER}source: o is ${ROWS} extend {\n  #@ preaggregate grain="category" bogus=1\n  measure: total is amount.sum()\n}\n`,
         "rollup-unknown-key",
      ],
      [
         "#@ preaggregate storage=@env.X",
         `${HEADER}source: o is ${ROWS} extend {\n  #@ preaggregate grain="category" storage=@env.X\n  measure: total is amount.sum()\n}\n`,
         "rollup-invalid",
      ],
      [
         "#@ persits",
         `${HEADER}source: raw is ${ROWS}\n#@ persits name="t"\nsource: d is raw -> { group_by: category }\n`,
         "materialization-near-miss",
      ],
      [
         "#(materialise)",
         `${HEADER}source: raw is ${ROWS}\n#(materialise) name="t"\nsource: o is raw extend { where: id > 0 }\n`,
         "materialization-near-miss",
      ],
   ] as const) {
      it(`refuses ${label}`, async () => {
         const plan = await computePackageBuildPlan(
            await packages.create({ "model.malloy": model }),
         );
         const text = loadGateText(
            {
               plan: plan.plan,
               annotationFindings: plan.annotationFindings,
               incrementalDeclarations: plan.incrementalDeclarations,
               ...(plan.modelGateLines
                  ? { modelGateLines: plan.modelGateLines }
                  : {}),
            },
            { enforceCollisions: true, versionScoped: false },
         );
         expect(text).toContain(`[${code}]`);
         expect(text).toContain("model.malloy: ");
      });
   }
});
