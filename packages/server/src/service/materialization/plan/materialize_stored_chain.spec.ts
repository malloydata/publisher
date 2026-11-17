// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { sourcesIn } from "../../../test_helpers/materialization_test_fixtures";
import { describe, expect, it } from "bun:test";
import {
   type ModelFiles,
   planPackages,
} from "../../../test_helpers/materialization_fixture";
import { addressSQL } from "./build_plan";
import { buildCopyText } from "./materialize_build_copy";
import { buildSourceName } from "./build_file_generator";
import { storedChainText } from "./materialize_stored_chain";
import {
   BASE,
   examplePlan,
   generatedPlans,
} from "../../../test_helpers/materialize_spec_models";

/**
 * A chained `storage=` extension table (`materialize_stored_chain.ts`, P10):
 * the text its build compiles in the destination over its parent's stored
 * table, its own access modifiers lifted over the parent's re-root, and the
 * raw chain it falls back to when that text does not compile.
 */

const packages = planPackages({}, { keepIncrementalUnbuildable: true });

const compiled = async (files: ModelFiles) => (await packages.plan(files)).c;

describe("planModelMaterialize and generateBuildFile", () => {
   const plan = examplePlan;

   it("lifts a chained extension's own access modifiers over its parent's re-root, and drops a stored hidden parent dimension with its label (P3 with P10)", () => {
      const text = `source: orders is duckdb.table('ev')

source: stored is orders extend {
  internal dimension: big is amt > 10
  private dimension: secret is amt * 2
  measure: total is amt.sum()
}

source: ext is stored extend {
  private dimension: mine is amt + 1
}
`;
      const lines = text.split("\n");
      const at = (name: string, endLine: number) => {
         const line = lines.findIndex((l) =>
            l.startsWith(`source: ${name} is`),
         );
         return {
            url: "file:///pkg/m.malloy",
            range: {
               start: { line, character: 8 },
               end: { line: endLine, character: 1 },
            },
         };
      };
      const parent = plan({
         sourceName: "stored",
         sourceID: "stored@file:///pkg/m.malloy",
         buildSourceName: buildSourceName("stored"),
         columns: [
            { dimension: "amt", column: "amt", computed: false },
            {
               dimension: "big",
               column: "big",
               computed: true,
               access: "internal",
            },
            {
               dimension: "secret",
               column: "secret",
               computed: true,
               access: "private",
            },
         ],
         declaration: at("stored", 6),
      });
      const ext = plan({
         sourceName: "ext",
         sourceID: "ext@file:///pkg/m.malloy",
         buildSourceName: buildSourceName("ext"),
         columns: [
            { dimension: "amt", column: "amt", computed: false },
            {
               dimension: "mine",
               column: "mine",
               computed: true,
               access: "private",
            },
         ],
         declaration: at("ext", 10),
         buildsOver: {
            sourceID: parent.sourceID,
            sourceName: "stored",
            declaration: at("stored", 6),
            path: [at("ext", 10)],
         },
      });
      const copy = buildCopyText(
         text,
         ext,
         { parent, importFrom: "m.malloy.0.materialize-level.malloy" },
         { liftAccess: true },
      );
      // The parent's stored hidden dimensions are gone, labels and all (a
      // label left behind would hide the next statement); the extension's
      // own label is lifted and its inherited internal fields lifted by the
      // wrap.
      expect(copy).toEqual({
         copy: `##! experimental.access_modifiers
import { stored__mat } from 'm.malloy.0.materialize-level.malloy'
source: orders is duckdb.table('ev')

source: stored is stored__mat extend {
  measure: total is amt.sum()
}

source: ext is stored extend {
  dimension: mine is amt + 1
} include { public: * }
`,
      });
      // The storage= build text carries the same lift, and its flag.
      const chain = storedChainText(text, ext, parent, { liftAccess: true });
      expect(chain?.flags).toEqual(["##! experimental.access_modifiers"]);
      expect(chain?.declarations).toContain(
         "dimension: mine is amt + 1\n} include { public: * }",
      );
      expect(chain?.declarations).not.toContain("private");
      expect(chain?.declarations).not.toContain("internal");
   });
});

describe("the generated companion, compiled into the build plan", () => {
   it(
      "builds a storage= extension over its parent's stored table when both are in one destination (P10)",
      async () => {
         const c = await compiled(`${BASE}
source: regions is duckdb.sql("select 'east' as code") extend {
  primary_key: code
}

#(materialize) storage="lake"
source: orders is orders_raw extend {
  dimension: big is amount > 10
  measure: total is amount.sum()
}

source: mid is orders extend { measure: n is count() }

#(materialize) storage="lake"
source: east is mid extend {
  where: region = 'east'
  dimension: label is concat(region, '!')
}

#(materialize) storage="other"
source: elsewhere is orders extend { dimension: w is region = 'west' }

#(materialize) storage="lake"
source: joined is orders extend {
  join_one: regions on region = regions.code
  dimension: amount2 is amount * 2
}
`);
         const plan = (name: string) =>
            generatedPlans(c).find((p) => p.sourceName === name)!;
         // Over its parent's stored table, through the unmaterialized `mid`:
         // the parent re-rooted on its build source, less the dimension its
         // table stores, then each declaration down to the extension's.
         expect(plan("east").buildsOver?.overStored).toEqual({
            parentBuildSourceName: "orders__mat",
            flags: [],
            declarations: `source: orders is orders__mat extend {
  measure: total is amount.sum()
}
source: mid is orders extend { measure: n is count() }
source: east is mid extend {
  where: region = 'east'
  dimension: label is concat(region, '!')
}`,
         });
         // Another destination, and a chain reaching a warehouse source the
         // destination cannot read (a join), build over the raw chain.
         expect(plan("elsewhere").buildsOver).toBeUndefined();
         expect(plan("joined").buildsOver).toBeUndefined();
         // Planned in order, the extension depending on its parent: the
         // build reads the parent's table, and its address stacks on it.
         const deps = c.builds.flatMap((b) => Object.entries(b.dependsOn));
         expect(Object.fromEntries(deps)[plan("east").sourceID]).toEqual([
            plan("orders").sourceID,
         ]);
         expect(addressSQL(sourcesIn(c)[plan("east").sourceID])).toContain(
            "big",
         );
      },
      { timeout: 30000 },
   );

   it(
      "builds an extension over the raw chain when its chained build does not compile",
      async () => {
         // `regions` is joined above the parent, so the parent's build source
         // does not carry it, and the extension's own dimension reads it.
         const c = await compiled(`${BASE}
source: regions is duckdb.sql("select 'east' as code, 'East' as label") extend {
  primary_key: code
}
source: joined is orders_raw extend {
  join_one: regions on region = regions.code
}

#(materialize) storage="lake"
source: orders is joined

#(materialize) inline=["regions"] storage="lake"
source: orders_ext is orders extend {
  dimension: label is regions.label
}
`);
         const ext = generatedPlans(c).find(
            (p) => p.sourceName === "orders_ext",
         )!;
         expect(ext.buildsOver).toBeUndefined();
         expect(ext.columns.map((col) => col.column)).toContain("label");
         expect(c.builds.flatMap((b) => b.order).length).toBe(2);
      },
      { timeout: 30000 },
   );
});
