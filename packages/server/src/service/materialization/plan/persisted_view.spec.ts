// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `#@ persist` on a view (materialization v2, phase 4, P14): which views are
 * planned, the generated companion's text, and what the package's build plan
 * holds, warns about and leaves alone with PUBLISHER_MATERIALIZE on and off.
 */

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { tempBuildPlanPackages } from "../../../test_helpers/incremental_test_harness";
import { installSettings } from "../../../test_helpers/settings_env";
import { compilePackageBuildPlan } from "./build_plan_compile";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
} from "@malloydata/malloy";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { viewNoteTexts } from "./persisted_view_compile";
import {
   generatePersistedViewFile,
   PERSIST_VIEW_NOT_BUILT,
   type PersistedViewPlan,
} from "./persisted_view";

const TABLE = `duckdb.sql("""
  select * from (values
    (1, 'aa', 't1', 10.5, timestamp '2026-01-01 10:00:00'),
    (2, 'bb', 't2', 20.0, timestamp '2026-01-02 10:00:00')
  ) as t(id, carrier, tenant, d, ts)
""")`;

const packages = tempBuildPlanPackages();
let restore: (() => void) | undefined;

function enable(on: boolean, gated = false) {
   restore?.();
   restore = installSettings({
      env: {
         PUBLISHER_MATERIALIZE: on ? "true" : "false",
         PUBLISHER_MATERIALIZE_GATED_SOURCES: gated ? "true" : "false",
      },
   }).restore;
}

beforeEach(() => enable(true));
afterEach(async () => {
   restore?.();
   restore = undefined;
   await packages.cleanup();
});

async function compiled(text: string) {
   const pkg = await packages.create({ "m.malloy": text });
   return compilePackageBuildPlan(pkg);
}

async function compiledFiles(files: Record<string, string>) {
   const pkg = await packages.create(files);
   return compilePackageBuildPlan(pkg);
}

function viewSources(c: Awaited<ReturnType<typeof compiled>>): string[] {
   return Object.values(c.sources)
      .map((s) => s.name)
      .filter((n) => n.endsWith("__view"))
      .sort();
}

function viewFindings(c: Awaited<ReturnType<typeof compiled>>): string[] {
   return (c.annotationFindings ?? [])
      .filter((f) => f.code === PERSIST_VIEW_NOT_BUILT)
      .map((f) => f.message);
}

describe("generatePersistedViewFile", () => {
   const plan = (o: Partial<PersistedViewPlan>): PersistedViewPlan => ({
      sourceID: "ops@file:///pkg/m.malloy",
      sourceName: "ops",
      viewName: "by_carrier",
      buildSourceName: "ops__by_carrier__view",
      buildSourceID:
         "ops__by_carrier__view@file:///pkg/m.malloy.persist-view.malloy",
      persistLines: ['#@ persist name="ops_by_carrier"'],
      declaringURL: "file:///pkg/m.malloy",
      ...o,
   });

   it("writes one query-shaped persist source per view, the author's line verbatim", () => {
      expect(generatePersistedViewFile([plan({})], "m.malloy")).toBe(
         `##! experimental.persistence
import { ops__pv_base is ops } from "m.malloy"

#@ persist name="ops_by_carrier"
source: ops__by_carrier__view is ops__pv_base -> by_carrier
`,
      );
   });

   it("quotes names Malloy would not read bare", () => {
      const text = generatePersistedViewFile(
         [
            plan({
               sourceName: "source",
               viewName: "by carrier",
               buildSourceName: "source__by carrier__view",
            }),
         ],
         "m.malloy",
      );
      expect(text).toContain("import { source__pv_base is `source` }");
      expect(text).toContain(
         "source: `source__by carrier__view` is source__pv_base -> `by carrier`",
      );
   });

   it("writes nothing for no views", () => {
      expect(generatePersistedViewFile([], "m.malloy")).toBeUndefined();
   });
});

describe("the build plan for #@ persist views", () => {
   const model = `##! experimental.persistence
source: ops is ${TABLE} extend {
  measure: n is count()
  #@ persist name="ops_by_carrier"
  view: by_carrier is { group_by: carrier; aggregate: n }
  view: plain is { group_by: carrier }
}

source: ops_ext is ops extend { where: carrier = 'aa' }
`;

   it("plans one build source per persisted view, on the source that writes it", async () => {
      const c = await compiled(model);
      // The extension inherits the view, but does not declare it.
      expect(viewSources(c)).toEqual(["ops__by_carrier__view"]);
      expect(viewFindings(c)).toEqual([]);
   });

   it("plans nothing with PUBLISHER_MATERIALIZE off", async () => {
      enable(false);
      const c = await compiled(model);
      expect(viewSources(c)).toEqual([]);
   });

   it("warns about and leaves out a caller-dependent view", async () => {
      const c = await compiled(`##! experimental { persistence givens }
given: CARRIER :: string is "aa"
source: ops is ${TABLE} extend {
  measure: n is count()
  #@ persist name="by_given"
  view: by_given is { where: carrier = $CARRIER; group_by: carrier; aggregate: n }
  #@ persist name="recent"
  view: recent is { where: ts > now - 1 day; group_by: carrier; aggregate: n }
}
`);
      expect(viewSources(c)).toEqual([]);
      const findings = viewFindings(c);
      expect(findings.some((m) => m.includes("reads a given"))).toBe(true);
      expect(findings.some((m) => m.includes("current time"))).toBe(true);
   });

   it("refuses an incremental key and an @env. reference", async () => {
      const c = await compiled(`##! experimental.persistence
source: ops is ${TABLE} extend {
  measure: n is count()
  #@ persist name="w" watermark="ts"
  view: w is { group_by: carrier; aggregate: n }
  #@ persist name=@env.TABLE_NAME
  view: e is { group_by: carrier; aggregate: n }
}
`);
      expect(viewSources(c)).toEqual([]);
      const findings = viewFindings(c);
      expect(findings.some((m) => m.includes("incremental"))).toBe(true);
      expect(findings.some((m) => m.includes("@env."))).toBe(true);
   });

   it("refuses a nested view stored with storage=, and builds it colocated", async () => {
      const c = await compiled(`##! experimental.persistence
source: ops is ${TABLE} extend {
  measure: n is count()
  #@ persist storage="lake"
  view: stored is { group_by: carrier; nest: t is { group_by: tenant; aggregate: n } }
  #@ persist name="colocated_nest"
  view: colocated is { group_by: carrier; nest: t is { group_by: tenant; aggregate: n } }
}
`);
      expect(viewSources(c)).toEqual(["ops__colocated__view"]);
      expect(viewFindings(c).some((m) => m.includes("nests"))).toBe(true);
   });

   describe("a gated source (question 3)", () => {
      const gated = `##! experimental { persistence givens }
given: TENANT :: string is ""
#(access_filter) tenant = $TENANT
source: ops is ${TABLE} extend {
  measure: n is count()
  #@ persist name="by_tenant"
  view: by_tenant is { group_by: tenant, carrier; aggregate: n }
  #@ persist name="by_carrier"
  view: by_carrier is { group_by: carrier; aggregate: n }
  #@ persist name="top"
  view: top is { group_by: tenant, carrier; aggregate: n; limit: 1 }
  #@ persist name="share"
  view: share is { group_by: tenant; aggregate: n, all_n is all(n) }
}
`;
      it("builds only a view that groups by the access filter's field, and warns about the rest", async () => {
         const c = await compiled(gated);
         expect(viewSources(c)).toEqual(["ops__by_tenant__view"]);
         const findings = viewFindings(c);
         expect(findings.some((m) => m.includes("`tenant`"))).toBe(true);
         expect(findings.some((m) => m.includes("limit:"))).toBe(true);
         expect(findings.some((m) => m.includes("all()"))).toBe(true);
      });

      it("reads a private access-filter column the source hides, as it reads a public one", async () => {
         // The gate is read where it was declared, so a `private` tenant
         // column is as readable to it as a public one (it was refused before:
         // the probe read at a caller's level).
         const c = await compiled(
            gated
               .replace(
                  "experimental { persistence givens }",
                  "experimental { persistence givens access_modifiers }",
               )
               .replace(
                  `source: ops is ${TABLE} extend {`,
                  `source: ops is ${TABLE} include { private: tenant; public: * } extend {`,
               ),
         );
         expect(viewSources(c)).toEqual(["ops__by_tenant__view"]);
         const findings = viewFindings(c);
         expect(findings.some((m) => m.includes("`tenant`"))).toBe(true);
         expect(findings.some((m) => m.includes("could not be read"))).toBe(
            false,
         );
      });
   });
});

describe("which source declares a persisted view", () => {
   // Malloy copies a view, with its notes, onto every extension, rename and
   // import of its source. Only the statement that annotates the view
   // declares it (governance/ir/declared_notes.ts, declaredFieldNotes).
   const persisted = `#@ persist
  view: v is { group_by: carrier; aggregate: n }`;
   const base = (name: string) =>
      `source: ${name} is ${TABLE} extend {
  measure: n is count()
  ${persisted}
}`;

   it("plans same-named views on two sources in one file, one each", async () => {
      const c = await compiled(`##! experimental.persistence
${base("a")}
${base("b")}
`);
      expect(viewSources(c)).toEqual(["a__v__view", "b__v__view"]);
   });

   it("plans a view an extension redefines, on the extension too", async () => {
      const c = await compiled(`##! experimental.persistence
${base("a")}
source: b is a extend {
  except: v
  #@ persist
  view: v is { group_by: tenant; aggregate: n }
}
`);
      expect(viewSources(c)).toEqual(["a__v__view", "b__v__view"]);
   });

   it("plans an imported source's view only in its own file", async () => {
      const c = await compiledFiles({
         "f.malloy": `##! experimental.persistence
${base("a")}
source: c is a extend { where: carrier = 'aa' }
`,
         "m.malloy": `##! experimental.persistence
import { c } from "f.malloy"
source: b is c extend { dimension: x is 1 }
`,
      });
      expect(viewSources(c)).toEqual(["a__v__view"]);
   });

   it("plans a view declared in an extend of a rename on that extension alone", async () => {
      const c = await compiled(`##! experimental.persistence
${base("a")}
source: r is a
source: re is r extend {
  #@ persist
  view: w is { group_by: tenant; aggregate: n }
}
`);
      expect(viewSources(c)).toEqual(["a__v__view", "re__w__view"]);
   });

   it("plans a view once for a multi-definition statement", async () => {
      const c = await compiled(`##! experimental.persistence
${base("a")}, b is a extend { dimension: x is 1 }
`);
      expect(viewSources(c)).toEqual(["a__v__view"]);
   });

   it("does not plan a view an extension renames (its notes are the base's)", async () => {
      const c = await compiled(`##! experimental.persistence
${base("a")}
source: b is a extend { rename: v2 is v }
`);
      expect(viewSources(c)).toEqual(["a__v__view"]);
   });

   it("plans an alias that carries its own #@ persist", async () => {
      const c = await compiled(`##! experimental.persistence
${base("a")}
source: b is a extend {
  #@ persist
  view: v2 is v
}
`);
      expect(viewSources(c)).toEqual(["a__v__view", "b__v2__view"]);
   });

   it("plans a view an include: re-annotates with #@ persist, on the including source", async () => {
      const c =
         await compiled(`##! experimental { persistence access_modifiers }
source: a is ${TABLE} extend {
  measure: n is count()
  view: v is { group_by: carrier; aggregate: n }
}
source: b is a include {
  #@ persist
  public: v
  public: *
}
`);
      expect(viewSources(c)).toEqual(["b__v__view"]);
   });
});

describe("viewNoteTexts", () => {
   it("carries the render tags an alias or an include inherits, never a #@ line", async () => {
      const duckdb = new DuckDBConnection("duckdb", ":memory:");
      try {
         const url = "file:///pkg/m.malloy";
         const runtime = new Runtime({
            urlReader: new InMemoryURLReader(
               new Map([
                  [
                     url,
                     `##! experimental.access_modifiers
source: a is ${TABLE} extend {
  measure: n is count()
  # bar_chart
  view: v is { group_by: carrier; aggregate: n }
}
source: b is a extend {
  #@ persist
  view: v2 is v
}
source: c is a include {
  #@ persist
  public: v
  public: *
}
`,
                  ],
               ]),
            ),
            connections: new FixedConnectionMap(
               new Map([["duckdb", duckdb]]),
               "duckdb",
            ),
         });
         const model = await runtime.loadModel(new URL(url)).getModel();
         const contents = (
            model as unknown as {
               _modelDef: {
                  contents: Record<
                     string,
                     {
                        fields: {
                           name: string;
                           as?: string;
                           annotations?: never;
                        }[];
                     }
                  >;
               };
            }
         )._modelDef.contents;
         const notesOf = (source: string, view: string) =>
            viewNoteTexts(
               contents[source].fields.find((f) => (f.as ?? f.name) === view)
                  ?.annotations,
            );
         expect(notesOf("a", "v")).toEqual(["# bar_chart"]);
         expect(notesOf("b", "v2")).toEqual(["# bar_chart"]);
         expect(notesOf("c", "v")).toEqual(["# bar_chart"]);
      } finally {
         await duckdb.close();
      }
   });
});
