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
   });
});
