// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `#@ persist` on a top-level `query:`: what the package's build plan holds,
 * and warns about, with PUBLISHER_MATERIALIZE off (a persisted query is not
 * built, warned `persist-query-not-built`, and answered live) and on.
 */

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { tempBuildPlanPackages } from "../../../test_helpers/incremental_test_harness";
import { installSettings } from "../../../test_helpers/settings_env";
import { compilePackageBuildPlan } from "./build_plan_compile";
import { PERSIST_QUERY_NOT_BUILT } from "./persisted_query";

const TABLE = `duckdb.sql("""
  select * from (values
    (1, 'aa', 't1', 10.5),
    (2, 'bb', 't2', 20.0)
  ) as t(id, carrier, tenant, d)
""")`;

const BASE = `source: base is ${TABLE} extend { measure: n is count() }\n`;

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

beforeEach(() => enable(false));
afterEach(async () => {
   restore?.();
   restore = undefined;
   await packages.cleanup();
});

async function compiledFiles(files: Record<string, string>) {
   const pkg = await packages.create(files);
   return compilePackageBuildPlan(pkg);
}

async function compiled(text: string) {
   return compiledFiles({ "m.malloy": text });
}

function codes(c: Awaited<ReturnType<typeof compiled>>): string[] {
   return (c.annotationFindings ?? []).map((f) => f.code);
}

describe("#@ persist on a named query, PUBLISHER_MATERIALIZE off", () => {
   it("builds nothing, and warns persist-query-not-built naming the workaround", async () => {
      const c = await compiled(
         `${BASE}#@ persist name="nq_tbl"\nquery: q is base -> { group_by: carrier; aggregate: n }\n`,
      );
      expect(Object.keys(c.sources)).toEqual([]);
      const finding = (c.annotationFindings ?? []).find(
         (f) => f.code === PERSIST_QUERY_NOT_BUILT,
      );
      expect(finding?.subject).toBe("q");
      expect(finding?.model).toBe("m.malloy");
      expect(finding?.tier).toBe("unservable");
      expect(finding?.message).toContain("answered live");
      expect(finding?.message).toContain("source: q_result is q");
      // Never the source warning, which told the author to "persist a query
      // source" about something that already is a query.
      expect(c.droppedPersistSources ?? []).toEqual([]);
   });

   it("reports a misspelled key on the query's line", async () => {
      const c = await compiled(
         `${BASE}#@ persist nmae="nq_tbl"\nquery: q is base -> { group_by: carrier; aggregate: n }\n`,
      );
      const typo = (c.annotationFindings ?? []).find(
         (f) => f.code === "persist-unknown-key",
      );
      expect(typo?.subject).toBe("q");
      expect(typo?.message).toContain('#@ persist query "q"');
      expect(typo?.message).toContain("did you mean `name=`");
   });

   it("warns once, in the file that declares the query, not in one that imports it", async () => {
      const c = await compiledFiles({
         "a.malloy": `${BASE}#@ persist\nquery: q is base -> { group_by: carrier; aggregate: n }\n`,
         "b.malloy": `import "a.malloy"\nrun: q\n`,
      });
      const found = (c.annotationFindings ?? []).filter(
         (f) => f.code === PERSIST_QUERY_NOT_BUILT,
      );
      expect(found.map((f) => f.model)).toEqual(["a.malloy"]);
   });

   it("says nothing about a query without the annotation, or one that opts out", async () => {
      const c = await compiled(
         `${BASE}query: q is base -> { group_by: carrier }\n#@ -persist\nquery: q2 is base -> { group_by: carrier }\n`,
      );
      expect(codes(c)).not.toContain(PERSIST_QUERY_NOT_BUILT);
   });

   it("still plans a persisted source over the query, the workaround", async () => {
      const c = await compiled(
         `##! experimental.persistence\n${BASE}query: q is base -> { group_by: carrier; aggregate: n }\n#@ persist name="q_tbl"\nsource: q_result is q\n`,
      );
      expect(Object.values(c.sources).map((s) => s.name)).toEqual(["q_result"]);
      expect(codes(c)).not.toContain(PERSIST_QUERY_NOT_BUILT);
   });
});
