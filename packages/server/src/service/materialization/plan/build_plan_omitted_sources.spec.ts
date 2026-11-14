// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `BuildPlan.omittedSources`: every path that leaves a source out of the
 * build plan because planning it FAILED reports it, by sourceID and a code,
 * so a host never reads a planner failure as the source leaving the plan.
 * Each companion case plans the same file twice, once with a working runtime
 * factory and once with one that fails, and checks the omission names
 * exactly the sources the working plan held.
 */

import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   Runtime,
   type ConnectionBuild,
   type PersistSource,
   type URLReader,
} from "@malloydata/malloy";
import { afterAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { pathToFileURL } from "url";
import {
   collectModelBuildPlan,
   deriveBuildPlan,
   deriveBuildPlanOutcome,
   emptyBuildPlanParts,
   plannedSources,
   type BuildPlanParts,
} from "./build_plan";
import { finalizeOmissions, omittedFromFile } from "./omitted_sources";
import { fakeSource } from "../../../test_helpers/materialization_test_fixtures";
import { installSettings } from "../../../test_helpers/settings_env";

const duckdb = new DuckDBConnection("duckdb", ":memory:");
const connections = new FixedConnectionMap(
   new Map([["duckdb", duckdb]]),
   "duckdb",
);
afterAll(() => duckdb.close());

function reader(overlay: ReadonlyMap<string, string>): URLReader {
   return {
      readURL: async (url: URL) => {
         const hit = overlay.get(url.href);
         if (hit !== undefined) return hit;
         return fs.readFileSync(url, "utf8");
      },
   };
}

/**
 * Plan one model file. `companionsFail`: the factory every generated
 * companion (and the v1 build copy) compiles through throws, as a companion
 * whose generated text does not compile would. `"unreadable"`: the factory
 * works, but the files it reads with no overlay cannot be read, so a `#@`
 * line of the author's file cannot be set aside. `"overlay"`: the author's
 * files read, and only a compile with generated text in it fails.
 */
async function plan(
   text: string,
   env: Record<string, string>,
   companionsFail: boolean | "unreadable" | "overlay",
): Promise<{
   parts: BuildPlanParts;
   /** The package directory, spelled as a file URL's path. */
   dir: string;
}> {
   const dir = fs.mkdtempSync(path.join(os.tmpdir(), "omitted-sources-"));
   fs.writeFileSync(path.join(dir, "m.malloy"), text);
   const { restore } = installSettings({ env });
   try {
      const base = pathToFileURL(dir + "/");
      const runtime = new Runtime({
         urlReader: reader(new Map()),
         connections,
      });
      const materializer = runtime.loadModel(new URL("m.malloy", base), {
         importBaseURL: base,
      });
      const parts = emptyBuildPlanParts();
      await collectModelBuildPlan(parts, {
         modelPath: "m.malloy",
         packagePath: dir,
         materializer,
         malloyModel: await materializer.getModel(),
         runtime,
         getRuntime: async (overlay) => {
            if (companionsFail === "unreadable" && overlay.size === 0) {
               return {
                  runtime: new Runtime({
                     urlReader: {
                        readURL: async () => {
                           throw new Error("unreadable");
                        },
                     },
                     connections,
                  }),
                  importBaseURL: base,
               };
            }
            if (
               companionsFail === true ||
               (companionsFail === "overlay" && overlay.size > 0)
            ) {
               throw new Error(
                  "generated companion did not compile: SELECT 'secret-value'",
               );
            }
            return {
               runtime: new Runtime({
                  urlReader: reader(overlay),
                  connections,
               }),
               importBaseURL: base,
            };
         },
      });
      // As the package appears inside a source ID's file URL, so a test can
      // replace it: on Windows the path is D:\..., the URL /D:/....
      return { parts, dir: pathToFileURL(dir).pathname };
   } finally {
      restore();
   }
}

/** The wire plan a package load would keep from `parts`. */
function wirePlan(parts: BuildPlanParts) {
   return deriveBuildPlanOutcome(
      { ...parts, connectionDigests: { duckdb: "digest" } },
      null,
   ).plan;
}

const V1_ROLLUP = `##! experimental { persistence composite_sources }
source: orders is duckdb.sql("""
  SELECT * FROM (VALUES (10, 'A'), (20, 'B')) AS t(amount, category)
""") extend {
  #@ preaggregate grain="category"
  measure: total is amount.sum()
}
`;

describe("BuildPlan.omittedSources: companion failures", () => {
   it("a clean plan reports an empty list", async () => {
      const { parts } = await plan(
         `##! experimental.persistence
source: base is duckdb.sql("select 1 as a")
#@ persist
source: p is base -> { group_by: a }
`,
         {},
         false,
      );
      const wire = wirePlan(parts);
      expect(Object.values(wire?.sources ?? {}).map((s) => s.name)).toEqual([
         "p",
      ]);
      expect(wire?.omittedSources).toEqual([]);
      expect(wire?.omittedFiles).toEqual([]);
   });

   it("a rollup companion that fails reports the v1 rollups as rollup_companion_failed", async () => {
      const env = {};
      const good = await plan(V1_ROLLUP, env, false);
      const planned = Object.keys(good.parts.preaggregatePlans).map((id) =>
         id.replace(good.dir, "<pkg>"),
      );
      expect(planned).toHaveLength(1);
      expect(planned[0]).toMatch(/__rollup__.*@file:.*\.preaggregate\.malloy$/);

      const bad = await plan(V1_ROLLUP, env, true);
      expect(
         wirePlan(bad.parts)?.omittedSources?.map((o) => ({
            ...o,
            sourceID: o.sourceID.replace(bad.dir, "<pkg>"),
         })),
      ).toEqual(
         planned.map((sourceID) => ({
            sourceID,
            reason: "rollup_companion_failed",
         })),
      );
   });

   it("a rollup companion whose author file cannot be set aside reports its rollups as rollup_companion_blanking_failed", async () => {
      // A `#@ persist` line is what the companion must set aside.
      const text = `${V1_ROLLUP}
#@ persist
source: by_category is orders -> { group_by: category }
`;
      const env = {};
      const good = await plan(text, env, false);
      const planned = Object.keys(good.parts.preaggregatePlans).map((id) =>
         id.replace(good.dir, "<pkg>"),
      );
      expect(planned).toHaveLength(1);
      const bad = await plan(text, env, "unreadable");
      expect(
         wirePlan(bad.parts)?.omittedSources?.map((o) => ({
            ...o,
            sourceID: o.sourceID.replace(bad.dir, "<pkg>"),
         })),
      ).toEqual(
         planned.map((sourceID) => ({
            sourceID,
            reason: "rollup_companion_blanking_failed",
         })),
      );
   });

   it("a v1 build copy that fails reports the file's #@ persist sources as v1_build_companion_failed", async () => {
      const text = `source: base is duckdb.sql("select 1 as a, 'x' as b")
#@ persist
source: p is base extend { measure: n is count() } -> { group_by: a, b; aggregate: n }
`;
      const env = {};
      const good = await plan(text, env, false);
      const planned = Object.keys(good.parts.sources).map((id) =>
         id.replace(good.dir, "<pkg>"),
      );
      expect(planned).toHaveLength(1);

      // The copy's text unreadable, or the copy not compiling.
      for (const fails of [true, "overlay"] as const) {
         const bad = await plan(text, env, fails);
         expect(Object.keys(bad.parts.sources)).toEqual([]);
         expect(
            wirePlan(bad.parts)?.omittedSources?.map((o) => ({
               ...o,
               sourceID: o.sourceID.replace(bad.dir, "<pkg>"),
            })),
         ).toEqual(
            planned.map((sourceID) => ({
               sourceID,
               reason: "v1_build_companion_failed",
            })),
         );
      }
   });

   it("a #(materialize) companion that fails reports its sources as materialize_companion_failed", async () => {
      const text = `#(materialize) name="plain_t"
source: plain is duckdb.sql("select 1 as a") extend {
  measure: n is count()
}
`;
      const env = { PUBLISHER_MATERIALIZE: "true" };
      const good = await plan(text, env, false);
      const planned = Object.keys(good.parts.materializePlans).map((id) =>
         id.replace(good.dir, "<pkg>"),
      );
      expect(planned).toHaveLength(1);

      const bad = await plan(text, env, true);
      expect(
         wirePlan(bad.parts)?.omittedSources?.map((o) => ({
            ...o,
            sourceID: o.sourceID.replace(bad.dir, "<pkg>"),
         })),
      ).toEqual(
         planned.map((sourceID) => ({
            sourceID,
            reason: "materialize_companion_failed",
         })),
      );
   });

   it("a persisted-view companion that fails reports its views as persisted_view_companion_failed", async () => {
      const text = `source: ops is duckdb.sql("select 1 as id, 'aa' as carrier") extend {
  measure: n is count()
  #@ persist name="ops_by_carrier"
  view: by_carrier is { group_by: carrier; aggregate: n }
}
`;
      const env = { PUBLISHER_MATERIALIZE: "true" };
      const good = await plan(text, env, false);
      const planned = Object.keys(good.parts.sources)
         .filter((id) => id.includes("__view@"))
         .map((id) => id.replace(good.dir, "<pkg>"));
      expect(planned).toHaveLength(1);

      const bad = await plan(text, env, true);
      expect(
         wirePlan(bad.parts)?.omittedSources?.map((o) => ({
            ...o,
            sourceID: o.sourceID.replace(bad.dir, "<pkg>"),
         })),
      ).toEqual(
         planned.map((sourceID) => ({
            sourceID,
            reason: "persisted_view_companion_failed",
         })),
      );
   });
});

describe("BuildPlan.omittedSources: a #@ line that does not parse", () => {
   // Malloy plans a source none of whose `#@` lines parse as no source at
   // all, and the finding names a line, not a source: without an entry here
   // the source would read as having left the plan.
   const BROKEN = `source: base is duckdb.sql("select 1 as a")
#@ persist name="unterminated
source: p is base -> { group_by: a }
#@ persist
source: q is base -> { group_by: a }
`;
   for (const [file, text] of [
      ["a flagged file", `##! experimental.persistence\n${BROKEN}`],
      ["an unflagged file", BROKEN],
   ] as const) {
      it(`${file}: reports the source as persist_annotation_unparseable, and only it`, async () => {
         const { parts, dir } = await plan(text, {}, false);
         const wire = wirePlan(parts);
         expect(Object.values(wire?.sources ?? {}).map((s) => s.name)).toEqual([
            "q",
         ]);
         expect(
            wire?.omittedSources?.map((o) => ({
               ...o,
               sourceID: o.sourceID.replace(dir, "<pkg>"),
            })),
         ).toEqual([
            {
               sourceID: "p@file://<pkg>/m.malloy",
               reason: "persist_annotation_unparseable",
            },
         ]);
      });
   }

   it("a source another #@ line of which parses is planned, not omitted", async () => {
      const { parts } = await plan(
         `##! experimental.persistence
source: base is duckdb.sql("select 1 as a")
#@ persist
#@ refresh="unterminated
source: p is base -> { group_by: a }
`,
         {},
         false,
      );
      const wire = wirePlan(parts);
      expect(Object.values(wire?.sources ?? {}).map((s) => s.name)).toEqual([
         "p",
      ]);
      expect(wire?.omittedSources).toEqual([]);
   });
});

describe("BuildPlan.omittedSources: projection failures", () => {
   /** A copy of `like` that getBuildTargets reported unbuildable. */
   function unkeyableLike(like: PersistSource, name: string): PersistSource {
      const source = { ...like, name, sourceID: name } as PersistSource;
      plannedSources({
         connections: [] as ConnectionBuild[],
         unbuildable: [
            {
               source,
               problem: { message: "given TENANT has no value" },
            },
         ] as never,
      });
      return source;
   }

   it("reports an unkeyable source, a never-planned source and a gate that errors, each by its code and never its message", () => {
      const ok = fakeSource({ name: "ok", sourceEntityId: "e-ok" });
      const unkeyable = unkeyableLike(ok, "unkeyable");
      // Not from plannedSources at all: addressing throws.
      const stray = {
         ...ok,
         name: "stray",
         sourceID: "stray",
      } as PersistSource;
      // A rollup whose gate cannot be read: the gate fails with an error,
      // not a refusal.
      const broken = fakeSource({ name: "broken", sourceEntityId: "e-broken" });
      const brokenRollup = {
         variant: true,
         get baseGated(): boolean {
            throw new Error("SELECT 'secret-value' FROM t");
         },
      };
      const sources = { ok, unkeyable, stray, broken };
      const wire = deriveBuildPlan(
         [
            {
               connectionName: "duckdb",
               order: Object.keys(sources),
               dependsOn: {},
            },
         ],
         sources,
         { duckdb: "digest" },
         undefined,
         undefined,
         null,
         { preaggregatePlans: { broken: brokenRollup as never } },
      );
      expect(Object.keys(wire.sources)).toEqual(["ok"]);
      expect(wire.omittedSources).toEqual([
         { sourceID: "broken", reason: "eligibility_check_failed" },
         { sourceID: "stray", reason: "projection_failed" },
         { sourceID: "unkeyable", reason: "unkeyable" },
      ]);
      expect(JSON.stringify(wire.omittedSources)).not.toContain("secret");
   });

   it("drops a recorded omission the plan did list, keeps the first reason per sourceID, and sorts", () => {
      const ok = fakeSource({ name: "ok2", sourceEntityId: "e-ok2" });
      const wire = deriveBuildPlan(
         [{ connectionName: "duckdb", order: ["ok2"], dependsOn: {} }],
         { ok2: ok },
         { duckdb: "digest" },
         undefined,
         undefined,
         null,
         {
            omittedSources: [
               { sourceID: "z@f", reason: "rollup_companion_failed" },
               // Another model's walk planned it: not omitted.
               { sourceID: "ok2", reason: "v1_build_companion_failed" },
               { sourceID: "a@f", reason: "materialize_companion_failed" },
               { sourceID: "z@f", reason: "projection_failed" },
            ],
         },
      );
      expect(wire.omittedSources).toEqual([
         { sourceID: "a@f", reason: "materialize_companion_failed" },
         { sourceID: "z@f", reason: "rollup_companion_failed" },
      ]);
   });

   it("reports a companion whose planner failed before naming its sources as an omitted file, never an in-band sourceID", () => {
      expect(
         omittedFromFile(
            "file:///p/m.malloy.preaggregate.malloy",
            undefined,
            "rollup_companion_failed",
         ),
      ).toEqual([
         {
            file: "file:///p/m.malloy.preaggregate.malloy",
            reason: "rollup_companion_failed",
         },
      ]);
      // A source may be named `*` (Malloy plans `*@<file>`), so a file
      // written as `*@<file>` would read as, and be dropped beside, it.
      const star = fakeSource({ name: "*", sourceEntityId: "e-star" });
      const wire = deriveBuildPlan(
         [{ connectionName: "duckdb", order: ["*@f"], dependsOn: {} }],
         { "*@f": star },
         { duckdb: "digest" },
         undefined,
         undefined,
         null,
         {
            omittedSources: [
               { file: "f", reason: "materialize_companion_failed" },
               { file: "f", reason: "projection_failed" },
               { file: "a", reason: "rollup_companion_failed" },
            ],
         },
      );
      expect(Object.keys(wire.sources)).toEqual(["*@f"]);
      expect(wire.omittedSources).toEqual([]);
      expect(wire.omittedFiles).toEqual([
         { file: "a", reason: "rollup_companion_failed" },
         { file: "f", reason: "materialize_companion_failed" },
      ]);
      expect(finalizeOmissions([], new Set())).toEqual({
         omittedSources: [],
         omittedFiles: [],
      });
   });
});
