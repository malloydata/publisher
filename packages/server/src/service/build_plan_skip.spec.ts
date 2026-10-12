// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import * as fs from "fs/promises";
import * as os from "os";
import * as path from "path";

import { computePackageBuildPlan } from "./build_plan";
import { Environment } from "./environment";
import { Model } from "./model";
import type { Package } from "./package";

/**
 * The package load compiles a model for the persist build plan only when the
 * worker's compile of it showed a `##! experimental.persistence` flag or a
 * usable `#@ preaggregate` — the two things `compilePackageBuildPlan` reads a
 * model for. These pin both halves: a package that uses neither is not
 * recompiled at all, and a package that does gets exactly the plan a full
 * recompile of every file produces.
 */
describe("build plan skips models that cannot contribute", () => {
   let rootDir: string;
   let envPath: string;

   async function loadPackage(files: Record<string, string>): Promise<Package> {
      const dir = path.join(envPath, "pkg");
      await fs.mkdir(dir, { recursive: true });
      await fs.writeFile(
         path.join(dir, "publisher.json"),
         JSON.stringify({ name: "pkg", description: "fixture" }),
      );
      for (const [name, text] of Object.entries(files)) {
         await fs.writeFile(path.join(dir, name), text);
      }
      const env = await Environment.create("testEnv", envPath, []);
      await env.addPackage("pkg");
      return env.getPackage("pkg", false);
   }

   /** The model paths `Model.getModelRuntime` compiled, while `fn` ran. */
   async function compiledPaths<T>(
      fn: () => Promise<T>,
   ): Promise<{ result: T; paths: string[] }> {
      const spy = spyOn(Model, "getModelRuntime");
      try {
         const result = await fn();
         return {
            result,
            paths: spy.mock.calls.map((call) => String(call[1])).sort(),
         };
      } finally {
         spy.mockRestore();
      }
   }

   beforeEach(async () => {
      rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-bp-skip-"));
      envPath = path.join(rootDir, "env");
      await fs.mkdir(envPath, { recursive: true });
   });

   afterEach(async () => {
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
   });

   const BASE = `source: base is duckdb.sql("SELECT 1 AS x, 'a' AS c") extend {
  measure: n is count()
}
`;
   const PLAIN = `import "base.malloy"
source: plain is base extend {
  view: by_c is { group_by: c; aggregate: n }
}
`;
   const PERSIST = `##! experimental.persistence
import "base.malloy"

#@ persist name="p_table"
source: p is duckdb.sql("SELECT 2 AS y")

source: q is base
`;
   const PREAGG = `source: orders is duckdb.sql("""
  SELECT * FROM (VALUES (10, 'A'), (20, 'B')) AS t(amount, category)
""") extend {
  #@ preaggregate grain="category"
  measure: total is amount.sum()
}
`;

   it(
      "compiles nothing for the plan when no model persists or pre-aggregates",
      async () => {
         const { result: pkg, paths } = await compiledPaths(() =>
            loadPackage({ "base.malloy": BASE, "plain.malloy": PLAIN }),
         );
         expect(paths).toEqual([]);
         expect(pkg.getBuildPlan()).toBeNull();
         // And the skip changed nothing: a full recompile plans the same.
         const full = await computePackageBuildPlan(pkg);
         expect(full.plan).toBeNull();
         expect(full.droppedPersistSources).toEqual([]);
      },
      { timeout: 60000 },
   );

   it(
      "compiles only the contributing models, and plans exactly what a full recompile plans",
      async () => {
         const { result: pkg, paths } = await compiledPaths(() =>
            loadPackage({
               "base.malloy": BASE,
               "plain.malloy": PLAIN,
               "persist.malloy": PERSIST,
               "preagg.malloy": PREAGG,
            }),
         );
         // persist.malloy for its flag; preagg.malloy for its rollup, plus the
         // synthesized companion compile (same model path) the rollup needs.
         expect(new Set(paths)).toEqual(
            new Set(["persist.malloy", "preagg.malloy"]),
         );
         expect(paths).not.toContain("base.malloy");
         expect(paths).not.toContain("plain.malloy");

         const loaded = pkg.getBuildPlan();
         expect(loaded).not.toBeNull();
         const names = Object.values(loaded?.sources ?? {})
            .map((s) => s.name)
            .sort();
         expect(names).toContain("p");
         expect(names.some((n) => n.startsWith("orders__"))).toBe(true);

         // The reference: every file compiled, no skip.
         const full = await computePackageBuildPlan(pkg);
         expect(loaded).toEqual(full.plan);
      },
      { timeout: 60000 },
   );

   it(
      "reports the build-plan inputs it gates on per model",
      async () => {
         const pkg = await loadPackage({
            "base.malloy": BASE,
            "plain.malloy": PLAIN,
            "persist.malloy": PERSIST,
            "preagg.malloy": PREAGG,
         });
         const contributes = (p: string) =>
            pkg.getModel(p)?.mayContributeToBuildPlan();
         expect(contributes("base.malloy")).toBe(false);
         expect(contributes("plain.malloy")).toBe(false);
         expect(contributes("persist.malloy")).toBe(true);
         expect(contributes("preagg.malloy")).toBe(true);
      },
      { timeout: 60000 },
   );
});
