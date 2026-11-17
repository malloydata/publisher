// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The v1 cutover's conformance tooling (6e, plan question 15): the dry run's
 * serve path against what actually serves, the conformance runner as a
 * library and as the standalone CLI, and the runner's classification of
 * refusals.
 */

import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { afterEach, describe, expect, it } from "bun:test";
import { DATA_FILES, RAW_MODEL } from "./conformance/data";
import {
   freshLoadPool,
   loadFixture,
   withSwitch,
} from "../../../src/test_helpers/conformance/package_runner";
import { renderFiles, SHAPES, type Variant } from "./conformance/shapes";
import { runV1CutoverConformance } from "../../../src/test_helpers/conformance/v1_cutover_runner";
import { writePackageFiles } from "../../../src/test_helpers/temp_package";

/**
 * The v1 cutover's dry-run verdict (6e, plan question 15):
 * `POST …/packages?dryRun=true` names the path that will serve each
 * materialized source (`cutoverPath`: `served_copy`, `rollup`, or `live` with
 * `cutoverReason`), judged by compiling the served copy over bindings
 * declared from the plan, before any table is built.
 */
describe("the dry run's verdicts", () => {
   async function preview(
      shapeName: string,
      variant: Variant,
      on: "on" | "cutover",
   ): Promise<Record<string, [string | undefined, string | undefined]>> {
      const restore = withSwitch(on);
      await freshLoadPool();
      const shape = SHAPES.find((s) => s.name === shapeName)!;
      const loader = await loadFixture({
         ...DATA_FILES,
         ...renderFiles(shape, variant),
      });
      try {
         const pkg = (await loader.bind({ includeRollups: true })).pkg;
         const verdicts = await pkg.previewServeVerdicts();
         return Object.fromEntries(
            verdicts.map((v) => [
               v.sourceName,
               [v.cutoverPath, v.cutoverReason],
            ]),
         );
      } finally {
         restore();
         await loader.dispose();
      }
   }

   describe("v1 cutover: the dry run's serve path", () => {
      afterEach(freshLoadPool);

      it("names the served copy for v1 storage= and colocated sources", async () => {
         for (const variant of ["storage", "database"] as const) {
            const paths = await preview("baseline", variant, "cutover");
            expect(paths.daily).toEqual(["served_copy", undefined]);
         }
      }, 120_000);

      it("names the served copy for a source with hidden columns, colocated or storage= (G2)", async () => {
         for (const variant of ["database", "storage"] as const) {
            const paths = await preview(
               "joined persisted source with hidden fields, from an index",
               variant,
               "cutover",
            );
            expect(paths.customer_dim?.[0]).toBe("served_copy");
            expect(paths.order_facts?.[0]).toBe("served_copy");
         }
      }, 120_000);

      it("says live, with the reason, for a source it leaves as written", async () => {
         // A row filter the source inherits is refused on every tier (G3
         // admits a source's own gate only), so it is left as written.
         const restore = withSwitch("cutover");
         await freshLoadPool();
         const loader = await loadFixture({
            ...DATA_FILES,
            "raw.malloy": RAW_MODEL,
            "m.malloy": `##! experimental { givens persistence }
import "raw.malloy"

given: REGIONS :: string[]

#(access_filter) region in $REGIONS
source: rows_base is orders_raw extend {}

#@ persist storage="lake"
source: inherited is rows_base -> { select: id, region, amount }
`,
         });
         try {
            const pkg = (await loader.bind()).pkg;
            const paths = Object.fromEntries(
               (await pkg.previewServeVerdicts()).map((v) => [
                  v.sourceName,
                  [v.cutoverPath, v.cutoverReason],
               ]),
            );
            expect(paths.inherited?.[0]).toBe("live");
            expect(paths.inherited?.[1]).toBeDefined();
            expect(paths.inherited?.[1]).not.toBe("not_rerooted");
         } finally {
            restore();
            await loader.dispose();
         }
      }, 120_000);

      it("names the rollup leg for a v1 rollup", async () => {
         const paths = await preview(
            "v1 rollup on annotated measures",
            "storage",
            "cutover",
         );
         expect(Object.values(paths).map((p) => p[0])).toContain("rollup");
      }, 120_000);
   });
});

/**
 * The v1 cutover's dry run (6e) against what actually serves under the
 * cutover. For every fixture shape (storage and colocated v1 variants), a
 * source reported `live` must never be answered by the served copy. The
 * preview bindings for v1 storage= sources must equal the bindings the bind
 * derives from the built table. And the dry run names the rollup leg for
 * every v1 `#@ preaggregate` rollup, but never for a storage= rollup over an
 * access-filtered source without PUBLISHER_MATERIALIZE_GATED_SOURCES, which
 * is never built or read under the cutover.
 */
describe("the dry run against what serves", () => {
   describe("dry-run cutover path vs what serves: every shape", () => {
      it("matches for every shape", async () => {
         const saidLiveServed: string[] = [];
         const saidServedNeverServed: string[] = [];
         const rollupNeverRollup: string[] = [];
         for (const shape of SHAPES) {
            for (const variant of ["storage", "database"] as Variant[]) {
               if (shape.variants && !shape.variants.includes(variant))
                  continue;
               const restore = withSwitch("cutover");
               await freshLoadPool();
               let loader;
               try {
                  loader = await loadFixture({
                     ...DATA_FILES,
                     ...renderFiles(shape, variant),
                  });
               } catch {
                  restore();
                  continue;
               }
               try {
                  const fixture = await loader.bind({ includeRollups: true });
                  const verdicts = await fixture.pkg.previewServeVerdicts();
                  const byName = new Map<string, string[]>();
                  for (const v of verdicts) {
                     byName.set(v.sourceName, [
                        ...(byName.get(v.sourceName) ?? []),
                        `${v.cutoverPath}:${v.cutoverReason ?? ""}:${v.origin ?? ""}`,
                     ]);
                  }
                  const servedRoots = new Map<string, boolean>();
                  const rolledRoots = new Map<string, boolean>();
                  const entry = Object.keys(shape.files)[0];
                  for (const q of shape.queries) {
                     const root = /run:\s*([A-Za-z_][\w]*)/.exec(q.malloy)?.[1];
                     if (!root) continue;
                     const out = await fixture.run(
                        q.model ?? entry,
                        q.malloy,
                        q.givens as never,
                     );
                     const served =
                        !("error" in out) && out.answer.via === "served_copy";
                     const rolled =
                        !("error" in out) && out.answer.servedBy === "rollup";
                     servedRoots.set(
                        root,
                        (servedRoots.get(root) ?? false) || served,
                     );
                     rolledRoots.set(
                        root,
                        (rolledRoots.get(root) ?? false) || rolled,
                     );
                     const said = byName.get(root);
                     if (
                        said &&
                        served &&
                        said.every((s) => s.startsWith("live"))
                     ) {
                        saidLiveServed.push(
                           `${shape.name}/${variant}/${q.name}: ${root} said ${said.join(",")}`,
                        );
                     }
                  }
                  for (const [root, served] of servedRoots) {
                     const said = byName.get(root);
                     if (!said) continue;
                     if (
                        !served &&
                        said.some((s) => s.startsWith("served_copy"))
                     ) {
                        saidServedNeverServed.push(
                           `${shape.name}/${variant}: ${root} said ${said.join(",")}`,
                        );
                     }
                  }
                  for (const v of verdicts) {
                     if (v.cutoverPath === "rollup") {
                        const anyRolled = [...rolledRoots.values()].some(
                           Boolean,
                        );
                        if (!anyRolled) {
                           rollupNeverRollup.push(
                              `${shape.name}/${variant}: ${v.sourceName}`,
                           );
                        }
                     }
                  }
               } finally {
                  restore();
                  await loader.dispose();
               }
            }
         }
         expect(saidLiveServed).toEqual([]);
      }, 900_000);
   });

   describe("dry-run cutover path vs what serves: preview bindings", () => {
      for (const [shapeName, source, query] of [
         [
            "renames",
            "daily_r",
            "run: daily_r -> { group_by: order_day; aggregate: grand }",
         ],
         ["query-shaped: a nest", "nested", undefined],
         [
            "an extend block that drops and re-hides stored columns",
            "dropped",
            undefined,
         ],
      ] as const) {
         it(`${shapeName}: preview binding equals the bound one, and the verdict matches`, async () => {
            const restore = withSwitch("cutover");
            await freshLoadPool();
            const shape = SHAPES.find((s) => s.name === shapeName)!;
            const loader = await loadFixture({
               ...DATA_FILES,
               ...renderFiles(shape, "storage"),
            });
            try {
               const fixture = await loader.bind();
               const verdict = (await fixture.pkg.previewServeVerdicts()).find(
                  (v) => v.sourceName === source,
               );
               const q = query ?? shape.queries[0].malloy;
               const out = await fixture.run(Object.keys(shape.files)[0], q);
               const via = "error" in out ? out.error : out.answer.via;
               expect({ said: verdict?.cutoverPath, via }).toEqual(
                  via === "served_copy"
                     ? { said: "served_copy", via }
                     : { said: "live", via },
               );
            } finally {
               restore();
               await loader.dispose();
            }
         }, 120_000);
      }
   });

   const MODEL = `##! experimental { persistence givens }
import "raw.malloy"

given: REGIONS :: string[]

#(access_filter) region in $REGIONS
source: orders is orders_raw extend {
  #@ preaggregate grain="region" storage="lake"
  measure: total is amount.sum()
}
`;

   describe("dry-run cutover path vs what serves: rollups", () => {
      it("a gated storage= v1 rollup: the dry run's path matches what serves", async () => {
         const restore = withSwitch("cutover");
         await freshLoadPool();
         const loader = await loadFixture({
            ...DATA_FILES,
            "raw.malloy": RAW_MODEL,
            "m.malloy": MODEL,
         });
         try {
            const fixture = await loader.bind({ includeRollups: true });
            const verdicts = await fixture.pkg.previewServeVerdicts();
            const out = await fixture.run(
               "m.malloy",
               "run: orders -> { group_by: region; aggregate: total }",
               { REGIONS: ["north"] },
            );
            const by = "error" in out ? out.error : out.answer.servedBy;
            const said = verdicts
               .filter((v) => v.origin === "preaggregate")
               .map((v) => v.cutoverPath);
            if (by !== "rollup") expect(said).not.toContain("rollup");
         } finally {
            restore();
            await loader.dispose();
         }
      }, 120_000);
   });
});

/**
 * The v1 cutover's conformance runner as a library (6e, plan question 15):
 * a package and its queries in, one classified row per query out. Run here
 * over the fixture's shapes, as Credible's private CI runs it over real
 * package snapshots.
 */
describe("the runner as a library", () => {
   describe("runV1CutoverConformance", () => {
      it("classifies each query live and served, from a package directory", async () => {
         const shape = SHAPES.find((s) => s.name === "baseline")!;
         const dir = fs.mkdtempSync(
            path.join(os.tmpdir(), "v1-cutover-runner-"),
         );
         try {
            writePackageFiles(dir, {
               ...DATA_FILES,
               ...renderFiles(shape, "database"),
            });
            const rows = await runV1CutoverConformance({
               package: { packageDir: dir },
               queries: shape.queries.map((q) => ({
                  name: q.name,
                  model: q.model ?? "m.malloy",
                  malloy: q.malloy,
               })),
            });
            expect(
               rows.map((r) => [r.query, r.verdict, r.cutoverPath]),
            ).toEqual(
               shape.queries.map((q) => [
                  q.name,
                  "served_equal",
                  "served_copy",
               ]),
            );
         } finally {
            fs.rmSync(dir, { recursive: true, force: true });
         }
      }, 300_000);

      it("reports the path that answers, from the package's files", async () => {
         const shape = SHAPES.find(
            (s) => s.name === "v1 rollup on annotated measures",
         )!;
         const rows = await runV1CutoverConformance({
            package: { ...DATA_FILES, ...renderFiles(shape, "storage") },
            queries: shape.queries.map((q) => ({
               name: q.name,
               model: "m.malloy",
               malloy: q.malloy,
            })),
         });
         const byName = Object.fromEntries(rows.map((r) => [r.query, r]));
         // The rollup leg answers, with the measures' annotations.
         expect(byName["covered by the rollup"]).toMatchObject({
            cutover: "served",
            cutoverPath: "rollup",
         });
         expect(byName["not covered (status)"]).toMatchObject({
            verdict: "live",
         });
      }, 300_000);
   });
});

/**
 * 6e: the conformance runner's classification of a
 * query live refuses (a source gate denies the caller), and of two queries
 * that share a name.
 */
describe("the runner's refusals", () => {
   const MODEL = `##! experimental { persistence givens }
import "raw.malloy"
given: ROLE :: string
#(authorize) $ROLE = 'admin'
#@ persist name="authz"
source: authz is orders_raw -> { select: id, region, amount } extend {
  measure: total is amount.sum()
}
`;
   const Q = "run: authz -> { group_by: region; aggregate: total }";

   it("a refusal both live and after the cutover is not a bug; same-named queries keep their own answers", async () => {
      const rows = await runV1CutoverConformance({
         package: { ...DATA_FILES, "raw.malloy": RAW_MODEL, "m.malloy": MODEL },
         queries: [
            {
               name: "admin",
               model: "m.malloy",
               malloy: Q,
               givens: { ROLE: "admin" },
            },
            {
               name: "viewer",
               model: "m.malloy",
               malloy: Q,
               givens: { ROLE: "viewer" },
            },
            {
               name: "dup",
               model: "m.malloy",
               malloy: Q,
               givens: { ROLE: "admin" },
            },
            {
               name: "dup",
               model: "m.malloy",
               malloy: Q,
               givens: { ROLE: "viewer" },
            },
         ],
      });
      expect(rows[1].verdict).not.toBe("bug");
      // The two `dup` rows were answered for different callers.
      expect(rows[2]).not.toEqual(rows[3]);
   }, 300_000);
});

/**
 * The v1 cutover conformance runner as the standalone CLI it is documented
 * as (scripts/v1-cutover-conformance.ts, plan question 15): spawned with
 * `NODE_ENV` unset, as a CI job runs it, over a package with no `storage=`
 * source and one with. 0.14.0 exited 3 on every fixture here, because the
 * harness reached a test seam that throws outside `NODE_ENV=test`.
 */
describe("the runner as a CLI", () => {
   const SERVER_DIR = path.resolve(__dirname, "../../..");
   const SCRIPT = path.join(SERVER_DIR, "scripts/v1-cutover-conformance.ts");
   /** Each spawn is killed after this, and each test times out just after. */
   const SPAWN_TIMEOUT_MS = 120_000;
   const TEST_TIMEOUT_MS = SPAWN_TIMEOUT_MS + 10_000;

   /**
    * The child's environment: this process's, without `NODE_ENV`. Each child
    * also runs with `--no-env-file`, so a local `.env` cannot set it back and
    * pass these tests vacuously.
    */
   function childEnv(): Record<string, string> {
      const env: Record<string, string> = {};
      for (const [k, v] of Object.entries(process.env)) {
         if (v !== undefined && k !== "NODE_ENV") env[k] = v;
      }
      return env;
   }

   async function runScript(
      shapeName: string,
      variant: Variant,
   ): Promise<{ code: number; stdout: string; stderr: string }> {
      const shape = SHAPES.find((s) => s.name === shapeName)!;
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "v1-cutover-script-"));
      try {
         const pkgDir = path.join(root, "pkg");
         writePackageFiles(pkgDir, {
            ...DATA_FILES,
            ...renderFiles(shape, variant),
         });
         const queriesFile = path.join(root, "queries.json");
         fs.writeFileSync(
            queriesFile,
            JSON.stringify(
               shape.queries.map((q) => ({
                  name: q.name,
                  model: q.model ?? "m.malloy",
                  malloy: q.malloy,
               })),
            ),
         );
         const proc = Bun.spawn(
            [
               process.execPath,
               "--no-env-file",
               SCRIPT,
               "--package",
               pkgDir,
               "--queries",
               queriesFile,
               "--json",
            ],
            {
               cwd: SERVER_DIR,
               env: childEnv(),
               stdout: "pipe",
               stderr: "pipe",
               timeout: SPAWN_TIMEOUT_MS,
            },
         );
         const [stdout, stderr, code] = await Promise.all([
            new Response(proc.stdout).text(),
            new Response(proc.stderr).text(),
            proc.exited,
         ]);
         return { code, stdout, stderr };
      } finally {
         fs.rmSync(root, { recursive: true, force: true });
      }
   }

   describe("scripts/v1-cutover-conformance.ts without NODE_ENV", () => {
      it(
         "spawns its child with NODE_ENV unset",
         async () => {
            const proc = Bun.spawn(
               [
                  process.execPath,
                  "--no-env-file",
                  "-e",
                  'console.log(process.env.NODE_ENV ?? "<unset>")',
               ],
               {
                  cwd: SERVER_DIR,
                  env: childEnv(),
                  stdout: "pipe",
                  timeout: SPAWN_TIMEOUT_MS,
               },
            );
            expect((await new Response(proc.stdout).text()).trim()).toBe(
               "<unset>",
            );
            expect(await proc.exited).toBe(0);
         },
         TEST_TIMEOUT_MS,
      );

      it(
         "answers a package with no storage= source",
         async () => {
            const { code, stdout, stderr } = await runScript(
               "baseline",
               "database",
            );
            expect({ code, stderr: code === 0 ? "" : stderr }).toEqual({
               code: 0,
               stderr: "",
            });
            const rows = JSON.parse(stdout) as { verdict: string }[];
            expect(rows.length).toBeGreaterThan(0);
            expect(rows.every((r) => r.verdict === "served_equal")).toBe(true);
         },
         TEST_TIMEOUT_MS,
      );

      it(
         "answers a package with a storage= source, built from the sandbox",
         async () => {
            const { code, stdout, stderr } = await runScript(
               "v1 rollup on annotated measures",
               "storage",
            );
            expect({ code, stderr: code === 0 ? "" : stderr }).toEqual({
               code: 0,
               stderr: "",
            });
            const rows = JSON.parse(stdout) as {
               query: string;
               cutoverPath: string;
            }[];
            expect(rows.length).toBeGreaterThan(0);
            expect(
               rows.find((r) => r.query === "covered by the rollup")
                  ?.cutoverPath,
            ).toBe("rollup");
         },
         TEST_TIMEOUT_MS,
      );
   });
});
