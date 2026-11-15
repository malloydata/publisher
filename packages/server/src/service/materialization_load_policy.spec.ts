// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The load-versus-refused policy, end to end over real packages, on every
 * load path:
 *
 *  - An INVALID materialization annotation (or invalid code it covers) fails
 *    the publish AND the load: a first load (what a publish and a
 *    control-plane worker load both run), a reload of a package that loaded
 *    before, and a restart that loads the package from config. The failure
 *    names the mistake with its code; the package never serves around it.
 *  - The SAME mistake gets the same outcome on every annotation: `#@ persist`
 *    on a source, `#(materialize)`, `#(preaggregate)`, v1 `#@ preaggregate`, a
 *    persisted view and a persisted named query (the table below).
 *  - A VALID annotation Publisher cannot build is refused alone: the package
 *    loads, the item is not built, and it answers live with live's rows.
 */

import {
   afterAll,
   afterEach,
   beforeEach,
   describe,
   expect,
   it,
} from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { BadRequestError } from "../errors";
import { installSettings } from "../test_helpers/settings_env";
import { startedStore } from "../test_helpers/started_store";
import {
   usePackageLoadPool,
   writePackageFiles,
} from "../test_helpers/temp_package";
import { unrecordedAudit } from "./access_record";
import { Environment } from "./environment";
import { EnvironmentStore } from "./environment_store";
import { parseEnvironmentName, parsePackageName } from "./identity";
import type { Package } from "./package";
import { tempBuildPlanPackages } from "../test_helpers/incremental_test_harness";
import { computePackageBuildPlan } from "./materialization/plan/build_plan_compile";
import { loadGateText } from "./materialization/plan/load_gate_text";

const ROWS = `duckdb.sql("""SELECT * FROM (VALUES (1, 'A', 10), (2, 'B', 20), (3, 'A', 5)) AS t(id, category, amount)""")`;
const HEADER = `##! experimental { persistence composite_sources }\n`;

/** One annotation route, and the model that carries `line` on it. */
interface Route {
   name: string;
   model(line: string): string;
}

const ROUTES: Record<string, Route> = {
   persist: {
      name: "#@ persist",
      model: (line) =>
         `${HEADER}source: raw is ${ROWS}\n${line}\nsource: daily is raw -> { group_by: category; aggregate: total is amount.sum() }\n`,
   },
   materialize: {
      name: "#(materialize)",
      model: (line) =>
         `${HEADER}source: raw is ${ROWS}\n${line}\nsource: orders is raw extend { dimension: big is amount > 6 }\n`,
   },
   preaggregate: {
      name: "#(preaggregate)",
      model: (line) =>
         `${HEADER}source: orders is ${ROWS} extend {\n  ${line}\n  measure: total is amount.sum()\n}\n`,
   },
   v1Preaggregate: {
      name: "#@ preaggregate",
      model: (line) =>
         `${HEADER}source: orders is ${ROWS} extend {\n  ${line}\n  measure: total is amount.sum()\n}\n`,
   },
   view: {
      name: "a persisted view",
      model: (line) =>
         `${HEADER}source: orders is ${ROWS} extend {\n  measure: total is amount.sum()\n  ${line}\n  view: by_category is { group_by: category; aggregate: total }\n}\n`,
   },
   query: {
      name: "a persisted named query",
      model: (line) =>
         `${HEADER}source: orders is ${ROWS}\n${line}\nquery: by_category is orders -> { group_by: category; aggregate: total is amount.sum() }\n`,
   },
};

/** One mistake, as each route writes it, and the code each fails with. */
interface Mistake {
   name: string;
   lines: Record<keyof typeof ROUTES, [line: string, code: string]>;
}

const MISTAKES: Mistake[] = [
   {
      name: "a storage= that names nothing",
      lines: {
         persist: ['#@ persist name="t" storage=""', "persist-invalid"],
         materialize: ['#(materialize) storage=""', "materialize-invalid"],
         preaggregate: [
            '#(preaggregate) grain=["category"] storage=""',
            "preaggregate-invalid",
         ],
         v1Preaggregate: [
            '#@ preaggregate grain="category" storage=""',
            "preaggregate-invalid",
         ],
         view: ['#@ persist storage=""', "persist-view-invalid"],
         query: ['#@ persist storage=""', "persist-query-invalid"],
      },
   },
   {
      name: "an unknown key",
      lines: {
         persist: ['#@ persist nmae="t"', "persist-unknown-key"],
         materialize: ['#(materialize) nmae="t"', "materialize-unknown-key"],
         preaggregate: [
            '#(preaggregate) grain=["category"] stroage="lake"',
            "preaggregate-unknown-key",
         ],
         v1Preaggregate: [
            '#@ preaggregate grain="category" stroage="lake"',
            "preaggregate-unknown-key",
         ],
         view: ['#@ persist nmae="t"', "persist-unknown-key"],
         query: ['#@ persist nmae="t"', "persist-unknown-key"],
      },
   },
   {
      name: "an unknown key written inside the directive or a group",
      lines: {
         persist: ['#@ persist { nmae="t" }', "persist-unknown-key"],
         materialize: [
            '#(materialize) freshness.windw="1h"',
            "materialize-invalid",
         ],
         preaggregate: [
            '#(preaggregate) grain=["category"] freshness.window="1h"',
            "preaggregate-unknown-key",
         ],
         v1Preaggregate: [
            '#@ preaggregate { grain="category" stroage="lake" }',
            "preaggregate-unknown-key",
         ],
         view: ['#@ persist { nmae="t" }', "persist-unknown-key"],
         query: ['#@ persist { nmae="t" }', "persist-unknown-key"],
      },
   },
   {
      name: "a line that does not parse",
      lines: {
         persist: ['#@ persist name="t', "persist-unparseable"],
         materialize: ['#(materialize) name="t', "materialize-unparseable"],
         preaggregate: [
            '#(preaggregate) grain=["category"',
            "preaggregate-unparseable",
         ],
         v1Preaggregate: [
            '#@ preaggregate grain="category',
            "preaggregate-unparseable",
         ],
         view: ['#@ persist name="t', "persist-view-invalid"],
         query: ['#@ persist name="t', "persist-query-invalid"],
      },
   },
   {
      name: "an @env. reference",
      lines: {
         // An unquoted name=@env. is the same mistake, not a quoting one.
         persist: ["#@ persist name=@env.TABLE", "persist-invalid"],
         materialize: [
            "#(materialize) storage=@env.LAKE",
            "materialize-invalid",
         ],
         preaggregate: [
            '#(preaggregate) grain=["category"] storage=@env.LAKE',
            "preaggregate-invalid",
         ],
         v1Preaggregate: [
            '#@ preaggregate grain="category" storage=@env.LAKE',
            "preaggregate-invalid",
         ],
         view: ["#@ persist name=@env.TABLE", "persist-view-invalid"],
         query: ["#@ persist name=@env.TABLE", "persist-query-invalid"],
      },
   },
   {
      name: "a name= Publisher reserves for its own tables",
      lines: {
         persist: ['#@ persist name="t__p0123abcd"', "persist-invalid"],
         materialize: [
            '#(materialize) name="t__p0123abcd"',
            "materialize-invalid",
         ],
         // A rollup takes no name=: it is an unknown key there.
         preaggregate: [
            '#(preaggregate) grain=["category"] name="t__p0123abcd"',
            "preaggregate-unknown-key",
         ],
         v1Preaggregate: [
            '#@ preaggregate grain="category" name="t__p0123abcd"',
            "preaggregate-unknown-key",
         ],
         view: ['#@ persist name="t__p0123abcd"', "persist-view-invalid"],
         query: ['#@ persist name="t__p0123abcd"', "persist-query-invalid"],
      },
   },
];

/** Every (mistake, route) pair, with a package name of its own. */
const CASES = MISTAKES.flatMap((mistake, m) =>
   Object.entries(mistake.lines).map(([route, [line, code]], r) => ({
      label: `${mistake.name} on ${ROUTES[route].name}`,
      route,
      pkg: `case${m}x${r}`,
      model: ROUTES[route].model(line),
      code,
   })),
);

/** A package each route loads cleanly with: the same shape, no annotation. */
const GOOD = (route: string) => ROUTES[route].model("");

let root: string;

beforeEach(() => {
   root = fs.realpathSync(
      fs.mkdtempSync(path.join(os.tmpdir(), "load-vs-refused-")),
   );
});

afterEach(() => {
   fs.rmSync(root, { recursive: true, force: true });
});

function write(
   dir: string,
   name: string,
   model: string,
   manifest: Record<string, unknown> = {},
): void {
   writePackageFiles(
      dir,
      { "model.malloy": model },
      { manifest: { name, description: "fixture", ...manifest } },
   );
}

/** The error a load raised, or a test failure when it loaded. */
async function failure(run: () => Promise<unknown>): Promise<Error> {
   try {
      await run();
   } catch (error) {
      return error as Error;
   }
   throw new Error("expected the load to fail, but the package loaded");
}

describe("the load-versus-refused policy", () => {
   // The load worker reads its settings when it starts, so they are
   // installed before the pool: materialization v2 on, as by default.
   const settings = installSettings({ env: { PUBLISHER_MATERIALIZE: "true" } });
   usePackageLoadPool();
   afterAll(() => settings.restore());

   describe("the same mistake fails the load the same way on every annotation", () => {
      it(
         "on a first load: what a publish and a control-plane worker load run",
         async () => {
            const envPath = path.join(root, "env");
            for (const c of CASES)
               write(path.join(envPath, c.pkg), c.pkg, c.model);
            const env = await Environment.create("testEnv", envPath, []);
            const outcomes = [];
            for (const c of CASES) {
               const error = await failure(() => env.addPackage(c.pkg));
               outcomes.push([
                  c.label,
                  error instanceof BadRequestError,
                  error.message.includes(`[${c.code}]`),
               ]);
            }
            expect(outcomes).toEqual(CASES.map((c) => [c.label, true, true]));
         },
         { timeout: 180000 },
      );

      it(
         "on a reload of a package that loaded before: it keeps serving the old model",
         async () => {
            const envPath = path.join(root, "env");
            for (const c of CASES) {
               write(path.join(envPath, c.pkg), c.pkg, GOOD(c.route));
            }
            const env = await Environment.create("testEnv", envPath, []);
            for (const c of CASES) await env.addPackage(c.pkg);
            const outcomes = [];
            for (const c of CASES) {
               fs.writeFileSync(
                  path.join(envPath, c.pkg, "model.malloy"),
                  c.model,
               );
               const error = await failure(() =>
                  env.getPackage(parsePackageName(c.pkg), true),
               );
               outcomes.push([c.label, error.message.includes(`[${c.code}]`)]);
            }
            expect(outcomes).toEqual(CASES.map((c) => [c.label, true]));
            // Each still serves the model it compiled before the bad save, and
            // says so (a stale load error).
            const stale = env.getStaleCompileErrors();
            expect(CASES.filter((c) => !stale.has(c.pkg))).toEqual([]);
         },
         { timeout: 180000 },
      );

      it(
         "on a restart that loads the packages from config: a loadErrors entry each",
         async () => {
            const server = path.join(root, "server");
            fs.mkdirSync(server, { recursive: true });
            for (const c of CASES) {
               // A version, as a publish into a versioned environment needs.
               write(path.join(root, "sources", c.pkg), c.pkg, c.model, {
                  version: "1.0.0",
               });
            }
            fs.writeFileSync(
               path.join(server, "publisher.config.json"),
               JSON.stringify({
                  frozenConfig: false,
                  environments: [
                     {
                        name: "org",
                        packages: CASES.map((c) => ({
                           name: c.pkg,
                           location: path.join(root, "sources", c.pkg),
                        })),
                        connections: [],
                     },
                  ],
               }),
            );
            const restore = installSettings({
               env: { PUBLISHER_MATERIALIZE: "true" },
               cwd: server,
            }).restore;
            const store = startedStore(new EnvironmentStore(server));
            try {
               await store.finishedInitialization;
               // Listing the environment loads its packages.
               await store
                  .getEnvironment(parseEnvironmentName("org"), false)
                  .catch(() => undefined);
               const status = await store.getStatus();
               const outcomes = CASES.map((c) => {
                  const entry = status.loadErrors?.find(
                     (e) => e.package === c.pkg,
                  );
                  return [c.label, !!entry?.message?.includes(`[${c.code}]`)];
               });
               expect(outcomes).toEqual(CASES.map((c) => [c.label, true]));
            } finally {
               for (const environment of store.getLoadedEnvironments()) {
                  await environment.closeAllConnections();
               }
               await (
                  store as unknown as {
                     storageManager: { close(): Promise<void> };
                  }
               ).storageManager.close();
               restore();
            }
         },
         { timeout: 180000 },
      );
   });

   describe("a run's gate text over the files equals the load's failure", () => {
      // The run reads the load gate's text off the plan it compiles from the
      // files (plan/load_gate_text.ts); it must say exactly what the load
      // fails with, row for row, near misses and #@ preaggregate included.
      const NEAR_MISSES: [string, string][] = [
         ["#@ persits", ROUTES.persist.model('#@ persits name="t"')],
         [
            "#(materialise)",
            ROUTES.materialize.model('#(materialise) name="t"'),
         ],
      ];
      const rows: [string, string][] = [
         ...CASES.map((c) => [c.label, c.model] as [string, string]),
         ...NEAR_MISSES,
      ];
      for (const [label, model] of rows) {
         it(
            label,
            async () => {
               const envPath = path.join(root, "env");
               write(path.join(envPath, "pkg"), "pkg", model);
               const env = await Environment.create("testEnv", envPath, []);
               const loadFailure = (await failure(() => env.addPackage("pkg")))
                  .message;
               const packages = tempBuildPlanPackages();
               try {
                  const plan = await computePackageBuildPlan(
                     await packages.create({ "model.malloy": model }),
                  );
                  expect(
                     loadGateText(
                        {
                           plan: plan.plan,
                           annotationFindings: plan.annotationFindings,
                           incrementalDeclarations:
                              plan.incrementalDeclarations,
                           ...(plan.materializePlans
                              ? { materializePlans: plan.materializePlans }
                              : {}),
                           ...(plan.modelGateLines
                              ? { modelGateLines: plan.modelGateLines }
                              : {}),
                        },
                        { enforceCollisions: true, versionScoped: false },
                     ),
                  ).toBe(loadFailure);
               } finally {
                  await packages.cleanup();
               }
            },
            60000,
         );
      }
   });

   describe("a valid annotation Publisher cannot build is refused alone", () => {
      async function load(model: string): Promise<Package> {
         const envPath = path.join(root, "env");
         write(path.join(envPath, "pkg"), "pkg", model);
         const env = await Environment.create("testEnv", envPath, []);
         await env.addPackage("pkg");
         return env.getPackage(parsePackageName("pkg"), false);
      }

      async function rows(pkg: Package, query: string): Promise<unknown[]> {
         const result = await pkg
            .getModel("model.malloy")!
            .runQuery(unrecordedAudit(), { query });
         return (result as unknown as { compactResult: unknown[] })
            .compactResult;
      }

      it(
         "a v1 #@ preaggregate over a fan-out join: the rollup is not built, the package loads, and the measure answers live",
         async () => {
            const pkg = await load(`${HEADER}source: lines is ${ROWS}
source: orders is ${ROWS} extend {
  join_many: lines on lines.id = id
  #@ preaggregate grain="category"
  measure: total is amount.sum()
}
`);
            const warnings = (pkg.getPackageMetadata().warnings ?? []).filter(
               (w) => w.code === "preaggregate-join-level",
            );
            expect(warnings.map((w) => [w.subject, w.severity])).toEqual([
               ["orders", "warn"],
            ]);
            expect(
               Object.values(pkg.getBuildPlan()?.sources ?? {}).filter(
                  (s) => s.preaggregate,
               ),
            ).toEqual([]);
            expect(
               await rows(
                  pkg,
                  "run: orders -> { group_by: category; aggregate: total; order_by: category }",
               ),
            ).toEqual([
               { category: "A", total: 15 },
               { category: "B", total: 20 },
            ]);
         },
         { timeout: 60000 },
      );

      it(
         "an incremental #(materialize) on a dialect that cannot refresh incrementally: refused alone, its sibling planned",
         async () => {
            const pkg = await load(`${HEADER}source: raw is ${ROWS}
#(materialize) refresh="incremental" watermark="id"
source: inc is raw extend { dimension: big is amount > 6 }

#(materialize)
source: whole is raw extend { dimension: small is amount < 6 }
`);
            const notBuilt = (pkg.getPackageMetadata().warnings ?? []).filter(
               (w) => w.code === "materialize-not-built",
            );
            expect(notBuilt.map((w) => [w.subject, w.severity])).toEqual([
               ["inc", "warn"],
            ]);
            expect(notBuilt[0].message).toContain('dialect "duckdb"');
            // The sibling is planned, so a run builds it.
            expect(
               Object.values(pkg.getBuildPlan()?.sources ?? {}).some((s) =>
                  s.name.includes("inc"),
               ),
            ).toBe(false);
            expect(
               Object.values(pkg.getBuildPlan()?.sources ?? {}).some((s) =>
                  s.name.includes("whole"),
               ),
            ).toBe(true);
            expect(
               await rows(
                  pkg,
                  "run: inc -> { group_by: category; aggregate: n is count(); order_by: category }",
               ),
            ).toEqual(
               await rows(
                  pkg,
                  "run: raw -> { group_by: category; aggregate: n is count(); order_by: category }",
               ),
            );
         },
         { timeout: 60000 },
      );
   });
});
