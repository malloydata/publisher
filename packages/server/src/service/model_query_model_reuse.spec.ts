// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   type Connection,
   FixedConnectionMap,
   InMemoryURLReader,
   MalloyConfig,
   Model as MalloyModel,
   modelDefToModelInfo,
   Runtime,
} from "@malloydata/malloy";
import {
   afterAll,
   afterEach,
   beforeAll,
   beforeEach,
   describe,
   expect,
   it,
   spyOn,
} from "bun:test";
import * as fs from "fs/promises";
import * as os from "os";
import * as path from "path";
import { pathToFileURL } from "url";

import { AccessDeniedError } from "../errors";
import { logger } from "../logger";
import { Environment } from "./environment";
import { Model } from "./model";
import type { Package } from "./package";
import { installSettings } from "../test_helpers/settings_env";

/** Puts back the settings the current test installed. */
let restoreSettings: (() => void) | undefined;

/**
 * Install PERSIST_STORAGE_MODE=`mode` for this test, before its model is built;
 * every other setting is at its default.
 */
function setStorageMode(mode: string): void {
   restoreSettings?.();
   restoreSettings = installSettings({
      env: { PERSIST_STORAGE_MODE: mode },
   }).restore;
}

afterEach(() => {
   restoreSettings?.();
   restoreSettings = undefined;
});

/**
 * A hydrated Model builds its Malloy `QueryModel` once and every render-tag
 * prepare and query compiled against it reuses it (see
 * `Model.ensureSharedQueryModel`). These pin that it is built once per Model,
 * and that the findings and answers are the ones building it per compile gave.
 */

// Counts the QueryModels Malloy builds, by watching the one place it builds
// them: `Model.queryModel`'s getter, on a model that does not have one yet.
type WithQueryModel = { _queryModel?: unknown };
const descriptor = Object.getOwnPropertyDescriptor(
   MalloyModel.prototype,
   "queryModel",
);
const builtFor: Map<unknown, number> = new Map();
let builds = 0;
// Set to make the next build throw, as a QueryModel that cannot be built would.
let failNextBuild = false;

beforeAll(() => {
   if (!descriptor?.get) throw new Error("Malloy Model.queryModel moved");
   const get = descriptor.get;
   Object.defineProperty(MalloyModel.prototype, "queryModel", {
      configurable: true,
      get(this: WithQueryModel & { modelDef: unknown }) {
         if (!this._queryModel) {
            if (failNextBuild) {
               failNextBuild = false;
               throw new Error("cannot build this QueryModel");
            }
            builds++;
            builtFor.set(this.modelDef, (builtFor.get(this.modelDef) ?? 0) + 1);
         }
         return get.call(this);
      },
   });
});

afterAll(() => {
   if (descriptor) {
      Object.defineProperty(MalloyModel.prototype, "queryModel", descriptor);
   }
});

const MODEL = `source: nums is duckdb.sql("SELECT 1 as one, 2 as two") extend {
  measure:
    total_one is one.sum()
    total_two is two.sum()

  # big_value
  view: kpis is {
    aggregate: total_one, total_two
  }

  view: trend is {
    group_by: one
    aggregate: total_two
  }

  # dashboard
  view: inert_colspan is {
    nest:
      # colspan=2
      kpis
    nest: trend
  }

  # dashboard { columns=2 }
  view: invalid_colspan is {
    nest:
      # colspan=abc
      kpis
    nest: trend
  }

  # bar_chart
  view: chart is { group_by: one; aggregate: total_two }
}

query: named_total is nums -> { aggregate: total_one }
`;

describe("one QueryModel per hydrated model", () => {
   let rootDir: string;

   async function loadPackage(): Promise<Package> {
      const envPath = path.join(rootDir, `env-${Math.random()}`);
      const dir = path.join(envPath, "pkg");
      await fs.mkdir(dir, { recursive: true });
      await fs.writeFile(
         path.join(dir, "publisher.json"),
         JSON.stringify({ name: "pkg", description: "fixture" }),
      );
      await fs.writeFile(path.join(dir, "m.malloy"), MODEL);
      const env = await Environment.create("testEnv", envPath, []);
      await env.addPackage("pkg");
      return env.getPackage("pkg", false);
   }

   async function answers(model: Model) {
      const out: unknown[] = [];
      for (const [source, view, query] of [
         ["nums", "kpis", undefined],
         ["nums", "trend", undefined],
         ["nums", "chart", undefined],
         [undefined, "named_total", undefined],
         [
            undefined,
            undefined,
            "run: nums -> { group_by: two; aggregate: total_one }",
         ],
      ] as const) {
         const r = await model.getQueryResults(source, view, query);
         out.push({ sql: r.result.sql, data: r.compactResult });
      }
      return out;
   }

   beforeEach(async () => {
      rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-qm-"));
      builds = 0;
      builtFor.clear();
   });

   afterEach(async () => {
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
   });

   it(
      "builds it once for every query, and none at load when the worker prepared the render-tag results",
      async () => {
         const pkg = await loadPackage();
         const model = pkg.getModel("m.malloy")!;
         const modelDef = model.getModelDef();
         // The load worker prepares the render-tag targets (see
         // SerializedModel.renderTagResults), so the load builds none here.
         expect(builtFor.get(modelDef) ?? 0).toBe(0);
         await answers(model);
         await answers(model);
         expect(builtFor.get(modelDef)).toBe(1);
      },
      { timeout: 60000 },
   );

   it(
      "gives the same findings and answers as a QueryModel per compile",
      async () => {
         const shared = await loadPackage();
         const sharedWarnings = shared.getPackageMetadata().warnings;
         const sharedAnswers = await answers(shared.getModel("m.malloy")!);
         const sharedBuilds = builds;

         // The old behaviour: never build the shared one.
         const spy = spyOn(
            Model.prototype as unknown as {
               ensureSharedQueryModel: () => Promise<void>;
            },
            "ensureSharedQueryModel",
         ).mockImplementation(() => Promise.resolve());
         try {
            builds = 0;
            const perCompile = await loadPackage();
            expect(perCompile.getPackageMetadata().warnings).toEqual(
               sharedWarnings,
            );
            expect(await answers(perCompile.getModel("m.malloy")!)).toEqual(
               sharedAnswers,
            );
            // Per compile: one per query.
            expect(builds).toBeGreaterThan(sharedBuilds);
         } finally {
            spy.mockRestore();
         }
         // The findings this fixture exists to produce are there.
         const messages = (sharedWarnings ?? []).map((w) => w.message);
         expect(messages.some((m) => /colspan/i.test(m ?? ""))).toBe(true);
      },
      { timeout: 60000 },
   );
});

describe("one QueryModel per materializer a query compiles against", () => {
   const dirs: string[] = [];

   beforeEach(() => {
      builds = 0;
      builtFor.clear();
      failNextBuild = false;
   });

   afterAll(async () => {
      for (const d of dirs) {
         await fs.rm(d, { recursive: true, force: true }).catch(() => {});
      }
   });

   async function createModel(
      text: string,
   ): Promise<{ model: Model; duckdb: DuckDBConnection }> {
      const dir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-qm-"));
      dirs.push(dir);
      await fs.writeFile(path.join(dir, "m.malloy"), text);
      const duckdb = new DuckDBConnection("duckdb", ":memory:");
      const model = await Model.create(
         "pkg",
         dir,
         "m.malloy",
         new Map<string, Connection>([["duckdb", duckdb]]),
      );
      return { model, duckdb };
   }

   async function run(
      model: Model,
      query: string,
      givens?: Record<string, unknown>,
   ): Promise<unknown> {
      const r = await model.getQueryResults(
         undefined,
         undefined,
         query,
         {},
         true,
         givens as never,
      );
      return r.compactResult;
   }

   it(
      "builds nothing for a query the authorize gate rejects",
      async () => {
         const { model, duckdb } = await createModel(`##! experimental.givens

given:
  ROLE :: string

#(authorize) 'admin' = $ROLE
source: secrets is duckdb.sql("SELECT 1 AS one") extend {}
`);
         try {
            // Only what the rejected query builds, not the load.
            builtFor.clear();
            const modelDef = model.getModelDef();
            await expect(
               run(model, "run: secrets -> { select: one }", {
                  ROLE: "viewer",
               }),
            ).rejects.toBeInstanceOf(AccessDeniedError);
            expect(builtFor.get(modelDef) ?? 0).toBe(0);
            // The same query from an admin compiles, and builds it.
            expect(
               await run(model, "run: secrets -> { select: one }", {
                  ROLE: "admin",
               }),
            ).toEqual([{ one: 1 }]);
            expect(builtFor.get(modelDef)).toBe(1);
         } finally {
            await duckdb.close();
         }
      },
      { timeout: 60000 },
   );

   it(
      "builds a row-level gated source's grafted QueryModel once, not per query",
      async () => {
         const { model, duckdb } = await createModel(`##! experimental.givens

given:
  GROUPS :: string[]

#(access_filter) org_id in $GROUPS
source: X is duckdb.sql("SELECT 'org1' AS org_id, 1 AS id UNION ALL SELECT 'org2' AS org_id, 2 AS id") extend {}
`);
         try {
            const q = "run: X -> { select: id; order_by: id }";
            expect(await run(model, q, { GROUPS: ["org1"] })).toEqual([
               { id: 1 },
            ]);
            const afterFirst = builds;
            expect(await run(model, q, { GROUPS: ["org2"] })).toEqual([
               { id: 2 },
            ]);
            expect(await run(model, q, { GROUPS: ["org1", "org2"] })).toEqual([
               { id: 1 },
               { id: 2 },
            ]);
            expect(builds).toBe(afterFirst);
         } finally {
            await duckdb.close();
         }
      },
      { timeout: 60000 },
   );

   it(
      "builds the storage serve shape's QueryModel once, not per query",
      async () => {
         setStorageMode("on");
         const text = `source: qm_orders is duckdb.sql("SELECT 10 AS amount") extend {
  measure: total is amount.sum()
}
`;
         const dir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-qm-"));
         dirs.push(dir);
         const file = path.join(dir, "m.malloy");
         await fs.writeFile(file, text);
         const fileUrl = pathToFileURL(file).toString();
         const duckdb = new DuckDBConnection("duckdb", ":memory:");
         try {
            // The stored table answers 99 where the live source answers 10.
            await duckdb.runSQL(
               "CREATE OR REPLACE TABLE qm_orders_mz AS SELECT 99 AS amount",
            );
            const connMap = new Map([["duckdb", duckdb]]);
            const runtime = new Runtime({
               urlReader: new InMemoryURLReader(new Map([[fileUrl, text]])),
               connections: new FixedConnectionMap(connMap, "duckdb"),
            });
            const mm = runtime.loadModel(new URL(fileUrl), {
               importBaseURL: new URL(".", fileUrl),
            });
            const modelDef = (
               (await mm.getModel()) as unknown as { _modelDef: never }
            )._modelDef;
            const model = new Model(
               "pkg",
               "m.malloy",
               {},
               "model",
               mm as never,
               modelDef,
               undefined,
               undefined,
               undefined,
               undefined,
               undefined,
               undefined,
               undefined,
               modelDefToModelInfo(modelDef),
            );
            const serveConfig = new MalloyConfig({ connections: {} });
            serveConfig.wrapConnections(
               () => new FixedConnectionMap(connMap, "duckdb"),
            );
            model.setServeDestinationConfig(() => serveConfig);
            model.setServeBindings([
               {
                  sourceName: "qm_orders",
                  destinationName: "duckdb",
                  virtualHandle: "qm_orders_h",
                  tablePath: "qm_orders_mz",
                  schema: [{ name: "amount", type: "BIGINT" }],
               },
            ]);

            const q = "run: qm_orders -> { aggregate: total }";
            // 99: served from the stored table, so the serve shape compiled it.
            expect(await run(model, q)).toEqual([{ total: 99 }]);
            const afterFirst = builds;
            expect(await run(model, q)).toEqual([{ total: 99 }]);
            expect(await run(model, q)).toEqual([{ total: 99 }]);
            expect(builds).toBe(afterFirst);
         } finally {
            await duckdb.close();
         }
      },
      { timeout: 60000 },
   );

   it(
      "logs a QueryModel it cannot build, once, and still answers",
      async () => {
         const { model, duckdb } = await createModel(
            'source: nums is duckdb.sql("SELECT 1 AS one") extend {}\n',
         );
         const warn = spyOn(logger, "warn");
         try {
            failNextBuild = true;
            const q = "run: nums -> { aggregate: c is count() }";
            expect(await run(model, q)).toEqual([{ c: 1 }]);
            expect(await run(model, q)).toEqual([{ c: 1 }]);
            const logged = warn.mock.calls.filter(
               ([message]) =>
                  String(message) ===
                  "Could not build a shared QueryModel; each query builds its own",
            );
            expect(logged).toHaveLength(1);
         } finally {
            warn.mockRestore();
            await duckdb.close();
         }
      },
      { timeout: 60000 },
   );
});
