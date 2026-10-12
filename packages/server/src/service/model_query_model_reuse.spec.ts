// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Model as MalloyModel } from "@malloydata/malloy";
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

import { Environment } from "./environment";
import { Model } from "./model";
import type { Package } from "./package";

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

beforeAll(() => {
   if (!descriptor?.get) throw new Error("Malloy Model.queryModel moved");
   const get = descriptor.get;
   Object.defineProperty(MalloyModel.prototype, "queryModel", {
      configurable: true,
      get(this: WithQueryModel & { modelDef: unknown }) {
         if (!this._queryModel) {
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
      "builds it once for load-time validation and every query after",
      async () => {
         const pkg = await loadPackage();
         const model = pkg.getModel("m.malloy")!;
         const modelDef = model.getModelDef();
         expect(builtFor.get(modelDef)).toBe(1);
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
            // Per compile: one per render-tag prepare and per query.
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
