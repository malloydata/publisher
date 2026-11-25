// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The import shapes a package load has to get right (#1566), as one package:
 * `tests/fixtures/load-shapes`. Each folder is one shape, listed in its README:
 * a diamond whose importers refine the same source differently, a six-level
 * chain, imports three folders deep with `../`, a file in a dot-folder that two
 * models import, a model at `my models/bäse.malloy` imported as written and
 * percent-encoded, data styles, selective and upper-case imports, and a
 * notebook importing across folders.
 *
 * Each shape ends in a `check_*` query whose answer the fixture's data fixes,
 * so an import resolved to the wrong file, one importer's refinement reaching
 * another, or a stalled load shows up here as a wrong number, a compile error
 * or a timeout. The same load pins how many imports came from the load's cache
 * (an import the load's scan misses still answers right, and only costs
 * reuse), which models report the dot-folder file's warning (each importer,
 * since it is not a package model) and the data styles merged in the order
 * Malloy reads imports. The notebook's two query cells are run as well.
 *
 * The package is also small and self-contained (DuckDB over inline values), so
 * it can be published as-is to a deployed server and its `check_*` queries run
 * there for the same answers.
 */
import { afterAll, beforeAll, describe, expect, it, spyOn } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { fileURLToPath } from "url";
import { unrecordedAudit } from "../service/access_record";
import { Package } from "../service/package";
import { memoryDuckdbConfig } from "../test_helpers/memory_duckdb";
import {
   PackageLoadPool,
   __setPackageLoadPoolForTests,
   type LoadPackageOutcome,
} from "./package_load_pool";

type LoadedModel = LoadPackageOutcome["models"][number];

const FIXTURE = path.join(
   path.dirname(fileURLToPath(import.meta.url)),
   "..",
   "..",
   "tests",
   "fixtures",
   "load-shapes",
);

/** Each check query and the one row it must return. */
const CHECKS: [model: string, query: string, row: Record<string, number>][] = [
   [
      "diamond/a.malloy",
      "check_ab",
      { row_count: 6, v_total: 210, a_scaled: 630, b_scaled: 420 },
   ],
   [
      "diamond/top.malloy",
      "check_diamond",
      { row_count: 6, left_total: 420, right_total: 1050 },
   ],
   ["chain/c5.malloy", "check_chain", { row_count: 6, s5_total: 300 }],
   [
      "nested/marts/deep/report.malloy",
      "check_nested_report",
      { order_count: 3, amount_total: 600, item_v: 30 },
   ],
   [
      "nested/index.malloy",
      "check_nested",
      { order_count: 3, amount_total: 600 },
   ],
   ["dotfile/first.malloy", "check_dotfile_first", { n: 1 }],
   ["dotfile/second.malloy", "check_dotfile_second", { n: 1 }],
   ["spelled/one.malloy", "check_spelled_one", { row_count: 6, v_total: 210 }],
   ["spelled/two.malloy", "check_spelled_two", { row_count: 6, v_total: 210 }],
   ["styles/root.malloy", "check_styles", { n: 1 }],
   ["selective/pick.malloy", "check_selective", { row_count: 6, v_total: 210 }],
   ["selective/upper.malloy", "check_upper", { row_count: 6, v_total: 210 }],
];

/** The notebook's query cells, by cell index, and the row each must return. */
const NOTEBOOK = "notebooks/shapes.malloynb";
const NOTEBOOK_CELLS: [query: string, row: Record<string, number>][] = [
   ["check_diamond", { row_count: 6, left_total: 420, right_total: 1050 }],
   ["check_nested", { order_count: 3, amount_total: 600 }],
];

/** A query result's rows, with DuckDB's bigints as numbers. */
function rowsOf(compactResult: unknown): unknown {
   return JSON.parse(
      JSON.stringify(compactResult ?? [], (_k, v) =>
         typeof v === "bigint" ? Number(v) : v,
      ),
   );
}

/** The package-relative file a problem is located in. */
function problemFile(problem: unknown, packagePath: string): string {
   const url = (problem as { at?: { url?: string } }).at?.url;
   if (!url) return "";
   return path.relative(packagePath, fileURLToPath(url)).replace(/\\/g, "/");
}

describe("the load-shapes fixture", () => {
   let rootDir: string;
   let packagePath: string;
   let outcome: LoadPackageOutcome;
   let models: Map<string, LoadedModel>;
   let pkg: Package;
   let closeDuckdb: () => Promise<void>;

   beforeAll(async () => {
      rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-shapes-"));
      packagePath = path.join(rootDir, "load-shapes");
      await fs.cp(FIXTURE, packagePath, { recursive: true });

      // One load, the one `Package.create` makes through the pool: the
      // assertions on the load's outcome and the queries run on the package
      // both read it. It is asked for its problems as well, which a served
      // load does not collect.
      const pool = new PackageLoadPool(2);
      await __setPackageLoadPoolForTests(pool);
      const loadPackage = pool.loadPackage.bind(pool);
      const loads: Promise<LoadPackageOutcome>[] = [];
      spyOn(pool, "loadPackage").mockImplementation((request) => {
         const load = loadPackage({ ...request, collectProblems: true });
         loads.push(load);
         return load;
      });

      const { malloyConfig, duckdb } = memoryDuckdbConfig();
      closeDuckdb = () => duckdb.close();
      pkg = await Package.create(
         "env",
         "load-shapes",
         packagePath,
         malloyConfig,
      );
      expect(loads).toHaveLength(1);
      outcome = await loads[0];
      models = new Map(outcome.models.map((m) => [m.modelPath, m]));
   }, 60_000);

   afterAll(async () => {
      // Shuts the pool down.
      await __setPackageLoadPoolForTests(null);
      await closeDuckdb?.().catch(() => {});
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
   });

   it("compiles every model and the notebook, and no file in the dot-folder", () => {
      expect([...models.keys()].sort()).toEqual([
         "chain/c0.malloy",
         "chain/c1.malloy",
         "chain/c2.malloy",
         "chain/c3.malloy",
         "chain/c4.malloy",
         "chain/c5.malloy",
         "diamond/a.malloy",
         "diamond/b.malloy",
         "diamond/base.malloy",
         "diamond/left.malloy",
         "diamond/right.malloy",
         "diamond/top.malloy",
         "dotfile/first.malloy",
         "dotfile/second.malloy",
         "nested/core/orders.malloy",
         "nested/index.malloy",
         "nested/mapping/joined.malloy",
         "nested/marts/deep/report.malloy",
         "notebooks/shapes.malloynb",
         "selective/pick.malloy",
         "selective/upper.malloy",
         "spelled/my models/bäse.malloy",
         "spelled/one.malloy",
         "spelled/two.malloy",
         "styles/b.malloy",
         "styles/c.malloy",
         "styles/d.malloy",
         "styles/e.malloy",
         "styles/f.malloy",
         "styles/root.malloy",
      ]);
      for (const m of models.values()) {
         expect({ model: m.modelPath, error: m.compilationError }).toEqual({
            model: m.modelPath,
            error: undefined,
         });
      }
   });

   it("takes every package import from the load's cache, however it is written", () => {
      // An import the load's scan of the files misses costs only reuse: the
      // importer does not wait for the file, translates it itself, and its
      // queries answer the same. So the reuse count is what fails when the
      // scan stops matching a way of writing an import (`import { items }
      // from`, `IMPORT`, `../`, a percent-encoded path).
      //
      // Every model waits for the package models it imports, so each of its
      // imports comes from the cache: 30 across the package. (The notebook
      // compiles outside the cache.)
      expect(outcome.timings.translationsReused).toBe(30);
      // Stored: the 18 package models another model imports under the path
      // the load lists them by, plus `spelled/my models/bäse.malloy`, whose
      // importers ask for it under another spelling than its own translation
      // has. Its two importers start together, so it is stored by the first,
      // or by both when the second asks before the first has finished.
      expect([19, 20]).toContain(outcome.timings.translationsStored ?? 0);
   });

   it("reports the dot-folder file's warning once from each model that imports it", () => {
      // `.lib/shared.malloy` is not a package model, so only its importers
      // translate it, and each must report its warning: second imports first
      // (so it compiles after first has translated the file) and must
      // translate the file itself rather than take first's translation, which
      // comes without its problems.
      const warnings = new Map<string, unknown[]>();
      for (const m of models.values()) {
         const codes = (m.problems ?? [])
            .filter((p) => problemFile(p, packagePath) === ".lib/shared.malloy")
            .map((p) => (p as { code?: string }).code);
         if (codes.length > 0) warnings.set(m.modelPath, codes);
      }
      expect(Object.fromEntries(warnings)).toEqual({
         "dotfile/first.malloy": ["null-typed-field-definition"],
         "dotfile/second.malloy": ["null-typed-field-definition"],
      });
   });

   it("merges each model's data styles in the order Malloy reads its imports", () => {
      // root imports b then c; b's subtree (d, then e) is read before c's (f),
      // so f, read last, wins for root. A level-by-level order would read e
      // last.
      expect(models.get("styles/root.malloy")?.dataStyles).toEqual({
         k: { renderer: "bar_chart" },
      });
      expect(models.get("styles/b.malloy")?.dataStyles).toEqual({
         k: { renderer: "line_chart" },
      });
      expect(models.get("styles/c.malloy")?.dataStyles).toEqual({
         k: { renderer: "bar_chart" },
      });
   });

   it.each(CHECKS)(
      "%s: %s returns its fixed answer",
      async (model, query, row) => {
         const result = await pkg
            .getModel(model)!
            .runQuery(unrecordedAudit(), { query: `run: ${query}` });
         expect(rowsOf(result.compactResult)).toEqual([row]);
      },
   );

   it.each(NOTEBOOK_CELLS)(
      "the notebook's cell running %s returns its fixed answer",
      async (query, row) => {
         const ran = await pkg
            .getModel(NOTEBOOK)!
            .runQuery(unrecordedAudit(), { query: `run: ${query}` });
         expect(rowsOf(ran.compactResult)).toEqual([row]);
      },
   );
});
