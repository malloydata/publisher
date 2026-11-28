// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A package that declares skill sets at their ceilings (32 sets, each with
 * SKILL_SET_INLINE_MAX_FILES inline files and 200 listed files) is walked and
 * hashed in the package-load worker, so loading or reloading it never holds the
 * event loop: the main thread receives the result and adopts it. The
 * inline-file cap is the one the walk's cost grows with, and is set to the most
 * this bound lets a set carry. Read on the main thread the same walk would hold
 * the loop for seconds (`perf:skills-reload` prints it).
 *
 * The cap package is the one `perf:skills-reload` measures
 * (tests/perf/skills_cap_package.ts), loaded through the real pool.
 */

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { FixedConnectionMap, MalloyConfig } from "@malloydata/malloy";
import {
   afterAll,
   afterEach,
   beforeAll,
   beforeEach,
   describe,
   expect,
   it,
} from "bun:test";
import fs from "fs";
import os from "os";
import path from "path";
import {
   PackageLoadPool,
   __setPackageLoadPoolForTests,
} from "../package_load/package_load_pool";
import {
   CAP_SETS,
   CAP_SHAPE,
   writeCapPackage,
} from "../../tests/perf/skills_cap_package";
import { Package } from "./package";
import {
   SKILL_SET_ASSETS_MAX_FILES,
   SKILL_SET_INLINE_MAX_FILES,
   SKILL_SET_MARKDOWN_MAX_BYTES,
} from "./skills/package_workflows";

/** The longest any one task may hold the event loop while a cap-size package loads or reloads, ms. */
const MAX_BLOCK_MS = 250;

describe("loading and reloading a cap-size package", () => {
   let dir: string;
   let duckdb: DuckDBConnection;
   let malloyConfig: MalloyConfig;

   beforeAll(async () => {
      await __setPackageLoadPoolForTests(new PackageLoadPool(1));
   });

   afterAll(async () => {
      await __setPackageLoadPoolForTests(null);
   });

   beforeEach(() => {
      dir = fs.mkdtempSync(path.join(os.tmpdir(), "publisher-cap-block-"));
      fs.writeFileSync(
         path.join(dir, "model.malloy"),
         `source: t is duckdb.sql("select 1 as id")\n`,
      );
      const declared = writeCapPackage(dir, CAP_SETS, CAP_SHAPE);
      fs.writeFileSync(
         path.join(dir, "publisher.json"),
         JSON.stringify({ name: "pkg", version: "1", ...declared }),
      );
      duckdb = new DuckDBConnection("duckdb", ":memory:");
      malloyConfig = new MalloyConfig({ connections: {} });
      malloyConfig.wrapConnections(
         () => new FixedConnectionMap(new Map([["duckdb", duckdb]]), "duckdb"),
      );
   });

   afterEach(async () => {
      await duckdb.close();
      fs.rmSync(dir, { recursive: true, force: true });
   });

   it("never holds the event loop for long, receipt of the worker's result included", async () => {
      // The first load starts the worker and warms the compiler and the page cache.
      await Package.create("env", "pkg", dir, malloyConfig);

      // The event loop's longest gap: a 10 ms timer's lateness, as the
      // main-thread regression probe measures it.
      let maxBlockMs = 0;
      let last = performance.now();
      const timer = setInterval(() => {
         const now = performance.now();
         maxBlockMs = Math.max(maxBlockMs, now - last - 10);
         last = now;
      }, 10);
      let pkg!: Package;
      try {
         await Bun.sleep(20);
         last = performance.now();
         pkg = await Package.create("env", "pkg", dir, malloyConfig);
         await pkg.reloadAllModels({});
         // A last tick, so a block at the very end is measured too.
         await Bun.sleep(20);
      } finally {
         clearInterval(timer);
      }

      // The walk ran, and served every set whole: a set past a cap would be refused, which is less work.
      expect(pkg.listWorkflows().map((w) => w.name)).toEqual([
         "w0",
         "w1",
         "w2",
         "w3",
      ]);
      expect(
         pkg.listWorkflows().flatMap((w) => w.skill_set_names),
      ).toHaveLength(CAP_SETS);
      expect(pkg.defaultSkills().skills.length).toBeGreaterThan(0);
      expect(pkg.getPackageMetadata().warnings ?? []).toEqual([]);
      // Every set spends its caps, so the block above is the most a set can cost.
      const sets = pkg
         .listWorkflows()
         .flatMap((w) => pkg.getWorkflow(w.name)!.skill_sets);
      expect(sets).toHaveLength(CAP_SETS);
      expect(sets.reduce((n, set) => n + set.skills.length, 0)).toBe(
         CAP_SETS * SKILL_SET_INLINE_MAX_FILES,
      );
      expect(sets.reduce((n, set) => n + (set.assets?.length ?? 0), 0)).toBe(
         CAP_SETS * SKILL_SET_ASSETS_MAX_FILES,
      );
      for (const set of sets) {
         const bytes = (set.skills as Array<{ file_contents: string }>).reduce(
            (n, file) => n + Buffer.byteLength(file.file_contents),
            0,
         );
         expect(bytes).toBeLessThanOrEqual(SKILL_SET_MARKDOWN_MAX_BYTES);
         expect(bytes).toBeGreaterThan(0.99 * SKILL_SET_MARKDOWN_MAX_BYTES);
      }
      expect(maxBlockMs).toBeLessThan(MAX_BLOCK_MS);
   }, 60_000);
});
