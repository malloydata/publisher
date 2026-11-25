// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// DEPRECATED FEATURE COVERAGE — delete together with
// `.malloynb` notebooks (see RELEASE_NOTES deprecations). Every test in
// this file is about that deprecated feature.

/**
 * The reports read decides whether a `.malloynb` cell's `queryInfo` is shown
 * by compiling the cell's `run:` under the package surface. Cells and the
 * model are fixed for a Model's life, so that is decided once per cell, not
 * on every read. Driven over a copy of
 * `tests/fixtures/notebooks-malloyyo-surface/`.
 */
import type { ModelMaterializer } from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it, spyOn } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import {
   PackageLoadPool,
   __setPackageLoadPoolForTests,
} from "../package_load/package_load_pool";
import { Package } from "./package";

const FIXTURE_DIR = path.resolve(
   __dirname,
   "../../tests/fixtures/notebooks-malloyyo-surface",
);

describe("a .malloynb cell's queryInfo verdict", () => {
   let dir = "";

   beforeAll(async () => {
      await __setPackageLoadPoolForTests(new PackageLoadPool(1));
      dir = fs.mkdtempSync(path.join(os.tmpdir(), "publisher-nb-memo-"));
      fs.cpSync(FIXTURE_DIR, dir, { recursive: true });
   });

   afterAll(async () => {
      await __setPackageLoadPoolForTests(null);
      fs.rmSync(dir, { recursive: true, force: true });
   });

   it("is decided on the first read and not compiled again on the next", async () => {
      const { MalloyConfig } = await import("@malloydata/malloy");
      const pkg = await Package.create(
         "env",
         "notebooks-malloyyo-surface",
         dir,
         new MalloyConfig({ connections: {} }),
      );
      const model = pkg.getModel("notebooks/legacy.malloynb");
      if (!model) throw new Error("legacy.malloynb is not in the package");
      const materializer = (
         model as unknown as { modelMaterializer: ModelMaterializer }
      ).modelMaterializer;
      const compiles = spyOn(materializer, "loadRestrictedQuery");
      const shown = async () =>
         ((await model.getNotebook()).notebookCells ?? []).map(
            (cell) => cell.queryInfo !== undefined,
         );

      // The published source's cell is shown, the hidden one's withheld.
      expect(await shown()).toEqual([true, false]);
      const first = compiles.mock.calls.length;
      expect(first).toBeGreaterThan(0);
      expect(await shown()).toEqual([true, false]);
      expect(compiles.mock.calls.length).toBe(first);
   });
});
