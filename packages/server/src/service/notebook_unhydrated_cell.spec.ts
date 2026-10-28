// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// DEPRECATED FEATURE COVERAGE — delete together with
// `.malloynb` notebooks (see RELEASE_NOTES deprecations). Every test in
// this file is about that deprecated feature.

/**
 * A `.malloynb` code cell whose query did not hydrate keeps its `queryInfo`
 * but has nothing to check it against the surface with. Driven over copies of
 * `tests/fixtures/notebooks-malloyyo-surface/`, with and without its surface.
 */
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
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
const NOTEBOOK = "notebooks/legacy_open.malloynb";

describe("a .malloynb cell with no hydrated query", () => {
   const tempDirs: string[] = [];

   beforeAll(async () => {
      await __setPackageLoadPoolForTests(new PackageLoadPool(1));
   });

   afterAll(async () => {
      await __setPackageLoadPoolForTests(null);
      for (const dir of tempDirs)
         fs.rmSync(dir, { recursive: true, force: true });
   });

   /** The notebook GET's last cell after dropping that cell's runnable. */
   const lastCellOf = async (withSurface: boolean, notebook = NOTEBOOK) => {
      const dir = fs.mkdtempSync(
         path.join(os.tmpdir(), "publisher-nb-unhydrated-"),
      );
      tempDirs.push(dir);
      fs.cpSync(FIXTURE_DIR, dir, { recursive: true });
      if (!withSurface) fs.unlinkSync(path.join(dir, "index.malloy"));
      const { MalloyConfig } = await import("@malloydata/malloy");
      const pkg = await Package.create(
         "env",
         "notebooks-malloyyo-surface",
         dir,
         new MalloyConfig({ connections: {} }),
      );
      const model = pkg.getModel(notebook);
      if (!model) throw new Error(`${notebook} is not in the package`);
      const cells = (
         model as unknown as {
            runnableNotebookCells: {
               runnable?: unknown;
               queryInfo?: unknown;
            }[];
         }
      ).runnableNotebookCells;
      const last = cells[cells.length - 1];
      expect(last.queryInfo).toBeDefined();
      last.runnable = undefined;
      const raw = await model.getNotebook();
      return raw.notebookCells?.[raw.notebookCells.length - 1];
   };

   it("withholds its queryInfo under a surface", async () => {
      expect((await lastCellOf(true))?.queryInfo).toBeUndefined();
   });

   it("shows its queryInfo when nothing is curated", async () => {
      expect((await lastCellOf(false))?.queryInfo).toContain("order_count");
   });
});
