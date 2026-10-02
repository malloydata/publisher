// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

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
const ORIGINAL_ENV = process.env.PACKAGE_LOAD_WORKERS;
const NOTEBOOK = "notebooks/legacy_open.malloynb";

describe("a .malloynb cell with no hydrated query", () => {
   const tempDirs: string[] = [];

   beforeAll(async () => {
      process.env.PACKAGE_LOAD_WORKERS = "1";
      await __setPackageLoadPoolForTests(new PackageLoadPool(1));
   });

   afterAll(async () => {
      await __setPackageLoadPoolForTests(null);
      if (ORIGINAL_ENV === undefined) delete process.env.PACKAGE_LOAD_WORKERS;
      else process.env.PACKAGE_LOAD_WORKERS = ORIGINAL_ENV;
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

   it("keeps a served notebook cell's own markdown under a surface, whose queryInfo it withholds", async () => {
      const cell = await lastCellOf(true, "notebooks/local.malloy");
      expect(cell?.queryInfo).toBeUndefined();
      expect(cell?.markdown).toStartWith("Reads the hidden file");
      expect(cell?.proseLines).toEqual([[0, 0]]);
      expect(cell?.codeLine).toBe(1);
   });
});
