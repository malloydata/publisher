// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// DEPRECATED FEATURE COVERAGE: delete with `.malloynb` notebooks. Every test
// in this file is about that deprecated feature.

/**
 * A `.malloynb` code cell runs through `POST …/models/{path}/query` as its
 * `run:` statement alone. A cell the query route refuses for every reader
 * gets one `malloynb-cell-refused` package warning at load, naming the
 * notebook, the cell and the rule, so the author hears it before a reader
 * does. Drives the `malloynb-parity` fixture through `Package.create` on the
 * worker path, behind its `index.malloy` surface and with that file removed.
 */
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import type { Package } from "../../src/service/package";
import {
   loadPackageCopy,
   usePackageLoadPool,
   type LoadedTempPackage,
} from "../../src/test_helpers/temp_package";

const FIXTURE = path.resolve(__dirname, "../fixtures/malloynb-parity");
const CODE = "malloynb-cell-refused";

/** The package's refused-cell warnings on `notebook`, in load order. */
const refusedCells = (pkg: Package, notebook: string) =>
   (pkg.getPackageMetadata().warnings ?? []).filter(
      (w) => w.code === CODE && w.model === `notebooks/${notebook}`,
   );

describe("a .malloynb cell the query route refuses warns at load", () => {
   usePackageLoadPool();
   let curated: LoadedTempPackage;
   let open: LoadedTempPackage;
   let openSource: string;

   beforeAll(async () => {
      curated = await loadPackageCopy(FIXTURE, { name: "curated" });
      // The same files with no index.malloy: every source is on the surface.
      openSource = fs.mkdtempSync(path.join(os.tmpdir(), "malloynb-open-"));
      fs.cpSync(FIXTURE, openSource, { recursive: true });
      fs.rmSync(path.join(openSource, "index.malloy"));
      open = await loadPackageCopy(openSource, { name: "open" });
   }, 120_000);

   afterAll(async () => {
      await curated?.cleanup();
      await open?.cleanup();
      fs.rmSync(openSource, { recursive: true, force: true });
   });

   it("off the surface: a source the notebook declares, or one index.malloy hides", () => {
      expect(refusedCells(curated.pkg, "surface.malloynb")).toEqual([
         {
            model: "notebooks/surface.malloynb",
            code: CODE,
            reason: "off_surface",
            severity: "warn" as const,
            message:
               "Cell 4 of notebooks/surface.malloynb won't run for any " +
               "reader: it reads hidden, which index.malloy doesn't export " +
               "(404). Fix: add hidden to the export { ... } in index.malloy.",
         },
         {
            model: "notebooks/surface.malloynb",
            code: CODE,
            reason: "off_surface",
            severity: "warn" as const,
            message:
               "Cell 6 of notebooks/surface.malloynb won't run for any " +
               "reader: it reads mine, a source the notebook declares, and " +
               "the query route takes only a source the package surface " +
               "publishes (404). Declare mine in a model file instead. Fix: " +
               "add mine to the export { ... } in index.malloy.",
         },
         {
            model: "notebooks/surface.malloynb",
            code: CODE,
            reason: "off_surface",
            severity: "warn" as const,
            message:
               "Cell 7 of notebooks/surface.malloynb won't run for any " +
               "reader: it reads from_hidden, a source the notebook " +
               "declares, and the query route takes only a source the " +
               "package surface publishes (404). Declare from_hidden in a " +
               "model file instead. Fix: add from_hidden to the export " +
               "{ ... } in index.malloy.",
         },
      ]);
      // With no index.malloy every source is queryable, the notebook's own too.
      expect(refusedCells(open.pkg, "surface.malloynb")).toEqual([]);
   });

   it("restricted mode: a direct table read or a sql_* call in the run:", () => {
      const restricted = (cell: number, problem: string, home: string) => ({
         model: "notebooks/restricted.malloynb",
         code: CODE,
         reason: "restricted_mode",
         severity: "warn" as const,
         message:
            `Cell ${cell} of notebooks/restricted.malloynb won't run for ` +
            `any reader: the query route runs its run: statement in ` +
            `restricted mode, which refuses it (400): ${problem} Fix: ` +
            `declare the table, or a dimension over the sql_* expression, ` +
            `in ${home}, and run that source.`,
      });
      const table =
         "`duckdb.table(...)` cannot be used in a restricted query — direct " +
         "table access is not permitted.";
      const sql =
         "`sql_number(...)` cannot be used in a restricted query — the " +
         "`sql_*` function family emits user-supplied SQL directly, which " +
         "is not permitted.";
      expect(refusedCells(open.pkg, "restricted.malloynb")).toEqual([
         restricted(3, table, "a source the notebook declares"),
         restricted(4, sql, "a source the notebook declares"),
      ]);
      // Behind index.malloy the surface refuses the table read first, as the
      // query route does, and a source the notebook declares is off it.
      const curatedWarnings = refusedCells(curated.pkg, "restricted.malloynb");
      expect(
         curatedWarnings.map((w) => [w.reason, w.message?.slice(0, 7)]),
      ).toEqual([
         ["off_surface", "Cell 3 "],
         ["restricted_mode", "Cell 4 "],
         ["off_surface", "Cell 5 "],
         ["off_surface", "Cell 6 "],
      ]);
      expect(curatedWarnings[0].message).toBe(
         "Cell 3 of notebooks/restricted.malloynb won't run for any reader: " +
            "it reads a source the package surface does not publish, or one " +
            "the notebook declares (404). Fix: add the source it reads to " +
            "the export { ... } in index.malloy, declaring it in a model " +
            `file first if the notebook declares it. Restricted mode also ` +
            `refuses it: ${table}`,
      );
      expect(curatedWarnings[1]).toEqual(
         restricted(4, sql, "a model file that index.malloy exports"),
      );
   });

   it("no warning for a cell that runs, or that only a reader's givens refuse", async () => {
      // later_given runs on both packages; authorize and row_level answer 403
      // to a reader without the right givens, which is the reader's, not the
      // author's, to fix.
      for (const { pkg } of [curated, open]) {
         const model = pkg.getModel("notebooks/later_given.malloynb")!;
         expect(await model.malloynbCellRefusals()).toEqual([]);
         expect(refusedCells(pkg, "later_given.malloynb")).toEqual([]);
      }
      for (const notebook of ["authorize.malloynb", "row_level.malloynb"]) {
         expect(refusedCells(open.pkg, notebook)).toEqual([]);
      }
      // The restricted notebook's remedies run: the source it declares over
      // the table or the sql_* dimension, and the published orders_sql.
      const restricted = await open.pkg
         .getModel("notebooks/restricted.malloynb")!
         .malloynbCellRefusals();
      expect(restricted.map(({ cell }) => cell)).toEqual([3, 4]);
   });
});
