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
import { Model } from "../../src/service/model";
import type { Package } from "../../src/service/package";
import {
   loadPackageCopy,
   loadTempPackage,
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
               "reader in the Console: it reads hidden, which index.malloy doesn't export " +
               "(404). Fix: add hidden to the export { ... } in index.malloy.",
         },
         {
            model: "notebooks/surface.malloynb",
            code: CODE,
            reason: "off_surface",
            severity: "warn" as const,
            message:
               "Cell 6 of notebooks/surface.malloynb won't run for any " +
               "reader in the Console: it reads mine, a source the notebook declares, and " +
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
               "reader in the Console: it reads from_hidden, a source the notebook " +
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
            `any reader in the Console: the query route runs its run: statement in ` +
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
      // The table read has no source to export: the fix is restricted mode's.
      expect(curatedWarnings[0].message).toBe(
         "Cell 3 of notebooks/restricted.malloynb won't run for any reader " +
            "in the Console: the package surface refuses it (404), and " +
            `restricted mode refuses it too: ${table} Fix: declare the ` +
            "table, or a dimension over the sql_* expression, in a model " +
            "file that index.malloy exports, and run that source.",
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

   it("the route's text guards: an annotation reserved for the author, and a required #(filter)", () => {
      const guarded = (cell: number, reason: string, why: string) => ({
         model: "notebooks/route_text.malloynb",
         code: CODE,
         reason,
         severity: "warn" as const,
         message:
            `Cell ${cell} of notebooks/route_text.malloynb won't run for any ` +
            `reader in the Console: ${why}`,
      });
      const annotation = guarded(
         2,
         "caller_annotation",
         "the query route refuses query text that carries an annotation " +
            "only the model author may write (400): An `authorize` " +
            "annotation is not permitted in caller-submitted Malloy text. " +
            "Fix: remove the annotation, or the text that spells it, from " +
            "the cell's run: statement, comments included.",
      );
      const required = guarded(
         4,
         "required_filter",
         "the source it runs declares a required #(filter), and the " +
            "Console sends no filter value for a notebook cell (400): " +
            'Required filter "region" (dimension: region) was not provided. ' +
            "Fix: drop required from that #(filter), which is deprecated.",
      );
      expect(refusedCells(open.pkg, "route_text.malloynb")).toEqual([
         annotation,
         required,
      ]);
      // Behind index.malloy, the named query over a hidden source is refused
      // too, and the warning names the source the query reads.
      expect(refusedCells(curated.pkg, "route_text.malloynb")).toEqual([
         annotation,
         guarded(
            3,
            "off_surface",
            "it runs the query hq, and hq reads hidden, which index.malloy " +
               "doesn't export (404). Fix: add hidden to the export { ... } " +
               "in index.malloy.",
         ),
         required,
      ]);
   });
});

// DEPRECATED FEATURE COVERAGE: delete with `.malloynb` notebooks.
describe("the .malloynb cell check, as a neighbor of the other lints", () => {
   usePackageLoadPool();

   const ORDERS = `source: orders is duckdb.sql("select 1 as id, 'US' as region") extend {
   # drill
   dimension: r is region
   measure: order_count is count()
}
`;

   it("a throw in it keeps the drill, self-drill and given findings", async () => {
      const loaded = await loadTempPackage({
         "orders.malloy": ORDERS,
         "notebooks/n.malloynb":
            '>>>malloy\nimport "../orders.malloy"\n>>>malloy\nrun: orders -> { aggregate: order_count }',
      });
      const original = Model.prototype.malloynbCellRefusals;
      try {
         const drill = (w: { message?: string }) =>
            (w.message ?? "").includes("names no destination");
         const warnings = () => loaded.pkg.getPackageMetadata().warnings ?? [];
         expect(warnings().filter(drill)).toHaveLength(1);
         Model.prototype.malloynbCellRefusals = async () => {
            throw new Error("cell check failed");
         };
         await loaded.pkg.relintReports();
         expect(warnings().filter(drill)).toHaveLength(1);
         expect(
            warnings().filter((w) =>
               (w.message ?? "").startsWith("Report lint stopped early"),
            ),
         ).toEqual([]);
         expect(
            warnings()
               .filter((w) =>
                  (w.message ?? "").startsWith("The .malloynb cell check"),
               )
               .map((w) => [w.model, w.severity]),
         ).toEqual([["notebooks/n.malloynb", "warn"]]);
      } finally {
         Model.prototype.malloynbCellRefusals = original;
         await loaded.cleanup();
      }
   });

   it("hands the event loop a turn while it checks many cells", async () => {
      // 20 notebooks of 20 code cells: one block of about 100 ms if the check
      // never yields.
      const files: Record<string, string> = { "orders.malloy": ORDERS };
      for (let n = 0; n < 20; n++) {
         const cells = ['>>>malloy\nimport "../orders.malloy"'];
         for (let c = 0; c < 20; c++) {
            cells.push(
               `>>>malloy\nrun: orders -> { group_by: r; aggregate: c${c} is count() }`,
            );
         }
         files[`notebooks/n${n}.malloynb`] = cells.join("\n");
      }
      const loaded = await loadTempPackage(files);
      try {
         let last = performance.now();
         let longest = 0;
         const timer = setInterval(() => {
            const now = performance.now();
            longest = Math.max(longest, now - last);
            last = now;
         }, 1);
         const started = performance.now();
         await loaded.pkg.relintReports();
         const took = performance.now() - started;
         // A block that never yielded ends here: the timer never ran inside it.
         longest = Math.max(longest, performance.now() - last);
         clearInterval(timer);
         // The check really ran long enough to need the yields.
         expect(took).toBeGreaterThan(60);
         expect(longest).toBeLessThan(50);
      } finally {
         await loaded.cleanup();
      }
   });
});
