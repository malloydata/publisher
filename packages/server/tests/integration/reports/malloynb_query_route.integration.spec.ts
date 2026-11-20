// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * A `.malloynb`'s cells run through the governed query endpoint, the way the
 * SDK's `Report` runs them: the cells read from `GET …/reports/{path}`'s
 * `notebook`, and each code cell's served `runQuery` (its `run:` statement
 * with the render tags right above it) sent as `query` to
 * `POST …/models/{path}/query`, the notebook's own path as the model. That path compiles every cell, so the
 * setup cells before a query (its imports, `source:` and `given:`
 * declarations) are already in scope and nothing is prepended.
 *
 * It pins the governance answers on that path: the package surface,
 * `#(authorize)`, a row-level `#(access_filter)` with and without its given, a
 * given declared only in a later cell, an import in the cell that runs, a
 * join the cell's query writes, and the restricted mode every query's text
 * runs in. It also pins that the
 * notebook read lists a source with its schema only where that path may
 * query it, and that the load warns about exactly the cells that path
 * refuses for every reader.
 */

import { rm } from "fs/promises";
import path from "path";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import {
   fixture,
   lf,
   startPackageEnv,
   warningsByCode,
   type PackageEnv,
} from "../../harness/reports";

/** The fixture as published, behind its `index.malloy` surface. */
const CURATED = "malloynb-curated";
/** The same files with no `index.malloy`: every source is on the surface. */
const OPEN = "malloynb-open";

interface Cell {
   type: "markdown" | "code";
   text: string;
   newSources?: string[];
   queryInfo?: string;
   runQuery?: string;
}

type Outcome = { rows: unknown[][] } | { refused: number; message?: string };

/** An answer as rows of plain values, or the refusal's status and message. */
const outcomeOf = (
   status: number,
   body: { result?: string; message?: string },
): Outcome => {
   if (status !== 200 || body.result === undefined)
      return { refused: status, message: body.message };
   const parsed = JSON.parse(body.result) as {
      data?: {
         array_value?: {
            record_value?: { number_value?: number; string_value?: string }[];
         }[];
      };
   };
   return {
      rows: (parsed.data?.array_value ?? []).map((row) =>
         (row.record_value ?? []).map(
            (cell) => cell.number_value ?? cell.string_value ?? null,
         ),
      ),
   };
};

const NOT_QUERYABLE = /^Query target is not queryable\./;

describe(".malloynb cells through the query route (E2E)", () => {
   let env: PackageEnv;

   beforeAll(async () => {
      env = await startPackageEnv(
         "malloynb-query-route-env",
         {
            [CURATED]: fixture("malloynb-parity"),
            [OPEN]: fixture("malloynb-parity"),
         },
         {
            prepare: async (dir, pkg) => {
               if (pkg === OPEN) await rm(path.join(dir, "index.malloy"));
            },
         },
      );
   }, 300_000);

   afterAll(() => env?.stop());

   /** A notebook, as `GET …/reports/{path}` serves it. */
   const notebookOf = async (pkg: string, nb: string) => {
      const report = await env.getJson<{
         notebook?: {
            format?: string;
            notebookCells?: Cell[];
            sources?: { name?: string }[];
         };
      }>(env.api(`/reports/notebooks/${nb}`, pkg));
      expect(report.notebook?.format).toBe("malloynb");
      return report.notebook ?? {};
   };

   /** A notebook's cells, as `GET …/reports/{path}` serves them. */
   const cellsOf = async (pkg: string, nb: string): Promise<Cell[]> =>
      (await notebookOf(pkg, nb)).notebookCells ?? [];

   /** Run cell `i` of `nb` as the SDK does, with `givens`. */
   const run = async (
      pkg: string,
      nb: string,
      i: number,
      givens: object = {},
   ): Promise<Outcome> => {
      const query = (await cellsOf(pkg, nb))[i].runQuery;
      if (query === undefined) throw new Error(`${nb} cell ${i} has no run:`);
      const res = await env.query(`notebooks/${nb}`, { query, givens }, pkg);
      return outcomeOf(res.status, await res.json());
   };

   const refusal = (outcome: Outcome) =>
      "refused" in outcome ? outcome : undefined;

   it("sends the code cells that run, and only those", async () => {
      const sent = async (nb: string) =>
         (await cellsOf(CURATED, nb)).flatMap((cell, i) =>
            cell.type === "code" && cell.runQuery ? [i] : [],
         );
      // Markdown and declaration-only cells are not sent.
      expect(await sent("surface.malloynb")).toEqual([2, 3, 5, 6]);
      expect(await sent("authorize.malloynb")).toEqual([1, 2]);
      expect(await sent("import_cell.malloynb")).toEqual([1, 2, 3]);
      expect(await sent("caller_join.malloynb")).toEqual([2, 3, 4, 5, 6, 7]);
      // The render tag goes with the run, the selective import does not.
      const cells = await cellsOf(CURATED, "import_cell.malloynb");
      expect(lf(cells[3].runQuery)).toBe(
         "# bar_chart\nrun: orders -> { group_by: region; aggregate: order_count; order_by: region }",
      );
   });

   it("serves each code cell's run: with the render tags above it, and runs what it serves", async () => {
      const nb = "render_tags.malloynb";
      const expected = [
         undefined,
         undefined,
         "#| bar_chart {\n  size: lg\n|#\nrun: orders -> { group_by: region; aggregate: order_count; order_by: region }",
         "#|(markdown)\nOrders (by region\n|#\nrun: orders -> { aggregate: order_count }",
         "run: orders -> { aggregate: order_count }",
         "run: importé -> { aggregate: order_count }",
      ];
      for (const pkg of [CURATED, OPEN]) {
         const served = (await cellsOf(pkg, nb)).map((cell) =>
            lf(cell.runQuery),
         );
         expect(served).toEqual(expected);
      }
      // The block annotation's chart config goes with the query.
      const chart = await env.query(
         `notebooks/${nb}`,
         { query: (await cellsOf(CURATED, nb))[2].runQuery },
         CURATED,
      );
      const body = (await chart.json()) as { result?: string };
      expect(chart.status).toBe(200);
      expect(body.result).toContain("size: lg");
      expect(outcomeOf(200, body)).toEqual({
         rows: [
            ["EU", 3],
            ["US", 3],
         ],
      });
      for (const i of [3, 4]) {
         expect(await run(CURATED, nb, i)).toEqual({ rows: [[6]] });
      }
      // `importé` is the cell's own source: off the surface, where the
      // queryInfo screen withholds its schema too.
      expect(await run(OPEN, nb, 5)).toEqual({ rows: [[6]] });
      expect(refusal(await run(CURATED, nb, 5))?.refused).toBe(404);
      const withSchema = async (pkg: string) =>
         (await cellsOf(pkg, nb)).flatMap((cell, i) =>
            cell.queryInfo !== undefined ? [i] : [],
         );
      expect(await withSchema(CURATED)).toEqual([2, 3, 4]);
      expect(await withSchema(OPEN)).toEqual([2, 3, 4, 5]);
   });

   it("holds the cells to the package surface", async () => {
      expect(await run(CURATED, "surface.malloynb", 2)).toEqual({
         rows: [[6]],
      });
      // A hidden source, one a cell derives from it, and a cell's own raw SQL.
      for (const [nb, i] of [
         ["surface.malloynb", 3],
         ["surface.malloynb", 6],
         ["raw_sql.malloynb", 1],
         ["import_cell.malloynb", 2],
      ] as const) {
         const refused = refusal(await run(CURATED, nb, i));
         expect(refused?.refused).toBe(404);
         expect(refused?.message).toMatch(NOT_QUERYABLE);
      }
      // A source a cell declares over a published one is not on the surface
      // either: the query route refuses it, where the cell route ran it.
      expect(refusal(await run(CURATED, "surface.malloynb", 5))?.refused).toBe(
         404,
      );
      // With no index.malloy every source is queryable, the cell's own too.
      expect(await run(OPEN, "surface.malloynb", 3)).toEqual({
         rows: [[1550]],
      });
      expect(await run(OPEN, "surface.malloynb", 5)).toEqual({ rows: [[4]] });
   });

   it("lists a source the cells declare, with its schema, only where the query route takes it", async () => {
      // surface.malloynb declares `mine` over the published orders in cell 4
      // and runs it in cell 5, which answers 404 on the curated package.
      const shown = async (pkg: string) => {
         const notebook = await notebookOf(pkg, "surface.malloynb");
         const cells = notebook.notebookCells ?? [];
         return {
            declared: (cells[4].newSources ?? []).map(
               (source) => (JSON.parse(source) as { name?: string }).name,
            ),
            listed: (notebook.sources ?? []).some((s) => s.name === "mine"),
            runSchema: cells[5].queryInfo !== undefined,
            publishedSchema: cells[2].queryInfo !== undefined,
         };
      };
      expect(await shown(CURATED)).toEqual({
         declared: [],
         listed: false,
         runSchema: false,
         publishedSchema: true,
      });
      expect(await shown(OPEN)).toEqual({
         declared: ["mine"],
         listed: true,
         runSchema: true,
         publishedSchema: true,
      });
      // A cell whose run: joins a hidden source, in its source or in its own
      // `-> { … }`, answers 404 on the curated package, so its schema is not
      // shown there either. A join to a published source keeps its own.
      const withSchema = async (pkg: string) =>
         (await cellsOf(pkg, "caller_join.malloynb")).flatMap((cell, i) =>
            cell.queryInfo !== undefined ? [i] : [],
         );
      expect(await withSchema(CURATED)).toEqual([2, 3, 5]);
      expect(await withSchema(OPEN)).toEqual([2, 3, 4, 5, 6, 7]);
   });

   it("runs every cell's query in restricted mode, and the way to keep each shape runs", async () => {
      const nb = "restricted.malloynb";
      // A direct table read: refused on the open package; on the curated one
      // the surface refuses it first, as before.
      const table = refusal(await run(OPEN, nb, 2));
      expect(table?.refused).toBe(400);
      expect(table?.message).toContain(
         "`duckdb.table(...)` cannot be used in a restricted query",
      );
      expect(refusal(await run(CURATED, nb, 2))?.refused).toBe(404);
      // A sql_* call in the query: refused on both.
      for (const pkg of [OPEN, CURATED]) {
         const sql = refusal(await run(pkg, nb, 3));
         expect(sql?.refused).toBe(400);
         expect(sql?.message).toContain(
            "`sql_number(...)` cannot be used in a restricted query",
         );
      }
      // The remedy: declare the table, or the sql_* dimension, in a source of
      // a cell and run that source. On the curated package a source a cell
      // declares is off the surface, so there it goes in a model file that
      // index.malloy exports.
      expect(await run(OPEN, nb, 4)).toEqual({ rows: [[6]] });
      expect(await run(OPEN, nb, 5)).toEqual({ rows: [[1, 6]] });
      expect(refusal(await run(CURATED, nb, 4))?.refused).toBe(404);
      expect(refusal(await run(CURATED, nb, 5))?.refused).toBe(404);
      for (const pkg of [OPEN, CURATED]) {
         expect(await run(pkg, nb, 6)).toEqual({ rows: [[1, 6]] });
      }
      // On the curated package the notebook read gives no schema for a cell
      // its restricted compile refuses, as for one the surface refuses.
      const withSchema = async (pkg: string) =>
         (await cellsOf(pkg, nb)).flatMap((cell, i) =>
            cell.queryInfo !== undefined ? [i] : [],
         );
      expect(await withSchema(CURATED)).toEqual([6]);
      expect(await withSchema(OPEN)).toEqual([2, 3, 4, 5, 6]);
   });

   it("applies #(authorize), on the source and on one a cell derives from it", async () => {
      const denied = {
         refused: 403,
         message: 'Access denied for source "locked".',
      };
      expect(await run(CURATED, "authorize.malloynb", 1)).toEqual(denied);
      expect(
         await run(CURATED, "authorize.malloynb", 1, { ROLES: ["viewer"] }),
      ).toEqual(denied);
      expect(
         await run(CURATED, "authorize.malloynb", 1, { ROLES: ["admin"] }),
      ).toEqual({ rows: [[6]] });
      expect(
         await run(OPEN, "authorize.malloynb", 2, { ROLES: ["viewer"] }),
      ).toEqual({
         refused: 403,
         message: 'Access denied for source "derived_locked".',
      });
      expect(
         await run(OPEN, "authorize.malloynb", 2, { ROLES: ["admin"] }),
      ).toEqual({ rows: [[6]] });
   });

   it("filters rows by #(access_filter), and denies a gate given left unsupplied", async () => {
      const unsupplied = {
         refused: 403,
         message: 'Access denied for source "gated".',
      };
      expect(await run(CURATED, "row_level.malloynb", 1)).toEqual(unsupplied);
      expect(
         await run(CURATED, "row_level.malloynb", 1, { ROLES: ["admin"] }),
      ).toEqual(unsupplied);
      expect(
         await run(CURATED, "row_level.malloynb", 1, { GROUPS: [1, 2] }),
      ).toEqual({ rows: [[2]] });
      expect(
         await run(CURATED, "row_level.malloynb", 1, { GROUPS: [] }),
      ).toEqual({ rows: [[0]] });
      expect(
         await run(CURATED, "row_level.malloynb", 3, { GROUPS: [4, 5] }),
      ).toEqual({ rows: [[4], [5]] });
      // The gate follows a source a cell derives from the gated one.
      expect(
         await run(OPEN, "row_level.malloynb", 2, { GROUPS: [1, 2] }),
      ).toEqual({ rows: [[2]] });
      expect(await run(OPEN, "row_level.malloynb", 2)).toEqual({
         refused: 403,
         message: 'Access denied for source "derived".',
      });
   });

   it("gates a join the cell's query writes as the caller's join, and keeps a join a cell's source declares the author's", async () => {
      const nb = "caller_join.malloynb";
      for (const pkg of [CURATED, OPEN]) {
         // An #(access_filter) on the joined source: denied with no value for
         // its given, and grafted into the join with one, so an order whose
         // joined row the caller may not see comes back with nulls there.
         const gatedDenied = {
            refused: 403,
            message: 'Access denied for source "g".',
         };
         expect(await run(pkg, nb, 2)).toEqual(gatedDenied);
         expect(await run(pkg, nb, 2, { GROUPS: [1, 2] })).toEqual({
            rows: [
               [1, 1],
               [2, 2],
               [3, null],
               [4, null],
               [5, null],
               [6, null],
            ],
         });
         // The same for a join inside the query's own `-> { … }`.
         expect(await run(pkg, nb, 5)).toEqual(gatedDenied);
         expect(await run(pkg, nb, 5, { GROUPS: [1, 2] })).toEqual({
            rows: [[6, 2]],
         });
         // An #(authorize) on the joined source.
         expect(await run(pkg, nb, 3, { ROLES: ["viewer"] })).toEqual({
            refused: 403,
            message: 'Access denied for source "l".',
         });
         expect(await run(pkg, nb, 3, { ROLES: ["admin"] })).toEqual({
            rows: [[6, 6]],
         });
      }
      // A join into a source the surface hides.
      const hidden = refusal(await run(CURATED, nb, 4));
      expect(hidden?.refused).toBe(404);
      expect(hidden?.message).toMatch(NOT_QUERYABLE);
      expect(await run(OPEN, nb, 4)).toEqual({ rows: [[6, 1550]] });
      // The same for a join inside the query's own `-> { … }`.
      const hiddenInStage = refusal(await run(CURATED, nb, 7));
      expect(hiddenInStage?.refused).toBe(404);
      expect(hiddenInStage?.message).toMatch(NOT_QUERYABLE);
      expect(await run(OPEN, nb, 7)).toEqual({ rows: [[6, 1550]] });
      // A join a cell's `source:` declares is the author's, which a gate does
      // not follow; on the curated package that source is off the surface, so
      // there the join goes in a source of a model file that index.malloy
      // exports.
      expect(await run(OPEN, nb, 6)).toEqual({
         rows: [
            [1, 1],
            [2, 2],
            [3, 3],
            [4, 4],
            [5, 5],
            [6, 6],
         ],
      });
      expect(refusal(await run(CURATED, nb, 6))?.refused).toBe(404);
   });

   it("takes a given an earlier cell does not declare, and leaves that cell's answer alone", async () => {
      // REGION is declared in cell 1 and GROUPS imported in cell 2; cell 0
      // reads neither, so either value is accepted and changes nothing.
      for (const givens of [{}, { REGION: "EU" }, { GROUPS: [1] }]) {
         expect(await run(CURATED, "later_given.malloynb", 0, givens)).toEqual({
            rows: [[6]],
         });
      }
      expect(await run(CURATED, "later_given.malloynb", 1)).toEqual({
         rows: [[350]],
      });
      expect(
         await run(CURATED, "later_given.malloynb", 1, { REGION: "EU" }),
      ).toEqual({ rows: [[1200]] });
      expect(
         await run(CURATED, "later_given.malloynb", 2, {
            REGION: "EU",
            GROUPS: [1],
         }),
      ).toEqual({ rows: [[1]] });
      expect(
         await run(CURATED, "later_given.malloynb", 2, { REGION: "EU" }),
      ).toEqual({ refused: 403, message: 'Access denied for source "gated".' });
      // A name the notebook never declares is refused, on every cell.
      const typo = refusal(
         await run(CURATED, "later_given.malloynb", 0, { NOtaGiven: 1 }),
      );
      expect(typo?.refused).toBe(400);
      expect(typo?.message).toContain("unknown given 'NOtaGiven'");
   });

   it("runs a cell that imports what it runs", async () => {
      expect(
         await run(CURATED, "import_cell.malloynb", 1, { GROUPS: [1, 2] }),
      ).toEqual({ rows: [[2]] });
      expect(await run(CURATED, "import_cell.malloynb", 1)).toEqual({
         refused: 403,
         message: 'Access denied for source "gated".',
      });
      expect(await run(CURATED, "import_cell.malloynb", 3)).toEqual({
         rows: [
            ["EU", 3],
            ["US", 3],
         ],
      });
   });

   it("warns at load about each cell the query route refuses for every reader, and only those", async () => {
      // Every gate the fixture declares, satisfied, sent to a notebook that
      // declares them: an unknown given is itself a 400.
      const allGates: Record<string, unknown> = {
         ROLES: ["admin"],
         GROUPS: [1, 2],
      };
      const notebooks = [
         "authorize.malloynb",
         "caller_join.malloynb",
         "import_cell.malloynb",
         "later_given.malloynb",
         "raw_sql.malloynb",
         "restricted.malloynb",
         "route_text.malloynb",
         "row_level.malloynb",
         "surface.malloynb",
      ];
      const seen = { warned: 0, clean: 0 };
      const disagreements: unknown[] = [];
      for (const pkg of [CURATED, OPEN]) {
         const warned = new Map(
            warningsByCode(
               await env.warnings(pkg),
               "malloynb-cell-refused",
            ).map((w) => [
               `${w.model} ${/^Cell (\d+) /.exec(w.message ?? "")?.[1]}`,
               w,
            ]),
         );
         for (const nb of notebooks) {
            const cells = await cellsOf(pkg, nb);
            // secured.malloy declares both gate givens; no other file declares one.
            const gates = cells.some((c) => c.text.includes("secured.malloy"))
               ? allGates
               : {};
            for (let i = 0; i < cells.length; i++) {
               // The Console runs code cells only (the SDK's `cellRuns`).
               if (cells[i].type !== "code") continue;
               if (malloynbCellQuery(cells[i]) === undefined) continue;
               const warning = warned.get(`notebooks/${nb} ${i + 1}`);
               const answered = await run(pkg, nb, i, gates);
               const status = "rows" in answered ? 200 : answered.refused;
               const expected =
                  warning === undefined
                     ? 200
                     : warning.reason === "off_surface"
                       ? 404
                       : 400;
               if (status !== expected) {
                  disagreements.push({
                     pkg,
                     nb,
                     cell: i + 1,
                     warning,
                     answered,
                  });
               }
               // With no givens, a gate may answer 403 first; never a 200
               // for a cell warned about, never a 400 or 404 for one not.
               const bare = await run(pkg, nb, i);
               const bareStatus = "rows" in bare ? 200 : bare.refused;
               if (
                  warning === undefined
                     ? bareStatus === 400 || bareStatus === 404
                     : bareStatus !== expected && bareStatus !== 403
               ) {
                  disagreements.push({ pkg, nb, cell: i + 1, warning, bare });
               }
               if (warning) seen.warned++;
               else seen.clean++;
            }
         }
      }
      expect(disagreements).toEqual([]);
      // Both outcomes were exercised, on both packages.
      expect(seen.warned).toBe(28);
      expect(seen.clean).toBeGreaterThan(15);
   });

   it("the warning is for a reader in the Console: an editor's surface lift still runs an off-surface cell", async () => {
      const lifted = async (nb: string, i: number) => {
         const query = malloynbCellQuery((await cellsOf(CURATED, nb))[i]);
         const res = await env.post(
            env.api(
               `/models/notebooks/${nb}/query?includeHiddenFilesAndSources=true`,
               CURATED,
            ),
            { query },
         );
         return outcomeOf(res.status, await res.json());
      };
      const warnings = warningsByCode(
         await env.warnings(CURATED),
         "malloynb-cell-refused",
      );
      // Every message says whom the refusal is for.
      for (const w of warnings) {
         expect(w.message).toContain(
            "won't run for any reader in the Console:",
         );
      }
      const warned = warnings.map((w) => [
         w.model,
         w.reason,
         /^Cell (\d+) /.exec(w.message ?? "")?.[1],
      ]);
      expect(warned).toContainEqual([
         "notebooks/surface.malloynb",
         "off_surface",
         "6",
      ]);
      expect(warned).toContainEqual([
         "notebooks/restricted.malloynb",
         "restricted_mode",
         "4",
      ]);
      // Cell 6 of surface.malloynb (index 5) runs with the lift; restricted
      // mode still refuses cell 4 of restricted.malloynb (index 3).
      expect(await lifted("surface.malloynb", 5)).toEqual({ rows: [[4]] });
      expect(refusal(await lifted("restricted.malloynb", 3))?.refused).toBe(
         400,
      );
   });
});
