// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * A `.malloynb`'s cells run through the governed query endpoint, the way the
 * SDK's `Report` runs them: the cells read from `GET …/reports/{path}`'s
 * `notebook`, and each code cell's `run:` statement (with the render tags
 * right above it) sent as `query` to `POST …/models/{path}/query`, the
 * notebook's own path as the model. That path compiles every cell, so the
 * setup cells before a query (its imports, `source:` and `given:`
 * declarations) are already in scope and nothing is prepended.
 *
 * The first block pins the governance answers on that path: the package
 * surface, `#(authorize)`, a row-level `#(access_filter)` with and without its
 * given, a given declared only in a later cell, and an import in the cell that
 * runs. It stays when the deprecated cell route is removed.
 *
 * The second block compares every cell against the deprecated cell route
 * (`GET …/notebooks/{path}/cells/{i}`) and lists where the two differ. It is
 * deleted with that route.
 */

import { rm } from "fs/promises";
import path from "path";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import {
   fixture,
   startPackageEnv,
   type PackageEnv,
} from "../../harness/reports";

/** The fixture as published, behind its `index.malloy` surface. */
const CURATED = "malloynb-curated";
/** The same files with no `index.malloy`: every source is on the surface. */
const OPEN = "malloynb-open";

/**
 * The text the SDK sends for a `.malloynb` code cell: from the cell's last
 * `run:` line to its end, with the `#` render tags directly above it. The
 * cell's imports and declarations are left out, because the notebook's path
 * already compiles them. Undefined for a cell with nothing to run. Mirrors
 * `malloynbCellQuery` in the SDK.
 */
function malloynbCellQuery(text: string): string | undefined {
   const lines = text.split("\n");
   let run = -1;
   lines.forEach((line, i) => {
      if (/^\s*run\s*:/.test(line)) run = i;
   });
   if (run < 0) return undefined;
   let start = run;
   while (start > 0 && /^\s*#(?!#)/.test(lines[start - 1])) start--;
   return lines.slice(start).join("\n").trim();
}

interface Cell {
   type: "markdown" | "code";
   text: string;
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

   /** A notebook's cells, as `GET …/reports/{path}` serves them. */
   const cellsOf = async (pkg: string, nb: string): Promise<Cell[]> => {
      const report = await env.getJson<{
         notebook?: { format?: string; notebookCells?: Cell[] };
      }>(env.api(`/reports/notebooks/${nb}`, pkg));
      expect(report.notebook?.format).toBe("malloynb");
      return report.notebook?.notebookCells ?? [];
   };

   /** Run cell `i` of `nb` as the SDK does, with `givens`. */
   const run = async (
      pkg: string,
      nb: string,
      i: number,
      givens: object = {},
   ): Promise<Outcome> => {
      const query = malloynbCellQuery((await cellsOf(pkg, nb))[i].text);
      if (query === undefined) throw new Error(`${nb} cell ${i} has no run:`);
      const res = await env.query(`notebooks/${nb}`, { query, givens }, pkg);
      return outcomeOf(res.status, await res.json());
   };

   const refusal = (outcome: Outcome) =>
      "refused" in outcome ? outcome : undefined;

   it("sends the code cells that run, and only those", async () => {
      const sent = async (nb: string) =>
         (await cellsOf(CURATED, nb)).flatMap((cell, i) =>
            cell.type === "code" && malloynbCellQuery(cell.text) ? [i] : [],
         );
      // Markdown and declaration-only cells are not sent.
      expect(await sent("surface.malloynb")).toEqual([2, 3, 5, 6]);
      expect(await sent("authorize.malloynb")).toEqual([1, 2]);
      expect(await sent("import_cell.malloynb")).toEqual([1, 2, 3]);
      // The render tag goes with the run, the selective import does not.
      const cells = await cellsOf(CURATED, "import_cell.malloynb");
      expect(malloynbCellQuery(cells[3].text)).toBe(
         "# bar_chart\nrun: orders -> { group_by: region; aggregate: order_count; order_by: region }",
      );
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

   // DEPRECATED ROUTE COMPARISON: deleted with `GET …/notebooks/{path}/cells/{i}`.
   describe("compared with the deprecated cell route", () => {
      const GIVENS: object[] = [
         {},
         { ROLES: ["admin"] },
         { ROLES: ["viewer"] },
         { GROUPS: [1, 2] },
         { GROUPS: [] },
         { GROUPS: [4, 5] },
         { REGION: "EU" },
         { REGION: "EU", GROUPS: [1] },
         { NOtaGiven: 1 },
      ];
      const NOTEBOOKS = [
         "surface.malloynb",
         "raw_sql.malloynb",
         "authorize.malloynb",
         "row_level.malloynb",
         "later_given.malloynb",
         "import_cell.malloynb",
      ];
      /**
       * Where the two differ, by `package/notebook#cell`. A source a cell
       * declares over a published one is not on a curated package's surface,
       * so the query route answers 404 whatever the givens; the cell route
       * ran it under its gate.
       */
      const SURFACE_DIFFERENCES = [
         `${CURATED}/surface.malloynb#5`,
         `${CURATED}/authorize.malloynb#2`,
         `${CURATED}/row_level.malloynb#2`,
      ];
      /** The unknown-given 400 lists the givens in scope: the cell's, or the notebook's. */
      const scopeFree = (outcome: Outcome): Outcome =>
         "refused" in outcome && outcome.message
            ? {
                 ...outcome,
                 message: outcome.message.replace(
                    / Model surfaces \[.*\]$/,
                    "",
                 ),
              }
            : outcome;

      it("agrees cell by cell, except where a curated package refuses a cell's own source", async () => {
         const differences = new Set<string>();
         const unexpected: unknown[] = [];
         const seen = { answered: 0, refused: 0, notSent: 0 };
         for (const pkg of [CURATED, OPEN]) {
            for (const nb of NOTEBOOKS) {
               const cells = await cellsOf(pkg, nb);
               for (let i = 0; i < cells.length; i++) {
                  if (cells[i].type !== "code") continue;
                  const query = malloynbCellQuery(cells[i].text);
                  for (const givens of GIVENS) {
                     const viaCell = await fetch(
                        env.api(
                           `/notebooks/notebooks/${nb}/cells/${i}?givens=${encodeURIComponent(JSON.stringify(givens))}`,
                           pkg,
                        ),
                     );
                     const cellBody = (await viaCell.json()) as {
                        result?: string;
                        message?: string;
                     };
                     if (query === undefined) {
                        // A cell the SDK does not send is one the cell route
                        // ran without a result.
                        expect(viaCell.status).toBe(200);
                        expect(cellBody.result).toBeUndefined();
                        seen.notSent++;
                        continue;
                     }
                     const viaQuery = await env.query(
                        `notebooks/${nb}`,
                        { query, givens },
                        pkg,
                     );
                     const a = scopeFree(outcomeOf(viaCell.status, cellBody));
                     const b = scopeFree(
                        outcomeOf(viaQuery.status, await viaQuery.json()),
                     );
                     const key = `${pkg}/${nb}#${i}`;
                     if (JSON.stringify(a) === JSON.stringify(b)) {
                        seen["rows" in a ? "answered" : "refused"]++;
                     } else if (
                        SURFACE_DIFFERENCES.includes(key) &&
                        "refused" in b &&
                        b.refused === 404
                     ) {
                        differences.add(key);
                     } else {
                        unexpected.push({
                           key,
                           givens,
                           cellRoute: a,
                           queryRoute: b,
                        });
                     }
                  }
               }
            }
         }
         expect(unexpected).toEqual([]);
         expect([...differences].sort()).toEqual(
            [...SURFACE_DIFFERENCES].sort(),
         );
         expect(seen.answered).toBeGreaterThan(60);
         expect(seen.refused).toBeGreaterThan(100);
         expect(seen.notSent).toBeGreaterThan(0);
      });
   });
});
