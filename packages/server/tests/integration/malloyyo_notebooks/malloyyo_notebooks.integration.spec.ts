// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * Linear `.malloy` reports through the real server: a file written as `run:`
 * cells, the retired format, is no report and says why; a report written as
 * tiles serves its manifest, runs each tile through `POST …/models/{path}/query`,
 * and reads as cells (its definitions, then its tiles) on the deprecated
 * notebook routes; and, on a package with a surface, each tile takes the same
 * governed path a `.malloynb` cell runs through.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import path from "path";
import { resetAuthorizeGuardTelemetryForTesting } from "../../../src/authorize_metrics";
import { resetNotebookMetricsForTest } from "../../../src/notebook_metrics";
import { resetReportMetricsForTest } from "../../../src/report_metrics";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "../../../src/test_helpers/metrics_harness";
import {
   fixture,
   startPackageEnv,
   type PackageEnv,
} from "../../harness/reports";

const PLAIN = "notebooks-malloyyo";
const SURFACE = "notebooks-malloyyo-surface";
/** The one report in `PLAIN`'s `notebooks/` written as tiles. */
const LAYOUT = "notebooks/layout.malloy";
const CELLS = "notebooks/cells.malloy";

const CELLS_REMOVED =
   "a linear report written as `run:` cells is no longer read, so this file is not a report. Fix: list its tiles in `tiles=[…]` (a text tile per prose block, a query tile per view), or remove the `## artifact` tag.";

interface Cell {
   type?: string;
   kind?: string;
   text?: string;
   queryInfo?: string;
   result?: string;
}

interface Problem {
   code?: string;
   severity: string;
   message: string;
   model?: string;
   at?: { range: { start: { line: number } } };
}

/** The rows of a result, as plain objects keyed by column. */
function rowsOf(result: string | undefined): Record<string, unknown>[] {
   const parsed = JSON.parse(result ?? "{}") as {
      data?: {
         array_value?: {
            record_value?: { number_value?: number; string_value?: string }[];
         }[];
      };
      schema?: { fields?: { name: string }[] };
   };
   const names = (parsed.schema?.fields ?? []).map((f) => f.name);
   return (parsed.data?.array_value ?? []).map((row) =>
      Object.fromEntries(
         (row.record_value ?? []).map((cell, i) => [
            names[i],
            cell.number_value ?? cell.string_value,
         ]),
      ),
   );
}

describe("Linear .malloy reports through the real server (E2E)", () => {
   let env: PackageEnv;
   let harness: MetricsHarness;

   const pkgUrl = (pkg: string, sub: string) => env.api(sub, pkg);
   const getJson = async <T>(
      url: string,
   ): Promise<{ status: number; body: T }> => {
      const res = await fetch(url);
      return { status: res.status, body: (await res.json()) as T };
   };
   /** A report's cells, off the deprecated `GET …/notebooks/{path}`. */
   const getCells = (pkg: string, report: string) =>
      getJson<{ format?: string; modelInfo?: string; notebookCells?: Cell[] }>(
         pkgUrl(pkg, `/notebooks/${report}`),
      );
   /** A report's tiles, off `GET …/reports/{path}`. */
   const tilesOf = async (pkg: string, report: string) =>
      (
         await getJson<{ tiles?: { kind?: string; query?: string }[] }>(
            pkgUrl(pkg, `/reports/${report}`),
         )
      ).body.tiles ?? [];
   /** Run one tile the way a viewer does: `run: <tile>` through the query route. */
   const runTile = async (
      pkg: string,
      report: string,
      index: number,
      givens?: Record<string, unknown>,
   ): Promise<{ status: number; body: { result?: string } }> => {
      const tile = (await tilesOf(pkg, report))[index];
      if (tile?.query === undefined)
         throw new Error(`${pkg}/${report} has no query tile ${index}`);
      const res = await env.query(
         report,
         { query: `run: ${tile.query}`, givens },
         pkg,
      );
      return {
         status: res.status,
         body: (await res.json()) as { result?: string },
      };
   };
   /** The deprecated per-cell route, by the cell's text. */
   const runCellRoute = async (
      pkg: string,
      report: string,
      text: string,
      params: {
         givens?: Record<string, unknown>;
         filterParams?: Record<string, unknown>;
      } = {},
   ) => {
      const cells = (await getCells(pkg, report)).body.notebookCells ?? [];
      const index = cells.findIndex((cell) => cell.text === text);
      if (index < 0) throw new Error(`${pkg}/${report} has no cell ${text}`);
      const query = new URLSearchParams();
      if (params.givens) query.set("givens", JSON.stringify(params.givens));
      if (params.filterParams)
         query.set("filter_params", JSON.stringify(params.filterParams));
      return getJson<Cell & { message?: string }>(
         pkgUrl(pkg, `/notebooks/${report}/cells/${index}?${query}`),
      );
   };
   const compile = async (
      pkg: string,
      modelPath: string,
      source: string,
      scope: string,
   ) => {
      const res = await env.post(pkgUrl(pkg, `/models/${modelPath}/compile`), {
         source,
         scope,
      });
      expect(res.status).toBe(200);
      return (await res.json()) as { problems: Problem[]; document?: unknown };
   };

   beforeAll(async () => {
      env = await startPackageEnv(
         "malloyyo-notebooks-env",
         { [PLAIN]: fixture(PLAIN), [SURFACE]: fixture(SURFACE) },
         {
            // After the server is imported, so its meter provider is the one
            // replaced, and before the packages load, so their discovery counts.
            beforeCreate: async () => {
               harness = await startMetricsHarness();
               resetNotebookMetricsForTest();
               resetReportMetricsForTest();
               resetAuthorizeGuardTelemetryForTesting();
            },
         },
      );
   }, 300_000);

   afterAll(async () => {
      await env?.stop();
      resetNotebookMetricsForTest();
      resetReportMetricsForTest();
      await harness?.shutdown();
   });

   describe("a file written as `run:` cells", () => {
      const cellFiles = fs
         .readdirSync(path.join(fixture(PLAIN), "notebooks"))
         .map((file) => `notebooks/${file}`)
         .filter((p) => p !== LAYOUT);

      it("is no report: not listed, and its report GET answers 404", async () => {
         expect(cellFiles.length).toBeGreaterThan(5);
         const { body } = await getJson<{ path: string }[]>(
            pkgUrl(PLAIN, "/reports"),
         );
         expect(body.map((r) => r.path)).toEqual([
            "dashboards/text_tiles.malloy",
            LAYOUT,
         ]);
         for (const report of cellFiles) {
            expect({
               report,
               status: (await fetch(pkgUrl(PLAIN, `/reports/${report}`)))
                  .status,
            }).toEqual({ report, status: 404 });
         }
      });

      it("stays the model it is", async () => {
         const { status, body } = await getJson<{ sourceText?: string }>(
            pkgUrl(PLAIN, "/models/notebooks/tagged_runs.malloy"),
         );
         expect(status).toBe(200);
         expect(body.sourceText).toContain("## artifact");
      });

      it("is one package error per file, naming the fix", async () => {
         const { body } = await getJson<{
            warnings?: {
               model?: string;
               code?: string;
               severity?: string;
               message?: string;
            }[];
         }>(pkgUrl(PLAIN, ""));
         const findings = (body.warnings ?? []).filter(
            (w) => w.code === "report-cells-removed",
         );
         expect(findings.map((w) => w.model).sort()).toEqual(
            [...cellFiles].sort(),
         );
         for (const finding of findings) {
            expect(finding.severity).toBe("error");
            expect(finding.message).toEndWith(CELLS_REMOVED);
         }
         expect(
            await harness.collectCounter(
               "publisher_report_lint_findings_total",
               { code: "report-cells-removed", severity: "error" },
            ),
         ).toBeGreaterThanOrEqual(cellFiles.length);
      });

      it("fails a compile check at file and package scope, at the tag's line", async () => {
         const report = "notebooks/revenue_review.malloy";
         const text = fs.readFileSync(
            path.join(fixture(PLAIN), report),
            "utf8",
         );
         const tagLine = text
            .split("\n")
            .findIndex((line) => line.startsWith("## artifact"));
         for (const scope of ["file", "package"]) {
            const { problems } = await compile(PLAIN, report, text, scope);
            const refusal = problems.find(
               (p) => p.code === "report-cells-removed",
            );
            expect(refusal).toMatchObject({
               severity: "error",
               message: `Line ${tagLine + 1}: ${CELLS_REMOVED}`,
            });
            expect(refusal?.at?.range.start.line).toBe(tagLine);
         }
      });

      it("answers text written as cells at append scope with the finding and no document", async () => {
         const { problems, document } = await compile(
            PLAIN,
            "models/orders.malloy",
            "## artifact { layout=linear }\n##(markdown) Prose\nrun: orders -> kpis\n",
            "append",
         );
         expect(document).toBeUndefined();
         expect(
            problems.map(({ code, severity, message }) => ({
               code,
               severity,
               message,
            })),
         ).toEqual([
            {
               code: "report-cells-removed",
               severity: "error",
               message: `Line 1: ${CELLS_REMOVED}`,
            },
         ]);
      });
   });

   describe("a linear report written as tiles", () => {
      it("serves its manifest, and runs a tile over the include's CSV, which resolves from the package root", async () => {
         const tiles = await tilesOf(PLAIN, LAYOUT);
         expect(tiles.map((t) => t.kind)).toEqual(["text", "query", "query"]);
         const { status, body } = await runTile(PLAIN, LAYOUT, 1);
         expect(status).toBe(200);
         expect(rowsOf(body.result)).toEqual([{ order_count: 6 }]);
      });

      // DEPRECATED FEATURE COVERAGE — delete together with
      // the `…/notebooks` routes (see RELEASE_NOTES deprecations).
      describe("deprecated: the …/notebooks routes", () => {
         it("reads it as its definitions, then its tiles", async () => {
            const { status, body } = await getCells(PLAIN, LAYOUT);
            expect(status).toBe(200);
            expect(body.format).toBe("malloy");
            expect(body.notebookCells?.map((cell) => cell.kind)).toEqual([
               "definition",
               "definition",
               "markdown",
               "query",
               "query",
            ]);
         });

         it("answers a definition cell with its text and kind and no result", async () => {
            const { status, body } = await runCellRoute(
               PLAIN,
               LAYOUT,
               'import "../models/orders.malloy"',
            );
            expect(status).toBe(200);
            expect(body).toMatchObject({ type: "code", kind: "definition" });
            expect(body.result).toBeUndefined();
         });

         it("runs a tile cell", async () => {
            const { status, body } = await runCellRoute(
               PLAIN,
               LAYOUT,
               "run: orders_tiles -> headline",
            );
            expect(status).toBe(200);
            expect(rowsOf(body.result)).toEqual([{ order_count: 6 }]);
         });
      });
   });

   describe("a package with a surface", () => {
      it("describes only the named queries over readable sources in the notebook GET's model info", async () => {
         const { status, body } = await getCells(SURFACE, CELLS);
         expect(status).toBe(200);
         expect(body.modelInfo ?? "").not.toContain("secret_total");
         const info = JSON.parse(body.modelInfo ?? "{}") as {
            entries?: { name?: string }[];
         };
         // `hidden_q` reads the hidden source, so only `own_cells_query` is described.
         expect(info.entries?.map((e) => e.name)).toEqual(["own_cells_query"]);
      });

      // DEPRECATED FEATURE COVERAGE — delete together with
      // `.malloynb` notebooks (see RELEASE_NOTES deprecations).
      describe("deprecated: .malloynb notebooks", () => {
         it("applies the cells' surface filter to a .malloynb GET's model info", async () => {
            const columns = async (notebook: string) => {
               const { status, body } = await getCells(SURFACE, notebook);
               expect(status).toBe(200);
               const info = JSON.parse(body.modelInfo ?? "{}") as {
                  anonymous_queries?: {
                     schema?: { fields?: { name: string }[] };
                  }[];
               };
               return {
                  modelInfo: body.modelInfo ?? "",
                  columns: (info.anonymous_queries ?? []).flatMap((q) =>
                     (q.schema?.fields ?? []).map((f) => f.name),
                  ),
               };
            };
            expect(
               (await columns("notebooks/legacy.malloynb")).modelInfo,
            ).not.toContain("secret_total");
            // Positive control: a last cell over a curated source is still described.
            const open = await columns("notebooks/legacy_open.malloynb");
            expect(open.columns).toEqual(["order_count"]);
            expect(open.modelInfo).not.toContain("secret_total");
         });
      });

      it("runs a tile over a curated source", async () => {
         const { status, body } = await runTile(SURFACE, CELLS, 0);
         expect(status).toBe(200);
         expect(rowsOf(body.result)).toEqual([{ order_count: 6 }]);
      });

      it("refuses a tile over an imported source the surface hides, as a .malloynb cell is", async () => {
         expect((await runTile(SURFACE, CELLS, 1)).status).toBe(404);
      });

      it("grafts a row-level gate onto the tile that runs over it", async () => {
         const { status, body } = await runTile(SURFACE, CELLS, 2, {
            GROUPS: [1, 2],
         });
         expect(status).toBe(200);
         expect(rowsOf(body.result)).toEqual([{ n: 2 }]);
      });

      // DEPRECATED FEATURE COVERAGE — delete together with
      // `#(filter)` / `filter_params` (see RELEASE_NOTES deprecations).
      describe("deprecated: #(filter)", () => {
         it("rebuilds a tile cell with the source's #(filter) values", async () => {
            const text = "run: filtered -> { aggregate: n }";
            const unfiltered = await runCellRoute(SURFACE, CELLS, text);
            expect(unfiltered.status).toBe(200);
            expect(rowsOf(unfiltered.body.result)).toEqual([{ n: 6 }]);
            const filtered = await runCellRoute(SURFACE, CELLS, text, {
               filterParams: { region: "EU" },
            });
            expect(filtered.status).toBe(200);
            expect(rowsOf(filtered.body.result)).toEqual([{ n: 3 }]);
         });
      });

      it("binds the request's givens", async () => {
         const { status, body } = await runTile(SURFACE, CELLS, 4, {
            REGION: "EU",
         });
         expect(status).toBe(200);
         expect(rowsOf(body.result)).toEqual([{ order_count: 3 }]);
      });

      it("does not admit the report's own named query by name on the model query route", async () => {
         const res = await env.query(
            CELLS,
            { queryName: "own_cells_query" },
            SURFACE,
         );
         expect(res.status).toBe(404);
      });

      it("refuses a tile naming its own query over a hidden source, and withholds its queryInfo", async () => {
         expect((await runTile(SURFACE, CELLS, 5)).status).toBe(404);
         const cells =
            (await getCells(SURFACE, CELLS)).body.notebookCells ?? [];
         const hidden = cells.find((cell) => cell.text === "run: hidden_q");
         expect(hidden?.kind).toBe("query");
         expect(hidden?.queryInfo).toBeUndefined();
         // Positive control: the tile over a curated source keeps its queryInfo.
         const open = cells.find(
            (cell) => cell.text === "run: orders -> { aggregate: order_count }",
         );
         expect(open?.queryInfo).toBeDefined();
      });

      it("answers 403 on a locked tile's query without the role, and runs it with the role", async () => {
         // Either denial counts: the gate refusing a role it does not admit,
         // or a gate it cannot decide (no ROLES given). Both answer 403.
         const denied = async () =>
            (await harness.collectCounter("publisher_authorize_lock_total", {
               decision: "denied_by_lock",
            })) +
            (await harness.collectCounter("publisher_authorize_lock_total", {
               decision: "denied_unresolvable",
            }));
         const before = await denied();
         expect((await runTile(SURFACE, CELLS, 6)).status).toBe(403);
         expect(
            (await runTile(SURFACE, CELLS, 6, { ROLES: ["viewer"] })).status,
         ).toBe(403);
         const allowed = await runTile(SURFACE, CELLS, 6, {
            ROLES: ["admin"],
         });
         expect(allowed.status).toBe(200);
         expect(rowsOf(allowed.body.result)).toEqual([{ n: 6 }]);
         expect((await denied()) - before).toBe(2);
      });

      // DEPRECATED FEATURE COVERAGE — delete together with
      // the `…/notebooks` routes (see RELEASE_NOTES deprecations).
      describe("deprecated: the …/notebooks routes", () => {
         it("answers 403 on a locked tile cell without the role, and runs it with the role", async () => {
            const text = "run: locked -> { aggregate: n }";
            expect((await runCellRoute(SURFACE, CELLS, text)).status).toBe(403);
            expect(
               (
                  await runCellRoute(SURFACE, CELLS, text, {
                     givens: { ROLES: ["viewer"] },
                  })
               ).status,
            ).toBe(403);
            const allowed = await runCellRoute(SURFACE, CELLS, text, {
               givens: { ROLES: ["admin"] },
            });
            expect(allowed.status).toBe(200);
            expect(rowsOf(allowed.body.result)).toEqual([{ n: 6 }]);
            expect(
               await harness.collectCounter(
                  "publisher_notebook_cell_executions_total",
                  { format: "malloy", kind: "query", outcome: "denied" },
               ),
            ).toBeGreaterThanOrEqual(2);
         });
      });
   });
});
