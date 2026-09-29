// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * Served notebooks (`notebooks/*.malloy` with a model-level `## artifact` note)
 * through the real server: the notebook GET, running cells, the reader's
 * refusal, and, on a package with a surface, the same governed cell path a
 * `.malloynb` runs through.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { resetNotebookMetricsForTest } from "../../../src/notebook_metrics";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "../../../src/test_helpers/metrics_harness";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const ENV_NAME = "malloyyo-notebooks-env";
const PLAIN = "notebooks-malloyyo";
const SURFACE = "notebooks-malloyyo-surface";
const REFUSED = "notebooks/refused.malloy";
const CELLS = "notebooks/cells.malloy";

const fixture = (name: string) =>
   path.resolve(__dirname, `../../fixtures/${name}`);

interface Cell {
   type?: string;
   kind?: string;
   text?: string;
   queryInfo?: string;
   result?: string;
}

/** The rows of a cell result, as plain objects keyed by column. */
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

/** Every served fixture notebook and the kinds of its cells, in order. */
const KINDS: Record<string, string[]> = {
   "notebooks/revenue_review.malloy": [
      "definition",
      "markdown",
      "definition",
      "markdown",
      "query",
   ],
   "notebooks/definitions_only.malloy": [
      "definition",
      "definition",
      "definition",
   ],
   "notebooks/adjacent_blocks.malloy": [
      "definition",
      "markdown",
      "markdown",
      "query",
   ],
   "notebooks/imported_prose.malloy": ["definition", "markdown", "query"],
   "notebooks/prose_lines.malloy": [
      "definition",
      "markdown",
      "markdown",
      "query",
   ],
   "notebooks/tagged_runs.malloy": ["definition", "query", "query", "markdown"],
   "notebooks/named_runs.malloy": [
      "definition",
      "definition",
      "query",
      "query",
      "query",
   ],
   "notebooks/structure.malloy": [
      "definition",
      "markdown",
      "definition",
      "definition",
      "query",
   ],
};

describe("Malloyyo notebooks served through the real server (E2E)", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let harness: MetricsHarness;
   let baseUrl: string;

   const pkgUrl = (pkg: string, sub: string) =>
      `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${pkg}${sub}`;
   const getJson = async <T>(
      url: string,
   ): Promise<{ status: number; body: T }> => {
      const res = await fetch(url);
      return { status: res.status, body: (await res.json()) as T };
   };
   const runCell = (
      pkg: string,
      notebook: string,
      index: number,
      params: {
         givens?: Record<string, unknown>;
         filterParams?: Record<string, unknown>;
      } = {},
   ) => {
      const query = new URLSearchParams();
      if (params.givens) query.set("givens", JSON.stringify(params.givens));
      if (params.filterParams)
         query.set("filter_params", JSON.stringify(params.filterParams));
      return getJson<Cell & { message?: string }>(
         pkgUrl(pkg, `/notebooks/${notebook}/cells/${index}?${query}`),
      );
   };

   beforeAll(async () => {
      env = await startRestE2E();
      baseUrl = env.baseUrl;
      // After the server is imported, so its meter provider is the one replaced.
      harness = await startMetricsHarness();
      resetNotebookMetricsForTest();
      const res = await fetch(`${baseUrl}/api/v0/environments`, {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            name: ENV_NAME,
            packages: [
               { name: PLAIN, location: fixture(PLAIN) },
               { name: SURFACE, location: fixture(SURFACE) },
            ],
            connections: [],
         }),
      });
      if (!res.ok) {
         throw new Error(
            `Failed to create test environment (${res.status}): ${await res.text()}`,
         );
      }
      // Loading is lazy: touch both packages so discovery has run before any assertion.
      for (const pkg of [PLAIN, SURFACE]) {
         const loaded = await fetch(pkgUrl(pkg, ""));
         if (!loaded.ok) {
            throw new Error(`${pkg} did not load (${loaded.status})`);
         }
      }
   });

   afterAll(async () => {
      if (baseUrl) {
         try {
            await fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}`, {
               method: "DELETE",
            });
         } catch {
            // best-effort
         }
      }
      resetNotebookMetricsForTest();
      await harness?.shutdown();
      await env?.stop();
      env = null;
   });

   describe("a package with no surface", () => {
      it("has a kinds expectation for every served fixture notebook", () => {
         const onDisk = fs
            .readdirSync(path.join(fixture(PLAIN), "notebooks"))
            .map((file) => `notebooks/${file}`)
            .filter((p) => p !== REFUSED);
         expect(onDisk.sort()).toEqual(Object.keys(KINDS).sort());
      });

      for (const [notebook, kinds] of Object.entries(KINDS)) {
         it(`serves ${notebook} as a malloy-format notebook with its cells`, async () => {
            const { status, body } = await getJson<{
               format?: string;
               notebookCells?: Cell[];
            }>(pkgUrl(PLAIN, `/notebooks/${notebook}`));
            expect(status).toBe(200);
            expect(body.format).toBe("malloy");
            expect(body.notebookCells?.map((cell) => cell.kind)).toEqual(kinds);
            expect(
               body.notebookCells?.map((cell) =>
                  cell.kind === "markdown" ? "markdown" : "code",
               ),
            ).toEqual(body.notebookCells?.map((cell) => cell.type));
         });
      }

      it("runs a query cell over the include's CSV, which resolves from the package root", async () => {
         const { status, body } = await runCell(
            PLAIN,
            "notebooks/revenue_review.malloy",
            4,
            { givens: { REGION: "US" } },
         );
         expect(status).toBe(200);
         expect(body.kind).toBe("query");
         expect(rowsOf(body.result).map((row) => row.total_amount)).toEqual([
            50, 200, 100,
         ]);
      });

      it("runs a cell over the notebook's own named query", async () => {
         const { status, body } = await runCell(
            PLAIN,
            "notebooks/named_runs.malloy",
            2,
         );
         expect(status).toBe(200);
         expect(rowsOf(body.result)).toEqual([
            { order_count: 6, total_amount: 1550 },
         ]);
      });

      it("answers a definition cell with its text and kind and no result", async () => {
         const { status, body } = await runCell(
            PLAIN,
            "notebooks/named_runs.malloy",
            1,
         );
         expect(status).toBe(200);
         expect(body).toMatchObject({
            type: "code",
            kind: "definition",
            text: "query: q is orders -> kpis",
         });
         expect(body.result).toBeUndefined();
      });

      describe("a notebook the reader refuses", () => {
         it("lists with the error, naming the line", async () => {
            const { body } = await getJson<{ path: string; error?: string }[]>(
               pkgUrl(PLAIN, "/notebooks"),
            );
            expect(body.find((nb) => nb.path === REFUSED)?.error).toContain(
               "Line 7:",
            );
         });

         it("fails the notebook GET and a cell run as a notebook that did not compile does", async () => {
            expect(
               (await getJson(pkgUrl(PLAIN, `/notebooks/${REFUSED}`))).status,
            ).toBe(424);
            expect((await runCell(PLAIN, REFUSED, 0)).status).toBe(424);
         });

         it("is a package warning of severity error", async () => {
            const { body } = await getJson<{
               warnings?: { model?: string; severity?: string }[];
            }>(pkgUrl(PLAIN, ""));
            expect(body.warnings).toContainEqual(
               expect.objectContaining({ model: REFUSED, severity: "error" }),
            );
         });

         it("fails an agent's compile check with an error problem at the line", async () => {
            const text = fs.readFileSync(
               path.join(fixture(PLAIN), REFUSED),
               "utf8",
            );
            for (const scope of ["file", "package"]) {
               const res = await fetch(
                  pkgUrl(PLAIN, `/models/${REFUSED}/compile`),
                  {
                     method: "POST",
                     headers: { "Content-Type": "application/json" },
                     body: JSON.stringify({ source: text, scope }),
                  },
               );
               expect(res.status).toBe(200);
               const { problems } = (await res.json()) as {
                  problems: {
                     severity: string;
                     message: string;
                     model?: string;
                     at?: { range: { start: { line: number } } };
                  }[];
               };
               const refusal = problems.find(
                  (p) =>
                     p.severity === "error" && p.message.includes("Line 7:"),
               );
               expect(refusal?.at?.range.start.line).toBe(6);
               expect(refusal?.model).toBe(REFUSED);
            }
         });

         it("compiles a readable notebook with no reader problem", async () => {
            const notebook = "notebooks/revenue_review.malloy";
            const text = fs.readFileSync(
               path.join(fixture(PLAIN), notebook),
               "utf8",
            );
            const res = await fetch(
               pkgUrl(PLAIN, `/models/${notebook}/compile`),
               {
                  method: "POST",
                  headers: { "Content-Type": "application/json" },
                  body: JSON.stringify({ source: text, scope: "file" }),
               },
            );
            expect(res.status).toBe(200);
            const { problems } = (await res.json()) as {
               problems: { severity: string }[];
            };
            expect(problems.filter((p) => p.severity === "error")).toEqual([]);
         });

         it("is counted at discovery as refused", async () => {
            expect(
               await harness.collectCounter(
                  "publisher_notebook_discovery_total",
                  { format: "malloy", outcome: "refused" },
               ),
            ).toBeGreaterThan(0);
         });
      });
   });

   describe("a package with a surface", () => {
      /** The column names the notebook GET's model info describes for its runs. */
      const describedRuns = async (notebook: string) => {
         const { status, body } = await getJson<{ modelInfo?: string }>(
            pkgUrl(SURFACE, `/notebooks/${notebook}`),
         );
         expect(status).toBe(200);
         const info = JSON.parse(body.modelInfo ?? "{}") as {
            anonymous_queries?: { schema?: { fields?: { name: string }[] } }[];
         };
         return {
            modelInfo: body.modelInfo ?? "",
            columns: (info.anonymous_queries ?? []).flatMap((q) =>
               (q.schema?.fields ?? []).map((f) => f.name),
            ),
         };
      };

      it("applies the cells' surface filter to a served notebook GET's model info", async () => {
         const { modelInfo, columns } = await describedRuns(CELLS);
         // Positive control: a run over a curated source is still described.
         expect(columns).toContain("order_count");
         expect(modelInfo).not.toContain("secret_total");
      });

      it("applies the cells' surface filter to a .malloynb GET's model info", async () => {
         expect(
            (await describedRuns("notebooks/legacy.malloynb")).modelInfo,
         ).not.toContain("secret_total");
         // Positive control: a last cell over a curated source is still described.
         const open = await describedRuns("notebooks/legacy_open.malloynb");
         expect(open.columns).toEqual(["order_count"]);
         expect(open.modelInfo).not.toContain("secret_total");
      });

      it("runs a cell over a curated source", async () => {
         const { status, body } = await runCell(SURFACE, CELLS, 5);
         expect(status).toBe(200);
         expect(rowsOf(body.result)).toEqual([{ order_count: 6 }]);
      });

      it("refuses a cell over an imported source the surface hides, as a .malloynb cell is", async () => {
         expect((await runCell(SURFACE, CELLS, 6)).status).toBe(404);
      });

      it("grafts a row-level gate onto the cell that runs over it", async () => {
         const { status, body } = await runCell(SURFACE, CELLS, 7, {
            givens: { GROUPS: [1, 2] },
         });
         expect(status).toBe(200);
         expect(rowsOf(body.result)).toEqual([{ n: 2 }]);
      });

      it("rebuilds a cell with the source's #(filter) values", async () => {
         const unfiltered = await runCell(SURFACE, CELLS, 8);
         expect(unfiltered.status).toBe(200);
         expect(rowsOf(unfiltered.body.result)).toEqual([{ n: 6 }]);
         const filtered = await runCell(SURFACE, CELLS, 8, {
            filterParams: { region: "EU" },
         });
         expect(filtered.status).toBe(200);
         expect(rowsOf(filtered.body.result)).toEqual([{ n: 3 }]);
      });

      it("binds the request's givens", async () => {
         const { status, body } = await runCell(SURFACE, CELLS, 9, {
            givens: { REGION: "EU" },
         });
         expect(status).toBe(200);
         expect(rowsOf(body.result)).toEqual([{ order_count: 3 }]);
      });

      it("does not admit the notebook's own named query by name on the model query route", async () => {
         const res = await fetch(pkgUrl(SURFACE, `/models/${CELLS}/query`), {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ queryName: "own_cells_query" }),
         });
         expect(res.status).toBe(404);
      });
   });
});
