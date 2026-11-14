// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * Classification compatibility for reports (`docs/reports-design.md`,
 * §Compatibility and the Test plan): every file served from `dashboards/` or
 * `notebooks/` before folders stopped meaning anything is classified the same
 * way by its tags alone.
 *
 * `classification_baseline.json` was captured from the server BEFORE the
 * change, over the same packages: the dashboards and notebooks routes, each
 * manifest and notebook body, the model listing, and the package warnings.
 * This test asserts that every report's layout equals its kind then (grid for a
 * dashboard, linear for a notebook), that the deprecated routes answer exactly
 * what they answered (plus the new `layout` field), and that the warnings are
 * the same apart from the new report findings and the two retired folder
 * findings, and apart from the deliberate changes in {@link DELIBERATE}, which
 * are applied to the baseline before comparing.
 *
 * The baseline is as captured except where a fixture moved off a form that
 * was removed rather than deprecated: a fixture dashboard whose only tag was a
 * query-level `# artifact` now carries `## artifact { layout=grid tiles=[…] }`
 * naming that query, so its manifest lists the query as its one tile where it
 * named it as `query`, and the single-query surface refusal reads as that
 * tile's; and the `explores` advice in `dashboards-curated` now names
 * `dashboards/listed.malloy`, which is a model (its query-level tag is not
 * read) and which the advice reads by the same tag reader as discovery.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import {
   fixture,
   STOREFRONT,
   startPackageEnv,
   type PackageEnv,
   type Warning,
} from "../../harness/reports";
import baseline from "./classification_baseline.json";

const PACKAGES: Record<string, string> = {
   ...Object.fromEntries(
      [
         "dashboards-test",
         "dashboards-curated",
         "dashboards-lint",
         "dashboards-convention",
         "notebooks-lint",
         "notebooks-malloyyo",
         "notebooks-malloyyo-surface",
      ].map((name) => [name, fixture(name)]),
   ),
   storefront: STOREFRONT,
};

/** Findings this change adds; any other new warning is a regression. */
const NEW_CODES = new Set([
   "report-kind-removed",
   "report-cells-removed",
   "report-on-surface",
   "index-is-not-a-report",
]);

/** The fixture notebooks written as `run:` cells, the retired format. */
const MALLOYYO_CELL_FILES = [
   "adjacent_blocks",
   "attached_prose",
   "definitions_only",
   "imported_prose",
   "named_runs",
   "prose_lines",
   "refused",
   "revenue_review",
   "structure",
   "tagged_runs",
].map((name) => `notebooks/${name}.malloy`);

/**
 * Files whose classification changed on purpose, per package: each is now a
 * model, listed by `GET …/models`.
 *
 * - `dashboards-curated`: `dashboards/listed.malloy`'s only tag is a
 *   query-level `# artifact`, which is no longer read.
 * - `notebooks-lint`: `dashboards/wrong_kind.malloy`'s tag sets `kind=`, which
 *   is no longer read; it is reported as `report-kind-removed`.
 * - A linear `.malloy` report written as `run:` cells, the retired format, is
 *   no report; it is reported as `report-cells-removed`.
 *
 * `rewritten` files were moved off the cell format onto tiles, so they are
 * still linear reports but their cells, listing and findings are new: they
 * are left out of every comparison but their layout.
 *
 * `removed` files were deleted with the form they tested: the
 * `dashboard_columns` alias, and a custom-component `.jsx` that was only
 * there to be warned about. They are left out of every comparison.
 */
const DELIBERATE: Record<
   string,
   { becameModels: string[]; rewritten?: string[]; removed?: string[] }
> = {
   "dashboards-curated": { becameModels: ["dashboards/listed.malloy"] },
   "dashboards-lint": {
      becameModels: [],
      removed: ["dashboards/orphan.jsx"],
   },
   "notebooks-lint": {
      removed: [
         "dashboards/columns_alias.malloy",
         "dashboards/columns_conflict.malloy",
      ],
      becameModels: [
         "dashboards/wrong_kind.malloy",
         "notebooks/header_statement.malloy",
      ],
      rewritten: ["notebooks/clean.malloy", "notebooks/linty.malloy"],
   },
   "notebooks-malloyyo": { becameModels: MALLOYYO_CELL_FILES },
   "notebooks-malloyyo-surface": {
      becameModels: [],
      rewritten: ["notebooks/cells.malloy", "notebooks/local.malloy"],
   },
   storefront: { becameModels: [], rewritten: ["category-review.malloy"] },
};

/** Files left out of the comparison of everything but their layout. */
const rewrittenIn = (pkg: string) => new Set(DELIBERATE[pkg]?.rewritten ?? []);

/** The baseline with the {@link DELIBERATE} changes applied. */
function expectedFor(pkg: string, was: Baseline[string]): Baseline[string] {
   const removed = new Set(DELIBERATE[pkg]?.removed ?? []);
   const moved = new Set([
      ...(DELIBERATE[pkg]?.becameModels ?? []),
      ...rewrittenIn(pkg),
      ...removed,
   ]);
   if (moved.size === 0) return was;
   return {
      ...was,
      models: was.models.filter((m) => !removed.has(m.path as string)),
      dashboards: was.dashboards.filter((d) => !moved.has(d.path as string)),
      manifests: Object.fromEntries(
         Object.entries(was.manifests).filter(
            ([, m]) => !moved.has(m.path as string),
         ),
      ),
      notebooks: was.notebooks.filter((n) => !moved.has(n.path as string)),
      notebookBodies: Object.fromEntries(
         Object.entries(was.notebookBodies).filter(([p]) => !moved.has(p)),
      ),
      // A finding about the file as a report no longer applies to it.
      warnings: was.warnings.filter((w) => !moved.has(w.model ?? "")),
   };
}

/** Findings this change retires: they told an author to move a file to its kind's folder. */
const RETIRED = [
   /where the other kind is created/, // notebook-other-folder
   /artifact tag has no `kind`/, // notebook-kind-missing
];

type Baseline = Record<
   string,
   {
      dashboards: Record<string, unknown>[];
      manifests: Record<string, Record<string, unknown>>;
      notebooks: Record<string, unknown>[];
      notebookBodies: Record<
         string,
         { dashboard: Record<string, unknown> | null; notebookCells: unknown[] }
      >;
      warnings: Warning[];
      models: Record<string, unknown>[];
   }
>;

/** The rewording that came with the change, applied to a baseline message. */
function reworded(message: string): string {
   return message
      .replace(
         "which is not a dashboard in this package.",
         "which is not a report in this package.",
      )
      .replace(
         "Convert it to a .malloy notebook under notebooks/.",
         "Convert it to a .malloy report.",
      );
}

const key = (w: Warning) =>
   `${w.model ?? ""}|${w.severity ?? ""}|${w.message ?? ""}`;

const without = <T extends Record<string, unknown>>(
   value: T,
   ...keys: string[]
): Record<string, unknown> => {
   const copy: Record<string, unknown> = { ...value };
   for (const k of keys) delete copy[k];
   return copy;
};

/**
 * The baseline was captured from an LF checkout. A Windows checkout gives the
 * fixtures CRLF line endings, and cell and prose text is the file's own text,
 * so every `\r` is dropped before comparing: the legacy `.malloynb` reader
 * splits on `\n` alone and leaves a stray `\r` at both ends of a cell.
 */
const lf = (value: unknown): unknown =>
   typeof value === "string"
      ? value.replace(/\r/g, "")
      : Array.isArray(value)
        ? value.map(lf)
        : value && typeof value === "object"
          ? Object.fromEntries(
               Object.entries(value).map(([k, v]) => [k, lf(v)]),
            )
          : value;

describe("Report classification compatibility (E2E)", () => {
   let env: PackageEnv;
   const expected = baseline as unknown as Baseline;
   const api = (pkg: string, sub = "") => env.api(sub, pkg);
   const get = <T>(url: string) => env.getJson<T>(url);

   beforeAll(async () => {
      env = await startPackageEnv("reports-compat-env", PACKAGES);
   }, 300_000);

   afterAll(() => env?.stop());

   for (const pkg of Object.keys(PACKAGES)) {
      describe(pkg, () => {
         it("gives every report the layout its kind had", async () => {
            const was = expectedFor(pkg, expected[pkg]);
            const reports = await get<{ path: string; layout: string }[]>(
               api(pkg, "/reports"),
            );
            const layouts = Object.fromEntries(
               reports.map((r) => [r.path, r.layout]),
            );
            const before: Record<string, string> = {};
            for (const d of was.dashboards) before[d.path as string] = "grid";
            for (const n of was.notebooks) before[n.path as string] = "linear";
            for (const p of rewrittenIn(pkg)) before[p] = "linear";
            expect(layouts).toEqual(before);
         });

         it("answers the deprecated routes as before", async () => {
            const was = expectedFor(pkg, expected[pkg]);
            const dashboards = await get<Record<string, unknown>[]>(
               api(pkg, "/dashboards"),
            );
            expect(lf(dashboards.map((d) => without(d, "resource")))).toEqual(
               was.dashboards,
            );
            for (const [name, manifest] of Object.entries(was.manifests)) {
               const now = await get<Record<string, unknown>>(
                  api(pkg, `/dashboards/${encodeURIComponent(name)}`),
               );
               expect(now.layout).toBe("grid");
               expect(lf(without(now, "resource", "layout"))).toEqual(manifest);
            }
            const notebooks = await get<Record<string, unknown>[]>(
               api(pkg, "/notebooks"),
            );
            const rewritten = rewrittenIn(pkg);
            expect(
               lf(
                  notebooks
                     .filter((n) => !rewritten.has(n.path as string))
                     .map((n) => without(n, "environmentName")),
               ),
            ).toEqual(was.notebooks);
            for (const [notebookPath, body] of Object.entries(
               was.notebookBodies,
            )) {
               // Raw: a notebook whose cells the reader refused answered 424
               // before too, and the baseline holds that body.
               const now = (await (
                  await fetch(api(pkg, `/notebooks/${notebookPath}`))
               ).json()) as {
                  dashboard?: Record<string, unknown>;
                  notebookCells?: Record<string, unknown>[];
               };
               if (body.dashboard) {
                  expect(now.dashboard?.layout).toBe("linear");
               }
               expect(
                  lf(
                     now.dashboard
                        ? without(now.dashboard, "resource", "layout")
                        : null,
                  ),
               ).toEqual(body.dashboard);
               expect(
                  lf(
                     (now.notebookCells ?? []).map((cell) =>
                        without(cell, "newSources", "queryInfo"),
                     ),
                  ),
               ).toEqual(body.notebookCells);
            }
            const models = await get<Record<string, unknown>[]>(
               api(pkg, "/models"),
            );
            const moved = new Set(DELIBERATE[pkg]?.becameModels ?? []);
            // A file that became a model is listed as one now.
            for (const p of moved)
               expect(models.map((m) => m.path)).toContain(p);
            expect(
               lf(
                  models
                     .filter((m) => !moved.has(m.path as string))
                     .map((m) => without(m, "environmentName")),
               ),
            ).toEqual(was.models);
         });

         it("keeps every finding but the retired ones, and adds only report findings", async () => {
            const was = expectedFor(pkg, expected[pkg]);
            const now = await env.warnings(pkg);
            const nowKeys = new Set(now.map(key));
            const kept = was.warnings
               .filter((w) => !RETIRED.some((re) => re.test(w.message ?? "")))
               .map((w) => ({ ...w, message: reworded(w.message ?? "") }));
            for (const w of kept) {
               expect(nowKeys.has(key(w)) ? key(w) : `MISSING ${key(w)}`).toBe(
                  key(w),
               );
            }
            const keptKeys = new Set(kept.map(key));
            const rewritten = rewrittenIn(pkg);
            const added = now.filter(
               (w) => !keptKeys.has(key(w)) && !rewritten.has(w.model ?? ""),
            );
            for (const w of added) {
               expect(
                  NEW_CODES.has(w.code ?? "") ? "new" : `UNEXPECTED ${key(w)}`,
               ).toBe("new");
            }
         });
      });
   }
});
