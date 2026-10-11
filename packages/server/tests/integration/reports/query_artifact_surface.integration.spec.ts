// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * The deprecated query-level `# artifact` never pulls a model off the curated
 * surface (`docs/reports-design.md` §1, "The query-level artifact form"): a
 * file whose only artifact tag is on a query is a report only when the root
 * `index.malloy` does not import it, directly or through another file, and
 * `explores` does not list it. On the surface, the tag is ignored with a
 * `query-artifact-ignored` warning and the file stays a model, whether or not
 * it declares sources of its own. A model-level `## artifact` still always
 * makes a report (`report-on-surface`).
 *
 * Over real HTTP against `tests/fixtures/query-artifact-surface`.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import path from "path";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const FIXTURE = path.resolve(
   __dirname,
   "../../fixtures/query-artifact-surface",
);
const ENV_NAME = "query-artifact-surface-env";
const PKG = "query-artifact-surface";

interface Warning {
   model?: string;
   subject?: string;
   message?: string;
   severity?: string;
   code?: string;
}

describe("Query-level # artifact and the curated surface (E2E)", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;
   const api = (sub = "") =>
      `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${PKG}${sub}`;
   const getJson = async <T>(url: string): Promise<T> => {
      const res = await fetch(url);
      expect(res.status).toBe(200);
      return (await res.json()) as T;
   };
   let warnings: Warning[] = [];
   const byCode = (code: string) =>
      warnings
         .filter((w) => w.code === code)
         .sort((a, b) => (a.model ?? "").localeCompare(b.model ?? ""));

   beforeAll(async () => {
      env = await startRestE2E();
      baseUrl = env.baseUrl;
      const res = await fetch(`${baseUrl}/api/v0/environments`, {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            name: ENV_NAME,
            packages: [{ name: PKG, location: FIXTURE }],
            connections: [],
         }),
      });
      if (!res.ok) {
         throw new Error(
            `Failed to create test environment (${res.status}): ${await res.text()}`,
         );
      }
      warnings =
         (await getJson<{ warnings?: Warning[] }>(api())).warnings ?? [];
   }, 300_000);

   afterAll(async () => {
      if (!env) return;
      await fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}`, {
         method: "DELETE",
      });
      await env.stop();
   });

   it("serves a query-level report only off the surface; a model-level one always", async () => {
      const reports = await getJson<{ path: string; layout: string }[]>(
         api("/reports"),
      );
      expect(
         Object.fromEntries(reports.map((r) => [r.path, r.layout])),
      ).toEqual({
         "off_surface.malloy": "grid",
         "models/model_level.malloy": "grid",
      });
   });

   it("keeps an on-surface file with a query-level tag a model", async () => {
      const models = await getJson<{ path: string }[]>(api("/models"));
      const paths = models.map((m) => m.path);
      expect(paths).toContain("listed.malloy");
      expect(paths).toContain("queries_only.malloy");
      expect(paths).not.toContain("off_surface.malloy");
      expect(paths).not.toContain("models/model_level.malloy");
   });

   it("keeps publishing the sources of the on-surface files", async () => {
      for (const [model, query] of [
         ["index.malloy", "run: direct_src -> v"],
         ["index.malloy", "run: mid_src -> v"],
         ["listed.malloy", "run: listed_src -> v"],
      ]) {
         const res = await fetch(api(`/models/${model}/query`), {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ query, compactJson: true }),
         });
         expect({ model, query, status: res.status }).toEqual({
            model,
            query,
            status: 200,
         });
      }
   });

   it("says the tag is ignored on the surface, naming what puts the file there", () => {
      const ignored = byCode("query-artifact-ignored");
      expect(ignored.map((w) => [w.model, w.subject, w.severity])).toEqual([
         ["listed.malloy", "listed_overview", "warn"],
         ["models/deep.malloy", "deep_overview", "warn"],
         ["models/direct.malloy", "direct_overview", "warn"],
         ["queries_only.malloy", "queries_overview", "warn"],
      ]);
      const message = (model: string) =>
         ignored.find((w) => w.model === model)?.message ?? "";
      expect(message("models/direct.malloy")).toContain(
         "index.malloy imports it",
      );
      expect(message("models/deep.malloy")).toContain(
         "models/mid.malloy imports it",
      );
      expect(message("listed.malloy")).toContain('"explores" lists it');
      expect(message("queries_only.malloy")).toContain('"explores" lists it');
      for (const w of ignored) {
         expect(w.message).toContain("stays a model");
         expect(w.message).toContain("## artifact { layout=grid");
         expect(w.message).toContain("its own file");
      }
   });

   it("keeps the deprecation warning for the off-surface report, and report-on-surface for the model-level one", () => {
      expect(
         byCode("query-artifact-deprecated").map((w) => [w.model, w.subject]),
      ).toEqual([["off_surface.malloy", "off_overview"]]);
      expect(byCode("report-on-surface").map((w) => w.model)).toEqual([
         "models/model_level.malloy",
      ]);
   });
});
