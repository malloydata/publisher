// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * A linear report (`notebooks/local.malloy`, written as tiles) on a package
 * that declares a surface. It is a model to every endpoint
 * that takes a model path, but it admits nothing of its own: what its model GET
 * publishes and what a query against it may read are held to the surface, the
 * same as a dashboard's.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import {
   fixture,
   startPackageEnv,
   type PackageEnv,
} from "../../harness/reports";

const PACKAGE_NAME = "notebooks-malloyyo-surface";
const NOTEBOOK = "notebooks/local.malloy";

interface Named {
   name?: string;
}

interface ModelBody {
   sources?: Named[];
   queries?: Named[];
   sourceInfos?: string[];
   modelInfo?: string;
   modelDef?: string;
   givens?: Named[];
   sourceText?: string;
}

describe("Served notebook on a package with a surface (E2E)", () => {
   let env: PackageEnv;
   const pkgUrl = (sub: string) => env.api(sub);
   const post = (sub: string, body: object) => env.post(pkgUrl(sub), body);

   beforeAll(async () => {
      env = await startPackageEnv("served-notebook-env", {
         [PACKAGE_NAME]: fixture(PACKAGE_NAME),
      });
   }, 300_000);

   afterAll(() => env?.stop());

   const getModel = async (): Promise<ModelBody> => {
      const res = await fetch(pkgUrl(`/models/${NOTEBOOK}`));
      expect(res.status).toBe(200);
      return (await res.json()) as ModelBody;
   };

   it("publishes only surface-readable names on the model GET", async () => {
      const body = await getModel();
      const infoEntries = (
         JSON.parse(body.modelInfo ?? "{}") as { entries?: Named[] }
      ).entries;
      const def = JSON.parse(body.modelDef ?? "{}") as {
         contents?: Record<string, unknown>;
         sourceRegistry?: Record<string, unknown>;
      };

      // Derived from a source the surface does not export, so it is hidden from
      // every list that carries a schema.
      expect(body.sources?.map((s) => s.name)).toEqual(["on_surface"]);
      expect(body.sourceInfos?.join("")).not.toContain('"loc"');
      expect(infoEntries?.map((e) => e.name)).not.toContain("loc");
      expect(Object.keys(def.contents ?? {})).not.toContain("loc");
      expect(Object.keys(def.sourceRegistry ?? {}).join(" ")).not.toContain(
         "loc@",
      );
      expect(Object.keys(def.contents ?? {})).toContain("on_surface");
   });

   const runColumns = (modelInfo: string | undefined) =>
      (
         (
            JSON.parse(modelInfo ?? "{}") as {
               anonymous_queries?: { schema?: { fields?: Named[] } }[];
            }
         ).anonymous_queries ?? []
      ).map((run) => (run.schema?.fields ?? []).map((field) => field.name));

   it("describes on the model GET only the runs over published names", async () => {
      const notebook = await getModel();
      // A report written as tiles has no runs of its own to describe.
      expect(runColumns(notebook.modelInfo)).toEqual([]);
      expect(notebook.modelInfo).not.toContain("secret_total");

      for (const file of ["notebooks/cells.malloy", "dashboards/runs.malloy"]) {
         const res = await fetch(pkgUrl(`/models/${file}`));
         expect(res.status).toBe(200);
         const body = (await res.json()) as ModelBody;
         expect(body.modelInfo).not.toContain("secret_total");
         expect(runColumns(body.modelInfo).flat()).not.toContain("amount");
      }
      const dashboard = (await (
         await fetch(pkgUrl("/models/dashboards/runs.malloy"))
      ).json()) as ModelBody;
      // Positive control: the dashboard's run over its surface-derived source.
      expect(runColumns(dashboard.modelInfo)).toEqual([["shown_count"]]);
   });

   it("withholds from the deprecated notebook GET a tile over a source derived from a hidden one", async () => {
      const res = await fetch(pkgUrl(`/notebooks/${NOTEBOOK}`));
      expect(res.status).toBe(200);
      const body = (await res.json()) as {
         modelInfo?: string;
         notebookCells?: { text?: string; queryInfo?: string }[];
      };
      expect(body.modelInfo).not.toContain("secret_total");
      const infoOf = (text: string) =>
         body.notebookCells?.find((cell) => cell.text === text)?.queryInfo;
      expect(infoOf("run: loc -> { aggregate: secret_total }")).toBeUndefined();
      // Positive control: the tiles over the surface and over a source derived from it.
      expect(infoOf("run: orders -> { aggregate: order_count }")).toContain(
         "order_count",
      );
      expect(infoOf("run: on_surface -> { aggregate: n }")).toBeDefined();
   });

   it("serves the report's tiles, and runs one over a source derived from the surface", async () => {
      const report = (await (
         await fetch(pkgUrl(`/reports/${NOTEBOOK}`))
      ).json()) as { tiles?: { kind?: string; query?: string }[] };
      expect(report.tiles?.map((t) => t.kind)).toEqual([
         "text",
         "query",
         "query",
         "text",
         "query",
      ]);
      const res = await post(`/models/${NOTEBOOK}/query`, {
         query: `run: ${report.tiles?.[2].query}`,
      });
      expect(res.status).toBe(200);
   });

   it("still returns the declared givens, and withholds file text that names a hidden-derived source", async () => {
      const body = await getModel();
      expect(body.givens?.map((g) => g.name)).toEqual(["REGION"]);
      // The text names the hidden-derived `loc`, so the model GET withholds it as it would any model's.
      expect(body.sourceText).toBeUndefined();
   });

   it("does not list the notebook as a model, and lists it as a report", async () => {
      const models = (await (await fetch(pkgUrl("/models"))).json()) as {
         path?: string;
      }[];
      // The listing does carry the surface's own model, so the absence below is not an empty list.
      expect(models.map((m) => m.path)).toContain("index.malloy");
      expect(models.map((m) => m.path)).not.toContain(NOTEBOOK);

      const notebooks = (await (await fetch(pkgUrl("/reports"))).json()) as {
         path?: string;
         title?: string;
         description?: string;
         error?: string;
      }[];
      const listed = notebooks.find((nb) => nb.path === NOTEBOOK);
      expect(listed).toEqual(
         expect.objectContaining({
            title: "Surface probe",
            description:
               "Reads the surface, and declares a source over a file the surface hides.",
         }),
      );
      expect(listed?.error).toBeUndefined();
   });

   it("runs a query over a source derived from the surface", async () => {
      const res = await post(`/models/${NOTEBOOK}/query`, {
         query: "run: on_surface -> { aggregate: c is count() }",
      });
      expect(res.status).toBe(200);
   });

   it("refuses a query over its own source derived from a hidden file", async () => {
      const res = await post(`/models/${NOTEBOOK}/query`, {
         query: "run: loc -> { aggregate: c is count() }",
      });
      expect(res.status).toBe(404);
   });

   it("admits none of its own named queries by name", async () => {
      const res = await post(`/models/${NOTEBOOK}/query`, {
         queryName: "own_query",
      });
      expect(res.status).toBe(404);
   });
});
