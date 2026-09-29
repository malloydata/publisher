// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * A served notebook (`notebooks/*.malloy` with a model-level `## artifact`
 * note) on a package that declares a surface. It is a model to every endpoint
 * that takes a model path, but it admits nothing of its own: what its model GET
 * publishes and what a query against it may read are held to the surface, the
 * same as a dashboard's.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import path from "path";
import { fileURLToPath } from "url";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const ENV_NAME = "served-notebook-env";
const PACKAGE_NAME = "notebooks-malloyyo-surface";
const NOTEBOOK = "notebooks/local.malloy";

const fixtureDir = path.resolve(
   __dirname,
   "../../fixtures/notebooks-malloyyo-surface",
);

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
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;

   const pkgUrl = (sub: string) =>
      `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${PACKAGE_NAME}${sub}`;
   const post = (sub: string, body: object) =>
      fetch(pkgUrl(sub), {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify(body),
      });

   beforeAll(async () => {
      env = await startRestE2E();
      baseUrl = env.baseUrl;
      const res = await fetch(`${baseUrl}/api/v0/environments`, {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            name: ENV_NAME,
            packages: [{ name: PACKAGE_NAME, location: fixtureDir }],
            connections: [],
         }),
      });
      if (!res.ok) {
         throw new Error(
            `Failed to create test environment (${res.status}): ${await res.text()}`,
         );
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
      await env?.stop();
      env = null;
   });

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

   it("still returns the declared givens and the file text", async () => {
      const body = await getModel();
      expect(body.givens?.map((g) => g.name)).toEqual(["REGION"]);
      // The text names the hidden-derived `loc`; a notebook's text is returned
      // anyway, as a dashboard's is.
      expect(body.sourceText).toContain("source: loc is hidden extend {}");
   });

   it("does not list the notebook as a model, and lists it as a notebook", async () => {
      const models = (await (await fetch(pkgUrl("/models"))).json()) as {
         path?: string;
      }[];
      expect(models.map((m) => m.path)).not.toContain(NOTEBOOK);

      const notebooks = (await (await fetch(pkgUrl("/notebooks"))).json()) as {
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
