// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The exact 400 bodies the routes answer for a malformed parameter, before any
 * controller runs. Most are `{ code, message }`; two deprecated or older
 * routes keep a shape of their own, pinned here so a shared parser cannot
 * quietly change them.
 */

import { describe, expect, it } from "bun:test";
import request from "supertest";
import { API_PREFIX, buildRouteApp } from "../test_helpers/route_app";

const app = buildRouteApp();
const ENV = `${API_PREFIX}/environments/env`;
const PKG = `${ENV}/packages/pkg`;
const PROJECT = `${API_PREFIX}/projects/env`;
const LEGACY_PKG = `${PROJECT}/packages/pkg`;

const invalidBoolean = (
   name: string,
   value: string,
   method: string,
   path: string,
) => ({
   code: 400,
   message:
      `Invalid ${name} value ${JSON.stringify(value)}: expected "true" or ` +
      `"false". Fix: ${method} ${path}?${name}=true.`,
});

const collectionReload = (route: string) => ({
   code: 400,
   message:
      "Reload recompiles one named resource, and this endpoint lists them. " +
      `Use GET ${route}?reload=true instead.`,
});

describe("a malformed boolean query param answers { code, message }", () => {
   it.each([
      ["GET", ENV, "reload"],
      ["GET", PKG, "reload"],
      ["GET", `${PKG}/models`, "includeHiddenFilesAndSources"],
      ["GET", `${PKG}/models/m.malloy`, "includeHiddenFilesAndSources"],
      ["GET", `${PKG}/entities`, "includeHiddenFilesAndSources"],
      ["POST", `${ENV}/packages`, "dryRun"],
      ["DELETE", `${PKG}/materializations/m1`, "dropTables"],
      ["GET", PROJECT, "reload"],
      ["GET", LEGACY_PKG, "reload"],
      ["DELETE", `${LEGACY_PKG}/materializations/m1`, "dropTables"],
   ])("%s %s?%s=1", async (method, path, name) => {
      const res = await request(app)
         [method.toLowerCase() as "get"](`${path}?${name}=1`)
         .send({});
      expect(res.status).toBe(400);
      expect(res.body).toEqual(invalidBoolean(name, "1", method, path));
   });
});

describe("reload on a collection answers { code, message }", () => {
   it.each([
      [
         `${API_PREFIX}/environments`,
         `${API_PREFIX}/environments/{environmentName}`,
      ],
      [`${ENV}/packages`, `${ENV}/packages/{packageName}`],
      [`${API_PREFIX}/projects`, `${API_PREFIX}/projects/{projectName}`],
      [`${PROJECT}/packages`, `${PROJECT}/packages/{packageName}`],
   ])("GET %s?reload=true", async (path, route) => {
      const res = await request(app).get(`${path}?reload=true`);
      expect(res.status).toBe(400);
      expect(res.body).toEqual(collectionReload(route));
   });
});

it("givens that are not one JSON object answer { code, message }", async () => {
   const res = await request(app).get(
      `${PKG}/models/m.malloy?givens=${encodeURIComponent("[1]")}`,
   );
   expect(res.status).toBe(400);
   expect(res.body).toEqual({
      code: 400,
      message: "givens must be one JSON object",
   });
});

it("an agent's bad skills value answers { message } without a code", async () => {
   const res = await request(app).get(`${PKG}/agents/a?skills=some`);
   expect(res.status).toBe(400);
   expect(res.body).toEqual({
      message: `'skills' must be 'full' or 'summary', got "some".`,
   });
});

describe("a notebook cell refusal answers { error }", () => {
   const cell = (pkg: string, index: string, query = "") =>
      `${pkg}/notebooks/n.malloynb/cells/${index}${query}`;

   it.each([PKG, LEGACY_PKG])(
      "%s: a cell index that is not a number",
      async (pkg) => {
         const res = await request(app).get(cell(pkg, "abc"));
         expect(res.status).toBe(400);
         expect(res.body).toEqual({ error: "Invalid cell index" });
      },
   );

   it.each([PKG, LEGACY_PKG])(
      "%s: filter_params that are not JSON",
      async (pkg) => {
         const res = await request(app).get(
            cell(pkg, "0", "?filter_params=%7B"),
         );
         expect(res.status).toBe(400);
         expect(res.body).toEqual({
            error: "Invalid filter_params: must be valid JSON",
         });
      },
   );

   it("givens that are not JSON", async () => {
      const res = await request(app).get(cell(PKG, "0", "?givens=%7B"));
      expect(res.status).toBe(400);
      expect(res.body).toEqual({ error: "Invalid givens: must be valid JSON" });
   });

   it.each([PKG, LEGACY_PKG])(
      "%s: a bad bypass_filters keeps { code, message }",
      async (pkg) => {
         const path = cell(pkg, "0");
         const res = await request(app).get(`${path}?bypass_filters=maybe`);
         expect(res.status).toBe(400);
         expect(res.body).toEqual(
            invalidBoolean("bypass_filters", "maybe", "GET", path),
         );
      },
   );
});
