// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The exact 400 bodies the routes answer for a malformed parameter, before any
 * controller runs. Most are `{ code, message }`; one older route keeps a
 * shape of its own, pinned here so a shared parser cannot quietly change it.
 */

import { describe, expect, it, spyOn } from "bun:test";
import request from "supertest";
import { logger } from "../logger";
import { linesMentioning } from "../test_helpers/log_lines";
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
      ["GET", PROJECT, "reload"],
      ["GET", LEGACY_PKG, "reload"],
   ])("%s %s?%s=1", async (method, path, name) => {
      const res = await request(app)
         [method.toLowerCase() as "get"](`${path}?${name}=1`)
         .send({});
      expect(res.status).toBe(400);
      expect(res.body).toEqual(invalidBoolean(name, "1", method, path));
   });
});

describe("dropTables=true on a materialization delete answers { code, message }", () => {
   it.each([`${PKG}/materializations/m1`, `${LEGACY_PKG}/materializations/m1`])(
      "DELETE %s?dropTables=true",
      async (path) => {
         const res = await request(app).delete(`${path}?dropTables=true`);
         expect(res.status).toBe(400);
         expect(res.body).toEqual({
            code: 400,
            message:
               "dropTables=true is no longer supported: deleting a " +
               "materialization is record-only and never drops a table. " +
               "Delete without dropTables, and drop any table you want gone " +
               "yourself through its connection, after checking that no " +
               "remaining run, of any version, names a table with that name in " +
               "any place.",
         });
      },
   );
});

describe("a malformed dropTables answers a fix that omits it", () => {
   it.each([
      [`${PKG}/materializations/m1`, "1"],
      [`${LEGACY_PKG}/materializations/m1`, "yes"],
   ])("DELETE %s?dropTables=%s", async (path, value) => {
      const res = await request(app).delete(`${path}?dropTables=${value}`);
      expect(res.status).toBe(400);
      expect(res.body).toEqual({
         code: 400,
         message:
            `Invalid dropTables value "${value}": a materialization delete ` +
            `is record-only and takes no dropTables. Fix: DELETE ${path} ` +
            `(omit dropTables, or send dropTables=false).`,
      });
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

it("a malformed param is logged once as a refusal", async () => {
   const warn = spyOn(logger, "warn");
   try {
      await request(app).get(
         `${API_PREFIX}/environments/refusal-marker?reload=1`,
      );
      expect(linesMentioning(warn, "refusal-marker")).toEqual([
         [
            "Request refused",
            expect.objectContaining({
               path: `${API_PREFIX}/environments/refusal-marker`,
               status: 400,
            }),
         ],
      ]);
   } finally {
      warn.mockRestore();
   }
});
