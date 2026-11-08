// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A REST query body is untyped JSON. A `query`, `sourceName` or `queryName`
 * that is not a string is the caller's mistake: it answers 400 naming the
 * field, on the query route and on its legacy `/projects` alias, rather than
 * the 500 it answered when the value reached the model. MCP types these
 * through its schema.
 */

import { describe, expect, it } from "bun:test";
import sinon from "sinon";
import request from "supertest";
import { QueryController } from "../controller/query.controller";
import { QuerySlots } from "../query_concurrency";
import type { PackageLookup } from "../service/environment_ports";
import { API_PREFIX, buildRouteApp } from "../test_helpers/route_app";
import { fakeOf } from "../test_helpers/store_fakes";
import { QueryUseCase } from "../usecases/query";

const getEnvironment = sinon.stub().rejects(new Error("not reached"));
const querySlots = new QuerySlots();
const app = buildRouteApp({
   querySlots,
   queryController: new QueryController(
      new QueryUseCase(fakeOf<PackageLookup>({ getEnvironment }), querySlots),
   ),
});

const ROUTES = [
   `${API_PREFIX}/environments/env/packages/pkg/models/m.malloy/query`,
   `${API_PREFIX}/projects/env/packages/pkg/models/m.malloy/query`,
];

describe("a non-string query field answers 400", () => {
   for (const route of ROUTES) {
      it.each([
         ["query", { query: { text: "run: s -> { group_by: a }" } }],
         ["query", { query: 42 }],
         ["sourceName", { sourceName: ["s"], queryName: "q" }],
         ["queryName", { queryName: { name: "q" } }],
      ])(`POST ${route} with a non-string %s`, async (field, body) => {
         const res = await request(app).post(route).send(body);
         expect(res.status).toBe(400);
         expect(res.body.message).toBe(`${field} must be a string`);
         expect(getEnvironment.called).toBe(false);
      });
   }
});
