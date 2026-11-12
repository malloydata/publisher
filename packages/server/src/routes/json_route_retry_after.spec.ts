// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A mapped error's response headers reach the HTTP response, not only the
 * mapped error: the build-plan refresh's 503 carries Retry-After, which a host
 * reads to back off (ms2data/service#6145).
 */

import { describe, expect, it } from "bun:test";
import express from "express";
import request from "supertest";
import { BuildPlanRefreshingError } from "../errors";
import { apiRoute } from "./json_route";

describe("apiRoute sends a mapped error's headers", () => {
   it("answers BuildPlanRefreshingError with 503, its reason and Retry-After", async () => {
      const app = express();
      app.get(
         "/plan",
         apiRoute(async () => {
            throw new BuildPlanRefreshingError("re-deriving");
         }),
      );
      const res = await request(app).get("/plan");
      expect(res.status).toBe(503);
      expect(res.headers["retry-after"]).toBe("5");
      expect(res.body).toMatchObject({
         code: 503,
         reason: "BUILD_PLAN_REFRESHING",
      });
   });

   it("sends no Retry-After for an error whose mapping has no headers", async () => {
      const app = express();
      app.get(
         "/plan",
         apiRoute(async () => {
            throw new Error("boom");
         }),
      );
      const res = await request(app).get("/plan");
      expect(res.status).toBe(500);
      expect(res.headers["retry-after"]).toBeUndefined();
   });
});
