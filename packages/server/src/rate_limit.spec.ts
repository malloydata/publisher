// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import express from "express";
import request from "supertest";
import { rateLimitMiddleware } from "./rate_limit";

function appWith(perMinute: number) {
   const app = express();
   app.use(rateLimitMiddleware(perMinute));
   app.get("/health", (_req, res) => res.send("ok"));
   app.get("/metrics", (_req, res) => res.send("ok"));
   app.get("/thing", (_req, res) => res.send("ok"));
   return app;
}

describe("rateLimitMiddleware", () => {
   it("passes everything through when off", async () => {
      const app = appWith(0);
      for (let i = 0; i < 5; i++) {
         expect((await request(app).get("/thing")).status).toBe(200);
      }
   });

   it("returns 429 on the request after the limit, with standard headers", async () => {
      const app = appWith(2);
      expect((await request(app).get("/thing")).status).toBe(200);
      expect((await request(app).get("/thing")).status).toBe(200);
      const third = await request(app).get("/thing");
      expect(third.status).toBe(429);
      expect(third.headers["ratelimit-policy"]).toContain("2;w=60");
      expect(third.body.code).toBe(429);
      expect(third.body.message).toContain("2 requests per minute");
   });

   it("never limits probes or metrics", async () => {
      const app = appWith(1);
      for (let i = 0; i < 3; i++) {
         expect((await request(app).get("/health")).status).toBe(200);
         expect((await request(app).get("/metrics")).status).toBe(200);
      }
   });
});
