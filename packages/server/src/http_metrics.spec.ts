// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   afterAll,
   afterEach,
   beforeAll,
   beforeEach,
   describe,
   expect,
   it,
} from "bun:test";
import express from "express";
import type { AddressInfo } from "net";
import type { Server } from "http";
import { httpMetricsMiddleware } from "./http_metrics";
import { resetInstrumentsForTesting } from "./telemetry";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "./test_helpers/metrics_harness";

describe("httpMetricsMiddleware", () => {
   let server: Server;
   let base: string;
   let harness: MetricsHarness;

   beforeAll(async () => {
      const app = express();
      app.use(httpMetricsMiddleware);
      app.get("/api/v0/things/:id", (_req, res) => {
         res.json({ ok: true });
      });
      app.get("/health", (_req, res) => {
         res.send("ok");
      });
      server = app.listen(0, "127.0.0.1");
      await new Promise((resolve) => server.once("listening", resolve));
      base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
   });
   afterAll(() => {
      server.close();
   });
   beforeEach(async () => {
      harness = await startMetricsHarness();
      resetInstrumentsForTesting();
   });
   afterEach(async () => {
      await harness.shutdown();
      resetInstrumentsForTesting();
   });

   /** Fetch each path and wait for its `finish` handler to have run. */
   async function get(...paths: string[]): Promise<void> {
      for (const path of paths) await (await fetch(base + path)).text();
      await new Promise((resolve) => setTimeout(resolve, 10));
   }

   it("labels a matched request by its route pattern, not its path", async () => {
      await get("/api/v0/things/1", "/api/v0/things/2");
      const labels = {
         "http.method": "GET",
         "http.route": "/api/v0/things/:id",
         "http.status_code": 200,
      };
      expect(
         await harness.collectCounter("http_server_requests_total", labels),
      ).toBe(2);
      expect(
         await harness.collectHistogram(
            "http_server_request_duration_ms",
            labels,
         ),
      ).toMatchObject({ count: 2 });
   });

   it("leaves probes and scrapes out", async () => {
      await get("/health");
      expect(await harness.collectCounter("http_server_requests_total")).toBe(
         0,
      );
   });
});
