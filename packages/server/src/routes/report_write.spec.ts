// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it, spyOn } from "bun:test";
import express from "express";
import request from "supertest";
import type { ReportController } from "../controller/report.controller";
import {
   ServiceUnavailableError,
   WriteConflictError,
   WriteRolledBackError,
} from "../errors";
import { logger } from "../logger";
import { linesMentioning } from "../test_helpers/log_lines";
import { reportWriteRoute } from "./report_write";

/**
 * The report save route logs each failed write once: a refusal at `warn`, a
 * fault at `error` — by the error mapping when it logs that class, by the
 * route otherwise — and a rollback nowhere, since the write logged it where
 * it happened.
 */
describe("reportWriteRoute", () => {
   const app = (fail: Error) => {
      const controller = {
         putReportSource: async () => {
            throw fail;
         },
      } as unknown as ReportController;
      const server = express();
      server.use(express.json());
      server.put(
         "/api/v0/environments/:environmentName/packages/:packageName/models/*?",
         reportWriteRoute(controller),
      );
      return server;
   };
   const put = (fail: Error, file: string) =>
      request(app(fail))
         .put(`/api/v0/environments/env/packages/pkg/models/${file}`)
         .send({ source: "## artifact { layout=grid }\n" });

   it("answers a write with its status, 201 when the file is new", async () => {
      const controller = {
         putReportSource: async () => ({ created: true }),
      } as unknown as ReportController;
      const server = express();
      server.use(express.json());
      server.put(
         "/api/v0/environments/:environmentName/packages/:packageName/models/*?",
         reportWriteRoute(controller),
      );
      const res = await request(server)
         .put("/api/v0/environments/env/packages/pkg/models/new.malloy")
         .send({ source: "x" });
      expect(res.status).toBe(201);
   });

   it("logs a 5xx once, at error, whether or not the mapping logs that class", async () => {
      const error = spyOn(logger, "error");
      try {
         // 503: a class the mapping answers without logging.
         const busy = await put(
            new ServiceUnavailableError("busy-write-503-marker"),
            "busy.malloy",
         );
         expect(busy.status).toBe(503);
         // 500: a class the mapping logs itself.
         const fault = await put(
            new Error("fault-write-500-marker"),
            "fault.malloy",
         );
         expect(fault.status).toBe(500);

         expect(
            linesMentioning(error, "busy-write-503-marker").map(
               ([text]) => text,
            ),
         ).toEqual(["Request failed"]);
         expect(linesMentioning(error, "fault-write-500-marker")).toHaveLength(
            1,
         );
      } finally {
         error.mockRestore();
      }
   });

   it("logs a refusal once, at warn", async () => {
      const warn = spyOn(logger, "warn");
      const error = spyOn(logger, "error");
      try {
         const res = await put(
            new WriteConflictError("stale-write-409-marker"),
            "stale.malloy",
         );
         expect(res.status).toBe(409);
         expect(linesMentioning(warn, "stale-write-409-marker")).toEqual([
            [
               "Report write refused",
               {
                  packageName: "pkg",
                  model: "stale.malloy",
                  status: 409,
                  error: "stale-write-409-marker",
               },
            ],
         ]);
         expect(linesMentioning(error, "stale-write-409-marker")).toEqual([]);
      } finally {
         warn.mockRestore();
         error.mockRestore();
      }
   });

   it("leaves a rollback to the write, which logged it where it happened", async () => {
      const warn = spyOn(logger, "warn");
      const error = spyOn(logger, "error");
      try {
         const res = await put(
            new WriteRolledBackError("rolled-write-500-marker"),
            "rolled.malloy",
         );
         expect(res.status).toBe(500);
         expect(linesMentioning(warn, "rolled-write-500-marker")).toEqual([]);
         expect(linesMentioning(error, "rolled-write-500-marker")).toEqual([]);
      } finally {
         warn.mockRestore();
         error.mockRestore();
      }
   });
});
