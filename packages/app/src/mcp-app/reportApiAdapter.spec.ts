// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import axios, { AxiosError, CanceledError } from "axios";
import {
   reportApiAdapter,
   toReportApiCall,
   type ReportApiCall,
} from "./reportApiAdapter";

const BASE = "https://publisher.example.com/api/v0";

describe("toReportApiCall", () => {
   it("reads /status", () => {
      expect(toReportApiCall("get", `${BASE}/status`, undefined)).toEqual({
         operation: "status",
      });
   });

   it("reads a report listing, with its version", () => {
      expect(
         toReportApiCall(
            "get",
            "/environments/my%20env/packages/store/reports?versionId=1.2.0",
            undefined,
         ),
      ).toEqual({
         operation: "listReports",
         environmentName: "my env",
         packageName: "store",
         versionId: "1.2.0",
      });
   });

   it("reads a report whose path the client encoded as one segment", () => {
      expect(
         toReportApiCall(
            "get",
            `${BASE}/environments/examples/packages/storefront/reports/sales%2Fq%233.malloy`,
            undefined,
         ),
      ).toEqual({
         operation: "getReport",
         environmentName: "examples",
         packageName: "storefront",
         path: "sales/q#3.malloy",
      });
   });

   it("reads a query, with its body", () => {
      expect(
         toReportApiCall(
            "post",
            `${BASE}/environments/examples/packages/storefront/models/sales%2Fq3.malloy/query`,
            JSON.stringify({
               query: "run: x -> y",
               givens: { REGION: "West" },
            }),
         ),
      ).toEqual({
         operation: "query",
         environmentName: "examples",
         packageName: "storefront",
         path: "sales/q3.malloy",
         body: { query: "run: x -> y", givens: { REGION: "West" } },
      });
   });

   it("does not recognise anything else", () => {
      for (const [method, url] of [
         ["put", `${BASE}/environments/e/packages/p/models/x.malloy`],
         ["post", `${BASE}/environments/e/packages/p/models/x.malloy/compile`],
         ["delete", `${BASE}/environments/e/packages/p/reports/x.malloy`],
         ["get", `${BASE}/environments/e/packages/p/connections`],
         ["get", `${BASE}/environments`],
         ["post", `${BASE}/status`],
      ]) {
         expect(toReportApiCall(method, url, undefined)).toBeUndefined();
      }
   });
});

describe("reportApiAdapter", () => {
   function client(
      answer: (call: ReportApiCall) => {
         status: number;
         data: unknown;
      },
   ) {
      const calls: ReportApiCall[] = [];
      const instance = axios.create({
         baseURL: BASE,
         adapter: reportApiAdapter(async (call) => {
            calls.push(call);
            return answer(call);
         }),
      });
      return { instance, calls };
   }

   it("answers a request with the tool's data", async () => {
      const { instance, calls } = client(() => ({
         status: 200,
         data: { title: "Q3" },
      }));
      const response = await instance.get(
         "/environments/examples/packages/storefront/reports/q3.malloy",
      );
      expect(response.status).toBe(200);
      expect(response.data).toEqual({ title: "Q3" });
      expect(calls[0].operation).toBe("getReport");
   });

   it("sends a JSON body on as an object", async () => {
      const { instance, calls } = client(() => ({ status: 200, data: {} }));
      await instance.post(
         "/environments/examples/packages/storefront/models/q3.malloy/query",
         { queryName: "kpis", givens: { YEAR: "2025" } },
      );
      expect(calls[0].body).toEqual({
         queryName: "kpis",
         givens: { YEAR: "2025" },
      });
   });

   it("rejects a refused request as axios would, with the server's message", async () => {
      const { instance } = client(() => ({
         status: 403,
         data: { message: "Not authorized for orders" },
      }));
      const error = await instance
         .post("/environments/e/packages/p/models/m.malloy/query", {})
         .catch((e) => e);
      expect(error).toBeInstanceOf(AxiosError);
      expect(error.response.status).toBe(403);
      expect(error.response.data).toEqual({
         message: "Not authorized for orders",
      });
      expect(error.message).toBe("Not authorized for orders");
   });

   it("passes the request's abort signal to the tool call, and cancels as axios would", async () => {
      const seen: (AbortSignal | undefined)[] = [];
      const controller = new AbortController();
      const instance = axios.create({
         baseURL: BASE,
         adapter: reportApiAdapter(async (_call, options) => {
            seen.push(options?.signal);
            controller.abort();
            return { status: 200, data: {} };
         }),
      });
      const error = await instance
         .post(
            "/environments/e/packages/p/models/m.malloy/query",
            {},
            { signal: controller.signal },
         )
         .catch((e) => e);
      expect(seen).toEqual([controller.signal]);
      expect(error).toBeInstanceOf(CanceledError);
   });

   it("never sends a request the panel does not make", async () => {
      const { instance, calls } = client(() => ({ status: 200, data: {} }));
      const error = await instance
         .put("/environments/e/packages/p/models/m.malloy", { source: "x" })
         .catch((e) => e);
      expect(calls).toHaveLength(0);
      expect(error.response.status).toBe(404);
   });
});
