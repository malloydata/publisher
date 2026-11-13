// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import fs from "fs";
import path from "path";
import vm from "vm";

/**
 * The runtime's "publisher:query" report: evaluates the real publisher.js in a vm context that
 * stands in for a framed page, with `fetch` answering the query and `parent.postMessage`
 * recorded, so what a host receives is read off the wire the runtime actually writes.
 */

const SOURCE = fs.readFileSync(path.join(__dirname, "publisher.js"), "utf8");

type Message = Record<string, unknown>;

function runtime({
   pathname = "/environments/sales-env/packages/sales/index.html",
   framed = true,
   answer = {
      ok: true,
      status: 200,
      body: { result: JSON.stringify([{ region: "EMEA", revenue: 120 }]) },
   },
}: {
   pathname?: string;
   framed?: boolean;
   answer?: { ok: boolean; status: number; body: unknown };
} = {}) {
   const posted: Message[] = [];
   const parent = { postMessage: (message: Message) => posted.push(message) };
   const sandbox: Record<string, unknown> = {
      location: {
         pathname,
         origin: "https://acme.data.example.com",
         search: "",
      },
      fetch: async () => ({
         ok: answer.ok,
         status: answer.status,
         statusText: "",
         json: async () => answer.body,
      }),
      // What the embedded-page branch touches when framed: no layout to measure here.
      document: {
         readyState: "complete",
         body: null,
         documentElement: { scrollHeight: 400 },
      },
      addEventListener: () => {},
      setInterval: () => 0,
      console,
   };
   sandbox.window = sandbox;
   sandbox.self = sandbox;
   sandbox.top = framed ? {} : sandbox;
   sandbox.parent = framed ? parent : sandbox;
   vm.runInContext(SOURCE, vm.createContext(sandbox), {
      filename: "publisher.js",
   });
   const Publisher = (
      sandbox as {
         Publisher: Record<string, (...args: unknown[]) => Promise<unknown>>;
      }
   ).Publisher;
   const reports = () => posted.filter((m) => m.type === "publisher:query");
   return { Publisher, reports };
}

describe("publisher:query reports", () => {
   it("tells the framing page what the page asked and the rows that came back", async () => {
      const { Publisher, reports } = runtime();
      await Publisher.query("sales.malloy", "run: orders -> by_region", {
         givens: { region: "EMEA" },
         filterParams: { quarter: "2026-Q3" },
      });
      expect(reports()).toEqual([
         {
            type: "publisher:query",
            environment: "sales-env",
            package: "sales",
            model: "sales.malloy",
            query: "run: orders -> by_region",
            givens: { region: "EMEA" },
            filterParams: { quarter: "2026-Q3" },
            rowCount: 1,
            rows: [{ region: "EMEA", revenue: 120 }],
         },
      ]);
   });

   it("names a named view instead of a query", async () => {
      const { Publisher, reports } = runtime();
      await Publisher.query("sales.malloy", undefined, {
         sourceName: "orders",
         queryName: "by_region",
      });
      expect(reports()[0]).toMatchObject({
         sourceName: "orders",
         queryName: "by_region",
      });
      expect(reports()[0]).not.toHaveProperty("query");
   });

   it("reports a failed query's error, and still throws it to the page", async () => {
      const { Publisher, reports } = runtime({
         answer: {
            ok: false,
            status: 400,
            body: { message: "unknown field revenue" },
         },
      });
      await expect(
         Publisher.query("sales.malloy", "run: orders -> x", {}),
      ).rejects.toThrow("unknown field revenue");
      expect(reports()).toEqual([
         expect.objectContaining({
            error: "Publisher.query: unknown field revenue",
         }),
      ]);
   });

   it("carries the row count and only the first 20 rows", async () => {
      const many = Array.from({ length: 50 }, (_, i) => ({ i }));
      const { Publisher, reports } = runtime({
         answer: {
            ok: true,
            status: 200,
            body: { result: JSON.stringify(many) },
         },
      });
      await Publisher.query("sales.malloy", "run: x", {});
      expect(reports()[0].rowCount).toBe(50);
      expect(reports()[0].rows).toEqual(many.slice(0, 20));
   });

   it("drops rows until the report fits", async () => {
      const wide = Array.from({ length: 20 }, () => ({
         note: "x".repeat(2000),
      }));
      const { Publisher, reports } = runtime({
         answer: {
            ok: true,
            status: 200,
            body: { result: JSON.stringify(wide) },
         },
      });
      await Publisher.query("sales.malloy", "run: x", {});
      const rows = reports()[0].rows as unknown[];
      expect(rows.length).toBeLessThan(20);
      expect(JSON.stringify(rows).length).toBeLessThanOrEqual(16000);
      expect(reports()[0].rowCount).toBe(20);
   });

   it("posts nothing on a hosted embed surface", async () => {
      const { Publisher, reports } = runtime({
         pathname: "/apps/sales-env/sales/index.html",
      });
      await Publisher.query("sales.malloy", "run: x", {});
      expect(reports()).toEqual([]);
   });

   it("posts nothing when the page is not framed", async () => {
      const { Publisher, reports } = runtime({ framed: false });
      await Publisher.query("sales.malloy", "run: x", {});
      expect(reports()).toEqual([]);
   });
});
