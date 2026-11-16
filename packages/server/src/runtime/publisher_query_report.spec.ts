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

/** The framing page's origin, as a subscribe from it carries. */
const HOST_ORIGIN = "https://host.example.com";

type Message = Record<string, unknown>;
type Answer = { ok: boolean; status: number; body: unknown };

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
   /** The server's answer to every query, or a function of the request body. */
   answer?: Answer | ((body: Message) => Answer | Promise<Answer>);
} = {}) {
   // Every postMessage the page makes, to its parent or to itself, with its target origin.
   const posted: { message: Message; targetOrigin: string }[] = [];
   const record = (message: Message, targetOrigin: string) =>
      posted.push({ message, targetOrigin });
   const parent = { postMessage: record };
   const listeners: Record<string, ((event: unknown) => void)[]> = {};
   const sandbox: Record<string, unknown> = {
      location: {
         pathname,
         origin: "https://acme.data.example.com",
         search: "",
      },
      fetch: async (_url: string, init: { body: string }) => {
         const a =
            typeof answer === "function"
               ? await answer(JSON.parse(init.body))
               : answer;
         return {
            ok: a.ok,
            status: a.status,
            statusText: "",
            json: async () => a.body,
         };
      },
      // What the embedded-page branch touches when framed: no layout to measure here.
      document: {
         readyState: "complete",
         body: null,
         documentElement: { scrollHeight: 400 },
      },
      addEventListener: (type: string, fn: (event: unknown) => void) => {
         (listeners[type] ??= []).push(fn);
      },
      postMessage: record,
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
   const sent = () =>
      posted.filter((p) => p.message.type === "publisher:query");
   const reports = () => sent().map((p) => p.message);
   /** A message arriving at the page, by default the framing page's subscribe. */
   const deliver = ({
      data = { type: "publisher:subscribe" } as unknown,
      source = sandbox.parent,
      origin = HOST_ORIGIN,
   }: { data?: unknown; source?: unknown; origin?: string } = {}) => {
      for (const fn of listeners.message ?? []) fn({ data, source, origin });
   };
   return { Publisher, reports, sent, deliver };
}

describe("publisher:query reports", () => {
   it("tells the framing page what the page asked and the rows that came back", async () => {
      const { Publisher, reports, deliver } = runtime();
      deliver();
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
      const { Publisher, reports, deliver } = runtime();
      deliver();
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
      const { Publisher, reports, deliver } = runtime({
         answer: {
            ok: false,
            status: 400,
            body: { message: "unknown field revenue" },
         },
      });
      deliver();
      await expect(
         Publisher.query("sales.malloy", "run: orders -> x", {}),
      ).rejects.toThrow("unknown field revenue");
      expect(reports()).toEqual([
         expect.objectContaining({
            error: "Publisher.query: unknown field revenue",
         }),
      ]);
   });

   it("reads queryFull's rows from the Malloy result, as plain rows", async () => {
      // The full shape the server sends when compactJson is false: record cells under
      // data.array_value, named by schema.fields, a nested view as an array of records.
      const full = {
         schema: {
            fields: [
               {
                  kind: "dimension",
                  name: "region",
                  type: { kind: "string_type" },
               },
               {
                  kind: "dimension",
                  name: "revenue",
                  type: { kind: "number_type" },
               },
               {
                  kind: "dimension",
                  name: "by_month",
                  type: {
                     kind: "array_type",
                     element_type: {
                        kind: "record_type",
                        fields: [
                           { name: "month", type: { kind: "date_type" } },
                        ],
                     },
                  },
               },
            ],
         },
         data: {
            kind: "array_cell",
            array_value: [
               {
                  kind: "record_cell",
                  record_value: [
                     { kind: "string_cell", string_value: "EMEA" },
                     { kind: "number_cell", number_value: 120 },
                     {
                        kind: "array_cell",
                        array_value: [
                           {
                              kind: "record_cell",
                              record_value: [
                                 {
                                    kind: "date_cell",
                                    date_value: "2026-07-01",
                                 },
                              ],
                           },
                        ],
                     },
                  ],
               },
               {
                  kind: "record_cell",
                  record_value: [
                     { kind: "null_cell" },
                     { kind: "number_cell", number_value: 5 },
                     { kind: "array_cell", array_value: [] },
                  ],
               },
            ],
         },
      };
      const { Publisher, reports, deliver } = runtime({
         answer: {
            ok: true,
            status: 200,
            body: { result: JSON.stringify(full) },
         },
      });
      deliver();
      expect(await Publisher.queryFull("sales.malloy", "run: x", {})).toEqual(
         full,
      );
      expect(reports()[0]).toMatchObject({
         rowCount: 2,
         rows: [
            {
               region: "EMEA",
               revenue: 120,
               by_month: [{ month: "2026-07-01" }],
            },
            { region: null, revenue: 5, by_month: [] },
         ],
      });
   });

   it("carries the row count and only the first 20 rows", async () => {
      const many = Array.from({ length: 50 }, (_, i) => ({ i }));
      const { Publisher, reports, deliver } = runtime({
         answer: {
            ok: true,
            status: 200,
            body: { result: JSON.stringify(many) },
         },
      });
      deliver();
      await Publisher.query("sales.malloy", "run: x", {});
      expect(reports()[0].rowCount).toBe(50);
      expect(reports()[0].rows).toEqual(many.slice(0, 20));
   });

   it("drops rows until the report fits", async () => {
      const wide = Array.from({ length: 20 }, () => ({
         note: "x".repeat(2000),
      }));
      const { Publisher, reports, deliver } = runtime({
         answer: {
            ok: true,
            status: 200,
            body: { result: JSON.stringify(wide) },
         },
      });
      deliver();
      await Publisher.query("sales.malloy", "run: x", {});
      const rows = reports()[0].rows as unknown[];
      expect(rows.length).toBeLessThan(20);
      expect(JSON.stringify(rows).length).toBeLessThanOrEqual(16000);
      expect(reports()[0].rowCount).toBe(20);
   });

   it("posts nothing on a hosted embed surface, even to a subscriber", async () => {
      const { Publisher, sent, deliver } = runtime({
         pathname: "/apps/sales-env/sales/index.html",
      });
      deliver();
      await Publisher.query("sales.malloy", "run: x", {});
      expect(sent()).toEqual([]);
   });

   it("posts nothing when the page is not framed, even to a subscriber", async () => {
      // Not framed, the page is its own parent: a subscribe from it would pass the source check.
      const { Publisher, sent, deliver } = runtime({ framed: false });
      deliver();
      await Publisher.query("sales.malloy", "run: x", {});
      expect(sent()).toEqual([]);
   });
});

describe("publisher:subscribe handshake", () => {
   it("posts nothing until the framing page subscribes", async () => {
      const { Publisher, sent } = runtime();
      await Publisher.query("sales.malloy", "run: x", {});
      expect(sent()).toEqual([]);
   });

   it("keeps the latest 10 reports from before the subscribe and sends them in order", async () => {
      const { Publisher, reports, deliver } = runtime();
      for (let i = 0; i < 12; i++) {
         await Publisher.query("sales.malloy", `run: q${i}`, {});
      }
      expect(reports()).toEqual([]);
      deliver();
      expect(reports().map((r) => r.query)).toEqual(
         Array.from({ length: 10 }, (_, i) => `run: q${i + 2}`),
      );
      await Publisher.query("sales.malloy", "run: after", {});
      expect(reports().map((r) => r.query)).toHaveLength(11);
      expect(reports()[10].query).toBe("run: after");
   });

   it("ignores a subscribe from a window other than its parent", async () => {
      const { Publisher, sent, deliver } = runtime();
      deliver({ source: {} });
      await Publisher.query("sales.malloy", "run: x", {});
      expect(sent()).toEqual([]);
   });

   it("ignores a subscribe from an opaque origin, which it could not address", async () => {
      const { Publisher, sent, deliver } = runtime();
      deliver({ origin: "null" });
      await Publisher.query("sales.malloy", "run: x", {});
      expect(sent()).toEqual([]);
   });

   it("posts every report to the subscriber's origin, never '*'", async () => {
      const { Publisher, sent, deliver } = runtime();
      await Publisher.query("sales.malloy", "run: before", {});
      deliver({ origin: "https://chat.example.com" });
      await Publisher.query("sales.malloy", "run: after", {});
      expect(sent().map((p) => [p.message.query, p.targetOrigin])).toEqual([
         ["run: before", "https://chat.example.com"],
         ["run: after", "https://chat.example.com"],
      ]);
   });
});
