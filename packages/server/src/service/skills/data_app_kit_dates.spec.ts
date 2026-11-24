// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * The data app kit's date math (skills/malloy-html-data-apps/assets/data-app/kit/period.js and state.js): which
 * days a period and its comparison cover, and which bucket each row lands in. The bucket rule is pinned against
 * DuckDB with SQL written by hand to compute what the kit's Malloy expression, `days(<field>.day to @<last day>)`,
 * does, so a row's bucket here is the one the server hands back.
 */
import { DuckDBInstance } from "@duckdb/node-api";
import { describe, expect, it } from "bun:test";
import path from "path";

const KIT = path.resolve(
   import.meta.dir,
   "../../../../../skills/malloy-html-data-apps/assets/data-app/kit",
);
const P = await import(path.join(KIT, "period.js"));
const S = await import(path.join(KIT, "state.js"));

const range = (from: string, to: string) => ({ from, to });
const lastDay = (r: { to: string }) => P.addDays(r.to, -1);

function configure(span = { first: "2024-01-01", last: "2026-11-16" }) {
   S.configure({
      source: "s",
      model: "m.malloy",
      date: { field: "d" },
      filters: [{ key: "user", field: "user_id", label: "User" }],
      entities: { user: { field: "user_id", label: "User" } },
      metrics: { n: {} },
      pages: [{ id: "overview" }, { id: "user", entity: "user" }],
   });
   S.setKnown({ span, user: [1, 2, 3] });
}

describe("comparison periods", () => {
   it("compares month to date through March 30 with all of February", () => {
      const cur = P.resolve(
         { preset: "mtd" },
         { first: "2024-01-01", last: "2026-03-30" },
      );
      expect(cur).toEqual(range("2026-03-01", "2026-03-31"));
      const prev = P.compareRange({ preset: "mtd" }, cur, "previous", {
         first: "2024-01-01",
      });
      expect(prev).toEqual(range("2026-02-01", "2026-03-01"));
      expect(lastDay(prev)).toBe("2026-02-28");
   });

   it("keeps the last day of a short month a quarter back", () => {
      const cur = range("2026-04-01", "2026-05-31"); // quarter to date through May 30
      const prev = P.compareRange({ preset: "qtd" }, cur, "previous", {
         first: "2024-01-01",
      });
      expect(prev).toEqual(range("2026-01-01", "2026-03-01"));
   });

   it("lines a calendar period up with the same dates a year earlier, a leap day included", () => {
      const cur = range("2024-02-01", "2024-03-01"); // February 2024, 29 days
      const prev = P.compareRange({ preset: "mtd" }, cur, "year", {
         first: "2020-01-01",
      });
      expect(prev).toEqual(range("2023-02-01", "2023-03-01"));
   });

   it("compares a period counted in days with the same number of days before it, and 364 days back for a year", () => {
      const cur = range("2026-08-18", "2026-11-17");
      expect(
         P.compareRange({ preset: "last_91" }, cur, "previous", {
            first: "2024-01-01",
         }),
      ).toEqual(range("2026-05-19", "2026-08-18"));
      expect(
         P.compareRange({ preset: "last_91" }, cur, "year", {
            first: "2024-01-01",
         }),
      ).toEqual(range("2025-08-19", "2025-11-18"));
   });

   it("compares a full month, quarter and year with the whole one before it", () => {
      const span = { first: "2020-01-01" };
      // Last full month in May 2024 is April (30 days); the month before is all 31 days of March.
      expect(
         P.compareRange(
            { preset: "last_month" },
            range("2024-04-01", "2024-05-01"),
            "previous",
            span,
         ),
      ).toEqual(range("2024-03-01", "2024-04-01"));
      expect(
         P.compareRange(
            { preset: "last_quarter" },
            range("2024-04-01", "2024-07-01"),
            "previous",
            span,
         ),
      ).toEqual(range("2024-01-01", "2024-04-01"));
      expect(
         P.compareRange(
            { preset: "last_month" },
            range("2024-02-01", "2024-03-01"),
            "year",
            span,
         ),
      ).toEqual(range("2023-02-01", "2023-03-01"));
   });

   it("gives no comparison that would start before the data does", () => {
      expect(
         P.compareRange(
            { preset: "last_91" },
            range("2024-01-10", "2024-04-10"),
            "previous",
            { first: "2024-01-01" },
         ),
      ).toBe(null);
   });
});

describe("rolling buckets, against DuckDB", () => {
   it("numbers the period's last day 0 and makes every week of a 91-day period whole", async () => {
      configure();
      const r = range("2026-08-18", "2026-11-17");
      // The Malloy the kit writes, and the SQL it compiles to.
      expect(S.rollingKey("d", r, "week")).toBe(
         "k is floor(days(d.day to @2026-11-16) / 7)",
      );
      const db = await (await DuckDBInstance.create(":memory:")).connect();
      const reader = await db.runAndReadAll(`
         select floor(date_diff('day', d, date '${lastDay(r)}') / 7)::int as k, count(*)::int as n
         from range(date '${r.from}', date '${r.to}', interval 1 day) t(d)
         group by 1 order by 1 desc`);
      const rows = reader.getRowObjectsJS() as { k: number; n: number }[];
      const weeks = S.buckets(rows, {
         period: { preset: "custom", from: r.from, to: lastDay(r) },
         compare: "none",
         grain: "week",
         filters: {},
      });
      expect(weeks.length).toBe(13);
      expect(
         weeks.every(
            (w: { n: number; partial: boolean }) => w.n === 7 && !w.partial,
         ),
      ).toBe(true);
      expect(weeks.at(-1)).toMatchObject({
         k: 0,
         start: "2026-11-10",
         end: "2026-11-17",
      });
   });

   it("labels a day bucket with its own day", async () => {
      configure();
      const r = range("2026-11-10", "2026-11-17");
      const db = await (await DuckDBInstance.create(":memory:")).connect();
      const reader = await db.runAndReadAll(`
         select date_diff('day', d, date '${lastDay(r)}')::int as k, strftime(d, '%Y-%m-%d') as day
         from range(date '${r.from}', date '${r.to}', interval 1 day) t(d)`);
      const rows = reader.getRowObjectsJS() as { k: number; day: string }[];
      const days = S.buckets(rows, {
         period: { preset: "custom", from: r.from, to: lastDay(r) },
         compare: "none",
         grain: "day",
         filters: {},
      });
      for (const b of days) expect(b.start).toBe(b.day);
   });
});

describe("every bucket of the range", () => {
   it("keeps a missing week as an empty bucket in its place", () => {
      configure();
      const s = {
         period: { preset: "custom", from: "2026-10-20", to: "2026-11-16" },
         compare: "none",
         grain: "week",
         filters: {},
      };
      const out = S.buckets(
         [
            { k: 0, n: 5 },
            { k: 2, n: 3 },
         ],
         s,
      );
      expect(out.map((b: { k: number }) => b.k)).toEqual([3, 2, 1, 0]);
      expect(
         out.map((b: { n?: number; empty?: boolean }) =>
            b.empty ? null : b.n,
         ),
      ).toEqual([null, 3, null, 5]);
   });

   it("matches calendar buckets on their start date, a missing month a gap", () => {
      configure();
      const s = {
         period: { preset: "custom", from: "2026-01-15", to: "2026-04-10" },
         compare: "none",
         grain: "month",
         filters: {},
      };
      const out = S.buckets(
         [
            { bucket: "2026-01-01", n: 1 },
            { bucket: "2026-04-01", n: 4 },
         ],
         s,
      );
      expect(out.map((b: { start: string }) => b.start)).toEqual([
         "2026-01-15",
         "2026-02-01",
         "2026-03-01",
         "2026-04-01",
      ]);
      expect(out.map((b: { n?: number }) => b.n ?? null)).toEqual([
         1,
         null,
         null,
         4,
      ]);
      expect(out[0].partial).toBe(true);
   });
});

describe("reading an address", () => {
   it("refuses prototype names and impossible dates", () => {
      configure();
      const s = S.fromHash("#/?p=__proto__&c=constructor&m=constructor");
      expect(s.period).toEqual({ preset: S.DEFAULT().period.preset });
      expect(s.compare).toBe(S.DEFAULT().compare);
      expect(s.metric).toBe(S.DEFAULT().metric);
      expect(
         S.fromHash("#/?p=custom&from=2024-13-01&to=2024-14-05").period.preset,
      ).not.toBe("custom");
      expect(
         S.fromHash("#/?p=custom&from=2024-02-30&to=2024-03-05").period.preset,
      ).not.toBe("custom");
      expect(
         S.fromHash("#/?p=custom&from=2024-02-28&to=2024-03-05").period,
      ).toEqual({ preset: "custom", from: "2024-02-28", to: "2024-03-05" });
   });

   it("keeps an unloaded numeric id a number", () => {
      configure();
      expect(S.fromHash("#/user/2001").id).toBe(2001);
      expect(S.fromHash("#/user/2").id).toBe(2);
      expect(S.fromHash("#/user/9007199254740993").id).toBe("9007199254740993");
   });

   it("writes control characters in a literal as escapes", () => {
      expect(S.lit("a\nb'c\\d\u0001")).toBe("'a\\nb\\'c\\\\d\\u0001'");
   });
});

describe("periods that end with their month", () => {
   it("compares a month to date a year back with the same days, a leap day's month keeping its last day", () => {
      expect(
         P.compareRange(
            { preset: "mtd" },
            range("2024-02-01", "2024-02-29"),
            "year",
            {
               first: "2020-01-01",
            },
         ),
      ).toEqual(range("2023-02-01", "2023-03-01"));
   });

   it("compares a quarter or month to date through its last day with whole months", () => {
      const span = { first: "2020-01-01" };
      const qtd = P.resolve(
         { preset: "qtd" },
         { first: "2020-01-01", last: "2026-06-30" },
      );
      expect(P.compareRange({ preset: "qtd" }, qtd, "previous", span)).toEqual(
         range("2026-01-01", "2026-04-01"),
      );
      const mtd = P.resolve(
         { preset: "mtd" },
         { first: "2020-01-01", last: "2026-04-30" },
      );
      expect(P.compareRange({ preset: "mtd" }, mtd, "previous", span)).toEqual(
         range("2026-03-01", "2026-04-01"),
      );
   });

   it("starts a quarter on its first month", () => {
      expect(
         ["2026-01-15", "2026-05-15", "2026-09-30", "2026-12-31"].map((d) =>
            P.bucketStart(d, "quarter"),
         ),
      ).toEqual(["2026-01-01", "2026-04-01", "2026-07-01", "2026-10-01"]);
   });
});

describe("shares of a whole", () => {
   it("are said only when the parts add up to the whole and none is below zero", () => {
      expect(S.adds({ n: 3, s: 100, t: 100, lo: 10 })).toBe(true);
      expect(S.adds({ n: 3, s: 100, t: 100, lo: -30 })).toBe(false);
      expect(S.adds({ n: 3, s: 130, t: 100, lo: 10 })).toBe(false);
   });
});

describe("calendar buckets, through Malloy", () => {
   it("keeps a month bucket on its first day under a model timezone east of UTC", async () => {
      const { SingleConnectionRuntime } = await import("@malloydata/malloy");
      const { DuckDBConnection } = await import("@malloydata/db-duckdb");
      const runtime = new SingleConnectionRuntime({
         connection: new DuckDBConnection("duckdb", ":memory:"),
      });
      const model = `source: t is duckdb.sql("""select timestamp '2026-02-10 12:00:00' + to_days(cast(i * 9 as integer)) as ts from range(0, 20) x(i)""") extend {
  timezone: 'Asia/Tokyo'
  measure: c is count()
}`;
      S.configure({
         source: "t",
         model: "m.malloy",
         date: { field: "ts" },
         filters: [],
         entities: {},
         metrics: { c: {} },
         pages: [{ id: "overview" }],
      });
      S.setKnown({ span: { first: "2026-02-10", last: "2026-07-08" } });
      const s = {
         period: { preset: "custom", from: "2026-02-10", to: "2026-07-08" },
         compare: "none",
         grain: "month",
         filters: {},
      };
      const result = await runtime
         .loadModel(model)
         .loadQuery(S.seriesQuery(["c"], s))
         .run();
      const rows = JSON.parse(JSON.stringify(result.data.toObject()));
      const buckets = S.buckets(rows, s);
      expect(buckets.map((b: { start: string }) => b.start)).toEqual([
         "2026-02-10",
         "2026-03-01",
         "2026-04-01",
         "2026-05-01",
         "2026-06-01",
         "2026-07-01",
      ]);
      expect(buckets.every((b: { empty?: boolean }) => !b.empty)).toBe(true);
   });
});
