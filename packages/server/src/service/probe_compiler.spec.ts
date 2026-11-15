// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { beforeAll, describe, expect, it } from "bun:test";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
   type ModelMaterializer,
   type RunSQLOptions,
} from "@malloydata/malloy";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   callerIndependent,
   currentTimeCalls,
   probeSourceDependence,
   readsAGiven,
} from "./probe_compiler";

/**
 * The probe compiler (P9's probes): which givens a field reads, whether it
 * reads the current time, and whether it depends on the query time zone,
 * each read off small queries that are compiled and never run.
 */

/** A DuckDB that refuses to run SQL once `refuse` is set, as the load worker's does. */
class CompileOnlyDuckDB extends DuckDBConnection {
   refuse = false;
   override async runSQL(sql: string, options?: RunSQLOptions) {
      if (this.refuse) throw new Error(`probe ran SQL: ${sql}`);
      return super.runSQL(sql, options);
   }
}

const MODEL = `##! experimental { givens access_modifiers }
given:
  ORG_ID :: number is 1
  REGION :: string is 'east'
source: raw is duckdb.sql("""
  select * from (values
    (1, 1, 'a', 10.5, TIMESTAMP '2026-01-01 03:00:00', DATE '2026-01-01')
  ) as t(id, org_id, s, amount, ts, d)
""")
source: orders is raw extend {
  dimension: mine is org_id = $ORG_ID
  dimension: label is pick 'me' when mine else 'other'
  internal dimension: imine is org_id = $ORG_ID
  private dimension: pmine is org_id = $ORG_ID
  dimension: hr is hour(ts)
  dimension: ts_month is ts.month
  dimension: d_month is d.month
  dimension: on_day is ts::date
  dimension: big is amount > 10
  dimension: age is hours(ts to now)
  dimension: up is upper(s)
}
source: scoped is raw extend {
  where: s = $REGION
  dimension: up is upper(s)
  dimension: by_region is s = $REGION
  dimension: mine is org_id = $ORG_ID
}
source: recent is raw extend {
  where: ts >= now - 30 days
  dimension: up is upper(s)
  dimension: age is hours(ts to now)
}
source: zoned_base is raw extend {
  where: ts.month = @2026-01
  dimension: up is upper(s)
}
`;

let materializer: ModelMaterializer;
let connection: CompileOnlyDuckDB;

beforeAll(async () => {
   connection = new CompileOnlyDuckDB("duckdb", ":memory:");
   const url = new URL("file:///probe/m.malloy");
   const runtime = new Runtime({
      urlReader: new InMemoryURLReader(new Map([[url.toString(), MODEL]])),
      connections: new FixedConnectionMap(
         new Map([["duckdb", connection]]),
         "duckdb",
      ),
   });
   materializer = runtime.loadModel(url);
   await materializer.getModel();
   // Every probe below compiles only: a run would throw.
   connection.refuse = true;
});

async function probe(source: string, fields: string[]) {
   return (await probeSourceDependence(materializer, source, fields)).fields;
}

describe("probe compiler: which givens a field reads", () => {
   it("finds a given read directly and through another dimension", async () => {
      const out = await probe("orders", ["mine", "label", "big"]);
      expect(out.get("mine")?.givens).toHaveLength(1);
      expect(out.get("label")?.givens).toEqual(out.get("mine")?.givens);
      expect(out.get("big")).toEqual({
         givens: [],
         currentTime: false,
         timeZone: false,
      });
   });

   it("probes an internal field through an extension; a private one stays unprobed", async () => {
      const out = await probe("orders", ["imine", "pmine"]);
      expect(out.get("imine")?.givens).toHaveLength(1);
      expect(out.get("pmine")?.givens).toBeUndefined();
      expect(out.get("pmine")?.unprobed).toContain("private");
      expect(callerIndependent(out.get("pmine"))).toBe(false);
   });

   it("does not charge a field with the given its source's own where: reads", async () => {
      const out = await probe("scoped", ["up", "by_region", "mine"]);
      expect(callerIndependent(out.get("up"))).toBe(true);
      // Reads the same given as the where: — the probe cannot tell it apart;
      // the IR walk, which runs beside it, does.
      expect(out.get("by_region")?.givens).toEqual([]);
      // A given the source does not read is the field's own.
      expect(out.get("mine")?.givens).toHaveLength(1);
   });
});

describe("probe compiler: the current time", () => {
   it("finds a field that reads now", async () => {
      const out = await probe("orders", ["age", "up"]);
      expect(out.get("age")?.currentTime).toBe(true);
      expect(out.get("up")?.currentTime).toBe(false);
   });

   it("counts against the source's own clock reads (a rolling window)", async () => {
      const out = await probe("recent", ["up", "age"]);
      expect(out.get("up")?.givens).toEqual([]);
      expect(out.get("up")?.currentTime).toBe(false);
      expect(out.get("age")?.currentTime).toBe(true);
      // The window's cut-off moves with the zone, so no field of the source
      // is cleared of it.
      expect(out.get("up")?.timeZone).toBeUndefined();
   });

   it("matches the dialects' spellings and an author's raw SQL", () => {
      expect(currentTimeCalls("SELECT LOCALTIMESTAMP")).toBe(1);
      expect(currentTimeCalls("CURRENT_TIMESTAMP() + now()")).toBe(2);
      expect(currentTimeCalls("SYSUTCDATETIME(), getdate()")).toBe(2);
      expect(currentTimeCalls('SELECT "today_sales", nowhere')).toBe(0);
   });
});

describe("probe compiler: the query time zone", () => {
   it("finds a field whose SQL moves with the zone, and clears one whose does not", async () => {
      const out = await probe("orders", [
         "hr",
         "ts_month",
         "on_day",
         "d_month",
         "up",
         "big",
      ]);
      for (const f of ["hr", "ts_month", "on_day"]) {
         expect(out.get(f)?.timeZone).toBe(true);
      }
      // A date is civil: its month does not move with the zone.
      for (const f of ["d_month", "up", "big"]) {
         expect(callerIndependent(out.get(f))).toBe(true);
      }
   });

   it("leaves the zone unsettled when the source's own SQL moves with it", async () => {
      const out = await probe("zoned_base", ["up"]);
      const up = out.get("up");
      expect(up?.givens).toEqual([]);
      expect(up?.timeZone).toBeUndefined();
      expect(callerIndependent(up)).toBe(false);
   });

   it("clears a source's rows of the zone only when its own SQL does not move with it, with or without fields", async () => {
      // A `where:` with a timestamp literal filters different rows per zone.
      for (const fields of [[], ["up"]]) {
         expect(
            (await probeSourceDependence(materializer, "zoned_base", fields))
               .rowsZoneFree,
         ).toBe(false);
         expect(
            (await probeSourceDependence(materializer, "orders", fields))
               .rowsZoneFree,
         ).toBe(true);
      }
      expect(
         (await probeSourceDependence(materializer, "no_such_source", []))
            .rowsZoneFree,
      ).toBe(false);
   });
});

describe("probe compiler: failing closed", () => {
   it("reports a field it cannot compile as unprobed, never as clear", async () => {
      const out = await probe("orders", ["no_such_field"]);
      expect(out.get("no_such_field")?.unprobed).toBeDefined();
      expect(callerIndependent(out.get("no_such_field"))).toBe(false);
   });

   it("reports every field unprobed when the source does not compile", async () => {
      const out = await probe("no_such_source", ["a"]);
      expect(out.get("a")?.unprobed).toContain("baseline");
   });

   it("names its probe fields so no author field is shadowed", async () => {
      const { fields: out } = await probeSourceDependence(
         materializer,
         "orders",
         ["big"],
         new Set(["__probe0"]),
      );
      expect(callerIndependent(out.get("big"))).toBe(true);
   });

   it("reads a given from the IR by either signal, and treats a runaway walk as a read", () => {
      expect(readsAGiven({ e: { node: "given", id: "g" } })).toBe(true);
      expect(readsAGiven({ refSummary: { givenUsage: [{ id: "g" }] } })).toBe(
         true,
      );
      expect(readsAGiven({ e: { node: "field", path: ["x"] } })).toBe(false);
      let deep: Record<string, unknown> = {};
      const root = deep;
      for (let i = 0; i < 300; i++) {
         deep.next = {};
         deep = deep.next as Record<string, unknown>;
      }
      expect(() => readsAGiven(root)).toThrow();
   });
});
