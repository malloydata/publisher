// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";

import {
   type Binding,
   EMPTY_SNAPSHOT,
   heldReadBinding,
   makeSnapshot,
   pin,
} from "./binding";
import type { FreshnessManifest } from "./types";

// A fixed instant so age math is deterministic.
const NOW = new Date("2026-07-07T12:00:00.000Z");

/** dataAsOf `ageSeconds` before NOW. */
function asOf(ageSeconds: number): string {
   return new Date(NOW.getTime() - ageSeconds * 1000).toISOString();
}

function binding(sourceName: string, over: Partial<Binding> = {}): Binding {
   return {
      kind: "persist",
      tier: "storage",
      sourceName,
      sourceIDs: [`${sourceName}@m`],
      destinationName: "lake",
      virtualHandle: `h_${sourceName}`,
      tablePath: `main.${sourceName}`,
      schema: [{ name: "x", type: "INTEGER" }],
      ...over,
   };
}

const window = (age: number, fallback?: "live" | "stale_ok" | "fail") => ({
   freshAsOf: asOf(age),
   freshnessWindowSeconds: 3600,
   ...(fallback ? { freshnessFallback: fallback } : {}),
});

describe("pin", () => {
   const fresh = binding("fresh", window(1800, "live"));
   const ungated = binding("ungated", { freshAsOf: asOf(999_999) });
   const staleLive = binding("stale_live", window(7200, "live"));
   const staleOk = binding("stale_ok", window(7200, "stale_ok"));
   const staleFail = binding("stale_fail", window(7200, "fail"));
   const snapshot = makeSnapshot([
      fresh,
      ungated,
      staleLive,
      staleOk,
      staleFail,
   ]);

   it("reads fresh, un-gated and stale_ok tables, and leaves stale live/fail ones to live", () => {
      const pinned = pin(snapshot, NOW);
      expect(pinned.fresh.map((b) => b.sourceName)).toEqual([
         "fresh",
         "ungated",
         "stale_ok",
      ]);
      expect(pinned.serves(staleLive)).toBe(false);
      expect(pinned.serves(staleOk)).toBe(true);
   });

   it("answers a lookup as fresh, stale_ok, stale or not built, by kind and sourceID", () => {
      const pinned = pin(snapshot, NOW);
      expect(pinned.tableFor("fresh@m", "persist")).toEqual({
         state: "fresh",
         binding: fresh,
      });
      expect(pinned.tableFor("stale_ok@m", "persist")).toEqual({
         state: "stale_ok",
         binding: staleOk,
      });
      expect(pinned.tableFor("stale_live@m", "persist")).toEqual({
         state: "stale",
         binding: staleLive,
      });
      // The kind is the plan's: a persist table is no rollup's.
      expect(pinned.tableFor("fresh@m", "rollup")).toEqual({
         state: "not_built",
      });
      expect(pinned.tableFor("nothing@m", "persist")).toEqual({
         state: "not_built",
      });
   });

   it("prefers a readable table when two store one source", () => {
      const again = binding("stale_live", { virtualHandle: "h_new" });
      const pinned = pin(makeSnapshot([staleLive, again]), NOW);
      expect(pinned.tableFor("stale_live@m", "persist")).toEqual({
         state: "fresh",
         binding: again,
      });
   });

   it("is a function of the instant: a later pin of one snapshot reads less", () => {
      const later = new Date(NOW.getTime() + 3600 * 1000);
      expect(pin(snapshot, NOW).serves(fresh)).toBe(true);
      expect(pin(snapshot, later).serves(fresh)).toBe(false);
   });

   it("pins the database entries as Malloy's build manifest, never undefined", () => {
      const entries: FreshnessManifest = {
         fresh: { tableName: "fresh_t", ...asOfEntry(1800, "live") },
         ungated: { tableName: "ungated_t", dataAsOf: asOf(999_999) },
         staleLive: { tableName: "stale_live_t", ...asOfEntry(7200, "live") },
         staleOk: { tableName: "stale_ok_t", ...asOfEntry(7200, "stale_ok") },
         staleFail: { tableName: "stale_fail_t", ...asOfEntry(7200, "fail") },
      };
      const pinned = pin(makeSnapshot([], entries), NOW);
      expect(pinned.hasEntries).toBe(true);
      expect(pinned.compilerManifest).toEqual({
         entries: {
            fresh: { tableName: "fresh_t" },
            ungated: { tableName: "ungated_t" },
            staleOk: { tableName: "stale_ok_t" },
         },
         strict: false,
      });
      const none = pin(EMPTY_SNAPSHOT, NOW);
      expect(none.hasEntries).toBe(false);
      expect(none.compilerManifest).toEqual({ entries: {}, strict: false });
   });

   it("evaluates each table once per pin, and the same pin answers alike every time", () => {
      const pinned = pin(snapshot, NOW);
      expect(pinned.fresh).toBe(pinned.fresh);
      expect(pinned.compilerManifest).toBe(pinned.compilerManifest);
   });
});

describe("makeSnapshot", () => {
   it("is frozen, and keeps the bindings array it is given", () => {
      const bindings = Object.freeze([binding("a")]);
      const snapshot = makeSnapshot(bindings);
      expect(Object.isFrozen(snapshot)).toBe(true);
      expect(snapshot.bindings).toBe(bindings);
      expect(snapshot.entries).toEqual({});
   });
});

function asOfEntry(age: number, fallback: "live" | "stale_ok" | "fail") {
   return {
      dataAsOf: asOf(age),
      freshnessWindowSeconds: 3600,
      freshnessFallback: fallback,
   };
}

describe("heldReadBinding", () => {
   it("records each moved column by the type it holds now, for the gate policy", () => {
      const b = heldReadBinding("rollup", "acct_rollup", {
         tableName: "mz_acct_rollup",
         heldRead: {
            connectionName: "warehouse",
            columns: [
               { name: "org_id", type: "BIGINT" },
               { name: "acct", type: "DOUBLE", moved: true },
               {
                  name: "big",
                  type: "VARCHAR",
                  nativeType: "HUGEINT",
                  moved: true,
               },
            ],
         },
      } as never);
      expect(b?.retypedColumns).toEqual([
         { name: "acct", type: "DOUBLE" },
         { name: "big", type: "HUGEINT" },
      ]);
   });

   it("records none when no column moved", () => {
      const b = heldReadBinding("rollup", "acct_rollup", {
         tableName: "mz_acct_rollup",
         heldRead: {
            connectionName: "warehouse",
            columns: [{ name: "org_id", type: "BIGINT" }],
         },
      } as never);
      expect(b?.retypedColumns).toBeUndefined();
   });
});
