// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { SchemaRefetches } from "./schema_refetch";

/** A stand-in for a Malloy connection's protected schema cache. */
function connection(entries: Record<string, unknown> = {}) {
   return { schemaCache: { ...entries } as Record<string, unknown> };
}

/** A clock the test moves by hand. */
function clock(start = 1000) {
   let now = start;
   return { now: () => now, set: (to: number) => (now = to) };
}

describe("SchemaRefetches", () => {
   it("drops the connection's entry before a refetch, whatever its timestamp says", async () => {
      // Stored under a clock since stepped back: a timestamp comparison
      // would judge it current and skip the refetch.
      const conn = connection({ t: { timestamp: Number.MAX_SAFE_INTEGER } });
      const seen: [string[], boolean][] = [];
      const refetches = new SchemaRefetches<string>(60_000, clock().now);
      const answers = await refetches.fetch(conn, ["t"], 0, async (keys) => {
         seen.push([keys, "t" in conn.schemaCache]);
         return new Map([["t", "fresh"]]);
      });
      expect(seen).toEqual([[["t"], false]]);
      expect(answers.get("t")).toBe("fresh");
   });

   it("leaves no entry behind when the refetch fails, as for a dropped table", async () => {
      const conn = connection({ t: { timestamp: 1 } });
      const refetches = new SchemaRefetches<{ error: string }>(
         60_000,
         clock().now,
      );
      const answers = await refetches.fetch(
         conn,
         ["t"],
         0,
         async () => new Map([["t", { error: "Table not found" }]]),
      );
      expect(answers.get("t")).toEqual({ error: "Table not found" });
      // The connection caches no error, so the next read asks the warehouse.
      expect("t" in conn.schemaCache).toBe(false);
   });

   it("answers a load requested before a refetch from that refetch, not from the connection's cache", async () => {
      const conn = connection({ t: "stale" });
      const time = clock(1000);
      const refetches = new SchemaRefetches<string>(60_000, time.now);
      const calls: string[][] = [];
      let release!: () => void;
      const gate = new Promise<void>((resolve) => (release = resolve));
      const refetch = async (keys: string[]) => {
         calls.push(keys);
         if (calls.length === 1) await gate;
         return new Map(keys.map((k) => [k, `fresh-${calls.length}`]));
      };

      // Two loads requested at 900 and 950; the first refetches at 1000.
      const first = refetches.fetch(conn, ["t", "u"], 900, refetch);
      time.set(1001);
      const second = refetches.fetch(conn, ["t"], 950, refetch);
      // An older, unrefreshed fetch writes the old schema back meanwhile.
      conn.schemaCache.t = "stale";
      release();
      const [a, b] = await Promise.all([first, second]);
      expect(calls).toEqual([["t", "u"]]);
      expect(a.get("t")).toBe("fresh-1");
      expect(b.get("t")).toBe("fresh-1");

      // A load requested after that refetch began refetches for itself.
      time.set(2000);
      const third = await refetches.fetch(conn, ["t"], 1500, refetch);
      expect(calls.at(-1)).toEqual(["t"]);
      expect(third.get("t")).toBe("fresh-2");
   });

   it("gives a load sharing a refetch that failed the same failure", async () => {
      const conn = connection();
      const time = clock(1000);
      const refetches = new SchemaRefetches<string>(60_000, time.now);
      let release!: () => void;
      const gate = new Promise<void>((resolve) => (release = resolve));
      const first = refetches.fetch(conn, ["t"], 900, async () => {
         await gate;
         throw new Error("warehouse unreachable");
      });
      const second = refetches.fetch(conn, ["t"], 950, async () => {
         throw new Error("not called");
      });
      release();
      const settled = await Promise.allSettled([first, second]);
      expect(
         settled.map((r) =>
            r.status === "rejected" ? (r.reason as Error).message : r.status,
         ),
      ).toEqual(["warehouse unreachable", "warehouse unreachable"]);
   });

   it("forgets refetches older than its window", async () => {
      const conn = connection();
      const time = clock(1000);
      const refetches = new SchemaRefetches<string>(100, time.now);
      let calls = 0;
      const refetch = async (keys: string[]) => {
         calls += 1;
         return new Map(keys.map((k) => [k, "x"]));
      };
      await refetches.fetch(conn, ["t"], 900, refetch);
      time.set(1200);
      // Requested before the old refetch, but it is past the window.
      await refetches.fetch(conn, ["t"], 950, refetch);
      expect(calls).toBe(2);
   });
});
