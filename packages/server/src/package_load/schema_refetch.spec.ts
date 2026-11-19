// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { SchemaRefetches } from "./schema_refetch";

/** A stand-in for a Malloy connection's protected schema cache. */
function connection(entries: Record<string, { timestamp: number }> = {}) {
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
      const seen: [string[], boolean, boolean][] = [];
      const refetches = new SchemaRefetches(60_000, clock().now);
      await refetches.fetch(conn, ["t"], 0, async (keys, refetch) => {
         seen.push([keys, refetch, "t" in conn.schemaCache]);
         return {};
      });
      expect(seen).toEqual([[["t"], true, false]]);
   });

   it("leaves no entry behind when the refetch fails, as for a dropped table", async () => {
      const conn = connection({ t: { timestamp: 1 } });
      const refetches = new SchemaRefetches(60_000, clock().now);
      const [answer] = await refetches.fetch(conn, ["t"], 0, async () => ({
         errors: { t: "Table not found" },
      }));
      expect(answer).toEqual({ errors: { t: "Table not found" } });
      // The connection caches no error, so the next read asks the warehouse.
      expect("t" in conn.schemaCache).toBe(false);
   });

   it("lets a load that started before a refetch share it, and refetches for one that started after", async () => {
      const conn = connection({ t: { timestamp: 1 } });
      const time = clock(1000);
      const refetches = new SchemaRefetches(60_000, time.now);
      const calls: [string[], boolean][] = [];
      let release!: () => void;
      const gate = new Promise<void>((resolve) => (release = resolve));
      const fetch = async (keys: string[], refetch: boolean) => {
         calls.push([keys, refetch]);
         if (refetch && calls.length === 1) await gate;
         return keys;
      };

      // Two loads that started at 900 and 950; the first refetches at 1000.
      const first = refetches.fetch(conn, ["t"], 900, fetch);
      time.set(1001);
      const second = refetches.fetch(conn, ["t"], 950, fetch);
      await Promise.resolve();
      // The second waits on the first's refetch rather than starting one.
      expect(calls).toEqual([[["t"], true]]);
      release();
      await Promise.all([first, second]);
      expect(calls).toEqual([
         [["t"], true],
         [["t"], false],
      ]);

      // A load that started after that refetch began refetches for itself.
      time.set(2000);
      await refetches.fetch(conn, ["t"], 1500, fetch);
      expect(calls.at(-1)).toEqual([["t"], true]);
   });

   it("forgets refetches older than its window", async () => {
      const conn = connection();
      const time = clock(1000);
      const refetches = new SchemaRefetches(100, time.now);
      const calls: boolean[] = [];
      const fetch = async (_keys: string[], refetch: boolean) => {
         calls.push(refetch);
         return undefined;
      };
      await refetches.fetch(conn, ["t"], 900, fetch);
      time.set(1200);
      // Started before the old refetch, but it is past the window.
      await refetches.fetch(conn, ["t"], 950, fetch);
      expect(calls).toEqual([true, true]);
   });
});
