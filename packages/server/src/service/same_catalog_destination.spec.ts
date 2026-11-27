// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { MalloyConfig } from "@malloydata/malloy";
import { describe, expect, it } from "bun:test";
import { allBaseTables } from "./same_catalog_destination";

/** A destination whose catalog is at `state.snapshot`, holding `state.types`. */
function destination(state: {
   snapshot: number;
   types: Record<string, string>;
   fail?: boolean;
}) {
   const sql: string[] = [];
   const connection = {
      runSQL: async (text: string) => {
         sql.push(text);
         if (state.fail) throw new Error("catalog unreachable");
         if (!text.includes("information_schema")) {
            return { rows: [{ id: state.snapshot }] };
         }
         const rows = Object.entries(state.types)
            .filter(([name]) => text.includes(`'${name.split(".")[1]}'`))
            .map(([name, type]) => ({
               snapshot_id: state.snapshot,
               table_schema: name.split(".")[0],
               table_name: name.split(".")[1],
               table_type: type,
            }));
         return {
            rows: rows.length ? rows : [{ snapshot_id: state.snapshot }],
         };
      },
   };
   const config = {
      connections: { lookupConnection: async () => connection },
   } as unknown as MalloyConfig;
   return { config, sql };
}

const T = [["credible", "main", "customers"]] as const;

describe("allBaseTables", () => {
   it("probes once per catalog snapshot, and again once the catalog changes", async () => {
      const state = { snapshot: 7, types: { "main.customers": "BASE TABLE" } };
      const { config, sql } = destination(state);
      expect(await allBaseTables(config, "a", T)).toBe(true);
      expect(await allBaseTables(config, "a", T)).toBe(true);
      // The second read is the snapshot id alone.
      expect(sql.filter((s) => s.includes("information_schema"))).toHaveLength(
         1,
      );
      // A writer swaps the table for a view: a new snapshot, probed again.
      state.snapshot = 8;
      state.types = { "main.customers": "VIEW" };
      expect(await allBaseTables(config, "a", T)).toBe(false);
      expect(await allBaseTables(config, "a", T)).toBe(false);
      expect(sql.filter((s) => s.includes("information_schema"))).toHaveLength(
         2,
      );
   });

   it("is false for a table it does not find", async () => {
      const { config } = destination({ snapshot: 1, types: {} });
      expect(await allBaseTables(config, "a", T)).toBe(false);
   });

   it("throws when the catalog cannot be read, which the caller takes as no", async () => {
      const { config } = destination({ snapshot: 1, types: {}, fail: true });
      await expect(allBaseTables(config, "a", T)).rejects.toThrow();
   });
});
