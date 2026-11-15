// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Serve bindings from manifest entries, and how serving reads a bound table.

import { describe, expect, it } from "bun:test";

import { buildVirtualMap, sqlReadsTable, tablesReadForGates } from "./bindings";
import { answerKeepsGates } from "../domain/gate_policy";
import {
   bindingServesModelSource,
   deriveServeBindings,
   type Binding,
} from "../domain/binding";

import { planSourceIds, groupAliasesByName } from "../domain/source_aliases";

describe("sqlReadsTable", () => {
   it("reads a table named after FROM or JOIN, never a result column of the same name", () => {
      expect(sqlReadsTable('SELECT 1 FROM "orders" as base', '"orders"')).toBe(
         true,
      );
      expect(
         sqlReadsTable('SELECT 1 FROM t LEFT JOIN "orders" AS j', '"orders"'),
      ).toBe(true);
      expect(
         sqlReadsTable(
            'SELECT 1 FROM (SELECT * FROM "s"."orders") x',
            '"s"."orders"',
         ),
      ).toBe(true);
      expect(
         sqlReadsTable(
            'SELECT count(1) as "orders" FROM "ev" as base',
            '"orders"',
         ),
      ).toBe(false);
      // Only a whole name: not the leading part of a longer path.
      expect(sqlReadsTable('SELECT 1 FROM "orders"."x"', '"orders"')).toBe(
         false,
      );
   });
});

describe("tablesReadForGates", () => {
   const colocated: Binding = {
      kind: "materialize",
      tier: "colocated",
      sourceName: "c",
      destinationName: "duckdb",
      virtualHandle: "h1",
      tablePath: '"c_tbl"',
      schema: [],
   };
   const storage: Binding = {
      kind: "materialize",
      tier: "storage",
      sourceName: "s",
      destinationName: "lake",
      virtualHandle: "h2",
      tablePath: "main.s_tbl",
      schema: [],
   };
   const G3_ONLY = {
      materialize: true,
      gatedSources: false,
      gatedStorageRollupsReviewed: false,
   };
   const keeps = (tables: readonly Binding[]) =>
      answerKeepsGates("served", tables, G3_ONLY, {
         colocatedReadsKeepGates: true,
      });

   it("counts a storage table spelled with other quoting as read, so a mixed answer never reads as colocated", () => {
      // The storage table read unquoted and upper-cased: the one spelling
      // sqlReadsTable matches (`"main"."s_tbl"`) is not there.
      const sql =
         'SELECT 1 FROM "c_tbl" AS base LEFT JOIN MAIN.S_TBL AS j ON j.id = base.id';
      const found = [colocated, storage].filter((b) =>
         sqlReadsTable(
            sql,
            b.tier === "colocated" ? b.tablePath : '"main"."s_tbl"',
         ),
      );
      expect(found).toEqual([colocated]);
      expect(keeps(found)).toBe(true);
      const judged = tablesReadForGates(sql, [colocated, storage], found);
      expect(judged).toEqual([colocated, storage]);
      expect(keeps(judged)).toBe(false);
   });

   it("leaves out a storage table the SQL never names, so G3 still serves a colocated-only answer", () => {
      const sql = 'SELECT 1 FROM "c_tbl" AS base WHERE base.s_tbl_flag';
      const judged = tablesReadForGates(sql, [colocated, storage], [colocated]);
      expect(judged).toEqual([colocated]);
      expect(keeps(judged)).toBe(true);
   });

   it("keeps a mixed answer off G3 when both tables are found", () => {
      expect(keeps([colocated, storage])).toBe(false);
      expect(keeps([])).toBe(false);
   });
});

describe("buildVirtualMap", () => {
   it("groups handles by connection and quotes the table path for DuckDB", () => {
      const map = buildVirtualMap([
         {
            kind: "persist",
            tier: "storage",
            sourceName: "a",
            destinationName: "lake",
            virtualHandle: "h1",
            tablePath: "analytics.a",
            schema: [],
         },
         {
            kind: "persist",
            tier: "storage",
            sourceName: "b",
            destinationName: "lake",
            virtualHandle: "h2",
            tablePath: "b",
            schema: [],
         },
      ]);
      expect(map.get("lake")?.get("h1")).toBe('"analytics"."a"');
      expect(map.get("lake")?.get("h2")).toBe('"b"');
   });

   it("passes an already-quoted path through unchanged (no double-quoting)", () => {
      // Mirrors #904's quoteManifestTablePath: an author-quoted `name=` is
      // canonical SQL and must not be re-quoted into `""foo""`.
      const map = buildVirtualMap([
         {
            kind: "persist",
            tier: "storage",
            sourceName: "a",
            destinationName: "lake",
            virtualHandle: "h",
            tablePath: '"My Table"',
            schema: [],
         },
      ]);
      expect(map.get("lake")?.get("h")).toBe('"My Table"');
   });
});

describe("deriveServeBindings", () => {
   it("binds only storage entries, keying the handle on sourceEntityId", () => {
      const bindings = deriveServeBindings(
         {
            se_storage: {
               sourceEntityId: "se_storage",
               sourceName: "mz",
               physicalTableName: "mz_g003",
               connectionName: "wh",
               storageDestinationName: "lake",
               schema: [{ name: "amount", type: "BIGINT" }],
               dataAsOf: "2026-07-20T00:00:00Z",
               realization: "COPY",
               rowCount: null,
            },
            se_pathC: {
               // In-warehouse (no storage): served via the manifest, not the
               // transform — must NOT produce a binding.
               sourceEntityId: "se_pathC",
               sourceName: "orders",
               physicalTableName: "orders_v1",
               connectionName: "wh",
               realization: "COPY",
               rowCount: null,
            },
            se_noschema: {
               // Storage but no captured schema — skipped (can't declare a shape).
               sourceEntityId: "se_noschema",
               sourceName: "x",
               physicalTableName: "lake.x",
               connectionName: "wh",
               storageDestinationName: "lake",
               schema: [],
               realization: "COPY",
               rowCount: null,
            },
         },
         {},
      );
      expect(bindings).toEqual([
         {
            kind: "persist",
            tier: "storage",
            sourceName: "mz",
            // An entry with no `origin` describes an authored `#@ persist`
            // source, which is what the wire default means and what every entry
            // written before the field existed can only have been.
            destinationName: "lake",
            virtualHandle: "se_storage",
            tablePath: "lake.mz_g003",
            schema: [{ name: "amount", type: "BIGINT" }],
            freshAsOf: "2026-07-20T00:00:00Z",
            freshnessWindowSeconds: undefined,
            freshnessFallback: undefined,
         },
      ]);
      // The table path is qualified with the destination catalog (attach alias)
      // so the serve reads <store>.<table>, not an unqualified name.
      expect(bindings[0].tablePath).toBe("lake.mz_g003");
   });

   it("binds every source sharing an address, not just the one that built it", () => {
      // A base and its `extend` share a content address and therefore one entry
      // and one table. The extension must not get a table of its own, but it must
      // READ the base's — so both names bind, to the same virtual handle. Binding
      // only `entry.sourceName` left the other silently serving live.
      const entry = {
         sourceEntityId: "se_shared",
         sourceName: "daily",
         physicalTableName: "daily_g001",
         connectionName: "wh",
         storageDestinationName: "lake",
         schema: [{ name: "total_amount", type: "BIGINT" }],
         realization: "COPY" as const,
         rowCount: null,
      };

      const bindings = deriveServeBindings(
         { se_shared: entry },
         { daily: ["daily", "daily_with_avg"] },
      );

      expect(bindings.map((b) => b.sourceName)).toEqual([
         "daily",
         "daily_with_avg",
      ]);
      // One table, one handle: several sources resolving to one virtual table is
      // what the identity-scoped handle is for.
      expect(new Set(bindings.map((b) => b.virtualHandle))).toEqual(
         new Set(["se_shared"]),
      );
      expect(new Set(bindings.map((b) => b.tablePath))).toEqual(
         new Set(["lake.daily_g001"]),
      );
   });

   it("does not duplicate the builder's own name", () => {
      // The builder's name is normally in the address group too, so the naive
      // concatenation would bind it twice and push a duplicate source declaration
      // into the serve model.
      const bindings = deriveServeBindings(
         {
            se_shared: {
               sourceEntityId: "se_shared",
               sourceName: "daily",
               physicalTableName: "daily_g001",
               connectionName: "wh",
               storageDestinationName: "lake",
               schema: [{ name: "total_amount", type: "BIGINT" }],
               realization: "COPY",
               rowCount: null,
            },
         },
         { daily: ["daily"] },
      );

      expect(bindings.map((b) => b.sourceName)).toEqual(["daily"]);
   });
   it("binds aliases even when the entry carries a HOST-assigned identity", () => {
      // An instructed build stamps the CALLER's sourceEntityId on its entry
      // (`executeInstructedBuild` treats it as opaque, so a host may derive it any
      // way it likes) while `entries` is keyed by the publisher's content address.
      // Grouping aliases by the entry's id would therefore work only for a host
      // that hashes exactly as the publisher does, and would silently fall back to
      // one-alias routing for any other — the bug this pins. The group is keyed by
      // NAME, which both sides mean the same thing by.
      const bindings = deriveServeBindings(
         {
            // map key = publisher content address; entry id = host's scheme
            "publisher-content-address": {
               sourceEntityId: "host-opaque-id-zzz",
               sourceName: "daily",
               physicalTableName: "daily__shared__g007__tok",
               connectionName: "wh",
               storageDestinationName: "lake",
               schema: [{ name: "total_amount", type: "BIGINT" }],
               realization: "COPY",
               rowCount: null,
            },
         },
         { daily: ["daily", "daily_with_avg"] },
      );

      expect(bindings.map((b) => b.sourceName)).toEqual([
         "daily",
         "daily_with_avg",
      ]);
      // The handle still comes from the entry, so it keeps agreeing with whatever
      // the build wrote into the virtual map.
      expect(new Set(bindings.map((b) => b.virtualHandle))).toEqual(
         new Set(["host-opaque-id-zzz"]),
      );
   });
   it("never lets an alias claim a name another entry OWNS", () => {
      // Two tables. `ext` is an alias of `daily`'s table, and also the source
      // that built its own. Binding it for both would put two `source: ext`
      // declarations in one serve shape — which fails to compile and drops the
      // storage tier for every model in the package, silently, since base-only is
      // the tier the ladder trusts without probing. The owner wins its name.
      const entry = (
         sourceEntityId: string,
         sourceName: string,
         table: string,
      ) => ({
         sourceEntityId,
         sourceName,
         physicalTableName: table,
         connectionName: "wh",
         storageDestinationName: "lake",
         schema: [{ name: "total_amount", type: "BIGINT" }],
         realization: "COPY" as const,
         rowCount: null,
      });

      const bindings = deriveServeBindings(
         {
            X: entry("X", "daily", "daily_t"),
            Z: entry("Z", "ext", "ext_t"),
         },
         // A stale/ambiguous group that still offers `ext` as an alias of X.
         { daily: ["daily", "ext"], ext: ["ext"] },
      );

      expect(bindings.map((b) => b.sourceName).sort()).toEqual([
         "daily",
         "ext",
      ]);
      // `ext` resolves to the table it OWNS, not to daily's.
      const ext = bindings.filter((b) => b.sourceName === "ext");
      expect(ext).toHaveLength(1);
      expect(ext[0].tablePath).toBe("lake.ext_t");
   });
});

describe("serve binding source identity", () => {
   const plan = {
      "daily@file:///p/a.malloy": { name: "daily", sourceEntityId: "addr_a" },
      "daily@file:///p/b.malloy": { name: "daily", sourceEntityId: "addr_b" },
      "daily_ext@file:///p/a.malloy": {
         name: "daily_ext",
         sourceEntityId: "addr_a",
      },
   };
   const entry = (sourceName: string, sourceEntityId: string) => ({
      sourceEntityId,
      sourceName,
      physicalTableName: `${sourceName}_g1`,
      connectionName: "wh",
      storageDestinationName: "lake",
      schema: [{ name: "n", type: "BIGINT" }],
      realization: "COPY" as const,
      rowCount: null,
   });

   it("planSourceIds names the plan sources with that name at that address", () => {
      const ids = planSourceIds(plan);
      expect(ids("daily", "addr_a")).toEqual(["daily@file:///p/a.malloy"]);
      expect(ids("daily", "addr_b")).toEqual(["daily@file:///p/b.malloy"]);
      expect(ids("nope", "addr_a")).toEqual([]);
   });

   it("planSourceIds names no plan source when no address matches, however few share the name", () => {
      // An entry at another address was built from another definition of a
      // same-named source (an edit, a restored manifest, an older release):
      // its table holds that definition's rows.
      expect(planSourceIds(plan)("daily_ext", "other_addr")).toEqual([]);
      expect(planSourceIds(plan)("daily", "other_addr")).toEqual([]);
   });

   it("an empty sourceIDs list serves no model's source of that name", () => {
      const binding: Binding = {
         kind: "persist",
         tier: "storage",
         sourceName: "daily",
         sourceIDs: [],
         destinationName: "lake",
         virtualHandle: "caller_addr",
         tablePath: "lake.daily_g1",
         schema: [{ name: "n", type: "BIGINT" }],
      };
      expect(
         bindingServesModelSource(binding, {
            daily: { sourceID: "daily@file:///p/a.malloy" },
         }),
      ).toBe(false);
   });

   it("deriveServeBindings stamps the builder and its aliases with their own sourceIDs", () => {
      const bindings = deriveServeBindings(
         { addr_a: entry("daily", "addr_a") },
         groupAliasesByName([
            { name: "daily", sourceEntityId: "addr_a" },
            { name: "daily_ext", sourceEntityId: "addr_a" },
         ]),
         undefined,
         planSourceIds(plan),
      );
      expect(bindings.map((b) => [b.sourceName, b.sourceIDs])).toEqual([
         ["daily", ["daily@file:///p/a.malloy"]],
         ["daily_ext", ["daily_ext@file:///p/a.malloy"]],
      ]);
   });

   it("deriveServeBindings leaves sourceIDs off when it has no plan to read", () => {
      const [binding] = deriveServeBindings(
         { addr_a: entry("daily", "addr_a") },
         {},
      );
      expect(binding.sourceIDs).toBeUndefined();
   });

   it("bindingServesModelSource refuses only a confirmed mismatch", () => {
      const binding: Binding = {
         kind: "persist",
         tier: "storage",
         sourceName: "daily",
         sourceIDs: ["daily@file:///p/a.malloy"],
         destinationName: "lake",
         virtualHandle: "addr_a",
         tablePath: "lake.daily_g1",
         schema: [{ name: "n", type: "BIGINT" }],
      };
      const own = { daily: { sourceID: "daily@file:///p/a.malloy" } };
      const other = { daily: { sourceID: "daily@file:///p/b.malloy" } };
      expect(bindingServesModelSource(binding, own)).toBe(true);
      expect(bindingServesModelSource(binding, other)).toBe(false);
      // Not defined here: left to schema narrowing, as before.
      expect(bindingServesModelSource(binding, {})).toBe(true);
      // No identity on the binding: the name decides, as before.
      expect(
         bindingServesModelSource({ ...binding, sourceIDs: undefined }, other),
      ).toBe(true);
      // A rollup's name is generated and names nothing in the author's model.
      expect(
         bindingServesModelSource({ ...binding, kind: "rollup" }, other),
      ).toBe(true);
   });
});
