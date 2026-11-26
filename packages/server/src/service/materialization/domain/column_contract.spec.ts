// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   compatible,
   contractColumn,
   contractDigest,
   declaredMoves,
   deriveContract,
   keyCheckPassed,
   parseContract,
   weakestStringCompare,
   contractKeyName,
   keyUniquenessOfContract,
} from "./column_contract";

const one = {
   v: 1,
   dialect: "duckdb",
   columns: [
      {
         name: "name",
         nativeType: "VARCHAR",
         collation: "binary",
         compare: "exact",
         gateExact: true,
      },
   ],
};

describe("parseContract", () => {
   it("reads no contract from an absent, empty or version-less value", () => {
      for (const raw of [undefined, null, {}, { columns: [] }, "", "{}", 3]) {
         expect(parseContract(raw)).toBeUndefined();
      }
   });

   it("reads a host's text the same as the object it holds", () => {
      expect(parseContract(JSON.stringify(one))).toEqual(parseContract(one));
      expect(parseContract("{not json")).toBeUndefined();
   });

   it("ignores keys it does not know, at every level", () => {
      const read = parseContract({
         ...one,
         later: { x: 1 },
         columns: [{ ...one.columns[0], later: true }],
         keys: { k: { unique: true, level: "leaf", later: 1 } },
      });
      expect(read).toEqual({
         v: 1,
         dialect: "duckdb",
         columns: [one.columns[0]] as never,
         keys: { k: { unique: true, level: "leaf" } },
      });
   });

   it("reads an unknown compare as none and a missing gateExact as false", () => {
      const read = parseContract({
         v: 1,
         columns: [{ name: "a", nativeType: "TEXT", compare: "fuzzy" }],
      })!;
      expect(read.columns[0]).toEqual({
         name: "a",
         nativeType: "TEXT",
         collation: null,
         compare: "none",
         gateExact: false,
      });
   });

   it("drops a column with no name or no native type", () => {
      const read = parseContract({
         v: 1,
         columns: [{ name: "a" }, { nativeType: "TEXT" }, "junk"],
      })!;
      expect(read.columns).toEqual([]);
   });
});

describe("contractColumn", () => {
   it("finds a column exactly, then by a unique case-fold", () => {
      const c = parseContract(one)!;
      expect(contractColumn(c, "name")).toMatchObject({ compare: "exact" });
      expect(contractColumn(c, "NAME")).toMatchObject({ compare: "exact" });
   });

   it("reads an unlisted or ambiguous column as unknown", () => {
      const c = parseContract({
         v: 1,
         columns: [
            { name: "a", nativeType: "T", compare: "exact", gateExact: true },
            { name: "A", nativeType: "T", compare: "exact", gateExact: true },
         ],
      })!;
      expect(contractColumn(c, "b")).toEqual({
         name: "b",
         compare: "none",
         gateExact: false,
      });
      expect(contractColumn(c, "a")).toMatchObject({ name: "a" });
      expect(contractColumn(c, "x").compare).toBe("none");
   });
});

describe("contractDigest", () => {
   it("is stable under key order, case and spacing of the native type, and extra keys", () => {
      const a = parseContract(one)!;
      const b = parseContract({
         producer: "x",
         columns: [
            {
               gateExact: false,
               compare: "none",
               collation: "binary",
               nativeType: " varchar ",
               name: "name",
            },
         ],
         dialect: "postgres",
         v: 1,
      })!;
      expect(contractDigest(a)).toBe(contractDigest(b));
   });

   it("moves with a column's type or collation", () => {
      const a = parseContract(one)!;
      const retyped = parseContract({
         ...one,
         columns: [{ ...one.columns[0], nativeType: "INTEGER" }],
      })!;
      const recollated = parseContract({
         ...one,
         columns: [{ ...one.columns[0], collation: "nocase" }],
      })!;
      expect(contractDigest(retyped)).not.toBe(contractDigest(a));
      expect(contractDigest(recollated)).not.toBe(contractDigest(a));
   });
});

describe("keyCheckPassed", () => {
   it("passes only a check recorded unique", () => {
      const c = parseContract({
         ...one,
         keys: { a: { unique: true }, b: { unique: false } },
      });
      expect(keyCheckPassed(c, "a")).toBe(true);
      expect(keyCheckPassed(c, "b")).toBe(false);
      expect(keyCheckPassed(c, "c")).toBe(false);
      expect(keyCheckPassed(undefined, "a")).toBe(false);
   });
});

describe("deriveContract", () => {
   const stored = (
      columns: { name: string; nativeType: string; isString: boolean }[],
      collation: string | null = "binary",
   ) => ({
      dialect: "duckdb",
      columns,
      collations: { byColumn: new Map(), otherwise: collation },
   });
   const name = { name: "name", nativeType: "VARCHAR", isString: true };
   const n = { name: "n", nativeType: "BIGINT", isString: false };
   const d = { name: "d", nativeType: "DOUBLE", isString: false };

   it("records every non-string column exact", () => {
      const c = deriveContract({ tier: "storage", stored: stored([n, d]) });
      expect(c.columns.map((x) => x.compare)).toEqual(["exact", "exact"]);
      expect(c.v).toBe(1);
   });

   it("derives a storage= string's compare from its live collation", () => {
      const at = (live: "binary" | "deterministic" | "unknown") =>
         deriveContract({
            tier: "storage",
            stored: stored([name]),
            live: new Map([["name", live]]),
         }).columns[0].compare;
      expect(at("binary")).toBe("exact");
      expect(at("deterministic")).toBe("equality");
      expect(at("unknown")).toBe("none");
      expect(
         deriveContract({ tier: "storage", stored: stored([name]) }).columns[0]
            .compare,
      ).toBe("none");
   });

   it("compares nothing on a storage= string whose stored collation is not binary or unread", () => {
      const live = new Map([["name", "binary" as const]]);
      expect(
         deriveContract({
            tier: "storage",
            stored: stored([name], "duckdb:nocase"),
            live,
         }).columns[0].compare,
      ).toBe("none");
      expect(
         deriveContract({
            tier: "storage",
            stored: { dialect: "duckdb", columns: [name] },
            live,
         }).columns[0],
      ).toMatchObject({ compare: "none", collation: null });
   });

   it("takes a stacked storage= table's string compare from the tables it read", () => {
      expect(
         deriveContract({
            tier: "storage",
            stored: stored([name]),
            stackedOn: "equality",
         }).columns[0].compare,
      ).toBe("equality");
   });

   it("records a database string exact once its collation is read", () => {
      const c = deriveContract({
         tier: "database",
         stored: {
            dialect: "postgres",
            columns: [name],
            collations: {
               byColumn: new Map([["name", "postgres:db|c|en_US"]]),
               otherwise: null,
            },
         },
      });
      expect(c.columns[0]).toMatchObject({
         collation: "postgres:db|c|en_US",
         compare: "exact",
      });
   });

   it("records a gate exact by tier", () => {
      const storage = deriveContract({
         tier: "storage",
         stored: stored([n, d]),
      });
      expect(storage.columns.map((x) => x.gateExact)).toEqual([true, false]);
      const database = deriveContract({
         tier: "database",
         stored: {
            dialect: "duckdb",
            columns: [d, { ...n, storedAsDouble: true }],
         },
      });
      expect(database.columns.map((x) => x.gateExact)).toEqual([true, false]);
   });

   it("records a storage double gate-exact only as a copy of a binary64", () => {
      const gateExactOver = (
         nativeType: string,
         upstream?: { dialect: string; nativeType: string },
      ): boolean =>
         deriveContract({
            tier: "storage",
            stored: stored([{ ...d, nativeType }]),
            ...(upstream ? { upstreamTypes: new Map([["d", upstream]]) } : {}),
         }).columns[0].gateExact;
      // A copy of an 8-byte IEEE double, as each dialect's catalog names it.
      expect(
         gateExactOver("DOUBLE", {
            dialect: "postgres",
            nativeType: "double precision",
         }),
      ).toBe(true);
      expect(
         gateExactOver("DOUBLE", { dialect: "duckdb", nativeType: "DOUBLE" }),
      ).toBe(true);
      expect(
         gateExactOver("DOUBLE", { dialect: "snowflake", nativeType: "FLOAT" }),
      ).toBe(true);
      // An exact number stored as a double, a narrower float, a 128-bit
      // integer, and a column with no upstream type (computed, or unread).
      expect(
         gateExactOver("DOUBLE", {
            dialect: "postgres",
            nativeType: "numeric",
         }),
      ).toBe(false);
      expect(
         gateExactOver("DOUBLE", { dialect: "postgres", nativeType: "real" }),
      ).toBe(false);
      expect(
         gateExactOver("HUGEINT", { dialect: "duckdb", nativeType: "HUGEINT" }),
      ).toBe(false);
      expect(gateExactOver("DOUBLE")).toBe(false);
      // Matched by stored name, case-blind when only one matches.
      expect(
         deriveContract({
            tier: "storage",
            stored: stored([d]),
            upstreamTypes: new Map([
               ["D", { dialect: "duckdb", nativeType: "DOUBLE" }],
            ]),
         }).columns[0].gateExact,
      ).toBe(true);
   });

   it("round-trips through the reader", () => {
      const c = deriveContract({
         tier: "storage",
         stored: stored([name, n]),
         live: new Map([["name", "binary" as const]]),
         keys: { "items.id": { unique: true, level: "leaf" } },
         producer: "0.0.0",
      });
      expect(parseContract(JSON.parse(JSON.stringify(c)))).toEqual(c);
   });
});

describe("weakestStringCompare", () => {
   it("is the weakest string compare, none for a missing contract", () => {
      const exact = parseContract(one)!;
      const equality = parseContract({
         ...one,
         columns: [{ ...one.columns[0], compare: "equality" }],
      })!;
      expect(weakestStringCompare([exact])).toBe("exact");
      expect(weakestStringCompare([exact, equality])).toBe("equality");
      expect(weakestStringCompare([exact, undefined])).toBe("none");
   });
});

describe("compatible: the one comparator", () => {
   const contract = parseContract({
      v: 1,
      dialect: "postgres",
      columns: [
         {
            name: "id",
            nativeType: "bigint",
            collation: null,
            compare: "exact",
         },
         {
            name: "name",
            nativeType: "text",
            collation: "postgres:db|en_US",
            compare: "exact",
         },
      ],
   })!;
   const collations = {
      byColumn: new Map([["name", "postgres:db|en_US"]]),
      otherwise: null,
   };

   it("passes a table as recorded, and one that gained a column", () => {
      expect(
         compatible(contract, {
            columns: [
               { name: "id", nativeType: "BIGINT" },
               { name: "name", nativeType: "text" },
               { name: "more", nativeType: "int" },
            ],
            collations,
         }),
      ).toEqual({ ok: true });
   });

   it("reports gone before retyped before recollated, naming every such column", () => {
      expect(
         compatible(contract, {
            columns: [{ name: "name", nativeType: "integer" }],
            collations,
         }),
      ).toEqual({ ok: false, code: "column_gone", columns: ["id"] });
      expect(
         compatible(contract, {
            columns: [
               { name: "id", nativeType: "numeric" },
               { name: "name", nativeType: "integer" },
            ],
            collations,
         }),
      ).toEqual({ ok: false, code: "column_retyped", columns: ["id", "name"] });
      expect(
         compatible(contract, {
            columns: [
               { name: "id", nativeType: "bigint" },
               { name: "name", nativeType: "text" },
            ],
            collations: {
               byColumn: new Map([["name", "binary"]]),
               otherwise: null,
            },
         }),
      ).toEqual({ ok: false, code: "collation_changed", columns: ["name"] });
   });

   it("does not judge collations its engine did not read", () => {
      expect(
         compatible(contract, {
            columns: [
               { name: "id", nativeType: "bigint" },
               { name: "name", nativeType: "text" },
            ],
         }),
      ).toEqual({ ok: true });
   });
});

describe("declaredMoves", () => {
   it("names a column whose source now declares another type than was built", () => {
      const contract = parseContract({
         v: 1,
         columns: [
            { name: "d", nativeType: "TIMESTAMP", declaredType: "date" },
            { name: "n", nativeType: "BIGINT", declaredType: "number" },
            { name: "x", nativeType: "VARCHAR" },
         ],
      })!;
      expect(
         declaredMoves(contract, [
            { name: "d", type: "date" },
            { name: "n", type: "string" },
            { name: "x", type: "number" },
         ]),
      ).toEqual(["n"]);
   });
});

describe("the key checks a contract records", () => {
   it("names each check by its level, subject and key column", () => {
      expect(contractKeyName("leaf", "items.options", "option_id")).toBe(
         "leaf:items.options.option_id",
      );
      expect(contractKeyName("leaf", "", "id")).toBe("leaf:id");
      expect(contractKeyName("rollup", "", "category")).toBe("rollup:category");
      expect(contractKeyName("parent", "orders@m/orders.malloy", "id")).toBe(
         "parent:orders@m/orders.malloy.id",
      );
   });

   const contract = (keys: Record<string, unknown>) => ({
      v: 1,
      dialect: "duckdb",
      columns: [],
      keys,
   });
   const ORDERS = {
      sourceID: "orders@file:///pkg/m/orders.malloy",
      relativeID: "orders@m/orders.malloy",
      primaryKey: "id",
   };
   const USERS = {
      sourceID: "users@file:///pkg/m/users.malloy",
      relativeID: "users@m/users.malloy",
      primaryKey: "user_id",
   };

   it("reads a rollup's own check and each source's under its current key, by sourceID", () => {
      expect(
         keyUniquenessOfContract(
            contract({
               "rollup:category": { unique: true, level: "rollup" },
               "parent:orders@m/orders.malloy.id": {
                  unique: true,
                  level: "parent",
               },
               "stored:users@m/users.malloy.user_id": {
                  unique: false,
                  level: "stored",
               },
               "leaf:items.item_id": { unique: true, level: "leaf" },
            }),
            [ORDERS, USERS],
         ),
      ).toEqual({
         keyColumn: "category",
         unique: true,
         parentKeys: { [ORDERS.sourceID]: true, [USERS.sourceID]: false },
         parentKeyColumns: {
            [ORDERS.sourceID]: "id",
            [USERS.sourceID]: "user_id",
         },
      });
   });

   it("reads nothing for a check of another key, a level that does not match, or none", () => {
      expect(
         keyUniquenessOfContract(
            contract({
               // The key the source declared when the table was built.
               "parent:orders@m/orders.malloy.old_id": {
                  unique: true,
                  level: "parent",
               },
               "parent:users@m/users.malloy.user_id": {
                  unique: true,
                  level: "leaf",
               },
            }),
            [ORDERS, USERS],
         ),
      ).toBeUndefined();
      expect(keyUniquenessOfContract(contract({}), [ORDERS])).toBeUndefined();
      expect(keyUniquenessOfContract(undefined, [ORDERS])).toBeUndefined();
   });

   const ITEMS = {
      sourceID: "items@file:///pkg/m/items.malloy",
      relativeID: "items@m/items.malloy",
      primaryKey: "item_id",
   };
   const OPTIONS = {
      sourceID: "options@file:///pkg/m/options.malloy",
      relativeID: "options@m/options.malloy",
      primaryKey: "option_id",
   };
   const LEAVES = [
      { joinPath: "items", sourceID: ITEMS.sourceID },
      { joinPath: "items.options", sourceID: OPTIONS.sourceID },
   ];

   it("reads a pair leaf's check by the join path the plan names, as the leaf source's", () => {
      expect(
         keyUniquenessOfContract(
            contract({
               "leaf:items.item_id": { unique: true, level: "leaf" },
               "leaf:items.options.option_id": { unique: false, level: "leaf" },
            }),
            [ITEMS, OPTIONS],
            LEAVES,
         ),
      ).toEqual({
         parentKeys: { [ITEMS.sourceID]: true, [OPTIONS.sourceID]: false },
         parentKeyColumns: {
            [ITEMS.sourceID]: "item_id",
            [OPTIONS.sourceID]: "option_id",
         },
      });
   });

   it("reads no leaf check recorded for another key, or for a leaf the plan does not name", () => {
      expect(
         keyUniquenessOfContract(
            contract({
               // The key `items` declared when the table was built.
               "leaf:items.old_id": { unique: true, level: "leaf" },
               "leaf:other.option_id": { unique: true, level: "leaf" },
            }),
            [ITEMS, OPTIONS],
            LEAVES,
         ),
      ).toBeUndefined();
      // A leaf source the models no longer declare.
      expect(
         keyUniquenessOfContract(
            contract({ "leaf:items.item_id": { unique: true, level: "leaf" } }),
            [OPTIONS],
            LEAVES,
         ),
      ).toBeUndefined();
   });

   it("reads a check that could not run (unverified) as not passed", () => {
      expect(
         keyUniquenessOfContract(
            contract({
               "leaf:items.item_id": {
                  unique: true,
                  level: "leaf",
                  unverified: true,
               },
               "rollup:category": {
                  unique: true,
                  level: "rollup",
                  unverified: true,
               },
            }),
            [ITEMS],
            LEAVES,
         ),
      ).toEqual({
         keyColumn: "category",
         unique: false,
         parentKeys: { [ITEMS.sourceID]: false },
         parentKeyColumns: { [ITEMS.sourceID]: "item_id" },
      });
   });
});

/**
 * A DuckDB GEOMETRY with a CRS: a build reads `GEOMETRY('OGC:CRS84')` inside
 * the transaction that wrote it, and DuckLake's catalog, which keeps no CRS,
 * reads `GEOMETRY` after the commit. The two are one committed type.
 */
describe("a GEOMETRY's CRS, on the storage dialect", () => {
   const geometryContract = (nativeType: string, dialect = "duckdb") =>
      parseContract({
         v: 1,
         dialect,
         columns: [
            { name: "id", nativeType: "BIGINT", collation: null },
            { name: "g", nativeType, collation: null },
         ],
      })!;
   const read = (g: string) => ({
      columns: [
         { name: "id", nativeType: "BIGINT" },
         { name: "g", nativeType: g },
      ],
      collations: { byColumn: new Map(), otherwise: "binary" },
   });

   it("holds a GEOMETRY('OGC:CRS84') contract compatible with a GEOMETRY read", () => {
      expect(
         compatible(
            geometryContract("GEOMETRY('OGC:CRS84')"),
            read("GEOMETRY"),
         ),
      ).toEqual({ ok: true });
      // A legacy contract with another CRS, spelled with spacing, alike.
      expect(
         compatible(
            geometryContract("geometry ( 'EPSG:4326' )"),
            read("GEOMETRY"),
         ),
      ).toEqual({ ok: true });
   });

   it("still finds a GEOMETRY retyped to another type", () => {
      expect(
         compatible(geometryContract("GEOMETRY('OGC:CRS84')"), read("VARCHAR")),
      ).toEqual({ ok: false, code: "column_retyped", columns: ["g"] });
   });

   it("folds nothing on another dialect", () => {
      expect(
         compatible(
            geometryContract("GEOMETRY('OGC:CRS84')", "postgres"),
            read("GEOMETRY"),
         ),
      ).toEqual({ ok: false, code: "column_retyped", columns: ["g"] });
   });

   it("records the committed type at capture, and digests a legacy contract as a new one", () => {
      const captured = deriveContract({
         tier: "storage",
         stored: {
            dialect: "duckdb",
            columns: [
               { name: "id", nativeType: "BIGINT", isString: false },
               {
                  name: "g",
                  nativeType: "GEOMETRY('OGC:CRS84')",
                  isString: false,
               },
               {
                  name: "gs",
                  nativeType: "STRUCT(g GEOMETRY('OGC:CRS84'))",
                  isString: false,
               },
            ],
            collations: { byColumn: new Map(), otherwise: "binary" },
         },
      });
      expect(captured.columns.map((c) => c.nativeType)).toEqual([
         "BIGINT",
         "GEOMETRY",
         "STRUCT(g GEOMETRY)",
      ]);
      const legacy = geometryContract("GEOMETRY('OGC:CRS84')");
      expect(contractDigest(legacy)).toBe(
         contractDigest(geometryContract("GEOMETRY")),
      );
   });
});
