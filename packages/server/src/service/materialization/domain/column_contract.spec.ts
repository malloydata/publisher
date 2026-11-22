// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   contractColumn,
   contractDigest,
   deriveContract,
   keyCheckPassed,
   parseContract,
   weakestStringCompare,
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
