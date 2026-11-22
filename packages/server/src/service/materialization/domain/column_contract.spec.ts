// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   contractColumn,
   contractDigest,
   keyCheckPassed,
   parseContract,
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
