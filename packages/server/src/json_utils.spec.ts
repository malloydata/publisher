// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { bigIntReplacer } from "./json_utils";

const ser = (v: unknown) => JSON.stringify(v, bigIntReplacer);

describe("bigIntReplacer", () => {
   it("still renders ordinary BigInt ids as numbers", () => {
      // The common case, and the one that must not change: a DuckDB BIGINT
      // column of ordinary ids has always reached callers as JSON numbers, and
      // the MCP rows are byte-compared against the REST payload.
      expect(ser({ id: 100001n })).toBe('{"id":100001}');
      expect(ser({ c: 0n })).toBe('{"c":0}');
      expect(ser({ c: -42n })).toBe('{"c":-42}');
   });

   it("keeps the largest exactly-representable values as numbers", () => {
      expect(ser({ v: 9007199254740991n })).toBe('{"v":9007199254740991}');
      expect(ser({ v: -9007199254740991n })).toBe('{"v":-9007199254740991}');
   });

   /**
    * The regression this guards. Number() rounds past 2^53, so an id came back
    * altered with no error: an agent filtering or joining on it gets a wrong or
    * empty answer. Malloy's own wrapResult keeps an exact string for the same
    * case, so a string here matches what the typed-cell path already did.
    */
   it("preserves values past the safe range exactly, as strings", () => {
      expect(ser({ id: 9007199254740993n })).toBe('{"id":"9007199254740993"}');
      expect(ser({ id: 1234567890123456789n })).toBe(
         '{"id":"1234567890123456789"}',
      );
      expect(ser({ id: -9007199254740993n })).toBe(
         '{"id":"-9007199254740993"}',
      );
   });

   it("round-trips a large id without losing a digit", () => {
      const exact = "9223372036854775807"; // int64 max
      expect(JSON.parse(ser({ id: BigInt(exact) })).id).toBe(exact);
   });

   it("leaves non-BigInt values alone", () => {
      expect(ser({ a: 1, b: "x", c: null, d: 1.5 })).toBe(
         '{"a":1,"b":"x","c":null,"d":1.5}',
      );
   });

   it("does not throw on the BigInt a plain stringify would reject", () => {
      expect(() => ser({ c: 150930n })).not.toThrow();
   });
});
