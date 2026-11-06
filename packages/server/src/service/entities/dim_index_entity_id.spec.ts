// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, test } from "bun:test";

import {
   buildFingerprintQuery,
   computeSqlFingerprint,
} from "./dim_index_entity_id";

// This server computes only the two compile-derived inputs to a dim-index
// entity's stable id — the canonical fingerprint query and the hash of its
// compiled SQL; the id itself is derived by the host that indexes the
// package. The ``sqlFingerprint`` pin below is the contract anchor: a host
// keys its dimension index on it, so a silent drift here would re-key (and
// double-index) every indexed dimension.

describe("computeSqlFingerprint", () => {
   test("is hex-lowercase SHA-256 of the UTF-8 bytes", () => {
      const fp = computeSqlFingerprint("SELECT 1");
      expect(fp).toMatch(/^[0-9a-f]{64}$/);
   });

   test("matches the pinned hex SHA-256 for the contract fixture", () => {
      const sql =
         "SELECT base.carrier, COUNT(*) AS weight FROM flights AS base GROUP BY 1 ORDER BY base.carrier";
      // Pinned hex SHA-256 of the UTF-8 input. Re-pin if-and-only-if the
      // fingerprint contract is deliberately rebased (a coordinated re-key of
      // every host's dimension index).
      expect(computeSqlFingerprint(sql)).toBe(
         "c8217f61924002d0cd291d688a5130c36919c4afeaaee33049cbde27a0079bad",
      );
   });
});

describe("buildFingerprintQuery", () => {
   test("quotes a source name that needs it", () => {
      expect(
         buildFingerprintQuery("Order Items", "region", [], []),
      ).toStartWith("run: `Order Items` -> { ");
   });

   test("sorts required filters alphabetically and brackets the value field", () => {
      const q = buildFingerprintQuery("flights", "carrier", ["tier", "region"]);
      expect(q).toBe(
         "run: `flights` -> { " +
            "group_by: `region`, `tier`, value is `carrier` " +
            "aggregate: weight is count() " +
            "order_by: value " +
            "}",
      );
   });

   test("omits the leading comma when there are no required filters", () => {
      const q = buildFingerprintQuery("flights", "carrier", []);
      expect(q).toBe(
         "run: `flights` -> { " +
            "group_by: value is `carrier` " +
            "aggregate: weight is count() " +
            "order_by: value " +
            "}",
      );
   });

   test("aliases only keys whose leaf name collides, by sorted position", () => {
      const q = buildFingerprintQuery("s", "carrier", [
         "id",
         "account.id",
         "region",
      ]);
      expect(q).toContain(
         "group_by: `__pk0` is `account`.`id`, `__pk1` is `id`, `region`, value is `carrier`",
      );
   });

   test("aliases a key named like the value or weight output", () => {
      const q = buildFingerprintQuery("s", "carrier", ["value"]);
      expect(q).toContain("group_by: `__pk0` is `value`, value is `carrier`");
   });

   test("does not mutate caller-supplied required filters", () => {
      const filters = ["tier", "region"];
      buildFingerprintQuery("flights", "carrier", filters);
      expect(filters).toEqual(["tier", "region"]);
   });

   test("backticks dotted dimension paths per segment", () => {
      // Malloy traverses joins with ``.`` but backtick-quotes single
      // identifiers, so a join-qualified path is quoted segment-wise
      // (``\`origin\`.\`iata\```) — one pair around the whole dotted
      // string would be read as a single bogus identifier and fail
      // the fingerprint compile.
      const q = buildFingerprintQuery("flights", "origin.iata", []);
      expect(q).toContain("value is `origin`.`iata`");
   });

   test("backticks dotted required-filter paths per segment", () => {
      const q = buildFingerprintQuery("flights", "carrier", ["origin.state"]);
      expect(q).toContain("group_by: `origin`.`state`, value is `carrier`");
   });

   test("gate columns follow the sorted filters, sorted among themselves and aliased by position", () => {
      const ungated = buildFingerprintQuery("flights", "carrier", ["tier"]);
      const gated = buildFingerprintQuery(
         "flights",
         "carrier",
         ["tier"],
         ["report_id", "account.region"],
      );
      expect(gated).toContain(
         "group_by: `tier`, `__g0` is `account`.`region`, `__g1` is `report_id`, value is `carrier`",
      );
      expect(gated).not.toBe(ungated);
   });

   test("a column moving between filter and gate role changes the query", () => {
      const asFilter = buildFingerprintQuery(
         "flights",
         "carrier",
         ["tier"],
         [],
      );
      const asGate = buildFingerprintQuery("flights", "carrier", [], ["tier"]);
      expect(asGate).not.toBe(asFilter);
   });

   test("a different gate set changes the query", () => {
      const one = buildFingerprintQuery("s", "carrier", [], ["a"]);
      const two = buildFingerprintQuery("s", "carrier", [], ["a", "b"]);
      expect(one).not.toBe(two);
   });

   test("a filters-only query is byte-stable", () => {
      expect(buildFingerprintQuery("flights", "carrier", ["tier"], [])).toBe(
         "run: `flights` -> { group_by: `tier`, value is `carrier` aggregate: weight is count() order_by: value }",
      );
   });

   test("does not mutate caller-supplied gate columns", () => {
      const gates = ["b", "a"];
      buildFingerprintQuery("flights", "carrier", [], gates);
      expect(gates).toEqual(["b", "a"]);
   });
});
