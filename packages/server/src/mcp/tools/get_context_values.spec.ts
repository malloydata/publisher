// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Value search's two shared pieces.
 *
 * 1. The matcher, pure: the scoring table is pinned number by number.
 * 2. The lookup query over a REAL Malloy model on DuckDB: this is where gates
 *    are checked, because the gate is enforced by the query path and a
 *    stand-in would only test itself.
 */

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { type Connection } from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { AccessDeniedError } from "../../errors";
import { parseRetrievalConfig } from "../../retrieval_config";
import type { EnvironmentStore } from "../../service/environment_store";
import { Model } from "../../service/model";
import { defaultIndexValueFetcher } from "../context/values_index";
import { defaultValueFetcher } from "./get_context_values";
import {
   editDistance,
   filterStem,
   matchValues,
   normalizeText,
   scoreValue,
} from "./get_context_value_match";

// ---------------------------------------------------------------------------
// 1. The matcher
// ---------------------------------------------------------------------------

describe("value matching", () => {
   const STORED = "Auto Intenders (In-Market)";

   it("scores each kind of match at its published number", () => {
      const score = (q: string) => scoreValue(q, STORED);
      expect(score("Auto Intenders (In-Market)")).toBe(1);
      // Same after case, punctuation and spacing are normalized.
      expect(score("auto intenders in market")).toBe(0.98);
      expect(score("AUTO  INTENDERS")).toBe(0.9167);
      // Prefix of a word ("intender" of "intenders").
      expect(score("auto intender")).toBe(0.8629);
      // One edit, and a swap of two letters counted as one edit.
      expect(score("auto intendrs")).toBe(0.7948);
      expect(score("auto intedners")).toBe(0.8021);
      // Part of the value.
      expect(score("in-market")).toBe(0.875);
      expect(score("market")).toBe(0.85);
      // No word in common.
      expect(score("home shoppers")).toBe(0);
   });

   it("does not match a value on one word of a two-word phrase", () => {
      // Found live: "region 3" returned every "Region N" at 0.5 each.
      expect(scoreValue("region 3", "Region 0")).toBe(0);
      expect(scoreValue("auto buyers", "Auto Intenders")).toBe(0);
      expect(scoreValue("region 3", "Region 3")).toBe(0.98);
   });

   it("ranks a closer value above a longer one and breaks ties stably", () => {
      const hits = matchValues("auto intenders", [
         "Home Shoppers",
         "Auto Intenders (In-Market) 2024",
         "Auto Intenders",
         "Auto Intenders (In-Market)",
         "Travel Seekers",
      ]);
      expect(hits).toEqual([
         { value: "Auto Intenders", score: 0.98 },
         { value: "Auto Intenders (In-Market)", score: 0.9167 },
         { value: "Auto Intenders (In-Market) 2024", score: 0.8966 },
      ]);
   });

   it("keeps at most the asked number of values", () => {
      const values = Array.from({ length: 30 }, (_, i) => `Auto ${i}`);
      expect(matchValues("auto", values)).toHaveLength(10);
      expect(matchValues("auto", values, 3)).toHaveLength(3);
   });

   it("normalizes case, accents, punctuation and spacing", () => {
      expect(normalizeText("  Café   Au-Lait! ")).toBe("cafe au lait");
   });

   it("counts a swap of neighbouring letters as one edit", () => {
      expect(editDistance("intedners", "intenders", 2)).toBe(1);
      expect(editDistance("abc", "xyz", 1)).toBe(2);
   });

   it("builds the text-filter stem from the longest word, four letters", () => {
      expect(filterStem("the Auto Intenders")).toBe("inte");
      expect(filterStem("ab")).toBeUndefined();
   });
});

// ---------------------------------------------------------------------------
// Settings
// ---------------------------------------------------------------------------

describe("retrieval.dimensionValues settings", () => {
   it("accepts the documented keys, and a zero cache lifetime", () => {
      expect(
         parseRetrievalConfig({
            dimensionValues: {
               enabled: false,
               maxDimensions: 5,
               maxValuesPerDimension: 100,
               timeoutMs: 1000,
               budgetMs: 2000,
               cacheTtlMs: 0,
            },
         })?.dimensionValues,
      ).toEqual({
         enabled: false,
         maxDimensions: 5,
         maxValuesPerDimension: 100,
         timeoutMs: 1000,
         budgetMs: 2000,
         cacheTtlMs: 0,
      });
   });

   it("names the valid keys on a typo", () => {
      expect(() =>
         parseRetrievalConfig({ dimensionValues: { maxDimension: 5 } }),
      ).toThrow(
         "Invalid retrieval.dimensionValues: unknown key 'maxDimension'. Valid keys: enabled, maxDimensions, maxValuesPerDimension, timeoutMs, budgetMs, cacheTtlMs.",
      );
   });

   it("refuses a bad value with the fix", () => {
      expect(() =>
         parseRetrievalConfig({ dimensionValues: { maxDimensions: 0 } }),
      ).toThrow(
         "Invalid retrieval.dimensionValues.maxDimensions: expected a positive integer, got 0. Fix: set it to e.g. 12",
      );
      expect(() =>
         parseRetrievalConfig({ dimensionValues: { cacheTtlMs: -1 } }),
      ).toThrow(
         "Invalid retrieval.dimensionValues.cacheTtlMs: expected a non-negative integer, got -1.",
      );
   });
});

const REAL_MODEL = `##! experimental.givens

given:
  role :: string
  TENANTS :: string[]

source: seg_data is duckdb.sql("""
  select * from (values
    ('acme',   'Auto Intenders (In-Market)', 'US'),
    ('acme',   'Home Shoppers',              'US'),
    ('globex', 'Auto Intenders (Lapsed)',    'EU'),
    ('globex', 'Travel Seekers',             'EU')
  ) t(tenant, segment_name, market)
""")

source: open_segments is seg_data extend {
  dimension: segment is segment_name
}

#(authorize) 'analyst' = $role
source: locked_segments is seg_data extend {
  dimension: segment is segment_name
}

#(access_filter) tenant in $TENANTS
source: tenant_segments is seg_data extend {
  dimension: segment is segment_name
}
`;

describe("the lookup query over a real model (gates are the query path's)", () => {
   let dir: string;
   let duckdb: DuckDBConnection;
   let store: EnvironmentStore;

   beforeAll(async () => {
      dir = fs.mkdtempSync(path.join(os.tmpdir(), "values-real-"));
      fs.writeFileSync(path.join(dir, "m.malloy"), REAL_MODEL);
      duckdb = new DuckDBConnection("duckdb", ":memory:", dir);
      const model = await Model.create(
         "pkg",
         dir,
         "m.malloy",
         new Map<string, Connection>([["duckdb", duckdb]]),
      );
      expect(
         (model as unknown as { compilationError?: Error }).compilationError,
      ).toBeUndefined();
      const pkg = {
         listModels: async () => [{ path: "m.malloy" }],
         getModel: (p: string) => (p === "m.malloy" ? model : undefined),
      };
      store = {
         getEnvironment: async () => ({
            assertCanAdmitQuery: () => undefined,
            getPackage: async () => pkg,
            getStaleCompileErrors: () => new Map(),
         }),
      } as unknown as EnvironmentStore;
   });

   afterAll(async () => {
      await duckdb.close();
      fs.rmSync(dir, { recursive: true, force: true });
   });

   const read = (source: string, givens?: Record<string, unknown>) =>
      defaultValueFetcher({
         environmentStore: store,
         environmentName: "env",
         packageName: "pkg",
         modelPath: "m.malloy",
         source,
         dimension: "segment",
         limit: 100,
         timeoutMs: 10_000,
         ...(givens ? { givens } : {}),
      });
   const sorted = (r: { values: string[] }) => [...r.values].sort();

   it("reads an ungated source's values, without any givens", async () => {
      const read1 = await read("open_segments");
      expect(read1.truncated).toBe(false);
      expect(sorted(read1)).toEqual([
         "Auto Intenders (In-Market)",
         "Auto Intenders (Lapsed)",
         "Home Shoppers",
         "Travel Seekers",
      ]);
   });

   it("withholds an #(authorize) source from a caller who sends no givens, then serves the one who sends the right ones", async () => {
      await expect(read("locked_segments")).rejects.toBeInstanceOf(
         AccessDeniedError,
      );
      await expect(
         read("locked_segments", { role: "intern" }),
      ).rejects.toBeInstanceOf(AccessDeniedError);
      const admitted = await read("locked_segments", { role: "analyst" });
      expect(sorted(admitted)).toHaveLength(4);
   });

   it("returns values only from the rows an #(access_filter) admits, and none when the gate cannot apply", async () => {
      const acme = await read("tenant_segments", { TENANTS: ["acme"] });
      expect(sorted(acme)).toEqual([
         "Auto Intenders (In-Market)",
         "Home Shoppers",
      ]);
      const globex = await read("tenant_segments", { TENANTS: ["globex"] });
      expect(sorted(globex)).toEqual([
         "Auto Intenders (Lapsed)",
         "Travel Seekers",
      ]);
      // A tenant that owns no rows sees no values, with no error.
      const nobody = await read("tenant_segments", { TENANTS: ["nobody"] });
      expect(nobody.values).toEqual([]);
      // Without the given the filter cannot apply, so nothing is read.
      await expect(read("tenant_segments")).rejects.toThrow();
   });

   it("reads only the values containing a stem, when one is given", async () => {
      const filtered = await defaultValueFetcher({
         environmentStore: store,
         environmentName: "env",
         packageName: "pkg",
         modelPath: "m.malloy",
         source: "open_segments",
         dimension: "segment",
         limit: 100,
         stem: "inte",
         timeoutMs: 10_000,
      });
      expect(sorted(filtered)).toEqual([
         "Auto Intenders (In-Market)",
         "Auto Intenders (Lapsed)",
      ]);
   });

   it("reads the index's counted values through the same query, gates included", async () => {
      const indexRead = (source: string, dimension: string, limit: number) =>
         defaultIndexValueFetcher({
            environmentStore: store,
            environmentName: "env",
            packageName: "pkg",
            modelPath: "m.malloy",
            source,
            dimension,
            limit,
            timeoutMs: 10_000,
         });
      const markets = await indexRead("open_segments", "market", 10);
      expect(markets.truncated).toBe(false);
      expect(
         [...markets.values].sort((a, b) => a.value.localeCompare(b.value)),
      ).toEqual([
         { value: "EU", count: 2 },
         { value: "US", count: 2 },
      ]);
      const cut = await indexRead("open_segments", "segment", 2);
      expect(cut.values).toHaveLength(2);
      expect(cut.truncated).toBe(true);
      // The index read sends no givens, so a gated source refuses it.
      await expect(
         indexRead("locked_segments", "segment", 10),
      ).rejects.toBeInstanceOf(AccessDeniedError);
   });
});
