// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Declaration resolution for incremental materialization, against the real
// compiler (in-memory DuckDB, the pattern of materialization_eligibility.spec.ts).
// The behaviors this module depends on are pinned separately in
// incremental_compiler_contract.spec.ts; these tests cover the resolution itself.
import type { FixedConnectionMap, PersistSource } from "@malloydata/malloy";
import { beforeAll, describe, expect, it } from "bun:test";
import { deriveAnnotationFields } from "./build_plan";
import {
   declaresIncremental,
   resolveIncrementalDeclaration,
   type IncrementalDeclaration,
} from "./incremental_declaration";
import {
   compilePersistSources,
   duckdbTestConnections,
} from "./incremental_test_harness";

let connections: FixedConnectionMap;

beforeAll(() => {
   ({ connections } = duckdbTestConnections());
});

async function persistSources(
   model: string,
): Promise<Record<string, PersistSource>> {
   return (await compilePersistSources(connections, model)).sources;
}

/** Resolve one named source of a compiled model. */
async function resolve(
   model: string,
   sourceName = "mz",
): Promise<IncrementalDeclaration> {
   const sources = await persistSources(model);
   const source = sources[sourceName];
   if (!source) {
      throw new Error(
         `source '${sourceName}' not in the build plan (have: ${Object.keys(sources).join(", ")})`,
      );
   }
   return resolveIncrementalDeclaration(source, deriveAnnotationFields(source));
}

const RAW = `##! experimental.persistence
source: raw is duckdb.sql("""
  SELECT 1 AS amount, DATE '2024-01-01' AS order_date,
         TIMESTAMP '2024-01-01 00:00:00' AS ts, 'US' AS region,
         'o-1' AS order_id, true AS flag
""")
`;

/** A query-defined (aggregating) source: order_date/region/order_id + revenue. */
const ROLLUP = `source: mz is raw -> {
  group_by: order_date, region, order_id
  aggregate: revenue is amount.sum()
}`;

describe("resolveIncrementalDeclaration", () => {
   describe("a query-defined source", () => {
      it("resolves a keyless incremental declaration to range_replace", async () => {
         const declaration = await resolve(
            `${RAW}#@ persist name="t" refresh="incremental" watermark="order_date"\n${ROLLUP}`,
         );
         expect(declaration.incremental).toBe(true);
         expect(declaration.watermark).toEqual({
            name: "order_date",
            kind: "dimension",
            malloyType: "date",
         });
         expect(declaration.watermarkOrderable).toBe(true);
         expect(declaration.mergeKeys).toEqual([]);
         expect(declaration.strategy).toBe("range_replace");
         expect(declaration.malformed).toEqual([]);
         expect(declaration.unknownKeys).toEqual([]);
      });

      it("resolves a compound merge_key= to merge, trimming around commas", async () => {
         const declaration = await resolve(
            `${RAW}#@ persist name="t" refresh="incremental" watermark="order_date" merge_key="order_id, region"\n${ROLLUP}`,
         );
         expect(declaration.strategy).toBe("merge");
         expect(declaration.mergeKeys).toEqual([
            { name: "order_id", kind: "dimension", malloyType: "string" },
            { name: "region", kind: "dimension", malloyType: "string" },
         ]);
         expect(declaration.watermarkInMergeKeys).toBe(false);
      });

      it("records a name that resolves to an aggregate: column", async () => {
         // The output schema cannot tell an aggregate from a dimension; this
         // comes from the query definition's field kinds.
         const declaration = await resolve(
            `${RAW}#@ persist name="t" refresh="incremental" watermark="revenue"\n${ROLLUP}`,
         );
         expect(declaration.watermark).toEqual({
            name: "revenue",
            kind: "aggregate",
            malloyType: "number",
         });
      });

      it("records a dangling name as unresolved", async () => {
         const declaration = await resolve(
            `${RAW}#@ persist name="t" refresh="incremental" watermark="ordr_date"\n${ROLLUP}`,
         );
         expect(declaration.watermark).toEqual({
            name: "ordr_date",
            kind: "unresolved",
         });
         expect(declaration.watermarkOrderable).toBe(false);
         expect(declaration.strategy).toBeUndefined();
         expect(declaration.outputColumns).toEqual([
            "order_date",
            "region",
            "order_id",
            "revenue",
         ]);
      });

      it("flags a merge_key= that includes the watermark", async () => {
         const declaration = await resolve(
            `${RAW}#@ persist name="t" refresh="incremental" watermark="order_date" merge_key="order_id,order_date"\n${ROLLUP}`,
         );
         expect(declaration.watermarkInMergeKeys).toBe(true);
      });

      it("enumerates calculate: (window) fields from the query definition", async () => {
         const declaration = await resolve(
            `${RAW}#@ persist name="t" refresh="incremental" watermark="order_date"
source: mz is raw -> {
  group_by: order_date
  aggregate: revenue is amount.sum()
  calculate: prev is lag(revenue)
}`,
         );
         expect(declaration.calculateFields).toEqual(["prev"]);
      });

      it("reports no calculate: fields for a plain aggregating source", async () => {
         // Includes a non-additive aggregate, which is deliberately NOT gated:
         // the delta recomputes whole output rows from full input.
         const declaration = await resolve(
            `${RAW}#@ persist name="t" refresh="incremental" watermark="order_date"
source: mz is raw -> {
  group_by: order_date
  aggregate: buyers is count(order_id)
}`,
         );
         expect(declaration.calculateFields).toEqual([]);
      });

      it("marks a non-orderable watermark type", async () => {
         const declaration = await resolve(
            `${RAW}#@ persist name="t" refresh="incremental" watermark="flag"
source: mz is raw -> { group_by: order_date, flag }`,
         );
         expect(declaration.watermark).toEqual({
            name: "flag",
            kind: "dimension",
            malloyType: "boolean",
         });
         expect(declaration.watermarkOrderable).toBe(false);
      });

      it("reads a timestamp watermark's type, which the bound literal follows", async () => {
         const declaration = await resolve(
            `${RAW}#@ persist name="t" refresh="incremental" watermark="ts"
source: mz is raw -> { group_by: ts; aggregate: revenue is amount.sum() }`,
         );
         expect(declaration.watermark).toMatchObject({
            malloyType: "timestamp",
         });
      });
   });

   describe("a table-backed source", () => {
      it("resolves against the table's own columns", async () => {
         const declaration = await resolve(
            `${RAW}#@ persist name="t" refresh="incremental" watermark="order_date"
source: mz is raw extend { }`,
         );
         expect(declaration.watermark).toMatchObject({ kind: "dimension" });
         expect(declaration.strategy).toBe("range_replace");
         expect(declaration.calculateFields).toEqual([]);
      });

      it("does NOT resolve a derived dimension, which is never materialized", async () => {
         const declaration = await resolve(
            `${RAW}#@ persist name="t" refresh="incremental" watermark="as_of"
source: mz is raw extend { dimension: as_of is order_date }`,
         );
         expect(declaration.watermark).toEqual({
            name: "as_of",
            kind: "unresolved",
         });
         expect(declaration.outputColumns).not.toContain("as_of");
      });
   });

   describe("the effective merged tag (extend fixtures)", () => {
      const PARENT = `#@ persist name="parent_t" refresh="incremental" watermark="order_date"\n${ROLLUP}`;

      it("a child that declares nothing inherits the parent's whole declaration", async () => {
         const declaration = await resolve(
            `${RAW}${PARENT}\n\nsource: child is mz extend { }`,
            "child",
         );
         expect(declaration.incremental).toBe(true);
         expect(declaration.watermark).toMatchObject({ name: "order_date" });
         expect(declaration.strategy).toBe("range_replace");
      });

      it("a child's watermark= overrides the parent's, key by key", async () => {
         const declaration = await resolve(
            `${RAW}${PARENT}\n
#@ persist name="child_t" watermark="region"
source: child is mz extend { }`,
            "child",
         );
         expect(declaration.incremental).toBe(true);
         expect(declaration.watermark).toMatchObject({
            name: "region",
            malloyType: "string",
         });
      });

      it('a child can opt out with refresh="full" and -watermark', async () => {
         const declaration = await resolve(
            `${RAW}${PARENT}\n
#@ persist name="child_t" refresh="full" -watermark
source: child is mz extend { }`,
            "child",
         );
         expect(declaration.incremental).toBe(false);
         expect(declaration.declaredWatermark).toBe(false);
         expect(declaration.watermark).toBeUndefined();
         expect(declaration.strategy).toBeUndefined();
         expect(declaresIncremental(declaration)).toBe(false);
      });
   });

   describe("malformed values", () => {
      it("reports the tag-array form instead of losing the key", async () => {
         const declaration = await resolve(
            `${RAW}#@ persist name="t" refresh="incremental" watermark=["order_date","region"]\n${ROLLUP}`,
         );
         // Declared, but with no usable scalar — the distinction that keeps this
         // from being diagnosed as "no watermark declared".
         expect(declaration.declaredWatermark).toBe(true);
         expect(declaration.watermark).toBeUndefined();
         expect(declaration.malformed).toEqual([
            { key: "watermark", problem: "array" },
         ]);
         expect(declaration.strategy).toBeUndefined();
      });

      it("reports an empty string value", async () => {
         const declaration = await resolve(
            `${RAW}#@ persist name="t" refresh="incremental" watermark=""\n${ROLLUP}`,
         );
         expect(declaration.declaredWatermark).toBe(true);
         expect(declaration.malformed).toEqual([
            { key: "watermark", problem: "empty" },
         ]);
      });

      it("reports an empty entry in a merge_key= list once", async () => {
         const declaration = await resolve(
            `${RAW}#@ persist name="t" refresh="incremental" watermark="order_date" merge_key="order_id,,region"\n${ROLLUP}`,
         );
         expect(declaration.malformed).toEqual([
            { key: "merge_key", problem: "empty-entry" },
         ]);
         // The names that DID parse are still resolved, so the message can be
         // specific about what it read.
         expect(declaration.mergeKeys.map((k) => k.name)).toEqual([
            "order_id",
            "region",
         ]);
      });

      it("reports a duplicated merge_key= name", async () => {
         const declaration = await resolve(
            `${RAW}#@ persist name="t" refresh="incremental" watermark="order_date" merge_key="order_id, order_id"\n${ROLLUP}`,
         );
         expect(declaration.malformed).toEqual([
            { key: "merge_key", problem: "duplicate", detail: "order_id" },
         ]);
         expect(declaration.mergeKeys.map((k) => k.name)).toEqual(["order_id"]);
      });

      it("reports an unrecognized refresh= value verbatim", async () => {
         const capitalized = await resolve(
            `${RAW}#@ persist name="t" refresh="Incremental" watermark="order_date"\n${ROLLUP}`,
         );
         expect(capitalized.incremental).toBe(false);
         expect(capitalized.invalidRefresh).toBe("Incremental");

         const boolish = await resolve(
            `${RAW}#@ persist name="t" refresh="true"\n${ROLLUP}`,
         );
         expect(boolish.invalidRefresh).toBe("true");
      });
   });

   describe("unknown keys", () => {
      it("lists a typo'd key, which would otherwise silently degrade", async () => {
         const declaration = await resolve(
            `${RAW}#@ persist name="t" refresh="incremental" watermark="order_date" mergekey="order_id"\n${ROLLUP}`,
         );
         expect(declaration.unknownKeys).toEqual(["mergekey"]);
         // …and the strategy really did degrade to keyless, which is why the
         // typo needs a warning of its own.
         expect(declaration.strategy).toBe("range_replace");
      });

      it("does not list recognized or retired keys", async () => {
         const declaration = await resolve(
            `${RAW}#@ persist name="t" storage="lake" refresh="full" freshness.window="1h" queryMetadata.tier="gold" sharing=private\n${ROLLUP}`,
         );
         expect(declaration.unknownKeys).toEqual([]);
      });
   });

   describe("sources that declare nothing", () => {
      it("is inert for a plain persist source", async () => {
         const declaration = await resolve(
            `${RAW}#@ persist name="t"\n${ROLLUP}`,
         );
         expect(declaration.refresh).toBeUndefined();
         expect(declaration.incremental).toBe(false);
         expect(declaration.strategy).toBeUndefined();
         expect(declaration.malformed).toEqual([]);
         expect(declaresIncremental(declaration)).toBe(false);
      });

      it('is inert for an explicit refresh="full"', async () => {
         const declaration = await resolve(
            `${RAW}#@ persist name="t" refresh="full"\n${ROLLUP}`,
         );
         expect(declaration.incremental).toBe(false);
         expect(declaresIncremental(declaration)).toBe(false);
      });
   });
});
