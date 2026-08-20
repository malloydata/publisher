// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// The run-level predicate that decides whether a build touches the delta path at
// all. `incrementalLineage` is read from more than one place — it gates the
// skip-if-unchanged exemption in deriveSelfInstructions AND the dispatch in
// buildOneSource, and the two must never disagree — so it is pinned here rather
// than only through the callers.
import { describe, expect, it } from "bun:test";
import { incrementalLineage } from "./incremental_build";
import type { IncrementalDeclaration } from "./incremental_declaration";

const DIMENSION = { kind: "dimension" as const, malloyType: "date" };

function declaration(
   overrides: Partial<IncrementalDeclaration> = {},
): IncrementalDeclaration {
   return {
      refresh: "incremental",
      incremental: true,
      declaredWatermark: true,
      declaredMergeKey: false,
      watermark: { name: "order_date", ...DIMENSION },
      watermarkOrderable: true,
      mergeKeys: [],
      watermarkInMergeKeys: false,
      strategy: "range_replace",
      malformed: [],
      unknownKeys: [],
      calculateFields: [],
      outputColumns: ["order_date", "revenue"],
      ...overrides,
   } as IncrementalDeclaration;
}

function lineage(
   overrides: {
      declaration?: IncrementalDeclaration | undefined;
      dialect?: string;
      targetDialect?: string;
      storageDestinationName?: string;
   } = {},
) {
   return incrementalLineage({
      declaration:
         "declaration" in overrides ? overrides.declaration : declaration(),
      dialect: overrides.dialect ?? "postgres",
      targetDialect: overrides.targetDialect ?? overrides.dialect ?? "postgres",
      physicalTableName: "orders_v1",
      connectionName: "wh",
      storageDestinationName: overrides.storageDestinationName,
      sourceEntityId: "addr-1",
   });
}

describe("incrementalLineage", () => {
   it("describes the source a delta would advance", () => {
      expect(
         lineage({
            declaration: declaration({
               declaredMergeKey: true,
               mergeKeys: [
                  { name: "order_id", ...DIMENSION, malloyType: "number" },
                  { name: "region", ...DIMENSION, malloyType: "string" },
               ],
               strategy: "merge",
            }),
         }),
      ).toEqual({
         physicalTableName: "orders_v1",
         connectionName: "wh",
         sourceEntityId: "addr-1",
         watermarkName: "order_date",
         watermarkType: "date",
         mergeKeys: ["order_id", "region"],
         strategy: "merge",
      });
   });

   // Every case below means "build this the ordinary way and do not touch the
   // ledger". The first two are the overwhelming majority of sources; the rest
   // are declarations the publish gate rejects, so reaching a build with one
   // means the package predates the gates.
   it("declines a source with no declaration", () => {
      expect(lineage({ declaration: undefined })).toBeUndefined();
   });

   it("declines a source that is not incremental", () => {
      expect(
         lineage({ declaration: declaration({ incremental: false }) }),
      ).toBeUndefined();
   });

   it("declines a dialect off the allowlist", () => {
      expect(lineage({ dialect: "duckdb" })).toBeUndefined();
   });

   it("describes a storage= build, whose table lives in the destination", () => {
      // The destination is part of the boundary's table identity, so the same
      // source materialized colocated and stored yields two distinct lineages.
      const stored = lineage({
         targetDialect: "duckdb",
         storageDestinationName: "lake",
      });
      expect(stored?.storageDestinationName).toBe("lake");
      expect(lineage()?.storageDestinationName).toBeUndefined();
   });

   it("declines a target engine whose transactional apply is unproven", () => {
      expect(lineage({ targetDialect: "mysql" })).toBeUndefined();
   });

   it("declines a watermark that is not a materialized dimension", () => {
      expect(
         lineage({ declaration: declaration({ watermark: undefined }) }),
      ).toBeUndefined();
      expect(
         lineage({
            declaration: declaration({
               watermark: {
                  name: "revenue",
                  kind: "aggregate",
                  malloyType: "number",
               },
            }),
         }),
      ).toBeUndefined();
      expect(
         lineage({
            declaration: declaration({
               watermark: {
                  name: "flag",
                  kind: "dimension",
                  malloyType: "boolean",
               },
               watermarkOrderable: false,
            }),
         }),
      ).toBeUndefined();
   });

   it("declines when a merge key is not a materialized dimension", () => {
      // Partial row identity is worse than none: the merge would match on fewer
      // columns than the author declared and quietly overwrite the wrong rows.
      expect(
         lineage({
            declaration: declaration({
               declaredMergeKey: true,
               mergeKeys: [
                  { name: "region", ...DIMENSION, malloyType: "string" },
                  { name: "revenue", kind: "aggregate", malloyType: "number" },
               ],
               strategy: "merge",
            }),
         }),
      ).toBeUndefined();
   });
});
