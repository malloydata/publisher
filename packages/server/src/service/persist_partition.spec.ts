// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// `partition=` decides a stored table's file layout and nothing about which
// rows a caller sees, so these cases are about one question only: does the
// column the author named exist in the table the build will write? The table is
// the source's PUBLIC projection, so "exists" means public — and the two ways
// it can fail to be get different answers, because they have different fixes.
import type { FixedConnectionMap, PersistSource } from "@malloydata/malloy";
import { beforeAll, describe, expect, it } from "bun:test";
import { deriveAnnotationFields } from "./build_plan";
import {
   compilePersistSources,
   duckdbTestConnections,
} from "./incremental_test_harness";
import {
   parsePartitionValue,
   resolvePartitionColumns,
} from "./persist_partition";

let connections: FixedConnectionMap;

beforeAll(() => {
   ({ connections } = duckdbTestConnections());
});

const HEAD = `##! experimental { persistence access_modifiers }
source: raw is duckdb.sql("""SELECT * FROM (VALUES (1,7,'a'),(2,9,'b')) AS t(org_id, user_id, s)""")
`;

async function resolve(body: string) {
   const { sources } = await compilePersistSources(connections, `${HEAD}\n${body}`);
   const source: PersistSource = sources["p"];
   expect(source).toBeDefined();
   // Through `deriveAnnotationFields`, not a hand-built record: the claim that
   // `partition=` needs no new tag parsing is only true if the existing
   // key=value harvest really does carry it.
   return resolvePartitionColumns(source, deriveAnnotationFields(source));
}

describe("parsePartitionValue", () => {
   it("keeps the author's order, because it is the directory nesting order", () => {
      expect(parsePartitionValue("org_id, day")).toEqual(["org_id", "day"]);
      expect(parsePartitionValue("day, org_id")).toEqual(["day", "org_id"]);
   });

   it("tolerates whitespace and a trailing comma", () => {
      expect(parsePartitionValue("  org_id ,  day , ")).toEqual([
         "org_id",
         "day",
      ]);
   });
});

describe("resolvePartitionColumns", () => {
   it("resolves a declared column off the persist tag", async () => {
      const result = await resolve(
         `#@ persist name="p" storage=credible partition="org_id"
source: p is raw -> { select: * }`,
      );
      expect(result).toEqual({ ok: true, columns: ["org_id"] });
   });

   it("resolves a composite list in the declared order", async () => {
      const result = await resolve(
         `#@ persist name="p" storage=credible partition="org_id,s"
source: p is raw -> { select: * }`,
      );
      expect(result).toEqual({ ok: true, columns: ["org_id", "s"] });
   });

   it("is absent for a source that declares none", async () => {
      const result = await resolve(
         `#@ persist name="p" storage=credible
source: p is raw -> { select: * }`,
      );
      expect(result).toEqual({ ok: true, columns: [] });
   });

   it("refuses a column the source does not project", async () => {
      const result = await resolve(
         `#@ persist name="p" storage=credible partition="nope"
source: p is raw -> { select: * }`,
      );
      expect(result.ok).toBe(false);
      if (!result.ok) {
         expect(result.reason).toBe("partition_column_unknown");
         expect(result.detail).toContain("'nope'");
      }
   });

   it("refuses a column the source hides, separately from one that is absent", async () => {
      // The fix differs — make it public, versus partition by something else —
      // so the two refusals stay distinct rather than collapsing into "unknown".
      const result = await resolve(
         `#@ persist name="p" storage=credible partition="tenant"
source: p is raw -> { select: * } extend { private dimension: tenant is org_id }`,
      );
      expect(result.ok).toBe(false);
      if (!result.ok) {
         expect(result.reason).toBe("partition_column_not_public");
         expect(result.detail).toContain("'tenant'");
      }
   });

   it("refuses partition= without storage=, rather than silently ignoring it", async () => {
      // Honouring it silently is the one outcome that misleads: a colocated
      // build writes into the customer's warehouse and lays out nothing.
      const result = await resolve(
         `#@ persist name="p" partition="org_id"
source: p is raw -> { select: * }`,
      );
      expect(result.ok).toBe(false);
      if (!result.ok) expect(result.reason).toBe("partition_without_storage");
   });
});
