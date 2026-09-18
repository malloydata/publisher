// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// `partition=` decides a stored table's file layout and nothing about which
// rows a caller sees, so these cases are about one question only: does the
// column the author named exist in the table the build will write? The table is
// the source's PUBLIC projection, so "exists" means public — and the two ways
// it can fail to be get different answers, because they have different fixes.
import type { FixedConnectionMap, PersistSource } from "@malloydata/malloy";
import { beforeAll, describe, expect, it } from "bun:test";
import { computeSourceEntityId, deriveAnnotationFields } from "./build_plan";
import {
   compilePersistSources,
   duckdbTestConnections,
} from "./incremental_test_harness";
import { RECOGNIZED_PERSIST_KEYS } from "./incremental_declaration";
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
   const { sources } = await compilePersistSources(
      connections,
      `${HEAD}\n${body}`,
   );
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

describe("partition= and the content address", () => {
   // A storage build is skipped when the address is unchanged, so a layout the
   // address cannot see is a layout the author can change with no effect.
   async function addressOf(body: string): Promise<string> {
      const { sources } = await compilePersistSources(
         connections,
         `${HEAD}\n${body}`,
      );
      return computeSourceEntityId(sources["p"], { duckdb: "dig-1" });
   }

   const UNPARTITIONED = `#@ persist name="p" storage=credible
source: p is raw -> { select: * }`;

   it("re-addresses when the declared partition changes, so the table is rebuilt", async () => {
      const byOrg = await addressOf(
         `#@ persist name="p" storage=credible partition="org_id"
source: p is raw -> { select: * }`,
      );
      const byS = await addressOf(
         `#@ persist name="p" storage=credible partition="s"
source: p is raw -> { select: * }`,
      );
      expect(byOrg).not.toBe(byS);
      // And declaring one at all differs from declaring none.
      expect(byOrg).not.toBe(await addressOf(UNPARTITIONED));
   });

   it("re-addresses when the partition ORDER changes, which is a different layout", async () => {
      const a = await addressOf(
         `#@ persist name="p" storage=credible partition="org_id,s"
source: p is raw -> { select: * }`,
      );
      const b = await addressOf(
         `#@ persist name="p" storage=credible partition="s,org_id"
source: p is raw -> { select: * }`,
      );
      expect(a).not.toBe(b);
   });

   it("leaves an unpartitioned source's address exactly where it was", async () => {
      // The property that keeps every artifact already in a lake from
      // re-addressing — and so rebuilding — the moment this ships.
      //
      // Asserted against the formula this replaced, spelled out, rather than
      // against another unpartitioned source: comparing two of those would hold
      // just as well if BOTH had shifted, which is the failure worth catching.
      const { sources } = await compilePersistSources(
         connections,
         `${HEAD}\n${UNPARTITIONED}`,
      );
      const source = sources["p"];
      expect(computeSourceEntityId(source, { duckdb: "dig-1" })).toBe(
         source.makeBuildId("dig-1", source.getSQL()),
      );
   });
});

describe("the wire plan reports what the build and the read need", () => {
   // The two fields exist for a consumer that supplies its own serve bindings:
   // without them it cannot tell that an artifact is under-filtered on its own,
   // nor which givens a reader has to bind for it to be served correctly.
   async function planFor(body: string) {
      const { sources } = await compilePersistSources(
         connections,
         `${HEAD}\n${body}`,
      );
      const source = sources["p"];
      const fields = deriveAnnotationFields(source);
      const partition = resolvePartitionColumns(source, fields);
      return { source, fields, partition };
   }

   it("reports the resolved partition columns in the author's order", async () => {
      const { partition } = await planFor(
         `#@ persist name="p" storage=credible partition="org_id,s"
source: p is raw -> { select: * }`,
      );
      expect(partition).toEqual({ ok: true, columns: ["org_id", "s"] });
   });

   it("reports nothing for a source that declares no layout", async () => {
      const { partition } = await planFor(
         `#@ persist name="p" storage=credible
source: p is raw -> { select: * }`,
      );
      expect(partition).toEqual({ ok: true, columns: [] });
   });
});

describe("partition= is a recognized persist key", () => {
   // Found by publishing through a real control plane rather than by any unit
   // test: the eligibility gate and the build both honoured `partition=`, while
   // the publish-time unknown-key warning still called it unrecognized and told
   // the author it was "passed through untouched". Every correct use of the key
   // drew a warning saying it did nothing.
   //
   // Asserted against the exported set rather than by publishing, because the
   // set is what the warning reads and a key absent from it cannot be warned
   // about correctly no matter what the rest of the pipeline does.
   it("does not warn an author that a working key is ignored", () => {
      expect(RECOGNIZED_PERSIST_KEYS.has("partition")).toBe(true);
   });

   it("still reports a genuinely unknown key, so the guard is not blanket", () => {
      expect(RECOGNIZED_PERSIST_KEYS.has("parition")).toBe(false);
   });
});
