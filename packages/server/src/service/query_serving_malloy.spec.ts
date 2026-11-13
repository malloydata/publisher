// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Real-compiler contract for `recordingManifest` (query_serving.ts). The path
// report names a colocated table as having answered exactly when Malloy looked
// its manifest entry up, and tells that lookup apart from Malloy's own
// enumeration of the entries by which object each reads. Neither is documented
// Malloy API: both are observed behaviour of the pinned @malloydata/malloy. So
// this file runs real compiles and real queries and checks the recorded keys
// against the SQL Malloy actually ran and the rows it returned. A Malloy bump
// that changes how it reads the manifest fails HERE, loudly, rather than the
// report silently naming tables that did not answer (or omitting ones that did).
import type { DuckDBConnection } from "@malloydata/db-duckdb";
import type {
   BuildManifest,
   FixedConnectionMap,
   ModelMaterializer,
} from "@malloydata/malloy";
import { beforeAll, describe, expect, it } from "bun:test";
import { recordingManifest } from "./query_serving";
import {
   duckdbTestConnections,
   loadTestRuntime,
} from "../test_helpers/incremental_test_harness";

const MODEL = `##! experimental.persistence
source: base is duckdb.sql("""SELECT * FROM (VALUES (1), (2), (3)) AS t(x)""")

#@ persist name="t_a"
source: a is base -> { aggregate: n is count() }

#@ persist name="t_b"
source: b is base -> { group_by: x }
`;

let connections: FixedConnectionMap;
let duckdb: DuckDBConnection;
let materializer: ModelMaterializer;
let ids: Record<string, string>;

beforeAll(async () => {
   ({ duckdb, connections } = duckdbTestConnections());
   // The "built" tables hold rows the live SQL never computes, so an answer
   // read from one is unmistakable.
   await duckdb.runSQL("CREATE TABLE t_a AS SELECT 99::BIGINT AS n");
   await duckdb.runSQL("CREATE TABLE t_b AS SELECT 42 AS x");
   const loaded = loadTestRuntime(connections, MODEL);
   materializer = loaded.materializer;
   const targets = await loaded.runtime.getBuildTargets(
      await materializer.getModel(),
   );
   ids = {};
   for (const build of targets.connections) {
      for (const target of build.targets) {
         for (const source of target.sources) ids[source.name] = target.buildId;
      }
   }
});

function manifest(): BuildManifest {
   return {
      entries: {
         [ids.a]: { tableName: "t_a" },
         [ids.b]: { tableName: "t_b" },
      },
   };
}

/** Prepare and run as `prepareAndRun` does, recording what both read. */
async function run(query: string) {
   const read = new Set<string>();
   const options = { buildManifest: recordingManifest(manifest(), read) };
   const runnable = materializer.loadQuery(query);
   const prepared = await runnable.getPreparedResult(options);
   const result = await runnable.run(options);
   return { read, sql: prepared.sql, rows: result.data.toObject() };
}

describe("recordingManifest against the pinned Malloy", () => {
   it("found a build target for both persisted sources", () => {
      expect(Object.keys(ids).sort()).toEqual(["a", "b"]);
      expect(ids.a).not.toBe(ids.b);
   });

   it("records the one entry Malloy substituted, not the ones it enumerated", async () => {
      const { read, sql, rows } = await run("run: a -> { select: n }");
      expect(sql).toContain("t_a");
      expect(sql).not.toContain("t_b");
      expect(rows).toEqual([{ n: 99 }]);
      expect([...read]).toEqual([ids.a]);
   });

   it("records the other entry for the other source", async () => {
      const { read, sql, rows } = await run("run: b -> { select: x }");
      expect(sql).toContain("t_b");
      expect(rows).toEqual([{ x: 42 }]);
      expect([...read]).toEqual([ids.b]);
   });

   it("records nothing for a query that reads no persisted source", async () => {
      const { read, sql, rows } = await run(
         "run: base -> { aggregate: n is count() }",
      );
      expect(sql).not.toMatch(/t_a|t_b/);
      expect(rows).toEqual([{ n: 3 }]);
      expect([...read]).toEqual([]);
   });

   it("still enumerates the manifest before it compiles (else this spec proves nothing about enumeration)", async () => {
      // Pin the premise: if Malloy stops enumerating, the enumeration half of
      // recordingManifest is dead code and the assertions above would pass
      // with the old, order-based recorder too.
      let enumerations = 0;
      const base = manifest();
      const counted: BuildManifest = {
         ...base,
         entries: new Proxy(base.entries, {
            ownKeys(t) {
               enumerations++;
               return Reflect.ownKeys(t);
            },
         }),
      };
      await materializer
         .loadQuery("run: a -> { select: n }")
         .getPreparedResult({ buildManifest: counted });
      expect(enumerations).toBeGreaterThan(0);
   });
});
