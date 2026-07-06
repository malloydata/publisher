/**
 * Serve-time manifest routing: a query on a `#@ persist` source must compile to
 * SQL that reads the materialized physical table, not the base table it was
 * rolled up from.
 *
 * This is the in-repo proof of the persistence v0 serve-path contract. The
 * control-plane builds a table and distributes a manifest keyed by
 * `sourceEntityId`; the publisher binds that manifest onto a package and threads
 * it into the query `Runtime` (`makeHydrationRuntime` on the serve path,
 * `getModelRuntime` on the create/compile path — both construct the `Runtime`
 * with the same `{ entries, strict:false }` option). With the manifest bound,
 * Malloy substitutes the persisted source for `(SELECT * FROM <table>)` at
 * getSQL time; without it, the source recomputes from the base table.
 *
 * The two keys have to agree for routing to fire: the build plan emits
 * `computeSourceEntityId(source) = source.makeBuildId(connDigest, source.getSQL())`,
 * and Malloy looks the entry up at serve time via `mkBuildID(connDigest, sql)`
 * over the same connection digest and canonical SQL. This test computes the
 * build-plan id, binds a manifest under that id, and asserts the compiled SQL
 * routes — so a future drift in either the id recipe or the runtime threading
 * fails here instead of silently serving live.
 */

import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   Connection,
   FixedConnectionMap,
   MalloyConfig,
} from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import type { BuildManifest } from "../storage/DatabaseInterface";
import type { BuildPlanPackage } from "./build_plan";
import { computePackageBuildPlan } from "./build_plan";
import { Model } from "./model";

const TEST_DIR = path.join(os.tmpdir(), "manifest-routing-tests");
const TEST_DB_DIR = path.join(TEST_DIR, "db");
const TEST_DB_PATH = path.join(TEST_DB_DIR, "test.duckdb");
const TEST_PKG_DIR = path.join(TEST_DIR, "pkg");
const MODEL_PATH = "rollup.malloy";
const PERSIST_SOURCE = "orders_rollup";
// Bare lowercase identifier — canonical for the DuckDB dialect, which the Malloy
// runtime enforces on every bound manifest entry. Deliberately shares no
// substring with the base table `orders`, so `/\borders\b/` can't false-match it.
const MATERIALIZED_TABLE = "mv_rollup";
const QUERY = `run: ${PERSIST_SOURCE} -> { group_by: status, n }`;

// A persist source that rolls up the base `orders` table. It is a query-derived
// source (`query_source`) — the shape a materialized rollup actually takes — so
// Malloy's serve-time manifest lookup applies to it. `##! experimental.persistence`
// is required for Malloy to honor a runtime-level manifest at all.
const ROLLUP_MODEL = `##! experimental.persistence

source: orders_base is duckdb.table('orders')

#@ persist name="${MATERIALIZED_TABLE}"
source: ${PERSIST_SOURCE} is orders_base -> { group_by: status; aggregate: n is count() }
`;

const SEED_SQL = `
CREATE TABLE IF NOT EXISTS orders (
   id INTEGER,
   status VARCHAR
);
INSERT INTO orders VALUES
   (1, 'new'),
   (2, 'shipped'),
   (3, 'new');
`;

let duckdbConnection: DuckDBConnection;
let malloyConfig: MalloyConfig;

beforeAll(async () => {
   await fs.mkdir(TEST_DB_DIR, { recursive: true });
   await fs.mkdir(TEST_PKG_DIR, { recursive: true });
   duckdbConnection = new DuckDBConnection("duckdb", TEST_DB_PATH, TEST_DB_DIR);
   for (const stmt of SEED_SQL.trim().split(";").filter(Boolean)) {
      await duckdbConnection.runSQL(stmt.trim() + ";");
   }
   await fs.writeFile(
      path.join(TEST_PKG_DIR, MODEL_PATH),
      ROLLUP_MODEL,
      "utf-8",
   );

   malloyConfig = new MalloyConfig({ connections: {} });
   malloyConfig.wrapConnections(
      () =>
         new FixedConnectionMap(
            new Map<string, Connection>([["duckdb", duckdbConnection]]),
            "duckdb",
         ),
   );
});

afterAll(async () => {
   try {
      await duckdbConnection.close();
      await new Promise((resolve) => setTimeout(resolve, 100));
      await fs.rm(TEST_DIR, { recursive: true, force: true });
   } catch {
      // Ignore cleanup errors.
   }
});

/** The BuildPlanPackage surface over the fixture package, so we can compute the
 *  same `sourceEntityId` the control plane would key its manifest by. */
function buildPlanPackage(): BuildPlanPackage {
   return {
      getModelPaths: () => [MODEL_PATH],
      getPackagePath: () => TEST_PKG_DIR,
      getMalloyConfig: () => malloyConfig,
      getMalloyConnection: (name: string) =>
         malloyConfig.connections.lookupConnection(name),
   };
}

/** Compile {@link QUERY} to SQL through the serve runtime, optionally binding a
 *  build manifest — mirrors the `Runtime` construction of `makeHydrationRuntime`. */
async function compileSql(
   buildManifest?: BuildManifest["entries"],
): Promise<string> {
   const { runtime, modelURL, importBaseURL } = await Model.getModelRuntime(
      TEST_PKG_DIR,
      MODEL_PATH,
      malloyConfig,
      buildManifest ? { buildManifest } : undefined,
   );
   return runtime
      .loadModel(modelURL, { importBaseURL })
      .loadQuery(QUERY)
      .getSQL();
}

describe("serve-time manifest routing", () => {
   it(
      "routes a persisted source's query to the materialized table",
      async () => {
         // The sourceEntityId the build plan emits — the same key the manifest
         // is written under and the same key Malloy recomputes at serve time.
         const plan = await computePackageBuildPlan(buildPlanPackage());
         const source = Object.values(plan?.sources ?? {}).find(
            (s) => s.name === PERSIST_SOURCE,
         );
         expect(source).toBeDefined();
         const sourceEntityId = source!.sourceEntityId;
         expect(sourceEntityId).toBeTruthy();

         const entries: BuildManifest["entries"] = {
            [sourceEntityId]: { tableName: MATERIALIZED_TABLE },
         };

         const liveSql = await compileSql();
         const boundSql = await compileSql(entries);

         // Live (unbound) recomputes from the base table.
         expect(liveSql).toMatch(/\borders\b/i);
         expect(liveSql).not.toContain(MATERIALIZED_TABLE);

         // Bound routes to the materialized table and no longer scans the base.
         expect(boundSql).toContain(MATERIALIZED_TABLE);
         expect(boundSql).not.toMatch(/\borders\b/i);

         // The two must differ — the whole point of the manifest.
         expect(boundSql).not.toBe(liveSql);
      },
      { timeout: 30000 },
   );

   it(
      "serves live (unchanged) when the manifest has no entry for the source",
      async () => {
         // A manifest whose only entry is for some other source must not
         // reroute ours — strict:false keeps it serving live.
         const liveSql = await compileSql();
         const unrelatedSql = await compileSql({
            "some-other-source-entity-id": { tableName: "unrelated_table" },
         });
         expect(unrelatedSql).toMatch(/\borders\b/i);
         expect(unrelatedSql).not.toContain(MATERIALIZED_TABLE);
         expect(unrelatedSql).toBe(liveSql);
      },
      { timeout: 30000 },
   );
});
