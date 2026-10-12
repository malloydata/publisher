// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { DuckDBConnection } from "@malloydata/db-duckdb";
import type { FixedConnectionMap, PersistSource } from "@malloydata/malloy";
import { beforeAll, describe, expect, it } from "bun:test";
import {
   compilePersistSources,
   duckdbTestConnections,
} from "./incremental_test_harness";
import { strictMissSourceId } from "./materialization_service";

/**
 * Pins the compiler's strict manifest miss as the publisher reads it: the
 * `code` that makes it a miss and, until core carries the id on the error
 * object, the message wording the `sourceID` is read from. A rewording in
 * core fails here, not in a production refusal that suddenly names nobody.
 */
describe("the compiler's strict manifest miss, as the publisher reads it", () => {
   let connections: FixedConnectionMap;
   let duckdb: DuckDBConnection;
   let connectionDigests: Record<string, string>;
   let sources: Record<string, PersistSource>;

   beforeAll(async () => {
      ({ duckdb, connections } = duckdbTestConnections());
      connectionDigests = { duckdb: await duckdb.getDigest() };
      ({ sources } = await compilePersistSources(
         connections,
         `##! experimental.persistence
source: base is duckdb.sql("""SELECT * FROM (VALUES (10, 'A'), (20, 'B')) AS t(amount, category)""")
#@ persist
source: daily is base -> { group_by: category; aggregate: total is amount.sum() }
#@ persist
source: weekly is daily -> { aggregate: grand is total.sum() }
`,
      ));
   });

   it("names the persist source the SQL reads that the manifest lacks, by sourceID", () => {
      let caught: unknown;
      try {
         sources.weekly.getSQL({
            buildManifest: { entries: {}, strict: true } as never,
            connectionDigests,
         });
      } catch (err) {
         caught = err;
      }
      expect(caught).toBeDefined();
      expect((caught as { code?: string }).code).toBe(
         "runtime-manifest-strict-miss",
      );
      const miss = strictMissSourceId(caught);
      expect(miss).toBeDefined();
      expect(miss?.sourceID).toMatch(/^daily@/);
   });

   it("is not raised when the manifest holds the source, nor for a non-strict render", () => {
      const address = sources.daily.makeBuildId(
         connectionDigests.duckdb,
         sources.daily.getSQL(),
      );
      expect(() =>
         sources.weekly.getSQL({
            buildManifest: {
               entries: { [address]: { tableName: "daily_t" } },
               strict: true,
            } as never,
            connectionDigests,
         }),
      ).not.toThrow();
      expect(() =>
         sources.weekly.getSQL({
            buildManifest: { entries: {}, strict: false } as never,
            connectionDigests,
         }),
      ).not.toThrow();
   });
});
