// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A query over a stored string-gated table waits for a capability read
 * still running only as long as `stringCapabilitiesOf` bounds it: a catalog
 * read that hangs (an unreachable host) declines the stored answer, which
 * is answered live within the bound, and once the read lands the table is
 * served. Through the real `Model.runQuery` over an in-memory DuckDB, a
 * query joining a source behind a string row gate; the stored tables hold
 * 99, the live sources 10.
 */
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { FixedConnectionMap, MalloyConfig } from "@malloydata/malloy";
import { afterAll, afterEach, describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import { unrecordedAudit } from "./access_record";
import type { Binding } from "./materialization/domain/binding";
import {
   refreshStringCapabilities,
   setCapabilityWaitMsForTests,
} from "./materialization/serve/source_capability";
import { Model } from "./model";
import { installSettings } from "../test_helpers/settings_env";

const SRC = `##! experimental.givens
given:
  REG :: string[]
#(access_filter) region_id in $REG
source: regions is duckdb.sql("SELECT 'r1' AS region_id, 'LIVE' AS region_name")
source: orders is duckdb.sql("SELECT 10 AS amount, 'r1' AS region_id") extend {
  join_one: author_regions is regions on region_id = author_regions.region_id
}
`;

const BINDING: Binding = {
   kind: "persist",
   tier: "storage",
   sourceName: "orders",
   destinationName: "duckdb",
   virtualHandle: "orders_h",
   tablePath: "orders_mz",
   schema: [
      { name: "amount", type: "BIGINT" },
      { name: "region_id", type: "VARCHAR" },
   ],
};

const REGIONS: Binding = {
   kind: "persist",
   tier: "storage",
   sourceName: "regions",
   destinationName: "duckdb",
   virtualHandle: "regions_h",
   tablePath: "regions_mz",
   schema: [
      { name: "region_id", type: "VARCHAR" },
      { name: "region_name", type: "VARCHAR" },
   ],
};

const dirs: string[] = [];
let restore: (() => void) | undefined;
afterEach(() => {
   restore?.();
   restore = undefined;
});
afterAll(() => {
   for (const d of dirs) rmSync(d, { recursive: true, force: true });
});

describe("a capability read that hangs", () => {
   it("declines the stored answer within the bound, and serves it once the read lands", async () => {
      const settings = installSettings({
         env: { PERSIST_STORAGE_MODE: "on" },
      }).restore;
      const wait = setCapabilityWaitMsForTests(100);
      restore = () => {
         wait();
         settings();
      };
      const dir = mkdtempSync(join(tmpdir(), "mz-cap-wait-"));
      dirs.push(dir);
      writeFileSync(join(dir, "m.malloy"), SRC);
      const duckdb = new DuckDBConnection("duckdb", ":memory:");
      await duckdb.runSQL(
         "CREATE OR REPLACE TABLE orders_mz AS SELECT 99 AS amount, 'r1' AS region_id",
      );
      await duckdb.runSQL(
         "CREATE OR REPLACE TABLE regions_mz AS SELECT 'r1' AS region_id, 'STORE' AS region_name",
      );
      const connMap = new Map([["duckdb", duckdb]]);
      const model = await Model.create("pkg", dir, "m.malloy", connMap);
      const serveConfig = new MalloyConfig({ connections: {} });
      serveConfig.wrapConnections(
         () => new FixedConnectionMap(connMap, "duckdb"),
      );
      model.setServeDestinationConfig(() => serveConfig);
      model.setServedCopyContext(dir, connMap);

      // The read reaches the catalog only once the spec releases it.
      let release!: () => void;
      const released = new Promise<void>((resolve) => (release = resolve));
      model.setStringCapabilityReader((runtime, args) =>
         refreshStringCapabilities(runtime, {
            ...args,
            connection: async (name) => {
               await released;
               return args.connection(name);
            },
         }),
      );
      const reading = model.refreshStringCapabilities();
      model.setServeBindings([BINDING, REGIONS]);

      const total = async () => {
         const res = await model.runQuery(unrecordedAudit(), {
            query: "run: orders -> { group_by: author_regions.region_name; aggregate: t is amount.sum() }",
            givens: { REG: ["r1"] },
         });
         return Number((res.compactResult as unknown as { t: number }[])[0].t);
      };

      const started = Date.now();
      expect(await total()).toBe(10);
      expect(Date.now() - started).toBeLessThan(2_000);

      release();
      await reading;
      expect(await total()).toBe(99);
   });
});
