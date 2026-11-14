// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A `#(preaggregate)` rollup whose build reads a given is left out of the
 * build plan with a `preaggregate-reads-given` warning: its table would hold
 * one given value's rows, served to every caller. A rollup in the same model
 * that reads no given is still planned. Drives `Package.create` on the worker
 * path with PUBLISHER_MATERIALIZE on, as the package warnings specs do.
 */
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { FixedConnectionMap, MalloyConfig } from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import {
   PackageLoadPool,
   __setPackageLoadPoolForTests,
} from "../../../package_load/package_load_pool";
import { installSettings } from "../../../test_helpers/settings_env";
import { writeTempPackage } from "../../../test_helpers/temp_package";
import { Package } from "../../package";

const ROWS = `duckdb.sql("select * from (values (1,'east','A',10.0),(2,'west','B',20.0)) as t(id, region, category, amount)")`;

const MODEL = `##! experimental.givens
given: REGION :: string is 'east'

source: scoped is ${ROWS} extend {
  where: region = $REGION
  #(preaggregate) grain=["category"]
  measure: total is amount.sum()
}

source: open is ${ROWS} extend {
  #(preaggregate) grain=["category"]
  measure: total is amount.sum()
}
`;

describe("#(preaggregate) rollups whose build reads a given", () => {
   let tempDir: string;
   let pool: PackageLoadPool;
   let duckdb: DuckDBConnection;
   let restore: () => void;
   let pkg: Package;

   beforeAll(async () => {
      pool = new PackageLoadPool(1);
      await __setPackageLoadPoolForTests(pool);
      restore = installSettings({
         env: { PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      tempDir = writeTempPackage(
         {},
         {
            prefix: "publisher-rollup-reads-given-",
            manifest: { name: "pkg", description: "rollup reads a given" },
         },
      ).dir;
      fs.writeFileSync(path.join(tempDir, "m.malloy"), MODEL);
      duckdb = new DuckDBConnection("duckdb", ":memory:");
      const config = new MalloyConfig({ connections: {} });
      config.wrapConnections(
         () => new FixedConnectionMap(new Map([["duckdb", duckdb]]), "duckdb"),
      );
      pkg = await Package.create("env", "pkg", tempDir, config);
   });

   afterAll(async () => {
      restore();
      await duckdb.close();
      await __setPackageLoadPoolForTests(null);
      fs.rmSync(tempDir, { recursive: true, force: true });
   });

   it("leaves the rollup out with a warning", () => {
      const warnings = (pkg.getPackageMetadata().warnings ?? []).filter(
         (w) => w.code === "preaggregate-reads-given",
      );
      expect(warnings.map((w) => [w.model, w.subject])).toEqual([
         ["m.malloy", "scoped"],
      ]);
      expect(warnings[0].message).toContain("read a given");
      const planned = Object.values(pkg.getBuildPlan()?.sources ?? {}).map(
         (s) => s.name,
      );
      expect(planned.some((n) => n.startsWith("scoped__rollup__"))).toBe(false);
   });

   it("still plans a rollup in the same model that reads no given", () => {
      const planned = Object.values(pkg.getBuildPlan()?.sources ?? {}).map(
         (s) => s.name,
      );
      expect(planned.some((n) => n.startsWith("open__rollup__"))).toBe(true);
   });
});
