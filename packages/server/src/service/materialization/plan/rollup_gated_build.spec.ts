// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Rows 1 and 21 on the build side: a `#(preaggregate)` rollup over a gated
 * base is planned and built (not refused by the colocated tier's `authorize`
 * check) only when a query can read it past the gate, which is
 * `rollupGateEnforcedAtRead`: `materialize.gatedSources` on, colocated.
 * With the switch off it is not planned at all (`preaggregate-access-filter`),
 * and a `storage=` one waits for question 14's review either way. Drives
 * `Package.create` on the worker path, as the package warnings specs do.
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

const MODEL = `##! experimental { givens access_modifiers }
given: REGION :: string

#(access_filter) region = $REGION
source: gated is ${ROWS} extend {
  #(preaggregate) grain=["category", "region"]
  measure: total is amount.sum()
  #(preaggregate) grain=["category", "region"] storage="lake"
  measure: stored_total is amount.sum()
}

#(access_filter) tenant = $REGION
source: hidden is ${ROWS} extend {
  internal dimension: tenant is region
  #(preaggregate) grain=["category", "tenant"]
  measure: total is amount.sum()
}
`;

for (const gated of [true, false]) {
   describe(`#(preaggregate) over a gated base, gated sources ${gated ? "on" : "off"} (rows 1, 21)`, () => {
      let tempDir: string;
      let pool: PackageLoadPool;
      let duckdb: DuckDBConnection;
      let restore: () => void;
      let pkg: Package;

      beforeAll(async () => {
         pool = new PackageLoadPool(1);
         await __setPackageLoadPoolForTests(pool);
         restore = installSettings({
            env: {
               PUBLISHER_MATERIALIZE: "true",
               PUBLISHER_MATERIALIZE_GATED_SOURCES: gated ? "true" : "false",
            },
         }).restore;
         tempDir = writeTempPackage(
            {},
            {
               prefix: "publisher-rollup-gated-build-",
               manifest: { name: "pkg", description: "gated rollups" },
            },
         ).dir;
         fs.writeFileSync(path.join(tempDir, "m.malloy"), MODEL);
         duckdb = new DuckDBConnection("duckdb", ":memory:");
         const config = new MalloyConfig({ connections: {} });
         config.wrapConnections(
            () =>
               new FixedConnectionMap(new Map([["duckdb", duckdb]]), "duckdb"),
         );
         pkg = await Package.create("env", "pkg", tempDir, config);
      });

      afterAll(async () => {
         restore();
         await duckdb.close();
         await __setPackageLoadPoolForTests(null);
         fs.rmSync(tempDir, { recursive: true, force: true });
      });

      const planned = () =>
         Object.values(pkg.getBuildPlan()?.sources ?? {}).map((s) => s.name);
      const refused = () =>
         Object.values(pkg.getBuildPlan()?.refusedSources ?? {}).map(
            (s) => s.name,
         );
      const warned = () =>
         (pkg.getPackageMetadata().warnings ?? [])
            .filter((w) => w.code === "preaggregate-access-filter")
            .map((w) => w.subject);

      it(
         gated
            ? "plans the colocated rollup and does not refuse it for its gate"
            : "plans no rollup over the gated base, with a warning",
         () => {
            const colocated = planned().filter((n) =>
               n.startsWith("gated__rollup__"),
            );
            expect(colocated.length).toBe(gated ? 1 : 0);
            expect(refused().some((n) => n.startsWith("gated__rollup__"))).toBe(
               false,
            );
            expect(warned()).toContain(
               gated ? "gated.stored_total" : "gated.total",
            );
         },
      );

      it(
         gated
            ? "reports the gated rollup's serve verdict without a gate veto (row 1)"
            : "plans nothing to report a verdict on",
         () => {
            const verdicts = (
               pkg as unknown as {
                  materialization: {
                     serveVerdicts(): { sourceName: string; reason?: string }[];
                  };
               }
            ).materialization
               .serveVerdicts()
               .filter((v) => v.sourceName.startsWith("gated__rollup__"));
            expect(verdicts.length).toBe(gated ? 1 : 0);
            for (const v of verdicts) {
               expect(v.reason).not.toBe("row_level_gate");
            }
         },
      );

      it("leaves the storage= rollup out until the managed-tier review", () => {
         expect(warned()).toContain("gated.stored_total");
      });

      it(
         gated
            ? "plans a rollup whose grain stores an internal gate field"
            : "plans no rollup over the internal-gated base",
         () => {
            expect(
               planned().some((n) => n.startsWith("hidden__rollup__")),
            ).toBe(gated);
            expect(
               refused().some((n) => n.startsWith("hidden__rollup__")),
            ).toBe(false);
         },
      );
   });
}
