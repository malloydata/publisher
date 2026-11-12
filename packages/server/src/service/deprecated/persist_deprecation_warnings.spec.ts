// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// DEPRECATED FEATURE COVERAGE: delete with the `#@ -persist` and
// `#@ preaggregate` readers (refused at 1.0.0, removed in 1.1).

/**
 * With materialization v2 switched on (`PUBLISHER_MATERIALIZE`), a package
 * that declares `#@ -persist` gets one coded warning per declaration, in the
 * file that declares it, naming the v2 rewrite. Off, it gets none. The
 * `#@ preaggregate` warning is held back even with the switch on, until
 * `#(preaggregate)` is read (materialization v2 phase 4), but its usage is
 * still counted. Either way the package loads and builds as before. Drives
 * `Package.create` on the worker path, as the other warnings specs do.
 */
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { FixedConnectionMap, MalloyConfig } from "@malloydata/malloy";
import {
   afterAll,
   afterEach,
   beforeAll,
   beforeEach,
   describe,
   expect,
   it,
} from "bun:test";
import * as fs from "fs";
import * as path from "path";
import {
   PackageLoadPool,
   __setPackageLoadPoolForTests,
} from "../../package_load/package_load_pool";
import { resetDeprecatedUsageMetricsForTest } from "../../deprecated_usage_metrics";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "../../test_helpers/metrics_harness";
import { installSettings } from "../../test_helpers/settings_env";
import { writeTempPackage } from "../../test_helpers/temp_package";
import { Package } from "../package";

const ROWS = `duckdb.sql("select * from (values (1,'east',10),(2,'west',20)) as t(id, region, amt)")`;

const MODEL = `##! experimental.persistence

#@ persist name="summary"
source: summary is ${ROWS} -> { group_by: region; aggregate: total is amt.sum() }

#@ -persist
source: summary_fresh is summary extend { dimension: r2 is region }

source: fresh_ext is summary_fresh extend { dimension: r3 is region }

source: orders is ${ROWS} extend {
  #@ preaggregate grain="region"
  #@ preaggregate grain="id"
  measure: total is amt.sum()
  measure: n is count()
}

source: orders_ext is orders extend { dimension: big is amt > 10 }
`;

const CODES = new Set([
   "deprecated-persist-opt-out",
   "deprecated-preaggregate-annotation",
]);

describe("deprecation warnings for #@ -persist and #@ preaggregate", () => {
   let tempDir: string;
   let pool: PackageLoadPool;
   let duckdb: DuckDBConnection;
   let restore: (() => void) | undefined;

   beforeAll(async () => {
      pool = new PackageLoadPool(1);
      await __setPackageLoadPoolForTests(pool);
   });

   afterAll(async () => {
      await __setPackageLoadPoolForTests(null);
   });

   beforeEach(() => {
      tempDir = writeTempPackage(
         {},
         {
            prefix: "publisher-v1-deprecations-",
            manifest: { name: "pkg", description: "v1 persist annotations" },
         },
      ).dir;
      duckdb = new DuckDBConnection("duckdb", ":memory:");
   });

   afterEach(async () => {
      restore?.();
      restore = undefined;
      await duckdb.close();
      fs.rmSync(tempDir, { recursive: true, force: true });
   });

   function write(file: string, text: string): void {
      const target = path.join(tempDir, file);
      fs.mkdirSync(path.dirname(target), { recursive: true });
      fs.writeFileSync(target, text);
   }

   async function load(materialize: boolean): Promise<Package> {
      restore?.();
      restore = installSettings({
         env: { PUBLISHER_MATERIALIZE: materialize ? "true" : "false" },
      }).restore;
      const config = new MalloyConfig({ connections: {} });
      config.wrapConnections(
         () => new FixedConnectionMap(new Map([["duckdb", duckdb]]), "duckdb"),
      );
      return Package.create("env", "pkg", tempDir, config);
   }

   async function deprecations(materialize: boolean) {
      const pkg = await load(materialize);
      return (pkg.getPackageMetadata().warnings ?? []).filter(
         (w) => w.code !== undefined && CODES.has(w.code),
      );
   }

   it("warns once per declaration, in its own file, naming the v2 rewrite", async () => {
      write("models/v1.malloy", MODEL);
      // An importer, and extensions in the declaring file, carry the same
      // notes but declare none: none of them is reported.
      write(
         "models/uses.malloy",
         `##! experimental.persistence\nimport { summary_fresh, orders } from "v1.malloy"\n\nsource: again is summary_fresh extend {}\nsource: more is orders extend {}\n`,
      );

      const found = await deprecations(true);

      expect(found.map((w) => [w.model, w.subject, w.code])).toEqual([
         ["models/v1.malloy", "summary_fresh", "deprecated-persist-opt-out"],
      ]);
      expect(found[0].severity).toBe("warn");
      expect(found[0].message).toContain("docs/materialization-migration.md");
      expect(found[0].message).toContain(
         "Source summary_fresh uses #@ -persist, which is deprecated",
      );
      expect(found[0].message).toContain("#(materialize)");
   });

   it("holds back the #@ preaggregate warning with the switch on, but still counts it", async () => {
      write("models/v1.malloy", MODEL);
      const harness: MetricsHarness = await startMetricsHarness();
      resetDeprecatedUsageMetricsForTest();
      try {
         const found = await deprecations(true);

         expect(
            found.some((w) => w.code === "deprecated-preaggregate-annotation"),
         ).toBe(false);
         // One per measure, however many grains it declares.
         expect(
            await harness.collectCounter("publisher_deprecated_usage_total", {
               feature: "preaggregate_annotation",
            }),
         ).toBe(1);
      } finally {
         resetDeprecatedUsageMetricsForTest();
         await harness.shutdown();
      }
   });

   it("warns about nothing while materialization v2 is switched off", async () => {
      write("models/v1.malloy", MODEL);

      expect(await deprecations(false)).toEqual([]);
   });

   it("changes nothing else: the build plan is the same with the warnings on", async () => {
      write("models/v1.malloy", MODEL);
      const planned = async (materialize: boolean) =>
         Object.values((await load(materialize)).getBuildPlan()?.sources ?? {})
            .map((s) => `${s.origin}:${s.sourceEntityId}`)
            .sort();

      const off = await planned(false);
      const on = await planned(true);

      expect(off.length).toBeGreaterThan(0);
      expect(off.some((s) => s.startsWith("preaggregate:"))).toBe(true);
      expect(on).toEqual(off);
   });
});
