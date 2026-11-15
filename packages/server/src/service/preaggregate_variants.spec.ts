// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// `#(preaggregate)` over a real package (materialization v2, phase 4): the build
// plan lists each rollup once however many models import its base (review bug
// 8), a malformed declaration is refused at publish, a rollup over a stored
// base is planned (#6293), and with the switch off nothing of it runs.
import {
   afterAll,
   afterEach,
   beforeAll,
   beforeEach,
   describe,
   expect,
   it,
} from "bun:test";
import * as fs from "fs/promises";
import * as os from "os";
import * as path from "path";

import { formatPublishRejections } from "./publish_checks";
import { Environment } from "./environment";
import type { Package } from "./package";
import { parsePackageName } from "./identity";
import { writePackageFiles } from "../test_helpers/temp_package";
import { installSettings } from "../test_helpers/settings_env";
import { tempBuildPlanPackages } from "../test_helpers/incremental_test_harness";
import { usePackageLoadPool } from "../test_helpers/temp_package";
import { compilePackageBuildPlan } from "./materialization/plan/build_plan_compile";

const buildPlanPackages = tempBuildPlanPackages();

// The switch is on for the whole file, set before this file's load worker
// starts: a worker keeps the settings it started with.
let fileSettings: (() => void) | undefined;
beforeAll(() => {
   fileSettings = installSettings({
      env: { PUBLISHER_MATERIALIZE: "true", PERSIST_STORAGE_MODE: "on" },
   }).restore;
});
afterAll(() => fileSettings?.());

let rootDir: string;
let envPath: string;
let restore: (() => void) | undefined;

beforeEach(async () => {
   rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-rollup-v2-"));
   envPath = path.join(rootDir, "env");
   await fs.mkdir(envPath, { recursive: true });
});

afterEach(async () => {
   restore?.();
   restore = undefined;
   await buildPlanPackages.cleanup();
   await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
});

function switchOn(on: boolean): void {
   restore = installSettings({
      env: {
         PUBLISHER_MATERIALIZE: on ? "true" : "false",
         PERSIST_STORAGE_MODE: "on",
      },
   }).restore;
}

async function loadPackage(files: Record<string, string>): Promise<Package> {
   writePackageFiles(path.join(envPath, "pkg"), files, {
      manifest: { name: "pkg", description: "fixture" },
   });
   const env = await Environment.create("testEnv", envPath, []);
   await env.addPackage("pkg");
   return env.getPackage(parsePackageName("pkg"), false);
}

const ORDERS = `source: orders is duckdb.sql("""
  SELECT * FROM (VALUES (10, 'A', 'x'), (20, 'B', 'y')) AS t(amount, category, region)
""") extend {
  #(preaggregate) grain=["category"]
  measure: total is amount.sum()
  #(preaggregate) grain=["category"]
  measure: n is count()
}
`;

function rollups(pkg: Package) {
   return Object.entries(pkg.getBuildPlan()?.sources ?? {}).filter(
      ([, s]) => s.origin === "preaggregate",
   );
}

describe("#(preaggregate) in a package", () => {
   usePackageLoadPool();
   it(
      "lists a rollup once, under its declaring model, however many models import its base",
      async () => {
         switchOn(true);
         const pkg = await loadPackage({
            "orders.malloy": ORDERS,
            "index.malloy": `import "orders.malloy"\nsource: all_orders is orders`,
            "more.malloy": `import { orders } from "orders.malloy"`,
         });
         const planned = rollups(pkg);
         expect(planned).toHaveLength(1);
         const [sourceID, source] = planned[0] ?? [];
         expect(sourceID).toContain("orders.malloy.preaggregate.malloy");
         expect(source?.modelPath).toBe("orders.malloy");
         expect(source?.preaggregate?.baseSourceName).toBe("orders");
         expect(source?.preaggregate?.measures).toEqual(["n", "total"]);
         expect(source?.name).toMatch(
            /^orders__rollup__category__[0-9a-f]{8}$/,
         );
      },
      { timeout: 60000 },
   );

   it(
      "plans a storage= rollup over a storage= base instead of leaving it out (#6293)",
      async () => {
         switchOn(true);
         const pkg = await loadPackage({
            "m.malloy": `##! experimental.persistence
#@ persist storage="lake"
source: daily is duckdb.sql("SELECT 10 AS amount, 'A' AS category") -> {
  group_by: category
  aggregate: amount_sum is amount.sum()
} extend {
  #(preaggregate) grain=["category"] storage="lake"
  measure: total is amount_sum.sum()
}
`,
         });
         expect(rollups(pkg)).toHaveLength(1);
         expect(
            pkg
               .annotationFindingWarnings()
               .map((w) => w.code)
               .filter((c) => c === "preaggregate-over-stored-base"),
         ).toEqual([]);
      },
      { timeout: 60000 },
   );

   it(
      "refuses a malformed declaration at publish, and warns at load",
      async () => {
         switchOn(true);
         const pkg = await loadPackage({
            "m.malloy": `source: orders is duckdb.sql("SELECT 10 AS amount, 'A' AS category") extend {
  #(preaggregate) grain=["category"]
  measure: average is amount.avg()
}
`,
         });
         expect(rollups(pkg)).toEqual([]);
         const warning = pkg
            .annotationFindingWarnings()
            .find((w) => w.code === "preaggregate-not-mergeable");
         expect(warning?.severity).toBe("error");
         expect(formatPublishRejections(pkg)).toContain(
            "its partials cannot be merged",
         );
      },
      { timeout: 60000 },
   );

   it(
      "warns, and serves live, for a well-formed declaration it cannot honour",
      async () => {
         switchOn(true);
         const pkg = await loadPackage({
            "m.malloy": `source: carriers is duckdb.sql("SELECT 'A' AS code, 3 AS fleet") extend { primary_key: code }
source: orders is duckdb.sql("SELECT 10 AS amount, 'A' AS category, 'A' AS carrier") extend {
  join_one: carriers with carrier
  #(preaggregate) grain=["category"]
  measure: fleet is carriers.fleet.sum()
}
`,
         });
         expect(rollups(pkg)).toEqual([]);
         const warning = pkg
            .annotationFindingWarnings()
            .find((w) => w.code === "preaggregate-join-level");
         expect(warning?.severity).toBe("warn");
         expect(formatPublishRejections(pkg)).toBeUndefined();
      },
      { timeout: 60000 },
   );

   it(
      "does nothing with the switch off, and says #(preaggregate) is not read",
      async () => {
         switchOn(false);
         // Compiled on this thread: a load worker already running keeps the
         // settings it started with.
         const plan = await compilePackageBuildPlan(
            await buildPlanPackages.create({ "orders.malloy": ORDERS }),
         );
         expect(Object.keys(plan.preaggregatePlans ?? {})).toEqual([]);
         const pkg = await loadPackage({ "orders.malloy": ORDERS });
         expect(pkg.annotationFindingWarnings().map((w) => w.code)).toContain(
            "annotation-near-miss",
         );
      },
      { timeout: 60000 },
   );
});
