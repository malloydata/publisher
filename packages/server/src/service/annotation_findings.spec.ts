// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The outcome tiers for `#@` annotation findings, over real packages: what a
 * load reports in `Package.warnings` (malformed is `error`, unservable `warn`),
 * what publish refuses (malformed), and the one metric that counts them.
 */

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs/promises";
import * as os from "os";
import * as path from "path";
import { resetMaterializationTelemetryForTesting } from "../materialization_metrics";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "../test_helpers/metrics_harness";
import { writePackageFiles } from "../test_helpers/temp_package";
import { Environment } from "./environment";
import { parsePackageName } from "./identity";
import type { Package } from "./package";
import { formatPublishRejections } from "./publish_checks";

let rootDir: string;
let envPath: string;
let harness: MetricsHarness;

beforeEach(async () => {
   rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-findings-"));
   envPath = path.join(rootDir, "env");
   await fs.mkdir(envPath, { recursive: true });
   harness = await startMetricsHarness();
   resetMaterializationTelemetryForTesting();
});

afterEach(async () => {
   resetMaterializationTelemetryForTesting();
   await harness.shutdown();
   await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
});

async function loadPackage(model: string): Promise<Package> {
   writePackageFiles(
      path.join(envPath, "pkg"),
      { "model.malloy": model, "rows.csv": "a\n1\n" },
      { manifest: { name: "pkg", description: "fixture" } },
   );
   const env = await Environment.create("testEnv", envPath, []);
   await env.addPackage("pkg");
   return env.getPackage(parsePackageName("pkg"), false);
}

const RAW = `##! experimental { persistence composite_sources }
source: raw is duckdb.sql("""SELECT 1 AS amount, 'A' AS category""")
`;
const ROLLUP = `source: daily is raw -> { group_by: category; aggregate: total is amount.sum() }`;

function coded(pkg: Package, code: string) {
   return (pkg.getPackageMetadata().warnings ?? []).filter(
      (w) => w.code === code,
   );
}

describe("annotation findings on a loaded package", () => {
   it(
      "loads a package whose #@ tag does not parse, with an error, and publish refuses it",
      async () => {
         const pkg = await loadPackage(`${RAW}#@ persist name="t\n${ROLLUP}\n`);
         const [warning] = coded(pkg, "persist-unparseable");
         expect(warning).toMatchObject({
            severity: "error",
            model: "model.malloy",
         });
         expect(warning.message).toContain("does not parse");
         // Malloy drops the source, so it is served live and nothing is planned.
         expect(Object.keys(pkg.getBuildPlan()?.sources ?? {})).toEqual([]);
         // Already published, it keeps loading; published anew, it is refused.
         expect(formatPublishRejections(pkg)).toContain("does not parse");
         expect(
            await harness.collectCounter(
               "publisher_annotation_findings_total",
               {
                  code: "persist-unparseable",
                  tier: "malformed",
               },
            ),
         ).toBe(1);
      },
      { timeout: 30000 },
   );

   it(
      "reports a retired key as an error with its rewrite",
      async () => {
         const pkg = await loadPackage(
            `${RAW}#@ persist name="t" schedule="0 * * * *"\n${ROLLUP}\n`,
         );
         const [warning] = coded(pkg, "persist-retired-key");
         expect(warning).toMatchObject({ severity: "error", subject: "daily" });
         expect(warning.message).toContain("materialization.schedule");
         // Refused at publish, as it was before the finding existed.
         expect(formatPublishRejections(pkg)).toContain("schedule=");
      },
      { timeout: 30000 },
   );

   it(
      "reports an unknown key on a plain persist source as a warning, and publish allows it",
      async () => {
         const pkg = await loadPackage(
            `${RAW}#@ persist name="t" merge_keys="category"\n${ROLLUP}\n`,
         );
         const [warning] = coded(pkg, "persist-unknown-key");
         expect(warning).toMatchObject({ severity: "warn", subject: "daily" });
         expect(warning.message).toContain("merge_keys=");
         expect(formatPublishRejections(pkg)).toBeUndefined();
         expect(
            await harness.collectCounter(
               "publisher_annotation_findings_total",
               {
                  code: "persist-unknown-key",
                  tier: "unservable",
               },
            ),
         ).toBe(1);
      },
      { timeout: 30000 },
   );

   it(
      "codes a persist source that cannot be materialized",
      async () => {
         const pkg = await loadPackage(
            `${RAW}#@ persist\nsource: passthrough is duckdb.table('rows.csv') extend { where: a > 0 }\n`,
         );
         const [warning] = coded(pkg, "persist-not-materializable");
         expect(warning).toMatchObject({
            severity: "warn",
            subject: "passthrough",
         });
      },
      { timeout: 30000 },
   );

   it(
      "codes a pre-aggregation of a field the source hides",
      async () => {
         const pkg = await loadPackage(`${RAW}source: orders is raw extend {
  #@ preaggregate grain="category"
  private measure: total is amount.sum()
}
`);
         const [warning] = coded(pkg, "preaggregate-hidden-field");
         expect(warning).toMatchObject({ severity: "warn" });
         expect(
            await harness.collectCounter(
               "publisher_annotation_findings_total",
               {
                  code: "preaggregate-hidden-field",
               },
            ),
         ).toBe(1);
      },
      { timeout: 30000 },
   );

   it(
      "reports nothing for a well-formed persist source",
      async () => {
         const pkg = await loadPackage(
            `${RAW}#@ persist name="t"\n${ROLLUP}\n`,
         );
         expect(
            (pkg.getPackageMetadata().warnings ?? []).filter((w) =>
               /^(persist|preaggregate)-/.test(w.code ?? ""),
            ),
         ).toEqual([]);
      },
      { timeout: 30000 },
   );
});
