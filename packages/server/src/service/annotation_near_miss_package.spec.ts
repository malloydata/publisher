// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The near-miss scan over real packages: what a load reports in
 * `Package.warnings`, that it never refuses a publish, that a shared import is
 * reported once, and that the findings metric counts it.
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
import {
   ANNOTATION_MALFORMED_ROUTE_CODE,
   ANNOTATION_NEAR_MISS_CODE,
} from "./annotation_near_miss";
import { Environment } from "./environment";
import { parsePackageName } from "./identity";
import type { Package } from "./package";
import { formatPublishRejections } from "./publish_checks";

let rootDir: string;
let envPath: string;
let harness: MetricsHarness;

beforeEach(async () => {
   rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-near-miss-"));
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

async function loadPackage(files: Record<string, string>): Promise<Package> {
   writePackageFiles(
      path.join(envPath, "pkg"),
      { ...files, "rows.csv": "a\n1\n" },
      { manifest: { name: "pkg", description: "fixture" } },
   );
   const env = await Environment.create("testEnv", envPath, []);
   await env.addPackage("pkg");
   return env.getPackage(parsePackageName("pkg"), false);
}

function coded(pkg: Package, code: string) {
   return (pkg.getPackageMetadata().warnings ?? []).filter(
      (w) => w.code === code,
   );
}

const BASE = `##! experimental { persistence }
source: raw is duckdb.sql("""SELECT 1 AS amount, 'A' AS category""")
`;

describe("near misses on a loaded package", () => {
   it(
      "warns on an annotation reaching for a Publisher route, names where, and counts it",
      async () => {
         const pkg = await loadPackage({
            "model.malloy": `${BASE}
#@persist name="daily"
source: daily is raw -> { group_by: category; aggregate: total is amount.sum() }

source: orders is raw extend {
  #(clasification) "phi.x"
  dimension: cat is category
}
`,
         });
         const warnings = coded(pkg, ANNOTATION_NEAR_MISS_CODE);
         expect(warnings.map((w) => [w.subject, w.severity])).toEqual([
            ["daily", "warn"],
            ["orders.cat", "warn"],
         ]);
         expect(warnings[0]?.model).toBe("model.malloy");
         expect(warnings[0]?.message).toContain("did you mean `#@ persist`?");
         expect(warnings[0]?.message).toContain("(line 4)");
         expect(warnings[1]?.message).toContain(
            "did you mean `#(classification)`?",
         );
         // The misspelled #@ line planned nothing: the source serves live.
         expect(Object.keys(pkg.getBuildPlan()?.sources ?? {})).toEqual([]);
         // A warning, never a refusal.
         expect(formatPublishRejections(pkg)).toBeFalsy();
         expect(
            await harness.collectCounter(
               "publisher_annotation_findings_total",
               {
                  code: ANNOTATION_NEAR_MISS_CODE,
               },
            ),
         ).toBe(2);
      },
      { timeout: 30000 },
   );

   it(
      "warns on a prefix Malloy cannot route",
      async () => {
         const pkg = await loadPackage({
            "model.malloy": `${BASE}
source: orders is raw extend {
  #percent
  dimension: share is amount / 100
}
`,
         });
         const [warning] = coded(pkg, ANNOTATION_MALFORMED_ROUTE_CODE);
         expect(warning).toMatchObject({
            severity: "warn",
            subject: "orders.share",
         });
         expect(warning?.message).toContain("`# …`");
      },
      { timeout: 30000 },
   );

   it(
      "reports a near miss in a shared import once, against the file that wrote it",
      async () => {
         const pkg = await loadPackage({
            "base.malloy": `${BASE}
#(Doc) the raw rows
source: base is raw
`,
            "a.malloy": `import "base.malloy"\nsource: a is base\n`,
            "b.malloy": `import "base.malloy"\nsource: b is base\n`,
         });
         expect(
            coded(pkg, ANNOTATION_NEAR_MISS_CODE).map((w) => [
               w.model,
               w.subject,
            ]),
         ).toEqual([["base.malloy", "base"]]);
      },
      { timeout: 30000 },
   );

   it(
      "reports nothing for a package whose annotations are all read",
      async () => {
         const pkg = await loadPackage({
            "model.malloy": `${BASE}
#(doc) Daily totals
#@ persist name="daily"
source: daily is raw -> { group_by: category; aggregate: total is amount.sum() }

source: orders is raw extend {
  # percent
  dimension: share is amount / 100
  #(classification) "none"
  dimension: cat is category
}
`,
         });
         expect(
            (pkg.getPackageMetadata().warnings ?? []).filter((w) =>
               /^annotation-/.test(w.code ?? ""),
            ),
         ).toEqual([]);
      },
      { timeout: 30000 },
   );
});
