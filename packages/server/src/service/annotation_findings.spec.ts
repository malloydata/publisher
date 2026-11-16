// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The outcome tiers for `#@` annotation findings, over real packages: an
 * invalid (`malformed`) annotation fails the load, and so the publish, with
 * its code in the message; a valid one Publisher cannot build (`unservable`)
 * is a `warn` package warning while the package loads; and the one metric
 * that counts the findings of a package that loads.
 */

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
import { resetMaterializationTelemetryForTesting } from "../materialization_metrics";
import {
   PackageLoadPool,
   __setPackageLoadPoolForTests,
} from "../package_load/package_load_pool";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "../test_helpers/metrics_harness";
import { writePackageFiles } from "../test_helpers/temp_package";
import { Environment } from "./environment";
import { parsePackageName } from "./identity";
import { Package } from "./package";
import { formatPublishRejections } from "./publish_checks";
import {
   PackageMaterialization,
   type PackageMaterializationOwner,
} from "./materialization/package_materialization";
import type { PackageAnnotationFinding } from "./annotation_reader";
import { installSettings } from "../test_helpers/settings_env";
import { BadRequestError } from "../errors";
import { recordConnectionsAsForTests } from "./connection_type";

// The package sandbox stands in for a warehouse in these fixtures: a
// `storage=` build cannot read the sandbox, and the plan refuses a `storage=`
// source on it (storage_source_unsupported).
let restoreConnectionTypes: () => void;
beforeAll(() => {
   restoreConnectionTypes = recordConnectionsAsForTests({ duckdb: "postgres" });
});
afterAll(() => restoreConnectionTypes());

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

async function loadPackage(
   model: string,
   manifest: Record<string, unknown> = {},
): Promise<Package> {
   writePackageFiles(
      path.join(envPath, "pkg"),
      { "model.malloy": model, "rows.csv": "a\n1\n" },
      { manifest: { name: "pkg", description: "fixture", ...manifest } },
   );
   const env = await Environment.create("testEnv", envPath, []);
   await env.addPackage("pkg");
   return env.getPackage(parsePackageName("pkg"), false);
}

const RAW = `##! experimental { persistence composite_sources }
source: raw is duckdb.sql("""SELECT 1 AS amount, 'A' AS category""")
`;
const ROLLUP = `source: daily is raw -> { group_by: category; aggregate: total is amount.sum() }`;

/** The load's failure message; fails the test when the package loads. */
async function loadError(
   model: string,
   manifest: Record<string, unknown> = {},
): Promise<string> {
   let error: unknown;
   try {
      await loadPackage(model, manifest);
   } catch (e) {
      error = e;
   }
   expect(error).toBeInstanceOf(BadRequestError);
   return (error as Error).message;
}

/** Run `body` with materialization v2 on, in a load worker that reads it. */
async function withMaterialize<T>(
   body: () => Promise<T>,
   env: Record<string, string> = {},
): Promise<T> {
   const { restore } = installSettings({
      env: { PUBLISHER_MATERIALIZE: "true", ...env },
   });
   // The load worker reads its settings when it starts.
   await __setPackageLoadPoolForTests(new PackageLoadPool(1));
   try {
      return await body();
   } finally {
      await __setPackageLoadPoolForTests(null);
      restore();
   }
}

function coded(pkg: Package, code: string) {
   return (pkg.getPackageMetadata().warnings ?? []).filter(
      (w) => w.code === code,
   );
}

describe("an invalid #@ persist annotation fails the load", () => {
   it(
      "a #@ tag that does not parse, with its code",
      async () => {
         const message = await loadError(
            `${RAW}#@ persist name="t\n${ROLLUP}\n`,
         );
         expect(message).toContain("model.malloy: ");
         expect(message).toContain("does not parse");
         expect(message).toContain("[persist-unparseable]");
      },
      { timeout: 30000 },
   );

   it(
      "a broken #@ line beside one that parses",
      async () => {
         const message = await loadError(
            `${RAW}#@ persist name="t"\n#@ refresh="full" queryMetadata="x\n${ROLLUP}\n`,
         );
         expect(message).toContain('#@ persist source "daily"');
         expect(message).toContain("[persist-unparseable]");
      },
      { timeout: 30000 },
   );

   it(
      "even with a schedule armed: the package does not load to schedule anything",
      async () => {
         const message = await loadError(
            `${RAW}#@ persist name="t\n${ROLLUP}\n`,
            { materialization: { scope: "version", schedule: "0 * * * *" } },
         );
         expect(message).toContain("[persist-unparseable]");
      },
      { timeout: 30000 },
   );

   it(
      "names a finding on an imported persist source once, in the model that declares it",
      async () => {
         writePackageFiles(
            path.join(envPath, "pkg"),
            {
               "model.malloy": `${RAW}#@ persist name="t" merge_keys="category"\n${ROLLUP}\n`,
               "other.malloy": `##! experimental { persistence composite_sources }\nimport "model.malloy"\nsource: more is daily extend {}\n`,
            },
            { manifest: { name: "pkg", description: "fixture" } },
         );
         const env = await Environment.create("testEnv", envPath, []);
         let error: unknown;
         try {
            await env.addPackage("pkg");
         } catch (e) {
            error = e;
         }
         expect(error).toBeInstanceOf(BadRequestError);
         const lines = (error as Error).message
            .split("\n")
            .filter(
               (l) =>
                  l.includes("[persist-unknown-key]") &&
                  l.includes('source "daily"'),
            );
         expect(lines).toHaveLength(1);
         expect(lines[0]).toStartWith("model.malloy: ");
      },
      { timeout: 30000 },
   );

   it(
      "a retired key, with its rewrite",
      async () => {
         const message = await loadError(
            `${RAW}#@ persist name="t" schedule="0 * * * *"\n${ROLLUP}\n`,
         );
         expect(message).toContain("materialization.schedule");
         expect(message).toContain("[persist-retired-key]");
      },
      { timeout: 30000 },
   );

   const GIVEN_IN_QUERY = `##! experimental { persistence composite_sources givens }
given: CAT :: string is 'A'
source: raw is duckdb.sql("""SELECT 1 AS amount, 'A' AS category""")
#@ persist name="t" partition="category"
source: daily is raw -> { where: category = $CAT; group_by: category; aggregate: total is amount.sum() }
`;
   it.each([
      [
         "a colocated",
         `${RAW}#@ persist name="t" partition="category"\n${ROLLUP}\n`,
      ],
      [
         "a storage=",
         `${RAW}#@ persist name="t" storage="lake" partition="category"\n${ROLLUP}\n`,
      ],
      [
         "a list-valued",
         `${RAW}#@ persist name="t" partition=[category]\n${ROLLUP}\n`,
      ],
      [
         "a nested",
         `${RAW}#@ persist { name="t" partition="category" }\n${ROLLUP}\n`,
      ],
      ["an otherwise-refused source's", GIVEN_IN_QUERY],
   ])(
      "%s #@ persist partition=, saying to remove it",
      async (_case, model) => {
         const message = await loadError(model);
         expect(message).toContain("`partition=`");
         expect(message).toContain("partition= is not supported");
         expect(message).toContain("remove it");
         expect(message).toContain("[persist-retired-key]");
         // Not the vaguer unknown key.
         expect(message).not.toContain("[persist-unknown-key]");
      },
      { timeout: 30000 },
   );

   it(
      "an unknown key, with the likely spelling",
      async () => {
         const message = await loadError(
            `${RAW}#@ persist nmae="t"\n${ROLLUP}\n`,
         );
         expect(message).toContain("did you mean `name=`?");
         expect(message).toContain("[persist-unknown-key]");
      },
      { timeout: 30000 },
   );

   it.each([
      [
         "a filtered pass-through",
         `${RAW}#@ persist\nsource: passthrough is duckdb.table('rows.csv') extend { where: a > 0 }\n`,
      ],
      [
         // A table base with only a measure added: Malloy plans nothing for
         // it, and a run used to refuse every sibling's build over it.
         "a table base with a measure, stored",
         `${RAW}#@ persist storage="lake"\nsource: passthrough is duckdb.table('rows.csv') extend { measure: n is count() }\n`,
      ],
   ])(
      "a #@ persist on a shape Malloy does not plan: %s",
      async (_case, model) => {
         const message = await loadError(model);
         expect(message).toContain('#@ persist source "passthrough"');
         expect(message).toContain("[persist-not-materializable]");
      },
      { timeout: 30000 },
   );
});

describe("annotation findings on a loaded package", () => {
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
         expect(formatPublishRejections(pkg)).toBeUndefined();
      },
      { timeout: 30000 },
   );

   it(
      "plans #(materialize) name= at that name, whatever the package's scope, colocated or stored",
      async () => {
         await withMaterialize(
            async () => {
               for (const [manifest, keys] of [
                  [{}, ""],
                  [
                     { materialization: { scope: "package" } },
                     ' storage="lake"',
                  ],
                  [{ materialization: { scope: "version" } }, ""],
               ] as const) {
                  for (const [name, written] of [
                     [' name="analytics.orders_t"', "analytics.orders_t"],
                     ["", "orders"],
                  ] as const) {
                     await fs.rm(path.join(envPath, "pkg"), {
                        recursive: true,
                        force: true,
                     });
                     const pkg = await loadPackage(
                        `${RAW}#(materialize)${name}${keys}\nsource: orders is raw\n`,
                        manifest,
                     );
                     // Planned, under the author's name (schema included) or
                     // the source's own; the run adds a suffix only to the latter.
                     expect(
                        Object.values(pkg.getBuildPlan()?.sources ?? {}).map(
                           (s) => s.annotationFields?.name,
                        ),
                     ).toEqual([written]);
                     expect(formatPublishRejections(pkg)).toBeUndefined();
                  }
               }
               // #(preaggregate) never took a name=: an unknown key.
               await fs.rm(path.join(envPath, "pkg"), {
                  recursive: true,
                  force: true,
               });
               const message = await loadError(
                  `${RAW}source: sales is raw extend {\n  #(preaggregate) grain=["category"] name="r"\n  measure: total is amount.sum()\n}\n`,
               );
               expect(message).toContain("[preaggregate-unknown-key]");
            },
            { PERSIST_STORAGE_MODE: "on" },
         );
      },
      { timeout: 60000 },
   );

   it(
      "with the switch off, reads neither v2 route: no finding, and the package loads",
      async () => {
         await withMaterialize(
            async () => {
               for (const line of [
                  '#(materialize) nmae="t"',
                  '#(materialize) name="t',
                  '#(preaggregate) grian=["category"]',
                  '#(preaggregate) grain=["category"',
               ]) {
                  const pkg = await loadPackage(
                     `${RAW}${line}\nsource: orders is raw\n`,
                  );
                  const codes = (pkg.getPackageMetadata().warnings ?? []).map(
                     (w) => w.code,
                  );
                  expect(
                     codes.filter(
                        (c) =>
                           c?.startsWith("materialize-") ||
                           c?.startsWith("preaggregate-"),
                     ),
                  ).toEqual([]);
                  expect(formatPublishRejections(pkg)).toBeUndefined();
               }
            },
            { PUBLISHER_MATERIALIZE: "false" },
         );
      },
      { timeout: 60000 },
   );
});

describe("a collision across routes", () => {
   const MIXED = `${RAW}#@ persist name="t4"
source: v1 is raw -> { group_by: category; aggregate: total is amount.sum() }

#(materialize) name="t4"
source: v2 is raw extend { where: amount > 0 }
`;
   it(
      "fails the load naming both sides of a #@ persist and a #(materialize) at one name=",
      async () => {
         await withMaterialize(async () => {
            const lines = (await loadError(MIXED))
               .split("\n")
               .filter((l) => l.includes("[materialization-table-collision]"));
            expect(
               lines.some((l) => l.includes('#@ persist source "v1"')),
            ).toBe(true);
            expect(lines.some((l) => l.includes('source "v2"'))).toBe(true);
         });
      },
      { timeout: 30000 },
   );

   it(
      "under PERSIST_COLLISION_ENFORCE=false, plans neither side, each refused under its own code",
      async () => {
         await withMaterialize(
            async () => {
               const pkg = await loadPackage(MIXED);
               expect(Object.keys(pkg.getBuildPlan()?.sources ?? {})).toEqual(
                  [],
               );
               expect(
                  coded(pkg, "persist-not-built").map((w) => w.subject),
               ).toEqual(["v1"]);
               expect(
                  coded(pkg, "materialize-not-built").map((w) => w.subject),
               ).toEqual(["v2"]);
            },
            { PERSIST_COLLISION_ENFORCE: "false" },
         );
      },
      { timeout: 30000 },
   );

   it(
      "names the author's views, not their generated build sources",
      async () => {
         await withMaterialize(async () => {
            const message =
               await loadError(`${RAW}source: orders is raw extend {
  measure: n is count()
  #@ persist name="t5"
  view: by_a is { group_by: category; aggregate: n }
  #@ persist name="t5"
  view: by_b is { group_by: amount; aggregate: n }
}
`);
            expect(message).toContain("view by_a of orders");
            expect(message).toContain("view by_b of orders");
            expect(message).not.toContain("__view");
         });
      },
      { timeout: 30000 },
   );
});

describe("an invalid #(materialize) fails the load", () => {
   it.each([
      [
         "an unknown key",
         '#(materialize) nmae="t"',
         "[materialize-unknown-key]",
         "did you mean `name=`?",
      ],
      [
         "partition=",
         '#(materialize) name="t" storage="lake" partition="category"',
         "[materialize-retired-key]",
         "partition= is not supported",
      ],
      [
         "a line that does not parse",
         '#(materialize) name="t',
         "[materialize-unparseable]",
         "does not parse",
      ],
   ])(
      "%s",
      async (_case, line, code, text) => {
         await withMaterialize(async () => {
            const message = await loadError(
               `${RAW}${line}\nsource: orders is raw\n`,
            );
            expect(message).toContain(code);
            expect(message).toContain(text);
            expect(message).toContain('source "orders"');
         });
      },
      { timeout: 30000 },
   );

   it(
      "a defaulted name never meets another source's table; two equal name=s do, unless a published scope: version version names them per version",
      async () => {
         await withMaterialize(
            async () => {
               const reload = async (
                  model: string,
                  manifest: Record<string, unknown> = {},
               ) => {
                  await fs.rm(path.join(envPath, "pkg"), {
                     recursive: true,
                     force: true,
                  });
                  return loadPackage(model, manifest);
               };
               const defaultedAndNamed = `${RAW}#(materialize) storage="lake"
source: daily is raw extend { where: amount > 0 }

#(materialize) name="daily" storage="lake"
source: other is raw extend { where: amount < 0 }
`;
               for (const manifest of [
                  {},
                  { materialization: { scope: "version" } },
               ]) {
                  const pkg = await reload(defaultedAndNamed, manifest);
                  expect(coded(pkg, "materialize-not-built")).toEqual([]);
               }
               const twoNamed = `${RAW}#(materialize) name="t" storage="lake"
source: daily is raw extend { where: amount > 0 }

#(materialize) name="t" storage="lake"
source: other is raw extend { where: amount < 0 }
`;
               const collisions = (message: string) =>
                  message
                     .split("\n")
                     .filter((l) =>
                        l.includes("[materialization-table-collision]"),
                     );
               // Unversioned, a name= is written as given, whatever the
               // scope: two equal ones are one table.
               for (const manifest of [
                  {},
                  { materialization: { scope: "version" } },
               ]) {
                  await fs.rm(path.join(envPath, "pkg"), {
                     recursive: true,
                     force: true,
                  });
                  expect(
                     collisions(await loadError(twoNamed, manifest)),
                  ).toHaveLength(2);
               }
               // A published version of a scope: version package takes the
               // name= as the start of a readable name hashed over the
               // version and source: two tables.
               await fs.rm(path.join(envPath, "pkg"), {
                  recursive: true,
                  force: true,
               });
               writePackageFiles(
                  path.join(envPath, "pkg"),
                  { "model.malloy": twoNamed, "rows.csv": "a\n1\n" },
                  {
                     manifest: {
                        name: "pkg",
                        description: "fixture",
                        materialization: { scope: "version" },
                     },
                  },
               );
               const version = await Package.create(
                  "testEnv",
                  "pkg",
                  path.join(envPath, "pkg"),
                  new Map(),
                  false,
                  { versionId: "1.0.0" },
               );
               try {
                  expect(coded(version, "materialize-not-built")).toEqual([]);
                  expect(version.formatInvalidAnnotations()).toBe("");
               } finally {
                  await version.getMalloyConfig().shutdown("close");
               }
            },
            { PERSIST_STORAGE_MODE: "on" },
         );
      },
      { timeout: 60000 },
   );

   it(
      "with PERSIST_COLLISION_ENFORCE=false, the colliding sources are refused alone and the package loads",
      async () => {
         await withMaterialize(
            async () => {
               const pkg = await loadPackage(
                  `${RAW}#(materialize) name="t"
source: daily is raw extend { where: amount > 0 }

#(materialize) name="t"
source: other is raw extend { where: amount < 0 }
`,
               );
               expect(
                  coded(pkg, "materialize-not-built")
                     .map((w) => [w.subject, w.severity])
                     .sort(),
               ).toEqual([
                  ["daily", "warn"],
                  ["other", "warn"],
               ]);
               expect(Object.keys(pkg.getBuildPlan()?.sources ?? {})).toEqual(
                  [],
               );
            },
            { PERSIST_COLLISION_ENFORCE: "false" },
         );
      },
      { timeout: 60000 },
   );
});

describe("the load failure for invalid materialization annotations", () => {
   // The contract every route's reader relies on: any `malformed` finding
   // fails the load (and so the publish), named with its code, and nothing
   // `unservable` does.
   function failureOf(findings: PackageAnnotationFinding[]): string {
      const materialization = new PackageMaterialization(
         {} as unknown as PackageMaterializationOwner,
      );
      (
         materialization as unknown as {
            annotationFindings: PackageAnnotationFinding[];
         }
      ).annotationFindings = findings;
      return materialization.formatInvalidAnnotations();
   }

   it("names every malformed finding on any route, and nothing unservable", () => {
      const finding = (
         route: string,
         tier: PackageAnnotationFinding["tier"],
         code: string,
      ): PackageAnnotationFinding => ({
         route,
         tier,
         code,
         message: `${code} on ${route}`,
         subject: "orders",
         model: "m.malloy",
      });
      expect(
         failureOf([
            finding("preaggregate", "malformed", "preaggregate-invalid"),
            finding("materialize", "malformed", "materialize-unparseable"),
            finding("materialize", "unservable", "materialize-not-built"),
            finding("@", "malformed", "persist-unknown-key"),
            finding("@", "unservable", "persist-view-not-built"),
         ]).split("\n"),
      ).toEqual([
         "m.malloy: preaggregate-invalid on preaggregate. [preaggregate-invalid]",
         "m.malloy: materialize-unparseable on materialize. [materialize-unparseable]",
         "m.malloy: persist-unknown-key on @. [persist-unknown-key]",
      ]);
   });
});
