// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The package set the main-thread regression storm publishes: two bundled
 * examples, two generated packages of fleet-like size, a materializing package
 * whose versions bind a build manifest, and a package that governs access over
 * array columns. Each is written as several versions (`<name>/<version>/`,
 * same content, `publisher.json` version bumped), because the fleet publishes
 * versions with `PUBLISHER_PACKAGE_VERSIONING=on`.
 *
 *   bun tests/perf/main-thread/fixtures.ts <out-dir> [--versions 2]
 *
 * Only syntax every compared build accepts belongs here: the A/B publishes the
 * same set to a baseline that may be several releases old. `run.ts` preflights
 * every package against both builds and drops one either side refuses, so a
 * new example that an old release cannot load narrows the set rather than
 * skewing it.
 */

import { DuckDBInstance } from "@duckdb/node-api";
import { cpSync, existsSync, mkdirSync, readFileSync, writeFileSync } from "fs";
import path from "path";
import { generatePackage } from "../../../k6-tests/perf/gen-package";

const REPO_ROOT = path.resolve(import.meta.dir, "../../../../..");

export interface FixtureVersion {
   version: string;
   dir: string;
}

export interface Fixture {
   name: string;
   /** What the fixture stands for in the fleet's mix. */
   kind: "example" | "generated" | "materializing" | "governed-arrays";
   versions: FixtureVersion[];
   /**
    * For a materializing fixture: the parquet files a manifest entry may name
    * as its physical table (DuckDB reads a quoted file path as a table).
    */
   tables?: string[];
   /**
    * The package the quiet re-bind check binds: one model, no reports, no
    * index, so an identical re-bind has nothing to compile on the main
    * thread except a served copy that should not have been invalidated.
    */
   rebindProbe?: boolean;
}

export interface FixtureSet {
   dir: string;
   fixtures: Fixture[];
}

// Persisted rollups over four of a generated package's fact sources, in a
// model of its own beside the generated ones. On a bind every model file of
// the package gets the new bindings, so the bind path's main-thread work
// scales with the package's model count, as it does in the fleet.
const ROLLUPS: Array<{
   name: string;
   over: string;
   keys: string;
   sql: string;
}> = [
   { name: "by_status", over: "sales_1_1", keys: "status", sql: "status" },
   { name: "by_channel", over: "sales_2_1", keys: "channel", sql: "channel" },
   {
      name: "by_status_channel",
      over: "sales_3_1",
      keys: "status, channel",
      sql: "status, channel",
   },
   {
      name: "by_quantity",
      over: "sales_4_1",
      keys: "quantity",
      sql: "quantity",
   },
];

// The re-bind probe: one model file, persisted rollups over a parquet file.
const MAT_MINI_MODEL = `##! experimental.persistence

source: orders is duckdb.table('data/orders.parquet') extend {
  measure:
    order_count is count()
    revenue is amount.sum()
}

#@ persist name="mtr_mini_by_category"
source: by_category is orders -> {
  group_by: category
  aggregate: order_count, revenue
}

#@ persist name="mtr_mini_by_region"
source: by_region is orders -> {
  group_by: region
  aggregate: order_count, revenue
}

source: category_report is by_category extend {
  view: top is { group_by: category; aggregate: total is revenue.sum(); limit: 5 }
}
`;

async function matMiniData(pkgDir: string): Promise<string[]> {
   const data = path.join(pkgDir, "data");
   mkdirSync(data, { recursive: true });
   const orders = path.join(data, "orders.parquet");
   await duckdb([
      `COPY (SELECT i AS id,
         (['books','games','garden','tools','toys'])[1 + i % 5] AS category,
         (['north','south','east','west'])[1 + i % 4] AS region,
         ((i * 37) % 997)::DOUBLE AS amount
       FROM range(20000) t(i)) TO ${q(orders)} (FORMAT parquet)`,
      ...["category", "region"].map(
         (
            k,
         ) => `COPY (SELECT ${k}, count(*) AS order_count, sum(amount) AS revenue
            FROM ${q(orders)} GROUP BY ALL) TO ${q(path.join(data, `by_${k}.parquet`))} (FORMAT parquet)`,
      ),
   ]);
   return ["data/by_category.parquet", "data/by_region.parquet"];
}

function matModel(): string {
   const imports = ROLLUPS.map(
      (r) =>
         `import { ${r.over} } from "model_${r.over.split("_")[1].padStart(2, "0")}.malloy"`,
   );
   const rollups = ROLLUPS.map(
      (r) => `#@ persist name="mtr_${r.name}"
source: ${r.name} is ${r.over} -> {
  group_by: ${r.keys}
  aggregate:
    order_count is count()
    units is quantity.sum()
}

source: ${r.name}_report is ${r.name} extend {
  view: top is { group_by: ${r.keys.split(",")[0]}; aggregate: total is units.sum(); limit: 5 }
}`,
   );
   return `##! experimental.persistence

${imports.join("\n")}

${rollups.join("\n\n")}
`;
}

// Access governed by a given, over a table with an array column and a
// repeated record: the joins Malloy makes for them are what the array-join
// load gate walks.
const GOV_ARRAYS_MODEL = `##! experimental.givens

#(secure)
given:
  TENANTS :: string[]

#(access_filter) tenant in $TENANTS
source: events is duckdb.table('data/events.parquet') extend {
  measure:
    event_count is count()
    item_qty is items.qty.sum()
  view: by_tag is { group_by: tag is tags.each; aggregate: event_count }
  view: by_sku is { group_by: items.sku; aggregate: item_qty }
  view: by_tenant is { group_by: tenant; aggregate: event_count, item_qty }
}

#(access_filter) tenant in $TENANTS
source: tagged is events extend {
  view: tag_items is {
    group_by: tag is tags.each
    nest: skus is { group_by: items.sku; aggregate: item_qty; limit: 3 }
  }
}
`;

const GOV_ARRAYS_INDEX = `##! experimental.givens

import "events.malloy"

export { events, tagged }
`;

async function duckdb(statements: string[]): Promise<void> {
   const db = await DuckDBInstance.create(":memory:");
   const c = await db.connect();
   for (const s of statements) await c.run(s);
   c.closeSync();
}

function q(p: string): string {
   return `'${p.replace(/'/g, "''")}'`;
}

/** One parquet file per rollup, with the columns its persisted query yields. */
async function materializingTables(pkgDir: string): Promise<string[]> {
   const fact = path.join(pkgDir, "data", "fact.parquet");
   await duckdb(
      ROLLUPS.map(
         (
            r,
         ) => `COPY (SELECT ${r.sql}, count(*) AS order_count, sum(quantity) AS units
            FROM ${q(fact)} GROUP BY ALL) TO ${q(path.join(pkgDir, "data", `${r.name}.parquet`))} (FORMAT parquet)`,
      ),
   );
   return ROLLUPS.map((r) => `data/${r.name}.parquet`);
}

async function governedArraysData(dataDir: string): Promise<void> {
   mkdirSync(dataDir, { recursive: true });
   await duckdb([
      `COPY (SELECT i AS id,
         (['acme','globex','initech'])[1 + i % 3] AS tenant,
         [(['red','green','blue'])[1 + i % 3], (['big','small'])[1 + i % 2]] AS tags,
         [{'sku': 'sku-' || (i % 50)::VARCHAR, 'qty': i % 7},
          {'sku': 'sku-' || ((i + 7) % 50)::VARCHAR, 'qty': i % 3}] AS items
       FROM range(5000) t(i)) TO ${q(path.join(dataDir, "events.parquet"))} (FORMAT parquet)`,
   ]);
}

function setVersion(dir: string, name: string, version: string): void {
   const file = path.join(dir, "publisher.json");
   const json = existsSync(file)
      ? (JSON.parse(readFileSync(file, "utf8")) as Record<string, unknown>)
      : {};
   json.name = name;
   json.version = version;
   writeFileSync(file, JSON.stringify(json, null, 2) + "\n");
}

/** Copy `from` once per version, bumping only publisher.json's version. */
function versionsOf(
   out: string,
   name: string,
   from: string,
   count: number,
): FixtureVersion[] {
   const versions: FixtureVersion[] = [];
   for (let v = 0; v < count; v++) {
      const version = `1.0.${v}`;
      const dir = path.join(out, "packages", name, version);
      cpSync(from, dir, {
         recursive: true,
         // Nothing a load reads: eval sets, tests and lint config.
         filter: (src) =>
            !/[/\\](evals|tests|node_modules)([/\\]|$)/.test(src) &&
            !src.endsWith("eslint.config.mjs"),
      });
      setVersion(dir, name, version);
      versions.push({ version, dir });
   }
   return versions;
}

export async function buildFixtures(
   out: string,
   versions = 2,
): Promise<FixtureSet> {
   const src = path.join(out, "src");
   mkdirSync(src, { recursive: true });
   const fixtures: Fixture[] = [];

   for (const name of ["storefront", "governed-analytics"]) {
      fixtures.push({
         name,
         kind: "example",
         versions: versionsOf(
            out,
            name,
            path.join(REPO_ROOT, "examples", name),
            versions,
         ),
      });
   }

   // Fleet-sized models: the ModelDef walks and hydration this catches scale
   // with sources, fields and views, not with rows.
   for (const preset of ["medium", "large"]) {
      const name = `gen-${preset}`;
      const dir = path.join(src, name);
      await generatePackage({ dir, preset, rows: 2000, seed: 7, name });
      fixtures.push({
         name,
         kind: "generated",
         versions: versionsOf(out, name, dir, versions),
      });
   }

   const matDir = path.join(src, "mat");
   await generatePackage({
      dir: matDir,
      preset: "medium",
      rows: 2000,
      seed: 11,
      name: "mat",
   });
   writeFileSync(path.join(matDir, "mat.malloy"), matModel());
   const tables = await materializingTables(matDir);
   fixtures.push({
      name: "mat",
      kind: "materializing",
      versions: versionsOf(out, "mat", matDir, versions),
      tables,
   });

   const miniDir = path.join(src, "mat-mini");
   mkdirSync(miniDir, { recursive: true });
   writeFileSync(path.join(miniDir, "mat.malloy"), MAT_MINI_MODEL);
   writeFileSync(
      path.join(miniDir, "publisher.json"),
      JSON.stringify({ name: "mat-mini", version: "1.0.0" }),
   );
   fixtures.push({
      name: "mat-mini",
      kind: "materializing",
      tables: await matMiniData(miniDir),
      versions: versionsOf(out, "mat-mini", miniDir, versions),
      rebindProbe: true,
   });

   const govDir = path.join(src, "gov-arrays");
   mkdirSync(govDir, { recursive: true });
   writeFileSync(path.join(govDir, "events.malloy"), GOV_ARRAYS_MODEL);
   writeFileSync(path.join(govDir, "index.malloy"), GOV_ARRAYS_INDEX);
   writeFileSync(
      path.join(govDir, "publisher.json"),
      JSON.stringify({ name: "gov-arrays", version: "1.0.0" }),
   );
   await governedArraysData(path.join(govDir, "data"));
   fixtures.push({
      name: "gov-arrays",
      kind: "governed-arrays",
      versions: versionsOf(out, "gov-arrays", govDir, versions),
   });

   const set: FixtureSet = { dir: out, fixtures };
   writeFileSync(path.join(out, "fixtures.json"), JSON.stringify(set, null, 2));
   return set;
}

if (import.meta.main) {
   const args = process.argv.slice(2);
   const out = args.find((a) => !a.startsWith("--"));
   const i = args.indexOf("--versions");
   if (!out) {
      console.error("usage: bun fixtures.ts <out-dir> [--versions 2]");
      process.exit(2);
   }
   const set = await buildFixtures(
      path.resolve(out),
      i >= 0 ? Number(args[i + 1]) : 2,
   );
   console.log(
      set.fixtures
         .map((f) => `${f.name} (${f.kind}) x${f.versions.length}`)
         .join("\n"),
   );
}
