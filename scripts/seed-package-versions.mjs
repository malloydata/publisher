#!/usr/bin/env node
// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Seed a running Publisher with two versioned packages and materialize them.
 *
 *   node scripts/seed-package-versions.mjs [--base http://localhost:4000] [--env demo]
 *
 * The server must publish versions: start it with
 * `PUBLISHER_PACKAGE_VERSIONING=on` (promotion `on-publish`, the default).
 *
 * It (re)creates the environment, so it can be run again, then publishes:
 *
 *   sales      scope: package  1.0.0  order_summary
 *                              1.1.0  order_summary, region_summary
 *   inventory  scope: version  1.0.0  stock_by_warehouse
 *                              1.1.0  stock_by_warehouse, low_stock
 *
 * and starts one auto-run materialization per version, waiting for each.
 * Under `scope: package` the tables are shared by every version, so only the
 * package's `latest` may auto-run; the older version's run is refused with
 * VERSION_NOT_LATEST, which the summary reports. Under `scope: version` each
 * version builds its own tables, named with a `__v<version>_<hash>` suffix.
 */

import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const args = process.argv.slice(2);
const option = (name, fallback) => {
   const at = args.indexOf(`--${name}`);
   return at === -1 ? fallback : args[at + 1];
};
const BASE = `${option("base", "http://localhost:4000").replace(/\/$/, "")}/api/v0`;
const ENV = option("env", "demo");

const ORDERS_CSV = `id,category,region,amount
1,books,west,12.5
2,books,east,30
3,games,west,45
4,games,east,15
5,music,west,9.99
6,music,north,22
7,books,north,18
8,games,north,60
`;

const STOCK_CSV = `sku,warehouse,qty
A-1,reno,40
A-2,reno,4
B-1,austin,12
B-2,austin,2
C-1,denver,75
C-2,denver,8
`;

const SALES_V1 = `##! experimental.persistence

source: raw_orders is duckdb.table('data/orders.csv')

#@ persist name="order_summary"
source: order_summary is raw_orders -> {
  group_by: category
  aggregate:
    total_orders is count()
    total_revenue is amount.sum()
}
`;

const SALES_V2 = `${SALES_V1}
#@ persist name="region_summary"
source: region_summary is raw_orders -> {
  group_by: region
  aggregate:
    total_orders is count()
    total_revenue is amount.sum()
}
`;

const INVENTORY_V1 = `##! experimental.persistence

source: stock is duckdb.table('data/stock.csv')

#@ persist name="stock_by_warehouse"
source: stock_by_warehouse is stock -> {
  group_by: warehouse
  aggregate:
    skus is count()
    units is qty.sum()
}
`;

const INVENTORY_V2 = `${INVENTORY_V1}
#@ persist name="low_stock"
source: low_stock is stock -> {
  where: qty < 10
  select: sku, warehouse, qty
}
`;

const PACKAGES = [
   {
      name: "sales",
      scope: "package",
      versions: [
         { version: "1.0.0", files: { "sales.malloy": SALES_V1, "data/orders.csv": ORDERS_CSV } },
         { version: "1.1.0", files: { "sales.malloy": SALES_V2, "data/orders.csv": ORDERS_CSV } },
      ],
   },
   {
      name: "inventory",
      scope: "version",
      versions: [
         { version: "1.0.0", files: { "inventory.malloy": INVENTORY_V1, "data/stock.csv": STOCK_CSV } },
         { version: "1.1.0", files: { "inventory.malloy": INVENTORY_V2, "data/stock.csv": STOCK_CSV } },
      ],
   },
];

const TERMINAL = new Set(["MANIFEST_FILE_READY", "FAILED", "CANCELLED"]);
const pause = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function call(method, route, body) {
   const res = await fetch(`${BASE}${route}`, {
      method,
      headers: { "Content-Type": "application/json" },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
   });
   const text = await res.text();
   let json;
   try {
      json = text ? JSON.parse(text) : undefined;
   } catch {
      json = { message: text };
   }
   return { status: res.status, ok: res.ok, json };
}

async function waitForServing() {
   for (let attempt = 0; attempt < 120; attempt++) {
      try {
         const { json } = await call("GET", "/status");
         if (json?.operationalState === "serving") return json;
      } catch {
         // Not listening yet.
      }
      await pause(1000);
   }
   throw new Error(`${BASE} did not reach serving within 2 minutes`);
}

function writeVersion(pkg, { version, files }) {
   const dir = fs.mkdtempSync(path.join(os.tmpdir(), `publisher-${pkg.name}-${version}-`));
   const manifest = {
      name: pkg.name,
      version,
      description: `${pkg.name}, release ${version} (scope: ${pkg.scope})`,
      ...(pkg.scope === "version" ? { materialization: { scope: "version" } } : {}),
   };
   fs.writeFileSync(path.join(dir, "publisher.json"), `${JSON.stringify(manifest, null, 2)}\n`);
   for (const [relative, text] of Object.entries(files)) {
      const target = path.join(dir, relative);
      fs.mkdirSync(path.dirname(target), { recursive: true });
      fs.writeFileSync(target, text);
   }
   return dir;
}

async function materialize(pkg, version) {
   const route = `/environments/${ENV}/packages/${pkg.name}/materializations`;
   const created = await call("POST", route, { versionId: version });
   if (!created.ok) {
      return {
         outcome: `refused ${created.status} ${created.json?.reason ?? ""}`.trim(),
         detail: created.json?.message ?? "",
         tables: [],
      };
   }
   const id = created.json.id;
   for (let attempt = 0; attempt < 300; attempt++) {
      const { json: run } = await call(
         "GET",
         `${route}/${id}?versionId=${encodeURIComponent(version)}`,
      );
      if (TERMINAL.has(run?.status)) {
         const entries = Object.values(run.manifest?.entries ?? {});
         return {
            outcome: run.status,
            detail: run.error ?? "",
            tables: entries.map((entry) => entry.physicalTableName).sort(),
         };
      }
      await pause(500);
   }
   return { outcome: "TIMED OUT", detail: `run ${id}`, tables: [] };
}

async function main() {
   const status = await waitForServing();
   if (status.packageVersioning !== "on") {
      console.error(
         `${BASE} has packageVersioning=${status.packageVersioning}; restart it with PUBLISHER_PACKAGE_VERSIONING=on.`,
      );
      process.exit(1);
   }

   // Start clean, so the script can run again.
   await call("DELETE", `/environments/${ENV}`);
   const env = await call("POST", "/environments", { name: ENV, packages: [], connections: [] });
   if (!env.ok) throw new Error(`create environment ${ENV}: ${env.status} ${JSON.stringify(env.json)}`);
   console.log(`environment ${ENV} created`);

   for (const pkg of PACKAGES) {
      for (const entry of pkg.versions) {
         const published = await call("POST", `/environments/${ENV}/packages`, {
            name: pkg.name,
            location: writeVersion(pkg, entry),
         });
         if (!published.ok) {
            throw new Error(
               `publish ${pkg.name} ${entry.version}: ${published.status} ${JSON.stringify(published.json)}`,
            );
         }
         console.log(`published ${pkg.name} ${entry.version}`);
      }
   }

   const rows = [];
   for (const pkg of PACKAGES) {
      const { json: versions } = await call("GET", `/environments/${ENV}/packages/${pkg.name}/versions`);
      const latest = versions.find((v) => v.latest)?.id;
      // Highest first, as the server lists them: latest builds before the older version.
      for (const { id } of versions) {
         const result = await materialize(pkg, id);
         rows.push({
            package: pkg.name,
            scope: pkg.scope,
            version: `${id}${id === latest ? " (latest)" : ""}`,
            materialization: result.outcome,
            tables: result.tables.join(", ") || "-",
            note: result.detail.length > 90 ? `${result.detail.slice(0, 87)}...` : result.detail,
         });
         console.log(`materialized ${pkg.name} ${id}: ${result.outcome}`);
      }
   }

   console.log("");
   console.table(rows);
   const console_ = option("console", "http://localhost:5173");
   console.log("\nOpen in the Console:");
   for (const pkg of PACKAGES) {
      for (const { version } of pkg.versions) console.log(`  ${console_}/${ENV}/${pkg.name}/${version}`);
   }
}

main().catch((error) => {
   console.error(error instanceof Error ? error.message : error);
   process.exit(1);
});
