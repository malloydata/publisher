// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A tile's `givenNames` is what its compiled query reads, so a client can say
 * which controls a tile ignores. Checked through `Package.create`, in process
 * and through the worker pool, because discovery reads the hydrated Model.
 */
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import {
   PackageLoadPool,
   __setPackageLoadPoolForTests,
} from "../package_load/package_load_pool";
import { Package } from "./package";

const DASHBOARD = `##! experimental.givens
given:
  BRAND :: string is 'a'
  REGION :: string is 'x'
  CAT :: string is 'c'
  ORG :: number[]
  STAGE :: string is 's'
  SCOPE :: string is 'q'
  UNUSED :: string is 'u'

source: products is duckdb.sql("select 1 as id, 'a' as brand") extend {
  where: brand = $BRAND
}

#(access_filter) org_id in $ORG
source: orders is duckdb.sql("select 1 as id, 1 as product_id, 'x' as region, 'c' as cat, 1 as org_id, 's' as st, 'q' as w") extend {
  where: w = $SCOPE
  join_one: products on product_id = products.id
  dimension: is_region is region = $REGION
  dimension: unused_dim is cat = $UNUSED
  view: by_join is { group_by: products.brand; aggregate: n is count() }
  view: by_dim is { group_by: is_region; aggregate: n is count() }
  view: multi is { group_by: st; aggregate: n is count() } -> { where: st = $STAGE; select: * }
  view: plain is { aggregate: n is count() }
}

source: open_orders is duckdb.sql("select 1 as id, 'x' as region") extend {
  view: plain is { aggregate: n is count() }
}

## artifact { kind=dashboard tiles=["orders -> by_join", "orders -> by_dim", "orders -> multi", "orders -> plain", "orders -> plain + { where: cat = $CAT }", "open_orders -> plain", "open_orders -> nothing_here"] }
`;

const EXPECTED: Record<string, string[] | undefined> = {
   // Static walk read SCOPE only: a joined source's own `where:` was missed.
   "orders -> by_join": ["SCOPE", "BRAND", "ORG"],
   // ...and a dimension defined with `$REGION`.
   "orders -> by_dim": ["SCOPE", "REGION", "ORG"],
   "orders -> multi": ["SCOPE", "STAGE", "ORG"],
   // An unused `$UNUSED` dimension is not read.
   "orders -> plain": ["SCOPE", "ORG"],
   // A refinement the static walk could not resolve at all.
   "orders -> plain + { where: cat = $CAT }": ["CAT", "SCOPE", "ORG"],
   "open_orders -> plain": [],
   // Does not compile: no single view to read, so the whole row applies.
   "open_orders -> nothing_here": undefined,
};

async function makeMalloyConfig() {
   const { MalloyConfig, FixedConnectionMap } = await import(
      "@malloydata/malloy"
   );
   const { DuckDBConnection } = await import("@malloydata/db-duckdb");
   const duckdb = new DuckDBConnection("duckdb", ":memory:");
   const connections = new FixedConnectionMap(
      new Map([["duckdb", duckdb]]),
      "duckdb",
   );
   const malloyConfig = new MalloyConfig({ connections: {} });
   malloyConfig.wrapConnections(() => connections);
   return { malloyConfig, duckdb };
}

async function tileGivens(): Promise<Record<string, string[] | undefined>> {
   const dir = fs.mkdtempSync(path.join(os.tmpdir(), "dash-tile-givens-"));
   const { malloyConfig, duckdb } = await makeMalloyConfig();
   try {
      fs.writeFileSync(
         path.join(dir, "publisher.json"),
         JSON.stringify({ name: "pkg" }),
      );
      fs.mkdirSync(path.join(dir, "dashboards"));
      fs.writeFileSync(path.join(dir, "dashboards", "d.malloy"), DASHBOARD);
      const pkg = await Package.create("env", "pkg", dir, malloyConfig);
      const manifest = pkg.getDashboard("d");
      expect(manifest).toBeDefined();
      return Object.fromEntries(
         (manifest?.tiles ?? []).flatMap((tile) =>
            tile.kind === "query" && tile.query
               ? [[tile.query, tile.givenNames]]
               : [],
         ),
      );
   } finally {
      await duckdb.close();
      fs.rmSync(dir, { recursive: true, force: true });
   }
}

describe("dashboard tile givenNames come from the compiled query", () => {
   it("in process", async () => {
      expect(await tileGivens()).toEqual(EXPECTED);
   });

   describe("through the worker pool", () => {
      const original = process.env.PACKAGE_LOAD_WORKERS;
      beforeAll(async () => {
         process.env.PACKAGE_LOAD_WORKERS = "1";
         await __setPackageLoadPoolForTests(new PackageLoadPool(1));
      });
      afterAll(async () => {
         await __setPackageLoadPoolForTests(null);
         if (original === undefined) delete process.env.PACKAGE_LOAD_WORKERS;
         else process.env.PACKAGE_LOAD_WORKERS = original;
      });

      it("matches", async () => {
         expect(await tileGivens()).toEqual(EXPECTED);
      });
   });
});
