// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A tile's `givenNames` is what its compiled query reads, so a client can say
 * which controls a tile ignores. Checked through `Package.create`, in process
 * and through the worker pool, because discovery reads the hydrated Model.
 */
import { describe, expect, it } from "bun:test";
import { getDashboard } from "../../src/service/legacy_report_views";
import {
   loadTempPackage,
   usePackageLoadPool,
} from "../../src/test_helpers/temp_package";

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

## artifact { layout=LAYOUT tiles=["orders -> by_join", "orders -> by_dim", "orders -> multi", "orders -> plain", "orders -> plain + { where: cat = $CAT }", "open_orders -> plain", "open_orders -> nothing_here"] }
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

type Tile = { kind?: string; query?: string; givenNames?: string[] };

/** The same tiles as a dashboard, or as a notebook read through the notebook route. */
async function tileGivens(
   kind: "dashboard" | "notebook",
): Promise<Record<string, string[] | undefined>> {
   const folder = kind === "dashboard" ? "dashboards" : "notebooks";
   const { pkg, cleanup } = await loadTempPackage({
      [`${folder}/d.malloy`]: DASHBOARD.replace(
         "LAYOUT",
         kind === "dashboard" ? "grid" : "linear",
      ),
   });
   try {
      const tiles: Tile[] | undefined =
         kind === "dashboard"
            ? (await getDashboard(pkg, "env", "d"))?.tiles
            : (await pkg.getModel("notebooks/d.malloy")?.getNotebook())
                 ?.dashboard?.tiles;
      expect(tiles).toBeDefined();
      return Object.fromEntries(
         (tiles ?? []).flatMap((tile) =>
            tile.kind === "query" && tile.query
               ? [[tile.query, tile.givenNames]]
               : [],
         ),
      );
   } finally {
      await cleanup();
   }
}

describe("dashboard tile givenNames come from the compiled query", () => {
   it("in process", async () => {
      expect(await tileGivens("dashboard")).toEqual(EXPECTED);
   });

   it("on the notebook route, in process", async () => {
      expect(await tileGivens("notebook")).toEqual(EXPECTED);
   });

   describe("through the worker pool", () => {
      usePackageLoadPool();

      it("matches", async () => {
         expect(await tileGivens("dashboard")).toEqual(EXPECTED);
      });

      it("matches on the notebook route", async () => {
         expect(await tileGivens("notebook")).toEqual(EXPECTED);
      });
   });
});
