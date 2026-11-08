// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The whole `GET …/entities` response, as `toApiEntities` builds it, for every
 * curated package the entities specs compile: the query-surface package (every
 * report shape, a derivation chain, shadowed and aliased imports), the same
 * package under `queryableSources: "all"`, a package whose `explores` lists
 * reports beside ordinary files, a re-rooting topology, and a package of
 * access-filter shapes. `to_api.snapshot.spec.ts` covers the uncurated rich
 * package.
 *
 * A golden: a change to which sources are queryable, what a report's source
 * names as its served source, or how a gate's terms are read shows up here as
 * a snapshot diff, so a refactor of the rules behind them shows whether it
 * changed the output.
 */

import { expect, test } from "bun:test";
import { QUERY_SURFACE_FILES } from "./__fixtures__/query_surface_cases";
import { compileFixture } from "./test_support";
import { toApiEntities } from "./to_api";

type GoldenPackage = {
   files: Record<string, string>;
   manifest?: { explores?: string[]; queryableSources?: "declared" | "all" };
};

const ORDERS = `
source: orders is duckdb.table('orders.csv') extend {
  dimension:
    #(index)
    status_label is upper(status)
}
source: orders_staging is duckdb.table('orders.csv')
`;

const INDEX = `
import "orders.malloy"
export { orders }
`;

const LISTED_DASHBOARDS: Record<string, string> = {
   "orders.csv": "id,status\n1,open\n2,closed\n",
   "orders.malloy": ORDERS,
   "index.malloy": INDEX,
   "dashboards/overview.malloy": `
## artifact { tiles=["staging -> by_status"] }
import "../orders.malloy"
source: staging is orders_staging extend {
  view: by_status is { group_by: status }
}
export { staging }
`,
   "dashboards/old.malloy": `
import "../orders.malloy"
source: listed is orders_staging
export { listed }
`,
   "dashboards/titled.malloy": `
## title="Q3" artifact { tiles=["titled -> by_status"] }
import "../orders.malloy"
source: titled is orders_staging extend {
  view: by_status is { group_by: status }
}
export { titled }
`,
   "dashboards/single.malloy": `
import "../orders.malloy"
source: single is orders_staging
# dashboard artifact
query: overview is single -> { group_by: status }
export { single }
`,
   "dashboards/derived.malloy": `
## artifact { tiles=["open_orders -> by_status"] }
import "../orders.malloy"
source: open_orders is orders extend {
  where: status = 'open'
  view: by_status is { group_by: status }
}
`,
   "dashboards/commented.malloy": `
// ## artifact { tiles=[] }
import "../orders.malloy"
source: commented is orders_staging
export { commented }
`,
};

const REROOT: Record<string, string> = {
   "inventory.csv": "id,cat\n1,shoes\n2,hats\n",
   "order_items.csv": "id,inv_id,oi_id\n10,1,100\n11,2,101\n",
   "analytics.csv": "id,oi_id\n1000,100\n1001,101\n",
   "inventory_items.malloy": `
source: inventory_items is duckdb.table('inventory.csv') extend {
  dimension:
    #(index)
    product_category is upper(cat)
}
`,
   "order_hub.malloy": `
import "inventory_items.malloy"
source: order_items is duckdb.table('order_items.csv') extend {
  join_one: inventory_items on inv_id = inventory_items.id
  dimension:
    #(index)
    oi_label is concat('oi-', inv_id::string)
}
`,
   "analytics.malloy": `
import "order_hub.malloy"
source: analytics is duckdb.table('analytics.csv') extend {
  join_one: order_items on oi_id = order_items.id
}
`,
};

// One gate shape per source: a scalar equality, a list membership, a dotted
// join path, a backticked segment, a backticked segment holding a dot (which
// a dotted field path cannot carry), two conjuncts, and an inherited filter
// ANDed with the derivation's own.
const GATES: Record<string, string> = {
   "orders.csv":
      "order_id,region,rep_id,tenant_id,amount\n1,us,1,t1,5\n2,eu,2,t2,7\n",
   "reps.csv": 'rep_id,rep_name,"cost.center"\n1,Ann,c1\n2,Bo,c2\n',
   "gates.malloy": `##! experimental.givens

given: REGION :: string is 'us'
given: IDS :: number[]
given: NAME :: string is 'Ann'
given: CENTER :: string is 'c1'
given: TENANT :: string is 't1'

#(access_filter) region = $REGION
source: scalar_gate is duckdb.table('orders.csv')

#(access_filter) rep_id in $IDS
source: list_gate is duckdb.table('orders.csv')

#(access_filter) reps.rep_name = $NAME
source: join_gate is duckdb.table('orders.csv') extend {
  join_one: reps is duckdb.table('reps.csv') on rep_id = reps.rep_id
}

#(access_filter) \`region\` = $REGION
source: backtick_gate is duckdb.table('orders.csv')

#(access_filter) reps.\`cost.center\` = $CENTER
source: dotted_segment_gate is duckdb.table('orders.csv') extend {
  join_one: reps is duckdb.table('reps.csv') on rep_id = reps.rep_id
}

#(access_filter) region = $REGION and tenant_id = $TENANT
source: two_terms is duckdb.table('orders.csv')

#(access_filter) tenant_id = $TENANT
source: derived_gate is scalar_gate extend {}
`,
};

const PACKAGES: Record<string, GoldenPackage> = {
   "query-surface": {
      files: QUERY_SURFACE_FILES,
      manifest: { explores: ["index.malloy"] },
   },
   "query-surface-all": {
      files: QUERY_SURFACE_FILES,
      manifest: { explores: ["index.malloy"], queryableSources: "all" },
   },
   "listed-dashboards": {
      files: LISTED_DASHBOARDS,
      manifest: {
         explores: [
            "index.malloy",
            "dashboards/overview.malloy",
            "dashboards/old.malloy",
            "dashboards/titled.malloy",
            "dashboards/single.malloy",
            "dashboards/commented.malloy",
         ],
         queryableSources: "declared",
      },
   },
   reroot: {
      files: REROOT,
      manifest: {
         explores: ["analytics.malloy"],
         queryableSources: "declared",
      },
   },
   gates: { files: GATES },
};

for (const [name, pkg] of Object.entries(PACKAGES)) {
   test(`the ${name} package's entities`, async () => {
      const results = await compileFixture(pkg.files, pkg.manifest);
      // A compile error's text is the compiler's, not the extraction's.
      const api = toApiEntities(results).map((m) =>
         m.error ? { ...m, error: "<compile error>" } : m,
      );
      expect(api).toMatchSnapshot();
   }, 120_000);
}
