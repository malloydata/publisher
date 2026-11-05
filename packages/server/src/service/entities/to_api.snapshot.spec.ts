// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The whole `GET …/entities` response for two packages that exercise every
 * part of the extraction: field kinds, fingerprinted `#(index)` dimensions,
 * docs, givens, filters, an access filter, joins (table, inline, gated),
 * agent-hidden and off-surface sources, a join-only gate and a model that
 * does not compile. A change to the extraction that changes what a client
 * reads shows up here as a snapshot diff.
 */

import { expect, test } from "bun:test";
import path from "node:path";
import { toApiEntities } from "./to_api";
import {
   ConnectionLookup,
   packageCompilationService,
   writeWorkspace,
} from "./test_support";

const ORDERS_CSV =
   "order_id,channel_name,region,rep_id,tenant_id,amount\n1,web,us,1,t1,5\n2,store,eu,2,t2,7\n";
const REPS_CSV = "rep_id,rep_name\n1,Ann\n2,Bo\n";

const CASES: Record<
   string,
   {
      files: Record<string, string>;
      manifest?: { explores?: string[] };
      errors?: number;
   }
> = {
   rich: {
      files: {
         "orders.malloy": `##! experimental.givens
##! experimental.parameters

#(secure)
given: TENANT :: string[]
given: REGION_PICK :: string is 'us'

#(doc) Orders placed in "any" channel, <b>tagged</b> & quoted \\ back.
#(filter) name=channel dimension=channel_name type=equal required
#(filter) name=region dimension=region type=in
#(access_filter) region = $REGION_PICK
source: orders is duckdb.table('orders.csv') extend {
  #(index)
  dimension: channel_upper is upper(channel_name)
  #(index)
  #(doc) The région, labelled.
  dimension: region_label is concat('r-', region)
  # hidden
  dimension: secret is order_id * 2
  measure: order_count is count()
  measure: total is amount.sum()
  join_one: reps is duckdb.table('reps.csv') on rep_id = reps.rep_id
  join_one: rep_names is duckdb.table('reps.csv') extend {
    dimension: shout is upper(rep_name)
  } on rep_id = rep_names.rep_id
  view: by_channel is { group_by: channel_name; aggregate: order_count }
  private dimension: hidden_dim is region
}

source: params(p_region::string is 'us') is duckdb.table('orders.csv') extend {
  where: region = p_region
}
`,
      },
   },
   surface: {
      manifest: { explores: ["index.malloy"] },
      errors: 1,
      files: {
         "base.malloy": `##(agent-hidden)
#(authorize) false
source: locked is duckdb.table('orders.csv')
source: open_orders is duckdb.table('orders.csv') extend {
  join_one: gated is locked on order_id = gated.order_id
}
`,
         "index.malloy": `import "base.malloy"
#(doc) The served orders.
source: served is open_orders extend {
  join_one: inner_gate is duckdb.table('reps.csv') extend {
    dimension: rn is rep_name
  } on rep_id = inner_gate.rep_id
}
`,
         "other.malloy": `source: off_surface is duckdb.table('reps.csv')
`,
         "broken.malloy": `source: broken is duckdb.table('orders.csv') extend { dimension: x is nope }
`,
      },
   },
};

for (const [name, c] of Object.entries(CASES)) {
   test(`the ${name} package's entities`, async () => {
      const ws = await writeWorkspace([
         { relpath: "orders.csv", contents: ORDERS_CSV },
         { relpath: "reps.csv", contents: REPS_CSV },
         ...Object.entries(c.files).map(([relpath, contents]) => ({
            relpath,
            contents,
         })),
      ]);
      const lookup = await ConnectionLookup.getInstance([], ws.dir);
      try {
         const results =
            await packageCompilationService.compilePackageModelFiles(
               {
                  modelFiles: Object.keys(c.files)
                     .sort()
                     .map((relpath) => ({
                        packageRootPath: ws.dir,
                        absolutePath: path.join(ws.dir, relpath),
                        relativePath: relpath,
                     })),
                  manifest: c.manifest,
               },
               lookup,
            );
         expect(results.filter((r) => r.compileStatus === "ERROR").length).toBe(
            c.errors ?? 0,
         );
         // A compile error's text is the compiler's, not the extraction's.
         const api = toApiEntities(results).map((m) =>
            m.error ? { ...m, error: "<compile error>" } : m,
         );
         expect(api).toMatchSnapshot();
      } finally {
         await lookup.dispose();
         await ws.cleanup();
      }
   }, 120_000);
}
