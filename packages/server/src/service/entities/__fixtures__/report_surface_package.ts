// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * One package that exercises every way a file is, or is not, a served report,
 * and every way a report's source relates to the served surface: re-imports,
 * derivations (one 64-hop chain, one 65), a shadowing declaration and an
 * aliased import. `model_admits_source.spec.ts` holds, per (file, source),
 * whether the query route runs it; `entities_golden.spec.ts` pins what the
 * entities report for it.
 *
 * The package is a root `index.malloy` that imports `orders.malloy` and
 * exports only `orders`, plus reports. With no `explores` key, the root
 * `index.malloy` is the surface.
 * Since Publisher 0.9.8 a report is any `.malloy` whose own model-level
 * `## artifact` tag makes one, wherever it lives, so the directory names below
 * mean nothing to Publisher.
 */

export const REPORT_SURFACE_FILES: Record<string, string> = {
   "publisher.json": JSON.stringify({ name: "idx-11-dashboard-served" }),
   "orders.malloy": `
source: orders is duckdb.sql("select 1 as id, 'open' as status, 10 as amount") extend {
  measure: order_count is count()
}
source: orders_staging is duckdb.sql("select 2 as id, 'closed' as status") extend {
  measure: staging_count is count()
}
`,
   "index.malloy": `
import "orders.malloy"
export { orders }
`,
   // A grid report. It reads `orders` through its own import, declares one
   // source on top of it, one on top of a source nothing exports, and one
   // straight off the connection.
   "dashboards/overview.malloy": `
## artifact { title="Overview" tiles=["open_count"] }
import "../orders.malloy"
source: open_orders is orders extend { where: status = 'open' }
source: staged is orders_staging extend { where: status = 'closed' }
source: scratch is duckdb.sql("select 1 as x")

query: open_count is open_orders -> { aggregate: order_count }
`,
   // A grid report that declares its own source called `orders`. Same name as
   // the exported one, different declaration.
   "dashboards/shadow.malloy": `
## artifact { title="Shadow" tiles=["shadow_count"] }
source: orders is duckdb.sql("select 3 as id, 'open' as status")

query: shadow_count is orders -> { aggregate: n is count() }
`,
   // A linear report. Like a grid one, an entry point whose own declarations
   // admit nothing by name.
   "notebooks/review.malloy": `
## artifact { layout=linear tiles=["reviewed_orders -> { aggregate: n is count() }"] }
import "../orders.malloy"
source: reviewed_orders is orders extend { where: amount > 5 }
`,
   // A broken note that mentions artifact. Publisher's parser finds no artifact
   // tag in it, so this is a shared include, not a report.
   "dashboards/notes.malloy": `
## note="exported to the artifact store
import "../orders.malloy"
source: noted_orders is orders extend {}
`,
   // An artifact tag that does not parse.
   "dashboards/malformed.malloy": `
## title="Q3 artifact { tiles=[
import "../orders.malloy"
source: malformed_orders is orders extend {}
`,
   // No tag of its own, but it imports a report. A file's own notes decide.
   "dashboards/detail.malloy": `
import "overview.malloy"
import "../orders.malloy"
source: detail_orders is orders extend {}
`,
   // The artifact tag on a named query, not at model level.
   // Publisher 0.9.7 served this as a dashboard; 0.9.8 reads the tag at model
   // level only, so it is no report.
   "dashboards/query_tag.malloy": `
import "../orders.malloy"
source: query_tag_orders is orders extend {}

# artifact { title="Query tag" }
query: query_tag_count is query_tag_orders -> { aggregate: n is count() }
`,
   // A derivation chain one link past Publisher's 64-name walk: chain_64 is
   // 64 hops from orders and runs, chain_65 is 65 and is refused.
   "dashboards/chain.malloy": [
      `## artifact { title="Chain" tiles=["chain_count"] }`,
      `import "../orders.malloy"`,
      `source: chain_1 is orders extend {}`,
      ...Array.from(
         { length: 64 },
         (_, i) => `source: chain_${i + 2} is chain_${i + 1} extend {}`,
      ),
      `query: chain_count is chain_1 -> { aggregate: n is count() }`,
   ].join("\n"),
   // A model-level artifact tag with no tiles: since Publisher 0.9.11, no report.
   "dashboards/bare.malloy": `
## artifact { title="Bare" }
import "../orders.malloy"
source: bare_orders is orders extend {}
`,
   // `kind=` is no longer read: a tag that sets it makes no report.
   "notebooks/legacy.malloy": `
## artifact { kind=dashboard tiles=["legacy_orders -> { aggregate: n is count() }"] }
import "../orders.malloy"
source: legacy_orders is orders extend {}
`,
   // Untagged, so a shared include and not a report.
   "dashboards/shared.malloy": `
import "../orders.malloy"
source: shared_orders is orders extend {}
`,
   // A grid report with the same basename as dashboards/overview.malloy. Two
   // reports may share one; neither hides the other.
   "reports/overview.malloy": `
## artifact { layout=grid tiles=["dup_count"] }
import "../orders.malloy"
source: dup_orders is orders extend {}

query: dup_count is dup_orders -> { aggregate: n is count() }
`,
   // `layout=grid` with no tiles: not served until it names one.
   "reports/tileless.malloy": `
## artifact { layout=grid }
import "../orders.malloy"
source: tileless_orders is orders extend {}
`,
   // `tiles=[]` makes the layout grid, and an empty grid is not served.
   "reports/empty.malloy": `
## artifact { tiles=[] }
import "../orders.malloy"
source: empty_orders is orders extend {}
`,
   // `layout=linear` written out.
   "reports/linear.malloy": `
## artifact { layout=linear tiles=["linear_orders -> { aggregate: n is count() }"] }
import "../orders.malloy"
source: linear_orders is orders extend {}
`,
   // A report at the package root: where a report lives means nothing.
   "top.malloy": `
## artifact { layout=linear tiles=["top_orders -> { aggregate: n is count() }"] }
import "orders.malloy"
source: top_orders is orders extend {}
`,
   // A report that imports the served source by name from the surface file,
   // the way the report builder writes one.
   "reports/named.malloy": `
## artifact { layout=linear tiles=["named_open -> { aggregate: n is count() }"] }
import { orders } from "../index.malloy"
source: named_open is orders extend { where: status = 'open' }
`,
   // A report that imports the served source under another name. Publisher
   // admits a re-import by name, so it refuses `o` and anything built on it.
   "reports/aliased.malloy": `
## artifact { layout=linear tiles=["aliased_orders -> { aggregate: n is count() }"] }
import { o is orders } from "../index.malloy"
source: aliased_orders is o extend {}
`,
   // `public/` is served as static files, so nothing in it is a report.
   "public/page.malloy": `
## artifact { layout=linear }
import "../orders.malloy"
source: page_orders is orders extend {}
`,
};
