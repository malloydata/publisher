/**
 * One package, and for each (file, source) in it, what Publisher's query route
 * answers (`publisherRuns`) and what the entities mark (`queryable`).
 * `query_surface.integ.spec.ts` asserts the entities mark each pair queryable
 * exactly as `queryable` says.
 *
 * The two columns differ in one place only, on purpose: a source in a served
 * report that Publisher runs (`servedElsewhere`). That is the report's plain
 * re-import of a source a listed non-report file exports, and any source the
 * report derives from one. The entities advertise only the served source, once,
 * from the file that exports it. The integ test fails if any other case
 * differs.
 *
 * The package is a root `index.malloy` that imports `orders.malloy` and
 * exports only `orders`, plus reports. With no `explores` key, the root
 * `index.malloy` is the surface.
 * Since Publisher 0.9.8 a report is any `.malloy` whose own model-level
 * `## artifact` tag makes one, wherever it lives, so the directory names below
 * mean nothing to Publisher.
 */

export const QUERY_SURFACE_FILES: Record<string, string> = {
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

export type QuerySurfaceCase = {
   file: string;
   source: string;
   /** Whether Publisher runs query text addressed to this source in this file. */
   publisherRuns: boolean;
   /** Whether the entities mark it queryable. */
   queryable: boolean;
   /**
    * A source in a served report that Publisher runs: a re-import of a served
    * declaration, or a source derived from one. The entities leave it to the
    * served source. The only case where the two columns may differ.
    */
   servedElsewhere?: true;
   /**
    * `<file>::<source>` the NOT_QUERYABLE marker names as the served source
    * the entities advertise in this one's place: set on a report's import of a
    * served source, aliased or not, and on a source a report derives from one.
    * Absent everywhere else.
    */
   servedFrom?: string;
   why: string;
};

const both = (
   file: string,
   source: string,
   runs: boolean,
   why: string,
): QuerySurfaceCase => ({
   file,
   source,
   publisherRuns: runs,
   queryable: runs,
   why,
});

const servedElsewhere = (file: string, why: string): QuerySurfaceCase => ({
   file,
   source: "orders",
   publisherRuns: true,
   queryable: false,
   servedElsewhere: true,
   servedFrom: "index.malloy::orders",
   why,
});

/**
 * A source a served report declares on top of `orders`: Publisher runs it,
 * and the entities advertise `orders` from index.malloy in its place.
 */
const derivedInReport = (
   file: string,
   source: string,
   why: string,
): QuerySurfaceCase => ({
   file,
   source,
   publisherRuns: true,
   queryable: false,
   servedElsewhere: true,
   servedFrom: "index.malloy::orders",
   why,
});

export const QUERY_SURFACE_CASES: QuerySurfaceCase[] = [
   both("index.malloy", "orders", true, "the surface file exports it"),
   both("index.malloy", "orders_staging", false, "imported but not exported"),
   both(
      "orders.malloy",
      "orders",
      false,
      "orders.malloy is not an entry point (not the surface, not a report)",
   ),
   both(
      "orders.malloy",
      "orders_staging",
      false,
      "orders.malloy is not an entry point",
   ),
   servedElsewhere(
      "dashboards/overview.malloy",
      "a report re-imports the declaration index.malloy exports; index.malloy advertises it",
   ),
   derivedInReport(
      "dashboards/overview.malloy",
      "open_orders",
      "the report builds it on an exported source",
   ),
   both(
      "dashboards/overview.malloy",
      "orders_staging",
      false,
      "no surface file exports it",
   ),
   both(
      "dashboards/overview.malloy",
      "staged",
      false,
      "built on a source no surface file exports",
   ),
   both(
      "dashboards/overview.malloy",
      "scratch",
      false,
      "built straight off the connection, not on an exported source",
   ),
   both(
      "dashboards/shadow.malloy",
      "orders",
      false,
      "same name as the exported source, but a different declaration",
   ),
   servedElsewhere(
      "notebooks/review.malloy",
      "a linear report re-imports the exported declaration",
   ),
   derivedInReport(
      "notebooks/review.malloy",
      "reviewed_orders",
      "the linear report builds it on an exported source",
   ),
   both(
      "notebooks/review.malloy",
      "orders_staging",
      false,
      "no surface file exports it",
   ),
   both(
      "dashboards/notes.malloy",
      "noted_orders",
      false,
      "a broken note that mentions artifact does not make a report",
   ),
   both(
      "dashboards/malformed.malloy",
      "malformed_orders",
      false,
      "an artifact tag that does not parse does not make a report",
   ),
   both(
      "dashboards/detail.malloy",
      "detail_orders",
      false,
      "an untagged file is no report, even when it imports one",
   ),
   both(
      "dashboards/query_tag.malloy",
      "query_tag_orders",
      false,
      "an artifact tag on a named query is not read, so no report",
   ),
   derivedInReport(
      "dashboards/chain.malloy",
      "chain_64",
      "64 derivation hops from an exported source",
   ),
   both(
      "dashboards/chain.malloy",
      "chain_65",
      false,
      "65 hops, one past Publisher's walk",
   ),
   both(
      "dashboards/bare.malloy",
      "bare_orders",
      false,
      "a tag that lists no tiles makes no report",
   ),
   both(
      "notebooks/legacy.malloy",
      "legacy_orders",
      false,
      "a tag that sets kind= makes no report",
   ),
   both(
      "dashboards/shared.malloy",
      "orders",
      false,
      "an untagged file is not a report, so not an entry point",
   ),
   both(
      "dashboards/shared.malloy",
      "shared_orders",
      false,
      "not an entry point",
   ),
   derivedInReport(
      "reports/overview.malloy",
      "dup_orders",
      "a report sharing a basename with another is still served",
   ),
   servedElsewhere(
      "reports/overview.malloy",
      "a grid report re-imports the exported declaration",
   ),
   both(
      "reports/tileless.malloy",
      "tileless_orders",
      false,
      "a grid tag that names no tiles is not served",
   ),
   both(
      "reports/empty.malloy",
      "empty_orders",
      false,
      "tiles=[] is a grid with no tiles, which is not served",
   ),
   derivedInReport(
      "reports/linear.malloy",
      "linear_orders",
      "layout=linear is a served report",
   ),
   derivedInReport(
      "top.malloy",
      "top_orders",
      "a report at the package root is served like any other",
   ),
   both(
      "public/page.malloy",
      "page_orders",
      false,
      "nothing under public/ is a report",
   ),
   servedElsewhere(
      "reports/named.malloy",
      "a report's named import from the surface file is the declaration it exports",
   ),
   derivedInReport(
      "reports/named.malloy",
      "named_open",
      "the report builds it on the source it imports by name",
   ),
   {
      file: "reports/aliased.malloy",
      source: "o",
      publisherRuns: false,
      queryable: false,
      servedFrom: "index.malloy::orders",
      why: "an aliased import is admitted by no name, but it is still the declaration index.malloy serves",
   },
   both(
      "reports/aliased.malloy",
      "aliased_orders",
      false,
      "built on an alias, which Publisher does not admit",
   ),
];
