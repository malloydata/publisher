// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The conformance fixture's synthetic packages (ms2data/service#7098), each
 * shaped like a construct real packages use. A shape's model text marks every
 * annotated source with `@PERSIST(name)`, which each variant renders:
 *
 *  - `storage`: v1 `#@ persist name=… storage="lake"`
 *  - `colocated`: v1 `#@ persist name=…` (built in the origin warehouse)
 *
 * (`@PERSIST()` renders no `name=`, so the table takes the source's name.)
 *  - `materialize`: `#(materialize) storage="lake"` (v2)
 *
 * `@PREAGG(dim)` marks a rollup measure (`#@ preaggregate grain=…`, with
 * `storage=` for the `storage` variant, and `#(preaggregate)` for
 * `materialize`), and `@PVIEW(name)` a persisted view (`#@ persist name=…`).
 *
 * A shape lists its queries and, per variant and switch, any outcome main does
 * not give yet (a gap): recorded with its reason so the v1 cutover can read
 * the list, and skipped rather than failed.
 */

import { RAW_MODEL } from "./data";

export type Variant = "storage" | "colocated" | "materialize";
export const VARIANTS: Variant[] = ["storage", "colocated", "materialize"];

/** The tier a served query must report. */
export type Expect =
   /** A stored table answered (the default). */
   | "served"
   /** Live by design: the query reads no stored source, or opts out. */
   | "live"
   /** A colocated table answered, whatever the variant renders. */
   | "colocated"
   /** A rollup answered (`servedBy: rollup`). */
   | "rollup"
   /** A persisted view answered (`servedBy: view`). */
   | "view";

export interface ConformanceQuery {
   name: string;
   /** The model file the query runs in (default the shape's entry). */
   model?: string;
   malloy: string;
   givens?: Record<string, unknown>;
   /** Default `served`; by variant when they differ. */
   expect?: Expect | Partial<Record<Variant, Expect>>;
}

/**
 * Why main does not answer a case as the fixture expects:
 *
 *  - `live`: the answer equals live, but no stored table answered;
 *  - `fallback`: with the switch on, today's serve shape answered (equal to
 *    live), not the served copy;
 *  - `metadata`: the rows are right, the result's field metadata is not;
 *  - `not-live`: a stored table answered a query that must be live;
 *  - `wrong`: a stored table answered with different rows;
 *  - `error`: the served query failed;
 *  - `load`: the package does not load or build in that variant.
 */
export interface Gap {
   kind:
      | "live"
      | "fallback"
      | "metadata"
      | "not-live"
      | "wrong"
      | "error"
      | "load";
   reason: string;
   /** The tracker issue it belongs to, when there is one. */
   issue?: string;
}

/**
 * Gaps keyed by `variant/switch` (`storage/off`), `variant` (both switches),
 * or `*`; a query's own `gaps` are read first.
 */
export type GapMap = Partial<Record<string, Gap>>;

export interface Shape {
   /** Short, unique: names the describe block and the gap list row. */
   name: string;
   /** What real-package construct it stands for. */
   about: string;
   issue?: string;
   /** Model files; `@PERSIST(name)` is rendered per variant. The first is the entry. */
   files: Record<string, string>;
   queries: (ConformanceQuery & { gaps?: GapMap })[];
   variants?: Variant[];
   gaps?: GapMap;
}

/** Render a shape's files for one variant. */
export function renderFiles(
   shape: Shape,
   variant: Variant,
): Record<string, string> {
   const out: Record<string, string> = { "raw.malloy": RAW_MODEL };
   for (const [file, text] of Object.entries(shape.files)) {
      out[file] = text
         .replace(/@PREAGG\(([^)]*)\)/g, (_m, grain: string) =>
            variant === "storage"
               ? `#@ preaggregate grain="${grain}" storage="lake"`
               : variant === "colocated"
                 ? `#@ preaggregate grain="${grain}"`
                 : `#(preaggregate) grain=["${grain}"] storage="lake"`,
         )
         .replace(/@PVIEW\(([^)]*)\)/g, (_m, name: string) =>
            variant === "storage"
               ? `#@ persist name="${name}" storage="lake"`
               : `#@ persist name="${name}"`,
         )
         .replace(/@PERSIST_SPLIT\(([^)]*)\)/g, (_m, name: string) =>
            variant === "storage"
               ? `#@ persist name="${name}"\n#@ storage="lake"`
               : `#@ persist\n#@ name="${name}"`,
         )
         .replace(/@PERSIST\(([^)]*)\)/g, (_m, name: string) => {
            switch (variant) {
               case "storage":
                  return `#@ persist${name ? ` name="${name}"` : ""} storage="lake"`;
               case "colocated":
                  return `#@ persist${name ? ` name="${name}"` : ""}`;
               case "materialize":
                  return `#(materialize) storage="lake"`;
            }
         });
   }
   return out;
}

const HEADER = "##! experimental { persistence }";

const GIVENS_HEADER = "##! experimental { persistence givens }";

/**
 * A `storage=` table and a warehouse table cannot meet in one query, so a
 * query joining a stored source to an unstored one (or rooted in an unstored
 * source joining a stored one) is answered live by design; colocated tables
 * sit beside the warehouse and serve it.
 */
const LIVE_ACROSS_CONNECTIONS: Partial<Record<Variant, Expect>> = {
   storage: "live",
   materialize: "live",
};

/**
 * Two sources whose tables take one name in one destination (#6644's class)
 * are both refused rather than mis-served: v1 reads each as `live:refused`,
 * and `#(materialize)` builds neither (materialize-not-built), so each
 * answers live.
 */
const SHARED_TABLE_LIVE: Expect = "live";

/**
 * `#(materialize)` renders no `name=`, so two same-named sources both default
 * to the table `summary` and are both refused (the shared-table class above).
 */
const SAME_NAMED_MATERIALIZE: Partial<Record<Variant, Expect>> = {
   materialize: SHARED_TABLE_LIVE,
};

/** Today's serve shape answers a v1 `storage=` rollup without its measures' annotations. */
const V1_STORAGE_ROLLUP_ANNOTATIONS: Gap = {
   kind: "metadata",
   reason:
      "today's serve shape answers a v1 #@ preaggregate storage= rollup with its measures' annotations (# currency, #(doc)) dropped; the cutover's rollup leg answers with them (G4), and the serve shape retires",
};

/** A `#@ persist` view is planned and read only with the materialize switch on. */
const PERSISTED_VIEW_OFF: Gap = {
   kind: "live",
   reason:
      "a #@ persist view is planned and served only with PUBLISHER_MATERIALIZE on (phase 4's view leg)",
};

/** Guard 1: a v1 `#@ persist` in a file without the flag is not planned before the cutover. */
const UNFLAGGED_BEFORE_CUTOVER: Gap = {
   kind: "live",
   reason:
      "a model file without ##! experimental.persistence plans no #@ persist source before the cutover (guard 1); the cutover's build copy carries the flag (D3)",
};

/** Today's serve shape compiles a model's sources together, so one that cannot be shaped takes the others live. */
const SERVE_SHAPE_FAILS_BESIDE: Gap = {
   kind: "live",
   reason:
      "today's serve shape does not compile this model (the re-hidden source's measure reads a column its storage= table does not hold), so every source in it answers live; the cutover's served copy contains that failure and serves the source beside it",
};

/** A query live answers wrong on Malloy 0.0.438, whose stored answer is right. */
export const LIVE_WRONG_NEST: Gap = {
   kind: "wrong",
   reason:
      "live is wrong here, not the stored answer: Malloy 0.0.438 (malloydata/malloy#3208) drops nested rows under a NULL parent key live (malloydata/malloy#3220); served_copy_conformance_gaps checks the stored answer against the truth",
};

export const SHAPES: Shape[] = [
   {
      name: "baseline",
      about: "A query-shaped persisted source with an extend block, its view and an extension (the bundled persist-test package's shape)",
      files: {
         "m.malloy": `${HEADER}
import "raw.malloy"

@PERSIST(daily)
source: daily is orders_raw -> {
  group_by: region, order_date
  aggregate: total is amount.sum(), n is count()
} extend {
  dimension: big is total > 300
  measure: grand is total.sum()
  view: by_region is { group_by: region; aggregate: grand }
}

source: daily_ext is daily extend {
  measure: avg_total is total.avg()
}
`,
      },
      queries: [
         {
            name: "group by a stored dimension",
            malloy: "run: daily -> { group_by: region, big; aggregate: grand }",
         },
         { name: "a declared view", malloy: "run: daily -> by_region" },
         {
            name: "an extension's measure",
            malloy:
               "run: daily_ext -> { group_by: region; aggregate: avg_total }",
         },
      ],
   },
   {
      name: "imports and re-exports",
      about: "The persisted source in one file, re-exported by an index, imported under another name and extended in an importer",
      files: {
         "index.malloy": `import "facts.malloy"
import "dash.malloy"
export { daily, daily_ext, d_plus, by_status }
`,
         "facts.malloy": `${HEADER}
import "raw.malloy"

@PERSIST(daily)
source: daily is orders_raw -> {
  group_by: region, status, order_date
  aggregate: total is amount.sum(), n is count()
} extend {
  measure: grand is total.sum()
}

source: daily_ext is daily extend {
  measure: orders is n.sum()
}
`,
         "dash.malloy": `${HEADER}
import { d is daily } from "facts.malloy"

source: d_plus is d extend {
  dimension: is_returned is status = 'returned'
}

source: by_status is d -> {
  group_by: status
  aggregate: s is total.sum()
}
`,
      },
      queries: [
         {
            name: "through the index's re-export",
            malloy: "run: daily -> { group_by: region; aggregate: grand }",
         },
         {
            name: "an extension through the re-export",
            malloy: "run: daily_ext -> { group_by: status; aggregate: orders }",
         },
         {
            name: "imported under another name, in the importer",
            model: "dash.malloy",
            malloy: "run: d -> { group_by: region; aggregate: grand }",
            gaps: {
               "storage/off": {
                  kind: "live",
                  reason:
                     "the serve shape does not serve a source imported under another name (`import { d is daily }`)",
               },
            },
         },
         {
            name: "an importer's extension of the renamed import",
            malloy:
               "run: d_plus -> { group_by: is_returned; aggregate: grand }",
         },
         {
            name: "an importer's query-shaped source over it",
            malloy:
               "run: by_status -> { group_by: status; aggregate: t is s.sum() }",
            gaps: {
               "storage/off": {
                  kind: "live",
                  reason:
                     "the serve shape does not serve a source imported under another name (`import { d is daily }`)",
               },
            },
         },
      ],
   },
   {
      name: "renames",
      about: "rename: in the persisted source's extend block, a chain of renames through extensions, and an aliased group_by",
      gaps: {
         "storage/off": {
            kind: "live",
            reason:
               "the serve shape does not serve a source with rename: in its extend block or an extension renaming its fields",
         },
      },
      files: {
         "m.malloy": `${HEADER}
import "raw.malloy"

@PERSIST(daily_r)
source: daily_r is orders_raw -> {
  group_by: area is region, order_date
  aggregate: total is amount.sum()
} extend {
  rename: order_day is order_date
  measure: grand is total.sum()
}

source: daily_r2 is daily_r extend {
  rename: zone is area
}

source: daily_r3 is daily_r2 extend {
  rename: territory is zone
  dimension: day_month is order_day.month
}
`,
      },
      queries: [
         {
            name: "a field renamed in the extend block",
            malloy: "run: daily_r -> { group_by: order_day; aggregate: grand }",
         },
         {
            name: "a rename in an extension",
            malloy: "run: daily_r2 -> { group_by: zone; aggregate: grand }",
         },
         {
            name: "a rename of a rename, and a dimension over a renamed field",
            malloy:
               "run: daily_r3 -> { group_by: territory, day_month; aggregate: grand }",
         },
      ],
   },
   {
      name: "inline extend in joins",
      about: "A persisted query that reads through a join to an inline-extended source; an extension, in another file, joining an inline-extended warehouse source",
      files: {
         "m.malloy": `${HEADER}
import "raw.malloy"

@PERSIST(by_customer)
source: by_customer is orders_raw extend {
  join_one: c is customers_raw extend {
    dimension: seg_up is upper(segment)
  } on customer_id = c.id
} -> {
  group_by: customer_id, c.seg_up
  aggregate: total is amount.sum(), n is count()
}
`,
         "plus.malloy": `${HEADER}
import "raw.malloy"
import "m.malloy"

source: by_customer_plus is by_customer extend {
  join_one: cust is customers_raw extend {
    dimension: home is upper(home_region)
  } on customer_id = cust.id
  measure: grand is total.sum()
}
`,
      },
      queries: [
         {
            name: "a stored column computed through the inline join",
            malloy:
               "run: by_customer -> { group_by: seg_up; aggregate: t is total.sum() }",
         },
         {
            name: "a join's inline dimension on the extension",
            model: "plus.malloy",
            malloy:
               "run: by_customer_plus -> { group_by: cust.home; aggregate: grand }",
            expect: LIVE_ACROSS_CONNECTIONS,
         },
         {
            name: "a join-level aggregate on the extension",
            model: "plus.malloy",
            malloy:
               "run: by_customer_plus -> { group_by: seg_up; aggregate: grand, customers is cust.count() }",
            expect: LIVE_ACROSS_CONNECTIONS,
         },
      ],
   },
   {
      name: "multi-line annotations",
      about: "The persist annotation inside a block of annotations, a #(doc) over two lines, annotations on the query's output fields and on the extend block's measure, and a model-level tag",
      files: {
         "m.malloy": `${HEADER}
## renderer_hint=table
import "raw.malloy"

#(doc) "Orders per region and month."
#(doc) "Rebuilt nightly."
@PERSIST(annotated)
# dashboard
source: annotated is orders_raw -> {
  group_by:
    #(doc) "The order's region"
    # label="Region"
    region
    order_month is order_date.month
  aggregate:
    # currency
    total is amount.sum()
} extend {
  #(doc) "Total over every row"
  # currency
  # number="#,##0.00"
  measure: grand is total.sum()
}
`,
      },
      queries: [
         {
            name: "an annotated output field and an annotated measure",
            malloy:
               "run: annotated -> { group_by: region, order_month; aggregate: grand }",
            gaps: {
               "storage/off": {
                  kind: "metadata",
                  issue: "#7089",
                  reason:
                     "the serve shape drops #(doc) and # label on a persisted query's output field",
               },
            },
         },
         {
            name: "a stored measure column re-aggregated",
            malloy:
               "run: annotated -> { group_by: order_month; aggregate: t is total.sum() }",
            gaps: {
               "storage/off": {
                  kind: "metadata",
                  issue: "#7089",
                  reason: "the serve shape drops a stored column's timeframe",
               },
            },
         },
      ],
   },
   {
      name: "split persist annotation",
      about: "The persist tag's properties over two #@ lines (v1 only)",
      variants: ["storage", "colocated"],
      files: {
         "m.malloy": `${HEADER}
import "raw.malloy"

@PERSIST_SPLIT(split_daily)
source: split_daily is orders_raw -> {
  group_by: region
  aggregate: total is amount.sum()
}
`,
      },
      queries: [
         {
            name: "the source the split tag persists",
            malloy:
               "run: split_daily -> { group_by: region; aggregate: t is total.sum() }",
         },
      ],
   },
   {
      name: "query-shaped: two stages",
      about: "A persisted multi-stage query (`-> {…} -> {…}`)",
      files: {
         "m.malloy": `${HEADER}
import "raw.malloy"

@PERSIST(two_stage)
source: two_stage is orders_raw -> {
  group_by: customer_id, region
  aggregate: t is amount.sum()
} -> {
  group_by: region
  aggregate: customers is count(), t2 is t.sum()
}
`,
      },
      queries: [
         {
            name: "the second stage's columns",
            malloy:
               "run: two_stage -> { group_by: region; aggregate: c is customers.sum(), t is t2.sum() }",
         },
      ],
   },
   {
      name: "query-shaped: from a view",
      about: "A source persisted from a view (`source: x is base -> view`), a query-shaped reader over it, and a named query on it",
      files: {
         "m.malloy": `${HEADER}
import "raw.malloy"

source: orders_v is orders_raw extend {
  view: by_region_status is {
    group_by: region, status
    aggregate: total is amount.sum(), n is count()
  }
}

@PERSIST(from_view)
source: from_view is orders_v -> by_region_status

source: from_view_rollup is from_view -> {
  group_by: status
  aggregate: total2 is total.sum()
}

query: top_regions is from_view -> {
  group_by: region
  aggregate: t is total.sum()
  order_by: t desc
  limit: 2
}
`,
      },
      queries: [
         {
            name: "the view's output",
            malloy:
               "run: from_view -> { group_by: status; aggregate: n2 is n.sum() }",
         },
         {
            name: "a query-shaped reader",
            malloy:
               "run: from_view_rollup -> { group_by: status; aggregate: t is total2.sum() }",
         },
         {
            name: "a named query",
            malloy: "run: top_regions",
            gaps: {
               "storage/off": {
                  kind: "live",
                  reason:
                     "the serve shape does not serve a named query (`run: q`) over a stored source",
               },
            },
         },
      ],
   },
   {
      name: "query-shaped: a nest",
      about: "A persisted query with a nest: (a repeated record column)",
      gaps: {
         "storage/off": {
            kind: "live",
            reason: "the serve shape answers a persisted nest live",
         },
      },
      files: {
         "m.malloy": `${HEADER}
import "raw.malloy"

@PERSIST(nested)
source: nested is orders_raw -> {
  group_by: region
  aggregate: total is amount.sum()
  nest: by_status is {
    group_by: status
    aggregate: n is count()
  }
}
`,
      },
      queries: [
         {
            name: "the top level",
            malloy:
               "run: nested -> { group_by: region; aggregate: t is total.sum() }",
         },
         {
            name: "through the nested rows",
            malloy:
               "run: nested -> { group_by: by_status.status; aggregate: n is by_status.n.sum() }",
            gaps: {
               "storage/on": LIVE_WRONG_NEST,
               "storage/cutover": LIVE_WRONG_NEST,
               "colocated/cutover": LIVE_WRONG_NEST,
               "materialize/on": LIVE_WRONG_NEST,
            },
         },
      ],
   },
   {
      name: "reserved-word names",
      about: "A source and its fields named by reserved words, quoted in backticks, persisted under a reserved table name",
      files: {
         "m.malloy": `${HEADER}
import "raw.malloy"

@PERSIST(order)
source: \`order\` is orders_raw -> {
  group_by: \`order\`, \`select\` is status
  aggregate: \`count\` is count(), amt is amount.sum()
} extend {
  measure: \`group\` is amt.sum()
}
`,
      },
      queries: [
         {
            name: "reserved-word fields",
            malloy:
               "run: `order` -> { group_by: `select`, `order`; aggregate: c is `count`.sum(), `group` }",
         },
      ],
   },
   {
      name: "a name with a space",
      about: "A stored column whose name has a space, quoted in backticks",
      files: {
         "m.malloy": `${HEADER}
import "raw.malloy"

@PERSIST(spaced)
source: spaced is orders_raw -> {
  group_by: status
  aggregate: \`line total\` is amount.sum()
}
`,
      },
      queries: [
         {
            name: "the spaced column",
            malloy:
               "run: spaced -> { group_by: status; aggregate: lt is `line total`.sum() }",
         },
      ],
   },
   {
      name: "same-named sources in two models",
      issue: "#6644",
      about: "Two models each persisting a source named summary, and a third importing both under other names",
      files: {
         "a.malloy": `${HEADER}
import "raw.malloy"

@PERSIST(summary_placed)
source: summary is orders_raw -> {
  where: status = 'placed'
  group_by: region
  aggregate: total is amount.sum()
}
`,
         "b.malloy": `${HEADER}
import "raw.malloy"

@PERSIST(summary_shipped)
source: summary is orders_raw -> {
  where: status = 'shipped'
  group_by: region
  aggregate: total is amount.sum()
}
`,
         "c.malloy": `${HEADER}
import { placed is summary } from "a.malloy"
import { shipped is summary } from "b.malloy"
`,
      },
      queries: [
         {
            name: "summary in a.malloy",
            model: "a.malloy",
            malloy:
               "run: summary -> { group_by: region; aggregate: t is total.sum() }",
            expect: SAME_NAMED_MATERIALIZE,
            gaps: {
               "storage/off": {
                  kind: "live",
                  issue: "#6644",
                  reason:
                     "the serve shape does not serve a source whose name another model also persists (the v1 fix refuses rather than mis-serves)",
               },
            },
         },
         {
            name: "summary in b.malloy",
            model: "b.malloy",
            malloy:
               "run: summary -> { group_by: region; aggregate: t is total.sum() }",
            expect: SAME_NAMED_MATERIALIZE,
            gaps: {
               "storage/off": {
                  kind: "live",
                  issue: "#6644",
                  reason:
                     "the serve shape does not serve a source whose name another model also persists (the v1 fix refuses rather than mis-serves)",
               },
            },
         },
         {
            name: "a.malloy's summary imported as placed",
            model: "c.malloy",
            malloy:
               "run: placed -> { group_by: region; aggregate: t is total.sum() }",
            expect: SAME_NAMED_MATERIALIZE,
         },
         {
            name: "b.malloy's summary imported as shipped",
            model: "c.malloy",
            malloy:
               "run: shipped -> { group_by: region; aggregate: t is total.sum() }",
            expect: SAME_NAMED_MATERIALIZE,
         },
      ],
   },
   {
      name: "same-named sources in two models, no name=",
      issue: "#6644",
      about: "Two models each persisting a source named summary with no name=, so both tables take the source's name",
      files: {
         "a.malloy": `${HEADER}
import "raw.malloy"

@PERSIST()
source: summary is orders_raw -> {
  where: status = 'placed'
  group_by: region
  aggregate: total is amount.sum()
}
`,
         "b.malloy": `${HEADER}
import "raw.malloy"

@PERSIST()
source: summary is orders_raw -> {
  where: status = 'shipped'
  group_by: region
  aggregate: total is amount.sum()
}
`,
      },
      queries: [
         {
            name: "summary in a.malloy",
            model: "a.malloy",
            malloy:
               "run: summary -> { group_by: region; aggregate: t is total.sum() }",
            expect: SHARED_TABLE_LIVE,
         },
         {
            name: "summary in b.malloy",
            model: "b.malloy",
            malloy:
               "run: summary -> { group_by: region; aggregate: t is total.sum() }",
            expect: SHARED_TABLE_LIVE,
         },
      ],
   },
   {
      name: "joined persisted source from another file",
      issue: "#6643",
      about: "A persisted dimension source declared in one file, joined by a persisted fact in a second, queried through an index that imports only the second",
      files: {
         "index.malloy": `import "facts.malloy"
export { order_facts }
`,
         "dims.malloy": `${HEADER}
import "raw.malloy"

@PERSIST(customer_dim)
source: customer_dim is customers_raw -> {
  select: id, segment, home_region
} extend {
  primary_key: id
}
`,
         "facts.malloy": `${HEADER}
import "raw.malloy"
import "dims.malloy"

@PERSIST(order_facts)
source: order_facts is orders_raw -> {
  select: id, customer_id, region, amount
} extend {
  join_one: c is customer_dim on customer_id = c.id
}
`,
         "live_join.malloy": `${HEADER}
import "raw.malloy"
import "dims.malloy"

source: orders_j is orders_raw extend {
  join_one: c is customer_dim on customer_id = c.id
}
`,
      },
      queries: [
         {
            name: "a persisted fact through its join, from the index",
            malloy:
               "run: order_facts -> { group_by: c.segment; aggregate: t is amount.sum() }",
         },
         {
            name: "a persisted fact through its join, in its own file",
            model: "facts.malloy",
            malloy:
               "run: order_facts -> { group_by: c.segment; aggregate: t is amount.sum() }",
         },
         {
            name: "the joined source directly, in its own file",
            model: "dims.malloy",
            malloy:
               "run: customer_dim -> { group_by: segment; aggregate: n is count() }",
         },
         {
            name: "a warehouse source joining it",
            model: "live_join.malloy",
            malloy:
               "run: orders_j -> { group_by: c.segment; aggregate: t is amount.sum() }",
            expect: LIVE_ACROSS_CONNECTIONS,
            gaps: {
               "colocated/off": {
                  kind: "live",
                  issue: "#6643",
                  reason:
                     "a warehouse source joining a colocated persisted source is answered live: the colocated manifest is not applied to a joined source",
               },
               "colocated/on": {
                  kind: "live",
                  issue: "#6643",
                  reason:
                     "a warehouse source joining a colocated persisted source is answered live: the colocated manifest is not applied to a joined source",
               },
            },
         },
      ],
   },
   {
      name: "joined persisted source with hidden fields, from an index",
      issue: "#1666",
      about: "A persisted dimension source with an internal column, and measures over it (one private), held by the index only as a join of a persisted fact in another file (P3 with #6643's shape)",
      files: {
         "index.malloy": `import "facts.malloy"
export { order_facts }
`,
         "dims.malloy": `##! experimental { persistence access_modifiers }
import "raw.malloy"

@PERSIST(customer_dim)
source: customer_dim is customers_raw -> {
  select: id, segment, home_region
} include { internal: home_region; public: * } extend {
  primary_key: id
  private measure: west_rows is count() { where: home_region = 'west' }
  measure: west_n is west_rows + 0
}
`,
         "facts.malloy": `${HEADER}
import "raw.malloy"
import "dims.malloy"

@PERSIST(order_facts)
source: order_facts is orders_raw -> {
  select: id, customer_id, region, amount
} extend {
  join_one: c is customer_dim on customer_id = c.id
}
`,
      },
      queries: [
         {
            name: "a measure over the joined source's internal column, from the index",
            malloy:
               "run: order_facts -> { group_by: c.segment; aggregate: w is c.west_n, t is amount.sum() }",
            expect: { storage: "live" },
         },
         {
            name: "the joined source's measure, in its own file",
            model: "dims.malloy",
            malloy:
               "run: customer_dim -> { group_by: segment; aggregate: west_n }",
            // v1 stores a persisted table's public columns only, so a measure
            // reading `home_region` is answered live there.
            expect: { storage: "live" },
         },
      ],
   },
   {
      name: "keyed join_one with",
      issue: "#6983",
      about: "A persisted source with primary_key: joined by `join_one: … with`, from a persisted source and from a warehouse source",
      files: {
         "m.malloy": `${HEADER}
import "raw.malloy"

@PERSIST(cust)
source: cust is customers_raw -> {
  select: id, segment, home_region
} extend {
  primary_key: id
}

@PERSIST(order_facts)
source: order_facts is orders_raw -> {
  select: id, customer_id, region, amount
} extend {
  join_one: c is cust with customer_id
}
`,
         "live_join.malloy": `${HEADER}
import "raw.malloy"
import "m.malloy"

source: orders_k is orders_raw extend {
  join_one: c is cust with customer_id
}
`,
      },
      queries: [
         {
            name: "a persisted source joining it with `with`",
            malloy:
               "run: order_facts -> { group_by: c.segment, region; aggregate: t is amount.sum() }",
            gaps: {
               "storage/off": {
                  kind: "live",
                  issue: "#6983",
                  reason:
                     "the serve shape drops primary_key:, so a `join_one: … with` to a stored source answers live",
               },
            },
         },
         {
            name: "a warehouse source joining it with `with`",
            model: "live_join.malloy",
            malloy:
               "run: orders_k -> { group_by: c.segment; aggregate: t is amount.sum() }",
            expect: LIVE_ACROSS_CONNECTIONS,
            gaps: {
               "colocated/off": {
                  kind: "live",
                  issue: "#6983",
                  reason:
                     "a warehouse source joining a colocated persisted source is answered live: the colocated manifest is not applied to a joined source",
               },
               "colocated/on": {
                  kind: "live",
                  issue: "#6983",
                  reason:
                     "a warehouse source joining a colocated persisted source is answered live: the colocated manifest is not applied to a joined source",
               },
            },
         },
      ],
   },
   {
      name: "doc, percent and timeframes carried",
      issue: "#7089",
      about: "Stored timeframe columns, #(doc) and # percent on stored and extend-block fields; results' types and annotations must match live",
      files: {
         "m.malloy": `${HEADER}
import "raw.malloy"

@PERSIST(monthly)
source: monthly is orders_raw -> {
  group_by:
    #(doc) "Calendar month of the order"
    order_month is order_date.month
    created_hour is created_at.hour
    order_week is order_date.week
    region
  aggregate:
    total is amount.sum()
    # percent
    discount_rate is discount.avg()
} extend {
  #(doc) "Share of all orders' amount"
  # percent
  measure: share is total.sum() / all(total.sum())
}
`,
      },
      queries: [
         {
            name: "a stored month, with its doc",
            malloy:
               "run: monthly -> { group_by: order_month; aggregate: share }",
            gaps: {
               "storage/off": {
                  kind: "metadata",
                  issue: "#7089",
                  reason: "the serve shape drops a stored column's timeframe",
               },
            },
         },
         {
            name: "a stored hour and week",
            malloy:
               "run: monthly -> { group_by: created_hour, order_week; aggregate: t is total.sum() }",
            gaps: {
               "storage/off": {
                  kind: "metadata",
                  issue: "#7089",
                  reason: "the serve shape drops a stored column's timeframe",
               },
            },
         },
         {
            name: "a filter on a stored timeframe",
            malloy:
               "run: monthly -> { where: order_month = @2025-01; group_by: region; aggregate: t is total.sum() }",
         },
         {
            name: "a percent column selected",
            malloy:
               "run: monthly -> { select: region, discount_rate; order_by: region, discount_rate }",
            gaps: {
               "storage/off": {
                  kind: "metadata",
                  issue: "#7089",
                  reason:
                     "the serve shape drops # percent on a persisted query's output field",
               },
            },
         },
      ],
   },
   {
      name: "extensions of persisted sources",
      about: "A chain of extensions with a where: and a dimension; a query-shaped source over an extension; an extension in another file; and one joining a warehouse source",
      files: {
         "m.malloy": `${HEADER}
import "raw.malloy"

@PERSIST(base_daily)
source: base_daily is orders_raw -> {
  group_by: customer_id, region, status, order_date
  aggregate: total is amount.sum(), n is count()
}

source: ext1 is base_daily extend {
  where: status != 'returned'
  measure: grand is total.sum()
}

source: ext2 is ext1 extend {
  dimension: winter is order_date.month = @2025-01 or order_date.month = @2024-12
}

source: ext2_rollup is ext2 -> {
  group_by: region, winter
  aggregate: g is grand
}
`,
         "other.malloy": `${HEADER}
import "m.malloy"

source: ext_other is ext1 extend {
  measure: orders is n.sum()
}
`,
         "joined.malloy": `${HEADER}
import "raw.malloy"
import "m.malloy"

source: ext_joined is ext2 extend {
  join_one: c is customers_raw on customer_id = c.id
}
`,
      },
      queries: [
         {
            name: "an extension with a where:",
            malloy: "run: ext1 -> { group_by: region; aggregate: grand }",
         },
         {
            name: "an extension of an extension",
            malloy: "run: ext2 -> { group_by: winter; aggregate: grand }",
         },
         {
            name: "a query-shaped source over an extension",
            malloy:
               "run: ext2_rollup -> { group_by: winter; aggregate: t is g.sum() }",
         },
         {
            name: "an extension in another file",
            model: "other.malloy",
            malloy:
               "run: ext_other -> { group_by: status; aggregate: orders, grand }",
         },
         {
            name: "an extension joining a warehouse source",
            model: "joined.malloy",
            malloy:
               "run: ext_joined -> { group_by: c.segment; aggregate: grand }",
            expect: LIVE_ACROSS_CONNECTIONS,
         },
      ],
   },
   {
      name: "a warehouse join beside a stored source",
      about: "One model holding a persisted source and, beside it, a warehouse source that joins it: a query on the persisted source alone must still be served",
      files: {
         "m.malloy": `${HEADER}
import "raw.malloy"

@PERSIST(cust_b)
source: cust_b is customers_raw -> {
  select: id, segment
} extend {
  primary_key: id
}

source: orders_b is orders_raw extend {
  join_one: c is cust_b with customer_id
}
`,
      },
      queries: [
         {
            name: "the persisted source alone",
            malloy:
               "run: cust_b -> { group_by: segment; aggregate: n is count() }",
         },
         {
            name: "the warehouse source through its join",
            malloy:
               "run: orders_b -> { group_by: c.segment; aggregate: t is amount.sum() }",
            expect: LIVE_ACROSS_CONNECTIONS,
            gaps: {
               "colocated/off": {
                  kind: "live",
                  issue: "#6643",
                  reason:
                     "a warehouse source joining a colocated persisted source is answered live: the colocated manifest is not applied to a joined source",
               },
               "colocated/on": {
                  kind: "live",
                  issue: "#6643",
                  reason:
                     "a warehouse source joining a colocated persisted source is answered live: the colocated manifest is not applied to a joined source",
               },
            },
         },
      ],
   },
   {
      name: "givens and access filters",
      about: "A per-caller where: on a persisted source's extend block, and #(access_filter) gates on an extension of it and on a persisted source itself",
      files: {
         "m.malloy": `${GIVENS_HEADER}
import "raw.malloy"

given: REGION :: string is 'north'
given: REGIONS :: string[]

@PERSIST(scoped)
source: scoped is orders_raw -> {
  select: id, region, status, amount, order_date
} extend {
  where: region = $REGION
  measure: total is amount.sum()
}

#(access_filter) region in $REGIONS
source: gated_ext is scoped extend {}

#(access_filter) region in $REGIONS
@PERSIST(gated)
source: gated is orders_raw -> {
  select: id, region, status, amount
} extend {
  measure: total is amount.sum()
}
`,
      },
      queries: [
         {
            name: "a per-caller where:, one caller",
            malloy: "run: scoped -> { group_by: status; aggregate: total }",
            givens: { REGION: "north" },
         },
         {
            name: "a per-caller where:, another caller",
            malloy: "run: scoped -> { group_by: status; aggregate: total }",
            givens: { REGION: "east" },
         },
         {
            // A gate is refused for storage= by design (docs/materialization.md).
            name: "an access-filtered extension",
            malloy: "run: gated_ext -> { group_by: status; aggregate: total }",
            givens: { REGION: "south", REGIONS: ["south", "west"] },
            expect: { storage: "live", materialize: "live" },
         },
         {
            name: "an access-filtered persisted source",
            malloy: "run: gated -> { group_by: region; aggregate: total }",
            givens: { REGIONS: ["north", "east"] },
            expect: { storage: "live", materialize: "live" },
         },
         {
            // The same table, another caller: after the cutover a colocated
            // table is read past the gate (G3), each caller's rows only.
            name: "an access-filtered persisted source, another caller",
            malloy: "run: gated -> { group_by: region; aggregate: total }",
            givens: { REGIONS: ["west"] },
            expect: { storage: "live", materialize: "live" },
         },
         {
            name: "an access-filtered extension, another caller",
            malloy: "run: gated_ext -> { group_by: status; aggregate: total }",
            givens: { REGION: "west", REGIONS: ["west"] },
            expect: { storage: "live", materialize: "live" },
         },
      ],
   },
   {
      name: "#@ -persist readers",
      about: "An extension that opts out with #@ -persist, a reader built on it, and a sibling that does not opt out",
      files: {
         "m.malloy": `${HEADER}
import "raw.malloy"

@PERSIST(summary)
source: summary is orders_raw -> {
  group_by: region, status
  aggregate: total is amount.sum()
}

#@ -persist
source: summary_fresh is summary extend {
  measure: grand is total.sum()
}

source: summary_fresh_rollup is summary_fresh -> {
  group_by: region
  aggregate: g is grand
}

source: summary_kept is summary extend {
  measure: grand is total.sum()
}
`,
      },
      queries: [
         {
            // Malloy core's opt-out, honoured in v1 and v2 alike (plan,
            // question 5, decided 2026-11-12): the reader recomputes live.
            name: "the opted-out reader",
            malloy:
               "run: summary_fresh -> { group_by: region; aggregate: grand }",
            expect: "live",
         },
         {
            name: "a reader built on the opted-out one",
            malloy:
               "run: summary_fresh_rollup -> { group_by: region; aggregate: t is g.sum() }",
            expect: "live",
         },
         {
            name: "a sibling that does not opt out",
            malloy:
               "run: summary_kept -> { group_by: region; aggregate: grand }",
         },
      ],
   },
   {
      name: "chained persisted sources",
      about: "A persisted source built over another, in one file and across files (the bundled persist-multi-level package's shape)",
      files: {
         "rollup.malloy": `${HEADER}
import "base.malloy"

@PERSIST(orders_rollup_x)
source: orders_rollup_x is orders_base -> {
  group_by: region
  aggregate: statuses is count(), t is total_amount.sum()
}
`,
         "base.malloy": `${HEADER}
import "raw.malloy"

@PERSIST(orders_base)
source: orders_base is orders_raw -> {
  group_by: region, status
  aggregate: total_amount is amount.sum()
}

@PERSIST(orders_rollup)
source: orders_rollup is orders_base -> {
  group_by: status
  aggregate: regions is count(), t is total_amount.sum()
}
`,
      },
      queries: [
         {
            name: "the downstream source in the same file",
            model: "base.malloy",
            malloy:
               "run: orders_rollup -> { group_by: status; aggregate: r is regions.sum(), t2 is t.sum() }",
         },
         {
            name: "the downstream source in another file",
            malloy:
               "run: orders_rollup_x -> { group_by: region; aggregate: s is statuses.sum(), t2 is t.sum() }",
         },
         {
            name: "the upstream source through the importer",
            malloy:
               "run: orders_base -> { group_by: status; aggregate: t is total_amount.sum() }",
         },
      ],
   },
   {
      name: "persist on a raw SQL source",
      about: "The persist annotation directly on a duckdb.sql(...) source, with no -> query",
      files: {
         "m.malloy": `${HEADER}

@PERSIST(raw_direct)
source: raw_direct is duckdb.sql("""
  SELECT region, CAST(amount AS DOUBLE) AS amount
  FROM read_csv('data/orders.csv', header=true, all_varchar=true)
""") extend {
  measure: total is amount.sum()
}
`,
      },
      queries: [
         {
            name: "the source itself",
            malloy: "run: raw_direct -> { group_by: region; aggregate: total }",
         },
      ],
   },
   {
      name: "colocated and storage= in one package",
      about: "A colocated dimension source and a storage= fact source in one model (fixed annotations, not rendered per variant)",
      variants: ["storage"],
      files: {
         "m.malloy": `${HEADER}
import "raw.malloy"

#@ persist name="dim_colocated"
source: dim_c is customers_raw -> {
  select: id, segment
} extend {
  primary_key: id
}

#@ persist name="fact_stored" storage="lake"
source: fact_s is orders_raw -> {
  select: id, customer_id, region, amount
} extend {
  measure: total is amount.sum()
}
`,
         "joined.malloy": `${HEADER}
import "m.malloy"

source: fact_joined is fact_s extend {
  join_one: c is dim_c with customer_id
}
`,
      },
      queries: [
         {
            name: "the colocated source alone",
            malloy:
               "run: dim_c -> { group_by: segment; aggregate: n is count() }",
            expect: "colocated",
         },
         {
            name: "the storage= source alone",
            malloy: "run: fact_s -> { group_by: region; aggregate: total }",
         },
         {
            // The two tables are on different connections.
            name: "the storage= source joined to the colocated one",
            model: "joined.malloy",
            malloy:
               "run: fact_joined -> { group_by: c.segment; aggregate: total }",
            expect: "live",
         },
      ],
   },
   {
      name: "materialize: a refined source with an inline join",
      about: "#(materialize) on a source declared by refinement, with an inline-extended join_one and computed dimensions (v2 only: v1 persists query-shaped sources)",
      variants: ["materialize"],
      files: {
         "m.malloy": `${HEADER}
import "raw.malloy"

@PERSIST(enriched)
source: enriched is orders_raw extend {
  join_one: c is customers_raw extend {
    dimension: seg_up is upper(segment)
  } on customer_id = c.id
  dimension:
    #(doc) "Month of the order"
    order_month is order_date.month
    big is amount > 100
  measure: total is amount.sum()
}

source: enriched_ext is enriched extend {
  measure: n is count()
}
`,
      },
      queries: [
         {
            name: "a computed timeframe dimension",
            malloy:
               "run: enriched -> { group_by: order_month; aggregate: total }",
         },
         {
            name: "the inline join's dimension",
            malloy:
               "run: enriched -> { group_by: c.seg_up, big; aggregate: total }",
            gaps: {
               "materialize/on": {
                  kind: "live",
                  reason:
                     "the inline join_one is not stored with the source yet (phase 3), so a query through it reads a warehouse source the stored table cannot reach, and is answered live (served-copy-not-served)",
               },
            },
         },
         {
            name: "an extension",
            malloy:
               "run: enriched_ext -> { group_by: region; aggregate: n, total }",
         },
      ],
   },
   {
      name: "materialize: a refined source",
      about: "#(materialize) on a source declared by refinement with a where:, computed dimensions and an extension (v2 only)",
      variants: ["materialize"],
      files: {
         "m.malloy": `${HEADER}
import "raw.malloy"

@PERSIST(east_west)
source: east_west is orders_raw extend {
  where: region = 'east' or region = 'west'
  dimension:
    #(doc) "Month of the order"
    order_month is order_date.month
    big is amount > 100
  measure: total is amount.sum()
}

source: east_west_ext is east_west extend {
  measure: n is count()
}
`,
      },
      queries: [
         {
            name: "a computed timeframe dimension",
            malloy:
               "run: east_west -> { group_by: order_month; aggregate: total }",
         },
         {
            name: "a computed boolean dimension",
            malloy: "run: east_west -> { group_by: big; aggregate: total }",
         },
         {
            name: "an extension",
            malloy:
               "run: east_west_ext -> { group_by: region; aggregate: n, total }",
         },
      ],
   },
   {
      name: "v1 rollup on annotated measures",
      about: "A #@ preaggregate rollup over measures carrying # currency and #(doc) (v2: #(preaggregate)); the v1 cutover's G4",
      files: {
         "m.malloy": `${HEADER}
import "raw.malloy"

source: orders is orders_raw extend {
  dimension: zone is upper(region)
  # currency
  #(doc) "Revenue"
  @PREAGG(region)
  measure: total is amount.sum()
  #(doc) "Orders"
  @PREAGG(region)
  measure: n is count()
}
`,
      },
      queries: [
         {
            name: "covered by the rollup",
            malloy: "run: orders -> { group_by: region; aggregate: total, n }",
            expect: "rollup",
            gaps: {
               "storage/off": V1_STORAGE_ROLLUP_ANNOTATIONS,
               "storage/on": V1_STORAGE_ROLLUP_ANNOTATIONS,
            },
         },
         {
            name: "a measure expression over the rollup",
            malloy:
               "run: orders -> { group_by: region; aggregate: avg_ is total / n }",
            expect: "rollup",
         },
         {
            name: "not covered (status)",
            malloy: "run: orders -> { group_by: status; aggregate: total }",
            expect: "live",
         },
      ],
   },
   {
      name: "persisted view",
      about: "#@ persist on a view, served only to a query that calls the view exactly (phase 4's view leg, behind PUBLISHER_MATERIALIZE)",
      variants: ["storage", "colocated"],
      files: {
         "m.malloy": `${HEADER}
import "raw.malloy"

source: ov is orders_raw extend {
  # currency
  #(doc) "Revenue"
  measure: total is amount.sum()
  @PVIEW(ov_by_region)
  view: by_region is { group_by: region; aggregate: total }
}
`,
      },
      gaps: {
         // The view leg exists only with the materialize switch on.
         "storage/off": PERSISTED_VIEW_OFF,
         "colocated/off": PERSISTED_VIEW_OFF,
      },
      queries: [
         {
            name: "the view called",
            malloy: "run: ov -> by_region",
            expect: "view",
         },
         {
            name: "a later stage after the view",
            malloy: "run: ov -> by_region -> { select: region, total }",
            expect: "view",
         },
         {
            name: "a refinement (live)",
            malloy: "run: ov -> by_region + { limit: 2 }",
            expect: "live",
         },
      ],
   },
   {
      name: "materialize: a base with measures and views",
      about: "A #(materialize) source extending a base that declares measures and a view in the same file (G8: the re-root replaces the base, so it keeps the base's blocks)",
      variants: ["materialize"],
      files: {
         "m.malloy": `${HEADER}
import "raw.malloy"

source: orders_base is orders_raw extend {
  # currency
  measure: total is amount.sum()
  measure: n is count()
  view: by_region is { group_by: region; aggregate: total, n }
}

@PERSIST()
source: orders_m is orders_base extend {
  dimension: zone is upper(region)
}
`,
      },
      queries: [
         {
            name: "a base measure by a stored dimension",
            malloy: "run: orders_m -> { group_by: zone; aggregate: total }",
         },
         { name: "the base's view", malloy: "run: orders_m -> by_region" },
         {
            name: "a base measure in a nest",
            malloy:
               "run: orders_m -> { group_by: status; nest: by_zone is { group_by: zone; aggregate: n; order_by: zone } }",
         },
      ],
   },
   {
      name: "a file without the persistence flag",
      about: "A v1 #@ persist in a model file with no ##! experimental.persistence: Malloy plans it only with the flag, so before the cutover it is never planned (guard 1); the cutover plans it from a build copy that carries the flag (D3)",
      files: {
         "m.malloy": `import "raw.malloy"

@PERSIST(daily_nf)
source: daily_nf is orders_raw -> {
  group_by: region, order_date
  aggregate: total is amount.sum()
} extend {
  measure: grand is total.sum()
}
`,
      },
      gaps: {
         "storage/off": UNFLAGGED_BEFORE_CUTOVER,
         "storage/on": UNFLAGGED_BEFORE_CUTOVER,
         "colocated/off": UNFLAGGED_BEFORE_CUTOVER,
         "colocated/on": UNFLAGGED_BEFORE_CUTOVER,
      },
      queries: [
         {
            name: "a stored measure by region",
            malloy: "run: daily_nf -> { group_by: region; aggregate: grand }",
         },
      ],
   },
   {
      name: "an extend block that drops and re-hides stored columns",
      about: "A persisted query whose extend block drops a stored column with except:, and one that renames a stored column and re-hides it with include (review F3: the stored table holds both, and the author's own text removes and re-hides them again)",
      files: {
         "m.malloy": `##! experimental { persistence access_modifiers }
import "raw.malloy"

@PERSIST(dropped)
source: dropped is orders_raw -> {
  select: id, region, status, amount
} extend {
  except: status
  measure: total is amount.sum()
}

@PERSIST(rehidden)
source: rehidden is customers_raw -> {
  select: id, segment, home_region
} extend {
  rename: region_raw is home_region
} include { internal: region_raw; public: * } extend {
  measure: n is count()
  measure: west_n is count() { where: region_raw = 'west' }
}
`,
      },
      variants: ["storage", "colocated"],
      queries: [
         {
            name: "a measure beside the excepted column",
            malloy: "run: dropped -> { group_by: region; aggregate: total }",
            gaps: {
               "storage/off": SERVE_SHAPE_FAILS_BESIDE,
               "storage/on": SERVE_SHAPE_FAILS_BESIDE,
            },
         },
         {
            name: "a measure over the re-hidden column",
            malloy:
               "run: rehidden -> { group_by: segment; aggregate: n, west_n }",
            // A storage= table holds public columns only, so a measure over
            // the hidden column is answered live there.
            expect: { storage: "live" },
         },
      ],
   },
];
