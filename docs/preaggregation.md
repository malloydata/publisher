# Pre-aggregation

Pre-aggregation rolls a measure up to a coarse grain and stores the result, so a query that only needs that grain reads a small table instead of scanning the base. You annotate the measure; Publisher builds the rollup and routes to it.

Queries do not change. They name the source they always named, ask for the measure they always asked for, and get the same answer — from the rollup when it covers the query, from the base when it does not. Nothing in the API or the model surface exposes a rollup to query authors, which is the point: pre-aggregation is a cache, and a cache you have to ask for is a schema change.

This is the same machinery as [materialization](materialization.md) with a different trigger. `#@ persist` stores a source you wrote; `#@ preaggregate` stores a rollup Publisher derives from a measure you annotated. Both build through the same plan, manifest, and scheduler.

## Declare a rollup

Annotate a **measure** with the grain to store it at:

```malloy
source: orders is duckdb.table('data/orders.parquet') extend {
  dimension: order_day is order_time.day

  #@ preaggregate grain="category"
  measure: revenue is amount.sum()
}
```

That builds one table grouped by `category`, holding `revenue`'s partial sums. A query grouping by `category` reads it; a query grouping by `order_time` does not and serves live.

No `##!` experimental flag is needed. Rollups use `compose()` and `#@ persist` internally, but they live in a model Publisher synthesizes alongside yours, and that model declares its own flags.

Name several dimensions with commas — order does not matter, since the grain is canonicalized before it names a table:

```malloy
  #@ preaggregate grain="category, order_day"
  measure: revenue is amount.sum()
```

A rollup also serves queries grouped by any **subset** of its grain, so this one answers by-category, by-day, and grand-total queries too.

### Several grains on one measure, and why you would

Each `#@ preaggregate` line is one grain, and each grain is its own table:

```malloy
  #@ preaggregate grain="category"
  #@ preaggregate grain="customer_id"
  measure: revenue is amount.sum()
```

Since one rollup covers every subset of its grain, `grain="category, customer_id"` would cover both queries with a single table. The reason to declare them separately is **cost, not coverage**. A combined grain has roughly the product of its dimensions' cardinalities, so `category, customer_id` can approach the base table's row count and save almost nothing, while either grain alone is tiny. Declaring both gives each query a small table to read, and costs two tables to build and refresh.

### Which rollup answers a query

Where more than one rollup covers a query, the one used is the first that covers it in the composite's member order — and that order is by generated name, which has nothing to do with size. So do not declare a grain expecting it to win a query another declared grain also covers: with `grain="b"` and `grain="a, b"`, a query grouping by `b` alone reads the `a, b` table, because `a_b` sorts first.

Declare grains that cover **different** queries, which is what they are for, and treat overlapping grains as one grain's worth of acceleration rather than a choice Publisher will make well.

Rollups are grouped by grain rather than by measure, so ten measures sharing a grain are one table and one `GROUP BY`:

```malloy
  #@ preaggregate grain="category"
  measure: revenue is amount.sum()
  #@ preaggregate grain="category"
  measure: order_count is count()
```

`#@ -preaggregate` clears every grain declared above it, which is how an `extend`ing source opts out of a base's rollups. A `#@ preaggregate` line after it declares again.

## What can be pre-aggregated

Publisher refuses a declaration it could not build, as a `400` from publish and `PATCH` and as a load failure on an already-published package (reported in `ServerStatus.loadErrors`). The rules are worth knowing before you write the annotation, because the alternative to refusing is worse: a rollup that silently does nothing returns correct numbers, so the only signal is the bill.

### Only measures

`#@ preaggregate` on a source, dimension, join, or view is refused. Dimensions appear in a measure's `grain=`; they are what the rollup groups by, not what it stores.

### Only measures whose partials can be merged

A rollup stores a partial aggregate per group, and answering a coarser query merges those partials. That works for `sum`, `count`, `min` and `max`, and not for anything else: an average of averages is not an average.

For `avg`, pre-aggregate the parts and divide where you use them:

```malloy
  #@ preaggregate grain="category"
  measure: revenue is amount.sum()
  #@ preaggregate grain="category"
  measure: order_count is count()
```

```malloy
run: orders -> {
  group_by: category
  aggregate: aov is revenue / order_count
}
```

`aov` is not annotated and does not need to be: it is derived from two measures that are, so the query reads the rollup. Note that putting it in a named `view:` would stop it routing — see below.

### Grains name dimensions

A grain may name only dimensions the source itself declares. An inline expression is refused, including a time truncation — this is rejected at publish:

```malloy
  #@ preaggregate grain="order_time.day"
  measure: revenue is amount.sum()
```

Declare the dimension and name it:

```malloy
  dimension: order_day is order_time.day

  #@ preaggregate grain="order_day"
  measure: revenue is amount.sum()
```

This one looks like a formality and is not. A rollup built from an inline truncation either answers untruncated queries from truncated rows — wrong numbers, quietly — or is never used at all, depending on how the stored column is named. Requiring the dimension makes the rollup addressable by the same name the query uses.

It also pays off: a **coarser** truncation of a stored time dimension routes to the same rollup, so one `order_day` rollup serves day, month, quarter and year queries.

For the same reason a grain cannot reach through a join (`grain="customer.region"`). Declare a dimension on the source that exposes the joined field, and name that.

### No fan-out joins on the base

A source whose joins can multiply rows — `join_many`, `join_cross` — cannot be rolled up, because the stored partials would double-count. `join_one` is fine, and a measure that aggregates through one is pre-aggregated normally.

This rule is wider than it sounds: it disqualifies the whole source, not just the measures that use the fan-out join. A source with one `join_many` cannot pre-aggregate any of its measures.

## What does not route

Three limits are worth knowing before you decide pre-aggregation will help your workload. None affects correctness — an uncovered query is answered from the base, with the answer it always had — but each means no acceleration.

**A query that names a `view:` does not route.** Rollups are offered to a query through a composite source, and a composite carries its members' fields but not their views, so a view defined on your source is not visible on it. Given `view: by_category is { group_by: category; aggregate: revenue }`, `run: orders -> by_category` serves live, while the same query written out — `run: orders -> { group_by: category; aggregate: revenue }` — reads the rollup. This affects the REST API's `{"queryName": …, "sourceName": …}` form and Console dashboards, since both run named views. If your traffic is mostly named views, expect little benefit today.

**A query that supplies a [given](givens.md) does not route.** A model-level `given:` does not cross into the model Publisher synthesizes, so a query passing one cannot be compiled against the rollup and is served live. This is not merely a technicality to be lifted: a rollup is built with the givens in force at build time, so serving a query at a different given value from the stored rows would be wrong. A filter-driven data app, whose every query supplies a given, gets no benefit today.

**A query grouping by an expression rather than a dimension does not route.** `group_by: order_time.month` is an expression, and the rollup has no such field. Group by the declared dimension (`order_day`) or a coarser truncation of it (`order_day.month`) instead.

## See what a package will build

Rollups appear in the package's build plan before anything is built. Each has `origin: "preaggregate"` and a `preaggregate` object naming the base source, the grain's dimensions, and the measures served at it:

```bash
curl -s localhost:4000/api/v0/environments/examples/packages/storefront \
  | jq '.buildPlan.sources[] | select(.origin == "preaggregate") | {name, preaggregate}'
```

A rollup is declared by no file, so it reports the `modelPath` of the model whose annotations produced it — where you would go to change it.

Nothing about a rollup appears in model discovery. Your model is never edited, so it exports the sources it always did.

## Build and refresh

Rollups build with the package's other materializations — on demand, or on the package's schedule. See [materialization.md](materialization.md) for triggering builds, the `malloy-pub` CLI, and scheduling.

Two things are specific to rollups:

**A rollup that has never been built costs nothing but latency.** Queries compile against the rollup and the base together, and until a table exists the base answers everything. You will not see an error, only the performance you had before.

**A stale rollup is skipped, not served.** When a rollup's table falls outside its freshness window it drops out of the serving set and queries recompute from the base. The answer is the same either way, which is what makes a refresh schedule a cost decision rather than a correctness one.

## What it costs

A rollup is a table Publisher builds and refreshes, so it is worth being deliberate:

- **A grain near the base's cardinality saves nothing and costs a build.** `customer_id, order_day` on a table with one row per customer per day is a copy of the base. Check the grain's cardinality before annotating.
- **Prefer several small grains to one combined grain** when the combination is large but each dimension is small, as above.
- **A package with no `#@ preaggregate` pays nothing.** Publisher derives rollups from the compiled model in memory and stops as soon as it finds no annotations, so an unannotated package plans, builds, and serves exactly as it did before.

The [`malloy-materialization-tuning`](../skills/malloy-materialization-tuning/SKILL.md) skill helps decide what is worth storing.
