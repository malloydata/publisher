<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Pre-aggregation

Pre-aggregation rolls a measure up to a coarse grain and stores the result, so a query that only needs that grain reads a small table instead of scanning the base. You annotate the measure; Publisher builds the rollup and routes to it.

Queries do not change. They name the source they always named, ask for the measure they always asked for, and get the same answer — from the rollup when it covers the query, from the base when it does not. Nothing in the API or the model surface exposes a rollup to query authors, which is the point: pre-aggregation is a cache, and a cache you have to ask for is a schema change. The same answer includes its metadata: each column a rollup returns carries the annotations live's does (`#(classification)`, `#(doc)`, render tags such as `# currency`), whether the rollup is the source's own or a joined source's, and the access record's field classes and the `#(carries)` ceiling are read from the query as live compiles it, so a rollup never answers a query the ceiling refuses.

This is the same machinery as [materialization](materialization.md) with a different trigger. `#@ persist` stores a source you wrote; `#@ preaggregate` stores a rollup Publisher derives from a measure you annotated. Both build through the same plan, manifest, and scheduler.

Since the v1 cutover, a `#@ preaggregate` line is read as a `#(preaggregate)` declaration: planned once in its base's model, built under the v2 rollup's table name (`<base>__rollup__<grain>__<hash>`; a rollup built before the cutover under `__preagg__` is built again once), and served by the same rollup leg, so the limits below are the rollup leg's for both spellings. Materialization v2 renames `#@ preaggregate` to `#(preaggregate)`; [materialization-migration.md](materialization-migration.md#preaggregate) maps one to the other. Which to write depends on the server: with `PUBLISHER_MATERIALIZE` on, write `#(preaggregate)` (below), and `#@ preaggregate` carries a deprecation warning; with it off (the default), `#(preaggregate)` is not read (an `annotation-near-miss` warning) and its measure serves live, so `#@ preaggregate` is the spelling that builds.

## `#(preaggregate)` (materialization v2, behind `PUBLISHER_MATERIALIZE`)

With `PUBLISHER_MATERIALIZE` on, write the rollup as `#(preaggregate)` on the measure, with its grain as a list:

```malloy
source: orders is duckdb.table('data/orders.parquet') extend {
  dimension: order_day is order_time.day
  #(preaggregate) grain=["category"]
  #(preaggregate) grain=["category", "order_day"] storage="lake"
  measure: revenue is amount.sum()
  #(preaggregate) grain=["category"]
  measure: aov is coalesce(amount.sum() / count(), 0)
}
```

The rest of this page describes `#@ preaggregate`, which keeps working (deprecated). Where `#(preaggregate)` differs:

- **Keys.** `grain=[…]` (required), and `storage=` or `namespace=`, never both. Any other key, a grain that is not a list of the source's own dimensions, or the annotation on anything but a measure is refused at publish.
- **Which measures.** Read from the measure's own text: `count()`, `x.sum()`, `x.min()`, `x.max()` (and `sum(x)` and so on), each optionally filtered, references to other such measures, and arithmetic or scalar functions over them. Each aggregate is stored as its own partial and merged at read, so `revenue / order_count` stays a ratio of totals. `avg`, a distinct count (`count(x)`), `stddev` and the like, `all()` and `exclude()` are refused at publish. Text outside what is read (`pick`, a cast, a date literal, `|` in a filter, `x.count()` on a field, a joined source's measure) is a warning, and the measure serves live. The reading is checked against the compiled measure: a measure whose aggregates the compiler records differently is a warning too, never stored.
- **Joins.** A source with a `join_many` or `join_cross` can be rolled up. An aggregate through a join (`carriers.fleet.sum()`) is stored only when the join's key (`carrier` in `join_one: carriers with carrier`) is in the grain; otherwise it is a warning and serves live. Its stored total is the joined rows' within each group, and groups that share the key hold the same joined rows, so a query reads a join-level sum or count from the rollup only when it groups by, or fixes, every other grain dimension at each level that aggregates it (a grain of the key alone has none). A join-level `min` or `max` (`carriers.fleet.max()`, key in the grain) is stored too, and merged with `min` or `max`: a repeated joined row cannot change either, so it is read from the rollup however the query groups, and a group with no joined row is `NULL`, as live. An aggregate through more than one join (`carriers.hubs.gates.sum()`) is a warning and serves live. A grain dimension read through a `join_many` is a fan-out dimension: a query reads the rollup only when it groups by it, or fixes it to one value with `where:`, at every level that aggregates.
- **Where it is declared.** A declaration belongs to the source that writes it. An extension inherits its base's rollups' meaning but does not get its own; an include-list item's annotation is the including source's. Each rollup is planned once, under the model that declares its base, and its name carries that model, so two same-named sources in two models never share a table.
- **Extensions.** A query on an extension or rename of a rolled-up source (`source: orders_plus is orders extend { … }`) is answered from that source's rollups when the extension adds no `where:` and no `timezone:` (and, past a gate, only gates the rollup can prove; see below), and the query reads only grain dimensions, stored measures, and dimensions the extension defines over grain dimensions alone (`dimension: cat_lower is lower(category)`). A dimension over anything else or written in raw SQL (`sql_string(…)`), a measure the extension adds, an extension that filters its rows, and an extension that binds a base field's name to another field (`rename: region_was is region; dimension: region is 'east'`, which changes what the base's stored measures read) are answered live (`GRAIN_TOO_COARSE`, `MISSING_FIELD`, `TIME_ZONE`).
- **Access filters.** A query whose entry point carries a row-level gate (`#(access_filter)`) or a lock (`#(authorize)`) is answered from a rollup only with `PUBLISHER_MATERIALIZE_GATED_SOURCES` on as well, and only from a colocated one. The queried source is then redeclared over the rollup with its gates, and the gate is grafted and proven on that compile as on live (route, then govern), so each caller gets live's answer over the rows its gate admits. A gate is exact on a rollup only when each field it reads is a grain dimension: a column (`#(access_filter) region = $REGION` with `region` in the grain), a dimension the source computes from its own columns, or a truncation (`order_day`). A lock that reads no field, a lock beside a filter, an inherited gate, and `#(authorize) true` re-opening a locked base beside a filter are proven as their parts. The gate's field is often `internal`; an `internal` grain dimension a gate reads is the one hidden field a rollup stores, and it stays `internal` on the rollup, so no query selects it (a caller's own `extend` reads it within the rows the gate admits, as live). A rollup whose gate reads a field the grain drops, reads through a join (`account.org_id`, or a dimension defined through one), or whose source joins a gated source, is not built (`preaggregate-access-filter`, a warning), and its queries are answered live. With the switch off, no rollup over a gated source is built at all (the same warning), since no query would read it; a rollup in a `storage=` destination over a gated source is not built with the switch on either, until the managed tier's security review. A query skipped for its gate reports `CONTRACT`, whose detail never names the gate. A caller's own `except:` or `rename:` of the gate's field is denied, as live is, and a caller's join into a gated source keeps the query off every rollup. Publisher does not authenticate callers: the gate holds only behind the gateway that sets the givens from a verified identity ([operating model](security-posture.md#operating-model-a-microservice-behind-your-gateway)).
- **Through a join (join-to-rollup).** A query on another source that reads this source's measures through a join (`run: flights -> { group_by: carrier; aggregate: segments.seg_miles }`, with `join_many: segments on segments.flight_id = id`) is answered from this source's rollup when the rollup's grain keeps the join's key (`grain=["flight_id"]`, or `["flight_id", "kind"]` to also group or filter by `segments.kind`). The join is re-pointed at the rollup for that query; everything else about the querying source is as written, so a parent with no rows answers as live does. It reads this source's stored measures only: an aggregate the query writes over the join (`segments.count()`, `count(segments.flight_id)`) is answered live. The rules for this source's own queries hold through the join, with each field named `segments.<field>`: a fan-out grain dimension (`line_kind is lines.kind`, read through `segments`' own `join_many`) must be grouped (`group_by: segments.line_kind`) or fixed to one value (`where: segments.line_kind = 'a'`) at every level that aggregates through the join, and a join-level sum or count needs every other grain dimension grouped or fixed there too. Grouping or fixing the querying source's key (`group_by: id`, with `on segments.flight_id = id`) fixes the join's key, since each rollup row a parent meets carries it. A level that aggregates only the querying source's own rows (`flight_count`) asks nothing; a level that reads this source's measures through an expression, a `having:`, or a measure of the querying source is held to every grain dimension a measure fans out across, and a dimension of the querying source over a fan-out dimension (`dimension: k is segments.line_kind`) does not count as grouping it. The rollup was built in this source's time zone, so a querying source that declares a different `timezone:` (or a joined source that declares its own) is answered live (`TIME_ZONE`). Through a `join_one`, a query that also aggregates the querying source's own rows (`flight_count` beside `crew.crew_size`, a `having:` on one, another join's measure) is served only when the rollup's build checked the join's key unique: when a `join_one` in the package is keyed on the rollup's one grain dimension (`join_one: crew with id` and `grain=["flight_id"]`), each build of the rollup also groups the rows it read on that key and records whether any non-NULL value repeats. Unique, the joined rows live and the rollup's are the same rows, and the query is served. A repeated key (the data breaks the declared `join_one`, which is logged at build and reported as a package warning, `preaggregate-join-one-key-repeated`, on the rollup's source while that build's table is bound) or no check on record is answered live (`COUNT_SEMANTICS`): live counts each parent once per joined row, which a rollup with one row per key cannot reproduce. No check is on record for a table a control plane's manifest binds, a build whose check could not run, or a table a run is rewriting; a check describes the rows the table was built from, and a rebuild checks again. A query that reads another `join_one` whose key a rollup's build found repeated is answered live too (`COUNT_SEMANTICS`), since re-pointing one join changes how Malloy aggregates over the rows the other repeats; one never checked is served as before. A join read as a `join_many` (a declared one, or a `join_one` read at a grain finer than its key) is used only when it is keyed on the querying source's primary key (`on segments.flight_id = id` with `primary_key: id`): keyed on another field (`join_one: crew with flight_id` from a source of segments, or a `join_many` from a source with no primary key), one rollup row meets several of the querying source's rows, which live counts differently, so it is answered live (`COUNT_SEMANTICS`). The data has to keep that primary key too: each build of a colocated rollup also checks every source the package declares that reads it through such a join, grouping that source's rows on its primary key, and records whether a value repeats or is NULL. Unique and never NULL, the query is served. A repeated key (two rows with `id = 1`, where the variant reads flight 1's rollup row twice and live sums each segment once) or a NULL key (where the variant's `count()` counts a row live's skips), logged at build, or no check on record, is answered live (`COUNT_SEMANTICS`). The check describes the querying source's rows as the rollup's build found them, is carried and forgotten with the rollup's table exactly as the `join_one` key check is, and is never on record for a table a control plane's manifest binds; a querying source the build could not check (one whose `where:` reads a given, with or without a default, since one check cannot stand for every caller's rows; on another connection; or defined in the query, `run: flights extend { … } -> …`) is answered live. A rollup in a `storage=` destination is read beside the querying source's stored table there, so that table's build checks the key instead, over every row it holds (a `where:` term the served copy applies at read only narrows them), with the same rules: unique and never NULL serves, anything else, or no check on record, is answered live. A rollup in a `storage=` destination is read through the join when the querying source is stored in the same destination (a `#(materialize) storage=` table, or a `#@ persist storage=` source, read through the served copy, with `PUBLISHER_MATERIALIZE` and the storage tier on): the querying source is read from its stored table and the join from the rollup, so the whole query runs in the destination, with the querying source's given-reading `where:` terms and rolling windows applied at read as the served copy applies them. The path report names the rollup as the table that answered (`servedBy`) and lists the querying source's table as `used` beside it. Both tables must be fresh. A querying source not stored there, one whose table is not built or not fresh, or a reader that opts out with `#@ -persist`, is answered without the rollup (`CROSS_CONNECTION`), since Malloy joins within one connection; so is a query that reads a definition of the querying source the served copy leaves out because it reads the join to a source not stored in the destination (a measure or view over `segments`, when `segments` itself is not stored there). A querying source stored as a pair table (`inline=` on a `join_many`) is not read this way (`COUNT_SEMANTICS`), and the served copy's rules for a join it stores inline hold (`COUNT_SEMANTICS`, `MISSING_FIELD`). A parent with no rows counts as no rows and sums nothing, live and from the rollup, so a `count()` through a `join_many` and a sum of any value (`sum(miles + 1)`, a constant) are served, as is a `count()`, `sum` or `avg` the query takes through another `join_many` while this join is re-pointed. Malloy does not skip that parent's row for a `min` or `max`, so one of a value that is not NULL there (`max(coalesce(miles, 0))`, a constant) is answered live (`COUNT_SEMANTICS`); a `min` or `max` of a column is served. A join-level measure's column (`kinds.weight` in `kinds.weight.sum()`) is NULL there when its join is keyed on a column of this source, so it is served; an aggregate at a join keyed on a computed dimension (`join_one: k is kinds with coalesce_kind`) can match a row for a parent with no rows and is answered live (`COUNT_SEMANTICS`). Since Malloy 0.0.438, live also takes a `count()`, `sum` or `avg` of this source's own rows only over the rows whose primary key is not NULL, and once per distinct key where the rows fan out, while the rollup reads every row. So when a join reaches this source on a column other than its primary key (`on segments.flight_id = id`, with `primary_key: seg_id`), each build of the rollup also checks this source's primary key, over its rows on its own connection, colocated or in a `storage=` destination, and records whether a value repeats or is NULL. Unique and never NULL, a count or sum through the join is served. A repeated or NULL `seg_id`, or no check on record (the same cases as the querying source's check above), is answered live (`COUNT_SEMANTICS`). A `min` or `max` is served either way, and a join keyed on this source's primary key (`join_one: crew with id`, with `primary_key: flight_id`) needs no check.
- **Its own time zone.** A rollup over a source that sets its own `timezone:`, or inherits one, is not built (`preaggregate-time-zone`, a warning): a query on that source is never answered from a rollup (`TIME_ZONE`, above), so the table would cost a build no query reads. The exception is a rollup that another source in the same `timezone:` joins, which a query on that source reads through the join: colocated, or in a `storage=` destination that source is stored in too (a rollup there is built, and a query from a source not stored there is answered live, `CROSS_CONNECTION`).
- **The current time.** A rollup over a source whose rows read `now` (a rolling window, `where: t > now - 7 days`), or whose grain dimension or measure reads it, is not built (`preaggregate-current-time`, a warning): the rollup would keep the window its build saw, while a live query moves it at every read.
- **A stored base.** A rollup builds from the author's rows, never from its base's stored table, so a rollup over a `#@ persist storage=` base builds and serves (#6293).
- **What routes.** The query's text is compiled where its source's name means the rollup. It routes when every field it groups or filters by is in the grain, every aggregate is a stored measure's merge (an aggregate written in the query or in a `having:`, or a `select:` stage, is not; this is read from the compiled query, not its text), and it sets no `timezone:`, in any stage or nest. A query on a source that sets its own `timezone:` is answered live (`TIME_ZONE`): the rollup's stored times are not read in that zone, so a truncation, a filter on one, or a gate on one would not match live. Since no query on it reads such a rollup, a `#(preaggregate)` on a source that sets (or inherits) its own `timezone:` is not built (`preaggregate-time-zone`, a warning), unless a source in the same zone joins it, the one read left (through a join, above). A `calculate:` over a measure (`lag(total)`) reads that measure at its level, so it is held to the same rules as an aggregate there. A fan-out dimension counts as fixed by `where: d = <one value>`, alone or as one conjunct of an `and`. A query that supplies a given routes. A named `view:` routes as its own query does: `run: orders -> by_category`, a refinement of it, a query starting from it, and a `nest:` of one are compiled with the view as you wrote it, and answered from the rollup when the view's query passes the same checks, its text, and the text of every view it builds on, still compiles to the view the served model holds (an edit not yet reloaded is answered live), and it reads no raw SQL (`sql_boolean(…)`, whose columns the rollup may not keep). A persisted view of that name (`#@ persist` on the view) answers its own call first. The path report names the reason for each skip, and an answer from a `storage=` rollup reports `servedFrom: storage`.
- **Givens.** A rollup whose build reads a given is not built (a warning): its table would hold one value's rows.

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

Where more than one rollup covers a query, the **coarsest** one is used: rollups are offered fewest-grain-dimensions first, and the first that covers answers. With `grain="b"` and `grain="a, b"`, a query grouping by `b` alone reads the `b` table.

Grain dimensions are counted, not measured, so this is a proxy for table size rather than a reading of it: three small dimensions can product out smaller than one large one, and where two grains have the same number of dimensions the order between them is arbitrary. Declare grains that cover **different** queries, which is what they are for, rather than several overlapping grains in the expectation that Publisher will pick the cheapest.

A rollup is offered whether or not its table has been built yet, so adding a coarse grain to a package that already has a built finer rollup costs acceleration until the new rollup builds: queries the coarse grain covers are answered from the base rather than from the finer table. Answers are unaffected — an unbuilt rollup recomputes from the base, which is what the query would have done anyway — and it lasts one build.

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

A filter written directly on the aggregate is fine — `paid is amount.sum() { where: is_paying }` pre-aggregates, because the measure *means* the filtered value and filtering commutes with merging per-group partials. Several conditions belong in that one `where:`, comma-separated. The filter has to sit on the aggregate itself, applied once: a chained refinement (`amount.sum() { where: a } { where: b }` — the spelling most likely to happen by accident; write `{ where: a, b }` instead), a filter refining a derived measure, or an aggregate wrapped in a further expression (`coalesce(amount.sum() { where: … }, 0)`) is refused at publish like any other shape whose merge cannot be proven.

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

**A query with an aggregate the rollup does not store does not route.** A rollup holds one row per grain group and a stored partial for each annotated measure, so it answers a query only when every aggregate in it is one of those measures, or an expression over them (`revenue / order_count`, `revenue { where: category = 'books' }`, `all(revenue)`). An aggregate written in the query itself — `count()`, `qty.sum()`, `count(category)`, `stddev(amount)`, including one in a `nest:`, a `having:`, or a measure an inline `extend` defines — would be computed over the rollup's rows rather than the base's, so the query is answered from the base. To accelerate a count, declare it as a measure and annotate it (`#@ preaggregate grain="category"` on `order_count is count()`), and ask for `order_count`. For the same reason a `select:` or `index:` stage over the source, including one in a `nest:`, is answered from the base: a rollup has one row per grain group, not one per source row.

**A named view routes when its own query does.** The rollup is read in a copy of your source's own file, so a view it declares is carried onto it and checked for coverage like a written-out query: `run: orders -> by_category` reads the rollup when `by_category`'s query is one the rollup covers.

**A rollup over rows that read a [given](givens.md) is not built.** A rollup holds the rows its build saw, one given value's, so a source whose rows depend on a given gets no rollup, with a `preaggregate-reads-given` warning, and its queries are answered from the base. A given the rows do not read changes nothing.

**A query grouping by an expression rather than a dimension does not route.** `group_by: order_time.month` is an expression, and the rollup has no such field. Group by the declared dimension (`order_day`) or a coarser truncation of it (`order_day.month`) instead.

## See what a package will build

Rollups appear in the package's build plan before anything is built. Each has `origin: "preaggregate"` and a `preaggregate` object naming the base source, the grain's dimensions, and the measures served at it:

```bash
curl -s localhost:4000/api/v0/environments/examples/packages/storefront/build-plan \
  | jq '.sources[] | select(.origin == "preaggregate") | {name, preaggregate}'
```

An older server has no `build-plan` route; read `.buildPlan` off `GET …/packages/storefront` there.

A rollup is declared by no file, so it reports the `modelPath` of the model whose annotations produced it — where you would go to change it.

Nothing about a rollup appears in model discovery. Your model is never edited, so it exports the sources it always did.

## Where the rollup's table is created

You do not name a rollup's table — Publisher derives it from the base, the grain and a
digest, so the resolver and the content address can rely on it. You *do* choose the
namespace it is created in, and on some warehouses you must.

```malloy
source: orders is bigquery.table('project.raw.orders') extend {
  measure:
    #@ preaggregate grain="category" namespace="analytics"
    total is amount.sum()
}
```

That builds `analytics.orders__preagg__category__<digest>`.

**BigQuery requires it.** An unqualified `CREATE` is rejected outright there
(`must be qualified with a dataset`), so a rollup without a namespace cannot be built
on BigQuery at all. Name a dataset the connection can write — typically a scratch
dataset, the same one you would give an authored `#@ persist name=`. Other engines
accept an unqualified name and create the table in the connection's default schema, so
`namespace` is optional there.

**The namespace has to exist already.** Nothing in the tier creates a dataset or a
schema, so a name that is merely plausible fails the build. Create it once, and let the
connection's credential create tables in it.

**It belongs to the grain, not to the measure.** A namespace is written on the same
line as the `grain=` it applies to, and applies to nothing else — two grains are two
tables and can be created in two places:

```malloy
measure:
  #@ preaggregate grain="category" namespace="analytics"
  #@ preaggregate grain="order_date" namespace="archive"
  total is amount.sum()
```

Measures that share a grain share its one table, so they must agree on where it goes.
Two naming different namespaces is refused at publish rather than resolved silently.

**A persisted base lends its own.** If the base carries `#@ persist
name="analytics.orders_tbl"`, its rollups are created in `analytics` without being told
— a rollup of a table belongs beside it. An explicit `namespace` overrides that, and is
the only option when the base is not persisted at all, which is common: the annotation
goes on a measure, and the source it belongs to need not be materialized.

A base that also carries `storage=` lends nothing — not its namespace, and not its
destination. See [Rollups in the managed store](#rollups-in-the-managed-store) below.

**Changing it does not move a table that already exists.** A namespace is not part of
what identifies a rollup's contents, so a package whose rollups have already built keeps
them where they are: the annotation changes and the table does not. Ask for the rebuild
explicitly — `malloy-pub materialize --force-refresh`, or `forceRefresh` on the build
endpoint — which is what defeats the unchanged-content skip. Dropping the table by hand
does not, and leaves the manifest pointing at something that is no longer there.

**A re-stated grain takes the namespace on its own line, including none.** Writing
`grain="category"` again without `namespace=` clears the one an earlier line gave that
grain, which matters most across an extend chain: an extending source that re-states its
base's grain must restate the namespace too, or the rollup moves. To turn a declaration
off rather than replace it, negate with `#@ -preaggregate`.

**It is a namespace, not a name.** Write it as plain identifiers — letters, digits,
underscore, dollar or hyphen — dot-separated where the warehouse needs more than one
part (`analytics`, `my-project.analytics` on BigQuery). A value needing quotes is
refused rather than mangled, because the generated table name is appended to it and the
two halves would then be quoted inconsistently.

## Rollups in the managed store

By default a rollup is built into the same warehouse its base reads from. `storage=` puts
it in a [storage destination](connections.md#storage-destinations) instead — built there,
served from there — the same placement choice `#@ persist storage=` makes for an authored
source.

```malloy
source: orders is orders_pg.table('public.orders') extend {
  measure:
    #@ preaggregate grain="category" storage=lake
    total is amount.sum()
}
```

Nothing about the query changes. It still names `orders` and still knows no rollup
exists; only where the answer is read from moves.

**A rollup may not store what the source hides.** A rollup stores its grain and each
measure's partial, and the stored table is served under the base's name without the
source's field visibility applying to it — so pre-aggregating a hidden field would publish
it. The planner therefore refuses to plan one: `#@ preaggregate` on a measure the source
does not publicly expose builds nothing, and neither does a grain naming a hidden
dimension.

That is reported as a **warning** rather than refusing your publish, unlike everything
else in this section. Partly because such a package published before the rule existed and
an upgrade must not break it, and partly because the refusal was unfollowable: an
annotation inherited onto a source that then hides the measure would raise it, while that
source produces no rollup at all and exposes nothing.

**Write it on the `#@ preaggregate` line.** A destination is not inherited from the
base's `#@ persist storage=`, and the reason is worth knowing because the opposite reads
as obviously right — a rollup of X belongs where X's rows live.

It does not work, in either of the two cases — and they are exhaustive, because inheriting
would require `#@ persist` on the base. If the base is **query-shaped** it builds a stored
table of its own, and a rollup over it is then built by reading that table rather than the
raw source — a path that recovers the rollup's definition from the model file, which a
rollup does not have, so the build fails. If the base is **not query-shaped** — a table
extended with measures — the annotation parses, and the build refuses the whole run for a
`#@ persist` source that was dropped from the plan.

That first case is reachable deliberately, not only by inheritance: writing `storage=` on
both the base's `#@ persist` and the `#@ preaggregate` line hits the same wall. Even had it
built, the base's own stored table already claims the name its rollups would be served
under, so nothing would have read it. So Publisher leaves such rollups out of the plan and
every run, rather than attempting a build that cannot succeed: the base still builds and
serves from its stored table, and the package carries a `preaggregate-over-stored-base`
warning (`warn`, one per base). Rollups over a stored base are planned for a later release.

So a rollup goes to a destination because its own line says so, which is also the
common case: a rollup's base is usually a table extended with measures, and Malloy admits
only query-shaped sources as build roots, so most bases cannot carry `#@ persist` at all.

A `name=` namespace *is* inherited, and the asymmetry is not about inheritance. Colocated
rollups are offered through a companion model whose members Publisher names itself, with
the base under an import alias, so nothing there is keyed on your source's name and there
is no binding to collide with.

**It belongs to the grain, like `namespace=`.** Two grains are two tables and can be
placed differently — one in the store, one alongside the base. Measures sharing a grain
share its one table, so two of them naming different destinations is refused at publish.

**`namespace=` and `storage=` cannot be combined.** Placement inside a destination is
derived, not authored: a freshly provisioned catalog has no schema to create the table
in. Writing both on one `#@ preaggregate` line is refused.

**The tier's switch applies.** With `PERSIST_STORAGE_MODE` off, a `storage=` rollup is
not built — and not built anywhere else either. Falling back to a build alongside the
base would put a table in your warehouse under a generated name you never wrote, so
nothing is built, queries are answered from the base, and the package reports the
degraded state as a warning. Under `write-only` the rollup builds but does not serve.

**An uncovered query is answered from the base.** A query no rollup covers, in the store
or alongside the base, is answered live, with the answer it always had; the path report
names each rollup it considered and why it was skipped.

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

The [materialization tuning guide](../skills/malloy-materialization/reference/tuning.md) helps decide what is worth storing.
