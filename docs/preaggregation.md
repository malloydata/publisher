<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Pre-aggregation

Pre-aggregation rolls a measure up to a coarse grain and stores the result, so a query that only needs that grain reads a small table instead of scanning the base. You annotate the measure; Publisher builds the rollup and routes to it.

Queries do not change. They name the source they always named, ask for the measure they always asked for, and get the same answer — from the rollup when it covers the query, from the base when it does not. Nothing in the API or the model surface exposes a rollup to query authors, which is the point: pre-aggregation is a cache, and a cache you have to ask for is a schema change. The same answer includes its metadata: each column a rollup returns carries the annotations live's does (`#(classification)`, `#(doc)`, render tags such as `# currency`), whether the rollup is the source's own or a joined source's, and the access record's field classes and the `#(carries)` ceiling are read from the query as live compiles it, so a rollup never answers a query the ceiling refuses.

This is the same machinery as [materialization](materialization.md) with a different trigger. `#@ persist` stores a source you wrote; `#@ preaggregate` stores a rollup Publisher derives from a measure you annotated. Both build through the same plan, manifest, and scheduler.

Since the v1 cutover, a `#@ preaggregate` line is read as a `#(preaggregate)` declaration: planned once in its base's model, built under the v2 rollup's table name (its [readable name](materialization.md#readable-table-names), `<package>__<file>__<base>_by_<grain>__p<hash>`; a rollup built before the cutover under `__preagg__` is built again once), and served by the same rollup leg, so the limits below are the rollup leg's for both spellings. Materialization v2 renames `#@ preaggregate` to `#(preaggregate)`; [materialization-migration.md](materialization-migration.md#preaggregate) maps one to the other. Which to write depends on the server: with `PUBLISHER_MATERIALIZE` on (the default since #1754), write `#(preaggregate)` (below), and `#@ preaggregate` carries a deprecation warning; with it off (`PUBLISHER_MATERIALIZE=false`), `#(preaggregate)` is not read (an `annotation-near-miss` warning) and its measure serves live, so `#@ preaggregate` is the spelling that builds.

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

- **Keys.** `grain=[…]` (required), and `storage=` or `name=`, never both (`name=` places the table; see [below](#where-the-rollups-table-is-created)). `namespace=` is `name=`'s deprecated spelling. Any other key, a grain that is not a list of the source's own dimensions, or the annotation on anything but a measure fails the publish and the load (`preaggregate-unknown-key`, `preaggregate-unknown-grain`, `preaggregate-not-a-measure`).
- **Which measures.** Read from the measure's own text: `count()`, `x.sum()`, `x.min()`, `x.max()` (and `sum(x)` and so on), each optionally filtered, references to other such measures, and arithmetic or scalar functions over them. Each aggregate is stored as its own partial and merged at read, so `revenue / order_count` stays a ratio of totals. `avg`, a distinct count (`count(x)`), `stddev` and the like, `all()` and `exclude()` fail the publish and the load (`preaggregate-not-mergeable`). Text outside what is read (`pick`, a cast, a date literal, `|` in a filter, `x.count()` on a field, a joined source's measure) is a warning, and the measure serves live. The reading is checked against the compiled measure: a measure whose aggregates the compiler records differently is a warning too, never stored.
- **Joins.** A source with a `join_many` or `join_cross` can be rolled up. An aggregate through a join (`carriers.fleet.sum()`) is stored only when the join's key (`carrier` in `join_one: carriers with carrier`) is in the grain; otherwise it is a warning and serves live. Its stored total is the joined rows' within each group, and groups that share the key hold the same joined rows, so a query reads a join-level sum or count from the rollup only when it groups by, or fixes, every other grain dimension at each level that aggregates it (a grain of the key alone has none). A join-level `min` or `max` (`carriers.fleet.max()`, key in the grain) is stored too, and merged with `min` or `max`: a repeated joined row cannot change either, so it is read from the rollup however the query groups, and a group with no joined row is `NULL`, as live. An aggregate through more than one join (`carriers.hubs.gates.sum()`) is a warning and serves live. A grain dimension read through a `join_many` is a fan-out dimension: a query reads the rollup only when it groups by it, or fixes it to one value with `where:`, at every level that aggregates.
- **Where it is declared.** A declaration belongs to the source that writes it. An extension inherits its base's rollups' meaning but does not get its own; an include-list item's annotation is the including source's. Each rollup is planned once, under the model that declares its base, and its name carries that model, so two same-named sources in two models never share a table.
- **Extensions.** A query on an extension or rename of a rolled-up source (`source: orders_plus is orders extend { … }`) is answered from that source's rollups when the extension adds no `where:` and no `timezone:` (and, past a gate, only gates the rollup can prove; see below), and the query reads only grain dimensions, stored measures, and dimensions the extension defines over grain dimensions alone (`dimension: cat_lower is lower(category)`). A dimension over anything else or written in raw SQL (`sql_string(…)`), a measure the extension adds, an extension that filters its rows, and an extension that binds a base field's name to another field (`rename: region_was is region; dimension: region is 'east'`, which changes what the base's stored measures read) are answered live (`GRAIN_TOO_COARSE`, `MISSING_FIELD`, `TIME_ZONE`).
- **Access filters.** A query whose entry point carries a row-level gate (`#(access_filter)`) or a lock (`#(authorize)`) is answered from a colocated rollup when every gate of the rollup's base is the base's own (G3, with `PUBLISHER_MATERIALIZE_GATED_SOURCES` off, as a stored source keeps its own row-level gate), or, with that switch on, whatever the base inherits; never from a `storage=` one yet. The queried source is then redeclared over the rollup with its gates, and the gate is grafted and proven on that compile as on live (route, then govern), so each caller gets live's answer over the rows its gate admits. A gate is exact on a rollup only when each field it reads is a grain dimension: a column (`#(access_filter) region = $REGION` with `region` in the grain), a dimension the source computes from its own columns, or a truncation (`order_day`). A lock that reads no field, a lock beside a filter, an inherited gate, and `#(authorize) true` re-opening a locked base beside a filter are proven as their parts. The gate's field is often `internal`; a hidden grain dimension is stored and stays hidden on the rollup (see **Hidden grain dimensions**), so no query selects it (a caller's own `extend` reads an `internal` one within the rows the gate admits, as live). A rollup whose gate reads a field the grain drops, reads through a join (`account.org_id`, or a dimension defined through one), or whose source joins a gated source, is not built (`preaggregate-not-built`, reason `access_filter`), and its queries are answered live. With the switch off, a rollup over a base whose gate it inherits from a source it extends (the gate's note written on that source, not the base's own statement) is not built (the same warning), since no query would read it; a rollup in a `storage=` destination over a gated source is not built with the switch on either, until the managed tier's security review. A query skipped for its gate reports `CONTRACT`, whose detail never names the gate. A caller's own `except:` or `rename:` of the gate's field is denied, as live is, and a caller's join into a gated source keeps the query off every rollup. Publisher does not authenticate callers: the gate holds only behind the gateway that sets the givens from a verified identity ([operating model](security-posture.md#operating-model-a-microservice-behind-your-gateway)).
- **Through a join (join-to-rollup).** A query on another source that reads this source's measures through a join (`run: flights -> { group_by: carrier; aggregate: segments.seg_miles }`, with `join_many: segments on segments.flight_id = id`) is answered from this source's rollup when the rollup's grain keeps the join's key (`grain=["flight_id"]`, or `["flight_id", "kind"]` to also group or filter by `segments.kind`). The join is re-pointed at the rollup for that query; everything else about the querying source is as written, so a parent with no rows answers as live does. It reads this source's stored measures only: an aggregate the query writes over the join (`segments.count()`, `count(segments.flight_id)`) is answered live. The rules for this source's own queries hold through the join, with each field named `segments.<field>`: a fan-out grain dimension (`line_kind is lines.kind`, read through `segments`' own `join_many`) must be grouped (`group_by: segments.line_kind`) or fixed to one value (`where: segments.line_kind = 'a'`) at every level that aggregates through the join, and a join-level sum or count needs every other grain dimension grouped or fixed there too. Grouping or fixing the querying source's key (`group_by: id`, with `on segments.flight_id = id`) fixes the join's key, since each rollup row a parent meets carries it. A level that aggregates only the querying source's own rows (`flight_count`) asks nothing; a level that reads this source's measures through an expression, a `having:`, or a measure of the querying source is held to every grain dimension a measure fans out across, and a dimension of the querying source over a fan-out dimension (`dimension: k is segments.line_kind`) does not count as grouping it. The rollup was built in this source's time zone, so a querying source that declares a different `timezone:` (or a joined source that declares its own) is answered live (`TIME_ZONE`). Through a `join_one`, a query that also aggregates the querying source's own rows (`flight_count` beside `crew.crew_size`, a `having:` on one, another join's measure) is served only when the rollup's build checked the join's key unique: when a `join_one` in the package is keyed on the rollup's one grain dimension (`join_one: crew with id` and `grain=["flight_id"]`), each build of the rollup, colocated or in a `storage=` destination, also groups the rows it read (the base's, on its own connection) on that key and records whether any non-NULL value repeats. A build that read a stored upstream's table instead (its entry says `upstreamReuse: "reused"`, as for a base that is itself stored) records no check, since the base's own rows may not be the rows it grouped, and the query is answered live. Unique, the joined rows live and the rollup's are the same rows, and the query is served. A repeated key (the data breaks the declared `join_one`, which is logged at build and reported as a package warning, `preaggregate-join-one-key-repeated`, on the rollup's source while that build's table is bound) or no check on record is answered live (`COUNT_SEMANTICS`): live counts each parent once per joined row, which a rollup with one row per key cannot reproduce. No check is on record for a table a control plane's manifest binds, a build whose check could not run, or a table a run is rewriting; a check describes the rows the table was built from, and a rebuild checks again. A query that reads another `join_one` whose key a rollup's build found repeated is answered live too (`COUNT_SEMANTICS`), since re-pointing one join changes how Malloy aggregates over the rows the other repeats; one never checked is served as before. A join read as a `join_many` (a declared one, or a `join_one` read at a grain finer than its key) is used only when it is keyed on the querying source's primary key (`on segments.flight_id = id` with `primary_key: id`): keyed on another field (`join_one: crew with flight_id` from a source of segments, or a `join_many` from a source with no primary key), one rollup row meets several of the querying source's rows, which live counts differently, so it is answered live (`COUNT_SEMANTICS`). The data has to keep that primary key too: each build of a colocated rollup also checks every source the package declares that reads it through such a join, grouping that source's rows on its primary key, and records whether a value repeats or is NULL. Unique and never NULL, the query is served. A repeated key (two rows with `id = 1`, where the variant reads flight 1's rollup row twice and live sums each segment once) or a NULL key (where the variant's `count()` counts a row live's skips), logged at build, or no check on record, is answered live (`COUNT_SEMANTICS`). The check describes the querying source's rows as the rollup's build found them, is carried and forgotten with the rollup's table exactly as the `join_one` key check is, and is never on record for a table a control plane's manifest binds; a querying source the build could not check (one whose `where:` reads a given, with or without a default, since one check cannot stand for every caller's rows; on another connection; or defined in the query, `run: flights extend { … } -> …`) is answered live. A rollup in a `storage=` destination is read beside the querying source's stored table there, so that table's build checks the key instead, over every row it holds (a `where:` term the served copy applies at read only narrows them), with the same rules: unique and never NULL serves, anything else, or no check on record, is answered live. A rollup in a `storage=` destination is read through the join when the querying source is stored in the same destination (a `#(materialize) storage=` table, or a `#@ persist storage=` source, read through the served copy, with `PUBLISHER_MATERIALIZE` and the storage tier on): the querying source is read from its stored table and the join from the rollup, so the whole query runs in the destination, with the querying source's given-reading `where:` terms and rolling windows applied at read as the served copy applies them. The path report names the rollup as the table that answered (`servedBy`) and lists the querying source's table as `used` beside it. Both tables must be fresh. A querying source not stored there, one whose table is not built or not fresh, or a reader that opts out with `#@ -persist`, is answered without the rollup (`CROSS_CONNECTION`), since Malloy joins within one connection; so is a query that reads a definition of the querying source the served copy leaves out because it reads the join to a source not stored in the destination (a measure or view over `segments`, when `segments` itself is not stored there). A querying source stored as a pair table (`inline=` on a `join_many`) is not read this way (`COUNT_SEMANTICS`), and the served copy's rules for a join it stores inline hold (`COUNT_SEMANTICS`, `MISSING_FIELD`). A parent with no rows counts as no rows and sums nothing, live and from the rollup, so a `count()` through a `join_many` and a sum of any value (`sum(miles + 1)`, a constant) are served, as is a `count()`, `sum` or `avg` the query takes through another `join_many` while this join is re-pointed. Malloy does not skip that parent's row for a `min` or `max`, so one of a value that is not NULL there (`max(coalesce(miles, 0))`, a constant) is answered live (`COUNT_SEMANTICS`); a `min` or `max` of a column is served. A join-level measure's column (`kinds.weight` in `kinds.weight.sum()`) is NULL there when its join is keyed on a column of this source, so it is served; an aggregate at a join keyed on a computed dimension (`join_one: k is kinds with coalesce_kind`) can match a row for a parent with no rows and is answered live (`COUNT_SEMANTICS`). Since Malloy 0.0.438, live also takes a `count()`, `sum` or `avg` of this source's own rows only over the rows whose primary key is not NULL, and once per distinct key where the rows fan out, while the rollup reads every row. So when a join reaches this source on a column other than its primary key (`on segments.flight_id = id`, with `primary_key: seg_id`), each build of the rollup also checks this source's primary key, over its rows on its own connection, colocated or in a `storage=` destination, and records whether a value repeats or is NULL. Unique and never NULL, a count or sum through the join is served. A repeated or NULL `seg_id`, or no check on record (the same cases as the querying source's check above), is answered live (`COUNT_SEMANTICS`). A `min` or `max` is served either way, and a join keyed on this source's primary key (`join_one: crew with id`, with `primary_key: flight_id`) needs no check.
- **Its own time zone.** A rollup over a source that sets its own `timezone:`, or inherits one, is not built (`preaggregate-not-built`, reason `time_zone`): a query on that source is never answered from a rollup (`TIME_ZONE`, above), so the table would cost a build no query reads. The exception is a rollup that another source in the same `timezone:` joins, which a query on that source reads through the join: colocated, or in a `storage=` destination that source (or a source it is built on) is stored in too. A rollup in a destination that no such source is stored in is not built either (reason `time_zone`): the join leg reads it only for a querying source stored beside it, and a query from one stored elsewhere is answered live (`CROSS_CONNECTION`).
- **The current time.** A rollup over a source whose rows read `now` (a rolling window, `where: t > now - 7 days`), or whose grain dimension or measure reads it, is not built (`preaggregate-not-built`, reason `current_time`): the rollup would keep the window its build saw, while a live query moves it at every read.
- **A stored base.** A rollup is planned over the author's rows, so a rollup over a `#@ persist storage=` base builds and serves (#6293). A run may build it from the base's stored table instead, with the same rows ([Build and refresh](#build-and-refresh)).
- **What routes.** The query's text is compiled where its source's name means the rollup. It routes when every field it groups or filters by is in the grain, every aggregate is a stored measure's merge (an aggregate written in the query or in a `having:`, or a `select:` stage, is not; this is read from the compiled query, not its text), and a `timezone:` it sets, in any stage or nest, changes nothing it reads from the rollup: it reads no date or timestamp, directly or through a dimension, a stored measure or the base's `where:`, and nothing through a join (`run: orders -> { timezone: 'America/New_York'; group_by: category; aggregate: total }` routes; grouping or filtering by `order_day` there does not, `TIME_ZONE`, since the rollup stored its days in the model's zone). A query on a source that sets its own `timezone:` is answered live (`TIME_ZONE`): the rollup's stored times are not read in that zone, so a truncation, a filter on one, or a gate on one would not match live. Since no query on it reads such a rollup, a `#(preaggregate)` on a source that sets (or inherits) its own `timezone:` is not built (`preaggregate-not-built`, reason `time_zone`), unless a source in the same zone joins it, the one read left (through a join, above). A `calculate:` over a measure (`lag(total)`) reads that measure at its level, so it is held to the same rules as an aggregate there. A fan-out dimension counts as fixed by `where: d = <one value>`, alone or as one conjunct of an `and`. A query that supplies a given routes. A named `view:` routes as its own query does: `run: orders -> by_category`, a refinement of it, a query starting from it, and a `nest:` of one are compiled with the view as you wrote it, and answered from the rollup when the view's query passes the same checks, its text, and the text of every view it builds on, still compiles to the view the served model holds (an edit not yet reloaded is answered live), and it reads no raw SQL (`sql_boolean(…)`, whose columns the rollup may not keep). A persisted view of that name (`#@ persist` on the view) answers its own call first. The path report names the reason for each skip, and an answer from a rollup reports its tier in `servedFrom`: `storage` for a `storage=` rollup, `database` for one in the source's own database.
- **Givens.** A `where:` of the source's own that reads a given (`where: region = $REGION`) and whose every field is a grain dimension is left out of the rollup's build, so its table holds every caller's groups, and is written again on the rollup when it is read, where each caller's givens bind it, so each caller gets exactly live's rows. The routed answer is re-proven in `govern()`, as a gate is: its source must apply exactly the given-reading terms the live source does, or the query is denied. Such a rollup is not read through a join, nor by a query on another source that extends its base (both answered live). A term over a field the grain does not keep, one the source inherits, or one that reads the current time too, refuses the rollup (`preaggregate-not-built`, reason `reads_given`), as does any other build that reads a given: its table would hold one value's rows.
- **Hidden grain dimensions.** A `private` or `internal` dimension may be in a grain: it is stored, and stays `private` or `internal` on the rollup, so no query selects it, while a query grouping by the rest of the grain, or a view that reads it, is answered from the rollup. A `private` dimension the base inherits from a source it extends is never stored (Malloy keeps it private there), so that rollup is not built (`preaggregate-not-built`, reason `hidden_field`). A measure the source hides is not built either (`hidden_field`).
- **Other compile-time refusals.** A valid rollup that a compile-time gate refuses alone is not built, and its measures are answered live, while the rest of the package loads: one on a dialect whose connection creates no table (SQL Server), one whose table name another rollup's table holds, one over a base scoped by a given, a v1 `#@ preaggregate` rollup over a base with a row-level gate, or one the `storage=` gate refuses. The warning is `preaggregate-not-built`, with `subject` the base, and its message names the base, the grain and the reason. Every rollup refused alone carries this one code, its reason (`access_filter`, `time_zone`, `current_time`, `reads_given`, `hidden_field`, `join_level`, `unreadable_measure`, `name_clash`, or a gate's) on its record in the build plan's `refusedSources`. A line Publisher cannot read fails the publish and the load, on `#@ preaggregate` as on `#(preaggregate)`: one that does not parse is `preaggregate-unparseable`, and a value that is wrong, such as an empty `grain=[]`, an empty or non-text `storage=` or an `@env.` reference, is `preaggregate-invalid`. The retired `preaggregate-malformed` is no longer produced ([fails the load, or refused alone](materialization.md#fails-the-load-or-refused-alone)).

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

A declaration that is wrong as written fails the publish and the load: a `400` from publish and `PATCH`, and a load failure on every other path, a reload, a restart or a package already published included (reported in `ServerStatus.loadErrors`). A v1 line fails under the same code as `#(preaggregate)` for the same mistake: `preaggregate-not-a-measure` when it is misplaced, `preaggregate-not-mergeable` when the measure is not additive, `preaggregate-unknown-grain` for a bad grain, `preaggregate-unknown-key` for a key it does not read, and `preaggregate-invalid` or `preaggregate-unparseable` otherwise. The rules are worth knowing before you write the annotation, because the alternative to refusing is worse: a rollup that silently does nothing returns correct numbers, so the only signal is the bill.

### Only measures

`#@ preaggregate` on a source, dimension, join, or view is refused. Dimensions appear in a measure's `grain=`; they are what the rollup groups by, not what it stores.

### Only measures whose partials can be merged

A rollup stores a partial aggregate per group, and answering a coarser query merges those partials. That works for `sum`, `count`, `min` and `max`, and not for anything else: an average of averages is not an average.

A filter written directly on the aggregate is fine — `paid is amount.sum() { where: is_paying }` pre-aggregates, because the measure *means* the filtered value and filtering commutes with merging per-group partials. Several conditions belong in that one `where:`, comma-separated. The filter has to sit on the aggregate itself, applied once: a chained refinement (`amount.sum() { where: a } { where: b }` — the spelling most likely to happen by accident; write `{ where: a, b }` instead), a filter refining a derived measure, or an aggregate wrapped in a further expression (`coalesce(amount.sum() { where: … }, 0)`) fails the publish and the load like any other shape whose merge cannot be proven.

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

A grain may name only dimensions the source itself declares. An inline expression is refused, including a time truncation — this fails the publish and the load:

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

This rule is wider than it sounds: it disqualifies the whole source, not just the measures that use the fan-out join. A source with one `join_many` cannot pre-aggregate any of its `#@ preaggregate` measures. The declaration is valid, so the package still loads: its v1 rollups are refused alone with warning code `preaggregate-not-built` (reason `join_level`), as `#(preaggregate)` refuses a join-level aggregate that fans out, and their measures are answered live. `#(preaggregate)` has the narrower rule [above](#preaggregate-materialization-v2-behind-publisher_materialize).

## What does not route

Three limits are worth knowing before you decide pre-aggregation will help your workload. None affects correctness — an uncovered query is answered from the base, with the answer it always had — but each means no acceleration.

**A query with an aggregate the rollup does not store does not route.** A rollup holds one row per grain group and a stored partial for each annotated measure, so it answers a query only when every aggregate in it is one of those measures, or an expression over them (`revenue / order_count`, `revenue { where: category = 'books' }`, `all(revenue)`). An aggregate written in the query itself — `count()`, `qty.sum()`, `count(category)`, `stddev(amount)`, including one in a `nest:`, a `having:`, or a measure an inline `extend` defines — would be computed over the rollup's rows rather than the base's, so the query is answered from the base. To accelerate a count, declare it as a measure and annotate it (`#@ preaggregate grain="category"` on `order_count is count()`), and ask for `order_count`. For the same reason a `select:` or `index:` stage over the source, including one in a `nest:`, is answered from the base: a rollup has one row per grain group, not one per source row.

**A named view routes when its own query does.** The rollup is read in a copy of your source's own file, so a view it declares is carried onto it and checked for coverage like a written-out query: `run: orders -> by_category` reads the rollup when `by_category`'s query is one the rollup covers.

**A rollup over rows that read a [given](givens.md) is built only when the term can be re-applied.** A `where:` of the source's own that reads a given over grain dimensions only is left out of the build and applied again per caller when the rollup is read. Any other given the rows read (a term over a field the grain drops, an inherited one, a given inside a query-shaped base) refuses the rollup, with a `preaggregate-not-built` warning (reason `reads_given`), and its queries are answered from the base: the table would hold one given value's rows. A given the rows do not read changes nothing.

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

A rollup's table is Publisher's to identify — its address and the hash ending its name
derive from the base, the grain and a digest, so the resolver and the content address can
rely on it. With the same `name=` every other table takes, you choose the schema or
dataset it is created in, which on some warehouses you must, and how its name starts.

```malloy
source: orders is bigquery.table('project.raw.orders') extend {
  #(preaggregate) grain=["category"] name="analytics.orders_rollup"
  measure: total is amount.sum()
}
```

That builds `analytics.orders_rollup__p<hash>`. Everything before the last dot is the
container, and the leaf takes the place of the
[readable name](materialization.md#readable-table-names)'s prefix
(`<package>__<file>__orders_by_category`), as a `name=` does under `scope: "version"`; the
`__p<hash>` that keeps the name unique is always Publisher's, so the table is never at the
`name=` itself. A `name=` with no dot (`name="orders_rollup"`) sets only the prefix, in the
connection's default schema. Without `name=`, the table takes the readable name. The leaf
is written into the name as it reads, lowercased (`OrdersRollup` names `ordersrollup__p<hash>`):
a leaf the name would otherwise rewrite (a space, punctuation, a leading digit, `__`), an
empty one, a container needing quotes, or a leaf ending as Publisher's own names do
(`__p<8 hex>`) fails the publish and the load (`preaggregate-invalid`), and so do measures at
one grain naming different leaves. Two rollups that compile alike but name different leaves
are two tables. Changing the leaf renames the table, which is rebuilt once under the new
name, and the old table is dropped once that run commits and is served. `#@ preaggregate grain="category" name="analytics.orders_rollup"`
places a v1 rollup the same way.

**`namespace=` is deprecated.** `namespace="analytics"` is the older spelling of the
container alone. It is read for one more release, places the table in `analytics` under
its readable name, and warns `deprecated-preaggregate-namespace` naming the rewrite,
`name="analytics.<table>"`; switching keeps the table's container and address, and
renames it under the leaf you choose. Writing both `name=` and
`namespace=` on one line fails the publish and the load (`preaggregate-invalid`).

**BigQuery requires it.** An unqualified `CREATE` is rejected outright there
(`must be qualified with a dataset`), so a rollup with no container, its own or its
base's, is not built on BigQuery: it is refused at plan, before any warehouse work
(`preaggregate-not-built`, reason `needs_qualified_name`), and its measures are answered
live. Name a dataset the connection can write — typically a scratch dataset, the same one
you would give an authored `#@ persist name=`. Other engines accept an unqualified name
and create the table in the connection's default schema, so the container is optional
there.

**The namespace has to exist already.** Nothing in the tier creates a dataset or a
schema, so a name that is merely plausible fails the build. Create it once, and let the
connection's credential create tables in it.

**It belongs to the grain, not to the measure.** A `name=` is written on the same
line as the `grain=` it applies to, and applies to nothing else — two grains are two
tables and can be created in two places:

```malloy
measure:
  #@ preaggregate grain="category" name="analytics.by_category"
  #@ preaggregate grain="order_date" name="archive.by_day"
  total is amount.sum()
```

Measures that share a grain share its one table, so they must agree on where it goes.
Two naming different containers fail the publish and the load (`preaggregate-invalid`) rather than being resolved silently.
One grain placed in two containers by two lines of one measure is two tables, each built.

**A materialized base lends its own.** If the base carries `#@ persist
name="analytics.orders_tbl"`, or `#(materialize) name="analytics.orders_tbl"`, its rollups
are created in `analytics` without being told — a rollup of a table belongs beside it,
on `#(preaggregate)` and `#@ preaggregate` alike, so the two spellings land in one
schema. A rollup's own `name=` overrides that, and is the only option when the base is
not materialized at all, which is common: the annotation goes on a measure, and the
source it belongs to need not be materialized. A rollup's table is never its base's,
whatever the two `name=`s say: the rollup's name always ends in its own `__p<hash>`.

A base that also carries `storage=` lends nothing — not its container, and not its
destination. See [Rollups in the managed store](#rollups-in-the-managed-store) below.

**Changing it does not move a table that already exists.** A container is not part of
what identifies a rollup's contents, so a package whose rollups have already built keeps
them where they are: the annotation changes and the table does not. Ask for the rebuild
explicitly — `malloy-pub materialize --force-refresh`, or `forceRefresh` on the build
endpoint — which is what defeats the unchanged-content skip. Dropping the table by hand
does not, and leaves the manifest pointing at something that is no longer there.

**A re-stated grain takes the `name=` on its own line, including none.** Writing
`grain="category"` again without `name=` clears the one an earlier line gave that
grain, which matters most across an extend chain: an extending source that re-states its
base's grain must restate the `name=` too, or the rollup moves. To turn a declaration
off rather than replace it, negate with `#@ -preaggregate`.

**Its container is plain names.** Write the container as plain identifiers — letters,
digits, underscore, dollar or hyphen — dot-separated where the warehouse needs more than
one part (`name="analytics.t"`, `name="my-project.analytics.t"` on BigQuery). A container
needing quotes fails the load rather than being mangled, because the generated table name
is appended to it and the two halves would then be quoted inconsistently.

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

**A rollup may not publish what the source hides.** A rollup stores its grain and each
measure's partial. A hidden grain dimension (`private` or `internal`) is stored and
redeclared hidden on the rollup, so no query selects it, as a stored source keeps its
hidden columns. A hidden measure would be served under its name, so `#@ preaggregate` on
a measure the source does not publicly expose builds nothing, and neither does a grain
naming a `private` dimension the base inherits from a source it extends, which its build
cannot read.

That is reported as a **warning** (`preaggregate-not-built`, reason `hidden_field`) that refuses the rollup
alone, rather than failing your publish and load as the mistakes in this section do. Partly because such a package published before the rule existed and
an upgrade must not break it, and partly because the refusal was unfollowable: an
annotation inherited onto a source that then hides the measure would raise it, while that
source produces no rollup at all and exposes nothing.

**Write it on the `#@ preaggregate` line.** A destination is not inherited from the
base's `#@ persist storage=`. A rollup's placement is its own line's, and it is part of
the rollup's identity, so adding or removing `storage=` on a base never moves its rollups'
tables, and a rollup goes to a destination because its own line says so. That is also the
common case: a rollup's base is usually a table extended with measures, and Malloy admits
only query-shaped sources as build roots, so most bases cannot carry `#@ persist` at all.

**A rollup over a stored base builds and serves.** A rollup is built from the author's
rows, never from its base's stored table, so a `#@ preaggregate` on a source that is itself
`#@ persist storage=` is planned, built and served exactly as the `#(preaggregate)` it
reads as. Each grain is one table, in the destination its own line names or, with no
`storage=`, alongside the base in the warehouse the base reads from, under the rollup's
[readable name](materialization.md#readable-table-names)
(`<package>__<file>__<base>_by_<grain>__p<hash>`), never the base's. The base's own
`name=` is neither a prefix nor a container for it. A query a rollup covers is answered
from the rollup; one it does not is answered as it was before:

- **A query-shaped base** builds its own stored table as well, which keeps answering every
  query no rollup covers, the base's own measures included.
- **A table extended with measures** is not a build root, so a `#@ persist` on it is
  invalid: the publish and the load fail (`persist-not-materializable`), as they do for any
  `#@ persist` on a shape Malloy does not plan. Drop the `#@ persist`, which could store
  nothing; the rollups build without it, and a query no rollup covers is answered live.

The refusals are the rollup leg's, for the same reasons as `#(preaggregate)`: a base whose
rows read a given (reason `reads_given`) or the current time
(reason `current_time`) builds no rollup, and neither does a gated base unless
`PUBLISHER_MATERIALIZE_GATED_SOURCES` lets a query read the rollup past its gate
(reason `access_filter`). Each is a `preaggregate-not-built` warning, and the measure is served live.

A colocated base's `name=` container *is* inherited, and the asymmetry is not about
inheritance. Colocated rollups are offered through a companion model whose members
Publisher names itself, with the base under an import alias, so nothing there is keyed on
your source's name and there is no binding to collide with.

**It belongs to the grain, like `name=`.** Two grains are two tables and can be
placed differently — one in the store, one alongside the base. Measures sharing a grain
share its one table, so two of them naming different destinations fail the publish and the
load (`preaggregate-invalid`).

**`name=` and `storage=` cannot be combined on a rollup.** Placement inside a destination is
derived, not authored: a freshly provisioned catalog has no schema to create the table
in. Writing both (or `namespace=` beside `storage=`) on one line fails the publish and
the load (`preaggregate-invalid`).

**The tier's switch applies.** With `PERSIST_STORAGE_MODE` off, a `storage=` rollup is
not built — and not built anywhere else either. Falling back to a build alongside the
base would put a table in your warehouse under a generated name you never wrote, so
nothing is built, queries are answered from the base, and the package reports the
degraded state as a warning (`materialization-storage-mode`). Under `write-only` the rollup builds but does not serve.

**An uncovered query is answered from the base.** A query no rollup covers, in the store
or alongside the base, is answered live, with the answer it always had; the path report
names each rollup it considered and why it was skipped.

## Build and refresh

Rollups build with the package's other materializations — on demand, or on the package's schedule. See [materialization.md](materialization.md) for triggering builds, the `malloy-pub` CLI, and scheduling.

Two things are specific to rollups:

**A rollup that has never been built costs nothing but latency.** Queries compile against the rollup and the base together, and until a table exists the base answers everything. You will not see an error, only the performance you had before.

**A stale rollup is skipped, not served.** When a rollup's table falls outside its freshness window it drops out of the serving set and queries recompute from the base, unless its fallback is `stale_ok`. The answer is the same either way, which is what makes a refresh schedule a cost decision rather than a correctness one.

**A rollup builds from its base's table when it can.** A colocated rollup over a colocated `#@ persist` base, or over a colocated `#(materialize)` base that stores the base's own rows (not a pair table, and not one whose `where:` reads a given or the current time), reads the base's stored table rather than the warehouse, when the run building it holds that table: built earlier in the same run, or carried forward from an earlier one. Such a rollup builds after every other table in the run, so a base the run builds is there to read; any other rollup keeps its place. It stores the rows of the base's stored table, which equal the warehouse's while that table is current. An incremental base's deletions and restatements below its watermark carry into the rollup, as they do for a rollup over a `#@ persist` base. Its `upstreamReuse` reads `reused`, and its `dataAsOf` is the base's when that is older. A rollup with a freshness window reads a carried base only when the base's `dataAsOf` is known and inside the rollup's window, so a rollup is never fresher than the rows it holds; otherwise it builds from the warehouse, and its entry has no `upstreamReuse`. A `storage=` rollup over a base whose own `#@ persist` stores it in the same destination does the same in the destination: when the run holds the base's table there, current and, for a rollup with a window, inside it, the rollup groups that table, read as the served copy reads the base for a query, and reads nothing from the warehouse. It keeps that table only when it has the columns a build from the warehouse writes, with the same types, and the base's table holds the columns its current build writes, with the same physical types; otherwise it builds from the warehouse, as planned. Its `upstreamReuse` reads `reused` when it read the base's table, and is absent when it built from the warehouse. A colocated rollup over a base that is not persisted, or is stored in a destination, builds from the warehouse, as does a `storage=` rollup over a base stored elsewhere, not persisted, or read through a build copy (a given-reading `where:`, a private grain dimension).

### Freshness

A rollup takes `freshness.window=` and `freshness.fallback=`, with the same meaning as on `#@ persist` ([materialization.md](materialization.md#freshness-a-window-and-a-fallback)), on either spelling:

```malloy
source: orders is duckdb.table('orders.parquet') extend {
  #(preaggregate) grain=["category"] freshness.window="1h" freshness.fallback="stale_ok"
  measure: total is amount.sum()

  #@ preaggregate grain="order_day" freshness.window="6h"
  measure: order_count is count()
}
```

Each key resolves on its own: the rollup's own declaration, then its base's own `#@ persist` or `#(materialize)` declaration, then the model file's, then the package's. Undeclared, a rollup goes stale with its base. Measures stored in one rollup (one grain and placement) share one table, so they share one freshness: a measure that says nothing takes the others', and two that disagree fail the load (`preaggregate-invalid`; windows compare as durations, so `24h` and `1d` agree). A key in the group other than `window` and `fallback` fails the load with `preaggregate-unknown-key`, and a window that is not a whole number and a unit (`s`, `m`, `h`, `d`, `w`), or a fallback other than `live`, `stale_ok` or `fail`, with `preaggregate-invalid`.

## What it costs

A rollup is a table Publisher builds and refreshes, so it is worth being deliberate:

- **A grain near the base's cardinality saves nothing and costs a build.** `customer_id, order_day` on a table with one row per customer per day is a copy of the base. Check the grain's cardinality before annotating.
- **Prefer several small grains to one combined grain** when the combination is large but each dimension is small, as above.
- **A package with no `#@ preaggregate` pays nothing.** Publisher derives rollups from the compiled model in memory and stops as soon as it finds no annotations, so an unannotated package plans, builds, and serves exactly as it did before.

The [materialization tuning guide](../skills/malloy-materialization/reference/tuning.md) helps decide what is worth storing.
