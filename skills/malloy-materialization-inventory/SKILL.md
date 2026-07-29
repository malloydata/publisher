---
name: malloy-materialization-inventory
description: Profile a slow Malloy package's underlying warehouse data to decide what to materialize and at what grain. Use when a model or dashboard is slow and you need to know where the row volume actually is, how selective each hard-coded filter is, what each query costs, and which physical tables to build - before writing any persist tags; pair with malloy-materialization-design for where the tags then go. Also use for "inventory the tables", "why is this model slow", "what should we materialize", "profile this dataset".
---

# Materialization inventory

Measure a dataset before you accelerate it. This skill produces the evidence that decides
*what* to materialize and *at what grain* - deliberately upstream of `skill:malloy-materialization`
(how to write and debug a persist tag) and of any tuning skill (cadence and scope).

> **Tool names** are written bare here - `get_context`, `execute_query`, `search_malloy_docs`. The exact prefixed name depends on the host surface; match each against the tools you actually have.

> **Hand-off:** this skill ends when you know *what* to build. `skill:malloy-materialization-design`
> covers where the `#@ persist` tags then go and the safety rule that stops a narrow table returning
> wrong answers; `skill:malloy-materialization` covers tag syntax and build debugging.

> **The one rule:** never design a materialization from the model's shape. Design it from
> measured row counts at the grain the queries actually use. Models routinely look expensive in
> places that hold 0.1% of the rows, and cheap in places that hold 90%.

## Why this order matters

The instinct on a slow model is to materialize the biggest table. That is usually wrong, because
"biggest" is a property of the table and "slow" is a property of the query. The useful questions
are all relational:

- What does *one* query actually need to read?
- How much of the table is that?
- Is the remainder ever read at all?

A dataset where every query filters to one entity, and one entity is 0.01% of the table, wants a
completely different treatment from one where queries scan the population.

## Step 1 - Read the access pattern off the model, not off the data

Before touching the warehouse, find the **required filter parameters** and static `where:` clauses
on every public source.

```bash
grep -rn "#(filter)" *.malloy          # required params → the mandatory access key
grep -rn "^  where:\|^    where:" *.malloy   # static pins per source
```

This tells you the shape of every possible query, for free:

- **A required entity filter on every fact source** (`customer_id`, `tenant_id`, `account_id`) means
  *no query can ever span entities*. Per-entity footprint becomes the number that matters, and
  partitioning or clustering on that key is the highest-value physical change available.
- **A static single-value pin** (`where: some_type_id = 17`) is a free, compile-time partition.
  Sources that pin one value can be repointed to a narrow table with zero interface change.
  Sources that leave the value to the caller cannot (see Step 7).
- **A range or multi-value pin** (`where: channel = 'web'`) may look selective and not be.
  Measure it; don't assume.

Record which sources pin what. This table drives everything downstream.

## Step 2 - Build a throwaway probe package

Put probes in **their own package directory, outside the package you publish**, with their own
`publisher.json`. Never add a probe file to the real package - every file in a package is compiled
at build time, and a probe with a missing experimental flag can abort the whole build plan.

```
<repo>/_introspect/
  publisher.json     # {"name":"introspect-scratch","version":"0.0.1","description":"throwaway"}
  probe.malloy
```

### Use `conn.table()` on catalog views, not `conn.sql()`

```malloy
// WORKS on any connection
source: t_tables is my_conn.table('MYDB.INFORMATION_SCHEMA.TABLES')
source: t_cols   is my_conn.table('MYDB.INFORMATION_SCHEMA.COLUMNS')
source: t_views  is my_conn.table('MYDB.INFORMATION_SCHEMA.VIEWS')
```

**`conn.sql("...")` fails on proxy / publisher-type connections.** Malloy's SQL-source path creates
a temp table first, and a proxy connection typically can't, so you get
`Table 'TT<hash>' does not exist or not authorized`. Two consequences worth knowing up front:

- Anything only reachable as a **table function** (`QUERY_HISTORY`, `TABLE(...)` generators) is
  **unreachable** on such a connection. If you were planning to attribute compile-vs-execute time
  that way, that plan is blocked - find out now, not after you've promised the measurement.
- Everything you need for an inventory is a *view* (`INFORMATION_SCHEMA.TABLES/COLUMNS/VIEWS`), so
  `conn.table()` covers it.

Run the probes against the probe package with your query tool (`execute_query`; the exact name depends on the host).

## Step 3 - Separate catalog facts from measured facts

Query the catalog first, because it's free - but know its limits.

| Want | Source | Caveat |
|---|---|---|
| row count, bytes | `INFORMATION_SCHEMA.TABLES` | **null for views.** Only base tables report these. |
| column count, types | `INFORMATION_SCHEMA.COLUMNS` | reliable for views too |
| view → base table | `INFORMATION_SCHEMA.VIEWS.VIEW_DEFINITION` | often **null** without extra privilege |

If the facts are views (common in a governed warehouse), you get **no free size data** and possibly
no lineage. Say so explicitly in the writeup and mark every fact figure as *measured by counting*.
Do not silently present a counted number as if it were catalog metadata.

Two cheap catalog wins that pay off immediately:

- **Semi-structured column census.** `where DATA_TYPE in ('VARIANT','OBJECT','ARRAY')`. These are
  the columns that make planning expensive; knowing exactly which ones exist, and their ordinal
  positions, tells you what a flattening pass has to do.
- **Width.** A 60-column fact with 7 semi-structured columns is a different problem from a
  23-column fact with 1.

Guard against timeouts: `INFORMATION_SCHEMA.COLUMNS` on a large warehouse is huge. Always filter by
`TABLE_SCHEMA` before `TABLE_NAME`, and avoid string aggregations in the first pass.

## Step 4 - Profile one entity before the population

If Step 1 found a required entity filter, **the per-entity profile is the primary measurement.**
It is cheap (the filter prunes), it is exactly what a real query reads, and it usually reframes the
whole problem.

```malloy
run: fact -> {
  where: entity_key = '<one real entity>'
  aggregate:
    all_rows is count()
    // one line per hard-coded filter in the model, applied cumulatively
    after_filter_a is count() { where: <filter a> }
    after_filter_b is count() { where: <filter a> and <filter b> }
    // fan-out population rate
    with_payload is count() { where: <json/array column> is not null }
    // grain cardinalities
    n_discriminator is count(<discriminator col>)
    n_dim2 is count(<other grain col>)
}
```

Then compare per-entity rows to the whole-table row count. A ratio like 0.01% means the latency is
a **pruning failure**, not a volume problem - and that reframing changes which fix is correct.

## Step 5 - Selectivity-test every hard-coded filter

Run each model filter as a separate `count()` and write down what it removes. Expect surprises in
both directions, and act on them:

- **Filters that remove nothing.** Very common for defensive guards (`status_level = 1`,
  `job_name = 'the_only_job'`). They cost nothing to keep, but you must not *design around* them
  as if they were selective, and you should not present them as part of the scope reduction.
- **Filters that look selective and aren't.** A two-way categorical split (`'web'` vs
  `'store'`) is often ~50/50. Splitting a table on a 50/50 axis halves it - almost never enough.
- **Filters that are wildly selective.** A single-value pin on a polymorphic discriminator can be
  100×–1000×. These are the ones worth building tables around.

Then measure the same filter at more than one place if the model applies it in more than one
combination - selectivity is not a constant. The same filter cut one dataset by orders of magnitude on one
discriminator value and did nothing on another.

## Step 6 - Census the grain, then the fan-out

### Grain census

If the fact is **polymorphic** - one wide table where a discriminator column (`record_type_id`,
`event_type`, `metric_type`) selects which of many nullable columns are live - group by the
discriminator and count. This is the single most informative query in the whole exercise.

```malloy
run: fact -> {
  where: <package scope>
  group_by: discriminator, disc_name is meta.name
  aggregate:
    rows is count()
    payload_rows is count() { where: <json col> is not null }
    entities is count(entity_key)
  order_by: rows desc
}
```

Run it twice: once for **one entity** (what a query reads) and once for the **population** (what a
table would cost). Then annotate each row with *which queries read it*. The pattern you are looking
for, and will often find, is that volume and demand are inversely correlated: the deep multi-axis
grains hold most of the rows and serve almost no queries, while the coarse rollup grains serve
almost every query and hold a rounding error.

Also check `entities` per discriminator value: uneven coverage (some values present on only a third
of entities) is real and belongs in the writeup, because it changes what "complete" means.

### Fan-out census

For a JSON/array column that flattens to N rows per source row, measure **two separate things** -
they can distribute in opposite directions and the difference decides the design:

1. **Population rate.** What share of rows carry the payload, *by discriminator value*. Payloads are
   often absent for whole discriminator values, not just sparse rows - which silently invalidates
   any documented recipe that pairs that value with the fan-out.
2. **Value distribution across the fan-out axis, for each metric.** Do not check one metric and
   generalize. A count-of-things metric and a count-of-people metric over the same axis routinely
   have opposite shapes, so a bucketing scheme that is harmless for one destroys the other.

Then compute the row cost honestly: `payload_rows × buckets`, at each candidate scope. Bucketing
schemes look more valuable than they are - collapsing 100 buckets to ~30 is only ~3×, which
rarely rescues an infeasible build. **Restricting scope beats coarsening resolution**, usually by
orders of magnitude.

## Step 7 - Map the workload, then check transparency

List every real query - dashboard tiles from the app source, plus any documented test/eval
questions - and for each record: source, discriminator value, grain pinned, and cost driver.

Two things fall out of this that nothing else surfaces:

- **Shared causes.** Several slow queries usually have *one* structural cause (a fan-out join, a
  `GROUP BY` inlined into a join). One fix, several tiles.
- **Coverage gaps.** Questions with no corresponding tile land on grains nobody optimized. Find
  them here, not after building.

Then the decisive question - **can each source be repointed without changing its interface?**

- **Yes**, if the source *statically* pins the discriminator. The table choice is a compile-time
  fact; swap the backing table, keep every field name and doc string. Fully transparent.
- **No**, if the caller chooses the discriminator at query time. Table names resolve at compile
  time; filter values don't exist then. Making the discriminator a source *parameter* would change
  the contract.

### `compose()` is not a safe escape hatch for row subsets

Malloy's composite sources (`compose(narrow, wide)`) pick the first source defining **all fields the
query references**. That is field-presence routing, *not* filter-satisfiability routing.

So if the narrow table is a **row subset** (only some discriminator values) and it still carries the
discriminator column, a query pinning an absent value routes to the narrow table and silently
returns **empty or wrong** results. `compose()` is safe when the narrow source has *fewer fields*
over the *same* row coverage; it is unsafe for row subsets. Verify which one you have before
reaching for it.

## Step 8 - Write it up with the limits attached

Deliver measured numbers, each labelled with its scope, plus an explicit list of what you could not
measure and why. In this kind of work the gaps are load-bearing: "views report no size", "table
functions unreachable on this connection", "population totals exceed the query timeout", "this is
one entity, and it is about half the average size". A reader who doesn't know the gaps will
over-trust the numbers.

Validate at least one measured figure against a value someone already trusts (a known dashboard
number, a previous build log). If the totals disagree even slightly, report the discrepancy rather
than rounding past it - a 0.5% gap in a total that should reconcile exactly is a real signal.

Finally: **delete the probe package** when done, and note that materialized tables are not dropped
by removing their persist tags - stale physical tables outlive the code that made them.

## Step 9 - The inventory artifact

An inventory is worth publishing as a page, because its audience is several people over several
weeks and the numbers are the whole argument. Build it as a self-contained HTML page (inline the styles); what
follows is what this *particular* deliverable needs.

### Sections that earn their place

1. **The finding, first.** One paragraph plus 3–4 stat tiles. If the headline is a ratio
   (one entity is 0.01% of the table), lead with the two numbers that make the ratio.
2. **Physical inventory** - every object: type, columns, semi-structured columns, rows in scope.
3. **Hard-coded filters and their measured selectivity** - including the ones that turn out to do
   nothing. That table changes designs.
4. **Model lineage DAGs.**
5. **Grain census** - per entity and population-wide, annotated with *which queries read each grain*.
6. **The workload** - every real query mapped to source, grain and cost driver.
7. **Fan-out anatomy** if there's a JSON/array axis.
8. **The proposal**, sized from the measurements.
9. **What could not be measured, and why.**

### Make scale legible

The whole point is usually orders of magnitude - tens of rows to billions. Linear bars are useless across
that range, so **log-scale the magnitude bars** (`width% = log10(n)/log10(max) * 100`) and print the
exact figure in monospace next to each. Use `font-variant-numeric: tabular-nums` everywhere digits
line up, and give any wide table its own `overflow-x: auto` container.

### DAG pitfalls that will bite you

- **Add a legend.** Four `classDef` colours with no key is unreadable, and the author never notices
  because they know what the colours mean. Colour by *role*: raw/scaffolding, persisted table, live
  path (the slow thing), public source, hidden base.
- **Give every unclassed node a class.** Nodes you forget fall through to the renderer's default -
  often near-black - which reads as deliberate emphasis on something arbitrary.
- **Label each diagram current-state or target-state.** A page containing both, unlabelled, is
  actively misleading. This is the single most common complaint on this kind of page.
- **`<br/>` may not render** in the host's mermaid build, silently concatenating label lines into
  things like `fact_flat2B rows`, which reads as a typo. **Don't fight it** - put the node *name*
  in the node and move counts into an adjacent key table. Better information design anyway.
- Put diagrams on a fixed light panel in both themes so they can't render dark-on-dark.

### Keep it truthful as the work proceeds

The artifact is a living document; republish to the **same URL** so links keep working. Two failure
modes to watch:

- **Superseded recommendations left standing.** If measurement later falsifies a proposal on the
  page, *correct the page*. A stale recommendation in a document people trust is worse than never
  having written it.
- **Estimates never replaced by measurements.** Mark anything derived by ratio as an estimate, and
  overwrite it once the real number exists. Ratio estimates are routinely off by 2× - the
  fan-out dimensions rarely divide evenly.

When you get post-build numbers, add a measured before/after section. Predicted-vs-actual is the
most convincing thing the page can contain, including where you were wrong.

## Cost discipline

Everything here runs against production. Keep it cheap:

- Filter to one entity first; only go population-wide for the few numbers that need it.
- Avoid `count(distinct …)` on billion-row facts in a first pass - it is the usual timeout cause.
- Scope by schema before name when querying `INFORMATION_SCHEMA`.
- Prefer `count()` with filtered aggregates over many separate round-trips: one query with ten
  `count() { where: … }` clauses costs about one scan.
- If a query times out, narrow the scope rather than retrying it unchanged.
