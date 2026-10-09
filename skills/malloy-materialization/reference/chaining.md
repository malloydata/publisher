<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Chaining: one stored source built over another

A `storage=` source may read another `storage=` source. When both land in the same destination, the downstream is built by **reading the upstream's stored table** in the destination's own engine - the upstream is never re-scanned from the warehouse - and the chain is consistent by construction: the child is a pure function of the parent's stored rows. Chaining is what turns a package of independent rebuilds into one that touches the warehouse only for its roots.

```malloy
#@ persist name="daily_orders" storage=lake
source: daily_orders is orders -> { group_by: order_date; aggregate: total_amount is amount.sum() }

#@ persist name="monthly_orders" storage=lake
source: monthly_orders is daily_orders -> { group_by: order_month is order_date.month; aggregate: monthly_total is total_amount.sum() }
```

The downstream need not name the upstream directly. Non-persisted sources between them - a `select: *` wrapper, an `extend` that adds a dimension, a named source over an inline `(parent extend { ... }) -> { ... }`, a query over the upstream, a parent reached only through an `import` - are carried into the build in dependency order, under the `##!` flags of the files that declare them, and declaring the author model's `given:`s, so the downstream still reads the upstream's table through them.

## The four rules that decide reuse

1. **Declare the stored roots before the sources that read them.** Malloy resolves names in file order. A rollup rewired to read a stored root that is declared further down fails to compile with `Reference to undefined object`, and so does everything after it. When you chain an existing package onto its roots, move the roots up.
2. **An extension of a stored source is the same table, not a new one.** `rollup_base is daily_orders extend { ... }` inherits the parent's persist tag and the same content address, so the publisher treats the two names as one build target. To make a distinct non-persisted intermediate, pipe through a query first: `rollup_base is daily_orders -> { select: * } extend { ... }`. (A host that checks persist names for collisions can refuse the bare `extend` outright, since two names now point at one table.)
3. **Every join on the path must land on a stored table.** An intermediate that joins a source nothing materializes - a `join_cross: sp is species_live` where only `species_base` is stored - reaches the warehouse, and the whole downstream is recomputed from the warehouse with the upstream inlined. Point the join at the stored sibling instead.
4. **A chained build runs in the destination's engine, under its memory bound.** The rows are read from the lake and rolled up in DuckDB with the build session's `memory_limit`. A grouped rollup fits; a self-join or a wide hash aggregate over a large fact may not, and fails with DuckDB's `Out of Memory Error` on every attempt. Such a source belongs in the warehouse: keep its input unpersisted so it is built there and only its result is copied in. Placement is a cost decision the build does not make for you.

## What the build does when it cannot reuse

If the downstream cannot be built over the stored tables - it reaches the warehouse through an intermediate (rule 3), or reads a field on the parent that is not a stored column - the publisher **recomputes the upstream from the warehouse**, inlining it, and the build succeeds. Under `strictUpstreams` (what an orchestrating host sets on every build) this is the **one** recompute strict permits, because no build over the stored tables exists for that shape. Strict still refuses what it exists to refuse: a persisted upstream the build neither materialized nor was handed by reference, one whose table lives in a *different* destination, and a source over stored upstreams that the build could not carry (a refinement the ladder below drops). A refusal names the upstream and where it lives.

A chained build re-declares on each parent's binding what the parent's `extend` block adds - its `where:`, dimensions, measures, joins and views - and thins them when they do not compile in the destination, in tiers: a view first, then a join, then a dimension or measure that reads a join the destination cannot bind. **The `where:` is never thinned**: a stored source declared `extend { where: total > 100 }` holds every row and is filtered when read, so a child built over it without that filter would sum the unfiltered rows.

## Reading which happened

Every manifest entry for a source that reads a persisted upstream carries:

- `upstreamReuse: "reused"` - every persisted upstream was read from its table.
- `upstreamReuse: "recomputed"` - at least one was recomputed from its definition; `upstreamRecomputeReason` names which and why (for example, which intermediate reached the warehouse through which join).

It is reported on both tiers, since a table built in the source's own warehouse also inlines a `storage=` upstream it cannot read there. The field is not a location and never names a table. A recomputed source's rows reflect the warehouse at *that build's* time rather than the upstream's snapshot, so the two can disagree when their refresh cadences differ - this field is how to tell.

The `publisher_storage_chained_build_total{outcome}` counter reports the path each chained build took: `parent_reuse`, `inline_fallback` (non-strict recompute), `strict_shape_fallback` (the strict-permitted recompute), `strict_refused`, `infra_failure` (the destination was unreachable or the build died in it - kept distinct so a store outage or an out-of-memory is not read as an un-carriable shape).

## Three limits to know

- A refresh of a chained `refresh="incremental"` source always rebuilds in full (`chained_storage` reason): its parent's delta can restate rows below the child's own frontier where no delta of the child's would revisit them.
- A `#@ persist` without `storage=` (built in the source's warehouse) never substitutes a `storage=` upstream's table, because the warehouse cannot see the lake; it inlines the upstream and reports `recomputed`.
- A chained build carries the intermediates' *text*, written for the source warehouse, into the destination's dialect. A construct the destination lacks fails the carry, and under strict that is a refusal rather than a recompute.
