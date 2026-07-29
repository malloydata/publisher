---
name: malloy-materialization-design
description: Decide WHERE to put #@ persist tags and how to shape the sources around them, so materialized tables are fast AND cannot return wrong answers. Use when adding persist to a real model, splitting a fact into hot/fallback tables, repointing semantic sources onto materialized tables, or debugging a materialized source that returns empty or is unexpectedly slow. Complements malloy-materialization (tag syntax + build debugging) and malloy-materialization-inventory (measure before you build).
---

# Materialization design

`skill:malloy-materialization` tells you how to write a `#@ persist` tag and why a build didn't run.
`skill:malloy-materialization-inventory` tells you how to measure before you build. This skill is the part in
between: **where the tag goes, what shape the sources around it must take, and which mistakes
return wrong answers instead of slow ones.**

> **The one rule that matters most:** a narrow table can only back a source when the source
> *statically pins* everything the table narrows on. If the **caller** can vary a filter the table
> doesn't fully cover, the query returns **empty - not slow**. That is a silent wrong answer, and
> it is the only failure mode here that a parity check won't catch.

## The safety rule, stated precisely

Before pointing any source at a materialized table, check both halves:

> Every filter the source **statically pins** must be present in the table, **and** every filter the
> **caller can vary** must be *fully represented* in the table for those pinned values.

Work it as a table. For each source: what does it pin in its own definition, and what does it leave
to the caller?

| Source pins statically | Caller varies | Narrow table must contain |
|---|---|---|
| `dimension_set_id = 17` | audience, time slice, window, medium | **all** audiences / slices / windows / mediums, at ds 17 |
| ds 17 + time slice 1 + model 4 | audience, conversion group | all audiences + CGs at that exact pin |
| *nothing* - caller picks the dim set | dim set, plus everything else | you cannot narrow on dim set at all |

The third row is the common trap. A source whose discriminator the caller chooses at query time
**cannot** be repointed onto a row subset, because table names resolve at compile time and filter
values do not exist then. Leave those on the full table and let them be slower. That is a correct
outcome, not a failure.

**Test the rule, don't reason about it.** For every repointed source, run a query that varies a
caller-controlled filter toward a value you suspect might be missing, and assert it returns **rows**:

```malloy
// If am_hot were narrowed to time slice 1, this would return EMPTY, not slow.
run: attribution_cross_screen -> {
  where: time_slice_key_id = 4      // caller-varied, deliberately not the common case
  group_by: attribution_model_id
  aggregate: n is count()
}
```

Sizing follows from the rule, not from ambition. If the rule says a table must carry all 7 time
slices and all 6 attribution models, then it is as big as that makes it - 126M rows instead of 9M - 
and that size is a **correctness requirement**, not a failure to optimize.

### `compose()` does not rescue this

Malloy composite sources pick the first source defining **all fields the query references**. That is
field-presence routing, not filter-satisfiability routing. A narrow row-subset table still carries
the discriminator column, so a query pinning an absent value routes to the narrow table and returns
empty. `compose()` is safe when the narrow source has *fewer fields over the same rows*; it is
unsafe for row subsets. Check which one you have before reaching for it.

## Where the tag goes

### Persist the fact; read it through `->`

`#@ persist` is **inherited through `extend`**. A source defined as `is <persisted> extend { … }`
becomes a second writer of the same table, and the build is rejected at pre-flight - typically in
milliseconds, with **zero tables written**, which reads exactly like "materialization silently
didn't happen."

```malloy
// ✅ the shape that works
#@ persist name="scratch.my_fact"
source: _my_fact is _raw -> { where: <scope>  select: * }

source: my_base is _my_fact -> { select: * }      // ← query, so NO tag inherited
  include { internal: * }
  extend { /* dimensions, measures, #(doc) */ }

source: my_public is my_base extend { where: … }  // ✅ safe: my_base carries no tag
```

```malloy
// ❌ collides as soon as anything extends it
#@ persist name="scratch.my_fact"
source: my_base is _raw -> { … } extend { /* dims */ }
source: my_public is my_base extend { … }   // ← inherits the tag → build rejected
```

A single source that *itself* uses `-> { } extend { }` is fine - the hazard is a **separate** source
extending a tagged one. If you have such a source, add a comment saying so, because the next person
to extend it will break the whole package build.

**Practical structuring rule:** put persisted tables and semantic bases in *different files*. The
file boundary then forces the `->` split, and you get the same discipline for free. In one real
package every chain was safe except the single file where the tag and the semantic base were fused - 
and that was the one place that broke.

### Define the extraction once, persist several queries over it

Don't copy a 100-column extraction list to make a second table. Put it in one non-persisted query
source and let each persisted table be a query over that:

```malloy
source: _extracted is _raw -> { where: <scope>  select: /* the long list, once */ }

#@ persist name="scratch.hot"
source: hot is _extracted -> { where: <narrowing>  select: * }

#@ persist name="scratch.full"
source: full is _extracted -> { select: * }
```

This also avoids persist-over-persist, which is untested territory in most deployments - prefer
deriving both from the shared non-persisted source rather than deriving one table from another.

### Denormalize the joins into the table

**The most common performance mistake in a materialized model**: persisting the fact but leaving its
metadata joins live in the semantic layer above it.

If the key callers filter on (`report_id`, `tenant_id`, …) lives on a *joined* source rather than as
a column on the persisted table, then filtering requires joining the lookup **first**, and the
predicate cannot push down into the fact scan. A 0.14 GB table can then behave like a huge one - 
measured at 5.3s where comparable tiles were 300–700ms.

Snapshot the join values as columns at build time:

```malloy
source: _raw_joined is conn.table('…') extend {
  join_one: report_ref on REPORT_ID = report_ref.internal_report_id
}

#@ persist name="scratch.fact"
source: fact is _raw_joined -> {
  select:
    *
    REPORT_UUID is report_ref.report_id      // ← the column callers filter on
    AUD_NAME is audience_ref.aud_name
}
```

Then the semantic layer reads columns, with no joins at all. Apply this consistently: if one table in
a package denormalizes and another doesn't, the one that doesn't will be mysteriously slow.

### Naming

- **Distinct target per persisted source.** A duplicate name is the same pre-flight rejection as the
  inheritance bug.
- **New name when the definition changes.** Persist output is content-addressed, so changing logic
  builds fresh - but reusing a coordinate that is already serving can hit generation-binding bugs
  where the serving pointer references a GC'd generation. A new name sidesteps it.
- Unchanged logic **reuses** the existing table, so republishing an app-only change costs nothing.

## When one field-definition block must back two tables

If a hot table and a full table both need the same dimensions and measures, you will want one base
over two backings. **Malloy cannot do this**: source parameters take values not sources, and
`virtual_source` swaps tables per *runtime* (dev vs prod), not per source within a model.

So the body gets duplicated. Make it a checked invariant rather than a maintenance hazard:

1. Author one base file.
2. **Generate** the twin with a one-line substitution (source name + backing table).
3. Put the regeneration command in the generated file's header, marked DO NOT EDIT.
4. Add a `diff` of the two bodies to your verification steps; anything beyond the substituted line
   is drift.

Surface this cost to whoever owns the model - it is a real tax and they may prefer to leave the
hot path slow rather than pay it.

## Verifying - parity is not enough

**Correct numbers prove nothing about materialization.** When a table is absent or stale, a
`fallback: "live"` / `"stale_ok"` policy silently computes live and returns *the right answer*. Every
verification below is necessary because of that.

| Question | How to answer it |
|---|---|
| Do the numbers match? | Compare against known-good figures per repointed source |
| Do the **tables exist**? | Query the warehouse catalog (`INFORMATION_SCHEMA.TABLES` is a view, so `conn.table()` reaches it) for row counts, bytes and `LAST_ALTERED` |
| Did **this** build write them? | `LAST_ALTERED` vs publish time. Beware: content-addressed reuse means an unchanged table may predate this build |
| Is a missing table being hidden? | Publish once with `fallback: "fail"` as a throwaway, then revert |
| Does the safety rule hold? | Caller-varied filter queries must return rows, not empty |
| Is it actually faster? | Per-query timings from your serving layer's request log - see below |

**Confirm the build finished before judging speed.** Materialization is async and can take
much longer than the publish call. Check `LAST_ALTERED` on every target; a load taken before the
last table lands is measuring the live fallback, not your design.

**Getting real timings when the warehouse won't tell you.** If `conn.sql()` fails (proxy
connections can't create the temp table SQL sources need) and `ACCOUNT_USAGE` isn't authorized,
warehouse-side attribution is closed off. Don't stop there - the *serving layer* usually logs
per-request durations with the query text. In one case the router's request log gave exact
per-query milliseconds for every dashboard tile, which is what actually located the two slow
queries. Look for the request log before concluding timings are unavailable.

## Where the remaining time goes (it's often not the model)

Once the tables are right, per-query time can be fine while the page is still slow. Check:

- **Serial awaits before the first real query.** Filter bars, pickers and headers awaited in
  sequence can cost more than every tile combined.
- **A support query on the biggest table.** A dropdown populated from the largest materialized table
  is a common and easily-missed cost. Point it at the smallest table that carries those values.
- **Data already in memory being re-fetched.** A header query whose fields were already returned by
  the list query is a pure round-trip.
- **Duplicate concurrent queries.** Two views of the same result should coalesce into one request.

## Checklist

- [ ] Measured first (`skill:malloy-materialization-inventory`) - sizes and grain come from data, not intuition
- [ ] Safety rule applied per source; caller-varied filters tested for rows, not just parity
- [ ] Every `#@ persist` on a source nothing `extend`s; `->` boundary below the semantic layer
- [ ] Persisted tables and semantic bases in separate files
- [ ] Extraction defined once; multiple tables as queries over it
- [ ] Join values denormalized into every persisted table, consistently
- [ ] Distinct target names; new name where logic changed
- [ ] `##! experimental { … persistence … }` on **every** file in the package
- [ ] Tables confirmed to exist, with `LAST_ALTERED` after the publish
- [ ] Timings taken after the build completed, from the serving request log
