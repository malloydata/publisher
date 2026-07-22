# Incremental materialization (`refresh="incremental"`)

By default a persisted Malloy source is **fully rebuilt** on every materialization run — the
publisher stages a new table with `CREATE TABLE … AS (<source SQL>)` and atomically renames it into
place. That is the right behavior for most sources, but it is prohibitively expensive when the
source contains a per-row cost that dominates the build: an `ML.GENERATE_TEXT` classification, an
embedding call, an expensive UDF. Reprocessing every row on every refresh burns that cost on rows
that have not changed.

`refresh="incremental"` makes a source **build once, then update in place**: the first run seeds the
whole table, and every subsequent run `MERGE`s only the rows the source produces into the existing
table. Combined with the compile-safe self-reference construct below, the source can filter itself
down to *new* rows before the expensive step runs, so the per-row cost is paid once per row for its
lifetime.

This is the publisher-side realization of the `refresh` knob documented in the control-plane
persistence spec (`ms2data/service/docs/persistence.md`, §3). It is intentionally a smaller design
than the spec's roadmapped Phase E1 (which envisions version-private, zero-copy clones keyed on a
`unique_key` + event-time watermark): this realization merges **in place** on the source's declared
Malloy `primary_key`, keeping the annotation surface minimal. The two can converge later; nothing
here forecloses the fuller design.

## Declaring an incremental source

```malloy
##! experimental.persistence

source: item_classifications_raw is bigquery.sql("""
  WITH __credible_incremental_keys AS (
    -- Empty at compile time and on the first run; the publisher rewrites this
    -- body to select the primary key from the target on incremental runs.
    SELECT CAST(NULL AS STRING) AS item_name WHERE false
  )
  SELECT
    item_name,
    ML.GENERATE_TEXT(...) AS category
  FROM `sales_mix.items`
  WHERE item_name NOT IN (SELECT item_name FROM __credible_incremental_keys)
""")

#@ persist name="credible_derived.item_classifications" refresh="incremental"
source: item_classifications is item_classifications_raw -> { select: * } extend {
  primary_key: item_name
}
```

Two things make this work:

1. **`refresh="incremental"`** on the `#@ persist` annotation selects the incremental build path.
   Unset or `refresh="full"` keeps the default full-rebuild path, byte-for-byte unchanged.
2. **`primary_key`** on the source is the MERGE key. It is required for incremental sources — the
   publisher throws a `BadRequestError` at build time if an incremental source has no primary key.
   No new persist-level `unique_key` knob is introduced; the merge key is the source's existing
   Malloy primary key.

## The compile-safe self-reference construct

The reserved CTE **`__credible_incremental_keys`** is how a source refers to "what I have already
materialized" without a bootstrap problem. This is the dbt `is_incremental()` + `{{ this }}` analog,
realized with no compiler change:

- **At compile time and on the first run**, the author writes the CTE with an empty body (a `WHERE
  false`, a `LIMIT 0` — anything that returns no rows). It compiles because it references no
  not-yet-existent target, and it matches nothing, so the first-run seed processes every row.
- **On an incremental run**, the publisher rewrites the CTE body to `SELECT <primary_key> FROM
  <target>`. Because the filter sits **inside** the source SQL — before the expensive step — the
  expensive step only runs for rows not already present.

The construct is optional. An incremental source that omits the CTE simply MERGEs every row it
returns on every run (an upsert-refresh); the CTE is what turns that into "touch only new rows."

## Build behavior

| Run | What the publisher does |
| --- | --- |
| **First run** (target absent) | `CREATE TABLE <target> AS (<source SQL>)`. The empty self-reference CTE means every row is seeded, and the warehouse infers the exact schema — the publisher never derives DDL from Malloy's intrinsic column types. |
| **Subsequent runs** (target present) | Hydrate the self-reference CTE to the target, then `MERGE INTO <target> USING (<source SQL>) ON <primary_key>`, `WHEN MATCHED THEN UPDATE`, `WHEN NOT MATCHED THEN INSERT`. |

Existence is probed with a cheap `SELECT 1 FROM <target> LIMIT 0`. The MERGE runs **in place** (no
staging table / rename) and is **idempotent under retry**: re-running the same MERGE is a no-op on
rows already merged, unlike a raw `INSERT`, which would double-insert.

Append-only vs upsert is not a separate mode — it falls out of the candidate set. A classification
source whose candidate set (via the CTE) is "items not yet classified" only ever inserts; a dedup
source whose candidate set is "recently changed orders" upserts. Both are the same MERGE.

## Force a full rebuild (`forceFullRebuild`)

After a schema or logic migration you need to rebuild an incremental table from scratch. Trigger a
run with `forceFullRebuild: true` (the dbt `--full-refresh` analog):

```
POST /api/v0/.../packages/{pkg}/materializations   { "forceFullRebuild": true }
```

Every incremental source is then rebuilt through the **full** staging + atomic-rename path this
run instead of MERGE-ing. Its self-reference CTE stays empty, so all rows are reprocessed, and the
clean table is swapped in atomically.

`forceFullRebuild` is deliberately distinct from `forceRefresh`. `forceRefresh` only bypasses
content-hash reuse for full-rebuild sources, and the scheduler sets it on **every** scheduled run —
so overloading it would silently turn every scheduled incremental refresh back into a full rebuild,
defeating the feature. Incremental sources are always (re)built on a scheduled run regardless, and
they stay incremental unless `forceFullRebuild` is explicitly requested.

## Freshness, scope, and validation

- **Freshness fallback must not be `live`.** A live serve recomputes the source, which for an
  incremental definition returns only the delta, not the full dataset. The publish gate rejects an
  incremental source combined with `materialization.freshness.fallback: "live"`; use `"stale_ok"`
  or `"fail"`.
- **Scope.** Incremental needs a physical table that persists across published versions, so use the
  default `scope: "package"` with hosted `freshness` (the recommended cadence surface). A
  version-scoped cron would give each version its own table and defeat the incrementality.
- **`refresh` values.** Only `"full"` and `"incremental"` are accepted; any other value fails the
  publish gate rather than silently degrading to full.

## Dialect support

The generated MERGE is standard ANSI SQL and its `SET` clause uses an **unqualified** target column
(`SET col = S.col`, not `SET T.col = S.col`) for portability:

| Warehouse | Incremental MERGE |
| --- | --- |
| BigQuery | ✅ (requires the unqualified `SET` form — which is what is emitted) |
| Snowflake | ✅ |
| Postgres 15+ | ✅ |
| Postgres < 15 | ❌ — no `MERGE` statement; use full rebuild |
| DuckDB | ❌ — persistence is unsupported for DuckDB generally |

## Backward compatibility

The feature is additive and off by default. A source with `refresh` unset dispatches to the
unchanged full-rebuild path; `getSQL()` is not affected by the annotation, so content-addressing and
reuse-rebind behave identically for full sources. The reuse-skip exemption and the new validation
rules fire only for incremental sources. A publisher running this code against a package that
declares no `refresh` behaves exactly as before.
