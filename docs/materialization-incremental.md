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
down to _new_ rows before the expensive step runs, so the per-row cost is paid once per row for its
lifetime.

This is a deliberately small, publisher-side realization of the `refresh` knob: it merges **in
place** on the source's declared Malloy `primary_key`, which keeps the annotation surface minimal
and requires no compiler change. A richer strategy (a separate `unique_key`, an event-time
watermark predicate, or building into a cloned generation rather than in place) is possible later
and nothing here forecloses it.

## Declaring an incremental source

```malloy
##! experimental.persistence

source: item_classifications_raw is bigquery.sql("""
  WITH __malloy_incremental_keys AS (
    -- Empty at compile time and on the first run; the publisher rewrites this
    -- body to select the primary key from the target on incremental runs.
    SELECT CAST(NULL AS STRING) AS item_name WHERE false
  )
  SELECT
    item_name,
    ML.GENERATE_TEXT(...) AS category
  FROM `raw.items` s
  WHERE NOT EXISTS (
    SELECT 1 FROM __malloy_incremental_keys k WHERE k.item_name = s.item_name
  )
""")

#@ persist name="analytics.item_classifications" refresh="incremental"
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

The reserved CTE **`__malloy_incremental_keys`** is how a source refers to "what I have already
materialized" without a bootstrap problem. This is the dbt `is_incremental()` + `{{ this }}` analog,
realized with no compiler change:

- **At compile time and on the first run**, the author writes the CTE with an empty body (a `WHERE
false`, a `LIMIT 0` — anything that returns no rows). It compiles because it references no
  not-yet-existent target, and it matches nothing, so the first-run seed processes every row.
- **On an incremental run**, the publisher rewrites the CTE body to `SELECT <primary_key> FROM
<target>`. Because the filter sits **inside** the source SQL — before the expensive step — the
  expensive step only runs for rows not already present.

Prefer a `NOT EXISTS` anti-join (as above) over `WHERE key NOT IN (SELECT key FROM
__malloy_incremental_keys)`: `NOT IN` returns no rows if the subquery ever yields a `NULL` (SQL
three-valued logic), which would silently process nothing. Primary keys are non-null so `NOT IN`
happens to be safe here, but `NOT EXISTS` is robust regardless.

Write the CTE in the canonical `WITH __malloy_incremental_keys AS ( ... )` form (the `AS` keyword may
be any case; a column-list form is not recognized). The construct is optional: an incremental source
that omits the CTE simply MERGEs every row it returns on every run (an upsert-refresh); the CTE is
what turns that into "touch only new rows."

## Build behavior

| Run                                  | What the publisher does                                                                                                                                                                                              |
| ------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **First run** (target absent)        | `CREATE TABLE <target> AS (<source SQL>)`. The empty self-reference CTE means every row is seeded, and the warehouse infers the exact schema — the publisher never derives DDL from Malloy's intrinsic column types. |
| **Subsequent runs** (target present) | Hydrate the self-reference CTE to the target, then `MERGE INTO <target> USING (<source SQL>) ON <primary_key>`, `WHEN MATCHED THEN UPDATE`, `WHEN NOT MATCHED THEN INSERT`.                                          |

The publisher decides seed-vs-MERGE, and derives the MERGE column set, from a single schema fetch
against the target table (the connection's dialect-aware introspection). Because the **target table
is the authority on its own columns**, the MERGE covers exactly what the first-run CTAS created —
including non-atomic (struct / array / record) columns and any name normalization the warehouse
applied. (A Malloy-side column derivation would drop non-atomic columns, silently diverging from the
seeded table.) The declared `primary_key` is resolved to the target's actual column spelling, so the
MERGE's key and column list are all in one casing. "No schema returned" is the first-run signal; a
transient schema-fetch error against a table that already exists therefore surfaces as a spurious
"table already exists" failure from the seed CTAS — a failed run, never data loss or divergence. The
MERGE runs **in place** (no staging table / rename) and is **idempotent under retry**: re-running the
same MERGE is a no-op on rows already merged, unlike a raw `INSERT`, which would double-insert.

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
- **Stable target name (assumption).** "Seed vs MERGE" is decided by whether the target table
  already exists, so the source's assigned physical table name must be **stable across runs**. The
  publisher's self-assigned name (standalone / scheduled builds) is stable. If an orchestrating
  layer supplies the physical name instead, it must assign a stable name per incremental source — a
  name that changes per version or per content hash makes every run miss the existing table and
  re-seed (a silent full rebuild).
- **`refresh` values.** Only `"full"` and `"incremental"` are accepted; any other value fails the
  publish gate rather than silently degrading to full.

## Dialect support

Incremental emits a `MERGE INTO ...`, so it is **gated at publish** to MERGE-capable dialects:
`refresh="incremental"` on any other dialect is rejected (rather than failing late with a raw
warehouse syntax error). The generated MERGE is standard ANSI SQL and its `SET` clause uses an
**unqualified** target column (`SET col = S.col`, not `SET T.col = S.col`) for portability.

| Warehouse                                   | Incremental MERGE                                                                                             |
| ------------------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| BigQuery                                    | ✅ allowed (requires the unqualified `SET` form — which is what is emitted)                                   |
| Snowflake                                   | ✅ allowed — see caveat below                                                                                 |
| Postgres 15+                                | ✅ allowed                                                                                                    |
| Postgres < 15                               | ⚠️ not gated (the major version isn't visible from the dialect name) but has no `MERGE`; use `refresh="full"` |
| MySQL / Trino / Databricks / DuckDB / other | ❌ rejected at publish — use `refresh="full"`                                                                 |

> **Not yet warehouse-verified.** The generated SQL is unit-tested for shape but not yet executed
> against a live engine. In particular the MERGE wraps a CTE-bearing source as `USING (WITH … SELECT
…)`; BigQuery and Postgres accept a parenthesized leading `WITH`, but Snowflake acceptance should
> be confirmed on a real connection before relying on it. (DuckDB does have `MERGE INTO` in recent
> versions, but persistence is unsupported for DuckDB generally, so it is rejected here regardless.)

## Backward compatibility

The feature is additive and off by default. A source with `refresh` unset dispatches to the
unchanged full-rebuild path; `getSQL()` is not affected by the annotation, so content-addressing and
reuse-rebind behave identically for full sources. The reuse-skip exemption and the new validation
rules fire only for incremental sources. A publisher running this code against a package that
declares no `refresh` behaves exactly as before.
