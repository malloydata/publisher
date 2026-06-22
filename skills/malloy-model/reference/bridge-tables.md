# Bridge Tables & Composite Keys

## Bridge Table Pattern (Many-to-Many)

Bridge table gets `join_one` to each source; other sources get `join_many` to bridge:

```malloy
// Bridge source
source: enrollments is conn.table('enrollments') extend {
  join_one: students with student_id   // each enrollment → one student
  join_one: courses with course_id     // each enrollment → one course
}
// Query source: students with their enrollments
source: student_courses is students extend {
  join_many: enrollments on student_id = enrollments.student_id
}
```

## Composite Key Joins

When no single column is unique (bridge tables, time-series snapshots), use `on` with `and`:

```malloy
// `with` is single-column only, composite keys MUST use `on` + `and`
join_one: items on order_id = items.order_id and product_id = items.product_id
```

`primary_key` is single-column only, pick the highest-cardinality column.

## Cardinality Verification

Before writing any join, check FK uniqueness with `malloy_executeQuery`:

```malloy
run: target_table -> { group_by: fk_col, aggregate: n is count(), having: n > 1, limit: 5 }
```

- 0 results = unique → `join_one` (more efficient)
- Any results = not unique → `join_many` (always safe)

For composite keys, test multi-column: `group_by: col_a, col_b`, same pattern.

## Post-Join Verification

After writing joins, verify row counts haven't inflated:

```malloy
run: source -> { aggregate: row_count is count() }
```

If higher than the raw table, a `join_one` target key isn't unique, switch to `join_many`.
