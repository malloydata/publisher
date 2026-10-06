<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

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

`primary_key` takes one column, and that column must be unique. When no single column is unique, add a dimension that joins the key columns and use that:

```malloy
dimension: row_key is concat(acct::string, '-', mon::string)
primary_key: row_key
```

Check the new key with the cardinality query below (`group_by: row_key`). If you cannot make a unique key, leave `primary_key` off and say in the source `#(doc)` what one row is. Never pick the column with the most distinct values: if it repeats, `count()` on the source comes back low and nothing reports an error. The grain (what one row is) is a decision for the user, so state it and ask them to confirm it.

## Cardinality Verification

Before writing any join, check FK uniqueness with `execute_query`:

```malloy
run: target_table -> { group_by: fk_col, aggregate: n is count(), having: n > 1, limit: 5 }
```

- 0 results = unique → `join_one` (more efficient)
- Any results = not unique → `join_many` (always safe)

For composite keys, test multi-column: `group_by: col_a, col_b`, same pattern.

## Post-Join Verification

A plain `count()` on the base source cannot show a bad join: Malloy leaves the join out of the SQL until the query uses a joined field, so the count equals the raw table even when a `join_one` target key repeats. Once the query uses the join (a `group_by` or `count(joined.field)`), rows do multiply: with 3 orders and one repeated target key, `count()` is 4 against 3 and a revenue sum is 340 against a true 240. Run the cardinality query above on the target before you write the join. After the join, group by one field of the joined table and check that the groups add up to the ungrouped total:

```malloy
// revenue stands for any additive measure on the source
run: source -> { group_by: customer_name is joined.name, aggregate: revenue }  // add the revenue column up
run: source -> { aggregate: revenue }                                          // must equal that sum
```

If the grouped total is higher, the target key is not unique. Make it `join_many`, or fix the key.
