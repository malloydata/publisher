<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

## JSON Files: Read Them In Place Like CSV

```malloy
// RIGHT: .json works like .csv/.parquet
source: reviews is duckdb.table('data/reviews.json')
// RIGHT: newline-delimited JSON is read the same way
source: events is duckdb.table('data/events.ndjson')
// RIGHT: read options need read_json_auto in a SQL source
source: nested is duckdb.sql("""SELECT * FROM read_json_auto('data/reviews.json')""")
// WRONG: shelling out to python, or converting to CSV first
```

DuckDB reads JSON directly, so never preprocess a `.json` file before modeling it and never reach for a scripting language to inspect one. Both a top-level array of objects and newline-delimited JSON work through `duckdb.table()`.

Quirk: JSON carries no schema, so a value written as `"90"` arrives as a string where the same data in CSV would be inferred as a number. Cast it in the source, under a new name (reusing the column's own name is a redefinition error):

```malloy
source: reviews is duckdb.table('data/reviews.json') extend {
  dimension: points_num is points::number
}
```
