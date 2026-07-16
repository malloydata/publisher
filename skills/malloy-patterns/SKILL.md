---
name: malloy-patterns
description: Index of Malloy documentation topics. Use to discover what's available in search_malloy_docs. Covers language reference (sources, queries, views, fields, aggregates, joins, filters, expressions, functions), common patterns (YoY, cohorts, percent of total), rendering, and experimental features.
---

# Malloy Documentation Topics

Call `search_malloy_docs` with these topics.

> **Tool names** are written bare here — `get_context`, `execute_query`, `search_malloy_docs`. The exact prefixed name depends on the host surface; match each against the tools you actually have.

## Language Reference

| Topic | Search for... |
|-------|---------------|
| Models | `"models"` or `"statement"` |
| Sources | `"sources"` or `"source definition"` |
| Queries | `"queries"` |
| Views | `"views"` |
| Data Types | `"data types"` |
| Fields | `"fields"` |
| Aggregates | `"aggregates"` |
| Expressions | `"expressions"` |
| Functions | `"functions"` |
| Filters | `"filters"` or `"where"` |
| Calculations (window functions) | `"calculations"` or `"window functions"` |
| Ordering and Limiting | `"order_by"` or `"limit"` |
| Joins | `"joins"` or `"join_one"` or `"join_many"` |
| Nested Views | `"nesting"` or `"nest"` |
| SQL Sources | `"sql sources"` |
| Imports | `"imports"` |
| Connections | `"connections"` |
| Tags | `"tags"` or `"#(doc)"` |

## Common Patterns

| Pattern | Search for... |
|---------|---------------|
| Comparing Timeframes (YoY) | `"comparing timeframes"` or `"year over year"` |
| Foreign Sums and Averages | `"foreign sums"` |
| Reading Nested Data | `"reading nested"` |
| Percent of Total | `"percent of total"` |
| Cohort Analysis | `"cohort analysis"` |
| Nested Subtotals | `"nested subtotals"` |
| Bucketing with 'Other' | `"bucketing other"` |
| Auto-binning Histograms | `"autobin"` or `"histogram"` |
| Moving Average | `"moving average"` |
| Transform Data | `"transform"` |
| Sessionize - Map/Reduce | `"sessionize"` |
| Dimensional Indexes | `"dimensional indexes"` |

## Rendering & Visualization

| Topic | Search for... |
|-------|---------------|
| Overview | `"rendering"` |
| Number/Currency Scaling | `"number formatting"` or `"currency formatting"` |
| Big Value / KPI Cards | `"big_value"` or `"big value"` |
| Bar Charts | `"bar charts"` or `"# bar_chart"` |
| Line Charts | `"line charts"` or `"# line_chart"` |
| Scatter Charts | `"scatter charts"` |
| Dashboards | `"dashboards"` |
| Transposed Tables | `"transpose"` |
| Pivoted Tables | `"pivots"` |
| Shape Maps | `"shape maps"` |

## Database Dialects

These search terms find docs on dialect-specific SQL *functions* available inside expressions. Malloy models themselves are dialect-portable — the connection supplies the dialect, so you don't write "BigQuery-specific" (or any vendor-specific) Malloy.

| Dialect | Search for... |
|---------|---------------|
| BigQuery | `"bigquery"` |
| DuckDB | `"duckdb"` |
| PostgreSQL | `"postgres"` |
| Snowflake | `"snowflake"` |
| Presto/Trino | `"presto"` or `"trino"` |
| MySQL | `"mysql"` |

## Experimental Features

| Feature | Search for... |
|---------|---------------|
| Join INNER/RIGHT/FULL | `"inner join"` or `"full join"` |
| SQL Expressions | `"sql expressions"` or `"sql_number"` |
| Window Partitions | `"window partitions"` |
| Parameters | `"parameters"` |
| Composite "Cube" Sources | `"composite sources"` |
| Access Modifiers | `"access modifiers"` or `"public"` or `"private"` |

## Multi-File & Computed Sources

| Topic | Search for... |
|-------|---------------|
| Imports | `"imports"` |
| Computed sources (from queries) | `"from"` or `"sql sources"` |
| Access Modifiers (include/public/private) | `"access modifiers"` or `"public"` or `"private"` |

## Other Topics

| Topic | Search for... |
|-------|---------------|
| Notebooks | `"notebooks"` |
| MalloySQL | `"malloysql"` |
| Python Package | `"python"` |
| Jupyter | `"jupyter"` |
| Publishing | `"publishing"` |
| REST API | `"rest api"` |
| MCP for AI Agents | `"mcp agents"` |
| Troubleshooting | `"troubleshooting"` |

## Example

```
Call: search_malloy_docs
Parameters: { question: "How do I use window functions in Malloy?" }
```
