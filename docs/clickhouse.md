# ClickHouse

> What this is: how to query a remote ClickHouse server from a Malloy model. For connections in
> general, see [connections.md](connections.md); for the config file, see
> [configuration.md](configuration.md).

Publisher reads ClickHouse through DuckDB. You declare the server once under a DuckDB connection,
and Publisher registers a **table macro** named after it. A model calls that macro with a ClickHouse
SQL string and gets the result set back as a table it can build a source on.

There is no ClickHouse driver and no Malloy ClickHouse dialect involved. ClickHouse's HTTP interface
can return Parquet, and DuckDB can read Parquet over HTTP — that is the whole mechanism.

## Set it up

Declare the server on an environment-level DuckDB connection in `publisher.config.json`:

```json
{
  "name": "clickhouse",
  "type": "duckdb",
  "duckdbConnection": {
    "clickhouseServers": [
      {
        "name": "demo",
        "host": "localhost",
        "port": 8123,
        "database": "demo",
        "user": "malloy",
        "password": "${CLICKHOUSE_PASSWORD}"
      }
    ]
  }
}
```

`port` defaults to `8123` (ClickHouse's HTTP port, **not** the native 9000 port). Set
`"useSsl": true` for https. `password` goes through the usual `${ENV_VAR}` substitution, so the
secret stays out of the config file — see [configuration.md](configuration.md).

Then write a model against it. The connection name (`clickhouse`) picks the connection; the macro
name (`demo`) picks the server:

```malloy
source: orders is clickhouse.sql("""
  SELECT * FROM demo('
    SELECT region, order_date, count() AS order_count, sum(amount) AS revenue
    FROM demo.orders
    GROUP BY region, order_date
  ')
""") extend {
  measure:
    total_orders is order_count.sum()
    total_revenue is revenue.sum()

  view: by_region is {
    group_by: region
    aggregate: total_orders, total_revenue
  }
}
```

A runnable version is in [examples/clickhouse](../examples/clickhouse), with a
`docker-compose.yml` that starts a seeded ClickHouse.

## Aggregate on the ClickHouse side

**Nothing is pushed down.** Whatever SQL you put in the macro string is exactly what ClickHouse runs.
Malloy's own filters, `group_by`, and measures are applied by DuckDB to the rows the macro already
returned.

So this is the shape to avoid:

```malloy
// Every query re-extracts the whole table over HTTP.
source: orders is clickhouse.sql("""SELECT * FROM demo('SELECT * FROM demo.orders')""")
```

and this is the shape that works — aggregate or filter inside the string, and let Malloy roll up the
partials:

```malloy
source: orders is clickhouse.sql("""
  SELECT * FROM demo('
    SELECT region, order_date, count() AS order_count, sum(amount) AS revenue
    FROM demo.orders GROUP BY region, order_date
  ')
""")
```

On a 5M-row table, the difference measured 1961 ms against 156 ms, and 1.63 MiB across the wire
against 3 KiB — same answers. The gap grows with the table.

Treat the macro as a **parameterized extract**, not as a pushdown connector. Good sources are a
pre-aggregated rollup, a date-bounded slice, or a small dimension table.

## What Publisher handles

- **URL encoding.** The query string is `url_encode`d, so a `+`, `&`, or `#` in your SQL survives.
- **Authentication.** Credentials become an HTTP Basic header stored in a DuckDB secret scoped to the
  server's base URL. They are deliberately kept out of the query URL (ClickHouse logs URLs in
  `system.query_log`) and out of the macro body (any model author can read that via
  `duckdb_functions()`).
- **`httpfs`.** Loaded automatically. It is baked into the Publisher image, so this works under
  `EXTENSION_FETCH_POLICY=local-only`.
- **Name validation.** A macro name must be a SQL identifier and must not be a reserved word —
  `"name": "primary"` is rejected at startup with a suggested fix, rather than registering a macro
  no model can call.

## Types

ClickHouse types arrive through Parquet, so the mapping is Parquet's:

| ClickHouse                | DuckDB / Malloy            |
| ------------------------- | -------------------------- |
| `Date`                    | `DATE`                     |
| `DateTime`                | `TIMESTAMP WITH TIME ZONE` |
| `Decimal(p, s)`           | `DECIMAL(p, s)`            |
| `Enum8` / `Enum16`        | `VARCHAR`                  |
| `LowCardinality(String)`  | `VARCHAR`                  |
| `Nullable(T)`             | nullable `T`               |
| `UInt8` … `UInt64`        | `UTINYINT` … `UBIGINT`     |

`DateTime` is stored UTC and comes back as a timezone-aware timestamp, so it renders in the server's
local zone. That is invisible at date grain and visible at hour grain — if you group by hour, convert
explicitly in the ClickHouse query (`toTimeZone(created_at, 'UTC')`) rather than relying on the
default.

## Notes

- **The macro argument is trusted input.** It is your model's SQL, sent verbatim. Publisher does not
  parse or rewrite it. Give the ClickHouse user read-only grants on just the tables you intend to
  expose.
- **The `chsql` community extension is not used and not needed.** Its `ch_scan` is a SQL macro over
  `read_parquet` — the same idea — but it has no build for the DuckDB version Malloy embeds
  (community builds stop at DuckDB 1.3.2), and its macro neither URL-encodes the query nor supports
  password authentication.
- **Writes are not supported.** This is a read path.
