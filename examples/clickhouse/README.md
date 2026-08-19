# clickhouse

A Malloy model over a remote **ClickHouse** server, reached through DuckDB's HTTP reader.

Unlike the other bundled examples, this one needs a running ClickHouse, so it is **not** in the
default `examples` environment — you add it to your config after starting the server below.

See [docs/clickhouse.md](../../docs/clickhouse.md) for the full reference.

## 1. Start ClickHouse

```bash
docker compose -f examples/clickhouse/docker-compose.yml up -d
```

That seeds `demo.orders` (50,000 rows) from [seed.sql](seed.sql) and exposes ClickHouse's HTTP
interface on port 8123. Wait for it to answer:

```bash
curl -s http://localhost:8123/ping   # -> Ok.
```

## 2. Declare the server and the package

Add both to your `publisher.config.json`:

```json
{
  "environments": [
    {
      "name": "examples",
      "packages": [{ "name": "clickhouse", "location": "../../examples/clickhouse" }],
      "connections": [
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
      ]
    }
  ]
}
```

## 3. Run Publisher

Connections are read from the config on a re-initializing boot, so pass `--init` the first time:

```bash
CLICKHOUSE_PASSWORD=malloy bun run build && CLICKHOUSE_PASSWORD=malloy bun run start:init
```

Open http://localhost:4000 and pick the `clickhouse` package, or query it directly:

```bash
curl -s -X POST \
  http://localhost:4000/api/v0/environments/examples/packages/clickhouse/models/clickhouse.malloy/query \
  -H 'Content-Type: application/json' \
  -d '{"sourceName":"orders","queryName":"revenue_by_region","compactJson":true}'
```

## What's in the model

[clickhouse.malloy](clickhouse.malloy) has two sources, showing both shapes:

| Source       | Shape                                                                                       |
| ------------ | ------------------------------------------------------------------------------------------- |
| `orders`     | Pre-aggregated in ClickHouse to region/day/status. Malloy sums the partials. **Do this.**   |
| `order_rows` | Row grain, date-filtered in ClickHouse. Fine at this size; the shape to avoid on big tables. |

The rule to remember: **nothing is pushed down.** Whatever SQL sits inside `demo('…')` is what
ClickHouse runs, and Malloy aggregates whatever came back. Filter and aggregate inside the string.

## Tear down

```bash
docker compose -f examples/clickhouse/docker-compose.yml down -v
```
