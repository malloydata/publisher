# governed-analytics — one package, the whole governance story

A tiny Malloy package that shows how **[givens](../../docs/givens.md)** — one runtime-parameter
mechanism — power three things at once:

- **Interactive filter controls** — `REGION`, `STATUS`, and `MIN_AMOUNT` become a dropdown, a
  multi-select, and a slider, and scope the `sales` source. The same three declarations render in the
  notebook Parameters panel and in the dashboard's control row.
  → [givens.md](../../docs/givens.md)
- **Source authorization** — `#(authorize)` gates *who* may query `orders_secured` (403 otherwise).
  → [authorize.md](../../docs/authorize.md)
- **Row-level access** — a `where:` over the caller's given controls *which rows* they see.
  → [row-level-access.md](../../docs/row-level-access.md)

…plus **discovery curation** — `orders_base` lives in a file not listed in `explores`, so it's hidden
and not directly queryable, while the public models still import it.
→ [discovery-and-access.md](../../docs/discovery-and-access.md)

It is deliberately small. The point is not breadth — [`storefront`](../storefront) is the package that
carries every surface over one dataset — but a governance boundary you can read in a sitting and
prove with a `curl`.

## Files

| File | Role |
| --- | --- |
| `orders.parquet` | ~4,900 orders over two years across 3 regions × 3 tenants × 3 statuses (no credentials — DuckDB reads it directly). |
| `internal.malloy` | `orders_base`, the shared base source. **Not** in `explores` → hidden + not directly queryable. |
| `orders.malloy` | `REGION` / `STATUS` / `MIN_AMOUNT` givens with their control tags, the `sales` source, and the two option-list queries behind the pickers. |
| `secured.malloy` | `ROLE` / `TENANT` givens and `orders_secured` (`#(authorize)` + row-level `where:`). |
| `orders.malloynb` | Notebook over `sales` — renders the Parameters panel and the overview dashboard. |
| `dashboards/governed-overview.malloy` | The same givens as a [dashboard](../../docs/dashboards.md) control row, over the same governed source. |
| `publisher.json` | `explores` + `queryableSources: "declared"` — the discovery/query boundary. |

## Run it

`governed-analytics` ships in Publisher's default config under the `examples`
environment, so with the server running just open the notebook at
<http://localhost:4000/examples/governed-analytics/orders.malloynb>
and change the Parameters panel to see the dashboard re-run. The dashboard is at
<http://localhost:4000/examples/governed-analytics/dashboards/governed-overview>,
with the same three controls in a row above the grid.

### One wrinkle worth knowing

Curation and dashboards interact. Because this package sets `explores` and
`queryableSources: "declared"`, a dashboard file has to be listed in `explores` to be queryable at
all, and it has to `export` both its own query and anything its controls read — an import resolves
at compile time but is not runnable under curation. `dashboards/governed-overview.malloy` ends with
that export list and explains why. A package with no `explores` (like `storefront`) needs none of
this.

### Run it standalone (live editing)

To edit this package and see changes hot-reload, mount it on its own in watch
mode. `--watch-env` symlinks the package so edits to your source dir are picked
up live:

```bash
# From the repo root — mount this package as an environment named "demo"
mkdir -p /tmp/gov-demo
cp -R examples/governed-analytics /tmp/gov-demo/
cat > /tmp/gov-demo/publisher.config.json <<'JSON'
{
  "frozenConfig": false,
  "environments": [
    { "name": "demo",
      "packages": [{ "name": "governed-analytics", "location": "./governed-analytics" }],
      "connections": [] }
  ]
}
JSON

SERVER_ROOT=/tmp/gov-demo bun run packages/server/src/server.ts --watch-env demo
```

That serves it under the `demo` environment instead — swap `examples` for `demo`
in the URLs and API paths below.

## Try each behavior

All queries go to
`POST /api/v0/environments/examples/packages/governed-analytics/models/<model>/query`.

```bash
API=http://localhost:4000/api/v0/environments/examples/packages/governed-analytics/models

# Givens scope the data (empty REGION filter = all regions):
curl -s -X POST $API/orders.malloy/query -H 'content-type: application/json' \
  -d '{"query":"run: sales -> by_region","givens":{"REGION":"us-east"}}'

# Authorize: no identity → 403
curl -s -X POST $API/secured.malloy/query -H 'content-type: application/json' \
  -d '{"query":"run: orders_secured -> by_status"}'                          # → 403

# Authorize + row-level: an admin sees all tenants…
curl -s -X POST $API/secured.malloy/query -H 'content-type: application/json' \
  -d '{"query":"run: orders_secured -> by_tenant","givens":{"ROLE":"admin"}}'   # → 3 tenants

# …a tenant caller sees only its own rows.
curl -s -X POST $API/secured.malloy/query -H 'content-type: application/json' \
  -d '{"query":"run: orders_secured -> by_tenant","givens":{"TENANT":"acme"}}'  # → 1 tenant

# Discovery: orders_base is hidden and not a valid query target → 404
curl -s -X POST $API/internal.malloy/query -H 'content-type: application/json' \
  -d '{"query":"run: orders_base -> { aggregate: c is count() }"}'          # → 404

# The dashboard runs the same givens, and its control option lists are ordinary queries:
curl -s -X POST $API/dashboards/governed-overview.malloy/query \
  -H 'content-type: application/json' \
  -d '{"queryName":"governed_overview","givens":{"REGION":"emea","MIN_AMOUNT":500}}'
curl -s -X POST $API/dashboards/governed-overview.malloy/query \
  -H 'content-type: application/json' \
  -d '{"queryName":"region_suggest","compactJson":true}'   # → emea, us-east, us-west
```

> **Security note.** Givens are **caller-asserted** — these gates enforce policy only behind a trusted
> tier that sets `ROLE` / `TENANT` from verified identity. See
> [authorize.md § Security model](../../docs/authorize.md#security-model).

## Learn more

Each file in this package maps to a docs page:

- [givens.md](../../docs/givens.md) — runtime parameters (`REGION`, `STATUS`, `MIN_AMOUNT`), their control tags, and the Parameters panel.
- [dashboards.md](../../docs/dashboards.md) — how `dashboards/governed-overview.malloy` works, including the curation wrinkle above.
- [authorize.md](../../docs/authorize.md) — `#(authorize)` source gates (who can query).
- [row-level-access.md](../../docs/row-level-access.md) — given-scoped `where:` (which rows a caller sees).
- [discovery-and-access.md](../../docs/discovery-and-access.md) — `explores` / `queryableSources` curation.
