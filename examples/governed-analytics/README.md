# governed-analytics — one package, the whole governance story

A tiny Malloy package that shows how **[givens](../../docs/givens.md)** — one runtime-parameter
mechanism — power three things at once:

- **Interactive filter controls** — `REGION` and `MIN_AMOUNT` become inputs in the notebook
  Parameters panel and scope the `sales` source. → [givens.md](../../docs/givens.md)
- **Source authorization** — `#(authorize)` gates *who* may query `orders_secured` (403 otherwise).
  → [authorize.md](../../docs/authorize.md)
- **Row-level access** — a `where:` over the caller's given controls *which rows* they see.
  → [row-level-access.md](../../docs/row-level-access.md)

…plus **discovery curation** — `orders_base` lives in a file not listed in `explores`, so it's hidden
and not directly queryable, while the public models still import it.
→ [discovery-and-access.md](../../docs/discovery-and-access.md)

## Files

| File | Role |
| --- | --- |
| `orders.parquet` | ~4,900 orders over two years across 3 regions × 3 tenants × 3 statuses (no credentials — DuckDB reads it directly). |
| `internal.malloy` | `orders_base`, the shared base source. **Not** in `explores` → hidden + not directly queryable. |
| `orders.malloy` | `REGION` / `MIN_AMOUNT` givens and the `sales` source (interactive controls + `# dashboard`). |
| `secured.malloy` | `ROLE` / `TENANT` givens and `orders_secured` (`#(authorize)` + row-level `where:`). |
| `orders.malloynb` | Notebook over `sales` — renders the Parameters panel and the overview dashboard. |
| `publisher.json` | `explores` + `queryableSources: "declared"` — the discovery/query boundary. |

## Run it

`governed-analytics` ships in Publisher's default config under the `examples`
environment, so with the server running just open the notebook at
<http://localhost:4000/examples/governed-analytics/orders.malloynb>
and change the Parameters panel to see the dashboard re-run.

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
```

> **Security note.** Givens are **caller-asserted** — these gates enforce policy only behind a trusted
> tier that sets `ROLE` / `TENANT` from verified identity. See
> [authorize.md § Security model](../../docs/authorize.md#security-model).

## Learn more

Each file in this package maps to a docs page:

- [givens.md](../../docs/givens.md) — runtime parameters (`REGION`, `MIN_AMOUNT`) and the Parameters panel.
- [authorize.md](../../docs/authorize.md) — `#(authorize)` source gates (who can query).
- [row-level-access.md](../../docs/row-level-access.md) — given-scoped `where:` (which rows a caller sees).
- [discovery-and-access.md](../../docs/discovery-and-access.md) — `explores` / `queryableSources` curation.
