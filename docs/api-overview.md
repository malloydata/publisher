# API overview

> What this is: the shape of Publisher's programmatic surfaces — the resource hierarchy, the REST and
> MCP APIs, and where to find the live, interactive API explorer. For connecting an AI agent, see
> [ai-agents.md](ai-agents.md); for the App, see [publisher-app.md](publisher-app.md).

## Two surfaces

| Surface | Port | For |
| --- | --- | --- |
| **REST API** | `4000` (base path `/api/v0`) | Applications, dashboards, scripts, and unattended agents: list content, compile models, run queries. See the [REST loop](ai-agents.md#unattended-and-one-shot-agents-the-rest-loop). |
| **MCP API** | `4040` (`/mcp`) | AI agents in interactive sessions: discovery, query, and authoring over the [Model Context Protocol](https://modelcontextprotocol.io), via the five `malloy_*` tools. See [ai-agents.md](ai-agents.md). |

Both are read-through onto the same resource hierarchy. Neither surface authenticates callers —
put the server behind your own gateway before exposing it beyond localhost.

## Resource hierarchy

```
/api/v0
└── /environments/{env}
    ├── /packages/{pkg}
    │   ├── /models/{path}              a .malloy model
    │   │   ├── /query                  POST — run a Malloy query
    │   │   └── /compile                POST — compile to SQL / metadata
    │   ├── /notebooks/{path}           a .malloynb notebook
    │   │   └── /cells/{index}          GET — run one notebook cell
    │   ├── /pages                      in-package HTML data apps
    │   ├── /events                     GET, the live-reload SSE stream (held open)
    │   ├── /databases                  the package's embedded data files (e.g. parquet)
    │   └── /materializations           persisted-source builds
    └── /connections/{name}             database connections
```

(`/projects/{env}/…` is accepted as an alias for `/environments/{env}/…`.)

## Key endpoints

| Method & path | Does |
| --- | --- |
| `GET /api/v0/status` | Server lifecycle (`operationalState`), plus `loadErrors` for anything configured that did not load. |
| `GET /api/v0/environments` | List environments, each with its packages. |
| `GET /api/v0/environments/{env}/packages/{pkg}` | Package metadata (models, `explores`, `buildPlan`, …). Add `?reload=true` to recompile the package from disk first, the REST form of `malloy_reloadPackage`. |
| `POST /api/v0/environments/{env}/packages` | Register a package at runtime; body `{ "name": "…", "location": "…" }` ([packages.md](packages.md)). |
| `GET  …/packages/{pkg}/models/{path}` | A model's compiled metadata (sources, views, givens). |
| `POST …/packages/{pkg}/models/{path}/query` | Run a Malloy query; see [request shapes](#query-request-shapes) below. |
| `POST …/packages/{pkg}/models/{path}/compile` | Compile Malloy to SQL / metadata. |
| `GET  …/packages/{pkg}/notebooks/{path}/cells/{index}` | Run one notebook cell. |
| `GET  …/packages/{pkg}/pages` | List a package's HTML pages. |
| `GET  …/packages/{pkg}/events` | Live-reload SSE stream ([html-data-apps.md](html-data-apps.md#live-reload)). Held open by design. |
| `GET  …/environments/{env}/connections` | List database connections. |

Example — run a query against the bundled `storefront` package:

```bash
curl -s -X POST \
  http://localhost:4000/api/v0/environments/examples/packages/storefront/models/storefront.malloy/query \
  -H 'content-type: application/json' \
  -d '{"query":"run: order_items -> by_category"}'
```

### Query request shapes

The query body takes one of two shapes: `query` alone (ad-hoc Malloy, compiled in the model's
context), or `queryName` without `query` (a named view when `sourceName` is set; a model-level
named query when it is not). Any other combination returns a 400. The response's `result` field is
a JSON string, so parse it; with `"compactJson": true` it holds plain row objects, without it the
full Malloy result envelope with type metadata. `givens` rides on either shape to supply
model-declared [runtime parameters](givens.md).

## Live API explorer

The running server hosts the full, interactive **Swagger UI** and the OpenAPI 3.1 spec:

| URL | What |
| --- | --- |
| **http://localhost:4000/api-doc.html** | Interactive Swagger UI — browse every endpoint, see schemas, try requests. |
| **http://localhost:4000/api-doc.yaml** | The raw OpenAPI 3.1 spec (feed it to codegen or Postman). |

The App's footer **Publisher API** link opens the same explorer.

The spec file ships inside the npm package, so every running server serves `/api-doc.yaml` even
with no internet access. `/api-doc.html` loads the Swagger UI assets from a CDN, so in a sandbox
that blocks CDNs the page can come up blank; the YAML is the dependable artifact. Without a
running server, the same spec is at
`https://raw.githubusercontent.com/malloydata/publisher/main/api-doc.yaml`.

![The interactive Swagger UI Publisher serves at /api-doc.html](screenshots/api-explorer.png)

> **Why isn't the explorer embedded directly in this page?** GitHub-rendered Markdown strips the
> JavaScript that Swagger UI needs, so an interactive explorer can't live inside a `.md` file on
> GitHub. Open the hosted URL above instead. (A docs site that permits inline HTML — e.g.
> docs.malloydata.dev — can embed Swagger/Redoc against `/api-doc.yaml`.)

## Generated clients

The spec drives a generated **Python client** ([`packages/python-client`](../packages/python-client))
and the TypeScript client the SDK uses. Regenerate them from `api-doc.yaml` after any spec change —
see [CONTRIBUTING.md](../CONTRIBUTING.md).
