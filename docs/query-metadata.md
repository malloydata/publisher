# Query metadata (backend query tags)

Every query Publisher sends arrives at the backend looking like every other one. Query metadata attaches a small bag of string properties to each statement — a team, a workload, a request id — so the backend's own reporting can tell them apart: what an interactive query cost versus a materialization build, which package drove a spike, which warehouse query belongs to which API call.

It is observability only. It never affects results, and it is excluded from connection fingerprints and build identity, so changing a property never re-addresses or rebuilds anything.

## What the backend sees

Each backend gets the bag through a mechanism that attaches **per query**:

| Backend | Mechanism | Where it shows up |
|---|---|---|
| Snowflake | per-statement `QUERY_TAG` (the bag as JSON) | `QUERY_HISTORY`, `QUERY_ATTRIBUTION_HISTORY` |
| BigQuery | per-job `labels` | `INFORMATION_SCHEMA.JOBS` |
| Trino / Presto, Databricks, Postgres, DuckDB, MySQL | a leading SQL comment — `-- team="finance" class="interactive"` | query history, `pg_stat_activity`, `system.runtime.queries` |

Session-scoped mechanisms (Trino client tags, Databricks `SET QUERY_TAGS`, Postgres `application_name`) are deliberately not used: a pooled session serves many queries, so a session-level tag would attribute all of them to whichever one set it last.

## What Publisher adds by itself

Publisher attaches its own context to every statement, so attribution needs no modeling work:

| Property | Meaning |
|---|---|
| `class` | `interactive`, `materialize`, `index` or `ops` |
| `environment`, `package`, `version` | where the query came from |
| `model` | the model a query ran against |
| `source` | the persist source a build was materializing |
| `trigger`, `run_id` | what started a build, and which run it belongs to |

These are properties of the unit of work, not of the individual call, so repeating the same query produces the same bag — the comment-carrying backends keep whatever result caching they do. Context wins over a declared property of the same name: a caller cannot label its own query as a build.

Set `PUBLISHER_QUERY_METADATA=off` to attach nothing at all, including caller-supplied properties. That is the escape hatch for a deployment that wants its statements left exactly as Malloy compiles them.

## Declaring your own properties

Most specific wins, property by property:

1. **Connection** — `queryMetadata` on the connection config. Use it for what is true of the whole connection.
2. **Package** — `materialization.queryMetadata` in `publisher.json`.
3. **Model file** — `## materialization.queryMetadata.<name>="<value>"`.
4. **Persist source** — `#@ persist queryMetadata.<name>="<value>"`.
5. **Request** — `queryMetadata` on a query or SQL request, plus `queryClass` to set `class`.

```json
// publisher.json — every statement this package's sources issue
{
   "name": "orders",
   "materialization": {
      "scope": "version",
      "queryMetadata": { "team": "finance", "tier": "gold" }
   }
}
```

```malloy
// model file: a default for every persist source in this file
## materialization.queryMetadata.surface="marts"

// one source, overriding `tier` and inheriting `team` and `surface`
#@ persist name="order_rollup" queryMetadata.tier="platinum"
source: order_rollup is orders -> { aggregate: revenue is sum(amount) }
```

Layers 2–4 describe how a source is **built**. A live query against the model is a different unit of work and does not inherit them — otherwise interactive traffic would be attributed to the build that happens to share the source.

## Correlating an API call with a backend query

There is no backend query id in the response, on purpose — the property bag is a better join key because it works the same way on every backend. Send something that identifies the call and look for it on the other side:

```bash
curl -X POST .../models/orders.malloy/query \
  -d '{"query":"run: order_rollup -> { aggregate: revenue }",
       "queryMetadata":{"request_id":"7f3a"}}'
```

The response echoes what was actually attached as `appliedQueryMetadata`, so you can see when a property you declared was clamped or dropped rather than assuming it landed.

## The contract

Malloy validates the bag when it issues the statement and **refuses a statement it cannot render**, so Publisher keeps every bag inside the contract:

- property names: ASCII letters, digits and underscore, at most 128 characters
- property values: printable ASCII without `"`, at most 256 characters
- at most 20 properties per bag

Two things worth knowing:

- **Start a name with a letter.** BigQuery drops a label name it cannot fit to its own grammar (a leading digit or underscore) while every other backend keeps it. Publisher reports that as a package warning rather than letting the property quietly go missing on one backend.
- **Nothing is silently dropped.** A declaration that violates the contract shows up as a warning on the package; at query time the bag is clamped rather than throwing, and every dropped property is counted in `publisher_query_metadata_properties_dropped_total`. A per-request bag is rejected outright with a 400, since there is a caller to tell.

Put no secrets or personal data in a tag — it is visible to anyone who can read the backend's query history, and on the comment-carrying backends it is visible in the query text itself.
