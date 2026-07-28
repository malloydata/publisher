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
| `trigger`, `run_id` | what started a build, and which run it belongs to (also the run whose tables a drop is retiring) |
| `query_id` | identifies this one query; the response hands it back (see below) |

Context wins over a declared property of the same name: a caller cannot label its own query as a build, and cannot supply its own `query_id`.

Everything except `query_id` is a property of the unit of work rather than of the individual call, so a repeated query produces the same values. `query_id` is per call by definition, which on a backend with no native tag mechanism makes the statement text unique and so bypasses an exact-text result cache. That is the deliberate cost of being able to find one query again.

Set `PUBLISHER_QUERY_METADATA=off` to attach nothing at all, including caller-supplied properties: the escape hatch for a deployment that wants its statements left exactly as Malloy compiles them, and the way out if you would rather have the result cache than the correlation id.

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

Under pressure — the 20-property cap, or Snowflake's tag limit — properties are given up least-specific-first, so a per-request property outlives a connection-wide one and the server's context outlives both.

## Correlating an API call with a backend query

A query response carries `queryCorrelationId` — the id Publisher minted for that query and attached as its `query_id` property. Look the same value up on the other side:

```sql
-- Snowflake: the bag is the JSON QUERY_TAG
select * from snowflake.account_usage.query_history
where try_parse_json(query_tag):query_id = '<queryCorrelationId>';

-- BigQuery: the bag is the job's labels
select * from `region-us`.INFORMATION_SCHEMA.JOBS
where exists (select 1 from unnest(labels) l
              where l.key = 'query_id' and l.value = '<queryCorrelationId>');
```

On the backends that carry the bag as a comment, the id is in the statement text, so a query-history text search finds it.

Publisher mints the id rather than accepting one because it is the platform's join key: a caller-supplied value can be omitted, reused across calls, or collide with another caller's, and then the response has nothing meaningful to hand back. To carry your own id as well, declare it under your own property name — `query_id` is reserved:

```bash
curl -X POST .../models/orders.malloy/query \
  -d '{"query":"run: order_rollup -> { aggregate: revenue }",
       "queryMetadata":{"request_id":"7f3a"}}'
```

A build needs no response field: every statement of one build carries `run_id`, which is the materialization id the create call already returned (or the caller's own, via `runContext.runId`).

## The contract

Malloy validates the bag when it issues the statement and **refuses a statement it cannot render**, so Publisher keeps every bag inside the contract:

- property names: ASCII letters, digits and underscore, at most 128 characters
- property values: printable ASCII without `"`, at most 256 characters
- at most 20 properties per bag

Two things worth knowing:

- **Start a name with a letter.** BigQuery drops a label name it cannot fit to its own grammar (a leading digit or underscore) while every other backend keeps it. Publisher reports that as a package warning rather than letting the property quietly go missing on one backend.
- **BigQuery rewrites values too.** Its label grammar is lowercase `[a-z0-9_-]`, at most 63 characters, so `model="orders/Rollup.malloy"` arrives as `orders_rollup_malloy` and `Team`/`team` collapse into one label. Values already in that grammar — `queryCorrelationId` among them — travel intact; anything else is matched on its transformed form in `INFORMATION_SCHEMA.JOBS`.
- **Leave room for the server.** The 20-property cap covers the whole bag, and the server's context is added on top of what you declare, so a declaration over ~11 properties loses its least specific ones at query time. Publisher warns at publish rather than at the warehouse.
- **Nothing is silently dropped.** Where the declaration has a human behind it, it is rejected: a per-request bag and a connection update both come back as a 400. Where it does not, it is reported — a package declaration becomes a package warning, a connection default in a loaded config becomes a log warning (a tag must never be the reason an environment refuses to come up). At query time the bag is clamped rather than throwing, and every dropped property is counted in `publisher_query_metadata_properties_dropped_total`; the accompanying log names the model or source. Under a budget the server's own context is shed last, so `queryCorrelationId` stays usable.

## What is not tagged

- **Schema fetches.** The bag is attached per query and Malloy's schema fetch takes no query options, so package-load introspection carries no attribution.
- **`POST …/sqlTemporaryTable`.** It issues a real `CREATE TEMPORARY TABLE AS`, through a driver call that takes no query options.
- **A `storage=` build's destination statements.** The DuckDB/DuckLake session path issues its own SQL; the warehouse-side read that the build's cost lives in is tagged.

Put no secrets or personal data in a tag — it is visible to anyone who can read the backend's query history, and on the comment-carrying backends it is visible in the query text itself.
