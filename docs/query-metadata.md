<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Query metadata (backend query tags)

Every query Publisher sends arrives at the backend looking like every other one. Query metadata attaches a small bag of string properties to each statement — a team, a workload, a request id — so the backend's own reporting can tell them apart: what an interactive query cost versus a materialization build, which package drove a spike, which warehouse query belongs to which API call.

It is observability only. It never affects results, and it is excluded from connection fingerprints and build identity, so changing a property never re-addresses or rebuilds anything.

**Off by default.** Set `PUBLISHER_QUERY_METADATA=on` to attach anything at all; until you do, statements go out exactly as Malloy compiles them and `queryCorrelationId` comes back null. This release ships the feature dark — it is the rare change that touches every statement the server sends, and on the backends that carry the bag as a comment it changes the statement text, so a deployment should turn it on having read what it does rather than discover it in a query log.

## What the backend sees

Each backend gets the bag through a mechanism that attaches **per query**:

| Backend                                             | Mechanism                                                       | Where it shows up                                           |
| --------------------------------------------------- | --------------------------------------------------------------- | ----------------------------------------------------------- |
| Snowflake                                           | per-statement `QUERY_TAG` (the bag as JSON)                     | `QUERY_HISTORY`, `QUERY_ATTRIBUTION_HISTORY`                |
| BigQuery                                            | per-job `labels`                                                | `INFORMATION_SCHEMA.JOBS`                                   |
| Trino / Presto, Databricks, Postgres, DuckDB, MySQL | a leading SQL comment — `-- team="finance" class="interactive"` | query history, `pg_stat_activity`, `system.runtime.queries` |

On the comment-carrying backends the bag is part of the statement text, so it shares whatever window that text is stored in: Postgres truncates `pg_stat_activity.query` at `track_activity_query_size` (1024 bytes by default), and a large declared bag can eat enough of it to cut off the SQL itself. Publisher's own context is about 200 characters; a bag near the 20-property cap is not.

Session-scoped mechanisms (Trino client tags, Databricks `SET QUERY_TAGS`, Postgres `application_name`) are deliberately not used: a pooled session serves many queries, so a session-level tag would attribute all of them to whichever one set it last.

**One path is the exception, and it is exempt from that reasoning rather than in spite of it.** A `storage=` build reads its source through DuckDB's native query-passthrough, where no Malloy connector is in the call path to apply a per-statement tag — so the choice there is a session tag or no attribution at all. What makes it safe is that the session is not pooled: each build runs on its own private DuckDB instance, created and disposed for that build, so "whichever one set it last" is always this build. The tag is cleared before the session is released, so a session the driver may hand over pre-tagged cannot leave a stale attribution behind either.

It does not escape the problem entirely, and the residue is worth knowing before reading a bill. The Snowflake driver issues its own connection probes (`SELECT 1`) around each passthrough call, and those run on the tagged session — so they carry the build's bag into `QUERY_HISTORY` alongside the read itself. They scan nothing, so cost grouped by `class` or `run_id` is unaffected; a COUNT of statements grouped the same way reads high.

## What Publisher adds by itself

Publisher attaches its own context to every statement, so attribution needs no modeling work:

| Property                 | Meaning                                                                                          |
| ------------------------ | ------------------------------------------------------------------------------------------------ |
| `class`                  | `interactive`, `materialize`, `index` or `ops`                                                   |
| `environment`, `package` | where the query came from                                                                        |
| `model`                  | the model a query ran against                                                                    |
| `source`                 | the persist source a build was materializing                                                     |
| `trigger`, `run_id`      | what started a build, and which run it belongs to (also the run whose tables a drop is retiring) |
| `query_id`               | identifies this one query; the response hands it back (see below)                                |

Context wins over a property of the same name: a _declaration_ cannot label the statements it describes as a build, and nothing can supply its own `query_id`.

The context names are also **reserved against a model-side declaration** — a package block, a model file or a `#@ persist` annotation. Context only overwrites a name it has a value for, and a served query has no `source`, `trigger` or `run_id`; without the reservation a declaration of one would stamp it on interactive traffic, where it reads in the warehouse's own history exactly like a build. A declared context name is dropped and metered (`reason=reserved_name`) rather than applied, and reported at publish so it arrives as a message rather than a counter tick.

The connection default and the per-request bag are **not** covered, which is deliberate and pre-existing: a request setting `queryClass` is a caller describing its own call, and narrowing either is a compatibility decision of its own. The reservation covers the layer that describes _someone else's_ statements — a model-side declaration rides every query against that source, including queries its author never sees.

Everything except `query_id` is a property of the unit of work rather than of the individual call, so a repeated query produces the same values. `query_id` is per call by definition, which on a backend with no native tag mechanism makes the statement text unique and so bypasses an exact-text result cache. That is the deliberate cost of being able to find one query again.

`PUBLISHER_QUERY_METADATA=off`, the default, attaches nothing at all — including caller-supplied properties. Leave it off for a deployment that wants its statements left exactly as Malloy compiles them, or if you would rather have the result cache than the correlation id.

## Declaring your own properties

Most specific wins, property by property:

1. **Connection** — `queryMetadata` on the connection config. Use it for what is true of the whole connection.
2. **Package** — `queryMetadata` at the root of `publisher.json`.
3. **Model file** — `## queryMetadata.<name>="<value>"`.
4. **Source** — `#@ queryMetadata.<name>="<value>"`.
5. **Request** — `queryMetadata` on a query or SQL request, plus `queryClass` to set `class`.

**Any source can be tagged, not only a persisted one.** `queryMetadata` is a sibling of `persist` in the `#@` namespace rather than a key inside it, so layer 4 applies to a source that is never materialized — its tags ride every query that reads it.

**The `materialization.` spelling is deprecated at layers 2 and 3.** `materialization.queryMetadata` in `publisher.json`, `## materialization.queryMetadata.*` in a model file, and `materialization.queryMetadata` on the package API all still work. None was ever a build-only setting: the properties ride served queries too, so declaring them inside the build-policy block described a scope the feature does not have and sent authors hunting through build settings for a way to label their traffic.

Only the manifest home warns today — that is where the two homes can disagree, and where the warning has something actionable to say. At layer 2 the canonical root wins a conflict and warns, unlike a conflicting `scope`, which fails the load: a tag must never be the reason a package refuses to load. At layer 3 the canonical bare form simply wins per property, silently, and a property only the deprecated form declares still resolves.

The package API carries both homes for as long as the old one is supported: `queryMetadata` on a `Package` is canonical, both are populated on read, both are accepted on write, and the canonical one wins if a request sends both. Omitting it on a PATCH preserves the package's existing tags, and so does sending null — a client that serializes unset fields as null must not untag a package by accident. An empty object clears.

```json
// publisher.json — every statement this package's sources issue
{
  "name": "orders",
  "queryMetadata": { "team": "finance", "tier": "gold" },
  "materialization": {
    "scope": "version"
  }
}
```

```malloy
// model file: a default for every source in this file
## queryMetadata.surface="marts"

// one source, overriding `tier` and inheriting `team` and `surface`
#@ persist name="order_rollup" queryMetadata.tier="platinum"
source: order_rollup is orders -> { aggregate: revenue is sum(amount) }
```

Layers 2–4 ride **every statement touching the source**, a build and a served query alike. They describe what the source _is_ — whose it is, what it costs, which tier it belongs to — and that is as true of a query reading it as of the build writing it. What distinguishes the two units of work is the context layer: a build carries `class=materialize` with a `source`, `trigger` and `run_id`, and a served query carries `class=interactive` with none of them. Those names are reserved, so no declaration can blur the distinction.

A served query resolves layer 4 from the source it runs against, which the server already resolves for the authorize gate. A statement with no resolvable source — some notebook cells — carries layers 2 and 3 only.

Under pressure — the 20-property cap, or Snowflake's tag limit — properties are given up least-specific-first, so a per-request property outlives a connection-wide one and the server's context outlives both.

### Properties the deployment owns

`queryMetadataEnforced` on the connection is the same kind of map, with two differences: **no package declaration or query request can overwrite it**, and it is given up only after every declared property when a bag has to shrink. Use it for what a host is billed or audited by.

The reason it exists: in a deployment where connection configuration is an operator's to write and package publishing is not, the two are different levels of trust, and only one of them should be able to set the tenant label. Without it, a host running one Publisher for several tenants would label a connection `tenant=acme` and any package author in that environment could relabel their queries `tenant=someone_else` — or push the label out of the bag entirely by declaring twenty properties of their own. The server's own context still wins over enforced properties, because it describes what the server is actually doing rather than who is paying for it.

That split is a property of the deployment, not of this code. Publisher's own REST and MCP surfaces are unauthenticated by design (see [ai-agents.md](ai-agents.md)), so on a bare Publisher whoever can publish a package can also `PATCH` the connection. The layer is worth using where a gateway or control plane restricts connection writes to operators — which is what it was built for — and is a convention rather than a guarantee anywhere else.

```jsonc
// connection config
{
  "name": "warehouse",
  "queryMetadata": { "team": "finance" }, // a default; any declaration overrides it
  "queryMetadataEnforced": { "tenant": "acme" }, // the deployment's; no declaration can
}
```

## Correlating an API call with a backend query

A query response carries `queryCorrelationId` — the id Publisher minted for that query and attached as its `query_id` property. Over MCP the same id is `_query_id` in the `malloy_executeQuery` envelope. Look the same value up on the other side:

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
