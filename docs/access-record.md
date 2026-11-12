<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# The access record

> What this is: the one structured line Publisher writes per query, carrying what an audit
> control asks for, so "who read patient charts last Tuesday, and what did they get" has an
> answer. It records; it decides nothing. The gates decide ([authorize.md](authorize.md)), the
> load refuses ([classification.md](classification.md)), and the pipeline validates.

> **Operating model.** Publisher is a microservice. It runs locally with your own permissions, or
> inside your cloud behind the gateway or application that authenticates your users; it does not
> authenticate anyone itself. The controls on this page are evaluated against what each request
> carries, so they enforce your policy only when that gateway authenticates the user, sets the
> givens from the verified identity, and is the only way to reach Publisher. See
> [the operating model](security-posture.md#operating-model-a-microservice-behind-your-gateway).

Every route that runs a query emits one record per request, on success and on refusal alike:

- the REST query route, `POST …/models/{path}/query`;
- the legacy notebook cell route;
- the `execute_query` MCP tool;
- value lookups in the `get_context` MCP tool, one record per lookup;
- raw SQL against a connection, recorded with `decision: "ungated"` because no gate runs on it.

A request to one of those routes writes exactly one record: a success, a refusal before the model is
reached (a bad query parameter or the concurrency cap included), a refusal inside it, a timeout, and
a request the client abandoned (`cancelled`) each produce one. Four paths write none:

- a request body that does not parse, which Express refuses before any route runs;
- `compile` and `sqlSource`, which return a schema or compiled SQL and no rows;
- an `execute_query` or `get_context` call the MCP SDK rejects for failing the tool's argument schema
  before the handler runs (an argument the handler itself refuses is recorded);
- `get_context` calls that look up no value.

Materialization builds run as the system, not as a caller, and are not recorded. The record is separate from the request
log, which is a debug-grade dump of the request and response bodies, and from the gate metrics,
which count decisions with no identity attached.

## What it carries

| Field                                                     | Meaning                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| --------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `event`                                                   | Always `access`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `environment`, `package`, `version`, `modelPath`          | Where the query went. `version` only on a versioned package.                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `queryName`, `requestedSource`, `adHoc`                   | What the request named: a named query, a source, or query text.                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `source`                                                  | The source the compiled query entered, which is the source whose gates decided.                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `gates`                                                   | The `#(authorize)` and `#(access_filter)` expressions in force on that source, as the model declares them.                                                                                                                                                                                                                                                                                                                                                                                                 |
| `decision`                                                | `open` (no gate), `admitted` (a lock admitted, no row filter), `filtered` (a row filter was grafted), `bypassed` (the data-management header skipped the gates), `ungated` (a route with no gate to run, raw SQL).                                                                                                                                                                                                                                                                                         |
| `givenNames`                                              | The names of every given supplied, secure or not.                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `secureGivens`                                            | The values of the supplied givens declared `#(secure)`: the caller's attributes as the trusted tier resolved them (roles, organizations), which is what an audit line is for. A plain given's value is never recorded: the caller chose it, and it already sits in the warehouse's own query log.                                                                                                                                                                                                          |
| `gateGivenNames`                                          | The names the grafted row filters read.                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `fieldsRead`                                              | Every dimension and measure the compiled query references, by path from the source (`patient.ssn` through a join), each with its classes as the model declares or infers them. A field that declares a class below what its inputs carry also lists those classes as `dropped`, and its inputs are listed as reads of their own.                                                                                                                                                                           |
| `fieldsReadComplete`                                      | `false` when a read could not be placed or the join walk stopped at its bound, so `fieldsRead` may be short. A record with `true` lists every field the query read.                                                                                                                                                                                                                                                                                                                                        |
| `outcome`, `status`                                       | `ok` 200; `denied_lock` 403; `refused_surface` 404; `bad_request` 400; `cancelled` 499 (the client abandoned the request); `error` otherwise.                                                                                                                                                                                                                                                                                                                                                              |
| `rowCount`, `durationMs`, `executionTimeMs`, `servedFrom` | What came back and how.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `scannedBytes`                                            | Warehouse bytes the query scanned, where the backend reports them inline (BigQuery today). Absent is not zero.                                                                                                                                                                                                                                                                                                                                                                                             |
| `shape`                                                   | The query's workload shape, with every literal replaced: the source, the view the request named, the dimensions and measures of its first stage, the fields its `where:` reads, its filter expressions with each value as `?`, its time zone, and a `key` two runs of the same question share. See [the workload log](#the-workload-log).                                                                                                                                                                  |
| `path`                                                    | What answered the query, from its [path report](materialization.md#which-table-answered-a-query-the-path-report): `servedBy` (`kind`, and the `tableId` that answered) and `considered` (each table's `tableId`, `kind`, `outcome`, skip `reason`, and the serve verdict's `code` on a `WARNED` skip). Absent when the query answered live with nothing considered, or when no report could be assembled; `pathUnavailable` tells the two apart. A `code` is kept only when it is a plain lower-case code. |
| `pathUnavailable`                                         | `true` when the query answered but its path report could not be assembled, so an absent `path` means unknown rather than live with nothing considered. Absent otherwise.                                                                                                                                                                                                                                                                                                                                   |
| `connection`                                              | The connection a raw SQL request ran against. Raw SQL records no `modelPath`, `source`, `gates` or `fieldsRead`.                                                                                                                                                                                                                                                                                                                                                                                           |
| `caller`                                                  | The value of the request header the deployment names (below), verbatim. Absent when the request carried none. The MCP routes carry it too.                                                                                                                                                                                                                                                                                                                                                                 |
| `traceId`                                                 | The W3C trace id from the request's `traceparent`, when it carried one, so the record joins the request log and the span.                                                                                                                                                                                                                                                                                                                                                                                  |
| `error`                                                   | The message on a non-ok outcome, with credentials a driver quotes back in it (connection strings, passwords) redacted and the text cut at 2000 characters. A gate's own detail is already redacted there. A connection error carries the text the HTTP answer carries instead: a driver's own message quotes the statement back, and a plain given's value is a literal in it.                                                                                                                             |

A refused query has no `source`, `gates` or `fieldsRead`, because nothing was compiled for the
caller; it has what the request named and the outcome. The exception is a query refused for reading
past its source's [`#(carries)`](classification.md#the-ceiling-is-checked-per-query): it compiled,
so its record is `bad_request` with the `source`, the `fieldsRead` that crossed the ceiling and
`fieldsReadComplete`, and `error` names the field.

A refused query carries no `shape` either.

## What it can leave out

`secureGivens` is read from the givens the entry model declares. On a gate shaped as a hub, where the
gate is written in another model and reads a `#(secure)` given the entry model does not itself
declare, the record can omit that given's value. The query is still gated on the real value; only the
record is short. An audit that needs that value reads it from the gateway that set it.

## Where it goes

The record is written through the server's logger at `info`, as JSON when the logger is in JSON
mode, under the message `Access record`, so it lands wherever the operator already ships logs.
The same facts are set as attributes on the active span (`publisher.access.*`), so a trace shows
the decision beside the query. Retention, integrity and tamper evidence are the log store's;
Publisher writes and forgets, unless the opt-in [workload log store](#the-workload-log) is on, which
keeps a bounded local copy for workload analysis and is not an audit store.

## The workload log

`shape` makes the access records a workload log too: what was asked of each source, keyed so the
runs of one question count together, beside how it was served (`servedFrom`, and `path` for which
materialized table answered and why the others did not) and what it cost (`durationMs`,
`scannedBytes`). It is read from the compiled query's first stage, which is the one
that reads the source, and it records names and the structure of expressions, never a value: a
filter `region = 'west'` is recorded as `region` among `filteredDimensions` and `=(region,?)`
among `filters`. A field the query defines is recorded by its expression without its alias, so
`n is count()` and `rows is count()` agree. A row filter a gate grafts is not in the shape; it is in
`gates` and `decision`. A list of values is one `?` however long, so a multi-select tile is one shape
whatever is selected, and a function call is recorded by its name and arguments (and a window
function by the fields it partitions by). `view` names the view the request named, but is not in
the `key`: `run: orders -> by_region`, the same view requested by name, and the same fields written
out are one shape. `complete` is `false` when a field could not be placed as a dimension or a
measure, and `nested` is `true` when the stage nests a query, inline or a named view; the nest is
not in the shape, and a nested view is not counted as a dimension.

The records go to the log either way. To keep them locally as well, set
`PUBLISHER_WORKLOAD_LOG_STORE=on` ([configuration.md](configuration.md)): each record is written to
the `workload_log` table of Publisher's own metadata database (`publisher.db`), with columns for
what the workload is read by (time, environment, package, model, source, shape key, served-from,
outcome, duration, rows, scanned bytes) and the whole record as JSON. The store keeps the record as
logged with two exceptions. The values of `#(secure)` givens are dropped, and their names stay in
`givenNames`: those values are the caller's verified attributes, which an audit line carries on
purpose and a workload store has no use for. And the error text has every quoted string and number
replaced by `?`, since a driver's error quotes the statement back, and a row filter built from a
secure given is a literal in it. It keeps 30 days and at most a million rows by default
(`PUBLISHER_WORKLOAD_LOG_RETENTION_DAYS`, `PUBLISHER_WORKLOAD_LOG_MAX_ROWS`), pruning at most once a
minute. Writes are buffered and flushed every two seconds, off the query path, in multi-row
inserts; a failed write drops its batch, logs a warning and counts in
`publisher_workload_log_dropped_total`, and never fails a query. At most 10,000 records are held
waiting for a write, and past that the oldest are dropped and counted the same way. A graceful
shutdown (SIGTERM) writes what is buffered; a process that dies loses at most the last interval's
records. Like the rest of
`publisher.db`, the table is wiped by `--init`.

The store holds what the record holds: query shapes, field paths, caller identities and error
text with its literals redacted. Treat `publisher.db` as you treat the log store it duplicates. Publisher still authenticates
no one, so `caller` is only what the gateway in front of it set.

Materialization v2's path report (which stored variants the router considered for a query, and why
each was skipped) will attach to the record as `path` when it lands; the store keeps whatever the
record carries, so it needs no change for it.

## Who the caller is

Publisher does not authenticate: it is meant to sit behind a gateway that does
([security-posture.md](security-posture.md)). The gateway names the caller by setting a request
header, and Publisher copies that header's value into the record as `caller`. The header is
`x-publisher-caller` by default and `PUBLISHER_CALLER_HEADER` renames it
([configuration.md](configuration.md)). On the MCP routes the caller and the `traceparent` trace id
come from the MCP request's own headers, the same way. The value is opaque to Publisher: a user id, an email, a
service account, whatever the gateway's identity is. A deployment that exposes the query port
without a gateway gets whatever the client wrote there, which is to say nothing it can trust, the
same caveat as a plain given.

## What it is for

A review that checks a package against a policy can mark an audit control (HIPAA 164.312(b), SOC
2 CC7.2, PCI DSS 10.2, NIST 800-53 AU-2) provable rather than "not provable here" when the
deployment ships these records somewhere durable. A breach investigation reads `fieldsRead` and
`caller` instead of reconstructing from SQL. And because `fieldsRead` carries classes, the record
reads in the policy's words: a line that says three `phi.direct_identifier` fields were read by a
caller admitted as `billing` is the line an auditor was going to ask for.

The record never changes what a query returns, and a failure to build or emit it never fails the
query: the query's own answer or error is what the caller gets, and the failure is logged.
