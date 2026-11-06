<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# The access record

> What this is: the one structured line Publisher writes per query, carrying what an audit
> control asks for, so "who read patient charts last Tuesday, and what did they get" has an
> answer. It records; it decides nothing. The gates decide ([authorize.md](authorize.md)), the
> load refuses ([classification.md](classification.md)), and the pipeline validates.

Every call to the query route, `POST …/models/{path}/query` and the `execute_query` MCP tool
behind it, emits one record, on success and on refusal alike. It is separate from the request
log, which is a debug-grade dump of the request and response bodies, and from the gate metrics,
which count decisions with no identity attached.

## What it carries

| Field                                                     | Meaning                                                                                                                                                                                                                                                                                           |
| --------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `event`                                                   | Always `access`.                                                                                                                                                                                                                                                                                  |
| `environment`, `package`, `version`, `modelPath`          | Where the query went. `version` only on a versioned package.                                                                                                                                                                                                                                      |
| `queryName`, `requestedSource`, `adHoc`                   | What the request named: a named query, a source, or query text.                                                                                                                                                                                                                                   |
| `source`                                                  | The source the compiled query entered, which is the source whose gates decided.                                                                                                                                                                                                                   |
| `gates`                                                   | The `#(authorize)` and `#(access_filter)` expressions in force on that source, as the model declares them.                                                                                                                                                                                        |
| `decision`                                                | `open` (no gate), `admitted` (a lock admitted, no row filter), `filtered` (a row filter was grafted), `bypassed` (the data-management header skipped the gates).                                                                                                                                  |
| `givenNames`                                              | The names of every given supplied, secure or not.                                                                                                                                                                                                                                                 |
| `secureGivens`                                            | The values of the supplied givens declared `#(secure)`: the caller's attributes as the trusted tier resolved them (roles, organizations), which is what an audit line is for. A plain given's value is never recorded: the caller chose it, and it already sits in the warehouse's own query log. |
| `gateGivenNames`                                          | The names the grafted row filters read.                                                                                                                                                                                                                                                           |
| `fieldsRead`                                              | Every dimension and measure the compiled query references, by path from the source (`patient.ssn` through a join), each with its classes as the model declares or infers them.                                                                                                                    |
| `outcome`, `status`                                       | `ok` 200; `denied_lock` 403; `refused_surface` 404; `bad_request` 400; `error` otherwise.                                                                                                                                                                                                         |
| `rowCount`, `durationMs`, `executionTimeMs`, `servedFrom` | What came back and how.                                                                                                                                                                                                                                                                           |
| `caller`                                                  | The value of the request header the deployment names (below), verbatim. Absent when the request carried none.                                                                                                                                                                                     |
| `traceId`                                                 | The W3C trace id from the request's `traceparent`, when it carried one, so the record joins the request log and the span.                                                                                                                                                                         |
| `error`                                                   | The refusal's message on a non-ok outcome. A gate's own detail is already redacted there.                                                                                                                                                                                                         |

A refused query has no `source`, `gates` or `fieldsRead`, because nothing was compiled for the
caller; it has what the request named and the outcome.

## Where it goes

The record is written through the server's logger at `info`, as JSON when the logger is in JSON
mode, under the message `Access record`, so it lands wherever the operator already ships logs.
The same facts are set as attributes on the active span (`publisher.access.*`), so a trace shows
the decision beside the query. Retention, integrity and tamper evidence are the log store's;
Publisher writes and forgets.

## Who the caller is

Publisher does not authenticate: it is meant to sit behind a gateway that does
([security-posture.md](security-posture.md)). The gateway names the caller by setting a request
header, and Publisher copies that header's value into the record as `caller`. The header is
`x-publisher-caller` by default and `PUBLISHER_CALLER_HEADER` renames it
([configuration.md](configuration.md)). The value is opaque to Publisher: a user id, an email, a
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

The record never changes what a query returns, and a failure to build it never fails the query:
the query is answered, and the record is emitted with what was known.
