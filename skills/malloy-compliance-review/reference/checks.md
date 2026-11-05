<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# The checks

Each check reads one of four things: the package read's classification summary (each published
domain's `#(carries)` and its field count by class), the model read (each domain's gates and which
givens are `#(secure)`), `get_context` (each published dimension with its classes), or the server
status. The rest are aggregate queries run as the register's personas. None reads a row.

| Check | Fails when | Reads |
| --- | --- | --- |
| `inventory` | The package read has no classification summary, or `get_context` could not list the published fields | package read, `get_context` |
| `loads` | The package did not load, or serves a stale model because its latest files were refused. One finding per refusal, tagged with the domain and kind (`ceiling`, `join`) | `/api/v0/status` |
| `refusals` | Never fails on a refusal: a refused change is evidence that a violating change is not deployed | `/api/v0/status` |
| `coverage` | A published field has no class. A field that declares a lower class than the fields it reads (a `classification-downgrade` warning) needs review | package read |
| `gated` | A published domain has neither `#(authorize)` nor a row filter | model read |
| `secure_gates` | A gate reads a given that is not `#(secure)` | model read |
| `locked_bases` | A source the package does not publish answers a count to an identity holding every role the register names. Its sources are read from the package's `.malloy` files and queried through each file's own path; with no files found, or a probe the server neither answered nor refused, it needs review | package files, a count per source |
| `ceiling` | A domain declares no `#(carries)`, declares a class its `must_not_carry` forbids, or serves a class beyond it | package read, register |
| `access` | A persona's outcome on a domain differs from the register, a gated domain answers a caller with no givens, or the register names a domain the package does not publish. A probe the server neither answered nor refused needs review | a count per domain per persona, and per gated domain with no givens |
| `values` | A field's values prove more than its class allows, a de-identified domain serves day dates, five-digit ZIPs or uncapped values. Free text there needs review | `get_context`, an aggregate profile per field |

## What the values prove

`values` runs one aggregate query per published dimension, as a persona admitted to its domain:
the row count, the filled count, the distinct count, lengths, and how many values match each
pattern. A signal counts when at least half the filled values match.

| Signal | Means | Least class |
| --- | --- | --- |
| `ssn`, `email`, `phone` | The values are that kind of identifier | direct |
| `uuid` | Record identifiers | direct |
| `unique_per_subject` | A text field through a `subjects` join with a distinct value for at least 85% of the individuals that have one, over 50 or more | direct |
| `date_day` | Dates to the day | quasi |
| `zip5` | Five-digit ZIP codes | quasi |
| `near_unique` | At the domain's own grain, distinct values for at least 90% of 500 or more rows | quasi |
| `year`, `zip3`, `free_text` | Hints only; never fail a class | -- |

A floor is a lower bound. A birth date's values show dates to the day, a quasi-identifier; the
policy's class table makes it direct. The check fails a field declared *below* its floor, never one
declared above.

A probe is **refused** when the server answers 403, or 404 with the query boundary's own refusal
(`Query target is not queryable.` or `No queryable ...`). Any other non-2xx (a 400 from a typo, a 401,
a 404 for a wrong model path) means the probe never reached the gate, so it needs review rather than
counting as a refusal.

## Scoping

A check runs once per review, however many controls name it. A control that names `values:research`
takes only the findings about `research`; a scope that names no published domain, declared source or
refusal kind fails as an unknown scope, and `locked_bases` scopes are checked against the sources the
package's files declare; if every finding is about another domain, the control
holds for that check and the report says nothing it found touches `research`.
