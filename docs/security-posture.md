<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Security posture

What Publisher does and does not defend against, stated once so individual features can be
judged against it instead of each inventing their own answer.

This is a statement of the current posture plus the known gaps in it. It is not a claim that
Publisher is hardened; several things below are open, and they are listed rather than glossed.

For how to report something, and for the line between "working as documented" and a vulnerability,
see [SECURITY.md](../SECURITY.md). That policy decides what gets triaged as a report; this document
explains the posture it decides against. Where the two touch, the policy is the authority on
reportability: the unauthenticated API and the permissive default framing policy are both
classified there as working as documented, so the gaps below are arguments for changing the design,
not vulnerability reports.

## Operating model: a microservice behind your gateway

Publisher is a microservice, not an internet-facing product. It has no user accounts and does not
authenticate anyone. It is meant to run in one of two ways:

- **Locally, with your own permissions.** On a laptop or workstation, bound to loopback
  (`--host 127.0.0.1`). Its connections use whatever credentials you configure, usually your own, so
  it reads what those credentials can read. A connection in `publisher.config.json` that carries
  someone else's credentials reads with theirs.
- **Inside your existing cloud, behind your gateway.** A service in your own network, reached only
  through an authenticating gateway or the application that embeds it. Its connections use a
  service identity you grant, scoped by your cloud's own IAM.

Every access control Publisher has is evaluated against what the request carries:
`#(authorize)`, `#(access_filter)`, `#(secure)` givens, the discovery and query boundaries,
`#(carries)`, and the access record. These controls enforce your policy only when a tier you trust
stands in front of Publisher and does all of the following:

1. Authenticates the end user.
2. Sets the givens from that verified identity, replacing any value a client sent. At minimum this
   covers every given a package declares `#(secure)`.
3. Is the only network path to Publisher's ports. REST (`:4000`) and MCP (`:4040`) are not exposed to
   untrusted callers.
4. Blocks the routes a gated package's callers must not use:
   - raw SQL against a connection;
   - `/compile` at `file` or `package` scope, and the MCP `compile_model` tool at the same scopes;
   - the `x-publisher-bypass-authorize` header;
   - the operator write routes: creating, updating or deleting an environment or package, writing a
     model (`PUT …/models/{path}`), and managing connections. A caller who can write a package can
     publish an ungated source over the same connection. Block them at the gateway, or set
     `"frozenConfig": true` so the server refuses them.
   - the materialization routes (`…/packages/{packageName}/materializations` and everything under
     them). A run's record carries its build manifest, and each entry names the physical table a
     source was built into. A database entry also lists `nativeSchema`: every column of the table,
     including ones the model marks private or internal, with its warehouse type. No row value is
     returned, but the names and types describe your warehouse, so restrict these routes to
     operators.
   - the materialized-table drop
     (`DELETE …/packages/{packageName}/connections/{connectionName}/materializedTables/{tableName}`,
     from 0.16.0). It drops a table in the warehouse, so allow it only to the operator that owns
     the package's materialization lifecycle. Publisher relies on the gateway for who may call it,
     and enforces ownership itself: whoever the gateway admits can drop only a database-tier table
     that a run record of the package names on that connection, or whose ownership mark names the
     package and the very place the warehouse's catalog says the requested name resolves to now
     (a mark copied onto another table by a rename, a move to another schema, a clone, or a copy
     a search path finds first proves nothing, and a mark from before 0.16.0 is not accepted). The
     mark proves ownership only if everyone who can edit a table's comment or description in the
     warehouse could also drop that table: on BigQuery `bigquery.tables.update` changes a
     description without `bigquery.tables.delete`, and on Databricks `MODIFY` allows
     `COMMENT ON` but not `DROP`. Grant comment and description edits accordingly. It never
     drops one that a committed run of the package, a loaded package's binding or an active run
     still uses, or that another package marked
     ([the drop route](materialization.md#the-drop-route)). Two limits remain. The in-use check
     sees only this server's records and loaded packages: not an unloaded package bound through a
     host manifest or `manifestLocation`, a reader outside Malloy, another environment or server
     on the same schema, or a re-pointed connection. And the answer tells a caller whether a
     relation the connection can see exists (404 `TABLE_NOT_FOUND` against a 409), for any name
     it asks about; it never names another table's owner. Both are acceptable only because the
     gateway admits the operator alone.
5. Forwards who is asking in the caller header (`x-publisher-caller`, renamed with
   `PUBLISHER_CALLER_HEADER`) and a `traceparent`, so
   the [access record](access-record.md) names the caller.

**Headers a gateway must strip.** `x-publisher-bypass-authorize` is the one request header that
changes what Publisher enforces: carrying the value of `PUBLISHER_BYPASS_AUTHORIZE_SECRET`, it skips
`#(authorize)` and `#(access_filter)` on a query or a compile check. Remove it from every inbound
request at the gateway, so only an internal service behind the gateway (an indexer) can set it. The
`/api/v1` spec states the same rule in its overview rather than as a parameter or a security scheme,
so no generated client offers it.

Without that tier the controls are advisory. Anyone who can reach the port can set their own givens
and pass any gate. The rest of this page is the detail behind that rule.

## The trust boundary

**Publisher trusts the operator and the packages the operator registers. It does not
authenticate end users.**

Concretely:

- **The API is unauthenticated.** REST on `:4000` and MCP on `:4040` have no authn or authz.
  Anyone who can reach the port can list packages, compile Malloy, and run queries against every
  connected database. This is deliberate and documented — network isolation or an authenticating
  gateway is the intended control, not anything in the process.
- **A query reaches the host, not just the warehouse.** DuckDB runs in-process with its own
  defaults, so a model can read and write local files and open network connections. That is what
  lets a package read the Parquet beside it, and on a single-tenant deployment it is ordinary. It
  also means **one worker must not serve two tenants unless something in front of it rejects
  caller-supplied Malloy first** -- see Known gaps below for why the sandbox settings are not the
  answer and what is.
- **Package content is first-party code.** A package's models, notebooks, and `public/` files are
  treated as code the operator chose to run, the same way you would treat your own web app
  deployed on your own origin. Publisher does not scan, sandbox, or vet them.
  Package-authored skill and agent text is served unauthenticated over MCP and REST too, bounded
  by the realpath rule (nothing that resolves outside the package) and the per-file size caps.
- **A package's Malloy reads only its own package's files.** The same realpath rule bounds every
  file the compiler reads for a package's models (an `import`, a `.malloynb`, a data-styles
  file) on every compile: the load and its worker, a reload, `/compile` and MCP `compile_model`
  at every scope, the build plan, and the generated build and served-copy models. A `file:` URL
  is read only when its realpath is the package root or inside it. For a versioned package the
  root is the version's own directory, so a version cannot read its siblings or the directory
  that holds them. Both sides are resolved, so a package mounted as a symlink (watch mode) reads
  its own files and a `../` between the package's own directories still works. A `../` that
  climbs past the root, an absolute path, a symlink inside the package that leads out of it, and
  any scheme but `file:` are refused. The refusal is one fixed message, "imports must stay inside
  the package", with no path in it and the same whether or not the target exists, so an import
  cannot probe the server's disk. A path outside the root as given and its realpath is refused
  before the filesystem is asked about it. A model or notebook file that is itself a symlink
  out of the package, dangling or not, is refused the same way, as a compile error rather than
  an outage. The walk that lists a package's files does not descend into a directory link that
  leads out. A model's saved text, read back for a report, a lint or `/compile` append, goes
  through the same reader; text the served copy and the chained build read back from a compiled
  model is read only from the files that model compiled from, and the chained build follows the
  imports the compile resolved, never text that merely looks like an import. Virtual files (a request's overlay, `/compile`'s `__compile_check.malloy`, the
  generated companions) are served from memory before the disk is asked. This bounds the
  compiler, not the query: SQL a model runs through DuckDB (`read_text`, `read_blob`,
  `read_csv`, `read_parquet`) reaches the filesystem through the connection, and is confined
  by the connection's DuckDB lockdown instead: the sandbox to its package, an environment
  connection to its environment directory or its data paths (see gap 6).
- **A package's `publisher.json` is read and written only as a file inside the package.** A
  manifest that is a link out of the package, to anything or to nothing, a directory, or a file
  Publisher cannot read, gets one answer, that the manifest does not exist, and none of what it
  leads to is echoed: an invalid manifest's error names the kind of value, never the value or
  the parser's quote of it. The versioned publish reads the declared version the same way. A
  PATCH never writes through a link: a `publisher.json` that leads outside the package is refused
  with 400, and any other is replaced through a temporary file and a rename, so a link (one
  inside the package, or one that appears in between) is replaced by the file rather than
  followed. An update that changes nothing in the manifest leaves it alone. A report write (`PUT …/models/{path}`) onto a link
  planted at the file, or onto a dangling link on the way to it, is refused with the answer a
  link out of the package gets, before anything about where it leads is looked at.
- **A copied package keeps its links as written.** Every copy of a package tree (an install
  from a local location, an environment's packages at boot) copies a symlink verbatim, so a
  relative link inside the package still leads inside the copy, and is served, rather than
  being rewritten to an absolute path back into the source, which the rule above refuses.
- **Where a link leads is decided as the kernel decides it.** A path that does not resolve is
  walked one component at a time, and a `..` after a symlinked directory steps to the parent of
  that link's real target, not to the parent its spelling suggests. So a link whose text climbs
  back through another link is judged by where opening it would really go. The walk starts at
  the package's real root and never looks at a path outside the package: a step into a directory
  that is neither inside the package nor an ancestor of it is refused before it is looked at, so
  a path that goes out and comes back, through a directory that exists or one that does not,
  gets the same refusal, and a target outside the package that does not exist is refused exactly
  like one that does. Every lookup Publisher makes through a path a package author controls uses this
  walk: model and notebook reads, the package listing and its data probe, `publisher.json`,
  the content hash, the retrieval prompt files, package skills and agent skills and their
  assets, the report write path, and the files of an HTML data app. When the walk stops at a
  missing directory, only that point and the next name are opened, never the rest of the
  spelled path, so a directory appearing there afterwards cannot carry the open out. (A symlink
  appearing there, or at a component already checked, between the check and the open still
  could; only someone changing the served tree at runtime can arrange that.)
- **Registering a package is an operator action.** Packages come from `publisher.config.json` or a
  `POST` to the packages endpoint. That endpoint is gated only by `frozenConfig`, so on a
  reachable server with the default config it is open — but so is the query API, and an attacker
  who can register a package can already read the data directly. Set `"frozenConfig": true` to
  close registration on a deployment where that matters. A `.zip` environment or package that
  contains a symlink entry is refused, and nothing from it is left on disk, because the
  extractor would otherwise follow the link out of the destination directory.
- **Writing a report is an operator action too.** `PUT …/models/{path}` — the builder's save —
  writes a report file into a package and reloads it. It compiles the text before writing, and is
  gated by `frozenConfig` like package registration; it has no authentication of its own, so on a
  reachable server it sits behind the same gateway or is closed by the same setting. An attacker
  who can reach it can already register a package, so it opens no door that was shut. A report may
  live anywhere in the package (its layout comes from its tags, never its folder), so the endpoint
  is confined by what a file is rather than where it is:
  - **Create** (no `expectedHash`) is allowed at any `.malloy` path that does not exist yet, under
    the same lexical path check as every other route (no `..`, no leading `/`, no dotfile segment),
    outside `public/`, and never at the root `index.malloy`. The text must carry an artifact tag
    (an `artifact` property on a `#` or `##` tag line, outside a string), or it is refused with 400
    before compiling. Missing directories are created inside the package root only: the nearest
    directory that already exists, and the file itself when it is a link, is resolved through
    `realpath` before anything is read through it and must lie inside the resolved package root
    and outside `public/`, so a symlink cannot carry a write out of the package or read a file
    outside it into the precondition. The root `index.malloy` is refused whatever its case. A
    create that is rolled back removes the directories it made; a rollback that itself fails is
    logged as an error and answered 500, saying the package may serve the new text.
  - **Replace** (with `expectedHash`) is allowed only for a file the package currently reads as a
    report (a tagged `.malloy`, including a tagged grid report with no tiles yet) whose text on
    disk still carries the tag. A model, an untagged file, a `.malloynb`, the root `index.malloy`
    and anything in `public/` cannot be overwritten, wherever they live, and are refused with 400,
    including a file whose only tag is commented out. So the endpoint can add files in more places
    than before, but can modify only what it could already modify: reports.
  - The tag is read off the text by the same reader discovery uses on the compiled model (its
    model-level notes), so a write whose only `## artifact` sits inside a `/* */` block comment or a
    string is refused with 400 before it compiles. After the reload the written file must be a
    report (served, or a grid report with no tiles yet), or the write is rolled back, so no
    untagged file lands anywhere. Two reports may share a
    basename, since a report is identified by its path, so no name is refused. The compile-first
    gate is per file, and the reload verify checks only the written model, so a model that imports
    the written file is not checked. The text goes through the same caller-text guard as
    `/compile`, so a save that declares a real `#(authorize)` or `#(access_filter)` gate outside
    prose is refused with 400; gates live in the model file.
- **Error bodies name the server's own paths, deliberately.** A filesystem access the server
  cannot make (`EACCES`, `EPERM`, `EROFS`) answers 500 naming the errno, the operation and the
  path, and `/api/v0/status` names a server root it cannot write in `initError` and the failing
  path in a `loadErrors` entry. Those are the server's own paths -- a mount the operator has to fix -- and
  the message is composed from the errno's fields, never copied from a driver or an SDK. Every
  other 5xx keeps the generic body, because its message can carry a warehouse host, caller SQL
  or a connection string, and a recorded load failure never says more than the response did.
  A caller who can reach the port can already register a package at any readable path, so
  naming the path of a refused one widens nothing; it does mean an unauthenticated reader of
  `/status` learns the layout of the server's data directory, which the gateway in front is
  expected to keep from the public.
- **Error bodies name no storage path.** Driver text that reaches a caller (a 4xx body, a Malloy
  error, compile diagnostics, MCP error text, a run's recorded errors, `/status` `loadErrors`) has
  every object-store and HTTP URL replaced with `<storage path>`, and the server log masks a signed
  URL's signature, credential and token. An object store refusing a read (`HTTP … error on
  '<url>' (HTTP 401|403)`, and Azure's and DuckDB's other forms) answers 403 on the query and raw `sqlQuery` routes, naming the source or connection the caller
  read and no URL: `STORAGE_ACCESS_DENIED`, saying the query is fine, only for a URL under a data
  path the environment configures, and `PATH_ACCESS_DENIED` for a path the query or model reads
  itself. The full text, bucket and path included, is in the server
  log.
- **A `managed` DuckLake connection reads only its catalog's `main` schema and the schemas its
  `readableSchemas` lists.** Its storage credential reaches only those schemas' files, and because
  DuckLake answers some reads from catalog metadata alone (columns, row counts, file statistics,
  listings, inlined rows), SQL on it is also held to those schemas before it runs: a `.table()` or
  `.sql()` source when the model compiles, a raw SQL expression when the package loads, and raw SQL
  at its route. Any other schema or catalog, the
  metadata catalog, DuckDB's catalog views and functions, `DESCRIBE`/`SHOW`/`SUMMARIZE`, and table
  functions other than `range`, `generate_series` and `unnest` are refused, as is a table name that is a
  path, glob or DuckDB catalog name however it is qualified; on a local data path its file allowlist
  is those schemas' files, as its secret is scoped elsewhere; and its schema listing
  shows only `main` and the listed schemas (`packages/server/src/service/managed_catalog_scope.ts`). DuckDB and DuckLake
  offer no attach option that hides a schema, the metadata, file statistics or inlined rows from a
  read-only attach, so this check is the control. A storage destination's own session still reads
  every schema.
- **Governance is mostly a modeling concern.** `#(authorize)`, `#(access_filter)`, given-scoped
  row-level access, and a package's `index.malloy` surface constrain what a _model_ exposes. They are
  real, and they are the right place to put data policy. They are not end-user authentication:
  a given is whatever the caller sends.
  One request-level exception, and it is load-bearing: `x-publisher-bypass-authorize` carrying
  the value of `PUBLISHER_BYPASS_AUTHORIZE_SECRET` skips gate evaluation on BOTH routes outright —
  the `#(authorize)` lock as well as the `#(access_filter)` filter — for trusted data-management
  callers (indexers). With that variable unset the bypass is refused, so
  the default is closed; a deployment that configures the secret removes the header from every
  inbound request at its gateway (see "Headers a gateway must strip" above) — see
  [authorize-bypass-deployment.md](authorize-bypass-deployment.md). It is the one place where a
  request, not a model, decides whether governance applies.

The corollary that keeps coming up in design review: **a feature cannot be made safe by
sandboxing it if an equivalent capability is available unsandboxed next to it.** Isolation is
worth building when it closes a boundary, not when it decorates one of several open doors. This
is why the custom JSX dashboard sandbox was cut after it was built and working — see
[malloyyo-dashboards-design.md](malloyyo-dashboards-design.md#custom-jsx-components-cut).

## Row-level access: rows are protected, the schema is not

This section is about `#(access_filter)` specifically. It is a row filter (see
[authorize.md § Row-level gates](authorize.md#row-level-gates)): its expression is grafted onto the
source and evaluated with the query, so a caller it matches nowhere reads zero rows rather than
being refused. `#(authorize)` is the other route and does not make this trade — it is decided
before the caller's query compiles and answers 403. The trade below is the price of a filter, and
a source that should not answer schema questions to outsiders wants a lock as well:

- **Rows are protected; the schema is not.** A filtered source is readable-but-empty rather than
  403 for a caller it matches nowhere. Any caller with package read can therefore name such a
  source, compile against it, and enumerate its columns through compile errors. That is accepted
  on purpose — resolving a gate out of untrusted text before compiling it is exactly the
  resolution-from-text this design already refuses elsewhere (see
  [authorize.md § Security model](authorize.md#security-model)). Adding `#(authorize)` closes it
  for the caller the lock refuses, since that decision happens first.
- **A filter's denial is 200-with-zero-rows.** A consumer keying its own logic on a 403 does not
  see it. On this route a 403 means only that the filter could not be _attached_ — not that a
  caller was denied by it.
- **Fail-closed is the only backstop.** A row filter has no boolean admission to fall back on, so
  every path that cannot _apply_ it denies instead — a gate whose column doesn't resolve at the
  entry point, an unresolved given, a compile that throws. There is no "serve unfiltered" failure
  mode.
- **The gate's own structure is still scrubbed.** Accepting schema disclosure above is not
  accepting ACL-model disclosure: a gate reading `childtable.name` names a relationship the caller
  may not otherwise see, so a failure to attach the gate returns an opaque error naming no column,
  join, or expression — only the source.
- **A gate is evaluated with the query it is grafted into.** A gate on a field computed from a
  timestamp follows the query's `timezone:`; served answers match a live run. Gate on a column that
  already holds the date or key. A gate inherited through a `->` derivation filters the base's rows
  before the derivation runs, so the derivation's `all()`, window functions, `limit` and renames see
  only the caller's rows; where it cannot be applied there (a base that hides the gate's column) the
  query is denied. See
  [row-level-access.md § Where a gate is evaluated](row-level-access.md#where-a-gate-is-evaluated).
- **A row-level gate's given values land in the warehouse query log.** The filter is inlined
  (`WHERE org_id IN (7, 8)`), not parameterized, so every caller's group set appears verbatim in
  the warehouse's own query logging. Weigh that when deciding what a given carries — a group set
  is a smaller disclosure than the rows themselves, but it is still a disclosure to whoever reads
  that log.
- **`/compile` returns a row-gated source's compile errors without evaluating the gate.** There is
  nothing to filter on a request that returns no rows, so a row-level gate on that door denies
  outright whenever the submitted text has a runnable query — including under `includeSql`, which
  would otherwise be a SQL oracle. But text that compiles only source DEFINITIONS has no run target
  to resolve, so no gate is evaluated and the caller gets `problems` back. That is the first bullet
  applied to `/compile` rather than a separate hole: it discloses schema, not rows. It is called out
  because the row-level change is what made it reachable — a whole-source gate denied on the source
  name before compiling anything.
- **A stored table holds every caller's rows, with no gate.** A materialized source under its own
  row-level gate (an `#(access_filter)`, an `#(authorize)` lock, or both) is built, database or in
  a `storage=` destination, with every row, and the gate is re-applied each time Publisher reads
  the table. Only reads through Publisher are gated. Anyone who reads the table directly reads
  every row: in the author's own warehouse for a database table, in the destination for a
  `storage=` one ([materialization.md](materialization.md#the-served-copy-materialization-v2-publisher_materialize)).
  Tables are named predictably, by package and source. So **never configure a storage
  destination's catalog, schema, data path or DuckDB file as an environment connection that models
  can name**: through it, any package could read another package's gated rows by table name.
  Publisher warns at environment load when a DuckLake connection names the same catalog and
  metadata schema as a destination, or a data path that overlaps the destination's. That check is
  not exhaustive (a DuckDB connection attaching the destination in its `setupSQL` is not caught),
  so treat it as a backstop, not the control. The destination's own credentials are the control.

- **Inlined rows are not files.** A DuckLake catalog that records a `data_inlining_row_limit` above 0
  keeps a small table as rows in its catalog database, where a read-only view or storage secret
  scoped to the catalog's files does not reach to keep a reader out, so a stored copy of a gated
  source could be read past the gate. Publisher records 0 on every catalog it creates and warns
  (`ducklake-data-inlining-on`, in the log and on each package storing there) about an existing one
  that records more; it never changes an existing catalog's setting. See
  [DuckLake](ducklake.md#how-publisher-attaches-a-ducklake-catalog).

### Stored tables read past a gate

A gated query answered from a stored table is held, by one decision (`admitRead`), to the rules
every stored answer meets (the caller's own filters, the result's order and grouping) and to the
gate's own: whether the leg and every table it read keep the gate, and whether every column the
gate reads is stored exactly, without a zone, and compared as its source compares it. A gate fact
that cannot be read answers live. The list is in
[materialization.md](materialization.md#the-rules-a-stored-answer-is-held-to).
The gate itself is still proven by `govern()` on every answer, and it holds only on reads through
Publisher, never on a direct read of a destination.

## Where author code executes today

One surface runs author-written JavaScript, and it runs it with everything the viewer has.

**HTML data apps** (a package's `public/` directory) are served as top-level documents on the
same origin as the REST API. The consequences follow from that and are all intended:

- Page JavaScript can call any same-origin endpoint directly. `Publisher.query` is a convenience
  wrapper, not a capability boundary.
- Requests carry cookies (`credentials: "include"`), so a page acts with the viewer's authority
  wherever a gateway has established one.
- The only CSP on these documents is `frame-ancestors`. There is no `script-src`, so a page may
  load and run anything, including third-party scripts.
- The routes are unauthenticated, and only `public/` is reachable. Path traversal is blocked
  lexically and again by the package's own path walk, which never looks outside the package.
  `public/` itself is resolved against the package root: a `public/` that is a link out of the
  package serves and lists nothing (403), whatever it leads to, while one that links inside the
  package (`public -> web`) serves. A file in it must resolve inside the real `public/`: a
  symlink escaping the directory returns 403 whether or not its target exists, and a page
  outside it is never opened, not even for its `<title>`.

**Reports carry no author-written JavaScript file.** A report (a tagged `.malloy`, of either
layout, or a legacy `.malloynb`) is Malloy, markdown and renderer tags. It is declarative, which
is what makes them reviewable in a pull request and agent-authorable. Keeping them that way is a
deliberate property, not an accident of scope. It is not absolute today: gap 3 below is where a
declarative artifact still carries author-controlled HTML.

**Process backstop.** An unhandled promise rejection, whether from a connection driver (a fire-and-forget cancel, for one) or from Publisher's own code, is logged at error and counted on `publisher_unhandled_rejection_total`, and the server keeps running, including a rejection inside a package-load worker. An uncaught synchronous exception still ends the process, because it can leave state half-written.

## Known gaps

Ordered by how much they would matter on a deployment that has put a gateway in front of
Publisher. Gap 1 is now fixed and is kept here, struck through, because the reasoning about
ordering is what the rest of the list is measured against; the others remain open.

**1. ~~Everything Publisher serves is framable by any origin, and the knob that looks like it
fixes that only covers part of it.~~ Fixed.** Both halves landed together, in the order this
section prescribed. One middleware now sets `Content-Security-Policy: frame-ancestors` ahead of
every route, so the Console catch-all, notebooks, dashboards, models and the Explorer carry the
same policy as in-package `public/` files — `PUBLISHER_FRAME_ANCESTORS` finally means what it
says ([#930](https://github.com/malloydata/publisher/issues/930)). The default is now `'self'`
rather than `*`, so a deployment is closed to cross-origin framing unless it opts in. A
deployment that embeds Publisher elsewhere sets that variable to its origins, or to `*` to
restore the previous behaviour deliberately.

**2. There is a token-shaped thing that authenticates nothing.** `Publisher.embed` appends an
`embed_token` query parameter, and `Publisher.setToken` attaches an `Authorization: Bearer`
header — and no server code reads either one. The docs describe signed embed tokens as a next
step, so this is unfinished rather than broken, but an affordance that looks like authentication
and is not is worse than its absence: it invites an integrator to believe a page is protected.
Either verify it or remove it until it can be verified. It also gates widening embedding to more
surfaces ([#931](https://github.com/malloydata/publisher/issues/931)).

**3. Package markdown is rendered without raw HTML parsing (closed).** The SDK's `Prose`, which draws
every markdown surface (text tiles, descriptions, notebook cells and an environment's About panel), runs
`markdown-to-jsx` with `disableParsingRawHTML: true`, so raw HTML in a package's markdown renders as text, and its `img` override renders a markdown
image (`![](url)`) as alt text only, so no image loads from a package-chosen server.
Link `href`s are scheme-checked as well, because packages can come from untrusted git or S3 sources.
A new markdown surface should go through `Prose` rather than call the library directly.

**4. Resize messages are not origin-checked.** Both the in-page host runtime
(`packages/server/src/runtime/publisher.js`) and the Console's data-app viewer
(`DataAppViewer.tsx`) validate `event.source` against the iframe's `contentWindow` but never
`event.origin`. Source-matching is the stronger of the two checks and the payload is a single
number, so the exposure is bounded, but the check is one line.

The light or dark messages make a different choice on each side. The Console's viewer takes
`publisher:color-scheme-request` and `-applied` only from its own frame and the frame's origin, and
sends the mode only to that origin. A page's runtime takes `publisher:color-scheme` from whatever
window framed it, at any origin, since the mode changes only how the page looks; it answers at the
origin the mode came from, and sends its request to the framing page's origin where the browser
reports it (`location.ancestorOrigins`) and to any origin where it does not. The request carries
nothing.

**5. MCP has no tenant scoping, so a directly-reachable worker must be single-tenant.** Every
MCP tool takes `environmentName` and `packageName` as ordinary arguments, and the two discovery
tools treat them as optional: `list_packages` declares an empty argument schema and walks every
loaded environment, and `search_database_schema` with no `environmentName` fans out across all of
them and lists each one's connections. That is deliberate (an agent with no prior knowledge has
to start somewhere) and it is correct on a deployment serving one tenant. It is a tenant
directory on a deployment serving several. The transport defaults now make MCP harder to reach
(loopback bind, no cross-origin by default), but neither narrows what a caller who does reach it
can enumerate, and `MCP_HOST=0.0.0.0` is exactly the setting an operator reaches for to use MCP
remotely. Publisher has no tenant model to scope these tools against, so until it has one the
control is a deployment constraint rather than code: a worker reachable by more than one tenant
must not expose MCP. The REST surface has no equivalent: every route is addressed under a
specific environment, and none returns the whole set.

**6. DuckDB is locked down, but org isolation inside one shared bucket needs scoped credentials.**
Caller SQL reaches DuckDB (a raw `sqlQuery`, on the package sandbox too), and so does model text
(`duckdb.sql(...)`, `duckdb.table(...)`). Every DuckDB instance Publisher creates now runs under one
of two policies, chosen by what the instance legitimately needs
(`packages/server/src/service/duckdb_lockdown.ts`):

| Instance                                                                  | Policy                                                                                                                       |
| ------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| The per-package `duckdb` sandbox                                          | Malloy's `securityPolicy: "sandboxed"`: no network, no extension install, locked configuration, files confined to the package |
| An environment `duckdb` connection with only `setupSQL`                   | The same confinement as options (the environment directory), then its `setupSQL`, then the lock                              |
| An environment `duckdb` connection with `attachedDatabases`               | Its own setupSQL, attaches and secrets, then an allowlist (the environment directory and the URL scheme of each attached bucket), external access off, and the lock |
| A `ducklake` connection, and a storage destination served from one        | Its catalog ATTACH and scoped secret, then an allowlist of exactly its data path, external access off, and the lock          |
| A `motherduck` connection                                                 | Locked after open, like the networked readers                                                                                |
| A build, load or copy session                                             | Its own attaches, then the allowlist and external access off; not configuration-locked, and no caller or author SQL runs in DuckDB on it |

Under every policy: no file outside the allowlist (`..` and symlinks included), no ambient `AWS_*`
credentials (httpfs's env-seeded settings are blanked and its global S3 fallback is off), no
cloud-metadata or other non-allowlisted URL, no ATTACH of another catalog, no extension install or
load beyond what the instance's own setup loaded (extension autoload is off), no persistent
secret, and no `SET`/`RESET` once locked. On a locked instance, Publisher also refuses, before
DuckDB sees it, any statement that creates or drops a secret, that reaches DuckDB's logging or
profiling (which DuckDB's configuration lock does not cover), that runs SQL taken from text
(`query()`, a serialized plan, `IMPORT DATABASE`), or that introspects the instance's catalog,
settings, secrets, variables, pragmas or its own files (its file cache, spill files and
extensions), calls DuckLake's own functions, or reaches an attached
database other than through its tables (an attached database's Postgres system schemas,
`pg_catalog`, `information_schema`, `pg_toast` and `pg_temp_N`, included; DuckDB's own
`information_schema` and `pg_catalog` still read, except the `pg_settings`, `pg_database` and
`pg_prepared_statements` views over refused functions); author SQL that does any of these,
logging, profiling and SQL run from text included, is refused when the package loads. A Postgres database attached to DuckDB
(a DuckLake catalog, an attached database, a federated build source) is attached through a
temporary DuckDB secret that holds its user, password and client-key password, so its attach path carries no credential. It reads each statement with DuckDB's own
parser where the parser can serialize it, and every statement with a fail-closed lexical check
that re-reads comment and string text, so a construct read differently from DuckDB hides nothing.
Publisher's own renewal of a storage secret is the one secret statement it admits. The lockdown is part of each instance's configuration identity, so an instance is never
shared with a connection that needs a different setup or a different policy.

What it does not cover, so a multi-tenant deployment still has to:

- **Raw SQL is read-only; the connection's own work is not.** Caller and author SQL on a
  DuckDB-backed connection (the sandbox, and an environment `duckdb`, `ducklake` or `motherduck`
  connection) runs only as a single SELECT, CTEs included, decided by DuckDB's parser: at the
  connection SQL routes, and, for a model, when the package loads (a `.sql()` source must be one
  SELECT and a raw SQL expression one expression of one SELECT, or the package does not load). What still writes inside a connection's roots is the operator's `setupSQL`
  and Publisher's own work (a `#@ persist` build, a temporary table it creates around a caller's
  SELECT).
- **A single SELECT can still change state.** The gate decides by statement shape, so a SELECT
  that calls a function with a side effect is not refused: `checkpoint()`, `nextval()`, or a
  `ducklake_*` maintenance function. For a catalog that serves callers, a read-only attach is the
  control (a `ducklake` connection attaches read-only unless its config says otherwise).
- **An attached `s3` or `gcs` bucket allowlists its whole URL scheme.** These entries name no
  bucket, so a `duckdb` connection with one may reach any URL of that scheme, and the secret is the
  control: callers cannot create, replace or drop secrets on a locked instance, so the only
  credential and endpoint in force are the configured ones. Give such a credential access to the
  buckets the connection should read and nothing else.
- **`setupSQL` on a `duckdb` connection with `attachedDatabases` runs before the lock.** It runs
  first, with external access still on, because the attaches that follow it need the network.
  Allowing `setupSQL` (`PUBLISHER_ALLOW_DUCKDB_SETUP_SQL`) trusts its authors with the host for that
  connection; on a connection with only `setupSQL`, it runs confined.

- **Org isolation inside one shared bucket.** The allowlist confines a connection to its data path
  as DuckDB compares paths, and that comparison is not a security boundary for object-store URLs. A
  connection that runs author or caller SQL with a credential for a shared bucket can reach other
  prefixes that credential can read. Isolating orgs inside one bucket requires credentials the host
  scopes per org (a GCS Credential Access Boundary, AWS STS session policies, or a bucket per org),
  so the credential itself cannot read another org's prefix.
- **Network reach through a database extension.** An instance whose own setup loads a database
  extension (the Postgres catalog of a DuckLake connection, an attached Postgres, BigQuery or
  Snowflake database) keeps that extension's own client, which DuckDB's external-access setting does
  not govern. Egress policy is the control.
- **The host's credential chain.** An instance whose own setup resolves the host's identity
  (`provider: credential_chain`) can resolve it again; the allowlist still bounds what it can read
  with it. `S3_CREDENTIAL_CHAIN_POLICY` restricts which connections may use the chain at all.
- **Relative reads in the sandbox** resolve against the process working directory for DuckDB's
  permission check, so the package's top-level names are allowed there too, unless the working
  directory already holds that name. A sandbox may therefore create, but never overwrite or read
  someone else's, a file of one of those names in the server's working directory; run Publisher
  from a directory nothing else reads. The names are taken when the instance opens, so a name
  created in the working directory after that is still allowed for that instance's lifetime. A relative glob over the package's top level
  (`'*.csv'`) is refused, because it lists the working directory; a glob in a subdirectory
  (`'data/*.csv'`), a named file, or an absolute path reads.
- **Windows** has no `sandboxed` policy (Malloy refuses it off POSIX, and its `local` policy
  refuses every file). There the package sandbox is confined to its package the way an
  environment connection with only `setupSQL` is: an allowlist, external access off, and the
  lock, without Malloy's own path checks.
- **MotherDuck** connections are locked like the networked readers, but are untested: they have
  not been exercised against MotherDuck with external access off. The lock fails safe: if any step
  of it, or a catalog read after it, fails, the connection fails with an error naming MotherDuck
  and the step, and is never served half-locked.
- **Malloy's restricted mode** remains the control for caller-supplied Malloy on a shared worker:
  it refuses `.sql()`, `.table()`, raw SQL functions, `import` and `##!` flags, and the query path
  uses it via `loadRestrictedQuery`. The DuckDB lockdown bounds what the SQL that remains can reach;
  it does not decide which SQL a caller may run. An egress policy denying link-local addresses is
  the network-layer backstop.

**7. With package versioning on, an older version keeps its own governance.** A published
version serves its own files, so its own `#(authorize)` gates, row filters and `index.malloy`
curation. A gate or a hidden source added in a new version does not reach the older ones: a
request that names an older version with `versionId` is answered under that version's rules
until it is archived, and moving `latest` back rolls the rules back too. The MCP tools serve
`latest` only. Archive a version whose rules must no longer apply
([package-versions.md](package-versions.md#limits-worth-knowing)).

## If isolation gets built

The mechanism to reuse already exists, preserved out of tree from the cut custom-JSX sandbox
(see [malloyyo-dashboards-design.md](malloyyo-dashboards-design.md#custom-jsx-components-cut)): an
`<iframe sandbox="allow-scripts">` in an
opaque origin, a `default-src 'none'` CSP with `connect-src 'none'` so the guest has no network
at all, a per-request nonce for injected state, and a postMessage broker in the trusted parent
that validates each run before executing it. It was built for dashboards and cut with them.

Pointed at HTML data apps instead, it would raise the floor for the surface that actually runs
author code (§Where author code executes today). It cannot be the default: an opaque origin breaks
`credentials: "include"`, direct `fetch`, and third-party scripts, which is to say it breaks
every page written against the current contract. The shape that fits is a per-package opt-in,
where a package declares it wants isolation and its data apps talk to the broker instead of to
the API directly.
