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
- **Registering a package is an operator action.** Packages come from `publisher.config.json` or a
  `POST` to the packages endpoint. That endpoint is gated only by `frozenConfig`, so on a
  reachable server with the default config it is open — but so is the query API, and an attacker
  who can register a package can already read the data directly. Set `"frozenConfig": true` to
  close registration on a deployment where that matters. A `.zip` environment or package that
  contains a symlink entry is refused, and nothing from it is left on disk, because the
  extractor would otherwise follow the link out of the destination directory.
- **Writing a dashboard or a notebook is an operator action too.** `PUT …/models/{path}` — the
  builder's save — writes a file into a package and reloads it. It accepts two path shapes and
  nothing else, `dashboards/<slug>.malloy` and `notebooks/<slug>.malloy`, compiles the text before
  writing, and is gated by `frozenConfig` like package registration; it has no authentication of
  its own, so on a reachable server it sits behind the same gateway or is closed by the same
  setting. An attacker who can reach it can already register a package, so it opens no door that
  was shut. What a document is comes from the `kind` in its `## artifact` tag; the path only
  confines where it may live, to the top of those two directories, so a notebook can sit in
  `dashboards/` and a dashboard in `notebooks/` and the confinement is unchanged. Notebook paths
  carry one gate the dashboard paths do not: the text must carry an `## artifact` tag (an untagged
  file there is a shared include that other models import, and is refused with 400), and a tagged
  write over an existing `notebooks/` file the package does not serve as a notebook is refused with
  400, including one whose only tag is commented out, so a shared include cannot be overwritten
  into a notebook. Both path shapes then share the same post-write check: after the reload the
  compiled model must carry the `## artifact` tag and no other file may hold the name, read as
  discovery reads it, off the compiled model (off the text only for a file that does not compile);
  a tagged dashboard with no tiles saves and is not served until it has one. An untagged
  `dashboards/` file, one with no `# artifact` or `## artifact` tag in its text (an `artifact`
  property anywhere on a tag line, outside a string), is refused with 400 before compiling. A write
  whose only `## artifact` sits inside a `/* */` block comment passes that text check and compile
  and is then rolled back with a 500, so no untagged file lands in either folder; and
  a dashboard whose name another file already holds is refused with 409 before anything is
  written, since the name is its URL and its `# drill` target. The compile-first gate is per file,
  and the reload verify checks only the written model, so a model that imports the written file is
  not checked; the editor's own edits are invisible to an importer, since markdown and `run:` order
  define nothing. The text goes through the same caller-text guard as `/compile`, so a save that
  declares a real `#(authorize)` or `#(access_filter)` gate outside prose is refused with 400;
  gates live in the model file.
- **Error bodies name the server's own paths, deliberately.** A filesystem access the server
  cannot make (`EACCES`, `EPERM`, `EROFS`) answers 500 naming the errno, the operation and the
  path, and `/api/v0/status` names the config path in `initError` and the failing path in a
  `loadErrors` entry. Those are the server's own paths -- a mount the operator has to fix -- and
  the message is composed from the errno's fields, never copied from a driver or an SDK. Every
  other 5xx keeps the generic body, because its message can carry a warehouse host, caller SQL
  or a connection string, and a recorded load failure never says more than the response did.
  A caller who can reach the port can already register a package at any readable path, so
  naming the path of a refused one widens nothing; it does mean an unauthenticated reader of
  `/status` learns the layout of the server's data directory, which the gateway in front is
  expected to keep from the public.
- **Governance is mostly a modeling concern.** `#(authorize)`, `#(access_filter)`, given-scoped
  row-level access, and a package's `index.malloy` surface constrain what a _model_ exposes. They are
  real, and they are the right place to put data policy. They are not end-user authentication:
  a given is whatever the caller sends.
  One request-level exception, and it is load-bearing: `x-publisher-bypass-authorize` carrying
  the value of `PUBLISHER_BYPASS_AUTHORIZE_SECRET` skips gate evaluation on BOTH routes outright —
  the `#(authorize)` lock as well as the `#(access_filter)` filter — for trusted data-management
  callers (indexers). With that variable unset the bypass is refused, so
  the default is closed; a deployment that configures the secret and reaches untrusted callers
  should still strip the header at its edge — see
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
  lexically and again through `realpath`, and a symlink escaping the directory returns 403.

**Notebooks and dashboards carry no author-written JavaScript file.** A notebook (`notebooks/*.malloy`, or a
legacy `.malloynb`) is markdown and Malloy cells; a `dashboards/*.malloy` is Malloy plus renderer tags. Both are declarative, which
is what makes them reviewable in a pull request and agent-authorable. Keeping them that way is a
deliberate property, not an accident of scope. It is not absolute today: gap 3 below is where a
declarative artifact still carries author-controlled HTML.

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

**6. A shared worker must restrict caller-supplied Malloy itself, because DuckDB reaches the
filesystem and the network by design.** Publisher creates its per-package DuckDB sandbox with
DuckDB's defaults, so `enable_external_access` is on: a model can read a local file with
`read_text('/etc/passwd')`, write one with `COPY ... TO`, or reach an address with
`read_csv('http://169.254.169.254/...')`. On a single-tenant deployment that is not a gap -- it
is the operator using their own machine, and the same capability is what lets a package read the
CSV sitting beside it.

It becomes a gap the moment one worker process serves more than one tenant, because those reads
happen on a host holding another tenant's data. The control belongs at the layer that knows a
request is untrusted, which Publisher does not: **a multi-tenant deployment must reject
caller-supplied Malloy constructs before they reach compile.** Malloy's restricted mode is the
mechanism -- it refuses `.sql()`, `.table()`, `import` and `given:` -- and the query path already
uses it via `loadRestrictedQuery`. An egress policy denying link-local is the network-layer
backstop that survives a restricted-mode bypass.

Do not reach for DuckDB's own `securityPolicy` to close this. `sandboxed` sets
`enable_external_access=false` and adds an `allowed_directories` carve-out, but DuckDB resolves a
relative path against the process working directory for its permission check rather than against
`file_search_path` -- so `duckdb.table('data/sales.csv')`, which is how a package addresses its
own files, is refused. Measured against the connector: absolute paths pass, relative ones do not,
and `allowed_paths` does not rescue them. Publisher compiles packages in `worker_threads` sharing
one process working directory, so per-package `chdir` is not available either.

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
