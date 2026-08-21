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
- **Package content is first-party code.** A package's models, notebooks, and `public/` files are
  treated as code the operator chose to run, the same way you would treat your own web app
  deployed on your own origin. Publisher does not scan, sandbox, or vet them.
- **Registering a package is an operator action.** Packages come from `publisher.config.json` or a
  `POST` to the packages endpoint. That endpoint is gated only by `frozenConfig`, so on a
  reachable server with the default config it is open — but so is the query API, and an attacker
  who can register a package can already read the data directly. Set `"frozenConfig": true` to
  close registration on a deployment where that matters.
- **Governance is mostly a modeling concern.** `#(authorize)`, given-scoped
  row-level access, `explores`, and `queryableSources` constrain what a _model_ exposes. They are
  real, and they are the right place to put data policy. They are not end-user authentication:
  a given is whatever the caller sends.
  One request-level exception, and it is load-bearing: `x-publisher-bypass-authorize: true` on a
  query request skips `#(authorize)` evaluation outright, for trusted data-management callers
  (indexers). Publisher bounds nobody, so a deployment reaching untrusted callers **must** strip
  that header at its edge — see
  [authorize-bypass-deployment.md](authorize-bypass-deployment.md). It is the one place where a
  request, not a model, decides whether governance applies.

The corollary that keeps coming up in design review: **a feature cannot be made safe by
sandboxing it if an equivalent capability is available unsandboxed next to it.** Isolation is
worth building when it closes a boundary, not when it decorates one of several open doors. This
is why the custom JSX dashboard sandbox was cut after it was built and working — see
[malloyyo-dashboards-design.md](malloyyo-dashboards-design.md#custom-jsx-components-cut).

## Row-level authorize: rows are protected, the schema is not

An `#(authorize)` gate that reads a row field (see
[authorize.md § Row-level gates](authorize.md#row-level-gates)) filters rows instead of admitting
or rejecting the whole source. It is a deliberate trade, stated plainly rather than left to be
discovered:

- **Rows are protected; the schema is not.** A row-gated source is readable-but-empty rather than
  403 for a caller the gate admits nowhere. Any caller with package read can therefore name a
  gated source, compile against it, and enumerate its columns through compile errors. That is
  accepted on purpose — resolving a gate out of untrusted text before compiling it is exactly the
  resolution-from-text this design already refuses elsewhere (see
  [authorize.md § Security model](authorize.md#security-model)).
- **403 becomes 200-with-zero-rows.** A caller a whole-source gate would have rejected outright
  now gets a successful, empty response from a row-level gate instead. That is wire-visible: a
  consumer keying its own logic on the 403 status must be checked and updated before upgrading a
  deployment it serves to a version carrying row-level gates.
- **Fail-closed is the only backstop.** A row filter has no boolean admission to fall back on the
  way a whole-source gate does, so every path that cannot *apply* the filter denies instead — a
  gate whose column doesn't resolve at the entry point, an unresolved given, a compile that
  throws. There is no "serve unfiltered" failure mode.
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

**Notebooks and dashboards carry no author-written JavaScript file.** A `.malloynb` is markdown
and Malloy cells; a `dashboards/*.malloy` is Malloy plus renderer tags. Both are declarative, which
is what makes them reviewable in a pull request and agent-authorable. Keeping them that way is a
deliberate property, not an accident of scope. It is not absolute today: gap 3 below is where a
declarative artifact still carries author-controlled HTML.

## Known gaps

These are open, ordered by how much they would matter on a deployment that has put a gateway in
front of Publisher. None are fixed as of this writing.

**1. Everything Publisher serves is framable by any origin, and the knob that looks like it fixes
that only covers part of it.** In-package HTML gets `Content-Security-Policy: frame-ancestors *`
by default, a standing clickjacking vector for any page with a control worth clicking.
`PUBLISHER_FRAME_ANCESTORS` narrows that — but only for files under a package's `public/`. The
Console catch-all sets no framing header at all and there is no global `X-Frame-Options`, so
notebooks, dashboards, models, and the Explorer stay framable from anywhere on a deployment that
has set the variable. That is worse than a permissive default, because setting the variable
implies a coverage it does not have. Two fixes, in order: apply one policy to every document
([#930](https://github.com/malloydata/publisher/issues/930)), then reconsider the default —
`'self'`, with embedding opt-in per deployment, costs embedders one env var and closes this for
everyone else.

**2. There is a token-shaped thing that authenticates nothing.** `Publisher.embed` appends an
`embed_token` query parameter, and `Publisher.setToken` attaches an `Authorization: Bearer`
header — and no server code reads either one. The docs describe signed embed tokens as a next
step, so this is unfinished rather than broken, but an affordance that looks like authentication
and is not is worse than its absence: it invites an integrator to believe a page is protected.
Either verify it or remove it until it can be verified. It also gates widening embedding to more
surfaces ([#931](https://github.com/malloydata/publisher/issues/931)).

**3. Package markdown is rendered with raw HTML parsing enabled.** `markdown-to-jsx` runs with
its default `disableParsingRawHTML: false`, so raw HTML in a package's markdown becomes JSX.
React will not execute an inline `<script>` this way, and link `href`s are already scheme-checked
precisely because packages can come from untrusted git or S3 sources — so this is a narrow
surface, not an open one. Still, it is the one place a declarative artifact touches
author-controlled HTML, and it is worth either disabling raw HTML or sanitizing deliberately.
Three call sites, not one: notebook cells, workbook cells, and an environment's About panel
(`NotebookCell.tsx`, `MutableCell.tsx`, `About.tsx`). None passes the option, so fixing one and
calling it done would leave the other two open.

**4. Resize messages are not origin-checked.** Both the in-page host runtime
(`packages/server/src/runtime/publisher.js`) and the Console's data-app viewer
(`DataAppViewer.tsx`) validate `event.source` against the iframe's `contentWindow` but never
`event.origin`. Source-matching is the stronger of the two checks and the payload is a single
number, so the exposure is bounded, but the check is one line.

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
