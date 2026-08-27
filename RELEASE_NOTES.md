<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Release Notes

Curated release notes for `@malloy-publisher/sdk`, `@malloy-publisher/app`, and `@malloy-publisher/server` (versioned in lockstep).

## How this file is used

The `Release (NPM + Docker)` workflow (`.github/workflows/release.yml`) creates GitHub releases automatically with a standard header (NPM/Docker links) plus an auto-generated "What's Changed" PR list via `gh release create --generate-notes`. That auto list is sufficient for routine patch releases.

For releases that warrant narrative — redesigns, breaking changes, migration steps — write a `## [Unreleased]` section below, in the PR that changes the behaviour. **The release workflow does almost all of the rest**: `gh-release` appends every `[Unreleased]` section to the release page alongside the generated PR list, then pushes a `release-notes-stamp-<version>` branch restamping those headings with the version that shipped them. Nothing to paste, and nothing to remember while writing.

One step is a human's, and it is the one that bites when it is skipped: someone has to open that branch as a PR and merge it. `main` requires a pull request, so the release cannot land the stamp itself, and the job summary prints the link. Until it merges the headings still read `[Unreleased]`, which is exactly what the *next* release matches — so the narrative here lands on that release's page too, and on every one after it. Whoever cuts the release owns that click; the `publisher-release` skill makes it a step.

Both steps handle several sections, which matters because unrelated narratives accumulate between releases: they are separate entries in the same release rather than alternatives. That is precisely what the old manual process got wrong. It also simply stopped happening — 0.0.243 through 0.0.247 each shipped with none of their narrative, and the pages were backfilled by hand afterwards.

Give the heading a title — `## [Unreleased] — what changed`, with an em dash, a colon or a hyphen. The version is already the release's own title, so the marker is stripped and the title is what appears on the page; a bare `## [Unreleased]` has nothing to put there and fails CI on the PR that writes it.

Two consequences worth knowing. A section merged to `main` ships in the **next** release, whenever that is, so do not write one for work that has not landed. And a heading already stamped with a version is history: a follow-up that changes that behaviour opens a **new** `[Unreleased]` section referencing the shipped version by number, rather than editing the old one.

## Packages that version on their own line

`@malloy-publisher/skills` and `@malloy-publisher/create-malloy-package` are not part of the lockstep version above, and their notes do not belong in this file. The release workflow still publishes them: for each one it reads the version from `main` and, when that version is not yet on npm, dispatches that package's own publish workflow (`skills-npm.yml`, `create-malloy-package-npm.yml`). A package whose version is unchanged is skipped, so a release that touched neither is unaffected.

To ship one of them, bump its `package.json` on `main` and run an ordinary release. The bump is what triggers the publish and nothing else in CI requires it, so a change that lands without one is skipped and the release stays green. A prerelease skips both packages outright, since their own versions carry no hyphen and would take over the `latest` tag. Either way the release's job summary says what it skipped and why. Each package can also still be published on its own by dispatching its workflow directly, which is the point of keeping them separate: a skill edit should not need a server release.

One behaviour change to know about: `skills-npm.yml` now publishes only from `main`, matching the guard `create-malloy-package-npm.yml` already had. Dispatching it from a branch still runs `check_pack`, but the publish job is skipped, and a skipped job reports success, so check the job list rather than the run's green tick if you expected a publish. See [.github/workflows/CONTEXT.md](.github/workflows/CONTEXT.md) for the publishing rules that are easy to get wrong.

---

## [Unreleased] — a colocated persist into a non-default schema now lands there (ACTION REQUIRED)

A colocated `#@ persist name=` that names a container — `name="analytics.orders"`
rather than `name="orders"` — was materialized into the connection's **default**
container instead, on Snowflake and MySQL.

The build finishes by staging a table and renaming it into place, and it named the
rename target by its bare table name. Snowflake and MySQL resolve an unqualified
rename target against the session's current container, so the table was created in
the right place and then moved to the wrong one. Two ways it showed up:

- **Silently.** The build reported success and the manifest recorded
  `analytics.orders`, while the table sat in the default container. Anything
  serving that source then resolved a path holding no table.
- **As a nonsense error.** ``Object '"orders"' already exists`` when something of
  that name was already in the default container — while `analytics` was empty.

The rename target is now qualified on the dialects that resolve a bare one against
the session (Snowflake, MySQL, Trino) and stays bare on those that reject a
qualified one (Postgres, DuckDB, BigQuery).

**Action required.** Tables built before this release from a container-qualified
colocated persist name are in the default container, with manifests pointing at a
path that holds nothing. Upgrading does not move them. Rebuild those sources — a
forced refresh or a republish — so the table is written where the name says. The
strays in the default container are not referenced by any manifest and can be
dropped once the rebuild is confirmed.

Unaffected: `storage=` sources (a different write path), colocated sources whose
`name=` carries no container, and every dialect other than the three above.

## [Unreleased] — every DuckDB session is now bounded

DuckDB sizes its `memory_limit` from the container, at roughly 80%, and it does that **independently per instance**. Publisher runs several instances in one process — the metadata store, a serve-shape gate session, the environment lookup funnel, a sandbox per loaded package, and a disposable session for each materialization build — and none of them accounts for the resident runtime baseline or for any of the others. Measured in a 3 GiB container, three instances each reported a 2.3 GiB limit: 6.9 GiB of committed budget against 3 GiB of real memory. The process is then killed by the kernel while every instance still believes it is comfortably inside its budget, so none of them looks at fault and the growth presents as untracked native memory.

Two new settings, `PUBLISHER_DUCKDB_MEMORY_LIMIT` and `PUBLISHER_DUCKDB_TEMP_DIRECTORY`, both **opt-in and no-ops when unset**, so nothing changes on upgrade. The limit is a flat absolute value rather than a share: Publisher cannot compute one, because the number of live instances is not known when a session opens and revising the division as instances appear would shrink a live cap underneath a running query. [docs/configuration.md](docs/configuration.md) has the sizing guidance — the divisor is not a number of builds — and the reasoning behind both.

Unset now logs a startup warning naming the condition, so the oversubscription is discoverable without reading this file.

One thing worth knowing before tuning: setting a `memory_limit` does **not** by itself introduce spill on the `storage=` build path. That pipeline pushes its SQL to the source warehouse and streams the result into the destination, with nothing to spill — measured at a flat peak across a 30× range of output, with zero bytes written to the temp directory at any limit, including one tight enough to fail. An over-tight limit fails the query and leaves the process up, which is the intended trade against losing the pod.

## [Unreleased] — `#(authorize)` is an expression on the `source:` line now, not a quoted string (BREAKING)

This is the headline change of this release, and it supersedes every earlier section on this page
that shows `#(authorize) "<expr>"` on a `source:` line — including the `[0.0.248]` and `[0.0.205]`
sections below, which describe the syntax as it was when they shipped. **That syntax no longer
loads.** A gate is now an unquoted, natural Malloy boolean expression, carried by an `#(authorize)`
annotation on its own line directly above the `source:` line it gates:

```malloy
##! experimental.givens

given:
  ROLE :: string

#(authorize) $ROLE = 'analyst'
source: orders is duckdb.table('orders.parquet') extend {
  measure: order_count is count()
}
```

### What breaks

- **The string form is refused at model load.** Any `#(authorize) "<expr>"` on a `source:` line
  fails the load with **HTTP 424**, and the message names each offending source and expression and
  tells you the rewrite: drop the quotes and keep the annotation on its own line above `source:`.
  **Every existing gated package must be rewritten**; there is no compatibility mode and no flag.
- **`##(authorize)` (file-level) is refused at load** as well, including one folded in from an
  imported file. Declare `#(authorize)` on each source it was meant to protect.
- **`#(authorize)` anywhere other than directly on a `source:` line is refused at load** — on a
  `dimension:`/`measure:`/`join_*:`/`view:` line inside a source, or on a top-level `query:`. A gate
  only applies where model load looks for one: a source's own annotation, or one it inherits from an
  `extend`/query-source base.
- **A source may declare at most one `#(authorize)` annotation.** Stacking two on one source to mean
  OR is gone — a second annotation fails the load naming both. Write the disjunction out inside the
  single expression (`$ROLE = 'admin' or $TENANT = 'acme'`).

### What is validated, and what only warns

Refusals fail the whole model load (424, naming the source):

- **G1** — the annotation's payload must compile as a boolean expression.
- **G4** — every given the expression references must be declared with **no default**, wherever it
  appears in the expression — including one reached through a bare field reference
  (`#(authorize) authorized` over `dimension: authorized is $ROLE = 'analyst'`). An unsupplied given
  would otherwise resolve to its default and admit or exclude rows the gate did not mean to.

There is no separate load-time G3 check: a given the expression references that is off the model's
own given surface fails to compile the gate's own probe query, and Malloy's own error covers it. The
`unreachable_given` rejection cause still exists for the request-time resolver, which sees references
the load-time probe could not (see the membership-operand fix below).

Warnings load fine and are counted on `publisher_authorize_row_level_rejected_total`:

- **W1** (`source_line_gate_no_given_reference`) — the expression references no given at all, so it
  is a fixed predicate rather than a rule keyed on the caller. Deliberate for a locked base
  (`#(authorize) false`); worth a look otherwise.
- **W2** (`source_line_gate_negated_membership`) — a negated membership test
  (`not (org_id in $GROUPS)`). It loads and filters correctly for a non-empty given, but an **empty**
  given then matches every row instead of none. Best-effort shape match: other spellings of the same
  inversion do not trigger it, so its absence is no evidence either way.

There is no accepted-shape allowlist any more. The string form validated its expression against a
small grammar of comparison shapes before it could be attached, so `upper(region) = $REGION`,
`region like $PAT`, `region is not null` and `amount + 1 > $AMOUNTMIN` were all refused at load; all
four are legal gates today. The trade is one case that used to be a named load-time refusal and is
now a request-time warehouse error: comparing a row field to an array-typed given with `=`/`!=`
(`org_id = $GROUPS`) loads and grafts cleanly and fails when the warehouse executes the cast. Use
`in` for an array-typed given.

### A derivation can no longer silently shed a gate

An intermediate form of this feature put the gate on an annotated boolean dimension inside the
source's body. It is also refused at load, and this is why: a derivation could drop that dimension —
`extend { except: authorized }`, or an `accept:` that just did not re-list it — and produce a source
with **no gate at all**, serving **every row to every caller**, with no load error and no warning.
Malloy's compiled IR keeps no link from an `extend`-derived struct back to its base, so nothing could
refuse the load over it.

A source-line expression has no field to shed. A derivation that drops a column the gate **reads**
leaves the grafted filter unable to compile, so that entry point is **denied** rather than served
ungated. The load succeeds, and a warning names the entry point whose gate is no longer expressible
so you find out before a caller does.

One narrower hole replaces it, and is worth knowing while you migrate: drop the gated column and then
`rename:` a *different* column onto that exact name. That grafts successfully and binds the gate to
the **wrong** column. It takes a drop, plus a rename onto the exact gated name, plus colliding data,
and it fails closed unless the data collides — but do not recycle a gated column's name.

### Migrating

1. Find every `#(authorize) "<expr>"` and `##(authorize)` in your packages. Load the package — every
   `.malloy` file in the tree compiles and any failure aborts the load, so the refusal will name
   each declaring source. The one case that escapes it is a declaring file *outside* the package
   tree: nothing compiles it, so it loads and then denies every request — and it increments no
   metric, since the request-time lift failure carries no `cause` at all. That gate is invisible
   except in a debug log naming the graft target, so grep your packages rather than waiting for a
   signal.
2. Paste the rewrite: drop the quotes, and put the annotation and the `source:` line it gates
   directly adjacent, e.g. `#(authorize) $ROLE = 'analyst'` above `source: orders is …`.
3. Collapse stacked annotations into one `or` expression.
4. Read the load warnings. A derivation that narrows away a column the gate reads now denies at that
   entry point rather than leaking, and the warning is where that shows up.

See [docs/authorize.md](docs/authorize.md) for the full reference, and
[docs/authorize.md § Enforcement](docs/authorize.md#enforcement) for the per-route behaviour
(notably: `/compile` admits any gate that references no given, whichever way it resolves, so a
constant-`false` lock does not hold there and `includeSql` returns the ungrafted SQL).

### Also fixed

- **An unsupplied gate given no longer leaks its name.** A gate's givens bind with the query's, so
  Malloy's own failure named the one that could not bind — "Given 'ROLE' has no value and no default.
  To fix: supply it via `.run({givens: {ROLE: ...}})`" — reaching the caller as a 400. That is exactly
  what `docs/authorize.md` promises never happens. It now maps back to the opaque `Access denied for
  source "…"` 403.
- **A membership test checks given reachability on both operands.** The membership *candidate*
  position skipped the check every other operand position makes, so a gate naming a given two import
  hops away bound that given's declaration **default** at request time instead of the caller's value.
  It is now refused (`unreachable_given`) like every other unreachable reference.
- **A documented gate example was never valid Malloy.** `$ROLE in ['analyst', 'admin']` fails to
  compile — a list literal is not valid in that position. Write the disjunction out
  (`$ROLE = 'analyst' or $ROLE = 'admin'`), or compare a row field to an array-typed given with `in`.
- **`/compile` no longer denies a gated source unconditionally.** Denying it made a gated source
  un-authorable while protecting nothing, since the query path answers a gated source with *filtered
  rows* rather than a 403. `/compile` never runs the query, so it now admits a gate it can decide
  without running one. **"Decidable" is presence, not truth:** a gate referencing no given is
  admitted whichever way it resolves — a constant `false` included — as is one whose every given the
  caller supplied, right or wrong. Only an unsupplied given denies. `includeSql` then returns the
  **ungrafted** SQL, without the gate's `where:`, so treat `/compile` as a schema/SQL surface that a
  `false` lock does not close. `/compile` at scope `append` still denies a gated source outright: the
  run target's `SourceDef` belongs to the virtual model, so there is no graft target there.

`docs/authorize.md` is reconciled with all of the above.

---

## [Unreleased] — a proven row-level `#(authorize)` gate can now be colocated-persisted

This supersedes the "A colocated `#@ persist` on an `#(authorize)`-gated source is now REFUSED" bullet
further down this file, before that section has even shipped: unconditional refusal is no longer the
whole story. A colocated `#@ persist` (no `storage=`) is now ELIGIBLE when the compiler can prove the
gate is the entry point's own row-level filter and nothing else is reachable beneath it
(`classification: "row_level", attributed: true`) — every other shape (unattributed/join-only,
`rejected`, or no outcome at all) still refuses exactly as before. `storage=` and `#@ preaggregate`
are unaffected; they remain unconditionally refused for any `#(authorize)`-gated source regardless of
classification. See [docs/materialization.md](docs/materialization.md#authorize-gated-sources-and-materialization).

**This ships unconditionally — there is no flag.** The migration it causes is worth reading before you
upgrade. The refusal being relaxed never fired at package _load_; it fired inside the build path
(`deriveSelfInstructions` / `executeInstructedBuild`). A package with a colocated `#@ persist` on an
`#(authorize)`-gated source therefore already loads, appears in `plan.sources`, and serves live — what
422'd was its materialization run. So such packages already exist, and upgrading changes them: a run
that used to fail succeeds, and the next auto-run or scheduled build materializes the source and binds
it for serving **with no author action**. A source that served live yesterday serves from a
possibly-stale artifact afterwards, subject to the staleness below.

**What this does NOT make fresh: the row data the gate filters on, not the gate itself.** The gate
expression and the querying principal's attributes are still evaluated live, every query, against the
persisted table. Only the values in the gating column are frozen at build time, so a row whose access
decision changes (say, it changes owner) keeps serving to its former owner until the next rebuild.
Bound that with `materialization.freshness` `{ "window": …, "fallback": "live" }`: the serve path
re-evaluates freshness per query, so a stale artifact drops out of the serving set and the query
recomputes live whether or not a rebuild lands. A cadence alone is not a bound (a failed build or a
stopped scheduler serves the old decisions indefinitely), and `refresh="incremental"` is not one
either — its delta is bounded by the watermark, so a row that changes owner without its watermark
advancing is never re-read. Only a full rebuild recomputes the gate column. See
[docs/materialization.md § freshness contract](docs/materialization.md#the-freshness-contract-for-a-gated-colocated-persist-source).

## [Unreleased] — `BuildPlan.refusedSources`, and a materialization-ordering fix

**`BuildPlan` gains a `refusedSources` collection**, alongside the existing `sources` map, so a host can tell
"this package declares no persist source" from "every persist source was refused". It is a SEPARATE
collection rather than a field on `PersistSourcePlan`: constructing that plan entry calls `getSQL()` and
computes the source's content address, and a free-parameter or given-referencing source cannot reliably
survive those calls, so a refused source needs a wire shape that requires neither. Each entry carries the
source's name/sourceID/modelPath, which tier it was evaluated against (`storage` or `colocated` — the SAME
tier the build path itself would use, post the colocated row-level relaxation; see below for the later-added
`preaggregate` tier), the bounded refusal reason,
and the full refusal message. No new reason was added to the existing `free_parameter | given | authorize |
not_duckdb_portable | public_surface_unknown` enum; the two compile-time asserts this collection is computed
from can only ever produce the first three.

**Fixed: one refused, uninstructed persist source used to abort an entire orchestrated build.** The build
loop checked a source's eligibility before checking whether the caller had actually instructed it, so a
package with several persist sources — one refused, and never instructed, alongside others the caller DID
instruct — threw on the refused one and lost every source in the run, not just the one that could not build.
An uninstructed source is now skipped without an eligibility check; an instructed refused source still 422s
exactly as before.

**Also added: `SourceFailure.connectionName` / `SourceFailure.storageDestinationName`.** A consuming service
resolving a failure-only source's destination (to release a destination-scoped claim) had no discriminator
to key on and would default to colocated even for a `storage=` source that failed a rebuild — a live claim
leak on a partially-successful run. Both fields mirror their `ManifestEntry` counterparts, so a consumer
computes the same destination key (`storageDestinationName ?? connectionName`) whether the source built or
failed. **This must land before the deprecated `ManifestEntry.error`/`entries`-mirror-for-failures removal**
(see that field's own deprecation note) — a consumer still reading failures off the `entries` mirror gets
neither field until it moves to `BuildManifest.failures`.

Also added a routing-outcome label, `blocked_by_row_level_gate`, on `publisher_storage_serve_routing_total` —
previously a row-level-gated entry point that vetoed both the storage and pre-aggregation tiers recorded no
routing outcome at all.

## [Unreleased] — a gated `#@ preaggregate` rollup now reports its own refusal

`BuildPlan.refusedSources` gains a `preaggregate` tier. A gated rollup's pre-aggregation gate refuses
unconditionally when its base is `#(authorize)`-gated (rollups group away the gate column, so there is
no row-level admission the way colocated has), and that refusal was previously invisible: the rollup
still appears in `sources` (synthesis is unaffected by the gate — see the `refusedSources` entry above),
but nothing said it would never materialize. Such a rollup now also appears in `refusedSources` with
`tier: "preaggregate"` and `reason: "authorize"`, alongside its `sources` entry — the one tier where
appearing in both maps at once is the correct, intended state, unlike `storage`/`colocated` where a
refusal means absence from `sources`. A host inspecting the plan can now tell a gated rollup will 422 on
instruction before instructing it.

---

## [Unreleased] — every `#(authorize)` gate is a row filter now, not just a field-referencing one (BREAKING)

This supersedes the "a gate that references only givens is unaffected" line in the section below, before
that section has even shipped: there is no longer a separate given-only shape. A gate that reads no row
field — `$ROLE = 'analyst'`, `$LEVEL > 3`, a bare `true`/`false` — used to be evaluated by a
one-row DuckDB probe with a whole-source admit/deny answer. It now classifies and enforces exactly like a
row-level gate: the condition becomes a constant `where:` (`true` admits every row, `false` matches none),
and everything in between resolves the same as before this whole redesign started. Classification now has
only two outcomes, `row_level` and `rejected` — `given_only` is gone.

### The breaking change

- **A gate whose verdict used to deny now returns 200 with zero rows, not 403** — for the query path. A
  caller supplying `ROLE: 'intern'` against a gate of `$ROLE = 'analyst'` gets an empty result instead
  of `AccessDeniedError`. Check any consumer that keys logic on the 403 status for this class of gate; the
  correct row-level equivalent is checking for zero rows.
- **Package-level FGA denials are unaffected — still 403.** `can_read_package` and every other
  organization/workspace access check remain exactly as they were; only a gate's _own_ verdict moved to
  filtering. Do not conflate the two: a package a caller cannot read at all still never reaches the gate.
- **`/compile` now denies a row-level gate unconditionally, whether or not the given satisfies it.**
  Superseded — see the `/compile` bullet in the section at the top of this page. It now
  admits any gate it can decide without running the query, which includes every gate that references
  no given at all.
- **A negated scalar comparison (`not ($ROLE = 'blocked')`) is now an accepted gate atom.** It was
  previously reachable only because a field-less condition skipped the row-level grammar entirely; now that
  every gate goes through it, negating a single comparison is explicitly supported (equivalent to
  flipping the operator). A negated **membership test** (`not (x in $GROUPS)`) is **also** accepted,
  with a **W2 load warning** rather than a refusal — there is no accepted-shape grammar any more, so
  nothing classifies expression shape at load time. The emptying-the-set hazard is real and is what
  the warning is for: an empty given makes the negation true for every row instead of none.

### Known, accepted narrowing: the "no schema oracle" guarantee is smaller

The early, pre-compile gate could previously deny a given-only mismatch synchronously, before the caller's
own query ever compiled — so a bad field name on a locked source came back as 403, never as a Malloy
compile error naming the field. Every gate is now enforced via a compiled backstop (a graft onto a
recompiled query), so a caller's own malformed query (an unknown field, a type error) can surface its
compile error before the gate ever gets a chance to deny. This is accepted, not fixed: no query ever
executes either way, so no row data leaks — only whether a field name is recognized, which was already the
case for a genuinely row-level gate before this change.

### Fixed: `/compile` gate-stripping bypass

`/compile`'s **`file`/`append`-scope backstop** used to discover a run target's own gate by walking the
_caller's own compiled struct_, not the on-disk model's. A caller who submitted edited text with the
`#(authorize)` annotation stripped could evade that backstop entirely for a row-level-classified gate — the
early, best-effort check (`assertAuthorizedForText`) that DOES read the authoritative on-disk annotation
only _deferred_ a row-level classification rather than denying, so nothing downstream re-checked it against
the on-disk source of truth. This was closed for a given-only gate before the row-filter collapse above
(the early check denied it synchronously, no struct needed); it was not closed for a row-level one.
`Model.collectAuthorizeEntryPointGates` now folds `entryPointGatesBySource` — computed once from this
model's own on-disk `modelDef`, which caller text cannot edit — into the struct walk's result, so a gate
the caller stripped from submitted text is still found and still denies.

---

## [0.0.250] — opt-in request rate limiting

The REST server can now cap how many requests one client makes per minute: set `PUBLISHER_RATE_LIMIT=<n>` and the `n+1`th request in a minute from the same peer address gets a `429` with standard `RateLimit-*` headers. It is off unless set, so nothing changes for an existing deployment. Health probes and `/metrics` are exempt, and the MCP port is not covered. Behind a reverse proxy every client arrives from the proxy's address and would share one bucket, so rate-limit at the proxy in that deployment instead. See [docs/configuration.md](docs/configuration.md).

Also in this release, the SDK's filter UI escapes backslashes in string values before quotes rather than after, so a value containing a backslash no longer reaches Malloy double-escaped.

## [0.0.249] — a filtered aggregate can be pre-aggregated

Since 0.0.246, a measure filtering its aggregate — `paid is amount.sum() { where: is_paying }` — was refused at publish by `#@ preaggregate`, with the workaround of rewriting it as `amount.sum(pick amount when is_paying else null)`-style expressions or filtering in a view. The refusal was the fail-closed gate doing its job, not a soundness limit: the rollup computes each stored partial from the measure by name, so the filter rides into the build, and a row-level filter commutes with merging per-grain partials — filtering then merging equals filtering the whole, for every merge the feature hands out, including `count`'s (a filtered count stores a count of matching rows and still merges with `sum`).

So the gate now accepts a filter **written directly on the measure's single aggregate** (several conditions comma-separated in one `where:`), and nothing else changed shape: a filter refining a derived measure, an aggregate wrapped in a further expression (`coalesce(amount.sum() { where: … }, 0)`), a chained refinement (`{ where: a } { where: b }` — use the comma form instead), or a non-scalar condition is refused exactly as before. A filtered `avg` is still refused as `avg`. No action needed on existing packages — this only admits annotations that previously failed publish — but measures rewritten around the old refusal can return to the plain filtered form, which now also pre-aggregates.

One caution in the rollback direction: the publish gate is also a load gate, and it is package-level. A package that adopts `#@ preaggregate` on a filtered aggregate loads only on servers carrying this change — roll a server back past it and that package does not merely lose its rollup, it fails to load entirely and drops into `loadErrors`. Inherent to any gate widening, but worth knowing before adopting the new shape in a fleet that pins older images.

---

## [0.0.248] — `#(authorize)` can gate rows, not just the whole source (BREAKING)

> **Syntax note (added later):** every `#(authorize) "<expr>"` sample below is the string form, which
> is what shipped in 0.0.248. It is **retired and refused at model load** as of the dimension-form
> section at the top of this page — read that for the current syntax before copying anything here.

A gate whose expression reads no row field works exactly as before; a gate that reads one — its
own source's, or a joined source's — now filters rows instead of only admitting or rejecting the
whole source. This ships on, unconditionally — there is no flag to stage the rollout.

### Breaking changes, in the order they bite

- **A denied caller now gets 200 with zero rows instead of 403**, for a row-level gate.
  `#(authorize) "org_id in $GROUPS"` and `#(authorize) "childtable.name = $BOB"` are now valid — the
  join a joined-field gate needs is compiled in as part of the entry source's own build, before any
  caller-controlled query stage exists. This cannot affect an existing package: a row-field gate
  could not be written before this ships (it always failed the one-row probe, which has no real
  columns for it to read), so no existing caller can be relying on the 403. It matters for gates
  authors write from now on — check any consumer that keys logic on the 403 status. See
  [docs/security-posture.md](docs/security-posture.md).
- **A colocated `#@ persist` on an `#(authorize)`-gated source is now REFUSED** — superseded by the
  relaxation in the section above this one, which admits exactly the proven `row_level` + attributed
  shape; every other shape still refuses as described below. This DOES break
  existing packages — one that has this will fail to build where it previously succeeded — so it is
  worth being precise about what it does and does not close. It is **not** closing an unfiltered
  leak: measured, a colocated substitution replaces only the source's relation SQL, while the gate
  is applied as the reading query's own `WHERE`, so the two compose and rows come back filtered.
  What it refuses is authorization decided against a **frozen** copy of the gating column. The
  artifact is built once; a row whose `org_id` changes in the warehouse keeps being served to its
  old owner and stays hidden from its new one. Nor does adding a gate refresh anything — the
  content address does not include the annotation, so a pre-gate artifact stays addressable
  indefinitely while every rebuild is refused. Note also that the check's reach is deliberately
  wider than that: it also refuses a source that merely _joins_ a gated source, which entry-point
  semantics never enforced anyway. Drop `#@ persist` from the source, or move the gate to a source
  that is not materialized. See [docs/materialization.md](docs/materialization.md).
- **An `#(authorize)` in a position nothing enforces now fails the model load** — a top-level
  `query:` statement, or a field (`dimension:`/`measure:`/`view:`) inside a source. This also
  breaks existing packages, also deliberately: today such a gate silently protects nothing. Move
  the annotation to the `source:` statement it is meant to protect.
- **An `#(authorize)` on a `join_one:`/`join_many:` line fails the load, in the common case.**
  Gating a join has no effect, so this is the same misplacement as the bullet above and is refused
  the same way — move it to the joined source's own `source:` declaration. The one exception is a
  join whose target is declared beyond what this model imported (a selective one-hop import of only
  the joiner, or a source two-plus hops away): there the annotation is indistinguishable from
  Malloy's own by-reference copy of the joined source's gate, so it is ignored silently rather than
  risk refusing a correct package.
- **`##(authorize)` (file-level) is deprecated and now fails the model load.** It was a mistake to
  ship a model-wide override in the first place: the raw-warehouse path it existed to close is
  already closed unconditionally by restricted mode, so the file-level fallback protected nothing a
  source-level gate couldn't already cover, while being easy to misuse into unlocking every source
  in a file at once. A `##(authorize)` annotation anywhere in the model — including one folded in
  from an imported file — now fails the load rather than silently applying; the remedy is
  `#(authorize)` on each `source:` it was meant to protect. See
  [docs/authorize.md § Declaring Gates](docs/authorize.md#declaring-gates).
- **A near-miss `authorize` spelling now fails the model load instead of silently doing nothing.**
  `# (authorize)` / `## (authorize)` (a space after the `#`), `#( authorize )`, `#(authorize )`,
  `#(authorize)X`, `#authorize`, and case variants of the name itself (`#(AUTHORIZE)`,
  `#(Authorize)`) are not `authorize` annotations to the Malloy compiler — the spaced pair are plain
  MOTLY/render tags, the next four are malformed prefixes, and the case variants route to a name that
  is not ours — so a source carrying one has always served every row while its author read it as
  locked, and the package loaded clean. A package with one of these will now fail to load, naming the
  spelling and the fix (`#(authorize) "<expression>"` on the `source:` statement). Refusing is
  deliberate rather than interpreting the intent: honouring the spelling would mean publisher
  assigning meaning inside a namespace Malloy reserves for itself, and would silently start enforcing
  a filter on packages that served every row yesterday. In the same change, the block form
  `#|(authorize)` … `|#` and the other bracket pairs (`#[authorize]`, `#<authorize>`, `#{authorize}`)
  are now recognized as gates, because the compiler routes them there — the block form in particular
  was previously a live fail-open, unenforced at query time _and_ eligible to be frozen into a
  materialized artifact.

  **What is deliberately NOT refused:** another application's `authorize`-prefixed route.
  Classification now asks the compiler for a note's route rather than matching its text, so
  `#(authorize-v2)`, `#(authorize.audit)`, `#(authorize/v2)`, `#(authorize_v2)` and `#(authorized)`
  are valid distinct routes belonging to whoever declared them, and load untouched. An earlier draft
  of this refusal matched them as near misses and failed the whole model load with advice aimed at
  someone else.

- **Known limitation — one notebook shape fails with a 400 instead of filtering.** A cell that both
  declares a gated source and runs it in the same cell, where the gate reads a JOINED field and the
  run query does not itself reference that field, is refused rather than answered. Reference the
  joined field in the run query's own projection to avoid it. Never a leak — no rows are returned
  either way, and the 400 is only the wrong status for a request that should have succeeded with
  filtered rows. Every other same-cell shape (the first code cell, one preceded only by markdown, or
  any later cell) filters correctly.

### What changed

- **A gate that references only givens is unaffected.** Most existing gates are this kind. They
  keep the one-row DuckDB probe and the whole-source admit/deny decision unchanged.
- **The accepted row-level shape is a restricted, positive allowlist**, and anything outside it —
  including a given that isn't on the model's own given surface — is refused at package load,
  naming the reason. A broken gate never serves. For exactly what is accepted and refused, and
  why, see [docs/authorize.md § Row-level gates](docs/authorize.md#row-level-gates).

### Author-facing behavior worth knowing

- A gate on a joined field turns a `join_one` LEFT JOIN into an INNER JOIN — a parent row with no
  matching child drops out rather than surviving with nulls.
- A gate must resolve at every entry point the declaring source is reached through, and an entry
  point where it cannot is closed rather than opened. `rename:`, `except:`, and `accept:` can remove
  the field a gate was written against. Where the entry point declares its **own** gate, package
  load fails with a 424 naming the source; where it only **inherits** one, load succeeds with a
  warning and that entry point denies every request, leaving the rest of the model serving.
- Entry-point-only semantics are unchanged: a gate on a source reached only through a join still
  does not fire. A gate may now _reference_ a joined field from the entry point's own expression —
  that is not the same thing.

---

## [0.0.247] — a build that loses one source keeps the rest

A build that failed on any source abandoned the whole command: it stopped at the first failure, reclaimed the tables already written, and reported one message for the entire run. A package where one source of five had a bad grant was indistinguishable from a package that was entirely broken, and the four tables that had already materialized were dropped on the way out.

A source that fails is now recorded in the manifest with the reason it gave, and the build continues. The sources that materialized stay usable, and a consumer can tell which source failed rather than inferring it from an absent entry. A build that loses _every_ source still fails — it produced nothing, so it must not report itself as a success with errors attached. A reuse-only run, which builds nothing of its own, is unaffected.

**New response field.** `BuildManifest.failures` maps a sourceEntityId to a `SourceFailure` carrying `reason`, redacted against that source's own connection. A consumer generating a strict client from `api-doc.yaml` rejects the field until it regenerates; the key is absent on a run where every source built.

Failures are reported _beside_ `entries` rather than inside one, which is the part worth knowing if you consume a manifest. A failure carries the `physicalTableName` the source was headed for — useful for correlating with the request, and never a table to read: the build that would have created it is what failed, and a failed _rebuild_ leaves the prior generation in place under that same name, so resolving it serves stale data rather than nothing.

**`ManifestEntry.error` is deprecated.** 0.0.245 and 0.0.246 report a failed source as an entry carrying `error`, and that remains true for one more deprecation cycle: a failure is written to **both** `failures` and a mirrored entry, so a consumer reading `error` keeps working unchanged. Move to `failures` — `error` will be removed, and once it is, `entries` holds only sources that built.

Until then a consumer that resolves an entry to a table must skip entries whose `error` is set. This is not hypothetical for stored manifests either: one committed by 0.0.245 or 0.0.246 on a partially-failed build records the failed source inside `entries` with a `physicalTableName` that was never created, and that state survives an upgrade. This build drops such entries where a persisted manifest is read back (serve rebind, reuse, reference resolution) rather than binding the name.

**New metric label values.** `outcome` on the run counter gains `partial`, for a run that committed a manifest while some of its sources failed; the sources counter gains `failed`. A success-rate expression written as `success / (success + failed)` now drops `partial` into neither bucket, so a partial failure reads as a dip in volume rather than a failure. Alerting on that ratio should add `partial` to the denominator, or to the numerator's complement, depending on whether a partially served package counts as healthy for that deployment.

**One existing label value changes meaning.** `outcome="built"` on the sources counter is counted from what the build returned, where before this release it was the length of the instruction list. An instruction can be skipped without building — an incremental source whose boundary already covers the requested range, or one with no matching compiled source — and the old count reported those as built. The new figure is lower by however many a run skips, so a deployment trending `built` will see a step change at upgrade that is a correction rather than a drop in work done. The change came with the partial-failure work above; it is called out here because the counter itself predates it.

---

## [0.0.246] — measures can be pre-aggregated

A measure annotated `#@ preaggregate grain="…"` is rolled up into a stored table at that grain, and a query the rollup covers reads the small table instead of the base. Queries name the original source and nothing about them changes; the rollup is selected behind it, or bypassed, with a per-query fallback to live for anything no rollup covers.

[docs/preaggregation.md](docs/preaggregation.md) is the guide. Two limits to know before reaching for it, both of which cost acceleration and not correctness. **A query that names a `view:` does not route:** rollups are offered through a composite source, which carries its members' fields but not their views, so `run: orders -> by_category` serves live while the same query written out reads the rollup — which covers the REST `queryName` form and Console dashboards. **A query that supplies a `given:` does not route** either, since a model-level given does not cross into the synthesized model; that one is partly inherent, because a rollup is built with the givens in force at build time and could not answer a different value from stored rows anyway. Between them, a workload of named views or a filter-driven data app sees little benefit today.

**The annotation is all it takes — there is no deployment flag to enable.** Writing one is the decision to build and serve a rollup, so a package that carries no `#@ preaggregate` is untouched: nothing extra is planned, built, or compiled for it. Worth knowing before adding your first annotation, because the build is not free: a rollup is materialized like any `#@ persist` source, and a grain whose cardinality approaches the base table's spends nearly as much as the base while saving little. `buildPlan.sources` with `origin: preaggregate` is where to see what a package will build before it builds it.

**A measure may be declared at several grains, one annotation line each, and that is a cost decision.** A rollup also serves queries grouped by any _subset_ of its grain, so one rollup at `category, order_day` correctly answers by-category, by-day and grand-total queries. But a combined grain has roughly the product of its dimensions' cardinalities, so `customer_id, order_day` can approach the base table's row count and save almost nothing where either grain alone is small. Declaring both separately gives each query a small table to read, at the price of two tables to build and refresh. Rollups are grouped by grain, not by measure: ten measures sharing a grain are one table and one `GROUP BY`. Note that where two declared grains both cover a query, the one used is the first in the composite's member order, which is by generated name rather than by size — so grains are worth declaring for queries they cover _differently_, not to offer the same query a choice.

**Unusable annotations are refused at publish, and again at load.** Pre-aggregation's failure mode is an annotation that silently does nothing while the plan looks correct, so anything that cannot be built is a 400 rather than a warning. Refused: an annotation anywhere but on a measure; a measure whose aggregate cannot be re-aggregated from a stored partial (only `sum`, `count`, `min` and `max` can — pre-aggregate a sum and a count and divide them in a view instead); a grain naming anything but a dimension the source itself declares, which rules out an inline truncation like `grain="order_time.day"` (declare `dimension: order_day is order_time.day` and name that, after which coarser truncations of it route too); and a base source with a fan-out join, since `join_many` and `join_cross` can multiply rows. A `join_one` is permitted, and a measure that aggregates through one is served normally. Enforcing at load as well as at publish matters because re-aggregatability is derived from the compiled model: a Malloy version change can in principle reclassify a measure that published cleanly, and that surfaces as a package that stops loading (reported in `ServerStatus.loadErrors`) rather than one quietly paying for rollups that answer nothing.

**API.** `PersistSourcePlan` gains `origin` (`persist` for a `#@ persist` the modeler wrote, `preaggregate` for a rollup the publisher synthesized) and `preaggregate`, a `PreaggregatePlan` naming the base source, the grain's dimensions, and the measures served at it. A synthesized rollup is declared by no file, so it reports the model holding the annotations it came from, which is where an author would go to change it. Nothing about a synthesized rollup appears in model discovery: the author's model is never edited, so it still exports the source it always did.

---

## [0.0.245] — `publisher.db` picks up new columns on upgrade

An existing `publisher.db` has always picked up a new **table** added by a later build, because `CREATE TABLE IF NOT EXISTS` is idempotent. It never picked up a new **column**: that same statement is a no-op against a table that already exists, however its columns differ. So a store created before a column was introduced never gained it, schema initialization reported success anyway, and the first write naming that column failed at the binder.

**If your `publisher.db` predates 2026-06-19, every `POST .../materializations` has been returning 500** with `Binder Error: Table "materializations" does not have a column with name "manifest"`. That store now repairs itself on the next boot. Materialization is the only thing that was affected; nothing else names the column.

### What changed

- **Schema init now reconciles columns.** After the `CREATE TABLE` pass, the declared shape is compared against what is on disk and anything declared-but-absent is added. Additions only, and only for columns carrying no constraint. A declared `DEFAULT` **is** carried across and backfills existing rows.
- **What it cannot fix, it now says at boot.** A constrained column that cannot be added, or a column already present whose type, nullability, default or constraints have changed, is logged as a warning naming the column, instead of surfacing later as a binder error on an unrelated request. This is the part that keeps earning its keep after this particular column is behind us.
- **Nothing is ever dropped.** Columns and tables an older store has and this build no longer declares are left in place and reported at debug level. `materializations.build_plan` (added and removed within four days in June 2026) and the `build_manifests` table are both inert relics of this kind; removing them is a decision for an operator, not something an upgrade should do quietly.

### What is and is not carried across

`ALTER TABLE ... ADD COLUMN` in DuckDB rejects a column carrying any constraint — `NOT NULL`, `PRIMARY KEY`, `UNIQUE`, `CHECK`, `FOREIGN KEY` — with or without a `DEFAULT`. A bare `DEFAULT` is accepted. So the safe subset is not a policy this code chose; it is the boundary the engine enforces.

Constraints are read from `duckdb_constraints()` rather than inferred from nullability, which matters more than it sounds: a `UNIQUE` or `CHECK` column reports as _nullable_, so screening on nullability alone would add it as a bare column and leave the store holding the right column under the wrong rules — two servers on the same build enforcing differently depending on how their store was created. Such a column is refused and named in the warning instead.

A future column outside the safe subset needs a hand-written step, and the boot warning is what tells you the day one appears.

Constraints are also now compared on columns both sides already have, and reported the same way. A constraint added to an existing table's DDL is as invisible to `CREATE TABLE IF NOT EXISTS` as a column is, and the consequence is quieter: the older store keeps accepting rows a fresh one rejects, with nothing failing to say so.

There is still no schema-version marker, and none is needed: the comparison is against the database itself. The expected shape is not written down twice either — it is read back from a scratch in-memory database the same DDL has just been run against, so the `CREATE TABLE` statements remain the single declaration of the schema.

### On a large store

Adding a column without a default is a catalog operation, not a data rewrite — on a 5M-row, 205MB `materializations` table it took 17ms and grew the file by 0.1%. Adding one **with** a `DEFAULT` backfills every existing row, so that path is a real write: ~81ms on the same table, with a longer checkpoint. Both are trivial against a boot that compiles packages, and both happen before the server accepts traffic, but only the first is free.

### Why it took an upgrade to find

CI starts from a clean checkout with no `publisher.db`, so the create path always runs with the current DDL and the drift cannot arise. The gap was never a missing assertion — it was that no test had ever booted against a store older than the build. There is one now.

---

## [0.0.244] — a `storage=` build's warehouse read is now attributable

A `storage=` build reads its source through DuckDB's native query-passthrough, where no Malloy connector is in the call path to apply the query-metadata bag. Every such build therefore reached the warehouse untagged — the one kind of work a deployment could not attribute. It now carries the same bag the colocated path does, resolved through the same layering.

**Snowflake** takes it as a session `QUERY_TAG`; its read is unchanged. **BigQuery** takes it as `@@query_label`, which it cannot do without splitting the read: `bigquery_query()` accepts no labels parameter and cannot run the script that would set one. So a labelled BigQuery read runs as `bigquery_execute` over a two-statement script, and the anonymous result table that job wrote is then read with `bigquery_scan`. Reading that table goes through the Storage Read API and creates no new query job, so the split is not a second scan. **Postgres** has no per-statement tag and is unaffected.

**New operational prerequisite on BigQuery.** A labelled read locates its result table by listing the executed script's child job, an API surface the unsplit read never touched. A connection that cannot list its own jobs keeps the read it had before and loses attribution rather than its build — the fallback is decided by a probe issued _before_ anything runs, and counted by `publisher_storage_build_attribution_skipped_total`. Separately, and independent of tagging, the passthrough streams its results over the Storage Read API on every path: `bigquery.readsessions.create` (`roles/bigquery.readSessionUser`) is a standing requirement for materializing any BigQuery source into a storage destination.

`ManifestEntry.queryCostBytes` is populated for a tagged `storage=` build, and the full per-engine cost — billed bytes, slot or execution time, the cache flag, the warehouse's own job ids — goes to the build's log line.

---

## [0.0.250]: an incremental refresh can advance a `storage=` table

`refresh="incremental"` alongside `storage=` was a publish rejection. It is now supported, with the
same declarations and the same guarantees as a colocated incremental source: the table is advanced by
a bounded `[covered_through, frontier)` delta instead of being rebuilt.

### What changed

- **The delta spans the two engines the tier already spans.** The source warehouse computes the
  bounded range — the predicate is pushed into its own query, so it never streams rows that will be
  discarded — and the `DELETE`+`INSERT` (or `MERGE`, for a `merge_key=` source) is applied in one
  DuckLake transaction against the stored table. The table is either at the old snapshot or the new
  one; the read-only serving attach sees the new one on its next query, with no re-attach.
- **A delta's warehouse read is attributed and costed** exactly as a full build's is, through the same
  call: a `queryMetadata` bag reaches it as a BigQuery `@@query_label` or a Snowflake `QUERY_TAG`, and
  `ManifestEntry.queryCostBytes` reports what a tagged read cost. Refresh spend on this tier was
  otherwise the one kind of warehouse work a deployment could not account for.
- **`LedgerEntry` gains `storageDestinationName`.** A boundary belongs to a table, and where a stored
  table LIVES is not implied by the connection whose SQL computes it — `storage=` enters neither the
  content address nor the physical name, so nothing else distinguishes a boundary measured on the
  stored table from one measured on a colocated table of the same name. A caller holding the ledger
  should store and return it like every other field. One that does not yet: an entry without it, for a
  source this run materializes into a destination, is treated as stale and the source seeds — it is
  not rejected.
- **A CHAINED stored source still rebuilds** every refresh, reported under its own reason code
  (`chained_storage`) rather than silently. Its parent's own delta can restate rows below the child's
  frontier, where no delta of the child's would revisit them.
- **`publisher_source_build_duration_seconds` gains a `delta_storage` engine label**, kept apart from
  `delta` for the same reason `storage` is kept apart from `in_warehouse`: the two have different cost
  profiles and pooling them averages one into the other.

### Upgrading

**Every incremental source rebuilds once.** A boundary is now keyed by the store its table lives in
as well as by the connection and the name, because a source persisted colocated and one persisted
into a destination under the same name are two different tables that coexist legitimately — sharing
one row made both seed forever, each overwriting the other. `publisher.db` re-keys the ledger on
boot and discards the recorded boundaries with it, so each incremental source takes one full rebuild
and then resumes advancing by delta. Same mechanism, and the same one-time cost, as the re-key in
0.0.240.

**If you hold the ledger yourself, store the new field before you upgrade.** An entry returned
without `storageDestinationName` for a source materialized into a destination describes a different
table, so that source seeds — every run, not once, until the caller round-trips it. That is
deliberate (an entry from a caller that predates the field is stale, not wrong, so it is not
rejected) but the only signal is a repeating `no_boundary`. Update the caller's ledger storage first,
or accept full rebuilds until you do.

**A source that was rejected for declaring both keys now publishes**, and takes the same one rebuild
as any other incremental source before it starts advancing.

## [0.0.249]: a given's control contract is read off its own tags

The `Given` control contract shipped in 0.0.242 as a schema with no reader: the fields were declared and no endpoint populated them. The server now derives them from the declaration's own tags, so they are populated wherever a `Given` is returned.

### What changed

- **`label`, `control`, `rangeMin`, `rangeMax` and `suggest` are now populated,** read from the `given:` declaration's own plain-`#` tags. How a given should be presented belongs to the given rather than to any one surface, which is what lets a notebook, a dashboard and an SDK host render the same control without restating it. Those tags sit in Malloy's reserved namespace and are dropped from `annotations`, so deriving them server-side is what lets a client read them without shipping a MOTLY parser of its own. A declaration carrying none of the tags carries none of the fields, and a value the contract does not accept (`control=radio`, a non-numeric bound) is dropped the same way rather than reported.
- **`Given` gains `description`,** helper text read from a `# description=` tag. This does not replace `#(description="…")`, which still works and is still what the notebook UI renders: that form stays on `annotations`, where the client that parses it today keeps finding it. The tag form is the one that compiles without a `malformed-route` warning, since Malloy reads an annotation's route up to the first whitespace and a multi-word `#(description="…")` therefore is not well formed. Nothing renders the new field yet.

## [0.0.249]: the Console says what this server can do

The home page described a three-feature Publisher, the package page gave four of its six kinds of
content the same colour, and Publisher's in-repo reference docs had nothing linking to them.

### What changed

- **Six feature cards on the home page instead of three**, covering notebooks, dashboards, data apps,
  the MCP endpoint, ad-hoc analysis and the governance model, each linking the reference doc for it.
  The card previously titled "Notebook dashboards" named a compound of the two surfaces it straddled rather than either of them, and
  linked the publishing setup guide. A closing paragraph names connections, materialized tables and
  the REST API, which have docs but do not earn a card.
- **`DOC_LINKS` gains a `REPO_DOCS` block**, six links to Publisher's own reference docs, which live
  in the repo rather than on the docs site and for several features are the only write-up there is.
  A spec checks each target exists in the repo, case-sensitively, so a doc renamed, deleted or
  mistyped fails the test suite rather than shipping a broken card. It cannot check that a target is
  on `main` yet, which is a merge-ordering question: this change was sequenced behind the dashboards
  slice for exactly that reason, and that slice has since landed.
- **`docs/choosing-a-surface.md`**, a comparison of notebooks, dashboards and HTML data apps with a
  decision guide. `docs/malloyyo-dashboards-design.md` has referenced it since it merged; it now
  exists.
- **Every content type on the package page has its own icon and its own colour.** Four of the six
  rows had been passing the same teal from four separate call sites, so colour distinguished two
  kinds out of six. The row now derives both from one `type` prop, which is why it cannot drift
  again. The three added colours each clear WCAG's 3:1 against white, measured, since they sit behind
  a white glyph.
- **`Add Connection` is a contained button with an icon**, matching the add-triggers on the home
  and environment screens. It was the only one of the three still outlined.

### For SDK consumers

Additive only. `DOC_LINKS` is a public export (`src/index.ts`) and gains six keys; none of the
existing four changed. Everything else here is internal: `PackageItemRow` is a file-local function
whose props changed, and `ContentTypeIcon`, `ContentType`, `CONTENT_TINT` and `MALLOY_ACCENT` are
not re-exported from `components/index.ts` or `src/index.ts`, so they are not on the published
surface at all.

## [0.0.244] — queries report how they were served, and what they cost

The server measured several things and then discarded them, and the query
histogram carried two labels that grew without bound. Both are addressed.

### What changed

- **`malloy_model_query_duration` no longer labels by query text or row count.** Both are unbounded — ad-hoc text yields a new series per distinct query, a row count one per distinct result size — and a histogram label multiplies by the bucket count, so the metric grew for as long as a process served traffic. On one deployment serving real traffic the query-text label alone carried ~637 distinct values across ~14.9k series for this histogram. They remain on the request log. `environment` and `package` take their place: the only identity on the metric was previously a bare model path, which is not unique across packages.
- **`malloy_model_query_duration` now spans execution, not just preparation.** The timer stopped before the warehouse round trip, so the histogram excluded the one part its own description ("how long it takes to execute a Malloy model query") named. It now covers compile, authorize, routing, prepare and execution. **Expect every existing p95 panel to step up at deploy** — that is the metric starting to measure what it always claimed, not a regression.
- **`QueryResult` gains `servedFrom`, `executionTimeMs` and `queryCostBytes`.** A storage-served answer is byte-identical to a live one, so `servedFrom` is the only way a caller can tell a materialized source did anything; `live_fallback` is reported separately from `storage` because it is a success answered by the live warehouse, and counting it as a hit would report a healthy hit rate for a broken store. `queryCostBytes` comes from `runStats`, which the BigQuery connector already populated and nothing read.
- **`ManifestEntry` gains `buildDurationMs` and `queryCostBytes`.** The duration was already measured for the build histogram and sent upward as null.
- **`malloy_model_query_scanned_bytes`**, a counter of bytes scanned by served queries, where the backend reports them — BigQuery today.

### Reading the cost numbers

**Bytes scanned is not bytes billed.** BigQuery rounds up to a 10MB minimum per query, so a small read bills an order of magnitude above what it scanned — and materialization refreshes are mostly small reads. Every figure reported here is SCANNED. Use it to compare queries against each other; it is not a spend number.

**Null is not zero, and on the build side which paths report differs.** `ManifestEntry.queryCostBytes` is populated for a COLOCATED build, from the Malloy connection's own statistics. An incremental delta reports null because its statements do not run through a single call whose result reaches the manifest, and a chained `storage=` build reports null because it read its parent's already-materialized table and touched no warehouse at all.

A plain `storage=` build reports a figure when its warehouse read carried query metadata, and null otherwise — see the attribution section above for what makes the difference. A Postgres source reports null on every path, since the label that makes a read reportable is BigQuery's.

On the serve side, check `servedFrom` before reading a null as "free": a `storage`-served query touched no warehouse, while a Snowflake or Postgres query touched one and simply reported nothing.

### For consumers generating clients from this spec

`QueryResult` and `ManifestEntry` both gain fields. Strict generated clients reject unknown properties — openapi-generator's Java/Gson `validateJsonElement` throws on any field absent from the client's `openapiFields` — so a consumer running this server against a client generated from an older spec fails at deserialize on every affected response. **Regenerate clients in the same change as the version bump**, not after it.

---

## [0.0.243] — `storage=` builds from a Snowflake source now work (Docker image)

The 0.0.236 notes below list `snowflake_query` among the native query-passthroughs a `storage=` source is materialized through. That was true of the code and never true of the published image: **materializing a Snowflake source into a storage destination has not worked at all.** Two independent faults, both fixed.

### What changed

- **The image now carries the ADBC Snowflake driver.** The Snowflake extension is a wrapper over it, and `INSTALL snowflake FROM community` does not bring it — so every `snowflake_query()` failed at run time with `ADBC Snowflake driver (libadbc_driver_snowflake.so) not found`. It is now fetched at a pinned version, for the image's architecture, into the extension directory the runtime reads.
- **A key-pair Snowflake connection can be federated.** The passthrough required a password and emitted `PASSWORD`, so a connection authenticating by key pair — which queries fine on the live path — could not be built from at all. Key pair or password is now accepted, with a private-key passphrase when supplied.
- **`ROLE` and `SCHEMA` travel with the federated connection.** Both are part of what identifies a Malloy connection; without them a build ran under the user's _default_ role while live queries on the same connection used the configured one.

### Why it went unnoticed

Both guards were blind for the same reason. The image build verified Snowflake with `SELECT snowflake_version()` — a scalar that never touches the driver and passes without it — and the offline extension smoke test asserted only that extensions `LOAD`. The driver is a query-time dependency, so nothing that ran at build time could see it missing.

### Scope, and what is still missing

The driver is installed by the **Docker image**. A local clone or `npx @malloy-publisher/server` still has no driver, so `storage=` builds from Snowflake continue to fail there with the same error — installing it is a manual step (`dbc install snowflake`, or the extension's installer script). Closing that properly means the bake step owning the driver alongside the extensions, which is worth doing and is not this change.

### Operational notes

A failed driver fetch now **fails the image build** rather than warning and continuing. An image without the driver cannot answer a Snowflake query, so it should not leave the builder reporting success — which is how this shipped. A release built from an unchecked ref is covered by the same assertion, since it lives in the Dockerfile rather than only in CI.

---

## [0.0.236] — DuckDB/DuckLake materialization tier (`storage=`)

This section describes the tier as it stands at 0.0.236. It first shipped in 0.0.232; the disjoint-set semantics between `storageDestinations` and `connections` landed in 0.0.236.

A `#@ persist` source can now be materialized into a **storage destination** — a DuckLake declared in the environment's `storageDestinations`, a disjoint set from `connections` — instead of its own warehouse, and served back from that materialized table cross-dialect, with no model change. Off by default; see [docs/persist-storage-tutorial.md](docs/persist-storage-tutorial.md).

### What changed

- **`storageDestinations`**, a per-environment list declared alongside `connections`, holds the warehouses a `storage=` source is materialized into and served from. It is a disjoint set: a destination is not resolvable by name from a model, a notebook cell, or query text, is absent from the connection endpoints (404), and its name is independent of the connection namespace — so the same name may appear in both lists and mean two different warehouses. Only the build and materialized-serve paths resolve one. Writing the list is all-or-nothing: a create or update carrying a value that is not a list of usable destinations is refused with 400 naming every defect and applies none of it, so a destination the server could not read is never silently left unregistered; an unusable entry in the config file, or in a row restored at boot, is instead dropped with a warning so one bad entry cannot take an environment offline. See [docs/connections.md](docs/connections.md#storage-destinations).
- **`#@ persist storage=<destination>`** materializes a source into that storage destination via native per-engine query-passthrough (`postgres_query`/`bigquery_query`/`snowflake_query`); absent or `storage=source` is the unchanged in-warehouse path. The reserved connection name `source` is rejected at registration.
- **`PERSIST_STORAGE_MODE`** deployment switch (`off` default | `write-only` | `on`): a kill switch that ships dark — `off` is a no-op, and moving it down never fails a loaded package (a `storage=` source reverts to serving live and surfaces a package warning). See [docs/configuration.md](docs/configuration.md).
- **Serve from storage:** when `on`, a query against a materialized source is served from the stored table via a virtual-source transform (its dimensions, measures, materialized-target joins, and views re-declared over the stored columns); anything not reproducible falls back to serving live, so turning it on can never make a query wrong.
- **Physical tables named by `name=` verbatim.** The auto-run server names a `storage=` table by its `#@ persist name=` value (or the source name) verbatim — exactly as the in-warehouse path does — and a rebuild atomically replaces it in place (DuckLake's catalog swap is transactional). No hashed suffix, no coexisting generations, and no operator convenience view. Assigning distinct physical names per generation (for immutable generations, safe schema evolution, or rollback) is the caller's responsibility on the orchestrated build path, where the caller supplies `physicalTableName` and distributes serve bindings via `manifestLocation`. `DELETE …/materializations/{id}?dropTables=true` reclaims a storage table (destination-aware drop).
- **Chained sources reuse the parent.** A `storage=` source that reads another `storage=` source in the same destination is built by **reading the parent's materialized table** (rolled up in DuckDB), so it reuses the parent's work and is consistent-by-construction. If it can't (a parent field that isn't a stored column, a live join, or a cross-destination parent) it falls back to recomputing the upstream from raw — refused instead under `strictUpstreams`. Reported by `publisher_storage_chained_build_total{outcome}`.
- **Eligibility gate (HTTP 422 / failed build):** a `storage=` source with an unbound free parameter, a given reference (a security refusal — a frozen given-filtered table would leak rows across tenants), or a non-DuckDB-portable served shape is refused. A source protected by `#(authorize)` should also not be materialized (the served shape carries no gate); that refusal lands alongside the upstream transitive-`#(authorize)` enforcement it reuses — until then, serve authorize-gated sources live.
- **Connection type `ducklake`** (catalog + `bucketUrl` storage) — see [docs/connections.md](docs/connections.md).
- **Observability:** `storageServeBindings` on package status; `publisher_storage_serve_routing_total{outcome=storage|live_fallback|runtime_live_fallback}`, `publisher_storage_chained_build_total{outcome=parent_reuse|inline_fallback|strict_refused|infra_failure}`, and a `served_from=storage|live_fallback` attribute on `malloy_model_query_duration`, plus build/GC/eligibility counters under the `publisher` meter. `runtime_live_fallback` is the signal that the tier is broken while queries still succeed — the hit rate alone won't show it.
- **A run-time store failure honours `freshnessFallback=live`.** If a routed query fails against the stored table (a reclaimed generation a binding hasn't caught up with), a source whose binding declares `live` is recomputed live rather than erroring — the same answer the compile-time fallback ladder already gives. `fail` and the `stale_ok` default keep surfacing the error, and the decision is read from the bindings actually serving the query, so a stale sibling can't veto it.

### Operational notes

- **Multi-replica serving via the manifest.** A `storage=` source can be served across a fleet by carrying its serve binding in the same manifest the publisher already fetches from a package's `manifestLocation`: a manifest entry that names a `storageDestinationName` (with the captured `schema` and `sourceName`) binds as a cross-connection serve binding applied to the already-compiled models (no recompile); entries without it remain same-connection `tableName` substitutions (which do recompile). A refresh is the usual manifest-rebind — rewrite the manifest and re-`PATCH` `manifestLocation` — and a storage-only refresh costs no recompile. Entries are keyed by the build's content `sourceEntityId` (= the serve handle), so a freshness refresh keeps the handle and only swaps the table path, while a schema-changing generation gets a new handle. Standalone (no `manifestLocation`), serve bindings are still re-derived per-replica from the local materialization store on package load; run that single-replica. When a `manifestLocation` is set the host is authoritative and the local-store rebind is skipped, so the two binding sources never fight.
- **Roll back cleanly.** Deleting a package's materializations before rolling back to a publisher version without this tier avoids a wedge: an older build reuses/binds a persisted `storage=` manifest entry as a same-connection table it can't resolve. Building with `storage=` only ever affects deployments that turned the mode on.

## [0.0.249]: shared given and drill controls, and the notebook adopts them

The control layer behind `given:` is now one implementation instead of one per surface: state, URL encoding, `suggest`-backed pickers, and `# drill` click handling all live in the SDK, and `Notebook` is the first surface built on them. A notebook's parameters are now part of its URL, so a filtered notebook is a link you can send.

This is a **breaking release for SDK consumers**: five removals and one narrowed prop type, all listed under Migration.

### What changed

- **A notebook's parameters live in its URL.** `Notebook` takes `givens` and `onGivensChange`, and the Console wires them to the query string. Opening a notebook at `?REGION=West` runs every cell with that value on the first pass rather than running bare and running again. The host is handed the names the notebook manages alongside the values, so it can update its own query string without disturbing parameters that are not its business.
- **`# drill { to=self }` works in a notebook cell.** A cell that groups by a dimension carrying the tag becomes clickable and filters the notebook in place with the clicked value, provided the notebook declares the given the tag names: one that names a given the document does not declare stays plain rather than offering a click that cannot be honoured. Drillable cells read as links on hover, via a new mode-keyed `drillLink` theme colour. Two cases are deliberately left unmarked: a blank cell, whose click is refused anyway (a blank value is far likelier a misclick than a request for the rows that are blank), and every cell of a `# transpose` table, because the renderer lays that layout out without the per-cell `grid-column` the marking matches on. A transposed table's drill still WORKS when clicked; it is undiscoverable, which is the one place on this surface where the affordance and the behaviour disagree. A drill naming a *dashboard* destination is honoured too, now that the dashboard route exists: the cell is marked, and clicking it opens that dashboard with the value seeded. On a host that has not wired the navigation the destination stays unmarked and inert rather than painting a cell as a link to a page that answers "Nothing to open at this path".
- **`select` / `multiselect` controls backed by `suggest`.** The option list comes from an ordinary query on the governed query endpoint, so row caps and `#(authorize)` gates apply to a dropdown exactly as they do to the surface's own queries. A suggest query that fails now says so on the control instead of rendering as a dimension with no values, and the generated query carries an explicit `limit:` and ordering rather than relying on the server's default row cap to truncate it in whatever order the warehouse returned.
- **Filter values are escaped by Malloy's own filter package.** A `filter<…>` value picked in a control is now printed with `@malloydata/malloy-filter`'s `StringFilterExpression.unparse`, and read back with its parser. The previous scheme wrapped values in double quotes, which Malloy's string-filter grammar has no notion of: backslash is its only escape, so the quoting escaped nothing and several ordinary values silently meant something else. Measured against the `storefront` model, filtering its `category` dimension: a picked `-Outerwear` ran as a negation and returned 22,821 of 25,356 rows instead of the 2,535 that category holds; `%` bypassed the filter and returned all 25,356; `null` hit the null operator and returned 0; and `Ben & Jerry, Inc` was read as two brands. All of these now match themselves, pinned by a round-trip test against the real parser.
- **The notebook's controls are `given:` only.** The Filters panel that rendered `#(filter)` and `##(filters)` annotations is gone, and the notebook no longer sends `filterParams`. **This is a behaviour change for a model that uses `#(filter)`, and the deprecation note under 0.0.201 said otherwise.** Concretely: a cell fails when its run target is a source that declares a `required` filter, because the server still refuses one with no value and there is no longer a UI that can supply it. That is narrower than "every cell" in two ways worth knowing before you audit a model: enforcement is per run-target source, so cells querying a source with no filters are unaffected, and a **block-form** `#(filter) … required` is not collected at all, so it never raised the error in the first place (`source_extraction.ts` documents that gap deliberately). A model with only optional `#(filter)` annotations still runs, but is no longer filterable from the notebook. The REST parameters, the `Deprecation` header, and the server-side enforcement are all unchanged: this is the UI half of the migration landing ahead of the server half. Migrate to `given:`: [docs/givens.md](docs/givens.md) has a **"Coming from `#(filter)`"** section with a worked conversion and the three things that do not map across.
- **A cell that cannot run says why.** A failed cell used to log to the console and render as blank space, which was survivable while only a server fault could reach it. The server's own message is now shown on the cell.
- **A superseded run is cancelled, and a burst of edits only runs once.** Changing a control while a run is in flight starts a new one, and the old run's requests are aborted rather than left to complete and have their results discarded. A changed value also waits 400ms to settle before anything is dispatched, so typing into a text control runs the notebook once rather than once per keystroke. Aborting alone was not enough: it cancels the request, but cells already sent are still compiling and running on the warehouse, and their answers are thrown away.
- **Reset means "back to where this document starts".** It restores the document's declared starting values and re-runs once. It previously cleared the controls to empty and, depending on `autorun`, either did nothing at all or ran the queries twice on the way back to the starting values.

### Migration

- **`useGivensForm` and `UseGivensFormResult` are removed.** Use `useGivensState`, which additionally covers URL round-tripping and Apply batching. `GivenValue` is still exported from the package root, but see the next entry.
- **`GivenValue` no longer includes `string[]` or `number[]`.** It is now `string | number | boolean | Date | null`. The array members promised something the new URL codec cannot deliver: `givenToParam` joined a list on `,` with no escaping and `paramToGiven` never returns an array for any type, so a list did not survive the round trip, and a value containing a comma could not even be split back to the right number of entries (`["Ben & Jerry, Inc", "Nike"]` became the single string `Ben & Jerry, Inc,Nike`). Nothing in the SDK produced an array value, so this narrows a promise rather than removing a working feature. Multi-value parameters are expressed today with a `filter<…>` given, whose values are escaped by Malloy's own filter package. Code annotated `GivenValue` that holds a list will stop compiling; hold the filter string instead.
- **`Notebook`'s `onNavigate` takes a narrower event.** The signature is now `(to: string, event?: NavigationClick) => void`, where `NavigationClick` is `Pick<MouseEvent, "metaKey" | "ctrlKey" | "shiftKey" | "button">`. A `# drill` click arrives from the Malloy renderer as a DOM event rather than a React synthetic one, and this is the subset both satisfy. Function parameters are contravariant under `strictFunctionTypes`, so **an existing handler annotated `(to: string, event?: React.MouseEvent)` stops compiling** with TS2322. Widen the annotation to `NavigationClick` (exported from the package root), or drop the annotation and let it be inferred.
- **`Notebook`'s and `Package`'s `retrievalFn` props are removed**, along with the semantic-search filter they fed. `RetrievalFunction` and `DimensionFilter` are still exported for consumers rendering their own filter UI.
- **`RenderedResult`'s `onDrill` prop is replaced by `drill`.** `onDrill` was an untyped `(element: unknown) => void` handed straight to the renderer's `onClick`; `drill` is a `DrillBinding` (`{onClick, canDrill}`), which is what lets a result both handle a `# drill` click and mark the cells it applies to. `RenderedResult` is exported from the package root, so a consumer passing `onDrill` loses drill handling silently: the prop is simply not read any more. Build the binding with `useDrill` and pass it as `drill`, or pass `{onClick: yourHandler, canDrill: () => false}` to keep the old click-only behaviour with no affordance.
- **`@malloydata/malloy-filter` is a new peer dependency** (`^0.0.427`). It is already in the dependency tree of `@malloydata/malloy-explorer` and `@malloydata/malloy-query-builder`, both existing peers, so an install that satisfies the current peers already has it.
- `GivensPanel` and `GivenInput` are now exported from the package root, and `GivensPanel`'s `onClearAll` prop is `onReset`: a rename with no compatibility concern, since neither component was reachable from the package root before this release.
- **`ResolvedTheme` gains a required `drillLink` field.** It is the hover colour for a drillable cell, keyed by mode. `ResolvedTheme` is an output type: you get one from `resolveTheme` or from the theme context, so reading it is unaffected. Only code that hand-constructs a `ResolvedTheme` literal (a test fixture, say) needs the extra field. The input type, `Theme`, is unchanged.
- **`ResultsDialog` accepts a `drill` prop.** Additive. A notebook cell now passes it, so a result stays clickable when it is expanded rather than only inline.

## [0.0.242]: one meaning for `givens` across the API

`givens` had come to mean four different things: declarations, typed values, string-encoded values, and a bare list of names. It now always means a collection of `Given` declarations, and the other three have names of their own. Renames and spec corrections only; no endpoint changes what it does.

### What changed

- **`Givens` is renamed `GivenValues`,** and the string-encoded form that survives a URL is a new named `EncodedGivenValues`. `Givens` read as the plural of `Given` and was not: `Given` describes what a model _accepts_ and is always carried in a plain array, while these two are the values a caller _sends_, decoded and string-encoded respectively. No field, shape, or wire format changed on any endpoint, but **the symbol rename is breaking for a generated client that names it**, so regenerate before upgrading. Which clients those are depends on the generator, and the two we run disagree: `openapi-typescript` (our server types) emits a named `Givens` and now emits `GivenValues` and `EncodedGivenValues` instead, and the Python generator likewise emits `given_values.py` and `encoded_given_values.py` in place of `givens.py`. The axios generator behind `@malloy-publisher/sdk` inlines the map and names nothing, so an SDK consumer is unaffected.
- **`Given` now carries the control contract** wherever it is returned: `label`, `control`, `rangeMin`, `rangeMax`, and `suggest` (a new `GivenSuggest`), all optional. How a given should be presented belongs to the given rather than to any one surface, which is what lets two surfaces render the same control without restating it. The server does not populate them yet; this release lands the contract so the readers that follow have one place to write to.
- **Package warnings name their subject `subject` rather than `target`.** `target` meant the opposite of a `# drill` target: it named where a finding sits, not where anything points. Every producer feeding `Package.warnings` now uses the new key, including the materialization-config findings, whose own `MaterializationConfigWarning` type carried the old one.
- **`RawNotebook` declares what the notebook endpoint actually returns.** `type`, `modelPath`, `modelInfo`, and `queries` are on every response and were undeclared, which forced a blanket cast in the server that would have accepted a stale field name after a rename. `resource` and `path` were declared and have never been sent. It also gains `startingGivens` (`EncodedGivenValues`), the name for a document's declared starting values.
- **A `versionId` request answers 501, not 500.** Every route that declares the parameter has documented `501 Not Implemented` all along, but `NotImplementedError` had no mapping and fell through to the 500 default.

### Migration

- Regenerate clients against `api-doc.yaml`. If your generator names `Givens`, that symbol becomes `GivenValues`, and the string-encoded form becomes `EncodedGivenValues`.
- `warnings[].target` becomes `warnings[].subject`.
- `RawNotebook.path` becomes `RawNotebook.modelPath`. `path` was declared but never populated, so anything reading it was already getting `undefined`; `modelPath` is the value it wanted.
- A caller that treated a `versionId` request's 500 as a server fault should expect 501.

## [0.0.242] — `PageViewer` is now `DataAppViewer`

The SDK component that embeds an in-package HTML data app is renamed, along with the docs page for the built-in web UI. No behavior changes.

### What changed

- **`PageViewer` → `DataAppViewer`**, exported from `components/DataAppViewer`. Props are unchanged (`resourceUri`). There is no alias, so an external consumer importing `PageViewer` will fail to build.
- **`utils/pageEmbed` → `utils/dataAppEmbed`**, same contents (`PUBLISHER_RESIZE_MESSAGE_TYPE`, `PublisherResizeMessage`, `isPublisherResizeMessage`, `serverBaseUrl`, `packageFileUrl`). The move itself is invisible to consumers: the module has no `./utils/*` subpath in `exports`, so it can only be reached through the package root, and the one symbol the root re-exports, `packageFileUrl`, keeps its name and its root export. Only the path behind it changed.
- **The package view's "Governed Reports" section is now labelled "Notebooks."** Label only; the same `.malloynb` files are listed, and no prop or route changed.
- **`docs/publisher-app.md` is now [docs/console.md](docs/console.md)**, and the built-in web UI is called the **Publisher Console** throughout the docs. The `packages/app` package name is unchanged.

### Migration

- Rename the import: `import { DataAppViewer } from "@malloy-publisher/sdk"`. That is the only change an embedder needs. `packageFileUrl` is the other symbol in this area reachable from the package root, and it is untouched.

The REST `/pages` endpoint is untouched by this change and still answers at its existing path. Renaming it to `/data-apps` is a separate, breaking change with its own release note.

## [0.0.242] — Breaking: `/pages` is now `/data-apps`

The endpoint that lists a package's in-package HTML data apps is renamed, along with its schema and the SPA route that opens one. **There is no alias and no deprecation period: a caller still requesting `/pages` stops getting the listing.**

A production deployment answers it **404 as JSON**, so a client sees a clean error rather than a surprise. That is worth stating because it was not true until recently: an unmatched path under `/api/v0/` used to fall through to the SPA's catch-all and answer 200 with `index.html` on any deployment serving the bundled web UI, which would have handed a migrating client an HTML body instead of an error. #962 fixed that catch-all. (Under `NODE_ENV=development` the JSON fallback is not mounted, so the same request gets an HTML 404 from Express instead.)

### What changed

- **`GET …/packages/{pkg}/pages` → `GET …/packages/{pkg}/data-apps`.** Same response shape, same query parameters, same status codes.
- **Schema `Page` → `DataApp`**, and the OpenAPI `operationId` `list-pages` → `list-data-apps` under a `data-apps` tag. Anything generated from `api-doc.yaml` changes accordingly: in the TypeScript client, `PagesApi.listPages` becomes `DataAppsApi.listDataApps`, and `apiClients.pages` on `<ServerProvider>`'s context becomes `apiClients.dataApps`.
- **The SPA route `/{env}/{pkg}/pages/{file}` → `/{env}/{pkg}/data-apps/{file}`, and the old form still works for one release.** A bookmark or shared link using `pages/` opens the data app as before and rewrites itself to the new URL in the address bar, carrying any query string or fragment with it so the rewrite never silently drops state a caller supplied. **This alias is deprecated and comes out one release after this one.** Update stored links now rather than relying on it. The standalone URL (`/environments/{env}/packages/{pkg}/{file}`) never changed. One thing the alias does take away: a model or notebook is excluded from the rewrite, so a `.malloy` or `.malloynb` living in a package's `pages/` directory still opens in the model viewer, but a non-model file under `public/pages/` is no longer reachable at `/{env}/{pkg}/pages/<file>` and is addressed as `/{env}/{pkg}/data-apps/pages/<file>` instead. That is the same collision described below for `public/data-apps/`, and nothing in this repo ships either directory.
- **The package view's "Pages" section is now labelled "Data Apps."**

### Migration

- Change the request path to `/data-apps`. If you generate a client from the spec, regenerate it.
- If you use the SDK's API clients directly, `apiClients.pages.listPages(env, pkg)` becomes `apiClients.dataApps.listDataApps(env, pkg)`.
- Update any stored link of the form `/{env}/{pkg}/pages/{file}`. It still works in this release and stops working in the next one.

Why the REST path breaks cleanly while the browser URL gets a grace period: the two have different costs and different owners. Carrying both spellings in the spec would mean two paths, two operationIds and two generated client methods for one listing, with every future change to it made twice, and the one known consumer of the endpoint reviewed this change and chose the clean break, having already accepted the short window during a rollout where some of its machines answer 404. A bookmark has no owner to consult, and the person who saved it is not reading these notes, so that surface redirects for one release rather than failing. The endpoint is documented (in [docs/html-data-apps.md](docs/html-data-apps.md) and [docs/api-overview.md](docs/api-overview.md), both updated here), so the REST break is a real one for anyone who took it up rather than a quiet one. If that trade is wrong for your deployment, say so on the PR.

One more consequence of the SPA route move, easy to miss: the app now claims the `data-apps` segment, so `/{env}/{pkg}/data-apps/<file>` is no longer redirected to the static route. Clicking a data app in the Console is unaffected, because the listing already includes the file's path relative to `public/`. What changes is a hand-written URL of that shape: it opens the embedded viewer one segment down, on `public/<file>`, rather than redirecting. A package that itself ships a `public/data-apps/` directory is the case to know about, since its files are addressed as `/{env}/{pkg}/data-apps/data-apps/<file>`; the standalone URL `/environments/{env}/packages/{pkg}/data-apps/<file>` serves them unchanged either way. This mirrors what `public/pages/` had before, so it is not a new class of collision, but `data-apps` is a likelier directory name than `pages` was.

## [0.0.249]: dashboards render, and the Console serves them

The server half of dashboards had no UI. It has one now: a package's dashboards are listed on its
page and open at `/{env}/{package}/dashboards/{name}`, and `<Dashboard>` is a public SDK export so an
embedding host renders the same component the Console does.

### What changed

- **A dashboard page.** `<Dashboard>` reads the manifest and renders either form: a single query
  whose result is the page, or a composite whose tiles each run on their own and combine into one
  `dashboardColumns` grid. A tile owning its own query is what lets one broken tile show its error in
  place while the rest of the grid still renders. It takes props rather than reading a router, so the
  Console and an external React app differ only in what they do with `onNavigate` and
  `onGivensChange`.
- **Control state is URL state.** Filtering replaces history so Back leaves the dashboard rather than
  walking through every value tried; a `to=<slug>` drill pushes it, so Back returns to the dashboard the
  drill started from. A `to=self` drill filters in place, so it takes the same replace as any other
  control change and Back leaves the dashboard rather than undoing the drill.
  A `# artifact { autorun=false }` dashboard batches changes behind Apply.
- **A Dashboards section on the package page**, listed first because it is the at-a-glance artifact a
  visitor most likely wants, and hidden when the package has none. A dashboard's own file is filtered
  out of Semantic Models so it is listed once; untagged shared includes under `dashboards/` are not
  dashboards and stay in the model list. Notebooks are now listed by title too, with the filename as
  the secondary label.
- **`## autorun=false` is honoured in notebooks**, which had hardcoded it true while nothing produced
  the field. The server derives it now, so a notebook gets the same Apply batching a dashboard does.
- **A `# drill` naming a dashboard navigates from a notebook cell**, which completes the primitive
  that shipped inert. Where a tag offers two destinations, the click opens a menu naming the
  dashboard and the current surface.
- **Drill is reachable without a mouse.** Marked cells take `role="button"`, focus is styled the way
  hover is, and Enter or Space activates. Previously the only signals a cell did anything were a
  pointer cursor and a hover colour, neither of which a keyboard or touch user can produce. A
  drillable column takes ONE tab stop rather than one per row, with ArrowUp/ArrowDown and Home/End
  moving within it: tabbing through every cell of a result would have been its own accessibility
  problem, since a result at the row cap would have stood between the reader and everything after
  it. The stop is per drillable COLUMN within a table, so a table grouping by two drilled dimensions
  carries two, and a drill inside a `nest:`, which the renderer draws as a table per parent row, still
  contributes one stop per parent row.
- **A `select` control looks like one.** MUI hides the dropdown arrow whenever a combobox accepts
  free text, which it must here since a `suggest` returns the common values rather than every legal
  one, so a picker rendered as a plain text box and its option list was undiscoverable.

### Two things to know when authoring

- **Write `given=` on a `# drill` tag whenever the dimension is not named after the given.**
  Without it the given is the dimension name exactly as the model spells it, so
  `dimension: brand_name` looks for a given called `brand_name` and a model declaring `BRAND` does
  not match: the cell still reads as clickable and the click lands on an unfiltered page. A
  difference of case alone is forgiven only for `to=self`, where the surface resolves the name
  against the givens it declares and folds case doing it. A `to=<slug>` drill does no lookup: the
  name goes into the destination's URL as the tag spells it and the destination binds only the
  parameters it declares, spelled identically, so `given=brand` into a dashboard declaring `BRAND`
  opens it unfiltered. A `to=self` drill seeding a given no model declares is reported at load.
- **A file that does not compile fails the whole package.** Loading aborts on the first model error,
  so the dashboards endpoint answers 424 and none of that package's dashboards are served, including
  the ones that compiled. A failed `?reload=true` is refused the same way and leaves the previously
  compiled package serving, which is the behaviour to rely on while editing.

One consequence of the new route, the same one the `data-apps` rename had: the app now claims the
`dashboards` segment, so `/{env}/{pkg}/dashboards/<file>` is no longer redirected to the static
route. It has to be claimed, because a slug is a filename with `.malloy` removed and
`dashboards/report.csv.malloy` therefore publishes the slug `report.csv`, which would otherwise be
diverted as an asset and 404 on a deep link or a refresh. The case to know about is a package that
itself ships a `public/dashboards/` directory. Unlike `data-apps`, there is no viewer one segment
down to catch those: `/{env}/{pkg}/dashboards/<file>` now opens the dashboard viewer, which reports
that the package has no dashboard by that name. Address them on the standalone URL,
`/environments/{env}/packages/{pkg}/dashboards/<file>`, which serves them unchanged as it always
did.

`docs/dashboards.md` is the guide. No bundled example ships a `dashboards/` directory yet; that
arrives with the examples change that follows this one.

## [0.0.249]: dashboards are discovered and served over REST

A `.malloy` file in a package's `dashboards/` directory carrying an `# artifact` tag is now discovered at load and served over two read-only endpoints. This is the server half only: there is no UI for it yet.

### What changed

- **Two new endpoints.** `GET …/packages/{packageName}/dashboards` lists a package's dashboards, and `GET …/packages/{packageName}/dashboards/{dashboardName}` returns one manifest: the artifact tag's declarations plus the control contract derived from the givens its query references, widened to the file's surfaced set when a tile is one discovery cannot resolve. New schemas `Dashboard`, `DashboardManifest`, and `DashboardTile`.
- **There is deliberately no run endpoint.** A dashboard's query, each composite tile, and each control's suggest query all run through the ordinary `POST …/models/{path}/query` with `givens`, using the manifest's `path` as the model, so row caps, byte caps, authorize gates, and render-tag validation all apply unchanged.
- **A dashboard whose entry file is not queryable is not listed.** When a package curates its query surface (`queryableSources: "declared"`), a dashboard whose entry file is not in `explores` is held back rather than published with a manifest whose every query and given name would 404, and the omission is reported in the package's `warnings`. Being listed is not always sufficient on its own: the queryable sources are the union of every listed file's `export {}` closure, so a tile reading a source that only an unlisted file exports is still refused, and the fix is to list that file too or re-export the source from one already listed.
- **Load-time lint.** Findings for a `# artifact` tag that does not parse or does not describe a dashboard, a tile or suggest query that does not resolve, an invalid grid width, and a `# drill` naming a destination that is not a dashboard, all on the existing non-fatal `Package.warnings` surface.
- **A notebook listing carries `title` and `description`,** resolved from its own `## title="…"`, then its `#"` doc comment, then its first markdown heading. `RawNotebook` gains `autorun`, the same flag with the same default that a dashboard's artifact tag carries.
- **`Notebook` declares `environmentName`,** which the response has always sent, and drops `resource`, which it declared and never sent (issue #979).

## [0.0.208] — Single-call materialization (plan-as-artifact)

**Breaking change to the materialization API.** Materialization moves from the two-round (compile-then-build) protocol to a single call. The build plan is now a compile-time property of the package, and a build is requested in one request.

### What changed

- **New `Package.buildPlan`.** `GET …/packages/{name}` (and every endpoint/MCP resource that returns package metadata) now includes a `buildPlan` describing the package's persist sources and their dependencies. It is `null` when the package has no persist sources. This is the artifact callers read to assemble build instructions.
- **Single-call builds via `buildInstructions`.** `POST …/materializations` accepts an optional `buildInstructions` body. With no instructions the publisher self-assigns names and runs the full build, auto-loading the resulting manifest (auto-run). With `buildInstructions` (validated against the live `Package.buildPlan` at create time) it builds directly into the caller-assigned names and does **not** auto-load — the caller distributes via `manifestLocation` (orchestrated).
- **Streamlined state machine.** `PENDING → MANIFEST_ROWS_READY → MANIFEST_FILE_READY` (terminal), or `FAILED` / `CANCELLED`. The transient `BUILD_PLAN_READY` status is removed.

### Removed (breaking)

- `pauseBetweenPhases` on `CreateMaterializationRequest`.
- The `BUILD_PLAN_READY` value from `MaterializationStatus`.
- `POST …/materializations/{id}?action=build` — `stop` is now the only supported action.
- `Materialization.buildPlan` — read the plan from `Package.buildPlan` instead.

### Client / UI impact

- **CLI:** the `--pause-between-phases` flag is gone; `malloy-pub materialize --wait` settles on `MANIFEST_FILE_READY` / `FAILED` / `CANCELLED`.
- **SDK UI:** the materialization detail dialog drops the "Mode" field and now renders its build-plan view from `Package.buildPlan`.
- Regenerate any SDK/Python/k6 clients against the updated `api-doc.yaml`.

## [0.0.229] — Package locations: `~/` expands, and relative paths anchor at the config

**A relative package `location` now resolves against the directory holding the config it appears in, not the server root.** Those are the same directory whenever the config is found at `<SERVER_ROOT>/publisher.config.json`, which covers the bundled samples, every Docker recipe in [docs/deployment.md](docs/deployment.md), and any setup that `cd`s to the config before starting. Nothing changes for them. Two cases keep the server root as the anchor: the config bundled inside the published package (a zero-arg `npx @malloy-publisher/server`), and a `--config` naming a directory rather than a file.

**Who is affected:** anyone whose `--config <path>` names a file in a directory other than the server root, including a subdirectory of it, and whose packages use a relative `location`. Those packages previously resolved against the server root (the working directory, unless `--server_root` was also passed) and now resolve next to the config. Fix either way: make the `location` absolute, or move the config next to the packages it points at, which is the arrangement this change exists to support.

**The symptom is quiet.** A location that cannot be mounted is not fatal to the process: the server still reports `serving`. It does fail the whole environment the location belongs to, so that environment is skipped and none of its packages load, including the ones that resolved fine. The reason is in the log: `Error initializing environment "<name>"; skipping environment`.

**`~/` in a `location` now works.** It was accepted and then never expanded, so it resolved to a literal `~` directory under the server root and failed to mount. Expansion is unconditional and happens before any anchor applies.

See [docs/configuration.md](docs/configuration.md) for the rule and the recommended layout.

## [0.0.205] — Source access gates (`#(authorize)`)

> **Syntax note (added later):** the string and file-level forms described here, and the OR semantics
> of stacked annotations, are all **retired and refused at model load** — see the dimension-form
> section at the top of this page for the current syntax.

**Sources can now gate query access on givens.** A `#(authorize) "<bool expr>"` annotation (source-level) or `##(authorize)` (file-level) is evaluated against the request's [givens](docs/givens.md) before any query that reads the source runs; access is denied with **HTTP 403** unless at least one in-scope expression is `true` (OR semantics). Enforced on `POST /…/query`, the notebook-cell `GET`, `POST /…/compile`, and the MCP `malloy_executeQuery` tool. Malformed or invalid annotations fail model load with **424**.

**Important — this is a trusted-tier boundary, not end-user authn.** Givens are caller-asserted, so `#(authorize)` enforces policy only when Publisher sits behind a trusted tier that sets givens from verified context and the query API is network-isolated from untrusted callers. See [docs/authorize.md](docs/authorize.md) (Security model) for the deployment contract, the locked-base + curated-extension pattern, and known limitations.

## [0.0.201] — Givens

**Givens are now the recommended way to supply runtime parameters.** Models declare `given:` blocks (per [Malloy's experimental givens feature](https://docs.malloydata.dev/documentation/experiments/givens)); callers send values via the new `givens` body field on `POST /…/query` and `POST /…/compile`, the `givens` query parameter on the notebook-cell GET, or the `givens` argument on the MCP `malloy_executeQuery` tool. The notebook UI automatically renders a Parameters panel for any model that declares givens.

`filterParams`, `bypassFilters`, the matching `filter_params` / `bypass_filters` query parameters, and `#(filter)` annotations are **deprecated** and will be removed in a future release after a coordinated migration with current users. Models that use `#(filter)` will continue to work unchanged during the deprecation window; affected responses now carry a `Deprecation: true` header (per RFC 8594) pointing at `docs/givens.md`, and the server logs a one-time migration notice when such a model is loaded. See [docs/givens.md](docs/givens.md) for the migration recipe.

> **This paragraph no longer describes current behaviour.** It was accurate at 0.0.201 and stopped being so when the notebook's Filters panel was removed: the notebook no longer sends `filterParams` at all. See "the notebook's controls are `given:` only" under **"[Unreleased]: shared given and drill controls, and the notebook adopts them"** for what a `#(filter)` model does now. Named rather than described by position: there are several unreleased sections and that one is not the topmost. The REST parameters themselves are untouched.

## [0.0.197] — SDK and app UI redesign

UI redesign of the SDK's pages and shell. Type-level public APIs are unchanged; rendered DOM, CSS, and visual treatment have changed across `Home`, `Project`, `Package`, `AddPackageDialog`, and the per-cell wrappers used by `Notebook` and `Model`. External embedders should review side-by-side before upgrading.

### Component visual changes

- **`<Home />`** — left-aligned hero, three feature columns (no icons, no chips), Credible-style project list. Same `onClickProject` prop.
- **`<Project />`** — h4 page title + "Packages" section heading, compact icon-tile cards (no underline, weight 600). Same `onSelectPackage`, `resourceUri` props.
- **`<Package />`** — replaces the 3-column grid (Config / Notebooks / Models / Databases / Connections) with a sectioned list (Governed Reports / Semantic Models / Package Data) plus a back link, h4 title, and inline README. Same `onClickPackageFile`, `resourceUri`, `retrievalFn` props. Subcomponents `Config`, `Connections`, `Databases`, `Models`, `Notebooks` under `components/Package/` are no longer rendered by `<Package>` (still importable; will be removed in a future release).
- **`<AddPackageDialog />`** — outlined text fields, pill buttons, refreshed copy. Same `resourceUri` prop.
- **`CleanMetricCard`** (used to wrap `<NotebookCell>` and `<ModelCell>` query results) — border, shadow, and white background removed; cells now flow without card chrome.
- **`<Notebook />` Filter Panel** — border + shadow removed.

### Theme token cleanup

- Replaced 16 hardcoded `color: "#666666"` instances across `Notebook`, `NotebookCell`, `Model`, `ModelCell`, `ResultsDialog`, and `ModelExplorerDialog` with `color: "text.secondary"`. Icons and section titles now follow the consumer's MUI theme.
- `PackageSectionTitle` (in `styles.ts`) refactored to read `theme.palette.text.secondary` and `theme.palette.divider`. Dropped uppercase + 0.5px letterspacing.

### App shell

- The top-bar `Header` in `packages/app` is replaced with a permanent left sidebar (260/64 collapse) + 56px content header with breadcrumb chips and a `#header-actions-portal` slot. Mobile navigation moves to a drawer.
- Theme: black/off-white palette, Inter + JetBrains Mono fonts (loaded from Google Fonts), pill button shape (20px radius), 4px card radius. ABC Diatype (paid commercial) is not used.
- MUI's click ripple animation is disabled globally via `MuiButtonBase` defaultProps (deliberate, matches the flat button aesthetic). Affects only consumers wrapped by Publisher's exported `theme` (i.e. `<MalloyPublisherApp />` users); embedders rendering individual SDK components inside their own `<ThemeProvider>` keep their own ripple defaults.
- Package-detail icon tiles use Malloy brand colors sampled from `public/logo.svg`: teal `#14b3cb` (reports), orange `#e47404` (models), dark blue `#1474a4` (data).

### New internal surface

- `Package/ContentTypeIcon.tsx` — inline-SVG icon component (`type: "report" | "model" | "data"`) for branded tiles. Not exported from the package root.

### Migration

- If you embed `<Notebook>` or `<Model>` and rely on the bordered card around each result, you'll need to add your own wrapper.
- If you provide a custom MUI theme, verify `palette.text.secondary` is defined — it now drives muted icon and text colors that were previously hardcoded.
- The `MalloyPublisherApp({ headerProps })` API is unchanged at the type level (`logoHeader?: ReactElement`, `endCap?: ReactElement`), but the slots render in different DOM positions with different size constraints than they did in 0.0.x:
  - **`logoHeader`** previously rendered on the left of a horizontal top bar. It now renders in the **sidebar header** (56 px tall, 260 px wide expanded, 64 px wide collapsed). Wide horizontal wordmarks designed for a top bar may crop or disappear in the collapsed sidebar — prefer a compact mark + short label, or an icon that reads alone at 64 px.
  - **`endCap`** previously rendered on the right of the top bar next to the doc links. It now renders into the **content header portal** (right-aligned slot in a 56 px content header above the page content). The portal is global across routes, so it's intended for cross-route primary actions (e.g. a sign-in or settings button), not per-page actions.
- The `app` package now declares `@tanstack/react-query` as a direct dependency. Consumers who rely on hoisting from the SDK's peerDep are unaffected; consumers installing `app` standalone will now resolve the dependency cleanly.
