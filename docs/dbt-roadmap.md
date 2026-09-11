# The dbt roadmap

> What this is: how Malloy and dbt fit together today, where the gaps are, and the plan to close
> them. Written for teams running — or considering — dbt below a Malloy semantic layer. The
> [package format](packages.md), [materialization](materialization.md), and
> [pre-aggregation](preaggregation.md) docs describe the shipped pieces this roadmap builds on.

We believe data should be modeled and transformed **top-down** — from what the business means, down
to the details — in a language that the spreadsheet-capable part of the world, and the agents
working alongside them, can actually read and write. Replacing SQL with Malloy is like replacing
C++ with TypeScript: most code moves to the accessible language, while core infrastructure stays in
the systems language — and that split is correct, not a concession. dbt is the systems layer of
today's data stack, and the seam between dbt and Malloy is real. But it isn't fixed.

The plan, in one sentence each:

1. **Adopt** — a dbt team (especially one using the dbt semantic layer) gets a served, documented,
   agent-queryable Malloy semantic model over their existing marts, with zero disruption to their
   pipeline.
2. **Convert** — teams move more of their pipeline into Malloy over time, because it is simpler,
   more maintainable, and readable by far more people and by AI. Never all of it.

## Why data went bottom-up, and what changes

A fair objection to all of this: the non-query half of dbt — orchestration, incremental models,
snapshots, tests, contracts, seeds — has no analog in Malloy. That is true of Malloy-the-language.
It is no longer true of the platform. The Publisher and the managed platform around it ship a
declarative orchestration layer: a content-addressed build DAG with per-unit classification, scoped
and selective reruns, freshness objectives the scheduler meets, incremental refresh with watermark
and merge-key semantics, and a version lifecycle — promote, soak, rollback, archive — beyond what
dbt-core runs. What dbt *runs*, this layer already runs, declaratively, with content-addressed
reuse dbt doesn't have.

There is a deeper way to say this. dbt's bottom-up shape — hand-assembling staged pipelines from
raw tables upward — is not a preference; it is a compensation. Data languages have never had an
optimizing runtime, so engineers hand-compile business intent into materialized layers the way C
programmers hand-managed memory. TypeScript could afford to be high-level because V8 absorbed the
performance and cost implications of high-level representation. Nothing absorbed them for data —
until now.

The persistence platform *is* that runtime: declare the model top-down in Malloy, and the platform
decides what to precompute (tables, rollups, indexes), routes each query to the cheapest covering
artifact, schedules refresh against declared objectives, and — as usage-driven optimization lands —
tunes the hot paths the way a JIT does, with a deliberate division of labor: **the runtime profiles
real usage, an agent proposes the performance and cost optimizations** ("this source is queried
400×/day and scans 2 TB — persist it"), **and a human reviews and approves**. The approval step is
how the loop earns trust, not a permanent limitation — as agents prove out, it closes fully and
optimization becomes automatic. Read the gap scoring below in that frame: each closed row is not
parity with dbt; it is the runtime absorbing another thing humans used to hand-compile.

One thing the framing should not imply: the sequencing below is governance and integration first.
The runtime is why the plan coheres, not what ships next.

## Open source first

A standing rule for everything in this plan: **capability lands in the open-source Publisher; the
managed platform is the reliable, enterprise-grade way to run it.** The split test — anything that
defines *what* the system can do (a declaration surface, a build mechanic, a check) belongs in the
OSS; anything that makes that capability dependable at scale (scheduling against objectives,
cross-version identity and reuse, garbage collection, retention, fleet operations) is the managed
layer on top. The entire day-one loop below — bindings, lint, the compile gate, package variants,
the semantic-layer converter — runs from the open-source Publisher alone, no account required.

## Where the seam sits — and where it moves

| dbt keeps (the C++ core) | The seam (day one)                  | Malloy absorbs (over time)          |
| ------------------------ | ----------------------------------- | ----------------------------------- |
| EL and raw landing       | manifest → Malloy bindings          | Semantic layer — already won        |
| Python/ML transforms     | Docs flow: schema.yml → `#(doc)`    | Materialization & rollups — shipped |
| The messiest staging SQL | Drift + compile gates in CI         | Tests, contracts, seeds             |
| Its package ecosystem    | Refresh signaling                   | SCD2 history                        |
|                          | Semantic-layer takeout              | Staging, as language lands          |

The seam moves right-to-left over time: every gap closed in the third column pulls work out of the
first. The first column never empties — by design.

## The gaps, scored

Scored against what teams actually use dbt for, weighted by measured usage in production
deployments running the dbt → Malloy pattern today:

| dbt capability | Where Malloy + the platform stand | Verdict |
| --- | --- | --- |
| DAG, `ref()`, build order, selective builds | Run + build-plan DAG, scoped reruns, content-addressed reuse, freshness-objective scheduling — all shipped | **Closed** |
| Incremental models | `refresh="incremental"` with `watermark=`/`merge_key=`, on Postgres and BigQuery; late-arriving data is operator-repaired | **Closed, narrowly** |
| Materialization configs | One annotation (`#@ persist` / `preaggregate` / `#(index)`); the platform absorbs identity, scheduling, and cleanup | **Closed — simpler than dbt** |
| Test framework (severities, scoping) | Nothing today — the clearest place dbt leads. Data tests: design in progress. Unit tests (fixture → expected result): cheap once source rebinding lands | Open · design in progress |
| Enforced contracts | Really two contracts, and neither exists yet. *Upstream* (warehouse → model): compile catches shape errors, but as an error deep in a model, not at a named, diagnosable boundary — and compile alone misses a class of errors that only a dry-run against the warehouse catches. *Downstream* (model → consumers): a view's fields are API surface for dashboards and agents; a removed field compiles clean and breaks them silently | Open · lands in `lint` |
| Seeds | Package-local CSVs already work — the per-package duckdb reads `.csv`/`.xlsx` in place, versioned with the package. Missing: joinability — the CSV sits on the sandbox connection, marts on the warehouse connection, and Malloy can't join across them | **Mostly closed** |
| SCD2 snapshots | The incremental machinery — watermark ledger, stable physical naming, transactional in-place advance, retry discipline — is most of the write path. But the shipped modes can't hold history (`merge` overwrites the prior value; the no-key range replace assumes append-only sources), and the cache lifecycle — re-address → clean rebuild → collection — discards accumulated observations | Open · a write path + lifecycle exemptions, not a new engine |
| Staging query shapes (`UNION ALL`, correlated date explosion, non-linear pipelines) | `connection.sql()` sources are already persistable, so staging SQL can be lifted verbatim and gain the orchestrator immediately | Open · bridged; closed only by language work |
| Feature-flag DAG pruning, dispatch hooks, templating | Givens cover value substitution; per-tenant variance requires an external build step today | Open · package variants |
| Macros & packages (reuse across projects and tenants) | npm-style package dependencies for Malloy packages — versioned, declared, lockfile-resolved — are being scoped; override sources (a package declares a typed default, a consumer rebinds it) follow; expression-level macros are language work | Open · package dependencies + override sources |

**The verdict:** the remaining gap is four things — governance (tests, contracts, seeds), state
(SCD2), staging expressiveness, and variance. Each one closed converts another slab of dbt pipeline
into declarations the platform runs. That is what makes conversion a staged engineering program
rather than a hope — and it is what "90% of dbt's power at 10% of the complexity" requires us to be
honest about: the complexity claim already holds (one annotation, optionally one freshness window);
the power claim is honest only with the missing 10% named. This roadmap is the plan for shrinking
that 10%.

## A. Adopt: the dbt on-ramp

- **Semantic-layer converter.** Read dbt's semantic manifest — semantic models, entities,
  dimensions, measures, metrics — and emit Malloy sources, measures, and views over the same marts.
  Your metrics become queryable by any agent over MCP, explorable in Publisher's UI, and readable
  by the analysts who could never read the YAML — with zero warehouse changes and dbt untouched.
- **Reconciliation harness: prove the number matches.** Store the incumbent hand-written SQL metric
  definitions and continuously check Malloy's output against them. "The new number matches the old
  number" is exactly what stalls semantic-layer and BI migrations; a working reconciliation harness
  turns the cutover from a leap of faith into a verified operation — and doubles as the acceptance
  test for the converter.
- **A "Malloy for dbt users" guide** that speaks dbt's language — `ref()` to bindings, metrics to
  measures, exposures to views — and is honest about the seam. The accessibility argument is the
  point: the part of the world that works in spreadsheets, and the agents working alongside them,
  can learn Malloy; they will never learn SQL or metric YAML.

## B. Integrate: make the seam first-class

Every team running dbt + Publisher today has had to build the same harness: a bindings generator,
CI gates, a per-tenant package producer, refresh coordination. None of it is differentiated work a
team should own. All of it becomes product:

- **Honest CI signals.** Today the readiness endpoint returns 200 even when a package failed to
  compile, so CI has to poll per-package model endpoints. Fix: `publisher --check` boots, compiles
  every package, prints per-model errors, and exits non-zero; a strict health variant returns 503
  while any package is failed or serving stale.
- **`malloy-pub dbt bind` / `dbt check`.** Generate the physical-binding layer from dbt's
  `manifest.json` — one binding source per selected model, with dbt column descriptions carried
  over as `#(doc)` annotations so documentation is authored once and flows to the agent — plus a
  regenerate-and-diff drift gate. Hand-authored Malloy never names a dataset; schema drift stays a
  reviewable file diff.
- **`malloy-pub ci compile`.** The full contract gate as one command: build seeded data into an
  ephemeral dataset, boot the Publisher against the working tree, assert every model compiles
  against the real warehouse schema, tear down. Bindings are relation-level; a renamed *column*
  only surfaces when Malloy compiles against real tables, so this is the only honest check for that
  class of drift. Ships with a reference CI workflow and a paired dbt example package.
- **`malloy-pub lint`.** A check framework seeded with contributed community checks: snapshot tests
  on view output spaces (the downstream contract — a removed field compiles clean and breaks every
  consumer; first to land), and a warehouse↔model schema contract with a dry-run pass (the upstream
  contract, for teams without dbt contracts guarding the boundary). Lighter rules — like label
  hygiene for synthetic fallback values — land as review rubrics in the open-source skills. Two
  adjacent issues are Malloy bugs to fix upstream rather than lint around: an order-by field
  outside the view's output space, and duplicate source definitions across files being silently
  dropped instead of erroring.
- **Refresh signaling.** A documented dbt `on-run-end` hook that calls the scoped-runs API for the
  sources whose upstream models just rebuilt: "dbt finished → re-materialize and re-index what
  changed," with data-freshness genuinely tracking the pipeline instead of a cron guessing.
  Orchestrator-agnostic — the same endpoint serves Airflow, Dagster, and SQLMesh.
- **Package variants.** One authored package, many compiled subsets. Capability flags declared per
  environment or tenant; the variant contains every model whose import closure is satisfied by the
  enabled bindings, and nothing else — the model set is *derived, not configured*, so the dbt
  configuration and the Malloy configuration cannot drift apart. Binding scoping fails closed (a
  package that would fail to compile can never ship), with injection points for per-tenant
  documentation and synonyms, and environment-resolved bindings so the dev→prod dataset swap is
  configuration.
- **Package dependencies and override sources.** The dbt pattern of tenant-specific projects over
  a shared package needs two things. *Dependencies:* an npm-style mechanism — a package declares a
  versioned dependency on another, resolved through a lockfile — so a conformed model is published
  once and consumed by many tenant packages. This is being scoped now, and "npm-style" is
  deliberate: the model is npm's (declared, versioned, lockfile-resolved, registry-backed); whether
  it rides on the npm registry itself is part of the scoping. *Override sources:* the shared
  package declares a typed default source (often empty-but-typed) and the consuming package
  supplies its own. That is what makes a shared dependency usable across tenants whose physical
  bindings differ — the conformed model is imported, the bindings are local — and it is dbt's
  macro-override pattern at the source level. Expression-level macros (one transformation applied
  across many fields) have no Malloy unit to publish today; that is language work, in Lane B.
- **Record the lockstep.** Stamp the upstream transform's git ref or manifest hash into the package
  at publish and surface it, so "the dbt build and the served Malloy package are on the same ref"
  is a recorded, checkable fact instead of a convention in someone's Terraform.

## C. Convert: move the seam down

Staged, cheapest first, each stage independently valuable.

**What incremental looks like.** One mart at a time, with dbt running below throughout. Bind to
the dbt mart today; when ready, lift its staging SQL verbatim into `connection.sql()` sources and
let the orchestrator run it; put tests on it; then Malloy-ify the SQL as the language lands. No
step requires the previous mart to have finished its journey. What gates the *first full cutover*
of a pipeline is Stage 1, Stage 2, and the reuse gap above — tests, snapshots, and shared packages
are what a team cannot leave dbt without — which is why they are ordered first.

### Stage 1 — Governance: tests, contracts, seeds

The cheapest stage, and the design requirements come from teams whose test suites made the layer
work rather than just exist: *severity is a routing decision* (fail where the model owner owns the
fix — grain violations, model drift; warn where the data owner does — source gaps; a permanently
red gate stops being read); *scope counts to the actionable subset*, so a number means "entities
affected now," not a count that grows on its own; and *the warn stream is a product surface, not
logs* — findings should land in the knowledge an analyst agent retrieves, so it can explain why a
number looks off. Contracts become typed, named checks at both edges of the model: upstream,
binding sources validated against the warehouse schema at a diagnosable point; downstream, view
output spaces held stable for the dashboards and agents consuming them. Both start life in `lint`
and graduate to publish-time enforcement. Seeds need one mechanic: a package CSV is already a
versioned, human-editable, servable table — it needs to be pushable into the warehouse connection
as a managed, content-addressed table (rebuilt when the file changes) so it can join the marts it
exists to enrich, which is what policy lookups are for.

Unit tests are a separate, cheaper thing. A Malloy model is pure declarations over sources, so a
unit test is a fixture CSV per source plus an expected result, run on the per-package duckdb
sandbox that already reads package files in place. It lands in `lint` alongside data tests and
reuses the same source-rebinding mechanism as package variants. Two honest limits: dialect-specific
expressions and `connection.sql()` sources test on duckdb, not the warehouse; and a test cannot
cross the sandbox/warehouse boundary, which is why the rebinding has to exist first.

### Stage 2 — State: SCD2 as a realization mode

Point-in-time analysis rests on history that source systems don't retain, and production
deployments of this pattern carry dozens of snapshot definitions each. The incremental machinery
already provides most of the write path — the ledger, watermark handling, stable physical naming,
transactional in-place advance, scheduling. What's genuinely new is two things. **A history write
path:** close-out + insert with validity columns and a current-row marker, supporting a timestamp
strategy *and* a check strategy (no source's updated-at is trustworthy until proven), plus
hard-delete close-out — none of which the shipped delta modes provide. **Lifecycle exemptions:** a
history artifact is *primary data, not a cache* — a definition change must carry history forward
instead of rebuilding from current state, collection must never reclaim the only copy, and
retention becomes a stated posture. Both land in the open-source Publisher, which already owns the
incremental ledger: the write path, and the invariants that make history safe — history artifacts
are exempt from collection and re-addressing, and a definition change carries history forward or
refuses rather than rebuilding from current state. As with every other mechanism here, the
Publisher ships a basic refresh schedule and exposes the run and lifecycle mechanics over the API
for external orchestration; the managed platform layers retention policy, backups, and freshness
objectives on top.

### Stage 3 — Staging: SQL sources now, language later

Two decoupled lanes, so conversion isn't gated on language design:

- **Lane A, platform, near-term:** bless `connection.sql()` sources as the staging idiom. They are
  already persistable, so a team lifts staging SQL *verbatim* out of dbt models and immediately
  gains content addressing, generations and rollback, freshness objectives, scoped reruns, and
  (Stage 1) tests gating cutover. This replaces dbt-the-runner without waiting for Malloy to grow
  set operations. One spike first: confirming dependency edges arise between SQL sources, so a
  pure-SQL staging DAG builds in order rather than flattening to unordered roots.
- **Lane B, language, long-term:** the Malloy language work, in measured-usage order — **set
  operations** first (the single disqualifier for staging: multi-source entities, taxonomy
  unpivots, past/future splits are all `UNION ALL`), then **correlated table functions** (per-row
  date explosion), then **non-linear pipelines** (a named stage consumed by several branches), then
  **expression-level macros** (one transformation declared once, applied across many fields), then
  quality-of-life (a `QUALIFY` equivalent, richer ordering, function breadth). Each feature that
  lands retires a class of raw SQL from Lane A: **Lane A makes conversion possible; Lane B makes it
  idiomatic** — and readable by the audience this whole plan serves.

### Stage 4 — Variance: per-tenant compilation as product

Builds on package variants and override sources (section B): feature-flag pruning becomes
capability-flag variants; dispatch hooks become override sources. Most templating dissolves along the way: value substitution
is already covered by givens and parameters, and defensive compile-time introspection exists mostly
because inputs lacked contracts — which Stage 1 fixes at the root. The honest expectation: a thin
per-tenant compile step survives every version of this plan. The goal is that the product does it,
driven by declarations, not a team-maintained build system.

## Non-goals

- **Total conversion.** The C++/TypeScript analogy is the policy: dbt and SQL keep the core they
  are genuinely better at — EL, Python and ML transforms, the package ecosystem, the gnarliest
  staging. We minimize that core because whatever stays there is inaccessible to most people and to
  agents; we never pretend it reaches zero. The boundary moves from "dbt builds everything below
  the semantic layer" to "the platform builds everything above raw landed tables."
- **Absorbing your control plane.** Tenant registries, infrastructure-as-code, and promotion policy
  stay yours. The Publisher consumes their outputs and provides the hooks.
- **Running dbt.** The platform reads dbt's artifacts and accepts its signals; it never invokes dbt
  builds or manages dbt's credentials.

## Sequencing

| Horizon | Items |
| --- | --- |
| Now | Honest CI signals · `dbt bind`/`check` · `lint` skeleton + contributed checks · refresh hook · the "Malloy for dbt users" guide · fold the severity/scoping/warn-stream requirements into the assertions design |
| Next | Semantic-layer converter + reconciliation harness · `ci compile` + example package · package variants · npm-style package dependencies · override sources · unit tests in `lint` · warehouse-joinable seeds · SCD2 design doc · SQL-source dependency-edge spike |
| Later | Language features in measured-usage order · full staging conversion (SQL sources → progressive Malloy-ification) · expression-level macros |

## Open questions — input welcome

Several of these are aimed squarely at teams running the dbt → Malloy pattern in production today;
answers will shape the design.

- **Binding shape.** Should generated bindings be bare `table()` sources, or carry more — column
  pins, an access-modifier default — so hand-authored tiers can assume less?
- **Selector surface.** Which subset of dbt's node-selection syntax matters in practice for
  choosing marts — tags, paths, groups?
- **Knowledge injection.** Does per-tenant documentation and synonym injection deserve a
  first-class package mechanism, or is splicing above matching source declarations the right level?
- **Manifest gates.** Feature-flag derivation needs a manifest with every input enabled; which
  build's manifest should it read?
- **Converter coverage.** Which dbt semantic-layer constructs must round-trip for the takeout to
  feel complete — derived metrics, cumulative metrics, saved queries — and where is the line beyond
  which the answer is "model it natively"?
- **SQL-source dependency edges.** Confirm build-plan edges materialize when one SQL source
  consumes another persist source; this decides how much of dbt's `ref()` Lane A replaces.
- **Where tests are declared** — annotations in the model, or a separate spec file.
- **The OSS-local floor.** Which managed behaviors get a lightweight single-node analog in the
  open-source Publisher — a local schedule, a local ledger, simple cutover — versus staying
  platform-only?
