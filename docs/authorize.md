<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Authorize (Source Access Gates)

> What this is: how `#(authorize)` annotations gate *who* may query a source and *which rows* they
> see. Every gate is enforced as a row filter, so a denial is usually a 200 with no rows rather than
> a 403 — see the note on denial shape below.
> Runnable example: [examples/governed-analytics](../examples/governed-analytics). For the base
> mechanism, see [givens.md](givens.md); for row scoping, see [row-level-access.md](row-level-access.md).

`#(authorize)` is the **source-authorization** application of [givens](givens.md). **Every gate is enforced as a row filter**, whether or not its expression happens to read a row field: the gate's condition is grafted onto the source the query enters through and evaluated by the same query. A gate that reads no field (`$ROLE = 'analyst'`) is simply a predicate that is constant across every row, so it still behaves as a whole-source admit or deny — it is just reached by the same mechanism. See [Row-level gates](#row-level-gates) below. To scope *which rows* a caller sees using a plain `where:` rather than the gate itself, see [Row-level access](row-level-access.md).

`#(authorize)` annotations gate query access to a Malloy source based on the request's [givens](givens.md). A source with no in-scope annotation is unrestricted.

**What a denial looks like on the wire follows from that.** A gate is applied to the query as a row filter, so a caller it admits nowhere gets **200 with zero rows** rather than a 403. A **403** is what you get when the gate cannot be applied at all — the entry point renamed/dropped the field it reads, or a given it names was not supplied. Both are denials and neither returns a row the caller may not read, but only one is visible as a status code: anything keying on 403 to mean "denied" — an alert, a retry rule, a client branch — will not see a filtered-to-nothing denial at all.

> ⚠️ **Read [Security model](#security-model) before deploying this as an access control.** Givens are **caller-asserted**: anyone who can reach the query API can claim a favorable given. `#(authorize)` is only a real boundary when the API sits behind a trusted tier that sets givens from its own verified context. It is not, on its own, end-user authentication.

For the Malloy expression reference, see [Malloy: Expressions](https://docs.malloydata.dev/documentation/language/expressions). For givens, see [givens.md](givens.md).

## Declaring Gates

A gate is an `#(authorize)` annotation on its own line directly above a `source:` line — an unquoted, natural Malloy boolean expression over declared givens (`$NAME`), row fields, or both:

```malloy
##! experimental.givens

given:
  ROLE :: string

#(authorize) $ROLE = 'analyst'
source: orders is duckdb.table('orders.parquet') extend {
  measure: order_count is count()
}
```

- **A source may declare at most one `#(authorize)` annotation.** Declaring a second on the same source fails the load naming both. See [OR semantics](#or-semantics) for how to combine more than one condition.
- **`#(authorize)` only gates from the `source:` line.** The same annotation on a `dimension:`/`measure:`/`join_*:`/`view:` line inside the source, or on a top-level `query:`, is never enforced from there — it fails the load naming the position instead of silently protecting nothing. See [Enforcement](#enforcement).
- A source with no `#(authorize)` annotation of its own or inherited is **unrestricted**.
- **A gate may also be a bare reference to a boolean field**, for a condition too long to read comfortably on one line: `#(authorize) authorized` over an ordinary `dimension: authorized is org_id in $GROUPS` declared elsewhere in the source. Validation follows the reference through to the dimension it names — a given the dimension reaches is checked exactly as if the gate had written the expression out directly.

> **`##(authorize)` (file-level) is deprecated and refused at load.** An earlier version of this
> feature let an annotation written above the file, rather than a source, apply to every source in
> it as a model-wide override. That capability was withdrawn — the raw-warehouse path it existed to
> close is already closed unconditionally by restricted mode (any caller-submitted `duckdb.sql(...)`/
> `duckdb.table(...)` is rejected before any gate runs), so the model-wide reach bought no additional
> protection and was easy to reason about incorrectly. A `##(authorize)` annotation anywhere in the
> model — including one folded in from an imported file — now fails the load with a message naming
> the remedy: declare `#(authorize)` on each source it was meant to protect.

> **The string form — `#(authorize) "<expr>"` on the `source:` line itself — is retired and refused
> at load.** If you are looking at an older example that annotates the `source:` line with a quoted
> expression, it is the form this page used to describe; it no longer loads. The refusal is:
>
> > The string form of `#(authorize)` (a Malloy-quoted expression on the `source:` line) is no
> > longer accepted. Replace it with the unquoted expression, carried by an `#(authorize)`
> > annotation on its own line directly above the `source:` line
>
> and names the exact rewrite per finding — drop the quotes, keep the annotation on its own line.
>
> **The refusal reads only the model's OWN top-level sources, but every `.malloy` in the package is
> its own model load.** The check reads the annotations a source declares in the model currently
> loading, so a string-form gate reached through an `import` is invisible to the importing model's
> load. That is not a corpus blind spot in practice: package load compiles **every** `.malloy` file
> in the package tree, and any per-model compile failure aborts the whole package load, so the file
> that declares the gate is itself refused with the paste-ready rewrite. A string-form gate anywhere
> in a package therefore fails that package's load, whatever imports it.
>
> **The residual case is a declaring file outside the package tree.** Nothing compiles it on its own,
> so no load error names it, and the importing model's own load does not see it. It still denies: a
> quoted string compiles to a string literal rather than a boolean, so it fails to lift as a filter
> condition and the request is refused, counted under
> `publisher_authorize_row_level_rejected_total{cause="legacy_string_gate"}` with no author-facing
> detail. Fail-closed, but confusing — the package loads, appears to work, and denies every caller
> with no compile-time hint of why.
>
> **An annotation anywhere other than directly above a `source:` line is also refused at load** —
> the earlier `internal dimension: authorized is <expr>` form (the annotation on a `dimension:`
> line inside the source) included. The refusal is:
>
> > An `#(authorize)` annotation is never enforced at:
> >   - on field "authorized" of source "orders"
> > A gate only applies where model load looks for one — a `source:`'s own annotation, or one it
> > inherits from an `extend`/query-source base.
>
> naming every misplaced annotation the load finds. The remedy is the same: move the expression to
> an `#(authorize)` annotation directly on the `source:` line.

### Expression Language

The expression is an ordinary Malloy boolean expression — anything Malloy itself accepts as the body of a `where:`, over `$given` references, row fields, and literals:

```malloy
#(authorize) $ROLE = 'analyst'
source: a is duckdb.table('orders.parquet') extend {}

#(authorize) $REGION = 'us-west' and $ROLE != 'guest'
source: b is duckdb.table('orders.parquet') extend {}

#(authorize) org_id in $GROUPS
source: c is duckdb.table('orders.parquet') extend {}
```

Unlike the retired string form, Publisher does **not** validate the expression against a restricted grammar of accepted shapes — see [Row-level gates](#row-level-gates) for what changed and why that matters for what fails at load versus at request time.

Embedded string literals follow ordinary Malloy syntax: single-quote them as usual (`$ROLE = 'analyst'`) — there is no surrounding quoted-annotation layer left to escape out of, since the expression is now Malloy source, not a string.

## Row-level gates

A gate expression may reference a row field — its own source's, or one reached through a join — instead of only givens and literals. **Every gate is enforced the same way** regardless: as a row filter on the entry-point source. A gate that reads no field is constant across every row, so it still admits all of them or none; it is not a separate mechanism. See [Row-level access](row-level-access.md#row-level-authorize) for what that means for the caller, and [security-posture.md](security-posture.md#row-level-authorize-rows-are-protected-the-schema-is-not) for the trade it makes.

```malloy
#(authorize) org_id in $GROUPS
source: a is duckdb.table('orders.parquet') extend {}

#(authorize) childtable.name = $BOB
source: b is duckdb.table('orders.parquet') extend { join_one: childtable on id = childtable.id }

#(authorize) region = $REGION and tier != $EXCLUDED_TIER
source: c is duckdb.table('orders.parquet') extend {}
```

**There is no accepted-shape allowlist any more.** The retired string form validated its expression against a small allowlist of comparison shapes (`classifyAuthorizeGate`) before it could ever be attached as a filter — a function call, `like`, `is not null`, or a `not in` failed the load outright. The current form does not classify the expression's shape at all: validation checks only that the expression compiles as a scalar boolean (G1) and that every given it references carries no default (G4, below); everything else Malloy itself accepts is accepted here too. Concretely, `upper(region) = $REGION`, `region like $PAT`, `region is not null`, and `amount + 1 > $AMOUNTMIN` are all legal gates today, where the string form refused every one of them at load.

**This trades an early, named refusal for a request-time failure in one specific case.** The string form additionally checked a given's *declared type* against the operator used on it, so a scalar comparison against an array-typed given (`org_id = $GROUPS`) was refused at load with a clear message. The current form has no such check: `org_id = $GROUPS` compiles cleanly (Malloy itself does not reject it at that point), grafts cleanly onto the entry point, and only fails when the warehouse tries to execute the comparison, surfacing as a request-time execution error rather than a load-time 424. The failure is safe — it errors and serves no rows — but it lands as a warehouse error, not a diagnostic on the author's own line:

```
Conversion Error: Type VARCHAR with value 'org1' can't be cast to the destination
type VARCHAR[] when casting from source column org_id
```

If you write a gate comparing a row field to an array-typed given, use `in`, not `=`/`!=`/`>`/`<` — and note `org_id = $GROUPS` and `org_id ? $GROUPS` are the same comparison node, so neither spelling escapes it. `in` against a *scalar*-typed given (`owner in $ROLE`) is still refused — that one is ordinary Malloy type-checking, not anything `#(authorize)`-specific, so it still fails at load.

**A negated membership test is accepted, with a load-time warning.** `not (org_id in $GROUPS)` used to be refused outright by the string form's grammar. The current form accepts it — G1 only asks "does this compile as a scalar boolean", which a negation satisfies — and it filters correctly for a non-empty given. The warning (W2) exists because an **empty** given then matches every row rather than none (the negation of "in nothing" is "true" for every row), the opposite of what `in` alone does for an empty given. The load succeeds with a warning naming the source; decide per-gate whether that empty-given behavior is what you want. The warning is a best-effort signal, never a gate on the load: it matches the `not (... in $GIVEN)` shape, and other spellings of the same inversion — `(org_id in $GROUPS) = false`, a `pick`, a negating function — will not trigger it, so treat its absence as no evidence either way.

**A given with a declared default is refused outright, whether or not the expression is field-less.** The string form's "vacuous admin-override atom" check applied only to a `<given> <operator> <literal>` atom (`$ROLE = 'admin'` with `ROLE` defaulting to `'blocked'`); a field-referencing comparison against a defaulted given was checked separately and less strictly. **G4** is now a single, unconditional check: *any* given the gate expression references, anywhere in it — including one reached through a bare reference to another dimension (`#(authorize) authorized` over `dimension: authorized is $ROLE = 'analyst'`) — must be declared with no default. An unsupplied given would otherwise silently resolve to its default and admit or exclude rows the gate meant not to, so this is refused at load rather than reasoned about case by case — declare every given a gate references with no default.

**A given the gate references that is not on the model's own given surface fails at load too, with no separate check needed for it.** The gate's expression is validated by compiling it as a probe query against the model's own given namespace, so an undeclared or out-of-reach given fails that compile with Malloy's own error before anything `#(authorize)`-specific runs. The reach is the same one hop [givens.md](givens.md) describes for the ambient namespace: import the given (`import { GROUPS } from "…"`) if it lives further away.

> **A gate on a joined field turns a `join_one` LEFT JOIN into an INNER JOIN.** The filter is applied inside the entry source's own build, before any aggregation, so a parent row with no matching child — and so no value to satisfy the gate — drops out of the result entirely rather than surviving with nulls. That is fail-closed (a row the gate cannot evaluate is a row it does not admit), but it changes cardinality invisibly if you expected the join's usual left-join behavior.

**The pre-compile check never grants, and rarely denies now either.** Every gate is now enforced the same `row_level` way, so the request-time graft is what decides admission — there is no more shape-based pre-compile classification that can refuse a gate before compiling. What is still checked before there is a compiled query is whether the gate's expression can be resolved against the entry point at all (an entry point that renamed or dropped a field it reads); that failure denies with a 403 rather than serving unfiltered. See [Enforcement](#enforcement).

**A derivation that drops the column a gate reads fails CLOSED, the opposite of the retired form's behavior.** `source: derived is locked extend { except: org_id }` (or an `accept:` that just doesn't re-list `org_id`) no longer compiles the graft at all — the probe that validates the gate can't resolve the field, so the derivation either fails the load (if `derived` is the one that authored the gate) or, when `derived` merely *inherits* `locked`'s gate, loads with a warning and denies every request against `derived` specifically at 403 while `locked` itself keeps serving normally. This is the reverse of the retired form's known gap, where the same derivation dropped the boolean gate *field* and served every row silently — see [Known limitations](#known-limitations).

> **Known hole: `except:`-ing the gate's column and then `rename:`-ing a *different* column onto that
> exact name grafts successfully and misbinds to the wrong column.** `source: w is locked extend {
> except: org_id } extend { rename: org_id is owner }` compiles: there is once again a field named
> `org_id` for the gate's probe to resolve, so the graft attaches — but it now filters on `owner`'s
> values, not the real `org_id`'s. This is narrow (it requires an author to both drop and re-populate
> the exact gated name) but real, and there is no load-time signal for it today.

### Row-level gates and colocated persistence

A row-level gate proven attributed to its entry point (see
[materialization.md](materialization.md#authorize-gated-sources-and-materialization)) is the ONE
`#(authorize)` shape eligible for a colocated `#@ persist`. Persisting the source changes only where
its rows are read FROM; the gate itself keeps running live, on every query, exactly as described
above — a materialized entry point is not served frozen with respect to who may see what. What can go
stale between rebuilds is the row DATA the gate filters on: a row whose access decision changed (say,
it changed owner) keeps serving to its former owner until the source rebuilds. See
[materialization.md](materialization.md#the-freshness-contract-for-a-gated-colocated-persist-source)
for the full contract and how to bound that staleness. `storage=` and `#@ preaggregate` remain
unconditionally refused for any `#(authorize)`-gated source — the relaxation applies to colocated
persistence only.

Declare the gate the same way as on any other source — directly above the `source:` line, whether or
not that line is also tagged `#@ persist`:

```malloy
#(authorize) org_id in $GROUPS
#@ persist name="orders_summary"
source: orders_summary is orders_raw -> {
  group_by: org_id, category
  aggregate: total is amount.sum()
} extend {}
```

## Semantics

### OR semantics

**A source may declare at most one `#(authorize)` annotation, so combine conditions with `or` inside that one expression** — there is no longer a way to stack several `#(authorize)` annotations on one source to mean OR; a second one fails the load, naming both:

```malloy
given:
  ROLE :: string
  TENANT :: string

#(authorize) $ROLE = 'admin' or $TENANT = 'acme'
source: orders is duckdb.table('orders.parquet') extend {}
```

`orders` is queryable by an admin **or** by an acme-tenant caller. This is a real change from the retired string form, which read multiple stacked `#(authorize) "<expr>"` annotations on one source as an implicit disjunction (`(a) or (b)`) — that stacking no longer exists; every OR has to be written out in the single gate expression.

To give one role access to every source in a file, declare the same admin-override sub-expression as one arm of each source's own gate rather than reaching for a model-wide gate — a file-level override existed for this once; it is deprecated (see [Declaring Gates](#declaring-gates)).

### The entry point, and only the entry point

Authorize is checked on the source the query **enters through** — the run target. A gate answers "who may query this source", not "who may read everything reachable beneath it".

- **Joins are not gated.** A gate on a source reached only via `join_*` **does not fire** — at any depth (A→B→C), aliased, across files, declared in a query-local `join_one` inside a `-> { … }` refinement, or as a member of a joined composite. `run: joiner -> { … }` where `joiner` joins a base gated `#(authorize) false` returns rows. **This is the rule authors have to design around:** joining sensitive data into an ungated source publishes it. Put the gate on the source callers enter through, and use [access modifiers](https://docs.malloydata.dev/documentation/experiments/include) (`include { public: …, private: * }`) to control what an extension re-exposes.
- **A gate MAY reference a field on a joined source — that is not the same as the joined source's own gate firing.** `#(authorize) childtable.name = $BOB` declared on the entry point itself is enforced: the join it needs is emitted as part of the entry point's own build. The rule above is unchanged — a gate declared ON `childtable` still does not fire when `childtable` is only reached via a join from an ungated entry point. Referencing a joined field from the entry point's *own* gate, and a joined source's *own* gate firing, are two different things; only the first is supported.
- **Extend: own gate replaces, otherwise the base's carries.** `source: b is a extend { … }` is governed by `b`'s own `#(authorize)` annotation when it declares one — that replacement is the [curated-extension idiom](#recommended-pattern-locked-base-and-curated-extensions). When `b` declares none, Malloy copies `a`'s annotation onto `b` by reference, so `a`'s gate still applies to `b` too. That holds however `b` is decorated: a render tag or doc comment on `b` does not remove it.
- **A derivation that drops the column a gate reads fails CLOSED** — see [Row-level gates](#row-level-gates) above, and the known rename-collision hole there.
- **Caller-submitted text may not declare a gate at all.** An `#(authorize)` annotation anywhere in the `query` text of a query request, or in the `source` text submitted to `/compile` — the deprecated `##(authorize)` and retired string-form spellings included — is rejected with a 400: a gate is the model author's to declare. Notebook cells are package content, so an author's gate there works normally.
- **Query-source derivation carries the gate too**, the same way `extend` does — `source: laundered is locked_src -> { … }` is gated by `locked_src`'s gate when it declares none of its own. Derivation is treated like `extend` for this purpose; reaching the same derived source via a *join* is still not gated.
- **A composite run target resolves precisely.** When the run target is `compose(a, b)`, Malloy resolves it to exactly one member branch per query and that branch's gate applies.

### Worked example

One model, six entry points. The rules above are subtle enough that it is worth reading the verdicts rather than deriving them:

```malloy
##! experimental.givens

given:
  ROLE :: string

// Locked: nobody queries this directly.
#(authorize) false
source: salaries is duckdb.table('salaries') extend {}

// (1) No gate of its own → the base's carries (extend copies the base's
//     annotation onto salaries_plain by reference). Still locked.
source: salaries_plain is salaries extend {
  measure: headcount is count()
}

// (2) A render tag does NOT change that.
# bar_chart
source: salaries_tagged is salaries extend {
  measure: headcount is count()
}

// (3) Its own gate REPLACES the base's — the curated-extension idiom.
#(authorize) $ROLE = 'hr'
source: salaries_hr is salaries extend {
  measure: avg_salary is avg(salary)
}

// (4) Derivation carries the gate, like extend.
source: salaries_derived is salaries -> { group_by: department }

// (5) A JOIN does not. `headcount_by_dept` has no gate of its own, and the
//     gate on `salaries` is not traced through the join.
source: headcount_by_dept is duckdb.table('departments') extend {
  join_one: salaries on id = salaries.department_id
  measure: headcount is count()
}
```

| Entry point | Verdict | Why |
| --- | --- | --- |
| `run: salaries -> …` | **no rows** always | its own `false` |
| `run: salaries_plain -> …` | **no rows** always | inherited `false` (1) |
| `run: salaries_tagged -> …` | **no rows** always | inherited `false`; the tag is irrelevant (2) |
| `run: salaries_hr -> …` | no rows unless `ROLE = 'hr'`; 403 if `ROLE` is unsupplied | own gate replaced the base's (3) |
| `run: salaries_derived -> …` | **no rows** always | derivation carried the base's gate (4) |
| `run: headcount_by_dept -> …` | **returns rows** | no gate at the entry point; the join is not traced (5) |

A constant-`false` gate compiles to a filter that matches nothing, so those rows are withheld with a **200 and an empty result**, not a 403 — see the note on denial shape at the top of this page. It is a real query: the `where: false` graft is dispatched and the warehouse returns no rows.

Entry point (5) is the one to internalize: `headcount_by_dept` reads salary rows and is queryable by anyone. That is not a bug, it is the rule — **joining sensitive data into an ungated source publishes it.** If `headcount_by_dept` should be restricted, give it its own gate; if it should expose only aggregates, use `include { public: headcount, private: * }` so the join cannot be drilled through.

And the caller cannot mint a gate to escape one:

```jsonc
// POST /…/models/hr.malloy/query — rejected with 400, not compiled
{
  "query": "#(authorize) true\nsource: mine is salaries extend {}\nrun: mine -> { select: * }"
}
```

## Recommended pattern: locked base and curated extensions

Lock sensitive base sources with a `false` gate and re-expose curated subsets through extension sources, each with its own gate and access modifiers:

```malloy
##! experimental.givens
##! experimental.access_modifiers

given:
  REGION :: string
  ROLE :: string

// Base source: locked. Direct queries are denied — the gate
// is a constant `false`.
#(authorize) false
source: customers_raw is duckdb.table('customers.parquet') extend {}

// Extension: re-exposes a curated subset and adds an analyst-role gate.
// `private: *` hides every other column on the base.
#(authorize) $ROLE = 'analyst'
source: customers_marketing is customers_raw include {
  public: name, region, signup_date
  private: *
} extend {
  measure: customer_count is count()
}

// A second extension with a different gate and field surface.
#(authorize) $REGION = 'us-west'
source: customers_us_west is customers_raw include {
  public: name, region, signup_date, lifetime_value
  private: *
} extend {}
```

- `run: customers_raw -> …` → **200 with no rows** (gate is `false`; nothing is readable).
- `run: customers_marketing -> …` → allowed with `$ROLE = 'analyst'`; the consumer can only touch `name`, `region`, `signup_date`. Supplying no `$ROLE` at all is a 403, since the gate's given cannot bind.
- `run: customers_us_west -> …` → allowed with `$REGION = 'us-west'`, on a different surface.

The `include { … private: * }` layer is what controls which base columns each extension can re-expose; each extension's own gate gates consumer access to that curated surface. The base's constant-`false` gate is a defense-in-depth backstop against a direct `run: customers_raw`. Remember the fail-closed rename-collision hole above: an `include { … }` that drops a column a gate reads and a later `rename:` that re-populates the exact same name can misbind the gate to the wrong data.

> ⚠️ **The `false` lock covers `/…/query`; it does not cover `/…/compile`.** `/compile` admits any gate
> that references no given, whichever way it resolves, so `#(authorize) false`
> is **admitted** there — and `includeSql` then returns the source's SQL with no gate `where:` in it.
> No rows are read (`/compile` never executes), so this is a schema/SQL exposure rather than a data
> one, and it is the same exposure [Known limitations](#known-limitations) already records for raw
> SQL on `/compile`. Keep `/compile` behind the trusted tier; do not read the `false` gate as a lock
> on every route.

## Enforcement

The gate runs, fail-closed, on every query entry point. Enforcement is in two parts:

1. **An expression-resolution check.** Where the run target can be resolved from the request's surface syntax, Publisher confirms the gate's expression still compiles against that entry point's own field space before compiling anything else. An entry point whose derivation renamed, excluded, or projected away a field the gate reads is refused here with a **403**.
2. **The graft.** The gate's expression is attached to the compiled query as a source-level `where:` on the entry point, and evaluated by the same `run()` as the query itself.

Step 2 is what a gate actually resolves through: a gate's condition is compiled *into* the query rather than decided by a separate probe beforehand. The practical consequences are listed under [Known limitations](#known-limitations): a denial by filtering is a 200, and a caller who compiles a deliberately malformed query against a source it may read nothing from still sees Malloy's compile errors for that source.

There is one documented exception, [the authorize bypass](#authorize-bypass-for-trusted-data-management-callers), which applies to `POST /…/query` only:

| Entry point | Behavior |
| --- | --- |
| `POST /…/query` | Gate the run-target source; deny → zero rows where the filter applied, 403 where the field could not be resolved. Skipped entirely when the request carries `x-publisher-bypass-authorize: true`. |
| `POST /…/projects/…/query` (legacy alias) | Gate as above. Accepts no bypass — it exists for pre-rename SDK compatibility and passes no `givens` either, so a gated source is denied there regardless. Use the `/environments/…` route. |
| Notebook cell `GET` | Gate each cell that runs a query. Accepts no bypass. |
| `POST /…/compile` | Gate the named source the submitted text targets (early, before compiling, plus a compiled-source backstop). `/compile` never runs the query, so the backstop cannot attach a filter; it admits a gate it can decide without running it and denies anything else. **"Decidable" is presence, not truth:** a gate referencing NO given is admitted whichever way it resolves — a constant `false` included — as is one whose every given the caller supplied, right or wrong. Only a gate with an unsupplied given denies. Note that `includeSql` then returns the **ungrafted** SQL, without the gate's `where:`. Accepts no bypass. |
| MCP `malloy_executeQuery` | Routes through the query path; a denial surfaces as `isError: true` naming the source. Sends no bypass. |

**Fail-closed.** Anything that stops the graft landing denies rather than admits. Where two *sources* gate one entry point — its own gate plus one carried from the source it derives from — each is grafted separately, which AND-s them.

### Validation

Authorize gates are validated at **model load** (compile-only, no execution). A misdeclared gate (more than one on a source, a payload that doesn't compile as a boolean, an unreachable given, a given with a default) fails the load with **HTTP 424** (`ModelCompilationError`), naming the source and the underlying reason. Fix the model before it serves.

**There is no separate field-less-vs-field-referencing carve-out any more.** The retired string form's row-level grammar treated a gate reading no row field as a special class, refused per-source rather than per-model so one field-less gate published under the old rules couldn't take a whole model file out of service. The current form has no such split: G1/G4 apply identically whether or not the expression happens to read a row field, and a field-less gate that references no given at all loads with a **warning** (W1, [Row-level gates](#row-level-gates)) rather than any refusal.

**A gate must resolve at every entry point through which the source declaring it is reached, and an entry point where it cannot is closed rather than opened.** Where the entry point declares its **own** `#(authorize)` annotation, a validation failure (G1/G4) is the author's own mistake and package load fails with a 424 naming the source. Where the entry point only **inherits** the annotation via `extend` and its own `except:`/`accept:`/`rename:` drops a field the gate reads, the load does not abort: it warns naming the affected entry point, and that entry point alone denies every request with a 403 (see [Row-level gates](#row-level-gates) for the fail-closed behavior this replaces, and the rename-collision hole that is the one way it can still misfire). This is distinct from *inheriting* a gate that still resolves (see the worked example above and [row-level-access.md](row-level-access.md#row-level-authorize)): there, the filter runs inside the base's own build, before the deriving source's projection ever applies, so a derived source that projects the gated column away still serves correctly filtered — there is no need to keep an inherited gate's column in the projection.

Validation covers only the entry points in the model being loaded. `compileMalloyModel` compiles each file independently, so an importing model's own entry points are validated when THAT model loads.

> ⚠️ **An inherited gate's givens bind in the ENTRY model, not the model that declared the gate.** The filter is compiled against the model the query enters through, and Malloy mints a fresh identity per `given:` declaration — so a model that imports a gated source and re-declares one of the base's given names re-points that gate to its own declaration, including its default. A base gated `#(authorize) tenant_id = $TENANT` with no default for `TENANT`, imported into a model declaring `given: TENANT :: number is 99`, serves tenant-99 rows to a caller who supplied nothing. This is an authoring hazard, not a caller-reachable bypass — a caller cannot introduce a declaration — but it means importing a locked source does not carry its gate's given namespace with it. Do not re-declare a base's given names in a model that imports its gated sources.

### Error contract & redaction

- **Runtime 403** names only the source — `{"code":403,"message":"Access denied for source \"orders\"."}` — never the gate expression. Gate logic is not leaked to (potentially untrusted) query callers.
- **Model-load 424** *keeps* the gate's expression text in its message — it is author-facing at package load and you need it to fix a malformed gate.
- **The reported `authorize` list is the author's expression, whether declared on the source itself or carried in from elsewhere** — a base it extends, a query-source base, a composite member. It is not one element per source: gates are flattened into a single array, so a query source over a composite reports the same authored gate twice (once from its base, once from the resolved member). Elements from different declaring sources are AND-ed at enforcement time even though the array does not show that. Use the field to decide *that* a source is gated; do not recompute the access decision from it.
- **Introspection is not redacted, and shows inherited gates too.** A source's `authorize` list in the API reports the expression gating it, including one it inherits from a base in another file that is not itself listed. That is deliberate — a list that omitted the inherited case would report a locked source as unrestricted, which is the more dangerous error — but it means gate logic is readable by anyone who can list sources. Same trust assumption as the rest of this page: keep the API behind the trusted tier.

## Security model

**The floor under all of this is structural, not a property of any particular predicate you write.**
Three facts hold regardless of the gate's shape: a gate that cannot be grafted onto the query denies
rather than admits (see [Fail-closed](#enforcement)); G4 refusing every defaulted given, combined
with Malloy's own failure for an unsupplied one, means a gate given the caller omits fails the
request before anything executes rather than silently resolving to a default; and `in` against an
empty array compiles to a live `WHERE FALSE`, so an empty or omitted array-typed given denies rather
than matching everything. None of the three depends on what the predicate looks like — a gate that
is a bare `false`, a multi-arm `or`, or a joined-field comparison all rest on the same floor. What is
*not* structural is everything the [known limitations](#known-limitations) above cover: a join that
doesn't carry the gate, a rename-collision that misbinds it to the wrong column, a scalar/array
mismatch that only fails at the warehouse. Those are gaps in the specific model, not in the mechanism.

`#(authorize)` evaluates expressions over **request-supplied givens**. There is no authentication in Publisher's query path: a given is whatever the caller sends. So:

- **Authorize is a real boundary only behind a trusted tier.** The intended deployment is Publisher behind an embedding application that authenticates end users and sets givens (role, tenant, region) from its own *verified* context, with the query/MCP API network-isolated from untrusted callers. In that setup the gate enforces the trusted tier's policy.
- **It does not defend against a caller who sets their own givens.** Exposed directly to untrusted users, anyone can send `{"ROLE":"admin"}` and pass a `$ROLE = 'admin'` gate. Do not treat `#(authorize)` as end-user authn/authz on a public endpoint.
- **Identity-bound givens** — a verified token or trusted-proxy header populating reserved "system givens" the caller cannot override — is a planned milestone that would make authorize a standalone boundary. It is not implemented yet.

### Authorize bypass, for trusted data-management callers

A `POST /…/query` request carrying the header `x-publisher-bypass-authorize: true` runs with gate evaluation **skipped**. It exists for data management: an indexer or similar back-office caller is a machine identity with no givens, so a gated source returns 403 and is never scanned — an author who tags a dimension `#(index)` on a gated source otherwise gets an empty index, the tag having opted in and the gate having silently revoked it.

What it does and does not touch:

- **Only** the gate's condition is skipped. The author's own `where:` clauses still narrow the scan, row and byte caps still apply, restricted mode still bans raw SQL, and a gate declared in caller-submitted text is still rejected with a 400.
- It is **not** `bypassFilters`, a separate deprecated `#(filter)`-only control in the request body. Neither reads or writes the other.
- Notebook cells, `/compile`, and MCP accept no bypass at all.

**Publisher does not bound who may send it.** There is no authentication in the query path, so the header is exactly as trustworthy as the network in front of it — and the header name is published here, so treat it as known to anyone. **A deployment that reaches untrusted callers must strip this header at its edge.** [docs/authorize-bypass-deployment.md](authorize-bypass-deployment.md) is the operator's page: what to strip, why an allowlist beats a blocklist, and how to tell whether a bypass ever happened.

Note what the residual case is if the strip is missing. Publisher has no tenant boundary of its own, so a fronting application's own authorization still decides which packages a caller reaches; what the header removes is the **in-model** gating — role- or row-level policy *within* data that caller is otherwise entitled to reach.

Every use is counted (`publisher_authorize_bypass_total`, labelled `entry_point`) and logged (`authorize bypass`, with source / model / package). The counter is the rate signal; the log line is what an investigation reads. Read the two carefully: `runnable` fires on every bypassed query, `source` only when a run target was resolvable before compilation, so they are not always paired.

### Row-level gate metrics

Two more counters cover row-level gates specifically:

- `publisher_authorize_row_level_total`, labelled `decision` — exactly two values, `denied_by_gate` and `empty_after_filter`. `denied_by_gate` is the fail-closed refusal when a gate's expression could not be resolved against the entry point; `empty_after_filter` is a normal 200 with zero rows after the filter matched none, which is not an error. A provably constant-`false` gate is not a third case: it runs the `where: false` graft and records `empty_after_filter`.
- `publisher_authorize_row_level_rejected_total`, labelled `cause`. The label set is generated from the `RowLevelGateRejectionCause` union (`ROW_LEVEL_GATE_REJECTION_CAUSES` in `authorize.ts`, which is the single source of truth — read it there rather than trusting this list) and has seven members: `source_line_gate_no_given_reference`, `source_line_gate_negated_membership`, `unreachable_given`, `entry_point_unexpressible`, `legacy_string_gate`, `given_usage_unresolvable`, `unclassifiable_condition`. The first two are **non-fatal** — the model still loads: W1 (the gate reads no given at all) and W2 (a negated membership test). A hard load-time failure (G1, G4) fails the whole model load but does **not** increment this counter today — it surfaces only as a load error on the package, not as a metric. `entry_point_unexpressible` is the inherited-and-unexpressible case above: the load warns naming the entry point rather than aborting, and that one entry point denies every request. `unreachable_given` and `given_usage_unresolvable` are request-time classification failures — a gate whose given lands off the model's own surface, or whose lifted condition references a field the graft target cannot resolve. `unclassifiable_condition` is a lifted condition carrying no usable expression at all, an IR shape not otherwise expected. `legacy_string_gate` is the one worth alerting on: it fires both for the load-time refusal of a string-form annotation the check can see, and — with no load-time refusal at all — for every request against a string-form gate declared outside the package (see [Declaring Gates](#declaring-gates)). A steady rate of it in a deployment predating this feature is evidence of the second case, not the first.

`publisher_authorize_guard_rejected_total`, labelled `field` (`query` | `source_name` | `query_name` | `compile_source`), counts requests rejected with 400 for declaring an `#(authorize)` annotation in caller-submitted Malloy text.

**This is an interim answer, not the intended one.** The shape that keeps the decision with the model author is identity-bound givens (above) — a reserved system given the caller cannot set, so an author writes `#(authorize) $ROLE = 'analyst' or $SYSTEM_CALLER = 'indexer'` and a source they never opted in stays gated. This header removes the gate globally instead, for callers the deployment trusts wholesale. When identity-bound givens land, this should narrow or go.

## Known limitations

- **A request can be exempted from the gate entirely** (see [Authorize bypass](#authorize-bypass-for-trusted-data-management-callers)). `x-publisher-bypass-authorize: true` on a query request skips evaluation, and Publisher does not bound who may send it — so on a deployment that does not strip the header at its edge, every gate is advisory for any caller who knows the name. Listed first here because it is the only limitation on this page that a *deployment*, not a model, has to close.
- **A gate does not follow a join** (see [above](#the-entry-point-and-only-the-entry-point)). This is the limitation with the largest practical consequence: any source that joins gated data and is itself ungated hands that data to every caller. Treat "which sources can a caller enter through, and what does each of them reach" as part of modelling, not as something the gate handles for you.
- **`except:`-ing, projecting away, or `accept:`-not-relisting the column a gate reads fails CLOSED** (see [Row-level gates](#row-level-gates)) — the graft cannot resolve, so the affected entry point warns at load and denies every request at 403. This is the reverse of the retired dimension form's own known gap, where the equivalent derivation dropped the gate as a plain field and served every row silently. The one narrow residual hole is `except:`-ing the gated column and then `rename:`-ing a *different* column onto that exact name: the graft resolves again, but against the wrong data — see [Row-level gates](#row-level-gates) for the pinned example.
- **An extension's own gate replaces the base's** (see [above](#the-entry-point-and-only-the-entry-point)) — that is the curated-extension idiom, so pair locked bases with access modifiers to keep the re-exposed column surface deliberate. (An extension with no gate of its own carries the base's.)
- **A gated source is a schema oracle wherever its gate expression resolves.** The expression-resolution refusal (see [Enforcement](#enforcement)) fires only when the gate's expression cannot be resolved against the entry point at all. Otherwise the gate is grafted into the query and evaluated with it, so compilation happens first, and a caller gets Malloy's own compile errors for the gated source even when the gate then admits them no rows: a malformed probe (`group_by: no_such_field`) returns "field is not defined", confirming whether a column exists. Behind the trusted tier the exposure is a column name, not data — see [security-posture.md](security-posture.md#row-level-authorize-rows-are-protected-the-schema-is-not).
- **`/compile` raw SQL is not gated.** The gate covers named Malloy sources; `/compile` still compiles unrestricted, so a caller could read a gated table's schema/SQL via raw `duckdb.sql(...)`. Closing this (restricted compilation on `/compile`, as on `/query`) is tracked as a follow-up; until then keep `/compile` behind the trusted tier.
- **A wrong-shape gate can fail loud at load, or fail confusingly at request time, depending on exactly what it does** (see [Row-level gates](#row-level-gates)). A scalar comparison against an array-typed given loads cleanly and only crashes when the warehouse executes it; a negated membership test loads with a warning rather than being refused. Both are new relative to the retired string form's stricter, load-time-only grammar.
- **A gate can only reference a given on the entry model's own surface.** A gate naming a given the entry model does not declare is refused outright (`unreachable_given`) rather than guessed at, because Malloy merges only one level of `import` and the gate would otherwise silently bind that given's *declaration default* instead of the caller's value. Practical effect: to gate a source through a base two or more import hops away, re-declare it (or `import { NAME } from …`) in the entry model. A permissive default on the base does not open the gate up, and no given the caller did not supply is ever resolved from an unrelated declaration of the same name.
- **A gate given the caller does not supply denies opaquely.** The gate's givens bind with the query's, so Malloy's own failure for an unsupplied one names it ("Given 'ROLE' has no value and no default"). Publisher maps that back to the same `Access denied for source "…"` 403 — a denied caller is never told which given the gate reads.
- **A notebook cell that both declares a gated source and runs it in the same cell, with a
  joined-field gate, needs the run query to reference the joined field.** A cell's row-level gate
  filters correctly whether it declares the gated source itself or inherits one declared earlier
  — with one narrow exception: when the
  gate is on a JOINED field (`#(authorize) childtable.name in $GROUPS`) and the cell's own `run:`
  query never itself references that joined field, the cell denies with a 400 rather than serving
  filtered rows (never a leak — no rows are returned either way). Reference the joined field
  somewhere in the run query's own projection or grouping to avoid it.
- **A given the entry model does not surface is usually refused, not ignored.** Malloy's given-namespace merge covers only one level of `import`, so a value for a given the entry model doesn't surface reaches Malloy's own resolution and errors (`unknown given`) rather than being silently dropped and falling back to a default. The one exception is a name some reachable gate actually references: that value is forwarded to the gate and withheld from the query (`filterGivensToModelSurface`, `model.ts`), which is what lets a gate carried in from a base in another file evaluate at all. Author-side implication: for a caller to supply a given the query itself reads, some source or file within one import hop of the entry model must declare (or import) it.

## Runnable example

[`examples/governed-analytics`](../examples/governed-analytics) gates a real source with a two-armed
disjunction — an admin override plus a tenant allow-list, in one gate — in
[`secured.malloy`](../examples/governed-analytics/secured.malloy):

```malloy
given:
  ROLE :: string
  TENANT :: string

#(authorize) $ROLE = 'admin' or $TENANT = 'acme' or $TENANT = 'globex' or $TENANT = 'initech'
source: orders_secured is orders_base extend {
  where: $ROLE = 'admin' or tenant = $TENANT   // row-level scoping
  ...
}
```

Neither given carries a default — the gate references both, and G4 refuses a referenced given that
has one. Because `where:` references the same two givens, a caller must send **both** keys on every
request (the one not on their path can be sent blank); an omitted key fails to resolve and denies
with a 403, there being no default left to fall back to. Against a running server, the
`governed-analytics` package ships in the default `examples` environment (see the
[example's README](../examples/governed-analytics/README.md)):

```bash
API=http://localhost:4000/api/v0/environments/examples/packages/governed-analytics/models

# No identity at all → denied with a 403: neither given resolves, so the gate
# never binds to evaluate against.
curl -s -X POST $API/secured.malloy/query -H 'content-type: application/json' \
  -d '{"query":"run: orders_secured -> by_status"}'                                        # → 403

# Admin, TENANT sent blank → allowed (all rows). TENANT still has to be present
# on the request even though this caller's access doesn't depend on it.
curl -s -X POST $API/secured.malloy/query -H 'content-type: application/json' \
  -d '{"query":"run: orders_secured -> by_status","givens":{"ROLE":"admin","TENANT":""}}'  # → 200
```

The row-level half of that source — how `where:` narrows an *allowed* caller to their own rows — is
covered in [row-level-access.md](row-level-access.md).
