<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Authorize (Source Access Gates)

> What this is: how authorize annotations gate *who* may query a source and *which rows* they
> see.
> Runnable example: [examples/governed-analytics](../examples/governed-analytics). For the base
> mechanism, see [givens.md](givens.md); for row scoping, see [row-level-access.md](row-level-access.md).

**Two annotations, one question each, and the name tells you which.**

| Annotation | The question it answers | A denial is |
| --- | --- | --- |
| `#(authorize)` | may this caller reach this source at all? | **403** |
| `#(access_filter)` | which rows may they see, once they may? | **200**, with their rows |

Both are applications of [givens](givens.md). They share one grammar and one set of inheritance
rules, and they compose in one direction: the lock runs first, and a caller it refuses never
reaches the filter. A source with no in-scope annotation of either kind is unrestricted.

**Nothing is inferred from the body.** The annotation you write declares which question you are
answering, and the body has to conform or the model does not load — a field path inside
`#(authorize)`, or a bare literal inside `#(access_filter)`, is refused at load naming the other
annotation. There is no shape that quietly changes which question a gate answers.

**The two denial shapes are the whole point of the split.** A filter is grafted onto the query as
a `where:`, so a caller it matches nowhere gets 200 and an empty result — honest, because there
genuinely are no rows for them. A lock is *decided*, before the caller's query is compiled, and
answers 403. That matters most for an aggregate: `sum(salary)` under a grafted `where: false` is
one row of `NULL`, and `count()` is `0`, which is an answer about data the caller was refused.
A lock refuses instead. Deciding before compilation is also what keeps a denied caller's compile
errors from being a schema oracle.

A **403 also covers the case where a lock or a filter cannot be applied at all** — the entry
point renamed or dropped the field the gate reads, or a given it names was not supplied. That is
the fail-closed path rather than a verdict, and it is the one denial shape both routes share.

> ⚠️ **Read [Security model](#security-model) before deploying this as an access control.** Givens are **caller-asserted**: anyone who can reach the query API can claim a favorable given. These gates are only a real boundary when the API sits behind a trusted tier that sets givens from its own verified context. They are not, on their own, end-user authentication.

For the gate's own grammar, see [Expression Language](#expression-language) below — it is not the full Malloy expression language. For givens, see [givens.md](givens.md).

## Declaring Gates

A gate is an annotation on its own line directly above a `source:` line — **not** an arbitrary Malloy boolean expression, but one or more terms joined by `and`. Which term shape is legal is decided by which annotation you wrote:

| Written | Legal body | Illegal body → refusal |
| --- | --- | --- |
| `#(authorize)` | `'<literal>' <op> $GIVEN` — a quoted literal and a given, in either order, since neither side is a column. Plus the `true`/`false` sentinels. | a field path on the left → `row_level_term_in_authorize` |
| `#(access_filter)` | `field_path <op> $GIVEN` — a single column or a dotted join path on the left. | a literal on the left → `source_level_term_in_access_filter`; either sentinel → refused |

`<op>` is fixed by the given's own declared arity: `in` for a list-typed given, `=` for a scalar one. Nothing else parses — no `or`, no `not`, no `!=`/`<`/`>`/`<=`/`>=`, no function calls, no literal on the right of a row-level term, no given compared to another given. See [Row-level gates](#row-level-gates) for the full grammar and every named refusal.

**Two deliberate exceptions, both on the lock: a body that is exactly `false`, and one that is exactly `true`.** Every ordinary term references a caller-suppliable given, so without them there would be no way to lock a source outright, nor to re-open one. `#(authorize) false` is an unconditional deny and is how you write a locked base; `#(authorize) true` is an unconditional admit, and is the only way to write an extension of a locked base that is deliberately open — a source declaring no gate of its own **inherits** its ancestor's, so omitting the annotation over a `false` base inherits the lock rather than lifting it. "Extension" means `extend` (`include … extend { … }` included), where an own gate replaces the inherited one **on that route**; a query-source derivation (`source: x is locked -> { … }`) carries the base's gate **in addition to** its own, so `true` cannot re-open one — see [The entry point](#the-entry-point-and-only-the-entry-point).

Neither sentinel is legal on `#(access_filter)`. `false` there would be the silent total deny the route split exists to remove — write `#(authorize) false`, which refuses rather than serving zero rows. `true` there is a no-op; delete the annotation, or write `#(authorize) true` if what you mean is re-opening a locked base. A `false` may not share a source with another note at all (`deny_all_with_sibling`); a `true` may not share the lock route with one (`admit_all_with_sibling`), but is live beside an `#(access_filter)` — `#(authorize) true` with `#(access_filter) org_id in $GROUPS` re-opens a locked base while still scoping the rows. See [Recommended pattern](#recommended-pattern-locked-base-and-curated-extensions) below for both in one model.

`true` is the one body in the grammar that turns a gate off, so treat it as a declaration rather than a shrug: on a source with no ancestor gate it changes nothing and reads as "open by decision, not by omission", but on an extension it re-opens whatever the base locked. Publisher counts every one at package load (`publisher_authorize_admit_all_total`) so a deployment can answer "which sources are gated open" without reading every model.

```malloy
##! experimental.givens

given:
  ROLE :: string

#(authorize) 'analyst' = $ROLE
source: orders is duckdb.table('orders.parquet') extend {
  measure: order_count is count()
}
```

- **A source may declare more than one annotation on a route; repeats AND together.** `#(access_filter) region = $REGION` and a second `#(access_filter) org_id in $GROUPS` on the same source both apply, and a caller must satisfy every one of them. `or` is still refused by the grammar, so there is still no way to admit-if-either inside a single gate — see [OR semantics](#or-semantics) for the two-sources alternative.
- **Both only gate from the `source:` line.** The same annotation on a `dimension:`/`measure:`/`join_*:`/`view:` line inside the source, or on a top-level `query:`, is never enforced from there — it fails the load naming the position instead of silently protecting nothing. See [Enforcement](#enforcement).
- A source with no annotation of its own or inherited is **unrestricted**.

> **`##(access_filter)` (file-level) is deprecated and refused at load.** An earlier version of this
> feature let an annotation written above the file, rather than a source, apply to every source in
> it as a model-wide override. That capability was withdrawn — the raw-warehouse path it existed to
> close is already closed unconditionally by restricted mode (any caller-submitted `duckdb.sql(...)`/
> `duckdb.table(...)` is rejected before any gate runs), so the model-wide reach bought no additional
> protection and was easy to reason about incorrectly. A `##(access_filter)` annotation anywhere in the
> model — including one folded in from an imported file — now fails the load with a message naming
> the remedy: declare `#(access_filter)` on each source it was meant to protect.

> **The string form — `#(access_filter) "<expr>"` on the `source:` line itself — is retired and refused
> at load.** Either quote is refused: `#(access_filter) 'org_id = 999'` takes the same path as the
> double-quoted spelling, since Malloy accepts either for a string literal. If you are looking at an
> older example that annotates the `source:` line with a quoted expression, it is the form this page
> used to describe; it no longer loads. The refusal is:
>
> > The string form of `#(access_filter)` (a Malloy-quoted expression on the `source:` line) is no
> > longer accepted. Replace it with the unquoted expression, carried by a `#(access_filter)`
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
> condition and the request is refused, with no author-facing detail. **It is counted, but only
> coarsely.** The refusal books `publisher_authorize_row_level_total{decision="denied_by_gate"}` like
> any other fail-closed rejection, so a package denying every caller *is* visible as a rate. What it
> does not book is the finer label: the request-time classifier returns `rejected` with no `cause`,
> and that recorder only fires when there is one, so do not go looking for it under
> `publisher_authorize_row_level_rejected_total{cause="legacy_string_gate"}` — that label covers only
> the in-tree string form the load-time check can see. See
> [Row-level gate metrics](#row-level-gate-metrics). Fail-closed, but confusing — the package loads,
> appears to work, and denies every caller with no compile-time hint of why. Alert on the
> `denied_by_gate` rate; a debug log naming the graft target is what tells you a string form is the
> cause.
>
> **An annotation anywhere other than directly above a `source:` line is also refused at load** —
> the earlier `internal dimension: authorized is <expr>` form (the annotation on a `dimension:`
> line inside the source) included. The refusal is:
>
> > A `#(access_filter)` annotation is never enforced at:
> >   - on field "authorized" of source "orders"
> > A gate only applies where model load looks for one — a `source:`'s own annotation, or one it
> > inherits from an `extend`/query-source base.
>
> naming every misplaced annotation the load finds. The remedy is the same: move the expression to
> a `#(access_filter)` annotation directly on the `source:` line.

### Expression Language

The body is **not** an arbitrary Malloy boolean expression. It is one or more terms joined only by `and`, each one of two shapes, spelled out by `authorize_grammar.ts` — and which shape is legal follows from which annotation carries it:

- **Source-level**, for `#(authorize)`: `'<literal>' <op> $GIVEN` — a quoted string literal and a given. Either order reads the same and both parse.
- **Row-level**, for `#(access_filter)`: `field_path <op> $GIVEN` — a single column, or a dotted join path, on the left. Not reversible: its column is what the graft filters on.

`<op>` is `in` when `$GIVEN` is declared as a list (array) type, and `=` when it is declared scalar — the operator is not a free choice, it is fixed by the given's own declaration. Everything else is refused at load, each with its own named cause (`AuthorizeGrammarRejectionCause`):

```malloy
// A lock: a rule about the caller. `=`, because `ROLE` is declared scalar.
#(authorize) 'analyst' = $ROLE
source: a is duckdb.table('orders.parquet') extend {}

// A row filter. `in`, because `GROUPS` is declared list-typed.
#(access_filter) org_id in $GROUPS
source: b is duckdb.table('orders.parquet') extend {}

// Two row-level terms on one line. The operator follows each given's arity.
#(access_filter) region = $REGION and org_id in $GROUPS
source: c is duckdb.table('orders.parquet') extend {}

// The same filter written as repeated notes: a route's own notes AND together.
#(access_filter) region = $REGION
#(access_filter) org_id in $GROUPS
source: d is duckdb.table('orders.parquet') extend {}

// A path through a `join_one` is a field path like any other. Through a join
// that fans out it is refused (`fanout_path`), because the gate would multiply rows.
#(access_filter) account.org_id = $ORG_ID
source: e is duckdb.table('orders.parquet') extend {
  join_one: account is duckdb.table('accounts.parquet') on account_id = account.id
}

// Both routes on one source. The lock decides first; a caller it refuses gets
// a 403 and never reaches the filter.
#(authorize) 'admin' in $GROUPS
#(access_filter) org_id in $GROUPS
source: f is duckdb.table('orders.parquet') extend {}

// Admit nobody. Only on the lock — `#(access_filter) false` is refused,
// because a total deny that answers 200 with zero rows is the shape this
// route split exists to remove.
#(authorize) false
source: g is duckdb.table('orders.parquet') extend {}

// Admit everybody, explicitly. The only way to re-open an extension of a
// locked base, since a source declaring no gate inherits the base's.
#(authorize) true
source: h is g extend {}
```

`false` and `true` are the only bodies naming no given, and the only ones that are not terms — neither may appear inside an `and`, so `true and org_id in $GROUPS` is read as an ordinary two-term body whose first term is malformed, not as an admit-all.

is legal; none of the following are:

| Body | Cause | Why |
| --- | --- | --- |
| (nothing / whitespace) | `empty_body` | a gate must say something |
| `$ROLE = 'analyst' or org_id in $GROUPS` | `compound_boolean` | `or`/`not` are not joins the grammar accepts — see [OR semantics](#or-semantics) |
| `org_id != $GROUPS` | `negated_operator` | no `!=` |
| `org_id > $MIN` | `comparison_operator` | no ordering comparisons |
| `$ROLE = 'analyst'` | `left_not_field_path` | the given must be on the right; a source-level term's left side must be a quoted literal |
| `region = 'us-west'` | `missing_given_reference` | every term must reference exactly one given |
| `authorized` (a bare field reference) | `malformed_body` | no function calls, no bare booleans other than the whole-body `false`/`true` sentinels, only the two term shapes above |
| `region = $REGION and org_id = $REGION` | `duplicate_given` | one given per term |
| `region = $A and region = $B` | `duplicate_field_path` | one field path per term |
| `'x' = $ROLE and org_id in $GROUPS` | `row_level_term_in_authorize` / `source_level_term_in_access_filter` | each annotation admits exactly one term shape, so a mixed body fails on whichever term does not belong |
| `$GROUPS in 'finance'` | `reversed_in_operands` | `in` is not reversible the way `=` is; Malloy rejects array-in-string membership, so write the literal first (`'finance' in $GROUPS`) |
| `org_id = $GROUPS` where `GROUPS` is list-typed | `operator_arity_mismatch` | a list-typed given takes `in`, not `=` |
| `kids.name in $GROUPS` where `kids` is a `join_many`/`join_cross` | `fanout_path` | a row-level term cannot read through a fan-out join — see [Row-level gates](#row-level-gates) |
| `org_id in $GROUPS` inside `#(authorize)` | `row_level_term_in_authorize` | the lock decides about the caller, not the row; move the term to `#(access_filter)` |
| `'finance' in $GROUPS` inside `#(access_filter)` | `source_level_term_in_access_filter` | a constant predicate is not a row filter — it would serve a refused caller 200 with zero rows. Move it to `#(authorize)`, which refuses them |
| `false` or `true` inside `#(access_filter)` | `sentinel_in_access_filter` | both sentinels live on the lock; `#(authorize) false` to deny, `#(authorize) true` to re-open, and delete the annotation if you meant nothing |
| `#(authorize) false` alongside any other note on the same source | `deny_all_with_sibling` | a deny-all admits nobody, so a sibling note can never change what is served; delete whichever is wrong |
| `#(authorize) true` alongside another `#(authorize)` note | `admit_all_with_sibling` | a route's notes AND together, so `true and x` reduces to `x` and the admit-all is dead text; delete whichever is wrong. Beside an `#(access_filter)` it is legal, since `true` sheds only the lock's inherited gate. Carrying **both** sentinels is reported as `deny_all_with_sibling` — the fail-closed reading |

Embedded string literals follow ordinary Malloy syntax: single-quote them as usual (`'analyst' = $ROLE`).

## Row-level gates

A row-level term references a row field — its own source's, or one reached through a join — instead of a literal, and it is the only shape `#(access_filter)` accepts. It is enforced as a row filter on the entry-point source: the condition is grafted onto the source the query enters through and evaluated by the same query, so a caller it matches nowhere gets a 200 with an empty result. See [Row-level access](row-level-access.md#row-level-authorize) for what that means for the caller, and [security-posture.md](security-posture.md#row-level-access-rows-are-protected-the-schema-is-not) for the trade it makes.

```malloy
#(access_filter) org_id in $GROUPS
source: a is duckdb.table('orders.parquet') extend {}

#(access_filter) childtable.name = $BOB
source: b is duckdb.table('orders.parquet') extend { join_one: childtable on id = childtable.id }

#(access_filter) region = $REGION and org_id in $GROUPS
source: c is duckdb.table('orders.parquet') extend {}
```

**The grammar is the accepted-shape allowlist now, checked at load, before anything touches the compiled IR.** A gate that is not one of the two term shapes above, joined only by `and`, is refused with a named `AuthorizeGrammarRejectionCause` before Publisher ever tries to graft it — there is no request-time surprise from an accepted-but-oddly-shaped expression, because nothing outside the grammar is accepted at all.

**The operator is fixed by the given's arity, so a scalar/array mismatch is a load-time refusal, not a request-time warehouse error.** `org_id in $GROUPS` requires `GROUPS` to be declared as an array type; `region = $REGION` requires `REGION` to be declared scalar. Either mismatch is `operator_arity_mismatch` at load, naming the source — there is no `upper(region) = $REGION`, `region like $PAT`, or `amount + 1 > $AMOUNTMIN` shape to reach the warehouse with the wrong comparison, because none of those parse as a term in the first place.

**Negation is banned outright, not merely warned about.** `not (org_id in $GROUPS)` is `compound_boolean` at load. The empty-given inversion the retired warning (W2) used to flag — "in nothing" negated matches every row — cannot occur, because there is no way to write a negated term at all.

**A given with a declared default is refused outright, unconditionally.** **G4** (`validateSourceLineGateGivenUsage` in `gate_dimension.ts`) checks that *any* given a gate references carries no default. An unsupplied given would otherwise silently resolve to its default and admit or exclude rows the gate meant not to, so this is refused at load rather than reasoned about case by case — declare every given a gate references with no default.

**A given the gate references that is not on the model's own given surface fails at load too, with no separate check needed for it.** The gate's expression is validated by compiling it as a probe query against the model's own given namespace, so an undeclared or out-of-reach given fails that compile with Malloy's own error before anything `#(access_filter)`-specific runs. The reach is the same one hop [givens.md](givens.md) describes for the ambient namespace: import the given (`import { GROUPS } from "…"`) if it lives further away.

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
`#(access_filter)` shape eligible for a colocated `#@ persist`. Persisting the source changes only where
its rows are read FROM; the gate itself keeps running live, on every query, exactly as described
above — a materialized entry point is not served frozen with respect to who may see what. What can go
stale between rebuilds is the row DATA the gate filters on: a row whose access decision changed (say,
it changed owner) keeps serving to its former owner until the source rebuilds. See
[materialization.md](materialization.md#the-freshness-contract-for-a-gated-colocated-persist-source)
for the full contract and how to bound that staleness. `storage=` and `#@ preaggregate` remain
unconditionally refused for any `#(access_filter)`-gated source — the relaxation applies to colocated
persistence only.

Declare the gate the same way as on any other source — directly above the `source:` line, whether or
not that line is also tagged `#@ persist`:

```malloy
#(access_filter) org_id in $GROUPS
#@ persist name="orders_summary"
source: orders_summary is orders_raw -> {
  group_by: org_id, category
  aggregate: total is amount.sum()
} extend {}
```

## The `#(authorize)` lock

`#(authorize)` answers one question: **may this caller reach this source at all?** Its body is a
rule about the CALLER (a source-level term, `'literal' <op> $GIVEN`) rather than about the row. It
shares the grammar, declaration position, and load-time validation described above — see
[Declaring Gates](#declaring-gates) and [Expression Language](#expression-language) — with four
things specific to it.

**Joins are not gated, and that is the first thing to know about the word.** A lock is a statement
about who may query the source it is declared on, not about everything reachable beneath it.
Joining a locked source into an ungated one does **not** carry the lock along, so the ungated
source serves its rows. The author-facing rule: joining sensitive data into an ungated source
publishes it — put the lock on the source callers enter through. See
[The entry point](#the-entry-point-and-only-the-entry-point).

- **Every term must be source-level.** A row-level term (a field path on the left) inside a
  `#(authorize)` body is refused at load as `row_level_term_in_authorize`, naming the rewrite: move
  the term to `#(access_filter)`. Both sentinels live on this route and only this route.
- **It is DECIDED, and a caller it does not admit gets a 403.** The condition is evaluated from the
  compiled expression against the caller's givens, before their query is compiled. Nothing is
  grafted, so there is no shape in which a refused caller receives a row count, a `NULL` aggregate,
  or a compile error that reveals whether a field exists.
- **It inherits through `extend` the same way `#(access_filter)` does, evaluated per route.** An
  extension that declares its own `#(authorize)` replaces the base's on that route only; it still
  carries whatever `#(access_filter)` it inherited (or declared) unchanged, and vice versa. A source
  that inherits a filter from its base but declares its own lock is correctly "own" for one route
  and "inherited" for the other.
- **It runs BEFORE the row filter — it never bypasses it, and the filter never softens it.** There
  is deliberately no spelling anywhere in this grammar for "admit and skip the row filter". A source
  gated by both `#(access_filter) org_id in $GROUPS` and `#(authorize) 'finance' in $GROUPS` refuses
  a non-member outright; a member then sees only their own orgs' rows.

```malloy
given:
  ORG_ID :: string
  GROUPS :: string[]

#(access_filter) org_id = $ORG_ID
#(authorize) 'finance' in $GROUPS
source: orders is duckdb.table('orders.parquet') extend {}
```

The API reports the two routes separately, each under the field named for its own annotation:
`Source.authorize` carries the lock's effective texts, `Source.accessFilter` the filter's. See
`api-doc.yaml`.

**Comparison is exact, and does not use the warehouse's collation.** The lock is decided in
publisher rather than by the database, so `'Finance'` does not match `['finance']`. On a MySQL
tenant, whose default collation is case-insensitive, a gate of this shape used to admit that
caller. It fails closed, but it is a real behavior change — normalize case where you resolve
identity into givens.

**`/compile` evaluates the lock.** If reaching a source is what the word means, then reading its
SQL is reaching it, so a caller the lock refuses cannot compile against a locked source and
`includeSql` returns nothing. The cost is deliberate and worth stating plainly: an author outside
the group can no longer compile-check a source they are not admitted to. `#(access_filter)` keeps
the presence-not-truth rule described under [Enforcement](#enforcement) — it has no whole-source
answer to evaluate, and `/compile` returns a schema rather than rows.

## Semantics

### OR semantics

**`or` is not accepted anywhere in a gate body — `compound_boolean` at load, unconditionally.** Repeats on the same route AND rather than OR (see [Declaring Gates](#declaring-gates)), so there is still no way to express disjunction on a single source: not by writing `or` in the expression, and not by declaring more than one note — that combines conditions, it does not offer a choice between them.

The replacement pattern is two sources instead of one disjunction: give each admitted population its own extension source, each with its own conjunctive gate, over the same base. Every ordinary term must reference a given — see [Declaring Gates](#declaring-gates) — but `#(authorize) false` is a deliberate exception, so a base can still be locked outright:

```malloy
given:
  TENANTS :: string[]

// Locked: a direct query is refused with a 403.
#(authorize) false
source: orders_base is duckdb.table('orders.parquet') extend {}

// Each admitted population is its own extension. `true` sheds the base's
// lock; the filter then scopes the rows.
#(authorize) true
#(access_filter) tenant in $TENANTS
source: orders_for_tenants is orders_base extend {}
```

A caller who would have satisfied one arm of an old `or` now queries the extension whose gate their given satisfies, instead of a single source whose gate tries to satisfy everyone at once.

The same shape is also the admin escape hatch: add a further extension over the same locked base whose rule is a lock on the caller rather than another arm of the row-level rule.

```malloy
given:
  ORG_ID :: string[]
  GROUPS :: string[]

#(authorize) false
source: orders_base is duckdb.table('orders.parquet') extend {}

#(authorize) true
#(access_filter) org_id in $ORG_ID
source: orders is orders_base extend {}

#(authorize) 'admin' in $GROUPS
source: orders_admin is orders_base extend {}
```

Each extension replaces the base's `false` on the lock route with its own rule. `orders_admin` is locked to the admin group and, admitting no filter, serves every row to whoever passes; `orders` is open to everyone and scoped per row. The disjunction between an ordinary caller and an admin one is expressed the only way this grammar allows a disjunction: as two entry points, never as `or` inside one gate.

### The entry point, and only the entry point

Authorize is checked on the source the query **enters through** — the run target. A gate answers "who may query this source", not "who may read everything reachable beneath it".

- **Joins are not gated — on either route.** A gate on a source reached only via `join_*` **does not fire** — at any depth (A→B→C), aliased, across files, declared in a query-local `join_one` inside a `-> { … }` refinement, or as a member of a joined composite. `run: joiner -> { … }` where `joiner` joins a gated base returns rows regardless of the base's own gate. **This is the rule authors have to design around:** joining sensitive data into an ungated source publishes it. Put the gate on the source callers enter through, and use [access modifiers](https://docs.malloydata.dev/documentation/experiments/include) (`include { public: …, private: * }`) to control what an extension re-exposes.
- **A gate MAY reference a field on a joined source — that is not the same as the joined source's own gate firing.** `#(access_filter) childtable.name = $BOB` declared on the entry point itself is enforced: the join it needs is emitted as part of the entry point's own build. The rule above is unchanged — a gate declared ON `childtable` still does not fire when `childtable` is only reached via a join from an ungated entry point. Referencing a joined field from the entry point's *own* gate, and a joined source's *own* gate firing, are two different things; only the first is supported.
- **Extend: own gate replaces, otherwise the base's carries.** `source: b is a extend { … }` is governed by `b`'s own `#(access_filter)` annotation when it declares one — that replacement is the [curated-extension idiom](#recommended-pattern-locked-base-and-curated-extensions). When `b` declares none, Malloy copies `a`'s annotation onto `b` by reference, so `a`'s gate still applies to `b` too. That holds however `b` is decorated: a render tag or doc comment on `b` does not remove it.
- **A derivation that drops the column a gate reads fails CLOSED** — see [Row-level gates](#row-level-gates) above, and the known rename-collision hole there.
- **Caller-submitted text may not declare a gate at all.** Either annotation anywhere in the `query` text of a query request, or in the `source` text submitted to `/compile` — the file-level `##(…)` and retired string-form spellings included — is rejected with a 400: a gate is the model author's to declare. Notebook cells are package content, so an author's gate there works normally.
- **Query-source derivation is ADDITIVE, not a replacement** — and this is where it parts company with `extend`. `source: laundered is locked_src -> { … }` always carries `locked_src`'s gate, whether or not `laundered` declares one of its own: the collection recurses into the base unconditionally, and the two gates apply as separate entries. So an own gate can only narrow a query-source derivation, never replace one — `#(authorize) true` on a `->` derivation of a `false` base is still refused, because the base's `false` is still there. To re-open a locked base, derive with `extend` (or `include … extend { … }`), where an own gate does replace. Reaching the same derived source via a *join* is still not gated at all.
- **A composite run target resolves precisely.** When the run target is `compose(a, b)`, Malloy resolves it to exactly one member branch per query and that branch's gate applies.

### Worked example

One model, six entry points. The rules above are subtle enough that it is worth reading the verdicts rather than deriving them:

```malloy
##! experimental.givens

given:
  ROLE :: string

// Locked: a direct query is refused.
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

// (3) Its own lock REPLACES the base's — the curated-extension idiom.
#(authorize) 'hr' = $ROLE
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
| `run: salaries -> …` | **403** always | its own `false` |
| `run: salaries_plain -> …` | **403** always | inherited lock (1) |
| `run: salaries_tagged -> …` | **403** always | inherited lock; the tag is irrelevant (2) |
| `run: salaries_hr -> …` | 403 unless `ROLE = 'hr'` — including when `ROLE` is unsupplied | own lock replaced the base's (3) |
| `run: salaries_derived -> …` | **403** always | derivation carried the base's lock (4) |
| `run: headcount_by_dept -> …` | **returns rows** | no gate at the entry point; the join is not traced (5) |

Every denial here is a 403 because every gate in this model is a lock. No query is dispatched for
any of them: the lock is decided before the caller's query compiles, so `run: salaries -> {
aggregate: total is sum(salary) }` is refused rather than answered with a row of `NULL`. Swap any
of these for an `#(access_filter)` and the same caller gets a 200 with the rows that filter admits
— possibly none.

Entry point (5) is the one to internalize: `headcount_by_dept` reads salary rows and is queryable by anyone. That is not a bug, it is the rule — **joining sensitive data into an ungated source publishes it.** If `headcount_by_dept` should be restricted, give it its own gate; if it should expose only aggregates, use `include { public: headcount, private: * }` so the join cannot be drilled through.

And the caller cannot mint a gate to escape one:

```jsonc
// POST /…/models/hr.malloy/query — rejected with 400, not compiled
{
  "query": "#(authorize) true\nsource: mine is salaries extend {}\nrun: mine -> { select: * }"
}
```

That refusal is the load-bearing one. `true` is legal in a *model*, where an author who can write it can already write anything else; minted by a *caller*, it would be a one-line unlock of any source they can name. Caller-submitted Malloy carrying either annotation — in any of its spellings, and in anything that near-misses one — is a 400 before anything compiles.

## Recommended pattern: locked base and curated extensions

Lock sensitive base sources with `#(authorize) false` and re-expose curated subsets through extension sources, each with its own gate and access modifiers:

```malloy
##! experimental.givens
##! experimental.access_modifiers

given:
  REGION :: string
  ROLE :: string

// Base source: locked. A direct query is refused with a 403.
#(authorize) false
source: customers_raw is duckdb.table('customers.parquet') extend {}

// Extension: re-exposes a curated subset and adds an analyst-role lock.
// `private: *` hides every other column on the base.
#(authorize) 'analyst' = $ROLE
source: customers_marketing is customers_raw include {
  public: name, region, signup_date
  private: *
} extend {
  measure: customer_count is count()
}

// A second extension with a different lock and field surface.
#(authorize) 'us-west' = $REGION
source: customers_us_west is customers_raw include {
  public: name, region, signup_date, lifetime_value
  private: *
} extend {}

// A third extension that is deliberately OPEN: a curated, non-sensitive
// surface anyone may read. The annotation is not decoration — omitting it
// would inherit the base's `false` and serve nobody.
#(authorize) true
source: customers_public is customers_raw include {
  public: region, signup_date
  private: *
} extend {
  measure: customer_count is count()
}
```

- `run: customers_raw -> …` → **403** (the lock is `false`; nothing is readable).
- `run: customers_marketing -> …` → allowed with `ROLE: 'analyst'`; the consumer can only touch `name`, `region`, `signup_date`. Any other `ROLE`, or none at all, is a 403.
- `run: customers_us_west -> …` → allowed with `REGION: 'us-west'`, on a different surface.
- `run: customers_public -> …` → allowed for every caller, on the `region`/`signup_date` surface only.

**Replacement is per route, and it is what makes all three extensions work.** An own `#(authorize)` replaces the ancestor's `#(authorize)` and nothing else; an own `#(access_filter)` replaces the ancestor's `#(access_filter)` and nothing else. So `#(authorize) true` on `customers_public` sheds the base's `false` — and would not shed an `#(access_filter)` the base also carried.

The `include { … private: * }` layer is what controls which base columns each extension can re-expose; each extension's own gate gates consumer access to that curated surface. The base's `false` lock is a defense-in-depth backstop against a direct `run: customers_raw`. Remember the fail-closed rename-collision hole above: an `include { … }` that drops a column a gate reads and a later `rename:` that re-populates the exact same name can misbind the gate to the wrong data.

**The `false` lock covers `/…/compile` too.** The lock is truth-evaluated on every route, so a
caller it refuses cannot compile against the source and `includeSql` returns nothing. This is a
change: while every gate was a row filter, `/compile` admitted any gate referencing no given and
returned the source's ungrafted SQL. `#(access_filter)` still behaves that way — see
[Enforcement](#enforcement) — so if a source's only gate is a filter, keep `/compile` behind the
trusted tier.

None of that is changed by the `append`-scope restriction below, which is a separate axis: it
refuses text declaring its own data roots, not text naming a gated source, and `file` and
`package` are not restricted at all.

## Enforcement

The gate runs, fail-closed, on every query entry point. Enforcement is in three parts:

1. **An expression-resolution check.** Where the run target can be resolved from the request's surface syntax, Publisher confirms the gate's expression still compiles against that entry point's own field space before compiling anything else. An entry point whose derivation renamed, excluded, or projected away a field the gate reads is refused here with a **403**.
2. **The lock's decision.** A `#(authorize)` gate's compiled condition is evaluated against the caller's givens, and a caller it does not admit is refused with a 403 — before their query is compiled, which is what keeps compile errors from being a schema oracle.
3. **The filter's graft.** A `#(access_filter)` gate's expression is attached to the compiled query as a source-level `where:` on the entry point, and evaluated by the same `run()` as the query itself.

Step 3 is what a filter resolves through: its condition is compiled *into* the query rather than decided beforehand, which is why a filter that matches nothing is a 200 with an empty result. The remaining consequences are listed under [Known limitations](#known-limitations).

There is one documented exception, [the authorize bypass](#authorize-bypass-for-trusted-data-management-callers), which applies to `POST /…/query` only:

| Entry point | Behavior |
| --- | --- |
| `POST /…/query` | Gate the run-target source; a lock denies with a 403, a filter with zero rows, and a gate that cannot be applied at all with a 403. Skipped entirely when the request carries `x-publisher-bypass-authorize` with the server's configured `PUBLISHER_BYPASS_AUTHORIZE_SECRET`. |
| `POST /…/projects/…/query` (legacy alias) | Gate as above. Accepts no bypass — it exists for pre-rename SDK compatibility and passes no `givens` either, so a gated source is denied there regardless. Use the `/environments/…` route. |
| Notebook cell `GET` | Gate each cell that runs a query. Accepts no bypass. |
| `POST /…/compile` | Gate the named source the submitted text targets (early, before compiling, plus a compiled-source backstop). **The lock is evaluated in full here** — reaching a source is what it decides, and reading its SQL is reaching it — so a refused caller gets a 403 and no SQL. **A filter is decided on presence, not truth:** `/compile` never runs the query, so there is no row-truth to check; a filter whose every given the caller supplied is admitted, right or wrong, and `includeSql` then returns the **ungrafted** SQL, without the filter's `where:`. Only a filter with an unsupplied given denies. Accepts no bypass. |
| MCP `execute_query` | Routes through the query path; a denial surfaces as `isError: true` naming the source. Sends no bypass. |

**Fail-closed.** Anything that stops a lock being decided, or a graft landing, denies rather than admits. Where two *sources* gate one entry point — its own gate plus one carried from the source it derives from — each is evaluated or grafted separately, which AND-s them.

### Validation

Authorize gates are validated at **model load** (compile-only, no execution). A misdeclared gate (a payload that doesn't compile as a boolean, a duplicate given or field path across a source's repeated notes on the same route, an unreachable given, a given with a default) fails the load with **HTTP 424** (`ModelCompilationError`), naming the source and the underlying reason. Fix the model before it serves.

**There is no separate field-less-vs-field-referencing carve-out any more.** A row-level term and a source-level term are validated the same way — G4 applies identically to both. Every ordinary term must reference exactly one given (`missing_given_reference`); the only field-less bodies still accepted are the `false` and `true` sentinels on the lock (see [Declaring Gates](#declaring-gates)). **W1 (a gate reading no given at all) still fires for `false`** — it genuinely references none — and loads with a warning naming the source; read it as confirmation the gate is a fixed predicate rather than as a mistake to fix. **W2 (a negated membership test) is unreachable code today**: the grammar refuses that shape outright (`compound_boolean`) before that logic ever runs. See [Row-level gate metrics](#row-level-gate-metrics).

**A gate must resolve at every entry point through which the source declaring it is reached, and an entry point where it cannot is closed rather than opened.** Where the entry point declares its **own** `#(access_filter)` annotation, a validation failure (G1/G4) is the author's own mistake and package load fails with a 424 naming the source. Where the entry point only **inherits** the annotation via `extend` and its own `except:`/`accept:`/`rename:` drops a field the gate reads, the load does not abort: it warns naming the affected entry point, and that entry point alone denies every request with a 403 (see [Row-level gates](#row-level-gates) for the fail-closed behavior this replaces, and the rename-collision hole that is the one way it can still misfire). This is distinct from *inheriting* a gate that still resolves (see the worked example above and [row-level-access.md](row-level-access.md#row-level-authorize)): there, the filter runs inside the base's own build, before the deriving source's projection ever applies, so a derived source that projects the gated column away still serves correctly filtered — there is no need to keep an inherited gate's column in the projection.

Validation covers only the entry points in the model being loaded. `compileMalloyModel` compiles each file independently, so an importing model's own entry points are validated when THAT model loads.

> ⚠️ **An inherited gate's givens bind in the ENTRY model, not the model that declared the gate.** The filter is compiled against the model the query enters through, and Malloy mints a fresh identity per `given:` declaration — so a model that imports a gated source and re-declares one of the base's given names re-points that gate to its own declaration, including its default. A base gated `#(access_filter) tenant_id = $TENANT` with no default for `TENANT`, imported into a model declaring `given: TENANT :: number is 99`, is refused at load with a 424 naming `$TENANT` and its default — G4 runs at every entry point through which the gate is reached, own or inherited, checked against the given surface that entry model actually exposes, however many import hops separate it from the declaring source. Do not re-declare a base's given names in a model that imports its gated sources; if you do, give the re-declaration no default, or G4 refuses the load.

### Error contract & redaction

- **Runtime 403** names only the source — `{"code":403,"message":"Access denied for source \"orders\"."}` — never the gate expression. Gate logic is not leaked to (potentially untrusted) query callers.
- **Model-load 424** *keeps* the gate's expression text in its message — it is author-facing at package load and you need it to fix a malformed gate.
- **Each reported list is the author's expression, whether declared on the source itself or carried in from elsewhere** — a base it extends, a query-source base, a composite member. It is not one element per source: gates are flattened into a single array, so a query source over a composite reports the same authored gate twice (once from its base, once from the resolved member). Elements from different declaring sources are AND-ed at enforcement time even though the array does not show that. Use the field to decide *that* a source is gated; do not recompute the access decision from it.
- **Introspection is not redacted, and shows inherited gates too.** A source's `authorize` and `accessFilter` lists in the API report the expressions gating it, including ones it inherits from a base in another file that is not itself listed. That is deliberate — a list that omitted the inherited case would report a locked source as unrestricted, which is the more dangerous error — but it means gate logic is readable by anyone who can list sources. Same trust assumption as the rest of this page: keep the API behind the trusted tier.

## Security model

**The floor under all of this is structural, not a property of any particular predicate you write.**
Three facts hold regardless of the gate's shape: a gate that cannot be decided or grafted denies
rather than admits (see [Fail-closed](#enforcement)); G4 refusing every defaulted given, combined
with Malloy's own failure for an unsupplied one, means a gate given the caller omits fails the
request before anything executes rather than silently resolving to a default; and `in` against an
empty array compiles to a live `WHERE FALSE`, so an empty or omitted array-typed given denies rather
than matching everything. None of the three depends on what the predicate looks like — a gate that
is a single row-level term, a source-level term, a constant `false`, or a joined-field comparison all rest on the same floor. What is
*not* structural is everything the [known limitations](#known-limitations) above cover: a join that
doesn't carry the gate, a rename-collision that misbinds it to the wrong column, a scalar/array
mismatch that only fails at the warehouse. Those are gaps in the specific model, not in the mechanism.

Both annotations evaluate expressions over **request-supplied givens**. There is no authentication in Publisher's query path: a given is whatever the caller sends. So:

- **Authorize is a real boundary only behind a trusted tier.** The intended deployment is Publisher behind an embedding application that authenticates end users and sets givens (role, tenant, region) from its own *verified* context, with the query/MCP API network-isolated from untrusted callers. In that setup the gate enforces the trusted tier's policy.
- **It does not defend against a caller who sets their own givens.** Exposed directly to untrusted users, anyone can send `{"ROLE":"admin"}` and pass an `'admin' = $ROLE` lock. Do not treat either annotation as end-user authn/authz on a public endpoint.
- **Identity-bound givens** — a verified token or trusted-proxy header populating reserved "system givens" the caller cannot override — is a planned milestone that would make authorize a standalone boundary. It is not implemented yet.

### Authorize bypass, for trusted data-management callers

A `POST /…/query` request carrying the header `x-publisher-bypass-authorize` with the value of the server's `PUBLISHER_BYPASS_AUTHORIZE_SECRET` runs with gate evaluation **skipped**. The bypass is unavailable unless that variable is set: with it unset the header is ignored and gates are enforced. It exists for data management: an indexer or similar back-office caller is a machine identity with no givens, so a gated source returns 403 and is never scanned — an author who tags a dimension `#(index)` on a gated source otherwise gets an empty index, the tag having opted in and the gate having silently revoked it.

What it does and does not touch:

- **Only** the gate's condition is skipped. The author's own `where:` clauses still narrow the scan, row and byte caps still apply, restricted mode still bans raw SQL, and a gate declared in caller-submitted text is still rejected with a 400.
- It is **not** `bypassFilters`, a separate deprecated `#(filter)`-only control in the request body. Neither reads or writes the other.
- Notebook cells, `/compile`, and MCP accept no bypass at all.

**Publisher does not bound who may send it.** There is no authentication in the query path, so the header is exactly as trustworthy as the network in front of it — and the header name is published here, so treat it as known to anyone. **A deployment that reaches untrusted callers must strip this header at its edge.** [docs/authorize-bypass-deployment.md](authorize-bypass-deployment.md) is the operator's page: what to strip, why an allowlist beats a blocklist, and how to tell whether a bypass ever happened.

Note what the residual case is if the strip is missing. Publisher has no tenant boundary of its own, so a fronting application's own authorization still decides which packages a caller reaches; what the header removes is the **in-model** gating — role- or row-level policy *within* data that caller is otherwise entitled to reach.

Every use is counted (`publisher_authorize_bypass_total`, labelled `entry_point`) and logged (`authorize bypass`, with source / model / package). The counter is the rate signal; the log line is what an investigation reads. Read the two carefully: `runnable` fires on every bypassed query, `source` only when a run target was resolvable before compilation, so they are not always paired.

### Gate metrics

`publisher_authorize_lock_total`, labelled `decision`, covers the lock: `admitted`,
`denied_by_lock` (the gate refusing a caller it does not admit — routine, and a 403), and
`denied_unresolvable` (the fail-closed refusal when the gate could not be decided at all). Only the
third is a signal that something is wrong. It is a separate counter rather than a third `decision`
on the row-level one below, whose `denied_by_gate` is documented as the fail-closed case operators
alert on; folding routine refusals in would fire that alert on ordinary traffic.

Two more counters cover row filters specifically:

- `publisher_authorize_row_level_total`, labelled `decision` — exactly two values, `denied_by_gate` and `empty_after_filter`. `denied_by_gate` is the fail-closed refusal when a gate's expression could not be resolved against the entry point, on either route; `empty_after_filter` is a normal 200 with zero rows after the filter matched none, which is not an error.
- `publisher_authorize_row_level_rejected_total`, labelled `cause`. The label set is generated from the `RowLevelGateRejectionCause` union (`ROW_LEVEL_GATE_REJECTION_CAUSES` in `authorize.ts`, which is the single source of truth — read it there rather than trusting this list). `entry_point_unexpressible` is the inherited-and-unexpressible case above: the load warns naming the entry point rather than aborting, and that one entry point denies every request. `unreachable_given` and `given_usage_unresolvable` are request-time classification failures — a gate whose given lands off the model's own surface, or whose lifted condition references a field the graft target cannot resolve. `unclassifiable_condition` is a lifted condition carrying no usable expression at all, an IR shape not otherwise expected. `legacy_string_gate` is the only cause that fires for a **load refusal**, and it is the one worth alerting on — but only for the string-form annotation the load-time check can see. A hard load-time failure of the grammar itself (`AuthorizeGrammarRejectionCause`, see [Expression Language](#expression-language)) fails the whole model load but does **not** increment this counter today — it surfaces only as a load error on the package, not as a metric. `source_line_gate_negated_membership` is unreachable in practice now: the grammar refuses that shape (`compound_boolean`) before this classifier ever runs. `source_line_gate_no_given_reference` is NOT unreachable — it still fires (as a non-fatal load warning, W1) for the two field-less bodies the grammar accepts, the `false` deny-all and the `true` admit-all (see [Declaring Gates](#declaring-gates)). Both are deliberate declarations rather than the authoring mistake the warning names, and W1 runs per ENTRY POINT rather than per declaration, so one sentinel on a base warns again for every source that inherits it. A model using the locked-base pattern therefore emits several of these by construction: do not alert on this cause alone.

`publisher_authorize_admit_all_total`, labelled `route`, counts the sources that DECLARE an unconditional `#(authorize) true` admit-all, once each at package load. **The label's meaning changed at the route split:** `route="authorize"` meant the ROW route in 0.4.1 and earlier, and means the LOCK now. A source that merely inherits one is not counted, so the number tracks authoring decisions rather than model shape, and it is a step function on publish rather than a rate. It is not an error signal — `true` is the only spelling for an extension that deliberately re-opens a gated base — but it is the one declaration that turns a gate off, so a jump since the last publish is worth a look, and the number answers "how many sources are gated open" without reading every model.

`publisher_authorize_guard_rejected_total`, labelled `field` (`query` | `source_name` | `query_name` | `compile_source`), counts requests rejected with 400 for declaring either annotation in caller-submitted Malloy text.

**This is an interim answer, not the intended one.** The shape that keeps the decision with the model author is identity-bound givens (above) — a reserved system given the caller cannot set. That shape does not compose with `or` any more than any other disjunction does (see [OR semantics](#or-semantics)), so it would need its own extension source (`#(access_filter) 'indexer' = $SYSTEM_CALLER`) rather than a second arm bolted onto an existing gate. This header removes the gate globally instead, for callers the deployment trusts wholesale. When identity-bound givens land, this should narrow or go.

## Known limitations

- **A request can be exempted from the gate entirely** (see [Authorize bypass](#authorize-bypass-for-trusted-data-management-callers)). `x-publisher-bypass-authorize` on a query request skips evaluation when it carries the server's configured `PUBLISHER_BYPASS_AUTHORIZE_SECRET`. With that variable unset the bypass is refused, so the default is closed; on a deployment that sets the secret and does not strip the header at its edge, every gate is advisory for any caller who learns the value. Listed first here because it is the only limitation on this page that a *deployment*, not a model, has to close.
- **A gate does not follow a join** (see [above](#the-entry-point-and-only-the-entry-point)). This is the limitation with the largest practical consequence: any source that joins gated data and is itself ungated hands that data to every caller. Treat "which sources can a caller enter through, and what does each of them reach" as part of modelling, not as something the gate handles for you.
- **`except:`-ing, projecting away, or `accept:`-not-relisting the column a gate reads fails CLOSED** (see [Row-level gates](#row-level-gates)) — the graft cannot resolve, so the affected entry point warns at load and denies every request at 403. This is the reverse of the retired dimension form's own known gap, where the equivalent derivation dropped the gate as a plain field and served every row silently. One misbinding hole is pinned by a test: `except:`-ing the gated column and then `rename:`-ing a *different* column onto that exact name, where the graft resolves again but against the wrong data — see [Row-level gates](#row-level-gates) for the example. It is **not** established as the only one, and this page previously claimed it was: a rename chain, an `except:` plus a join reintroducing the name, and a composite member supplying it should all reproduce the same class, and none of those is tested. The author rule that covers all of them is not to recycle a gated column's name.
- **An extension's own gate replaces the base's** (see [above](#the-entry-point-and-only-the-entry-point)) — that is the curated-extension idiom, so pair locked bases with access modifiers to keep the re-exposed column surface deliberate. (An extension with no gate of its own carries the base's.)
- **A source gated ONLY by `#(access_filter)` is a schema oracle.** A filter is grafted into the query and evaluated with it, so compilation happens first, and a caller gets Malloy's own compile errors for the source even when the filter then admits them no rows: a malformed probe (`group_by: no_such_field`) returns "field is not defined", confirming whether a column exists. A `#(authorize)` lock closes this for the caller it refuses — it is decided before their query compiles — so a source that should not answer schema questions to outsiders wants a lock, not only a filter. Behind the trusted tier the exposure is a column name, not data — see [security-posture.md](security-posture.md#row-level-access-rows-are-protected-the-schema-is-not).
- **`/compile` raw SQL is gated at `append` only.** The gate covers named Malloy sources. `/compile` at the default `append` scope now compiles under Malloy's restricted mode, the same refusal `/query` applies, so raw `duckdb.sql(...)`, `connection.table(...)`, `import` and the raw-SQL function forms are refused there, as are a `given:` declaration (a `$NAME` reference to a given the model already declares is still accepted) and a `##!` compiler-flag annotation. Text that parses only as a continuation of the model's last statement is refused too, since the check reads the fragment by itself. `file` and `package` still compile unrestricted, and `scope` is a caller-chosen request field with no authorization difference between its values, so a caller refused at `append` can ask for `file`. Restricting those two would make an ordinary package un-authorable, since that is where a model legitimately declares what it reads; containing them means authorizing the scope, which is separate work. Keep `/compile` behind the trusted tier.
- **A wrong-shape gate can fail loud at load, or fail confusingly at request time, depending on exactly what it does** (see [Row-level gates](#row-level-gates)). A scalar comparison against an array-typed given loads cleanly and only crashes when the warehouse executes it; a negated membership test loads with a warning rather than being refused. Both are new relative to the retired string form's stricter, load-time-only grammar.
- **A gate expressing genuine disjunction (admit population A OR population B) has no single-source spelling.** `or` is refused at load (`compound_boolean`), and repeated notes on one route AND together rather than OR, so the replacement is two sources — see [OR semantics](#or-semantics). The sentinels do not help here: `false` denies everyone and `true` admits everyone (see [Declaring Gates](#declaring-gates)), and neither can share the lock route with the term that would have been the disjunction's other arm.
- **A gate can only reference a given on the entry model's own surface.** A gate naming a given the entry model does not declare is refused outright (`unreachable_given`) rather than guessed at, because Malloy merges only one level of `import` and the gate would otherwise silently bind that given's *declaration default* instead of the caller's value. Practical effect: to gate a source through a base two or more import hops away, `import { NAME } from …` the SAME declaration into the entry model rather than writing a fresh `given: NAME …` of your own — importing carries the base's given identity (and its no-default-ness) forward; re-declaring mints a new one. **Do not "fix" `unreachable_given` by re-declaring the name with a default** — see the warning [above](#validation): G4 checks that re-declaration too, at every entry point the gate reaches, and refuses the load if it carries one, however many import hops separate the entry model from the source that declared the gate.
- **A gate given the caller does not supply denies opaquely.** The gate's givens bind with the query's, so Malloy's own failure for an unsupplied one names it ("Given 'ROLE' has no value and no default"). Publisher maps that back to the same `Access denied for source "…"` 403 — a denied caller is never told which given the gate reads.
- **A notebook cell that both declares a gated source and runs it in the same cell, with a
  joined-field gate, needs the run query to reference the joined field.** A cell's row-level gate
  filters correctly whether it declares the gated source itself or inherits one declared earlier
  — with one narrow exception: when the
  gate is on a JOINED field (`#(access_filter) childtable.name in $GROUPS`) and the cell's own `run:`
  query never itself references that joined field, the cell denies with a 400 rather than serving
  filtered rows (never a leak — no rows are returned either way). Reference the joined field
  somewhere in the run query's own projection or grouping to avoid it.
- **A given the entry model does not surface is usually refused, not ignored.** Malloy's given-namespace merge covers only one level of `import`, so a value for a given the entry model doesn't surface reaches Malloy's own resolution and errors (`unknown given`) rather than being silently dropped and falling back to a default. The one exception is a name some reachable gate actually references: that value is forwarded to the gate and withheld from the query (`filterGivensToModelSurface`, `model.ts`), which is what lets a gate carried in from a base in another file evaluate at all. Author-side implication: for a caller to supply a given the query itself reads, some source or file within one import hop of the entry model must declare (or import) it.

## Runnable example

[`examples/governed-analytics`](../examples/governed-analytics) gates a real source with a
row-level allow-list term in
[`secured.malloy`](../examples/governed-analytics/secured.malloy):

```malloy
given:
  TENANTS :: string[]

#(access_filter) tenant in $TENANTS
source: orders_secured is orders_base extend {
  ...
}
```

`TENANTS` carries no default — the gate references it, and G4 refuses a referenced given that has
one. There is no separate admin role: a caller whose identity resolves to every tenant on the list
is handed all of them and simply sees all of them, through the same gate as everyone else. An
omitted `TENANTS` fails to resolve and denies with a 403, there being no default left to fall back
to. Against a running server, the `governed-analytics`
package ships in the default `examples` environment (see the
[example's README](../examples/governed-analytics/README.md)):

```bash
API=http://localhost:4000/api/v0/environments/examples/packages/governed-analytics/models

# No identity at all → denied with a 403: TENANTS never resolves, so the gate
# never binds to evaluate against.
curl -s -X POST $API/index.malloy/query -H 'content-type: application/json' \
  -d '{"query":"run: orders_secured -> by_status"}'                                        # → 403

# Resolved to every tenant → allowed (all rows).
curl -s -X POST $API/index.malloy/query -H 'content-type: application/json' \
  -d '{"query":"run: orders_secured -> by_status","givens":{"TENANTS":["acme","globex","initech"]}}'  # → 200
```

The row-level half of that source — how `where:` narrows an *allowed* caller to their own rows — is
covered in [row-level-access.md](row-level-access.md).
