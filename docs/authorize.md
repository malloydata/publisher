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

A gate is a boolean **dimension**, declared in field position inside the source's own body and tagged `#(authorize)`. The dimension's own expression is the gate — a Malloy boolean expression over declared givens (`$NAME`), row fields, or both:

```malloy
##! experimental.givens

given:
  ROLE :: string

source: orders is duckdb.table('orders.parquet') extend {
  measure: order_count is count()

  #(authorize)
  internal dimension: authorized is $ROLE = 'analyst'
}
```

- **A source may declare at most one gate dimension.** The annotation identifies the *field*, not a fixed name — `authorized` above is only a convention; name it whatever reads best. Declaring a second `#(authorize)`-tagged dimension on the same source fails the load naming both. See [OR semantics](#or-semantics) for how to combine more than one condition.
- **Use `internal`, never `private`.** The graft that enforces the gate references the dimension by name from outside the source (`extend { where: (authorized) }`), so a `private` gate dimension would compile the model but never be reachable by its own enforcement — Publisher refuses `private` on a gate dimension at load, naming the fix. `internal` still blocks a caller's own direct `select:`/`where:` reference to it; it is permeable only to the graft's own shape.
- A source with no annotated dimension is **unrestricted**.

> **`##(authorize)` (file-level) is deprecated and refused at load.** An earlier version of this
> feature let an annotation written above the file, rather than a source, apply to every source in
> it as a model-wide override. That capability was withdrawn — the raw-warehouse path it existed to
> close is already closed unconditionally by restricted mode (any caller-submitted `duckdb.sql(...)`/
> `duckdb.table(...)` is rejected before any gate runs), so the model-wide reach bought no additional
> protection and was easy to reason about incorrectly. A `##(authorize)` annotation anywhere in the
> model — including one folded in from an imported file — now fails the load with a message naming
> the remedy: declare a gate dimension on each source it was meant to protect.

> **The string form — `#(authorize) "<expr>"` on the `source:` line itself — is retired and refused
> at load.** If you are looking at an older example that annotates the `source:` line with a quoted
> expression, it is the form this page used to describe; it no longer loads. The refusal names the
> rewrite: move the expression into a field-position dimension, as above.
>
> **Known blind spot: that refusal only sees a string-form gate declared within one `import` hop of
> the model being loaded.** The check reads only the annotations a source declares in the model
> currently loading (`authorizeOwnNotes`) — the same one-hop reach [givens.md](givens.md) describes
> elsewhere. A string-form gate declared two or more import hops from the entry model is invisible
> to it, so the model that declared the gate is never refused and never rewrites it for you. It still
> denies: at request time the gate fails to classify as either the dimension form or a bare `false`
> literal, so it is treated as an unclassifiable shape and every request against it is refused. What
> you get is not the load-time message naming the exact rewrite — it is a plain denial, counted under
> `publisher_authorize_row_level_rejected_total{cause="legacy_string_gate"}` with no author-facing
> detail. Fail-closed, but confusing: a package with a string-form gate this far from its entry
> points loads, appears to work, and then denies every caller with no compile-time hint of why. If a
> deployment predates this feature, check for the string form across the whole corpus, not just what
> a given package's own load errors surface.

### Expression Language

The dimension's expression is an ordinary Malloy boolean expression — anything Malloy itself accepts as the body of a boolean `dimension:`, over `$given` references, row fields, and literals:

```malloy
#(authorize)
internal dimension: authorized is $ROLE = 'analyst'

#(authorize)
internal dimension: authorized is $REGION = 'us-west' and $ROLE != 'guest'

#(authorize)
internal dimension: authorized is org_id in $GROUPS
```

Unlike the retired string form, Publisher does **not** validate the expression against a restricted grammar of accepted shapes — see [Row-level gates](#row-level-gates) for what changed and why that matters for what fails at load versus at request time.

Embedded string literals follow ordinary Malloy syntax: single-quote them as usual (`$ROLE = 'analyst'`) — there is no surrounding quoted-annotation layer left to escape out of, since the expression is now Malloy source, not a string.

## Row-level gates

A gate expression may reference a row field — its own source's, or one reached through a join — instead of only givens and literals. **Every gate is enforced the same way** regardless: as a row filter on the entry-point source. A gate that reads no field is constant across every row, so it still admits all of them or none; it is not a separate mechanism. See [Row-level access](row-level-access.md#row-level-authorize) for what that means for the caller, and [security-posture.md](security-posture.md#row-level-authorize-rows-are-protected-the-schema-is-not) for the trade it makes.

```malloy
#(authorize)
internal dimension: authorized is org_id in $GROUPS

#(authorize)
internal dimension: authorized is childtable.name = $BOB

#(authorize)
internal dimension: authorized is region = $REGION and tier != $EXCLUDED_TIER
```

**There is no accepted-shape allowlist any more.** The retired string form validated its expression against a small allowlist of comparison shapes (`classifyAuthorizeGate`) before it could ever be attached as a filter — a function call, `like`, `is not null`, or a `not in` failed the load outright. The dimension form does not classify the expression's shape at all: validation reads the compiled dimension's own type (must be a scalar boolean) and its given references (must resolve, must carry no default — see below), and the graft attaches it **by name** (`extend { where: (authorized) }`), letting Malloy evaluate whatever the dimension computes. Concretely, `upper(region) = $REGION`, `region like $PAT`, `region is not null`, and `amount + 1 > $AMOUNTMIN` are all legal gate dimensions today, where the string form refused every one of them at load.

**This trades an early, named refusal for a request-time failure in one specific case.** The string form additionally checked a given's *declared type* against the operator used on it, so a scalar comparison against an array-typed given (`org_id = $GROUPS`) was refused at load with a clear message. The dimension form has no such check: `org_id = $GROUPS` compiles cleanly (Malloy itself does not reject it at that point), grafts cleanly onto the entry point, and only fails when the warehouse tries to execute the comparison, surfacing as a request-time execution error rather than a load-time 424. The failure is safe — it errors and serves no rows — but it lands as a warehouse error, not a diagnostic on the author's own line:

```
Conversion Error: Type VARCHAR with value 'org1' can't be cast to the destination
type VARCHAR[] when casting from source column org_id
```

If you write a gate dimension comparing a row field to an array-typed given, use `in`, not `=`/`!=`/`>`/`<` — and note `org_id = $GROUPS` and `org_id ? $GROUPS` are the same comparison node, so neither spelling escapes it. `in` against a *scalar*-typed given (`owner in $ROLE`) is still refused — that one is ordinary Malloy type-checking, not anything `#(authorize)`-specific, so it still fails at load.

**A negated membership test is accepted, with a load-time warning.** `not (org_id in $GROUPS)` used to be refused outright by the string form's grammar. The dimension form accepts it — G1 only asks "is this a scalar boolean dimension", which a negation satisfies — and it filters correctly for a non-empty given. The warning exists because an **empty** given then matches every row rather than none (the negation of "in nothing" is "true" for every row), the opposite of what `in` alone does for an empty given. The load succeeds with a warning naming the source; decide per-gate whether that empty-given behavior is what you want.

**A given with a declared default is refused outright, whether or not the expression is field-less.** The string form's "vacuous admin-override atom" check applied only to a `<given> <operator> <literal>` atom (`$ROLE = 'admin'` with `ROLE` defaulting to `'blocked'`); a field-referencing comparison against a defaulted given was checked separately and less strictly. The dimension form's **G4** rule is a single, unconditional check: *any* given the gate expression references, anywhere in it, must be declared with no default. An unsupplied given would otherwise silently resolve to its default and admit or exclude rows the gate meant not to, so this is refused at load rather than reasoned about case by case — declare every given a gate references with no default.

**The given a gate references must be on the gating model's own given surface** — declared there, or reachable through one `import` hop, the same reach [givens.md](givens.md) describes for the ambient namespace. Malloy does not flatten a `given:` declaration past that one hop, so a gate whose given lives further away would otherwise silently bind that given's own declaration DEFAULT rather than the caller's value. Rather than risk that, it is refused at load instead (**G3**), naming the fix: import the given (`import { GROUPS } from "…"`).

> **A gate on a joined field turns a `join_one` LEFT JOIN into an INNER JOIN.** The filter is applied inside the entry source's own build, before any aggregation, so a parent row with no matching child — and so no value to satisfy the gate — drops out of the result entirely rather than surviving with nulls. That is fail-closed (a row the gate cannot evaluate is a row it does not admit), but it changes cardinality invisibly if you expected the join's usual left-join behavior.

**The pre-compile check never grants, and rarely denies now either.** Every gate is now the dimension form's `row_level` shape, so the request-time graft is what decides admission — there is no more shape-based pre-compile classification that can refuse a gate before compiling. What is still checked before there is a compiled query is whether the gate's *field* can be found and referenced at all (an entry point that renamed or dropped it); that failure denies with a 403 rather than serving unfiltered. See [Enforcement](#enforcement).

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

**Declare the gate dimension via `extend {}` after the persist source's own `-> { … }` pipeline, not selected inside it.** Selecting the gate dimension as an output column of the persist source's own defining query (`-> { select: …, authorized }`) forces the build step that compiles the physical `CREATE TABLE AS` — which runs with no request/given context at all — to evaluate the dimension at *build* time: a given-referencing gate then fails the build itself, and even a givenless one has produced SQL that qualified the filter against the wrong table alias. Adding the gate dimension with a trailing `extend {}` instead keeps it a lazy field layered on the persisted query's own output, exactly how it behaves on an unpersisted source, and the by-name graft resolves against it normally:

```malloy
#@ persist name="orders_summary"
source: orders_summary is orders_raw -> {
  group_by: org_id, category
  aggregate: total is amount.sum()
} extend {
  #(authorize)
  internal dimension: authorized is org_id in $GROUPS
}
```

## Semantics

### OR semantics

**A source may declare at most one gate dimension, so combine conditions with `or` inside that one expression** — there is no longer a way to stack several `#(authorize)`-tagged dimensions on one source to mean OR; a second one fails the load, naming both:

```malloy
given:
  ROLE :: string
  TENANT :: string

source: orders is duckdb.table('orders.parquet') extend {
  #(authorize)
  internal dimension: authorized is $ROLE = 'admin' or $TENANT = 'acme'
}
```

`orders` is queryable by an admin **or** by an acme-tenant caller. This is a real change from the retired string form, which read multiple stacked `#(authorize) "<expr>"` annotations on one source as an implicit disjunction (`(a) or (b)`) — that stacking no longer exists; every OR has to be written out in the single gate expression.

To give one role access to every source in a file, declare the same admin-override sub-expression as one arm of each source's own gate dimension rather than reaching for a model-wide gate — a file-level override existed for this once; it is deprecated (see [Declaring Gates](#declaring-gates)).

### The entry point, and only the entry point

Authorize is checked on the source the query **enters through** — the run target. A gate answers "who may query this source", not "who may read everything reachable beneath it".

- **Joins are not gated.** A gate on a source reached only via `join_*` **does not fire** — at any depth (A→B→C), aliased, across files, declared in a query-local `join_one` inside a `-> { … }` refinement, or as a member of a joined composite. `run: joiner -> { … }` where `joiner` joins a base gated `internal dimension: authorized is false` returns rows. **This is the rule authors have to design around:** joining sensitive data into an ungated source publishes it. Put the gate on the source callers enter through, and use [access modifiers](https://docs.malloydata.dev/documentation/experiments/include) (`include { public: …, private: * }`) to control what an extension re-exposes.
- **A gate MAY reference a field on a joined source — that is not the same as the joined source's own gate firing.** `internal dimension: authorized is childtable.name = $BOB` declared on the entry point itself is enforced: the join it needs is emitted as part of the entry point's own build. The rule above is unchanged — a gate declared ON `childtable` still does not fire when `childtable` is only reached via a join from an ungated entry point. Referencing a joined field from the entry point's *own* gate, and a joined source's *own* gate firing, are two different things; only the first is supported.
- **Extend: own gate replaces, otherwise the base's carries.** `source: b is a extend { … }` is governed by `b`'s own gate dimension when it declares one — that replacement is the [curated-extension idiom](#recommended-pattern-locked-base-and-curated-extensions). When `b` declares none, Malloy's `extend` flattens the base's unchanged fields into `b`'s own `fields`, so `a`'s gate dimension (still carrying its annotation) is found there and gates `b` too. That holds however `b` is decorated: a render tag or doc comment on `b` does not remove it.
- **`b` redefining the gate dimension's NAME without re-annotating it fails the load,** naming the source and the field — silently shedding an inherited gate by redeclaring its name is refused rather than left to admit everything. **One known gap:** `b is a extend { except: authorized … }` followed by a bare (unannotated) `dimension: authorized is …` redefinition is not caught by this check — Malloy leaves no discoverable link from `b` back to `a` for that specific `except:`-then-redefine shape, so `b` loads cleanly and its `authorized` silently reads the new, ungated predicate instead of `a`'s. See `gate_dimension_integration.spec.ts`'s `KNOWN GAP` test.
- **A caller cannot shadow the gate dimension.** A query that tries to redefine the field by name — `run: X extend { dimension: authorized is true } -> { select: id }` against a source whose own `authorized` is `#(authorize)`-gated — does not shadow it, and does not lose a race to it either: Malloy refuses the redefinition outright at compile time ("Cannot redefine …"), so the query never runs at all. This is a real guarantee, not an artifact of enforcement order. The usability cost is symmetric: a caller who merely happens to need a field with the same name gets the same compile error, gate or no gate — the name is reserved the moment a source declares a gate dimension under it. See `gate_dimension_integration.spec.ts`'s `PIN 1`.
- **A MODEL-AUTHORED derived source that drops the gate dimension entirely fails OPEN, silently, with no warning** — `source: laundered is locked_src extend { except: authorized }`, or even a plain `accept: id, org_id` that just doesn't re-list it, produces a source with no gate dimension of its own and none inherited (the field is simply gone from `fields`), so it is treated as unrestricted. This is the one guarantee the string form provided that the dimension form does not: the string form's annotation lived on the `source:` line itself and survived a field-level `except:`/`accept:`, so the equivalent derivation failed **closed** there (403, plus an operator warning) rather than open. This is confirmed unfixable in the current design — the gate is now *just a field*, and Malloy's own field-exclusion syntax has no way to know that one particular field carries special weight. **Model authors must treat a gate dimension's name as a field no derivation may ever drop**, the same discipline `include { private: * }` already asks for other sensitive columns. This is scoped to derivations the *model* declares: the same text submitted by a **caller** (`source: mine is locked_src extend { except: authorized }`, then `run: mine -> { … }`) is denied with a 403, at any alias depth, because the run target is a derivation the request's own text declared and Publisher can follow that text back to the gated base. See `gate_dimension_integration.spec.ts`'s `caller-submitted derivations cannot launder the gate away`.
- **Query-source derivation carries the gate too**, the same way `extend` does — `source: laundered is locked_src -> { … }` is gated by `locked_src`'s gate when it declares none of its own. Derivation is treated like `extend` for this purpose; reaching the same derived source via a *join* is still not gated.
- **A composite run target resolves precisely.** When the run target is `compose(a, b)`, Malloy resolves it to exactly one member branch per query and that branch's gate applies. For a colocated `#@ persist` over a composite, the same by-name graft limitation as query-source derivation applies: if the persist source's own projection does not carry the winning member's gate field forward, the name can't be grafted and the source is excluded from the build plan rather than served unfiltered.
- **Caller-submitted text may not declare a gate at all.** A field-position `#(authorize)` annotation (the deprecated `##(authorize)` and string-form spellings included) in the `query` text of a query request, or in the `source` text submitted to `/compile`, is rejected with a 400: a gate is the model author's to declare. Notebook cells are package content, so an author's gate there works normally.

### Worked example

One model, six entry points. The rules above are subtle enough that it is worth reading the verdicts rather than deriving them:

```malloy
##! experimental.givens

given:
  ROLE :: string

// Locked: nobody queries this directly.
source: salaries is duckdb.table('salaries') extend {
  #(authorize)
  internal dimension: authorized is false
}

// (1) No gate of its own → the base's carries (extend flattens the
//     unchanged, still-annotated field into salaries_plain's own fields).
//     Still locked.
source: salaries_plain is salaries extend {
  measure: headcount is count()
}

// (2) A render tag does NOT change that.
# bar_chart
source: salaries_tagged is salaries extend {
  measure: headcount is count()
}

// (3) Its own gate REPLACES the base's — the curated-extension idiom.
source: salaries_hr is salaries extend {
  measure: avg_salary is avg(salary)
  #(authorize)
  internal dimension: authorized is $ROLE = 'hr'
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

A constant-`false` gate compiles to a filter that matches nothing, so those rows are withheld with a **200 and an empty result**, not a 403 — see the note on denial shape at the top of this page. Publisher recognizes a provably-constant-`false` gate and answers it without dispatching anything to the warehouse.

Entry point (5) is the one to internalize: `headcount_by_dept` reads salary rows and is queryable by anyone. That is not a bug, it is the rule — **joining sensitive data into an ungated source publishes it.** If `headcount_by_dept` should be restricted, give it its own gate; if it should expose only aggregates, use `include { public: headcount, private: * }` so the join cannot be drilled through.

And the caller cannot mint a gate to escape one:

```jsonc
// POST /…/models/hr.malloy/query — rejected with 400, not compiled
{
  "query": "source: mine is salaries extend { #(authorize)\n internal dimension: authorized is true }\nrun: mine -> { select: * }"
}
```

## Recommended pattern: locked base and curated extensions

Lock sensitive base sources with a `false` gate dimension and re-expose curated subsets through extension sources, each with its own gate and access modifiers:

```malloy
##! experimental.givens
##! experimental.access_modifiers

given:
  REGION :: string
  ROLE :: string

// Base source: locked. Direct queries are denied — the gate dimension
// is a constant `false`.
source: customers_raw is duckdb.table('customers.parquet') extend {
  #(authorize)
  internal dimension: authorized is false
}

// Extension: re-exposes a curated subset and adds an analyst-role gate.
// `private: *` hides every other column on the base.
source: customers_marketing is customers_raw include {
  public: name, region, signup_date
  private: *
} extend {
  measure: customer_count is count()
  #(authorize)
  internal dimension: authorized is $ROLE = 'analyst'
}

// A second extension with a different gate and field surface.
source: customers_us_west is customers_raw include {
  public: name, region, signup_date, lifetime_value
  private: *
} extend {
  #(authorize)
  internal dimension: authorized is $REGION = 'us-west'
}
```

- `run: customers_raw -> …` → **200 with no rows** (gate is `false`; nothing is readable).
- `run: customers_marketing -> …` → allowed with `$ROLE = 'analyst'`; the consumer can only touch `name`, `region`, `signup_date`. Supplying no `$ROLE` at all is a 403, since the gate's given cannot bind.
- `run: customers_us_west -> …` → allowed with `$REGION = 'us-west'`, on a different surface.

The `include { … private: * }` layer is what controls which base columns each extension can re-expose; each extension's own gate dimension gates consumer access to that curated surface. The base's constant-`false` gate is a defense-in-depth backstop against a direct `run: customers_raw`. Remember the fail-open hazard above: neither extension's `include { … }` may drop the gate dimension's own field, or that extension silently ungates itself.

## Enforcement

The gate runs, fail-closed, on every query entry point. Enforcement is in two parts:

1. **A field-resolution check.** Where the run target can be resolved from the request's surface syntax, Publisher confirms the gate's field is still reachable on that entry point before compiling anything. An entry point that has renamed or dropped it is refused here with a **403**.
2. **The graft.** The gate dimension is attached to the compiled query as a source-level `where:` on the entry point, referenced by name, and evaluated by the same `run()` as the query itself.

Step 2 is what a gate actually resolves through: a gate's condition is compiled *into* the query rather than decided by a separate probe beforehand. The practical consequences are listed under [Known limitations](#known-limitations): a denial by filtering is a 200, and a caller who compiles a deliberately malformed query against a source it may read nothing from still sees Malloy's compile errors for that source.

There is one documented exception, [the authorize bypass](#authorize-bypass-for-trusted-data-management-callers), which applies to `POST /…/query` only:

| Entry point | Behavior |
| --- | --- |
| `POST /…/query` | Gate the run-target source; deny → zero rows where the filter applied, 403 where the field could not be resolved. Skipped entirely when the request carries `x-publisher-bypass-authorize: true`. |
| `POST /…/projects/…/query` (legacy alias) | Gate as above. Accepts no bypass — it exists for pre-rename SDK compatibility and passes no `givens` either, so a gated source is denied there regardless. Use the `/environments/…` route. |
| Notebook cell `GET` | Gate each cell that runs a query. Accepts no bypass. |
| `POST /…/compile` | Gate the named source the submitted text targets (early, before compiling, plus a compiled-source backstop). `/compile` never runs the query, so the backstop cannot attach a filter; it admits only a gate decided without running it — one that is constant-`true`, or one whose every given the caller supplied — and denies anything else. Note that `includeSql` then returns the **ungrafted** SQL, without the gate's `where:`. Accepts no bypass. |
| MCP `malloy_executeQuery` | Routes through the query path; a denial surfaces as `isError: true` naming the source. Sends no bypass. |

**Fail-closed.** Anything that stops the graft landing denies rather than admits. Where two *sources* gate one entry point — its own gate plus one carried from the source it derives from — each is grafted separately, which AND-s them.

### Validation

Authorize gate dimensions are validated at **model load** (compile-only, no execution). A misdeclared gate dimension (more than one on a source, `private`, an unreachable given, a given with a default) fails the load with **HTTP 424** (`ModelCompilationError`), naming the source and the underlying reason. Fix the model before it serves.

**There is no separate field-less-vs-field-referencing carve-out any more.** The retired string form's row-level grammar treated a gate reading no row field as a special class, refused per-source rather than per-model so one field-less gate published under the old rules couldn't take a whole model file out of service. The dimension form has no such split: G1/G3/G4 apply identically whether or not the expression happens to read a row field, and a field-less gate that references no given at all loads with a **warning** (W1, [Row-level gates](#row-level-gates)) rather than any refusal.

**A gate must resolve at every entry point through which the source declaring it is reached, and an entry point where it cannot is closed rather than opened — with the fail-open exception named above.** Where the entry point declares its **own** gate dimension, a validation failure (G1/G3/G4) is the author's own mistake and package load fails with a 424 naming the source. Where the entry point only **inherits** the gate dimension via `extend`, `except:`/`accept:`/`rename:` dropping the gate dimension's field ungates that entry point silently (see the fail-open note above) rather than warning; there is currently no load-time signal for this shape. This is distinct from *inheriting* a gate that still resolves (see the worked example above and [row-level-access.md](row-level-access.md#row-level-authorize)): there, the filter runs inside the base's own build, before the deriving source's projection ever applies, so a derived source that projects the gated column away still serves correctly filtered — there is no need to keep an inherited gate's column in the projection.

Validation covers only the entry points in the model being loaded. `compileMalloyModel` compiles each file independently, so an importing model's own entry points are validated when THAT model loads.

> ⚠️ **An inherited gate's givens bind in the ENTRY model, not the model that declared the gate.** The filter is compiled against the model the query enters through, and Malloy mints a fresh identity per `given:` declaration — so a model that imports a gated source and re-declares one of the base's given names re-points that gate to its own declaration, including its default. A base gated `internal dimension: authorized is tenant_id = $TENANT` with no default for `TENANT`, imported into a model declaring `given: TENANT :: number is 99`, serves tenant-99 rows to a caller who supplied nothing. This is an authoring hazard, not a caller-reachable bypass — a caller cannot introduce a declaration — but it means importing a locked source does not carry its gate's given namespace with it. Do not re-declare a base's given names in a model that imports its gated sources.

### Error contract & redaction

- **Runtime 403** names only the source — `{"code":403,"message":"Access denied for source \"orders\"."}` — never the gate expression. Gate logic is not leaked to (potentially untrusted) query callers.
- **Model-load 424** *keeps* the gate dimension's expression text in its message — it is author-facing at package load and you need it to fix a malformed gate.
- **The reported `authorize` list reports the gate dimension's own expression text (`getAuthorize()`), one element per source.** Where two *sources* gate one entry point — its own gate plus one carried from the source it derives from — the API reports both, flattened into a single array; those are AND-ed at enforcement time even though the array does not show that. Use the field to decide *that* a source is gated; do not recompute the access decision from it.
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
doesn't carry the gate, a derivation that drops its field, a scalar/array mismatch that only fails at
the warehouse. Those are gaps in the specific model, not in the mechanism.

`#(authorize)` evaluates expressions over **request-supplied givens**. There is no authentication in Publisher's query path: a given is whatever the caller sends. So:

- **Authorize is a real boundary only behind a trusted tier.** The intended deployment is Publisher behind an embedding application that authenticates end users and sets givens (role, tenant, region) from its own *verified* context, with the query/MCP API network-isolated from untrusted callers. In that setup the gate enforces the trusted tier's policy.
- **It does not defend against a caller who sets their own givens.** Exposed directly to untrusted users, anyone can send `{"ROLE":"admin"}` and pass a `$ROLE = 'admin'` gate. Do not treat `#(authorize)` as end-user authn/authz on a public endpoint.
- **Identity-bound givens** — a verified token or trusted-proxy header populating reserved "system givens" the caller cannot override — is a planned milestone that would make authorize a standalone boundary. It is not implemented yet.

### Authorize bypass, for trusted data-management callers

A `POST /…/query` request carrying the header `x-publisher-bypass-authorize: true` runs with gate evaluation **skipped**. It exists for data management: an indexer or similar back-office caller is a machine identity with no givens, so a gated source returns 403 and is never scanned — an author who tags a dimension `#(index)` on a gated source otherwise gets an empty index, the tag having opted in and the gate having silently revoked it.

What it does and does not touch:

- **Only** the gate dimension's condition is skipped. The author's own `where:` clauses still narrow the scan, row and byte caps still apply, restricted mode still bans raw SQL, and a gate declared in caller-submitted text is still rejected with a 400.
- It is **not** `bypassFilters`, a separate deprecated `#(filter)`-only control in the request body. Neither reads or writes the other.
- Notebook cells, `/compile`, and MCP accept no bypass at all.

**Publisher does not bound who may send it.** There is no authentication in the query path, so the header is exactly as trustworthy as the network in front of it — and the header name is published here, so treat it as known to anyone. **A deployment that reaches untrusted callers must strip this header at its edge.** [docs/authorize-bypass-deployment.md](authorize-bypass-deployment.md) is the operator's page: what to strip, why an allowlist beats a blocklist, and how to tell whether a bypass ever happened.

Note what the residual case is if the strip is missing. Publisher has no tenant boundary of its own, so a fronting application's own authorization still decides which packages a caller reaches; what the header removes is the **in-model** gating — role- or row-level policy *within* data that caller is otherwise entitled to reach.

Every use is counted (`publisher_authorize_bypass_total`, labelled `entry_point`) and logged (`authorize bypass`, with source / model / package). The counter is the rate signal; the log line is what an investigation reads. Read the two carefully: `runnable` fires on every bypassed query, `source` only when a run target was resolvable before compilation, so they are not always paired.

### Row-level gate metrics

Two more counters cover row-level gates specifically:

- `publisher_authorize_row_level_total`, labelled `decision` (`denied_by_gate` | `empty_after_filter` | `short_circuited`). `denied_by_gate` is the fail-closed refusal when a gate's field could not be resolved; `empty_after_filter` is a normal 200 with zero rows after the filter matched none, which is not an error; `short_circuited` is a provably constant-`false` gate answered with zero rows without ever querying the warehouse.
- `publisher_authorize_row_level_rejected_total`, labelled `cause`. Two causes are gate-dimension-specific and **non-fatal** — the model still loads: `gate_dimension_no_given_reference` (W1, the gate reads no given at all) and `gate_dimension_negated_membership` (W2, a negated membership test). A hard gate-dimension failure (more than one on a source, `private`, not a scalar boolean, an unreachable given, a given with a default) fails the whole model load but does **not** increment this counter today — it surfaces only as a load error on the package, not as a metric. Most of the remaining causes (`array_given_needs_in`, `scalar_given_rejects_in`, `field_given_has_default`, `unsupported_node`, `no_given_reference`, `unreachable_given`, `vacuous_default_atom`, `entry_point_unexpressible`) are legacy — emitted by the retired string form's own checks; a package written entirely in the dimension form should never produce them. `legacy_string_gate` is the exception still worth watching for: it fires both for the load-time refusal of a string-form annotation the check can see, and — with no load-time refusal at all — for every request against a string-form gate declared two or more import hops from the entry model (see [Declaring Gates](#declaring-gates)). A steady rate of it in a deployment predating this feature is evidence of the second case, not the first.

`publisher_authorize_guard_rejected_total`, labelled `field` (`query` | `source_name` | `query_name` | `compile_source`), counts requests rejected with 400 for declaring an `#(authorize)` annotation in caller-submitted Malloy text.

**This is an interim answer, not the intended one.** The shape that keeps the decision with the model author is identity-bound givens (above) — a reserved system given the caller cannot set, so an author writes `internal dimension: authorized is $ROLE = 'analyst' or $SYSTEM_CALLER = 'indexer'` and a source they never opted in stays gated. This header removes the gate globally instead, for callers the deployment trusts wholesale. When identity-bound givens land, this should narrow or go.

## Known limitations

- **A request can be exempted from the gate entirely** (see [Authorize bypass](#authorize-bypass-for-trusted-data-management-callers)). `x-publisher-bypass-authorize: true` on a query request skips evaluation, and Publisher does not bound who may send it — so on a deployment that does not strip the header at its edge, every gate is advisory for any caller who knows the name. Listed first here because it is the only limitation on this page that a *deployment*, not a model, has to close.
- **A gate does not follow a join** (see [above](#the-entry-point-and-only-the-entry-point)). This is the limitation with the largest practical consequence: any source that joins gated data and is itself ungated hands that data to every caller. Treat "which sources can a caller enter through, and what does each of them reach" as part of modelling, not as something the gate handles for you.
- **A model-authored extension that drops the gate dimension's field fails open, silently** (see [above](#the-entry-point-and-only-the-entry-point)) — a *caller* submitting the same derivation is denied instead. This is new relative to the retired string form, which stayed attached to the `source:` line and so survived a field-level `except:`/`accept:` — failing closed there instead. There is no load-time warning for this shape today; a gate dimension's name must be treated as a field no derivation may ever drop.
- **An extension's own gate replaces the base's** (see [above](#the-entry-point-and-only-the-entry-point)) — that is the curated-extension idiom, so pair locked bases with access modifiers to keep the re-exposed column surface deliberate. (An extension with no gate of its own carries the base's.)
- **A gated source is a schema oracle wherever its gate field resolves.** The field-resolution refusal (see [Enforcement](#enforcement)) fires only when the gate's field cannot be found on the entry point at all. Otherwise the gate is grafted into the query and evaluated with it, so compilation happens first, and a caller gets Malloy's own compile errors for the gated source even when the gate then admits them no rows: a malformed probe (`group_by: no_such_field`) returns "field is not defined", confirming whether a column exists. Behind the trusted tier the exposure is a column name, not data — see [security-posture.md](security-posture.md#row-level-authorize-rows-are-protected-the-schema-is-not).
- **`/compile` raw SQL is not gated.** The gate covers named Malloy sources; `/compile` still compiles unrestricted, so a caller could read a gated table's schema/SQL via raw `duckdb.sql(...)`. Closing this (restricted compilation on `/compile`, as on `/query`) is tracked as a follow-up; until then keep `/compile` behind the trusted tier.
- **A wrong-shape gate can fail loud at load, or fail confusingly at request time, depending on exactly what it does** (see [Row-level gates](#row-level-gates)). A scalar comparison against an array-typed given loads cleanly and only crashes when the warehouse executes it; a negated membership test loads with a warning rather than being refused. Both are new relative to the retired string form's stricter, load-time-only grammar.
- **A gate can only reference a given on the entry model's own surface.** A gate naming a given the entry model does not declare is refused outright (`unreachable_given`) rather than guessed at, because Malloy merges only one level of `import` and the gate would otherwise silently bind that given's *declaration default* instead of the caller's value. Practical effect: to gate a source through a base two or more import hops away, re-declare it (or `import { NAME } from …`) in the entry model. A permissive default on the base does not open the gate up, and no given the caller did not supply is ever resolved from an unrelated declaration of the same name.
- **A gate given the caller does not supply denies opaquely.** The gate's givens bind with the query's, so Malloy's own failure for an unsupplied one names it ("Given 'ROLE' has no value and no default"). Publisher maps that back to the same `Access denied for source "…"` 403 — a denied caller is never told which given the gate reads.
- **A notebook cell that both declares a gated source and runs it in the same cell, with a
  joined-field gate, needs the run query to reference the joined field.** A cell's row-level gate
  filters correctly whether it declares the gated source itself or inherits one declared earlier
  — with one narrow exception: when the
  gate is on a JOINED field (`internal dimension: authorized is childtable.name in $GROUPS`) and the cell's own `run:`
  query never itself references that joined field, the cell denies with a 400 rather than serving
  filtered rows (never a leak — no rows are returned either way). Reference the joined field
  somewhere in the run query's own projection or grouping to avoid it.
- **A given the entry model does not surface is usually refused, not ignored.** Malloy's given-namespace merge covers only one level of `import`, so a value for a given the entry model doesn't surface reaches Malloy's own resolution and errors (`unknown given`) rather than being silently dropped and falling back to a default. The one exception is a name some reachable gate actually references: that value is forwarded to the gate and withheld from the query (`filterGivensToModelSurface`, `model.ts`), which is what lets a gate carried in from a base in another file evaluate at all. Author-side implication: for a caller to supply a given the query itself reads, some source or file within one import hop of the entry model must declare (or import) it.

## Runnable example

[`examples/governed-analytics`](../examples/governed-analytics) gates a real source with a two-armed
disjunction — an admin override plus a tenant allow-list, in one gate dimension — in
[`secured.malloy`](../examples/governed-analytics/secured.malloy):

```malloy
given:
  ROLE :: string
  TENANT :: string

source: orders_secured is orders_base extend {
  where: $ROLE = 'admin' or tenant = $TENANT   // row-level scoping
  #(authorize)
  internal dimension: authorized is
    $ROLE = 'admin' or $TENANT = 'acme' or $TENANT = 'globex' or $TENANT = 'initech'
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
