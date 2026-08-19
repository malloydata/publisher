# Authorize (Source Access Gates)

> What this is: how `#(authorize)` annotations gate *who* may query a source and *which rows* they
> see. Every gate is enforced as a row filter, so a denial is usually a 200 with no rows rather than
> a 403 — see the note on denial shape below.
> Runnable example: [examples/governed-analytics](../examples/governed-analytics). For the base
> mechanism, see [givens.md](givens.md); for row scoping, see [row-level-access.md](row-level-access.md).

`#(authorize)` is the **source-authorization** application of [givens](givens.md). **Every gate is enforced as a row filter**, whether or not its expression happens to read a row field: the gate's condition is grafted onto the source the query enters through and evaluated by the same query. A gate that reads no field (`$ROLE = 'analyst'`) is simply a predicate that is constant across every row, so it still behaves as a whole-source admit or deny — it is just reached by the same mechanism. See [Row-level gates](#row-level-gates) below. To scope *which rows* a caller sees using a plain `where:` rather than the gate itself, see [Row-level access](row-level-access.md).

`#(authorize)` annotations gate query access to a Malloy source based on the request's [givens](givens.md). A source with no in-scope annotations is unrestricted.

**What a denial looks like on the wire follows from that.** A gate Publisher *can* express as a row filter is applied to the query, so a caller it admits nowhere gets **200 with zero rows** rather than a 403. A **403** is what you get when the gate cannot be applied at all — its compiled condition is not an allowed shape, or the entry point renamed/dropped the field it reads, or a given it names was not supplied. Both are denials and neither returns a row the caller may not read, but only one is visible as a status code: anything keying on 403 to mean "denied" — an alert, a retry rule, a client branch — will not see a filtered-to-nothing denial at all.

> ⚠️ **Read [Security model](#security-model) before deploying this as an access control.** Givens are **caller-asserted**: anyone who can reach the query API can claim a favorable given. `#(authorize)` is only a real boundary when the API sits behind a trusted tier that sets givens from its own verified context. It is not, on its own, end-user authentication.

For the Malloy expression reference, see [Malloy: Expressions](https://docs.malloydata.dev/documentation/language/expressions). For givens, see [givens.md](givens.md).

## Declaring Gates

Authorize annotations attach to a source: `#(authorize) "<expr>"`. The body is a quoted Malloy boolean expression over declared givens (`$NAME`):

```malloy
##! experimental.givens

given:
  ROLE :: string

#(authorize) "$ROLE = 'analyst'"
source: orders is duckdb.table('orders.parquet') extend {
  measure: order_count is count()
}
```

- **Source-level** `#(authorize) "<expr>"` — gates that one source. Stack multiple on a source; see [OR semantics](#or-semantics).
- A source with no in-scope annotations is **unrestricted**.

> **`##(authorize)` (file-level) is deprecated and refused at load.** An earlier version of this
> feature let an annotation written above the file, rather than a `source:`, apply to every source in
> it as a model-wide override. That capability was withdrawn — the raw-warehouse path it existed to
> close is already closed unconditionally by restricted mode (any caller-submitted `duckdb.sql(...)`/
> `duckdb.table(...)` is rejected before any gate runs), so the model-wide reach bought no additional
> protection and was easy to reason about incorrectly. A `##(authorize)` annotation anywhere in the
> model — including one folded in from an imported file — now fails the load with a message naming
> the remedy: declare `#(authorize)` on each `source:` it was meant to protect.

### Expression Language

The expression is a Malloy boolean expression over `$given` references, row fields, and literals, built from `=`, `!=`, `<`, `>`, `<=`, `>=`, `and`, `or`, `not`, and `in $ARRAY_GIVEN`. Examples:

```malloy
#(authorize) "$ROLE = 'analyst'"
#(authorize) "$REGION = 'us-west' and $ROLE != 'guest'"
#(authorize) "$TENANT = 'acme'"
#(authorize) "org_id in $GROUPS"
```

A list literal is not valid Malloy in this position — write `#(authorize) "$ROLE = 'analyst'"` and `#(authorize) "$ROLE = 'admin'"` as two stacked annotations (they OR — see [OR semantics](#or-semantics)), or declare an array given and compare a row field to it with `in`.

**Not every Malloy boolean expression is an accepted gate.** The gate's condition is grafted onto the source as a `where:`, and only a small allowlist of shapes is permitted there — see [Row-level gates](#row-level-gates) for the grammar, which governs field-referencing and field-less gates alike. Anything outside it (a function call, `like`, `is not null`, a literal-vs-literal comparison) is refused; see [Validation](#validation) for where that refusal lands.

Embedded quotes follow Malloy string rules: write inner string literals with single quotes inside the double-quoted annotation, e.g. `#(authorize) "$ROLE = 'analyst'"`.

## Row-level gates

A gate expression may reference a row field — its own source's, or one reached through a join — instead of only givens and literals. **Every gate is enforced the same way** regardless: as a row filter on the entry-point source. A gate that reads no field is constant across every row, so it still admits all of them or none; it is not a separate mechanism. See [Row-level access](row-level-access.md#row-level-authorize) for what that means for the caller, and [security-posture.md](security-posture.md#row-level-authorize-rows-are-protected-the-schema-is-not) for the trade it makes.

Because the same allowlist governs every gate, it also governs gates written before it existed — see [Validation](#validation) for what happens to a field-less gate it does not accept.

The accepted shape is a positive allowlist — a boolean combination (`and`/`or`, parentheses) of `<field> <operator> $GIVEN` comparisons, plus two self-contained atoms: a bare `true`/`false`, and a `<given> <operator> <literal>` admin override:

```malloy
#(authorize) "org_id in $GROUPS"
#(authorize) "childtable.name = $BOB"
#(authorize) "region = $REGION and tier != $EXCLUDED_TIER"
```

- **`in` is required for an array-typed given** (`org_id in $GROUPS`); `=`, `!=`, `>`, `>=`, `<`, `<=` for a scalar one. A scalar comparison against an array-typed given is refused at load, and note that `org_id = $GROUPS` and `org_id ? $GROUPS` are the SAME thing here — they compile to one comparison node, so neither spelling escapes the refusal. The refusal is on the given's declared type, because without it the query compiles clean and then fails in the warehouse with a cast error. `in` also has the behaviour you want for an empty array: no rows, which is correct for a caller who was given no groups.
- **A comparison between two constants is refused** (`region = 'us-west'`, `1 = 1`) — that is a fixed filter and belongs in the source's own `where:`, not in an access gate. A `<given> <operator> <literal>` atom (`$ROLE = 'admin'`) IS accepted: it is the admin-override idiom, constant for one request and OR-able with a real row condition.
- **An admin-override atom that is TRUE at its given's declared default is refused** — `$ROLE != 'blocked'` with `ROLE` defaulting to `''` is vacuously true for a caller who supplies nothing, which makes the whole disjunction admit every row. Declare the given with no default, or one the atom evaluates false against.
- **Function calls, arithmetic, `like`, `is not null`, and `not in` are refused.** `not in` specifically: a row-level gate expresses the set of rows a caller MAY read, not a set to exclude. A `not (...)` wrapping a single scalar comparison is accepted, since it is just the negated operator spelled differently.
- **A field compared against a given that has a declared default is refused** — an unsupplied given then filters every row against that default, admitting rows the gate meant to exclude.

All of the above is refused **at package load**, naming the cause — a broken gate never serves.

**The given a row-level gate compares against must be on the gating model's own given surface** — declared there, or reachable through one `import` hop, the same reach [givens.md](givens.md) describes for the ambient namespace. Malloy does not flatten a `given:` declaration past that one hop, so a gate whose given lives further away would otherwise silently bind that given's own declaration DEFAULT rather than the caller's value. Rather than risk that, it is refused at load instead, naming the fix: import the given (`import { GROUPS } from "…"`).

> **A gate on a joined field turns a `join_one` LEFT JOIN into an INNER JOIN.** The filter is applied inside the entry source's own build, before any aggregation, so a parent row with no matching child — and so no value to satisfy the gate — drops out of the result entirely rather than surviving with nulls. That is fail-closed (a row the gate cannot evaluate is a row it does not admit), but it changes cardinality invisibly if you expected the join's usual left-join behavior.

**The pre-compile check never grants.** It runs before there is a compiled query to filter, so a gate it can classify as an expressible row filter is neither granted nor denied there — the decision waits until the filter is actually applied. It only ever refuses, for a gate it cannot classify or graft at all.

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

## Semantics

### OR semantics

Multiple in-scope expressions on one source are evaluated as a single **disjunction** — access is granted if **any one** returns `true`.

```malloy
given:
  ROLE :: string
  TENANT :: string

#(authorize) "$ROLE = 'admin'"
#(authorize) "$TENANT = 'acme'"
source: orders is duckdb.table('orders.parquet')
```

`orders` is queryable by an admin **or** by an acme-tenant caller. Stacking gates **widens** access — if you expect AND, this will surprise you. Express conjunction within a single expression instead: `#(authorize) "$ROLE = 'admin' and $TENANT = 'acme'"`.

To give one role access to every source in a file, declare the same admin-override expression on each source's own `#(authorize)` rather than reaching for a model-wide gate — a file-level override existed for this once; it is deprecated (see [Declaring Gates](#declaring-gates)).

### The entry point, and only the entry point

Authorize is checked on the source the query **enters through** — the run target. A gate answers "who may query this source", not "who may read everything reachable beneath it".

- **Joins are not gated.** A gate on a source reached only via `join_*` **does not fire** — at any depth (A→B→C), aliased, across files, declared in a query-local `join_one` inside a `-> { … }` refinement, or as a member of a joined composite. `run: joiner -> { … }` where `joiner` joins a base locked with `#(authorize) "false"` returns rows. **This is the rule authors have to design around:** joining sensitive data into an ungated source publishes it. Put the gate on the source callers enter through, and use [access modifiers](https://docs.malloydata.dev/documentation/experiments/include) (`include { public: …, private: * }`) to control what an extension re-exposes.
- **A gate MAY reference a field on a joined source — that is not the same as the joined source's own gate firing.** `#(authorize) "childtable.name = $BOB"` declared on the entry point itself is enforced (with [row-level gates](#row-level-gates) on): the join it needs is emitted as part of the entry point's own build. The rule above is unchanged — a gate declared ON `childtable` still does not fire when `childtable` is only reached via a join from an ungated entry point. Referencing a joined field from the entry point's *own* gate, and a joined source's *own* gate firing, are two different things; only the first is supported.
- **Extend: own gate replaces, otherwise the base's carries.** `source: b is a extend { … }` is governed by `b`'s own `#(authorize)` when it declares one — that replacement is the [curated-extension idiom](#recommended-pattern-locked-base-and-curated-extensions). When `b` declares none it is gated by `a`'s. That holds however `b` is decorated: a render tag or doc comment on `b` moves `a`'s annotations off `b`'s own notes (Malloy files them under `annotations.inherits`), so resolution follows both the inherits chain and the source registry to the declaration `b` derives from.
- **Query-source derivation carries the gate too.** `source: laundered is locked_src -> { … }` is gated by `locked_src`'s gate when it declares none of its own — the compiled `QuerySourceDef` keeps the base reachable via `query.structRef`, and a chained derivation resolves through it as well. Derivation is treated like `extend`; reaching the same derived source via a *join* is still not gated.
- **A composite run target resolves precisely.** When the run target is `compose(a, b)`, Malloy resolves it to exactly one member branch per query and that branch's gate applies.
- **Caller-submitted text may not declare a gate at all.** An `#(authorize)` annotation (the deprecated `##(authorize)` spelling included) in the `query` text of a query request, or in the `source` text submitted to `/compile`, is rejected with a 400: a gate is the model author's to declare. Notebook cells are package content, so an author's gate there works normally.

### Worked example

One model, six entry points. The rules above are subtle enough that it is worth reading the verdicts rather than deriving them:

```malloy
##! experimental.givens

given:
  ROLE :: string

// Locked: nobody queries this directly.
#(authorize) "false"
source: salaries is duckdb.table('salaries')

// (1) No gate of its own → the base's "false" carries. Still locked.
source: salaries_plain is salaries extend {
  measure: headcount is count()
}

// (2) A render tag does NOT change that. Malloy moves the base's annotations
//     under `annotations.inherits`, and resolution follows the chain.
# bar_chart
source: salaries_tagged is salaries extend {
  measure: headcount is count()
}

// (3) Its own gate REPLACES the base's — the curated-extension idiom.
#(authorize) "$ROLE = 'hr'"
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
| `run: salaries -> …` | **no rows** always | its own `"false"` |
| `run: salaries_plain -> …` | **no rows** always | inherited `"false"` (1) |
| `run: salaries_tagged -> …` | **no rows** always | inherited `"false"`; the tag is irrelevant (2) |
| `run: salaries_hr -> …` | no rows unless `ROLE = 'hr'`; 403 if `ROLE` is unsupplied | own gate replaced the base's (3) |
| `run: salaries_derived -> …` | **no rows** always | derivation carried `"false"` (4) |
| `run: headcount_by_dept -> …` | **returns rows** | no gate at the entry point; the join is not traced (5) |

`"false"` compiles to a filter that matches nothing, so those rows are withheld with a **200 and an empty result**, not a 403 — see the note on denial shape at the top of this page. Publisher recognizes a provably-constant-`false` gate and answers it without dispatching anything to the warehouse.

Entry point (5) is the one to internalize: `headcount_by_dept` reads salary rows and is queryable by anyone. That is not a bug, it is the rule — **joining sensitive data into an ungated source publishes it.** If `headcount_by_dept` should be restricted, give it its own gate; if it should expose only aggregates, use `include { public: headcount, private: * }` so the join cannot be drilled through.

And the caller cannot mint a gate to escape one:

```jsonc
// POST /…/models/hr.malloy/query — rejected with 400, not compiled
{
  "query": "#(authorize) \"true\"\nsource: mine is salaries extend {}\nrun: mine -> { select: * }"
}
```

## Recommended pattern: locked base and curated extensions

Lock sensitive base sources with `#(authorize) "false"` and re-expose curated subsets through extension sources, each with its own gate and access modifiers:

```malloy
##! experimental.givens
##! experimental.access_modifiers

given:
  REGION :: string
  ROLE :: string

// Base source: locked. Direct queries are denied — the only in-scope
// authorize expression is the constant `false`.
#(authorize) "false"
source: customers_raw is duckdb.table('customers.parquet')

// Extension: re-exposes a curated subset and adds an analyst-role gate.
// `private: *` hides every other column on the base.
#(authorize) "$ROLE = 'analyst'"
source: customers_marketing is customers_raw include {
  public: name, region, signup_date
  private: *
} extend {
  measure: customer_count is count()
}

// A second extension with a different gate and field surface.
#(authorize) "$REGION = 'us-west'"
source: customers_us_west is customers_raw include {
  public: name, region, signup_date, lifetime_value
  private: *
}
```

- `run: customers_raw -> …` → **200 with no rows** (gate is `false`; nothing is readable).
- `run: customers_marketing -> …` → allowed with `$ROLE = 'analyst'`; the consumer can only touch `name`, `region`, `signup_date`. Supplying no `$ROLE` at all is a 403, since the gate's given cannot bind.
- `run: customers_us_west -> …` → allowed with `$REGION = 'us-west'`, on a different surface.

The `include { … private: * }` layer is what controls which base columns each extension can re-expose; the extension's own `#(authorize)` gates consumer access to that curated surface. The base's `#(authorize) "false"` is a defense-in-depth backstop against a direct `run: customers_raw`.

## Enforcement

The gate runs, fail-closed, on every query entry point. Enforcement is in two parts, and only the first runs before compilation:

1. **A pre-compile refusal.** Where the run target can be resolved from the request's surface syntax, Publisher classifies its gate before compiling anything. A gate it cannot express as a row filter is refused here with a **403**, so an unclassifiable gate can never be used as a schema oracle.
2. **The graft.** A gate that *is* expressible is attached to the compiled query as a source-level `where:` on the entry point, and evaluated by the same `run()` as the query itself.

Step 2 is what changed. A gate is no longer decided by a separate probe before the query compiles; its condition is compiled *into* the query. The practical consequences are listed under [Known limitations](#known-limitations): a denial by filtering is a 200, and a caller who compiles a deliberately malformed query against a source it may read nothing from still sees Malloy's compile errors for that source.

There is one documented exception, [the authorize bypass](#authorize-bypass-for-trusted-data-management-callers), which applies to `POST /…/query` only:

| Entry point | Behavior |
| --- | --- |
| `POST /…/query` | Gate the run-target source; deny → zero rows where the filter applied, 403 where it could not be applied. Skipped entirely when the request carries `x-publisher-bypass-authorize: true`. |
| `POST /…/projects/…/query` (legacy alias) | Gate as above. Accepts no bypass — it exists for pre-rename SDK compatibility and passes no `givens` either, so a gated source is denied there regardless. Use the `/environments/…` route. |
| Notebook cell `GET` | Gate each cell that runs a query. Accepts no bypass. |
| `POST /…/compile` | Gate the named source the submitted text targets (early, before compiling, plus a compiled-source backstop). `/compile` never runs the query, so the backstop cannot attach a filter; it admits only a gate decided without running it — one that is constant-`true`, or one whose every given the caller supplied — and denies anything else. Note that `includeSql` then returns the **ungrafted** SQL, without the gate's `where:`. Accepts no bypass. |
| MCP `malloy_executeQuery` | Routes through the query path; a denial surfaces as `isError: true` naming the source. Sends no bypass. |

**Fail-closed, evaluated as a disjunction.** A source's own expressions are OR-ed into one condition (`(a) or (b)`) and grafted as a single filter, so a row satisfying any branch is admitted. Where two *sources* gate one entry point — its own gate plus one carried from the source it derives from — each is grafted separately, which AND-s them. Anything that stops the graft landing denies rather than admits.

### Validation

Authorize expressions are validated at **model load** (compile-only, no execution). A malformed annotation (missing quotes), an unknown given, or a source-field reference that isn't a valid [row-level gate](#row-level-gates) fails the load with **HTTP 424** (`ModelCompilationError`), naming the source and the underlying reason. Fix the model before it serves.

**A gate whose compiled condition reads no row field is refused per-source, not per-model.** The row-level grammar is a small allowlist, and it now governs gates that predate it — `#(authorize) "$ROLE like 'ana%'"` published fine when a field-less gate was a separate whole-source boolean and is not an allowed row-filter shape. Refusing it by failing the load would turn the whole model *file* into a compilation-failure placeholder, taking every ungated source in it out of service, so instead the load succeeds with a warning and **that one source denies every request**. A gate that DOES read a row field is one the grammar always governed, and still fails the load.

**A gate must resolve at every entry point through which the source declaring it is reached, and an entry point where it cannot is closed rather than opened.** `rename:`, `except:`, and `accept:` on an extending source can remove the field a gate was written against. Which way that fails depends on *who wrote the gate*. Where the entry point declares its **own** `#(authorize)`, the annotation is the author's own mistake and package load fails with a 424 naming the source. Where the entry point only **inherits** the gate — `W is X extend { except: org_id }`, whose `extend` dropped the field X's gate needs — load succeeds with a warning and **that entry point denies every request**; the rest of the model still serves. The split exists so one unreachable derived entry point cannot take down a whole package, and neither branch can serve a silently unfiltered or wrongly filtered result. This is distinct from *inheriting* a gate that still resolves (see the worked example below and [row-level-access.md](row-level-access.md#row-level-authorize)): there, the filter runs inside the base's own build, before the deriving source's projection ever applies, so a derived source that projects the gated column away still serves correctly filtered — there is no need to keep an inherited gate's column in the projection.

Validation covers only the entry points in the model being loaded. `compileMalloyModel` compiles each file independently, so an importing model's own entry points are validated when THAT model loads; anything this pass misses fails closed at request time — the request-path classification denies rather than leaks. An expression that cannot even be parsed becomes a single unsatisfiable `false`.

> ⚠️ **An inherited gate's givens bind in the ENTRY model, not the model that declared the gate.** The filter is compiled against the model the query enters through, and Malloy mints a fresh identity per `given:` declaration — so a model that imports a gated source and re-declares one of the base's given names re-points that gate to its own declaration, including its default. A base gated `#(authorize) "tenant_id = $TENANT"` with no default for `TENANT`, imported into a model declaring `given: TENANT :: number is 99`, serves tenant-99 rows to a caller who supplied nothing. This is an authoring hazard, not a caller-reachable bypass — a caller cannot introduce a declaration — but it means importing a locked source does not carry its gate's given namespace with it. Do not re-declare a base's given names in a model that imports its gated sources.

### Error contract & redaction

- **Runtime 403** names only the source — `{"code":403,"message":"Access denied for source \"orders\"."}` — never the authorize expression. Gate logic is not leaked to (potentially untrusted) query callers.
- **Model-load 424** *keeps* the full expression in its message — it is author-facing at package load and you need it to fix a malformed annotation.
- **The reported `authorize` list is not always a pure disjunction.** A source's own expressions are OR-ed, but where two *sources* gate one entry point — its own gate plus one carried from the source it derives from — those are AND-ed, and the API reports both sets flattened into a single array. Use the field to decide *that* a source is gated; do not recompute the access decision from it, because reading a two-source list as OR grants where Publisher denies. (Enforcement is unaffected: each source's gate is evaluated as its own disjunction and the results AND-ed.)
- **Introspection is not redacted, and now shows inherited gates too.** A source's `authorize` list in the API reports the expressions gating it, including one it inherits from a base in another file that is not itself listed. That is deliberate — a list that omitted the inherited case would report a locked source as unrestricted, which is the more dangerous error — but it means gate logic is readable by anyone who can list sources. Same trust assumption as the rest of this page: keep the API behind the trusted tier.

## Security model

`#(authorize)` evaluates expressions over **request-supplied givens**. There is no authentication in Publisher's query path: a given is whatever the caller sends. So:

- **Authorize is a real boundary only behind a trusted tier.** The intended deployment is Publisher behind an embedding application that authenticates end users and sets givens (role, tenant, region) from its own *verified* context, with the query/MCP API network-isolated from untrusted callers. In that setup the gate enforces the trusted tier's policy.
- **It does not defend against a caller who sets their own givens.** Exposed directly to untrusted users, anyone can send `{"ROLE":"admin"}` and pass an `$ROLE = 'admin'` gate. Do not treat `#(authorize)` as end-user authn/authz on a public endpoint.
- **Identity-bound givens** — a verified token or trusted-proxy header populating reserved "system givens" the caller cannot override — is a planned milestone that would make authorize a standalone boundary. It is not implemented yet.

### Authorize bypass, for trusted data-management callers

A `POST /…/query` request carrying the header `x-publisher-bypass-authorize: true` runs with gate evaluation **skipped**. It exists for data management: an indexer or similar back-office caller is a machine identity with no givens, so a gated source returns 403 and is never scanned — an author who tags a dimension `#(index)` on a gated source otherwise gets an empty index, the tag having opted in and the gate having silently revoked it.

What it does and does not touch:

- **Only** expressions collected from `#(authorize)` annotations are skipped. The author's own `where:` clauses still narrow the scan, row and byte caps still apply, restricted mode still bans raw SQL, and a gate declared in caller-submitted text is still rejected with a 400.
- It is **not** `bypassFilters`, a separate deprecated `#(filter)`-only control in the request body. Neither reads or writes the other.
- Notebook cells, `/compile`, and MCP accept no bypass at all.

**Publisher does not bound who may send it.** There is no authentication in the query path, so the header is exactly as trustworthy as the network in front of it — and the header name is published here, so treat it as known to anyone. **A deployment that reaches untrusted callers must strip this header at its edge.** [docs/authorize-bypass-deployment.md](authorize-bypass-deployment.md) is the operator's page: what to strip, why an allowlist beats a blocklist, and how to tell whether a bypass ever happened.

Note what the residual case is if the strip is missing. Publisher has no tenant boundary of its own, so a fronting application's own authorization still decides which packages a caller reaches; what the header removes is the **in-model** gating — role- or row-level policy *within* data that caller is otherwise entitled to reach.

Every use is counted (`publisher_authorize_bypass_total`, labelled `entry_point`) and logged (`authorize bypass`, with source / model / package). The counter is the rate signal; the log line is what an investigation reads. Read the two carefully: `runnable` fires on every bypassed query, `source` only when a run target was resolvable before compilation, so they are not always paired.

### Row-level gate metrics

Two more counters cover row-level gates specifically:

- `publisher_authorize_row_level_total`, labelled `decision` (`denied_by_gate` | `empty_after_filter` | `short_circuited`). `denied_by_gate` is the fail-closed refusal when a gate could not be applied; `empty_after_filter` is a normal 200 with zero rows after the filter matched none, which is not an error; `short_circuited` is a provably constant-`false` gate answered with zero rows without ever querying the warehouse.
- `publisher_authorize_row_level_rejected_total`, labelled `cause` (`array_given_needs_in` | `scalar_given_rejects_in` | `field_given_has_default` | `unsupported_node` | `no_given_reference` | `unreachable_given` | `vacuous_default_atom` | `entry_point_unexpressible`). Fires at package load — alert on any nonzero value since the last publish, not on a rate. The first six mean the gate's compiled condition is not an allowed shape; `vacuous_default_atom` means an atom evaluates `true` against its own given's declared default, which would admit every row for a caller who supplies nothing. Each of those fails the whole model load, with two exceptions that warn and leave one source denying while the rest of the model serves: `entry_point_unexpressible` (a valid gate one derived entry point renamed/excluded/projected the field away from), and **any** cause at all when the gate reads no row field (see [Validation](#validation)). `vacuous_default_atom` is never excused that way — it is found by probing, which the request path does not repeat, so warning would leave the source serving unfiltered.

`publisher_authorize_guard_rejected_total`, labelled `field` (`query` | `source_name` | `query_name` | `compile_source`), counts requests rejected with 400 for declaring an `#(authorize)` annotation in caller-submitted Malloy text.

**This is an interim answer, not the intended one.** The shape that keeps the decision with the model author is identity-bound givens (above) — a reserved system given the caller cannot set, so an author writes `#(authorize) "$ROLE = 'analyst' or $SYSTEM_CALLER = 'indexer'"` and a source they never opted in stays gated. This header removes the gate globally instead, for callers the deployment trusts wholesale. When identity-bound givens land, this should narrow or go.

## Known limitations

- **A request can be exempted from the gate entirely** (see [Authorize bypass](#authorize-bypass-for-trusted-data-management-callers)). `x-publisher-bypass-authorize: true` on a query request skips evaluation, and Publisher does not bound who may send it — so on a deployment that does not strip the header at its edge, every gate is advisory for any caller who knows the name. Listed first here because it is the only limitation on this page that a *deployment*, not a model, has to close.
- **A gate does not follow a join** (see [above](#the-entry-point-and-only-the-entry-point)). This is the limitation with the largest practical consequence: any source that joins gated data and is itself ungated hands that data to every caller. Treat "which sources can a caller enter through, and what does each of them reach" as part of modelling, not as something the gate handles for you.
- **An extension's own gate replaces the base's** (see [above](#the-entry-point-and-only-the-entry-point)) — that is the curated-extension idiom, so pair locked bases with access modifiers to keep the re-exposed column surface deliberate. (An extension with no gate of its own carries the base's.)
- **A gated source is a schema oracle wherever its gate IS expressible.** The pre-compile refusal (see [Enforcement](#enforcement)) fires only for a gate Publisher cannot express as a filter. An expressible gate is grafted into the query and evaluated with it, so compilation happens first, and a caller gets Malloy's own compile errors for the gated source even when the gate then admits them no rows: a malformed probe (`group_by: no_such_field`) returns "field is not defined", confirming whether a column exists. This used to be limited to a source the caller declared in its own query text (`source: mine is locked_base extend {}`, which does not exist until that text compiles); it now applies to a plainly-named gated source too. Closing it would mean deciding a row filter's outcome before compiling the query it filters. Behind the trusted tier the exposure is a column name, not data — see [security-posture.md](security-posture.md#row-level-authorize-rows-are-protected-the-schema-is-not).
- **`/compile` raw SQL is not gated.** The gate covers named Malloy sources; `/compile` still compiles unrestricted, so a caller could read a gated table's schema/SQL via raw `duckdb.sql(...)`. Closing this (restricted compilation on `/compile`, as on `/query`) is tracked as a follow-up; until then keep `/compile` behind the trusted tier.
- **A gate's classification is memoized per model, its outcome never is.** The compiled shape of a gate on a given entry point is cached for the life of the loaded model — it cannot change without a reload. The filter itself is re-evaluated by every query, against that request's own givens.
- **A gate can only reference a given on the entry model's own surface.** A gate naming a given the entry model does not declare is refused outright (`unreachable_given`) rather than guessed at, because Malloy merges only one level of `import` and the gate would otherwise silently bind that given's *declaration default* instead of the caller's value. Practical effect: to gate a source through a base two or more import hops away, re-declare it (or `import { NAME } from …`) in the entry model. A permissive default on the base does not open the gate up, and no given the caller did not supply is ever resolved from an unrelated declaration of the same name.
- **A gate given the caller does not supply denies opaquely.** The gate's givens bind with the query's, so Malloy's own failure for an unsupplied one names it ("Given 'ROLE' has no value and no default"). Publisher maps that back to the same `Access denied for source "…"` 403 — a denied caller is never told which given the gate reads.
- **A notebook cell that both declares a gated source and runs it in the same cell, with a
  joined-field gate, needs the run query to reference the joined field.** A cell's row-level gate
  filters correctly whether it declares the gated source itself or inherits one declared earlier
  — with one narrow exception: when the
  gate is on a JOINED field (`#(authorize) "childtable.name in $GROUPS"`) and the cell's own `run:`
  query never itself references that joined field, the cell denies with a 400 rather than serving
  filtered rows (never a leak — no rows are returned either way). Reference the joined field
  somewhere in the run query's own projection or grouping to avoid it.
- **A given the entry model does not surface is usually refused, not ignored.** Malloy's given-namespace merge covers only one level of `import`, so a value for a given the entry model doesn't surface reaches Malloy's own resolution and errors (`unknown given`) rather than being silently dropped and falling back to a default. The one exception is a name some reachable gate actually references: that value is forwarded to the gate and withheld from the query (`filterGivensToModelSurface`, `model.ts`), which is what lets a gate carried in from a base in another file evaluate at all. Author-side implication: for a caller to supply a given the query itself reads, some source or file within one import hop of the entry model must declare (or import) it.

## Runnable example

[`examples/governed-analytics`](../examples/governed-analytics) gates a real source with two stacked
annotations — an admin override plus a tenant allow-list — in
[`secured.malloy`](../examples/governed-analytics/secured.malloy):

```malloy
given:
  ROLE :: string is ''
  TENANT :: string is ''

#(authorize) "$ROLE = 'admin'"
#(authorize) "$TENANT = 'acme' or $TENANT = 'globex' or $TENANT = 'initech'"
source: orders_secured is orders_base extend {
  where: $ROLE = 'admin' or tenant = $TENANT   // row-level scoping
  ...
}
```

Empty defaults keep each given bound so supplying just one still grants (the other annotation simply
doesn't match). Against a running server, the `governed-analytics` package ships in the default
`examples` environment (see the [example's README](../examples/governed-analytics/README.md)):

```bash
API=http://localhost:4000/api/v0/environments/examples/packages/governed-analytics/models

# No identity → denied. Both givens carry empty defaults, so the gate BINDS and
# evaluates false rather than failing to resolve: the filter matches no row and
# the response is a 200 with an empty result, not a 403. See the note on denial
# shape at the top of this page.
curl -s -X POST $API/secured.malloy/query -H 'content-type: application/json' \
  -d '{"query":"run: orders_secured -> by_status"}'                            # → 200, no rows

# Admin → allowed (all rows)
curl -s -X POST $API/secured.malloy/query -H 'content-type: application/json' \
  -d '{"query":"run: orders_secured -> by_status","givens":{"ROLE":"admin"}}'  # → 200
```

The row-level half of that source — how `where:` narrows an *allowed* caller to their own rows — is
covered in [row-level-access.md](row-level-access.md).
