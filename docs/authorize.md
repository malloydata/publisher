<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Authorize (Source Access Gates)

> What this is: how `#(authorize)` annotations gate *who* may query a source (HTTP 403), and — when
> the gate reads a row field — *which rows* they see once admitted, instead.
> Runnable example: [examples/governed-analytics](../examples/governed-analytics). For the base
> mechanism, see [givens.md](givens.md); for row scoping, see [row-level-access.md](row-level-access.md).

`#(authorize)` is the **source-authorization** application of [givens](givens.md). Most gates reference only givens, and for those nothing here has changed: the gate allows or denies access to an *entire* source. A gate whose expression reads a row field — its own source's, or one reached through a join — is instead enforced as a **row filter**; see [Row-level gates](#row-level-gates) below. Either way, to scope *which rows* a caller sees using a plain `where:` rather than the gate itself, see [Row-level access](row-level-access.md).

`#(authorize)` annotations gate query access to a Malloy source based on the request's [givens](givens.md). Before Publisher runs any query that reads a gated source, it evaluates the source's in-scope authorize expressions against the supplied givens; if **at least one** returns `true` the request proceeds, otherwise it is rejected with **HTTP 403**. A source with no in-scope annotations is unrestricted. That is the whole-source decision; a row-level gate (below) replaces the 403 with a filter instead.

A gate that references only givens is evaluated by Publisher (not core Malloy) using a synthetic probe query against bundled DuckDB (a one-row `SELECT 1`), so the expression language is Malloy's, but the gate runs entirely over `given:` values — it never touches your warehouse data. A row-level gate is compiled into the query itself instead — see [Row-level gates](#row-level-gates).

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

The expression is any Malloy boolean expression over `$given` references and literals: `=`, `!=`, `<`, `>`, `<=`, `>=`, `and`, `or`, `not`, `in [...]`, etc. Examples:

```malloy
#(authorize) "$ROLE = 'analyst'"
#(authorize) "$ROLE in ['analyst', 'admin']"
#(authorize) "$REGION = 'us-west' and $ROLE != 'guest'"
#(authorize) "$TENANT = 'acme'"
```

**No source-field references, unless the expression is a row-level gate.** The probe above evaluates your expression against its own synthetic one-column row, not against your source, so an expression may reference only givens and literals — a column of the gated source isn't in scope and fails at model load. A field reference is accepted only when it fits the [row-level gate](#row-level-gates) grammar below; otherwise it fails at model load (see [Validation](#validation)).

Embedded quotes follow Malloy string rules: write inner string literals with single quotes inside the double-quoted annotation, e.g. `#(authorize) "$ROLE = 'analyst'"`.

## Row-level gates

A gate expression may reference a row field — its own source's, or one reached through a join — instead of only givens and literals. Malloy's own reference tracking on the compiled condition decides which mechanism a gate gets: an expression that references no row field keeps the one-row probe above, exactly as it always has. **Most existing gates are this kind.** An expression that references at least one field is enforced as a **row filter** on the entry-point source instead of a whole-source boolean — see [Row-level access](row-level-access.md#row-level-authorize) for what that means for the caller, and [security-posture.md](security-posture.md#row-level-authorize-rows-are-protected-the-schema-is-not) for the trade it makes.

The accepted shape is a positive allowlist — a boolean combination (`and`/`or`, parentheses) of `<field> <operator> $GIVEN` comparisons:

```malloy
#(authorize) "org_id in $GROUPS"
#(authorize) "childtable.name = $BOB"
#(authorize) "region = $REGION and tier != $EXCLUDED_TIER"
```

- **`in` is required for an array-typed given** (`org_id in $GROUPS`); `=`, `!=`, `>`, `>=`, `<`, `<=` for a scalar one. A scalar comparison against an array-typed given is refused at load, and note that `org_id = $GROUPS` and `org_id ? $GROUPS` are the SAME thing here — they compile to one comparison node, so neither spelling escapes the refusal. The refusal is on the given's declared type, because without it the query compiles clean and then fails in the warehouse with a cast error. `in` also has the behaviour you want for an empty array: no rows, which is correct for a caller who was given no groups.
- **A comparison against a constant is refused** (`region = 'us-west'`) — that is a fixed filter and belongs in the source's own `where:`, not in an access gate.
- **Function calls, arithmetic, `like`, and `not in` are refused.** `not in` specifically: a row-level gate expresses the set of rows a caller MAY read, not a set to exclude.

All of the above is refused **at package load**, naming the cause — a broken gate never serves.

**The given a row-level gate compares against must be on the gating model's own given surface** — declared there, or reachable through one `import` hop, the same reach [givens.md](givens.md) describes for the ambient namespace. Malloy does not flatten a `given:` declaration past that one hop, so a gate whose given lives further away would otherwise silently bind that given's own declaration DEFAULT rather than the caller's value. Rather than risk that, it is refused at load instead, naming the fix: import the given (`import { GROUPS } from "…"`).

> **A gate on a joined field turns a `join_one` LEFT JOIN into an INNER JOIN.** The filter is applied inside the entry source's own build, before any aggregation, so a parent row with no matching child — and so no value to satisfy the gate — drops out of the result entirely rather than surviving with nulls. That is fail-closed (a row the gate cannot evaluate is a row it does not admit), but it changes cardinality invisibly if you expected the join's usual left-join behavior.

**A row-level gate never produces a 403 from the whole-source check.** That check runs before there is a compiled query to filter, so a gate it finds to be row-level is neither granted nor denied there — the decision waits until the filter can actually be applied. Denying earlier would refuse every row-level-gated query outright.

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
| `run: salaries -> …` | **403** always | its own `"false"` |
| `run: salaries_plain -> …` | **403** always | inherited `"false"` (1) |
| `run: salaries_tagged -> …` | **403** always | inherited `"false"`; the tag is irrelevant (2) |
| `run: salaries_hr -> …` | 403 unless `ROLE = 'hr'` | own gate replaced the base's (3) |
| `run: salaries_derived -> …` | **403** always | derivation carried `"false"` (4) |
| `run: headcount_by_dept -> …` | **returns rows** | no gate at the entry point; the join is not traced (5) |

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

- `run: customers_raw -> …` → **403** (gate is `false`).
- `run: customers_marketing -> …` → allowed with `$ROLE = 'analyst'`; the consumer can only touch `name`, `region`, `signup_date`.
- `run: customers_us_west -> …` → allowed with `$REGION = 'us-west'`, on a different surface.

The `include { … private: * }` layer is what controls which base columns each extension can re-expose; the extension's own `#(authorize)` gates consumer access to that curated surface. The base's `#(authorize) "false"` is a defense-in-depth backstop against a direct `run: customers_raw`.

## Enforcement

The gate runs, fail-closed, on every query entry point — **before** any filter injection or compilation, so a denial is never masked by a later error.

**What a denial looks like on the wire depends on the kind of gate, and the two differ.** A given-only gate is a whole-source decision and denies with a clean **403**, as it always has. A [row-level gate](#row-level-gates) is a filter, so a caller it admits nowhere gets **200 with zero rows** — the request succeeds and returns nothing. Both are denials and neither returns a row the caller may not read, but only one is visible as a status code. Anything keying on 403 to mean "denied" — an alert, a retry rule, a client branch — will not see a row-level denial at all. The table below reads `deny` in that sense: 403 for a whole-source gate, an empty result for a row-level one.

There is one documented exception, [the authorize bypass](#authorize-bypass-for-trusted-data-management-callers), which applies to `POST /…/query` only:

| Entry point | Behavior |
| --- | --- |
| `POST /…/query` | Gate the run-target source; deny → 403 (whole-source) or zero rows (row-level). Skipped entirely when the request carries `x-publisher-bypass-authorize: true`. |
| `POST /…/projects/…/query` (legacy alias) | Gate as above. Accepts no bypass — it exists for pre-rename SDK compatibility and passes no `givens` either, so a gated source is denied there regardless. Use the `/environments/…` route. |
| Notebook cell `GET` | Gate each cell that runs a query. Accepts no bypass. |
| `POST /…/compile` | Gate the named source the submitted text targets (early, before compiling — so compile errors can't be used as a schema oracle — plus a compiled-source backstop). Accepts no bypass. |
| MCP `malloy_executeQuery` | Routes through the query path; a denial surfaces as `isError: true` naming the source. Sends no bypass. |

**Fail-closed, evaluated as a disjunction.** Each in-scope expression is probed independently; a branch that errors, references an unset given, or returns null / non-`true` is treated as *not granting*, and the next branch is tried. The request is denied only when **no** branch returns `true`. So a single-gate source with an unset given is denied, but a source whose *other* gate is satisfied still grants — the skip keeps OR semantics intact.

### Validation

Authorize expressions are validated at **model load** (compile-only, no execution). A malformed annotation (missing quotes), an unknown given, or a source-field reference that isn't a valid [row-level gate](#row-level-gates) fails the load with **HTTP 424** (`ModelCompilationError`), naming the source and the underlying reason. Fix the model before it serves.

**A gate must resolve at every entry point through which the source declaring it is reached, and an entry point where it cannot is closed rather than opened.** `rename:`, `except:`, and `accept:` on an extending source can remove the field a gate was written against. Which way that fails depends on *who wrote the gate*. Where the entry point declares its **own** `#(authorize)`, the annotation is the author's own mistake and package load fails with a 424 naming the source. Where the entry point only **inherits** the gate — `W is X extend { except: org_id }`, whose `extend` dropped the field X's gate needs — load succeeds with a warning and **that entry point denies every request**; the rest of the model still serves. The split exists so one unreachable derived entry point cannot take down a whole package, and neither branch can serve a silently unfiltered or wrongly filtered result. This is distinct from *inheriting* a gate that still resolves (see the worked example below and [row-level-access.md](row-level-access.md#row-level-authorize)): there, the filter runs inside the base's own build, before the deriving source's projection ever applies, so a derived source that projects the gated column away still serves correctly filtered — there is no need to keep an inherited gate's column in the projection.

Validation probes every entry point, but it does not re-derive an inherited gate's *given* reachability. A gate a source only *inherits* is authored in its base's given namespace, and Malloy merges one level of `import`, so a base two or more hops away can reference a given the extending model cannot see. Probing that from here would fail the load with a 424 blaming an annotation that is perfectly valid where it was written. Inherited gates are still enforced at request time, fail-closed: an expression that cannot be parsed becomes a single unsatisfiable `false`.

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

- `publisher_authorize_row_level_total`, labelled `decision` (`denied_by_gate` | `empty_after_filter`). `denied_by_gate` is the fail-closed refusal when a row-level gate could not be applied; `empty_after_filter` is a normal 200 with zero rows after the filter matched none, which is not an error.
- `publisher_authorize_row_level_rejected_total`, labelled `cause` (`array_given_needs_in` | `scalar_given_rejects_in` | `field_given_has_default` | `unsupported_node` | `no_given_reference` | `unreachable_given` | `entry_point_unexpressible`). Fires at package load — alert on any nonzero value since the last publish, not on a rate. The first six mean the gate's compiled condition is not an allowed shape, and each fails the whole model load. `entry_point_unexpressible` is the exception in both respects: the gate is a valid shape, but one derived entry point (an `extend` that renamed/excluded/projected away the gated field, or a `query_source` projection) cannot express it. That does **not** fail the load — the rest of the model serves normally, and every request against that one entry point is denied.

`publisher_authorize_guard_rejected_total`, labelled `field` (`query` | `source_name` | `query_name` | `compile_source`), counts requests rejected with 400 for declaring an `#(authorize)` annotation in caller-submitted Malloy text.

**This is an interim answer, not the intended one.** The shape that keeps the decision with the model author is identity-bound givens (above) — a reserved system given the caller cannot set, so an author writes `#(authorize) "$ROLE = 'analyst' or $SYSTEM_CALLER = 'indexer'"` and a source they never opted in stays gated. This header removes the gate globally instead, for callers the deployment trusts wholesale. When identity-bound givens land, this should narrow or go.

## Known limitations

- **A request can be exempted from the gate entirely** (see [Authorize bypass](#authorize-bypass-for-trusted-data-management-callers)). `x-publisher-bypass-authorize: true` on a query request skips evaluation, and Publisher does not bound who may send it — so on a deployment that does not strip the header at its edge, every gate is advisory for any caller who knows the name. Listed first here because it is the only limitation on this page that a *deployment*, not a model, has to close.
- **A gate does not follow a join** (see [above](#the-entry-point-and-only-the-entry-point)). This is the limitation with the largest practical consequence: any source that joins gated data and is itself ungated hands that data to every caller. Treat "which sources can a caller enter through, and what does each of them reach" as part of modelling, not as something the gate handles for you.
- **An extension's own gate replaces the base's** (see [above](#the-entry-point-and-only-the-entry-point)) — that is the curated-extension idiom, so pair locked bases with access modifiers to keep the re-exposed column surface deliberate. (An extension with no gate of its own carries the base's.)
- **A source the caller declares in its own query text is gated after compiling, not before.** For a source the package declares — named plainly, or as an expression over the name like `locked extend { … } -> { … }` or a refinement `locked_q + { … }` — the gate runs before compilation, so a denial cannot be used to read the schema. A caller who writes `source: mine is locked_base extend {}` in the `query` text is different: `mine` does not exist until that text compiles, so there is nothing to gate first. The gate still fires — the compiled run target carries the base's gate and the request is denied with a 403, and no rows are ever returned — but a *malformed* probe (`group_by: no_such_field`) gets Malloy's "field is not defined" instead, which confirms whether a column exists on the locked base. Closing it would mean resolving a gate out of untrusted text before compiling it, which is exactly the resolution-from-text this design refuses to do (see [Security model](#security-model)). Behind the trusted tier the exposure is a column name, not data.
- **`/compile` raw SQL is not gated.** The gate covers named Malloy sources; `/compile` still compiles unrestricted, so a caller could read a gated table's schema/SQL via raw `duckdb.sql(...)`. Closing this (restricted compilation on `/compile`, as on `/query`) is tracked as a follow-up; until then keep `/compile` behind the trusted tier.
- **No per-request caching.** Each gate runs a fresh probe against bundled DuckDB (microseconds); a security decision is intentionally not memoized.
- **A gate inherited from a base in another file only ever sees caller-supplied given values, never that base's own `given:` defaults.** The isolated probe (`bindProbeGivens`) declares a given only when the caller actually supplied a value for it. This is intentionally conservative: a probe compiled from name-only identity (see [Security model](#security-model)) has no reliable way to attribute a `given:` default to the *specific* source it's gating rather than to an ambient/entry-model given of the same name — so an unsupplied given always denies rather than risk resolving someone else's default. Practical effect: to pass such a gate the caller must supply every given the expression references; a permissive default on the base does not open it up.
- **A row-level gate's given, unlike an inherited given-only gate's, gets no runtime fallback.** The two limitations above describe an inherited *given-only* gate reaching a given through caller-supplied values at request time; a [row-level gate](#row-level-gates)'s given is instead checked once, at load.
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

# No identity → denied
curl -s -X POST $API/secured.malloy/query -H 'content-type: application/json' \
  -d '{"query":"run: orders_secured -> by_status"}'                            # → 403

# Admin → allowed (all rows)
curl -s -X POST $API/secured.malloy/query -H 'content-type: application/json' \
  -d '{"query":"run: orders_secured -> by_status","givens":{"ROLE":"admin"}}'  # → 200
```

The row-level half of that source — how `where:` narrows an *allowed* caller to their own rows — is
covered in [row-level-access.md](row-level-access.md).
