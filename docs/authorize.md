# Authorize (Source Access Gates)

> What this is: how `#(authorize)` annotations gate *who* may query a source (HTTP 403 otherwise).
> Runnable example: [examples/governed-analytics](../examples/governed-analytics). For the base
> mechanism, see [givens.md](givens.md); for row scoping, see [row-level-access.md](row-level-access.md).

`#(authorize)` is the **source-authorization** application of [givens](givens.md): it allows or denies access to an *entire* source. (To scope *which rows* a caller sees within an allowed source, see [Row-level access](row-level-access.md).)

`#(authorize)` annotations gate query access to a Malloy source based on the request's [givens](givens.md). Before Publisher runs any query that reads a gated source, it evaluates the source's in-scope authorize expressions against the supplied givens; if **at least one** returns `true` the request proceeds, otherwise it is rejected with **HTTP 403**. A source with no in-scope annotations is unrestricted.

Authorize is evaluated by Publisher (not core Malloy) using a synthetic probe query against bundled DuckDB (a one-row `SELECT 1`), so the expression language is Malloy's, but the gate runs entirely over `given:` values — it never touches your warehouse data.

> ⚠️ **Read [Security model](#security-model) before deploying this as an access control.** Givens are **caller-asserted**: anyone who can reach the query API can claim a favorable given. `#(authorize)` is only a real boundary when the API sits behind a trusted tier that sets givens from its own verified context. It is not, on its own, end-user authentication.

For the Malloy expression reference, see [Malloy: Expressions](https://docs.malloydata.dev/documentation/language/expressions). For givens, see [givens.md](givens.md).

## Declaring Gates

Authorize annotations attach to a source (`#(authorize)`) or to the whole file (`##(authorize)`). The body is a quoted Malloy boolean expression over declared givens (`$NAME`):

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
- **File-level** `##(authorize) "<expr>"` — applies to every query in the file; folded into the same disjunction as each source's own gates. A permissive file-level gate is a **model-wide override** (see [file-level override](#file-level-is-a-model-wide-override)).
- A source with no in-scope annotations is **unrestricted**.

### Expression Language

The expression is any Malloy boolean expression over `$given` references and literals: `=`, `!=`, `<`, `>`, `<=`, `>=`, `and`, `or`, `not`, `in [...]`, etc. Examples:

```malloy
#(authorize) "$ROLE = 'analyst'"
#(authorize) "$ROLE in ['analyst', 'admin']"
#(authorize) "$REGION = 'us-west' and $ROLE != 'guest'"
#(authorize) "$TENANT = 'acme'"
```

**No source-field references.** The probe evaluates your expression against its own synthetic one-column row, not against your source, so an expression may reference only givens and literals — a column of the gated source isn't in scope and fails at model load (see [Validation](#validation)).

Embedded quotes follow Malloy string rules: write inner string literals with single quotes inside the double-quoted annotation, e.g. `#(authorize) "$ROLE = 'analyst'"`.

## Semantics

### OR semantics

Multiple in-scope expressions (source-level + file-level) are evaluated as a single **disjunction** — access is granted if **any one** returns `true`.

```malloy
given:
  ROLE :: string
  TENANT :: string

#(authorize) "$ROLE = 'admin'"
#(authorize) "$TENANT = 'acme'"
source: orders is duckdb.table('orders.parquet')
```

`orders` is queryable by an admin **or** by an acme-tenant caller. Stacking gates **widens** access — if you expect AND, this will surprise you. Express conjunction within a single expression instead: `#(authorize) "$ROLE = 'admin' and $TENANT = 'acme'"`.

### File-level is a model-wide override

A `##(authorize)` expression is in scope for **every** source in the file and joins the disjunction for each. Because the disjunction grants on any `true`, a permissive file-level gate **unlocks every source in the file**, regardless of stricter source-level gates:

```malloy
##(authorize) "$ROLE = 'admin'"      // admins can query ANY source in this file

#(authorize) "$ROLE = 'analyst'"     // analysts can ALSO query `orders`
source: orders is duckdb.table('orders.parquet')
```

This is the intended admin-override idiom — use it deliberately.

### The entry point, and only the entry point

Authorize is checked on the source the query **enters through** — the run target. A gate answers "who may query this source", not "who may read everything reachable beneath it".

- **Joins are not gated.** A gate on a source reached only via `join_*` **does not fire** — at any depth (A→B→C), aliased, across files, declared in a query-local `join_one` inside a `-> { … }` refinement, or as a member of a joined composite. `run: joiner -> { … }` where `joiner` joins a base locked with `#(authorize) "false"` returns rows. **This is the rule authors have to design around:** joining sensitive data into an ungated source publishes it. Put the gate on the source callers enter through, and use [access modifiers](https://docs.malloydata.dev/documentation/experiments/include) (`include { public: …, private: * }`) to control what an extension re-exposes.
- **Extend: own gate replaces, otherwise the base's carries.** `source: b is a extend { … }` is governed by `b`'s own `#(authorize)` when it declares one — that replacement is the [curated-extension idiom](#recommended-pattern-locked-base-and-curated-extensions). When `b` declares none it is gated by `a`'s. That holds however `b` is decorated: a render tag or doc comment on `b` moves `a`'s annotations off `b`'s own notes (Malloy files them under `annotations.inherits`), so resolution follows both the inherits chain and the source registry to the declaration `b` derives from.
- **Query-source derivation carries the gate too.** `source: laundered is locked_src -> { … }` is gated by `locked_src`'s gate when it declares none of its own — the compiled `QuerySourceDef` keeps the base reachable via `query.structRef`, and a chained derivation resolves through it as well. Derivation is treated like `extend`; reaching the same derived source via a *join* is still not gated.
- **A composite run target resolves precisely.** When the run target is `compose(a, b)`, Malloy resolves it to exactly one member branch per query and that branch's gate applies.
- **Caller-submitted text may not declare a gate at all.** An `#(authorize)` / `##(authorize)` annotation in the `query` text of a query request, or in the `source` text submitted to `/compile`, is rejected with a 400: the override above is the model author's to make. Notebook cells are package content, so an author's gate there works normally.

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

The gate runs, fail-closed, on every query entry point — **before** any filter injection or compilation, so a denial is a clean 403 and never masked by a later error:

| Entry point | Behavior |
| --- | --- |
| `POST /…/query` | Gate the run-target source; deny → 403. |
| Notebook cell `GET` | Gate each cell that runs a query. |
| `POST /…/compile` | Gate the named source the submitted text targets (early, before compiling — so compile errors can't be used as a schema oracle — plus a compiled-source backstop). |
| MCP `malloy_executeQuery` | Routes through the query path; a denial surfaces as `isError: true` naming the source. |

**Fail-closed, evaluated as a disjunction.** Each in-scope expression is probed independently; a branch that errors, references an unset given, or returns null / non-`true` is treated as *not granting*, and the next branch is tried. The request is denied only when **no** branch returns `true`. So a single-gate source with an unset given is denied, but a source whose *other* gate is satisfied still grants — the skip keeps OR semantics intact.

### Validation

Authorize expressions are validated at **model load** (compile-only, no execution). A malformed annotation (missing quotes), an unknown given, or a source-field reference fails the load with **HTTP 424** (`ModelCompilationError`), naming the source and the underlying reason. Fix the model before it serves.

Validation covers the gates each source **declares** — its own `#(authorize)` plus the file-level `##(authorize)` — and deliberately stops there. A gate a source only *inherits* is authored in its base's given namespace, and Malloy merges one level of `import`, so a base two or more hops away can reference a given the extending model cannot see. Probing it from here would fail the load with a 424 blaming an annotation that is perfectly valid where it was written. Inherited gates are still enforced at request time, fail-closed: an expression that cannot be parsed becomes a single unsatisfiable `false`.

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

## Known limitations

- **A gate does not follow a join** (see [above](#the-entry-point-and-only-the-entry-point)). This is the limitation with the largest practical consequence: any source that joins gated data and is itself ungated hands that data to every caller. Treat "which sources can a caller enter through, and what does each of them reach" as part of modelling, not as something the gate handles for you.
- **An extension's own gate replaces the base's** (see [above](#the-entry-point-and-only-the-entry-point)) — that is the curated-extension idiom, so pair locked bases with access modifiers to keep the re-exposed column surface deliberate. (An extension with no gate of its own carries the base's.)
- **A source the caller declares in its own query text is gated after compiling, not before.** For a source the package declares — named plainly, or as an expression over the name like `locked extend { … } -> { … }` or a refinement `locked_q + { … }` — the gate runs before compilation, so a denial cannot be used to read the schema. A caller who writes `source: mine is locked_base extend {}` in the `query` text is different: `mine` does not exist until that text compiles, so there is nothing to gate first. The gate still fires — the compiled run target carries the base's gate and the request is denied with a 403, and no rows are ever returned — but a *malformed* probe (`group_by: no_such_field`) gets Malloy's "field is not defined" instead, which confirms whether a column exists on the locked base. Closing it would mean resolving a gate out of untrusted text before compiling it, which is exactly the resolution-from-text this design refuses to do (see [Security model](#security-model)). Behind the trusted tier the exposure is a column name, not data.
- **`/compile` raw SQL is not gated.** The gate covers named Malloy sources; `/compile` still compiles unrestricted, so a caller could read a gated table's schema/SQL via raw `duckdb.sql(...)`. Closing this (restricted compilation on `/compile`, as on `/query`) is tracked as a follow-up; until then keep `/compile` behind the trusted tier.
- **No per-request caching.** Each gate runs a fresh probe against bundled DuckDB (microseconds); a security decision is intentionally not memoized.
- **A gate inherited from a base in another file only ever sees caller-supplied given values, never that base's own `given:` defaults.** The isolated probe (`bindProbeGivens`) declares a given only when the caller actually supplied a value for it. This is intentionally conservative: a probe compiled from name-only identity (see [Security model](#security-model)) has no reliable way to attribute a `given:` default to the *specific* source it's gating rather than to an ambient/entry-model given of the same name — so an unsupplied given always denies rather than risk resolving someone else's default. Practical effect: to pass such a gate the caller must supply every given the expression references; a permissive default on the base does not open it up.
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
