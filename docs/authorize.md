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

### Run target, plus every joined source (not inherited through extend)

Authorize is checked on the source the query directly runs against **and on every source reached transitively via `join_*`** — a gate on a joined source is not bypassed by joining instead of naming it directly. It is still **not** inherited through `extend`:

- **Extend footgun:** a source that extends a locked base is governed *solely by its own* annotations. `source: b is a extend { … }` does **not** inherit `a`'s gate — if `b` declares none, `b` is unrestricted even if `a` is `#(authorize) "false"`.
- **Joins are enforced.** A gate on a source reached only via `join_*` — including a deep transitive join (A→B→C), a query-local `join_one` inside a `-> { … }` refinement, and a composite source (`compose(a, b)`) resolved to a locked branch — fires the same as a gate on the run target itself. Semantics are AND across sources: any single reachable gate failing denies the whole query.
- **Query-source derivation doesn't launder the gate away.** `source: laundered is locked_src -> { … }` gates on `locked_src` too — Malloy's compiled `QuerySourceDef` keeps the base reachable via `query.structRef`, so the walk resolves and gates it (recursing through a chained derivation, or through a join to a query-source). This also covers a query-source reached only via `join_*`.

To keep a locked base's data from leaking through an extension, pair the gate with Malloy [access modifiers](https://docs.malloydata.dev/documentation/experiments/include) (`include { public: …, private: * }`) so the extension re-exposes only a curated column surface. See the [recommended pattern](#recommended-pattern-locked-base-and-curated-extensions).

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

### Error contract & redaction

- **Runtime 403** names only the source — `{"code":403,"message":"Access denied for source \"orders\"."}` — never the authorize expression. Gate logic is not leaked to (potentially untrusted) query callers.
- **Model-load 424** *keeps* the full expression in its message — it is author-facing at package load and you need it to fix a malformed annotation.

## Security model

`#(authorize)` evaluates expressions over **request-supplied givens**. There is no authentication in Publisher's query path: a given is whatever the caller sends. So:

- **Authorize is a real boundary only behind a trusted tier.** The intended deployment is Publisher behind an embedding application that authenticates end users and sets givens (role, tenant, region) from its own *verified* context, with the query/MCP API network-isolated from untrusted callers. In that setup the gate enforces the trusted tier's policy.
- **It does not defend against a caller who sets their own givens.** Exposed directly to untrusted users, anyone can send `{"ROLE":"admin"}` and pass an `$ROLE = 'admin'` gate. Do not treat `#(authorize)` as end-user authn/authz on a public endpoint.
- **Identity-bound givens** — a verified token or trusted-proxy header populating reserved "system givens" the caller cannot override — is a planned milestone that would make authorize a standalone boundary. It is not implemented yet.

## Known limitations

- **Not inherited through extend** (see [above](#run-target-plus-every-joined-source-not-inherited-through-extend)) — pair locked bases with access modifiers. (Joins ARE enforced — see above.)
- **Joining a composite over-denies conservatively.** When a composite (`compose(a, b)`) is reached via `join_*`, Malloy does not surface which branch resolved, so every member's gate is applied. A query using only an ungated member of a *joined* composite is therefore denied if any sibling member is locked. This fails closed (safe) and affects only the experimental composite-source feature. (A composite used as the run target resolves precisely via `compositeResolvedSourceDef` and does not over-deny.)
- **`/compile` raw SQL is not gated.** The gate covers named Malloy sources; `/compile` still compiles unrestricted, so a caller could read a gated table's schema/SQL via raw `duckdb.sql(...)`. Closing this (restricted compilation on `/compile`, as on `/query`) is tracked as a follow-up; until then keep `/compile` behind the trusted tier.
- **No per-request caching.** Each gate runs a fresh probe against bundled DuckDB (microseconds); a security decision is intentionally not memoized.
- **A joined source's gate only ever sees caller-supplied given values, never the joined source's own `given:` defaults.** The isolated probe (`bindProbeGivens`) declares a given only when the caller actually supplied a value for it; it never falls back to the joined source's own default for a given the caller left unsupplied. This is intentionally conservative: a probe compiled from name-only identity (see [Security model](#security-model)) has no reliable way to attribute a `given:` default to the *specific* joined source it's gating rather than to an ambient/entry-model given of the same name — so an unsupplied given always denies rather than risk resolving someone else's default. Practical effect: to grant access through a joined gate, the caller must supply every given the gate's expression references; declaring a permissive default on the joined source itself does not open it up.
- **A given-based gate on a source reached through a multi-hop transitive import works, but needs a self-contained probe to get there.** Malloy's own given-namespace merge covers only one level of `import` — a `given:` declared two-or-more imports away from the entry model is not in the entry model's namespace, so a request-scoped `Model.getQueryResults`/`executeNotebookCell` value for it is also dropped before it ever reaches the real query (`filterGivensToModelSurface`, `model.ts`). The gate's own probe is unaffected: it's self-contained (`bindProbeGivens`, `authorize.ts`) — it declares just the given(s) an expression references, inferring type from the supplied value, rather than depending on the entry model's namespace — so the *gate* correctly evaluates a value the real query can't otherwise see. Author-side implication: for a caller to actually reach data behind such a gate, some source or file within one import hop of the entry model must also declare (or import) the same given, so the real query's own given-resolution surfaces it.

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
