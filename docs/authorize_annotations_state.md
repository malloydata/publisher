# Authorize Annotations — Implementation State

Running status + known-gap list for the `#(authorize)` / `##(authorize)` access-gate feature. User-facing reference is [authorize.md](authorize.md); the original design is [authorize-annotations-plan.md](authorize-annotations-plan.md).

## Status

| Area | Status | Landed in |
| --- | --- | --- |
| Parse + surface annotations on the API | ✅ | #804 |
| Compile-time validation (malformed / unknown given / field ref → 424) | ✅ | #805 |
| Runtime gate on `POST /query` + notebook cells | ✅ | #806 |
| Gate on `POST /compile` (early + compiled-source backstop) | ✅ | #814 |
| MCP `malloy_executeQuery` denial → `isError` naming the source | ✅ | #814 |
| HTTP 403 / 424 error mapping + redaction split | ✅ | #814 |
| Docs (`authorize.md`, this file, givens cross-link, release notes) | ✅ | this change |
| End-to-end sample + Playwright + filter×given×authorize compose | ⬜ | follow-up |

## Behavior (as shipped)

- **OR semantics:** file-level `##(authorize)` + a source's own `#(authorize)` are one disjunction; any `true` grants. Empty → unrestricted. Stacking widens access.
- **File-level is a model-wide override:** a permissive `##(authorize)` unlocks every source in the file.
- **Top-level-source only:** not inherited through `extend`, not walked through joins.
- **Fail-closed:** probe error / missing given / null / non-true → deny (403).
- **Redaction:** runtime 403 names the source only; model-load 424 keeps the expression (author-facing).

## Known gaps (intentional, documented)

1. **Extend footgun** — an extension of a locked base with no gate of its own is unrestricted. Mitigation: access modifiers (`include { … private: * }`). Pinned by `authorize_integration` ("does NOT inherit … through extend").
2. **Join bypass** — a gated source reached only via `join_*` is not gated (gate applies to the run target). Documented top-level-only boundary.
3. **`/compile` raw SQL not gated** — the gate covers named Malloy sources; `/compile` compiles unrestricted, so raw `duckdb.sql(...)` can read a gated table's schema/SQL. **Follow-up:** extend `#807`-style restricted compilation to `/compile`. Bounded by the trusted-tier model; keep `/compile` behind the trusted tier until closed.
4. **MCP integration test gap** — no full `callTool` integration test against a gated model; the MCP E2E harness (`mcpApp`-only, process-singleton env, no reload) can't seed a gated package per-test without ordering fragility. The transform is covered piecewise (unit + HTTP e2e + `handler_utils` branch). **Follow-up:** make the MCP E2E harness seedable, then add the `callTool` test.

## Deferred (not started)

- **Identity-bound givens** — verified token / trusted-proxy header populating reserved "system givens" the caller cannot override; turns authorize into a standalone boundary not dependent on the trusted-tier assumption. See [authorize.md § Security model](authorize.md#security-model).
- **Per-request probe memoization** — declined; microsecond DuckDB cost, stale-allow risk.

## Trust model (must stay loud in the docs)

Givens are **caller-asserted**; there is no auth in Publisher's query path. `#(authorize)` is a real boundary **only behind a trusted middle tier** that sets givens from verified context, with the query/MCP API network-isolated from untrusted callers. This is stated prominently at the top of [authorize.md](authorize.md) and in its Security model section.
