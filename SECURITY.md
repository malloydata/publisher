# Security policy

## Supported versions

Publisher releases continuously off `main`; there are no maintenance branches. Fixes land in the next
release, and the ask is that you upgrade rather than expect a backport.

| Version             | Supported |
| ------------------- | --------- |
| Latest release      | ✅         |
| Any earlier release | ❌         |

## Reporting a vulnerability

> Report privately: **[Report a vulnerability](https://github.com/malloydata/publisher/security/advisories/new)**
> (repo → Security → Advisories → Report a vulnerability).

What to include, as much as you have:

- The affected version or commit.
- How Publisher was configured and reached — flags, host binding, gateway or none.
- Reproduction steps, or a proof of concept.
- The impact you believe it has.

## Private or public?

- If you're unsure, report privately. We'd rather triage a non-issue in private than have a real one
  land in public.
- Maintainers may move a report into a public issue once it's clear it isn't sensitive.
- Anything that exposes data or credentials, or executes code across a boundary Publisher claims,
  goes private — always.
- An issue that only weakens defense in depth behind the documented trusted tier is reasonable to
  file in the open.

## Scope

Publisher is unauthenticated by design, and several of its governance features are documented as
caller-asserted conventions rather than boundaries. That shapes what is and isn't a vulnerability
here. Each item below defers to the doc that already documents the behavior.

### Working as documented (not vulnerabilities)

- The REST and MCP surfaces being unauthenticated, and the server binding `0.0.0.0` by default
  ([README.md § Point your agent at it](README.md#point-your-agent-at-it),
  [docs/ai-agents.md](docs/ai-agents.md)). The supported posture is loopback plus an
  authenticating gateway.
- `givens`, `#(authorize)`, and row-level access being caller-asserted
  ([docs/authorize.md § Security model](docs/authorize.md#security-model),
  [docs/row-level-access.md](docs/row-level-access.md)).
- Broad reach for whoever can publish a package or `PATCH` a connection on a bare Publisher
  ([docs/query-metadata.md](docs/query-metadata.md), [docs/packages.md](docs/packages.md)).
- The default `Content-Security-Policy: frame-ancestors *`, which `PUBLISHER_FRAME_ANCESTORS` exists
  to tighten ([docs/html-data-apps.md § Security model](docs/html-data-apps.md#security-model),
  [docs/configuration.md](docs/configuration.md)).
- Findings that require an operator to have ignored the documented deployment guidance.

### In scope

Anything that breaks a boundary Publisher does claim, including:

- A request that reaches or returns rows from a source the package never exported — the
  **queryable == discoverable** rule in
  [docs/discovery-and-access.md](docs/discovery-and-access.md).
- An `#(authorize)` gate that fails open when the given _is_ supplied.
- Connection credentials or secrets leaking through an API response, log line, or error.
- Escaping a package's static root — path traversal or symlink past the 403s in
  [docs/html-data-apps.md § Security model](docs/html-data-apps.md#security-model).
- Remote code execution via package load or model compile.
- A reachable vulnerability in a bundled dependency.

## What to expect

- We acknowledge a report within 5 business days.
- Within 10 business days, an in-scope / not-in-scope assessment and a rough timeline.
- The advisory stays private until a fix ships.
- You're credited in the published GHSA unless you'd rather not be.

## Hardening

Running Publisher somewhere it can be reached? Start with
[docs/deployment.md](docs/deployment.md), then the trust models in
[docs/authorize.md § Security model](docs/authorize.md#security-model),
[docs/row-level-access.md](docs/row-level-access.md), and
[docs/discovery-and-access.md](docs/discovery-and-access.md).
