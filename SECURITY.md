# Security policy

## Supported versions

Publisher releases continuously off `main`; there are no maintenance branches, so only the latest
release is supported. Fixes land in the next release — upgrade rather than expect a backport.

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

- If you're unsure, report privately — we'd rather triage a non-issue in private, and we'll move it
  into the open once it's clearly not sensitive.
- Anything that exposes data or credentials, or executes code across a boundary Publisher claims,
  goes private — always.
- An issue that only weakens defense in depth behind the documented trusted tier is reasonable to
  file in the open.

## Scope

Publisher is unauthenticated by design, and several governance features are documented as
caller-asserted conventions rather than boundaries — that shapes what counts as a vulnerability here.

### Working as documented (not vulnerabilities)

- The REST and MCP surfaces being unauthenticated, and the server binding `0.0.0.0` by default
  ([README.md § Point your agent at it](README.md#point-your-agent-at-it),
  [docs/ai-agents.md](docs/ai-agents.md)). The supported posture is loopback for local use, an
  authenticating gateway in front for anything wider.
- `givens`, `#(authorize)`, and row-level access being caller-asserted, including the gaps in
  [docs/authorize.md § Security model](docs/authorize.md#security-model) and
  [§ Known limitations](docs/authorize.md#known-limitations), and in
  [docs/row-level-access.md](docs/row-level-access.md).
- Broad reach for whoever can publish a package or `PATCH` a connection on a bare Publisher
  ([docs/query-metadata.md](docs/query-metadata.md), [docs/packages.md](docs/packages.md)).
- The default `Content-Security-Policy: frame-ancestors *`, which `PUBLISHER_FRAME_ANCESTORS` exists
  to tighten ([docs/html-data-apps.md § Security model](docs/html-data-apps.md#security-model),
  [docs/configuration.md](docs/configuration.md)).
- Findings that require ignoring the deployment posture above.

### In scope

Anything that breaks a boundary Publisher does claim, including:

- A direct query succeeding against a source the package never exported — past the
  **queryable == discoverable** boundary in
  [docs/discovery-and-access.md](docs/discovery-and-access.md).
- An `#(authorize)` gate granting access its expression should deny for the givens actually sent,
  including none.
- Connection credentials or secrets leaking through an API response, log line, or error.
- Escaping a package's static root — path traversal or symlink past the rejections in
  [docs/html-data-apps.md § Security model](docs/html-data-apps.md#security-model).
- Remote code execution via package load or model compile.
- A reachable vulnerability in a bundled dependency.

## What to expect

- Acknowledgement within 5 business days.
- An in-scope-or-not call, with a rough timeline, within 10.
- The advisory stays private until a fix ships.
- You're credited in the published advisory unless you'd rather not be.

## Hardening

Running Publisher where it can be reached? The deployment posture is in
[README.md § Point your agent at it](README.md#point-your-agent-at-it), and the trust models behind
givens are in [docs/authorize.md § Security model](docs/authorize.md#security-model) and
[docs/discovery-and-access.md](docs/discovery-and-access.md).
