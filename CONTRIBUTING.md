<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

## Project Committers

Our committers are the following GitHub accounts:

- lloydtabb
- mtoy-googly-moogly
- carlineng
- christopherswenson
- nachoarreola
- skokenes
- whscullin

## Developer Certificate of Origin

All new inbound code contributions must also be accompanied by a Developer
Certificate of Origin (http://developercertificate.org) sign-off in the source
code system that is submitted through a TSC-approved contribution process which
will bind the authorized contributor and, if not self-employed, their employer
to the applicable license.

Contributors sign-off that they adhere to these requirements by adding a
Signed-off-by line to commit messages.

Git has a -s command line option to append this automatically to your commit
message, for example:

```
$ git commit -s -m 'This is my commit message'
```

## Code Reviews

All submissions, including submissions by project members, require review. We
use GitHub pull requests for this purpose. Consult
[GitHub Help](https://help.github.com/articles/about-pull-requests/) for more
information on using pull requests.

## Code of Conduct

This project follows
[The Linux Foundation's Code of Conduct](https://lfprojects.org/policies/code-of-conduct/).

## Security

Report a security vulnerability privately rather than as a public issue.
[SECURITY.md](SECURITY.md) has the reporting form, what's in scope, and when filing in the open is
fine.

## Contributing to the Python SDK (`packages/python-client`)

The Python SDK is **auto-generated** from `api-doc.yaml` using OpenAPI Generator plus a thin build script.

### How to regenerate the client

```bash
# From repo root
cd packages/python-client
scripts/build-python-sdk.sh  # validates spec, regenerates, formats, tests
```
This script must run **cleanly** (no drift, tests pass) before your PR can be merged.

### When you MUST regenerate

* Any change to `api-doc.yaml` (the REST spec)
* Upgrading generator templates or build tooling

The GitHub Action (`.github/workflows/python-sdk.yml`) will fail if the generated code is out of date.

### Tests

Unit tests live in `packages/python-client/tests/` and run automatically in CI.
Use `uv pip install -e ".[test]"` inside `packages/python-client` to install dev deps, then:

```bash
pytest tests/ -q
```

On every PR into `main`, CI also builds the wheel and sdist, starts a Publisher server built from
the PR, installs the wheel into a clean venv on every Python version `pyproject.toml` declares, and
drives the server through it (`scripts/smoke-test-wheel.py`). That runs on server-only PRs too,
since a server change can break the SDK without touching `api-doc.yaml`. A PR that changes
`requires-python` or the version classifiers changes what that job tests.

### Releasing to PyPI

1. Bump `version` in `pyproject.toml` in your PR, as a plain `major.minor.patch`. The
   `python-sdk.yml` check fails a PR that changes the client or `api-doc.yaml` without a version that
   is free on PyPI and above its latest.
2. Merge it. The next Publisher release (`release.yml`) builds and verifies the SDK from its release
   branch and uploads it to PyPI through trusted publishing (OIDC; there is no API token). A release
   whose SDK version is already on PyPI skips the upload.

There are no tags to push and nothing to dispatch by hand. See *Publishing to PyPI* in
`.github/workflows/CONTEXT.md`.

### Pre-commit hook

The repo defines a pre-commit entry which auto-regenerates the SDK when `api-doc.yaml` changes.  Run `pre-commit install` after cloning.
