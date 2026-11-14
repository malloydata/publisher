<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# The package policy: `scope`, `queryMetadata`, and where the rules are enforced

```jsonc
{
  "name": "my-package",
  "queryMetadata": { "team": "finance" },   // root level
  "materialization": {
    "scope": "package",                      // default; or "version"
    "freshness": { "window": "24h", "fallback": "live" },
    "schedule": "0 6 * * *"                  // never together with freshness
  }
}
```

`freshness` and `schedule` are explained in `reference/refresh.md`. This file covers the rest of the block and the gate that checks it.

## `scope`

`package` (default) or `version`. It is a package-level contract about whether a persisted table may be shared across published versions: under `package` an unchanged source's table is reused by every version that computes the same SQL; under `version` each published version owns its own tables. There is no per-source scope and no per-source schedule; both are declared once for the package.

Two things to know about it on a standalone server:

- **It changes nothing there except gating `schedule`.** A standalone auto-run keys tables by content address regardless of scope; true per-version tables are produced when a hosted control plane assigns versioned build targets. Standalone is the place to exercise the *policy* rules, not per-version fan-out.
- **A root-level `scope` is the deprecated home** and still works, with a warning. Whenever the server rewrites the manifest (any package PATCH, including a description-only one) it writes **both** homes with the same value, so the root key reappears after an API edit. **The two homes holding different values is not a warning: the package fails to load**, disappears from the server, and says so only in `/status` `loadErrors`. Editing by hand, change `materialization.scope` and delete the root copy; never edit the root one.

## `queryMetadata`

A bag of up to 20 string properties attached to **every statement a build issues and every query against the source**, for the backend's own cost attribution: Snowflake `QUERY_TAG`, BigQuery job labels, a leading SQL comment elsewhere. Publisher adds its own context on top (`class=materialize` with the package, source, trigger and run id on a build; `class=interactive` on a served query), and drops a model-side declaration that reuses one of those server-owned names.

- **Declare it at the root of `publisher.json`.** The older home inside `materialization` still works but emits a deprecation warning on every load and publish.
- **Three layers, most specific wins:** a sibling line on the source (`#@ queryMetadata.team="risk"` above the `source:`), a model-file line (`## queryMetadata.team="risk"`), then the package block.
- **Observability only.** It never changes what gets built, is not part of the content address, and problems with it (a bad key, an over-long value, the deprecated home) are warnings, never rejections.

See `docs/query-metadata.md` for the per-backend rendering.

## Where the rules are enforced

The scope / schedule / freshness coherence rules - a single package-level scope, `schedule` requires `scope: "version"`, `schedule` and `freshness` (including a per-source `freshness.*`) are mutually exclusive, a valid 5-field UTC cron - are checked in four places:

| Where | Outcome |
|---|---|
| publish | rejected (400) |
| a package edit that touches the policy | rejected (400); an edit that does not touch it is not re-checked |
| package load | warned; the package still serves |
| the scheduler | an offending package is skipped |

Only those coherence rules are gates. `queryMetadata` findings, the deprecated homes, and an unrecognized `#@ persist` key are advisory. An invalid `refresh="incremental"` declaration is the other way round: it **fails the load** outright (`reference/refresh.md`). The `malloy-pub schedule` commands share the publish gate, so a rejection from them means the change was unsafe - and `schedule set` replaces the whole `materialization` block, dropping a freshness policy it finds there.
