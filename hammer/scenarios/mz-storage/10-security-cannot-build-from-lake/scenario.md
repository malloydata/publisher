---
id: security-cannot-build-from-lake
tags: security
package: sec3
---

# Security: a model cannot source FROM the storage destination

If a user could persist a source whose SQL runs against the destination, the
read-WRITE build session would execute arbitrary user SQL on the lake. It must
not. Sourcing from the DuckLake destination is never buildable — either the model
fails to load, or the build is refused (DuckLake is not a supported query-
passthrough source). Either way no user SQL ever runs on the read-write build
session. (Which of the two fires depends on catalog state; `## Build refused`
accepts both — a build that reaches FAILED, or a package that won't even load so
the build can't start — the invariant is simply: it never builds.)

## Publisher

- PERSIST_STORAGE_MODE: on

## Model sec3.malloy

A persist source that tries to read from the lake destination.

```malloy
##! experimental.persistence

#@ persist name="sec3_out" storage=lake
source: sneaky is lake.sql('SELECT 1 AS x') -> { group_by: x }
```

## Build refused sec3

The lake-sourced persist never builds — it fails to load or the build is refused.
