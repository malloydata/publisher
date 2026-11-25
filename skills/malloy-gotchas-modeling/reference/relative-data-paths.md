<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

## Relative Data-File Paths: Set an Absolute `workingDirectory`

`duckdb.table('data/x.csv')` is resolved against the DuckDB connection's `workingDirectory`, not
against the model file. Publisher sets that to the package root, so relative paths work there with
no config at all. Every other host reads it from `malloy-config.json`, and **a relative value there
is resolved against the process's current directory** - not the config file's directory, and not the
VS Code workspace root. `canonicalizeConfigPath` in `malloy-db-duckdb/src/duckdb_config.ts` calls
`canonicalizePath` with no `baseDirectory`, which is a bare `path.resolve(input)`.

So the same config works or fails depending on which directory the editor or shell was launched
from. That is why this breaks intermittently and appears to be a model bug.

```json
// WRONG: resolved against the process cwd, so it works from the repo root and
// fails from anywhere else - including however your editor happened to launch
{"connections": {"duckdb": {"is": "duckdb", "workingDirectory": "malloy"}}}

// RIGHT: absolute, so the cwd cannot change the answer
{"connections": {"duckdb": {"is": "duckdb", "workingDirectory": "/abs/path/to/pkg"}}}
```

Point it at the directory the model's table paths are written relative to - the package root, the
one holding `data/`. Keep the model's paths relative (`data/x.csv`) so Publisher still serves it;
only the config carries the absolute path.

**The symptom, and the cascade.** One `IO Error` at the `source:` line, then a "not defined" error
for every field of that source:

```
line 87: IO Error: No files found that match the pattern "data/product_usage.csv"
line 91: 'org_slug' is not defined
line 93: Reference to undefined value active_users
```

Those field errors are not real. Fix the first error and they all go. Do not start renaming
columns.

**Check the config before the model** when a source that Publisher queries fine fails in the editor
or CLI with a missing-file error. The model is the same file; the resolution base is not.
