<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# ADBC Snowflake driver shim

A ~100-line C shared library that the Docker image installs **as**
`libadbc_driver_snowflake.so` beside the DuckDB `snowflake` extension, with the
real driver renamed to `libadbc_driver_snowflake.real.so` in the same directory.
It forwards every ADBC call to the real driver unchanged and does one thing of
its own: after each successful `StatementNew` it sets the two statement options
the extension has no way to set.

| env var | ADBC option | driver default | image default |
| --- | --- | --- | --- |
| `ADBC_RESULT_QUEUE_SIZE` | `adbc.rpc.result_queue_size` | 100 | unset — opt in at deploy time with `1` |
| `ADBC_PREFETCH_CONCURRENCY` | `adbc.snowflake.rpc.prefetch_concurrency` | 5 | unset — leave at the driver default |

An unset or empty variable sets nothing for that option, so with neither set — the
image default — the shim is a pure pass-through and the driver behaves exactly as
upstream ships it. Set `ADBC_RESULT_QUEUE_SIZE=1` on the deployment to turn the
bound on; the image does not default it, for the same reason `PUBLISHER_DUCKLAKE_*`
have no defaults: an upgrade must not change behaviour until an operator asks.

## Why it exists

The ADBC Snowflake driver prefetches result chunks ahead of the consumer with
no bound tied to consumption: each chunk gets its own buffered channel and a
chunk's goroutine releases its concurrency slot the moment its download
finishes, while the decoded records stay queued. Whenever DuckDB consumes a
`snowflake_query()` stream more slowly than the network delivers it — a
`CREATE TABLE AS` into DuckLake on object storage is the case that matters
here — the *remaining result set* accumulates in memory, outside DuckDB's
buffer manager, so `memory_limit` neither sees nor bounds it.

`result_queue_size = 1` makes every stream's goroutine block after one record
until the consumer reaches that stream, which re-couples the read-ahead to
consumption. Measured on `TPCH_SF100.ORDERS LIMIT 20M` (9 columns) with a
deliberately slow single-threaded Parquet writer, peak cgroup `anon`:

| driver options | peak memory | wall |
| --- | --- | --- |
| defaults (queue 100 / prefetch 5) | 3325 MiB — the whole result, with 1% consumed | 598 s |
| queue 1 / prefetch 5 | 286 MiB, flat | 524 s |

Byte-identical output. On a fast consumer (100M rows aggregated) queue 1 costs
nothing measurable (22–26 s vs 21–26 s) and even there removes the 400–1000 MiB
the defaults buffered. Lowering `prefetch_concurrency` *does* cost throughput
(2 → 1.3–1.6× slower), which is why the image leaves it alone.

The extension (`iqea-ai/duckdb-snowflake`) never calls `AdbcStatementSetOption`
and exposes no secret field, `ATTACH` option, setting or environment variable
that reaches these options, so a wrapper at the driver boundary is the only
place to set them without forking the extension.

## When it goes away, and how

This is an interim. It comes out once **either**:

- [iqea-ai/duckdb-snowflake#66](https://github.com/iqea-ai/duckdb-snowflake/issues/66)
  ships and the baked extension is at that version: the extension exposes the
  options (proposed as `SET snowflake_result_queue_size` /
  `SET snowflake_prefetch_concurrency`), and the server issues the `SET` itself
  on the build path (`federateSourceForPassthrough`) and the live `ATTACH` path
  in `connection.ts`, best-effort so an older extension does not fail the read; or
- [adbc-drivers/snowflake#197](https://github.com/adbc-drivers/snowflake/issues/197)
  ships and `ADBC_SNOWFLAKE_VERSION` in the Dockerfile is bumped to it: the
  driver bounds buffered records by consumption, as its documentation already
  implies, and nothing needs to set anything.

Every touchpoint carries the marker `ADBC-SHIM`, so the removal set is:

```
$ grep -rn 'ADBC-SHIM' --exclude-dir=node_modules .
```

Checklist:

1. **`Dockerfile`, `adbc-driver` stage** — delete the `COPY packages/server/adbc-shim/`
   and the `RUN gcc … selftest` lines; change the `mv` so the downloaded driver keeps
   its real name (`/out/libadbc_driver_snowflake.so`). The stage itself stays: the
   pinned-digest download and fail-the-build posture predate the shim.
2. **`Dockerfile`, final stage** — `COPY --from=adbc-driver` only
   `libadbc_driver_snowflake.so`; drop `.real.so` from the `cp` and from the
   count-match (`shim` becomes the only count); delete the operator note block
   that documents `ADBC_RESULT_QUEUE_SIZE` / `ADBC_PREFETCH_CONCURRENCY`.
3. **`.github/workflows/build.yml`, docker smoke test 4a** — delete the
   `.real.so` assertion line and the `ADBC-SHIM` note above it; restore the ✓
   line's wording.
4. **`docs/configuration.md`** — delete the two `ADBC_*` rows.
5. **`packages/server/adbc-shim/`** — delete the directory (this README included).
6. **Deployments** — remove `ADBC_RESULT_QUEUE_SIZE` / `ADBC_PREFETCH_CONCURRENCY`
   from any deployment that set them; with the shim gone they are inert, not
   harmful, so this can trail.
7. **`RELEASE_NOTES.md`** — do not edit the stamped section; write a new
   `[Unreleased]` entry saying the bound is now provided by the extension or the
   driver and the variables are retired.

`grep -rn 'ADBC-SHIM'` returning nothing is the definition of done.

## Files

- `shim.c` — the shim. Exports `AdbcDriverInit` (and `AdbcDriverSnowflakeInit`,
  the name a driver manager may derive from the file name). Finds the real
  driver beside itself via `dladdr`; `ADBC_REAL_DRIVER` overrides the path.
  Logs one line at load — `[adbc-shim] wrapping <path> (adbc <version>);
  result_queue_size=… prefetch_concurrency=…` — and a line per rejected
  option; `ADBC_SHIM_DEBUG=1` adds a line per applied option.
- `selftest.c` — build-time check run in the Dockerfile's `adbc-driver` stage:
  `dlopen`s the shim as the driver manager would, calls `AdbcDriverInit`, and
  asserts the driver table is populated with the shim's `StatementNew` in place.
  No network, no credentials; a broken shim/driver pair fails the image build.
- `adbc.h` — the ADBC C header, vendored verbatim from
  [apache/arrow-adbc](https://github.com/apache/arrow-adbc/blob/main/c/include/arrow-adbc/adbc.h)
  (Apache-2.0; header retained). The `AdbcDriver` struct layout is the ABI the
  shim wraps, so it is the upstream definition and not a local copy of it.

## Verifying it in a running image

```
$ docker run --rm --entrypoint sh <image> -c \
    'ls /root/.duckdb/extensions/*/*/libadbc_driver_snowflake*.so'
…/libadbc_driver_snowflake.so        # the shim
…/libadbc_driver_snowflake.real.so   # the driver
```

At runtime the first Snowflake statement logs
`[adbc-shim] wrapping … result_queue_size=<value | (unset) | (invalid "…", ignored)> prefetch_concurrency=…`
on stderr — the line that says, per process, whether the bound is on. Values must be
positive integers; anything else is reported there and not applied, so an operator
typo such as `0` cannot print as if the bound were on. A driver that rejects an option logs `rejected by driver (status N)` and
the statement proceeds unbounded — the behaviour without the shim, never a
failed query.
