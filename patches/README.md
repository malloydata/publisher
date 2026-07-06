# Dependency patches

Local overrides applied to installed npm dependencies via bun's
[`patchedDependencies`](https://bun.sh/docs/install/patch). The `package.json`
`patchedDependencies` map points each dependency version at its patch file here;
`bun install` reapplies them automatically (in local dev and in the Docker image
build — see the `COPY patches/` steps in `Dockerfile`).

---

## `@malloydata/malloy@0.0.421` — persist-source `getSQL()` uses `finalize=false`

**Temporary.** Tracks upstream fix **[malloydata/malloy#2964](https://github.com/malloydata/malloy/pull/2964)**.
Remove this patch as soon as that fix ships in a released `@malloydata/malloy`
and the dependency is bumped past it (see teardown below).

### What it changes

`PersistSource.getSQL()` compiles the source's inner query with `finalize=false`
(one argument added to the two `queryModel.compileQuery(...)` calls in
`dist/api/foundation/core.js`), so it returns the **bare source SELECT** instead
of the dialect's runnable / result-serialized form.

### Why

`getSQL()` feeds two build-time uses (see `packages/server/src/service/build_plan.ts`
`computeBuildId` + the plan `sql` field, and `materialization_service.ts`
`buildOneSource`'s `CREATE TABLE AS`). With the default `finalize=true`, Malloy
appends the dialect's `sqlFinalStage` whenever `Dialect.hasFinalStage` is true.
**Postgres is the only such dialect**, and its final stage wraps the result in
`SELECT row_to_json(finalStage) as row ...`. That wrapper caused two
Postgres-only failures:

1. `CREATE TABLE AS (getSQL())` materialized a **single JSON `row` column**
   instead of the real columns (BigQuery, `hasFinalStage=false`, was unaffected
   and worked).
2. The source's BuildID (`mkBuildID` over this SQL) no longer matched the
   **serve-time** manifest lookup, which resolves a persist source from the bare
   (unfinalized) SELECT — so queries fell through to a base-table scan instead of
   routing to the materialized table.

Compiling with `finalize=false` makes both the physical table (real columns) and
the build-time BuildID agree with the serve path. It is a **no-op** for dialects
without a final stage (BigQuery, duckdb, etc.), so it is safe across connectors.

Root-cause detail lives in the service repo:
`docs/handoffs/publisher-serve-time-manifest-binding.md`.

### How to remove it (once #2964 is released)

1. **Confirm the fix is in the target release.** Pick the first published
   `@malloydata/malloy` version that contains #2964 and verify its shipped dist
   already compiles `getSQL()` with `finalize=false`:

   ```bash
   npm view @malloydata/malloy@<version> version
   # after bumping (step 2), spot-check the installed file:
   grep -n "compileQuery(sd.query, options, false)" \
     node_modules/@malloydata/malloy/dist/api/foundation/core.js
   ```

2. **Bump Malloy** (updates every `@malloydata/*` dep + resolutions, reinstalls,
   syncs DuckDB):

   ```bash
   bun run upgrade-malloy <version>
   ```

3. **Delete the patch and its registration.** Remove this file's sibling
   `@malloydata%2Fmalloy@0.0.421.patch`, and delete the matching entry from the
   `patchedDependencies` map in `package.json`:

   ```jsonc
   // package.json — remove this block entirely
   "patchedDependencies": {
     "@malloydata/malloy@0.0.421": "patches/@malloydata%2Fmalloy@0.0.421.patch"
   }
   ```

   (The version key is exact, so once step 2 bumps the version the patch no
   longer matches anyway — but leaving a dangling entry makes `bun install` fail,
   so remove it in the same change.)

4. **Revert the Dockerfile `patches/` copies** if this `patches/` directory
   becomes empty. Two `COPY patches/ ...` lines were added for this patch:
   - builder stage, just before the first `bun install`
   - final stage, just after `COPY --from=builder .../package.json .../bun.lock`

   `COPY` of an empty/absent directory fails, so drop both lines when the last
   patch is gone. Keep them if any other patch remains.

5. **Reinstall and confirm clean:**

   ```bash
   bun install
   git status          # bun.lock's patchedDependencies block should be gone
   ```
