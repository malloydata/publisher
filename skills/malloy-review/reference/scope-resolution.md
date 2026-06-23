# Scope Resolution

Every `/malloy-review` invocation must resolve to a **bounded, explicit scope** before any reviewer runs. This file is the single source of truth for how that resolution happens and why.

## Why scope is always explicit

Malloy repos commonly contain **multiple folders that each target different database connections**. A folder that compiles cleanly against connection A may fail in the context of connection B. Running a review blindly across the whole repo will:

1. Produce spurious "cannot resolve column" findings in folders that can't compile without their connection.
2. Under-count diagnostics because the IDE only has diagnostics for files it's opened against the right connection.
3. Mislead the user about coverage, the report will claim to have reviewed N files when some of them are in contexts the reviewer couldn't understand.

The skill never walks above a scoped root. The review output always echoes the resolved scope so the user can verify coverage at a glance.

## Resolution order (first match wins)

1. **Positional file argument** (`/malloy-review path/to/file.malloy`), file mode on that file. If the file doesn't exist or isn't `.malloy`, stop.
2. **Positional directory argument** (`/malloy-review path/to/folder/`), audit mode on that folder. Walks only `.malloy` files within the folder (recursive, but not above it).
3. **`--pr <n>`**, PR mode. Fetch diff via `gh pr diff <n>`, then intersect with the scope resolved by the rules above (if any). If no other scope was given, the intersected set becomes the scope. See SKILL.md § Mode notes for the PR workflow.
4. **No argument, CWD is inside a `publisher.json` package**, walk up from CWD until a directory containing `publisher.json` is found. That directory is the package root and the default scope. Review all `.malloy` files at or below it.
5. **No argument, CWD contains `.malloy` files but no `publisher.json`**, scope is CWD (not recursive above it).
6. **No argument, CWD is a repo root with multiple `publisher.json` files at child level**, list the packages and ask the user to pick one (or multiple, explicitly). Never auto-fan-out across packages.
7. **No argument, CWD has no `.malloy` files anywhere nearby**, stop with a helpful error: "No `.malloy` files found at or below this directory. Pass a path or cd into a Malloy project."

## Detecting `publisher.json` packages

A `publisher.json` file marks a Malloy package boundary. The file doesn't need to be parsed; its presence is the signal.

```bash
# Walking up from CWD:
find_package_root() {
  local dir="$PWD"
  while [[ "$dir" != "/" ]]; do
    [[ -f "$dir/publisher.json" ]] && echo "$dir" && return
    dir="$(dirname "$dir")"
  done
  return 1
}

# Discovering packages below a root (for multi-package disambiguation):
find "$ROOT" -name publisher.json -not -path '*/node_modules/*' -not -path '*/.git/*'
```

## Multi-package disambiguation

When scope resolution hits rule 7 above, present the packages as a lettered list with a one-line summary each:

```
Multiple Malloy packages found in this repo. Which would you like me to review?

  (A) packages/finance         , 23 .malloy files (.malloy) · 3,412 LOC
  (B) packages/marketing       , 11 .malloy files · 1,890 LOC
  (C) packages/shared-joins    ,  4 .malloy files · 402 LOC

You can pick one, several (e.g., "A and C"), or say "all" if you really want cross-package, but note that each package may target a different database connection and findings may conflict.
```

Do not default to "all". Wait for the user to pick.

## PR-diff intersection

When `--pr <n>` is used:

1. Fetch diff: `gh pr diff <n> --name-only` → list of changed files.
2. Filter to `.malloy` files only.
3. If a scope was also provided (positional path), intersect: `changed_files ∩ scope_files`. Files outside the scope are not reviewed even if they appear in the diff.
4. If the intersection is empty, stop with a message: "The PR changes no files in the scope you selected. Either drop the scope filter or target a different PR."
5. The output reports both the PR number **and** the effective scope after intersection.

## Emitting scope in the output

The `## Scope` section of the review file always lists:

- **Reviewed:** the exact list of folders/files/globs that drove the walk.
- **Package(s):** the `publisher.json` directories covered.
- **Excluded:** anything skipped (generated dirs, fixtures, files outside the positional path, files outside the intersected PR diff).
- **File count and LOC total.**

The user should be able to read the Scope section and know exactly what was and wasn't reviewed.

## Excluded-by-default paths

These are skipped unless the user passes a positional path that explicitly includes them:

- Anything under `node_modules/`, `.git/`, `.cache/`, `dist/`, `build/`
- Files named `*.fixture.malloy` or under a `fixtures/` or `__fixtures__/` directory
- Files under a `tests/` directory where the filename matches `*_test.malloy` or `test_*.malloy`
- Symlinks (follow cautiously or skip, do not cross filesystem boundaries)

These exclusions are always reported in the `## Scope` block's **Excluded** line so the user can see what was skipped.

## Stopping the user politely

If the skill can't resolve scope (nothing found, ambiguous, wrong file type), stop *before* reading any `.malloy` files. Don't guess, don't default to "the whole repo". The skill should feel conservative: it does exactly what the user asked for and nothing more.

Good stop messages name what was tried and offer a concrete fix:

> I couldn't find any `.malloy` files at or below `/path/to/cwd`. Run from inside a Malloy package or pass a file or folder as an argument (`/malloy-review path/to/models`).

> I found `.malloy` files under three different packages in this repo (`packages/finance`, `packages/marketing`, `packages/shared-joins`). Each may target a different database connection, so I won't fan out automatically. Pick one with `/malloy-review packages/finance/`.
