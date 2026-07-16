---
name: malloy-review
description: Malloy semantic-model code review. Invoke when the user asks to review, audit, or critique a `.malloy` file, a folder of Malloy models, or a GitHub PR that touches Malloy. Enforces project modeling standards and emits a navigable review file.
---

# Malloy Code Review

Single-pass code reviewer for `.malloy` files. The deliverable is one Markdown file in the canonical shape (see `reference/output-template.md`). Findings cite the project's standards file (e.g., `CLAUDE.md`) where it applies; otherwise they cite rubric IDs (`reference/rubric-*.md`).

> **Tool names** are written bare here — `get_context`, `execute_query`, `search_malloy_docs`. The exact prefixed name depends on the host surface; match each against the tools you actually have.

## Before you start

Read these in order:
1. **Project standards**: whatever conventions exist in your host environment (`CLAUDE.md`, `AGENTS.md`, etc.). Treat as the higher-priority source of truth; rubric rules defer to it where they overlap.
2. **`reference/severity-taxonomy.md`**: the vocabulary every finding uses.
3. **`reference/rubric-*.md`**: the seven dimension rubrics. Don't re-read for every review; load them on demand based on what's in scope.
4. **`reference/output-template.md`**: the shape of the review file you produce.
5. **`.malloy-review.local.md`** in the scope folder, if present, project-specific severity overrides or extra rules.

Make sure the Malloy MCP tools are configured before running: this skill uses `execute_query` for data checks and `search_malloy_docs` for verifying Malloy capabilities. Both are optional; the review degrades gracefully if either is unavailable.

## Inputs

```
/malloy-review [<path>] [--pr <n>] [--out <file>] [--comment]
```

| Argument | Effect |
|---|---|
| (no arg) | Auto-detect scope per `reference/scope-resolution.md` |
| `<path>` | Review that file or directory |
| `--pr <n>` | Review the `.malloy` files changed in GitHub PR `<n>` (see PR mode below) |
| `--out <file>` | Write review to this path. Default is `./malloy-review-<YYYYMMDD-HHMMSS>.md` |
| `--comment` | PR mode only: post the review as a PR comment via `gh pr comment` |

If scope is ambiguous (multiple packages, wrong file type, empty result), **stop and ask**: don't guess. See `reference/scope-resolution.md` for the rules.

## Workflow

```
resolve scope → read files → verify PKs → apply rubrics → write output
```

### 1. Resolve scope
Per `reference/scope-resolution.md`. Echo the resolved scope to the user before doing anything expensive. Multi-package repos → present packages as A/B/C and let the user pick. **Never auto-fan-out across packages**: different packages may target different database connections.

### 2. Read files
For each in-scope `.malloy` file, read the full content. As you read, track each source's `primary_key:` and its `join_one`/`join_many`/`join_cross` targets, you'll use this for the PK uniqueness check (C-12) and join-style consistency (Y-03).

If `mcp__ide__getDiagnostics` is available, call it on the in-scope files and promote each diagnostic to a finding: errors → blocker (confidence 95), warnings → major (confidence 85), info/hint → minor (confidence 75), all with `source: "diagnostic"`. Skip rubric rules these already cover. If unavailable, skip, the LLM rubric pass still runs.

### 3. PK data verification (if `execute_query` is available)
For every source with a declared `primary_key:`, run a uniqueness check and store the result on `source_index.<src>.pk_verified`:

```malloy
run: <source> -> {
  aggregate:
    rows is count()
    distinct_pk is count(<pk_col>)
}
```

| Result | `pk_verified` |
|---|---|
| `rows == distinct_pk` | `true` |
| `rows != distinct_pk` | `false` |
| `execute_query` is unavailable for the scope | `"skipped"` |
| The query fails (source unreachable, connection error, etc.) | `"error"` |

This step only collects evidence, the emit decision and fix template live in `reference/rubric-correctness.md` § C-12. When any source is `"skipped"` or `"error"`, note the coverage gap in the output's Scope section.

### 4. Apply rubrics
Score the in-scope files against each applicable rubric. **You don't need to read all seven**: pick by content:

| Rubric | Read when |
|---|---|
| `rubric-correctness.md` | Always |
| `rubric-documentation.md` | Always (every source/measure/dimension should be documented) |
| `rubric-style.md` | Always |
| `rubric-structure.md` | Always |
| `rubric-queries.md` | Any in-scope file has `view:` or `run:` |
| `rubric-rendering.md` | Any in-scope file has a `#` rendering tag |
| `rubric-governance.md` | Any in-scope file has `##! experimental.access_modifiers` or `include {}` blocks |

Findings use the canonical shape from `reference/severity-taxonomy.md`:

```json
{
  "id": "C1",
  "severity": "critical",
  "category": "correctness-join",
  "file": "packages/x/customers.malloy",
  "line_range": [12, 12],
  "rule": "C-12 declared primary_key is not unique in the data",
  "current": "primary_key: customer_id (customer_id has duplicates per execute_query check)",
  "expected": "customer_id is unique per row, or the source carries a where: that scopes to a uniquely-keyed subset",
  "suggested_fix": "...",
  "confidence": 95,
  "source": "rule"
}
```

**Drop findings with confidence < 80.** The output should feel curated, not exhaustive.

### 5. Look for cross-cutting themes
After per-file findings, scan for systemic patterns. **This is the highest-value output.** Examples that emerged from real reviews:
- N base sources all missing `include {}` curation
- M files use raw `conn.sql()` where Malloy-native patterns exist
- Canonical naming violations across N files (`total_revenue` vs `net_revenue`, etc.)
- "Junk-drawer" files with multiple unrelated sources

Promote these to a **Cross-Cutting Themes** section in the output. They are usually more actionable than per-line findings, collapse 3+ findings of the same rule into one theme with the file list, and emit individual findings only for the top 2–3 worst offenders.

### 6. Assemble output
Apply `reference/output-template.md`. Section order is fixed (skip if empty):

1. Header (scope, file count, LOC, timestamp)
2. Scope (paths reviewed)
3. Executive Summary (recommendation + top 1–3 risks)
4. Coverage & Risk Map (file table; skip if scope is one file)
5. Cross-Cutting Themes
6. Top Issues (blockers + criticals)
7. Detailed Findings (collapsed `<details>` per file)
8. Questions for the Author (≤5 bullets)
9. Positive Notes (only if real)
10. Suggested Follow-ups (aggregated minor/nit findings)
11. Suggested Split (only when scope is >2000 LOC or >30 files)

### 7. Write the file
Default `./malloy-review-<YYYYMMDD-HHMMSS>.md`. Tell the user the path and the top 1–3 issues inline. End with an offer to start fixing, most reviews are the start of iterative work, not a static report.

## Scaling notes

| Scope | Behavior |
|---|---|
| ≤5 files / ≤500 LOC | Trim Coverage Map and Cross-Cutting sections, likely nothing to surface. Skip the Suggested Split section. |
| 6–20 files / 500–2000 LOC | The canonical workflow above. Include all sections that have content. |
| >20 files / >2000 LOC | Add a mandatory Suggested Split section. Risk-tier files into DEEP / SAMPLE / SKIM (DEEP = top ~25% by content signals: joins, access modifiers, source-level `where:`, public surface). Cap per-file finding count at ~12 per dimension; promote overflow into Cross-Cutting Themes. Blocker, critical, and diagnostic findings never count against the cap. |

## Mode notes

**Single-file mode** (user passes one `.malloy` file): trim the template hard. Drop the Coverage Map, Cross-Cutting Themes, and Suggested Split sections. Lean conversational; write the file AND print the Summary + Findings inline so the user doesn't need to open the file for a small review. End with a fix offer ("Want me to fix B1?").

**Audit mode** (user passes a directory or invokes inside a `publisher.json` package): every file is in play. Coverage Map matters more, the user needs to know what was deep-reviewed vs. skimmed. For unfamiliar packages, consider running just the source-level summary first, presenting it, and asking whether to proceed with full review.

**PR mode** (`--pr <n>`):
1. `gh pr view <n> --json state,isDraft,title,baseRefName,headRefName,url,author` and `gh pr diff <n> --name-only`. Stop if the PR is closed or has no `.malloy` changes.
2. Filter changed files to `.malloy` and intersect with any path argument, that's the scope.
3. Fetch full file contents on the PR's head (via `gh pr checkout <n>` if the working tree is clean, otherwise via `gh api repos/<owner>/<repo>/contents/<path>?ref=<headRef>`). Don't clobber working state without asking.
4. Run the workflow above. PR header: `# Malloy Model Review, PR #<n>: <title>` with branch/package/LOC/url metadata.
5. With `--comment`, post via `gh pr comment <n> -F <file>` (cap at GitHub's 65,536-char limit; if larger, post the exec summary + top issues and reference the local file). Lead the comment body with `<!-- malloy-review: <timestamp> -->` so re-runs can detect prior reviews. **Never post without `--comment`.**

## What this skill does NOT do

- **Does not modify any `.malloy` files.** Output is the Markdown review only.
- **Does not post PR comments without `--comment`.**
- **Does not walk above the resolved scope.** Even if a finding would benefit from cross-scope context, scope is fixed once resolved.
- **Does not guess at multi-package disambiguation.** Always asks.
- **Does not flag modeling features as hazards.** Source-level `where:` clauses and deliberate `private:` choices are part of how Malloy models work. If something looks unusual, surface it as a "Question for the Author," not a finding.
