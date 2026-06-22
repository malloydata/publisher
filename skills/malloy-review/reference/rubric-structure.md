# Rubric: Structure

**Dimension:** `structure`
**Rules:** 5

Applies to the organization of `.malloy` files within a package. Skip a file for structural review if it's a `.malloynb` notebook (which has its own conventions) or a one-off sandbox script.

For every rule, the linked instruction-skill section is the canonical source for rationale and examples.

---

## S-01: Base vs joined source separation

- **Severity:** major (non-blocking) · **Category:** structure · LLM-judgment
- **Detection:** LLM heuristic, a source named after a base table (`customers`, `orders`) that includes cross-table joins, or a file with multiple sources where one clearly joins the others, is a candidate for splitting
- **Fix:** move the joined source into its own file; leave the base source clean
- **See:** `skill:malloy-model` § Base vs Joined Sources · `skill:malloy-model` § Joined Source File Template

---

## S-02: Sources should declare `primary_key:` when one exists

- **Severity:** minor (non-blocking) · **Category:** structure · machine-checkable
- **Why this isn't a correctness rule.** The Malloy compiler auto-synthesizes a UUID-based `__distinct_key` for any `join_one:` target without a declared PK (`packages/malloy/src/model/field_instance.ts:644-670`, `query_query.ts:946-951`), so symmetric aggregation is correct either way, see the C-07 entry in `rubric-correctness.md`'s "Rules we dropped" section. The `with` shortcut, which DOES require a declared PK, errors at compile time and surfaces as a diagnostic. The actual silent-correctness hazard ("declared PK isn't actually unique in the data") is `rubric-correctness.md` § C-12.
- **What this rule catches.** A discoverability / hygiene gap: when a source has a natural primary key, declaring it lets downstream code use the `with` shortcut, makes grain explicit in the model, and gives tooling a stable identifier per row. Treat as a recommendation, not a merge gate.
- **Detection:** for every `source:` declaration, check whether its body contains a `primary_key:` clause. Skip flagging when:
  - The source is a query-based / computed source (`source: x is t -> {...}` or `source: x is from(...)`) where grain is determined by the `group_by` columns, declaring a `primary_key:` on the result is fine but not required.
  - The source represents an event/log table or a denormalized analytical source where no natural PK exists. Both situations are legitimate; the LLM should recognize them and skip the finding.
- **Fix (when the source does have a natural PK):** declare it, `primary_key: <col>` inside `extend {}`. When there isn't a natural PK, leave it undeclared; if the absence is non-obvious, a one-line `#(doc)` on the source explaining the grain helps future readers.
- **See:** `skill:malloy-model` § Key Rules · `rubric-correctness.md` § C-12 (the related correctness rule that checks whether a declared PK is actually unique in the data) · `rubric-style.md` § Y-03 (`join_one:` style consistency, which is the other consequence of declared-vs-undeclared PKs)

---

## S-03: Joined tables defined before the source that references them

- **Severity:** blocker (blocking) · **Category:** structure · machine-checkable
- **Detection:** AST, within a single file, for each source reference inside a `join_*:` or `source is <name>`, verify the target source is declared earlier
- **Fix:** reorder source declarations so dependencies come first. If two sources reference each other, split into separate files with explicit `import` (cross-file declarations don't have this ordering constraint).
- **See:** `skill:gotchas-modeling` § Source Order, Define Joined Tables First

---

## S-04: Base sources curate their column surface

- **Severity:** major (non-blocking) · **Category:** structure · machine-checkable
- **Detection:** AST, for each `source: ... is conn.table(...)` declaration (a direct-table base source), check whether the source uses **any** of the three column-curation mechanisms below. The finding fires only when the source uses none of them, every column from the underlying table is implicitly public. Query-based sources and sources derived from other sources are exempt.
- **Fix:** pick whichever option matches the source's needs. Curation options, lightest to heaviest:
  1. `extend { except: a, b, c }`, drop a small named set of columns. Compatible with `rename:`. No experimental flag.
  2. `extend { accept: a, b, c }`, keep only a small named set. Compatible with `rename:`. No experimental flag.
  3. `##! experimental.access_modifiers` + `include { public: …, internal: …, private: … }`, full per-column visibility tiers. **Incompatible with `rename:`** and disallows measures/dimensions whose names shadow `internal:` columns. Use only when the visibility distinction matters (e.g., shared sources joined into multiple consumers).

  Most files only need option 1 or 2, for the small set of columns that shouldn't be public, prefer `except:` over the heavier `include {}` machinery. Reach for `include {}` only when per-column tiers are genuinely worth the constraints.
- **See:** `skill:gotchas-modeling` § Field Management, `extend {}` vs `include {}` Don't Compose · `skill:malloy-model` § Base Source Templates · `malloy-model/reference/access-modifiers.md`

---

## S-05: One file per table (base) / one per analytical domain (joined)

- **Severity:** minor (non-blocking) · **Category:** structure · machine-checkable
- **Detection:** count `source: X is conn.table(...)` base-source declarations per file. More than one → flag.
- **Fix:** split each base source into its own file named after the source
- **See:** `skill:malloy-model` § Base Source Templates (file convention is implicit; weak counterpart)

---

## Cross-cutting note

When a file violates both S-01 (base+joined mixed) and S-05 (multiple base sources), surface the combined observation as a **Cross-Cutting Theme** in the review output: "File layout suggests a restructure, see S-series findings for suggested splits."
