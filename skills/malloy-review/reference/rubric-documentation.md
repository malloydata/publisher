# Rubric: Documentation

**Dimension:** `docs`
**Rules:** 5

A field is not "complete" until it has its definition, a `#(doc)` string, and any rendering tags it needs. This rubric enforces that standard against any `.malloy` file that defines sources, dimensions, measures, or views.

For every rule, the linked instruction-skill section is the canonical source for rationale, examples, and the writing-style guidance that drives the rule. The rubric carries detection and fix metadata only.

---

## D-01: Public-surface fields must have `#(doc)`

- **Severity:** major (blocking) on public · minor (non-blocking) on internal · **Category:** docs · machine-checkable
- **What counts as a public-surface field**, every field a downstream consumer can pick:
  - **Raw columns listed under `public:` in an `include {}` block** (requires `##! experimental.access_modifiers`). ID columns count too, the team standard is "every public column has a `#(doc)`." See `skill:malloy-document` § Annotating Columns in Include (Experimental) for the canonical shape.
  - **Every `dimension:` and `measure:` declaration in `extend {}`**, these are public unless the source is fully internal-tagged.
  - Raw columns under `internal:` / `private:` are exempt (use D-01 internal severity if you want to flag missing docs as a soft nudge).
- **Detection:** for each public-surface field in scope, check whether the immediately preceding tag block contains a `#(doc)` tag. Missing → flag.
  - When access modifiers are in use (`##! experimental.access_modifiers`), missing `#(doc)` on any public-surface field is `major (blocking)`, the curated `include {}` is the contract, and an undocumented public column short-circuits the documentation discipline (see G-01).
  - Without access modifiers, default to `major (non-blocking)` for `dimension:` / `measure:` declarations because we can't tell which raw columns are public.
- **Fix:** add a one-line `#(doc)` above the field describing business meaning and units. For raw columns under `public:`, this is the only place to attach business documentation, there's no `dimension:` declaration to hang it on.
- **See:** `skill:malloy-document` § #(doc) Tag · `skill:malloy-document` § Annotating Columns in Include (Experimental)

---

## D-02: `#(doc)` strings avoid Malloy jargon

- **Severity:** minor (non-blocking) · **Category:** docs · LLM-judgment
- **Detection:** keyword heuristic first pass, flag `#(doc)` strings containing `aggregation`, `filterable`, `groupable`, `dimension`, `measure`, `aggregate`, `field`. Confirm with LLM judgment on the actual string.
- **Fix:** rewrite in business language, what is it, what are its units, what values does it take
- **See:** `skill:malloy-document` § Writing Doc Strings for Retrieval

---

## D-04: One tag per line

- **Severity:** minor (if-minor) · **Category:** docs · machine-checkable
- **Detection:** regex, a line containing two or more `#` tag openers outside quoted strings
- **Fix:** split onto separate lines
- **See:** `skill:gotchas-rendering` § One Tag Per Line

---

## D-06: Source-level documentation describes **when to use** the source

- **Severity:** major (blocking) on joined sources · minor (non-blocking) on base sources · **Category:** docs · LLM-judgment
- **Detection:** LLM judgment on whether the source-level `#(doc)` is substantive. A string that just echoes the table name (e.g., `#(doc) Customers`) is flagged.
- **Fix:** rewrite to describe grain (base) or analytical use (joined)
- **See:** `skill:malloy-document` § Source-Level Documentation

---

## D-07: Filtered measures document the filter intent

- **Severity:** major (blocking) · **Category:** docs · LLM-judgment
- **Detection:** for every `measure:` containing a `{ where: }` filter block, the `#(doc)` string must mention the filter criterion (heuristic: contains at least one word from the filter expression, or mentions `filter`/`only`/`excludes`/`includes`). LLM confirms.
- **Fix:** extend the doc to explain what the filter includes/excludes
- **See:** `skill:malloy-document` § Writing Doc Strings for Retrieval (filtered-measure intent is implicit; weak counterpart)

---

## A note on coverage metrics

The docs reviewer also emits an aggregate observation in the review's Cross-Cutting Themes when coverage drops:

- **Doc coverage** = (fields with `#(doc)`) / (all public fields) × 100

If it drops notably relative to the rest of the reviewed scope (e.g., one file at 40% when the rest are 90%+), surface it as a theme, not just per-field findings.
