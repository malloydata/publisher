# Rubric: Governance & Access

**Dimension:** `governance`
**Rules:** 2

Governance findings cover access modifiers when the experimental feature is in use. Source-level `where:` filters and `private:` choices are **deliberate modeling decisions**, the reviewer trusts the modeler on those, not the rubric. If something looks unusual, surface it as a "Question for the Author," not a finding.

For every rule, the linked instruction-skill section is the canonical source for rationale and examples.

---

## G-01: Enumerate `public:` columns explicitly with `#(doc)`

- **Severity:** minor (non-blocking) · **Category:** governance · machine-checkable
- **Detection:** regex for `public:\s*\*` inside an `include {}` block
- **Fix:** enumerate public columns explicitly, each with a `#(doc)` tag and any necessary rendering tags. If you really do want every column public, list each one, the per-column `#(doc)` is the point.
- **See:** `skill:malloy-document` § Annotating Columns in Include (Experimental) · `malloy-model/reference/access-modifiers.md`

The reason `public: *` is discouraged is **documentation discipline**, not access semantics. With `public: *`, none of the wildcard-included columns get individual `#(doc)` tags, which short-circuits AI discoverability. The team standard is "every public column has a `#(doc)` tag." If a modeler genuinely wants everything public on a small curated source, that's their call, the rule raises awareness (`minor`/`non-blocking`), it doesn't block.

---

## G-02: Access modifiers are explicit when `##! experimental.access_modifiers` is on

- **Severity:** major (blocking) · **Category:** governance · LLM-judgment (cross-file)
- **Detection:** if a file has `##! experimental.access_modifiers`, cross-reference columns in `include {}` against the source's full column set. Read the model with `malloy_modelGetText` or `malloy_packageGet` to learn which columns the underlying table exposes (or sample the data with `malloy_executeQuery`: `run: <source> -> { select: * limit: 1 }`). Any column missing from the block is a finding.
- **Fix:** add every column to `include {}` as `public:` (with `#(doc)`) or `internal:`; confirm `private:` with the user before marking
- **See:** `skill:malloy-document` § Annotating Columns in Include (Experimental) · `malloy-model/reference/access-modifiers.md`

---

## What this rubric does NOT flag

- **Source-level `where:` clauses.** Encapsulating filters at the source level is a core feature of semantic modeling, it lets callers query the source without knowing the filter details. The rubric does not flag this as a hazard. If a reviewer thinks a specific source-level `where:` looks unusual (e.g., a new scoping filter introduced in a PR), surface that as a "Question for the Author" in the output, not as a rubric finding.
- **`private:` choices on PII-adjacent columns.** Marking email/phone/address as `private:` is the modeler's deliberate choice. The reviewer doesn't second-guess it. Same fallback applies, if it looks unusual, ask in the questions section.

These are *modeling features*, not *governance violations*. The rubric should catch real mistakes, not impose style preferences on deliberate access-modifier choices.
