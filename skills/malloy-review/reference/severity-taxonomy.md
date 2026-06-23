# Severity, Confidence, Categorization

The shared vocabulary every finding uses. Read this alongside any rubric file.

## A finding

Every finding emitted during a review has this shape (JSON):

```json
{
  "id": "C1",
  "severity": "critical",
  "category": "correctness-join",
  "blocking": "blocking",
  "file": "packages/x/customers.malloy",
  "line_range": [12, 12],
  "rule": "C-12 declared primary_key is not unique in the data",
  "current": "primary_key: customer_id (customer_id has duplicates per malloy_executeQuery check)",
  "expected": "customer_id is unique per row, or the source carries a where: that scopes to a uniquely-keyed subset",
  "suggested_fix": "Pick a different (or composite) PK that IS unique, OR add a source-level where: that makes customer_id unique within the filtered set, OR declare a #(filter) ... required annotation so consumers must supply the discriminating filter.",
  "confidence": 95,
  "evidence": "rubric-correctness C-12; pk_verified=false from SKILL.md step 3 malloy_executeQuery check",
  "source": "rule"
}
```

`source` is one of:
- `"rule"`, from a rubric rule applied during the review
- `"diagnostic"`, promoted from IDE diagnostics (see SKILL.md § Workflow step 2)
- `"judgment"`, LLM judgment, not tied to a specific rule

## Severity scale

Five labels, ordered by blast radius:

| Severity | What it means | Typical examples |
|---|---|---|
| `blocker` | Code will not compile, run, or produce defined behavior | Syntax error; unbacked reserved word; `run:` without `->` |
| `critical` | Code compiles but will return silently wrong numbers or bypass governance | Wrong `join_one` vs `join_many` declaration; missing `primary_key:` on a `join_one:` target; method-syntax violation on joined aggregates |
| `major` | Clear functional or usability problem, not silently wrong but the result is off | `having:` vs `where:` misuse; two aggregates in a chart view; `#(doc)` missing on a public measure |
| `minor` | Style or consistency issue that affects readability but not correctness | Tag-ordering wrong; one-tag-per-line violation; fixed scale baked into a measure definition |
| `nit` | Subjective suggestion | Business-language naming preference; praise-adjacent rewrites |

## Blocking axis

Independent of severity. Answers "should this hold up a merge?"

- `blocking`, cannot ship without fixing
- `non-blocking`, should fix but won't hold up a merge
- `if-minor`, block only if other work is already touching this area

Default mappings (overridable in `.malloy-review.local.md`):
- `blocker` + `critical` → `blocking`
- `major` → `blocking` for public-surface / `non-blocking` for internal
- `minor` → `if-minor`
- `nit` → `non-blocking`

## Category axis

A finding's category tells the reader *what kind of problem*, independent of severity:

| Category | Covers |
|---|---|
| `correctness-syntax` | Parse errors, type errors, unbacked reserved words, `is` vs `as`, missing colons, missing `->` |
| `correctness-join` | Cardinality declarations, primary-key hygiene, `join_one`/`join_many`/`join_cross` selection, `with`/`on` correctness |
| `correctness-aggregation` | Symmetric-aggregate locality, method-syntax for joined aggregates, `count()` vs `count(field)`, casts for string aggregates |
| `correctness-filter` | `where:` vs `having:`, `!= null` vs `is not null`, safe division, alternation-operator precedence |
| `correctness-type` | Date/time function vs property confusion, boolean-column quoting, `::number` cast misses |
| `queries` | Chart view multi-aggregate, `order_by` joined-field aliasing, trailing commas, inline measure definitions (DRY) |
| `docs` | `#(doc)` presence, doc-string quality (no jargon), tag ordering |
| `rendering` | Scale on measure def (no), `# big_value` missing `# label`, sparkline `# hidden`+`.sparkline=` pair, `# number=id` on years/IDs |
| `structure` | Base vs joined split, `primary_key:` declared, `include {}` curation on base sources, source-definition order |
| `governance` | Access modifiers explicit when `##! experimental.access_modifiers` is on, `public: *` softened to a documentation-discipline nudge |
| `style` | Reserved-word aliases, business-language naming |

## Confidence

**Confidence is the reviewer's per-finding judgment**, how sure are you that *this specific instance* is a real violation, not a false positive? It's not a property of the rule, and rubric files do not pre-assign it. Every finding carries a 0–100 score; the reviewer assigns it based on the strength of the evidence in the cited code. **Findings with confidence < 80 are dropped** before the review is assembled.

Use these as a starting point and adjust per finding:

| Evidence source | Starting confidence |
|---|---|
| IDE diagnostic, error | 95 |
| IDE diagnostic, warning | 85 |
| IDE diagnostic, information / hint | 75 |
| Live data check (e.g. `malloy_executeQuery` returns a definitive yes/no, like C-12 PK uniqueness) | 95 |
| Machine-checkable rubric rule matched against unambiguous code | 90–95 |
| Machine-checkable rubric rule matched but the context could be misread | 80–90 |
| LLM-judgment rubric rule with a clearly matching signal (e.g. integer-typed comparison against `ts.month`) | 80–90 |
| LLM-judgment rubric rule with a softer signal | 70–80 (often falls below threshold and is dropped) |
| LLM judgment outside any rubric rule, with `rule: "judgment"` | 75–85 |

What raises confidence: a live data check that returns the wrong answer; a regex / AST hit that's structurally unambiguous; a finding cross-supported by an IDE diagnostic.

What lowers confidence: the rule itself contradicts the Malloy compiler's own warning (e.g. C-01); the detection signal is real but could be deliberate (e.g. a `where:` filter that *looks* like a missing one); the file lacked IDE diagnostic coverage so the compiler-level signal isn't available.

When a diagnostic is the only evidence for a finding and re-reading the cited line shows the issue is gone (stale per-open-file diagnostic against a since-fixed file), drop it. Otherwise diagnostics are strong evidence, start above the threshold and usually pass.

## Default severity per rule family

The rubric files set the default severity per rule. Defaults (can be overridden in `.malloy-review.local.md`):

| Rule family | Default severity | Blocking |
|---|---|---|
| Compile-breaking syntax (parse errors, missing colons / arrows, `as` vs `is`, bare `join:`, reserved-word collisions, redefined query-source columns, trailing commas, etc.) | `blocker` | `blocking`, **handled exclusively by IDE diagnostic pre-pass; not in the rubrics** |
| Declared `primary_key:` is not actually unique in the data (C-12, verified by `malloy_executeQuery` in SKILL.md step 3) | `critical` | `blocking` |
| Chart view with >1 aggregate (Q-01, silent render bug) | `major` | `non-blocking` |
| `!= null` vs `is not null` (C-01), contradicts the Malloy compiler's own warning, see rule for context | `major` | `non-blocking` |
| Safe division / boolean-quote (C-03, C-05) | `major` | `blocking` |
| `#(doc)` missing on public field (D-01) | `major` | `blocking` (public) / `non-blocking` (internal) |
| One-tag-per-line (D-04) | `minor` | `if-minor` |
| Base vs joined separation violation (S-01) | `major` | `non-blocking` |
| `primary_key:` missing on a source (S-02) | `minor` | `non-blocking` |
| Inconsistent `join_one:` style across project (Y-03) | `nit` | `non-blocking` |
| Access modifier missing when flag is on (G-02) | `major` | `blocking` (governance) |
| `public: *` used (G-01, documentation discipline) | `minor` | `non-blocking` |
| Business-language naming suggestion (Y-02) | `nit` | `non-blocking` |

## ID conventions

Each finding gets a human-readable ID for cross-reference in the output. The scheme is **severity-letter for top issues, dimension-letter for everything else**:

- **Blockers** → `B#` (regardless of dimension; these get top billing)
- **Criticals** → `C#` (regardless of dimension; these get top billing)
- **Majors and minors** → per-dimension letter, numbered sequentially within the dimension:
  - `J#` correctness-join
  - `A#` correctness-aggregation
  - `T#` correctness-type (and correctness-syntax)
  - `F#` correctness-filter
  - `Q#` queries
  - `D#` docs
  - `R#` rendering
  - `S#` structure
  - `G#` governance
  - `Y#` style
- **Nits** → `N#` (regardless of dimension; aggregated under Suggested Follow-ups)

Severity is shown in the finding row itself (and in the JSON), so the dimension-letter is enough to identify a finding without conflating it with severity. Numbering restarts at 1 per prefix within a single review file. Cross-references in the Executive Summary / Cross-Cutting Themes use these IDs so a reader can jump to the detailed finding.
