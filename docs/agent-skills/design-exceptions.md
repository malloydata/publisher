<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Design exceptions

This file records deliberate divergences from [design principles](./design-principles.md).
Each entry names the divergence, the principle it is in tension with, and the justification.

Treat this as a contract for reviews. Read it alongside the principles doc, and treat anything
not listed here as a bug, not a deliberate decision. When the rationale for an exception no
longer holds (the tool or system that justified it is replaced), update or remove the entry.

## Active exceptions

### Contract rules precede Parameters and Response, against the template's section numbering

Tension: the [tool description template](./tool-description-template.md) numbers its five sections
When to use, Parameters, Response, Contract rules, Worked examples, and tools otherwise follow that
order literally.

Decision: Contract rules are placed immediately after the opening paragraph, ahead of Parameters and
Response. The section numbering stays as the list of what a description must contain; it is not the
emission order.

Why: some MCP clients truncate a tool description, and a tail cut removes whatever was placed last.
Under the literal numbering that is Contract rules and Worked examples, which is the worst possible
outcome: an agent that loses the worked example still calls the tool correctly, while one that loses
the invariants cannot, and nothing signals that anything was dropped. `malloy_getContext` was
observed arriving cut off mid-sentence at 2271 characters, with `malloy_searchDocs` carrying the same
inverted ordering. This applies to every tool, so it is a standing rule rather than a per-tool
divergence, and `server.protocol.spec.ts` enforces it over the real protocol.

### Chart-annotation placement rules duplicated across malloy-queries and malloy-charts

Tension: Principle 1 and the prevention-versus-reference distinction discourage duplicating the
same content across two skills.

Decision: the placement rules for chart annotations (a chart tag goes before `run:`, never
inside `{ }`, and so on) live in `malloy-queries` as prevention content, while the chart-type
catalog and tag properties live in `malloy-charts` as reference content. The `malloy-queries`
"when a query fails" table also keeps the pointer for the "field is a bar chart, but is not a
repeated record" error.

Why: `malloy-queries` loads before any query is written, and a misplaced chart annotation is
one of the most common compile errors, dangerous because it often surfaces as a confusing
semantic error rather than a syntax error. `malloy-charts` loads only when picking a
visualization, which is too late to catch a mistake already encoded in the query body. Treating
placement as prevention (loaded early) and selection as reference (on-demand) costs a small
amount of duplication and materially reduces first-pass error rates.

## Prospective exceptions

These divergences are expected to apply once the corresponding tools land in the open-source
surface. They are recorded now so the rationale is not lost when the tools arrive. Promote each
to an active exception (or discard it) when the tool exists.

### A retrieval tool description explaining what its parameters do

Tension: Principle 3 says tools should not carry workflow instructions; the workflow belongs in
skills.

Decision (when a retrieval tool ships): the description states what each parameter does and what
the response groups by, with a request example per shape. It does NOT prescribe a call sequence.
For `malloy_getContext` that means saying a query returns whole source cards with their matched
fields nested, so one call answers, and that the listing tiers exist for finding an environment
or package name rather than as steps to walk. Refinement, retry, and gap-handling workflow stays
in the analysis skill; `search_text` phrasing stays in `malloy-phrase-detection`.

Why: what a response groups by is API behaviour, not optional workflow advice. An earlier version
of this exception blessed a two-phase "source discovery then entity drill-down" summary, and that
turned a capability into a prescribed sequence: callers spent an extra call per question fetching
a source list the query tier already returns. The examples are still the most efficient way to
disambiguate the parameter shape, and the skill still owns the reasoning (when to retry, when to
widen, when to flag a gap).

### malloy-analysis-report carrying cell-shape examples

Tension: Principle 3 and the lean-skills norm say structure already on a tool description should
not be duplicated in a skill.

Decision (when a report-authoring tool ships): `malloy-analysis-report` keeps per-cell shape examples
(markdown versus Malloy cell variants) and the chart-annotation walk-throughs for the report
patterns it recommends.

Why: a report tool's cell payload is a JSON-encoded string of an array, so the tool's own worked
example necessarily double-escapes its contents and obscures the per-cell shape. Showing the
per-cell objects unwrapped in the skill is the most efficient way for the agent to internalize
the shape. Chart selection still defers to `malloy-charts`; the walk-throughs here are reusable
report templates, not a chart-type catalog.

### A topic index for docs search: malloy-patterns (resolved)

Tension: Principle 1's under-20% rule for reference content suggests docs lookups should be
on-demand via a tool, not pre-loaded as a skill.

Resolution: now that the docs-search tool has shipped (`malloy_searchDocs`), a single small
topic-index skill, `malloy-patterns`, holds the table of valid topic strings. It loads only when
the agent already knows it needs to search docs, giving it a vocabulary of topics so it does not
waste calls on poorly phrased queries; it carries no doc content itself. The earlier
`malloy-docs-index` skill was a near-duplicate of `malloy-patterns` and has been removed, so there
is one topic index rather than two.
