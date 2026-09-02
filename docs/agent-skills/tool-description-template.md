<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Tool description template

A tool description is the interface a model reads to decide when and how to call a tool (see
Principle 2 in [design principles](./design-principles.md)). Every tool description follows
the same five-section structure so the surface stays consistent and an agent can rely on
finding the same information in the same place.

The description is written for a model, not a human reader. It is unambiguous about what the
tool does, when to call it, when not to call it, and what comes back.

## The five sections

1. **When to use.** The trigger conditions, and just as important, when not to call the tool.
   State the cases that cause the most misuse. Where the response shape is not obvious from
   the parameters, say what a call groups by, in two or three sentences, so the response is
   interpretable on first read. Describe what the parameters do; do NOT prescribe a call
   sequence, which turns a capability into a mandatory extra call. Leave the reasoning (when
   to retry, when to widen) to the skills.

2. **Parameters.** Each input, its type, whether it is required, and the meaning of the value.
   Describe what good input looks like per field. For typed or enumerated parameters, list the
   allowed values and what each selects. Note any cross-field requirements (parameter A is
   required when parameter B has a given shape).

3. **Response.** The shape that comes back, field by field, in the vocabulary of the tool's
   single domain. Call out which response fields are meant to pass into another tool verbatim
   (the composability contract). Note what gets omitted or capped, so the agent does not read a
   truncated listing as a complete catalog.

4. **Contract rules.** The invariants the agent must respect: use field paths verbatim, never
   invent entities, only combine results from compatible scopes, do not mix call modes. These
   are the rules that prevent the common, hard-to-self-correct errors.

5. **Worked examples.** One concrete request (and where it clarifies, the response) per call
   mode. A JSON example is the most efficient way to disambiguate parameter shape for a model.
   Keep it minimal and real.

## Ordering: contract rules must survive truncation

The five sections above are listed in the order an author thinks about them, **not** the order they
should appear in. Some MCP clients truncate a tool description, and a tail cut removes whatever the
description put last. Numbered as written, that is Contract rules and Worked examples: exactly the
invariants an agent cannot self-correct without, and no signal that anything was dropped.

`malloy_getContext` was observed arriving cut off mid-sentence at 2271 characters, and the same
inverted ordering was present in `malloy_searchDocs`. So:

- **Put Contract rules immediately after the opening paragraph**, ahead of Parameters, Response, and
  Worked examples. Losing the worked example still leaves a callable tool; losing the invariants does
  not, and the agent cannot tell.
- **Keep a description short enough that truncation is unlikely at all.** No portable number exists
  (the cap belongs to the client and is not published), so `server.protocol.spec.ts` enforces a
  budget as a regrowth guard, not as a guarantee. When a description approaches it, move the
  long-form narrative into a skill, which no cap applies to.
- Prefer merging over adding. `malloy_getContext` carried a "Progressive discovery" section and a
  "Parameters" section that stated the same call levels twice; merging them cut 16% with no loss.

`server.protocol.spec.ts` pins both rules over the real protocol, so a description that regrows or
reverts the ordering fails the build.

## Trimming the template

The template is a default, not a straitjacket. A tool may drop a section when that section
would be padding, and the divergence is recorded in [design exceptions](./design-exceptions.md).
For example, a tool whose response is a fixed acknowledgment with no agent-actionable fields
can omit the Response section. The bar for dropping a section is that it carries no information
the agent can act on; the other sections still earn their place.

The Contract-rules section is sometimes called Critical rules. Use one name consistently
within a tool.

## Annotated example

The retrieval tool below shows all five sections. It is a strong reference because its response
shape is not obvious from its parameters, so the description has to say what a call groups by for
the response to read clearly, while still leaving the workflow reasoning to skills. The example
also carries a "How targets and scopes work" heading, which is the documented divergence for a
retrieval tool (see the [design exceptions](./design-exceptions.md)), not a sixth required
section. Note what it does not do: it never tells the caller which call to make next.

```
Retrieve relevant Malloy entities (sources, dimensions, measures, views, dimensional values)
by matching typed search targets against indexed semantic-model data.

## When to use
- Before writing any Malloy query, and whenever the conversation shifts to a new topic,
  source, or set of entities, to ground the next step in what is actually in the model.
- Do NOT guess environment or package names in scopes. Specify them only when you know them.

## How targets and scopes work
- Entity targets find their sources. A dimension, measure, view or dimensional_value target
  with search_text matches fields anywhere in scope, and the response groups the matches by
  source. Typically this is the whole search: describe the fields the question needs, and the
  cards that come back say which sources hold them. A source scope is optional.
- source targets find or list sources, not fields. With search_text they match sources by
  subject; without it they list the catalog. Entity and source targets can be combined.
- scopes narrows any call. Scoped to one source, the call returns that source's full metadata.

## Parameters
search_targets (required): list of typed targets. Each has a target_type (source, dimension,
  measure, view, dimensional_value) and a search_text describing what to match (null returns
  the most-used items).
scopes (optional): narrows the search to an environment / package / model_path / source.
filter_params (optional): values for sources that declare #(filter) annotations.

## Response
sources: matched sources, each with source_info (a resource_id plus optional docs, summary,
  filter_params), source-level scores, and a list of entities sorted by relevance. The
  resource_id fields are the scoping parameters of the query tool 1:1; pass them through
  verbatim. Null/empty fields are stripped; listings are capped, so a listing is not a
  complete catalog.

## Contract rules
- Use exact field paths verbatim; join-namespace prefixes are part of the name, never strip
  them.
- Only combine entities from calls with identical scope.
- Never invent entities; only use what the response returned.

## Worked examples
Entity targets, no scopes (the response says which sources hold these fields):
{ "search_targets": [
    { "target_type": "measure", "search_text": "the rate at which customers leave" },
    { "target_type": "dimension", "search_text": "the city where the subscriber lives" }
  ] }

Scoped to one source:
{ "search_targets": [ { "target_type": "measure", "search_text": "customer churn rate" } ],
  "scopes": [ { "environment": "demo", "package": "saas", "model_path": "subs.malloy", "source": "subscriptions" } ] }
```

Notice what the description does and does not carry. It says what a call groups by (without
which the response is inscrutable) and it pins the composability contract (resource_id maps 1:1
to the query tool's scope). It does not teach when to retry, when to widen the search, how to
phrase a good search_text, or which call to make next. That reasoning lives in skills.
