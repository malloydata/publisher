# Tool description template

A tool description is the interface a model reads to decide when and how to call a tool (see
Principle 2 in [design principles](./design-principles.md)). Every tool description follows
the same five-section structure so the surface stays consistent and an agent can rely on
finding the same information in the same place.

The description is written for a model, not a human reader. It is unambiguous about what the
tool does, when to call it, when not to call it, and what comes back.

## The five sections

1. **When to use.** The trigger conditions, and just as important, when not to call the tool.
   State the cases that cause the most misuse. If the tool participates in a multi-call
   pattern (a discovery call followed by a drill-down call, for instance), name the pattern
   here in two or three sentences so the response is interpretable on first read. Leave the
   reasoning (when to retry, when to widen) to the skills.

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

## Trimming the template

The template is a default, not a straitjacket. A tool may drop a section when that section
would be padding, and the divergence is recorded in [design exceptions](./design-exceptions.md).
For example, a tool whose response is a fixed acknowledgment with no agent-actionable fields
can omit the Response section. The bar for dropping a section is that it carries no information
the agent can act on; the other sections still earn their place.

The Contract-rules section is sometimes called Critical rules. Use one name consistently
within a tool.

## Annotated example

The retrieval tool below shows all five sections. It is a strong reference because its two-call
pattern is tightly coupled to its response shape, so the description has to frame the pattern
enough that the response reads clearly, while still leaving the workflow reasoning to skills. The
example also carries a "Call modes" heading, which is the documented divergence for a two-phase
retrieval tool (see the [design exceptions](./design-exceptions.md)), not a sixth required section.

```
Retrieve relevant Malloy entities (sources, dimensions, measures, views, dimensional values)
by matching typed search targets against indexed semantic-model data.

## When to use
- Before writing any Malloy query, and whenever the conversation shifts to a new topic,
  source, or set of entities, to ground the next step in what is actually in the model.
- Do NOT guess environment or package names in scopes. Specify them only when you know them.

## Call modes (two-phase pattern)
1. Source discovery: one call with source targets and usually no scopes; pick the most
   promising sources from the response.
2. Entity drill-down: for each chosen source, a parallel call scoped to it, with
   dimension / measure / view / dimensional_value targets describing what you need.

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
- Do not mix source targets with other target types in the same call.

## Worked examples
Phase 1, source discovery (no scopes):
{ "search_targets": [ { "target_type": "source", "search_text": "subscriber accounts and churn" } ] }

Phase 2, entity drill-down (scoped to a source from phase 1):
{ "search_targets": [
    { "target_type": "measure", "search_text": "the rate at which customers leave" },
    { "target_type": "dimension", "search_text": "the city where the subscriber lives" }
  ],
  "scopes": [ { "environment": "demo", "package": "saas", "model_path": "subs.malloy", "source": "subscriptions" } ] }
```

Notice what the description does and does not carry. It frames the two-call pattern (without
which the response is inscrutable) and it pins the composability contract (resource_id maps 1:1
to the query tool's scope). It does not teach when to retry, when to widen the search, or how
to phrase a good search_text. That reasoning lives in skills.
