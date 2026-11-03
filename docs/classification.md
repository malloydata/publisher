<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Classification (`#(classification)`)

> What this is: how a field declares the data class it carries, in the words of the package's
> access policy, so an agent sees the class of a field before it reads it and a review can check
> every field against the policy. It is a statement, not a control: to hide a column, use
> `private:`; to decide who may query a source, see [authorize.md](authorize.md); to decide what
> is discoverable at all, see [discovery-and-access.md](discovery-and-access.md).

**One annotation, one token, on the field.**

```malloy
source: patients is duckdb.table('patients.csv') extend {
  #(doc) Social security number.
  #(classification) phi.direct_identifier
  dimension: ssn is SSN

  #(classification) phi.quasi_identifier
  dimension: birth_year is BIRTHDATE.year

  #(classification) phi.financial
  measure: total_expenses is HEALTHCARE_EXPENSES.sum()
}
```

The class is the one token after the closing parenthesis. **Publisher reads the shape and never the
vocabulary.** `phi.direct_identifier`, `confidential`, `pci.pan` and `none` are all one token to
it; which tokens a package may use, and what each permits, is the policy's to say and a review's to
check. A dotted path is the convention (`<regime>.<class>`), not a rule.

Write the class after the route, not inside it. Malloy ends an annotation's route at the first
whitespace, so `#(classification phi.direct_identifier)` compiles with a `malformed-route` warning
and declares nothing; `#(classification) phi.direct_identifier` is the form.

## What Publisher does with it

- **Discovery reports it.** Every dimension and measure card `get_context` returns carries
  `classification` when the field declares one, on the source's own fields and on the copies
  reached through a join (`patient.ssn` carries the class `ssn` declares). An agent deciding
  whether to read a field, or a review walking what a surface exposes, reads it there. It is
  never doc text: a field with a class and no `#(doc)` has no description rather than the
  annotation line, and the class is not embedded or sent to a ranking model.
- **REST carries it verbatim.** The model endpoint's `sourceInfos` hold each field's annotations
  as written, so a REST reader parses the `#(classification)` line itself.
- **Two spellings of one column share one class.** When discovery folds `dimension: state is ST`
  and `dimension: raw_state is ST` into one entity (see `aliases` in
  [ai-agents.md](ai-agents.md)), the class declared on either spelling is the entity's, because
  the class belongs to the column.
- **The load warns about coverage, once the package classifies at all.** The rule is
  self-declaring: a package with no `#(classification)` anywhere is not warned about, and a
  package that classifies one field has said it classifies its fields. From then on every
  published source with a dimension or measure that carries no class gets one
  `classification-coverage` warning naming the fields (`model` is its file, `subject` the source),
  in `GET …/packages/{pkg}` `warnings` and so in `get_status`. The point is the one an access
  policy makes: anything unclassified is data the policy has nothing to say about.
- **A malformed note warns.** `#(classification)` with no class, with more than one word, or
  twice on one field warns `classification-malformed` naming the field and the fix, whether or
  not coverage is in force.

## What coverage reads

Coverage follows what a caller can reach, not what a file declares:

- Only models on the package's [surface](discovery-and-access.md) are read, and only the sources
  their `export { … }` publishes. A hidden base is not asked to classify its fields for its own
  sake.
- A published source's joined fields count, under their dotted path (`person.ssn`), three joins
  deep. A hidden base reached through a join from a published source is therefore reported
  **there**, which is where a caller reads it. Classify the field where it is declared; the class
  rides every join to it.
- Fields with an access modifier (`private:`, `internal:`) never reach discovery, so they are not
  demanded a class. Classify them anyway if a review will read the file: the annotation is still
  the record of what the column is.
- A source locked with `#(authorize) false` publishes nothing a caller can read, so its own fields
  are not reported (its fields reached through a join from an open source are).
- Views and joins are not classified; a view is a query over classified fields, and a join names a
  relationship.

## What it is not

A classification changes nothing about what a query returns. A field marked
`phi.direct_identifier` is served to anyone who may query its source, exactly as before. The
controls are elsewhere, and the class is what lets a review say whether they are the right ones:

| To …                                | Use                                       |
| ----------------------------------- | ----------------------------------------- |
| say what a field is                 | `#(classification)` (this page)           |
| keep a column off a source's API    | `include { private: ssn }` / `internal:`  |
| decide who may query a source       | [`#(authorize)`](authorize.md)            |
| decide which rows a caller sees     | [`#(access_filter)`](row-level-access.md) |
| decide what is listed and queryable | [`index.malloy`](discovery-and-access.md) |

A review that holds a policy ("researchers may read no direct identifier") checks it by reading
each surface a researcher may enter, the classes it exposes, and the gates on it. The class is the
half of that statement the model supplies.
