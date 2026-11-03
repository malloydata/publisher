<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Classification (`#(classification)` and `#(carries)`)

> What this is: how a field declares the data class it carries, how a derived field inherits
> one, and how a source declares the classes a caller may reach from it, all in the words of
> the package's access policy, so an agent sees the class of a field before it reads it, a
> review can check every surface against the policy, and a surface cannot serve what it says
> it does not carry. To hide a column, use `private:`; to decide who may query a source, see
> [authorize.md](authorize.md); to decide what is discoverable at all, see
> [discovery-and-access.md](discovery-and-access.md).

**One annotation on the field, one on the source.**

```malloy
#(authorize) 'researcher' in $ROLES
#(carries) phi.clinical phi.financial phi.quasi_identifier none
source: research is core extend {
  #(doc) Year of birth.
  #(classification) phi.quasi_identifier
  dimension: birth_year is BIRTHDATE.year

  #(classification) phi.financial
  measure: total_expenses is HEALTHCARE_EXPENSES.sum()

  // No annotation: inferred phi.financial, from total_expenses.
  measure: avg_expenses is total_expenses / patient_count
}
```

The class is the one token after the closing parenthesis. **Publisher reads the shape and never the
vocabulary.** `phi.direct_identifier`, `confidential`, `pci.pan` and `none` are all one token to
it; which tokens a package may use, and what each permits, is the policy's to say and a review's to
check. A dotted path is the convention (`<regime>.<class>`), not a rule.

Write the class after the route, not inside it. Malloy ends an annotation's route at the first
whitespace, so `#(classification phi.direct_identifier)` compiles with a `malformed-route` warning
and declares nothing; `#(classification) phi.direct_identifier` is the form. `#(carries)` lists
its classes the same way, separated by spaces.

## Lineage: a derived field takes the class of what it reads

A field that declares no class takes the class of the fields its expression reads.
`dimension: copied is ssn`, `concat('***', ssn)` and `count(ssn)` all carry `ssn`'s class;
a `rename:` keeps the original definition and so its class; a field reached through a join
carries the class declared where it lives; and a reference through a join
(`concat(patient.ssn, ...)`) reads the joined field's class. Lineage crosses an unclassified
intermediate: if `masked` reads `copied` and `copied` reads `ssn`, `masked` carries `ssn`'s
class and reports `copied` as its input.

- **A declared class wins, and the inputs stay visible.** `birth_year is BIRTHDATE.year` with
  `#(classification) phi.quasi_identifier` is a quasi-identifier derived from a direct one. The
  card reports both, so a review can read a declared `none` against what it was derived from.
- **Disagreeing inputs are not ranked.** `concat(first_name, total_expenses)` reads a direct
  identifier and a financial field. Publisher does not know which class is "higher", because that
  is the policy's ordering, so the field is reported unclassified with both inputs named
  (`classification-malformed`), and the fix is to declare it.
- **A field that reads nothing classified stays unclassified.** `upper(state)` over an
  unclassified `state` is unclassified; `count()` reads nothing and is unclassified. Classify
  the column, or the measure, and the lineage does the rest.
- A hidden field (`private:`, `internal:`) is still an input: a public field derived from a
  private identifier carries the identifier's class.

## `#(carries)`: the classes a surface may expose

`#(carries)` on a source lists every class a caller may reach from it. It is written on the
source line next to the gate that admits callers, so the two statements a policy makes about a
surface, who may enter and what they may see, sit on the same object and cannot drift. A source
that extends another and declares none inherits the base's list, the way it inherits a gate; its
own declaration replaces it.

**A leak refuses the package load.** When a dimension or measure reachable from a `#(carries)`
source, its own, inherited, inferred, or reached through a join three deep, carries a class not
on the list, or carries no class at all, the package does not load, the way a gate that does not
parse refuses it. The error names every leak, with the lineage it came through:

```
source research (index.malloy) carries [phi.clinical, phi.financial, none] but exposes masked,
classified phi.direct_identifier, inferred from social (phi.direct_identifier). Remove the field
from the surface, derive one of a class the surface carries and declare it, or add the class to
#(carries) if the policy allows it.
```

An unclassified field on a `#(carries)` source is a leak too, since it has no class to be on
the list; on such a surface every field must be classified. A `#(carries)` that names nothing is
refused rather than enforced, because it would make every field a leak. A refused load is a 400
on publish and reload, and the package is absent from `list_packages` and reported in
`get_status`'s `loadErrors` with the message, like any other load failure.

## What Publisher does with a class

- **Discovery reports it.** Every dimension and measure card `get_context` returns carries
  `classification` when the field has one, declared or inferred, on the source's own fields and
  on the copies reached through a join (`patient.ssn` carries the class `ssn` declares). An
  inferred class comes with `classification_inferred: true` and `classification_inputs`, the
  classified fields the expression reads, nearest first, each with its class; a declared class on
  a derived field reports its inputs too. It is never doc text: a field with a class and no
  `#(doc)` has no description rather than the annotation line, and the class is not embedded or
  sent to a ranking model.
- **REST carries it verbatim.** The model endpoint's `sourceInfos` hold each field's annotations
  as written, so a REST reader parses the `#(classification)` line itself; lineage is a
  discovery-side computation.
- **Two spellings of one column share one class.** When discovery folds `dimension: state is ST`
  and `dimension: raw_state is ST` into one entity (see `aliases` in
  [ai-agents.md](ai-agents.md)), the class declared on either spelling is the entity's, because
  the class belongs to the column.
- **The load warns about coverage, once the package classifies at all.** The rule is
  self-declaring: a package with no `#(classification)` anywhere is not warned about, and a
  package that classifies one field has said it classifies its fields. From then on every
  published source with a dimension or measure that has no class, declared or inferred, gets one
  `classification-coverage` warning naming the fields (`model` is its file, `subject` the source),
  in `GET …/packages/{pkg}` `warnings` and so in `get_status`. The point is the one an access
  policy makes: anything unclassified is data the policy has nothing to say about.
- **A malformed note warns.** `#(classification)` with no class, with more than one word, or
  twice on one field warns `classification-malformed` naming the field and the fix, whether or
  not coverage is in force. A field whose inputs disagree is reported the same way.

## What coverage and the ceiling read

Both follow what a caller can reach, not what a file declares:

- Only models on the package's [surface](discovery-and-access.md) are read, and only the sources
  their `export { … }` publishes. A hidden base is not asked to classify its fields for its own
  sake.
- A published source's joined fields count, under their dotted path (`person.ssn`), three joins
  deep. A hidden base reached through a join from a published source is therefore reported
  **there**, which is where a caller reads it. Classify the field where it is declared; the class
  rides every join to it.
- Fields with an access modifier (`private:`, `internal:`) never reach a caller, so they are not
  demanded a class and cannot leak. They still feed lineage.
- A source locked with `#(authorize) false` publishes nothing a caller can read, so coverage does
  not report its own fields (its fields reached through a join from an open source are). A
  `#(carries)` on it is still enforced.
- Views and joins are not classified; a view is a query over classified fields, and a join names a
  relationship.

## What it is not

A classification changes nothing about what a query returns. A field marked
`phi.direct_identifier` is served to anyone who may query its source, exactly as before. The
controls are elsewhere, and the class is what lets a review, and `#(carries)`, say whether they
are the right ones:

| To …                                           | Use                                       |
| ---------------------------------------------- | ----------------------------------------- |
| say what a field is                            | `#(classification)` (this page)           |
| say what a surface may expose, and refuse more | `#(carries)` (this page)                  |
| keep a column off a source's API               | `include { private: ssn }` / `internal:`  |
| decide who may query a source                  | [`#(authorize)`](authorize.md)            |
| decide which rows a caller sees                | [`#(access_filter)`](row-level-access.md) |
| decide what is listed and queryable            | [`index.malloy`](discovery-and-access.md) |

A review that holds a policy ("researchers may read no direct identifier") checks it by reading
each surface a researcher may enter, the classes it exposes, and the gates on it. The class is the
half of that statement the model supplies, and `#(carries)` is the half the model enforces.
