<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Classification (`#(classification)` and `#(carries)`)

> What this is: how a field declares the data classes it carries, how a derived field inherits
> them, and how a source declares the classes a caller may reach from it, all in the words of the
> package's access policy, so an agent sees the classes of a field before it reads it, a review can
> check every entry point against the policy, an entry point cannot serve what it says it does not
> carry, and a pipeline can read what each entry point serves. To hide a column, use `private:`; to decide who may query a source, see
> [authorize.md](authorize.md); to decide what is discoverable at all, see
> [discovery-and-access.md](discovery-and-access.md).

**The classes on the field; the ceiling on the source.** Every value is quoted, and every list is a quoted, comma-separated list in brackets, the form every list-valued annotation in Publisher uses (`tiles=[…]`, `# drill { to=[…] }`).

```malloy
#(authorize) 'researcher' in $ROLES
#(carries) ["phi.clinical", "phi.financial", "phi.quasi_identifier", "gdpr.personal", "none"]
source: research is core extend {
  #(doc) Year of birth.
  #(classification) ["phi.quasi_identifier", "gdpr.personal"]
  dimension: birth_year is BIRTHDATE.year

  #(classification) "phi.financial"
  measure: total_expenses is HEALTHCARE_EXPENSES.sum()

  // No annotation: inferred phi.financial, from total_expenses.
  measure: avg_expenses is total_expenses / patient_count
}
```

A field's `#(classification)` is one quoted class, `"phi.financial"`, or a quoted list when the
field sits under several regimes at once, `["phi.direct_identifier", "gdpr.personal"]`: an SSN is
a HIPAA direct identifier, GDPR personal data and NIST PII in the same column. A field has one
`#(classification)`; a second is malformed, because the list says it in one place. **Publisher
reads the shape and never the vocabulary.** `"phi.direct_identifier"`, `"confidential"`,
`"pci.pan"` and `"none"` are all just text to it; which classes a package may use, and what each
permits, is the policy's to say and a review's to check. A dotted path is the convention
(`<regime>.<class>`) that keeps regimes apart in one package, not a rule.

The content after the route is read by Malloy's own tag parser, so a malformed value is a parse
error with its reason, never a silent split: an unquoted class (`#(classification) phi.clinical`),
a space-delimited list, or a list missing a comma warns `classification-malformed` and names the
form to write. Write the value after the route, not inside it: Malloy ends an annotation's route at
the first whitespace, so `#(classification "phi.clinical")` compiles with a `malformed-route`
warning and declares nothing.

## Lineage: a derived field takes the class of what it reads

A field that declares no class takes the union of the classes of the fields its expression reads.
`dimension: copied is ssn`, `concat('***', ssn)` and `count(ssn)` all carry `ssn`'s class;
a `rename:` keeps the original definition and so its class; a field reached through a join
carries the class declared where it lives; and a reference through a join
(`concat(patient.ssn, ...)`) reads the joined field's class. Lineage crosses an unclassified
intermediate: if `masked` reads `copied` and `copied` reads `ssn`, `masked` carries `ssn`'s
class and reports `copied` as its input.

- **A declared class wins, and the inputs stay visible.** `birth_year is BIRTHDATE.year` with
  `#(classification) "phi.quasi_identifier"` is a quasi-identifier derived from a direct one. The
  card reports both, so a review can read a declared `none` against what it was derived from.
- **Inputs of different classes union.** `concat(first_name, total_expenses)` reads a direct
  identifier and a financial field and carries both. That is the fail-closed reading: Publisher
  does not know which class is "higher", because that is the policy's ordering, so it keeps every
  class the field could reveal, and an author who knows better declares the set, which wins.
- **A field that reads nothing classified stays unclassified.** `upper(state)` over an
  unclassified `state` is unclassified; `count()` reads nothing and is unclassified. Classify
  the column, or the measure, and the lineage does the rest.
- A hidden field (`private:`, `internal:`) is still an input: a public field derived from a
  private identifier carries the identifier's class.
- A relabelled copy passes its input's classes on. A field that is a plain reference to
  another (`dimension: yr is birth_year`, with or without a cast) and declares a lower class does
  not drop the input's classes for whatever reads the copy: a field or view that reads `yr` carries
  `birth_year`'s classes as well as `yr`'s own declaration.
- A read Publisher cannot resolve is not skipped. A field whose expression reads something
  that does not resolve against the source (a name a view declared with `extend:`, a reference
  through a join the walk could not follow) has no known class. Under `#(carries)` that is a
  refusal, not an unclassified pass.

## A declared class below its inputs

A declared class wins, but a declaration that is lower than what the field reads is the shape of a
mistake or of a de-identification, and Publisher cannot tell which. It treats the two kinds of
field differently:

- A computed field warns. `dimension: birth_year is pick year(BIRTHDATE) when age < 90 else null`
  with `#(classification) "phi.quasi_identifier"` over a `BIRTHDATE` classified
  `phi.direct_identifier` loads, with a `classification-downgrade` warning that names the field and
  the classes it drops. The warning is in `GET …/packages/{pkg}` `warnings` and `get_status`, and a
  [compliance review](compliance-review.md) reports it as a finding for a person to decide.
- A passthrough refuses, under `#(carries)`. A plain copy (a reference, optionally cast) cannot
  de-identify anything, so a copy that declares a class below its input's refuses the package load.
  Remove the declaration so the copy keeps its input's class, or derive a new field with a
  function that does the de-identifying.

A declared class is a statement of what the field is, and Publisher checks declarations, not
truth: nothing reads the values. A field declared `none` whose values are SSNs loads. Proving the
values match the classes is the review's value check.

## `#(carries)`: the classes an entry point may expose

`#(carries)` on a source lists every class a caller may reach from it. It is written on the
source line next to the gate that admits callers, so the two statements a policy makes about a
entry point, who may enter and what they may see, sit on the same object and cannot drift. A source
that extends another and declares none inherits the base's list, the way it inherits a gate; its
own declaration replaces it.

**A leak refuses the package load.** When a dimension, measure or view reachable from a
`#(carries)` source, its own, inherited, inferred, or reached through a join, carries any class not
on the list, or carries no class at all, the package does not load, the way a gate that does not
parse refuses it. The walk covers:

- views: a view returns what it groups by, aggregates and filters on, so the classes of every
  field it references count. A view that reads a field with no class, or one it declares itself
  with `extend:`, refuses; declare the field on the source and read it from there. A view whose
  references cannot be read refuses too.
- `->` query sources: the fields of `source: s is base -> { ... }` read what the query reads.
- joins hidden from discovery: a field reached through a join carries the class declared where it
  lives, whether or not the joined source is published.
- fields a source declares as copies of others, as described above.

A read that cannot be resolved fails closed: the field is treated as unknown and refused, not
skipped. The error names every leak, with the lineage it came through:

```
source research (index.malloy) carries [phi.clinical, phi.financial, none] but exposes masked,
classified phi.direct_identifier, inferred from social (phi.direct_identifier). Remove the field
from the source, derive one of a class the source carries and declare it, or add the class to
#(carries) ["if", "the", "policy", "allows", "it."]
```

An unclassified field on a `#(carries)` source is a leak too, since it has no class to be on
the list; on such a source every field must be classified. A `#(carries)` that names nothing is
refused rather than enforced, because it would make every field a leak. A refused load is a 400
on publish and reload, and the package is absent from `list_packages` and reported in
`get_status`'s `loadErrors` with the message, like any other load failure.

## The classification summary on the package read

`GET …/packages/{pkg}` carries `classification`: for each published entry point, the classes its
`#(carries)` declares and how many of its fields carry each class, inherited classes included,
with the count left unclassified. It is on the single-package read only, never on a listing, and
it covers only the sources discovery shows, so it reveals nothing curation hides. A source locked
to every caller is left out, and the field is absent when nothing published carries a class.

```json
"classification": {
  "sources": [
    {
      "modelPath": "index.malloy",
      "name": "research",
      "carries": ["none", "phi.clinical", "phi.financial", "phi.quasi_identifier"],
      "fields": 19,
      "unclassified": 0,
      "classCounts": { "none": 4, "phi.clinical": 5, "phi.financial": 2, "phi.quasi_identifier": 8 }
    }
  ]
}
```

Everything else about a published entry point is already on the other reads: its gates, and which
givens they read are `#(secure)`, on the model read (`GET …/models/{path}`); each field's classes,
and what an inherited set was inherited from, on its `get_context` card. A review reads all three
([compliance-review.md](compliance-review.md)). Which policy control each part implements belongs
in the review's control register, where a check proves it.

## What Publisher does with a class

- **Discovery reports it.** Every dimension and measure card `get_context` returns carries
  `classifications` when the field has any, declared or inferred, on the source's own fields and
  on the copies reached through a join (`patient.ssn` carries the classes `ssn` declares). An
  inferred set comes with `classification_inferred: true` and `classification_inputs`, the
  classified fields the expression reads, nearest first, each with its classes; a declared set on
  a derived field reports its inputs too. It is never doc text: a field with a class and no
  `#(doc)` has no description rather than the annotation line, and the class is not embedded or
  sent to a ranking model.
- **REST carries it verbatim.** The model endpoint's `sourceInfos` hold each field's annotations
  as written, so a REST reader parses the `#(classification)` line itself; lineage is a
  discovery-side computation.
- **Two spellings of one column share one set.** When discovery folds `dimension: state is ST`
  and `dimension: raw_state is ST` into one entity (see `aliases` in
  [ai-agents.md](ai-agents.md)), the classes declared on either spelling are the entity's,
  because the classes belong to the column.
- **The load warns about coverage, once the package classifies at all.** The rule is
  self-declaring: a package with no `#(classification)` anywhere is not warned about, and a
  package that classifies one field has said it classifies its fields. From then on every
  published source with a dimension or measure that has no class, declared or inferred, gets one
  `classification-coverage` warning naming the fields (`model` is its file, `subject` the source),
  in `GET …/packages/{pkg}` `warnings` and so in `get_status`. The point is the one an access
  policy makes: anything unclassified is data the policy has nothing to say about.
- **A malformed note warns.** A `#(classification)` line with no class, unquoted, or that does
  not parse warns `classification-malformed` naming the field or
  source and the fix, whether or not coverage is in force; the usable lines beside it still count.

## What coverage and the ceiling read

Both follow what a caller can reach, not what a file declares:

- Only models on the package's [surface](discovery-and-access.md) are read, and only the sources
  their `export { … }` publishes. A hidden base is not asked to classify its fields for its own
  sake.
- A published source's joined fields count, under their dotted path (`person.ssn`), up to 16
  joins deep and 1024 join paths per source. Past either bound Publisher cannot read what the
  source's fields carry, so a `#(carries)` source refuses rather than assume the rest is clean:
  drop the joins past the bound, or split the source. A hidden base reached through a join from a published source is therefore reported
  **there**, which is where a caller reads it. Classify the field where it is declared; the class
  rides every join to it.
- Fields with an access modifier (`private:`, `internal:`) are not demanded a class. They still
  feed lineage, so a public field derived from one carries its class. A caller's own query can
  re-expose an `internal:` field, so `internal:` is not a barrier; use `private:` to keep a column
  off a source's surface.
- A source locked with `#(authorize) false` publishes nothing a caller can read, so coverage does
  not report its own fields (its fields reached through a join from an open source are). A
  `#(carries)` on it is still enforced.
- A view is not classified itself; a view under `#(carries)` is checked by the classes of the
  fields it reads. A join names a relationship and is not classified.

## What it is not

A classification changes nothing about what a query returns. A field marked
`phi.direct_identifier` is served to anyone who may query its source, exactly as before. The
controls are elsewhere, and the class is what lets a review, and `#(carries)`, say whether they
are the right ones:

| To …                                                | Use                                       |
| --------------------------------------------------- | ----------------------------------------- |
| say what a field is                                 | `#(classification)` (this page)           |
| say what an entry point may expose, and refuse more | `#(carries)` (this page)                  |
| keep a column off a source's API                    | `include { private: ssn }` / `internal:`  |
| decide who may query a source                       | [`#(authorize)`](authorize.md)            |
| decide which rows a caller sees                     | [`#(access_filter)`](row-level-access.md) |
| decide what is listed and queryable                 | [`index.malloy`](discovery-and-access.md) |

A review that holds a policy ("researchers may read no direct identifier") checks it by reading
each entry point a researcher may enter, the classes it exposes, and the gates on it. The class is the
half of that statement the model supplies, and `#(carries)` is the half the model enforces.
