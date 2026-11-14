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

> **Operating model.** Publisher is a microservice. It runs locally with your own permissions, or
> inside your cloud behind the gateway or application that authenticates your users; it does not
> authenticate anyone itself. The controls on this page are evaluated against what each request
> carries, so they enforce your policy only when that gateway authenticates the user, sets the
> givens from the verified identity, and is the only way to reach Publisher. See
> [the operating model](security-posture.md#operating-model-a-microservice-behind-your-gateway).

> **Preview.** `#(carries)` does not yet close every path a read can take; [Known gaps](#known-gaps)
> lists the open ones. Treat a `#(carries)` list as a review aid, not an enforcement boundary,
> until they close. A package that declares neither annotation is unaffected.

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

**What refuses and what warns.** These two annotations add to a package and change nothing about
the ones already there. Only `#(carries)` refuses, and only for what a query or a source can reach:
a leak from a carried source, or a read of one that Publisher cannot complete. `#(classification)`
labels and warns (`classification-malformed`, `classification-downgrade`,
`classification-coverage`). `#(authorize)`, `#(access_filter)` and givens load exactly as in 0.11.2,
with one added `join-bypass` warning when an author join reaches a stricter gate than the entry
source's (see [Gates are edge controls](authorize.md#gates-are-edge-controls)). Publisher refuses
a join bypass in no package, including one that declares `#(carries)`; the hard stop is a
[compliance review](compliance-review.md) run in CI, which fails on each `join-bypass` warning.

**A leak refuses the package load.** When a dimension, measure or view reachable from a
`#(carries)` source, its own, inherited, inferred, or reached through a join, carries any class not
on the list, or carries no class at all, the package does not load, the way a gate that does not
parse refuses it. The walk covers:

- views: a view returns what it groups by, aggregates and filters on, so the classes of every
  field it references count. A view that reads a field with no class, or one it declares itself
  with `extend:`, refuses; declare the field on the source and read it from there. A view whose
  references cannot be read refuses too.
- `->` query sources: the fields of `source: s is base -> { ... }` read what the query reads, and the
  stage's filters count, since every returned row reveals what they select on. That holds for a
  field the query declares with `extend:` (read against the query's outputs), for the outputs of a
  `nest:`, and for a query source that is joined from another source. A filter's class is charged on
  its own, so an output declared below it is not called a copy of it, and it reaches every field
  and view that reads the output, hidden or not.
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

## The classification summary

`GET …/packages/{pkg}/classification` returns, for each published entry point, the classes its
`#(carries)` declares and how many of its fields carry each class, inherited classes included,
with the count left unclassified. It is a single-package read, never part of a listing or
`/status`, and it covers only the sources discovery shows, so it reveals nothing curation hides. A
source locked to every caller is left out, and `sources` is empty when nothing published carries a
class.

```json
{
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
  not parse warns `classification-malformed` with the fix, whether or not coverage is in force;
  the usable lines beside it still count. A source gets one such warning however many of its
  fields are malformed, naming up to twelve of them and counting the rest.

## The ceiling is checked per query

The load check above judges what a model exposes. A caller's own query can reach past it: it can
`extend` a source with a field of its own (`run: s extend { dimension: x is ssn } -> { group_by: x }`),
join the source to itself, or declare a source over it, and an `internal:` column is readable
through that. So a query that enters a `#(carries)` source is checked against the list too, on the
fields the compiled query reads, and refused with a 400 before it runs when a read has no
`#(classification)`, carries a class off the list, or cannot be read completely. The message names
the field:

```
source research carries [none, phi.clinical]; this query reads x, which is classified phi.direct_identifier, not on the list
```

- The ceiling is the one the entry struct carries, read down its own chain, so a source a caller
  declares over a carried one (`source: mine is research extend { … }`) has it. A `->` query source
  is also held to the list of the source its query runs on, and to that source's base in turn: the
  strictest list along the chain applies, at load as well as per query.
- A read through a join is held to the list of every carried source it passes through, whether the
  entry carries a list or not: an open source that joins `research`, in the model, in a file it
  imports, or in the caller's own `join_one:`, still cannot read `research`'s fields past
  `research`'s list. The list
  follows the read to where it comes from: a field a caller's `extend:` or a `->` source's output
  derives from `research.patient.ssn` is held to `research`'s list, however it is renamed. A
  source that carries nothing is held to the lists of the carried sources it joins at load too.
  When a struct on that path cannot be read, or a query's shape cannot (an `index:`), the query is
  refused rather than assumed clean when it can reach a `#(carries)`, through a source it enters,
  imports, joins or composes. A query that provably reaches none is not checked, whatever else the
  model carries; a name Publisher cannot resolve counts as reaching one.
- Every stage of a pipeline is read: a later stage's own `extend:` (a join to a source, a dimension)
  is held to the list like the first stage's, against what the stage before it returned. A
  `compose(…)` source is held to the ceiling of the member Malloy resolved for the query as well as
  its own.
- What counts as a read: every field the query selects, aggregates, groups, filters, nests or
  indexes on, a field a caller's `extend:` adds with the classes of what it reads, and each
  reference in the `on` of a join the caller writes. An `internal:` field is not exempt here; it
  is exempt only from the load check. A `count()` that reads no field is served.
- A caller cannot write a `#(classification)` or a `#(carries)` in query text: either would let the
  query decide its own check. The query is refused with a 400.
- Named queries and notebook cells an author declares on a `#(carries)` source are held to the same
  check at load, so one that reads past the list refuses the package rather than the first request.
- A source with no `#(carries)` is not checked, and neither is a request carrying the
  data-management bypass.
- The check reads the compile that answers. A query answered from a materialized source's
  stored table through the served copy ([materialization](materialization.md#the-served-copy-in-development-publisher_materialize))
  is checked on the served copy's own compile, which is your text re-rooted on the stored
  tables with every `#(classification)` and `#(carries)` kept, and on the live compile as well
  whenever that compiles. This is what lets a clean query through a join between sources on
  two connections be served once both sides are stored: such a model loads live with that join
  left out, so the query does not compile live. Anything the served copy's compile cannot show
  is refused, and a served answer that falls back to live is checked against live.

This is breaking for a query that reads past a `#(carries)` source's list, which 0.12.0 served.

## What coverage and the ceiling read

Both follow what a caller can reach, not what a file declares:

- Only models on the package's [surface](discovery-and-access.md) are read, and only the sources
  their `export { … }` publishes. A hidden base is not asked to classify its fields for its own
  sake. Under `queryableSources: "all"` a hidden source answers a query by name, so every
  top-level source of every model is read instead.
- A published source's joined fields count, under their dotted path (`person.ssn`), up to 16
  joins deep and 1024 join paths per source. Past either bound Publisher cannot read what the
  source's fields carry, so a `#(carries)` source refuses rather than assume the rest is clean:
  drop the joins past the bound, or split the source. A hidden base reached through a join from a published source is therefore reported
  **there**, which is where a caller reads it. Classify the field where it is declared; the class
  rides every join to it.
- Fields with an access modifier (`private:`, `internal:`) are not demanded a class at load.
  They still feed lineage, so a public field derived from one carries its class. A `private:` field
  is never reachable. A caller's own query can re-expose an `internal:` field through `extend`, so
  `#(carries)` is enforced again per query, on what the query reads: classify any `internal:` field a
  caller could read (see [The ceiling is checked per query](#the-ceiling-is-checked-per-query)).
- A source locked with `#(authorize) false` publishes nothing a caller can read, so coverage does
  not report its own fields (its fields reached through a join from an open source are). A
  `#(carries)` on it is still enforced.
- A view is not classified itself; a view under `#(carries)` is checked by the classes of the
  fields it reads. A join names a relationship and is not classified.
- A record or array dimension is a field. One built by an expression (`{ v is ssn }`, `[ssn]`)
  carries what the expression reads, and each of its parts carries the same. A struct column is
  charged the union of its parts, and a part of a classified record or array column
  (`purchases.ssn`, under `#(classification) "phi.direct_identifier" rename: purchases is items`)
  carries the classes of the nearest column around it that declares some. A part that cannot be
  resolved fails closed.

## Known gaps

Reads `#(carries)` does not yet hold to the list:

- A nest of a multi-stage view is not traced past the view's first stage, whether a caller's query
  writes it (`run: s -> { group_by: k nest: v }`), an author's view does (`view: outer is
  { group_by: k nest: v }`), or a named query does. Running the multi-stage view directly is held to
  the list; nesting it is not, and its access record claims a complete field list.
- An expression over a classified and an unclassified field takes only the classified input's class.
- A field that is both read and filtered on loses the filter's contribution.
- In a multi-stage `->` source, a field computed in a middle stage and declared below its inputs
  (`#(classification) "none"` over `concat(social, '')`) loads with no `classification-downgrade`
  warning, and a later stage serves it.
- In a multi-stage `->` source, an earlier stage's filter is dropped when a later stage, or a `->`
  source built on it, reads or filters a field of the same name, so its class is lost.
- A `->` source built on another `->` source does not inherit the filters of that source's query.
  An output there that reads nothing, such as `count()` declared `"none"`, is not charged the
  filter, so a count can still test which rows a classified filter admits.

The ceiling also refuses shapes it could serve:

- an `index:` query that can reach a `#(carries)`, even on a source with no ceiling of its own;
- an output Publisher cannot follow to what it reads, such as a `nest:` with stages of its own,
  whether or not it declares a class. Flatten the nest into one stage, or move it out of the source;
- a later `->` stage whose `extend:` re-declares a name the source or an earlier stage already has,
  when either meaning carries a class or a ceiling. Give the new name a different one.

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
