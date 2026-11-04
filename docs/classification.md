<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Classification (`#(classification)`, `#(carries)` and `#(control)`)

> What this is: how a field declares the data classes it carries, how a derived field inherits
> them, how a source declares the classes a caller may reach from it, and how either names the
> policy controls it implements, all in the words of the package's access policy, so an agent
> sees the classes of a field before it reads it, a review can check every entry point against the
> policy, an entry point cannot serve what it says it does not carry, and a pipeline can export the
> control-to-implementation map. To hide a column, use `private:`; to decide who may query a source, see
> [authorize.md](authorize.md); to decide what is discoverable at all, see
> [discovery-and-access.md](discovery-and-access.md).

**The classes on the field; the ceiling and the controls on the source.** Every value is quoted, and every list is a quoted, comma-separated list in brackets, the form every list-valued annotation in Publisher uses (`tiles=[…]`, `# drill { to=[…] }`).

```malloy
#(authorize) 'researcher' in $ROLES
#(carries) ["phi.clinical", "phi.financial", "phi.quasi_identifier", "gdpr.personal", "none"]
#(control) ["AC-01", "DI-01", "soc2:CC6.1"]
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

## `#(carries)`: the classes an entry point may expose

`#(carries)` on a source lists every class a caller may reach from it. It is written on the
source line next to the gate that admits callers, so the two statements a policy makes about a
entry point, who may enter and what they may see, sit on the same object and cannot drift. A source
that extends another and declares none inherits the base's list, the way it inherits a gate; its
own declaration replaces it.

**A leak refuses the package load.** When a dimension or measure reachable from a `#(carries)`
source, its own, inherited, inferred, or reached through a join three deep, carries any class not
on the list, or carries no class at all, the package does not load, the way a gate that does not
parse refuses it. The error names every leak, with the lineage it came through:

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

## `#(control)`: which controls a source or field implements

Every compliance framework decomposes its policy into identified controls, SOC 2's `CC6.1`,
NIST 800-53's `AC-3`, PCI DSS's `7.2.1`, ISO 27001's `A.5.15`, HIPAA's `164.312(a)(1)`, and an
organization under several keeps a crosswalk between them. `#(control)` names the ones a source
or a field implements, as a quoted list of identifiers in any format, with an optional framework
prefix when a package answers to more than one (a single quoted identifier is accepted too):

```malloy
#(authorize) 'clinician' in $ROLES
#(access_filter) encounters.organization_id in $ORGANIZATIONS
#(control) ["AC-01", "MN-01", "hipaa:164.312(a)(1)", "soc2:CC6.1"]
source: patient_chart is core extend {
  #(control) ["DI-01"]
  #(classification) "phi.quasi_identifier"
  dimension: birth_year is BIRTHDATE.year
}
```

Publisher reads the identifiers and never the vocabulary; quoting is what lets one carry a colon or
parentheses (`"hipaa:164.312(a)(1)"`). A source's controls are its own line's, not inherited from a
base it extends: a control is a claim about what this source implements, and an extension that
replaces the base's gate does not implement the base's control by inheritance. A `#(control)` that
is empty, unquoted or does not parse warns `classification-malformed`.

## The governance export

`GET …/packages/{pkg}/governance`, or `malloy-pub get governance --environment <env> --package
<pkg>`, returns one JSON document describing what the loaded package serves, in policy terms. For
every source of every compiled model, hidden ones included and marked `published: false`: the
gates in force with the givens each reads, each marked `#(secure)` or not (a gate keyed on a
plain given is a filter the caller can set, and the load refuses the package), its
`#(carries)` list, its control identifiers, and
every dimension and measure a caller can reach from it with its classes, whether they were
declared or inferred, what they were inferred from, and the field's own controls. Everything
comes from the compiled model, so inherited gates, renamed fields, folded aliases and lineage are
what the server actually serves. It is sorted by codepoint throughout and reads no clock, so two
servers on identical bytes answer identical documents.

Its reader is a pipeline. Check it in next to the policy and a pull request's diff shows "research
now exposes `phi.direct_identifier`" before anything deploys; feed it to a review as the model
inventory, so the review's checks are over the served truth rather than the files; hand it to an
auditor as the control-to-implementation map, which is the document they ask for. Nothing in it
depends on who is asking, and it is not an access decision for any caller: enforcement stays with
the gates and the load, and validation stays with the pipeline.

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
- **A malformed note warns.** A `#(classification)` line with no class or with more than one
  word, and a `#(control)` line with no token, warn `classification-malformed` naming the field or
  source and the fix, whether or not coverage is in force; the usable lines beside it still count.

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
