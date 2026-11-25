---
name: malloy-classify
description: 'Propose #(classification) annotations for a Malloy model from an access policy''s class scheme, backed by aggregate value evidence from the served data, and show them as a diff before writing anything. Use when the user asks to "classify the model", "tag PHI/PII", "label sensitive fields", or adds fields to a package that a policy governs, and before running malloy-compliance-review on a model nobody has classified.'
---

<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Classifying a Malloy Model Against a Policy

> A classification says which of the policy's data classes a field carries. It is what every
> other control reads: `#(carries)` on an analytical domain is checked against it at load, the
> agent sees it on every `get_context` card, and the compliance review checks it against the
> values. This skill proposes classes. A person approves them. Nothing is written until they do.

Two kinds of evidence decide a field's class, and neither is enough alone:

- **The values** prove a floor. A column of `123-45-6789` is a Social Security number whatever it
  is called. A text field with one distinct value per patient singles out a person whatever it is
  called. The values cannot say *which* day-level dates the policy treats as direct identifiers.
- **The policy's class table** decides the rest, by what the field means: its name, its `#(doc)`,
  the column it reads. A diagnosis code is `phi.clinical` because the table says conditions are,
  not because of its values.

A field is never proposed below its floor. Above the floor, the class table decides.

## Before you start

You need a running Publisher serving the package, and the policy. The profiling runs as a persona
the policy admits to each domain, so you also need the part of a control register that names the
classes, the personas and who is admitted where. If the package has a register (usually
`policies/controls.json`), use it. If not, draft these keys from the policy and show them to the
user first; `skill:malloy-compliance-review` (`reference/register.md`) describes each one:

```json
{
  "classes": ["phi.direct_identifier", "phi.quasi_identifier", "phi.clinical", "phi.financial", "none"],
  "identifier_classes": { "direct": ["phi.direct_identifier"], "quasi": ["phi.quasi_identifier"] },
  "subjects": ["patient"],
  "personas": { "clinician": { "givens": { "ROLES": ["clinical"], "ORGANIZATIONS": ["…"] } } },
  "access": { "patient_chart": { "clinician": "scoped" } }
}
```

`subjects` names the joins whose records are the individuals the policy protects. A text field
reached through one of them with a distinct value for nearly every individual is a name, an
address or a license number, and the floor for it is a direct identifier.

## Workflow

### 1. Read the scheme

Find the policy's class table: each class, what it means, and its examples. Use the class names
exactly as the policy writes them. If the policy has no table, stop and say so: inventing a scheme
is the policy owner's decision, not yours.

### 2. Collect the evidence (aggregates only)

The script lives in `skill:malloy-compliance-review`. From the package directory:

```bash
C=<skills>/malloy-compliance-review/scripts/compliance.py
python3 $C profile --environment <env> --package <pkg> --register policies/controls.json --out profile.json
python3 $C propose --profile profile.json --register policies/controls.json --out proposals.md
```

`profile` runs one aggregate query per published dimension: counts, distinct counts, lengths,
pattern-match counts and a numeric range. It never selects a row. `propose` turns that into one
row per field with its value floor, the basis for it, and how the field is declared today. Read
`proposals.md`; never read rows of the data into the conversation to classify them. If a domain
cannot be profiled (no persona admits it), say which, and classify its fields by the class table
alone.

What each signal proves (`skill:malloy-compliance-review` `reference/checks.md` has the thresholds):

| Signal | Least class |
| --- | --- |
| `ssn`, `email`, `phone`, `uuid` (a record identifier), `unique_per_subject` (one value per protected individual) | direct identifier |
| `date_day`, `zip5`, `near_unique` (one value per row at the domain's own grain, such as a timestamp) | quasi-identifier |
| `year`, `zip3`, `free_text` | none: a hint for the class table, never a floor |

The verdicts:

| Verdict | Meaning | What you do |
| --- | --- | --- |
| Declared below what its values prove | The current class is lower than the floor | Propose the floor or higher. This is the finding that matters most |
| Undeclared | No class yet | Propose one: the floor, or the class table if it says higher |
| Declared at or above its value floor | Consistent | Keep it unless the class table says higher |
| A hint, not a floor | Years, three-digit codes, free text | The class table decides; say what the hint was |
| No value evidence | Nothing in the values | The class table decides, by meaning |

### 3. Decide each field

The evidence has one row per field *path*, and a path can stand for several definitions: `patient`
may join a different patient source on each domain, and `patient.age` may be capped on one and not
on another. Before deciding, list every definition behind each row (step 4 says how to find them)
and decide each definition on its own.

Measures are not profiled, but every published field needs a class. Decide them by the class table
and lineage: a measure over a classified field inherits that field's class, and a plain `count()`
reads nothing, so it needs a declaration; the policy usually says what a count carries.

For each field, in order:

1. **Start at the floor.** Never propose below it. If you believe the floor is wrong, say why and
   keep the field at the floor until the user decides.
2. **Apply the class table.** Match the field's meaning to the table's rows and examples. When the
   table names the thing (birth date, medical record number), that row wins. When two rows fit, take
   the more protective one and say so. The class table outranks the policy's prose for
   classification: a list of identifiers to remove for de-identification (HIPAA Safe Harbor) is what
   the de-identification controls enforce, not a second class table. Where the two disagree, follow
   the table and mark the field for the policy owner.
   For a field with a `uuid` floor, confirm whose record it is from its `#(doc)` and the join keys
   that read it: a record of a protected individual (a patient, an encounter of a patient) is a
   direct identifier; a record of anything else goes to the policy owner.
3. **Apply the derivation rule.** A field derived from a classified one carries its input's class
   unless the policy reduces it (a birth year from a birth date, under a de-identification control).
   Publisher's lineage follows Malloy field references, including through joins, and never raw
   columns: `birth_year is BIRTHDATE.year` reads a column, inherits nothing, and needs its own
   declaration, while `birth_year is birth_date.year` inherits `birth_date`'s class. Do not declare
   a class a field already inherits. Where the policy reduces one, declare the reduced class and
   name the control that allows it in the field's `#(doc)`.
4. **Propose, then ask** when the class table does not name a field. Propose the most protective
   class that fits its meaning and mark it for the policy owner with one question. Demographics,
   state, units and specialties are typical. Never leave a field unproposed.
5. **One class per regime.** A list is for a field under several regimes at once
   (`["phi.direct_identifier", "gdpr.personal"]`), not for two classes of the same scheme. When two
   classes of one scheme fit, step 2 decides.

### 4. Place each declaration where the field is defined

A class is declared on the dimension or measure that defines the field, usually in the package's
core sources rather than on each analytical domain. A field defined on a domain itself is
classified there. A field reached through a join takes
the class declared where it lives, so `patient.ssn` is classified on `ssn` in the patient source,
not on every domain that joins it. Find the definition with the `.malloy` files: for each domain
the row lists, follow the join named by the path into the source that domain joins, and take the
last segment's definition there. Two domains may join two different sources under one name; each
definition gets its own annotation.

The form is one quoted class, or a quoted, comma-separated list when a field sits under several
regimes. One `#(classification)` per field; a second is malformed:

```malloy
#(doc) The patient's Social Security number.
#(classification) "phi.direct_identifier"
dimension: ssn is SSN

#(classification) ["phi.direct_identifier", "gdpr.personal"]
dimension: email is EMAIL
```

`docs/classification.md` is the reference, including how lineage and `#(carries)` read these.

### 5. Show the diff and wait

Present one table, every field that would change first:

| Field | Defined in | Now | Value floor | Proposed | Basis |
| --- | --- | --- | --- | --- | --- |
| `patient.first_name` | `core.malloy` `patients_core` | `none` | direct | `phi.direct_identifier` | One value per patient; the table lists names |
| `conditions.condition` | `core.malloy` `conditions_core` | -- | -- | `phi.clinical` | No value evidence; the table lists conditions |

`--` under Now means undeclared; `none` means declared `none`.

Then the fields that keep their class, briefly, and every field you need the policy owner to
answer. **Do not edit a `.malloy` file until the user approves.** Approval of some rows is approval
of those rows only.

### 6. Write, reload, verify

After approval:

1. Add the annotations to the defining sources.
2. `reload_package` (or `GET …/packages/<pkg>?reload=true`). A reload that fails is safe: the
   previous model keeps serving and the errors come back. A `#(carries)` refusal here means a
   domain now exposes a class it does not declare; that is the classification working. Report it,
   do not widen `#(carries)` to make it go away; widening a domain's ceiling is a policy decision.
3. Check `get_status` (or `GET /api/v0/status`) for a `stale: true` entry.
4. Run the review's coverage and value checks, which fail any published field left unclassified or
   declared below its values: `python3 $C check … --out-dir review`, then read `review/report.md`.

## Rules

- **Aggregates only.** Classify from `profile` output and the class table. A sample of rows from a
  field that might hold identifiers is the disclosure the classification exists to prevent.
- **Never below the floor**, and never lower an existing class without the user's explicit approval
  of that row. Lowering a class can admit data to a domain that excluded it.
- **The policy's vocabulary, exactly.** Publisher reads the shape and never the words, so a typo is
  a new class that no `#(carries)` lists.
- **One place per field.** Declare at the definition; let lineage carry it.
- **Propose, don't write.** The output of this skill is a diff a person approves.
