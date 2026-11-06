<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# clinical-records: a model governed by an access policy

Synthetic patient records (Synthea, 1,171 patients) served under a HIPAA-shaped access policy. The
package is the worked example for three things: writing the policy's classes and controls into the
model, letting Publisher refuse a model that breaks them, and handing a review everything it needs
to check what Publisher cannot.

Start with the policy, [`policies/phi-access-and-minimum-necessary.md`](policies/phi-access-and-minimum-necessary.md).
It names seven workforce classes, a five-class data classification, and controls with identifiers.
Everything below implements one of them.

## Layers

| File                | Layer   | What it does                                                                                                                                                                                            |
| ------------------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `base.malloy`       | Base    | One source per table, each `#(authorize) false` (AC-02). Nobody queries a table.                                                                                                                        |
| `core.malloy`       | Core    | One classified source per entity. Every column is hidden and re-exposed as a named field with its `#(classification)` (CL-01). Identifiers only some domains may serve are left out.                    |
| `deidentify.malloy` | Core    | The Safe Harbor derivations, declared once: years for dates (the year of birth withheld past 89), ages capped at 90, state and three-digit ZIP with the small-population areas as `000` (DI-01, DI-02). |
| `domains.malloy`    | Domains | One entry point per access decision, each gated on `#(secure)` givens, and declaring what it `#(carries)`.                                                                                              |
| `index.malloy`      | Surface | The package's published surface: exports the five domains and nothing else.                                                                                                                             |

Not yet capped: `encounter_year` and `onset_year` for a patient over 89 are still published as the year they happened, which can place the patient's age above 89 (DI-01).

## Analytical domains

| Domain              | Workforce class               | Gate                                                          | Carries                       |
| ------------------- | ----------------------------- | ------------------------------------------------------------- | ----------------------------- |
| `patient_chart`     | treating clinician, care team | `'clinical' in $ROLES`, rows `ORGANIZATION in $ORGANIZATIONS` | full PHI                      |
| `billing`           | billing and claims            | `'billing' in $ROLES`, rows `ORGANIZATION in $ORGANIZATIONS`  | identifiers, codes, financial |
| `population_health` | quality analyst               | `'quality' in $ROLES`                                         | limited data set              |
| `partner_lds`       | external partner              | `'partner' in $ROLES and 'dua-signed' in $ATTRIBUTES`         | limited data set              |
| `research`          | IRB researcher                | `'researcher' in $ROLES`                                      | de-identified                 |

`ROLES`, `ORGANIZATIONS` and `ATTRIBUTES` are `#(secure)`: Riverbend's identity provider sets them
for the caller, and the request cannot (AC-03). A domain that dropped its gate would not open,
because each domain extends a core that inherits its base's lock.

## The data app

`public/index.html` is a compliance scorecard for the person who signs off on a release: it reads
the package's classification summary (`GET …/classification`) and the model's gates, and asks every domain the same question as every workforce class, live, and
checks each answer against the policy's workforce table. It leads with four verdicts (classified
fields, gates on identity-provider attributes, domains within their declared ceiling, access
decisions that match the policy), then shows what each domain may serve against what it serves,
the access grid with the query and identity behind every cell, the de-identification measured on
the same patients, the controls register, and the seeded violations. Served at
`/environments/examples/packages/clinical-records/index.html`.

On a local server the page sends each persona's identity attributes itself, and says so in a
banner: in a deployment they are `#(secure)`, set by the identity provider, and a page could not
choose a persona.

## What Publisher enforces, and what it leaves to a review

`violations/` holds six broken versions of the package. One is refused at load: a de-identified
domain that joins back to identifiers, which breaks a `#(carries)` ceiling. One loads with a
`join-bypass` warning that the review fails: a domain re-opened over locked joins. Four load clean:
an identifier declared `none`, a billing domain that lost its organization scope, a gate on a given
the requester sets, and a de-identified patient that publishes the year of birth past age 89. Those
are false or missing statements, not inconsistent ones, and catching them takes reading the policy
and the data. [`violations/README.md`](violations/README.md)
has the table.

## Try it

```bash
# Every published domain, what it may serve, and its fields by class:
curl -s http://localhost:4000/api/v0/environments/examples/packages/clinical-records/classification | jq '.sources[] | {name, carries, classCounts}'

# A clinician at one organization (the trusted tier sets these givens in a real deployment):
curl -s -X POST http://localhost:4000/api/v0/environments/examples/packages/clinical-records/models/index.malloy/query \
  -H 'content-type: application/json' \
  -d '{"query":"run: patient_chart -> { group_by: organization.organization_name; aggregate: encounter_count }","givens":{"ROLES":["clinical"],"ORGANIZATIONS":["e002090d-4e92-300e-b41e-7d1f21dee4c6"]}}'

# The same query as a researcher: 403.
```

The data is synthetic; see [`data/README.md`](data/README.md).
