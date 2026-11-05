<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# The control register

The register is a policy written as checks: one JSON file, usually `policies/controls.json` in the
package, beside the policy it was drafted from. `compliance.py` reads it; nothing in Publisher
does. [`examples/clinical-records/policies/controls.json`](../../../examples/clinical-records/policies/controls.json)
is a complete one for a HIPAA minimum-necessary policy.

## Keys

| Key | Required | What it is |
| --- | --- | --- |
| `policy` | yes | Path to the policy document, relative to the package. Quoted in the report. |
| `version` | yes | The policy version or approval date the register was drafted from. |
| `classes` | yes | Every class the policy defines, exactly as `#(classification)` spells them. |
| `identifier_classes` | yes | `{"direct": [...], "quasi": [...]}`: which classes identify a person outright and which in combination. The value checks read these. |
| `subjects` | no | Join names whose records are the individuals the policy protects (`["patient"]`). A text field through one of them with a distinct value per individual must be a direct identifier. |
| `personas` | yes | `{id: {"class": "...", "givens": {...}}}`: one per workforce class the policy names, plus the ones it refuses. `givens` are the model's `#(secure)` givens, named exactly. |
| `access` | yes | `{domain: {persona: outcome}}`: what each persona gets from each published domain. An omitted pair means `refused`. |
| `domains` | no | Rules on particular domains, below. |
| `controls` | yes | The policy's requirements, below. |

### Access outcomes

| Outcome | Means | Proven by |
| --- | --- | --- |
| `rows` | Admitted, with every row | The count probe answers, and the domain has no row filter |
| `scoped` | Admitted, with only the rows its givens select | The count probe answers, and the model read shows a row filter in force |
| `refused` | Not admitted | The count probe answers 403, or 404 with the query boundary's refusal |

`scoped` is read from the model's row filter, never from a smaller count: a persona may be the only
one admitted to a domain and still be scoped.

### Domain rules

```json
"research": {
  "deidentified": true,
  "must_not_carry": ["phi.direct_identifier"],
  "caps": [{ "field": "patient.age", "max": 90 }, { "field": "patient.birth_year", "min": 1930 }]
}
```

- `deidentified`: the domain may serve no day-level date, no five-digit ZIP, and nothing that reads
  as free text.
- `must_not_carry`: classes the domain's `#(carries)` may not list.
- `caps`: a numeric field's `max`, `min` or both, as the values show it. A capped field that was not
  profiled, or whose numeric query errored, needs review.

### Controls

```json
{ "id": "MN-04", "text": "A researcher reads de-identified data and nothing else.",
  "checks": ["access:research", "ceiling:research", "values:research"] }
```

- `id` and `text` are the policy's, verbatim.
- `checks` name what proves the control. A check scoped with `:domain` takes only that domain's
  findings; `loads:` also takes a refusal kind (`ceiling`, `join`).
- A control nothing in the model can prove takes `"status": "not_provable_here"` and a `why`.
- `why` on any control is printed in the report beside it.

`reference/checks.md` lists every check.

## Drafting it

Work requirement by requirement. For each, ask what in a served package would show it holding:
a gate (`gated`, `secure_gates`), a classification (`coverage`, `values`), a ceiling (`ceiling`),
an answer to a role (`access`), a refusal (`loads`). If the answer is "nothing in the package", it
is `not_provable_here`; name who proves it. Show the draft to the policy owner before the first
run, and treat their edits as the policy's meaning.
