---
name: malloy-compliance-review
description: 'Check a Malloy package and its controls against a written access policy: turn the policy''s prose into a control register, run compliance.py check against a served package (structure, access as each role, and the values themselves), judge what needs a person, and produce a report and a signable attestation. Use when the user asks whether a model "complies with", "satisfies" or "meets" a policy, HIPAA, SOC 2, PCI or the like, asks for a compliance review or attestation, or wants the review in CI.'
---

<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Reviewing a Malloy Package Against an Access Policy

> A policy is prose: who may read which data, and under what conditions. A compliant package is
> controls a machine checks: gates on the analytical domains, classifications on the fields, a
> ceiling on what each domain carries. This skill connects the two. It writes the policy down as a
> register of checks, runs them against the served package, and reports each control as holding,
> failing, needing a person, or not provable from the model at all.

The review has two halves, and they are kept apart on purpose:

- **The register** (`policies/controls.json`) is the policy as checks. You draft it from the prose;
  the policy owner approves it. It changes when the policy changes.
- **The check** (`scripts/compliance.py check`) runs the register against the package as it is
  served. It changes nothing, reads only aggregates, and exits 1 when a control fails, so the same
  command gates every change in CI.

What Publisher already refuses at load (a domain exposing a class its `#(carries)` omits, a gate
on a given the caller can set, a join that reaches a gated source around its gate) the check
reports as the control it breaks. It does not re-derive them.

## Before you start

- A Publisher serving the package, reachable over REST (default `http://localhost:4000`). Run the
  review against a server with **no gateway in front**: each persona's givens are sent with the
  request and honored, which is how the check asks "what would this role see". Never point it at a
  production endpoint, where a trusted tier sets those givens and the probe would be meaningless.
- The policy document.
- A classified model. If fields are unclassified, `skill:malloy-classify` first.

## Workflow

### 1. Draft the register from the policy

Read the whole policy. For each numbered requirement, write one control with its id and text as
the policy states them, and the checks that prove it. Then the context the checks need: the class
scheme, the personas the policy names, what each persona may read on each domain, and the rules on
particular domains (de-identified, never carries a class, a cap). `reference/register.md` is the
format and `reference/checks.md` says what each check proves.

Rules for drafting:

- **One control per requirement, in the policy's words.** The attestation quotes them back.
- **A control with no check is `not_provable_here`**, with a `why` naming who can prove it (a log
  store's retention, a training record). Saying so is a result, not a gap to paper over.
- **Name a persona for every workforce class the policy names**, and one that should be refused
  (a partner without an agreement). The refusals are half of what the access check proves.
- **Givens come from the model.** A persona's givens are the model's `#(secure)` givens, named
  exactly; `get_context` lists them on each domain, and the package read lists every one under `secureGivens`.

Show the register to the user and stop for approval. It is the policy owner's statement of what
the policy means; you drafted it.

### 2. Run the check

From the package directory:

```bash
python3 <skills>/malloy-compliance-review/scripts/compliance.py check \
  --environment <env> --package <pkg> --register policies/controls.json --out-dir review
```

It reads the package read's classification summary, the model read's gates, `get_context` over
the MCP endpoint (`--mcp`, default `http://127.0.0.1:4040/mcp`) and `GET /api/v0/status`. It probes
every published domain as every persona with a count, queries every source the package does not
publish (read from the `.malloy` files in the current directory, or `--package-dir`) to confirm it
refuses, and profiles every published dimension with aggregates (seconds to a minute for 140
fields). It writes `review/report.json` and `review/report.md`. Against a server that is not on
localhost, `--mcp` is required (a usage error otherwise).

Exit codes: **0** every control holds, **1** a control fails, **2** usage (a bad flag or register),
**3** nothing failed but something could not be checked, **4** an internal error (traceback on
stderr). A probe the server neither answered nor refused (a 400, a 401, a 404 that is not the query
boundary's refusal) is never a pass: it needs review, so a typo cannot make a control hold.
`--allow-review` turns 3 into 0 for a pipeline that triages review items by hand.

### 3. Judge what needs a person

Read `review/report.md`. `reference/report.md` explains each section. For each control:

- **Holds**: report it with its evidence. Do not restate it as stronger than the evidence.
- **FAILS**: name the domain and field from the finding, the requirement it breaks, and the
  smallest change to the model that would fix it. A failure the server refused at load (the report
  says the package is serving a stale model) is already contained: the change was not deployed.
  Say both.
- **Needs review**: a check that could not decide (free text on a de-identified domain, a probe
  the server neither answered nor refused, a cap on a field that was not profiled, a field whose
  declared class is lower than what its expression reads). Look at the evidence and say what you concluded and why; if it needs the
  policy owner, say so.
- **Not provable here**: say who can prove it.

Do not fix the model in the same step. Report first; a fix is a separate change that goes through
the same review.

### 4. Attest

```bash
python3 <skills>/malloy-compliance-review/scripts/compliance.py attest \
  --report review/report.json --out review/attestation.md
```

The attestation lists every control with its status and signature lines for the policy owner.
It is generated from the report, not edited: if a status is wrong, fix the register or the model
and rerun.

### 5. In CI

Run the same `check` on every change to a `.malloy` file, against a Publisher started on the
change's checkout, and fail the build on any non-zero exit: 1 and 4 are never acceptable, and 3
means a person has to look. `docs/compliance-review.md` has a GitHub Actions example. Upload
`review/` as the build's artifact so the evidence travels with the change.

```yaml
      - name: Review
        run: |
          python3 <skills>/malloy-compliance-review/scripts/compliance.py check \
            --environment ci --package <pkg> --register policies/controls.json --out-dir review
          # add --allow-review to pass on exit 3 (needs review, none failing)

      - if: always()
        uses: actions/upload-artifact@v4
        with: { name: compliance-review, path: review/ }
```

## Rules

- **Aggregates only.** The check never selects a row. Do not add a query that does, and do not
  read rows to investigate a finding: the finding's evidence is a count for that reason.
- **The register is approved, not inferred.** Never change an expected outcome to make a check
  pass. A check that fails because the register is wrong is fixed by the policy owner.
- **Report, then fix.** A review that edits what it reviews proves nothing.
- **Exact names.** Domain, persona and given names in the register must match the served model;
  the check refuses an unknown check name and reports an unknown domain as a failure.
