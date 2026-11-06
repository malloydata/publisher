<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Compliance review

> What this is: how to check a package against a written access policy, on every change, with the
> `malloy-compliance-review` and `malloy-classify` skills and the `compliance.py` script they share.
> The annotations it reads are in [classification.md](classification.md), the gates in
> [authorize.md](authorize.md) and [row-level-access.md](row-level-access.md), and the record of each
> query in [access-record.md](access-record.md).

A policy says who may read which data. A package enforces it with gates on its analytical domains,
a class on every field, and a ceiling on what each domain carries. Some of that Publisher refuses at
load: a domain exposing a class its `#(carries)` omits, a gate on a given the caller can set, a join
around a gate. The rest is a review: does each role get what the policy says, and do the values
match their declared classes? The review is a script, so it runs in CI and fails the change that
breaks a control.

## The pieces

| Piece | What it is | Who writes it |
| --- | --- | --- |
| The policy | Prose: classes, roles, requirements | The policy owner |
| The register, `policies/controls.json` | The policy as checks: classes, personas with their givens, what each persona gets from each domain, the requirements and the checks that prove them | An agent drafts it with `malloy-compliance-review`; the policy owner approves it |
| Classifications | `#(classification)` on each field | An agent proposes them with `malloy-classify`; a person approves the diff |
| `compliance.py check` | Runs the register against the served package and writes a report | Ships in the skill |

[`examples/clinical-records`](../examples/clinical-records) has all four for a HIPAA minimum-necessary
policy over Synthea data, plus five changes that each break a control
([`violations/`](../examples/clinical-records/violations)).

## What the check does

It reads what the served package publishes from three places: the package's
[classification summary](classification.md#the-classification-summary)
(`GET …/packages/{pkg}/classification`: each domain's `#(carries)` and its fields by class), the
model read (each domain's gates, and which
givens are `#(secure)`), and `get_context` over the MCP endpoint (each published field with its
classes). It reads `GET /api/v0/status` for load refusals. Then:

- **Structure.** Every published field is classified; every domain is gated, on `#(secure)` givens
  only; every domain declares `#(carries)` and serves nothing beyond it or forbidden by the
  register.
- **Unpublished sources.** It reads the package's own `.malloy` files and queries every source the
  package does not publish, through that source's own file, as an identity holding every role the
  register names. Each must refuse. This is why the check runs from the package directory.
- **Access.** It runs a count on every domain as every persona and compares the answer with the
  register: every row, the rows a row filter selects, or a 403.
- **Values.** It runs one aggregate per published dimension, as a persona admitted to its domain,
  and fails a field whose values prove more than its class allows: SSNs, record identifiers, or a
  name with one value per patient declared `none`, or day-level dates on a de-identified domain.
- **Load refusals.** A refusal is reported as the control it breaks. When the server keeps serving
  an earlier model, the report says so at the top.

No query selects a row, and the report holds only counts. It exits 1 when any control fails. The
skill's `reference/checks.md` lists every check and threshold.

A requirement nothing in a model can prove, such as how long access records are kept, is
`not_provable_here` in the register with who can prove it. The report says so rather than passing
it.

## Run it

Against a local Publisher serving the package, from the package directory:

```bash
cd examples/clinical-records
python3 ../../skills/malloy-compliance-review/scripts/compliance.py check \
  --environment examples --package clinical-records \
  --register policies/controls.json --out-dir review
python3 ../../skills/malloy-compliance-review/scripts/compliance.py attest \
  --report review/report.json --out review/attestation.md
```

`--server` defaults to `http://localhost:4000` and `--mcp` to `http://127.0.0.1:4040/mcp`, the
server's two default ports. `--package-dir` defaults to the current directory. Python 3.10 or
newer, standard library only.

**Run it only against a server with no gateway in front.** Each persona's givens travel with its
requests and are honored, because nothing upstream sets them. That is what lets a script ask "what
does a billing clerk see"; behind a gateway that sets `#(secure)` givens from a verified identity,
the probes would all run as the script's own identity and prove nothing.

## In CI

Start Publisher on the change's checkout, in a server root outside the checkout, and run the check.
This workflow reviews a repository whose root is one package with its register in `policies/`:

```yaml
name: compliance review
on:
  pull_request:
    paths: ["**/*.malloy", "policies/**", "publisher.json"]

jobs:
  review:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: 20

      - name: Serve the change
        run: |
          root="$RUNNER_TEMP/publisher"
          mkdir -p "$root"
          cat > "$root/publisher.config.json" <<JSON
          { "frozenConfig": true,
            "environments": [{ "name": "ci", "connections": [],
              "packages": [{ "name": "clinical-records", "location": "$GITHUB_WORKSPACE" }] }] }
          JSON
          npx -y @malloy-publisher/server@latest --server_root "$root" --port 4000 \
            --no-mcp-config > "$RUNNER_TEMP/publisher.log" 2>&1 &
          for i in $(seq 1 120); do
            state=$(curl -s localhost:4000/api/v0/status | jq -r .operationalState 2>/dev/null)
            [ "$state" = serving ] && exit 0
            sleep 2
          done
          cat "$RUNNER_TEMP/publisher.log"; exit 1

      - name: Review
        run: |
          npm install --no-save --prefix "$RUNNER_TEMP/skills" @malloy-publisher/skills@latest
          python3 "$RUNNER_TEMP/skills/node_modules/@malloy-publisher/skills/skills/malloy-compliance-review/scripts/compliance.py" \
            check --environment ci --package clinical-records \
            --register policies/controls.json --out-dir review

      - if: always()
        uses: actions/upload-artifact@v4
        with:
          name: compliance-review
          path: review/
```

Three things in it matter:

- **The server root is outside the checkout.** Publisher copies the package into
  `publisher_data/` under its root; a root inside the package would copy the package into itself.
- **A change Publisher refuses still produces a report.** On a fresh server there is no earlier
  model to fall back to, so the package is not served at all. The report says so, fails the
  controls the refusal names, and marks every other control as not checked rather than holding.
- **The report is the build's artifact**, so the evidence for a pass or a failure travels with the
  change.

## Classifying a model

A package nobody has classified fails the coverage check on every field. `malloy-classify` proposes
classes from two kinds of evidence: the values, through `compliance.py profile` and `propose`, which
prove a floor (an SSN is a direct identifier whatever it is called), and the policy's class table,
which decides the rest by what a field means. It shows the proposals as a diff and writes nothing
until a person approves. Run against the clinical-records model with its classifications stripped,
it matched 51 of 53 hand classes and sent both differences to the policy owner as questions.
