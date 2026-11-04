<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Reading the report

`check` writes two files to `--out-dir`:

- `report.json`: every control with its status and findings, the summary, and whether the package
  was serving a stale model. `attest` reads it; so can a CI step.
- `report.md`: the same, for a person.

## Statuses

| Status | Means |
| --- | --- |
| Holds | Every check the control names passed |
| FAILS | At least one finding failed. The command exits 1 |
| Needs review | Nothing failed, but a finding could not decide on its own |
| Not provable here | The register says the model cannot prove it, and why |

## A stale model

When the latest files did not load, Publisher keeps serving the model it compiled before, and the
report says so at the top. Read the two halves of that together: the controls the refusal names
fail, because the change on disk breaks them, and `refusals` holds, because the change was not
deployed. The fix is to the files on disk; the served model is still the last good one.

## Evidence

Every finding carries its evidence as counts: `53346 distinct of 53346 rows, uuid 100%`,
`64 distinct values for 70 patient records`, `403`. That is all a reader needs to see why, and it
is deliberately all the report holds. To investigate further, run another aggregate; never read
the rows.

## The attestation

`attest` writes one page: the policy, its version, the server and time, every control with its
status in the policy's words, and signature lines. It is a statement about the package as served
at that time, generated from the report; it is not edited by hand.
