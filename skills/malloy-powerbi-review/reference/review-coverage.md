<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Power BI Coverage Review (Step 7)

> Compare the Malloy model against the Power BI model it came from. Report what was modeled, renamed, deferred, and skipped, each with a reason. Coverage without parity numbers is not a review.

## 1. Table Coverage

| Power BI table | Malloy source | Status | Reason |
|---|---|---|---|

Statuses: **modeled**, **renamed**, **merged** (several Power BI tables into one source), **deferred**, **skipped**.

Every auto date table is **skipped**, and one line stating how many is enough. Every other skip needs its own reason.

## 2. Measure Coverage

Group by the three classes from `translate-measures.md` and report counts first, then detail.

| Class | Count | Modeled | Deferred | Dropped |
|---|---|---|---|---|
| 1: Translatable | | | | |
| 2: Silently divergent | | | | |
| 3: Untranslatable | | | | |

Then a row per measure that is not a clean Class 1 translation. A Class 1 measure that matched on validation needs no individual line; the count carries it.

## 3. Relationship Coverage

| From | To | Cardinality | Direction | Active | Malloy join | Status |
|---|---|---|---|---|---|---|

Every inactive, bidirectional, and many-to-many relationship needs a line saying what was decided, not just that it was noticed.

## 4. Parity Results

**This is the section that decides whether the migration is trusted.** Coverage counts describe effort; parity numbers describe correctness.

| Measure | Filter context | Power BI | Malloy | Match |
|---|---|---|---|---|

Requirements:

- Cover **every Class 2 measure**, each at a filter context that exercises its divergence. A Class 2 measure validated only at the grand total is not validated.
- Cover a **sample of Class 1 measures**, including at least one that sums other measures (the `BLANK()` path).
- Include the measures the user named as the ones the business actually watches.
- Where data was lifted from a `.pbix`, include row count and one column sum per table.

A mismatch is a finding, not a failure to hide. Report it with both numbers and the context.

## 5. Security Coverage

From `rls-roles.md`: every role, whether it translated, and which sources ended up gated. Any role that did not translate is listed explicitly.

Three things this section must state rather than imply, because each is a protection that can go missing while the report still reads "translated":

- **Whether the enforcement posture changed.** Power BI RLS is enforced by the service against an authenticated principal; givens are caller-asserted. If the gates are not behind a trusted tier that sets givens from verified context, say so per role.
- **Every source a query can enter through**, not just the one the DAX named. A gate on a source reached only through a join never fires.
- **Object-level security** (`metadataPermission` / `columnPermission`), separately from row filters. It is the one real object permission in the source model and it is easy to read past.

## 6. Known Gaps

Close with what the Malloy model does **not** do that the Power BI model did, stated plainly:

- Untranslatable measures, by intent rather than by function name
- Report-layer behavior that has no model equivalent
- Anything depending on the refresh, if the data was lifted from a snapshot
- Roles that did not translate

A review that reports only what was achieved is not usable for the decision the user has to make.
