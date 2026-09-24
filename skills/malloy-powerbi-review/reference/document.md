<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Power BI Documentation Extraction (Step 9)

> Carry Power BI descriptions across as `#(doc)` tags. They were written by people who had to answer for the number, which makes them better than anything written fresh during a migration.

## 1. Where Descriptions Live

TMDL carries them as `///` comments directly above the object:

```
/// Net revenue after returns and discounts, excluding tax.
/// Excludes intercompany transfers from 2023 onward.
measure 'Net Revenue' = SUMX(...)
```

Consecutive `///` lines are one description. In the JSON `model.bim` form, the same content is a `description` property.

Every object type can carry one: tables, columns, measures, hierarchies, and roles.

## 2. What to Carry Across

Carry a description that says something a good name does not:

- Business rules and exclusions ("excludes intercompany transfers")
- Grain statements ("one row per shipment, not per order")
- Provenance ("sourced from the finance close, not the operational system")
- Caveats and known issues ("understates Q1 2022 due to the migration")
- Units and currency where not obvious

Skip a description that restates the name (`Customer Name` described as "The name of the customer"). It adds nothing and it makes the real ones harder to find.

## 3. Descriptions That Have Gone Stale

A migration surfaces documentation nobody has read in years. Some of it is wrong.

Where a description makes a checkable claim and a connection exists, check it. A description saying "excludes cancelled orders" over a measure whose DAX has no such filter is a finding worth more than the tag: either the documentation is wrong or the measure is, and the business has been reading one of them.

Do not silently carry across a description you have reason to doubt. Flag it.

## 4. Descriptions on Untranslated Objects

A Class 3 measure that did not translate still has a description explaining what the business wanted. That text is the specification for whatever replaces it. Keep it with the intent list from `translate-measures.md` rather than dropping it with the measure.

## 5. Output

Apply as `#(doc)` tags on the corresponding Malloy objects. Report a table of source object, target object, and whether the description was carried, rewritten, or flagged as suspect.
