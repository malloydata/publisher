# Audit dbt's Definitions for Ambiguity

> The failure mode that reconciliation cannot catch: a number that matches dbt exactly and answers
> the wrong question. Run this audit before declaring a conversion done.

A faithful conversion preserves dbt's ambiguities perfectly, because faithfulness is the objective.
Every example below matched dbt to the digit and was still the wrong answer to the question asked.
None of them is a capability gap, and none would fail a reconciliation check.

The general shape: **dbt names metrics, and a name is not a definition.** Where two names could
answer one business question differently, the conversion has to settle it or the next asker will
settle it by accident.

## 1. Two metrics that both mean "revenue"

Look for a gross figure and a net figure that differ by tax, discounts, refunds, or fees, then
check whether anything says which one the business calls revenue.

On dbt's own jaffle-shop, six metric names resolve to two numbers: `revenue`, `subtotal` and
`lifetime_spend_pretax` give 100,441; `order_total` and `lifetime_spend` give 105,826.18. The
5,385.18 difference is `tax_paid`, collected for the state rather than earned. `order_total`'s dbt
description is *"Includes tax + revenue"*, which names the problem and resolves nothing.

**The test:** list every money metric, total each one over the same population, and group them by
value. Any group with more than one member is an ambiguity. Then ask which is reported externally.

**Settle it by:** saying in the gross metric's `#(doc)` that it is not the revenue figure, naming
the difference and the field to use instead, and adding a reconciling view
(`net + tax = gross`) so the relationship is checkable rather than folklore.

## 2. Boolean flags presented as a mix, which overlap

dbt's `food_orders` (2,336) and `drink_orders` (9,319) against 9,568 orders read as 24.4% and
97.4%: 122%. They overlap on 2,164 orders containing both, and omit 77 with no items at all.

**The test:** for any set of flag-counting metrics that look like a breakdown, sum the counts and
compare with the population. Over 100% means overlap; under means an omitted bucket. Both are
common and neither is documented.

**Settle it by:** adding one dimension with mutually exclusive, exhaustive buckets that sums to the
population, and caveating the original metrics rather than deleting them (they are dbt's and they
reconcile). The exclusive version usually changes the story: food-*only* turned out to be 172
orders, 1.8%, against the 24.4% the overlapping flag implies.

## 3. One business concept with two definitions: state against event

The most dangerous class, because both readings are defensible and the answers can differ by
orders of magnitude.

jaffle-shop has `customers.customer_type` ('new' while lifetime order count is 1) and
`customer_order_number = 1` (the customer's first ever order). Asked "how much revenue from new
customers", the first answers **$6.36** and the second **$2,586.13** - 400x apart. The first is a
*current-state* label, so on a dataset of repeat buyers almost nobody is ever 'new'.

**The test:** for each business noun (new customer, active account, churned user, first purchase),
list every field that could express it and check whether they are states or events. A field
computed from a lifetime total is a state; a field computed from a row's position in a sequence is
an event. They answer different questions and are rarely both documented.

**Settle it by:** documenting which each field is, naming the gap in the misleading one's `#(doc)`,
and adding a view that answers the business question with the right definition.

## 4. Cohort comparisons confounded by tenure

A cohort table showing a lifetime total per cohort is comparing groups that have had different
amounts of time to accumulate it. Lifetime spend by first-order month fell from $37,422 (12 months
old) to $904 (1 month old), which reads as a 97% collapse in cohort quality and is mostly
arithmetic.

**The test:** any per-cohort total is suspect. Divide by a tenure measure and see whether the
ranking changes. Here it inverted: normalized, the strongest cohort was the second one, and the
real decline was about 31%.

**Settle it by:** adding the tenure dimension and the normalized measure, and saying in the view's
`#(doc)` which column to read and why the raw total misleads.

## 5. Measures that change value when reached across a join

A `count()` means "rows in scope", and a join narrows the scope. Any ratio with a `count()`
denominator is therefore grain-bound: reaching it from a child source silently changes the
population.

77 of 9,568 orders have no items, so from the item side dbt's `orders` metric reads 9,491 and its
`new_customer_orders` reads 148 rather than 150. An `avg_order_value` defined as
`order_total / count()` reads 11.15 from the item grain against 11.06 from the order grain. Both
figures are correct; they describe different populations, and 0.8% passes review.

**The test:** total each `count()`-based metric at its own grain and again from a child source. Any
difference is the population changing.

**Prefer a ratio of two sums**, which is grain-invariant: an average built as
`spend.sum() / order_count.sum()` returns the same value from every grain. Where the populations
genuinely differ there is no invariant form, so say in the `#(doc)` which grain the measure means
and what a join returns.

## What to hand back

These are decisions, not bugs, so they belong in the record rather than only in the model. For each
one: the ambiguity, the two candidate answers with their numbers, the definition chosen, and who
confirmed it. `modeling-notes.md`'s **Decisions** section is the home; anything not yet confirmed by
someone who owns the metric goes under **Open decisions** with the hedge repeated in the field's
`#(doc)`.

And state the limit plainly when handing over a converted model: reconciliation proves the numbers
match dbt, and says nothing about whether dbt's definitions answer the questions being asked.
