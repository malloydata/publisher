---
name: malloy-phrase-detection
description: How to turn a storefront question into get_context search targets, including the shop vocabulary that does not appear anywhere in the model. Replaces the built-in phrase-detection guide for this package.
---

# Search target construction for storefront

This REPLACES the built-in `malloy-phrase-detection` for this package, so it has
to stand on its own. It covers the general rules briefly and then the part that
is specific here: the words people use for this business, none of which are
field names.

Once you have the entity names, read `storefront-conventions` before trusting a
measure. Several of them do not mean what they are called.

## The general rules, briefly

Write `search_text` as a short description of the concept, not the user's word
back at us. "region" becomes "the geographic region the customer lives in".

One target per distinct concept. Do not stack three dimension targets at the
same field.

Pick the target type by what the thing is:

- `dimension`: something to group or filter by, including dates and numeric
  bands.
- `measure`: something aggregated. Counts, sums, rates, averages.
- `view`: a pre-built analysis. Ask for one before assembling your own.
- `source`: use alone, never mixed with the types above in one call.

A numeric-looking category ("tier 2", "18-30") is a dimension, not a measure.
"Top N" with no metric named still needs a measure target for the ranking.

## Storefront vocabulary

The left column is what people say. None of it appears in the model.

| They say | They mean | Target |
| --- | --- | --- |
| GMV, gross merchandise value, top line | `total_sales` | measure, "total revenue summed over sold line items" |
| AOV, basket size, average ticket | `avg_order_value` | measure, "average revenue per order" |
| units, units sold | `order_item_count` | measure, "count of line items sold" |
| SKU, item, article | a product | dimension, "the product sold" |
| category, department, product line | `category` | dimension, "product category" |
| territory, area, patch | `region` | dimension, "the customer's home sales region" |
| returns, RTV, send-backs | `return_rate` | measure, "share of line items returned" |
| margin, markup, profit | `margin_rate` or `total_margin` | measure, "gross margin, revenue minus product cost" |

Two more that need care rather than translation:

- **"Revenue" and "sales" both land on `total_sales`**, which includes cancelled
  and returned lines. Retrieve it, then read `storefront-conventions` before you
  report it.
- **"Profit" is not modelled.** `margin_rate` is gross margin over revenue and
  nothing else. Retrieve it if that is what they want, and name it gross margin
  in the answer.

## Words with no home here

Do not search for a near miss and answer with it. There is no entity for any of
these, and the honest answer names what is missing:

churn, retention, cohort, LTV, CAC, discount, coupon, tax, shipping cost,
delivery time, stock, inventory, warehouse, supplier, refund amount.

"Ship to" is the trap in that list: `region` looks like it answers it and does
not. It is where the customer lives.

## Finding a literal value

`get_context` searches the model, not the values in it. To find what a
categorical dimension actually holds, target the dimension, then read its values
with a query. `status` holds exactly `Complete`, `Shipped`, `Processing`,
`Returned`, `Cancelled`.
