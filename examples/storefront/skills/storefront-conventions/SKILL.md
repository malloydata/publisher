---
name: storefront-conventions
description: What storefront's measures actually count, and the four places a reasonable reading of them is wrong. Read before answering any revenue, order, margin, or region question against this package.
---

# Storefront conventions

Everything here is a property of this package's model, not of Malloy. The
numbers quoted were measured against the bundled data on 2026-09-04 and are
there to show the size of each effect, not to be reused as answers.

## The grain is a line item

`order_items` has one row per product sold on an order. 25,356 rows, 11,000
orders.

So `order_item_count` (`count()`) counts lines and `order_count`
(`count(order_id)`) counts orders. A question about orders that reaches for the
row count is wrong by a factor of about 2.3 here.

## total_sales includes cancelled and returned lines

This is the one that bites. `total_sales` is `sale_price.sum()` over every row,
with no status filter:

| Scope | Revenue |
| --- | --- |
| Everything (`total_sales` as defined) | 2,098,177.97 |
| Excluding `Cancelled` and `Returned` | 1,905,657.16 |

9.2% of what the model calls revenue is a line that was cancelled or sent back.
Nothing in the field name or its `#(doc)` says so.

Decide which one the question wants, and say which you used. If it wants
realised revenue, filter it yourself:

```malloy
run: order_items -> {
  where: status != 'Cancelled' and status != 'Returned'
  aggregate: total_sales
}
```

`status` takes five values: `Complete`, `Shipped`, `Processing`, `Returned`,
`Cancelled`. Every line on an order shares that order's status, so filtering
lines by status does not split an order across groups.

## return_rate is a share of lines

`return_rate` is returned line items over all line items. It is not the share of
orders that had a return. If the question is about orders, aggregate
`order_count` with a `Returned` filter against total `order_count` instead.

## region is where the customer lives

`regions` joins through `customers.state`, so `region` is the customer's home
region. It is not a shipping destination or a sales territory. A question about
where goods went cannot be answered from this model.

## margin is gross, and per line

`gross_margin` is `sale_price - products.cost` on a single line, and
`margin_rate` is `total_margin / total_sales`. Cost of goods only: no
discounts, no shipping, no returns netted out, no operating cost. Call it gross
margin in the answer, never "profit".

## What this package cannot answer

Say so plainly rather than substituting a proxy:

- Net revenue, discounts, tax, shipping. Not modelled.
- Customer churn or retention cohorts. There is a `signup_date` on `customers`
  but no subscription or activity table.
- Inventory, stock levels, fulfilment times. Not modelled.
- Anything about where an order shipped. See region, above.
