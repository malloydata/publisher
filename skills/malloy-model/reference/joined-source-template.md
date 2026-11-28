<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

## Joined Source File Template

```malloy
import "customers.malloy"
import "orders.malloy"
import "user_order_facts.malloy"

#(doc) Customer health analysis. Use for retention, segmentation, and churn risk.
source: customer_health is customers extend {
  join_one: user_order_facts with customer_id
  join_many: orders on customer_id = orders.customer_id

  dimension:
    is_at_risk is user_order_facts.days_since_last_order > 90
      and user_order_facts.total_orders > 1

  measure:
    revenue_per_customer is orders.sale_price.sum() / nullif(customer_count, 0)
    at_risk_count is count() { where: is_at_risk = true }
}
```
