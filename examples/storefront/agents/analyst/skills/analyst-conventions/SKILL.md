---
name: analyst-conventions
description: House conventions for revenue questions on the storefront package. Read before answering any revenue, sales, or order-value question.
---

# Analyst conventions

## Revenue

`total_sales` is already net of cancellations: it counts only lines whose
`status` is `Complete` or `Shipped`. Use it as is for revenue and do not add a
status filter.
