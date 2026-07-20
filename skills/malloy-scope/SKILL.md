---
name: malloy-scope
description: Present discovery findings and propose an analytical scope before modeling. Use after inspecting a package's model and data, to classify tables and recommend an analytical focus the user can pick from.
---

# Propose Analytical Scope

**When:** After you have inspected the model and its underlying data. You have read the package's sources and fields and looked at the data distributions.

> **Tool names** are written bare here - `get_context`, `execute_query`, `search_malloy_docs`. The exact prefixed name depends on the host surface; match each against the tools you actually have.

**Goal:** Present what you found and recommend an analytical focus. The user selects a direction.

Ground yourself first with `get_context`: it returns the package's sources, views, and fields, so it tells you what data exists, how it relates, and what is already modeled. Query the data with `execute_query` to get row counts and spot data-quality issues. Keep your proposal and the user's decision in the conversation; there is no separate scope file to write.

## What to Present

### 1. Table Summary

Present a table of the discovered tables with key metadata:

| Table | Rows | Columns | Role | Key Relationships |
|-------|------|---------|------|-------------------|
| orders | 1.2M | 24 | Fact | FK: customer_id → customers, product_id → products |
| customers | 50K | 15 | Dimension | PK: customer_id |
| products | 2K | 12 | Dimension | PK: product_id |
| order_items | 3.5M | 8 | Bridge | FK: order_id → orders, product_id → products |
| audit_log | 10M | 6 | Operational | No joins to business tables |

Classify each table by role:
- **Fact**: the events or transactions you measure (orders, sessions, payments).
- **Dimension**: the entities you slice by (customers, products, regions).
- **Bridge**: many-to-many linking tables (order_items, tags).
- **Operational**: ETL, staging, or audit tables that aren't analytical.

### 2. Recommended Analytical Focus

Identify 2-3 analytical domains: categories of questions the data can answer.

- **Order Analysis**: Revenue, order trends, product performance, customer purchasing patterns. (Covers: orders, order_items, products, customers)
- **Customer Health**: Retention, segmentation, lifetime value, churn risk. (Covers: customers, orders)
- **Inventory Management**: Stock levels, reorder patterns, supplier performance. (Covers: products, inventory, suppliers)

**Recommend one** as the starting point with reasoning:

"I'd recommend starting with **Order Analysis**. It covers your most-used tables and the core business questions around revenue and performance. We can add Customer Health as a separate source later."

### 3. Tables to Skip

Flag tables that shouldn't be modeled (with reasoning):

- **audit_log**: Operational/ETL table, not analytical
- **staging_orders**: Staging table, use `orders` instead
- **monthly_summary**: Pre-aggregated snapshot, compute fresh in Malloy instead

### 4. Scope Options

Present 2-3 concrete options as a **numbered list the user can easily select from**. Each option should be a single line with a label, tables included, and key questions it answers. Mark your recommendation.

Format choices for easy selection, so the user can reply "A", "B", or "C":

**A. Order Analysis (recommended)**: orders + customers + products + order_items. Answers: What's selling? How is revenue trending? Who are the top customers?

**B. Full Commerce**: Everything in A plus inventory and suppliers. Broader but more complex.

**C. Customer Focus**: customers + orders only. Narrower, focused on retention and segmentation.

Pick one (or mix, e.g., "A plus suppliers").

## User Interaction

**Present choices in a format that's easy to type a short answer to.** Avoid long prose that requires the user to read and synthesize. Numbered/lettered options with one-line descriptions.

The user will:
- **Select** one of the options (or combine them, e.g., "A plus inventory")
- **Add** tables you missed
- **Remove** tables they don't want
- **Redirect** to a different analytical focus entirely

## After User Confirms

Restate the confirmed scope in the conversation so it's clear what you'll model next. A useful shape to summarize:

- **Connection / schema**: the connection name and schema the sources draw from.
- **Tables in scope**: table, row count, role (Fact/Dimension/Bridge), and any notes.
- **Analytical focus**: one line describing the analytical domain.
- **Deferred**: tables left out, with the reason.

Then hand off to modeling: use your modeling workflow to turn the confirmed scope into Malloy sources, dimensions, measures, and views.

## Tips

- **Don't overwhelm**: if there are 20+ tables, group them by domain and focus on the most relevant cluster.
- **Show evidence**: mention row counts, column counts, relationship density to justify your recommendation.
- **Be opinionated**: recommend one option clearly, don't present them as equal.
- **Flag data quality issues** discovered while inspecting the data (e.g., "The `orders` table has ~3% duplicate rows on `order_id`").

## Done

Scope confirmed in the conversation: tables in scope, analytical focus, and what's deferred. Continue with your modeling workflow.
