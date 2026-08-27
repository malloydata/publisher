---
name: malloy-model-as-you-go
description: After answering a data question, write down what the answer assumed so the next reader can trust the number. A field with a #(doc) in the model when you can edit it, an extend in the notebook when you can only author reports, or a stated assumption plus a Malloy snippet when you can only chat. Use after every answered question that rested on a judgment call, and whenever a question is asked against tables that have no model yet.
---

<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Model as you go

An analysis that lives in a chat transcript is not reproducible. The numbers were right, and
six weeks later nobody can say what "revenue" excluded, which of four timestamps was the order
date, or whether the last period was complete. The work is unauditable, so it gets redone.

The fix is to write the assumptions down while the query is still in front of you, in the
most durable place your session can write. Answer the question, codify what the answer
assumed, answer the next one. After a handful of questions there is a model, or a notebook,
where every definition exists because a real question needed it, and every judgment call is
on the record.

This skill is the codify step. `skill:malloy-analysis` answers the question; this skill
decides what to write down afterwards, and where.

> **Tool names** are bare here - `get_context`, `execute_query`, `search_database_schema`. The
> exact prefixed name depends on the host; match against the tools you actually have.

## The loop

```
QUESTION → ANSWER → CODIFY  ⟲
              ↑___________|
```

**The user gets a real answer on question one.** If three tool calls have gone by without
producing an insight they can read, you have drifted into modelling for its own sake. Stop and
answer something.

## 1. Answer the question, with `skill:malloy-analysis`

This skill starts when someone asks a data question. **That question is the unit of work, and
it is theirs.** Do not widen it into a modelling project, and do not swap it for a more
interesting one you found on the way.

**A broad ask is still an ask.** "Analyse the sales data" is a question whose subject is given
and whose metric is not. Pick the most obvious question about that subject, say in one line
which one you picked, and answer it. Never open with a row-count table and a menu of
"analytical domains"; the menu is worth less than the first real answer would have been.

Load `skill:malloy-analysis` and follow it: discover the model, construct the query, run it,
verify it, present it. Two of its rules matter most here:

- **Every number you present comes out of a query.** Adding up the rows of a `limit: 15` table
  by hand drops everything below the cut, and nobody can re-run it.
- **Your first result is a draft.** Work `skill:malloy-analysis-pitfalls` before presenting.

### Name the decisions the answer rests on

**This is what makes the analysis auditable, and it is the step most easily skipped.**

Almost every query needs at least one judgment call the data cannot settle. Which of four
timestamps is "the order date". Whether returns count as revenue. Whether to count lines or
orders. Whether a partial final period belongs in a trend.

A judgment call made silently becomes a hidden assumption. It will be wrong for someone
eventually, and by then nobody remembers it was a choice rather than a fact. So make the call,
run with it, and **say what you chose and what you rejected**, with the other number attached
whenever it is cheap to get:

> Revenue here excludes cancelled **and** returned orders - that's $8.10M. Counting returns as
> revenue and netting them separately gives $9.18M. I went with the stricter one; say if your
> reporting does it the other way.

Not every choice rises to this. Raise it when **any** of these holds:

| Raise it when | Because |
|---|---|
| The alternative changes the number materially | The user would answer differently depending on which they meant |
| You are about to codify it | It stops being your choice and becomes everyone's definition |
| A reasonable analyst would pick the other one | It is a convention, not a fact |

Otherwise state it in a clause and move on. **Asking about everything is as bad as asking about
nothing**; it turns the loop into a form and trains the reader to skim past it. And **give the
number under each option, not an abstract question**: "$8.10M excluding them, $9.18M
including" lets the user answer in one word.

## 2. When there is no model yet

If the tables have no model, define only enough to run the first query. One line is normal:

```malloy
source: order_items is my_conn.table('ecommerce.order_items')
```

**Read the columns with `run: source -> { select: * limit: 3 }`.** Prefer it to a schema
listing: it returns the columns *and* real values, and the values are what catch the surprises
a column list hides - a date stored as a string, a `total` that excludes tax, a metric column
that is null on every row.

If a schema tool returns something you cannot explain (no columns, or no tables for a filter
you can see matches), that is a bug in the tool. Report it. Do not write the workaround into
your model or your notes as though it were a property of the data.

No `primary_key:`, no dimensions, no measures, no joins, **not yet**. Those arrive in CODIFY,
each one paid for by a question that needed it. Adding fields because the table has them is the
habit this skill exists to break.

Add a join only when *this* question cannot be answered without it, then verify its cardinality
before trusting any aggregate (`group_by: fk, aggregate: n is count(), having: n > 1`).

If a model already exists, ground yourself in it with `get_context` and reuse what is there.

## 3. CODIFY: write the answer's assumptions down

Run this after *every* answered question, while the query is still in front of you. Skipping it
is how a session ends with a transcript and nothing else.

### Where it goes depends on what you can write

The same assumption lands in a different place depending on the session. Pick the **highest
rung your tools allow**, and never skip codifying because the top rung is out of reach.

| You can | Codify as | Who inherits it |
|---|---|---|
| **Edit the model files** (a local package, a draft package, a compile or reload tool) | A `dimension:`, `measure:`, `join_*:`, or `view:` with a `#(doc)`, in the `.malloy` file | Everyone who queries the model. **Confirm binding decisions first** (below) |
| **Author notebooks or reports, but not the model** (a viewer of a published package) | In the notebook: `source: orders_q is orders extend { measure: ... }` with the `#(doc)` above it, plus a markdown cell stating the assumption. A question worth re-asking becomes a cell | Readers of the report. State the decision in the cell, and say which definitions the model should adopt |
| **Only answer in chat** (no file or report tools) | The assumption stated in the answer, plus the Malloy snippet a modeler could paste: the `measure:` with its `#(doc)` | Nobody, until someone acts on it. That is why the snippet matters |

Tell the rungs apart by the tools you have, not by guessing at the user's role: a file-write or
compile tool means the top rung; a report or notebook tool without model edits means the middle
one; neither means the bottom.

### What to codify

Look at the query you just ran. For each dimension, measure, join, filter, **and for the shape
of the query itself**, ask **in this order**:

| Codify it when | Example |
|---|---|
| **A reader needs it to trust the number** | the cancelled/returned exclusion - without it every revenue figure reads high |
| **It encodes a business rule** | a regex parsing a messy column, a status mapping, tier cutoffs |
| **Another question would reuse it** | `revenue is sum(sale_price)` - everything about orders needs it |
| **It was hard to get right** | a window function, a multi-step derivation, a verified join |

The first row is the one that matters. A definition is worth keeping less because it saves
typing than because it stops the next reader misreading the number.

**Codify thin, and leave the ad-hoc behind**: filters tied to one finding, calculations that
answered exactly one question. A question that needed one measure codifies one measure, not the
neighbouring columns as well. An empty CODIFY is a fine outcome; say so and move on.

**Then say what you did**, in one line, every time:

> Codified: `revenue is sum(sale_price)` excluding cancelled and returned, `category` via the
> products join. Left ad-hoc: the `where: created_at > @2023` - just this question's window.

That line shows the model growing and gives the reader a place to object. Never codify
silently.

### A decision becomes binding the moment it enters the model

While a judgment call lives in one ad-hoc query it is yours, and stating it is enough. Once it
is in the model file it is the definition everyone inherits, and nobody downstream sees the
reasoning. So on the top rung, **stop, ask, and wait for an answer** before writing it down.
Reporting it afterwards ("I used the midpoint, say if you'd rather have the floor") is not
confirming it; by then it is already the default. On the middle rung, confirm when the report
will be shared; a private notebook is still yours.

This is not only about measures. Confirm anything that changes what later questions return:

| Confirm before codifying | Because it silently sets |
|---|---|
| A source-level `where:` | the scope of *every* later question against that source |
| A `measure:` definition | the default value of that metric for everyone |
| A `dimension:` that buckets, maps, or parses | which rows land in which group |
| A `join_one:`/`join_many:` and its grain | whether aggregates fan out |
| A saved `view:` | the shape people will re-run and cite |

Ask with the numbers attached:

> I want to make `net_revenue` exclude cancelled and returned. That makes it the default revenue
> number for every later question - $8.10M rather than $10.81M gross. Good, or does your
> reporting treat returns differently?

**One confirmation per decision, not per question.** Once the user has settled how revenue
treats returns, it is settled: reuse it and stop asking. And if the user has told you to stop
checking in, believe them; state each decision in a clause and keep going.

### Write it down twice

**In the model (or the notebook), as a `#(doc)` on the definition.** This is the part that
survives. Use `#(doc)`, never a `//` comment, for anything a consumer needs: `#(doc)` is
machine-readable, so it renders in the UI and retrieval tools read it, while a `//` comment
reaches nobody but whoever opens the file. Keep `//` for maintainer-only notes. Tag formats on
the definition too: bare `# currency` on the measure, any scale (`# currency=usd0m`) only in
views.

```malloy
#(doc) Order line items joined to products. Grain is one row per line, not per order.
source: order_items is my_conn.table('ecommerce.order_items') extend {
  join_one: products is my_conn.table('ecommerce.products') on product_id = products.id

  dimension:
    #(doc) Canonical order date. Data runs 2019-01-05 to 2026-03-14, so the final year is PARTIAL - never present it as a full period.
    order_date is created_at::date

  measure:
    #(doc) Revenue in USD, excluding cancelled and returned orders. Those are 25% of gross, so excluding them is not optional.
    # currency
    net_revenue is sum(sale_price) { where: status != 'Cancelled' and status != 'Returned' }

  #(doc) Monthly revenue trend. The question asked on 2026-03-02.
  # line_chart
  view: revenue_trend is {
    group_by: order_month is order_date.month
    aggregate:
      # currency=usd0m
      net_revenue
    order_by: order_month
  }
}
```

One file per analytical domain, named for it: `order_revenue.malloy`, not `model.malloy`. It
grows monotonically across the session.

**In a notes file, as the reasoning.** Keep an `analysis-notes.md` beside the model (or a
markdown cell in the notebook) recording each question, what it found, what got codified and
what was left ad-hoc, and every verification finding. The model carries the *what*; the notes
carry the *why* and the evidence. It is also what lets you resume after losing context.

### Save the view when a question is worth re-asking

A saved `view:` turns "we answered that once" into "re-run it". A trend wanted again next
month belongs in the file as a `view:` with its chart tag (`skill:malloy-charts`); views wanted
side by side belong in a dashboard (`skill:malloy-dashboards`) or a notebook
(`skill:malloy-notebooks`). A genuine one-off does not.

> **This departs from `skill:malloy-model` on purpose.** Its "no views in source files" rule
> assumes a schema-first model, written before anyone asked a question, so its views would be
> guesses. Here every view is a question that was asked and verified. Two more of its rules do
> not apply either: skip access modifiers and curation (there is no discovery surface to curate
> when every field was paid for by a question), and keep one domain file rather than one file
> per table until it genuinely gets unwieldy. Everything else in `skill:malloy-model` applies:
> `#(doc)` on every field, verified join cardinality, `nullif` on division, a `given:` for a
> runtime parameter.

### Then loop

Go back to the question, and let what you just found sharpen the next one:

> Revenue is concentrated in three categories. Worth asking whether that's new - want the same
> cut by year?

## When the analysis is going to be shared

Docs are not on this list; they happen in CODIFY, as you go. When the user wants to hand it
over:

1. **Names.** Rename anything whose name only made sense inside one question.
2. **Doc pass.** Re-read the `#(doc)` lines for anything that drifted as the model grew. Check
   grain, units, and null handling are each stated somewhere.
3. **Re-verify.** Re-run the session's key queries against the finished sources. If a number
   moved, something was codified wrong. This is the check that proves reproducibility; do not
   skip it.
4. **Structure**, only if one file has genuinely become unwieldy: base sources per table plus a
   joined source per domain, per `skill:malloy-model`. Being shared is not itself a reason.

## When to do something else

| Situation | Go to |
|---|---|
| Porting prior art (LookML, dbt, a metrics doc): the definitions exist and are agreed, the job is translation | `skill:malloy-lookml-review`, then `skill:malloy-model` |
| The user names the sources they want built outright, before any question | `skill:malloy-model` |
| A model already exists, the question rests on no judgment call, and nothing is worth keeping | `skill:malloy-analysis` alone |
| Open-ended exploration with no intent to keep anything | `skill:malloy-analyze` |

## Anti-patterns

```
WRONG  Turn "what's our default rate?" into a modelling project
RIGHT  Answer it, then codify what the answer assumed

WRONG  Propose every dimension and measure for each table in scope
RIGHT  Codify the two fields this question actually needed

WRONG  Quietly pick one of four timestamps as "the order date"
RIGHT  "Using created_at; shipped_at would drop 38% as nulls. OK?"

WRONG  Skip codifying because you cannot edit the model
RIGHT  Put the extend and its #(doc) in the notebook, or hand over the snippet

WRONG  Codify the source-level where:, then mention it in the write-up
RIGHT  Stop and confirm it; it scopes every question anyone asks later

WRONG  The assumption lives in the chat transcript, or in a // comment
RIGHT  #(doc) on the definition for what a consumer needs, plus a line in the notes

WRONG  Leave the answered question as a transcript table and move on
RIGHT  Save it as a view; a question worth answering is usually worth re-asking

WRONG  Curate access modifiers and split one file per table to "do it properly"
RIGHT  Skip both; they solve a schema-first problem this model does not have
```
