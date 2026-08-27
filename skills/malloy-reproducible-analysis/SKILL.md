---
name: malloy-reproducible-analysis
description: Answer a user's data question, and leave behind a semantic model that makes the answer reproducible - every assumption written down, every number traceable to the query that produced it. Use whenever a user asks a data question, wants a metric, a breakdown, or a trend, and the tables it must come from have no model yet.
---

# Reproducible analysis

An analysis that lives in a chat transcript is not reproducible. The numbers were right, and
six weeks later nobody can say what "revenue" excluded, which of four timestamps was the order
date, or whether the last period was complete. The work is unauditable, so it gets redone.

**A semantic model is the fix** - not a project after the analysis, but the thing the analysis
deposits into while it runs. Answer the question, codify what the answer assumed, answer the
next one. After a handful of questions the `.malloy` file is a model where every field exists
because a real question needed it, and every judgment call is on the record.

> **Tool names** are bare here - `get_context`, `execute_query`, `search_database_schema`. The
> exact prefixed name depends on the host; match against the tools you actually have.
>
> **Check which toolset you have before the first call**, because the query tool differs. A
> Publisher host exposes `malloy_executeQuery`; a Credible host exposes `execute_query_draft`
> for the local working copy and `execute_query` for the published model. If you are iterating
> on a draft, run it against the draft. Say in your answer which one produced the numbers.

## The loop

```
QUESTION → POINT → ANSWER → CODIFY  ⟲
                       ↑_____________|
```

**The user gets a real answer on question one.** If three tool calls have gone by without
producing an insight they can read, you have drifted into modelling for its own sake. Stop and
answer something.

---

## 1. QUESTION - the one you were asked

This skill starts when someone asks a data question. **That question is the unit of work, and
it is theirs.** Do not widen it into a modelling project, and do not quietly swap it for a more
interesting one you found on the way. Answer what was asked.

Restate it before you query: which metric, which breakdown, which filters, which time range.
Decide what a right answer would look like - its shape, its rough magnitude, its grain - so you
can recognise a wrong one when it appears.

**If the question is ambiguous, take the most reasonable reading and say which one you took.**
Stalling to ask is worse than proceeding under a stated assumption. The assumptions that
actually move the number get raised properly in ANSWER, below.

**A broad ask is still an ask.** "Analyse the sales data" is not an empty start - it is a
question whose subject is given and whose metric is not. Pick the most obvious question about
that subject, say in one line which one you picked, and answer it. Then let what you found
suggest the next one.

Never open with a row-count table and a menu of "analytical domains". That asks someone to
choose a scope before they have seen a single number, and the menu is worth less than the first
real answer would have been.

## 2. POINT - the smallest thing that runs

Define only enough to execute the query. One line is normal:

```malloy
source: order_items is my_conn.table('ecommerce.order_items')
```

**Read the columns with `run: source -> { select: * limit: 3 }`.** Prefer it to a schema
listing on the merits: it returns the columns *and* real values, and the values are what catch
the surprises a column list hides - a date stored as a string, a `total` that excludes tax, a
metric column that is null on every row.

If a schema tool returns something you cannot explain - no columns, or no tables for a filter
you can see matches - that is a bug in the tool. Report it to whoever owns the server. Do not
write the workaround into your model or your notes as though it were a property of the data.

No `primary_key:`, no dimensions, no measures, no joins - **not yet**. Those arrive in CODIFY,
each one paid for by a question that needed it. Adding fields because the table has them is the
habit this skill exists to break.

Add a join only when *this* question cannot be answered without it, then verify its cardinality
before trusting any aggregate (`group_by: fk, aggregate: n is count(), having: n > 1`).

If a model already exists, ground yourself in it with `get_context` and reuse what is there.

## 3. ANSWER - and verify before you present

Write the query. Load `skill:malloy-queries` for syntax and `skill:malloy-gotchas-queries`
before writing views. Three that account for most first-attempt failures:

- `count(field)` is **already** the distinct count. `count(distinct x)` is a parse error.
- Fields are separated by commas or newlines, never `;`.
- A dotted path (`users.state`) resolves only if the source declares that join.

**Your first result is a draft, not an answer.** Before presenting anything, work
`skill:malloy-analysis-pitfalls`. At minimum:

- **State the scope.** `min`/`max` of the date field and the row count. Every number is
  meaningless without them.
- **Ask what would make this wrong**, then run the query that would expose it.
- **Check fan-out**: compare `count()` to `count(key)`. A gap means a join is inflating
  your aggregates.
- **Check the denominator** on any rate or percentage.
- **Check that the metric is measured, not encoded.** Before trusting any figure, confirm the
  column actually carries it: nulls on every row, a value that is only valid for one segment, a
  range or bucket rather than a number, a sentinel like `0` or `-1` standing in for "unknown".
  This is the one that silently produces a confident wrong answer.

**Every number you present comes out of a query.** If you catch yourself adding up rows from a
result you already printed, stop and run the aggregate instead. Hand arithmetic over a `limit:
15` table silently drops everything below the cut, and nobody can re-run it. You have a query
language and an execute tool - there is no number worth estimating by eye.

Then answer in plain language, leading with the number that was asked for. If verification
fails, fix it - never present a bad number with a caveat attached.

### Name the decisions the answer rests on

**This is what makes the analysis auditable, and it is the step most easily skipped.**

Almost every query needs at least one judgment call the data cannot settle. Which of four
timestamps is "the order date". Whether returns count as revenue. Whether to count lines or
orders. Whether a partial final period belongs in a trend.

**A judgment call made silently becomes a hidden assumption in the model.** It will be wrong
for someone eventually, and by then nobody remembers it was a choice rather than a fact.

So make the call, run with it, and **say what you chose and what you rejected** - with the other
number attached whenever it is cheap to get:

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
nothing** - it turns the loop into a form and trains the reader to skim past it.

**Give the number under each option, not an abstract question.** "Should returns count as
revenue?" makes the user do the work. "$8.10M excluding them, $9.18M including" lets them answer
in one word.

## 4. CODIFY - write the answer's assumptions into the model

Run this after *every* answered question, while the query is still in front of you. Skipping it
is how a session ends with a transcript and no model.

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

**Codify thin, and leave the ad-hoc behind** - filters tied to one finding, calculations that
answered exactly one question. A question that needed one measure codifies one measure, not the
neighbouring columns as well. An empty CODIFY is a fine outcome; say so and move on.

**Then say what you did**, in one line, every time:

> Codified: `revenue is sum(sale_price)` excluding cancelled and returned, `category` via the
> products join. Left ad-hoc: the `where: created_at > @2023` - just this question's window.

That line shows the model growing and gives the reader a place to object. Never codify
silently.

**A decision becomes binding the moment you codify it. Stop and confirm it first.** While a
judgment call lives in one ad-hoc query it is yours, and stating it is enough. Once it is in the
file it is the definition everyone inherits, and nobody downstream sees the reasoning. So
**stop, ask, and wait for an answer** before writing it down. Reporting it afterwards - "I used
the midpoint, say if you'd rather have the floor" - is not confirming it. By then it is already
the default.

This is not only about measures. Confirm anything that changes what later questions return:

| Confirm before codifying | Because it silently sets |
|---|---|
| A source-level `where:` | the scope of *every* later question against that source |
| A `measure:` definition | the default value of that metric for everyone |
| A `dimension:` that buckets, maps, or parses | which rows land in which group |
| A `join_one:`/`join_many:` and its grain | whether aggregates fan out |
| A saved `view:` | the shape people will re-run and cite |

Ask with the numbers attached, not in the abstract:

> I want to make `net_revenue` exclude cancelled and returned. That makes it the default revenue
> number for every later question - $8.10M rather than $10.81M gross. Good, or does your
> reporting treat returns differently?

**One confirmation per decision, not per question.** Once the user has settled how revenue
treats returns, it is settled: reuse it and stop asking. And if the user has told you to stop
checking in, believe them - state each decision in a clause and keep going.

### Write it down twice

**In the model, as a `#(doc)` on the field.** This is the part that survives - six months on it
is the only thing between the next reader and a silent redefinition. Use `#(doc)`, never a `//`
comment, for anything a consumer needs: `#(doc)` is machine-readable, so Publisher renders it
and retrieval tools read it, while a `//` comment reaches nobody but whoever opens the file.
Keep `//` for maintainer-only notes - why there is no `primary_key:`, where a view came from.
Tag formats on the definition too: bare `# currency` on the measure, any scale
(`# currency=usd0m`) only in views.

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

    #(doc) Distinct orders.
    order_count is count(order_id)

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

One file per analytical domain, named for it - `order_revenue.malloy`, not `model.malloy`. It
grows monotonically across the session.

**In a notes file, as the reasoning.** Keep an `analysis-notes.md` beside the model recording
each question, what it found, what got codified and what was left ad-hoc, and every verification
finding. The model carries the *what*; the notes carry the *why* and the evidence. It is also
what lets you resume after losing context.

**Save the view when a question is worth re-asking.** This is part of CODIFY, not an
afterthought - a saved `view:` turns "we answered that once" into "re-run it". A trend wanted
again next month belongs in the file as a `view:` with its chart tag (`skill:malloy-charts`);
views wanted side by side belong in a dashboard (`skill:malloy-dashboards`). A genuine one-off
does not.

> **This departs from `skill:malloy-model` on purpose.** Its "no views in source files" rule
> assumes a schema-first model, written before anyone asked a question, so its views would be
> guesses. Here every view is a question that was asked and verified.

**Two more of its rules do not apply. Skip them.** Access modifiers and curation - there is no
discovery surface to curate when every field was paid for by a question. And one file per
table - keep the single domain file until it genuinely gets unwieldy.

Everything else in `skill:malloy-model` does apply: `#(doc)` on every field, verified join
cardinality, `nullif` on division, and a `given:` if the model ever needs a runtime parameter.

### Then loop

Go back to QUESTION, and let what you just found sharpen the next one:

> Revenue is concentrated in three categories. Worth asking whether that's new - want the same
> cut by year?

Each answered question makes the next sharper and the model slightly larger.

---

## When the analysis is going to be shared

**Docs are not on this list - they happen in CODIFY, as you go.** A field goes into the file with
its `#(doc)` already on it. Defer them to hand-over and the session ends with a model nobody can
read, because hand-over often never comes and the reasoning is gone by then.

When the user wants to hand it over:

1. **Names.** Rename anything whose name only made sense inside one question.
2. **Doc pass.** Not writing docs from scratch - re-reading the ones you wrote for anything that
   drifted as the model grew. Check grain, units, and null handling are each stated somewhere.
3. **Re-verify.** Re-run the session's key queries against the finished sources. If a number
   moved, something was codified wrong. This is the check that proves reproducibility, so do not
   skip it.
4. **Structure**, only if one file has genuinely become unwieldy - base sources per table plus a
   joined source per domain, per `skill:malloy-model`. Being shared is not itself a reason.

**Still skip curation and access modifiers.** Sharing an analysis-first model does not turn it
into a browsable catalogue.

## When to do something else

| Situation | Go to |
|---|---|
| Porting prior art - LookML, dbt, a metrics doc. The definitions exist and are agreed; the job is translation. | `skill:malloy-lookml-review`, then `skill:malloy-model` |
| The user names the sources they want built outright | `skill:malloy-model` |
| A model already exists and they just have a question | `skill:malloy-analysis` |
| Open-ended exploration with no intent to keep anything | `skill:malloy-analyze` |

## Anti-patterns

```
WRONG  Turn "what's our default rate?" into a modelling project
RIGHT  Answer it, then codify what the answer assumed

WRONG  Propose every dimension and measure for each table in scope
RIGHT  Codify the two fields this question actually needed

WRONG  Quietly pick one of four timestamps as "the order date"
RIGHT  "Using created_at; shipped_at would drop 38% as nulls. OK?"

WRONG  Analyse for six steps, then "shall I turn this into a model?"
RIGHT  Codify after every question; the model already exists by the end

WRONG  Answer "analyse the sales data" with a row count and three suggested questions
RIGHT  Pick the obvious question, say which one you picked, answer it

WRONG  Add up the rows of a `limit: 15` table to get a total
RIGHT  Run the aggregate - hand arithmetic drops everything below the cut

WRONG  Codify the source-level `where:`, then mention it in the write-up
RIGHT  Stop and confirm it - it scopes every question anyone asks later

WRONG  The assumption lives in the chat transcript, or in a // comment
RIGHT  #(doc) on the field for what a consumer needs, plus a line in the notes

WRONG  Leave the answered question as a transcript table and move on
RIGHT  Save it as a view - a question worth answering is usually worth re-asking

WRONG  Curate access modifiers and split one file per table to "do it properly"
RIGHT  Skip both - they solve a schema-first problem this model does not have
```
