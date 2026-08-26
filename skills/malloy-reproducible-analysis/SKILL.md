---
name: malloy-reproducible-analysis
description: Make an analysis reproducible by codifying it as a semantic model as you go - every assumption written down, every number traceable to the query that produced it. Use when analyzing data that has no model yet, when a data question needs answering from raw tables, or when a past analysis needs to be made auditable and re-runnable.
---

# Reproducible analysis

An analysis that lives in a chat transcript is not reproducible. The numbers were right, and
six weeks later nobody can say what "revenue" excluded, which of four timestamps was the order
date, or whether the last period was complete. The work is unauditable, so it gets redone.

**A semantic model is the fix.** Not as a separate project after the analysis, but as the thing
the analysis deposits into while it runs. Every field in it carries a decision someone made,
written down where the next reader will find it. The model is what makes the analysis
re-runnable and the numbers traceable.

So: answer the question, codify what the answer assumed, answer the next one. After a handful
of questions the `.malloy` file is a model — one where every field exists because a real
question needed it, and every judgment call is on the record.

> **Tool names** are bare here - `get_context`, `execute_query`, `search_database_schema`. The
> exact prefixed name depends on the host; match against the tools you actually have.

## The loop

```
QUESTION → POINT → ANSWER → CODIFY  ⟲
                       ↑_____________|
```

**The user gets a real answer on question one.** If three tool calls have gone by without
producing an insight they can read, you have drifted into modelling for its own sake. Stop and
answer something.

---

## 1. QUESTION - get a real question on the table

**If the user brought a question, use it.** Skip to POINT. Do not make them pick tables first.

**If they didn't** - "model my data", "what's in here?" - profile quickly and quietly, then
**propose questions, not tables**: three concrete ones, ranked, each a single sentence, with
the shape of the data stated up front so they can judge.

**Never open with a table inventory and a menu of domains.** A row-count table is not an
insight, and choosing a scope before seeing a single number is guessing. The question list *is*
the scope proposal - it just pays for itself immediately.

## 2. POINT - the smallest thing that runs

Define only enough to execute the query. One line is normal:

```malloy
source: order_items is my_conn.table('ecommerce.order_items')
```

**Get column names by querying.** A schema tool may list tables reliably and still return no
columns. Do not debug that - `run: source -> { select: * limit: 3 }` gives you the columns *and*
real sample values, which is what you needed anyway. Sample values catch the type surprises a
column list hides: a date stored as a string, a `total` that excludes tax.

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

Look at the query you just ran. For each dimension, measure, join, or filter, ask **in this
order**:

| Codify it when | Example |
|---|---|
| **A reader needs it to trust the number** | the cancelled/returned exclusion - without it every revenue figure reads high |
| **It encodes a business rule** | a regex parsing a messy column, a status mapping, tier cutoffs |
| **Another question would reuse it** | `revenue is sum(sale_price)` - everything about orders needs it |
| **It was hard to get right** | a window function, a multi-step derivation, a verified join |

The first row is the one that matters. A definition is worth keeping less because it saves
typing than because it stops the next reader misreading the number.

**Leave behind** the ad-hoc: filters tied to one finding, calculations that answered exactly one
question, view shapes specific to this narrative.

**Codify thin.** A question that needed one measure codifies one measure. Do not model the
neighbouring columns because they are there. An empty CODIFY is a fine outcome; say so and move
on.

**Then say what you did**, in one line, every time:

> Codified: `revenue is sum(sale_price)` excluding cancelled and returned, `category` via the
> products join. Left ad-hoc: the `where: created_at > @2023` - just this question's window.

That line is the contract. It shows the model growing, names what went in, and gives the reader
a place to object. Never codify silently.

**A decision becomes binding the moment you codify it.** While a judgment call lives in one
ad-hoc query it is yours and stating it is enough. Once it is a `measure:` in the file it is the
definition everyone inherits, and nobody downstream sees the reasoning. So when what you are
codifying *encodes* one of the decisions from ANSWER, confirm it rather than report it:

> I want to make `net_revenue` exclude cancelled and returned. That makes it the default revenue
> number for every later question - $8.10M rather than $10.81M gross. Good, or does your
> reporting treat returns differently?

**One confirmation per decision, not per question.** Once the user has settled how revenue
treats returns, it is settled: reuse it and stop asking.

### Write it down twice

**In the model, as a comment on the field.** This is the part that survives. Six months on it is
the only thing between the next reader and a silent redefinition.

```malloy
source: order_items is my_conn.table('ecommerce.order_items') extend {
  join_one: products is my_conn.table('ecommerce.products') on product_id = products.id

  dimension:
    // Canonical order date. Data runs 2019-01-05 to 2026-03-14, so the final
    // year is PARTIAL - never present it as a full period.
    order_date is created_at::date

  measure:
    // Cancelled and returned are 25% of gross - excluding them is not optional.
    net_revenue is sum(sale_price) { where: status != 'Cancelled' and status != 'Returned' }
    order_count is count(order_id)
}
```

One file per analytical domain, named for the domain - `order_revenue.malloy`, not
`model.malloy`. It grows monotonically across the session.

**In a notes file, as the reasoning.** Keep an `analysis-notes.md` beside the model recording
each question, what it found, what got codified and what was left ad-hoc, and every verification
finding. The model carries the *what*; the notes carry the *why* and the evidence. It is also
what lets you resume after losing context.

**Save the view when a question is worth re-asking.** A trend someone will want again next month
belongs in the file as a `view:` with its chart tag - see `skill:malloy-charts`. A one-off does
not.

### Then loop

Go back to QUESTION, and let what you just found sharpen the next one:

> Revenue is concentrated in three categories. Worth asking whether that's new - want the same
> cut by year?

Each answered question makes the next sharper and the model slightly larger.

---

## When the analysis is going to be shared

Do **not** do this during the loop. Documenting and curating a model that is still moving is
wasted work, and it makes the loop feel like a chore. When the user wants to hand it over:

1. **Docs.** Every codified field gets a doc string - grain, units, null handling. The original
   question is usually the text.
2. **Names.** Rename anything whose name only made sense inside one question.
3. **Re-verify.** Re-run the session's key queries against the finished sources. If a number
   moved, something was codified wrong. This is the check that proves reproducibility, so do not
   skip it.
4. **Structure**, if it has outgrown one file - base sources per table plus a joined source per
   domain. `skill:malloy-model` has the layout. Do this because the file got unwieldy, not on
   principle.

## When to do something else

| Situation | Go to |
|---|---|
| Porting prior art - LookML, dbt, a metrics doc. The definitions exist and are agreed; the job is translation. | `skill:malloy-lookml-review`, then `skill:malloy-model` |
| The user names the sources they want built outright | `skill:malloy-model` |
| A model already exists and they just have a question | `skill:malloy-analysis` |
| Open-ended exploration with no intent to keep anything | `skill:malloy-analyze` |

## Anti-patterns

```
WRONG  Silent profiling, then a table inventory and a scope menu
RIGHT  Quick profile, then three questions the data can answer

WRONG  Propose every dimension and measure for each table in scope
RIGHT  Codify the two fields this question actually needed

WRONG  Quietly pick one of four timestamps as "the order date"
RIGHT  "Using created_at; shipped_at would drop 38% as nulls. OK?"

WRONG  Codify a revenue definition without saying what it excludes
RIGHT  Confirm it - codifying makes it everyone's definition

WRONG  Analyse for six steps, then "shall I turn this into a model?"
RIGHT  Codify after every question; the model already exists by the end

WRONG  The assumption lives in the chat transcript
RIGHT  The assumption is a comment on the field, and a line in the notes
```
