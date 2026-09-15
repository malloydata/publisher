<!-- Validating a model's definitions once, so answer keys need not be re-derived per case. Read before building or reading a ledger. -->

# The definition ledger

## Why it exists

The loop's standing rule is that a golden must never be derived through the model
under test, because a model bug would otherwise certify its own golden. That rule
is sound, and it does not scale: on a model with real transformation, deriving
every key independently means reimplementing the model. You end up with two
implementations and no reason to trust either, and the eval task becomes *rebuild
the model to verify the model*.

The ledger is the other half. **A definition is checked once, against the layer
directly beneath it, and every case depending on it inherits the result.** Work
scales with the number of definitions rather than the number of cases, and stops
growing as cases are added.

The rule this replaces:

> A key must be independent of **the definitions the question tests**, not of the
> whole model.

And the rule that makes that sound:

> A golden is trustworthy if it was derived independently, **or** if every
> definition it tests has itself been validated.

## Deep models are cheaper per case, not dearer

The tractability argument is about definitions being **shared**, not about models
being shallow. An earlier draft claimed every measure sits one or two hops from a
raw column; measured across four models, that is false:

| Model | Definitions | within 2 hops | deepest chain |
|---|---|---|---|
| ecommerce (sample) | 32 | 90.6% | 3 |
| storefront (sample) | 19 | 94.7% | 3 |
| a 492-definition benchmark | 492 | 100% | 2 |
| a real 548-definition customer model | 548 | **42.5%** | **12** |

On that customer model, the twenty deepest measures close over **812**
definition-checks with no reuse and **116** with the ledger: an 86% saving.
Reuse pays off *more* as chains deepen, because deep chains share their lower
layers. Depth is what makes the ledger worth having.

## What a record holds

```json
{
  "entityId": "measure:order_items:total_sales",
  "kind": "measure",
  "source": "order_items",
  "name": "total_sales",
  "expr": "sale_price.sum()",
  "exprSha": "3f9a…",
  "depends": ["measure:order_items:order_item_count"],
  "check": {"kind": "within_model", "slice": "first 200 rows"},
  "verdict": "agrees",
  "needs": null
}
```

`exprSha` is a **Merkle chain**, not a hash of the one line: it covers the
definition and everything beneath it. A measure whose own text never moved is
still stale once something it builds on changes, and a flat hash would report it
as current. `packageSha` exists already but is whole-package, so it invalidates
everything on any edit and distinguishes nothing.

## The two check kinds

### `within_model` -- no raw access, no truth package

Ask the model for the entity and for the expression it **claims** to be, in one
query, and compare:

```malloy
run: order_items -> {
  aggregate:
    stated is total_sales
    control is sale_price.sum()
}
```

Both sides come from **one** query against one source, so they see identical
rows: a difference is the definition, not a moving population.

A dimension is scalar, and `aggregate:` rejects it outright ("Cannot use a scalar
field in an aggregate"), so a dimension is compared row by row over a slice. The
slice is recorded, because a slice is weaker evidence than a whole table and the
report must not imply otherwise.

This catches a definition that is not what it says: a filter nobody mentioned, a
renamed column, the wrong aggregate, or a served model that has moved away from
the text the ledger recorded.

### `raw` -- needs a lens onto the base tables

**A within-model check is blind to fanout.** A join that duplicates rows inflates
the stated measure and the control expression equally, and the comparison stays
green on a genuinely broken join. So an expression reaching through a join records
`unchecked` with a reason, **never `agrees`** -- reporting it as validated would be
the tool committing the exact error it exists to find.

The exception is aggregates that survive uniform duplication: `AVG`, `STDDEV`,
`MIN`, `MAX`. Those stay checkable across a join.

Detection is by join **name**, not by spotting a dot. `sale_price.sum()` is a
method call on a column in the same source and is perfectly checkable;
`inventory_items.cost.sum()` traverses a join and is not. A first implementation
matched any `word.word` and held back `total_sales` -- the measure twelve of the
ecommerce set's cases depend on -- for a join it does not cross.

Raw checks are **not run by `verify_definitions.py` today.** They need a model
file in a package that declares the base tables, because restricted-mode
compilation rejects `duckdb.table(...)` and `connection.sql(...)` in any ad-hoc
query whatever `queryableSources` says. The existing truth package is that lens.

## What it will not claim

Every one of these records `unchecked`, and a reader must treat `unchecked` as
"nobody has established this", never as "fine":

| Situation | Why not checked |
|---|---|
| reaches through a join | fanout moves both sides equally |
| spans more than one line | only the first line was read, so the recorded expression is a fragment |
| the query failed | a check that could not run is not a check that passed |
| no rows came back | nothing to compare |
| every sampled row null on both sides | proves nothing about the definition |
| no enclosing source | nothing to query it against |

## Exit codes

Matching `verify_goldens.py`, deliberately:

| | |
|---|---|
| **0** | every check that could run ran, and agreed |
| **1** | a definition disagrees with its own stated expression |
| **3** | could not run: no model, no server, or a ledger built and never checked |

**3 is load-bearing.** Building the ledger without `--publisher` returns 3, not 0:
it validated nothing, and a caller must never read that as a pass.

## Running it

```bash
# build the ledger only (exits 3: nothing was checked)
python3 verify_definitions.py --model ecommerce.malloy --out evals/definitions/ecommerce.jsonl

# build and check
python3 verify_definitions.py --model ecommerce.malloy \
  --publisher http://localhost:4811 --environment samples \
  --package ecommerce --model-path ecommerce.malloy \
  --out evals/definitions/ecommerce.jsonl
```

Measured on the ecommerce sample: **28 definitions, 15 agree, 13 unchecked**, of
which 4 need a raw check. The remainder are multi-line definitions and fields
declared inside a nested block, which the line scanner attributes to the
enclosing source and cannot address.

## What a run reports

`run_baseline.py --definitions <ledger>` adds an EVIDENCE block saying what the
pass rate rests on, per case:

```
EVIDENCE
  basis         5 independent, 8 definitions, 36 unchecked
                ! those cases rest on a definition nobody has validated. Not a
                  failure, and not a pass either.
```

| basis | meaning |
|---|---|
| `independent` | the key was established outside the model: two differently shaped derivations agree, or it holds no value at all (`criteria`, `unanswerable`) |
| `definitions` | the key went through the model, and every definition it tests is validated |
| `unchecked` | at least one tested definition is unvalidated, stale, or absent from the ledger |
| `disagrees` | a definition the case depends on contradicts its own expression, so the model is wrong before the answer is |

Independence is read from `golden.verification` (or `gold/<qid>.json`) through
`verify_goldens.has_second_derivation` -- the structured record of "two
differently shaped derivations agree". **Not from `verifiedBy`,** which is free
text: a first implementation matched it by prefix and classified 34 goldens of
the ecommerce set as `unchecked` when every one of them reads "authored and
re-derived against ecommerce-truth". A well-founded set reading as unvalidated
is the same over-claim as an unfounded one reading as validated, pointed the
other way.

Which definitions a case tests comes from `expectedEntities`, which already
names entities in the `kind:source:name` form the ledger keys on. No new
hand-maintained field: a wrong entity id is the failure mode that cost a real
set two days, so this reuses a link the set already maintains and that
`verify_goldens` check 5 audits against the model.

**Without `--definitions` there is no EVIDENCE block at all.** Absent is a
different fact from checked, and a reassuring empty block would be the
over-claim this whole direction exists to remove.

Staleness is a hash comparison against the run's own `model.malloy` snapshot --
the bytes that actually answered -- so it costs nothing and runs every arm.

## Where it goes

`evals/definitions/<package>.jsonl` -- keyed by **package, not by set**, because
definitions belong to the model and outlive any one set, and several sets against
the same package share them. Never under the served package tree; the golden side
door forbids eval artifacts there.

## What this is not

It is not a unit test. Publisher's roadmap plans fixture-based unit tests (a CSV
per source plus an expected result) alongside data tests in `malloy-pub lint`.
A fixture unit test proves an expression computes what you wrote -- so it would
**not** catch a measure whose expression is correct and whose *population* is
wrong, which is the defect class this exists for. The ledger is a data test, and
it should hand off to `lint` when that governance work lands rather than becoming
a permanent parallel mechanism.
