---
id: persist-without-flag-served-live
tags: eligibility, build-control, needs-attention
package: pwf
---

# Missing `##! experimental.persistence`: a `#@ persist` source is silently served live

The `#@ persist` machinery is gated behind Malloy's `##! experimental.persistence`
model flag. Malloy's `getBuildPlan()` THROWS `Model must have ##!
experimental.persistence` on any model lacking the flag — it does not return an
empty plan. So `compilePackageBuildPlan` flag-checks each `.malloy` model (the same
`experimental.persistence` predicate Malloy uses) and **skips** a flagless model
before ever calling `getBuildPlan()`; without that skip, one flagless model would
throw and abort the whole package's build plan.

A consequence of that skip: a source annotated `#@ persist storage=lake` in a model
that **forgot the `##! experimental.persistence` header** is skipped along with the
model. It never materializes — neither into the lake (external) nor colocated — no
binding is produced, and (because the skip happens before the dropped-persist-source
detector runs) **no warning is surfaced**. The `#@ persist` annotation is inert and
the source is served live, exactly as if the annotation were absent.

This pins that footgun. The complementary case — a flagless model coexisting with a
*separate* flagged model whose persist source still materializes (the skip prevents
the abort) — is proven by `cross-model-dag`.

The proof mirrors `off-builds-colocated` in reverse. With the tier fully `on`, build
the (flagless) `storage=lake` source, then mutate and re-query:
- **served live (skipped, correct):** the query recomputes from the source warehouse
  ⇒ the mutation is visible ⇒ **fresh**.
- if it had somehow materialized: the frozen snapshot would serve the pre-mutation
  value ⇒ **stale**.

So a **fresh** re-query proves nothing was materialized — the annotation was ignored.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.pwf_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |

## Model pwf.malloy

Note the **missing** `##! experimental.persistence` header — the `#@ persist`
annotation below is therefore inert.

```malloy
source: orders is orders_pg.table('public.pwf_orders')

#@ persist name="pwf_daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}
```

## Publish

The flagless model is skipped: no persist source is recognized, so the build is a
no-op. No binding is produced (no `expect binding`), and no warning is surfaced.

## Query base

```malloy
run: daily -> { select: order_date, total_amount; order_by: order_date asc }
```

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 200          |

## Mutate orders_pg.pwf_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 99           | 2026-01-01      | 1000       |

## Query base (again)

Fresh `1150` ⇒ the query recomputed live from the source warehouse. Nothing was
materialized — the `#@ persist storage=lake` annotation was silently ignored because
the model lacks `##! experimental.persistence`.

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 1150         |
| 2026-01-02 | 200          |

## Note (since=2026-07-23)

> Open question — should a `#@ persist` annotation on a model that lacks `##!
> experimental.persistence` be **silently ignored**, or should it **warn**? Today it
> is silent: `compilePackageBuildPlan` skips the flagless model (it must — Malloy's
> `getBuildPlan()` throws without the flag, and skipping is what keeps one flagless
> model from aborting the whole package), and because the skip happens *before* the
> dropped-persist-source detector runs, an author who simply forgot the header gets
> no signal — their source is served live with no materialization and no warning.
> That is an easy mistake to make. Consider surfacing a package warning ("source
> `daily` carries `#@ persist` but the model is missing `##! experimental.persistence`;
> the annotation was ignored and the source is served live"), analogous to the
> kill-switch "ignored … served live" warning. This scenario pins the CURRENT
> (silent) behavior so a change flags loudly.
