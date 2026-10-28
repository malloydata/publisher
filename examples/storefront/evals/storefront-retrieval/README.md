# storefront-retrieval

24 `get_context` requests over the bundled `storefront` package, each labelled
with the fields it should return. It is the example set for the retrieval
replay (`skills/eval-loop/reference/retrieval-replay.md`): change a retrieval
setting or a field's `#(doc)`, replay, and see which fields stop or start
coming back, with no answering agent and no judge.

There are 13 questions. Ten come from [`../storefront-tour`](../storefront-tour/README.md).
Each of those is sent twice: once with the question itself as every target's
search text (tag `questionText`), and once with one short phrase per field,
written from the field's `#(doc)` without using the field's name. One more
question needs a field reached through a join (the customer's state; only the
`customers` source counts, since a joined copy shows only customers that have
order lines). Two ask for things the model does not hold (shipping cost,
warehouse). They carry no label, so they have no recall, and the replay reports
how many fields came back for them anyway.

The labels started as storefront-tour's `expectedEntities` and differ from them
in four cases, because a retrieval label must name a field the model defines:

- `customer-count-basis`: the tour requires `order_items.status`, because a
  customer there means someone with a delivered order. The model does not
  define that rule, so retrieval cannot be asked to find it. These cases are
  tagged `modelGap` and require only the customer count.
- `signup-cohort-2025`: `customers.signup_date` reached through the join from
  `order_items` also counts, since revenue by signup year is a question about
  order lines.
- `top-margin-brand` and `orders-per-customer`: only the measure that answers
  the question is required (`margin_rate`, `orders_per_customer`). The component
  measures are acceptable.

Tags say how a case is hard, for the per-tag breakdown: `vocabGap` (the
question shares no word with the labelled field), `nearTwin` and
`equivalentSources` (the field also exists in a source that extends or repeats
the one it is in), `qualifierField` (the answer needs a flag or status field),
`undocumentedField` (the field has no `#(doc)`), `valueInQuestion` (the question
names a value that lives in a dimension), `modelGap` and `refusal`.

## Run it

```bash
export EMBEDDING_API_KEY=...      # without it, every ranking is lexical
python3 skills/eval-loop/scripts/eval.py serve model --set examples/storefront/evals/storefront-retrieval --warm-retrieval
python3 skills/eval-loop/scripts/eval.py replay --set examples/storefront/evals/storefront-retrieval --label baseline-01
```

The run lands in the set's workdir (`~/.malloy-eval/storefront-retrieval-<hash>/runs/baseline-01`),
outside this repository.
