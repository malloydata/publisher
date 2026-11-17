<!-- Measuring get_context retrieval on its own, with no answerer and no judge. Read it before changing a retrieval setting, a model's docs for findability, or the server. -->

# Replaying retrieval requests

`scripts/retrieval_replay.py` sends each case's search targets to `get_context`
as one call and scores the field ids that come back against the case's labels.
No answering agent and no judge run, so a set of 50 cases takes a few minutes
and costs only what the server's own retrieval spends.

Use it to ask "did this change make the right fields come back?". Use a full
run (`running-a-run.md`) to ask "does the agent now answer correctly?". The
replay cannot see the second: it never reads an agent's own search text and
never checks an answer.

## The case format

A replay reads the set's `cases.jsonl`. A case is replayed when it carries
`searchTargets`; a case without them is skipped and counted, so one set can
serve both kinds of run.

```json
{"qid": "storefront-retrieval-top-category-phrases",
 "question": "Which product category brings in the most revenue, and how much?",
 "tags": ["nearTwin"],
 "searchTargets": [
   {"target_type": "measure", "search_text": "total revenue",
    "expectedEntities": {"required": ["measure:order_items:total_sales"]}},
   {"target_type": "dimension", "search_text": "product category of the item sold",
    "expectedEntities": {"requiredAnyOf": [["dimension:order_items:category",
                                            "dimension:products:category"]]}}]}
```

- The targets are the same `searchTargets` an answer run scores the agent
  against (`skill:eval-answer`'s `reference/ledger-schema.md`), checked by the
  same validator, `import_cases.py`. `target_type` and `search_text` are sent
  as written. A target with no `search_text` is sent with none, and lists every
  entity of its type. `example_values` is not sent: Publisher's request has no
  such field.
- The case's labels are its own `expectedEntities` plus every target's,
  merged. `required`, `requiredAnyOf` and `acceptable` mean what they mean in
  `skill:eval-answer`'s `reference/ledger-schema.md`. A need named twice counts
  once. An any-of group whose members would come back from different targets
  (a measure or a view that answers it) goes on the case's own
  `expectedEntities`, not on one target.
- A target whose `expectedEntities` says `"absent": true` is labelled as not
  found in the model: nothing answers it. That is a label with a right answer,
  unlike a target nobody labelled. `closest` lists the near misses a response
  may offer instead (gross revenue for a "net revenue" target). An absent
  target names no required id; `import_cases.py` refuses one that does.
- A target may carry its own `tags`, for what is hard about that one search
  (`not-found`, `several-readings`). They add to the per-tag breakdown of the
  per-target numbers, and never choose which cases run: `--tag` and
  `slices.json` read case tags only.
- A case may carry its own `scope`, a `get_context` scope object
  (`{"environment", "package", "model_path", "source"}`). It is sent in place
  of the set's scope, so a set built from real requests can span packages,
  model files and pinned sources. `--case-scopes` says every case has one, and
  then no `--environment` or `--package` is needed.
- A case that searches several packages at once carries `scopes`, a list of
  scope objects, in place of `scope`.
- A case may carry `givens` and `filter_params`, objects of name to value,
  sent as written. A model that scopes its rows by a given returns no stored
  values without it, so a value search on such a model needs the given on
  the case.
- A case whose labels name no required field has no recall. It is reported
  as unlabelled with the number of entities that came back, which is the
  measurement for a question the model cannot answer.
- `tags` give the per-tag breakdown.
- `question` goes along as `user_prompt`, which the server records and does
  not match on.

The scope is the set's: `[model]` environment and package from `eval.toml`, and
`targetModelPath` from `set.json`. Flags override each.

## Running it

```bash
python3 skills/eval-loop/scripts/eval.py serve model --set examples/storefront-evals/storefront-retrieval --warm-retrieval
python3 skills/eval-loop/scripts/eval.py replay --set examples/storefront-evals/storefront-retrieval --label baseline-01
```

Start the server with `EMBEDDING_API_KEY` set, or every ranking is lexical
(the run records `retrievalMode`). Before the first case the replay waits for
two semantic answers in a row, as an answer run does; a call answered
`indexing` or `error` mid-run is counted as an error, not scored as a miss.

Another MCP server works too: pass `--mcp-url`, and `--target platform` with
`--environment` and `--package` when its names are not the ones in
`eval.toml`. `--mcp-header 'Name: value'` (repeatable) adds a header to every
request, for example `Authorization`. Only header names are written to the run.

A host whose retrieval service is reachable over plain HTTP can be replayed
without its MCP server: `--get-context-url <url>` posts each request as the
JSON body and reads the reply as the payload, with the `--mcp-header` values as
its headers. Use it to send a header the MCP server does not forward, such as
one that selects a retrieval engine. No readiness probe runs on this route.

When the set came from a versioned bundle, `--set-version` and `--set-manifest`
record the version and the sha256 of its manifest file in `run.json`, beside
the `datasetVersion` and `datasetSha` every run records.

## What it reports

Per case, in `results.jsonl`, and pooled over the scored cases (each case
weighs the same), in `summary.json` and on the console:

| field | meaning |
|---|---|
| `recall_exact` | the share of needs with an id that came back exactly. The headline |
| `recall` | `score_retrieval.py`'s recall, which also credits the same field name under another source, or a name in a returned source's docs. When it is above `recall_exact`, the field reached the result only by that weaker route |
| `precision` | returned ids that are required, in an any-of group, or acceptable, over every returned id, source cards included. None when nothing came back, so the pooled mean leaves that case out and `empty_responses` counts it |
| `missing`, `noise` | the needs that did not come back, and the returned ids that are not labelled |
| `targets` | per target: the recall and precision of what THAT search returned against the target's own `expectedEntities`, scored by `target_outcomes.py` (`skill:eval-answer`). Shows which search of a case missed. Pooled as `target_recall` |

`summary.json` also holds `per_target`, the same run pooled over search
targets instead of cases (`target_outcomes.py` in `skill:eval-answer`). These
are the numbers to tune a retrieval engine on, because a case with seven
targets and a case with one otherwise weigh the same:

| field | meaning |
|---|---|
| `recall` | required answers that came back for their own target, over required answers; exact ids only. A target with two required groups counts twice |
| `recall_anywhere` | the same, counting an answer that came back under any target of the call |
| `precision` | of the entities the labelled targets returned, the share that are required, acceptable or a labelled closest field. One ratio over all targets, and source cards count only for a `source` target |
| `best_pick` | targets whose highest-relevance entity is a required answer. Entities tied at the top all count |
| `not_found` | `absent` targets that returned nothing, or only fields the label lists as `closest` or `acceptable` |

`latency_p50_s` and `latency_p95_s` are the request times as the replay saw
them, one request at a time.

`checks` counts four promises that hold for every response whatever the labels
say, each as responses checked and responses that broke it, with examples:

| check | holds when |
|---|---|
| `not_found_says_so` | every target that returned nothing is named in one of the response's `warnings`, by its search text or as "target N". Counted only on a host that returns `warnings` |
| `pinned_source_only` | with a scope that pins one source, every returned entity is on that source's card |
| `one_card_per_source` | no source of one package comes back on two cards. A source that several model files import is one source |
| `cut_says_so` | a response that returns fewer sources than were available (`returned` under `total_available`) carries a warning. Counted only on a host that reports both |

`by_tag` repeats the pooled numbers for each tag, with its own `per_target`. `events.jsonl` holds one
`tool_call` event per case, in the shape an answer run writes, with every
returned id; `ledger.py validate` checks the run like any other.

Compare two replays with `flip_table.py --a <run> --b <run>` (add `--set <set>
--slice <name>` or `--tag` for part of the set). It lists the required answers
the second run gained and lost for their own target, the best picks and
not-found targets that changed, and precision and latency side by side. A
target whose search or label differs between the runs is set aside, not
counted as a flip.

Two runs compare only when they replayed the same cases (`datasetSha`, or
`setVersion` and `setManifestSha256`) against the same retriever
(`retrievalMode`). Run the same configuration twice first: the gap between
those two runs is the noise any change has to beat.
