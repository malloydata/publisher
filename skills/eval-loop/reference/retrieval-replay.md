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

- `target_type` and `search_text` are sent as written. `search_text` is
  required: a target without one lists every entity of its type instead of
  searching.
- The case's labels are its own `expectedEntities` plus every target's,
  merged. `required`, `requiredAnyOf` and `acceptable` mean what they mean in
  `skill:eval-answer`'s `reference/ledger-schema.md`. A need named twice counts
  once. An any-of group whose members would come back from different targets
  (a measure or a view that answers it) goes on the case's own
  `expectedEntities`, not on one target.
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
python3 skills/eval-loop/scripts/eval.py serve model --set examples/storefront/evals/storefront-retrieval --warm-retrieval
python3 skills/eval-loop/scripts/eval.py replay --set examples/storefront/evals/storefront-retrieval --label baseline-01
```

Start the server with `EMBEDDING_API_KEY` set, or every ranking is lexical
(the run records `retrievalMode`). Before the first case the replay waits for
two semantic answers in a row, as an answer run does; a call answered
`indexing` or `error` mid-run is counted as an error, not scored as a miss.

Another MCP server works too: pass `--mcp-url`, and `--target platform` with
`--environment` and `--package` when its names are not the ones in
`eval.toml`. `--mcp-header 'Name: value'` (repeatable) adds a header to every
request, for example `Authorization`. Only header names are written to the run.

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

`by_tag` repeats the pooled numbers for each tag. `events.jsonl` holds one
`tool_call` event per case, in the shape an answer run writes, with every
returned id; `ledger.py validate` checks the run like any other.

Two runs compare only when they replayed the same cases (`datasetSha`, or
`setVersion` and `setManifestSha256`) against the same retriever
(`retrievalMode`). Run the same configuration twice first: the gap between
those two runs is the noise any change has to beat.
