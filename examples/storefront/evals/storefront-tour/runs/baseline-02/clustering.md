I'll start by reading the skill's output contract.I read the contract at `.claude/skills/eval-diagnose/reference/output-contract.md` and am emitting the **Per run, clustering** object (`clusters` + `reasoning`), not the per-case one.

## What I was handed

12 distinct qids: 10 passes, 2 answer failures, and 1 finding sitting on a **passing** case (`storefront-tour-top-category` appears in both lists — correct answer, retrieval miss, exactly the case the skill says to diagnose anyway).

**Coverage caveat:** no `diagnose.py` coverage account came with this input, so I cannot state which non-passing cases were holdout, contaminated, out-of-verdict, or selected-and-not-diagnosed. These three clusters cover the three diagnoses I was given, and I make no claim about the rest of the run. The `summer-sales` diagnosis names a Question 6 ("net sales excluding cancelled and returned items") that is not among the 12 qids here, so at minimum one adjacent case is outside my view.

## Merges considered

**`summer-sales` + `top-customer-spend`.** Both are, abstractly, "a business convention lives in someone's head (or inside a view) and not on the fields an agent searches." I kept them apart. They differ on every operational axis: one is `insufficient`/`COVERAGE` (nothing encoding a season window exists anywhere), the other is `sufficient`/`WRONG-PICK` (the fields arrived; nothing on them warned). Different entities, different files, different edits — the skill's own rule that two coverage gaps about different entities are two clusters applies, and the per-case diagnosis for summer-sales pre-emptively reached the same conclusion about its own sibling case. Merging them yields an edit that closes neither cleanly.

The controls actively support keeping the abstraction from becoming a cluster: **`storefront-tour-net-sales-2025` passed** while depending on exactly that "business exclusion convention not in the model" shape — at 4 get_context calls, 5 executes and 12 turns, the second-most expensive pass in the run. So "undocumented business convention" as a single root cause does not predict failure; the specific entity does.

**Falsifier check against the passes.** I found no behaviour that separates the two groups, so no cluster is routed on behaviour:

- *"Agent skipped the curated view"* (tempting for `top-customer-spend`, whose fix is already encoded in `top_customers`): only 2 of 10 passes touched a view at all. Eight passed without one. View usage does not separate the groups; the model-owned documentation gap survives only because it is a claim about what the fields say, not about what the agent did.
- *"Bare targets"* (`top-category` carries 2 `targetsWithoutSearchText`, the run's high): `orders-per-customer` passed with 1. Present in both groups; it stays a confound, not the cause.
- *"Call volume"*: `top-category`'s 4 get_context / 6 targets is matched by `net-sales-2025`'s 4 / 7, a pass. The extra calls there are the documented recovery from the empty first call, i.e. downstream of the defect.

`NEVER-ASKED` on `top-customer-spend` stays a contributing code for this reason and is not promoted.

The retrieval cluster rests on a single replay divergence (n=1, same call, ranked list one time and zero results the next), which is why I hold it at medium confidence and low severity despite it being the cleanest-owned finding of the three.

```json
{"clusters": [
  {"cluster_id": "customer-name-non-unique-undocumented",
    "qids": ["storefront-tour-top-customer-spend"],
    "owner": "model",
    "component": "construction",
    "codes": ["WRONG-PICK"],
    "rootCause": "Neither customers.full_name nor customer_id carries any field-level statement that names are non-unique (1,000 customers, 769 distinct names), so an agent grouping by name alone silently merges distinct people; the correcting convention exists only inside the top_customers view body, where an agent that answers without reaching for that view never sees it.",
    "evidence": "Context was sufficient -- both fields came back in the same get_context response -- so the defect is what the fields say, not what arrived. Falsified if the field docs do carry a uniqueness warning, or if the 769/1000 figure is wrong. Notably NOT a behavioural cluster: 8 of the 10 passing cases used no view at all, so 'agent skipped the view' occurs at high rate in the passes and cannot be the root cause; contributing code NEVER-ASKED is held down for the same reason. Any question ranking or naming an individual customer shares this cause.",
    "confidence": "high"},
  {"cluster_id": "season-window-convention-absent",
    "qids": ["storefront-tour-summer-sales-2025"],
    "owner": "model",
    "component": "get_context/model",
    "codes": ["COVERAGE"],
    "rootCause": "No field, given, filter, or doc anywhere in storefront.malloy encodes the business's summer window (25 May-15 Sep), leaving no entity for any utterance to retrieve; the agent fell back to the meteorological Jun-Aug, the wrong-answer figure the rubric names.",
    "evidence": "Insufficient context, not a construction error -- the entity does not exist to be returned. The model already carries the matching pattern one file over (givens.malloy's SINCE date given), which shows the gap is entity-specific rather than structural. Falsified if a season window is encoded somewhere I was not shown. Deliberately NOT merged with the customer-name cluster despite both being 'business convention not on a searchable entity': storefront-tour-net-sales-2025 passed while depending on that same abstract shape, so the abstraction does not predict failure and an edit aimed at it would close neither case cleanly. The Question 6 net-sales exclusion named in the per-case diagnosis is a third instance of the shape and belongs in its own cluster, outside the qids I was given.",
    "confidence": "high"},
  {"cluster_id": "get-context-empty-result-nondeterminism",
    "qids": ["storefront-tour-top-category"],
    "owner": "retrieval",
    "component": "get_context/retrieval",
    "codes": ["RETRIEVAL"],
    "rootCause": "A package-scoped, on-target first get_context call returned 0 results across all three targets, where an identical replay returned a full ranked list (order_items:total_sales and overview:total_sales at ~0.74-0.75); the same request produced different result sets on different sessions.",
    "evidence": "The replay divergence is the whole case for retrieval ownership -- same utterance, same scope, same model, different outcome -- so this is falsified the moment the empty return reproduces on replay, which would move it back to NOT-RETURNED against the model. n=1, hence medium confidence. Reported on a case whose ANSWER was correct: the agent recovered via a directly-named view and answered through overview's total_sales, which is inherited unchanged from order_items via extend and is right only while the dashboard never shadows that measure -- a second, latent alias-collision concern I am not clustering, since one occurrence does not establish it. Behavioural confounds checked and rejected: this case's 2 bare targets and 4 get_context calls both appear at comparable levels among the passes (orders-per-customer, net-sales-2025).",
    "confidence": "medium"}
 ],
 "reasoning": "Three diagnoses, three clusters, all singletons -- I found no pair sharing a cause. The one merge I seriously weighed was summer-sales + top-customer-spend as a single 'business convention not attached to a searchable entity' cluster. I rejected it on three grounds: they sit at opposite ends of the sufficiency axis (nothing exists vs. everything arrived unlabelled), they name different entities in different files so one edit cannot close both, and the controls contain a live counterexample -- storefront-tour-net-sales-2025 passed while depending on exactly that abstract shape, so the abstraction has no discriminating power. The per-case diagnosis for summer-sales independently reached the same split about its own sibling. I did not merge top-category with anything: different owner (retrieval, the only non-model owner here), and only owner:model may proceed to eval-improve, so keeping it separate is what stops a tool defect being routed into the semantic model. Against the CONTROLS block I tested three behavioural framings -- agent skipped the curated view, targets carrying no search_text, and retrieval call volume -- and every one of them occurs at a similar or higher rate among the 10 passes (8 of 10 passes used no view; orders-per-customer passed with a bare target; net-sales-2025 passed with 4 get_context calls and 7 targets). None separates the groups, so no cluster here is routed on behaviour and NEVER-ASKED stays contributing rather than primary. What I cannot speak to: no coverage account accompanied this input, so I make no claim that these three clusters cover the run's non-passing cases beyond the three diagnoses handed to me."}
```