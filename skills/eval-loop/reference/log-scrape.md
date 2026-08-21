# Scraping cases from production logs

The scrape step's best source is real traffic. Invented questions test what you
thought to ask; logs test what people actually asked, in their own words, and
question variety is what moves an evaluation (far more than repeating the same
case).

This file is the adapter contract: what a log-derived case looks like, what can
and cannot be scored from logs alone, and the order to do it in. It is
deliberately generic. Where your platform keeps its logs, what its tables are
called, and how you authenticate are host concerns; look for a host-specific
log-fetching skill, and keep its internals out of this tree.

## First, know which archetype you have

The single most important question is what the client propagated, because it
decides which components you can score at all.

| Archetype | What the log carries | What you can score |
|---|---|---|
| **Full-session trace** (a first-party app that propagates a session id) | the user's prompt, the retrieval calls it produced, and often a stored transcript with the assistant's answer | everything: answer, retrieval, and whether the agent asked for the right things |
| **Machine integration** (a service calling the API directly) | request payloads and, if responses are captured, what retrieval returned. No prompt, no session, ever | retrieval only |
| **Connector client** (a third-party host talking over MCP) | search text, and the user's question ONLY if the client forwards it. Usually no session id | retrieval, plus the answer side when the prompt is present |

Do not average these together. A machine integration's traffic is templated and
will swamp a handful of real human sessions.

## What a log line becomes in the ledger

A retrieval call becomes an **intent** (`intents.jsonl`): the search text, the
entity type, and a description you write of what the user meant. If the logged
response is captured, store it as the retrieval baseline. Judge the entities that
came back exactly as `skill:eval-answer` describes.

A user prompt becomes a **case** (`cases.jsonl`). It only becomes a *scorable*
case once it has a golden, and logs rarely hand you one. Three ways to get there,
best first: a stored transcript containing the answer the user accepted; a value
you re-derive yourself and verify by execution; or, failing both, keep the case
with `golden.status: candidate` and score retrieval only.

**A search text is not a user intent.** It is the agent's guess at what to look
for, already downstream of the question. Scoring retrieval against it measures
retrieval given that guess. It cannot tell you whether the guess was reasonable,
which is a different failure with a different owner: a concept genuinely missing
from the model is the model's problem, while an utterance unrelated to what the
user asked is the agent's. Without the originating prompt you cannot separate
those two, and you cannot honestly fill an intent's `valid` flag. Say so in the
report rather than guessing.

## Stitching a session

- **Join on the session id.** Not on the tenant or organization, which client
  code often fails to send on some endpoints, and not on a trace id, which
  frequently does not survive a hop between services. Find the sessions in
  whichever table has a reliable tenant, then look each id up in the others.
- **When there is no session id**, cluster by (user identity, tenant) and a time
  gap, starting a new pseudo-session after roughly thirty minutes of silence.
  Treat the result as an inference, and label it as one.
- **Verify the stitch on one known session before trusting it in bulk.** A prompt
  followed within a minute by retrieval calls that plausibly serve it is the
  confirmation you want.

## Constraints to check before promising an eval

Check each of these against your own logs, because each one silently changes what
the eval can claim:

1. **Is the assistant's answer stored anywhere?** Often only the retrieval
   endpoint has its response captured, and the answer exists only in a separate
   transcript store, or not at all. No stored answer means no answer-side golden
   from logs alone.
2. **Are query results stored?** Usually not, sometimes only errors. If not, a
   query-correctness eval must re-execute the logged query, which means you need
   the model it ran against.
3. **Can you replay at all?** The log gives you the request and what production
   returned. Reproducing it needs that package served locally. If you cannot get
   it, you can still score retrieval as a regression against the logged response,
   which is a real measurement with a narrower claim.
4. **Whose traffic is it?** Internal dogfooding, load tests, and probers can
   outnumber real users, and internal users appear inside customer tenants. Filter
   by identity, not by tenant, and record the split. If most traffic is internal,
   the honest headline is the small real number.
5. **Which rows are not searches?** Listing or browse-mode calls, with an entity
   type and no search text, exercise neither the model nor the ranking. Exclude
   them from retrieval metrics rather than letting them inflate coverage.

## Order of work

1. **Volume triage.** Calls, distinct users, sessions, and last activity per
   tenant. Pick one tenant and one window; state both.
2. **Dedupe, then sample.** Real traffic is heavily templated, and thousands of
   calls routinely collapse to a hundred distinct search texts. Group by entity
   type and search text, and sample distinct shapes weighted by frequency. Never
   sample raw rows.
3. **Add a recency tail.** Top-N alone describes the steady state; the newest
   distinct rows are where new behavior shows up.
4. **Pick a handful by hand**, spanning entity types, users, and sessions, and
   favouring variety over volume.
5. **Hydrate each pick** into the ledger: its request, the logged response, its
   sibling calls in the same session, and the transcript if one exists.

## Handling what logs contain

Production logs hold user prompts and user identities. Two rules follow.

Derive PII-free artifacts. An intent row needs the search text, the entity type,
and your description; it does not need an email address. Keep raw pulls outside
the model repository, and commit only the derived ledger.

Never let a raw pull reach the package the answerer queries, and never let it
reach a commit unreviewed. A prompt is also gold-adjacent: a stored transcript
contains the answer, so an answerer that can read transcripts is contaminated by
construction.

## What logs will not give you

Logs show what was asked, never what should have been asked. Concepts users
wanted and no agent ever searched for are invisible here. A log-derived set is a
strong sample of real demand and a poor map of coverage, so pair it with cases
written from the model's own documented scope.
