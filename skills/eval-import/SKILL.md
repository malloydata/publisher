---
name: eval-import
description: 'Turn a list of questions into an eval set, whatever shape it arrived in: a JSONL a customer sent, a CSV, a spreadsheet export, a markdown doc, an email thread, or a pull from production logs. Classify each item by what came WITH the question (a query, a number, prose criteria, or nothing), write cases.jsonl and set.json per reference/case-format.md, keep the file as it arrived, and seal each question so a later edit is detectable. Never marks a golden verified and never invents a question. Use when questions arrive from outside and need to become a set, or before the first run against a set nobody here authored.'
---

# Import questions into an eval set

Questions arrive in whatever shape their author had. This turns them into
`evals/<set>/`, ready for a run, and records what each key is actually worth.

**Scope boundary:** cases only. This never answers a question, scores an
answer, or edits a model. It is `skill:eval-loop`'s scrape step, expanded,
and it is where the set's honesty is decided.

The file shapes are defined once, in `reference/ledger-schema.md` of
`skill:eval-answer`. Read them there; this skill does not restate them.
`reference/case-format.md` covers what is specific to an import: which fields
an arriving item maps to, and the four physical formats.

## Where the set goes

Two separate rules, and "package" blurs them:

- **The same git repository as the model it evaluates**, so one commit pins
  the model and the ledger together. That is what a checkpoint is.
- **Never inside the directory tree the answerer's package serves.** The
  answerer can read files in the served tree, and gold there is a
  contamination path.

So the set is a SIBLING of the package under test, in its repo. A repo with
`ecommerce/` under test gets `evals/ecommerce-questions/`, beside it.

## Step 1: keep the file as it arrived

Copy it verbatim into `evals/<set>/as-received/`, and never edit that copy.
Not the encoding, not the line endings, not an obvious typo.

It is there so a later disagreement is about the key and not about whether
somebody transcribed the question correctly. That disagreement is guaranteed:
`skill:eval-loop`'s `reference/auditing-an-answer-key.md` is about what happens
when it arrives, and its first check is the question against this file.

If the source cannot be committed, because it carries names, addresses,
internal URLs, or row values somebody pasted into a thread, say so. Record in
`set.json` where the original lives and a hash of it, note under
`sourceNote` that the copy is withheld and why, and tell the user that the
byte-for-byte check is now impossible for anyone without the original. Do not
redact silently: a redaction you got wrong becomes the provenance record.

## Step 2: classify each item by what arrived WITH the question

This is the whole job, and getting it wrong is how an unverified number gets
scored as a fact.

| what arrived | write | scorable on day one |
|---|---|---|
| the question only | a case with **no `golden`** | no. It measures coverage and discoverability, which is a real measurement |
| question + their query + a number | `golden.value`, their query as `canonicalQuery`, `verifiedBy: authored_query`, `status: provisional` | after step 3 agrees |
| question + a number, no query | `golden.value`, `status: provisional`, no `verifiedBy` | no. Somebody typed a number |
| question + prose criteria | see step 4 | depends on what the criterion is |

Two rules over the whole table:

**No golden holding a VALUE imports as `verified`.** `verified` means two
differently shaped derivations agreed through the truth package, and an import
has performed neither. `provisional` goldens are unscorable by design:
`skill:eval-answer` issues `verdict: null` on them. That is not a gap to work
around.

It is also not a dead end, and it used to read like one. The way out is
`verify_goldens.py --promote`, which marks a `provisional` golden `verified`
once its value re-derives cleanly from the truth package and a second
derivation exists. It is the only thing anywhere that writes `golden.status`.
Tell the user that command when you hand over a set of provisionals, because
until it is run the set scores nothing. A set that arrives with 60 typed numbers genuinely has 60 unverified
numbers, and the number it can honestly produce on day one is a coverage score.

The exception is a golden that holds no value at all, and there are two
kinds. Prose criteria (`kind: criteria`, step 4) have nothing to re-derive:
the author's words ARE the key. So does a case whose stated answer is that the
data is not there (`kind: unanswerable`), where the pass is a refusal that
names what is missing and a confident number is the failure. Both import
`verified` with `verifiedBy: authored_criteria` and score immediately. The
rule is about numbers, because a number is the thing that can be wrong while
looking right.

Watch for the second one in what arrives. A criterion reading "a refusal that
names the missing data" is not a rubric clause on a normal case, it is the
whole key, and importing it as `criteria` on a case with a value would make a
refusal fail.

**A question with no golden is a case, not a reject.** Do not hold questions
back until somebody derives keys. A set of bare questions already measures
whether the model can express an answer at all, and the answers it produces
are what the keys get derived from.

## Step 3: run their query, if they gave one

Do it at import, before any run. It is the cheapest finding in the whole loop.

Execute their query with `execute_query` against the model under test and
compare with the number they stated. Three outcomes, three different things
learned:

- **It returns their number.** `verifiedBy: authored_query`. Keep
  `status: provisional`: this proves the number came from that query, and NOT
  that the query is right. It ran against the model under test, so a model bug
  certifies its own golden, which is the exact circularity the truth package
  exists to break. It is still far more than a typed number is worth.
- **It returns a different number.** A finding on day one, before a single
  answer was scored. Record BOTH numbers on the case and leave the status
  `provisional`. Do not quietly adopt either one: their number may be stale,
  the model may have moved, or the query may have always been wrong, and
  which of those it is decides who owns the fix.
- **It does not compile.** Also a finding, and usually the most informative
  one: their query names entities this model version does not have. Record the
  error. This is what a coverage gap looks like before anyone has phrased a
  question about it.

Never repair their query to make it run. A query that does not compile against
this model is evidence about this model.

## Step 4: prose criteria are two different things

Decide which, per criterion. The test:

> **Can you write a query whose result settles it?**

**If yes, it is a value in disguise.** "Should be about 4.2M." "The top
category is Denim." "Should come back with twelve rows." There is one right
answer, so the criterion is not the key, it describes one. Write it as
`golden.rubric`, derive the value, and until it is derived the case is
`provisional` and unscorable. Grading such a criterion as prose is how an
unverified number passes: the judge reads "about 4.2M", the answer says 4.2M,
and nothing ever checked whether 4.2M is right.

**If no, it is a judgment about the shape of the answer**, and prose is the
right home for it. "Must break the total out by region." "Must say the window
excludes returns." "Must not use list price." No query settles these, the
judge grades them against the answer, and these cases score on day one. Write
the golden as `kind: criteria` with no `value`, the criteria as
`golden.rubric`, and `verifiedBy: authored_criteria`. Use `golden.mustState`
and `golden.mustNotUse` where the criterion fits those fields exactly, per
`reference/case-format.md`.

Most arriving criteria are mixed: one sentence naming a number and a shape.
Split it. The number half goes provisional; the shape half scores.

Where you cannot decide, mark the case and report it rather than guessing. A
criterion nobody could classify is a question for its author, and
`skill:eval-loop`'s golden side door is where it waits.

## Step 5: never change a question, and seal it

The question is the stimulus. It is what a human asked, and it is not yours to
tidy. Do not fix a typo, normalize casing, expand an abbreviation, or sharpen a
vague phrase. Vagueness is often the thing the case tests, and narrowing a
question to match what an answerer keeps doing deletes the test and hands the
model a pass. `reference/auditing-an-answer-key.md` has the audit where that
happened to four questions.

At conversion, stamp `questionSha`: the SHA-256 of the exact question text, as
written. `scripts/import_cases.py --stamp` does it, and refuses to overwrite a
stamp that already exists.

The seal is not a derivation from the arriving file, which is why it works for
an email thread as well as a CSV. It records the decision you made at
conversion about what the question is. From then on, `verify_goldens.py`
compares the stamp against the question in `cases.jsonl` on every audit, and a
mismatch means somebody edited a question after it was imported.

A question that genuinely has to change gets a **new `qid`**, not a new stamp.
A changed question is a different stimulus, and scores on the old wording must
not roll into the new one.

## Step 6: freeze the split

Every case gets `split`: `dev` or `holdout`. Diagnose and improve read dev only;
the acceptance check runs both, and a set that is all dev cannot defend an
accept. `skill:eval-loop` owns the rule; import is where it is frozen, because
a split chosen after the first failures is not a holdout.

Prefer variety over volume. Cases differing in grain, source, filter shape and
phrasing are what move a measurement.

## Step 7: validate, and report what the set is worth

```
python3 scripts/import_cases.py --set evals/<set> --stamp
```

It checks the required fields, unique `qid`s, a `split` on every case, a golden
status from the allowed four, that nothing claims `verified` without the
evidence for it, and that no question has drifted from its stamp. Exit 0 clean,
1 with a finding, 2 on a usage error.

Then tell the user, in these terms, what arrived:

```
47 cases from 50 lines
  12 scorable now
  29 provisional (21 with their query, 5 a number alone, 3 nothing to compare yet)
  6 no golden (question only)
```

Say what turns the 29 into scorable cases, in the same breath: a truth package
(`init_truth_package.py`), then `verify_goldens.py --set <set> --publisher
<truth> --promote`. A reader told only the counts has no way to know the set is
not simply broken, and the provisional bucket is the one that looks like
progress and is not.

The three provisional buckets are three different amounts of work, which is
why they are counted apart. "With their query" needs a re-run. "A number
alone" needs a derivation. "Nothing to compare yet" is a case whose criteria
describe a key nobody has derived, and it is the one most easily mistaken for
progress: measured on a real markdown thread, all three of its asks landed
there.

Say the scorable count out loud when you report it, not just the case count.
"47 cases" reads like a 47-case measurement, and 12 is the number a first run
can actually score. Read the two numbers on the first line against each other
too: 47 from 50 means three lines did not parse, and the findings say which.

## Never

- invent a question, or a number, or a criterion;
- edit a question, including to fix a typo;
- mark a golden that holds a value `verified`;
- drop an item you could not classify, instead of reporting it;
- put `evals/` inside the served package tree;
- score anything. That is `skill:eval-answer`.

## Related skills

- `skill:eval-loop`: the conductor. Import is its scrape step; its
  `reference/auditing-an-answer-key.md` is the procedure for a key you doubt,
  and its golden side door is where an unclassifiable item waits.
- `skill:eval-answer`: `reference/ledger-schema.md` defines every file and
  field this skill writes.
- Pulling questions from production logs is the other good source, and where
  those logs live is a host concern. Look for a host-specific log-fetching
  skill; this skill takes over once you have the text.
