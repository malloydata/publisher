<!-- What an arriving item maps to, and how to read the four formats questions actually arrive in. The file shapes themselves are in eval-answer/reference/ledger-schema.md; this is only what is specific to an import. -->

# Reading an arriving question list

`reference/ledger-schema.md` in `skill:eval-answer` defines `set.json`,
`cases.jsonl` and every golden field. This covers two things it does not: what
an arriving item maps onto, and how the four formats break.

## What an import fills in

On the case:

| field | from the source | when it is missing |
|---|---|---|
| `qid` | their id, if they gave one, prefixed with the set name | derive from the question: a slug of its first words plus a counter. Stable, because scores are keyed on it |
| `question` | the question text, byte for byte | it is not a case. Do not reconstruct a question from a criterion |
| `questionSha` | never from the source | stamped at conversion by `import_cases.py --stamp` |
| `split` | rarely present | you choose, and freeze it at import |
| `source` | how it arrived: the filename, or the log query | say `unknown` rather than guessing a provenance |
| `golden` | see the classification table in `SKILL.md` | absent. A case with no golden is legitimate |
| `expectedEntities` | almost never present | leave it out. Guessing which fields an answer needs invents a retrieval expectation nobody stated, and it scores as a retrieval miss forever |

`expectedEntities` is the one most worth leaving empty. A wrong `required` id
cannot be delivered by any run, so it reads as a model failure on every case
it touches: five copied ids cost a real set two days.

On `set.json`: `name`, `description`, `datasetVersion: 1`,
`targetModelPath`, and a `sourceNote` saying where the questions came from and
what shape they arrived in. `truthPackage` is usually absent at import, and
until it exists no golden can reach `verified`. `init_truth_package.py` in
`skill:eval-answer` scaffolds one.

## Their field names will not be your field names

An arriving JSONL is somebody else's schema. Map it explicitly rather than
trusting a name that looks familiar, and write down the mapping you used in
`sourceNote`. Names seen in real sets:

| theirs | usually means | trap |
|---|---|---|
| `prompt`, `query`, `input`, `nl_question` | the question | `query` is the trap: in half of these it is the SQL or Malloy they ran, not the question |
| `answer`, `expected`, `output`, `ground_truth` | a golden value, prose, or both mixed in one string | a paragraph under `answer` is criteria, not a value. Classify it, do not store it as a number |
| `sql`, `malloy`, `gold_query` | their derivation | run it (`SKILL.md` step 3). This is the field that upgrades a typed number into something checkable |
| `criteria`, `rubric`, `assertions`, `must_include` | prose criteria | apply the query test in `SKILL.md` step 4 to EACH one, not to the field |
| `id`, `case_id`, `uuid` | their id | keep it, prefixed. Two sets from one customer collide otherwise |
| `difficulty`, `category`, `tags` | their own labels | keep as-is on the case for slicing. Never let one become `split` |

A field you cannot map is a question for its author. Keep it verbatim under a
`sourceFields` object on the case rather than discarding it or forcing it into
a field it does not mean.

## The four formats

**JSONL.** One object per line, and the easy one. The failures are all in the
mapping above, plus two mechanical ones: a trailing incomplete line, and a
`\n` inside a question that a naive line split breaks. Parse it as JSON per
line, and count the lines you produced against the lines they sent. Report the
count in the import summary: an import that silently dropped 3 of 50 is a
measurement on 47 cases claiming to be one on 50.

**CSV or a spreadsheet export.** Questions contain commas, quotes and newlines,
so use a real CSV reader, never a split on `,`. Watch for a header row that is
not row one (an exported sheet often has a title and a blank line above it), for
merged cells arriving as empty strings that belong to the row above, and for a
number that arrived as text with a thousands separator or a currency symbol. A
number you cannot parse cleanly is a string; keep it as their text and say so,
rather than reading `1,234` as `1.234` or as `1234` without checking.

**A markdown doc or an email thread.** The questions are prose among other
prose, so the hard part is deciding what IS a question. Rules that hold up:
a heading or a numbered list item is usually a case boundary; a sentence
ending in a question mark inside a paragraph about something else usually is
not. Criteria attached to a question tend to follow it as a bullet list or a
"should" sentence. Quote a boundary you are unsure of and ask, rather than
splitting on your best guess: a question invented by a bad split is
indistinguishable, later, from one a human asked. Nothing in this format is
byte-clean, which makes the verbatim copy in `as-received/` load-bearing.

**A pull from production logs.** The best source, because real traffic asks
what people actually ask, and the messiest. Three things to do:
de-duplicate near-identical prompts before you import 40 of one question;
strip the parts that are not the question (a system preamble, a UI-injected
scope line, a pasted table); and keep the trace or request id on the case as
`source`, so a case can be taken back to the session it came from. Almost
none of these arrive with a golden, which is fine: bare questions are a
coverage measurement. Where the log also holds the query the agent ran, that
is NOT a golden. It is what an unevaluated agent did, and importing it as a
key scores the model against its own past behavior.

## A worked line

Theirs:

```json
{"case_id": "q-014", "prompt": "How did denim sell last quarter versus the one before?",
 "sql": "SELECT ...", "answer": "Up 12.4% to 1.83M. Should break out by channel.",
 "difficulty": "hard"}
```

Yours, after applying step 3 and step 4:

```json
{"qid": "ecom-q-014", "question": "How did denim sell last quarter versus the one before?",
 "questionSha": "<stamped>", "split": "dev", "source": "questions.jsonl",
 "golden": {"status": "provisional", "kind": "scalar", "value": 1830000,
            "canonicalQuery": "SELECT ...", "verifiedBy": "authored_query",
            "rubric": "Reports the quarter-over-quarter change for denim, and breaks the figure out by channel."},
 "sourceFields": {"difficulty": "hard"}}
```

And one that arrived as criteria alone, with no number anywhere:

```json
{"qid": "ecom-q-021", "question": "Which channels are underperforming?",
 "questionSha": "<stamped>", "split": "dev", "source": "questions.jsonl",
 "golden": {"status": "verified", "kind": "criteria",
            "verifiedBy": "authored_criteria",
            "rubric": "Names the channels and says what it measured underperformance against. Does not use list price.",
            "mustNotUse": ["retail_price"]}}
```

No query settles "underperforming", so there is nothing to derive and nothing
that can be numerically wrong: the clauses are the whole key, and this case
scores on the first run.

`criteria` and `unanswerable` are the two kinds an import may mark `verified`,
and they arrive looking alike. A criterion reading "PASS: a refusal that names
the missing data" belongs to the second: the key is that the data is absent,
so `kind: unanswerable`, and filing it as `criteria` beside a value would make
a correct refusal fail.

What happened to that one `answer` string: the number became a provisional
value, checkable because they sent the query that produced it; "should break
out by channel" is a shape nothing can settle with a query, so it became
rubric prose and scores on day one; and "Up 12.4%" is a second figure the
rubric now has to state consistently with the value, which
`verify_goldens.py` reads the accepting clause for.
