<!-- How a person or a team uses the loop day to day: tag cases, fix on a slice, accept, and let full runs record each version. Read this first; the other reference files are the detail behind each step. -->

# Working the loop, alone or as a team

Two kinds of run do two different jobs. Keep them apart and the loop stays
cheap.

- **An experiment** asks "does this change fix that?" It runs a slice of the
  set, a few cases, as often as you like. It is yours, and it stays on your
  machine.
- **A record** asks "how good is this version?" It runs the whole set on the
  main line, less often: on a schedule, or after a publish. It is the team's,
  and it is what every experiment is compared with.

You never re-run a full baseline to try an idea. You run the idea on a slice
and compare it with the newest record.

## Once per set

```bash
E="python3 skills/eval-loop/scripts/eval.py"
$E check --set <set>                  # names every gap; ready when it says so
$E serve model --set <set>            # no Publisher clone needed: see setting-up-a-set.md
$E serve truth --set <set>
$E verify --set <set> --promote       # each key derived twice, then trusted
$E run --set <set> --label baseline-01   # the first record
```

Tag every case by what it exercises (`tags` in cases.jsonl: `join`,
`time-window`, `ambiguous-docs`, whatever your failures turn out to be about),
and name the slices you work on in `slices.json`. Tags are yours: they are the
categories of failure your model has, and nothing generic replaces them.

## An experiment

```bash
$E experiment start tip-fix --set <set>          # your edit, served beside the main package
$E run --set <set> --experiment tip-fix --slice tips --label tip-01
$E run --set <set> --experiment tip-fix --slice tips --label tip-02
$E accept --set <set> --run tip-01 --run tip-02  # against the newest record
$E experiment stop tip-fix --set <set>
```

1. **Watch it fail first.** The slice holds the cases the edit is meant to
   fix, a few that pass and must keep passing, and some holdout cases. It is
   named in `slices.json` before any verdict exists.
2. **Make the change** in the model or its docs. `experiment start` serves it
   as `<package>.<name>` on the same server, so the main package, and the
   record measured on it, stay as they were. Several experiments can run at
   once.
3. **Watch it pass, twice.** Two runs with fresh answerers, because one pass
   can be luck.
4. **Accept.** `eval.py accept` checks the rules (acceptance-check.md): no
   case that passed in the record fails now, every target passes in both runs,
   holdout cases were answered, nothing but the model changed. It prints a
   commit message naming the runs. A speed edit accepts on a query replay
   instead (query-performance.md).

When it is not accepted, change the edit or drop it. The record is untouched
either way.

Before the agent runs at all, a retrieval problem can be fixed cheaper:
`eval.py replay` re-asks `get_context` the stored questions with no answerer
(retrieval-replay.md).

## A record

```bash
$E publish --set <set>                # the accepted edits, served and built
$E routine --set <set>                # check, verify, run, diagnose, package, replay, history
```

`publish` puts the model on disk in front of the answerer: it publishes the
package (with `versioning = true`, as the version its publisher.json names),
confirms it loaded, and builds anything it persists. `routine` then runs the
whole set and records it. Run it on a schedule, or after each publish. It is
one command so that whatever runs it, a person, CI or an agent, needs nothing
else.

`eval.py versions` shows how each version scored and how fast its queries
ran. `eval.py history` builds the same as a page.

## Alone, or as a team

The commands are the same. The only difference is a `[store]` in eval.toml,
where finished runs are shared:

| You are | eval.toml | What happens |
|---|---|---|
| alone | nothing | runs stay in your workdir |
| a team with a shared folder or bucket | `[store] path = "..."` | every record is pushed when it finishes; `--baseline record` pulls the newest one before it compares |
| a team on anything else | `[store] push / pull / list` commands | the same, through your commands |

The rules hold everywhere:

- A run is shared whole, once, and never changes after.
- Records share themselves. An experiment stays yours unless you
  `eval.py push` it.
- Runs never go in git. The set does, beside the package; the runs do not
  (sharing-runs.md).
- The store holds the answer key, so the agent under test must not be able
  to read it.

## When the set changes

Editing a case, a key or a convention does not throw the record away. A
comparison sets aside each case whose question or key changed since the
record and compares the rest, so you keep working against the same record
until the next one replaces it.
