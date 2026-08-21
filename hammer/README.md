<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Hammer — a scenario harness for the Publisher

Hammer runs **markdown scenarios** against a **real** Publisher server. Each
scenario is a folder with a `scenario.md` that reads like a story — seed some
data, write a model, publish, query, assert on the rows — and one command
executes it end to end: it stands up real infrastructure (a throwaway Postgres,
a local DuckLake), builds and boots the actual server, runs the steps, prints a
check report, and tears everything down. No cloud creds; nothing to hand-wire.

It runs **separately** from the `bun test` unit/integration suites: one command,
real infrastructure, production-shaped confidence. Today you run it by hand.
Wiring it into CI is an accepted follow-up — reviewers have asked more than once —
and the harness is built for it: the whole suite is ~50s, self-contained apart from
Docker, order-independent, and exits nonzero on failure.

The step vocabulary lives in **[GRAMMAR.md](GRAMMAR.md)**. This file is about
what a scenario is _for_.

---

## Why this exists

Testing a data-intensive product is its own kind of hard, and it fails in a
recognisable way.

An integration test needs a running system, so it opens with pages of setup. Then
it needs data, and both of the usual answers cost something. **A shared seed
fixture**: loaded once, which is efficient — but if tests can _change_ it, it
never quite has what the next one needs, so you add a row and three other tests
change their answers. **Per-test data**: nothing to collide over, but pages of
loader code and inline structs, and it still collides anyway if everything shares
the same tables.

So people copy. You find the closest working test, paste it, change a few values,
and move on — because writing one from scratch means re-deriving all that setup.
The suite grows by cloning. Then someone asks "do we have a test for this?" and
the answer is "yeah… somewhere… I think this one?" — and reading it, you spend
several minutes wading through fixtures and loaders before you can even tell what
behaviour it was defending.

Slow, hard to read, occasionally flaky. It grows to some size and plateaus,
because navigating it costs more than the confidence it returns. And eventually:
_let's delete the integration tests and start over — the new ones will be better._

This is that new one. So the fair question is why it won't plateau the same way.

**The data lives in the scenario, and so does the assertion.** Most rules need
only a handful of rows, and then they belong as a markdown table immediately above
the query that reads them and the rows expected back — no loader, no separate
fixture file to keep in sync.

**Nothing is shared, structurally.** Every scenario gets its own source database,
its own DuckLake catalog, its own environment, and a publisher started from a
config naming only its own packages. What that rules out is specifically the
_mutable_ shared fixture, where adding a row for one test changes three other
answers — there is nothing shared to add a row to. That is what most of the
harness's complexity buys, and it is the reason the suite can keep growing.

Isolation is not free, and bytes are bytes: this suite got sluggish fast enough to
need real work on parallelism and startup cost, and that pressure only grows. A
**sealed** dataset is the escape valve — a historical snapshot nobody edits, of
the kind already used in demos and other user-facing places. It has none of the
failure mode above, because the thing that rots a shared fixture is _mutation_,
not sharing. When a scenario needs data bigger or more realistic than a table of
rows, reaching for one of those is the right move. The line to hold is narrower
than "share nothing": a scenario must never mutate data another scenario reads.
Loading the same immutable snapshot twice is a cost question, not a correctness
one — and cost questions have ordinary engineering answers.

**It is deliberately not code.** Code pulls attention toward the shape of the test
— the helpers, the builders, the mocks — and away from the behaviour being
described. Prose plus a table has nowhere to hide: if a scenario is hard to read,
it is because the _rule_ is unclear, which is worth knowing. It also means a
refactor underneath usually changes nothing here; at most a small part of the
engine moves.

**Both people and agents can read and write them.** A coding agent can add a
scenario without absorbing a test framework, and — the direction that matters more
— anyone, human or agent, can learn what the system actually promises by reading
runnable specs instead of inferring it from implementation.

None of this is new. It is the FitNesse / BDD / executable-specification argument,
and the debt is happily acknowledged. What is specific here is the isolation
model: those approaches usually still sit on a fixture every test can write to,
and that is the part that historically rots.

## What a scenario is

**A scenario states a rule the Publisher commits to, and proves it end to end.**

That is the whole test. Not "here is a sequence I tried and here is what
happened" — a promise, written so a reader who has never opened the source can
tell what the system guarantees.

The distinction that matters is **rule vs. observation**, and it is not the same
as positive vs. negative. Plenty of good scenarios have negative outcomes:

- `givens-refused` — a given-referencing source is refused. That refusal _is_ the
  contract.
- `security-user-sql-cannot-mutate-lake` — the boundary is the promise.
- `off-serves-live` — the kill switch's guarantee.

Those read as spec because each says _this is the rule_. A scenario drifts wrong
when its title and prose describe **what currently happens** instead:

> ~~A host-supplied serve binding is not re-checked for eligibility~~
> **A host-supplied binding must not bypass row-level access control**

Same steps, same assertion, same red — but the first is a bug report that stops
making sense the day it is fixed, and the second is a promise that stays true
forever. Write the second. Put the investigation in a `## Note`.

## Red scenarios, and why there is no `bug-report` tag

A scenario may assert a rule the Publisher does **not yet** meet. Tag it
`known-red`: it fails on purpose, and the fix turns it green.

The runner treats that as a first-class status, not a convention:

- it reports **KRED** rather than FAIL, and **does not fail the run** — so a suite
  carrying known debt can still be green, which is what makes it usable as a gate;
- a known-red that **passes** reports **FIXED** and _does_ fail the run. The rule
  now holds and the tag has become a lie — which is exactly how a fix lands
  without anyone remembering to retire the scenario;
- they get their own report block with ages, taken from `## Note (since=…)`, and
  `--attention-older-than N` filters it the same way it filters the callouts. Debt
  nobody re-reads stops being a decision and becomes furniture, so
  "what have we been red on for a month?" is one flag away. An undated known-red
  always shows — it cannot be proven fresh.

What a scenario must never do is assert the _broken_ behaviour as if it were the
contract. That rots in a specific and nasty way — someone fixes the bug, the
scenario goes red, and the next person "fixes the test" by re-pinning the bug.
The guard silently becomes its own opposite.

So: always assert the rule. `known-red` carries "we don't comply yet." There is
deliberately no `bug-report` tag, because it would license the rotting kind.

## Two kinds of tag

- **Topic** — `serve-correctness`, `security`, `eligibility`, `lifecycle`, … What
  area this is about. Free-form; used by `--tags` and for grouping the report.
- **Status** — how to read a result:
   - `known-red` — asserts a rule not yet met. Expected to fail; reported KRED and
     excluded from the exit code, but FIXED (and failing) if it starts passing.
   - `needs-attention` — passes, but carries an open question. Surfaced in the
     report's **⚠ needs attention** block along with the `## Note`.

Date a callout as `## Note (since=YYYY-MM-DD)` so `--attention-older-than DAYS`
can surface follow-ups that have gone stale.

## When _not_ to write a scenario

When the **mechanism** is the point rather than the **promise**. A scenario is
the wrong shape for "this function handles an empty array" — that is a unit test,
and it will be clearer, faster, and easier to run there.

The useful signal is the hook. `## Hook` exists for what markdown genuinely
cannot express, and reaching for one means **one of two things**:

1. **The grammar is missing a step** — the flow _is_ user-visible and should be
   expressible. Add the step; the scenario gets shorter and every future scenario
   benefits.
2. **The scenario is reaching below the API** — it wants an internal, which is
   unit-test work.

Ask which one every time — the first case is common. The scenario that motivated
`## Manifest` started as a hook that hand-built a manifest; the flow turned out to
be a real consumer's (a host authors manifests), so it became a step and the hook
disappeared. Of ~65 scenarios, two keep a hook; that ratio is the health metric,
not any individual hook.

The reason to hold this line: a scenario's value is that it is _separate from the
code_. Steps in, outcome out, and when it fails you can see why without reading
the implementation. The same reason a wireframe beats a prototype for settling
what a screen means — get too close to the real thing and the essence disappears
into the mechanics. A scenario that starts specifying internals has stopped being
a wireframe.

---

## Prerequisites

- **docker** (a throwaway `postgres:16` container is auto-spawned)
- **bun**
- A server build. The harness builds it once (`build:server-only`, which bakes
  the DuckDB ducklake/postgres extensions) if `packages/server/dist/server.mjs`
  is absent. Pass `--rebuild` to force a fresh build after code changes.

## Run

```bash
bun hammer/run.ts                          # all scenarios
bun hammer/run.ts --scenarios flat-source  # one (by id substring; comma-separated for several)
bun hammer/run.ts --tags security          # by tag (match-any; comma-separated for several)
bun hammer/run.ts --attention-older-than 30 # show only ⚠ callouts raised ≥30 days ago
bun hammer/run.ts --workers 2              # scenarios in parallel (default 6)
bun hammer/run.ts --rebuild                # force a fresh server build first
bun hammer/run.ts --keep                   # leave the pg container + workdir up to inspect
bun hammer/run.ts --reuse-pg               # reuse a running hammer pg container (fast iteration)
```

`--scenarios` and `--tags` narrow together (a scenario must match both).

Flags: `--pg-port` (default 55432), `--port` (14000), `--mcp-port` (14040),
`--quiet`. Exit code is nonzero if any scenario fails.

Set `HAMMER_STEP_TIMING=1` to report any step over 500ms — how you localise a
scenario that is slow only inside a full run.

The harness picks non-default ports so it won't collide with your own stacks.
If you used `--keep`, tear the container down with:

```bash
docker rm -fv publisher-hammer-pg
```

## Each scenario is a clean room

This is the property the harness spends most of its complexity on.

1. Build the server if needed; spawn one Postgres for the whole run.
2. Give **every scenario its own** source database, DuckLake catalog, storage
   directory and environment — addressed through the same connection names
   (`orders_pg`, `lake`) so the markdown never mentions the physical layout.
3. Write each scenario's packages, and generate **a config per scenario** naming
   only its own environments, connections and packages.
4. Run it against **a publisher started for it**, from that config, torn down
   with its storage afterwards.
5. Print a per-scenario check report; tear everything down (unless `--keep`).

Steps 2–4 are why a scenario can be trusted as a specification: the server it
talks to has never seen another scenario's packages, so what the markdown
describes really is the whole world the server knows about. A shared publisher
would be cheaper and would quietly make every scenario a half-truth.

Scenarios run in parallel (default 6 workers) and are order-independent by
construction.

## Layout

```
hammer/
  README.md            # this file — what a scenario is for
  GRAMMAR.md           # the step vocabulary
  run.ts               # orchestrator / entrypoint
  lib/
    util.ts            # process spawn, polling, logging
    postgres.ts        # throwaway docker Postgres (DuckLake catalog + source warehouse)
    server.ts          # build + spawn a real Publisher server process
    config.ts          # generate publisher.config.json (connections + packages)
    packages.ts        # write generated Malloy packages to disk
    rest.ts            # REST client: build, poll, query (SQL + row values), bind, republish, reclaim
    scenario_md.ts     # the markdown scenario interpreter (parse -> Scenario)
    scenario_md.spec.ts # grammar tests — a malformed scenario must fail loudly
  scenarios/
    framework.ts       # Scenario contract + assertion recorder + ServerControl
    index.ts           # recursively discovers */scenario.md
    mz-storage/        # the current suite (the storage= materialization tier)
      01-flat-source/scenario.md
      02-refinements/scenario.md
      ...                              # (a folder per scenario)
      32-rebuild-on-model-change/{scenario.md, hooks.ts}  # + hooks.ts where markdown can't reach
```

Scenarios live in **suites** under `scenarios/<suite>/`. The harness itself is
suite-agnostic. Today the one suite, `mz-storage`, exercises the `storage=`
materialization tier; a new suite needing the same fixtures is just a new folder,
one needing different infra extends `run.ts`.
