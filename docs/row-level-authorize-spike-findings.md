# Row-level `#(authorize)` — spike findings

Measured against `@malloydata/malloy@0.0.427` (confirmed from
`node_modules/@malloydata/malloy/package.json` and `dist/version.js`; two spike
runs misreported `0.0.430` from a bun cache path — the installed version is
0.0.427). Every claim below was produced by compiling and running, not by
reading. Scripts are throwaway and live outside the repo.

This file exists because the spike overturned the mechanism the implementation
plan had settled on. It is the evidence for what replaced it.

## 1. The planned mechanism is a data leak, not just a limitation

The plan chose to reuse `injectFilterRefinement` (`service/filter.ts`) — append
`+ {where: <gate>}` to the query text and recompile. It records the belief that
the refinement "lands in stage 0 only".

**It lands on the LAST stage.** Two consequences, and the second is the serious
one:

1. A joined-field gate against a multi-stage query fails to compile:
   `run: X -> {group_by: id} -> {group_by: id}` + `+ {where: childtable.name = 'bob'}`
   → `'childtable' is not defined`.

2. **A caller can neutralize the gate.** Because the refinement binds to the last
   stage, it resolves against the _previous stage's output_, which the caller
   controls. Given a gate `org_id = 1`:

   ```
   run: X -> { group_by: org_id is 1, id, val } -> { group_by: org_id, id, val }
   + {where: org_id = 1}
   ```

   ```sql
   WITH __stage0 AS (SELECT 1 as "org_id", base."id", base."val" FROM parent as base GROUP BY 1,2,3)
   SELECT base."org_id", base."id", base."val" FROM __stage0 as base WHERE base."org_id"=1 GROUP BY 1,2,3
   ```

   All four rows come back, including the two whose real `org_id` is 2. The gate
   tested the caller's constant, not the column.

Within a SINGLE segment the same trick does not work — Malloy resolves a
segment's `where:` against its INPUT field space, so `group_by: org_id is 999`
does not shadow `where: org_id`. The bypass needs a stage boundary to put a
caller-controlled projection between the gate and the data.

`injectFilterRefinement` is therefore not usable for a gate. (It remains correct
for `#(filter)`, which is a convenience filter rather than an access rule — but
that path has the same last-stage binding, which is worth knowing.)

## 2. What does work: a source-level filter on the entry source

A source-level `where:` — `source: g is X extend { where: <gate> }`, or inline
`run: X extend { where: <gate> } -> …` — lands the predicate and any join it
needs inside `__stage0`, against the base table, before any caller projection
exists:

```sql
WITH __stage0 AS (
  SELECT base."id" FROM parent as base
   LEFT JOIN child AS childtable_0 ON childtable_0."id"=base."child_id"
  WHERE childtable_0."name"='bob'
  GROUP BY 1
)
SELECT base."id" FROM __stage0 as base GROUP BY 1
```

It is not shadowable. A caller redefining the gated field downstream cannot
reach it (the predicate is resolved before their pipeline begins), and a caller
redeclaring the gate's join name is rejected outright by the compiler
(`Cannot redefine 'childtable'`) in both restricted and unrestricted mode.

## 3. Applying it without touching the caller's query text

Two routes were compared on eight caller-text shapes.

**Route A — text surgery**: rewrite every `run: <ident>` to
`run: <ident> extend { where: <gate> }`. Works for seven of eight shapes but
structurally cannot handle a named-query target (`query: q is …` + `run: q`) —
`extend` is not legal on a query reference (`Cannot run this object as a
query`). It also puts a regex on the security path.

**Route B — graft the compiled condition onto a copied `ModelDef`, then
recompile the caller's UNMODIFIED text against it.** Works for every shape,
including the named-query target, because the caller's text is never rewritten —
only the `SourceDef` that text resolves against.

This is NOT the load-time `filterList` graft the plan rejected. That one failed
because derivations snapshot their base at compile time, so a graft applied
after the model was compiled reached only `run: X`. Recompiling the caller's
text after the graft is what makes derivations pick it up, and it is also why
joined-field gates emit their JOIN here while the rejected `pipeline[0]` graft
did not: the join is compiled fresh as part of this query's own `__stage0`
rather than spliced in as pre-computed IR.

### The given-identity trap

Every `runtime.loadModel()` call mints a FRESH identity for each declared given
(`given/6:GROUPS,57:internal://loadModel/<uuid>`), even for byte-identical text.
Lifting a gate's compiled condition from a separately-loaded probe model and
grafting it onto the real model produces a dangling reference:

```
run: statement references given `given/6:GROUPS,57:internal://loadModel/<uuid>`,
which is not surfaced in this model and has no default.
```

The condition must be lifted through the SAME loaded model. Compiling the probe
via that model's own materializer — `modelMaterializer.loadQuery("run: X extend
{ where: <gate> } -> { select: __p is 1; limit: 1 }")` — shares given identity
and works. Verified for both an array-given gate and a joined-field gate, across
single-stage, multi-stage, named-view, named-query, caller-declared-derivation,
two-run-statement and derived-entry-point (`Y is X extend`) shapes, under
`loadQuery` and `loadRestrictedQuery` alike. Empty array ⇒ `WHERE FALSE` ⇒ zero
rows, on every shape.

### P0: the graft MUST be scoped to the entry point

Grafting a source's `filterList` propagates into join copies compiled
afterwards. With a gate grafted onto `X` and an UNGATED entry point that joins
`X`, the gate fires through the join — for a joined-field gate visibly
(`WHERE childtable_0."name"='bob'`), and for an own-column gate inside the join
itself (the joined rows come back `null`).

That is a P0 violation _if the implementation ever grafts a source that is not
on the entry point's own ancestry_ — so it must not. Scoping the graft to the
gates `collectEntryPointGates` returns for the run target keeps P0 intact:
an ungated parent joining a gated child collects no gate, so nothing is grafted
and the child's gate correctly does not fire.

The reverse case — a gated entry point whose query also joins the same source —
over-filters, which is fail-closed and acceptable.

An earlier spike round tested only the own-column gate here and reported "no
leak"; that conclusion was wrong, and the joined-field gate is what exposes it.

## 3a. The graft is provable, which is what makes it safe to trust

After the graft and recompile, the gate's presence can be PROVEN from the
compiled query: resolve the executed query's run-target `SourceDef` and look for
the condition's `code` string in its `filterList`. That proof holds for a
single-stage query, a multi-stage one, a named view, a named query in caller
text, a caller-declared `extend` derivation and a derived entry point.

It needs one recursion. For a caller-declared QUERY SOURCE
(`source: qs is X -> {…}` + `run: qs -> {…}`) the executed source's own
`filterList` is empty — the filter was consumed inside the query-source's inner
pipeline — but it IS present on the base reached through `query.structRef`.
Recursing there (bounded, as `collectEntryPointGates` already recurses for
`query_source`) makes the proof hold there too, PROVIDED the graft actually
reached that base in the first place.

**Correction — the recursion alone does not make the proof hold for a
MODEL-declared query source (`Z is X -> {…}`, a top-level `contents` entry).**
The graft target has to be `Z` itself, not `X` (§3b) — grafting `X`, recompiling,
and then having the landing proof recurse into `X` looking for the condition
finds nothing, because the condition was never grafted onto `X` at all for this
shape. The recursion in the proof and the recursion this section originally
conflated it with (the one deciding WHERE to graft) are two different walks;
getting the graft target right is what §3b is about, and it has to happen
before this proof can ever succeed for `Z`.

So the implementation denies whenever the proof fails, which is what converts a
future Malloy change (or a graft-target bug) that silently stops the graft from
landing into a refusal rather than a leak — the failure this whole design is
organised around.

## 3b. The query-source regression is real, and narrower than planned

The plan accepted "a narrow 403 regression on a working idiom": for
`Z is X -> {…}` where the gate's column is PROJECTED AWAY, it expected the gate
to be inexpressible and the request to be denied, with an author-facing error
naming the column.

**The caller-declared case does not regress.** For `source: qs is X -> {…}` +
`run: qs -> {…}` declared in the CALLER's own query text, `qs` is compiled fresh
against the grafted `ModelDef` on every request, so grafting `X` (`qs` is not
itself a `contents` entry) reaches it: `source: qs2 is X -> { group_by: id, val }`
with a gate on `X.org_id` serves, correctly filtered, even with the gate's
column projected away — the filter applies to `X`'s rows inside the
query-source's inner pipeline, before the projection drops the column. A
source-level filter does not need the column to survive the projection; only a
segment-level refinement did.

**The regression is real for a MODEL-declared query source.** `Z is X -> {…}`
is a top-level `contents` entry whose compiled `SourceDef` snapshotted `X` at
declaration time; grafting `X` afterward never reaches it (recompiling only the
caller's text does not rebuild `Z`). The fix grafts `Z` itself instead, which
works exactly when `Z`'s own field space still has the gated column — matched
against the entry-point matrix in §4: `Z is X -> { group_by: org_id, val }`
(column kept) serves, correctly filtered.

For `Z2 is X -> { group_by: val }` (column PROJECTED AWAY), there is no graft
target that works: grafting `X` doesn't reach `Z2` (same snapshot problem), and
grafting `Z2` itself cannot resolve `org_id` because it is not in `Z2`'s field
space at all. **This is the narrow, author-fixable 403 the original plan
anticipated** — denied with `a row-level gate condition did not land on the
recompiled query` (from the graft-target-resolution failure, not the landing
proof) — just narrower than planned: it is specific to a MODEL-declared query
source whose projection drops the gated column, not every query source, and not
a caller-declared one.

## 4. Field-space resolution is per entry point, and fails loudly

A gate written against `org_id` does not resolve at an entry point that renamed,
excluded or dropped it. All of these fail with `'org_id' is not defined`:

| Entry-point shape                                      | Gate on `org_id` | Gate on `childtable.name`             |
| ------------------------------------------------------ | ---------------- | ------------------------------------- |
| `W is X extend { rename: tenant is org_id }`           | fails            | —                                     |
| `W is X extend { except: org_id }`                     | fails            | —                                     |
| `W is X extend { accept: id, val }`                    | fails            | —                                     |
| `Y is X extend { dimension: d is val*2 }`              | resolves         | —                                     |
| `Z is X -> { group_by: org_id, val }`                  | resolves         | —                                     |
| `Z2 is X -> { group_by: val }` (column projected away) | fails            | —                                     |
| `Z3 is X -> { group_by: id, val }` (join dropped)      | —                | fails (`'childtable' is not defined`) |
| `X` itself                                             | resolves         | resolves                              |

So publish-time validation must probe **each reachable entry point**, not just
the declaring source. `getPreparedQuery()` is the right probe: it is
compile-only (never reaches the warehouse) and, unlike `getSQL()`, does not
require a given to have a bound value — an unbound given fails `getSQL()` with a
distinct "has no value" error that must not be confused with a field-resolution
failure.

Loading the model is NOT sufficient evidence. A source whose inherited
source-level `where:` references a field a later `rename:` removed still
compiles at model level; the break surfaces only when a query touches it, as
`Field 'org_id' not found.` on every query against that source.

## 5. The compiled condition, and why the grammar check reads IR not text

`source: X extend { where: <gate> }` produces a `filterCondition` carrying the
author's text in `code`, `isSourceFilter: true`, a `refSummary` and an
expression tree `e`. Observed node kinds:

| node                                   | from                                                    |
| -------------------------------------- | ------------------------------------------------------- |
| `inGiven`                              | `field in $ARRAY` — `{not, givenRef: given, e: field}`  |
| `=` `!=` `>` `>=` `<=`                 | binary comparison, `kids.left` / `kids.right`           |
| `and` `or`                             | boolean, `kids.left` / `kids.right`                     |
| `()`                                   | parenthesis, wraps `e`                                  |
| `not`                                  | negation, wraps `e`                                     |
| `field`                                | `{path: ["org_id"]}` or `{path: ["childtable","name"]}` |
| `given`                                | `{refName: "GROUPS", id: "given/…"}`                    |
| `numberLiteral` `stringLiteral` `true` | literals                                                |
| `function_call` `+` `like` `in`        | rejected shapes                                         |

Two facts this settles:

- **`refSummary.fieldUsage` is the exact row-level discriminator.** Empty ⇒ the
  gate reads no row field and keeps the existing one-row probe. Non-empty ⇒ row
  filter. Malloy has already decided which names are fields and which are
  givens, so no text scan is needed and none would be reliable.
- **`org_id ? $GROUPS` and `org_id = $GROUPS` compile to the SAME `=` node.**
  There is nothing to tell apart by spelling. Both are a scalar comparison
  against an array-typed given, which is what gets rejected — on the given's
  declared TYPE. The plan's "reject the `?` and `=` spellings" is satisfied by
  one type rule rather than two syntax rules.

`isSourceFilter: true` comes for free with a source-level filter, which is what
keeps the gate's predicate out of the response's `drill_filters` annotation.

## 5a. `not` must be refused: it inverts fail-closed into fail-open

The empty-array property that makes `field in $ARRAY` safe is that an empty given
compiles to `WHERE FALSE` and matches nothing. Negation destroys it, and Malloy's
NULL-safe rendering makes it worse:

| gate                      | `$GROUPS` | emitted predicate                                 | rows returned          |
| ------------------------- | --------- | ------------------------------------------------- | ---------------------- |
| `org_id in $GROUPS`       | `[1]`     | `WHERE base."org_id" IN (1)`                      | 2 of 3 (correct)       |
| `org_id in $GROUPS`       | `[]`      | `WHERE FALSE`                                     | 0 (fail-closed)        |
| `not (org_id in $GROUPS)` | `[1]`     | `WHERE COALESCE(NOT (base."org_id" IN (1)),TRUE)` | 1 of 3 (the OTHER org) |
| `not (org_id in $GROUPS)` | `[]`      | `WHERE COALESCE(NOT (FALSE),TRUE)`                | **3 of 3 — every row** |

A caller with no groups reads everything. The `COALESCE(…, TRUE)` also means a
NULL on the gated column admits the row rather than excluding it.

So `not` is refused outright by the allowlist rather than permitted in some
"monotone context". A gate states which rows a caller MAY read; the inverse
spelling is both semantically odd for an access rule and fail-open on exactly the
inputs an ACL has to get right.

## 6. An extension cannot re-point an inherited gate

The concern was that `filter.e` holds a late-bound path (`{node:'field',
path:['org_id']}`) an extension might redirect. It cannot:

- Redefining the gated field (`dimension: org_id is 999`) or a joined alias
  (`join_one: childtable is …`) in a deriving `extend {}` is rejected at compile
  time — `Cannot redefine '<name>'`. Malloy has no shadowing for source-level
  names.
- `rename:` is the one construct that changes what a name means, and it does not
  rewrite the inherited `filterList` — the IR keeps saying `path: ["org_id"]`.
  Since that name no longer exists, every query against the source fails loudly
  rather than silently testing a different column.

## 7. Notebook cells: a self-declaring cell needs a different graft, not a different scope search

A notebook cell's row-level gate grafts against the model AS OF THAT CELL — the
nearest EARLIER code cell's own compiled `(modelDef, modelMaterializer)` — never
the cell's own post-declaration model, because recompiling a cell's `source:`
line against a model that already has that name (its own, or the model-wide
cumulative one) fails with `Cannot redefine`. That mechanism has no earlier
model to offer when a cell both DECLARES a gated source and RUNS it in the SAME
cell: the first code cell, one preceded only by markdown, or — measured here,
not just theorized — ANY later cell in the same shape (`#(authorize) "…"` +
`source: gated is …` + `run: gated -> …` all in one cell). The earlier-cell
walk finds a scope for that later cell, same as for any other; the problem is
that the declared source simply does not exist there to graft. Treating this
as a cell-INDEX problem (only cell 0 is broken) misses that case entirely — the
run target resolving in the earlier scope is the actual condition, and it fails
identically regardless of which cell.

**The fix does not need a text recompile at all for this case.** For a
self-declaring cell, the compiled query's `structRef` is a bare STRING name
(`"gated"`), not an embedded `SourceDef` snapshot — because the run target IS a
top-level `contents` entry of the cell's own model, `#(authorize)` annotation
intact. That means the query can be re-pointed at a grafted CLONE of its own
model without re-parsing any text: `runtime._loadModelFromModelDef(copy)` (copy
= the cell's own `modelDef`, structuredClone'd, with the lifted condition
appended to the target's `filterList` — same graft `buildGraftedMaterializer`
already does) produces a materializer; `graftedMaterializer._loadQueryFromQueryDef(originalQueryDef)`
repoints the cell's ALREADY-COMPILED query at it. Neither call parses any
text, so the redefinition collision this whole problem is about never arises.
Held on four shapes: plain declare-and-run, a multi-stage pipeline
(`gated -> {…} -> {…}`), a named view (`gated -> byorg`), and a joined-field
gate (`childtable.name in $GROUPS` via `join_one`). Empty given ⇒ zero rows
(fail-closed) on every shape. P0 (§3's join-scoping rule) holds unchanged: an
ungated entry point joining a gated source declared in the SAME self-declaring
cell collects no gate for the joined source, exactly as it does outside
notebooks.

**Residual limitation — joined-field gate, run query doesn't reference the
joined field.** The queryDef-repoint mechanism works when the RUN query's own
projection/pipeline already references the joined field (e.g.
`run: gated -> { group_by: id, org_id, childtable.name }`); it does NOT when
the run query never references the joined field at all (e.g.
`run: gated -> { aggregate: n is count() }` with the gate on
`childtable.name`). The already-compiled queryDef's join wiring was resolved
against the UNGRAFTED struct — which had no reason to include the join, since
nothing in the original query referenced it — so repointing it at the grafted
struct produces a query whose WHERE clause names a join alias
(`childtable_0`) the executed SQL never establishes: a DuckDB "Referenced
table … not found" error at `run()` time, surfaced as a 400 (not a 403 — this
is a defect in the shape, not a rejected gate), never a leak (no rows are
returned). This is narrower than the pre-fix limitation it replaces: it is
specific to a joined-field gate whose run query doesn't itself touch the
joined field, on the self-declaring-cell path only — the ordinary (declared
in an earlier cell) path recompiles text fresh and is unaffected.
