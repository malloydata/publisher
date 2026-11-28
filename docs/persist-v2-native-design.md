# Planning `#@ persist` natively in the v2 planner

Status: **decided, not yet implemented.** Requested by Kyle Nesbit, 2026-11-28. Kyle took
decisions D1–D10 the same day; they are recorded in [Decisions (Kyle, 2026-11-28)](#decisions-kyle-2026-11-28).
Read against `origin/main` at `7d5c46ff1`. Every `file:line` below is under
`packages/server/src/service/materialization/` unless it names another root.

Items marked **Signed off** are new contract values or behaviour changes that Kyle approved with the
decisions. Section 7 lists all of them.

## Summary

`#@ persist` on a query-shaped source is still planned by the v1 walk. That walk is Malloy
core's persistence walk (`getBuildTargets`) run over each author file. Where a file lacks
`##! experimental.persistence`, it runs over a flagged build copy of the file
(`plan/v1_build_companion.ts`). Every other kind of table is planned by v2 legs: `#(materialize)`,
`#(rollup)`, and `#@ persist` on a view or a named query. Serving moved onto v2 in 0.13 (the
served copy and the rollup leg). Planning did not move.

This design moves `#@ persist` planning into a v2 **persist leg** and deletes the v1 code. The
persist leg follows one ownership rule for every leg:

- **One leg owns each `sourceID`.** An own `#@ persist` on a query-shaped source belongs to the
  persist leg. An own `#(materialize)` belongs to the materialize leg. Both on one source stays
  malformed.
- **An inherited-only `#@ persist` never creates a table.** Such a source is a chain over its
  base's table. It gets a table of its own only with its own `#(materialize)`, which the
  materialize leg builds over the base's stored table (the #2004 machinery).
- **An own `#@ persist` on a table-shaped source stays `persist-not-materializable`.** It is
  malformed and fails the package load, word for word as today.

The persist leg plans each source through its own **generated companion**, as the view, query and
rollup legs do. The companion is compiled over the author's files with source-level `#@` lines
blanked, and it declares one build source per persisted source, presented as the author's source.
The leg still asks Malloy for each build source's `BuildTarget` (D1), so content addresses and
physical table names stay as they are. Two parity gates prove that before the flip: every address is
unchanged, and every chained build still reads its persisted upstream's table. What goes is
everything core's walk decides today:

- which sources persist;
- that an extension claims its base's table;
- the refusal of a `#(materialize)` placed on that extension;
- the backstop that guesses which sources core dropped without saying so.

The flag stops mattering to authors. Planning failures are isolated per file.

There is **no runtime switch** (D3). The v1-to-v2 comparison runs only in CI and in the service's
conformance job:

- the required `--plan-diff` on the service PR that bumps `publisher_version`;
- a `publisher_ref` dispatch input for shadowing candidates;
- the daily job on Publisher `main` as the early warning.

The flip is a code change in one breaking minor. That release also carries:

- lifting the `storage=` persist-over-`#(materialize)` refusal (D6);
- making `#@ preaggregate` a deprecated synonym of `#(rollup)`, read by the rollup leg, with the v1
  synthesis and classifier deleted (D8);
- removing `materialize.enabled`'s off position (D9);
- one shape predicate everywhere (D10);
- deleting the v1 walk.

The one temporary piece is a bind rule for manifests named after inherited-only extensions (D4). It
ships in that release and is deleted once every cell has replanned.

## 1. Where `#@ persist` is planned today

### The v1 walk

`collectModelBuildPlan` (`plan/build_plan.ts:2011`) plans one compiled model in two stages:

1. `collectGeneratedTables` (`:2075`) runs the v2 legs: the `#(materialize)` companion, the
   persisted views and queries, and the rollups. Each compiles a generated companion that
   carries the flag itself, so the author's file never needs it (comment at `:2069-2073`).
2. The v1 tail runs:
   - `v1PlanModel` (`:2241`) chooses the model to walk: the author's file when it declares the
     flag, otherwise the build copy from `compileV1BuildCompanion` (`plan/v1_build_companion.ts:69`).
     Its comment says why (`:2246-2248`): core's `getBuildTargets` throws on an unflagged model,
     which would abort the whole package's plan. The `##! experimental.persistence` check is in
     core (`node_modules/@malloydata/malloy/dist/api/foundation/core.js:1118`, Malloy 0.0.436).
   - `runtime.getBuildTargets(model)` (`:2053`) walks every `#@ persist` source the model reaches.
     `plannedSources` (`plan/addressing.ts:127`) keeps them, including the unbuildable ones.
   - `reportPersistLineFindings` (`:2286`) turns core's `tagParseLog` into findings. Then
     `detectDroppedPersistSources` (`:544`) names every source that carries a `#@ persist` line but
     is missing from core's targets. Each such source becomes a malformed `persist-not-materializable`
     finding (`:2361-2390`). Its own comment calls it a backstop for a gap in core's walk (`:534-543`).
   - `planV1PersistSources` (`:2393`) records one `kind: "persist"` table per planned source. It
     records the annotation findings, the `storage=` over `#(materialize)` refusal and the gate
     classification (`classifyPersistSourceGate`, `:656`).

`plan/README.md:17` still lists the `persist` kind as planned by "the v1 walk
(`collectModelBuildPlan`)".

### What the 0.13 cutover moved, and what it left

The cutover had three parts:

- **#1693** designed the cutover.
- **#1703** (steps 6a–6e) bound v1 database tables on the served copy, keyed by content address
  (step G1). Step 6d added the v1 build companion so that an unflagged file plans. The spec
  `plan/v1_build_companion_corpus.spec.ts` is the gate that pins that no address moved.
- **#1713** (6f–6g) turned the cutover on and deleted the storage serve shape.

The release notes record the result (`RELEASE_NOTES.md:2388-2400`, `:2436`). Serving became v2.
**Planning stayed on core's walk.** The cutover only extended the walk to unflagged files.
`docs/materialization-v2-plan.md:132` states the intent this design ends:

> A v1 `#@ persist` on a query-shaped source keeps core's meaning.

### What already exists in v2 for persisted upstreams

- `chainReachesPersisted` (`plan/materialize_plan.ts:2457`) treats an own `#@ persist`, and an own
  `#(materialize)`, as a stored upstream that a chain builds over. The cross-file walk uses it
  (`plan/materialize_compile.ts:227`, `:2038`).
- #1998 builds persisted views, named queries and rollups from their upstream's stored table, or
  refuses them at plan (`upstream_table_unusable`). Addresses stayed the same, pinned in a test.
- #2004 builds a `#(materialize)` extension over its parent's stored table, or refuses it at plan.
  The link is made by `chainVerdict` (`plan/materialize_plan.ts:2158`). Today it only links to a
  parent that has a `MaterializePlan`.

### What core's walk costs

1. **Core decides membership.** Core reads `#@ persist` through the whole annotation chain. An
   `extend` of a persisted query source therefore inherits the line and is planned as its own
   persist source. Two consequences follow:
   - A `#(materialize)` on that extension is refused `inherited_persist`
     (`plan/materialize_plan.ts:1488-1500`). Credible's gtme `deals` is this case.
   - The extension inherits `name=` as well. The name comparisons in `foldDistinctTableNames`
     (`plan/addressing.ts:697`, comment at `:666-671`) must then sort out tables that two sources
     claim. The author's way out is a `name=` of their own on the extension.
2. **Core's silence needs a backstop.** Core plans nothing for a source shape it does not persist,
   and says nothing. `detectDroppedPersistSources` finds those sources by difference, reading
   `declares(...)` over the annotation chain. An inherited line therefore triggers it as well as an
   own one.
3. **The flag, and blast radius.** The build copy fixes the flag. If the copy fails to compile, the
   file's sources are omitted (`v1_build_companion_failed`, `api-doc.yaml:11076-11096`). Any throw
   in `collectModelBuildPlan` still discards the whole package plan:
   - On the load worker, one model's error becomes `planError` (`package_load/package_load_worker.ts:1366-1373`).
     That error then fails the plan (`:1645`).
   - On the main thread, `collectPackageParts` (`plan/build_plan_compile.ts:216-275`) has no
     per-model catch.
   - A package with no plan binds nothing (`serve/bind.ts:83`, `build_plan_unavailable`).
4. **Core cannot see v2 tables.** A `storage=` `#@ persist` over a `#(materialize)` table in its own
   destination is refused (`persistOverMaterializedReason`, `plan/downstream_upstreams.ts:186-200`;
   `RELEASE_NOTES.md:311`). The refusal exists because core's walk cannot order the persisted
   source after that table.
5. **Three shape predicates disagree.**
   - Core persists `query_source` and `sql_select` (`isPersistableSourceDef`, `malloy/dist/model/malloy_types.js:518`).
   - `reachedUpstreams` only counts `query_source` (`plan/downstream_upstreams.ts:118-124`). A view
     or rollup over a persisted `.sql()` source therefore does not see it as an upstream.
   - `detectDroppedPersistSources` uses a third rule: whatever core left out.

## 2. Fixed positions

These are Kyle's, and the design treats them as constraints:

- `#@ persist` on a query-shaped source, a named query and a view is correct and supported
  indefinitely. Sources are materialized with `#(materialize)`. Metrics roll up with `#(rollup)`.
  `#@ preaggregate` is deprecated. Since D8 (revised) it is a synonym of `#(rollup)` that warns; it
  does not fail the load. The authoring rule is unchanged: docs, skills and examples use `#(rollup)`
  only.
- `persist-not-materializable` keeps today's tier, message and load failure, and does not weaken.
  Only an **own** `#@ persist` on a table-shaped source produces it.
- One leg per `sourceID`. An inherited-only `#@ persist` never creates a table.
  `inherited_persist` goes away, except where ownership is genuinely ambiguous.
- Kyle's general rule: an invalid annotation fails publish and load. Only a valid declaration that
  cannot be built is refused.

## 3. The design

### 3.1 One ownership rule

A single function, `tableOwnerOf(sourceID)`, lives in `plan/declaration.ts`. That file is already
"the one reader of materialization's annotations" (`plan/README.md`). Every planner, and X1, reads
the function instead of its own copy of the rule. Today the rule is spread across five places:

- `declaresOwnPersist` and `inheritedPersist` (`plan/materialize_plan.ts:252-297`);
- `declaresOwnPersist` (`plan/addressing.ts:560`);
- `reachedUpstreams`;
- `chainReachesPersisted`;
- `detectDroppedPersistSources`.

The function reads only the source's **own** notes, through `declaredNotes` (governance
`declared_notes.ts`). It reads the shape with core's own predicate: `query_source` or
`sql_select`, as `isPersistableSourceDef` does.

| Own notes on the source                        | Shape        | Owner and outcome                                                                                                    |
| ---------------------------------------------- | ------------ | -------------------------------------------------------------------------------------------------------------------- |
| `#@ persist`                                   | query-shaped | persist leg                                                                                                          |
| `#@ persist`                                   | table-shaped | malformed `persist-not-materializable`, which fails the load (unchanged)                                             |
| `#(materialize)`                               | any          | materialize leg (unchanged)                                                                                          |
| both                                           | any          | malformed `materialize-invalid` ("declare one or the other", `plan/materialize_plan.ts:1482-1486`, unchanged)        |
| neither, and the chain reaches a stored source | any          | no table. The source is a chain over that table, served by the served copy re-rooting the stored source in its chain |
| own and inherited cannot be told apart         | any          | refused alone, answered live (see decision [D5](#decisions-kyle-2026-11-28))                                         |

The last row is the only "genuinely ambiguous" case. It happens when an annotation can be read but
its notes cannot be attributed to the source's own statement. That is the identity rule in
`declared_notes.ts`, known limitation 4 in `docs/materialization-v2-plan.md`. Today that rule fails
open as "declares", in `plan/addressing.ts:560-573`.

### 3.2 The persist leg

**Where it lives.** It gets its own planner, `plan/persist_plan.ts`, and a compiler,
`plan/persist_compile.ts`. That is the same split as `materialize_plan.ts` and
`materialize_compile.ts`, and as `persisted_view.ts` and `persisted_view_compile.ts`. It is called
from `collectGeneratedTables` beside the other legs. It is not gated on `materialize.enabled`, because that switch's off position is removed first, in the same release (D9). The `TableKind`
stays `persist`. Run, bind, telemetry and the wire read the kind off the table
(`plan/README.md`, "Nothing downstream derives a kind"), so none of them changes.

**What it plans, per model file.** Following the persisted-view leg's rule, a source is planned
only in the file that declares it (`plan/persisted_view.ts:33-35`):

1. **Selection.** Take every source declared in this file that `tableOwnerOf` assigns to the
   persist leg. Report the table-shaped ones as `persist-not-materializable`, with the same message
   text as `plan/build_plan.ts:2361-2390`. The leg reports these itself, from the ownership rule.
   It no longer infers them from what core left out.
2. **Validation, before any compile.** Run the line checks the walk runs today:
   - `persistAnnotationFindings` and `persistDeclarationProblems` (`plan/build_plan.ts:2416-2433`);
   - `persistNameProblems` read from the raw text (`:2112-2121`);
   - unparseable lines, reported through `plan/declaration.ts`'s `readNotes`, as the view and query
     legs report them. These replace core's `tagParseLog`.

   Each of these is malformed and fails publish and load, as now.

3. **Compile: the persist companion (D2).** The leg generates one companion model per declaring
   file, `<file>.persist.malloy`. Like the persisted-view companion, it is never on disk and never
   in the package's model list (`plan/persisted_view.ts:17-24`).
   - It reads the author's files with every source-level `#@` line blanked, positions kept.
   - For each selected source `s` it imports `s` under an alias. It then declares
     `source: s__persist is <alias>` under the author's own `#@ persist` line, copied verbatim, and
     opens with `##! experimental.persistence`.
   - Each build source is presented as the author's source with `presentAs`
     (`plan/addressing.ts:249`), as the materialize leg presents `<src>__mat`. The plan, the wire,
     manifests and warnings therefore never show the generated name.
   - The leg asks Malloy for the build sources' `BuildTarget`s. A target's `sql` compiles the
     source's `query`, or for a `.sql()` source its SQL (core.js `_compileSQL`). An alias adds
     nothing to either, so the `sql` is expected to equal the author's source's `sql` byte for
     byte. The **address-parity gate** (4.1) proves it before the flip, rather than taking it on
     trust.
   - Because source-level lines are blanked, core sees exactly the leg's selection and no
     inherited-only extension. The flagless/flagged split, `v1PlanModel` and the build copy all go.
4. **Build SQL over persisted upstreams (the chained case).** Blanking also hides an upstream's
   `#@ persist` from the first compile, so core's `buildSQL(manifest)` would not substitute its
   table. A persist source that reads stored upstreams therefore takes the second compile #1998
   built for views and queries (`plan/over_stored_proof.ts:1-26`):
   - over a `#@ persist` upstream, the second compile keeps that upstream's own `#@ persist` line,
     so `buildSQL(manifest)` reads its table and the downstream's address stays equal (checked);
   - over a `#(materialize)` upstream, the upstream is re-rooted on its generated build source
     (`plan/stored_base_copy.ts`). This is what lifts the D6 refusal and serves D10's `.sql()`
     upstreams.

   The plan records the proof, so a run only reads the decision, and a source the proof fails is
   refused at plan (`generated_build_failed`), never built from raw. The **chained-build parity
   gate** (4.1) shows that every chained persist build in the corpus reads the same upstream tables
   and writes the same rows as under v1.

5. **Facts per table.** These are the same calls the walk makes, on the same kind of
   `PersistSource`: `plannedFacts`, the declaring model path, `chainedStringReads` and
   `classifyPersistSourceGate`. The `persistOverMaterializedReason` refusal
   (`plan/downstream_upstreams.ts:186-200`) is deleted (D6).
6. **Edges.** The leg gives each planned source the planned tables its definition reaches, through
   `withDownstreamEdges` (`plan/addressing.ts:306`, extended to the `persist` kind). These are
   `#@ persist` and `#(materialize)` upstreams alike. The edges among persist sources must equal
   core's `dependsOn` from the v1 walk (`sourceBuildsOf`, `:47-125`). The plan-diff compares them.
7. **Isolation.** If the companion fails to compile or plan, the file's persist-leg sources become
   `omittedSources`, or the file goes in `omittedFiles` when the failure comes before selection.
   The failure is coded `persist_companion_failed` (**Signed off:** a new `OmittedSource.reason`
   value). `v1_build_companion_failed` becomes "retained, no longer produced". The leg catches its
   own throws, as `compileMaterialize` does (`plan/materialize_compile.ts:198-245`), so no persist
   error can fail the package plan. Phase 0 gives the rest of `collectModelBuildPlan` the same
   per-leg containment.

**What is still asked of Malloy core (D1).** Only `Runtime.getBuildTargets` over a generated,
flagged file, for each build source's `BuildTarget` (`sql`, `buildSQL`, `buildId`). Every v2 leg
already uses this primitive (`plan/materialize_compile.ts:890`, `:995`; `plan/rollup_compile.ts:710`;
`plan/persisted_view_compile.ts:1020`). Every address is a digest of `BuildTarget.sql`
(`plan/addressing.ts:376-411`). Publisher calls neither core's deprecated `Model.getBuildPlan` nor
`PersistSource.getSQL`. `Package.getBuildPlan` (`package.ts:1956`) is Publisher's own wire plan.
After the move, **the author's file never needs the flag**, and nothing core's walk decides about
the author's model reaches the plan.

### 3.3 Extensions of a persisted query source

This is the gtme shape, written as the model has it today. This is not authoring advice; new
sources use `#(materialize)`:

```malloy
#@ persist name="deals_including_deleted"
source: deals_including_deleted is crm.table('deals') -> { select: * }

#(materialize)
source: deals is deals_including_deleted extend { where: not is_deleted }
```

How the two planners treat it:

- **Today:** `deals` inherits `#@ persist`. Core plans it at `deals_including_deleted`'s address,
  because a query source's target compiles its `query` and an `extend` adds nothing to that
  (core.js `_compileSQL`; the `extend adds nothing` note in `run/shared_address_ownership.spec.ts`).
  The `#(materialize)` is then refused `inherited_persist`.
- **After:** `deals` is the materialize leg's. Its extension chain reaches `deals_including_deleted`,
  which the persist leg owns. `chainVerdict` then has to link to a parent that has no
  `MaterializePlan`.

**The change:** `chainVerdict` (`plan/materialize_plan.ts:2158`) and the cross-file parent walk
(`crossFileParents`, `plan/materialize_compile.ts:1990-2060`) take a narrow `StoredParent` record
instead of a full `MaterializePlan`. A `StoredParent` holds:

- `sourceID`;
- placement, meaning tier and destination;
- stored columns (`deriveColumns`, `plan/persist_source_fields.ts:27`);
- the incremental declaration;
- the parent `where:` terms left out of its build, which is always none for a persist parent,
  because its query is built whole;
- a pair layout, which is always absent for a persist parent.

Both legs produce a `StoredParent`, and the existing tier, incremental and term rules then apply
unchanged. The materialize build copy re-roots `deals_including_deleted` on its table (`materialize_build_copy.ts`).
`deals`'s `where:` is applied in its own build. **This is the only new planning mechanism in the
design. Everything else is selection.** It also removes the v1 trap: `deals` no longer inherits
its base's `name=` or claims its table. With `#(materialize)` it gets its own readable table or its
own `name=`. Without it, it has no table at all.

An extension with no annotation of its own (case 55 in the hammer suite,
`hammer/scenarios/mz-storage/55-extended-source-inherits-persist`) leaves the plan. It is served
the way `docs/materialization.md:557` and `:1255` already describe: through its base's table, with
its own refinements applied at read. An extension with its **own** `#@ persist` (case 57,
`.../57-extend-persist-materializes-nothing-new`) is query-shaped and keeps its own table, unchanged.

### 3.4 `persist-not-materializable`, pinned

A parity spec, `plan/persist_not_materializable_parity.spec.ts`, is written **against v1 first**.
It passes on `main` before any planner code changes, and must pass unchanged on the flip. It
asserts the finding's code, tier `malformed`, the exact message text, the load failure, and the
build refusal (`run/materialization_service.ts:1360-1363`). It covers each case below:

| Case                                                                                    | Expected                                                                                                                                      |
| --------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| own `#@ persist` on `t extend { where: … }` (hammer case 19)                            | malformed, load fails                                                                                                                         |
| own `#@ persist` on `t extend { dimension: … }`                                         | malformed, load fails                                                                                                                         |
| own `#@ persist` on `t` (a bare table source)                                           | malformed, load fails                                                                                                                         |
| own `#@ persist` on a table-shaped unbound parameterized source                         | malformed, load fails                                                                                                                         |
| own `#@ persist` on `q extend { dimension: … }` where `q is t -> { … }`                 | persists (query-shaped)                                                                                                                       |
| own `#@ persist` on `t -> { … } extend { dimension: … }`                                | persists                                                                                                                                      |
| own `#@ persist` on `conn.sql("…")` (hammer cases 62, 63)                               | persists: it stores the SQL's result                                                                                                          |
| own `#@ persist` on a query-shaped source with an unbound parameter                     | refused, as today: `free_parameter` for `storage=` (`plan/materialization_eligibility.ts:145-160`), `unkeyable` omission on the database tier |
| inherited-only `#@ persist` on a table-shaped extend of a table-shaped persisted source | **v1:** a second finding on the extend. **v2:** the finding is on the declaring source only, and the load still fails (expected class E5)     |
| `#(materialize)` on an unbound parameterized source                                     | refused (`free_parameter`), unchanged                                                                                                         |

### 3.5 Deleted in the flip release

These are deleted:

- `v1PlanModel`, `reportPersistLineFindings`, `planV1PersistSources`, `detectDroppedPersistSources`,
  `declaredPersistSourceIDs` and `sourceIDsWithNoteAt` (`plan/build_plan.ts`);
- `plan/v1_build_companion.ts`. Its corpus spec becomes the address-parity spec;
- `persistOverMaterializedReason` (D6);
- the internal `droppedPersistSources` field (`domain/compiled_build_plan.ts:94`,
  `domain/package_plan.ts`) and its golden key;
- the `inherited_persist` refusal, apart from decision [D5](#decisions-kyle-2026-11-28);
- the five copies of the ownership rule;
- the v1 `#@ preaggregate` synthesis, classifier and validation (D8). The line itself is still read,
  by the rollup leg, as a `#(rollup)` synonym;
- the off position of `materialize.enabled` (D9).

The D4 bind rule is added in that release and deleted in a later one.

## 4. Parity inventory

Every row is something the v1 walk does today, with its v2 home. "Unchanged" means the persist leg
calls the same function on the same `PersistSource`, so behaviour cannot drift. The rows that are
not "unchanged" are marked.

| What                                                                                                      | Where today                                                                                                                                                                                                                                | v2 home                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| --------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `name=`, `storage=`, database tier                                                                        | `placementOf` (`plan/declaration.ts:150`), `deriveAnnotationFields` and `declaredStorage` (`plan/persist_source_fields.ts:51`, `:61`), `persist.storageMode` (`settings/registry/storage.ts:119`)                                          | Unchanged: read off the planned `PersistSource`                                                                                                                                                                                                                                                                                                                                                                                         |
| `name=` validity                                                                                          | `persistNameProblems` (`plan/persist_annotation_validation.ts:103`), `tableNameProblem` (`:69`)                                                                                                                                            | Unchanged (step 2 of 3.2)                                                                                                                                                                                                                                                                                                                                                                                                               |
| Incremental: `refresh=`, watermark, `merge_key`, rolling terms                                            | `resolveIncrementalDeclaration` (`plan/incremental_declaration.ts:163`), `incrementalPolicyRejections` and `incrementalUnbuildableReason` (`plan/incremental_policy.ts:161`, `:178`), ledger keyed by address (`run/incremental_build.ts`) | Unchanged. The `persist-incremental-shared-address` advisory (`plan/incremental_policy.ts:516`) no longer fires where an inherited-only extension was the other sharer (expected diff E3)                                                                                                                                                                                                                                               |
| Partitions                                                                                                | Retired: `partition=` is `persist-retired-key` (#1758/#1759, `docs/publisher-v1.md:1050`)                                                                                                                                                  | Unchanged, nothing to port                                                                                                                                                                                                                                                                                                                                                                                                              |
| Freshness                                                                                                 | `resolveFreshness` (`plan/plan_layers.ts:115`), read from the source tag, model tags and the package                                                                                                                                       | Unchanged                                                                                                                                                                                                                                                                                                                                                                                                                               |
| Schedule, refresh cadence                                                                                 | Package level only (`materialization.schedule`, `run/materialization_scheduler.ts`). Per-source `schedule=` and `sharing=` are retired (`docs/publisher-v1.md:1048`)                                                                       | Unchanged, nothing per source                                                                                                                                                                                                                                                                                                                                                                                                           |
| Query metadata                                                                                            | `resolveQueryMetadata` (`plan/plan_layers.ts:166`), kept out of the address                                                                                                                                                                | Unchanged                                                                                                                                                                                                                                                                                                                                                                                                                               |
| Givens                                                                                                    | `buildSubstitutesAGiven` (`plan/materialization_eligibility.ts:413`), `inherited_where_with_given` (`:321`), `admitGivenPositions` (`plan/persist_dynamic_terms.ts:418`)                                                                   | Unchanged: eligibility is per `PersistSource` and does not depend on kind                                                                                                                                                                                                                                                                                                                                                               |
| Current time                                                                                              | `storedAsOfBuild` (`plan/persist_dynamic_terms.ts:229`), read for `kind: "persist"` (`:233`)                                                                                                                                               | Unchanged                                                                                                                                                                                                                                                                                                                                                                                                                               |
| Gates                                                                                                     | `classifyPersistSourceGate` (`plan/build_plan.ts:656`), gate-policy row `persist` (`domain/gate_policy.ts:133`, `:187`), `materialize.gatedSources`                                                                                        | Unchanged for planned sources. **Changed:** an inherited-only extension is no longer classified at plan; its gate is grafted and proven at read by the served copy, through its base's table. A base is no longer refused because such an extension's gate was refused (`refuseDatabaseTablesSharedWithARefusal`, `plan/build_plan.ts:2681`; expected class E6, **Signed off**, D7). A security review of that read path gates the flip |
| Content address                                                                                           | `computeSourceEntityId` (`plan/addressing.ts:376`): connection digest over `BuildTarget.sql`, plus the folds                                                                                                                               | Unchanged, proven by the address-parity gate over the persist companion. See [4.1](#41-addresses-and-table-names-do-not-move)                                                                                                                                                                                                                                                                                                           |
| Compiler key                                                                                              | `compilerBuildId` (`plan/addressing.ts:479`)                                                                                                                                                                                               | Unchanged: the target's `buildId`, covered by the address-parity gate                                                                                                                                                                                                                                                                                                                                                                   |
| Folds (distinct `name=`, projection, twins)                                                               | `foldDistinctTableNames` (`plan/addressing.ts:697`), decided over the whole plan                                                                                                                                                           | Unchanged for every retained source (argued in 4.1)                                                                                                                                                                                                                                                                                                                                                                                     |
| Upstream schema fold                                                                                      | `upstreamFoldOf` (`plan/addressing.ts:426`)                                                                                                                                                                                                | Unchanged                                                                                                                                                                                                                                                                                                                                                                                                                               |
| Physical table names                                                                                      | `readableTableName` (`domain/table_name.ts:269`), `namingSources` (`run/run_instructions.ts:327`), sticky by address (`tableNameDecision`, `:603-670`)                                                                                     | Unchanged for retained sources. A shared address that an inherited-only extension named keeps its recorded name through the sticky rule. Hosted names are not sticky; reuse is by address, and only the next generation may be renamed (4.1)                                                                                                                                                                                            |
| Table ownership (author `name=`)                                                                          | `hasAuthorTableName` (`run/table_ownership.ts:174-187`), table claims, reclaimer                                                                                                                                                           | Unchanged: they read the kind and the annotation                                                                                                                                                                                                                                                                                                                                                                                        |
| GC: standalone                                                                                            | Reclaimer, the rename rule (`docs/materialization.md`, "The rename rule")                                                                                                                                                                  | Unchanged. An inherited-only extension had no table of its own: it shared its base's address, so nothing is orphaned                                                                                                                                                                                                                                                                                                                    |
| GC: hosted                                                                                                | The control plane's `AnchorRetirementPolicy` (service repo, `controlplane/.../services/AnchorRetirementPolicy.java`) retires an anchor that is absent from a complete plan                                                                 | **Changed on the wire:** the inherited-only extension's anchor leaves the plan and retires as `absent_from_complete_plan`. It drops no physical table that its base's anchor still holds at the same address: confirmed by the service review (`MaterializationGarbageCollector.isCollectible` refcounts the serving table and the address; gate G3)                                                                                    |
| Run telemetry                                                                                             | `buildKindOf` (`run/run_telemetry.ts:89`) reads `table.kind`                                                                                                                                                                               | Unchanged: the kind stays `persist`                                                                                                                                                                                                                                                                                                                                                                                                     |
| Refusal and finding codes                                                                                 | `persist-invalid`, `persist-not-built`, `persist-unparseable`, `persist-unknown-key`, `persist-retired-key` (`annotation_reader.ts:444-767`), `persist-not-materializable`, `persist-incremental-*`, `persist-reads-current-time`          | Unchanged codes. `persist-unparseable` comes from the leg's own `readNotes` rather than from core's `tagParseLog`; the plan-diff compares the finding sets                                                                                                                                                                                                                                                                              |
| Wire: `PersistSourcePlan` (`origin`, `name`, `sourceID`, `sql`, `sourceEntityId`, `refresh`, `freshness`) | `deriveBuildPlan` and `wireSourceOf` (`plan/build_plan.ts:2511`, `:2612`); `origin: originOfTable` (`:2625`, `domain/table_plans.ts:824`); `sql: addressSQL` (`:2634`)                                                                     | Unchanged: `origin` stays `persist` (`api-doc.yaml:11499-11501`), and the control plane's `Materialization.origin` default `persist` stays true (`MaterializationDao.java:163-171`)                                                                                                                                                                                                                                                     |
| Wire: `buildPlan.graphs` and `dependsOn`                                                                  | `sourceBuildsOf` (`plan/addressing.ts:106`)                                                                                                                                                                                                | Unchanged among retained sources. Inherited-only extensions leave the graph. New edges to `#(materialize)` upstreams (D6) and persisted `.sql()` upstreams (D10)                                                                                                                                                                                                                                                                        |
| Wire: `refusedSources`                                                                                    | Per-table verdicts                                                                                                                                                                                                                         | Unchanged reasons. **Changed:** no entry for an inherited-only extension. `inherited_persist` stops being produced (`api-doc.yaml:11220`); the control plane does not branch on it (service review)                                                                                                                                                                                                                                     |
| Wire: `omittedSources` and `omittedFiles`                                                                 | `v1_build_companion_failed`, `unkeyable` (`plan/build_plan.ts:2577-2588`)                                                                                                                                                                  | **New value** `persist_companion_failed` (**Signed off**). `unkeyable` is unchanged                                                                                                                                                                                                                                                                                                                                                     |

### 4.1 Addresses and table names do not move

This needs to be shown, not assumed, because a moved address rebuilds a table across the whole
fleet and re-seeds every incremental source on it.

1. **`BuildTarget.sql` is byte-identical, by gate.** Under D2 the target comes from the persist
   companion rather than the author's file, so equality is proven, not inherited. The
   **address-parity gate** (item 6) checks every corpus source's address, compiler key and wire SQL
   against v1's. It is the successor of `v1_build_companion_corpus.spec.ts`, which proved the
   same thing for the build copy. Selecting from the targets cannot change a target.
2. **Folds do not change for any retained source.** `foldDistinctTableNames` groups sources by
   unfolded address and folds only when the group's members have different keys: name, stored
   projection or tier (`plan/addressing.ts:763-826`). An inherited-only extension takes its name
   (`deriveAnnotationFields`), its stored projection ("an extension that inherits a declaration is
   on its table", `:782-801`) and its tier all from its base's declaration. Its key therefore always
   equals its base's key, so removing it never changes whether the group folds or what any other
   member folds to.
3. **Upstream-schema folds** are per source (`upstreamFoldOf`), so a retained source is unaffected.
4. **Physical names.**
   - A retained source's readable name hashes (environment, package, declaring model path, source
     name, version) (`domain/table_name.ts:269-300`), and none of those changes.
   - For an address several sources share, `namingSources` picks the smallest (model path, label).
     If an inherited-only extension sorted first, it named the table. The run keeps a recorded
     readable name for the same address (`keepsReadableName` through `tableNameDecision`,
     `run/run_instructions.ts:603-670`, "a source that comes to share the address, does not rebuild
     it under another"), so the base keeps that table under its existing name.
   - Hosted naming is **not sticky by address, and that is harmless** (Credible's service review of
     this PR). The control plane's `BuildPlanClassifier.namingSources` recomputes the name from the
     current plan on every classify, taking the smallest (`authorModelPath`, `sourceLabel`). Reuse
     is keyed by content address, so a READY generation keeps its physical name. Only the next
     generation built at that address may take the base-derived readable name. Making hosted names
     sticky is an optional service change, not a gate.
5. **Manifest entry names. This is a real risk, and it needs a step.**
   - A standalone manifest entry is keyed by address and carries one `sourceName`
     (`run/source_build.ts:898`).
   - A hosted manifest is composed by the control plane. `ManifestPublisher.foldSharedAddress` takes
     a shared address's table identity from the entry with the smallest source name. For gtme,
     `deals` sorts before `deals_including_deleted`, so the hosted entry names the inherited-only
     source.
   - The bind declines an entry when no plan source with that `sourceName` sits at its address
     (`serve/bind.ts:124-136`, from #1713).
   - So, in both modes: once an inherited-only extension that names a shared entry leaves the plan,
     the base's table is declined and serves live until a replan rewrites the manifest.
   - The fix is decision [D4](#decisions-kyle-2026-11-28), signed off: a temporary bind rule that accepts an
     entry whose named source is absent from the plan when a persist-leg source holds the entry's
     address. It needs no control-plane change, and it covers standalone and hosted manifests alike.
     It ships in the flip release and is deleted once every cell has replanned.
6. **Pins. These are flip gates.**
   - **Address parity:** `plan/persist_v2_address_parity.spec.ts`, over
     `test_helpers/persist_corpus.ts` (fixture models and every hammer model). For every source the
     v1 goldens record, it compares the persist companion's address, compiler key, wire `sql`, fold
     and auto-run table name with the address recorded in
     `tests/goldens/materialization/corpus/hammer/*/*.build.sql`. Those files record each
     `sourceEntityId` under a fixed connection digest, so they hold v1's addresses after the v1 code
     is gone. A source v1 recorded and v2 does not plan must be an inherited-only extension whose
     address equals a retained source's address.
   - **Chained-build parity:** every chained persist case in the corpus is built under the base
     release and under the flip branch, on DuckDB and Postgres. That covers hammer 11, 54, 71–74,
     85, 91, 95, 96, 101, 106, 107 and 114, and the service fixture `persist-dag`. Each persisted
     source with a stored upstream must be `reused`, name the same upstream tables in `builtFrom`,
     and write the same rows.
   - **Goldens:** the plan goldens (148 hammer cases) are regenerated on the flip branch. They must
     differ only by the expected classes in [5](#5-rollout).

## 5. Rollout

### No runtime switch (D3)

There is no `persist.planner` setting, no shadow mode on the fleets, no release that carries both
planners, and no per-cell ConfigMap step. The flip is a code change in one breaking minor. The
v1-to-v2 comparison runs where the two releases already meet: in Publisher CI, against the goldens
and the parity gates, and in the service's conformance job, between the pinned release and the
candidate.

### The plan-diff

The service's `v1-cutover-conformance` job runs Publisher's
`packages/server/scripts/v1-cutover-conformance.ts` over the service fixtures
(`e2e/conformance/v1-cutover/`: `persist-dag`, `persist-storage`, `preaggregate-v1-colocated`,
`preaggregate-v1-storage`, `unflagged-persist`). The runner gains a `--plan-diff` mode, which plans
each fixture with a base and a candidate Publisher.

**What it compares,** per package and keyed by `sourceID`:

- the set of planned `sourceID`s and their kind;
- address, compiler key, fold, wire `sql`;
- the auto-run table name, with no prior entry and with the base's recorded entry;
- placement, freshness, refresh and the incremental declaration;
- gate classification;
- `dependsOn` among the sources both plans hold;
- `refusedSources` by reason;
- `omittedSources` and `omittedFiles`;
- the package's malformed findings by code, subject and message. `persist-not-materializable` must
  match byte for byte.

It prints classed rows per fixture, and the Python wrapper fails on any diff outside the expected
classes:

| Class                                    | What                                                                                                                                                                                                                | Verdict  |
| ---------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------- |
| E1 `inherited_only_left_plan`            | A base-only `sourceID` that is an inherited-only extension whose address equals a retained source's address                                                                                                         | expected |
| E2 `materialize_on_inherited_planned`    | An `inherited_persist` refusal replaced by a planned `#(materialize)` chain over the base's table                                                                                                                   | expected |
| E3 `shared_address_advisory_gone`        | `persist-incremental-shared-address` gone where only an inherited-only extension shared                                                                                                                             | expected |
| E4 `omission_code_renamed`               | `v1_build_companion_failed` reported as `persist_companion_failed`                                                                                                                                                  | expected |
| E5 `not_materializable_on_declarer_only` | A `persist-not-materializable` finding that v1 also raised on an inherited-only extension; the declaring source's finding is identical, and the load fails either way                                               | expected |
| E6 `base_unrefused`                      | A base refused in v1 only through `refuseDatabaseTablesSharedWithARefusal` against an inherited-only extension (D7)                                                                                                 | expected |
| E7 `persist_over_materialize_planned`    | A `storage=` `#@ persist` over a `#(materialize)` table in its destination, refused `upstream_table_unusable` by the base, now planned over that table, with its new edge (D6)                                      | expected |
| E8 `sql_upstream_stored`                 | A view, query, rollup or persist source over a persisted `.sql()` source that now has an edge to its table and builds over it, or is refused at plan as `upstream_table_unusable` or `generated_build_failed` (D10) | expected |
| E9 `preaggregate_warning_text`           | A `deprecated-preaggregate-annotation` warning whose message changed to the synonym text (D8); the rollup plans, addresses and tables are identical                                                                 | expected |
| everything else                          | any moved address, name or edge; a changed refusal reason; a new or missing finding; a plan that fails on one side                                                                                                  | **bug**  |

An E1 whose address does **not** equal a retained source's would mean the extension had a table of
its own that v2 drops. It is a bug until shown otherwise.

The two `preaggregate-v1-*` service fixtures keep their `#@ preaggregate` lines and must plan exactly
as before: the same rollups, names, addresses and tables. Only their deprecation warning's text
differs (E9). Any other diff on them is a bug (see
[`#@ preaggregate` as a synonym of `#(rollup)`](#-preaggregate-as-a-synonym-of-rollup)).

The runner's existing served-versus-live check runs on the candidate too. Every `served_equal` must
stay `served_equal`. An inherited-only extension that is now served through its base's table is
therefore checked for equal answers, not only equal plans.

**Where it runs** (settled by Credible):

- **The gate is the bump PR.** The plan-diff runs in the service PR that bumps `publisher_version`
  in `infrastructure/malloy-versions.json`. The base is the version being replaced and the candidate
  is the new one, so it runs exactly the release under test. It is required on every PR that touches
  that file.
- **Before a release.** `v1-cutover-conformance.yml`'s `workflow_dispatch` gets an optional
  `publisher_ref` input. It takes an npm version, an image tag or a git ref, and defaults to
  `publisher_version`, so a candidate can be diffed before it is released.
- **Early warning.** `v1-cutover-conformance-main.yml` stays daily on Publisher `main`.

**Rule:** a cell takes the flip release only after that release's bump-PR plan-diff is green. The
service owns pinning the release in each cell.

### Phases

The critical path is marked ★. Everything breaking is in phase 2.

0. **★ Harness and non-breaking groundwork.** This lands on `main` in ordinary releases, with no
   behaviour change:
   - the `persist-not-materializable` parity spec (3.4), written against v1;
   - the address-parity and chained-build parity specs (4.1), written against v1's goldens and
     builds;
   - the diff classifier, the runner's `--plan-diff`, and the service's required bump-PR check and
     `publisher_ref` input;
   - per-leg containment in `collectModelBuildPlan`, so one leg's throw costs only that leg's file;
   - moving the three helpers `rollup_plan.ts` takes from the `#@ preaggregate` modules (below);
1. **★ `StoredParent` and the persist companion generator, unused.** `chainVerdict` and the
   cross-file walk take `StoredParent` (3.3). With no persist parent planned yet this changes
   nothing. The companion text generator lands with its unit tests. Both can merge before the
   breaking minor.
2. **★ The breaking minor.** It holds, in this order on its branch:
   1. removing `materialize.enabled`'s off position (D9);
   2. `tableOwnerOf` with core's shape predicate (D10);
   3. the persist leg through its companion (D2), replacing the walk;
   4. the `StoredParent` link that plans a `#(materialize)` on an inherited-only extension;
   5. `inherited_persist` narrowed (D5);
   6. lifting the D6 refusal;
   7. the D4 bind rule;
   8. `#@ preaggregate` read as a `#(rollup)` synonym, with its new warning text, and the v1
      synthesis, classifier and validation deleted (D8);
   9. deleting the v1 walk and everything in 3.5.

   Its gates:
   - **Address parity:** every persist address, compiler key, wire `sql` and table name is
     unchanged under the companion (4.1, item 6).
   - **Chained-build parity:** persisted upstreams still substitute (4.1, item 6).
   - **D7 security review:** an adversarial review of the read path for a gated inherited-only
     extension over an ungated base's stored table.
   - **Goldens:** the regenerated plan goldens differ only by E1–E9.
   - **Preaggregate parity:** every `#@ preaggregate` case plans the same rollups under the synonym
     as under the v1 reader (see the preaggregate section).
   - **Service, already confirmed** (G3, Credible's review): retiring an inherited-only anchor
     (`AnchorRetirementPolicy.REASON_ABSENT`) drops nothing its base holds.
     `MaterializationGarbageCollector.isCollectible` drops a table only when
     `countByServingTableId == 0` and no binding remains at the content address. Retirement teardown
     passes the same refcount, so same-name dedup and the package-wide refusal still hold
     (`MaterializationCommandService`, around lines 1738–1830). Hosted names are not sticky, which
     is harmless (4.1, item 4).
   - **Before cells take it:** a green bump-PR plan-diff for the release.

   The release notes name every **Signed off** item in section 7.

3. **Delete the D4 bind rule** in a later release, once the service confirms that every cell runs
   the flip release and has replanned.
4. **Docs.** `plan/README.md`'s `persist` row and `docs/materialization-v2-plan.md:132` are
   rewritten with the flip. `docs/publisher-v1.md:1049`, which still calls
   `persist-not-materializable` "never refused", and `:1320`, the 1.0.0 plan for
   `#@ preaggregate`, are corrected at the same time.

### `#@ preaggregate` as a synonym of `#(rollup)`

**Signed off ([D8](#decisions-kyle-2026-11-28), revised):** `#@ preaggregate` is not retired and
does not fail the load. It becomes a pure synonym of `#(rollup)`. It behaves identically and shares
one implementation, the v2 rollup leg, and using it warns. The authoring rule is unchanged: docs,
skills and examples write `#(rollup)` only.

**How the synonym is read.** The rollup leg already reads a measure's `#@ preaggregate` lines as its
own declarations. It builds and serves them under the `#(rollup)` rollup's name
(`plan/rollup_plan.ts:892-920`, step G4 of the 0.13 cutover). It still reads them through v1's own
reader (`readPreaggregateAnnotation`) and v1's validation. Under the synonym, each `#@ preaggregate`
note is read by `#(rollup)`'s declaration reader (`readDeclaration`, `plan/rollup_plan.ts:499`),
through a small normalizer for the spellings v1 accepts and `#(rollup)` does not:

- `grain="a, b"`, a comma-separated string, is read as `grain=["a", "b"]`;
- `#@ -preaggregate` clears the grains declared above it on that measure, as it does today
  (`plan/preaggregation_annotation.ts:24-46`);
- `namespace=` is `name=`'s deprecated spelling on both routes already.

Everything else is `#(rollup)`'s. That covers the key set, the grain rules, and the refusal and
malformed codes (`rollup-unknown-key`, `rollup-unknown-grain`, `rollup-not-mergeable`,
`rollup-invalid`, `rollup-not-built` and the rest). `preaggregateViolationCode` already maps v1's
violations onto those codes (`plan/load_gate_text.ts:387-402`). `PREAGG_WARN_ONLY`'s two refusals
are `#(rollup)`'s own reasons, `hidden_field` and `join_not_in_grain` (`:372-382`). A v1 line that
fails today therefore fails the same way, and a valid one plans the same rollup.

**Deleted with the v1 walk:**

- the v1 synthesis (`preaggregation_synthesis.ts`);
- the classifier (`preaggregation_classifier.ts`);
- v1's validation (`preaggregation_validation.ts`, `preaggregateGateLines` and `PREAGG_WARN_ONLY` in
  `plan/load_gate_text.ts:372-420`, `v1RollupRefusals` in `plan/build_plan.ts:2192`, and the
  model-level import in `service/model.ts:145`);
- v1's reader (`preaggregation_annotation.ts`), whose two rules above move into the normalizer.

Three helpers `rollup_plan.ts` imports from those modules move first, in phase 0:

- `hasFanoutJoin` (`plan/preaggregation_validation.ts:160`);
- `basePersistNamespace` (`plan/preaggregation_synthesis.ts:119`), which reads the base's
  `#@ persist` `name=` container and so uses `tableOwnerOf`;
- `nameContainer` and `readRollupContainer` (`plan/preaggregation_namespace.ts`).

**The warning.** The code stays `deprecated-preaggregate-annotation` (`annotation_reader.ts:468`).
Its reason for existing is unchanged, and hosts and dashboards already key on it. The usage counter
it feeds (`preaggregate_annotation` in `deprecated_usage_metrics.ts`) stays. The warning is
emitted by `deprecated/lints.ts:84-97`. Today it is shown only with `materialize.enabled` on
(`:111-117`); with D9 it is always shown.

Today's message is:

> Measure M on source S uses #@ preaggregate, which is deprecated: its v2 spelling is #(rollup) grain=[…] (see …). Until you switch, #@ preaggregate still builds and serves its rollups.

**Proposed message, for Kyle's sign-off.** It names the exact rewrite and drops "until you switch",
since the synonym is not retired:

> Measure `total` on source `orders` uses `#@ preaggregate`, a deprecated synonym of `#(rollup)`: it is read and built exactly as `#(rollup) grain=["category", "order_day"]`. Write `#(rollup)` instead (see docs/materialization-migration.md#preaggregate).

The `#(rollup)` text in the message is the normalized declaration, with `storage=` or `name=` added
when the line has one. There is one warning per measure and line, with `subject` the source.

`#(preaggregate)`, the existing route alias of `#(rollup)` (`plan/rollup_plan.ts:877-884`), keeps
working the same way under the same code. Its text says "one release", which is no longer true, so it
is reworded to match. **Sign-off:** keep `#(preaggregate)` as a synonym too, rather than retiring it
on its own.

**Addresses stay the same, and how that is proven.** A rollup's name, and so its readable table
name and address, derives from its plan: the base, the canonical grain, placement and measures
(`plan/preaggregation_synthesis.ts`, "Determinism is a correctness property"). Since the 0.13
cutover, v1 lines are already planned as `#(rollup)` plans by the rollup leg. The synonym changes
only the reader that produces the declaration. When the normalizer yields the same grain, placement
and measure set, the plan is the same, and so are the name, address and table. Three things prove
it:

- `plan/v1_rollup_plan.spec.ts:24-35` already shows that a `#@ preaggregate` line and a `#(rollup)`
  line of the same grain plan one rollup;
- a new `plan/preaggregate_synonym_parity.spec.ts` plans every `#@ preaggregate` case under the v1
  reader (on `main`, before the change) and under the synonym. The cases come from the hammer
  `preaggregation` scenario group and `preaggregation_*` specs, and from the service's
  `preaggregate-v1-colocated` and `preaggregate-v1-storage` fixtures. The spec asserts equal rollup
  names, grains, measures, placements, addresses and auto-run table names, and equal findings apart
  from the warning text;
- the bump-PR plan-diff on the two service fixtures allows only E9.

The service already treats `origin: preaggregate` as "retained, no longer produced"
(`api-doc.yaml:11499-11517`), and a synonym rollup's entry stays `origin: rollup`.

## 6. Sequencing with X1

X1, a separate item not yet approved, re-roots joined stored sources onto their tables and touches
chain re-rooting. It overlaps this design in four places:

1. **The stored-source predicate.** X1 needs to know which joined sources are stored. Today that
   rule has five copies and disagrees on `.sql()` sources (§1, item 5). `tableOwnerOf` is that
   predicate. It lands in the breaking minor, together with D10's shape change. **X1 consumes it and lands after the breaking minor.** This is the one hard dependency.
2. **Build edges.** X1 adds join edges. The persist leg takes its edges from `withDownstreamEdges` instead of core's walk. Both should end up in one edge function in
   `plan/addressing.ts`. Whichever lands second extends it.
3. **The re-root text.** `chainVerdict` and the materialize build copy's parent re-root
   (`materialize_build_copy.ts`, `stored_base_copy.ts`, `pair_reroot.ts`) are touched by both:
   - this design passes them a `StoredParent` for a persist parent;
   - X1 re-roots joins in the same copies.

   `StoredParent` lands in phase 1, ahead of the breaking minor, so X1 builds on it either way.
   They are **not to be changed in parallel on one branch**.

4. **Addresses.** X1 must follow #1998's rule: a build reads upstream tables, but its address is the
   digest of the un-substituted `BuildTarget.sql`. Otherwise X1 moves addresses. Its release goes through the same bump-PR plan-diff. X1 changes build
   SQL, not plans, so the diff should be clean; the chained-build parity and served-versus-live runs
   are repeated for it.

The order is:

1. Phases 0 and 1 here.
2. The breaking minor (phase 2).
3. X1.

X1 does not block the flip. The flip blocks X1 only through `tableOwnerOf`.

## 7. Contract and behaviour changes (signed off)

All of these ship in the one breaking minor, except where an item says otherwise.

1. An inherited-only `#@ persist` source leaves `buildPlan.sources`, `graphs` and `refusedSources`.
   It gets no table of its own and is served through its base's table. Hosts retire its anchor.
   That is the only change the control plane sees, and the service review confirmed the retirement
   drops nothing its base holds.
2. A `#(materialize)` on an extension of a persisted query source is planned as a chain over the
   base's stored table. Today it is refused `inherited_persist`. This is the gtme fix.
3. `inherited_persist` is produced only where own and inherited notes cannot be told apart. That
   source is refused alone and answered live (D5).
4. New `OmittedSource.reason` value `persist_companion_failed`. `v1_build_companion_failed` is
   retained and no longer produced.
5. `persist-not-materializable` is raised on the declaring source only. An inherited-only extension
   no longer gets a second finding, and the load fails as before.
6. A base is no longer refused because an inherited-only extension's gate was refused. The
   extension's gate is enforced at read (D7). A security review gates the flip.
7. A `storage=` `#@ persist` over a `#(materialize)` table in its destination is built over that
   table, view-style, instead of refused `upstream_table_unusable` (D6).
8. A persisted `.sql()` source is a stored upstream. Views, queries, rollups and persist sources
   over it build over its table or are refused at plan, never built from raw (D10).
9. For one release window, the bind accepts a manifest entry whose named source is absent from the
   plan when a persist-leg source holds the entry's address (D4). This is a temporary relaxation of
   #1713's same-name check, deleted once every cell has replanned.
10. `#@ preaggregate` is a deprecated synonym of `#(rollup)`: it is read by the rollup leg, plans
    the same rollups, and does not fail the load. It warns with `deprecated-preaggregate-annotation`
    and the new message text (D8, revised). `#(preaggregate)` stays a synonym as well.
11. `materialize.enabled` loses its off position (D9).

## Decisions (Kyle, 2026-11-28)

Several choices differ from this doc's earlier recommendations. The design above follows the
choices.

**D1. Keep Malloy's `BuildTarget` as the per-source SQL and key primitive: (a), keep it.** Only
core's walk is deleted. Addresses stay digests of `BuildTarget.sql`, and the persist leg asks core
for targets over a generated, flagged file, as every v2 leg does.

**D2. The compile vehicle: (b), the per-source generated companion, at the flip.** Source-level
`#@` lines are blanked and each build source is presented as the author's source (3.2). The build
copy is not used. Two parity runs gate the flip itself: address parity (every persist address
unchanged under the companion) and chained-build parity (persisted upstreams still substitute).

**D3. The switch: (b), none.** There is no `persist.planner` setting, no shadow mode, no per-cell
ConfigMap step and no release that carries both planners. The comparison runs only in CI and in the
service's conformance job:

- the required `--plan-diff` on the PR that bumps `publisher_version`;
- the `publisher_ref` dispatch input for candidates;
- the daily job on `main` as the early warning.

The flip is a code change in one release. A cell takes it only after a green bump-PR plan-diff.

**D4. Manifest entries named after an inherited-only extension: (b), the temporary bind rule.** The
bind accepts an entry whose named source is absent from the plan when a persist-leg source holds the
entry's address. It ships in the flip release and is deleted once every cell has replanned. Kyle
signed off on this temporary relaxation of the bind's same-name check. The alternatives, (a) with a
control-plane `inheritsPersist` flag and (c) accepting live serving, are not taken.

**D5. What remains of `inherited_persist`: (a).** It stays only where own and inherited notes
cannot be told apart. That source is refused alone and answers live.

**D6. A `storage=` `#@ persist` over a `#(materialize)` table in its destination: (b), lift the
refusal in the flip.** It builds over the stored table, view-style (3.2, step 4). Those plans are
expected class E7. There is no "lift after" follow-up.

**D7. The gate coupling: (a), accept the decoupling.** A base is no longer refused through an
inherited-only extension's gate refusal. An adversarial security review of the read path, a gated
extension over an ungated base's stored table, is a flip gate.

**D8. `#@ preaggregate`: revised by Kyle, a synonym, not a retirement.** `#@ preaggregate` does
not fail the load. It is a pure synonym of `#(rollup)`: it behaves identically and shares the v2
rollup leg's implementation. The line is read as `#(rollup)`, and the v1 synthesis and classifier
are deleted with the v1 walk. Using it warns that it is deprecated and to write `#(rollup)`, under
the existing `deprecated-preaggregate-annotation` code; the proposed message text is in the
preaggregate section, for sign-off. The authoring rule is unchanged: docs, skills and examples use
`#(rollup)` only. This replaces the earlier choice of `rollup-retired-annotation` and its
counter-triggered fallback.

**D9. `materialize.enabled`: (a).** Its off position is removed first, in the same breaking minor.

**D10. One shape predicate: (a), in the flip.** Core's `query_source` or `sql_select` is used
everywhere. A persisted `.sql()` source becomes a stored upstream, and the plans that moves are
expected class E8.

## Critical path, in one line

1. **Phase 0:** harness, parity specs against v1, the service's required bump-PR plan-diff and
   `publisher_ref` input, per-leg containment, and the helper moves.
2. **Phase 1:** `StoredParent` and the companion generator.
3. **Phase 2, one breaking minor:** D9, D10, the persist leg through its companion, D5, D6, the D4
   bind rule, the D8 synonym, and deleting v1. Gated on address parity, chained-build parity,
   the D7 security review, the goldens and preaggregate parity. Cells take it after a green bump-PR plan-diff.
4. **Phase 3:** delete the D4 bind rule once every cell has replanned.

X1 follows phase 2.
