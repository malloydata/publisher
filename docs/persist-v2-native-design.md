# Planning `#@ persist` natively in the v2 planner

Status: **design for sign-off, no implementation.** Requested by Kyle Nesbit, 2026-11-28.
Read against `origin/main` at `7d5c46ff1`. Every `file:line` below is under
`packages/server/src/service/materialization/` unless it names another root.

Items marked **Sign-off** are new contract values or behaviour changes. They need an explicit
yes before anything is built. The decisions that need Kyle are gathered at the end, in
[Decisions for Kyle](#decisions-for-kyle).

## Summary

`#@ persist` on a query-shaped source is still planned by the v1 walk. That walk is Malloy
core's persistence walk (`getBuildTargets`) run over each author file. Where a file lacks
`##! experimental.persistence`, it runs over a flagged build copy of the file
(`plan/v1_build_companion.ts`). Every other kind of table is planned by v2 legs: `#(materialize)`,
`#(rollup)`, and `#@ persist` on a view or a named query. Serving moved onto v2 in 0.13 (the
served copy and the rollup leg). Planning did not move.

This design moves `#@ persist` planning into a v2 **persist leg** so the v1 code can be deleted.
The persist leg follows one ownership rule for every leg:

- **One leg owns each `sourceID`.** An own `#@ persist` on a query-shaped source belongs to the
  persist leg. An own `#(materialize)` belongs to the materialize leg. Both on one source stays
  malformed.
- **An inherited-only `#@ persist` never creates a table.** Such a source is a chain over its
  base's table. It gets a table of its own only with its own `#(materialize)`, which the
  materialize leg builds over the base's stored table (the #2004 machinery).
- **An own `#@ persist` on a table-shaped source stays `persist-not-materializable`.** It is
  malformed and fails the package load, word for word as today.

The persist leg still asks Malloy for each selected source's `BuildTarget`, as every v2 leg does.
That keeps every content address and physical table name unchanged. What goes is everything core's
walk decides today:

- which sources persist;
- that an extension claims its base's table;
- the refusal of a `#(materialize)` placed on that extension;
- the backstop that guesses which sources core dropped without saying so.

The flag stops mattering to authors. Planning failures are isolated per file.

Rollout follows the 0.13 pattern:

1. A registry setting (`persist.planner`: `v1`, `shadow`, `v2`).
2. A shadow diff on the dev and staging fleets and in the service's conformance job.
3. A release that carries both planners.
4. The flip.
5. Deleting the v1 walk, `v1_build_companion.ts` and the `#@ preaggregate` readers.

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
  `#@ preaggregate` is deprecated and goes.
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
| own and inherited cannot be told apart         | any          | refused alone, answered live (see decision [D5](#decisions-for-kyle))                                                |

The last row is the only "genuinely ambiguous" case. It happens when an annotation can be read but
its notes cannot be attributed to the source's own statement. That is the identity rule in
`declared_notes.ts`, known limitation 4 in `docs/materialization-v2-plan.md`. Today that rule fails
open as "declares", in `plan/addressing.ts:560-573`.

### 3.2 The persist leg

**Where it lives.** It gets its own planner, `plan/persist_plan.ts`, and a compiler,
`plan/persist_compile.ts`. That is the same split as `materialize_plan.ts` and
`materialize_compile.ts`, and as `persisted_view.ts` and `persisted_view_compile.ts`. It is called
from `collectGeneratedTables` beside the other legs. It is **not** gated on `materialize.enabled`,
because the v1 walk is not gated today; see decision [D9](#decisions-for-kyle). The `TableKind`
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

3. **Compile.** Compile the leg's **build copy** of the file, then ask Malloy for `BuildTarget`s.
   The build copy is the author's text with the flag as its first line, served over the file's own
   URL, which is what `v1BuildCompanionText` does today. It moves to `text/` beside the other
   generated files, as the persist leg's own generated file. It is used for every file that declares
   a persist-leg source, flagged or not, so one path serves both. The leg keeps only the targets for
   the sources it selected in step 1. Every other `#@ persist` source that core walks in the copy
   is dropped without a word, because it is not this leg's. That includes inherited-only extensions,
   and sources declared in other files, which their own file plans. The flagless/flagged split, and
   `v1PlanModel`, go away.
4. **Facts per table.** These are the same calls the walk makes, on the same `PersistSource`:
   `plannedFacts`, the declaring model path, `chainedStringReads`, the
   `persistOverMaterializedReason` check, and `classifyPersistSourceGate`.
5. **Edges.** The leg takes the planned sources' `dependsOn` from the targets (`sourceBuildsOf`,
   `plan/addressing.ts:47-125`), restricted to planned sources. When a persist source reads a
   `#(materialize)` table, it also takes the `withDownstreamEdges` reach (`:306`). Core cannot see
   that edge; see decision [D6](#decisions-for-kyle).
6. **Isolation.** If the build copy fails to compile or plan, the file's persist-leg sources become
   `omittedSources`, or the file goes in `omittedFiles` when the failure comes before selection.
   The failure is coded `persist_companion_failed` (**Sign-off:** a new `OmittedSource.reason`
   value). `v1_build_companion_failed` becomes "retained, no longer produced". The leg catches its
   own throws, as `compileMaterialize` does (`plan/materialize_compile.ts:198-245`), so no persist
   error can fail the package plan. The rest of `collectModelBuildPlan` should get the same per-leg
   containment. That is a separate item ([5](#5-rollout)).

**Why a build copy, not a blanked per-source companion.** The view and rollup companions blank
source-level `#@` lines and declare a generated build source per declaration. The persist leg could
do the same, presenting each generated source as the author's with `presentAs`
(`plan/addressing.ts:249`). That is more uniform. But it changes the compile that the target's
`sql` and `buildId` come from. A persisted upstream's table would also no longer be substituted by
core's `buildSQL(manifest)`. Today v1 chained database builds and v2's over-stored proofs both rely
on that substitution (`run/source_build.ts:441-498`, `run/build_upstreams.ts:405`,
`plan/over_stored_proof.ts:76`).

The build copy keeps the same compile as today, so it cannot move an address. It keeps
`v1_build_companion_corpus.spec.ts` true by construction. The blanked companion is a later
convergence step once the shadow diff is clean; see decision [D2](#decisions-for-kyle).

**What is still asked of Malloy core.** Only `Runtime.getBuildTargets` over a generated, flagged
file, for each selected source's `BuildTarget` (`sql`, `buildSQL`, `buildId`). Every v2 leg already
uses this primitive (`plan/materialize_compile.ts:890`, `:995`; `plan/rollup_compile.ts:710`;
`plan/persisted_view_compile.ts:1020`). Every address is a digest of `BuildTarget.sql`
(`plan/addressing.ts:376-411`). Publisher calls neither core's deprecated `Model.getBuildPlan` nor
`PersistSource.getSQL`. `Package.getBuildPlan` (`package.ts:1956`) is Publisher's own wire plan.

After the move, **the author's file never needs the flag**, and nothing core's walk decides about
the author's model reaches the plan. Whether to replace `BuildTarget` itself is decision
[D1](#decisions-for-kyle). Keeping it is recommended, because replacing it moves every address.

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
unchanged. The build copy re-roots `deals_including_deleted` on its table (`materialize_build_copy.ts`).
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
It passes on `main` before any planner code changes, then runs under both planner settings. It
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
| inherited-only `#@ persist` on a table-shaped extend of a table-shaped persisted source | **v1:** a second finding on the extend. **v2:** the finding is on the declaring source only, and the load still fails (expected diff E5)      |
| `#(materialize)` on an unbound parameterized source                                     | refused (`free_parameter`), unchanged                                                                                                         |

### 3.5 Deleted with the v1 planner

These are deleted:

- `v1PlanModel`, `reportPersistLineFindings`, `planV1PersistSources`, `detectDroppedPersistSources`,
  `declaredPersistSourceIDs` and `sourceIDsWithNoteAt` (`plan/build_plan.ts`);
- `plan/v1_build_companion.ts`, with its text function moved, as described in 3.2;
- the internal `droppedPersistSources` field (`domain/compiled_build_plan.ts:94`,
  `domain/package_plan.ts`) and its golden key;
- the `inherited_persist` refusal, apart from decision [D5](#decisions-for-kyle);
- the five copies of the ownership rule;
- the `persist.planner` setting itself, in step 5.

## 4. Parity inventory

Every row is something the v1 walk does today, with its v2 home. "Unchanged" means the persist leg
calls the same function on the same `PersistSource`, so behaviour cannot drift. The rows that are
not "unchanged" are marked.

| What                                                                                                      | Where today                                                                                                                                                                                                                                | v2 home                                                                                                                                                                                                                                                                                                                                                                                                 |
| --------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `name=`, `storage=`, database tier                                                                        | `placementOf` (`plan/declaration.ts:150`), `deriveAnnotationFields` and `declaredStorage` (`plan/persist_source_fields.ts:51`, `:61`), `persist.storageMode` (`settings/registry/storage.ts:119`)                                          | Unchanged: read off the planned `PersistSource`                                                                                                                                                                                                                                                                                                                                                         |
| `name=` validity                                                                                          | `persistNameProblems` (`plan/persist_annotation_validation.ts:103`), `tableNameProblem` (`:69`)                                                                                                                                            | Unchanged (step 2 of 3.2)                                                                                                                                                                                                                                                                                                                                                                               |
| Incremental: `refresh=`, watermark, `merge_key`, rolling terms                                            | `resolveIncrementalDeclaration` (`plan/incremental_declaration.ts:163`), `incrementalPolicyRejections` and `incrementalUnbuildableReason` (`plan/incremental_policy.ts:161`, `:178`), ledger keyed by address (`run/incremental_build.ts`) | Unchanged. The `persist-incremental-shared-address` advisory (`plan/incremental_policy.ts:516`) no longer fires where an inherited-only extension was the other sharer (expected diff E3)                                                                                                                                                                                                               |
| Partitions                                                                                                | Retired: `partition=` is `persist-retired-key` (#1758/#1759, `docs/publisher-v1.md:1050`)                                                                                                                                                  | Unchanged, nothing to port                                                                                                                                                                                                                                                                                                                                                                              |
| Freshness                                                                                                 | `resolveFreshness` (`plan/plan_layers.ts:115`), read from the source tag, model tags and the package                                                                                                                                       | Unchanged                                                                                                                                                                                                                                                                                                                                                                                               |
| Schedule, refresh cadence                                                                                 | Package level only (`materialization.schedule`, `run/materialization_scheduler.ts`). Per-source `schedule=` and `sharing=` are retired (`docs/publisher-v1.md:1048`)                                                                       | Unchanged, nothing per source                                                                                                                                                                                                                                                                                                                                                                           |
| Query metadata                                                                                            | `resolveQueryMetadata` (`plan/plan_layers.ts:166`), kept out of the address                                                                                                                                                                | Unchanged                                                                                                                                                                                                                                                                                                                                                                                               |
| Givens                                                                                                    | `buildSubstitutesAGiven` (`plan/materialization_eligibility.ts:413`), `inherited_where_with_given` (`:321`), `admitGivenPositions` (`plan/persist_dynamic_terms.ts:418`)                                                                   | Unchanged: eligibility is per `PersistSource` and does not depend on kind                                                                                                                                                                                                                                                                                                                               |
| Current time                                                                                              | `storedAsOfBuild` (`plan/persist_dynamic_terms.ts:229`), read for `kind: "persist"` (`:233`)                                                                                                                                               | Unchanged                                                                                                                                                                                                                                                                                                                                                                                               |
| Gates                                                                                                     | `classifyPersistSourceGate` (`plan/build_plan.ts:656`), gate-policy row `persist` (`domain/gate_policy.ts:133`, `:187`), `materialize.gatedSources`                                                                                        | Unchanged for planned sources. **Changed:** an inherited-only extension is no longer classified at plan; its gate is grafted and proven at read by the served copy, through its base's table. A base is no longer refused because such an extension's gate was refused (`refuseDatabaseTablesSharedWithARefusal`, `plan/build_plan.ts:2681`; expected diff E6, **Sign-off**, [D7](#decisions-for-kyle)) |
| Content address                                                                                           | `computeSourceEntityId` (`plan/addressing.ts:376`): connection digest over `BuildTarget.sql`, plus the folds                                                                                                                               | Unchanged by construction, because it is the same compile. See [4.1](#41-addresses-and-table-names-do-not-move)                                                                                                                                                                                                                                                                                         |
| Compiler key                                                                                              | `compilerBuildId` (`plan/addressing.ts:479`)                                                                                                                                                                                               | Unchanged                                                                                                                                                                                                                                                                                                                                                                                               |
| Folds (distinct `name=`, projection, twins)                                                               | `foldDistinctTableNames` (`plan/addressing.ts:697`), decided over the whole plan                                                                                                                                                           | Unchanged for every retained source (argued in 4.1)                                                                                                                                                                                                                                                                                                                                                     |
| Upstream schema fold                                                                                      | `upstreamFoldOf` (`plan/addressing.ts:426`)                                                                                                                                                                                                | Unchanged                                                                                                                                                                                                                                                                                                                                                                                               |
| Physical table names                                                                                      | `readableTableName` (`domain/table_name.ts:269`), `namingSources` (`run/run_instructions.ts:327`), sticky by address (`tableNameDecision`, `:603-670`)                                                                                     | Unchanged for retained sources. A shared address that an inherited-only extension named keeps its recorded name through the sticky rule. Hosted naming must be verified (4.1)                                                                                                                                                                                                                           |
| Table ownership (author `name=`)                                                                          | `hasAuthorTableName` (`run/table_ownership.ts:174-187`), table claims, reclaimer                                                                                                                                                           | Unchanged: they read the kind and the annotation                                                                                                                                                                                                                                                                                                                                                        |
| GC: standalone                                                                                            | Reclaimer, the rename rule (`docs/materialization.md`, "The rename rule")                                                                                                                                                                  | Unchanged. An inherited-only extension had no table of its own: it shared its base's address, so nothing is orphaned                                                                                                                                                                                                                                                                                    |
| GC: hosted                                                                                                | The control plane's `AnchorRetirementPolicy` (service repo, `controlplane/.../services/AnchorRetirementPolicy.java`) retires an anchor that is absent from a complete plan                                                                 | **Changed on the wire:** the inherited-only extension's anchor leaves the plan and retires as `absent_from_complete_plan`. It must not drop the physical table that its base's anchor still holds at the same address. Verify before the flip (gate G3)                                                                                                                                                 |
| Run telemetry                                                                                             | `buildKindOf` (`run/run_telemetry.ts:89`) reads `table.kind`                                                                                                                                                                               | Unchanged: the kind stays `persist`                                                                                                                                                                                                                                                                                                                                                                     |
| Refusal and finding codes                                                                                 | `persist-invalid`, `persist-not-built`, `persist-unparseable`, `persist-unknown-key`, `persist-retired-key` (`annotation_reader.ts:444-767`), `persist-not-materializable`, `persist-incremental-*`, `persist-reads-current-time`          | Unchanged codes. `persist-unparseable` comes from the leg's own `readNotes` rather than from core's `tagParseLog`; the shadow diff compares the finding sets                                                                                                                                                                                                                                            |
| Wire: `PersistSourcePlan` (`origin`, `name`, `sourceID`, `sql`, `sourceEntityId`, `refresh`, `freshness`) | `deriveBuildPlan` and `wireSourceOf` (`plan/build_plan.ts:2511`, `:2612`); `origin: originOfTable` (`:2625`, `domain/table_plans.ts:824`); `sql: addressSQL` (`:2634`)                                                                     | Unchanged: `origin` stays `persist` (`api-doc.yaml:11499-11501`), and the control plane's `Materialization.origin` default `persist` stays true (`MaterializationDao.java:163-171`)                                                                                                                                                                                                                     |
| Wire: `buildPlan.graphs` and `dependsOn`                                                                  | `sourceBuildsOf` (`plan/addressing.ts:106`)                                                                                                                                                                                                | Unchanged among retained sources. Inherited-only extensions leave the graph                                                                                                                                                                                                                                                                                                                             |
| Wire: `refusedSources`                                                                                    | Per-table verdicts                                                                                                                                                                                                                         | Unchanged reasons. **Changed:** no entry for an inherited-only extension. `inherited_persist` stops being produced (`api-doc.yaml:11220`)                                                                                                                                                                                                                                                               |
| Wire: `omittedSources` and `omittedFiles`                                                                 | `v1_build_companion_failed`, `unkeyable` (`plan/build_plan.ts:2577-2588`)                                                                                                                                                                  | **New value** `persist_companion_failed`. `unkeyable` is unchanged                                                                                                                                                                                                                                                                                                                                      |

### 4.1 Addresses and table names do not move

This needs to be shown, not assumed, because a moved address rebuilds a table across the whole
fleet and re-seeds every incremental source on it.

1. **`BuildTarget.sql` is byte-identical.** The persist leg compiles the same build copy that
   `v1PlanModel` compiles for an unflagged file. For a flagged file it compiles that same copy,
   which `v1_build_companion_corpus.spec.ts` already proves equal to the author's own file for every
   corpus model (address, compiler key and wire SQL). Selecting from the targets cannot change a
   target.
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
   - Hosted naming comes from the control plane, which "names a shared table the same way"
     (`run_instructions.ts:322-326`). It must be confirmed sticky the same way (gate G3).
5. **Manifest entry names. This is a real risk, and it needs a step.**
   - A manifest entry is keyed by address and carries one `sourceName` (`run/source_build.ts:898`).
   - The bind declines an entry when no plan source with that `sourceName` sits at its address
     (`serve/bind.ts:124-136`, from #1713).
   - Suppose a shared address's entry names an inherited-only extension. Once that extension leaves
     the plan, the base's table is declined and serves live until the base is rebuilt.
   - The fix is decision [D4](#decisions-for-kyle). The recommended option: in the release with both
     planners, v1 mode already names such an entry after the source that declares its own
     `#@ persist`. By flip time, every committed manifest then names a source that v2 keeps.
6. **Pins.**
   - `plan/persist_v2_address_parity.spec.ts`, over `test_helpers/persist_corpus.ts` (fixture
     models and every hammer model). For each `sourceID` in both plans it asserts that the address,
     compiler key, wire `sql`, fold and auto-run table name are equal. For each `sourceID` only in
     v1's plan, it asserts that the source is an inherited-only extension whose address equals a
     retained source's address.
   - The plan goldens (`tests/goldens/materialization/corpus/hammer/*/*.plan.json`, 148 cases) are
     regenerated under v2. They must differ only by the expected-diff classes in [5](#5-rollout).

## 5. Rollout

### The switch

**Sign-off:** a registry setting, added per `CLAUDE.md`:

- **Key** `persist.planner`, `oneOf(["v1", "shadow", "v2"])`. Under the naming rule this gives
  `PUBLISHER_PERSIST_PLANNER`, `--persist-planner`, and the file key `persist.planner`.
- **Kind.** Not a launch setting and not a secret. Read with `settings.get("persist.planner")`, at
  startup, on both the load worker and the main-thread run plan (`run/run_plan.ts`). Both must use
  the same value.
- **Registry chores.** Add `--persist-planner` to `SERVER_VALUE_FLAGS` in
  `packages/create-malloy-package/src/scaffold.ts:1598`. Run `bun run generate-settings-reference`.
  Write a `RELEASE_NOTES.md` section.
- **Fleet-uniform per environment**, like `persist.addressIncludesDestination`
  (`settings/registry/storage.ts:128`). Two workers on different planners give one package version
  two plans. The control plane would then see the inherited-only anchors appear and disappear.
  The notes say so.
- **Modes.**
  - `v1`: today.
  - `shadow`: plan, build and serve with v1, and also plan v2 and diff the two.
  - `v2`: the persist leg and the ownership rule.
- **Retiring it.** The setting is deleted with the v1 walk. The registry's way to retire a setting
  is still a follow-up (#1713); this setting is one more user of it.

### What the shadow diff compares

The comparison is per package version, between the v1 and v2 `PackagePlan`s, keyed by `sourceID`:

- the set of planned `sourceID`s and their kind;
- address, compiler key, fold, wire `sql`;
- the auto-run table name the run would choose, both with no prior entry and with v1's recorded
  entry;
- placement, freshness, refresh and the incremental declaration;
- gate classification;
- `dependsOn` among the sources both plans hold;
- `refusedSources` by reason;
- `omittedSources` and `omittedFiles`;
- the package's malformed findings by code, subject and message. `persist-not-materializable` must
  match byte for byte.

Each difference is classed:

| Class                                    | What                                                                                                                                                                  | Verdict            |
| ---------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------ |
| E1 `inherited_only_left_plan`            | A v1-only `sourceID` that is an inherited-only extension whose address equals a retained source's address                                                             | expected           |
| E2 `materialize_on_inherited_planned`    | An `inherited_persist` refusal replaced by a planned `#(materialize)` chain over the base's table                                                                     | expected           |
| E3 `shared_address_advisory_gone`        | `persist-incremental-shared-address` gone where only an inherited-only extension shared                                                                               | expected           |
| E4 `omission_code_renamed`               | `v1_build_companion_failed` reported as `persist_companion_failed`                                                                                                    | expected           |
| E5 `not_materializable_on_declarer_only` | A `persist-not-materializable` finding that v1 also raised on an inherited-only extension; the declaring source's finding is identical, and the load fails either way | expected           |
| E6 `base_unrefused`                      | A base refused in v1 only through `refuseDatabaseTablesSharedWithARefusal` against an inherited-only extension                                                        | expected, after D7 |
| everything else                          | any moved address, name or edge; a changed refusal reason; a new or missing finding; a plan that fails in one mode                                                    | **bug**            |

An E1 whose address does **not** equal a retained source's would mean the extension had a table of
its own that v2 drops. It is a bug until shown otherwise.

**Where shadow runs.**

- **On the fleets.** Shadow runs on the load worker. With the build copy as the vehicle it reuses
  the same build-copy compile, so the extra work is the ownership pass plus a materialize re-plan
  for files that hold an `inherited_persist` refusal. The diff is computed as data beside the v1
  parts.
- **On failure.** It never fails or delays the v1 plan. A shadow error is a counted outcome, and
  shadow is skipped when the plan has been left to the main thread.
- **Outputs.** It emits `publisher_persist_planner_shadow_total{outcome, class}` and one info log
  line per bug-class difference, carrying codes, `sourceID`s and the class only. It never carries
  SQL or a message (`never_log_rule.spec.ts`). Nothing about the data leaves the worker.
- **In the service.** The service's `v1-cutover-conformance` job runs Publisher's
  `packages/server/scripts/v1-cutover-conformance.ts` over the service fixtures
  (`e2e/conformance/v1-cutover/`: `persist-dag`, `persist-storage`, `preaggregate-v1-colocated`,
  `preaggregate-v1-storage`, `unflagged-persist`). The runner gains a `--plan-diff` mode that prints
  the classed rows. The Python wrapper fails on any bug class. The runner also runs its existing
  served-versus-live check with `PUBLISHER_PERSIST_PLANNER=v2`. Every `served_equal` must stay
  `served_equal`, so an inherited-only extension that is now served through its base's table is
  checked for equal answers, not only equal plans.

### Phases

The critical path is marked ★.

0. **★ Harness, no behaviour change.**
   - The `persist-not-materializable` parity spec (3.4) and the address parity spec (4.1), written
     against v1.
   - The diff classifier.
   - The runner's `--plan-diff`.
   - The per-leg containment in `collectModelBuildPlan`, so one leg's throw costs only that leg's
     file.
1. **★ The ownership rule and the persist leg, behind `persist.planner` (default `v1`).**
   - `tableOwnerOf`.
   - The persist leg.
   - `StoredParent` in `chainVerdict` and the cross-file walk, active only under `v2`.
   - The manifest-name step from decision [D4](#decisions-for-kyle), active under `v1`.
   - Unit and goldens under both settings.
2. **★ A release with both planners.** This is a patch release, because the default is unchanged.
   The dev and staging fleets run `shadow`, and the service job runs `--plan-diff` on its fixtures.
   Bug-class diffs are driven to zero. Gates before the flip:
   - **G1:** zero bug-class diffs on dev and staging for one full schedule cycle.
   - **G2:** the conformance job is green under `v2`.
   - **G3:** the service confirms two things for a retired inherited-only anchor: it drops no table
     still held at its address, and hosted naming is sticky by address.
3. **★ The flip: default `v2`.** This goes in the next breaking minor, batched with the other
   breaking changes rather than cut on its own. The release notes name every **Sign-off** item.
4. **★ Delete v1.**
   - The walk, `v1_build_companion.ts`, the backstop, `droppedPersistSources`, the five rule copies
     and the setting.
   - Wire values become retained, no longer produced: `inherited_persist` and
     `v1_build_companion_failed`.
   - `plan/README.md`'s `persist` row and the design doc's line 132 are rewritten.

### Removing `#@ preaggregate`

The removal can go **alongside** phase 1 and must land **before** phase 4. It is independent of the
persist leg except for three helpers that `rollup_plan.ts` imports from v1 modules:

- `hasFanoutJoin` (`plan/preaggregation_validation.ts:160`);
- `basePersistNamespace` (`plan/preaggregation_synthesis.ts:119`);
- `nameContainer` and `readRollupContainer` (`plan/preaggregation_namespace.ts`).

These move into `rollup_plan.ts` or `domain/` first. `basePersistNamespace` reads the base's
`#@ persist` `name=` container, so it uses `tableOwnerOf`.

**Sign-off ([D8](#decisions-for-kyle)):** in the breaking minor that carries the flip, a
`#@ preaggregate` line becomes a retired annotation. It fails publish and load, malformed, with the
code `rollup-retired-annotation` and a message naming the `#(rollup) grain=[…]` that it reads as.
This follows the retired-key precedent (`persist-retired-key`) and the rule that an invalid
annotation fails.

The readers then go. Lines are cited so the deletion can be checked:

- the `readsPreaggregateLines` path (`plan/rollup_plan.ts:653`, `:892-920`);
- `preaggregation_annotation.ts`, `preaggregation_classifier.ts`, `preaggregation_synthesis.ts` and
  `preaggregation_validation.ts`;
- `preaggregateGateLines` and `PREAGG_WARN_ONLY` (`plan/load_gate_text.ts:372-420`);
- `v1RollupRefusals` (`plan/build_plan.ts:2192`);
- the model-level validation import (`service/model.ts:145`);
- `deprecated-preaggregate-annotation` (`annotation_reader.ts:469`).

`#(preaggregate)`, the one-release alias of `#(rollup)` (`plan/rollup_plan.ts:877-884`), goes in
the same release.

**The rollup's table survives the rewrite.** The rollup leg already plans a v1 line and a
`#(rollup)` line of the same grain as one rollup (`plan/v1_rollup_plan.spec.ts:24-35`). An author
who rewrites `#@ preaggregate grain="d"` as `#(rollup) grain=["d"]` therefore keeps the rollup's
name, address and table. The removal adds a spec that pins this for every preaggregate case in the
hammer and fixture corpus.

**Fleet evidence before the removal:** the existing deprecation warning gets a counter by package,
and dev and staging run with it for one release. The service already treats `origin: preaggregate`
as "retained, no longer produced" (`api-doc.yaml:11499-11517`). Its `PreaggregateProjection` keeps
reading old rows.

## 6. Sequencing with X1

X1, a separate item not yet approved, re-roots joined stored sources onto their tables and touches
chain re-rooting. It overlaps this design in four places:

1. **The stored-source predicate.** X1 needs to know which joined sources are stored. Today that
   rule has five copies and disagrees on `.sql()` sources (§1, item 5). `tableOwnerOf` is that
   predicate. **Land it first (phase 1), and have X1 consume it.** This is the one hard dependency.
2. **Build edges.** X1 adds join edges. The persist leg keeps core's edges and adds
   `withDownstreamEdges` reach for v2 upstreams. Both should end up in one edge function in
   `plan/addressing.ts`. Whichever lands second extends it.
3. **The re-root text.** `chainVerdict` and the build copy's parent re-root
   (`materialize_build_copy.ts`, `stored_base_copy.ts`, `pair_reroot.ts`) are touched by both:
   - this design passes them a `StoredParent` for a persist parent;
   - X1 re-roots joins in the same copies.

   If X1 goes first, `StoredParent` lands with it. If this design goes first, X1 builds on it.
   Either order works, but **not in parallel on one branch**.

4. **Addresses.** X1 must follow #1998's rule: a build reads upstream tables, but its address is the
   digest of the un-substituted `BuildTarget.sql`. Otherwise X1 moves addresses. If X1 lands
   between this design's shadow baseline and the flip, the shadow baseline is re-taken. X1 changes
   build SQL, not plans, so the plan diff should not move. The served-versus-live run does need to
   run again.

The recommended order is:

1. Phase 0 here, then phase 1 here: ownership, the persist leg and `StoredParent`.
2. X1.
3. The rest of this design's phases.

X1 does not block the flip, and the flip does not block X1.

## 7. Contract and behaviour changes for sign-off

1. An inherited-only `#@ persist` source leaves `buildPlan.sources`, `graphs` and `refusedSources`.
   It gets no table of its own and is served through its base's table. Hosts retire its anchor.
2. A `#(materialize)` on an extension of a persisted query source is planned as a chain over the
   base's stored table. Today it is refused `inherited_persist`. This is the gtme fix.
3. `inherited_persist` is no longer produced, except as decision D5 keeps it.
4. New `OmittedSource.reason` value `persist_companion_failed`. `v1_build_companion_failed` is
   retained and no longer produced.
5. `persist-not-materializable` is raised on the declaring source only. An inherited-only extension
   no longer gets a second finding, and the load fails as before.
6. A base is no longer refused because an inherited-only extension's gate was refused. The
   extension's gate is enforced at read (D7).
7. New setting `persist.planner` (`PUBLISHER_PERSIST_PLANNER`, `--persist-planner`): temporary and
   fleet-uniform.
8. `#@ preaggregate` and `#(preaggregate)` become retired annotations that fail publish and load
   (`rollup-retired-annotation`, D8).
9. Only if decided: D6 (a `storage=` `#@ persist` over a `#(materialize)` table in its destination
   is built), and D10 (a persisted `.sql()` source counts as a stored upstream).

## Decisions for Kyle

**D1. Keep Malloy's `BuildTarget` as the per-source SQL and key primitive?**

- (a) Keep it. Each address stays a digest of `BuildTarget.sql`, and the persist leg asks core for
  targets over a generated, flagged file, as every v2 leg does.
- (b) Generate the persist SQL in Publisher, for example from a `-> { select: * }` compile.

_Recommend (a)._ Option (b) moves every address, so every table rebuilds across the fleet and every
incremental source re-seeds, and the gain is only a primitive v2 already shares. "Dependency on
core's build plan" then means the walk, which goes, and not the target, which stays.

**D2. The compile vehicle at the flip?**

- (a) The build copy: the author's file with the flag, as today's companion.
- (b) A per-source generated companion with source-level `#@` lines blanked, presented as the author's.

_Recommend (a) for the flip._ It is the same compile, so it cannot move an address, and core's
`buildSQL` still substitutes persisted upstreams. Option (b) is a convergence step after the flip,
gated on the same address spec plus a chained-build parity run.

**D3. The switch?**

- (a) `persist.planner` registry setting with `v1`, `shadow` and `v2`.
- (b) No runtime switch: shadow only in CI and the conformance job, then flip in code.

_Recommend (a)._ The fleets' real packages are the evidence the fixtures lack. The workflow header
says customer snapshots "need their own data-handling decision", and shadow mode sends out only
counts and `sourceID`s.

**D4. Manifest entries named after an inherited-only extension (4.1, item 5)?**

- (a) In the release with both planners, name a shared address's entry after the source that
  declares its own `#@ persist`, so manifests are rewritten before the flip.
- (b) A one-release bind rule: accept an entry whose named source is absent when a persist-leg
  source holds its address.
- (c) Accept live serving until each base is rebuilt.

_Recommend (a)._ It changes only the entry's `sourceName`, never the physical name, which the
sticky rule keeps. It also leaves the bind's other-definition guard (#1713) as strict as it is.

**D5. What remains of `inherited_persist`?**

- (a) Keep it only for a source whose own and inherited notes cannot be told apart. The source is
  refused alone and answered live.
- (b) Fail such a source closed as malformed.
- (c) Drop the value entirely.

_Recommend (a)._ An unattributable annotation is not shown to be invalid, so by Kyle's rule it is
refused, not failed. The value then means exactly "ownership could not be read".

**D6. A `storage=` `#@ persist` over a `#(materialize)` table in its destination?**

- (a) Keep it refused at the flip, for parity, and lift it after with a view-style over-stored build.
- (b) Lift it in the flip.

_Recommend (a)._ The flip stays a pure planner move, so the shadow diff stays quiet.

**D7. The gate coupling (expected diff E6)?** Today a base is refused when an inherited-only
extension sharing its address is refused at the database gate. After the flip that extension has no
table. Its queries reach the base's table through the served copy, which grafts and proves the
extension's own gate at read, or declines.

- (a) Accept, with an adversarial security review of the read path for a gated extension over an
  ungated base's table before the flip.
- (b) Keep the coupling by refusing the base's table whenever any extension of it is gated.

_Recommend (a)._ The read path already decides per answer. Option (b) would refuse tables that serve
ungated callers correctly.

**D8. When and how `#@ preaggregate` goes?**

- (a) Retired, malformed (`rollup-retired-annotation`), failing publish and load, in the breaking
  minor with the flip. The readers are deleted with the v1 walk.
- (b) Warn and serve live for one more release, then fail.
- (c) Keep `docs/publisher-v1.md:1320`'s plan: refused at 1.0.0, readers deleted in 1.1.

_Recommend (a)._ It meets Kyle's "goes" and the invalid-fails rule, and the rewrite keeps every
rollup table. If the counter finds a hosted package still using it, fall back to (b) for that
release only.

**D9. `materialize.enabled` and the persist leg?** The v1 walk runs with the switch off. The persist
leg must too, but it reuses chain machinery that the switch gates today.

- (a) Remove the switch's off position first. It is already slated for removal one release after
  its default shipped (`settings/registry/storage.ts:137-144`).
- (b) Make the persist leg ignore the switch while the materialize leg honours it.

_Recommend (a)._ There is one less mode to keep equal.

**D10. One shape predicate everywhere: `query_source` or `sql_select`?** Adopting core's predicate
in `tableOwnerOf` makes `reachedUpstreams` count a persisted `.sql()` source as a stored upstream. A
view or rollup over one then builds over its table, or is refused at plan, instead of reading raw.

- (a) Adopt it in the flip.
- (b) Adopt it after.

_Recommend (a)._ It is the strict-materialization rule (#1998) applied evenly. Include it in the
shadow classes if it moves any plan.

## Critical path, in one line

Harness (phase 0), then the ownership rule and persist leg with `StoredParent` and the manifest-name
step (1), then a both-planners release with clean shadow on dev and staging and the service's gates
G1–G3 (2), then the flip in the next breaking minor alongside the `#@ preaggregate` retirement (3),
then deleting v1 (4). X1 consumes the ownership rule from phase 1.
