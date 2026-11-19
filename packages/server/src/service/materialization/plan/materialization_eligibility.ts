// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The eligibility gates: whether a planned persist source may be built on its
 * tier, and what an eligible source carries into the build. Decided after
 * compilation and before any build, so a refusal costs no warehouse work.
 * Each gate throws a
 * `MaterializationEligibilityError` (HTTP 422) naming the source and the
 * reason, and counts nothing: a plan counts each refused source once, at its
 * verdict (`planVerdicts`).
 *
 * Surface: `assertTierEligible`, the gate for the tier a source builds into
 * (the storage gate `assertMaterializationEligible`, or the database gate
 * `assertDatabasePersistNotAuthorizeGated`), and `assertPlacementNamed`
 * beside it, read by the build plan, eligibility collection, the run's
 * instructions and the instructed build; `gateEnforcedAtRead`, the one
 * question the build, the serve bind and the host-instructed build ask of a
 * gated source; `isAuthorizeAttributedToEntryPoint`, for the build plan's
 * gate classification; and `READ_ONLY_DIALECTS`, shared with the connection
 * attributes and the dialect table.
 *
 * Fails closed throughout: IR it cannot read is a refusal, since the failure
 * it guards against is a stored table served past a gate or to the wrong
 * caller.
 */

import { sourceDefOf } from "../../governance/ir/compiled_model";
import type { PersistSource } from "@malloydata/malloy";
import { gateSwitches } from "../../../config";
import { tableKeepsGates } from "../domain/gate_policy";
import {
   gateRefusalReason,
   type GateRefusalReason,
} from "../domain/gate_refusal";
import { capabilitiesOf } from "../domain/dialect";
import { MaterializationEligibilityError } from "../../../errors";
import type { AnnotationNote } from "../../annotations";
import { parseAuthorizeAnnotation } from "../../authorize";
import {
   declaresEmptyStorage,
   deriveAnnotationFields,
   deriveColumns,
} from "./persist_source_fields";
import {
   isPersistedResultSourceID,
   type PersistSourceGateOutcome,
   type TablePlan,
} from "../domain/table_plans";
import {
   buildReadsCurrentTime,
   buildSubstitutesAGiven,
   admitGivenPositions,
   inheritedGivenWhereRefusal,
   readTimeDynamicTerms,
} from "./persist_dynamic_terms";
import { routeReadsEnv } from "../../motly";
import { EVERY_SOURCE } from "../domain/compiled_build_plan";
import { EMPTY_STORAGE_REASON } from "./declaration";
import { rollupGateEnforcedAtRead } from "./rollup_plan";
import {
   storageBuildReadsConnectionType,
   unreadableSourceConnectionMessage,
} from "../domain/source_connection_type";

/**
 * What an eligible source carries into the build. Returned by the gate rather
 * than re-derived, because it is only valid because the gate proved it: where
 * each term sits.
 */
/**
 * Compile-time HARD REFUSE gate for materializing a persist source into a
 * *storage* destination (a DuckDB/DuckLake table). Such a table is built once
 * and served frozen, losing per-query recomputation and per-tenant filtering,
 * so a source is eligible only when its relation is fixed at build time:
 *
 *  1. **No unbound parameters.** A free parameter makes the source a template
 *     with no single relation. Constant-bound parameters are fine.
 *  2. **No given in a position the build bakes in.** A frozen given is one
 *     tenant's slice served to every tenant. An extend-block `where:` term is
 *     left out of the build and re-applied per caller, so it is stripped and
 *     returned; any other position is refused (fail closed). See
 *     {@link admitGivenPositions}. The stripped terms are a promise: the
 *     served copy must re-apply them, or the query must not route.
 *  3. **No `#(authorize)` / `#(access_filter)` gate.** Both are evaluated per
 *     request and a table built once carries neither, so it would bypass
 *     them. Fails closed. The one exception is `gateEnforcedAtRead` (see
 *     {@link gateEnforcedAtRead}): a source's own row-level gate, kept by the
 *     served copy, which re-roots the author's text, gates included, and has
 *     them grafted and re-proven on every query (and own-gate keeping for a
 *     `storage=` table as for a database one). Every other check here still
 *     applies.
 *
 *     Join reach is PARTIAL: the deep walk finds a gate on a plainly-joined
 *     source and under `annotations.inherits`, but NOT through an ANNOTATED
 *     join, where Malloy replaces the target's annotations and only a
 *     `sourceID` into `ModelDef.sourceRegistry` survives (measured:
 *     `join_one: base_locked` refuses, the same join under a `# render_tag` is
 *     eligible). The serve path gates only the entry point, so this gap
 *     exposes nothing; do not treat this scan as proof joined-in gated data is
 *     safe to freeze.
 *
 * DuckDB portability of the served source is checked at build time against
 * the captured schema, not here. This must run after compilation (only the
 * compiled `ModelDef` sees inherited annotations, parameters and givens) and
 * before the build, so it fails fast before warehouse work.
 *
 * @param annotationFields the source's `#@ persist` key=value fields, which is
 *   where `merge_key=` is read from.
 * @throws {MaterializationEligibilityError} (HTTP 422) naming the source and the
 *   specific reason, so the author can either fix the source or drop `storage=`.
 */
export function assertMaterializationEligible(
   persistSource: PersistSource,
   annotationFields: Record<string, string>,
   options: {
      /** From {@link gateEnforcedAtRead}: skip only the gate refusal (3). */
      gateEnforcedAtRead?: boolean;
      /** The plan's connection types (`TierGateInputs.connectionTypes`). */
      connectionTypes?: TierGateInputs["connectionTypes"];
   } = {},
): void {
   const sourceName = persistSource.name;

   assertStorageBuildReadsConnection(persistSource, options.connectionTypes);

   // Fail closed: an unreadable parameter surface is a refusal.
   let unbound: string[];
   try {
      unbound = unboundParameterNames(persistSource);
   } catch (err) {
      throw new MaterializationEligibilityError({
         reason: "free_parameter",
         message:
            `Source '${sourceName}' cannot be materialized into a storage ` +
            `destination: its parameter surface could not be determined ` +
            `(${err instanceof Error ? err.message : String(err)}), so the ` +
            `publisher cannot prove the source has no free parameters. This is ` +
            `refused for safety. Drop 'storage=' to serve it live from the ` +
            `source warehouse.`,
      });
   }
   if (unbound.length > 0) {
      throw new MaterializationEligibilityError({
         reason: "free_parameter",
         message:
            `Source '${sourceName}' cannot be materialized into a storage ` +
            `destination: it has unbound parameter(s) ` +
            `${unbound.map((p) => `'${p}'`).join(", ")}. A source with a free ` +
            `parameter is a template instantiated per query, so there is no ` +
            `single relation to materialize. Bind the parameter to a constant, ` +
            `or drop 'storage=' to serve it live from the source warehouse.`,
      });
   }

   const classified = admitGivenPositions(persistSource);
   if (!classified.ok) {
      throw new MaterializationEligibilityError({
         reason: classified.reason,
         message:
            `Source '${sourceName}' cannot be materialized into a storage ` +
            `destination: ${classified.detail}.`,
      });
   }

   assertMergeKeyScopeResolvable(
      persistSource,
      readTimeDynamicTerms(persistSource),
      annotationFields,
      `Source '${sourceName}'`,
   );

   if (!options.gateEnforcedAtRead && referencesAuthorize(persistSource)) {
      throw new MaterializationEligibilityError({
         reason: gateReasonOf(persistSource),
         message:
            `Source '${sourceName}' cannot be materialized into a storage ` +
            `destination: it is protected by a gate — #(authorize) or ` +
            `#(access_filter), its own or a joined source's. Both are evaluated ` +
            `per request; a materialized-once table served frozen carries no ` +
            `gate, so it would be served to everyone, bypassing authorization. ` +
            `This is refused for safety. Serve this source live (drop ` +
            `'storage=').`,
      });
   }
}

/**
 * Refuse a `storage=` source whose connection the build cannot read. The build
 * passes a query through to the source's warehouse and copies the result into
 * the destination, which it can do for a passthrough warehouse and a DuckLake
 * catalog only (`storageBuildReadsConnectionType`). The package's DuckDB
 * sandbox, or an environment DuckDB connection, holds data the build's own
 * session cannot reach, so the source is not built and is answered live.
 *
 * Decided from the connection's configured type, resolved through the package
 * (the sandbox is the package's, every other connection the environment's),
 * never from its name. A connection with no recorded type, or not resolved, is
 * left to the build, which refuses that source alone
 * (storage_build_strategy.ts).
 */
function assertStorageBuildReadsConnection(
   persistSource: PersistSource,
   connectionTypes: TierGateInputs["connectionTypes"],
): void {
   const type = connectionTypes?.[persistSource.connectionName];
   if (type === undefined || storageBuildReadsConnectionType(type)) return;
   throw new MaterializationEligibilityError({
      reason: "storage_source_unsupported",
      message: unreadableSourceConnectionMessage(persistSource, type),
   });
}

/**
 * Refuse a `merge_key=` whose match cannot be scoped to the caller's rows.
 *
 * The key was chosen against the source filtered to one caller, but the
 * artifact holds every caller's rows, so an unscoped MERGE overwrites others'
 * rows. The apply folds the stripped terms' columns into the match, which only
 * works when every term names stored columns of this source; scoping by the
 * remaining terms would look scoped and not be. Refused at publish, where the
 * author can still act.
 */
function assertMergeKeyScopeResolvable(
   persistSource: PersistSource,
   terms: readonly { code: string; columns: string[] }[],
   annotationFields: Record<string, string>,
   what: string,
): void {
   if (!annotationFields.merge_key?.trim()) return;
   // Resolved against the STORED columns: an extend-block `dimension:` is
   // read-time, so a term over it names a column the table lacks and every
   // refresh's MERGE `ON` would fail.
   const stored = new Set(
      deriveColumns(persistSource)
         .map((column) => column.name)
         .filter((name): name is string => typeof name === "string"),
   );
   const unscopable = terms.filter(
      (term) =>
         term.columns.length === 0 ||
         term.columns.some((name) => !stored.has(name)),
   );
   if (unscopable.length === 0) return;
   throw new MaterializationEligibilityError({
      reason: "merge_key_scope_unresolved",
      message:
         `${what} cannot be materialized: it declares 'merge_key=' and is ` +
         `scoped by a term this pass cannot express as a predicate over the ` +
         `stored table's own columns (${unscopable
            .map((term) => `'${term.code}'`)
            .join(", ")}). An incremental refresh matches rows by the merge ` +
         `key, and the stored table holds every caller's rows, so the match ` +
         `must also carry the scoping columns or it would update rows ` +
         `belonging to other callers. Scope this source with a term over its ` +
         `own columns, or drop 'merge_key=' to refresh by watermark range.`,
   });
}

/**
 * Dialects whose connection cannot create a table, so a database CTAS has
 * nowhere to write. Must agree with `canPersist: false` in
 * `getStaticConnectionAttributes` (connection_config.ts), which clients read
 * to decide whether to offer persistence. Refused at load so the author sees a
 * package warning rather than a failed first DDL.
 *
 * A `storage=` source writes elsewhere and is not gated here. A rollup reaches
 * this gate whatever its destination, so the check reads the annotation rather
 * than the caller's tier.
 */
export const READ_ONLY_DIALECTS: ReadonlySet<string> = new Set(["sqlserver"]);

/**
 * A database `#@ persist` whose given-reading `where:` is inherited from a
 * base is refused: only the declaration carrying its `#@ persist` is
 * re-applied over its table at read, so the source answers live, under the
 * term (as on storage=, through `admitGivenPositions`).
 */
function assertNoInheritedGivenWhere(
   persistSource: PersistSource,
   sourceName: string,
): void {
   const inherited = inheritedGivenWhereRefusal(persistSource);
   if (inherited === undefined) return;
   throw new MaterializationEligibilityError({
      reason: "inherited_where_with_given",
      message:
         `Source '${sourceName}' cannot be materialized (database ` +
         `'#@ persist'): ${inherited}.`,
   });
}

/**
 * Compile-time eligibility gate for the DATABASE persist path (`#@ persist`
 * with no `storage=`, a CTAS into the source's own warehouse). Deliberately
 * narrower than {@link assertMaterializationEligible}: the storage tier's
 * free-parameter and positional-given rules are its own, and applying them
 * here would refuse many packages that build and serve correctly.
 *
 * A database artifact is not served frozen with respect to the gate: the
 * entry point's `#(authorize)` is grafted onto every query
 * (`Model.buildGraftedMaterializer` / `resolveGraftTarget`) whether it reads
 * live or the materialized table. So a `gateOutcome` of
 * `{classification: "row_level", attributed: true}` (proven to be a row filter
 * and the only gate beneath the source; see
 * {@link isAuthorizeAttributedToEntryPoint}) is admitted, losing only
 * freshness: a changed access decision applies from the next rebuild. Anything
 * else with a gate fails closed, since this pass cannot prove the artifact
 * matches what a live query enforces.
 *
 * This gate is also the ONLY authorize check on pre-aggregation: rollups are
 * synthesized as database `#@ persist` sources and no `preaggregation_*`
 * module is authorize-aware. A rollup groups across the gated column, so the
 * relaxation never applies to `origin === "rollup"` (pinned by the
 * rollup-shaped test in this module's spec). Do not narrow this gate without
 * accounting for that.
 *
 * `origin` names the annotation the author wrote: a rollup's name is
 * synthesized, so the refusal must point at `#@ preaggregate`, not
 * `#@ persist`. Callers pass `"rollup"` when
 * the source's planned table is a `rollup`.
 *
 * @throws {MaterializationEligibilityError} (HTTP 422) naming the source, the
 *   annotation to remove, and the alternative of moving the gate to a source
 *   that is not materialized.
 */
export function assertDatabasePersistNotAuthorizeGated(
   persistSource: PersistSource,
   sourceName: string = persistSource.name,
   origin: "persist" | "rollup" = "persist",
   gateOutcome?: PersistSourceGateOutcome,
   annotationFields: Record<string, string> = {},
   options: {
      /**
       * From {@link gateEnforcedAtRead}, for a `#(rollup)` rollup whose
       * base gate is grafted and proven on its variant at read: skip
       * only the gate refusal.
       */
      gateEnforcedAtRead?: boolean;
   } = {},
): void {
   if (
      !annotationFields.storage &&
      READ_ONLY_DIALECTS.has(persistSource.dialectName)
   ) {
      const what =
         origin === "rollup"
            ? `Pre-aggregation rollup '${sourceName}'`
            : `Source '${sourceName}'`;
      throw new MaterializationEligibilityError({
         reason: "connection_cannot_hold_table",
         message:
            `${what} cannot be materialized (database '#@ persist'): its ` +
            `connection '${persistSource.connectionName}' is on the ` +
            `'${persistSource.dialectName}' dialect, whose connection is ` +
            `read-only and creates no tables, so there is nowhere in the ` +
            `source warehouse to write the result. The source serves live. ` +
            `Remove the annotation, or persist a source on a connection that ` +
            `can write.`,
      });
   }

   assertDatabaseTablePlaceable(
      persistSource,
      sourceName,
      origin,
      annotationFields,
   );

   // A given INSIDE the persisted query is frozen at its default and served to
   // every caller; one in the source's `filterList` is applied per caller at
   // read and is the documented form (docs/row-level-access.md). Only the first
   // is refused. No parameter equivalent exists: the compiler rejects a
   // parameter in a persisted source's own query.
   if (buildSubstitutesAGiven(persistSource)) {
      // A rollup gets its own message, under the one reason every mechanism
      // gives a build that reads a given (`reads_given`): the
      // generic advice (move the given to the extend block) is where a
      // rollup's given already is, since the rollup READING the source bakes
      // it. And nothing else guards rollups: their
      // bindings bypass `serveBindingsWithRefinements`, so there is no filter
      // re-emission and no fail-closed shape compile. If the rollup build ever
      // stripped its base's read-time terms, it would aggregate across every
      // caller and serve that to all of them.
      if (origin === "rollup") {
         throw new MaterializationEligibilityError({
            reason: "reads_given",
            message:
               `Pre-aggregation rollup '${sourceName}' cannot be materialized: ` +
               `the source it rolls up is scoped by a given. Building the rollup ` +
               `reads that source, which applies its extend-block \`where:\` and ` +
               `substitutes the given's default — so the rollup would hold one ` +
               `caller's aggregate and serve it to everyone. Unlike a persisted ` +
               `source, a rollup has no read-time re-application to put the term ` +
               `back. Drop the '#@ preaggregate' from this measure, or roll up a ` +
               `source that is not caller-scoped.`,
         });
      }
      const what = `Source '${sourceName}'`;
      throw new MaterializationEligibilityError({
         reason: "reads_given",
         message:
            `${what} cannot be materialized (database '#@ persist'): its ` +
            `persisted query references a given. A given the persisted query ` +
            `is built with is substituted at BUILD time, so the table would ` +
            `hold the default's rows and serve them to every caller, ignoring ` +
            `the value each one supplies. Keep the given OUT of the persisted ` +
            `query and apply it when the source is read — a \`where:\` in the ` +
            `source's extend block, or a dimension, measure or join declared ` +
            `there — where it binds per caller over the materialized rows. ` +
            `Otherwise stop persisting this source.`,
      });
   }

   if (origin === "persist") {
      assertNoInheritedGivenWhere(persistSource, sourceName);
   }

   // The database artifact also omits extend-block `where:` terms, so the
   // same merge hazard applies (some warehouses, e.g. Postgres, refuse the
   // ambiguous MERGE themselves; DuckDB does not). Terms come from
   // `readTimeDynamicTerms`, not the classification, which returns none for
   // positional cases this gate admits.
   assertMergeKeyScopeResolvable(
      persistSource,
      readTimeDynamicTerms(persistSource),
      annotationFields,
      origin === "rollup"
         ? `Pre-aggregation rollup '${sourceName}'`
         : `Source '${sourceName}'`,
   );

   if (!referencesAuthorize(persistSource)) return;
   if (origin === "rollup" && options.gateEnforcedAtRead) return;

   if (origin === "persist" && gateOutcome?.inheritedRowFilter) {
      throw new MaterializationEligibilityError({
         // An inherited row filter is a row filter.
         reason: "access_filter",
         message: inheritedRowFilterRefusal(sourceName, "database"),
      });
   }
   // Own-gate keeping, the gate policy's: the entry point's own gate compiles
   // to a row filter and nothing else is reachable beneath it, so database
   // serving grafts exactly what a live query would.
   if (
      origin === "persist" &&
      tableKeepsGates(
         {
            kind: "persist",
            tier: "database",
            v1: true,
            gate: {
               rowLevel: gateOutcome?.classification === "row_level",
               attributed: gateOutcome?.attributed === true,
               // Refused above, with its own message.
               inheritedRowFilter: false,
            },
         },
         gateSwitches(),
      )
   ) {
      return;
   }

   // Named by the gates' kind, as the storage gate is, different cause: this
   // pass cannot prove the gate is the entry point's own row filter.
   const gated =
      origin === "rollup"
         ? `the source '${sourceName}' rolls up is protected by an ` +
           `#(authorize) gate (its own, a joined source's, or inherited ` +
           `from a source it derives from)`
         : `it is protected by an #(authorize) gate (its own, a joined ` +
           `source's, or inherited from a source it derives from)`;
   // For a joined or inherited gate, "move the gate to a source that is not
   // materialized" is already true; the fix there is making the gate the entry
   // point's OWN row filter (see `isAuthorizeAttributedToEntryPoint`).
   const remedy =
      origin === "rollup"
         ? `If the gate is on the rolled-up source itself, remove the ` +
           `'#@ preaggregate' annotation or move the gate to a source that ` +
           `is not pre-aggregated. If it is reached through a join or a ` +
           `derivation instead, that source is already unaggregated -- the ` +
           `fix is to stop pre-aggregating this entry point, not to move ` +
           `the gate again.`
         : `If the gate is on this source itself, drop '#@ persist' or ` +
           `move the gate to a source that is not materialized. If it is ` +
           `reached through a join or a derivation instead, that source is ` +
           `already not materialized -- express the condition as this ` +
           `entry point's own row-level gate, or stop persisting it.`;
   const what =
      origin === "rollup"
         ? `Pre-aggregation rollup '${sourceName}' cannot be built`
         : `Source '${sourceName}' cannot be materialized (database ` +
           `'#@ persist')`;
   // Only true of a rollup: it GROUPS, so the gated column is not even
   // present to filter on afterwards.
   const alsoRollup =
      origin === "rollup"
         ? ` A rollup also groups ACROSS the gated column, so it could not ` +
           `be row-filtered afterwards even in principle.`
         : "";
   // Generic message: this function cannot see WHY `gateOutcome` did not
   // clear the bar (missing, rejected, or a join-only gate).
   throw new MaterializationEligibilityError({
      reason: gateReasonOf(persistSource),
      message:
         `${what}: ${gated}. An authorize expression is evaluated per ` +
         `request; without a proven row-level, fully-attributed ` +
         `classification this pass cannot show the served artifact matches ` +
         `what a live query would enforce.` +
         `${alsoRollup} This is refused for safety. ${remedy}`,
   });
}

/**
 * A BigQuery table needs a dataset, and only a dotted `name=` gives a
 * database one its container (a rollup's own, or its base's): without one the
 * build fails on BigQuery's own error, so it is refused here, before any
 * warehouse work, as a `#(materialize)` source is at its plan
 * (`needs_qualified_name`), on every mechanism.
 */
function assertDatabaseTablePlaceable(
   persistSource: PersistSource,
   sourceName: string,
   origin: "persist" | "rollup",
   annotationFields: Record<string, string>,
): void {
   if (
      annotationFields.storage ||
      !capabilitiesOf(persistSource.dialectName).needsQualifiedTable ||
      annotationFields.name?.includes(".")
   ) {
      return;
   }
   throw new MaterializationEligibilityError({
      reason: "needs_qualified_name",
      message:
         origin === "rollup"
            ? `Pre-aggregation rollup '${sourceName}' cannot be built in ` +
              `its base's warehouse: its connection ` +
              `'${persistSource.connectionName}' is BigQuery, where a ` +
              `table needs a dataset, and neither the rollup nor its base ` +
              `names one. The measure serves live. Give the rollup a ` +
              `name= whose container is a dataset ` +
              `(name="analytics.orders_rollup"), or build it into a ` +
              `storage destination with storage=.`
            : `Source '${sourceName}' cannot be materialized (database): ` +
              `its connection '${persistSource.connectionName}' is ` +
              `BigQuery, where a table needs a dataset, and its name= ` +
              `names none. The source serves live. Give it a ` +
              `dataset-qualified name= (\`dataset.table\`), or build it ` +
              `into a storage destination with storage=.`,
   });
}

/**
 * Names of the source's declared parameters with no bound value (`value` null).
 * Fail-closed: a parameter is unbound unless demonstrably bound, and an
 * unexpected shape throws (the caller refuses). A missing `parameters` field is
 * normal, so a renamed field is the one drift this misses; the real-compiler
 * eligibility spec pins that shape.
 */
function unboundParameterNames(persistSource: PersistSource): string[] {
   const def = sourceDefOf(persistSource) as unknown;
   if (def === null || typeof def !== "object") {
      throw new Error("compiled source definition is not readable");
   }
   const parameters = (def as { parameters?: unknown }).parameters;
   if (parameters === undefined || parameters === null) return [];
   if (typeof parameters !== "object" || Array.isArray(parameters)) {
      throw new Error(
         `compiled source 'parameters' has an unexpected shape (${
            Array.isArray(parameters) ? "array" : typeof parameters
         })`,
      );
   }
   const unbound: string[] = [];
   for (const [name, param] of Object.entries(
      parameters as Record<string, unknown>,
   )) {
      const bound =
         param !== null &&
         typeof param === "object" &&
         (param as { value?: unknown }).value !== null &&
         (param as { value?: unknown }).value !== undefined;
      if (!bound) unbound.push(name);
   }
   return unbound;
}

/** Max IR depth to walk; deep enough for real sources, a hang backstop. */
const MAX_AUTHORIZE_WALK_DEPTH = 200;

/**
 * Whether the compiled source carries an `#(authorize)` gate anywhere beneath
 * it, including through plain joins and `annotations.inherits` (not annotated
 * joins; see {@link assertMaterializationEligible}). Any failure counts as a
 * gate (fail closed).
 *
 * Deliberately separate from `Model.collectEntryPointGates`, which collects the
 * gates to EVALUATE per request along identity edges only. This asks whether
 * ANY gate exists beneath the source, because the output is a frozen table.
 * They are different rules and should not be merged.
 */
function referencesAuthorize(persistSource: PersistSource): boolean {
   try {
      return walkForAuthorize(sourceDefOf(persistSource), new WeakSet(), 0);
   } catch {
      return true;
   }
}

/**
 * {@link referencesAuthorize} over a slice of compiled IR: a source or query
 * definition of a model's contents. Fails closed: a walk that cannot finish
 * reads as gated.
 */
export function irCarriesAGate(def: unknown): boolean {
   try {
      return walkForAuthorize(def, new WeakSet(), 0);
   } catch {
      return true;
   }
}

/**
 * True if an annotation note is an authorize gate, via
 * {@link parseAuthorizeAnnotation}. Must share the enforcement path's parser: a
 * spelling enforcement gates on but this misses (e.g. the block form
 * `#|(authorize)` under a prefix regex) would be frozen and served to everyone.
 */
function isAuthorizeAnnotation(text: string): boolean {
   try {
      return parseAuthorizeAnnotation(text) !== null;
   } catch {
      // A malformed authorize annotation still means the author intended a gate.
      return true;
   }
}

/** A note entry's text: entries are strings or `{ text }`; anything else has none. */
function noteText(note: unknown): string | undefined {
   if (typeof note === "string") return note;
   if (note === null || typeof note !== "object") return undefined;
   const text = (note as { text?: unknown }).text;
   return typeof text === "string" ? text : undefined;
}

function walkForAuthorize(
   node: unknown,
   seen: WeakSet<object>,
   depth: number,
): boolean {
   if (depth > MAX_AUTHORIZE_WALK_DEPTH) {
      throw new Error("authorize-usage walk exceeded max depth");
   }
   if (node === null || typeof node !== "object") return false;
   if (seen.has(node as object)) return false;
   seen.add(node as object);

   if (Array.isArray(node)) {
      for (const item of node) {
         if (walkForAuthorize(item, seen, depth + 1)) return true;
      }
      return false;
   }

   const record = node as Record<string, unknown>;

   // `blockNotes` and `notes` are both per-item slots, keyed by the author's
   // syntax rather than scope, so both are read. Entries are strings or `{ text }`.
   for (const key of ["blockNotes", "notes"]) {
      const arr = record[key];
      if (!Array.isArray(arr)) continue;
      for (const n of arr) {
         const text = noteText(n);
         if (text !== undefined && isAuthorizeAnnotation(text)) return true;
      }
   }

   for (const value of Object.values(record)) {
      if (walkForAuthorize(value, seen, depth + 1)) return true;
   }
   return false;
}

/**
 * The refusal reason for a source refused for the gates beneath it (crossing
 * joins and inherits, as {@link referencesAuthorize} reads them), named by
 * their kind ({@link gateRefusalReason}). A walk that fails reads as a lock.
 */
function gateReasonOf(persistSource: PersistSource): GateRefusalReason {
   const routes = new Set<string>();
   try {
      walkForGateRoutes(sourceDefOf(persistSource), new WeakSet(), 0, routes);
   } catch {
      routes.add("authorize");
   }
   return gateRefusalReason(routes);
}

/** {@link walkForAuthorize}, collecting each gate note's route. */
function walkForGateRoutes(
   node: unknown,
   seen: WeakSet<object>,
   depth: number,
   routes: Set<string>,
): void {
   if (depth > MAX_AUTHORIZE_WALK_DEPTH) {
      throw new Error("authorize-usage walk exceeded max depth");
   }
   if (node === null || typeof node !== "object") return;
   if (seen.has(node as object)) return;
   seen.add(node as object);
   if (Array.isArray(node)) {
      for (const item of node) walkForGateRoutes(item, seen, depth + 1, routes);
      return;
   }
   const record = node as Record<string, unknown>;
   for (const key of ["blockNotes", "notes"]) {
      const arr = record[key];
      if (!Array.isArray(arr)) continue;
      for (const n of arr) {
         const text = noteText(n);
         if (text === undefined) continue;
         try {
            const parsed = parseAuthorizeAnnotation(text);
            if (parsed) routes.add(parsed.route);
         } catch {
            // A malformed gate still means a gate: fail closed to a lock.
            routes.add("authorize");
         }
      }
   }
   for (const value of Object.values(record)) {
      walkForGateRoutes(value, seen, depth + 1, routes);
   }
}

/**
 * Whether every `#(authorize)` note beneath the compiled source (crossing
 * joins) is also reachable WITHOUT crossing a join. `false` means some gate is
 * reachable only through a join, which a row-level classification of the entry
 * point's gate says nothing about, so relaxing the database refusal requires
 * `true` as well as `row_level`.
 *
 * The no-joins walk is believed to cover what `collectEntryPointGates`
 * (`./gate_classification`) reaches along identity edges, but the two are not
 * proven equivalent. Notes are compared by object identity, not text (two
 * gates can share text). Fails closed: any failure is unattributed.
 */
export function isAuthorizeAttributedToEntryPoint(persistSource: {
   _sourceDef: unknown;
}): boolean {
   try {
      const deep = new Set<AnnotationNote>();
      walkForAuthorizeNotes(
         sourceDefOf(persistSource),
         new WeakSet(),
         0,
         deep,
         true,
      );
      const noJoins = new Set<AnnotationNote>();
      walkForAuthorizeNotes(
         sourceDefOf(persistSource),
         new WeakSet(),
         0,
         noJoins,
         false,
      );
      for (const note of deep) {
         if (!noJoins.has(note)) return false;
      }
      return true;
   } catch {
      return false;
   }
}

/**
 * Deep walk collecting authorize NOTE objects (by identity), unlike
 * {@link walkForAuthorize}'s early-returning boolean. `crossJoins=false` stops
 * at a join field, detected like Malloy's `isJoined` (`'join' in sd`);
 * `crossJoins=true` matches `walkForAuthorize`'s reach.
 */
function walkForAuthorizeNotes(
   node: unknown,
   seen: WeakSet<object>,
   depth: number,
   found: Set<AnnotationNote>,
   crossJoins: boolean,
): void {
   if (depth > MAX_AUTHORIZE_WALK_DEPTH) {
      throw new Error("authorize-usage walk exceeded max depth");
   }
   if (node === null || typeof node !== "object") return;
   if (seen.has(node as object)) return;
   seen.add(node as object);

   if (Array.isArray(node)) {
      for (const item of node) {
         walkForAuthorizeNotes(item, seen, depth + 1, found, crossJoins);
      }
      return;
   }

   const record = node as Record<string, unknown>;
   if (!crossJoins && "join" in record) return;

   for (const key of ["blockNotes", "notes"]) {
      const arr = record[key];
      if (!Array.isArray(arr)) continue;
      for (const n of arr) {
         const text = noteText(n);
         // Strings are excluded: Set membership is by value for a string, so
         // two gates sharing text could make a join-only gate read attributed.
         if (
            text !== undefined &&
            isAuthorizeAnnotation(text) &&
            typeof n === "object"
         ) {
            found.add(n as AnnotationNote);
         }
      }
   }

   for (const value of Object.values(record)) {
      walkForAuthorizeNotes(value, seen, depth + 1, found, crossJoins);
   }
}

/**
 * Whether a gated source may be built because its gate is enforced when the
 * table is read (contracts at read; own-gate keeping). True only
 * when all hold:
 *
 * - the source is a `#(materialize)` source (a `materialize` table) with
 *   `materialize.enabled` on, database or `storage=`, or a v1 `#@ persist`
 *   source asked about as `storage=` (a database one is decided by
 *   {@link assertDatabasePersistNotAuthorizeGated}); never a persisted
 *   view's or query's build, whose leg passes a gate only with
 *   `materialize.gatedSources`. Its table is read past a gate only through
 *   the served copy, which keeps the author's gates; the router keeps every
 *   other stored shape away from a gated entry point;
 * - its gate is the entry point's own and classifies `row_level` against the
 *   author's model (the table's `gate`), an attributed row filter or lock
 *   with no row filter inherited from a source it extends: the bar the
 *   database tier sets. Under own-gate keeping this holds on both tiers with
 *   `materialize.gatedSources` off, by maintainer decision ahead of the
 *   question-14 review; the managed tier then holds the gated source's
 *   columns, every caller's rows.
 *
 * A rollup is asked {@link rollupGateEnforcedAtRead} instead (rows 1 and 21).
 *
 * The build, the serve bind and the host-instructed build all ask this one
 * question, so a table is never built that cannot be served, nor served
 * because it was built. A caller deciding a `storage=` table passes
 * `storage`, so the table is asked about as stored whatever the plan says:
 * it decides whether a v1 source is asked at all.
 */
export function gateEnforcedAtRead(
   sourceID: string,
   plan: Pick<TierGateInputs, "tables"> | undefined,
   storage = false,
): boolean {
   const table = plan?.tables?.[sourceID];
   // A rollup asks its own question: its base gates are grafted and proven on
   // its variant (for rollups), for a `#(rollup)` rollup and a v1
   // `#@ preaggregate` line read as one alike.
   if (table?.rollup !== undefined) {
      return rollupGateEnforcedAtRead(table.rollup);
   }
   // A `#(materialize)` source's gate is admitted only with `#(materialize)`'s
   // own switch on, and only for the entry point's own row-level gate (the
   // gate policy).
   const materialized = table?.materialize;
   const outcome = table?.gate;
   const gate = {
      rowLevel: outcome?.classification === "row_level",
      attributed: outcome?.attributed === true,
      // The served copy cannot graft a row filter the entry point inherits.
      inheritedRowFilter: outcome?.inheritedRowFilter === true,
   };
   if (!materialized) {
      // A v1 `#@ persist` source, asked only as `storage=`. A persisted
      // view's or query's build (its companion file) is not one.
      if (!storage || isPersistedResultSourceID(sourceID)) return false;
      return tableKeepsGates(
         { kind: "persist", tier: "storage", v1: true, gate },
         gateSwitches(),
      );
   }
   return tableKeepsGates(
      {
         kind: "materialize",
         // Database only when neither the caller nor the plan says storage:
         // Own-gate keeping admits a database table alone, so a doubt reads as
         // storage.
         tier:
            !storage && materialized.persist?.storage === undefined
               ? "database"
               : "storage",
         v1: false,
         gate,
      },
      gateSwitches(),
   );
}

/**
 * Whether a source's `storage=` table is read past its own row-level gate
 * under own-gate keeping: it is gated, and {@link gateEnforcedAtRead} admits it
 * as stored. Its bindings carry `ownGateKept`, so a served answer reading it
 * may pass that gate as one reading a database table may.
 */
export function storageKeepsOwnGate(
   source: PersistSource,
   plan: Parameters<typeof gateEnforcedAtRead>[1],
): boolean {
   return (
      referencesAuthorize(source) &&
      gateEnforcedAtRead(source.sourceID, plan, true)
   );
}

/**
 * Whether a source's table holds only ungated rows: no `#(authorize)` or
 * `#(access_filter)` gate anywhere beneath its compiled definition, its own,
 * one it inherits or derives from, or one on a source it joins
 * ({@link referencesAuthorize}'s walk, the one that refuses a gated build).
 * A walk that fails reads as gated (fail closed). A gated entry point over
 * such a table is served from it on every tier: its gate is grafted and
 * proven on the answer, as on a database table (the ungated-rows rule).
 */
export function holdsOnlyUngatedRows(source: PersistSource): boolean {
   return !referencesAuthorize(source);
}

/**
 * The refusal of a source whose row-level gate it inherits (see
 * PersistSourceGateOutcome.inheritedRowFilter): built, the table would hold
 * every caller's rows and answer none of them.
 */
function inheritedRowFilterRefusal(
   sourceName: string,
   tier: "database" | "storage",
): string {
   return (
      `Source '${sourceName}' cannot be materialized (${tier}): it reads an ` +
      `#(access_filter) row-level gate from a source it derives from or ` +
      `extends, not one it declares itself. A table of it would hold every ` +
      `caller's rows, and the served copy, which reads the table in place of ` +
      `the source that declares the gate, cannot apply that gate to it, so ` +
      `every query would be answered live anyway. It is not built: no ` +
      `answer changes. To serve it from a table, declare the row-level gate ` +
      `on this source itself.`
   );
}

/** What {@link assertTierEligible} reads from a compiled plan. */
export interface TierGateInputs {
   /** The planned tables, by sourceID: each one's kind, plans and gate. */
   tables?: Readonly<
      Record<
         string,
         Pick<TablePlan, "kind" | "materialize" | "rollup" | "gate"> & {
            /** The planned source, which the access-input rule reads. */
            source?: PersistSource;
         }
      >
   >;
   /**
    * Each connection's configured type, by name, resolved through the
    * package (`CompiledBuildPlan.connectionTypes`): whether a `storage=`
    * build can read a source.
    */
   connectionTypes?: Readonly<Record<string, string>>;
   /**
    * Every sourceID an access decision of the package reads
    * (`CompiledBuildPlan.accessReads`); unknown refuses every relation that
    * reads the current time.
    */
   accessReads?: readonly string[];
}

/**
 * Refuse a `#@ persist` source whose `storage=` names no destination
 * (declaration.ts): a destination left blank names no place to build, so it is
 * refused, never built in the source's own warehouse instead.
 *
 * Not part of {@link assertTierEligible}, on purpose. That gate is also read
 * for every OTHER source on a database table's address (the plan's, the
 * build's and the bind's sibling rule), so that a gated source's rows are
 * never served past its gate through a table it shares. A blank `storage=` is
 * one source's own mistake, not a gate: it refuses that source alone, and a
 * sibling with the same SQL still builds and serves. Called beside
 * assertTierEligible wherever a source is gated for its own build.
 *
 * @throws {MaterializationEligibilityError} when the source is refused.
 */
export function assertPlacementNamed(source: PersistSource): void {
   if (!declaresEmptyStorage(source)) return;
   throw new MaterializationEligibilityError({
      reason: "empty_storage",
      message: `'${source.name}' cannot be materialized: ${EMPTY_STORAGE_REASON}.`,
   });
}

/**
 * The eligibility gate for the tier a source builds into: the storage-destination
 * gate ({@link assertMaterializationEligible}) when it is written to a storage
 * destination, else the database authorize gate
 * ({@link assertDatabasePersistNotAuthorizeGated}), naming `#@ preaggregate`
 * for a rollup. Running the storage gate on a database source would misreport
 * a buildable one as refused.
 *
 * @throws {MaterializationEligibilityError} when the source is refused.
 */
export function assertTierEligible(
   source: PersistSource,
   storage: boolean,
   plan: TierGateInputs | undefined,
   annotationFields: Record<string, string> = deriveAnnotationFields(source),
): void {
   // Its `#@` route was dropped whole (parseRouteTag), so Publisher reads no
   // `storage=` or `name=` for it while Malloy still plans it. Building would
   // write a table into the source warehouse its author never named, so it is
   // refused and serves live.
   if (routeReadsEnv(source.annotations, "@")) {
      throw new MaterializationEligibilityError({
         reason: "env_reference",
         message:
            `'${source.name}' cannot be materialized: one of its '#@' lines ` +
            `reads the server's environment (@env.), so Publisher ignores all ` +
            `of its '#@' directives (name=, storage=, queryMetadata) and serves ` +
            `it live rather than build a table where its author did not say. ` +
            `Move the value out of the model.`,
      });
   }
   if (storage) {
      // Admitted at read but for a row filter it inherits: say so, rather
      // than the generic gate refusal.
      const table = plan?.tables?.[source.sourceID];
      const outcome = table?.gate;
      if (
         table &&
         outcome?.inheritedRowFilter &&
         gateEnforcedAtRead(
            source.sourceID,
            {
               tables: {
                  ...plan?.tables,
                  [source.sourceID]: {
                     ...table,
                     gate: { ...outcome, inheritedRowFilter: false },
                  },
               },
            },
            true,
         )
      ) {
         throw new MaterializationEligibilityError({
            // An inherited row filter is a row filter.
            reason: "access_filter",
            message: inheritedRowFilterRefusal(source.name, "storage"),
         });
      }
      assertMaterializationEligible(source, annotationFields, {
         gateEnforcedAtRead: gateEnforcedAtRead(source.sourceID, plan, true),
         connectionTypes: plan?.connectionTypes,
      });
      assertBuildReadsNoCurrentTime(source, plan);
      return;
   }
   const table = plan?.tables?.[source.sourceID];
   const rollup = table?.rollup;
   assertDatabasePersistNotAuthorizeGated(
      source,
      source.name,
      rollup ? "rollup" : "persist",
      table?.gate,
      annotationFields,
      { gateEnforcedAtRead: !!rollup && rollupGateEnforcedAtRead(rollup) },
   );
   // A database `#(materialize)` table is read only through the served copy
   // (never Malloy's compiler manifest), which keeps the entry point's own
   // row-level gate under own-gate keeping as v1 database does: the gate policy
   // admits it with `materialize.enabled`, and the gated-sources switch is not
   // needed. Every other gate is refused above (a joined, inherited or not
   // row-level one), so this refuses a gated one only when the policy does not
   // keep it, and a table is never built that cannot be served.
   if (
      table?.materialize &&
      referencesAuthorize(source) &&
      !gateEnforcedAtRead(source.sourceID, plan)
   ) {
      throw new MaterializationEligibilityError({
         reason: gateReasonOf(source),
         message:
            `Source '${source.name}' cannot be materialized (database ` +
            `'#(materialize)'): it is protected by an #(authorize) or ` +
            `#(access_filter) gate, and its table is read only through the ` +
            `served copy, which keeps only the entry point's own row-level ` +
            `gate, and only with PUBLISHER_MATERIALIZE on. The source serves ` +
            `live. Turn that setting on, or use '#@ persist', whose database ` +
            `table the served copy reads past the source's own row-level ` +
            `gate under the same rule with that setting off.`,
      });
   }
   // A persisted view's or query's table over a gated source is read past
   // the gate only by its own leg, which passes a gate only with
   // PUBLISHER_MATERIALIZE_GATED_SOURCES on (the view leg; the query leg
   // never). Off, the table would be built and never read past the gate, so
   // it is refused, on this tier as on `storage=`; on, it is built as before.
   if (
      isPersistedResultSourceID(source.sourceID) &&
      !gateSwitches().gatedSources &&
      referencesAuthorize(source)
   ) {
      throw new MaterializationEligibilityError({
         reason: gateReasonOf(source),
         message:
            `'${source.name}' cannot be materialized (database): the ` +
            `source it reads is protected by an #(authorize) or ` +
            `#(access_filter) gate, and a persisted view's or query's table ` +
            `is read past a gate only with ` +
            `PUBLISHER_MATERIALIZE_GATED_SOURCES on, so it would be built and ` +
            `never read. It is not built, and its queries are answered live, ` +
            `as on 'storage='.`,
      });
   }
   assertBuildReadsNoCurrentTime(source, plan);
}

/**
 * Refuse a v1 `#@ persist` source, persisted view or persisted query whose
 * build reads the current time ({@link buildReadsCurrentTime}: `now`, or a
 * relative time filter string) where a table as of its build is unsafe:
 *
 * - it is gated (`#(authorize)` or `#(access_filter)` beneath it): the rows a
 *   caller may see would be decided by a clock frozen at the build;
 * - it is an access input ({@link currentTimeAccessInput}): the rows it
 *   holds decide what a caller may see, so a frozen clock is a stale grant;
 * - it refreshes incrementally: a delta reads only rows past its watermark,
 *   so a window over the clock would never drop the rows it has passed.
 *
 * Every other such source builds, and is served as of its build and rebuilt
 * on every run (see {@link storedAsOfBuild}). A rollup (rollup_plan.ts) and
 * a `#(materialize)` source (materialize_plan.ts) are judged by their own
 * planners.
 */
export function assertBuildReadsNoCurrentTime(
   source: PersistSource,
   plan: TierGateInputs | undefined,
): void {
   const table = plan?.tables?.[source.sourceID];
   if (table?.rollup || table?.materialize) return;
   if (!buildReadsCurrentTime(source)) return;
   const why = currentTimeRefusalCause(source, plan);
   if (why === undefined) return;
   throw new MaterializationEligibilityError({
      reason: "current_time",
      message:
         `Source '${source.name}' cannot be materialized: the relation it ` +
         `persists reads the current time (\`now\`, or a relative time ` +
         `filter such as \`f'last 7 days'\`), so its table would hold the ` +
         `rows as of its build, and ${why}. It is not built and is answered ` +
         `live. To serve it from a table, move the \`now\` or the filter into ` +
         `the persisted source's own extend block (a \`where:\`, dimension or ` +
         `measure there is applied at every read).`,
   });
}

/**
 * Why a source whose build reads the current time must not be stored as of
 * its build, or undefined when it may be ({@link assertBuildReadsNoCurrentTime}).
 */
function currentTimeRefusalCause(
   source: PersistSource,
   plan: TierGateInputs | undefined,
): string | undefined {
   if (referencesAuthorize(source)) {
      return (
         "it is protected by an #(authorize) or #(access_filter) gate, whose " +
         "decision a table as of its build would freeze"
      );
   }
   if (deriveAnnotationFields(source).refresh === "incremental") {
      return (
         "it refreshes incrementally, and a delta never re-reads the rows " +
         "its window has passed"
      );
   }
   if (currentTimeAccessInput(source, plan)) {
      return (
         "its rows feed an access decision (it is scoped by a given, joins " +
         "a source scoped by one, or a source that carries a gate or reads " +
         "a given extends, derives from, joins or composes it), so a table " +
         "as of its build would keep a grant past its time"
      );
   }
   return undefined;
}

/**
 * Whether a source's rows feed an access decision: its own read-time terms
 * or joins read a given (a caller-scoped source, or one that joins a grant
 * table, {@link admitGivenPositions}), or a definition of the package that
 * carries a gate or reads a given depends on it, at any depth: extends it,
 * derives from it with `->`, joins it, composes it, or builds a stored
 * relation over it (`accessReads`). Fails closed: an unknown closure, or a
 * classification that cannot be read, counts.
 */
function currentTimeAccessInput(
   source: PersistSource,
   plan: TierGateInputs | undefined,
): boolean {
   const admitted = admitGivenPositions(source);
   if (!admitted.ok) return true;
   if (admitted.terms.length > 0 || (admitted.joinedTerms ?? []).length > 0) {
      return true;
   }
   const reads = plan?.accessReads;
   if (reads === undefined) return true;
   return reads.includes(source.sourceID) || reads.includes(EVERY_SOURCE);
}
