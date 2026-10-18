// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { PersistSource } from "@malloydata/malloy";
import { MaterializationEligibilityError } from "../errors";
import { recordEligibilityRefused } from "../materialization_metrics";
import type { AnnotationNote } from "./annotations";
import { parseAuthorizeAnnotation } from "./authorize";
import { deriveColumns, type PersistSourceGateOutcome } from "./build_plan";
import {
   buildSubstitutesAGiven,
   classifyDynamicTerms,
   readTimeDynamicTerms,
   type DynamicTerm,
} from "./persist_dynamic_terms";
import { resolvePartitionColumns } from "./persist_partition";

/**
 * What an eligible source carries into the build and the serve shape. Returned
 * by the gate rather than re-derived, because each is only valid because the
 * gate proved it: where a term sits, and that a partition column resolves.
 */
interface PersistDynamicPlan {
   /** Terms absent from the build, re-applied per caller at read. */
   terms: DynamicTerm[];
   /** Declared `partition=` columns, resolved and ordered. Empty for none. */
   partitionColumns: string[];
}

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
 *     {@link classifyDynamicTerms}. The stripped terms are a promise: the
 *     serve shape must declare their givens and re-emit their text, or the
 *     query must not route.
 *  3. **No `#(authorize)` / `#(access_filter)` gate.** Both are evaluated per
 *     request and the served shape carries neither, so the table would bypass
 *     them. Fails closed.
 *
 *     Join reach is PARTIAL: the deep walk finds a gate on a plainly-joined
 *     source and under `annotations.inherits`, but NOT through an ANNOTATED
 *     join, where Malloy replaces the target's annotations and only a
 *     `sourceID` into `ModelDef.sourceRegistry` survives (measured:
 *     `join_one: base_locked` refuses, the same join under a `# render_tag` is
 *     eligible). No exposure today, since the serve path gates only the entry
 *     point; do not treat this scan as proof joined-in gated data is safe to
 *     freeze.
 *
 * DuckDB portability of the served source is checked at build time against
 * the captured schema, not here. This must run after compilation (only the
 * compiled `ModelDef` sees inherited annotations, parameters and givens) and
 * before the build, so it fails fast before warehouse work.
 *
 * @param annotationFields the source's `#@ persist` key=value fields, which is
 *   where `partition=` is read from.
 * @returns the terms the build strips and the columns it lays the table out by.
 * @throws {MaterializationEligibilityError} (HTTP 422) naming the source and the
 *   specific reason, so the author can either fix the source or drop `storage=`.
 */
export function assertMaterializationEligible(
   persistSource: PersistSource,
   annotationFields: Record<string, string>,
): PersistDynamicPlan {
   const sourceName = persistSource.name;

   // Fail closed: an unreadable parameter surface is a refusal.
   let unbound: string[];
   try {
      unbound = unboundParameterNames(persistSource);
   } catch (err) {
      recordEligibilityRefused("free_parameter");
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
      recordEligibilityRefused("free_parameter");
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

   const classified = classifyDynamicTerms(persistSource);
   if (!classified.ok) {
      recordEligibilityRefused(classified.reason);
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

   if (referencesAuthorize(persistSource)) {
      recordEligibilityRefused("authorize");
      throw new MaterializationEligibilityError({
         reason: "authorize",
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

   // Last: the only check not about isolation, so the refusals above are
   // reported ahead of a mistyped column.
   const partition = resolvePartitionColumns(persistSource, annotationFields);
   if (!partition.ok) {
      recordEligibilityRefused(partition.reason);
      throw new MaterializationEligibilityError({
         reason: partition.reason,
         message:
            `Source '${sourceName}' cannot be materialized into a storage ` +
            `destination: ${partition.detail}.`,
      });
   }

   return { terms: classified.terms, partitionColumns: partition.columns };
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
   recordEligibilityRefused("merge_key_scope_unresolved");
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
 * Dialects whose connection cannot create a table, so a colocated CTAS has
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
 * Compile-time eligibility gate for the COLOCATED persist path (`#@ persist`
 * with no `storage=`, a CTAS into the source's own warehouse). Deliberately
 * narrower than {@link assertMaterializationEligible}: the storage tier's
 * free-parameter and positional-given rules exist because its serve shape must
 * re-declare everything, and applying them here would refuse many packages
 * that build and serve correctly.
 *
 * A colocated artifact is not served frozen with respect to the gate: the
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
 * synthesized as colocated `#@ persist` sources and no `preaggregation_*`
 * module is authorize-aware. A rollup groups across the gated column, so the
 * relaxation never applies to `origin === "preaggregate"` (pinned by the
 * rollup-shaped test in this module's spec). Do not narrow this gate without
 * accounting for that.
 *
 * `origin` names the annotation the author wrote: a rollup's name is
 * synthesized, so the refusal must point at `#@ preaggregate`, not
 * `#@ persist`. Callers pass `"preaggregate"` when
 * `CompiledBuildPlan.preaggregatePlans` has an entry for the source.
 *
 * @throws {MaterializationEligibilityError} (HTTP 422) naming the source, the
 *   annotation to remove, and the alternative of moving the gate to a source
 *   that is not materialized.
 */
export function assertColocatedPersistNotAuthorizeGated(
   persistSource: PersistSource,
   sourceName: string = persistSource.name,
   origin: "persist" | "preaggregate" = "persist",
   gateOutcome?: PersistSourceGateOutcome,
   annotationFields: Record<string, string> = {},
): void {
   if (
      !annotationFields.storage &&
      READ_ONLY_DIALECTS.has(persistSource.dialectName)
   ) {
      recordEligibilityRefused("read_only_dialect");
      const what =
         origin === "preaggregate"
            ? `Pre-aggregation rollup '${sourceName}'`
            : `Source '${sourceName}'`;
      throw new MaterializationEligibilityError({
         reason: "read_only_dialect",
         message:
            `${what} cannot be materialized (colocated '#@ persist'): its ` +
            `connection '${persistSource.connectionName}' is on the ` +
            `'${persistSource.dialectName}' dialect, whose connection is ` +
            `read-only and creates no tables, so there is nowhere in the ` +
            `source warehouse to write the result. The source serves live. ` +
            `Remove the annotation, or persist a source on a connection that ` +
            `can write.`,
      });
   }

   // `partition=` is meaningless without `storage=`. Only this gate sees a
   // colocated source declaring it, which would otherwise reach a build that
   // silently ignores it.
   const partition = resolvePartitionColumns(persistSource, annotationFields);
   if (!partition.ok) {
      recordEligibilityRefused(partition.reason);
      const what =
         origin === "preaggregate"
            ? `Pre-aggregation rollup '${sourceName}'`
            : `Source '${sourceName}'`;
      throw new MaterializationEligibilityError({
         reason: partition.reason,
         message:
            `${what} cannot be materialized (colocated '#@ persist'): ` +
            `${partition.detail}.`,
      });
   }

   // A given INSIDE the persisted query is frozen at its default and served to
   // every caller; one in the source's `filterList` is applied per caller at
   // read and is the documented form (docs/row-level-access.md). Only the first
   // is refused. No parameter equivalent exists: the compiler rejects a
   // parameter in a persisted source's own query.
   if (buildSubstitutesAGiven(persistSource)) {
      // A rollup gets its own reason: the generic advice (move the given to the
      // extend block) is where a rollup's given already is, since the rollup
      // READING the source bakes it. And nothing else guards rollups: their
      // bindings bypass `serveBindingsWithRefinements`, so there is no filter
      // re-emission and no fail-closed shape compile. If the rollup build ever
      // stripped its base's read-time terms, it would aggregate across every
      // caller and serve that to all of them.
      if (origin === "preaggregate") {
         recordEligibilityRefused("preaggregate_over_dynamic_source");
         throw new MaterializationEligibilityError({
            reason: "preaggregate_over_dynamic_source",
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
      recordEligibilityRefused("given_in_persisted_query");
      const what = `Source '${sourceName}'`;
      throw new MaterializationEligibilityError({
         reason: "given_in_persisted_query",
         message:
            `${what} cannot be materialized (colocated '#@ persist'): its ` +
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

   // The colocated artifact also omits extend-block `where:` terms, so the
   // same merge hazard applies (some warehouses, e.g. Postgres, refuse the
   // ambiguous MERGE themselves; DuckDB does not). Terms come from
   // `readTimeDynamicTerms`, not the classification, which returns none for
   // positional cases this gate admits.
   assertMergeKeyScopeResolvable(
      persistSource,
      readTimeDynamicTerms(persistSource),
      annotationFields,
      origin === "preaggregate"
         ? `Pre-aggregation rollup '${sourceName}'`
         : `Source '${sourceName}'`,
   );

   if (!referencesAuthorize(persistSource)) return;

   if (
      origin === "persist" &&
      gateOutcome?.classification === "row_level" &&
      gateOutcome.attributed
   ) {
      // Proven safe above: the entry point's own gate compiles to a row
      // filter and nothing else is reachable beneath it, so colocated serving
      // grafts exactly what a live query would.
      return;
   }

   // Same "authorize" reason as the storage gate, different cause: this pass
   // cannot prove the gate is the entry point's own row filter.
   recordEligibilityRefused("authorize");
   const gated =
      origin === "preaggregate"
         ? `the source '${sourceName}' rolls up is protected by an ` +
           `#(authorize) gate (its own, a joined source's, or inherited ` +
           `from a source it derives from)`
         : `it is protected by an #(authorize) gate (its own, a joined ` +
           `source's, or inherited from a source it derives from)`;
   // For a joined or inherited gate, "move the gate to a source that is not
   // materialized" is already true; the fix there is making the gate the entry
   // point's OWN row filter (see `isAuthorizeAttributedToEntryPoint`).
   const remedy =
      origin === "preaggregate"
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
      origin === "preaggregate"
         ? `Pre-aggregation rollup '${sourceName}' cannot be built`
         : `Source '${sourceName}' cannot be materialized (colocated ` +
           `'#@ persist')`;
   // Only true of a rollup: it GROUPS, so the gated column is not even
   // present to filter on afterwards.
   const alsoRollup =
      origin === "preaggregate"
         ? ` A rollup also groups ACROSS the gated column, so it could not ` +
           `be row-filtered afterwards even in principle.`
         : "";
   // Generic message: this function cannot see WHY `gateOutcome` did not
   // clear the bar (missing, rejected, or a join-only gate).
   throw new MaterializationEligibilityError({
      reason: "authorize",
      message:
         `${what}: ${gated}. An authorize expression is evaluated per ` +
         `request; without a proven row-level, fully-attributed ` +
         `classification this pass cannot show the served artifact matches ` +
         `what a live query would enforce.` +
         `${alsoRollup} This is refused for safety. ${remedy}`,
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
   const def = persistSource._sourceDef as unknown;
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
      return walkForAuthorize(persistSource._sourceDef, new WeakSet(), 0);
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
         const text =
            typeof n === "string"
               ? n
               : n &&
                   typeof n === "object" &&
                   typeof (n as { text?: unknown }).text === "string"
                 ? (n as { text: string }).text
                 : undefined;
         if (text !== undefined && isAuthorizeAnnotation(text)) return true;
      }
   }

   for (const value of Object.values(record)) {
      if (walkForAuthorize(value, seen, depth + 1)) return true;
   }
   return false;
}

/**
 * Whether every `#(authorize)` note beneath the compiled source (crossing
 * joins) is also reachable WITHOUT crossing a join. `false` means some gate is
 * reachable only through a join, which a row-level classification of the entry
 * point's gate says nothing about, so relaxing the colocated refusal requires
 * `true` as well as `row_level`.
 *
 * The no-joins walk is believed to cover what `collectEntryPointGates`
 * (`./gate_classification`) reaches along identity edges, but the two are not
 * proven equivalent. Notes are compared by object identity, not text (two
 * gates can share text). Fails closed: any failure is unattributed.
 */
export function isAuthorizeAttributedToEntryPoint(
   persistSource: PersistSource,
): boolean {
   try {
      const deep = new Set<AnnotationNote>();
      walkForAuthorizeNotes(
         persistSource._sourceDef,
         new WeakSet(),
         0,
         deep,
         true,
      );
      const noJoins = new Set<AnnotationNote>();
      walkForAuthorizeNotes(
         persistSource._sourceDef,
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
         const text =
            typeof n === "string"
               ? n
               : n &&
                   typeof n === "object" &&
                   typeof (n as { text?: unknown }).text === "string"
                 ? (n as { text: string }).text
                 : undefined;
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
