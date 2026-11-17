// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The bind: committed manifest entries joined to the package's plan, as the
 * bindings and the colocated entries a snapshot holds, and the entries it
 * declines with why. One function per half of a manifest, both pure.
 *
 * - {@link bindStorageEntries}: a `storage=` entry binds only when a plan
 *   source of its name was built at its content address (never by name
 *   alone: another version's or another edit's table is declined), and only
 *   for the sources the eligibility gate found eligible. Eligibility must be
 *   positive: no plan, or a source missing from it, declines.
 * - {@link bindColocatedEntries}: a colocated entry binds only when its
 *   content address is colocated-eligible, its table name quoted for its
 *   plan's dialect (Malloy pastes a manifest's table name verbatim).
 * - {@link deriveColocatedServeBindings}: the served copy's bindings for the
 *   colocated `#(materialize)` and v1 `#@ persist` tables those entries name,
 *   read on the source's own connection. A `#(materialize)` build source is
 *   read by no author query, so Malloy's own manifest substitution never
 *   serves it; the served copy re-roots the author's source on it, exactly as
 *   on a `storage=` table.
 * - {@link applyObservedColumns}: those bindings and entries held to the
 *   columns the bind read from each table's warehouse, so a column altered
 *   outside a build is read with the type the table holds now.
 *
 * Called by the facade, which logs and counts what each declines; no I/O.
 */

import {
   quoteManifestTablePath,
   UnquotableIdentifierError,
} from "../../quoting";
import {
   isPersistedQuerySourceID,
   type MaterializePlan,
   type StoredColumn,
} from "../domain/table_plans";
import { capabilitiesOf } from "../domain/dialect";
import {
   storedColumnsOfOutput,
   storedMalloyType,
} from "../stored_column_types";
import type {
   BuildPlan,
   FreshnessManifest,
   ManifestEntry,
   ObservedColumns,
   UnreadReason,
} from "../domain/types";
import {
   type Binding,
   type BindingKind,
   deriveServeBindings,
   primaryKeysOf,
} from "../domain/binding";
import { groupAliasesByName, planSourceIds } from "../domain/source_aliases";
import type { BindDeclineReason as SourceBindDecline } from "../domain/source_decline";

/** Why the bind declined an entry: the plan could not be computed, or it refused the source. */
type BindDeclineReason = Exclude<SourceBindDecline, "missing_table">;

/** One `storage=` binding the bind declined, for the facade's log and counter. */
interface StorageBindDecline {
   reason: BindDeclineReason;
   sourceName: string;
   /** A rollup, whose generated name means nothing to an operator alone. */
   rollup: boolean;
   /** Why, in a sentence, for the log. */
   detail: string;
}

const PLAN_UNAVAILABLE =
   "the package build plan could not be computed, so no source was examined for eligibility";
const NOT_IN_PLAN =
   "the source is not in the package build plan, so it was never examined (a `#@ persist` on a non-query-shaped source is dropped)";

/**
 * The `storage=` bindings a manifest's entries make for this package.
 *
 * A binding is kept for its ELIGIBLE sources only: two models may declare
 * same-named sources with identical SQL (one table), one gated and refused.
 * Narrowing its sourceIDs keeps the refused one off the table in its own
 * model, where admitting the binding whole would have served it.
 */
export function bindStorageEntries(args: {
   entries: Record<string, ManifestEntry>;
   /** The plan bound against, or null when it could not be computed. */
   plan: BuildPlan | null;
   /** A destination's schema by name (see storageTablePath). */
   destinationSchema?: (destinationName: string) => string | undefined;
   /** The plan's eligible sourceIDs, and why each other was refused; undefined when unknown. */
   eligibility:
      | {
           eligible: readonly string[];
           refused: Record<string, string>;
           /** The eligible sourceIDs admitted past their own gate (G3). */
           gateKept?: readonly string[];
        }
      | undefined;
   /** Each binding's kind, from the plan (see {@link kindFromPlan}). */
   kindOf: (binding: Binding) => BindingKind;
}): {
   bindings: Binding[];
   /** Entries built from another definition of their source, by source name. */
   otherDefinition: string[];
   declined: StorageBindDecline[];
} {
   const { plan, eligibility } = args;
   const sourceIDsFor = planSourceIds(plan?.sources ?? {});
   const otherDefinition: string[] = [];
   // An entry at an address no same-named plan source has was built from
   // another definition (another version's, an earlier edit's, an older
   // release's), so it is declined and that source serves live.
   const own: Record<string, ManifestEntry> = {};
   for (const [key, entry] of Object.entries(args.entries)) {
      if (
         !plan ||
         !entry.sourceName ||
         sourceIDsFor(entry.sourceName, entry.sourceEntityId).length > 0
      ) {
         own[key] = entry;
      } else {
         otherDefinition.push(entry.sourceName);
      }
   }
   const derived = deriveServeBindings(
      own,
      groupAliasesByName(Object.values(plan?.sources ?? {})),
      args.destinationSchema,
      sourceIDsFor,
   );
   const eligible = new Set(eligibility?.eligible ?? []);
   const keptTables = keptTableAddresses(
      eligibility?.gateKept,
      (id) => plan?.sources?.[id]?.sourceEntityId,
   );
   const declined: StorageBindDecline[] = [];
   const admittedBindings = derived.flatMap((derivedBinding) => {
      const binding = { ...derivedBinding, kind: args.kindOf(derivedBinding) };
      const ids = binding.sourceIDs ?? [];
      const admitted = ids.filter((id) => eligible.has(id));
      if (admitted.length > 0) {
         return [
            admitted.length === ids.length
               ? binding
               : { ...binding, sourceIDs: admitted },
         ];
      }
      const refusal = ids
         .map((id) => eligibility?.refused[id])
         .find((reason) => reason !== undefined);
      declined.push({
         reason:
            eligibility === undefined ? "build_plan_unavailable" : "refused",
         sourceName: binding.sourceName,
         rollup: binding.kind === "rollup",
         detail:
            eligibility === undefined
               ? PLAN_UNAVAILABLE
               : (refusal ??
                 (ids.length === 0
                    ? "the table names no source of this package's build plan, so none was examined"
                    : NOT_IN_PLAN)),
      });
      return [];
   });
   // The table, not the name, is what G3 admits: every name a G3 table is
   // bound under is marked, since every one reads that one table (an
   // extension that shares its base's table re-roots the base). So any
   // sharer of a gate-kept source's content address counts as G3, an
   // ungated sharer included: a gated entry point that joins an ungated
   // sharer is served from the table because a gated sibling stores into
   // it. Nothing leaks: the SQL, and so the rows, are identical, and
   // `govern()` grafts and proves the entry point's own gate on the answer
   // as on live. A rollup or a view is never marked (their legs pass a gate
   // only with the switch).
   const bindings = admittedBindings.map((binding) =>
      keptTables.has(binding.virtualHandle) &&
      (binding.kind === "persist" || binding.kind === "materialize")
         ? { ...binding, ownGateKept: true }
         : binding,
   );
   return { bindings, otherDefinition, declined };
}

/**
 * The tables (content addresses) plan admitted past a stored source's own
 * row-level gate under G3: the address of every `gateKept` source. The one
 * rule both the bind (which marks each binding of such a table
 * `ownGateKept`) and the serve verdict (for a table not bound yet) read.
 */
export function keptTableAddresses(
   gateKept: readonly string[] | undefined,
   addressOf: (sourceID: string) => string | undefined,
): Set<string> {
   const out = new Set<string>();
   for (const id of gateKept ?? []) {
      const address = addressOf(id);
      if (address) out.add(address);
   }
   return out;
}

/** One colocated entry the bind declined, for the facade's log and counter. */
interface ColocatedBindDecline {
   reason: BindDeclineReason;
   sourceEntityId: string;
   detail: string;
}

/**
 * The colocated entries a manifest binds, filtered POSITIVELY against the
 * colocated eligibility (a miss is declined, not fatal), each table name
 * quoted for its plan's dialect: a table named by a reserved word or needing
 * its case kept reaches the warehouse only quoted, as the builder created it.
 * A name written as SQL is parsed and re-quoted; one its dialect cannot read
 * or quote is declined (`unquotable_name`), and the rest still bind.
 */
export function bindColocatedEntries(args: {
   entries: FreshnessManifest;
   eligibility:
      | {
           eligibleEntityIds: ReadonlySet<string>;
           refused: Record<string, string>;
        }
      | undefined;
   /** Each planned source's dialect, by its content address. */
   dialects: ReadonlyMap<string, string>;
}): { entries: FreshnessManifest; declined: ColocatedBindDecline[] } {
   const { eligibility } = args;
   const entries: FreshnessManifest = {};
   const declined: ColocatedBindDecline[] = [];
   for (const [sourceEntityId, entry] of Object.entries(args.entries)) {
      if (eligibility?.eligibleEntityIds.has(sourceEntityId)) {
         const dialect = args.dialects.get(sourceEntityId);
         if (!dialect) {
            // Never inlined as given: with no dialect to quote it in, the
            // entry is not bound.
            declined.push({
               reason: "unquotable_name",
               sourceEntityId,
               detail:
                  "no dialect is known for its table, to quote its name in",
            });
            continue;
         }
         try {
            entries[sourceEntityId] = {
               ...entry,
               tableName: quoteManifestTablePath(entry.tableName, dialect),
            };
         } catch (err) {
            if (!(err instanceof UnquotableIdentifierError)) throw err;
            declined.push({
               reason: "unquotable_name",
               sourceEntityId,
               detail: err.message,
            });
         }
         continue;
      }
      declined.push({
         reason:
            eligibility === undefined ? "build_plan_unavailable" : "refused",
         sourceEntityId,
         detail:
            eligibility === undefined
               ? PLAN_UNAVAILABLE
               : (eligibility.refused[sourceEntityId] ?? NOT_IN_PLAN),
      });
   }
   return { entries, declined };
}

/**
 * A binding's kind, read off the plan: a rollup when its entry says so, a
 * persisted view's table when it stores a view's generated build source, a
 * `#(materialize)` table when it stores a `#(materialize)` source, else a
 * `#@ persist` table.
 */
export function kindFromPlan(
   binding: Pick<Binding, "kind" | "sourceIDs">,
   plan: {
      materializeSourceIDs: ReadonlySet<string>;
      viewBuildSourceIDs: ReadonlySet<string>;
   },
): BindingKind {
   if (binding.kind === "rollup") return "rollup";
   const ids = binding.sourceIDs ?? [];
   const result = ids.find((id) => plan.viewBuildSourceIDs.has(id));
   if (result !== undefined) {
      return isPersistedQuerySourceID(result) ? "query" : "view";
   }
   if (ids.some((id) => plan.materializeSourceIDs.has(id))) {
      return "materialize";
   }
   return "persist";
}

/** What a binding reads of a build-plan source. */
interface ColocatedPlanSource {
   name: string;
   sourceEntityId: string;
   connectionName: string;
   dialect?: string;
   origin?: "persist" | "preaggregate";
   annotationFields?: Record<string, string>;
}

/**
 * The served copy's bindings for the colocated `#(materialize)` tables the
 * bound manifest holds: one per planned `#(materialize)` source without
 * `storage=` whose content address the manifest names, admitted by
 * `eligible` (the same positive eligibility a `storage=` binding needs).
 *
 * Its columns are the plan's compiled output columns
 * ({@link MaterializePlan.storedColumns}) and each one the build captured from
 * the table it wrote that the plan cannot spell (a zoned timestamp), so a
 * source over one is bound as a `storage=` source is. With neither, it binds
 * nothing: its table's columns are unknown, so it serves live.
 */
export function deriveColocatedServeBindings(args: {
   /** The bound colocated manifest, freshness fields included. */
   entries: FreshnessManifest | undefined;
   /** The build plan's sources, by sourceID. */
   planSources: Readonly<Record<string, ColocatedPlanSource>>;
   /** The `#(materialize)` plans, by the author's sourceID. */
   materializePlans: Readonly<Record<string, MaterializePlan>>;
   /** Whether the bind admits a planned source (positive eligibility). */
   eligible: (sourceID: string) => boolean;
   /**
    * A v1 `#@ persist` source's stored columns derived
    * from its compiled output, for a table whose build captured no schema;
    * undefined when it cannot be derived. Absent, no v1 source is bound.
    */
   v1Schema?: (sourceID: string) => StoredColumn[] | undefined;
   /**
    * Planned sources that are not v1 `#@ persist` sources of the author's
    * (a persisted view's generated build source), never bound as v1.
    */
   notV1?: (sourceID: string) => boolean;
}): Binding[] {
   const { entries, planSources, materializePlans } = args;
   if (!entries) return [];
   const bindings: Binding[] = [];
   for (const [sourceID, source] of Object.entries(planSources)) {
      const plan = materializePlans[sourceID];
      if (source.origin === "preaggregate") continue;
      if (!plan && (!args.v1Schema || args.notV1?.(sourceID))) continue;
      // A `storage=` table is bound from the storage manifest, never here.
      if (
         plan?.persist.storage !== undefined ||
         source.annotationFields?.storage?.trim()
      ) {
         continue;
      }
      const entry = entries[source.sourceEntityId];
      if (!entry?.tableName) continue;
      const schema = plan
         ? plannedWithCaptured(plan.storedColumns, entry.schema)
         : // What its build captured, else what its compiled output derives
           // (a table built before the capture).
           entry.schema?.length
           ? entry.schema
           : args.v1Schema?.(sourceID);
      if (!schema?.length) continue;
      // Built on another connection than the plan names (a manifest from
      // another configuration): not this source's table.
      if (
         entry.connectionName !== undefined &&
         entry.connectionName !== source.connectionName
      ) {
         continue;
      }
      if (!args.eligible(sourceID)) continue;
      // Never inlined as given: with no dialect, or a name the dialect
      // cannot read or quote, it is not bound and the source answers live.
      if (!source.dialect) continue;
      let tablePath: string;
      try {
         tablePath = quoteManifestTablePath(entry.tableName, source.dialect);
      } catch (err) {
         if (!(err instanceof UnquotableIdentifierError)) throw err;
         continue;
      }
      bindings.push({
         kind: plan ? "materialize" : "persist",
         tier: "colocated",
         sourceName: source.name,
         sourceIDs: [sourceID],
         destinationName: source.connectionName,
         virtualHandle: source.sourceEntityId,
         // Canonical for the connection's dialect, as Malloy's manifest
         // substitution pastes it (a reserved or case-preserved name quoted).
         tablePath,
         schema: schema.map((c) => ({ ...c })),
         ...(entry.nativeSchema?.length
            ? { nativeSchema: entry.nativeSchema.map((c) => ({ ...c })) }
            : {}),
         ...(entry.dataAsOf !== undefined ? { freshAsOf: entry.dataAsOf } : {}),
         ...(entry.freshnessWindowSeconds !== undefined
            ? { freshnessWindowSeconds: entry.freshnessWindowSeconds }
            : {}),
         ...(entry.freshnessFallback !== undefined
            ? { freshnessFallback: entry.freshnessFallback }
            : {}),
         // The key checks the run that wrote the table recorded beside it
         // (a pair table's leaf key, known limitation 40); internal, never
         // from a host's manifest.
         ...(primaryKeysOf(entry) ? { primaryKeys: primaryKeysOf(entry) } : {}),
      });
   }
   return bindings;
}

/** A captured zoned timestamp's spelling (`storedColumnsOfOutput`). */
const ZONED_TIMESTAMP = /^(TIMESTAMP WITH TIME ZONE|TIMESTAMPTZ)$/i;

/**
 * A colocated `#(materialize)` table's columns: the plan's, typed as Malloy
 * declares them (a truncation's `DATE`, which the warehouse may hold as a
 * timestamp), and each zoned timestamp the build captured, which the plan
 * cannot spell, as it captured it, as a `storage=` binding declares one. No
 * other captured column is added: one the plan leaves out has no faithful
 * declaration. With no capture (a table built before it), the plan's alone.
 */
function plannedWithCaptured(
   planned: StoredColumn[] | undefined,
   captured: StoredColumn[] | undefined,
): StoredColumn[] | undefined {
   if (!planned?.length) return planned;
   const named = new Set(planned.map((c) => c.name));
   return [
      ...planned,
      ...(captured ?? []).filter(
         (c) => !named.has(c.name) && ZONED_TIMESTAMP.test(c.type.trim()),
      ),
   ];
}

/**
 * The columns a v1 colocated `#@ persist` build wrote, derived from the
 * source's compiled definition (for a table built before its build captured
 * a schema). A v1 build source is query-shaped, so its
 * stored columns are the query's output fields. They are read from the
 * persisted query's output struct when the compiled source records it (what
 * the `extend` block drops with `except:` or `accept:` is still a column);
 * otherwise from the source's fields with no expression, minus views, with
 * two corrections that make the compiled field list read as the table:
 *
 * - a stored column is the field's name before any `rename:` (`as`), since
 *   the rename is applied in the source's `extend` block, after the build;
 * - a nest is a column (an array or record), though Malloy gives it a
 *   `join`, while every other joined field is not stored.
 *
 * A computed dimension in the `extend` block has an expression, so it is not
 * a column (the served copy keeps its definition). A field whose type has no
 * faithful stored spelling is left out ({@link storedColumnsOfOutput}), so
 * the served copy finds it missing and that source serves live.
 */
export function v1StoredColumnsOf(def: unknown): StoredColumn[] | undefined {
   const d = def as
      | {
           fields?: unknown[];
           dialect?: unknown;
           query?: { pipeline?: { outputStruct?: { fields?: unknown[] } }[] };
        }
      | undefined;
   // The persisted query's own output, when the compiled source records it:
   // exactly the build's columns, before the `extend` block renames, drops
   // (`except:`, `accept:`) or re-hides any.
   const output = d?.query?.pipeline?.at(-1)?.outputStruct?.fields;
   const fields = Array.isArray(output) ? output : d?.fields;
   if (!Array.isArray(fields)) return undefined;
   const outputs = fields
      .filter((f) => {
         const x = f as Record<string, unknown>;
         return (
            x !== null &&
            typeof x === "object" &&
            !("e" in x) &&
            x.type !== "turtle" &&
            (!("join" in x) || x.type === "array" || x.type === "record")
         );
      })
      .map((f) => {
         const { as: _as, ...rest } = f as Record<string, unknown>;
         return rest;
      });
   return storedColumnsOfOutput(outputs, {
      nested: capabilitiesOf(d?.dialect).nestedColumns,
      bigint: true,
      timestamptz: true,
   });
}

/** A bound database table the bind could not read, and why. */
export interface UnreadTable {
   sourceEntityId: string;
   reason: UnreadReason;
   /** The driver's message, secrets redacted: for the log alone. */
   detail?: string;
}

/** One bound colocated table whose warehouse columns moved since its build. */
export interface ObservedDrift {
   sourceEntityId: string;
   /** Each column whose type is no longer the one its build wrote, or that is gone. */
   columns: { column: string; builtType: string; observedType?: string }[];
}

/**
 * The colocated bindings and entries held to the columns the bind read from
 * each table's warehouse ({@link ObservedColumns}). A colocated table's types
 * are otherwise the plan's, or what its build captured, so a column altered
 * outside a build would be read with a type the table no longer holds.
 *
 * A column has drifted when the Malloy type its warehouse type reads as is
 * not the one of the type its build wrote (its captured `schema`, else the
 * binding's own): the bind compares like with like, so a warehouse that
 * stores a type of its own (a Postgres truncation as a timestamp) is never
 * drift. Then:
 *
 * - a served-copy binding (`#(materialize)`, `#@ persist`) declares each
 *   drifted column with its observed type, and the one stored-column read
 *   plan casts it or leaves it out (a gone column is left out);
 * - an entry read by Malloy's build-manifest substitution alone (a rollup, a
 *   persisted view or query), which no cast reaches, is left out of the
 *   entries, so its leg finds no table and the query answers live;
 * - a table whose read failed (`unread`), or that was not read, is left out
 *   of both: it answers live (fail safe).
 *
 * With no observation (`observed` undefined) everything is returned as is.
 */
export function applyObservedColumns(args: {
   entries: FreshnessManifest;
   bindings: readonly Binding[];
   observed: ReadonlyMap<string, ObservedColumns> | undefined;
}): {
   entries: FreshnessManifest;
   bindings: Binding[];
   drifted: ObservedDrift[];
   unread: UnreadTable[];
} {
   const { observed } = args;
   if (!observed) {
      return {
         entries: args.entries,
         bindings: [...args.bindings],
         drifted: [],
         unread: [],
      };
   }
   const entries: FreshnessManifest = {};
   const drifted: ObservedDrift[] = [];
   const unread: UnreadTable[] = [];
   const left = new Set<string>();
   const reads = new Map<
      string,
      Extract<ObservedColumns, { columns: unknown }>
   >();
   const served = new Set(
      args.bindings
         .filter((b) => b.tier === "colocated")
         .map((b) => b.virtualHandle),
   );
   for (const [id, entry] of Object.entries(args.entries)) {
      const read: ObservedColumns = observed.get(id) ?? {
         unread: "not_read",
      };
      if ("unread" in read) {
         unread.push({
            sourceEntityId: id,
            reason: read.unread,
            ...(read.detail !== undefined ? { detail: read.detail } : {}),
         });
         left.add(id);
         continue;
      }
      reads.set(id, read);
      const columns = entry.schema?.length
         ? entry.schema.flatMap((c) => {
              const d = columnDrift(
                 c,
                 nativeTypeOf(entry.nativeSchema, c.name),
                 read,
              );
              return d.kind === "none" ? [] : [d.report];
           })
         : // A build that captured only native types (a warehouse whose
           // schema reader refused the name): those are what moved or not.
           nativeDriftOf(entry.nativeSchema ?? [], read);
      if (columns.length > 0) {
         drifted.push({ sourceEntityId: id, columns });
         // Read by Malloy's manifest substitution alone: no cast reaches it.
         if (!served.has(id)) {
            left.add(id);
            continue;
         }
      }
      entries[id] = entry;
   }
   const bindings = args.bindings.flatMap((b): Binding[] => {
      if (b.tier !== "colocated") return [b];
      if (left.has(b.virtualHandle)) return [];
      const read = reads.get(b.virtualHandle);
      if (!read) return [b];
      const entry = args.entries[b.virtualHandle];
      const built = new Map((entry?.schema ?? []).map((c) => [c.name, c]));
      const extra: ObservedDrift["columns"] = [];
      let moved = false;
      const retyped = new Map<string, string>();
      const schema = b.schema.flatMap((c) => {
         const d = columnDrift(
            built.get(c.name) ?? c,
            nativeTypeOf(b.nativeSchema ?? entry?.nativeSchema, c.name),
            read,
         );
         if (d.kind === "none") return [c];
         moved = true;
         if (!built.has(c.name)) extra.push(d.report);
         if (d.kind === "drop") return [];
         if (d.nativeType !== undefined) retyped.set(c.name, d.nativeType);
         return [{ name: c.name, type: d.type }];
      });
      if (extra.length > 0) {
         const known = drifted.find(
            (x) => x.sourceEntityId === b.virtualHandle,
         );
         if (known) {
            for (const e of extra) {
               if (!known.columns.some((k) => k.column === e.column)) {
                  known.columns.push(e);
               }
            }
         } else {
            drifted.push({ sourceEntityId: b.virtualHandle, columns: extra });
         }
      }
      if (!moved) return [b];
      // A big-integer column is cast to its native type at read: the type
      // the warehouse holds now, never the one it was built with.
      const nativeSchema = b.nativeSchema?.map((n) =>
         retyped.has(n.name) ? { ...n, type: retyped.get(n.name)! } : n,
      );
      return [{ ...b, schema, ...(nativeSchema ? { nativeSchema } : {}) }];
   });
   return { entries, bindings, drifted, unread };
}

/** The built native columns whose native type moved, or that are gone. */
function nativeDriftOf(
   built: readonly { name: string; type: string }[],
   read: Extract<ObservedColumns, { columns: unknown }>,
): ObservedDrift["columns"] {
   return built.flatMap((c) => {
      const now = columnNamed(read.columns, c.name);
      const report = {
         column: c.name,
         builtType: c.type,
         ...(now?.nativeType !== undefined
            ? { observedType: now.nativeType }
            : {}),
      };
      if (!now) return [report];
      return now.nativeType !== undefined &&
         normalizedType(now.nativeType) !== normalizedType(c.type)
         ? [report]
         : [];
   });
}

/** A column's type as the build's native capture named it, by name. */
function nativeTypeOf(
   nativeSchema: readonly { name: string; type: string }[] | undefined,
   name: string,
): string | undefined {
   return columnNamed(nativeSchema ?? [], name)?.type;
}

/** What one built column's observed type does to its read. */
type ColumnDrift =
   | { kind: "none" }
   | {
        kind: "retype";
        /** The observed type, in the binding schema's spelling. */
        type: string;
        /** The observed native type, for a big-integer cast. */
        nativeType?: string;
        report: ObservedDrift["columns"][number];
     }
   | { kind: "drop"; report: ObservedDrift["columns"][number] };

/**
 * Whether a built column drifted, and how it is read now:
 *
 * - gone, or a type Malloy reads as no basic type whose native type moved:
 *   `drop` (left out, so a query reading it answers live);
 * - a timestamp now zoned, or a zoned one now naive: `drop`, unless the
 *   dialect is DuckDB, whose serving sessions run in UTC so either reads
 *   alike;
 * - another Malloy type, or the same one with another native type (a
 *   `bigint` altered to `numeric(10,2)`): `retype`, read through the one
 *   stored-column read plan with the type it holds now;
 * - otherwise `none`.
 */
function columnDrift(
   built: { name: string; type: string },
   builtNative: string | undefined,
   read: Extract<ObservedColumns, { columns: unknown }>,
): ColumnDrift {
   const now = columnNamed(read.columns, built.name);
   const report = {
      column: built.name,
      builtType: built.type,
      ...(now?.type !== undefined ? { observedType: now.type } : {}),
   };
   if (!now) return { kind: "drop", report };
   const nativeMoved =
      builtNative !== undefined &&
      now.nativeType !== undefined &&
      normalizedType(builtNative) !== normalizedType(now.nativeType);
   if (now.type === undefined) {
      return nativeMoved ? { kind: "drop", report } : { kind: "none" };
   }
   if (
      typeFamily(now.type) === "timestamp" &&
      typeFamily(built.type) === "timestamp" &&
      isZoned(now.type) !== isZoned(built.type)
   ) {
      return capabilitiesOf(read.dialect).utcSessions
         ? { kind: "none" }
         : { kind: "drop", report };
   }
   if (typeFamily(now.type) !== typeFamily(built.type) || nativeMoved) {
      return {
         kind: "retype",
         type: now.type,
         ...(now.nativeType !== undefined
            ? { nativeType: now.nativeType }
            : {}),
         report,
      };
   }
   return { kind: "none" };
}

/** A native type compared by its spelling, case and spacing aside. */
function normalizedType(type: string): string {
   return type.trim().replace(/\s+/g, " ").toLowerCase();
}

/** A zoned timestamp, as the capture spells it. */
function isZoned(type: string): boolean {
   return /^(TIMESTAMP WITH TIME ZONE|TIMESTAMPTZ)$/i.test(type.trim());
}

/**
 * A column by name: exactly, else by a unique case-insensitive match (a
 * warehouse may fold the case its catalog reports).
 */
function columnNamed<C extends { name: string }>(
   columns: readonly C[],
   name: string,
): C | undefined {
   const exact = columns.find((c) => c.name === name);
   if (exact) return exact;
   const folded = columns.filter(
      (c) => c.name.toLowerCase() === name.toLowerCase(),
   );
   return folded.length === 1 ? folded[0] : undefined;
}

/** The Malloy type a stored type reads as, or its own spelling when none. */
function typeFamily(type: string): string {
   return storedMalloyType(type) ?? type.trim().toUpperCase();
}
