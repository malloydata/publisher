// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The bind: committed manifest entries joined to the package's plan, as the
 * bindings and the database entries a snapshot holds, and the entries it
 * declines with why. One function per half of a manifest, both pure.
 *
 * - {@link bindStorageEntries}: a `storage=` entry binds only when a plan
 *   source of its name was built at its content address (never by name
 *   alone: another version's or another edit's table is declined), and only
 *   for the sources the eligibility gate found eligible. Eligibility must be
 *   positive: no plan, or a source missing from it, declines.
 * - {@link bindDatabaseEntries}: a database entry binds only when its
 *   content address is database-eligible, its table name quoted for its
 *   plan's dialect (Malloy pastes a manifest's table name verbatim).
 * - {@link deriveDatabaseServeBindings}: the served copy's bindings for the
 *   database `#(materialize)` and v1 `#@ persist` tables those entries name,
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
   isRollupOrigin,
   type TableOrigin,
} from "../domain/table_plans";
import { capabilitiesOf } from "../domain/dialect";
import {
   driftDeclaredType,
   driftedColumnRead,
   storedColumnsOfOutput,
   storedMalloyType,
   storedTypeServesAs,
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
           /** The eligible sourceIDs whose tables hold only ungated rows. */
           ungated?: readonly string[];
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
   // as on live. A `storage=` rollup plan admitted past its base's gates is
   // marked too: its variant grafts and proves them (rollup_variants.ts). A
   // view is never marked (its leg passes a gate only with the switch).
   const bindings = admittedBindings.map((binding) =>
      keptTables.has(binding.virtualHandle) &&
      (binding.kind === "persist" ||
         binding.kind === "materialize" ||
         binding.kind === "rollup")
         ? { ...binding, ownGateKept: true }
         : binding,
   );
   // A table that holds only ungated rows is read past a gated entry point's
   // gate as a database table is (g-mixed): every source stored at its
   // address, by the plan, not only those bound, is proven ungated. Only a
   // table the served copy reads past a gate is marked: a rollup's, a
   // persisted view's or a persisted query's leg decides a gate on its own.
   const ungatedTables = ungatedTableAddresses(
      eligibility?.ungated,
      plan?.sources ?? {},
   );
   const marked = bindings.map((binding) =>
      ungatedTables.has(binding.virtualHandle) &&
      (binding.kind === "persist" || binding.kind === "materialize") &&
      (binding.sourceIDs ?? []).length > 0 &&
      (binding.sourceIDs ?? []).every((id) =>
         eligibility?.ungated?.includes(id),
      )
         ? { ...binding, holdsOnlyUngatedRows: true }
         : binding,
   );
   return { bindings: marked, otherDefinition, declined };
}

/**
 * The tables (content addresses) that hold only ungated rows: those at which
 * every source the plan stores is in `ungated` (the plan's
 * `SourceEligibility.ungated`). One source on the address that is gated,
 * refused, or whose gates could not be read keeps the address out (fail
 * closed). The one rule both the bind (which marks each binding of such a
 * table `holdsOnlyUngatedRows`) and the serve verdict (for a table not bound
 * yet) read.
 */
export function ungatedTableAddresses(
   ungated: readonly string[] | undefined,
   sources: Readonly<Record<string, { sourceEntityId?: string }>>,
): Set<string> {
   const proven = new Set(ungated ?? []);
   const byAddress = new Map<string, boolean>();
   for (const [id, source] of Object.entries(sources)) {
      const address = source.sourceEntityId;
      if (!address) continue;
      byAddress.set(
         address,
         (byAddress.get(address) ?? true) && proven.has(id),
      );
   }
   return new Set(
      [...byAddress].filter(([, all]) => all).map(([address]) => address),
   );
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

/** One database entry the bind declined, for the facade's log and counter. */
interface DatabaseBindDecline {
   reason: BindDeclineReason;
   sourceEntityId: string;
   detail: string;
}

/**
 * The database entries a manifest binds, filtered POSITIVELY against the
 * database eligibility (a miss is declined, not fatal), each table name
 * quoted for its plan's dialect: a table named by a reserved word or needing
 * its case kept reaches the warehouse only quoted, as the builder created it.
 * A name written as SQL is parsed and re-quoted; one its dialect cannot read
 * or quote is declined (`unquotable_name`), and the rest still bind.
 */
export function bindDatabaseEntries(args: {
   entries: FreshnessManifest;
   eligibility:
      | {
           eligibleEntityIds: ReadonlySet<string>;
           refused: Record<string, string>;
        }
      | undefined;
   /** Each planned source's dialect, by its content address. */
   dialects: ReadonlyMap<string, string>;
}): { entries: FreshnessManifest; declined: DatabaseBindDecline[] } {
   const { eligibility } = args;
   const entries: FreshnessManifest = {};
   const declined: DatabaseBindDecline[] = [];
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
interface DatabasePlanSource {
   name: string;
   sourceEntityId: string;
   connectionName: string;
   dialect?: string;
   origin?: TableOrigin | "preaggregate";
   annotationFields?: Record<string, string>;
}

/**
 * The served copy's bindings for the database `#(materialize)` tables the
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
export function deriveDatabaseServeBindings(args: {
   /** The bound database manifest, freshness fields included. */
   entries: FreshnessManifest | undefined;
   /** The build plan's sources, by sourceID. */
   planSources: Readonly<Record<string, DatabasePlanSource>>;
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
      if (isRollupOrigin(source.origin)) continue;
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
         tier: "database",
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
 * A database `#(materialize)` table's columns: the plan's, typed as Malloy
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
 * The columns a v1 database `#@ persist` build wrote, derived from the
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

/** One bound database table whose warehouse columns moved since its build. */
export interface ObservedDrift {
   sourceEntityId: string;
   /**
    * Each column whose type is no longer the one its build wrote, or that is
    * gone, or a 128-bit integer a DuckLake table holds as a DOUBLE
    * (`storedAsDouble`).
    */
   columns: {
      column: string;
      builtType: string;
      observedType?: string;
      /** The table no longer has the column. */
      gone?: true;
      /**
       * The bind leaves the column out of a served-copy binding, so a query
       * reading it answers live (gone, a type Malloy reads as no basic type,
       * or a timestamp that gained or lost its zone off DuckDB); otherwise
       * it is read with the type it holds now.
       */
      leftOut?: true;
      /** A 128-bit integer a DuckLake table holds as a DOUBLE. */
      storedAsDouble?: true;
      /**
       * The table holds the type its build wrote, but its source now
       * declares another: what the source reads changed type since the
       * build ({@link declaredMoves}).
       */
      declaredMoved?: true;
      /**
       * Read through its faithful cast but not reported: a type a build
       * writes for the declaration by design ({@link declaredMoves}).
       */
      castOnly?: true;
   }[];
}

/**
 * The database bindings and entries held to the columns the bind read from
 * each table's warehouse ({@link ObservedColumns}). A database table's types
 * are otherwise the plan's, or what its build captured, so a column altered
 * outside a build would be read with a type the table no longer holds.
 *
 * A column has drifted when the Malloy type its warehouse type reads as is
 * not the one of the type its build wrote (its captured `schema`, else the
 * binding's own): the bind compares like with like, so a warehouse that
 * stores a type of its own (a Postgres truncation as a timestamp) is never
 * drift. With no captured `schema` the binding's declared type stands in, so
 * a column whose native type is still the one its build captured is read
 * with the type it holds but is not drift: no warning, no count. Then:
 *
 * - a served-copy binding (`#(materialize)`, `#@ persist`) declares each
 *   drifted column with its observed type, and the one stored-column read
 *   plan casts it or leaves it out (a gone column is left out);
 * - an entry read by Malloy's build-manifest substitution (a rollup, a
 *   persisted view or query), which no cast reaches, records the columns its
 *   table holds now (`heldRead`) and those it left out (`leftOutColumns`):
 *   its leg then reads it through a typed source as a `storage=` table is
 *   read, each moved column cast faithfully or left out by the same rule
 *   ({@link driftedColumnRead}), so only a query reading a left-out column
 *   answers live;
 * - a table whose read failed (`unread`), or that was not read, is left out
 *   of both: it answers live (fail safe).
 *
 * With no observation (`observed` undefined) everything is returned as is.
 */
export function applyObservedColumns(args: {
   entries: FreshnessManifest;
   bindings: readonly Binding[];
   observed: ReadonlyMap<string, ObservedColumns> | undefined;
   /**
    * The columns a substituted entry's build source declares, by name, and
    * the time zone its reader reads it in: what a drifted column must read
    * as. Without it, each column's declared type is the one its build wrote.
    */
   declared?: (sourceEntityId: string) => DriftDeclaration | undefined;
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
         .filter((b) => b.tier === "database")
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
      // Read by Malloy's manifest substitution alone, a table no compile
      // re-types: a column whose source now declares a type the table's
      // value does not read as (a warehouse column retyped upstream since the
      // build) has moved as surely as one altered in the table.
      const declaration = served.has(id) ? undefined : args.declared?.(id);
      if (declaration) {
         columns.push(...declaredMoves(entry, read, declaration, columns));
      }
      if (columns.length > 0 && !served.has(id)) {
         // Each moved column cast in a projection substituted for the table,
         // or left out.
         const substituted = substitutedRead(entry, columns, read, declaration);
         drifted.push({
            sourceEntityId: id,
            columns: substituted?.columns ?? columns,
         });
         if (substituted) entries[id] = substituted.entry;
         continue;
      }
      if (columns.length > 0) drifted.push({ sourceEntityId: id, columns });
      entries[id] = entry;
   }
   const bindings = args.bindings.flatMap((b): Binding[] => {
      if (b.tier !== "database") return [b];
      if (left.has(b.virtualHandle)) return [];
      const read = reads.get(b.virtualHandle);
      if (!read) return [b];
      const entry = args.entries[b.virtualHandle];
      const declaration = args.declared?.(b.virtualHandle);
      const built = new Map((entry?.schema ?? []).map((c) => [c.name, c]));
      const extra: ObservedDrift["columns"] = [];
      let moved = false;
      const retyped = new Map<string, string>();
      const movedColumns: { name: string; type: string }[] = [];
      // The binding's decision for each drifted column.
      const decided = new Map<string, ObservedDrift["columns"][number]>();
      const schema = b.schema.flatMap((c) => {
         const builtNative = nativeTypeOf(
            b.nativeSchema ?? entry?.nativeSchema,
            c.name,
         );
         const d = columnDrift(built.get(c.name) ?? c, builtNative, read);
         if (d.kind === "none") {
            // The table is as built, but its source now declares a type the
            // stored value does not read as (a column retyped upstream since
            // the build): declared with the type it holds, so the served
            // copy's read plan casts it or leaves it out.
            const [declaredMove] = declaration
               ? declaredMoves(
                    {
                       schema: [built.get(c.name) ?? c],
                    } as FreshnessManifest[string],
                    read,
                    declaration,
                    [],
                 )
               : [];
            if (!declaredMove?.observedType) return [c];
            extra.push(declaredMove);
            decided.set(c.name, declaredMove);
            moved = true;
            movedColumns.push({
               name: c.name,
               type: declaredMove.observedType,
            });
            return [{ name: c.name, type: declaredMove.observedType }];
         }
         moved = true;
         // With no captured type the binding's declared one stands in, which
         // is not what the build wrote when the warehouse returns a type of
         // its own (a date truncation as a timestamp): read with the type it
         // holds, as any drift is, but not drift when its native type is the
         // one the build captured.
         if (built.has(c.name)) {
            decided.set(c.name, d.report);
         } else if (!asBuilt(builtNative, c.name, read)) {
            extra.push(d.report);
            decided.set(c.name, d.report);
         }
         if (d.kind === "drop") return [];
         if (d.nativeType !== undefined) retyped.set(c.name, d.nativeType);
         // What the gate policy judges a gate over this column by.
         movedColumns.push({ name: c.name, type: d.nativeType ?? d.type });
         return [{ name: c.name, type: d.type }];
      });
      const known = drifted.find((x) => x.sourceEntityId === b.virtualHandle);
      if (known) {
         known.columns = known.columns.map((k) => {
            const d = decided.get(k.column);
            return d?.leftOut ? { ...k, leftOut: true as const } : k;
         });
      }
      if (extra.length > 0) {
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
      return [
         {
            ...b,
            schema,
            ...(nativeSchema ? { nativeSchema } : {}),
            ...(movedColumns.length > 0
               ? { retypedColumns: movedColumns }
               : {}),
         },
      ];
   });
   return { entries, bindings, drifted, unread };
}

/**
 * The columns of a substituted table that have not moved in the table but
 * whose build source now declares another type than the one its build wrote,
 * and that the value the table holds does not read as ({@link driftedColumnRead}
 * declines it as is): what its source reads changed type since the build,
 * which leaves its content address, and so its table, where they were.
 * Reported as any drift is, so the leg casts or leaves each out and the
 * package warns.
 */
function declaredMoves(
   entry: FreshnessManifest[string],
   read: Extract<ObservedColumns, { columns: unknown }>,
   declaration: DriftDeclaration,
   already: readonly ObservedDrift["columns"][number][],
): ObservedDrift["columns"] {
   const reported = new Set(already.map((c) => c.column));
   return (entry.schema ?? []).flatMap((built) => {
      if (reported.has(built.name)) return [];
      const declared = declaration.columns.find(
         (c) => c.name === built.name,
      )?.type;
      if (declared === undefined || declared === "") return [];
      // The type that serves the declaration: nothing to read differently.
      if (storedTypeServesAs(built.type, declared) !== false) return [];
      const now = columnNamed(read.columns, built.name);
      if (now?.type === undefined) return [];
      // A type a build writes for this declaration by design (a `date`
      // truncation as a naive timestamp, a `boolean` flag as an integer):
      // read through its faithful cast, which leaves a value the build wrote
      // as it is and corrects one a retype upstream left behind, but not
      // reported, since the manifest cannot tell the two apart.
      const report = {
         column: built.name,
         builtType: built.type,
         observedType: now.type,
         declaredMoved: true as const,
         ...(byDesign(built.type, declared) ? { castOnly: true as const } : {}),
      };
      const how = driftedColumnRead(
         report,
         declared,
         declaration.queryTimezone
            ? { queryTimezone: declaration.queryTimezone }
            : {},
      );
      return how.read === "as_is" ? [] : [report];
   });
}

/**
 * Whether a build of a column the source declares as `declared` writes
 * `storedType` by design, though the type does not serve it: a warehouse
 * answers a `date` truncation (or `date + interval`) with a naive timestamp,
 * and a `boolean` over an integer flag with the integer.
 */
function byDesign(storedType: string, declared: string): boolean {
   const family = typeFamily(storedType);
   if (declared === "date") {
      return family === "timestamp" && !isZoned(storedType);
   }
   if (declared === "boolean") {
      return family === "number" && /INT/i.test(storedType);
   }
   return false;
}

/** What a substituted table's moved columns must read as. */
export interface DriftDeclaration {
   /** The build source's declared columns, with their Malloy types. */
   columns: readonly { name?: string; type?: string }[];
   /** The connection its table is on, when its entry does not name it. */
   connectionName?: string;
   /** The time zone its reader reads it in ({@link BuildCastOptions}). */
   queryTimezone?: string;
}

/**
 * A table read by Malloy's build-manifest substitution whose columns moved,
 * each column left out where the bind's own rule leaves it out (gone, a type
 * Malloy reads as no basic type, a zone gained or lost off DuckDB), or where
 * its digits are lost (a 128-bit integer a DuckLake table holds as a DOUBLE,
 * a big integer now a float). Its leg reads the rest as they are now
 * (`heldRead`), through the one stored-column read plan, which casts a moved
 * column faithfully or leaves it out ({@link driftedColumnRead}). Each column
 * report carries `leftOut` as {@link driftedColumnRead} decides it, for the
 * warning. Undefined when the table's connection is not known: it answers
 * live (fail safe).
 */
function substitutedRead(
   entry: FreshnessManifest[string],
   columns: ObservedDrift["columns"],
   read: Extract<ObservedColumns, { columns: unknown }>,
   declared: DriftDeclaration | undefined,
):
   | { entry: FreshnessManifest[string]; columns: ObservedDrift["columns"] }
   | undefined {
   const connectionName = entry.connectionName ?? declared?.connectionName;
   if (!connectionName) return undefined;
   const leftOut: string[] = [];
   const decided = columns.map((c) => {
      const lost =
         c.leftOut ||
         c.storedAsDouble ||
         roundsBigInteger(c) ||
         !c.observedType;
      if (lost) leftOut.push(c.column);
      // Decided on the type as the build's capture spells it, whatever the
      // report names (a warehouse's own spelling, when the build captured
      // only those).
      const spelled = columnNamed(read.columns, c.column)?.type;
      const how = lost
         ? ({ read: "left_out" } as const)
         : driftedColumnRead(
              { ...c, ...(spelled ? { observedType: spelled } : {}) },
              driftDeclaredType(
                 declared?.columns,
                 c.column,
                 storedMalloyType(c.builtType) ?? c.builtType,
              ),
              declared?.queryTimezone
                 ? { queryTimezone: declared.queryTimezone }
                 : {},
           );
      // Read as is only when no value can change: a lossless widening
      // (INTEGER to BIGINT, DECIMAL(10,1) to DECIMAL(12,2), FLOAT to DOUBLE).
      // Any other move inside a type family (DECIMAL(10,1) to DECIMAL(10,0),
      // a BIGINT past 2^53 to DOUBLE) rounds what the table holds, so the
      // column is left out, never served as live's value.
      const narrowed =
         how.read === "as_is" &&
         !losslessWidening(
            nativeTypeOf(entry.nativeSchema, c.column) ?? c.builtType,
            columnNamed(read.columns, c.column)?.nativeType ??
               spelled ??
               c.observedType ??
               "",
         );
      if (narrowed) leftOut.push(c.column);
      return how.read === "left_out" || narrowed
         ? { ...c, leftOut: true as const }
         : c;
   });
   const out = new Set(leftOut.map((c) => c.toLowerCase()));
   const moved = new Set(columns.map((c) => c.column.toLowerCase()));
   const held = read.columns.flatMap((c) =>
      c.type !== undefined && !out.has(c.name.toLowerCase())
         ? [
              {
                 name: c.name,
                 type: c.type,
                 ...(c.nativeType !== undefined
                    ? { nativeType: c.nativeType }
                    : {}),
                 ...(moved.has(c.name.toLowerCase())
                    ? { moved: true as const }
                    : {}),
              },
           ]
         : [],
   );
   return {
      entry: {
         ...entry,
         heldRead: { connectionName, columns: held },
         ...(leftOut.length > 0 ? { leftOutColumns: leftOut } : {}),
      },
      columns: decided,
   };
}

/** A stored type, parsed as far as {@link losslessWidening} compares one. */
type ParsedType =
   | { kind: "int"; rank: number; unsigned: boolean }
   | { kind: "decimal"; precision: number; scale: number }
   | { kind: "float"; rank: number }
   | { kind: "text"; length: number }
   /** Fixed-length, blank-padded text (`character(n)`, `bpchar`). */
   | { kind: "char"; length: number }
   | { kind: "other"; spelling: string };

/** Each integer's width, by its spellings, with the decimal digits it holds. */
const INTEGER_WIDTHS: Record<string, { rank: number; unsigned: boolean }> = {
   tinyint: { rank: 1, unsigned: false },
   int1: { rank: 1, unsigned: false },
   smallint: { rank: 2, unsigned: false },
   int2: { rank: 2, unsigned: false },
   short: { rank: 2, unsigned: false },
   integer: { rank: 3, unsigned: false },
   int: { rank: 3, unsigned: false },
   int4: { rank: 3, unsigned: false },
   signed: { rank: 3, unsigned: false },
   bigint: { rank: 4, unsigned: false },
   int8: { rank: 4, unsigned: false },
   int64: { rank: 4, unsigned: false },
   long: { rank: 4, unsigned: false },
   hugeint: { rank: 5, unsigned: false },
   int128: { rank: 5, unsigned: false },
   utinyint: { rank: 1, unsigned: true },
   usmallint: { rank: 2, unsigned: true },
   uinteger: { rank: 3, unsigned: true },
   ubigint: { rank: 4, unsigned: true },
   uhugeint: { rank: 5, unsigned: true },
};

/** The decimal digits each integer width holds in full. */
const INTEGER_DIGITS = [0, 3, 5, 10, 19, 39];

function parsedType(type: string): ParsedType {
   const t = type.trim().replace(/\s+/g, " ").toLowerCase();
   const width = INTEGER_WIDTHS[t];
   if (width) return { kind: "int", ...width };
   const decimal =
      /^(decimal|numeric|number|dec)\s*(?:\((\d+)\s*(?:,\s*(\d+))?\))?$/.exec(
         t,
      );
   if (decimal) {
      if (decimal[2] === undefined) {
         // Unconstrained: Postgres's `numeric` holds any value exactly;
         // Snowflake's `NUMBER` is NUMBER(38,0).
         return decimal[1] === "number"
            ? { kind: "decimal", precision: 38, scale: 0 }
            : { kind: "decimal", precision: Infinity, scale: Infinity };
      }
      return {
         kind: "decimal",
         precision: Number(decimal[2]),
         scale: Number(decimal[3] ?? 0),
      };
   }
   if (t === "bignumeric") return { kind: "decimal", precision: 76, scale: 38 };
   if (/^(real|float4|float)$/.test(t)) return { kind: "float", rank: 1 };
   if (/^(double|double precision|float8|float64)$/.test(t)) {
      return { kind: "float", rank: 2 };
   }
   // Fixed-length text pads with blanks, so a move to or from it adds or
   // strips them ('ab' against 'ab '): a kind apart from varying text.
   const fixed = /^(bpchar|char|character)\s*(?:\((\d+)\))?$/.exec(t);
   if (fixed) {
      return {
         kind: "char",
         length: fixed[2] !== undefined ? Number(fixed[2]) : Infinity,
      };
   }
   const text =
      /^(varchar|text|string|character varying|char varying|nvarchar)\s*(?:\((\d+)\))?$/.exec(
         t,
      );
   if (text) {
      return {
         kind: "text",
         length: text[2] !== undefined ? Number(text[2]) : Infinity,
      };
   }
   return { kind: "other", spelling: t };
}

/**
 * Whether a column moved from `built` to `now` holds every value it held
 * exactly as before: the same type, or a widening that loses nothing (a
 * wider integer, a decimal with no fewer integer or fraction digits, an
 * integer into a decimal or float that holds all its digits, FLOAT to DOUBLE,
 * a longer varying text). A move between fixed-length text (`character(n)`)
 * and varying text, or between two fixed lengths, adds or strips padding, so
 * it is never one. Anything else inside a family may have rounded or cut a
 * value, so it is never read as is.
 */
function losslessWidening(built: string, now: string): boolean {
   const from = parsedType(built);
   const to = parsedType(now);
   if (from.kind === "int") {
      if (to.kind === "int") {
         return from.unsigned
            ? to.rank > from.rank || (to.unsigned && to.rank === from.rank)
            : !to.unsigned && to.rank >= from.rank;
      }
      if (to.kind === "decimal") {
         return to.precision - to.scale >= INTEGER_DIGITS[from.rank];
      }
      // A DOUBLE holds every 32-bit integer exactly; a FLOAT every 16-bit one.
      if (to.kind === "float") return from.rank <= to.rank + 1;
      return false;
   }
   if (from.kind === "decimal") {
      return (
         to.kind === "decimal" &&
         to.scale >= from.scale &&
         to.precision - to.scale >= from.precision - from.scale
      );
   }
   if (from.kind === "float") {
      return to.kind === "float" && to.rank >= from.rank;
   }
   if (from.kind === "text") {
      return to.kind === "text" && to.length >= from.length;
   }
   // Another length pads differently; varying text holds no padding.
   if (from.kind === "char") {
      return to.kind === "char" && to.length === from.length;
   }
   return to.kind === "other" && to.spelling === from.spelling;
}

/**
 * A 64- or 128-bit integer now held as a float: its digits past 2^53 are
 * already lost, so no read gives live's exact integer (the stored-column read
 * plan's refusal of a big integer stored as a float).
 */
function roundsBigInteger(c: ObservedDrift["columns"][number]): boolean {
   return (
      /^(BIGINT|HUGEINT|UBIGINT|UHUGEINT|INT8|LONG)$/i.test(
         c.builtType.trim(),
      ) &&
      /^(DOUBLE|FLOAT|REAL|FLOAT4|FLOAT8)$/i.test((c.observedType ?? "").trim())
   );
}

/**
 * The built native columns whose native type moved, or that are gone, each
 * left out as {@link columnDrift} leaves one out: gone, now a type Malloy
 * reads as no basic type, or a timestamp that gained or lost its zone off
 * DuckDB.
 */
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
         ...(now?.storedAsDouble ? { storedAsDouble: true as const } : {}),
      };
      if (!now)
         return [{ ...report, gone: true as const, leftOut: true as const }];
      if (now.storedAsDouble) return [report];
      if (
         now.nativeType === undefined ||
         normalizedType(now.nativeType) === normalizedType(c.type)
      ) {
         return [];
      }
      const zoneMoved =
         isNativeTimestamp(c.type) &&
         isNativeTimestamp(now.nativeType) &&
         isNativeZoned(c.type) !== isNativeZoned(now.nativeType);
      if (zoneMoved && capabilitiesOf(read.dialect).utcSessions) return [];
      return now.type === undefined || zoneMoved
         ? [{ ...report, leftOut: true as const }]
         : [report];
   });
}

/** A timestamp, as a warehouse's catalog names it. */
function isNativeTimestamp(type: string): boolean {
   return /^(timestamp|datetime)/i.test(type.trim());
}

/** A zoned timestamp, as a warehouse's catalog names it. */
function isNativeZoned(type: string): boolean {
   return /with time zone|^timestamptz|^timestamp_tz|^timestamp_ltz/i.test(
      type.trim(),
   );
}

/**
 * Whether a column is as its build wrote it: the warehouse's native type for
 * it now is the one the build captured. A DuckLake 128-bit integer held as a
 * DOUBLE is never as built: its read is guarded whatever its build wrote.
 */
function asBuilt(
   builtNative: string | undefined,
   name: string,
   read: Extract<ObservedColumns, { columns: unknown }>,
): boolean {
   const now = columnNamed(read.columns, name);
   return (
      builtNative !== undefined &&
      now?.nativeType !== undefined &&
      !now.storedAsDouble &&
      normalizedType(now.nativeType) === normalizedType(builtNative)
   );
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
 * - a 128-bit integer in a DuckLake table (`storedAsDouble`): `retype`, as
 *   the DOUBLE its Parquet files hold;
 * - the same Malloy type with another native type: `retype` (read as it
 *   is now) only when the move loses nothing (`losslessWidening`: a
 *   `bigint` altered to `numeric(22,2)`), else `drop` (a `bigint` altered
 *   to `numeric(10,2)`, DECIMAL(10,1) to DECIMAL(10,0));
 * - another Malloy type: `retype`, read through the one stored-column read
 *   plan with the type it holds now (a faithful cast, or left out);
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
   if (!now) {
      return {
         kind: "drop",
         report: { ...report, gone: true, leftOut: true },
      };
   }
   const dropped = { ...report, leftOut: true as const };
   const nativeMoved =
      builtNative !== undefined &&
      now.nativeType !== undefined &&
      normalizedType(builtNative) !== normalizedType(now.nativeType);
   if (now.type === undefined) {
      return nativeMoved ? { kind: "drop", report: dropped } : { kind: "none" };
   }
   if (
      typeFamily(now.type) === "timestamp" &&
      typeFamily(built.type) === "timestamp" &&
      isZoned(now.type) !== isZoned(built.type)
   ) {
      return capabilitiesOf(read.dialect).utcSessions
         ? { kind: "none" }
         : { kind: "drop", report: dropped };
   }
   // A DuckLake table's 128-bit integer holds a DOUBLE's digits, whatever its
   // build wrote: read as the DOUBLE it is, never as exact.
   if (now.storedAsDouble) {
      return {
         kind: "retype",
         type: now.type,
         ...(now.nativeType !== undefined
            ? { nativeType: now.nativeType }
            : {}),
         report: { ...report, storedAsDouble: true },
      };
   }
   // A move inside one Malloy type is read as it is only when it loses
   // nothing (a lossless widening); any other (DECIMAL(10,1) to
   // DECIMAL(10,0), a big integer into a decimal too narrow for its digits)
   // may have rounded what the table holds, so the column is left out. A
   // move across types goes to the read plan, which casts it faithfully or
   // leaves it out.
   // The spelled types decide only with no native capture to compare: a
   // binding's declared type may stand in for a captured one, and a column
   // whose native type is the one its build captured has not moved.
   const spelledMoved =
      builtNative === undefined &&
      normalizedType(now.type) !== normalizedType(built.type);
   if (
      typeFamily(now.type) === typeFamily(built.type) &&
      (nativeMoved || spelledMoved) &&
      !losslessWidening(
         nativeMoved ? builtNative! : built.type,
         nativeMoved ? now.nativeType! : now.type,
      )
   ) {
      return { kind: "drop", report: dropped };
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
