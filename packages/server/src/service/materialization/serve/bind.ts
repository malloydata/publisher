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
   compatible,
   declaredMoves as contractDeclaredMoves,
   type IncompatibleCode,
   servableContract,
} from "../domain/column_contract";
import {
   spelledNativeType,
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
   primaryKeyColumnsOf,
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
           /** The eligible sourceIDs admitted past their own gate. */
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
   // The table, not the name, is what own-gate keeping admits: every name a
   // gate-keeping table is bound under is marked, since every one reads that
   // one table (an extension that shares its base's table re-roots the base).
   // So any sharer of a gate-kept source's content address counts as
   // gate-keeping, an ungated sharer included: a gated entry point that joins
   // an ungated sharer is served from the table because a gated sibling stores
   // into it. Nothing leaks: the SQL, and so the rows, are identical, and
   // `govern()` grafts and proves the entry point's own gate on the answer as
   // on live. A `storage=` rollup plan admitted past its base's gates is marked
   // too: its variant grafts and proves them (rollup_variants.ts). A view is
   // never marked (its leg passes a gate only with the switch).
   const bindings = admittedBindings.map((binding) =>
      keptTables.has(binding.virtualHandle) &&
      (binding.kind === "persist" ||
         binding.kind === "materialize" ||
         binding.kind === "rollup")
         ? { ...binding, ownGateKept: true }
         : binding,
   );
   // A table that holds only ungated rows is read past a gated entry point's
   // gate as a database table is (the ungated-rows rule): every source stored
   // at its address, by the plan, not only those bound, is proven ungated. A
   // rollup too (its base ungated): the rollup leg's join branch reads one past
   // a gated query source in a storage destination. A persisted view's or
   // query's leg decides a gate on its own, so neither is marked.
   const ungatedTables = ungatedTableAddresses(
      eligibility?.ungated,
      plan?.sources ?? {},
   );
   const marked = bindings.map((binding) =>
      ungatedTables.has(binding.virtualHandle) &&
      (binding.kind === "persist" ||
         binding.kind === "materialize" ||
         binding.kind === "rollup") &&
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
 * row-level gate under own-gate keeping: the address of every `gateKept`
 * source. The one rule both the bind (which marks each binding of such a table
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
         ? plannedWithCaptured(
              plan.storedColumns,
              entry.schema?.length
                 ? entry.schema
                 : capturedOfNative(entry.nativeSchema, source.dialect),
           )
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
         // The key checks the table's contract records (a pair table's leaf
         // key), as a bind read them off it; internal, never on the wire.
         ...(primaryKeysOf(entry) ? { primaryKeys: primaryKeysOf(entry) } : {}),
         ...(primaryKeyColumnsOf(entry)
            ? { primaryKeyColumns: primaryKeyColumnsOf(entry) }
            : {}),
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
 * A table's columns from the warehouse types its build captured, spelled as
 * a capture spells them (`spelledNativeType`): for a table whose build could
 * not read its columns back (Postgres's schema reader refuses a name with no
 * schema, which is how a build names its tables). Undefined with none to
 * read.
 */
function capturedOfNative(
   nativeSchema: readonly { name: string; type: string }[] | undefined,
   dialect: string | undefined,
): StoredColumn[] | undefined {
   if (!nativeSchema?.length || !dialect) return undefined;
   try {
      return nativeSchema.flatMap((c) => {
         const type = spelledNativeType(c.type, dialect);
         return type ? [{ name: c.name, type }] : [];
      });
   } catch {
      // A dialect Malloy does not know: no spelling to read them with.
      return undefined;
   }
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

/**
 * One bound table that no longer matches its column contract, which the bind
 * left out (it answers live): the generation it judged (`tableName`, and the
 * `builtAt` it compared against), what changed, and which columns.
 */
export interface IncompatibleTable {
   sourceEntityId: string;
   tableName: string;
   builtAt?: string;
   code: IncompatibleCode;
   columns: string[];
}

/**
 * The database bindings and entries held to the columns the bind read from
 * each table's warehouse ({@link ObservedColumns}), judged against each
 * table's column contract, the one comparator:
 *
 * - a table with no contract, or one listing no column, is left out
 *   (`contractMissing`);
 * - a table whose read failed, or that was not read, is left out (`unread`);
 * - a table that no longer matches its contract, or whose source now declares
 *   another type than it was built for, is left out (`incompatible`).
 *
 * A table left out answers live until a build rebuilds it: never cast or
 * narrowed. A kept table's columns are declared as the types it holds, and a
 * column its engine holds as a DOUBLE (a DuckLake 128-bit integer) is left out
 * of every read. With no observation (`observed` undefined) everything with a
 * contract is returned as is.
 */
export function applyObservedColumns(args: {
   entries: FreshnessManifest;
   bindings: readonly Binding[];
   observed: ReadonlyMap<string, ObservedColumns> | undefined;
   /**
    * The columns a bound entry's build source declares now, by name, and its
    * connection. Without it, no declared move is judged.
    */
   declared?: (sourceEntityId: string) => DriftDeclaration | undefined;
}): {
   entries: FreshnessManifest;
   bindings: Binding[];
   unread: UnreadTable[];
   /** The tables left out because their entry carries no column contract. */
   contractMissing: string[];
   /** The tables left out because they no longer match their contract. */
   incompatible: IncompatibleTable[];
} {
   const { observed } = args;
   if (!observed) return asBound(args.entries, args.bindings);
   const entries: FreshnessManifest = {};
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
   /** Each bound table's columns its engine holds lossily, by address. */
   const lossy = new Map<string, string[]>();
   const contractMissing: string[] = [];
   const incompatible: IncompatibleTable[] = [];
   for (const [id, entry] of Object.entries(args.entries)) {
      // A table with no contract, or one listing no column, is never served
      // from (contract_missing): it answers live, and the next build
      // rebuilds it in full.
      const contract = servableContract(entry.contract);
      if (!contract) {
         contractMissing.push(id);
         left.add(id);
         continue;
      }
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
      // The one comparator: the table as read now against its contract.
      // A table that no longer matches is left out, never cast or narrowed.
      const verdict = compatible(contract, read);
      const tableOf = {
         sourceEntityId: id,
         tableName: entry.tableName,
         ...(entry.builtAt ? { builtAt: entry.builtAt } : {}),
      };
      if (!verdict.ok) {
         incompatible.push({
            ...tableOf,
            code: verdict.code,
            columns: verdict.columns,
         });
         left.add(id);
         continue;
      }
      // A table whose address cannot see its source retyped (a `.sql()`
      // source's, or a source reading one): its source now declaring a type
      // the table's value does not read as is drift as surely as an altered
      // column (the bind's declared-versus-stored check).
      const declaration = args.declared?.(id);
      const moved = declaration
         ? contractDeclaredMoves(
              contract,
              declaration.columns.flatMap((c) =>
                 c.name ? [{ name: c.name, type: c.type }] : [],
              ),
           )
         : [];
      if (moved.length > 0) {
         incompatible.push({
            ...tableOf,
            code: "declared_moved",
            columns: moved,
         });
         left.add(id);
         continue;
      }
      reads.set(id, read);
      // A column its engine holds as a DOUBLE whatever it was built as (a
      // DuckLake 128-bit integer): a fact of what the build wrote, not a
      // drift, so the table serves, but never that column, which no read
      // gets exact.
      const asDouble = read.columns
         .filter((c) => c.storedAsDouble)
         .map((c) => c.name);
      if (asDouble.length > 0) {
         lossy.set(id, asDouble);
         if (!served.has(id)) {
            const connectionName =
               entry.connectionName ?? declaration?.connectionName;
            if (!connectionName) {
               left.add(id);
               continue;
            }
            entries[id] = {
               ...entry,
               heldRead: {
                  connectionName,
                  columns: read.columns.flatMap((c) =>
                     c.type !== undefined && !c.storedAsDouble
                        ? [
                             {
                                name: c.name,
                                type: c.type,
                                ...(c.nativeType !== undefined
                                   ? { nativeType: c.nativeType }
                                   : {}),
                             },
                          ]
                        : [],
                  ),
               },
               leftOutColumns: asDouble,
            };
            continue;
         }
      }
      entries[id] = entry;
   }
   const bindings = args.bindings.flatMap((b): Binding[] => {
      if (b.tier !== "database") return [b];
      if (left.has(b.virtualHandle)) return [];
      // Each column declared as the type its table holds, as its contract
      // recorded it (a date truncation a warehouse stores as a timestamp):
      // a fact of what the build wrote, which the served copy reads its
      // columns by, never a drift.
      b = asStored(b, reads.get(b.virtualHandle));
      const doubles = lossy.get(b.virtualHandle);
      if (!doubles) return [b];
      // Declared as the DOUBLE it holds, so the served copy's read plan
      // leaves it out and a gate over it is never read as exact.
      const named = new Set(doubles.map((n) => n.toLowerCase()));
      return [
         {
            ...b,
            schema: b.schema.map((c) =>
               named.has(c.name.toLowerCase()) ? { ...c, type: "DOUBLE" } : c,
            ),
            ...(b.nativeSchema
               ? {
                    nativeSchema: b.nativeSchema.map((n) =>
                       named.has(n.name.toLowerCase())
                          ? { ...n, type: "DOUBLE" }
                          : n,
                    ),
                 }
               : {}),
            retypedColumns: doubles.map((name) => ({ name, type: "DOUBLE" })),
         },
      ];
   });
   return {
      entries,
      bindings,
      unread,
      contractMissing,
      incompatible,
   };
}

/**
 * The `storage=` bindings held to their tables' column contracts, as
 * {@link applyObservedColumns} holds a database table: a binding whose entry
 * carries no servable contract (none, or one that lists no column) is left
 * out (`contractMissing`), and, with an
 * observation, one whose table did not read is left out unread, and one that
 * no longer matches its contract is left out incompatible. Never cast or
 * narrowed. With no observation (a spec's bind), a binding with a contract
 * is kept as it is.
 */
export function holdStorageToContracts(args: {
   bindings: readonly Binding[];
   entries: Readonly<Record<string, ManifestEntry>>;
   observed: ReadonlyMap<string, ObservedColumns> | undefined;
}): {
   bindings: Binding[];
   unread: UnreadTable[];
   contractMissing: string[];
   incompatible: IncompatibleTable[];
} {
   const unread: UnreadTable[] = [];
   const contractMissing: string[] = [];
   const incompatible: IncompatibleTable[] = [];
   const left = new Set<string>();
   const judged = new Set<string>();
   for (const b of args.bindings) {
      const id = b.virtualHandle;
      if (judged.has(id)) continue;
      judged.add(id);
      const entry = args.entries[id];
      const contract = servableContract(entry?.contract);
      if (!contract) {
         contractMissing.push(id);
         left.add(id);
         continue;
      }
      if (!args.observed) continue;
      const read: ObservedColumns = args.observed.get(id) ?? {
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
      const verdict = compatible(contract, read);
      if (!verdict.ok) {
         incompatible.push({
            sourceEntityId: id,
            tableName: entry.physicalTableName,
            ...(entry.builtAt ? { builtAt: entry.builtAt } : {}),
            code: verdict.code,
            columns: verdict.columns,
         });
         left.add(id);
      }
   }
   return {
      bindings: args.bindings.filter((b) => !left.has(b.virtualHandle)),
      unread,
      contractMissing,
      incompatible,
   };
}

/**
 * `b` with each column declared as the type its table holds (`read`, the
 * bind's read, which matched the table's contract), where that reads as
 * another Malloy type than the binding declares: the stored type the served
 * copy's read plan casts from. A column of no basic type keeps its own.
 */
function asStored(
   b: Binding,
   read: Extract<ObservedColumns, { columns: unknown }> | undefined,
): Binding {
   if (!read) return b;
   const retyped: { name: string; type: string }[] = [];
   const schema = b.schema.map((c) => {
      const held = read.columns.find(
         (r) => r.name.toLowerCase() === c.name.toLowerCase(),
      );
      if (!held?.type || held.type === c.type) return c;
      const family = (t: string) =>
         storedMalloyType(t) ?? t.trim().toUpperCase();
      if (family(held.type) === family(c.type)) return c;
      retyped.push({ name: c.name, type: held.nativeType ?? held.type });
      return { ...c, type: held.type };
   });
   return retyped.length === 0 ? b : { ...b, schema, retypedColumns: retyped };
}

/** With no observation, everything with a contract as bound: nothing read. */
function asBound(
   entries: FreshnessManifest,
   bindings: readonly Binding[],
): ReturnType<typeof applyObservedColumns> {
   const contractMissing = Object.entries(entries)
      .filter(([, e]) => !servableContract(e.contract))
      .map(([id]) => id);
   const missing = new Set(contractMissing);
   return {
      entries: Object.fromEntries(
         Object.entries(entries).filter(([id]) => !missing.has(id)),
      ),
      bindings: bindings.filter(
         (b) => b.tier !== "database" || !missing.has(b.virtualHandle),
      ),
      unread: [],
      contractMissing,
      incompatible: [],
   };
}

/** What a bound table's source declares now. */
export interface DriftDeclaration {
   /** The build source's declared columns, with their Malloy types. */
   columns: readonly { name?: string; type?: string }[];
   /** The connection its table is on, when its entry does not name it. */
   connectionName?: string;
}
