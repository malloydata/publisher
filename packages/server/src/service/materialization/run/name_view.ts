// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type {
   Connection as MalloyConnection,
   PersistSource,
} from "@malloydata/malloy";
import { getMaterializeEnabled } from "../../../config";
import { BadRequestError } from "../../../errors";
import { logger } from "../../../logger";
import { errMessage } from "../../../utils";
import type { Materialization } from "../../../storage/DatabaseInterface";
import type { ApiConnection } from "../../model";
import { quoteTablePath } from "../../quoting";
import {
   SUPERSEDED_TABLES_KEY,
   tableIdentity,
   tableNamesIn,
} from "../../versions/materialization_scope";
import type { CompiledBuildPlan } from "../plan/build_plan";
import {
   isLegacyFailedEntry,
   type BuildInstruction,
   type ManifestEntry,
} from "../domain/types";
import {
   STORAGE_TARGET_DIALECT,
   withStorageDestinationSession,
} from "./materialization_build_session";
import {
   nameViewComment,
   nameViewDialectSupported,
   nameViewDropSql,
   nameViewProbeSql,
   nameViewReplaceDdl,
   nameViewReplaceTableDdl,
   renameTableSql,
   nameViewSlotOf,
   NameViewUnsupportedError,
   pathSegments,
   sqlString,
   type NameViewMark,
   type NameViewSlot,
   type NameViewTableReplaceDdl,
} from "./name_view_sql";
import { backupSuffix } from "./materialization_run_support";
import {
   destinationSchemaOf,
   storageTablePath,
} from "../domain/storage_table_path";

/**
 * The `name=` view (materialization v2, phase 3, table behaviours): for a
 * `#(materialize)` source whose table a run names by its generation (an
 * orchestrated host's content-addressed name, or a `scope: version` run's
 * versioned name), a view at the declared `name=` reading that table, so a
 * reader outside Malloy has one stable name for the current generation.
 *
 * Display only, never a key: declarations are keyed by sourceID and stored
 * tables by (sourceEntityId, destination). The view is recorded on the run
 * that pointed it (run metadata, {@link NAME_VIEWS_KEY}) so the dropper and
 * the reclaimers can find it, and recognized in the warehouse by its comment
 * mark (name_view_sql.ts), which names the package that made it and the table
 * it reads, never by its name alone.
 *
 * Lifecycle:
 * - before a source builds, its `name=` is probed: a name held by anything
 *   Publisher did not mark, or by another package's view, fails that source's
 *   build (nothing is written). One table is the exception: a table this
 *   package's own committed run wrote at `name=` (a self-instructed build
 *   names its table `name=` itself), as its run records show
 *   ({@link ownNameTableIn}). The build goes ahead, and the view replaces
 *   that table once the new generation is built
 *   ({@link replaceOwnTableWithView});
 * - once every source of the run has built, each view is created or
 *   repointed, view and mark in one statement or transaction, the mark read
 *   back, and the outcome recorded on the run; a run whose commit then fails
 *   puts each view back as it found it ({@link restoreNameViews});
 * - when a table is dropped (a deleted run's tables, a reclaimed version's),
 *   a view that still reads it is repointed to the newest other committed
 *   run's table for the same view, or dropped with its last generation. A
 *   view that reads another table, or that is not Publisher's, is left alone.
 * - the other way round, a build that names its table `name=` itself finds
 *   this package's own view there: its table replaces the view, in the same
 *   transaction as the build's swap where the dialect allows
 *   ({@link viewToTableSwap}); a table a view replaced is no longer there to
 *   drop, and the dropper retires its entry ({@link heldByOwnNameView}).
 *
 * Only `#(materialize)`, behind `PUBLISHER_MATERIALIZE`. A v1 `#@ persist
 * name=` makes no view.
 */

/** Where a run's metadata records the `name=` views it pointed. */
export const NAME_VIEWS_KEY = "nameViews";

/** One source's `name=` view: what it is called, and the table it reads. */
export interface NameViewIntent {
   sourceEntityId: string;
   sourceName: string;
   /** The package that makes the view, as its mark names it (nameViewOwner). */
   owner: string;
   /**
    * The view, unquoted: `name=` as written when it names a container, else
    * in the container of the table it reads.
    */
   viewName: string;
   /** The table the view reads, as the manifest records it. */
   physicalTableName: string;
   /** The source's connection: where a colocated view lives. */
   connectionName: string;
   /** The destination a stored table, and so its view, lives in. */
   storageDestinationName?: string;
}

/**
 * What became of one view, as the run records it. `outcome` is an open
 * string: `created`, `repointed`, `unchanged`, `replaced_own_table` (the
 * view took the place of a table this package's own run wrote at the name),
 * `replaced_by_table` (this run's table took the place of this package's
 * view; `physicalTableName` is that table, `previousTarget` what the view
 * read), `name_taken`, `unsupported`, `conflict` or `failed`. A run whose
 * commit fails records, for each table it had replaced with a view, whether
 * the table was put back: `table_restored` or `restore_failed`.
 */
export interface NameViewRecord extends NameViewIntent {
   outcome: string;
   message?: string;
   /** On `repointed`: the table the view read before. */
   previousTarget?: string;
   /**
    * On `replaced_own_table`: where the replaced table was kept aside until
    * the run committed (the old entry's backup name, see
    * {@link replaceOwnTableWithView}).
    */
   asideTableName?: string;
}

/** The outcomes after which the view reads the record's table. */
const POINTED_OUTCOMES: ReadonlySet<string> = new Set([
   "created",
   "repointed",
   "unchanged",
   "replaced_own_table",
]);

/** A `name=` held by something Publisher did not make: the build is refused. */
export class NameViewTakenError extends BadRequestError {}

/**
 * The view's name: `name` as written when it names a container (`a.b`), else
 * `name` in the container of the table it reads, so a host that routes tables
 * into a container gets the view beside them.
 */
export function nameViewName(name: string, physicalTableName: string): string {
   if (name.includes(".")) return name;
   const dot = physicalTableName.lastIndexOf(".");
   return dot >= 0 ? `${physicalTableName.slice(0, dot + 1)}${name}` : name;
}

/**
 * The `name=` view a source's build calls for, or undefined: only a
 * `#(materialize)` source that declares `name=`, only with
 * `PUBLISHER_MATERIALIZE` on, and only when its table is not already called
 * `name=` (a self-instructed, unversioned build names the table itself).
 */
export function nameViewIntentFor(
   compiled: Pick<CompiledBuildPlan, "materializePlans">,
   persistSource: Pick<PersistSource, "sourceID" | "name" | "connectionName">,
   instruction: Pick<BuildInstruction, "physicalTableName" | "destination">,
   sourceEntityId: string,
   owner: string,
): NameViewIntent | undefined {
   if (!getMaterializeEnabled()) return undefined;
   const name = declaredName(compiled, persistSource.sourceID);
   if (!name) return undefined;
   const viewName = nameViewName(name, instruction.physicalTableName);
   if (viewName === instruction.physicalTableName) return undefined;
   return {
      sourceEntityId,
      sourceName: persistSource.name,
      owner,
      viewName,
      physicalTableName: instruction.physicalTableName,
      connectionName: persistSource.connectionName,
      ...(instruction.destination
         ? { storageDestinationName: instruction.destination }
         : {}),
   };
}

/**
 * The `name=` a `#(materialize)` declaration states, or undefined. A table
 * that only defaults to its source's name gets no view: that name is
 * Publisher's, not the author's, so no outside reader was promised it, and
 * the table itself is named for its package or version (packageTableSuffix,
 * versionTableSuffix), which two packages declaring a source of the same
 * name would otherwise each claim a view at.
 */
function declaredName(
   compiled: Pick<CompiledBuildPlan, "materializePlans">,
   sourceID: string,
): string | undefined {
   const plan = compiled.materializePlans?.[sourceID];
   return plan && !plan.nameDefaulted ? plan.persist.name : undefined;
}

/** The views a run's metadata records, or empty. */
export function nameViewsIn(
   metadata: Record<string, unknown> | null | undefined,
): NameViewRecord[] {
   const recorded = metadata?.[NAME_VIEWS_KEY];
   if (!Array.isArray(recorded)) return [];
   return recorded.filter(
      (r): r is NameViewRecord =>
         typeof r === "object" &&
         r !== null &&
         typeof (r as NameViewRecord).viewName === "string" &&
         typeof (r as NameViewRecord).owner === "string" &&
         typeof (r as NameViewRecord).physicalTableName === "string" &&
         typeof (r as NameViewRecord).connectionName === "string",
   );
}

/** Which view a record is: where it lives, and its name. */
export function nameViewKey(
   view: Pick<
      NameViewIntent,
      "viewName" | "connectionName" | "storageDestinationName"
   >,
): string {
   return JSON.stringify(
      view.storageDestinationName
         ? ["destination", view.storageDestinationName, view.viewName]
         : ["connection", view.connectionName, view.viewName],
   );
}

/**
 * A table at a `name=` that this package's own committed run wrote: the run,
 * and the content address its entry carries (which names the aside copy a
 * non-transactional replacement keeps, `backupSuffix`, so the old run's
 * reclaim drops one a crash left behind).
 */
export interface OwnNameTable {
   materializationId: string;
   sourceEntityId: string;
}

/**
 * Whether this package's own records say the table at `intent`'s `name=` is
 * one it wrote: among the package's committed runs (`runs`, the package's
 * own listing, so another package's runs are never consulted), the newest
 * whose manifest names a table at exactly `intent.viewName`, on the same
 * connection and destination, for the source of the same name. And no
 * committed run made a view at that name after it: then the table there now
 * is not the one the record describes. An entry its run's metadata retires
 * (`SUPERSEDED_TABLES_KEY`) is never read.
 *
 * The records carry the source's name, not its sourceID (whose model URL is
 * a version's own path): within one package, one source writes one `name=`
 * (assertNoTableCollisions refuses two), so the name and the exact table
 * name, connection and destination identify it. Undefined whenever anything
 * differs: that table is not provably this package's, and is refused.
 */
export function ownNameTableIn(
   runs: Pick<
      Materialization,
      "id" | "status" | "manifest" | "metadata" | "completedAt" | "createdAt"
   >[],
   intent: Pick<
      NameViewIntent,
      "viewName" | "sourceName" | "connectionName" | "storageDestinationName"
   >,
): OwnNameTable | undefined {
   const at = (r: (typeof runs)[number]) =>
      (r.completedAt ?? r.createdAt ?? new Date(0)).getTime();
   const key = nameViewKey(intent);
   let written: { at: number; table: OwnNameTable } | undefined;
   let viewMadeAt = -Infinity;
   for (const run of runs) {
      if (run.status !== "MANIFEST_FILE_READY") continue;
      // An entry a later run retired (SUPERSEDED_TABLES_KEY: its table was
      // replaced by a view, or is held by another package) no longer
      // describes the table there.
      const retired = new Set(
         tableNamesIn(run.metadata, SUPERSEDED_TABLES_KEY).map(tableIdentity),
      );
      for (const entry of Object.values(run.manifest?.entries ?? {})) {
         if (
            isLegacyFailedEntry(entry) ||
            retired.has(tableIdentity(entry.physicalTableName)) ||
            entry.physicalTableName !== intent.viewName ||
            entry.sourceName !== intent.sourceName ||
            entry.connectionName !== intent.connectionName ||
            (entry.storageDestinationName ?? undefined) !==
               (intent.storageDestinationName ?? undefined)
         ) {
            continue;
         }
         if (!written || at(run) > written.at) {
            written = {
               at: at(run),
               table: {
                  materializationId: run.id,
                  sourceEntityId: entry.sourceEntityId,
               },
            };
         }
      }
      if (
         nameViewsIn(run.metadata).some(
            (r) => nameViewKey(r) === key && POINTED_OUTCOMES.has(r.outcome),
         )
      ) {
         viewMadeAt = Math.max(viewMadeAt, at(run));
      }
   }
   return written && written.at > viewMadeAt ? written.table : undefined;
}

/** Where a view's SQL runs, in its dialect. */
export interface NameViewSite {
   dialect: string;
   /** The unquoted segments of the view's full path. */
   segments(viewName: string): string[];
   /** The quoted reference the view's SELECT reads `physicalTableName` by. */
   targetSql(physicalTableName: string): string;
   run(sql: string): Promise<Record<string, unknown>[]>;
}

function rowsOf(result: unknown): Record<string, unknown>[] {
   const rows = (result as { rows?: unknown } | null)?.rows;
   return Array.isArray(rows) ? (rows as Record<string, unknown>[]) : [];
}

/** A colocated view's site: the source's own connection. */
export function colocatedNameViewSite(
   connection: Pick<MalloyConnection, "runSQL" | "dialectName">,
   runOptions: Record<string, unknown> = {},
): NameViewSite {
   const dialect = connection.dialectName;
   return {
      dialect,
      segments: pathSegments,
      targetSql: (physical) => quoteTablePath(physical, dialect),
      run: async (sql) => rowsOf(await connection.runSQL(sql, runOptions)),
   };
}

/**
 * A stored table's view site: a session with the destination attached under
 * its own name. The view lives in the destination beside its table and reads
 * it WITHOUT the catalog name, so a reader attaching the destination under
 * another name still resolves it (DuckDB binds a view in its own catalog).
 */
export function storageNameViewSite(
   destinationName: string,
   destinationConnection: ApiConnection,
   session: { runSQL(sql: string): Promise<unknown> },
): NameViewSite {
   const schema = destinationSchemaOf(destinationConnection);
   return {
      dialect: STORAGE_TARGET_DIALECT,
      segments: (viewName) =>
         pathSegments(storageTablePath(destinationName, viewName, schema)),
      targetSql: (physical) =>
         quoteTablePath(
            physical.includes(".") ? physical : `${schema}.${physical}`,
            STORAGE_TARGET_DIALECT,
         ),
      run: async (sql) => rowsOf(await session.runSQL(sql)),
   };
}

/** What a view's SQL needs from the run or the sweep it belongs to. */
export interface NameViewContext {
   /** The source connection a colocated view lives on, or undefined. */
   connectionFor(
      connectionName: string,
   ): Promise<MalloyConnection | undefined> | MalloyConnection | undefined;
   /** A destination's config, or undefined when it is no longer configured. */
   destinationFor(destinationName: string): ApiConnection | undefined;
   environmentPath: string;
   /** Options every colocated statement carries (query metadata). */
   runOptions?(connectionName: string): Record<string, unknown>;
   /**
    * Whether this package's own records say it wrote the table at a view's
    * name ({@link ownNameTableIn}). Absent: no table is ever its own.
    */
   ownTableAt?(
      intent: NameViewIntent,
   ): Promise<OwnNameTable | undefined> | OwnNameTable | undefined;
   /**
    * Whether another run of this package is building now, read fresh: an
    * interrupted replacement is put back only when none is
    * ({@link recoverInterruptedReplacement}), since what looks interrupted
    * may be another run's replacement in flight. Absent: never recovered.
    */
   otherRunActive?(): Promise<boolean> | boolean;
}

/**
 * Run `fn` at the site of the view `view` names: on its connection, or on a
 * session attached to its destination for the call. Undefined when the site
 * cannot be reached (an unloaded connection, a destination no longer
 * configured), which every caller treats as "leave the view alone".
 */
export async function atNameViewSite<T>(
   view: Pick<NameViewIntent, "connectionName" | "storageDestinationName">,
   context: NameViewContext,
   fn: (site: NameViewSite) => Promise<T>,
): Promise<T | undefined> {
   const destinationName = view.storageDestinationName;
   if (destinationName) {
      const destinationConnection = context.destinationFor(destinationName);
      if (!destinationConnection) return undefined;
      return withStorageDestinationSession(
         {
            destinationName,
            destinationConnection,
            environmentPath: context.environmentPath,
         },
         (session) =>
            fn(
               storageNameViewSite(
                  destinationName,
                  destinationConnection,
                  session,
               ),
            ),
      );
   }
   const connection = await context.connectionFor(view.connectionName);
   if (!connection) return undefined;
   return fn(
      colocatedNameViewSite(
         connection,
         context.runOptions?.(view.connectionName),
      ),
   );
}

/** What holds the view's name at `site`. */
export async function probeNameView(
   site: NameViewSite,
   viewName: string,
): Promise<NameViewSlot> {
   return nameViewSlotOf(
      await site.run(nameViewProbeSql(site.segments(viewName), site.dialect)),
   );
}

/** What holds a name, as `owner` sees it: another package's view is taken. */
type OwnedSlot =
   | { state: "empty" }
   | { state: "ours"; target: string }
   | { state: "taken"; holder: string }
   /** A table this package's own run wrote at the name (preflight only). */
   | { state: "own_table"; table: OwnNameTable };

export function ownedSlot(slot: NameViewSlot, owner: string): OwnedSlot {
   switch (slot.state) {
      case "empty":
         return slot;
      case "ours":
         return slot.owner === owner
            ? { state: "ours", target: slot.target }
            : {
                 state: "taken",
                 holder: `a view package '${slot.owner}' made`,
              };
      case "taken":
         return {
            state: "taken",
            holder:
               slot.tableOwner !== undefined && slot.tableOwner !== owner
                  ? `a table package '${slot.tableOwner}' built`
                  : `a ${slot.kind.toLowerCase()} Publisher did not create`,
         };
   }
}

/**
 * Whether `slot` is a table this package's records may claim as its own: a
 * table, and not one another package has marked as its own since
 * (table_ownership.ts). The records say this package wrote a table at the
 * name once; another package's mark says that package has built there
 * since, so the table is no longer this package's to replace.
 */
function claimableTable(slot: NameViewSlot, owner: string): boolean {
   return (
      slot.state === "taken" &&
      slot.kind !== "VIEW" &&
      (slot.tableOwner === undefined || slot.tableOwner === owner)
   );
}

/** Make or repoint the view to read `mark.target`, mark and all. */
export async function pointNameView(
   site: NameViewSite,
   viewName: string,
   mark: NameViewMark,
): Promise<void> {
   const ddl = nameViewReplaceDdl(
      site.segments(viewName),
      site.targetSql(mark.target),
      mark,
      site.dialect,
   );
   try {
      await site.run(ddl.primary);
   } catch (err) {
      if (ddl.rollback) {
         try {
            await site.run(ddl.rollback);
         } catch {
            // Usually no transaction was left open, which is harmless.
         }
      }
      if (!ddl.fallback) throw err;
      await site.run(ddl.fallback);
   }
}

/** Drop the view. Callers drop only a view the probe showed is Publisher's. */
export async function dropNameView(
   site: NameViewSite,
   viewName: string,
): Promise<void> {
   await site.run(nameViewDropSql(site.segments(viewName), site.dialect));
}

/**
 * The name a table this package's own run wrote at `viewName` is kept under
 * while a view replaces it: that entry's own backup name, which its run's
 * reclaim drops with the table (reclaimer.ts drops `<table><backupSuffix>`),
 * so a copy a crash or a failed restore leaves there is never stranded.
 */
function asideTableNameFor(
   viewName: string,
   table: Pick<OwnNameTable, "sourceEntityId">,
): string {
   return `${viewName}${backupSuffix(table.sourceEntityId)}`;
}

/**
 * Put the view `intent` names in place of the table this package's own run
 * wrote at that name (`table`), reading `intent.physicalTableName`, the
 * generation the run just built. Called only once that generation stands.
 *
 * The old table is renamed aside ({@link asideTableNameFor}) and kept there
 * until the run commits: {@link dropReplacedTablesAside} drops it after the
 * commit, and {@link restoreNameViews} puts it back when the commit fails.
 * On DuckDB (a file, or a DuckLake or DuckDB storage destination) and
 * Postgres the rename and the view are one transaction, so a failure leaves
 * the table as it was; elsewhere a view that cannot be made, or whose mark
 * does not read back, puts the table back. Returns the aside name; throws
 * when the view is not in place (the run records `failed` with why).
 */
export async function replaceOwnTableWithView(
   site: NameViewSite,
   intent: NameViewIntent,
   table: OwnNameTable,
   context: Pick<NameViewContext, "otherRunActive"> = {},
): Promise<string> {
   const mark = { target: intent.physicalTableName, owner: intent.owner };
   const aside = asideTableNameFor(intent.viewName, table);
   const ddl = nameViewReplaceTableDdl(
      site.segments(intent.viewName),
      site.segments(aside),
      site.targetSql(intent.physicalTableName),
      mark,
      site.dialect,
   );
   const markReadsBack = async () => {
      const after = ownedSlot(
         await probeNameView(site, intent.viewName),
         intent.owner,
      );
      return (
         after.state === "ours" && after.target === intent.physicalTableName
      );
   };
   // The table is at the name, so a table under its backup name is a stale
   // older copy (a crashed swap's, as swapByRenamingAside reads it), and
   // would make the rename fail. Unless another run of the package is
   // building: then it may be that run's copy in flight (a swap that moved
   // the served table there, or a replacement of its own), the only copy of
   // the table until it finishes. Left alone, and the view is not made.
   if ((await probeNameView(site, aside)).state === "taken") {
      if (!context.otherRunActive || (await context.otherRunActive())) {
         throw new Error(
            `'${aside}', where the table is kept while the view replaces ` +
               `it, holds a table, and another run of this package may be ` +
               `using it`,
         );
      }
      await site.run(ddl.dropAside);
   }
   if (ddl.transactional) {
      try {
         await site.run(ddl.replace);
      } catch (err) {
         if (ddl.rollback) {
            try {
               await site.run(ddl.rollback);
            } catch {
               // Usually no transaction was left open, which is harmless.
            }
         }
         throw err;
      }
   } else {
      await site.run(ddl.moveAside);
      try {
         try {
            await site.run(ddl.create.primary);
         } catch (err) {
            if (!ddl.create.fallback) throw err;
            await site.run(ddl.create.fallback);
         }
      } catch (err) {
         await putTableBack(site, intent, aside, ddl, err);
         throw err;
      }
   }
   if (!(await markReadsBack())) {
      const err = new Error(
         `the view's mark did not read back from '${intent.viewName}'`,
      );
      await putTableBack(site, intent, aside, ddl, err);
      throw err;
   }
   return aside;
}

/**
 * Drop the view at `intent`'s name and rename the aside table back. False,
 * logged, when that fails: the table is then still under its aside name.
 */
async function putTableBack(
   site: NameViewSite,
   intent: Pick<NameViewIntent, "viewName">,
   aside: string,
   ddl: NameViewTableReplaceDdl,
   cause: unknown,
): Promise<boolean> {
   try {
      if (ddl.transactional) {
         try {
            await site.run(ddl.restore);
         } catch (err) {
            if (ddl.rollback) {
               try {
                  await site.run(ddl.rollback);
               } catch {
                  // Usually no transaction was left open.
               }
            }
            throw err;
         }
      } else {
         await site.run(
            nameViewDropSql(site.segments(intent.viewName), site.dialect),
         );
         await site.run(ddl.restore);
      }
   } catch (restoreErr) {
      logger.error(
         "Failed to put back a table a name= view was to replace; it is kept under its aside name",
         {
            viewName: intent.viewName,
            asideName: aside,
            error: errMessage(cause),
            restoreError: errMessage(restoreErr),
         },
      );
      return false;
   }
   return true;
}

/** The DDL a recorded replacement is undone or finished with. */
function replaceDdlFor(
   site: NameViewSite,
   record: Pick<NameViewRecord, "viewName" | "physicalTableName" | "owner">,
   aside: string,
): NameViewTableReplaceDdl {
   return nameViewReplaceTableDdl(
      site.segments(record.viewName),
      site.segments(aside),
      site.targetSql(record.physicalTableName),
      { target: record.physicalTableName, owner: record.owner },
      site.dialect,
   );
}

/**
 * After the run that recorded `records` committed: drop each table a
 * `replaced_own_table` view kept aside. Best-effort, never throws: a copy
 * that stays is dropped with the old entry's table when its run's tables
 * are dropped (it carries that entry's backup name).
 */
export async function dropReplacedTablesAside(
   records: NameViewRecord[],
   context: NameViewContext,
): Promise<void> {
   for (const record of records) {
      const aside = record.asideTableName;
      if (record.outcome !== "replaced_own_table" || !aside) continue;
      try {
         await atNameViewSite(record, context, async (site) => {
            await site.run(replaceDdlFor(site, record, aside).dropAside);
         });
      } catch (err) {
         logger.warn(
            "Failed to drop a table a name= view replaced; its run's reclaim drops it",
            {
               viewName: record.viewName,
               asideName: aside,
               error: errMessage(err),
            },
         );
      }
   }
}

/**
 * Whether `view`'s name is held by this package's own marked view, as the
 * dropper asks when a table's `DROP TABLE` fails: a table a `name=` view
 * replaced ({@link replaceOwnTableWithView}) is gone, and its entry is
 * retired rather than owed. False when the name holds anything else or
 * cannot be probed. Never throws.
 */
export async function heldByOwnNameView(
   view: Pick<
      NameViewIntent,
      "viewName" | "owner" | "connectionName" | "storageDestinationName"
   >,
   context: NameViewContext,
): Promise<boolean> {
   try {
      return (
         (await atNameViewSite(view, context, async (site) => {
            if (!nameViewDialectSupported(site.dialect)) return false;
            const slot = ownedSlot(
               await probeNameView(site, view.viewName),
               view.owner,
            );
            return slot.state === "ours";
         })) ?? false
      );
   } catch {
      return false;
   }
}

/**
 * What a Postgres swap does with a `name=` view over the very table it
 * replaces, which would otherwise refuse the swap's `DROP TABLE` (a Postgres
 * view binds to its table). Each attempt is run as the swap's whole
 * transaction, in order; staging is given back only once the last fails.
 *
 * 1. The view is repointed at the staging table (`CREATE OR REPLACE`), the
 *    old table dropped, and staging renamed into place, which the view
 *    follows. The view is never dropped, so views a reader built over it
 *    survive, and its mark (the physical name) stays true.
 * 2. When the column list changed so `CREATE OR REPLACE` refuses: the view is
 *    dropped and made again, mark and all, after the rename. A reader's own
 *    view over it refuses that drop, and the build fails with both intact.
 *
 * Undefined on every other dialect, whose views do not bind to a table: the
 * swap leaves them reading the name, which the new table takes.
 */
export interface SwapDependents {
   attempts(stagingTableName: string): { before: string[]; after: string[] }[];
   /**
    * For a swap that is not one transaction (it renames the served table
    * aside): `before` runs first, and `restore` (best-effort) when the swap
    * then fails. Only {@link viewToTableSwap} sets it.
    */
   aside?: { before: string[]; restore: string[] };
   /**
    * The name holds this package's own `name=` view, which the build's table
    * replaces: a storage build drops the view in the transaction that
    * creates its table ({@link viewToTableSwap}).
    */
   replacesView?: boolean;
}

export function swapDependentsFor(
   intent: NameViewIntent,
   slot: OwnedSlot,
   dialect: string,
): SwapDependents | undefined {
   if (dialect !== "postgres") return undefined;
   if (slot.state !== "ours" || slot.target !== intent.physicalTableName) {
      return undefined;
   }
   const view = quoteTablePath(intent.viewName, dialect);
   const comment = `COMMENT ON VIEW ${view} IS ${sqlString(
      nameViewComment({
         target: intent.physicalTableName,
         owner: intent.owner,
      }),
      dialect,
   )}`;
   const select = (table: string) =>
      `VIEW ${view} AS SELECT * FROM ${quoteTablePath(table, dialect)}`;
   return {
      attempts: (stagingTableName) => [
         {
            before: [`CREATE OR REPLACE ${select(stagingTableName)}`, comment],
            after: [],
         },
         {
            before: [`DROP VIEW ${view}`],
            after: [`CREATE ${select(intent.physicalTableName)}`, comment],
         },
      ],
   };
}

/**
 * The `name=` a build whose table IS called `name=` (a self-instructed,
 * unversioned build of a `#(materialize)` source) must check for this
 * package's own view, which an earlier build that named its table by
 * generation made there: the same shape as {@link nameViewIntentFor}, with
 * `viewName` and `physicalTableName` both the table. Undefined in every
 * other case, including without `PUBLISHER_MATERIALIZE`.
 */
export function tableAtNameIntentFor(
   compiled: Pick<CompiledBuildPlan, "materializePlans">,
   persistSource: Pick<PersistSource, "sourceID" | "name" | "connectionName">,
   instruction: Pick<BuildInstruction, "physicalTableName" | "destination">,
   sourceEntityId: string,
   owner: string,
): NameViewIntent | undefined {
   if (!getMaterializeEnabled()) return undefined;
   const name = declaredName(compiled, persistSource.sourceID);
   if (!name) return undefined;
   if (
      nameViewName(name, instruction.physicalTableName) !==
      instruction.physicalTableName
   ) {
      return undefined;
   }
   return {
      sourceEntityId,
      sourceName: persistSource.name,
      owner,
      viewName: instruction.physicalTableName,
      physicalTableName: instruction.physicalTableName,
      connectionName: persistSource.connectionName,
      ...(instruction.destination
         ? { storageDestinationName: instruction.destination }
         : {}),
   };
}

/**
 * Before a build whose table is called `name=`: the table this package's own
 * view at that name reads, and the site's dialect, or undefined when the name
 * holds anything else (the build then meets it as it always has: a table is
 * rebuilt, anything else fails the swap) or cannot be probed. Never throws.
 */
export async function ownViewAtTable(
   intent: NameViewIntent,
   context: NameViewContext,
): Promise<{ target: string; dialect: string } | undefined> {
   try {
      return await atNameViewSite(intent, context, async (site) => {
         if (!nameViewDialectSupported(site.dialect)) return undefined;
         const slot = ownedSlot(
            await probeNameView(site, intent.viewName),
            intent.owner,
         );
         return slot.state === "ours"
            ? { target: slot.target, dialect: site.dialect }
            : undefined;
      });
   } catch (err) {
      logger.warn("Could not check a name= table's name before the build", {
         sourceName: intent.sourceName,
         viewName: intent.viewName,
         error: errMessage(err),
      });
      return undefined;
   }
}

/**
 * Before a colocated build whose table is called `name=` (a self-instructed,
 * unversioned build of a `#(materialize)` source with no `storage=`): what
 * holds the name in the source's own warehouse, which also holds the
 * author's tables. The build swaps its table onto the name, dropping
 * whatever table is there, so it goes ahead only onto an empty name, a table
 * this package's own committed run wrote there ({@link ownNameTableIn}), or
 * this package's own view (returned, which the table then replaces, as
 * {@link ownViewAtTable}). Anything else (a table no run of this package
 * wrote, such as the very table the source reads, or a view this package did
 * not mark) throws {@link NameViewTakenError}, so the source fails before
 * anything is written. Undefined, and the build goes ahead, when the name
 * cannot be probed (an unsupported dialect, an unreachable connection, a
 * failed probe), as {@link preflightNameView} does.
 */
export async function preflightColocatedTableAtName(
   intent: NameViewIntent,
   context: NameViewContext,
): Promise<{ target: string; dialect: string } | undefined> {
   let probed:
      | { own?: { target: string; dialect: string }; holder?: string }
      | undefined;
   try {
      probed = await atNameViewSite(intent, context, async (site) => {
         if (!nameViewDialectSupported(site.dialect)) return undefined;
         const raw = await probeNameView(site, intent.viewName);
         if (raw.state === "empty") return {};
         // A table: rebuilt only when this package's own run wrote it, and
         // no other package has marked it as its own since.
         if (raw.state === "taken" && raw.kind !== "VIEW") {
            if (
               raw.tableOwner !== undefined &&
               raw.tableOwner !== intent.owner
            ) {
               return { holder: `a table package '${raw.tableOwner}' built` };
            }
            return (await context.ownTableAt?.(intent))
               ? {}
               : {
                    holder: `a ${raw.kind.toLowerCase()} no run of this package wrote`,
                 };
         }
         const slot = ownedSlot(raw, intent.owner);
         if (slot.state === "ours") {
            return { own: { target: slot.target, dialect: site.dialect } };
         }
         return {
            holder:
               slot.state === "taken"
                  ? slot.holder
                  : "an object Publisher did not create",
         };
      });
   } catch (err) {
      logger.warn("Could not check a name= table's name before the build", {
         sourceName: intent.sourceName,
         viewName: intent.viewName,
         error: errMessage(err),
      });
      return undefined;
   }
   if (probed?.holder) {
      throw new NameViewTakenError(
         `'${intent.sourceName}' is not built: its name= '${intent.viewName}' is ` +
            `held by ${probed.holder}, and a build into the source's own ` +
            `warehouse never replaces one. Drop or rename that object, or give ` +
            `#(materialize) another name=.`,
      );
   }
   return probed?.own;
}

/**
 * How a build replaces this package's own view at its table's name with the
 * table (orchestrated, then self-instructed again). The view is dropped
 * only in the step that puts the table in its place:
 * - a colocated swap on DuckDB or Postgres drops it in the swap's
 *   transaction, so a reader sees the view or the new table, and a failure
 *   (a reader's own view over it, on Postgres) leaves the view;
 * - a colocated swap elsewhere drops it just before, and remakes it, mark
 *   and all, reading `previousTarget`, when the swap fails;
 * - a storage build drops it in the transaction that creates the table.
 * The generation the view read is left to the records that name it.
 */
export function viewToTableSwap(
   intent: NameViewIntent,
   previousTarget: string,
   dialect: string,
): SwapDependents {
   const segments = pathSegments(intent.viewName);
   const drop = nameViewDropSql(segments, dialect);
   return {
      replacesView: true,
      attempts: () => [{ before: [drop], after: [] }],
      aside: {
         before: [drop],
         restore: [
            nameViewReplaceDdl(
               segments,
               quoteTablePath(previousTarget, dialect),
               { target: previousTarget, owner: intent.owner },
               dialect,
            ).primary,
         ],
      },
   };
}

/**
 * A replacement of this package's own table that never committed (the
 * server stopped between renaming the table aside and the run's commit)
 * leaves the name empty, or holding this package's view of a generation no
 * committed run names, and the table under its aside name, while the
 * package's records still say the table is at the name. Put it back: drop
 * that view, rename the table back, provided no other run of the package is
 * building (whose replacement in flight would look the same). Returns the table, which the build then
 * replaces as usual; undefined when the name is in no such state.
 */
async function recoverInterruptedReplacement(
   site: NameViewSite,
   intent: NameViewIntent,
   raw: NameViewSlot,
   context: NameViewContext,
): Promise<OwnNameTable | undefined> {
   const ourView = raw.state === "ours" && raw.owner === intent.owner;
   if (raw.state !== "empty" && !ourView) return undefined;
   const table = await context.ownTableAt?.(intent);
   if (!table) return undefined;
   const aside = asideTableNameFor(intent.viewName, table);
   const asideSlot = await probeNameView(site, aside);
   if (asideSlot.state !== "taken" || asideSlot.kind === "VIEW") {
      return undefined;
   }
   // Another run's replacement in flight looks the same: leave it.
   if (!context.otherRunActive || (await context.otherRunActive())) {
      return undefined;
   }
   const record = {
      viewName: intent.viewName,
      owner: intent.owner,
      physicalTableName: raw.state === "ours" ? raw.target : "",
   };
   if (ourView) {
      if (
         !(await putTableBack(
            site,
            record,
            aside,
            replaceDdlFor(site, record, aside),
            new Error("an interrupted replacement"),
         ))
      ) {
         return undefined;
      }
   } else {
      await site.run(renameBack(site, record, aside));
   }
   logger.warn(
      "Put back a table an interrupted name= view replacement left aside",
      {
         sourceName: intent.sourceName,
         viewName: intent.viewName,
         asideName: aside,
         materializationId: table.materializationId,
      },
   );
   return table;
}

/**
 * Before a source builds: what holds its `name=`. Throws
 * {@link NameViewTakenError} when something Publisher did not mark, or
 * another package's view, holds it, so the source fails before any table is
 * written. Undefined when the name could not be probed (an unsupported
 * dialect, an unreachable site, a failed probe): the build goes ahead and the
 * view step after it reports why.
 */
export async function preflightNameView(
   intent: NameViewIntent,
   context: NameViewContext,
): Promise<{ slot: OwnedSlot; dialect: string } | undefined> {
   let probed: { slot: OwnedSlot; dialect: string } | undefined;
   try {
      probed = await atNameViewSite(intent, context, async (site) => {
         if (!nameViewDialectSupported(site.dialect)) return undefined;
         const raw = await probeNameView(site, intent.viewName);
         // A table this package's own run wrote at the name, and no other
         // package has marked as its own since: the view replaces it once
         // this build is done.
         if (claimableTable(raw, intent.owner)) {
            const table = await context.ownTableAt?.(intent);
            if (table) {
               return {
                  slot: { state: "own_table", table },
                  dialect: site.dialect,
               };
            }
         }
         const recovered = await recoverInterruptedReplacement(
            site,
            intent,
            raw,
            context,
         );
         if (recovered) {
            return {
               slot: { state: "own_table", table: recovered },
               dialect: site.dialect,
            };
         }
         return { slot: ownedSlot(raw, intent.owner), dialect: site.dialect };
      });
   } catch (err) {
      logger.warn("Could not check a name= view's name before the build", {
         sourceName: intent.sourceName,
         viewName: intent.viewName,
         error: errMessage(err),
      });
      return undefined;
   }
   if (probed?.slot.state === "taken") {
      throw new NameViewTakenError(
         `'${intent.sourceName}' is not built: its name= '${intent.viewName}' is ` +
            `held by ${probed.slot.holder}, and the view naming the source's ` +
            `current table never replaces one. Drop or rename that object, or ` +
            `give #(materialize) another name=.`,
      );
   }
   return probed;
}

/**
 * After every source of a run has built: create or repoint each source's
 * view, and record what came of it. Never throws: a view that cannot be made
 * leaves its table built and served, and the record says why.
 *
 * Two sources of one run that ask for the same view for different tables get
 * neither (`conflict`): which one an outside reader would see would depend on
 * build order.
 *
 * `ownTables` holds, by {@link nameViewKey}, the views whose name preflight
 * found held by a table this package's own run wrote: the view replaces
 * that table ({@link replaceOwnTableWithView}) when a table is still there.
 */
export async function publishNameViews(
   intents: NameViewIntent[],
   context: NameViewContext,
   ownTables: ReadonlyMap<string, OwnNameTable> = new Map(),
): Promise<NameViewRecord[]> {
   const byKey = new Map<string, NameViewIntent[]>();
   for (const intent of intents) {
      const key = nameViewKey(intent);
      const same = byKey.get(key);
      if (same) same.push(intent);
      else byKey.set(key, [intent]);
   }
   const records: NameViewRecord[] = [];
   for (const group of byKey.values()) {
      const targets = new Set(group.map((i) => i.physicalTableName));
      if (targets.size > 1) {
         for (const intent of group) {
            records.push({
               ...intent,
               outcome: "conflict",
               message:
                  `sources ${group.map((i) => `'${i.sourceName}'`).join(", ")} ` +
                  `all ask for the view '${intent.viewName}'`,
            });
         }
         continue;
      }
      // A base and its extension share a table: one view, recorded once.
      records.push(
         await publishNameView(
            group[0],
            context,
            ownTables.get(nameViewKey(group[0])),
         ),
      );
   }
   for (const record of records) {
      const failed = !POINTED_OUTCOMES.has(record.outcome);
      logger[failed ? "warn" : "info"](
         failed ? "A name= view was not made" : "Pointed a name= view",
         {
            sourceName: record.sourceName,
            viewName: record.viewName,
            physicalTableName: record.physicalTableName,
            outcome: record.outcome,
            ...(record.message ? { reason: record.message } : {}),
         },
      );
   }
   return records;
}

async function publishNameView(
   intent: NameViewIntent,
   context: NameViewContext,
   ownTable: OwnNameTable | undefined,
): Promise<NameViewRecord> {
   const mark = { target: intent.physicalTableName, owner: intent.owner };
   try {
      const record = await atNameViewSite(
         intent,
         context,
         async (site): Promise<NameViewRecord> => {
            if (!nameViewDialectSupported(site.dialect)) {
               return {
                  ...intent,
                  outcome: "unsupported",
                  message: `a view in dialect '${site.dialect}' cannot carry Publisher's mark`,
               };
            }
            const raw = await probeNameView(site, intent.viewName);
            if (ownTable && claimableTable(raw, intent.owner)) {
               const asideTableName = await replaceOwnTableWithView(
                  site,
                  intent,
                  ownTable,
                  context,
               );
               return {
                  ...intent,
                  outcome: "replaced_own_table",
                  asideTableName,
                  message:
                     `the view replaced the table '${intent.viewName}' that ` +
                     `materialization ${ownTable.materializationId} of this ` +
                     `package wrote`,
               };
            }
            const slot = ownedSlot(raw, intent.owner);
            if (slot.state === "taken") {
               return {
                  ...intent,
                  outcome: "name_taken",
                  message: `'${intent.viewName}' is held by ${slot.holder}`,
               };
            }
            // Already reading this table (a rebuild in place): nothing to do.
            if (
               slot.state === "ours" &&
               slot.target === intent.physicalTableName
            ) {
               return { ...intent, outcome: "unchanged" };
            }
            await pointNameView(site, intent.viewName, mark);
            // The mark must read back, or the next build would find its own
            // view "taken" and refuse the source. A view this call made whose
            // mark does not read back is dropped again; one that was already
            // Publisher's read back before, so the dialect round-trips.
            const after = ownedSlot(
               await probeNameView(site, intent.viewName),
               intent.owner,
            );
            if (
               after.state !== "ours" ||
               after.target !== intent.physicalTableName
            ) {
               if (slot.state === "empty") {
                  await dropNameView(site, intent.viewName);
               }
               return {
                  ...intent,
                  outcome: "failed",
                  message: `the view's mark did not read back from '${intent.viewName}'`,
               };
            }
            return slot.state === "ours"
               ? {
                    ...intent,
                    outcome: "repointed",
                    previousTarget: slot.target,
                 }
               : { ...intent, outcome: "created" };
         },
      );
      return (
         record ?? {
            ...intent,
            outcome: "failed",
            message: "the view's connection or destination is not available",
         }
      );
   } catch (err) {
      return {
         ...intent,
         outcome:
            err instanceof NameViewUnsupportedError ? "unsupported" : "failed",
         message: errMessage(err),
      };
   }
}

/**
 * Undo {@link publishNameViews} for a run that did not commit, so no view
 * is left reading a table no committed manifest names (and which the failed
 * run's cleanup may drop): a view the run created is dropped, one it
 * repointed is pointed back, and one that replaced this package's own table
 * is dropped and the table, kept aside until the commit, renamed back. Only
 * a view that is still this package's and still reads the run's table is
 * touched. Never throws.
 *
 * Returns what became of each replaced table, for the failed run to record:
 * `table_restored`, or `restore_failed` with why.
 */
export async function restoreNameViews(
   records: NameViewRecord[],
   context: NameViewContext,
): Promise<NameViewRecord[]> {
   const restored: NameViewRecord[] = [];
   for (const record of records) {
      if (record.outcome === "replaced_own_table") {
         restored.push(await restoreReplacedTable(record, context));
         continue;
      }
      if (record.outcome !== "created" && record.outcome !== "repointed") {
         continue;
      }
      try {
         await atNameViewSite(record, context, async (site) => {
            const slot = ownedSlot(
               await probeNameView(site, record.viewName),
               record.owner,
            );
            if (
               slot.state !== "ours" ||
               slot.target !== record.physicalTableName
            ) {
               return;
            }
            if (record.previousTarget) {
               await pointNameView(site, record.viewName, {
                  target: record.previousTarget,
                  owner: record.owner,
               });
            } else {
               await dropNameView(site, record.viewName);
            }
         });
      } catch (err) {
         logger.warn("Failed to put back a name= view after a failed commit", {
            viewName: record.viewName,
            physicalTableName: record.physicalTableName,
            error: errMessage(err),
         });
      }
   }
   return restored;
}

/**
 * Put back the table a `replaced_own_table` view took the place of, for a
 * run that did not commit ({@link restoreReplacedTableAt}, at the view's
 * site). Never throws.
 */
async function restoreReplacedTable(
   record: NameViewRecord,
   context: NameViewContext,
): Promise<NameViewRecord> {
   try {
      return (
         (await atNameViewSite(record, context, (site) =>
            restoreReplacedTableAt(site, record),
         )) ?? restoreFailed(record, "the view's site could not be reached")
      );
   } catch (err) {
      return restoreFailed(record, errMessage(err));
   }
}

function restoreFailed(
   record: NameViewRecord,
   message: string,
): NameViewRecord {
   logger.warn("Could not put back a table a name= view replaced", {
      viewName: record.viewName,
      asideName: record.asideTableName,
      error: message,
   });
   return { ...record, outcome: "restore_failed", message };
}

/**
 * At `site`: drop the view a `replaced_own_table` record made (when it is
 * still this package's and reads the run's table) and rename the aside
 * table back to the name. When the aside table is gone (its own run's
 * tables were dropped meanwhile) the view is still dropped: the run that
 * made it did not commit. A name that holds anything else is left alone.
 */
export async function restoreReplacedTableAt(
   site: NameViewSite,
   record: NameViewRecord,
): Promise<NameViewRecord> {
   const aside = record.asideTableName;
   if (!aside) return restoreFailed(record, "no aside name was recorded");
   const slot = ownedSlot(
      await probeNameView(site, record.viewName),
      record.owner,
   );
   const viewIsTheRuns =
      slot.state === "ours" && slot.target === record.physicalTableName;
   if (!viewIsTheRuns && slot.state !== "empty") {
      return restoreFailed(
         record,
         `'${record.viewName}' no longer holds this run's view; ` +
            `the table stays at '${aside}'`,
      );
   }
   const asideSlot = await probeNameView(site, aside);
   if (asideSlot.state !== "taken" || asideSlot.kind === "VIEW") {
      if (viewIsTheRuns) await dropNameView(site, record.viewName);
      return restoreFailed(record, `the table kept at '${aside}' is gone`);
   }
   if (!viewIsTheRuns) {
      // Nothing at the name: only the rename back is left to do.
      await site.run(renameBack(site, record, aside));
   } else if (
      !(await putTableBack(
         site,
         record,
         aside,
         replaceDdlFor(site, record, aside),
         undefined,
      ))
   ) {
      return restoreFailed(record, `the table stays at '${aside}'`);
   }
   return {
      ...record,
      outcome: "table_restored",
      message: `the commit failed: '${record.viewName}' is the table again`,
   };
}

/** The rename alone that puts an aside table back at `record`'s name. */
function renameBack(
   site: NameViewSite,
   record: Pick<NameViewRecord, "viewName">,
   aside: string,
): string {
   return renameTableSql(
      site.segments(aside),
      site.segments(record.viewName),
      site.dialect,
   );
}

/**
 * Before a table is dropped: settle every recorded view that reads it. A view
 * the probe shows is this package's and still reads `entry`'s table is repointed
 * to `successorFor`'s table, or dropped when there is none. A view that reads
 * another table, or that is not this package's, is left alone. Done BEFORE
 * the table's drop because a Postgres view refuses its table's drop.
 *
 * Never throws: a view that cannot be settled is logged, and the table's own
 * drop goes ahead (and on Postgres fails, so the table is owed, not lost).
 */
export async function settleNameViewsBeforeDrop(
   entry: Pick<
      ManifestEntry,
      "physicalTableName" | "connectionName" | "storageDestinationName"
   >,
   recorded: NameViewRecord[],
   successorFor: (view: NameViewRecord) => NameViewRecord | undefined,
   context: NameViewContext,
): Promise<void> {
   const reading = recorded.filter(
      (r) =>
         r.physicalTableName === entry.physicalTableName &&
         r.connectionName === entry.connectionName &&
         (r.storageDestinationName ?? undefined) ===
            (entry.storageDestinationName ?? undefined),
   );
   const seen = new Set<string>();
   for (const view of reading) {
      const key = nameViewKey(view);
      if (seen.has(key)) continue;
      seen.add(key);
      try {
         await atNameViewSite(view, context, async (site) => {
            if (!nameViewDialectSupported(site.dialect)) return;
            const slot = ownedSlot(
               await probeNameView(site, view.viewName),
               view.owner,
            );
            if (
               slot.state !== "ours" ||
               slot.target !== view.physicalTableName
            ) {
               return;
            }
            const successor = successorFor(view);
            if (successor) {
               await pointNameView(site, view.viewName, {
                  target: successor.physicalTableName,
                  owner: view.owner,
               });
               logger.info("Repointed a name= view off a table being dropped", {
                  viewName: view.viewName,
                  from: view.physicalTableName,
                  to: successor.physicalTableName,
               });
            } else {
               await dropNameView(site, view.viewName);
               logger.info("Dropped a name= view with its last table", {
                  viewName: view.viewName,
                  physicalTableName: view.physicalTableName,
               });
            }
         });
      } catch (err) {
         logger.warn("Failed to settle a name= view before its table's drop", {
            viewName: view.viewName,
            physicalTableName: view.physicalTableName,
            error: errMessage(err),
         });
      }
   }
}

/**
 * The newest other table each recorded view could read instead: among the
 * runs given (committed, and not being reclaimed), newest first, the first
 * record of the same view, by the same package, that pointed the view at a
 * different table the run's manifest still names. A record whose view was
 * never made (`conflict`, `name_taken`, `failed`, ...) is not a candidate.
 */
export function nameViewSuccessors(
   runs: {
      metadata: Record<string, unknown> | null;
      manifest?: { entries?: Record<string, ManifestEntry> } | null;
      completedAt?: Date | null;
      createdAt?: Date | null;
   }[],
): (view: NameViewRecord) => NameViewRecord | undefined {
   const at = (r: (typeof runs)[number]) =>
      (r.completedAt ?? r.createdAt ?? new Date(0)).getTime();
   const ordered = [...runs].sort((a, b) => at(b) - at(a));
   return (view) => {
      const key = nameViewKey(view);
      for (const run of ordered) {
         const tables = new Set(
            Object.values(run.manifest?.entries ?? {}).map(
               (e) => e.physicalTableName,
            ),
         );
         const found = nameViewsIn(run.metadata).find(
            (r) =>
               nameViewKey(r) === key &&
               r.owner === view.owner &&
               POINTED_OUTCOMES.has(r.outcome) &&
               r.physicalTableName !== view.physicalTableName &&
               tables.has(r.physicalTableName),
         );
         if (found) return found;
      }
      return undefined;
   };
}
