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
import type { ApiConnection } from "../../model";
import { quoteTablePath } from "../../quoting";
import type { CompiledBuildPlan } from "../plan/build_plan";
import type { BuildInstruction, ManifestEntry } from "../types";
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
   nameViewSlotOf,
   NameViewUnsupportedError,
   pathSegments,
   sqlString,
   type NameViewSlot,
} from "./name_view_sql";
import { destinationSchemaOf, storageTablePath } from "./storage_table_path";

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
 * mark (name_view_sql.ts), never by its name alone.
 *
 * Lifecycle:
 * - before a source builds, its `name=` is probed: a name held by anything
 *   Publisher did not mark fails that source's build (nothing is written);
 * - once every source of the run has built, each view is created or
 *   repointed, view and mark in one statement or transaction, and the
 *   outcome recorded on the run;
 * - when a table is dropped (a deleted run's tables, a reclaimed version's),
 *   a view that still reads it is repointed to the newest other committed
 *   run's table for the same view, or dropped with its last generation. A
 *   view that reads another table, or that is not Publisher's, is left alone.
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
 * string: `created`, `repointed`, `unchanged`, `name_taken`, `unsupported`,
 * `conflict` or `failed`.
 */
export interface NameViewRecord extends NameViewIntent {
   outcome: string;
   message?: string;
}

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
 * `#(materialize)` source, only with `PUBLISHER_MATERIALIZE` on, and only
 * when its table is not already called `name=` (a self-instructed,
 * unversioned build names the table itself).
 */
export function nameViewIntentFor(
   compiled: Pick<CompiledBuildPlan, "materializePlans">,
   persistSource: Pick<PersistSource, "sourceID" | "name" | "connectionName">,
   instruction: Pick<BuildInstruction, "physicalTableName" | "destination">,
   sourceEntityId: string,
): NameViewIntent | undefined {
   if (!getMaterializeEnabled()) return undefined;
   const name =
      compiled.materializePlans?.[persistSource.sourceID]?.persist.name;
   if (!name) return undefined;
   const viewName = nameViewName(name, instruction.physicalTableName);
   if (viewName === instruction.physicalTableName) return undefined;
   return {
      sourceEntityId,
      sourceName: persistSource.name,
      viewName,
      physicalTableName: instruction.physicalTableName,
      connectionName: persistSource.connectionName,
      ...(instruction.destination
         ? { storageDestinationName: instruction.destination }
         : {}),
   };
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

/** Make or repoint the view to read `physicalTableName`, mark and all. */
export async function pointNameView(
   site: NameViewSite,
   viewName: string,
   physicalTableName: string,
): Promise<void> {
   const ddl = nameViewReplaceDdl(
      site.segments(viewName),
      site.targetSql(physicalTableName),
      physicalTableName,
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
 * Statements a Postgres swap brackets its DROP and RENAME with, so a rebuild
 * of the very table the `name=` view reads is not refused by the view that
 * depends on it: the view is dropped and made again, mark and all, inside
 * the swap's transaction, so a reader never finds it missing. Undefined on
 * every other dialect, whose views do not bind to a table: the swap leaves
 * them reading the name, which the new table takes.
 */
export interface SwapDependents {
   before: string[];
   after: string[];
}

export function swapDependentsFor(
   intent: NameViewIntent,
   slot: NameViewSlot,
   dialect: string,
): SwapDependents | undefined {
   if (dialect !== "postgres") return undefined;
   if (slot.state !== "ours" || slot.target !== intent.physicalTableName) {
      return undefined;
   }
   const segments = pathSegments(intent.viewName);
   const view = quoteTablePath(segments.join("."), dialect);
   return {
      before: [`DROP VIEW ${view}`],
      after: [
         `CREATE VIEW ${view} AS SELECT * FROM ${quoteTablePath(intent.physicalTableName, dialect)}`,
         `COMMENT ON VIEW ${view} IS ${sqlString(nameViewComment(intent.physicalTableName), dialect)}`,
      ],
   };
}

/**
 * Before a source builds: what holds its `name=`. Throws
 * {@link NameViewTakenError} when something Publisher did not mark holds it,
 * so the source fails before any table is written. Undefined when the name
 * could not be probed (an unsupported dialect, an unreachable site, a failed
 * probe): the build goes ahead and the view step after it reports why.
 */
export async function preflightNameView(
   intent: NameViewIntent,
   context: NameViewContext,
): Promise<{ slot: NameViewSlot; dialect: string } | undefined> {
   let probed: { slot: NameViewSlot; dialect: string } | undefined;
   try {
      probed = await atNameViewSite(intent, context, async (site) =>
         nameViewDialectSupported(site.dialect)
            ? {
                 slot: await probeNameView(site, intent.viewName),
                 dialect: site.dialect,
              }
            : undefined,
      );
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
            `held by a ${probed.slot.kind.toLowerCase()} Publisher did not create, ` +
            `and the view naming the source's current table never replaces one. ` +
            `Drop or rename that object, or give #(materialize) another name=.`,
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
 */
export async function publishNameViews(
   intents: NameViewIntent[],
   context: NameViewContext,
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
      records.push(await publishNameView(group[0], context));
   }
   for (const record of records) {
      const failed = !["created", "repointed", "unchanged"].includes(
         record.outcome,
      );
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
): Promise<NameViewRecord> {
   try {
      const record = await atNameViewSite(intent, context, async (site) => {
         if (!nameViewDialectSupported(site.dialect)) {
            return {
               ...intent,
               outcome: "unsupported",
               message: `a view in dialect '${site.dialect}' cannot carry Publisher's mark`,
            };
         }
         const slot = await probeNameView(site, intent.viewName);
         if (slot.state === "taken") {
            return {
               ...intent,
               outcome: "name_taken",
               message: `'${intent.viewName}' is held by a ${slot.kind.toLowerCase()} Publisher did not create`,
            };
         }
         // Already reading this table (a rebuild in place): nothing to do.
         if (
            slot.state === "ours" &&
            slot.target === intent.physicalTableName
         ) {
            return { ...intent, outcome: "unchanged" };
         }
         await pointNameView(site, intent.viewName, intent.physicalTableName);
         return {
            ...intent,
            outcome: slot.state === "ours" ? "repointed" : "created",
         };
      });
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
 * Before a table is dropped: settle every recorded view that reads it. A view
 * the probe shows is Publisher's and still reads `entry`'s table is repointed
 * to `successorFor`'s table, or dropped when there is none. A view that reads
 * another table, or that is not Publisher's, is left alone. Done BEFORE the
 * table's drop because a Postgres view refuses its table's drop.
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
            const slot = await probeNameView(site, view.viewName);
            if (
               slot.state !== "ours" ||
               slot.target !== view.physicalTableName
            ) {
               return;
            }
            const successor = successorFor(view);
            if (successor) {
               await pointNameView(
                  site,
                  view.viewName,
                  successor.physicalTableName,
               );
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
 * record of the same view for a different table that the run's manifest
 * still names.
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
               r.physicalTableName !== view.physicalTableName &&
               tables.has(r.physicalTableName),
         );
         if (found) return found;
      }
      return undefined;
   };
}
