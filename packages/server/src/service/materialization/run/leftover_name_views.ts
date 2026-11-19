// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The `name=` views earlier releases left behind.
 *
 * Earlier releases made, for a `#(materialize)` source whose table a run
 * named by generation (a host's instructions, or a `scope: version` run), a
 * view at the declared `name=` reading it, marked with the comment
 * `malloy-publisher:name-view owner=<env>/<pkg> target=<table>`, and
 * recorded it on the run (`metadata.nameViews`). Publisher makes no view now,
 * and never repoints one. What is left is cleanup, and only of this
 * package's own marked views that the warehouse proves still read the table
 * their mark names ({@link provenOwnView}: a view repointed in place keeps
 * its comment, so the mark alone never decides):
 *
 * - a reclaim's drop of a table (a version's tables reclaimed) also drops
 *   each such view the package's runs recorded over it
 *   ({@link dropLeftoverNameViews}), so no view is left reading nothing
 *   (and on Postgres, where a view binds to its table, the table's drop is
 *   not refused);
 * - a build at an author's `name=` (v1 `#@ persist` or `#(materialize)`)
 *   whose name holds such a view replaces it with the table: in the build's
 *   own transaction on DuckDB and Postgres ({@link replaceOwnViewStatements}),
 *   just before the build elsewhere ({@link dropOwnNameView});
 *   table_ownership.ts decides the name is this package's;
 * - a table's drop that fails because this package's own view holds its name
 *   (a view replaced the table in an earlier release) retires the entry
 *   rather than owing it ({@link heldByOwnNameView}).
 *
 * A view without the mark, or marked for another package, is never touched.
 */

import { errorCodeOf } from "./error_code";
import { logger } from "../../../logger";
import type { ManifestEntry } from "../domain/types";
import {
   parseNameViewComment,
   viewDefinitionReadsOnly,
} from "./table_mark_sql";
import {
   dropViewSql,
   relationProbeSql,
   relationProbeSupported,
   replaceViewStatements,
   viewReadsSql,
} from "./warehouse_sql";
import {
   atTableSite,
   type TableSite,
   type TableSiteContext,
} from "./table_site";

/** Where a run's metadata recorded the `name=` views it made. */
const NAME_VIEWS_KEY = "nameViews";

/** One view a run of an earlier release recorded making. */
export interface RecordedNameView {
   viewName: string;
   /** The package that made it, as its mark names it (packageOwner). */
   owner: string;
   /** The table the view read when the run recorded it. */
   physicalTableName: string;
   connectionName: string;
   storageDestinationName?: string;
}

/** The views a run's metadata records, or empty. */
export function nameViewsIn(
   metadata: Record<string, unknown> | null | undefined,
): RecordedNameView[] {
   const recorded = metadata?.[NAME_VIEWS_KEY];
   if (!Array.isArray(recorded)) return [];
   return recorded.filter(
      (r): r is RecordedNameView =>
         typeof r === "object" &&
         r !== null &&
         typeof (r as RecordedNameView).viewName === "string" &&
         typeof (r as RecordedNameView).owner === "string" &&
         typeof (r as RecordedNameView).physicalTableName === "string" &&
         typeof (r as RecordedNameView).connectionName === "string",
   );
}

/** A column of a result row, whatever case the driver reports it in. */
function field(row: Record<string, unknown>, name: string): unknown {
   if (name in row) return row[name];
   const key = Object.keys(row).find((k) => k.toLowerCase() === name);
   return key === undefined ? undefined : row[key];
}

/**
 * The table this package's own marked view at `name` is marked as reading,
 * or undefined when the name holds anything else (nothing, a table, an
 * unmarked view, another package's view) or the dialect cannot be probed.
 */
export async function ownNameViewTarget(
   site: TableSite,
   name: string,
   owner: string,
): Promise<string | undefined> {
   if (!relationProbeSupported(site.dialect)) return undefined;
   return ownMarkedTarget(
      await site.run(relationProbeSql(site.segments(name), site.dialect)),
      owner,
   );
}

/**
 * This package's own marked view at `name`, proven to read the table its mark
 * names: the mark alone does not say, since a view repointed in place
 * (Postgres's `CREATE OR REPLACE VIEW`, SQL Server's `ALTER VIEW`) keeps its
 * comment. Undefined when the name holds anything else, when `expectedTarget`
 * is given and the mark names another table, or when what the view reads
 * cannot be proven (the view is then left alone).
 */
interface ProvenOwnView {
   name: string;
   /** The table the mark names, and the view is proven to read. */
   target: string;
   /** The view's comment, exactly as read. */
   comment: string;
   /** The view's definition, where the proof read it. */
   definition?: string;
}

export async function provenOwnView(
   site: TableSite,
   name: string,
   owner: string,
   expectedTarget?: string,
): Promise<ProvenOwnView | undefined> {
   if (!relationProbeSupported(site.dialect)) return undefined;
   const rows = await site.run(
      relationProbeSql(site.segments(name), site.dialect),
   );
   const target = ownMarkedTarget(rows, owner);
   if (target === undefined) return undefined;
   if (expectedTarget !== undefined && target !== expectedTarget)
      return undefined;
   const comment = String(field(rows[0], "comment"));
   const ask = viewReadsSql(
      site.segments(name),
      site.segments(target),
      site.dialect,
   );
   const [answer] = await site.run(ask.sql);
   if (ask.answer === "reads") {
      const reads = answer === undefined ? undefined : field(answer, "reads");
      return reads === true || reads === 1 || reads === "1" || reads === "t"
         ? { name, target, comment }
         : undefined;
   }
   const definition =
      answer === undefined ? undefined : field(answer, "definition");
   return typeof definition === "string" &&
      viewDefinitionReadsOnly(
         definition,
         target,
         field(answer!, "view_schema"),
         site.dialect,
      )
      ? { name, target, comment, definition }
      : undefined;
}

/** The table `owner`'s mark on the probed relation names, when it is one. */
function ownMarkedTarget(
   rows: Record<string, unknown>[],
   owner: string,
): string | undefined {
   if (rows.length === 0) return undefined;
   let target: string | undefined;
   for (const row of rows) {
      if (String(field(row, "kind") ?? "").toUpperCase() !== "VIEW") {
         return undefined;
      }
      const mark = parseNameViewComment(field(row, "comment"));
      if (!mark || mark.owner !== owner) return undefined;
      target ??= mark.target;
   }
   return target;
}

/**
 * Drop this package's own marked view at `name`, proven to read the table its
 * mark names (and, with `expectedTarget`, that it is that table) by a probe
 * just before the drop. Returns whether it was dropped. A window remains
 * between the probe and the drop, which only an actor outside this server can
 * use: a build at the name holds the name's lock (table_ownership.ts).
 */
export async function dropOwnNameView(
   site: TableSite,
   name: string,
   owner: string,
   expectedTarget?: string,
): Promise<boolean> {
   const view = await provenOwnView(site, name, owner, expectedTarget);
   if (!view) return false;
   await site.run(dropViewSql(site.segments(name), site.dialect));
   return true;
}

/**
 * The statements that drop `view` inside a build's own transaction, before
 * the table takes its name, on DuckDB and Postgres: a guard that fails the
 * transaction unless the name still holds the view the probe proved (on
 * Postgres after locking it, so a change in flight is waited for and seen),
 * then the drop. A failed build rolls both back and leaves the view.
 * Undefined on other dialects, which drop it just before the build. The
 * statements' text is warehouse_sql.ts's.
 */
export function replaceOwnViewStatements(
   view: ProvenOwnView,
   site: Pick<TableSite, "dialect" | "segments">,
): string[] | undefined {
   return replaceViewStatements(
      {
         segments: site.segments(view.name),
         targetSegments: site.segments(view.target),
         comment: view.comment,
         definition: view.definition,
      },
      site.dialect,
   );
}

/**
 * Before `entry`'s table is dropped: drop each view `recorded` (the
 * package's runs' records) says was made over it, where the view still
 * carries `owner`'s mark naming that table and the warehouse proves it still
 * reads it ({@link provenOwnView}). A view repointed to another table since,
 * even in place with its comment kept, is left. Never throws: a view
 * that cannot be dropped is logged, and the table's drop goes ahead.
 */
export async function dropLeftoverNameViews(
   entry: Pick<
      ManifestEntry,
      "physicalTableName" | "connectionName" | "storageDestinationName"
   >,
   recorded: RecordedNameView[],
   owner: string,
   context: TableSiteContext,
): Promise<void> {
   const seen = new Set<string>();
   for (const view of recorded) {
      if (
         view.owner !== owner ||
         view.physicalTableName !== entry.physicalTableName ||
         view.connectionName !== entry.connectionName ||
         (view.storageDestinationName ?? undefined) !==
            (entry.storageDestinationName ?? undefined) ||
         seen.has(view.viewName)
      ) {
         continue;
      }
      seen.add(view.viewName);
      try {
         const dropped = await atTableSite(view, context, (site) =>
            dropOwnNameView(
               site,
               view.viewName,
               owner,
               entry.physicalTableName,
            ),
         );
         if (dropped) {
            logger.info("Dropped a leftover name= view with its table", {
               viewName: view.viewName,
               physicalTableName: entry.physicalTableName,
            });
         }
      } catch (err) {
         logger.warn("Failed to drop a leftover name= view with its table", {
            viewName: view.viewName,
            physicalTableName: entry.physicalTableName,
            code: errorCodeOf(err),
         });
      }
   }
}

/**
 * Whether `name` at `target`'s site holds this package's own marked view, as
 * the dropper asks when a table's `DROP TABLE` fails: an earlier release
 * replaced that table with the view, so the table is gone and its entry is
 * retired rather than owed. False when the name holds anything else or
 * cannot be probed. Never throws.
 */
export async function heldByOwnNameView(
   target: { connectionName: string; storageDestinationName?: string },
   name: string,
   owner: string,
   context: TableSiteContext,
): Promise<boolean> {
   try {
      return (
         (await atTableSite(
            target,
            context,
            async (site) =>
               (await ownNameViewTarget(site, name, owner)) !== undefined,
         )) ?? false
      );
   } catch {
      return false;
   }
}
