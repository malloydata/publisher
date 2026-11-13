// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { PersistSource } from "@malloydata/malloy";
import { BadRequestError } from "../../../errors";
import { logger } from "../../../logger";
import type { Materialization } from "../../../storage/DatabaseInterface";
import { errMessage } from "../../../utils";
import { quoteTablePath } from "../../quoting";
import type { CompiledBuildPlan } from "../plan/build_plan";
import { deriveAnnotationFields } from "../plan/persist_source_fields";
import { isLegacyFailedEntry, type ManifestEntry } from "../types";
import {
   nameViewDialectSupported,
   nameViewProbeSql,
   parseNameViewComment,
   sqlString,
} from "./name_view_sql";
import {
   atNameViewSite,
   type NameViewContext,
   type NameViewSite,
} from "./name_view";
import type { BuildInstruction } from "../types";

/**
 * Who owns a table an auto-run writes at a name the author chose.
 *
 * Auto-run names every other table for its package (`__p<hash>`, see
 * packageTableSuffix), so two packages, or two environments, that share a
 * connection or a storage destination never build one table. An explicit
 * `name=` keeps its name: the author chose it, and a reader outside Malloy
 * may rely on it. Two packages that declare the same `name=` on one schema
 * would still write one table, so before an auto-run writes at a `name=`, it
 * checks who holds the name:
 *
 * - nothing: built, and the table marked as this package's;
 * - a table marked as this package's, or this package's own `name=` view
 *   (which the table replaces, name_view.ts): built;
 * - a table with no mark that this package's own committed runs say they
 *   wrote (a table built before the mark existed, or one whose mark could not
 *   be written): built, and marked;
 * - anything else (a table another package marked, another package's view, a
 *   table or view Publisher did not make, or a table with no mark that this
 *   package's records do not name): refused, `table-owned-elsewhere`, before
 *   anything is written. The source serves live.
 *
 * A name whose holder cannot be read (the probe fails, or the dialect has no
 * way to ask) is refused too, `table-ownership-unverified`: building would
 * overwrite whatever is there.
 *
 * The mark is a comment on the table, `malloy-publisher:table owner=<env>/<pkg>`
 * (URI-encoded), the warehouse half of ownership: it is what decides between
 * two packages whose records both name the table (a collision from before the
 * mark existed: the first to build again claims it, and the other is
 * refused). The run records are the Publisher half. Written after every build,
 * best-effort: a table whose mark could not be written is still this
 * package's by its records once the run commits.
 *
 * A drop (deleting a run's tables, reclaiming a version's) skips a table
 * marked as another package's (see ownedElsewhere): a table this package
 * once wrote that another package has built since is no longer this
 * package's to drop.
 *
 * Host-orchestrated builds are not checked: the host chooses their names.
 * Within one server only: two servers whose environments and packages have
 * the same names write the same mark.
 */

/** The prefix of a Publisher table's ownership mark. */
export const OWNED_TABLE_MARK = "malloy-publisher:table";

/** The code of a build refused because another holds its `name=`. */
export const TABLE_OWNED_ELSEWHERE = "table-owned-elsewhere";

/** The code of a build refused because who holds its `name=` is unknown. */
export const TABLE_OWNERSHIP_UNVERIFIED = "table-ownership-unverified";

/**
 * A build refused before it wrote, because its table's name is held by
 * something that is not this package's, or by something it cannot read.
 * `code` is one of the two codes above (an open string).
 */
export class TableOwnershipError extends BadRequestError {
   constructor(
      readonly code: string,
      message: string,
   ) {
      super(message);
   }
}

/**
 * Whether the author chose the table's name: an explicit `name=` on a
 * `#@ persist` source, a persisted view or a `#(materialize)` declaration. A
 * rollup's name (its namespace and grain digest) and a `#(materialize)`
 * table that defaults to its source's name are Publisher's.
 */
export function hasAuthorTableName(
   compiled: Pick<CompiledBuildPlan, "preaggregatePlans" | "materializePlans">,
   persistSource: PersistSource,
): boolean {
   if (compiled.preaggregatePlans?.[persistSource.sourceID]) return false;
   const materialize = compiled.materializePlans?.[persistSource.sourceID];
   if (materialize) return !materialize.nameDefaulted;
   return !!deriveAnnotationFields(persistSource).name;
}

/** URI-encoded, the single quote included, so no value needs escaping. */
function encodeMarkValue(value: string): string {
   return encodeURIComponent(value).replace(/'/g, "%27");
}

/** The ownership mark for a table `owner` (`<environment>/<package>`) wrote. */
export function ownedTableComment(owner: string): string {
   return `${OWNED_TABLE_MARK} owner=${encodeMarkValue(owner)}`;
}

/**
 * The owner a table's comment names, or undefined when the comment is not an
 * ownership mark: exactly the prefix, then `owner=` once. Accepts BigQuery's
 * double-quoted TABLE_OPTIONS spelling.
 */
export function parseOwnedTableComment(comment: unknown): string | undefined {
   if (typeof comment !== "string") return undefined;
   let text = comment.trim();
   if (text.length >= 2 && text.startsWith('"') && text.endsWith('"')) {
      text = text.slice(1, -1);
   }
   const fields = text.split(" ");
   if (fields.length !== 2 || fields[0] !== OWNED_TABLE_MARK) return undefined;
   if (!fields[1].startsWith("owner=")) return undefined;
   const encoded = fields[1].slice("owner=".length);
   if (encoded === "") return undefined;
   try {
      return decodeURIComponent(encoded);
   } catch {
      return undefined;
   }
}

/** The dialects whose tables' holders can be read (and marked). */
export function tableOwnershipDialectSupported(dialect: string): boolean {
   return (
      nameViewDialectSupported(dialect) ||
      dialect === "mysql" ||
      dialect === "presto"
   );
}

/**
 * A query answering what holds the name `segments` names: one row per
 * relation, with `kind` (`VIEW`, or anything else for a table) and `comment`.
 * The `name=` view's probe (name_view_sql.ts), plus MySQL, and Presto with no
 * comments (a Presto table is never read as marked, only as this package's
 * by its records).
 */
export function ownedTableProbeSql(
   segments: string[],
   dialect: string,
): string {
   const lit = (v: string) => sqlString(v, dialect);
   const name = segments[segments.length - 1];
   if (dialect === "mysql") {
      const schema =
         segments.length > 1
            ? lit(segments[segments.length - 2])
            : "DATABASE()";
      return (
         `SELECT TABLE_TYPE AS kind, TABLE_COMMENT AS comment FROM information_schema.TABLES ` +
         `WHERE TABLE_SCHEMA = ${schema} AND TABLE_NAME = ${lit(name)}`
      );
   }
   if (dialect === "presto") {
      const schema =
         segments.length > 1
            ? `lower(${lit(segments[segments.length - 2])})`
            : "current_schema";
      const from =
         segments.length === 3
            ? `${quoteTablePath(segments[0], dialect)}.information_schema.tables`
            : "information_schema.tables";
      return (
         `SELECT table_type AS kind, CAST(NULL AS varchar) AS comment FROM ${from} ` +
         `WHERE table_schema = ${schema} AND table_name = lower(${lit(name)})`
      );
   }
   return nameViewProbeSql(segments, dialect);
}

/** The DDL that marks the table `segments` names as `owner`'s. */
export function ownedTableMarkSql(
   segments: string[],
   owner: string,
   dialect: string,
): string {
   const table = quoteTablePath(segments.join("."), dialect);
   const comment = sqlString(ownedTableComment(owner), dialect);
   switch (dialect) {
      case "standardsql":
         return `ALTER TABLE ${table} SET OPTIONS(description = ${comment})`;
      case "mysql":
         return `ALTER TABLE ${table} COMMENT = ${comment}`;
      case "sqlserver": {
         const schema =
            segments.length === 2
               ? sqlString(segments[0], dialect)
               : "SCHEMA_NAME()";
         const name = sqlString(segments[segments.length - 1], dialect);
         const property =
            `@name = N'MS_Description', @value = ${comment}, @level0type = N'SCHEMA', ` +
            `@level0name = @schema, @level1type = N'TABLE', @level1name = ${name}`;
         return [
            `DECLARE @schema sysname = ${schema};`,
            `IF EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 1 ` +
               `AND major_id = OBJECT_ID(${sqlString(table, dialect)}) AND minor_id = 0 ` +
               `AND name = N'MS_Description')`,
            `   EXEC sys.sp_updateextendedproperty ${property};`,
            "ELSE",
            `   EXEC sys.sp_addextendedproperty ${property};`,
         ].join("\n");
      }
      default:
         return `COMMENT ON TABLE ${table} IS ${comment}`;
   }
}

/** What holds a table's name, as `owner` sees it. */
export type TableHolder =
   | { state: "free" }
   /**
    * A table marked as `owner`'s, or (`view`) `owner`'s own `name=` view,
    * which a build of the table replaces.
    */
   | { state: "ours"; view?: true }
   /** A table with no mark: `owner`'s only if its records say so. */
   | { state: "unmarked" }
   | { state: "taken"; holder: string };

/** Read {@link ownedTableProbeSql}'s rows as `owner` sees them. */
export function tableHolderOf(
   rows: Record<string, unknown>[],
   owner: string,
): TableHolder {
   if (rows.length === 0) return { state: "free" };
   let unmarked = false;
   let table = false;
   for (const row of rows) {
      const kind = String(field(row, "kind") ?? "").toUpperCase();
      const comment = field(row, "comment");
      if (kind === "VIEW") {
         const view = parseNameViewComment(comment);
         if (!view) {
            return {
               state: "taken",
               holder: "a view Publisher did not create",
            };
         }
         if (view.owner !== owner) {
            return {
               state: "taken",
               holder: `a name= view package '${view.owner}' made`,
            };
         }
         continue;
      }
      table = true;
      const marked = parseOwnedTableComment(comment);
      if (marked === undefined) {
         unmarked = true;
      } else if (marked !== owner) {
         return {
            state: "taken",
            holder: `a table package '${marked}' built`,
         };
      }
   }
   if (unmarked) return { state: "unmarked" };
   return table ? { state: "ours" } : { state: "ours", view: true };
}

/** A column of a result row, whatever case the driver reports it in. */
function field(row: Record<string, unknown>, name: string): unknown {
   if (name in row) return row[name];
   const key = Object.keys(row).find((k) => k.toLowerCase() === name);
   return key === undefined ? undefined : row[key];
}

/** Where a table lives, as a manifest entry or an instruction says. */
export interface OwnedTableTarget {
   physicalTableName: string;
   connectionName: string;
   storageDestinationName?: string;
}

/**
 * Whether this package's own committed runs (`runs`, the package's own
 * listing) name exactly this table: the same name, on the same connection
 * and in the same destination.
 */
export function packageRecordsName(
   runs: Pick<Materialization, "status" | "manifest">[],
   target: OwnedTableTarget,
): boolean {
   return runs.some(
      (run) =>
         run.status === "MANIFEST_FILE_READY" &&
         Object.values(run.manifest?.entries ?? {}).some(
            (entry: ManifestEntry) =>
               !isLegacyFailedEntry(entry) &&
               entry.physicalTableName === target.physicalTableName &&
               entry.connectionName === target.connectionName &&
               (entry.storageDestinationName ?? undefined) ===
                  (target.storageDestinationName ?? undefined),
         ),
   );
}

/** What holds `physicalTableName` at `site`, as `owner` sees it. */
export async function probeTableHolder(
   site: Pick<NameViewSite, "dialect" | "segments" | "run">,
   physicalTableName: string,
   owner: string,
): Promise<TableHolder> {
   return tableHolderOf(
      await site.run(
         ownedTableProbeSql(site.segments(physicalTableName), site.dialect),
      ),
      owner,
   );
}

/**
 * Before an auto-run writes `target` at a name the author chose: throw a
 * {@link TableOwnershipError} unless the name is free or `owner`'s
 * (`ownRecords` answers whether this package's records name the table, asked
 * only for a table with no mark). `site` is undefined when the table's
 * connection or destination cannot be reached, which refuses too.
 */
export async function assertTableOwnable(
   site: Pick<NameViewSite, "dialect" | "segments" | "run"> | undefined,
   target: OwnedTableTarget,
   owner: string,
   sourceName: string,
   ownRecords: () => Promise<boolean>,
): Promise<void> {
   const where = target.storageDestinationName
      ? `in storage destination '${target.storageDestinationName}'`
      : `on connection '${target.connectionName}'`;
   const unverified = (why: string) =>
      new TableOwnershipError(
         TABLE_OWNERSHIP_UNVERIFIED,
         `'${sourceName}' is not built (${TABLE_OWNERSHIP_UNVERIFIED}): ` +
            `whether its name= '${target.physicalTableName}' ${where} is held ` +
            `by another package could not be read (${why}), and the build ` +
            `would overwrite whatever is there. It is answered live.`,
      );
   if (!site) throw unverified("the connection could not be reached");
   if (!tableOwnershipDialectSupported(site.dialect)) {
      throw unverified(`dialect '${site.dialect}' is not supported`);
   }
   let holder: TableHolder;
   try {
      holder = await probeTableHolder(site, target.physicalTableName, owner);
   } catch (err) {
      throw unverified(errMessage(err));
   }
   if (holder.state === "unmarked") {
      if (await ownRecords()) return;
      holder = {
         state: "taken",
         holder:
            "a table with no Publisher mark that this package's records do not show it built",
      };
   }
   if (holder.state === "taken") {
      throw new TableOwnershipError(
         TABLE_OWNED_ELSEWHERE,
         `'${sourceName}' is not built (${TABLE_OWNED_ELSEWHERE}): its name= ` +
            `'${target.physicalTableName}' ${where} is held by ${holder.holder}, ` +
            `and two packages never write one table. It is answered live. ` +
            `Give the source another name=, or drop that table if nothing ` +
            `needs it.`,
      );
   }
}

/**
 * Mark the table `physicalTableName` names at `site` as `owner`'s.
 * Best-effort, never throws: an unmarked table stays this package's by its
 * records once the run commits.
 */
export async function markOwnedTable(
   site: Pick<NameViewSite, "dialect" | "segments" | "run">,
   physicalTableName: string,
   owner: string,
): Promise<void> {
   try {
      await site.run(
         ownedTableMarkSql(
            site.segments(physicalTableName),
            owner,
            site.dialect,
         ),
      );
   } catch (err) {
      logger.warn(
         "Could not mark a built table as this package's; its run's records say so instead",
         { physicalTableName, owner, error: errMessage(err) },
      );
   }
}

/**
 * The package other than `owner` whose mark the table `physicalTableName`
 * names at `site` carries, or undefined (no such mark, no table, or the
 * probe cannot run). Never throws: a drop that cannot tell goes ahead as
 * before, since the table is in this package's records.
 */
export async function ownedElsewhere(
   site: Pick<NameViewSite, "dialect" | "segments" | "run">,
   physicalTableName: string,
   owner: string,
): Promise<string | undefined> {
   if (!tableOwnershipDialectSupported(site.dialect)) return undefined;
   try {
      const rows = await site.run(
         ownedTableProbeSql(site.segments(physicalTableName), site.dialect),
      );
      for (const row of rows) {
         if (String(field(row, "kind") ?? "").toUpperCase() === "VIEW") {
            continue;
         }
         const marked = parseOwnedTableComment(field(row, "comment"));
         if (marked !== undefined && marked !== owner) return marked;
      }
   } catch {
      // Unknown: the caller drops as before.
   }
   return undefined;
}

/**
 * Before an auto-run builds: take back out of `carried` each table at the
 * author's `name=` (`recheck`, from deriveSelfInstructions) that is not a
 * table marked as `owner`'s, and rebuild it instead (its instruction joins
 * `instructions`), so the build's ownership check decides it. Skip-if-
 * unchanged trusts that a table still holds what this package wrote; at a
 * name another package can also write, only the mark says so. A table whose
 * holder cannot be read is rebuilt too, which the check then refuses.
 */
export async function uncarryUnownedTables(
   recheck: Record<string, BuildInstruction>,
   carried: Record<string, ManifestEntry>,
   instructions: BuildInstruction[],
   owner: string,
   context: NameViewContext,
): Promise<void> {
   for (const [address, instruction] of Object.entries(recheck)) {
      const entry = carried[address];
      if (!entry?.connectionName) continue;
      const target = {
         connectionName: entry.connectionName,
         ...(entry.storageDestinationName
            ? { storageDestinationName: entry.storageDestinationName }
            : {}),
      };
      let ours = false;
      try {
         ours =
            (await atNameViewSite(target, context, async (site) => {
               if (!tableOwnershipDialectSupported(site.dialect)) return false;
               const holder = await probeTableHolder(
                  site,
                  entry.physicalTableName,
                  owner,
               );
               return holder.state === "ours" && !holder.view;
            })) ?? false;
      } catch {
         ours = false;
      }
      if (ours) continue;
      delete carried[address];
      instructions.push(instruction);
      logger.info(
         "Rebuilding a table at an author's name= that is not marked as this package's",
         {
            physicalTableName: entry.physicalTableName,
            sourceName: entry.sourceName,
            owner,
         },
      );
   }
}
