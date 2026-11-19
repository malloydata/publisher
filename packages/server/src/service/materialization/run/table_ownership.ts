// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Who owns a table an auto-run writes at a name the author chose.
 *
 * Auto-run names every other table readably, ending `__p<hash>` of the
 * table's identity (environment, package, model, source; see
 * domain/table_name.ts), so two packages, or two environments, that share a
 * connection or a storage destination never build one table. An explicit
 * `name=` keeps its name: the author chose it, and a reader outside Malloy
 * may rely on it. Two packages that declare the same `name=` on one schema
 * would still write one table, so before an auto-run writes at a `name=`, it
 * checks who holds the name:
 *
 * - nothing: built, and the table marked as this package's;
 * - a table marked as this package's, or this package's own `name=` view
 *   an earlier release left there (dropped first, leftover_name_views.ts):
 *   built;
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
 * A `name=` spelled like one of Publisher's own names (ending `__p<hash>` or
 * `__v<version>_<hash>`, see hasReservedTableSuffix) is refused,
 * `table-owned-elsewhere`: it may be another package's default-named table,
 * which carries no mark. And one server builds one `name=` at a time
 * (withOwnedNameLock), from the check until the mark is written, so two
 * packages that find the name free at once never both build it.
 *
 * The mark is a comment on the table, `malloy-publisher:table
 * owner=<env>/<pkg> table=<location>` (each value URI-encoded; `location`
 * is where the table is, fully qualified, as the warehouse's catalog reports
 * it when the mark is written, each part URI-encoded and joined with `.`,
 * see relationLocationOf; a mark before 0.16.0,
 * or one whose location could not be read, has no `table`), the warehouse
 * half of ownership: it is what decides between
 * two packages whose records both name the table (a collision from before the
 * mark existed: the first to build again claims it, and the other is
 * refused). The run records are the Publisher half. Written after every
 * database-tier build, whoever named the table (an author's `name=`,
 * Publisher, or a host's instruction), best-effort: a table whose mark could
 * not be written is still this package's by its records once the run
 * commits. The mark is what lets a caller's drop
 * (Reclaimer.dropConnectionTable) prove a table is this package's after its
 * run's record is gone; that drop accepts a mark only on the table it names
 * (markedFor), since a comment travels with a renamed, moved or copied table.
 * A build and a reclaim read the owner alone, for marks of every release:
 * they act only on a name this package's own records or `name=` hold.
 *
 * A drop (deleting a run's tables, reclaiming a version's) skips a table
 * marked as another package's (see ownedElsewhere): a table this package
 * once wrote that another package has built since is no longer this
 * package's to drop.
 *
 * The SQL of the ownership probe and mark is in warehouse_sql.ts.
 *
 * Host-orchestrated builds are not checked (the host chooses their names),
 * only marked.
 * Within one server only: two servers whose environments and packages have
 * the same names write the same mark.
 */

import { errorCodeOf } from "../../../error_code";
import { leafKey } from "../domain/physical_table";
import { AsyncLocalStorage } from "node:async_hooks";
import type { PersistSource } from "@malloydata/malloy";
import { TableOwnershipError } from "../../../errors";
import { logger } from "../../../logger";
import { externalEnv } from "../../../settings/external";
import type { Materialization } from "../../../storage/DatabaseInterface";
import { errMessage } from "../../../utils";
import { deriveAnnotationFields, type RunPlan } from "./run_plan";
import { isLegacyFailedEntry, type ManifestEntry } from "../domain/types";
import { capabilitiesOf } from "../domain/dialect";
import { hasReservedTableSuffix } from "../domain/table_name";
import {
   OWNED_TABLE_MARK,
   parseNameViewComment,
   parseOwnedTableComment,
   parseOwnedTableMark,
} from "./table_mark_sql";
import { ownedTableProbeSql, tableCommentSql } from "./warehouse_sql";
import { KeyedLock } from "./keyed_lock";
import {
   atTableSite,
   type TableSite,
   type TableSiteContext,
} from "./table_site";
import type { BuildInstruction } from "../domain/types";

/** The code of a build refused because another holds its `name=`. */
export const TABLE_OWNED_ELSEWHERE = "table-owned-elsewhere";

/** The code of a build refused because who holds its `name=` is unknown. */
export const TABLE_OWNERSHIP_UNVERIFIED = "table-ownership-unverified";

/**
 * A build refused before it wrote, because its table's name is held by
 * something that is not this package's, or by something it cannot read.
 * `code` is one of the two codes above (an open string).
 */
export { TableOwnershipError };

/**
 * One server's builds at the author's `name=`, serialized by name: two
 * packages that probe one free name at once would both build it, and each
 * serve whichever wrote last. Held from the ownership check until the mark is
 * written, so the second build's check sees the first one's mark. Keyed by
 * `leafKey` (the last segment, unquoted, lower-cased): two connections
 * or environments may reach one schema under different names, and an
 * over-wide key only serializes.
 */
const OWNED_NAME_LOCK = new KeyedLock();

/**
 * The owned-name holds of this async context, each marked released once
 * done: work its holder started without waiting inherits the context, not
 * the hold.
 */
const ownedNameHolds = new AsyncLocalStorage<{ held: boolean }>();

/**
 * Run `fn` holding the lock on builds at `physicalTableName`. It is the last
 * lock in the order (run/README.md): nothing else is taken while it is held,
 * which {@link assertNoOwnedNameHeld} checks.
 */
export async function withOwnedNameLock<T>(
   physicalTableName: string,
   fn: () => Promise<T>,
): Promise<T> {
   // Nested, it would wait on itself for the same name.
   assertNoOwnedNameHeld("owned-name");
   return OWNED_NAME_LOCK.withLock(leafKey(physicalTableName), async () => {
      const hold = { held: true };
      try {
         return await ownedNameHolds.run(hold, fn);
      } finally {
         hold.held = false;
      }
   });
}

/** A lock taken out of the run's order (run/README.md); thrown under test. */
export class LockOrderError extends Error {
   constructor(message: string) {
      super(message);
      this.name = "LockOrderError";
   }
}

/**
 * Refuse, under test, taking the `lock` lock while an owned-name lock is
 * held, which comes after it in the lock order; logged otherwise. A
 * best-effort catch rethrows the refusal, so a test sees it.
 */
export function assertNoOwnedNameHeld(lock: string): void {
   if (!ownedNameHolds.getStore()?.held) return;
   const message = `Lock order: the ${lock} lock was taken while an owned-name lock was held`;
   if (externalEnv("NODE_ENV") === "test") throw new LockOrderError(message);
   logger.error(message);
}

/**
 * Whether the author chose the table's name: an explicit `name=` on a
 * `#@ persist` source, a persisted view or a `#(materialize)` declaration. A
 * rollup's name (its namespace and grain digest) and a `#(materialize)`
 * table that defaults to its source's name are Publisher's.
 */
export function hasAuthorTableName(
   compiled: Pick<RunPlan, "tables">,
   persistSource: PersistSource,
): boolean {
   const table = compiled.tables[persistSource.sourceID];
   if (table?.rollup) return false;
   const materialize = table?.materialize;
   if (materialize) return !materialize.nameDefaulted;
   return !!deriveAnnotationFields(persistSource).name;
}

/** URI-encoded, the single quote included, so no value needs escaping. */
function encodeMarkValue(value: string): string {
   return encodeURIComponent(value).replace(/'/g, "%27");
}

/**
 * The ownership mark for a table `owner` (`<environment>/<package>`) wrote,
 * naming where the table is (`table`: its `location` as the ownership probe
 * reads it from the warehouse's catalog, fully qualified). Without `table`
 * it is the mark releases before 0.16.0 wrote, or one written where the
 * location could not be read, which a caller's drop does not accept as proof
 * (see {@link markedFor}).
 */
export function ownedTableComment(owner: string, table?: string): string {
   const mark = `${OWNED_TABLE_MARK} owner=${encodeMarkValue(owner)}`;
   return table === undefined
      ? mark
      : `${mark} table=${encodeMarkValue(table)}`;
}

/**
 * Where the one table among the probe's `rows` is, as the warehouse's catalog
 * spells it ({@link relationLocationOf}), or undefined when the rows
 * hold no table, more than one, or none with a location.
 */
export function tableLocationOf(
   rows: Record<string, unknown>[],
): string | undefined {
   const tables = rows.filter(
      (row) => String(field(row, "kind") ?? "").toUpperCase() !== "VIEW",
   );
   if (tables.length !== 1) return undefined;
   return relationLocationOf(tables[0]);
}

/**
 * Where a probe row's relation is, as a mark records it: its catalog
 * location's parts (`location_database`, absent on MySQL, then
 * `location_schema` and `location_name`), each URI-encoded with its dots
 * too, joined with `.`, so a database or schema name holding a dot cannot
 * spell the same location as another. Undefined when the row has no schema
 * or name: that location cannot be read, and proves nothing.
 */
export function relationLocationOf(
   row: Record<string, unknown>,
): string | undefined {
   const part = (key: string) => {
      const value = field(row, key);
      return typeof value === "string" && value !== "" ? value : undefined;
   };
   const schema = part("location_schema");
   const name = part("location_name");
   if (schema === undefined || name === undefined) return undefined;
   const database = part("location_database");
   return [...(database === undefined ? [] : [database]), schema, name]
      .map((p) => encodeURIComponent(p).replace(/\./g, "%2E"))
      .join(".");
}

/** The dialects whose tables' holders can be read (and marked). */
function tableOwnershipDialectSupported(dialect: string): boolean {
   return capabilitiesOf(dialect).tableOwnership;
}

/** The DDL that marks the table `segments` names as `owner`'s. */
export function ownedTableMarkSql(
   segments: string[],
   owner: string,
   dialect: string,
   location?: string,
): string {
   return tableCommentSql(
      segments,
      ownedTableComment(owner, location),
      dialect,
   );
}

/** What holds a table's name, as `owner` sees it. */
export type TableHolder =
   | { state: "free" }
   /**
    * A table marked as `owner`'s, or (`view`) `owner`'s own `name=` view an
    * earlier release left, which a build of the table replaces.
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

/**
 * {@link tableHolderOf} for a caller's drop, which accepts a mark as proof
 * only on the very table it names: `ours` becomes `unmarked` (proof must
 * come from a run record) unless every table row's mark names that row's
 * own location ({@link relationLocationOf}), the place the warehouse's catalog reports
 * for it (wherever a search path or default schema resolved the name). A
 * renamed, moved or copied table carries the mark of the table it came from,
 * which names another place; a mark an earlier release wrote names none, and
 * a row with no location (the probe could not read one) proves nothing. A
 * build and a reclaim read the owner alone, as before.
 */
export function markedFor(
   rows: Record<string, unknown>[],
   owner: string,
): TableHolder {
   const holder = tableHolderOf(rows, owner);
   if (holder.state !== "ours" || holder.view) return holder;
   for (const row of rows) {
      if (String(field(row, "kind") ?? "").toUpperCase() === "VIEW") continue;
      const location = relationLocationOf(row);
      const marked = parseOwnedTableMark(field(row, "comment"))?.table;
      if (location === undefined || marked !== location) {
         return { state: "unmarked" };
      }
   }
   return holder;
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
   site: Pick<TableSite, "dialect" | "segments" | "run">,
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
 * connection or destination cannot be reached, which refuses too. Returns
 * whether the name holds `owner`'s own leftover `name=` view, which the
 * caller drops before it builds (leftover_name_views.ts).
 */
export async function assertTableOwnable(
   site: Pick<TableSite, "dialect" | "segments" | "run"> | undefined,
   target: OwnedTableTarget,
   owner: string,
   sourceName: string,
   ownRecords: () => Promise<boolean>,
): Promise<{ ownView: boolean }> {
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
   if (hasReservedTableSuffix(target.physicalTableName)) {
      throw new TableOwnershipError(
         TABLE_OWNED_ELSEWHERE,
         `'${sourceName}' is not built (${TABLE_OWNED_ELSEWHERE}): its name= ` +
            `'${target.physicalTableName}' ends the way Publisher names a ` +
            `package's or a version's own tables (__p<hash>, __v<version>_<hash>), ` +
            `so it may be another package's table. It is answered live. ` +
            `Give the source another name=.`,
      );
   }
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
      if (await ownRecords()) return { ownView: false };
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
   return { ownView: holder.state === "ours" && holder.view === true };
}

/**
 * Mark the table `physicalTableName` names at `site` as `owner`'s.
 * Best-effort, never throws: an unmarked table stays this package's by its
 * records once the run commits.
 */
export async function markOwnedTable(
   site: Pick<TableSite, "dialect" | "segments" | "run">,
   physicalTableName: string,
   owner: string,
): Promise<void> {
   try {
      const segments = site.segments(physicalTableName);
      // Where the table actually is, read from the warehouse's catalog, so
      // the mark names it however the name resolved. Unread (an unsupported
      // dialect, a failed probe): the mark names no place, and proves
      // nothing to a caller's drop.
      let location: string | undefined;
      if (tableOwnershipDialectSupported(site.dialect)) {
         location = await site
            .run(ownedTableProbeSql(segments, site.dialect))
            .then(tableLocationOf, () => undefined);
      }
      await site.run(
         ownedTableMarkSql(segments, owner, site.dialect, location),
      );
   } catch (err) {
      logger.warn(
         "Could not mark a built table as this package's; its run's records say so instead",
         { physicalTableName, owner, code: errorCodeOf(err) },
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
   site: Pick<TableSite, "dialect" | "segments" | "run">,
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
      // Unknown: the caller drops.
   }
   return undefined;
}

/**
 * Whether a table (not a view) named `physicalTableName` exists at `site`,
 * or undefined when the probe cannot tell (a dialect it cannot ask, or a
 * failed probe). Never throws.
 */
export async function tableExistsAt(
   site: Pick<TableSite, "dialect" | "segments" | "run">,
   physicalTableName: string,
): Promise<boolean | undefined> {
   if (!tableOwnershipDialectSupported(site.dialect)) return undefined;
   try {
      const rows = await site.run(
         ownedTableProbeSql(site.segments(physicalTableName), site.dialect),
      );
      return rows.some(
         (row) => String(field(row, "kind") ?? "").toUpperCase() !== "VIEW",
      );
   } catch {
      return undefined;
   }
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
   context: TableSiteContext,
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
            (await atTableSite(target, context, async (site) => {
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
