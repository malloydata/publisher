// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Mutex } from "async-mutex";
import { MaterializationConflictError } from "../../../errors";
import { logger } from "../../../logger";
import { ResourceRepository } from "../../../storage/DatabaseInterface";
import { errMessage } from "../../../utils";
import {
   isVersionOwnedRun,
   SUPERSEDED_TABLES_KEY,
   tableIdentity,
   tableNamesIn,
   WRITES_TABLES_KEY,
   WRITES_TARGETS_KEY,
   type WriteTarget,
} from "../../versions/materialization_scope";
import { BuildInstruction } from "../domain/types";

/**
 * Which tables an active run of a package writes, so two runs never write one
 * table at once. A run records the tables it writes on its own metadata
 * (WRITES_TABLES_KEY); the checks and the records are made under one lock per
 * package, in-process: one publisher owns its store.
 */
export class TableClaims {
   private readonly tableLocks = new Map<string, Mutex>();

   constructor(private readonly repository: () => ResourceRepository) {}

   /**
    * The lock under which a published version's run checks and records the
    * tables it writes, per package. Runs of different versions hold
    * different active slots, so this is what keeps two of them from writing
    * one table at once. In-process: one publisher owns its store.
    */
   lockFor(environmentId: string, packageName: string): Mutex {
      const key = `${environmentId}|${packageName}`;
      let lock = this.tableLocks.get(key);
      if (!lock) {
         lock = new Mutex();
         this.tableLocks.set(key, lock);
      }
      return lock;
   }

   /**
    * Refuse tables another active run of the package writes, and, for a run
    * with instructions, the tables the package's versions share (the
    * newest auto-run's): those are rebuilt only by an auto-run of `latest`,
    * which marks what it makes stale. Called under lockFor.
    */
   async assertTablesFree(
      environmentId: string,
      packageName: string,
      tables: string[],
      excludeId: string | undefined,
      instructed: boolean,
   ): Promise<void> {
      // Compared by what a name names (see tableIdentity), not its spelling.
      const wanted = new Set(tables.map(tableIdentity));
      if (wanted.size === 0) return;
      const runs = await this.repository().listMaterializations(
         environmentId,
         packageName,
      );
      for (const m of runs) {
         if (m.id === excludeId) continue;
         if (m.status !== "PENDING" && m.status !== "MANIFEST_ROWS_READY") {
            continue;
         }
         const clash = tableNamesIn(m.metadata, WRITES_TABLES_KEY).find((t) =>
            wanted.has(tableIdentity(t)),
         );
         if (clash !== undefined) {
            throw new MaterializationConflictError(
               `Table ${clash} is being written by materialization ${m.id}${m.version ? ` of version ${m.version}` : ""} of package ${packageName}; two runs never write one table at once.`,
            );
         }
      }
      if (!instructed) return;
      // Every committed run the package's versions share: neither one
      // version's own nor one version's instructed run. Runs from before the
      // package's first versioned publish count, instructed ones included:
      // every version may serve them.
      const shared = runs
         .filter(
            (m) =>
               !isVersionOwnedRun(m) &&
               (m.metadata?.mode !== "orchestrated" || m.version === null) &&
               m.status === "MANIFEST_FILE_READY",
         )
         .flatMap((m) =>
            Object.values(m.manifest?.entries ?? {}).map(
               (e) => e.physicalTableName,
            ),
         );
      const clash = shared.find((t) => wanted.has(tableIdentity(t)));
      if (clash !== undefined) {
         throw new MaterializationConflictError(
            `Table ${clash} is one the versions of package ${packageName} share, which only an auto-run of its latest version rebuilds; name tables of your own in buildInstructions.`,
         );
      }
   }

   /**
    * Before an auto-run of a version builds the tables in `instructions`:
    * refuse them if another active run writes one, and record them as this
    * run's (WRITES_TABLES_KEY), under the package's table lock. For the
    * shared tables of a `scope: package` version (`supersede`), also mark
    * every other run's entries for them superseded (SUPERSEDED_TABLES_KEY),
    * in the same lock hold.
    */
   async claimTables(
      environmentId: string,
      packageName: string,
      id: string,
      instructions: BuildInstruction[],
      {
         supersede,
         targets,
      }: { supersede: boolean; targets?: WriteTarget[] } = { supersede: true },
   ): Promise<void> {
      const tables = [...new Set(instructions.map((i) => i.physicalTableName))];
      await this.lockFor(environmentId, packageName).runExclusive(async () => {
         await this.assertTablesFree(
            environmentId,
            packageName,
            tables,
            id,
            false,
         );
         const run = await this.repository().getMaterializationById(id);
         await this.repository().updateMaterialization(id, {
            metadata: {
               ...(run?.metadata ?? {}),
               [WRITES_TABLES_KEY]: tables,
               ...(targets ? { [WRITES_TARGETS_KEY]: targets } : {}),
            },
         });
         if (!supersede) return;
         await this.markSuperseded(
            environmentId,
            packageName,
            id,
            instructions,
         );
      });
   }

   /**
    * Mark, on every other run of the package, the entries a run about to
    * write `instructions` makes stale: an entry naming one of those tables
    * built from another definition (another content address) no longer says
    * what the table holds, whatever happens to this run. An entry from the
    * same definition still does, and is left alone; so is a run already
    * marked. Called under lockFor.
    */
   async markSuperseded(
      environmentId: string,
      packageName: string,
      id: string,
      instructions: BuildInstruction[],
   ): Promise<void> {
      // Keyed by what a name names (see tableIdentity), not its spelling.
      const rebuilt = new Map<string, Set<string>>();
      for (const i of instructions) {
         const table = tableIdentity(i.physicalTableName);
         const addresses = rebuilt.get(table) ?? new Set<string>();
         addresses.add(i.sourceEntityId);
         rebuilt.set(table, addresses);
      }
      for (const m of await this.repository().listMaterializations(
         environmentId,
         packageName,
      )) {
         if (m.id === id) continue;
         const marked = tableNamesIn(m.metadata, SUPERSEDED_TABLES_KEY);
         const already = new Set(marked.map(tableIdentity));
         const stale = Object.values(m.manifest?.entries ?? {})
            .filter((e) => {
               const table = tableIdentity(e.physicalTableName);
               const addresses = rebuilt.get(table);
               return (
                  addresses !== undefined &&
                  !addresses.has(e.sourceEntityId) &&
                  !already.has(table)
               );
            })
            .map((e) => e.physicalTableName);
         if (stale.length === 0) continue;
         try {
            await this.repository().updateMaterialization(m.id, {
               metadata: {
                  ...(m.metadata ?? {}),
                  [SUPERSEDED_TABLES_KEY]: [...new Set([...marked, ...stale])],
               },
            });
         } catch (err) {
            // Deleted meanwhile: nothing of it is read any more.
            logger.debug("Could not mark a run's entries superseded", {
               materializationId: m.id,
               error: errMessage(err),
            });
         }
      }
   }

   /**
    * After run `id` committed a `name=` view in place of a table this
    * package's own run wrote at that name (name_view.ts): mark, on every
    * other run, the entries naming that table there superseded
    * (SUPERSEDED_TABLES_KEY). The table is gone, so no reader may serve or
    * reuse it again; the dropper retires those entries when their run goes
    * (heldByOwnNameView). Matched exactly: the same name, connection and
    * destination. Best-effort: a mark that fails is logged.
    */
   async markReplacedByView(
      environmentId: string,
      packageName: string,
      id: string,
      tables: {
         physicalTableName: string;
         connectionName: string;
         storageDestinationName?: string;
      }[],
   ): Promise<void> {
      await this.markTablesGone(environmentId, packageName, id, tables);
   }

   /**
    * When an auto-run finds a table its package's records name held by
    * another package, or cannot tell who holds it (table_ownership.ts): mark,
    * on every run of the package but `id`, the entries naming that table
    * there superseded (SUPERSEDED_TABLES_KEY), so the package serves the
    * source live rather than another package's rows, and never reuses the
    * entry. Matched exactly, as {@link markReplacedByView}. Best-effort: a
    * mark that fails is logged.
    */
   async markHeldElsewhere(
      environmentId: string,
      packageName: string,
      id: string,
      table: {
         physicalTableName: string;
         connectionName: string;
         storageDestinationName?: string;
      },
   ): Promise<void> {
      await this.markTablesGone(environmentId, packageName, id, [table]);
   }

   /** Mark every other run's entries for `tables`, exactly, superseded. */
   private async markTablesGone(
      environmentId: string,
      packageName: string,
      id: string,
      tables: {
         physicalTableName: string;
         connectionName: string;
         storageDestinationName?: string;
      }[],
   ): Promise<void> {
      if (tables.length === 0) return;
      await this.lockFor(environmentId, packageName).runExclusive(async () => {
         for (const m of await this.repository().listMaterializations(
            environmentId,
            packageName,
         )) {
            if (m.id === id) continue;
            const marked = tableNamesIn(m.metadata, SUPERSEDED_TABLES_KEY);
            const replaced = Object.values(m.manifest?.entries ?? {})
               .filter((e) =>
                  tables.some(
                     (t) =>
                        t.physicalTableName === e.physicalTableName &&
                        t.connectionName === e.connectionName &&
                        (t.storageDestinationName ?? undefined) ===
                           (e.storageDestinationName ?? undefined),
                  ),
               )
               .map((e) => e.physicalTableName)
               .filter((t) => !marked.includes(t));
            if (replaced.length === 0) continue;
            try {
               await this.repository().updateMaterialization(m.id, {
                  metadata: {
                     ...(m.metadata ?? {}),
                     [SUPERSEDED_TABLES_KEY]: [
                        ...new Set([...marked, ...replaced]),
                     ],
                  },
               });
            } catch (err) {
               logger.warn(
                  "Could not retire a run's entry for a table it no longer holds",
                  { materializationId: m.id, error: errMessage(err) },
               );
            }
         }
      });
   }
}
