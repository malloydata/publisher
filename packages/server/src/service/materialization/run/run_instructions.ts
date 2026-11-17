// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The build instructions a run takes: an auto-run's own, derived from the
 * compiled plan with readable table names and carry-forward of unchanged
 * sources ({@link deriveSelfInstructions}); a host's, checked against the plan
 * ({@link validateInstructions}); and where each instructed table lives
 * ({@link writeTargetsFor}). Called by the materialization service.
 *
 * Reads the plan and the gates and writes nothing; logs and counts what it
 * refuses. A source the eligibility gate refuses is returned as refused and serves live.
 */

import { freshnessOf } from "../domain/freshness";
import type { PersistSource } from "@malloydata/malloy";
import * as path from "path";
import { fileURLToPath } from "url";
import { tableNamesFoldCase } from "../domain/storage_table_path";
import {
   authorModelPath,
   namesBefore,
   readableTableName,
   readableVersion,
   sourceLabel,
   hasReservedTableSuffix,
   tableNameParts,
   tableNamesMeet,
} from "../domain/table_name";
import { isPersistedQuerySourceID } from "../domain/table_plans";
import { getPersistStorageMode } from "../../../config";
import {
   BadRequestError,
   MaterializationEligibilityError,
} from "../../../errors";
import { logger } from "../../../logger";
import { recordEligibilityRefused } from "../../../materialization_metrics";
import { errMessage } from "../../../utils";
import {
   packageTableSuffix,
   tableIdentity,
   versionedTableName,
   versionTableSuffix,
   type WriteTarget,
} from "../../versions/materialization_scope";
import {
   computeSourceEntityId,
   foldDistinctTableNames,
   planAddresses,
   sharedTables,
   storedColumnNames,
   tierGateInputs,
} from "../plan/build_plan";
import {
   type CompiledBuildPlan,
   sourcesInBuildOrder,
} from "../domain/compiled_build_plan";
import {
   assertPlacementNamed,
   assertTierEligible,
} from "../plan/materialization_eligibility";
import {
   BuildInstruction,
   BuildPlan,
   LedgerEntry,
   ManifestEntry,
   RefusedSource,
} from "../domain/types";
import {
   sourceIncrementalRefresh,
   type IncrementalRunContext,
} from "./incremental_build";
import type { EligibilityTier } from "./materialization_instructed_run";
import {
   allSourcesRefusedError,
   declaredStorage,
   refusalRecord,
   resolveStorageDestination,
   selfAssignTableName,
   STAGING_ID_LEN,
   tableKeyOf,
} from "./materialization_run_support";
import { hasAuthorTableName } from "./table_ownership";

/**
 * Whose tables an auto-run names (see deriveSelfInstructions). Absent: the
 * names as written, as only a test seam asks for.
 */
export interface SelfTableNaming {
   environmentName: string;
   packageName: string;
   /** The version that owns the run's tables, under `scope: version`. */
   ownedVersion?: string;
   /**
    * Where the package is on this server, which a source's declaring model
    * path is read relative to (see modelPathOf).
    */
   packagePath?: string;
}

/**
 * A table an auto-run built under a new name or in a new place, in place of
 * the table an earlier run built for the same source (the same content
 * address) on the same connection: its name changed, or its `storage=`
 * destination did. Once the run commits, the old table is reclaimed when
 * Publisher can prove it is its own, and is otherwise kept, and listed as
 * kept when it sits at an author's `name=` (Reclaimer.reclaimRenamedTables).
 */
export interface RenamedTable {
   sourceEntityId: string;
   /** The source the table is built for, which a kept table's warning names. */
   sourceName?: string;
   /** The earlier run's table. */
   previousName: string;
   /** The table this run builds instead. */
   physicalTableName: string;
   connectionName: string;
   /** Where this run builds the table: its storage destination, if any. */
   storageDestinationName?: string;
   /**
    * Where the earlier table is, when that is not where this run builds:
    * its storage destination, or none for the source's own database. Absent
    * when the place is the same.
    */
   previousPlace?: { storageDestinationName?: string };
}

/**
 * Whether two written names, in one place, may be one table: compared
 * unquoted and case-folded, with {@link tableNamesMeet}'s container rule. The
 * conservative answer, for deciding a table is NOT another: an over-wide
 * match only keeps a table.
 */
export function mayBeOneTable(a: string, b: string): boolean {
   const canon = (name: string) =>
      name
         .split(".")
         .map((s) => s.replace(/["`[\]]/g, "").toLowerCase())
         .join(".");
   return tableNamesMeet(canon(a), canon(b));
}

/**
 * A written name's table segment, unquoted and case-folded: the key a
 * reclaim or a run's delete compares names by. Containers are left out, since
 * a container does not say which table a name reaches (on DuckDB a two-part
 * name may be a catalog's or a schema's: `memory.t` and `main.t` are one
 * table), and nor does a place (two names for one DuckLake).
 */
export function tableLeafKey(name: string): string {
   const parts = name.split(".");
   return parts[parts.length - 1].replace(/["`[\]]/g, "").toLowerCase();
}

/**
 * Whether two written names may be one table wherever each is, compared by
 * table segment alone ({@link tableLeafKey}). The conservative answer for
 * deciding whether to drop a table: an over-wide match only keeps one.
 */
export function mayBeOneTableAnywhere(a: string, b: string): boolean {
   return tableLeafKey(a) === tableLeafKey(b);
}

/*
 * A run's build instructions: derived by auto-run from the compiled plan, or
 * checked when a caller supplies them, and where each instructed table lives.
 */

/** A source the eligibility gate cleared, at its content address. */
interface ClearedSource {
   persistSource: PersistSource;
   sourceEntityId: string;
   destination: string | undefined;
}

/**
 * The persist sources a run may build (respecting `sourceNames`), in build
 * order, and those the eligibility gate refused. Throws when `sourceNames`
 * targets a refused source.
 */
function gateSources(
   compiled: CompiledBuildPlan,
   include: Set<string> | null,
): {
   sources: ClearedSource[];
   refused: Record<string, RefusedSource>;
   refusals: MaterializationEligibilityError[];
   cleared: Map<string, EligibilityTier>;
} {
   const sources: ClearedSource[] = [];
   const refused: Record<string, RefusedSource> = {};
   const refusals: MaterializationEligibilityError[] = [];
   const cleared = new Map<string, EligibilityTier>();
   // A compiled package's plan carries its folds; a hand-made one is decided
   // here, over every source.
   const tableFolds =
      compiled.tableFolds ??
      foldDistinctTableNames(compiled, storedColumnNames);
   // Two definitions writing one table (sharedTables): neither is built,
   // whether or not the other is rebuilt this run or carried.
   const shared = sharedTables({
      ...compiled,
      tableFolds,
      addresses:
         compiled.addresses ?? planAddresses({ ...compiled, tableFolds }),
   });
   for (const build of compiled.builds) {
      for (const persistSource of sourcesInBuildOrder(build, compiled.tables)) {
         if (include && !include.has(persistSource.name)) continue;

         // A source that DECLARES `storage=` must never silently downgrade
         // to a database CTAS into its own warehouse when
         // PERSIST_STORAGE_MODE=off: the author did not ask for that, and
         // production grants read-only warehouse access. It serves LIVE and
         // Package.storageWarnings reports it. A plain `#@ persist` is
         // unaffected; database is its intent.
         if (
            getPersistStorageMode() === "off" &&
            declaredStorage(persistSource)
         ) {
            continue;
         }

         const destination = resolveStorageDestination(persistSource);
         const isRollup = !!compiled.tables[persistSource.sourceID]?.rollup;
         // Gate BEFORE computeSourceEntityId: an unbound parameter or a given
         // makes its getSQL() throw opaquely.
         //
         // A refused source is SKIPPED and recorded, not thrown for, so one
         // ineligible source does not stop the rest of the package refreshing.
         // The run throws only when nothing else is left to build, or when
         // `sourceNames` asked for exactly this source.
         // A database `#@ persist` is just as frozen as a storage build, so
         // an authorize-gated source materialized there would be served to
         // every caller.
         try {
            assertPlacementNamed(persistSource);
            assertTierEligible(
               persistSource,
               !!destination,
               tierGateInputs(compiled),
            );
            // Two sources an author's name= puts on one table: refused
            // alone, never built, so neither overwrites the other (only
            // reachable under PERSIST_COLLISION_ENFORCE=false; otherwise the
            // package does not load).
            const sharing = shared.get(persistSource.sourceID);
            if (sharing !== undefined) {
               throw new MaterializationEligibilityError({
                  reason: "table_name_collision",
                  message: sharing,
               });
            }
         } catch (err) {
            if (!(err instanceof MaterializationEligibilityError)) throw err;
            if (include) throw err;
            if (!refused[persistSource.sourceID]) {
               refused[persistSource.sourceID] = refusalRecord(
                  persistSource,
                  err,
                  destination ? "storage" : "database",
                  compiled.tables[persistSource.sourceID]?.modelPath,
                  compiled.tables[persistSource.sourceID]?.kind,
               );
               // A refused rollup never fails the run: the author asked for
               // nothing they can see.
               if (!isRollup) refusals.push(err);
               logger.warn(
                  "Skipping a persist source the eligibility gate refuses",
                  {
                     sourceName: persistSource.name,
                     reason: errMessage(err),
                  },
               );
            }
            continue;
         }
         cleared.set(
            persistSource.sourceID,
            destination ? "storage" : "database",
         );
         sources.push({
            persistSource,
            sourceEntityId: computeSourceEntityId(
               persistSource,
               compiled.connectionDigests,
               undefined,
               tableFolds,
            ),
            destination,
         });
      }
   }
   return { sources, refused, refusals, cleared };
}

/**
 * The package-relative path of the model that declares `persistSource`, read
 * from the file its sourceID names (a generated companion's being its
 * author's file), so it never depends on which model imported the source.
 * Without `packagePath`, or for a sourceID that names no file in the package:
 * the path the plan recorded, else the file's base name.
 */
function modelPathOf(
   compiled: CompiledBuildPlan,
   persistSource: PersistSource,
   packagePath: string | undefined,
): string {
   const at = persistSource.sourceID.indexOf("@");
   const url = at >= 0 ? persistSource.sourceID.slice(at + 1) : "";
   if (packagePath && url.startsWith("file:")) {
      try {
         const file = authorModelPath(fileURLToPath(url));
         const relative = path.relative(packagePath, file);
         if (
            relative &&
            !relative.startsWith("..") &&
            !path.isAbsolute(relative)
         ) {
            return relative.split(path.sep).join("/");
         }
      } catch {
         // Not a file URL Node reads: the recorded path below.
      }
   }
   const known = compiled.tables[persistSource.sourceID]?.modelPath;
   if (known) return known;
   return url.slice(url.lastIndexOf("/") + 1);
}

/** The source a table's readable name reads (see sourceLabel). */
function labelOf(
   compiled: CompiledBuildPlan,
   persistSource: PersistSource,
): string {
   const rollup = compiled.tables[persistSource.sourceID]?.rollup;
   return sourceLabel(
      persistSource.name,
      rollup?.baseSourceName && rollup.grainDimensions ? rollup : undefined,
      { persistedQuery: isPersistedQuerySourceID(persistSource.sourceID) },
   );
}

/**
 * Per content address, the source its table is named after: the smallest
 * model path, then source, among the sources that share it, so the name does
 * not depend on build order. The control plane names a shared table the same
 * way.
 */
function namingSources(
   compiled: CompiledBuildPlan,
   sources: ClearedSource[],
   packagePath: string | undefined,
): Map<string, PersistSource> {
   const chosen = new Map<
      string,
      { source: PersistSource; modelPath: string; label: string }
   >();
   for (const { persistSource, sourceEntityId } of sources) {
      const candidate = {
         source: persistSource,
         modelPath: modelPathOf(compiled, persistSource, packagePath),
         label: labelOf(compiled, persistSource),
      };
      const current = chosen.get(sourceEntityId);
      if (!current || namesBefore(candidate, current)) {
         chosen.set(sourceEntityId, candidate);
      }
   }
   return new Map([...chosen].map(([address, c]) => [address, c.source]));
}

/**
 * The table an auto-run writes for `persistSource`. A `name=` the author chose
 * is written as it stands in an unversioned or `scope: package` package.
 * Every other name is Publisher's readable one (domain/table_name.ts), named
 * after `namer` (see namingSources) and placed in the container the name the
 * build source carries gives, if any: a rollup's namespace, or a `name=`'s
 * schema or dataset under `scope: version`, where the `name=`'s last segment
 * also takes the place of `<pkg>__<file>__<source>` before the tail.
 */
function selfTableName(
   compiled: CompiledBuildPlan,
   persistSource: PersistSource,
   namer: PersistSource,
   destination: string | undefined,
   naming: SelfTableNaming | undefined,
): string {
   const written = selfAssignTableName(persistSource);
   if (!naming) return written;
   if (
      naming.ownedVersion === undefined &&
      hasAuthorTableName(compiled, persistSource)
   ) {
      return written;
   }
   const dot = written.lastIndexOf(".");
   const prefixed = hasAuthorTableName(compiled, persistSource);
   // A rollup's `name=` leaf, which its build source does not carry.
   const rollupPrefix = compiled.tables[persistSource.sourceID]?.rollup?.prefix;
   return readableTableName({
      identity: {
         environmentName: naming.environmentName,
         packageName: naming.packageName,
         modelPath: modelPathOf(compiled, namer, naming.packagePath),
         sourceName: namer.name,
         ...(naming.ownedVersion !== undefined
            ? { version: naming.ownedVersion }
            : {}),
      },
      label: labelOf(compiled, namer),
      ...(dot > 0 ? { container: written.slice(0, dot) } : {}),
      ...(prefixed
         ? { prefix: written.slice(dot + 1) }
         : rollupPrefix !== undefined
           ? { prefix: rollupPrefix }
           : {}),
      dialect: persistSource.dialectName,
      inDestination: !!destination,
   });
}

/** A readable name's tail: `__p` and 8 hex. */
const READABLE_TAIL = /__p[0-9a-f]{8}$/;

/**
 * How a persisted query's readable name ended before its label dropped the
 * build source's `__query` (`<query>__query`, sanitized to `<query>_query`).
 */
const LEGACY_QUERY_LABEL_END = "_query";

/**
 * Whether a table an earlier run of this package built for the same source,
 * in the same destination, at `recorded`, keeps that name rather than taking
 * `fresh`: when `recorded` is a readable name this scheme gave it (all
 * sanitized, ending `__p<8 hex>`, under this run's version, if any), in the
 * same container. A `name=` under `scope: version` sets the name's prefix, so
 * its table keeps its name only while that prefix is the same, and removing
 * the `name=` changes it.
 */
function keepsReadableName(
   recorded: string,
   fresh: string,
   naming: SelfTableNaming,
   compiled: CompiledBuildPlan,
   persistSource: PersistSource,
): boolean {
   if (recorded === fresh) return true;
   const split = (name: string) => {
      const dot = name.lastIndexOf(".");
      return { container: name.slice(0, dot + 1), leaf: name.slice(dot + 1) };
   };
   const was = split(recorded);
   const now = split(fresh);
   if (was.container !== now.container) return false;
   if (!/^[a-z0-9_]+$/.test(was.leaf) || !READABLE_TAIL.test(was.leaf)) {
      return false;
   }
   const head = (leaf: string) => leaf.replace(READABLE_TAIL, "");
   // A persisted query's table named before its label dropped the build
   // source's `__query` (`…__q_query__p…`): renamed, and rebuilt, once.
   if (
      isPersistedQuerySourceID(persistSource.sourceID) &&
      head(was.leaf).endsWith(LEGACY_QUERY_LABEL_END) &&
      !head(now.leaf).endsWith(LEGACY_QUERY_LABEL_END)
   ) {
      return false;
   }
   // A `name=` that sets the prefix (under `scope: version`, or a rollup's
   // in any package) keeps the table's name only while the prefix is the
   // same; a readable name's own prefix (with `__`) stands for none.
   const rollup = compiled.tables[persistSource.sourceID]?.rollup;
   const prefixed =
      rollup?.prefix !== undefined ||
      (!rollup && hasAuthorTableName(compiled, persistSource));
   if (rollup && naming.ownedVersion === undefined) {
      return prefixed
         ? head(was.leaf) === head(now.leaf)
         : head(was.leaf).includes("__");
   }
   if (naming.ownedVersion !== undefined) {
      const version = `__${readableVersion(naming.ownedVersion)}`;
      if (!head(was.leaf).endsWith(version)) return false;
      if (prefixed) return head(was.leaf) === head(now.leaf);
      // A recorded name with no `__` before its version was a `name=`
      // prefix: the `name=` is gone, so the prefix changed.
      if (!head(was.leaf).slice(0, -version.length).includes("__")) {
         return false;
      }
   }
   return true;
}

/**
 * The name Publisher gave `persistSource`'s table before readable names: the
 * name as written with the package's suffix (`__p<8 hex>` of the environment
 * and package), or under `scope: version` the version's, on every name. A
 * `name=` the author chose outside `scope: version` had none, and is
 * undefined here. Re-derived from this package's own identity, so a match
 * proves the table was this package's default-named one.
 */
function previousTableName(
   compiled: CompiledBuildPlan,
   persistSource: PersistSource,
   naming: SelfTableNaming,
): string | undefined {
   const written = selfAssignTableName(persistSource);
   if (naming.ownedVersion !== undefined) {
      return versionedTableName(
         written,
         versionTableSuffix({
            environmentName: naming.environmentName,
            packageName: naming.packageName,
            version: naming.ownedVersion,
         }),
      );
   }
   if (hasAuthorTableName(compiled, persistSource)) return undefined;
   return versionedTableName(
      written,
      packageTableSuffix({
         environmentName: naming.environmentName,
         packageName: naming.packageName,
      }),
   );
}

/**
 * The refusal of a source whose readable table name another source's table
 * already takes in this run (`table_name_collision`): it is not built, and is
 * answered live, so two sources never write one table.
 */
function tableNameCollision(
   compiled: CompiledBuildPlan,
   persistSource: PersistSource,
   tableName: string,
   destination: string | undefined,
): RefusedSource {
   logger.warn(
      "Not building a persist source: its table name is another source's",
      { sourceName: persistSource.name, physicalTableName: tableName },
   );
   recordEligibilityRefused("table_name_collision");
   return refusalRecord(
      persistSource,
      new MaterializationEligibilityError({
         message:
            `'${persistSource.name}' is not built: its table name ` +
            `'${tableName}' is another source's table in this run, so it is ` +
            `answered live.`,
         reason: "table_name_collision",
      }),
      destination ? "storage" : "database",
      compiled.tables[persistSource.sourceID]?.modelPath,
      compiled.tables[persistSource.sourceID]?.kind,
   );
}

/**
 * Each address's fresh name (selfTableName), and whether a name is one
 * another address takes fresh: a table keeps a recorded name only while no
 * other address takes it fresh, so a source renamed beside a new source of
 * its old name never locks the new one out of its table.
 */
function freshTableNames(
   compiled: CompiledBuildPlan,
   sources: ClearedSource[],
   namers: Map<string, PersistSource>,
   naming: SelfTableNaming | undefined,
): {
   freshNames: Map<string, string>;
   takenByAnother: (
      persistSource: PersistSource,
      destination: string | undefined,
      name: string,
      sourceEntityId: string,
   ) => boolean;
} {
   const freshNames = new Map<string, string>();
   const takenFresh = new Map<string, Set<string>>();
   for (const { persistSource, sourceEntityId, destination } of sources) {
      if (freshNames.has(sourceEntityId)) continue;
      const fresh = selfTableName(
         compiled,
         persistSource,
         namers.get(sourceEntityId) ?? persistSource,
         destination,
         naming,
      );
      freshNames.set(sourceEntityId, fresh);
      const key = tableKeyOf(
         persistSource.connectionName,
         destination,
         tableIdentity(fresh),
      );
      takenFresh.set(
         key,
         (takenFresh.get(key) ?? new Set()).add(sourceEntityId),
      );
   }
   return {
      freshNames,
      takenByAnother: (persistSource, destination, name, sourceEntityId) =>
         [
            ...(takenFresh.get(
               tableKeyOf(
                  persistSource.connectionName,
                  destination,
                  tableIdentity(name),
               ),
            ) ?? []),
         ].some((address) => address !== sourceEntityId),
   };
}

/**
 * The table name an auto-run gives one source, and what decided it: whether
 * the author chose it (`name=`), the source's prior entry and whether it is in
 * the same destination, and the name Publisher gave it before readable names
 * when the run moves it off that name.
 */
function tableNameDecision(
   compiled: CompiledBuildPlan,
   persistSource: PersistSource,
   sourceEntityId: string,
   destination: string | undefined,
   priorEntries: Record<string, ManifestEntry>,
   fresh: string,
   takenByAnother: ReturnType<typeof freshTableNames>["takenByAnother"],
   naming: SelfTableNaming | undefined,
): {
   named: boolean;
   prior: ManifestEntry | undefined;
   sameDestination: boolean;
   previous: string | undefined;
   renaming: boolean;
   tableName: string;
} {
   // Orchestrated builds use the host-supplied `physicalTableName`. A
   // storage build replaces its table atomically (`CREATE OR REPLACE` in
   // ONE transaction), and a database one swaps it in, so a rebuild
   // never empties the table it serves and no generation is needed.
   const named =
      !!naming &&
      naming.ownedVersion === undefined &&
      hasAuthorTableName(compiled, persistSource);
   const prior = priorEntries[sourceEntityId];
   const sameDestination =
      !!prior?.physicalTableName &&
      (prior.storageDestinationName ?? undefined) === destination;
   // The table an earlier run built under the name Publisher gave it
   // before readable names (see previousTableName).
   const previous =
      naming && sameDestination && !named
         ? previousTableName(compiled, persistSource, naming)
         : undefined;
   const renaming =
      previous !== undefined &&
      prior!.physicalTableName === previous &&
      previous !== fresh;
   // Sticky: a table an earlier run built under a readable name keeps it,
   // so a moved or renamed file, or a source that comes to share the
   // address, does not rebuild it under another (see keepsReadableName).
   const tableName =
      naming &&
      !named &&
      sameDestination &&
      !renaming &&
      keepsReadableName(
         prior!.physicalTableName,
         fresh,
         naming,
         compiled,
         persistSource,
      ) &&
      !takenByAnother(
         persistSource,
         destination,
         prior!.physicalTableName,
         sourceEntityId,
      )
         ? prior!.physicalTableName
         : fresh;
   return {
      named,
      prior,
      sameDestination,
      previous,
      renaming,
      tableName,
   };
}

/**
 * The table an earlier run built for this source that the table this run
 * builds (`built`, in `destination`) replaces, as a {@link RenamedTable}, or
 * undefined when there is none or it may be the same table. The earlier
 * table must be on the source's connection: a connection change is a new
 * content address, and out of scope.
 */
function supersededTable(
   prior: ManifestEntry | undefined,
   persistSource: PersistSource,
   sourceEntityId: string,
   destination: string | undefined,
   built: string,
): RenamedTable | undefined {
   if (!prior?.physicalTableName) return undefined;
   if (prior.connectionName !== persistSource.connectionName) return undefined;
   // The bare name a run built before package-owned names (the source's
   // own name, unsuffixed), which another package on the same schema may
   // have written since: not provably this package's, nor an author's
   // name=, so neither reclaimed nor listed.
   if (
      !hasReservedTableSuffix(prior.physicalTableName) &&
      mayBeOneTable(
         tableNameParts(prior.physicalTableName).leaf,
         persistSource.name,
      )
   ) {
      return undefined;
   }
   const before = prior.storageDestinationName ?? undefined;
   const moved = before !== destination;
   if (!moved && mayBeOneTable(prior.physicalTableName, built)) {
      return undefined;
   }
   return {
      sourceEntityId,
      sourceName: persistSource.name,
      previousName: prior.physicalTableName,
      physicalTableName: built,
      connectionName: persistSource.connectionName,
      ...(destination ? { storageDestinationName: destination } : {}),
      ...(moved
         ? {
              previousPlace: before ? { storageDestinationName: before } : {},
           }
         : {}),
   };
}

/**
 * `renamed` less any old table this run writes or carries in the same place
 * under a name that may be the same table: it is another source's now, or
 * this source's own again, and never offered for a drop.
 */
function notWrittenThisRun(
   renamed: RenamedTable[],
   instructions: BuildInstruction[],
   carried: Record<string, ManifestEntry>,
   sources: ClearedSource[],
): RenamedTable[] {
   const connectionOf = new Map(
      sources.map((s) => [s.sourceEntityId, s.persistSource.connectionName]),
   );
   const written = [
      ...instructions.map((i) => ({
         connectionName: connectionOf.get(i.sourceEntityId),
         storageDestinationName: i.destination,
         physicalTableName: i.physicalTableName,
      })),
      ...Object.values(carried).map((e) => ({
         connectionName: e.connectionName,
         storageDestinationName: e.storageDestinationName ?? undefined,
         physicalTableName: e.physicalTableName,
      })),
   ];
   return renamed.filter((t) => {
      const where = previousDestinationOf(t);
      return !written.some(
         (w) =>
            w.connectionName === t.connectionName &&
            (w.storageDestinationName ?? undefined) === where &&
            mayBeOneTable(w.physicalTableName, t.previousName),
      );
   });
}

/** The storage destination a {@link RenamedTable}'s earlier table is in, if any. */
export function previousDestinationOf(
   t: Pick<RenamedTable, "storageDestinationName" | "previousPlace">,
): string | undefined {
   return t.previousPlace
      ? t.previousPlace.storageDestinationName
      : t.storageDestinationName;
}

/**
 * Derive auto-run's own build instructions. Each persist source (respecting
 * `sourceNames`) gets a self-assigned physical name and COPY realization,
 * unless its sourceEntityId is unchanged since `priorEntries`, in which case
 * it is carried forward.
 *
 * An INCREMENTAL source is exempt from carry-forward: unchanged SQL says
 * nothing about whether its DATA moved, so carrying it would never reach its
 * delta. The ledger decides instead, and "nothing new" is a cheap skip. So is
 * a table past its declared freshness window (`pastWindow`): carried, it
 * would stay past it until a host or a forced run rebuilt it.
 *
 * A source the eligibility gate refuses is returned in `refused`. It throws
 * instead when `sourceNames` targets it, or when every authored source the
 * run targets was refused. A refused rollup never causes the throw.
 *
 * `cleared` names the tier each source passed the gate for, so the build loop
 * consults that verdict rather than walking the source again.
 *
 * A source whose readable name is another address's (two sources of one
 * model that share a name, or a hash collision) is not built, and serves
 * live: two sources never write one table. It is returned in `refused`,
 * `table_name_collision`. A recorded name another address takes fresh is
 * never kept (see takenByAnother), so a sticky name does not cause one.
 */
export function deriveSelfInstructions(
   compiled: CompiledBuildPlan,
   sourceNames: string[] | undefined,
   priorEntries: Record<string, ManifestEntry>,
   incremental?: IncrementalRunContext,
   naming?: SelfTableNaming,
   /**
    * Rebuild every source (a scheduled fire, `forceRefresh`): nothing is
    * carried. `priorEntries` still decide each table's name. `pastWindow`
    * names a prior table past its declared freshness window, which is
    * rebuilt rather than carried (freshness_stamp.ts); absent, none is.
    */
   options: {
      forceRefresh?: boolean;
      pastWindow?: (sourceEntityId: string, prior: ManifestEntry) => boolean;
      /**
       * What a rebuilt table with a declared window reads: a carried table
       * it reads (through the plan's edges, transitively) whose age is
       * unknown or past that window is rebuilt too (see
       * {@link rebuildStaleUpstreams}).
       */
      upstreams?: StaleUpstreamRule;
   } = {},
): {
   instructions: BuildInstruction[];
   carried: Record<string, ManifestEntry>;
   refused: Record<string, RefusedSource>;
   cleared: Map<string, EligibilityTier>;
   /**
    * Carried tables at a `name=` the author chose, by content address, with
    * the instruction that rebuilds each: carried only while the table is
    * still marked as this package's (see uncarryUnownedTables).
    */
   recheck: Record<string, BuildInstruction>;
   /** Tables this run builds under a new name in place of an earlier run's. */
   renamed: RenamedTable[];
} {
   const include = sourceNames ? new Set(sourceNames) : null;
   const instructions: BuildInstruction[] = [];
   const carried: Record<string, ManifestEntry> = {};
   const recheck: Record<string, BuildInstruction> = {};
   const renamed: RenamedTable[] = [];
   const { sources, refused, refusals, cleared } = gateSources(
      compiled,
      include,
   );
   const namers = namingSources(compiled, sources, naming?.packagePath);
   const { freshNames, takenByAnother } = freshTableNames(
      compiled,
      sources,
      namers,
      naming,
   );
   const seen = new Set<string>();
   // The instruction that would rebuild each carried table.
   const carriedInstructions = new Map<string, BuildInstruction>();
   // Readable names this run writes, by where they live, and whose.
   const derived = new Map<string, string>();

   for (const { persistSource, sourceEntityId, destination } of sources) {
      if (seen.has(sourceEntityId)) continue;
      seen.add(sourceEntityId);

      const { named, prior, sameDestination, previous, renaming, tableName } =
         tableNameDecision(
            compiled,
            persistSource,
            sourceEntityId,
            destination,
            priorEntries,
            freshNames.get(sourceEntityId)!,
            takenByAnother,
            naming,
         );
      if (naming && !named) {
         const key = tableKeyOf(
            persistSource.connectionName,
            destination,
            tableIdentity(tableName),
         );
         const holder = derived.get(key);
         if (holder !== undefined && holder !== sourceEntityId) {
            refused[persistSource.sourceID] = tableNameCollision(
               compiled,
               persistSource,
               tableName,
               destination,
            );
            cleared.delete(persistSource.sourceID);
            continue;
         }
         derived.set(key, sourceEntityId);
      }

      // The SAME predicate the build's dispatch uses, so the two never
      // disagree about which sources are incremental. A declaration the
      // delta path cannot act on (unsupported dialect, unusable watermark)
      // keeps skip-if-unchanged rather than rebuilding fully every run.
      const deltaEligible =
         sourceIncrementalRefresh(incremental, persistSource, {
            physicalTableName: tableName,
            storageDestinationName: destination,
            sourceEntityId,
         }) !== undefined;

      // An incremental source keeps a table named before readable names
      // until it is reseeded: its ledger is keyed by the table's name, so
      // moving it would re-seed it in full, the costliest build there is.
      // Any other source is rebuilt under its readable name, and the old
      // table reclaimed once the run commits and is served.
      const logicalName =
         renaming && deltaEligible && !incremental?.forceRefresh
            ? previous!
            : tableName;

      const instruction: BuildInstruction = {
         sourceEntityId,
         materializedTableId: `local-${sourceEntityId.substring(
            0,
            STAGING_ID_LEN,
         )}`,
         physicalTableName: logicalName,
         realization: "COPY",
         ...(destination ? { destination } : {}),
      };
      // Carry forward only within the SAME destination, and only a table
      // at the name this run would write: sourceEntityId encodes neither.
      // A source that changes `storage=`, or whose table name changes (a
      // `name=` removed or changed, a v1 source moved to `#(materialize)`,
      // a table named before readable names, or the unsuffixed table a run
      // before package-owned names built, which another package on the same
      // schema may have written since) is rebuilt under its name now, and
      // the old table is offered to the reclaim once this run commits
      // (supersededTable), which drops it only when Publisher can prove it
      // is its own.
      if (
         !deltaEligible &&
         !options.forceRefresh &&
         sameDestination &&
         !options.pastWindow?.(sourceEntityId, prior!) &&
         samePath(
            prior!.physicalTableName,
            logicalName,
            tableNamesFoldCase(persistSource.dialectName, !!destination),
         )
      ) {
         carried[sourceEntityId] = prior!;
         carriedInstructions.set(sourceEntityId, instruction);
         // A table at the author's `name=` may have been written by
         // another package since (or before the mark existed).
         if (named && prior!.physicalTableName === logicalName) {
            recheck[sourceEntityId] = instruction;
         }
         continue;
      }

      // The table this one replaces, decided once the run commits
      // (Reclaimer.reclaimRenamedTables).
      const superseded =
         naming &&
         supersededTable(
            prior,
            persistSource,
            sourceEntityId,
            destination,
            logicalName,
         );
      if (superseded) renamed.push(superseded);
      instructions.push(instruction);
   }

   if (options.upstreams) {
      rebuildStaleUpstreams(
         instructions,
         carried,
         carriedInstructions,
         recheck,
         options.upstreams,
      );
   }

   if (
      refusals.length > 0 &&
      instructions.length === 0 &&
      Object.keys(carried).length === 0
   ) {
      throw allSourcesRefusedError(refusals);
   }

   return {
      instructions,
      carried,
      refused,
      cleared,
      recheck,
      renamed: notWrittenThisRun(renamed, instructions, carried, sources),
   };
}

/** What {@link rebuildStaleUpstreams} reads. */
interface StaleUpstreamRule {
   windowSecondsAt(sourceEntityId: string): number | undefined;
   /** The tables a table at `sourceEntityId` is built from (the plan's edges). */
   upstreamsOf(sourceEntityId: string): readonly string[];
   /** The run's start, which a carried table is aged at. */
   at: Date;
}

/**
 * Rebuild, rather than carry, each table a rebuilt table with a declared
 * window reads (transitively) whose age is unknown or past that window at
 * the run's start: a rebuilt table is anchored at the oldest table it read
 * (freshness_stamp.ts), so a table read at an unknown or stale age would
 * leave it stale on arrival, or claim fresh rows it does not hold. The
 * narrowest window over a path decides. Moves each such table from
 * `carried` (and `recheck`) to `instructions`.
 */
function rebuildStaleUpstreams(
   instructions: BuildInstruction[],
   carried: Record<string, ManifestEntry>,
   carriedInstructions: ReadonlyMap<string, BuildInstruction>,
   recheck: Record<string, BuildInstruction>,
   rule: StaleUpstreamRule,
): void {
   const narrowest = new Map<string, number>();
   const queue: [string, number][] = [];
   const visit = (address: string, window: number) => {
      const seen = narrowest.get(address);
      if (seen !== undefined && seen <= window) return;
      narrowest.set(address, window);
      queue.push([address, window]);
   };
   for (const instruction of instructions) {
      const window = rule.windowSecondsAt(instruction.sourceEntityId);
      if (window !== undefined) visit(instruction.sourceEntityId, window);
   }
   while (queue.length > 0) {
      const [address, window] = queue.pop()!;
      for (const upstream of rule.upstreamsOf(address)) {
         const prior = carried[upstream];
         if (prior !== undefined) {
            const stale =
               prior.dataAsOf === undefined ||
               freshnessOf(
                  { dataAsOf: prior.dataAsOf, freshnessWindowSeconds: window },
                  rule.at,
               ) !== "fresh";
            // A fresh carried table's own anchor already covers what it read.
            if (!stale) continue;
            const instruction = carriedInstructions.get(upstream);
            if (instruction === undefined) continue;
            delete carried[upstream];
            delete recheck[upstream];
            instructions.push(instruction);
         }
         visit(upstream, window);
      }
   }
}

/**
 * Validate caller-supplied build instructions against the compiled build
 * plan: every instructed sourceEntityId must be planned, and only COPY is
 * supported. Throws when the package declares no persist source.
 *
 * Each caller-supplied ledger entry must name a table this run instructs,
 * name it once, belong to a source declaring incremental refresh, and carry
 * the plan's content address for it. An invalid entry is a 400 here, where
 * the caller can react, rather than a failed background run; the address
 * check fires routinely after a publish or rollback changes a source. An
 * entry for a source being reseeded is not validated: `reseed` is the
 * documented recovery from this 400.
 */
export function validateInstructions(
   plan: BuildPlan | null,
   instructions: BuildInstruction[],
   ledger?: LedgerEntry[],
   runReseed = false,
): void {
   if (!plan) {
      throw new BadRequestError(
         "Package has no persist sources; buildInstructions cannot be applied",
      );
   }
   const plannedSourceEntityIds = new Set<string>();
   for (const source of Object.values(plan.sources)) {
      plannedSourceEntityIds.add(source.sourceEntityId);
   }

   for (const instruction of instructions) {
      if (!plannedSourceEntityIds.has(instruction.sourceEntityId)) {
         throw new BadRequestError(
            `Instruction references unknown sourceEntityId '${instruction.sourceEntityId}'`,
         );
      }
      // COPY-only for now; SNAPSHOT lands once clone semantics are defined.
      if (instruction.realization === "SNAPSHOT") {
         throw new BadRequestError(
            "realization=SNAPSHOT is not supported (COPY only)",
         );
      }
   }

   if (ledger === undefined || ledger.length === 0) return;

   // Instructed tables, keyed like a ledger entry, with the plan's facts
   // about each writing source. Resolved by sourceID, else content address
   // (the build loop's order).
   const instructed = new Map<
      string,
      { sourceEntityId: string; refresh: string | null; reseed: boolean }
   >();
   // The same tables keyed WITHOUT their destination, so a destination-less
   // entry can be recognized as stale.
   const instructedIntoStorage = new Set<string>();
   const byAddress = new Map(
      Object.values(plan.sources).map((s) => [s.sourceEntityId, s]),
   );
   for (const instruction of instructions) {
      const planSource =
         (instruction.sourceID
            ? plan.sources[instruction.sourceID]
            : undefined) ?? byAddress.get(instruction.sourceEntityId);
      if (!planSource) continue; // Unreachable: every instruction matched above.
      instructed.set(
         tableKeyOf(
            planSource.connectionName,
            instruction.destination,
            instruction.physicalTableName,
         ),
         {
            sourceEntityId: planSource.sourceEntityId,
            refresh: planSource.refresh ?? null,
            reseed: runReseed || instruction.reseed === true,
         },
      );
      if (instruction.destination) {
         instructedIntoStorage.add(
            tableKeyOf(
               planSource.connectionName,
               undefined,
               instruction.physicalTableName,
            ),
         );
      }
   }

   const seen = new Set<string>();
   for (const entry of ledger) {
      const key = tableKeyOf(
         entry.connectionName,
         entry.storageDestinationName,
         entry.physicalTableName,
      );
      const table =
         `'${entry.physicalTableName}' ` +
         (entry.storageDestinationName
            ? `in storage destination '${entry.storageDestinationName}'`
            : `on connection '${entry.connectionName}'`);
      if (seen.has(key)) {
         throw new BadRequestError(
            `Ledger entry for table ${table} appears more than once`,
         );
      }
      seen.add(key);
      const target = instructed.get(key);
      if (!target) {
         // An entry without `storageDestinationName` for a table this run
         // builds into storage is STALE, not wrong: the source seeds (the
         // build's index keys on the destination, so it finds no boundary).
         // An entry naming a DIFFERENT destination is refused below.
         const storedHere = instructedIntoStorage.has(
            tableKeyOf(
               entry.connectionName,
               undefined,
               entry.physicalTableName,
            ),
         );
         if (entry.storageDestinationName === undefined && storedHere) {
            continue;
         }
         throw new BadRequestError(
            `Ledger entry names table ${table}, which this run's ` +
               `instructions do not build`,
         );
      }
      if (target.reseed) continue;
      if (target.refresh !== "incremental") {
         throw new BadRequestError(
            `Ledger entry names table ${table}, whose source does not ` +
               `declare refresh="incremental"`,
         );
      }
      if (entry.sourceEntityId !== target.sourceEntityId) {
         throw new BadRequestError(
            `Ledger entry for table ${table} was measured under a ` +
               `different definition of its source (its sourceEntityId does ` +
               `not match the plan's) — expected after a publish or ` +
               `rollback that changed the source. Delete the entry to ` +
               `rebuild the source, or set reseed.`,
         );
      }
   }
}

/**
 * Where each instructed table lives, as {@link WRITES_TARGETS_KEY} records it:
 * the instruction's table and destination, with the connection of the plan
 * source it builds. Matched by sourceID when the instruction carries one, else
 * by content address, the order the build matches instructions in. An
 * instruction the plan does not know is left out.
 */
export function writeTargetsFor(
   plan: BuildPlan | null,
   instructions: BuildInstruction[],
): WriteTarget[] {
   if (!plan) return [];
   const byAddress = new Map(
      Object.values(plan.sources).map((s) => [s.sourceEntityId, s]),
   );
   const targets: WriteTarget[] = [];
   for (const instruction of instructions) {
      const source =
         (instruction.sourceID
            ? plan.sources[instruction.sourceID]
            : undefined) ?? byAddress.get(instruction.sourceEntityId);
      if (!source?.connectionName) continue;
      targets.push({
         sourceEntityId: instruction.sourceEntityId,
         physicalTableName: instruction.physicalTableName,
         connectionName: source.connectionName,
         ...(instruction.destination
            ? { storageDestinationName: instruction.destination }
            : {}),
      });
   }
   return targets;
}

/**
 * Whether two physical names are one table: the same segments, container
 * included, unquoted, compared in one case only where the destination folds
 * table names (tableNamesFoldCase). Elsewhere a case-only rename is another
 * table, rebuilt and checked under its new name.
 */
function samePath(a: string, b: string, foldCase: boolean): boolean {
   const canon = (name: string) =>
      name
         .split(".")
         .map((s) => s.replace(/["`[\]]/g, ""))
         .map((s) => (foldCase ? s.toLowerCase() : s))
         .join(".");
   return canon(a) === canon(b);
}
