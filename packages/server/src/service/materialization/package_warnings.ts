// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The package warnings and publish rejections materialization raises, each a
 * pure function of what the facade holds (the plan, the bound tables, the
 * models): one wording per code, read by `Package` through the facade.
 */

import {
   findingSeverity,
   MATERIALIZATION_STORAGE_MODE_CODE,
   MATERIALIZE_NOT_BUILT_CODE,
   ROLLUP_NOT_BUILT_CODE,
   type PackageAnnotationFinding,
} from "../annotation_reader";
import { components } from "../../api";
import { PACKAGE_MANIFEST_NAME } from "../../constants";
import { CLASSIFICATION_COVERAGE } from "../classification";
import type { Model } from "../model";
import type {
   PackagePlan,
   RefusalNotices,
   RefusedRollup,
} from "./domain/package_plan";
import {
   loadGateText,
   preaggregateGateLines,
   refusalCodeOf,
} from "./plan/load_gate_text";
import { CronEvaluator } from "./run/cron_evaluator";
import type { Binding } from "./domain/binding";
import { INCREMENTAL_LATE_ROWS_CODE } from "./domain/incremental";
import {
   isPersistedQuerySourceID,
   isRollupOrigin,
   type MaterializePlan,
} from "./domain/table_plans";
import type {
   BuildPlan,
   FreshnessManifest,
   KeptTable,
   LateRows,
   UnreadReason,
} from "./domain/types";
import {
   SERVED_COPY_WARNING_CODE,
   type ServedCopyProblem,
} from "./serve/served_copy_serve";
import { namesServedCopyAlias } from "./serve/served_copy_config";
import {
   NATIVE_SCHEMA_MISSING_CODE,
   type NativeSchemaNotice,
   nativeSchemaNotices,
} from "./serve/native_schema_notices";
import {
   VERDICT_WARNING_CODES,
   type ServeVerdict,
} from "./serve/serve_verdict";
import {
   STORED_COLUMN_TYPE_MISMATCH_CODE,
   type StoredColumnTypeMismatch,
} from "./stored_column_types";
import type {
   BoundTableTypeFinding,
   DatabaseDriftFinding,
   MismatchHandling,
} from "./serve/storage_bindings";

/** What a stored column type warning says of how the column is read. */
const MISMATCH_HANDLING_TEXT: Record<
   MismatchHandling,
   (declaredType: string) => string
> = {
   cast: (declaredType) =>
      `It is read through a cast to \`${declaredType}\`, so it still serves ` +
      `from storage; the next build stores the declared type.`,
   live: () =>
      `No cast reproduces its values faithfully, so queries that read it are ` +
      `answered live. Give the field a native cast in the model (for example ` +
      `\`::"double"\`) so the warehouse returns the declared type.`,
};

/**
 * What a database-table drift warning says of how the column is read: the table
 * was altered outside a build, so rebuilding it is the fix.
 */
const DRIFT_HANDLING_TEXT: Record<
   DatabaseDriftFinding["mismatches"][number]["handling"],
   (declaredType: string) => string
> = {
   cast: (declaredType) =>
      `It is read through a cast to \`${declaredType}\`, so it still serves ` +
      `from the table; rebuilding the table clears this.`,
   live: () =>
      `No cast reads it as declared, so queries that read it are answered ` +
      `live until the table is rebuilt.`,
};

/**
 * What a drift warning says when the table is as built but its source now
 * declares another type: the table's content address did not move, so a run
 * reuses it, and only a forced rebuild stores the declared type.
 */
const UPSTREAM_DRIFT_HANDLING_TEXT: Record<
   DatabaseDriftFinding["mismatches"][number]["handling"],
   (declaredType: string) => string
> = {
   cast: (declaredType) =>
      `It is read through a cast to \`${declaredType}\`, so it still serves ` +
      `from the table; a forced rebuild (forceRefresh) clears this.`,
   live: () =>
      `No cast reads it as declared, so queries that read it are answered ` +
      `live until the table is rebuilt with a forced refresh (forceRefresh): ` +
      `its content address did not move, so a run reuses it.`,
};

type ApiPackage = components["schemas"]["Package"];
type ApiPackageWarning = NonNullable<ApiPackage["warnings"]>[number];
type Models = ReadonlyMap<string, Model>;

/** The code on the warning for a rollup whose build found its join_one key repeated. */
const ROLLUP_JOIN_ONE_KEY_REPEATED_CODE = "rollup-join-one-key-repeated";

/** The code on the warning for a database table whose columns drifted outside a build. */
export const DATABASE_COLUMN_TYPE_MISMATCH_CODE =
   "database-column-type-mismatch";

/**
 * Warning codes renamed in 0.15.0, each with the code 0.14.0 shipped for it:
 * a warning under the new code is repeated under the old one, with the same
 * message, for this release only, so a client switching on the old code
 * keeps working. Removed in the next release.
 */
export const RETAINED_WARNING_ALIASES: Readonly<Record<string, string>> = {
   [ROLLUP_NOT_BUILT_CODE]: "preaggregate-not-built",
   [ROLLUP_JOIN_ONE_KEY_REPEATED_CODE]: "preaggregate-join-one-key-repeated",
   [NATIVE_SCHEMA_MISSING_CODE]: "stored-native-schema-missing",
};

/**
 * `warnings`, each one whose code was renamed followed by its repeat under
 * the code 0.14.0 shipped ({@link RETAINED_WARNING_ALIASES}).
 */
export function withRetainedAliases<T extends object>(
   warnings: readonly T[],
): T[] {
   return warnings.flatMap((w) => {
      const code = (w as { code?: unknown }).code;
      const alias =
         typeof code === "string" ? RETAINED_WARNING_ALIASES[code] : undefined;
      return alias ? [w, { ...w, code: alias }] : [w];
   });
}

/** `warnings` less each retained-alias repeat of a warning beside it (for a reader that reads each once). */
export function withoutRetainedAliases<
   T extends { code?: string; message?: string; subject?: string },
>(warnings: readonly T[]): T[] {
   const key = (code: string | undefined, w: T) =>
      `${code}\u0000${w.subject ?? ""}\u0000${w.message ?? ""}`;
   const present = new Set(warnings.map((w) => key(w.code, w)));
   const aliasOf = new Map(
      Object.entries(RETAINED_WARNING_ALIASES).map(([now, old]) => [old, now]),
   );
   return warnings.filter((w) => {
      const now = w.code !== undefined ? aliasOf.get(w.code) : undefined;
      return now === undefined || !present.has(key(now, w));
   });
}

/**
 * The findings on authored annotations: an unservable one (a valid
 * annotation not built here) is `warn`. A malformed one fails the load, so a
 * serving package carries none; it would be `error`.
 */
export function annotationFindingWarnings(
   findings: readonly PackageAnnotationFinding[],
): ApiPackageWarning[] {
   return findings.map((finding) => ({
      ...(finding.model ? { model: finding.model } : {}),
      ...(finding.subject ? { subject: finding.subject } : {}),
      message: finding.rewrite
         ? `${finding.message}; ${finding.rewrite}`
         : finding.message,
      code: finding.code,
      ...(finding.reason ? { reason: finding.reason } : {}),
      severity: findingSeverity(finding),
   }));
}

/**
 * What a load of a derived plan would say of its annotations, for a compile
 * that checks the package without loading it: the load gate's lines, which
 * fail the load (plan/load_gate_text.ts, the same text and order the load
 * and a run read), and the warnings a valid annotation that is not built
 * here leaves on the package. Read off the worker's plan, as a re-derivation
 * reads it.
 */
export function loadVerdictOfPlan(
   outcome: Pick<
      PackagePlan<PackageAnnotationFinding>,
      | "plan"
      | "annotationFindings"
      | "incrementalDeclarations"
      | "materializePlans"
      | "modelGateLines"
      | "persistedViews"
   >,
   judgement: { enforceCollisions: boolean; versionScoped: boolean },
): { refusals: string[]; warnings: ApiPackageWarning[] } {
   const refusals = loadGateText(outcome, judgement)
      .split("\n")
      .filter(Boolean);
   return {
      refusals,
      // A malformed finding is one of the refusals above.
      warnings: annotationFindingWarnings(
         outcome.annotationFindings.filter((f) => f.tier !== "malformed"),
      ),
   };
}

/**
 * One warning per bound `#(rollup)` rollup whose last build found its
 * join_one key repeated (the rollup key check, `keyUniqueness`
 * false): the data breaks a `join_one` declared on that key, so queries that
 * read it beside other aggregates are answered live. Recomputed from what is
 * bound, so it clears once a build finds the key unique, or forgets the check.
 */
export function repeatedJoinOneKeyWarnings(
   plan: BuildPlan | null,
   databaseEntries: FreshnessManifest | undefined,
   storageBindings: readonly Binding[],
): ApiPackageWarning[] {
   const warnings: ApiPackageWarning[] = [];
   const bySourceEntity = new Map(
      Object.values(plan?.sources ?? {}).flatMap((source) =>
         source.sourceEntityId ? [[source.sourceEntityId, source]] : [],
      ),
   );
   // The rollup's own key check only: an entry may carry just the query
   // sources' primary key checks (`parentKeys`), which say nothing of a
   // join_one keyed on the rollup's grain. A stored rollup's rides on its
   // binding, once per table.
   const checks = new Map<string, { keyColumn?: string; unique?: boolean }>(
      Object.entries(databaseEntries ?? {}).flatMap(([id, entry]) =>
         entry.keyUniqueness ? [[id, entry.keyUniqueness]] : [],
      ),
   );
   for (const b of storageBindings) {
      if (b.rollupKey) checks.set(b.virtualHandle, b.rollupKey);
   }
   for (const [sourceEntityId, check] of checks) {
      if (check?.unique !== false || check.keyColumn === undefined) continue;
      const source = bySourceEntity.get(sourceEntityId);
      const rollup = source?.preaggregate;
      if (!rollup) continue;
      warnings.push({
         ...(source?.modelPath ? { model: source.modelPath } : {}),
         subject: rollup.baseSourceName,
         code: ROLLUP_JOIN_ONE_KEY_REPEATED_CODE,
         severity: "warn",
         message:
            `holds \`${check.keyColumn}\` more than once, found by its ` +
            `#(rollup) rollup's last build (grain ` +
            `${rollup.grainDimensions.join(", ")}): a join_one keyed on it ` +
            `is not unique in the data, so live repeats each joining row ` +
            `once per match. Queries that read that join_one beside other ` +
            `aggregates are answered live. Fix the data or declare the ` +
            `join as join_many.`,
      });
   }
   return warnings;
}

/**
 * One warning per incremental source whose bound table a run found missing
 * rows below its delta's lower bound (`ManifestEntry.lateRows`, see
 * domain/types.ts): the table undercounts them, and no later delta reads
 * them. A delta or skip carries them onto its entry, so the warning is
 * cleared only when a rebuild's manifest is bound. The window's bounds are values from
 * the data, so the message points at the run's entry rather than quoting
 * them.
 */
export function lateRowWarnings(
   plan: BuildPlan | null,
   bound: ReadonlyMap<string, LateRows>,
): ApiPackageWarning[] {
   if (bound.size === 0) return [];
   const bySourceEntity = new Map(
      Object.values(plan?.sources ?? {}).flatMap((source) =>
         source.sourceEntityId ? [[source.sourceEntityId, source]] : [],
      ),
   );
   return [...bound].flatMap(([sourceEntityId, late]) => {
      const source = bySourceEntity.get(sourceEntityId);
      if (!source) return [];
      const rows = late.count === 1 ? "1 row" : `${late.count} rows`;
      const them = late.count === 1 ? "it" : "them";
      const annotation =
         source.origin === "materialize" ? "#(materialize)" : "#@ persist";
      const fields = source.annotationFields ?? {};
      // lookback= is valid only beside merge_key=, so the advice names both
      // when the source has no merge key.
      const lookback = fields.lookback
         ? `widen its lookback= (now ${JSON.stringify(fields.lookback)})`
         : fields.merge_key
           ? "declare a lookback= on it"
           : "declare a merge_key= (the row's identity) and a lookback= on it";
      return [
         {
            ...(source.modelPath ? { model: source.modelPath } : {}),
            subject: source.name,
            code: INCREMENTAL_LATE_ROWS_CODE,
            severity: "warn" as const,
            message:
               `its incremental refreshes found ${rows} in the source stamped ` +
               `just below where a delta started reading (the newest ` +
               `watermark its table held, less any lookback=), which the ` +
               `table does not hold. No delta reads below that point, so ` +
               `queries served from the table leave ${them} out, and this ` +
               `stays until the table is rebuilt. Rebuild it once (a run ` +
               `with reseed) to recover ${them}, and, so later deltas read ` +
               `such rows, ${lookback} in its ${annotation} annotation, as ` +
               `long as rows can arrive late. The count is a lower bound (a ` +
               `stretch of the source is checked once), and this warning ` +
               `stays until a full rebuild, even after a wider lookback= has ` +
               `written the rows. The run's manifest entry names the window ` +
               `(\`lateRows\`).`,
         },
      ];
   });
}

/**
 * The code on the warning for a table a run replaced that Publisher keeps:
 * after a rename, a `storage=` or tier move, or in a destination no longer
 * configured. Its `reason` is the kept table's.
 */
export const TABLE_KEPT_CODE = "table-kept";

/** Why a kept table stays, and what to do about it. */
const KEPT_REASON_TEXT: Record<KeptTable["reason"], string> = {
   author_name:
      "It is at a name= the author chose, which Publisher never drops, since " +
      "a reader outside Malloy may rely on it. Drop it yourself once nothing " +
      "reads it: check that no remaining run, of any version, names a table " +
      "with that name in any place, then drop it through its connection.",
   destination_missing:
      "Its storage destination is no longer configured under that name, so " +
      "Publisher cannot reach it to drop it. If the destination was renamed " +
      "rather than removed, this may be a table the package still serves " +
      "under the new name: check that before dropping anything. Configure " +
      "the destination again for the next run to decide it.",
   may_be_served:
      "A table's name does not say where it is: two places can reach one " +
      "table (two names for one DuckLake, a connection that reaches the " +
      "destination, or on DuckDB a catalog and a schema of one database). " +
      "So this may be the very table the package serves now, and Publisher " +
      "never drops it. Check whether it is before doing anything with it. " +
      "To clear this warning, delete the runs that built it here (a delete " +
      "is record-only); the next auto-run stops listing it.",
};

/**
 * One warning per table the newest auto-run lists as kept after a rename
 * (Reclaimer.reclaimRenamedTables), which Publisher does not drop: the
 * source no longer reads it, or (`may_be_served`) it may be the very table
 * the source reads now. `subject` the source (a rollup's base), the
 * message naming the table and where it is. Cleared once the run that lists
 * it no longer does: the table was dropped, or its name taken again.
 */
export function keptTableWarnings(
   plan: BuildPlan | null,
   kept: readonly KeptTable[],
): ApiPackageWarning[] {
   if (kept.length === 0) return [];
   const bySourceEntity = new Map(
      Object.values(plan?.sources ?? {}).flatMap((source) =>
         source.sourceEntityId ? [[source.sourceEntityId, source]] : [],
      ),
   );
   return kept.map((table) => {
      const source = bySourceEntity.get(table.sourceEntityId);
      const subject =
         source?.preaggregate?.baseSourceName ??
         source?.name ??
         table.sourceName;
      const where = table.storageDestinationName
         ? `in storage destination \`${table.storageDestinationName}\``
         : `on connection \`${table.connectionName}\``;
      return {
         ...(source?.modelPath ? { model: source.modelPath } : {}),
         ...(subject ? { subject } : {}),
         code: TABLE_KEPT_CODE,
         reason: table.reason,
         severity: "warn" as const,
         message:
            (table.reason === "may_be_served"
               ? `table \`${table.physicalTableName}\` ${where} was kept: ` +
                 `the package now serves a table of the same name from ` +
                 `another place or container. `
               : `table \`${table.physicalTableName}\` ${where} is no longer ` +
                 `read: the source's table was renamed or moved, and its new ` +
                 `table serves now. `) + KEPT_REASON_TEXT[table.reason],
      };
   });
}

/**
 * One `*-not-built` warning per refusal record in the plan's
 * `refusedSources`, whatever the mechanism. A record a planner's finding
 * made carries that finding's code and subject ({@link RefusalNotices}). A
 * compile-time gate's refusal is coded by its mechanism with the gate's
 * message verbatim: a rollup's {@link ROLLUP_NOT_BUILT_CODE}, naming
 * its base and grain, with its BASE source as `subject` (its own name is
 * generated). Each carries the record's `reason`.
 */
export function persistRefusalWarnings(
   plan: BuildPlan | null,
   refusedRollups: Record<string, RefusedRollup>,
   materializePlans: Record<string, MaterializePlan>,
   notices: RefusalNotices = {},
   /** Only the records it keeps; every one when absent. */
   only: (sourceID: string) => boolean = () => true,
): ApiPackageWarning[] {
   const refused = plan?.refusedSources;
   if (!refused) return [];
   return Object.entries(refused).flatMap(([sourceID, source]) => {
      if (!only(sourceID)) return [];
      const notice = notices[sourceID];
      if (notice) {
         return [
            {
               ...(source.modelPath ? { model: source.modelPath } : {}),
               ...(notice.subject !== undefined
                  ? { subject: notice.subject }
                  : {}),
               message: source.message,
               code: notice.code,
               reason: source.reason,
               severity: "warn" as const,
            },
         ];
      }
      const preaggregate =
         isRollupOrigin(source.origin) || source.tier === "preaggregate";
      const rollup = preaggregate
         ? (refusedRollups[sourceID] ?? plan?.sources?.[sourceID]?.preaggregate)
         : undefined;
      const subject = preaggregate ? rollup?.baseSourceName : source.name;
      const code = preaggregate
         ? ROLLUP_NOT_BUILT_CODE
         : materializePlans[sourceID]
           ? MATERIALIZE_NOT_BUILT_CODE
           : refusalCodeOf(sourceID, source.name);
      const warning: ApiPackageWarning = {
         message: preaggregate
            ? (rollup
                 ? `#(rollup) rollup of source "${rollup.baseSourceName}" at ` +
                   `(${rollup.grainDimensions.join(", ")})`
                 : "A #(rollup) rollup") +
              ` is not built, so its measures are served live: ` +
              source.message
            : source.message,
         code,
         reason: source.reason,
         severity: "warn",
      };
      if (source.modelPath) warning.model = source.modelPath;
      if (subject) warning.subject = subject;
      return [warning];
   });
}

/**
 * The code on the warning for a table stored as of its build: a v1
 * `#@ persist` source, persisted view or persisted query whose build reads
 * the current time (`now`, or a relative time filter string).
 */
export const PERSIST_READS_CURRENT_TIME_CODE = "persist-reads-current-time";

/**
 * One warning per table stored as of its build ({@link PERSIST_READS_CURRENT_TIME_CODE}):
 * its answers are as of its last build, it is rebuilt on every run, where
 * its build reads the clock, and what to do to answer as of each read.
 * `subject` the source, the view (`<source> -> <view>`) or the query.
 */
export function asOfBuildWarnings(
   asOfBuild: Readonly<
      Record<string, { name: string; modelPath?: string; reads: string[] }>
   >,
   persistedViews: Readonly<
      Record<string, { sourceName: string; viewName: string }>
   >,
): ApiPackageWarning[] {
   return Object.entries(asOfBuild).map(([sourceID, table]) => {
      const view = persistedViews[sourceID];
      const query =
         view === undefined && isPersistedQuerySourceID(sourceID)
            ? table.name.replace(/__query$/, "")
            : undefined;
      const subject = view
         ? `${view.sourceName} -> ${view.viewName}`
         : (query ?? table.name);
      const what = view
         ? `view "${view.viewName}" of source "${view.sourceName}"`
         : query !== undefined
           ? `query "${query}"`
           : `source "${table.name}"`;
      const where =
         table.reads.length > 0 ? ` (${table.reads.join(", ")})` : "";
      return {
         ...(table.modelPath ? { model: table.modelPath } : {}),
         subject,
         code: PERSIST_READS_CURRENT_TIME_CODE,
         severity: "warn" as const,
         message:
            `#@ persist on ${what} stores a relation that reads the ` +
            `current time${where}, so its answers are as of its last build ` +
            `rather than of each query, and it is rebuilt on every run. To ` +
            `answer as of each query instead, move the window into the ` +
            `persisted source's own extend block (a \`where:\`, dimension ` +
            `or measure there is applied at every read), read a given there, ` +
            `or persist a finer source that keeps the time column and filter ` +
            `it at query time.`,
      };
   });
}

/**
 * Warnings for `storage=` sources the serve path is not honoring, per
 * PERSIST_STORAGE_MODE: `off` ignores the annotation, `write-only` builds but
 * does not route. `on` warns nothing. A source whose table is also bound,
 * and fresh, in its own database is answered from that table, and the
 * warning says so.
 */
export function storageWarnings(
   plan: BuildPlan | null,
   mode: string,
   /** Whether a table of this content address is bound and fresh in its own database. */
   databaseFresh: (sourceEntityId: string) => boolean = () => false,
): ApiPackageWarning[] {
   if (mode === "on" || !plan?.sources) return [];
   const warnings: ApiPackageWarning[] = [];
   for (const source of Object.values(plan.sources)) {
      const storage = source.annotationFields?.storage?.trim();
      if (!storage) continue;
      // A rollup is named by its base source and grain (what the author
      // typed), never its generated name; the grain is a SET, sorted on the
      // wire. In the default `off` mode this is the only signal.
      const rollup = source.preaggregate;
      const at = rollup
         ? `Measures of \`${rollup.baseSourceName}\` pre-aggregated at grain ` +
           `(${rollup.grainDimensions.join(", ")}) declare `
         : "";
      const fromDatabase =
         !!source.sourceEntityId && databaseFresh(source.sourceEntityId);
      const message = fromDatabase
         ? `${rollup ? at : "declares "}storage="${storage}", but ` +
           `PERSIST_STORAGE_MODE is ${mode}, so no query reads the storage ` +
           `table. Queries are answered from its table in the source's own ` +
           `database, which is bound and fresh.`
         : rollup
           ? mode === "off"
              ? `${at}storage="${storage}", but PERSIST_STORAGE_MODE is off, so ` +
                `the rollup is not built. Queries that would have used it are ` +
                `answered from \`${rollup.baseSourceName}\` instead — correct, ` +
                `but unaccelerated.`
              : `${at}storage="${storage}", and the rollup is built, but ` +
                `PERSIST_STORAGE_MODE is write-only, so the serve path does not ` +
                `route to it. Queries are answered from ` +
                `\`${rollup.baseSourceName}\` — correct, but unaccelerated.`
           : mode === "off"
             ? `declares storage="${storage}" but PERSIST_STORAGE_MODE is off; ` +
               `the annotation is ignored and the source is served live from ` +
               `its own warehouse.`
             : `is materialized into storage "${storage}" but ` +
               `PERSIST_STORAGE_MODE is write-only; the serve path is not ` +
               `routed to the materialized table (served live).`;
      warnings.push({
         model: source.modelPath ?? "",
         subject: rollup ? rollup.baseSourceName : source.name,
         message,
         code: MATERIALIZATION_STORAGE_MODE_CODE,
         reason: "storage_mode",
         severity: "warn",
      });
   }
   return warnings;
}

/**
 * Warnings for a model whose served copy leaves sources unserved: it did not
 * compile, a join it moved across connections reads a source that is not
 * stored, or a source could not be re-rooted. Those sources answer live.
 */
export function servedCopyWarnings(
   models: Models,
   /**
    * Whether a source's declaration takes `inline=`, by sourceID: only a
    * `#(materialize)` one does. Absent, every source is advised to inline.
    */
   takesInline?: (sourceID: string) => boolean,
): ApiPackageWarning[] {
   const warnings: ApiPackageWarning[] = [];
   for (const [modelPath, model] of models) {
      const problems = model.servedCopyProblems() ?? [];
      // A join already inlined, but kept as written: its own warning says
      // why, so the cross-connection one does not advise inlining it.
      const keptInline = new Set(
         problems
            .filter((p) => p.reason === "inline_join_kept")
            .map((p) => `${p.sourceName}\u0000${p.join}`),
      );
      for (const p of problems) {
         const subject = p.sourceName ? { subject: p.sourceName } : {};
         warnings.push({
            model: modelPath,
            ...subject,
            code: SERVED_COPY_WARNING_CODE,
            reason: p.reason,
            severity: "warn",
            message: servedCopyProblemMessage(
               p,
               keptInline.has(`${p.sourceName}\u0000${p.join}`),
               p.sourceID === undefined || !takesInline
                  ? true
                  : takesInline(p.sourceID),
            ),
         });
         // The coded warning the compile of record's own check raises.
         for (const message of p.coverage ?? []) {
            warnings.push({
               model: modelPath,
               ...subject,
               code: CLASSIFICATION_COVERAGE,
               severity: "warn",
               message,
            });
         }
      }
   }
   return warnings;
}

/**
 * Warnings for stored columns whose type does not serve as the type Malloy
 * declares, one per source and column, read from the bindings bound now:
 * each model's `#@ persist` sources', then every other `storage=` table's
 * (`bound`), then each database table's whose columns moved outside a build
 * (`database`). Keyed by the source's sourceID, so two models' same-named
 * sources both report and a source several models import reports once.
 */
export function storedColumnTypeWarnings(
   models: Models,
   bound: readonly BoundTableTypeFinding[],
   database: readonly DatabaseDriftFinding[] = [],
): ApiPackageWarning[] {
   const warnings: ApiPackageWarning[] = [];
   const seen = new Set<string>();
   const add = (
      modelPath: string,
      subject: string,
      key: string,
      m: StoredColumnTypeMismatch & { handling: MismatchHandling },
   ) => {
      if (seen.has(`${key}\u0000${m.column}`)) return;
      seen.add(`${key}\u0000${m.column}`);
      warnings.push({
         model: modelPath,
         subject,
         code: STORED_COLUMN_TYPE_MISMATCH_CODE,
         severity: "warn",
         message:
            `stores column \`${m.column}\` as ${m.storedType}, not its ` +
            `declared Malloy type \`${m.declaredType}\`. ` +
            MISMATCH_HANDLING_TEXT[m.handling](m.declaredType),
      });
   };
   for (const [modelPath, model] of models) {
      for (const {
         sourceName,
         sourceID,
         mismatches,
      } of model.storedColumnTypeMismatches() ?? []) {
         const key = sourceID ?? `${modelPath}\u0000${sourceName}`;
         for (const m of mismatches) add(modelPath, sourceName, key, m);
      }
   }
   for (const finding of bound) {
      for (const m of finding.mismatches) {
         add(finding.modelPath ?? "", finding.subject, finding.sourceID, m);
      }
   }
   for (const finding of database) {
      for (const m of finding.mismatches) {
         const key = `${finding.sourceID}\u0000${m.column}`;
         if (seen.has(key)) continue;
         seen.add(key);
         warnings.push({
            model: finding.modelPath ?? "",
            subject: finding.subject,
            code: DATABASE_COLUMN_TYPE_MISMATCH_CODE,
            severity: "warn",
            message:
               (m.gone
                  ? `no longer has column \`${m.column}\` (declared Malloy ` +
                    `type \`${m.declaredType}\`): `
                  : `stores column \`${m.column}\` as ` +
                    `${m.storedType ?? "a type Malloy reads as no basic type"}, ` +
                    `not its declared Malloy type \`${m.declaredType}\`: `) +
               (m.declaredMoved
                  ? `what its source reads changed type since the build. ` +
                    UPSTREAM_DRIFT_HANDLING_TEXT[m.handling](m.declaredType)
                  : `its table was altered outside a build. ` +
                    DRIFT_HANDLING_TEXT[m.handling](m.declaredType)),
         });
      }
   }
   return warnings;
}

/**
 * Warnings for database tables bound with no captured warehouse type for a
 * big-integer column, one per source (by sourceID).
 */
export function nativeSchemaWarnings(
   bindings: readonly Binding[],
   models: Models,
   materializePlans: Record<string, MaterializePlan>,
): ApiPackageWarning[] {
   const warnings: ApiPackageWarning[] = [];
   const seen = new Set<string>();
   for (const [modelPath, model] of models) {
      const modelDef = model.getModelDef();
      if (!modelDef) continue;
      for (const n of nativeSchemaNotices(
         bindings,
         modelDef,
         materializePlans,
      )) {
         if (seen.has(n.sourceID)) continue;
         seen.add(n.sourceID);
         warnings.push({
            model: modelPath,
            subject: n.sourceName,
            code: NATIVE_SCHEMA_MISSING_CODE,
            severity: "warn",
            message: nativeSchemaMissingMessage(n),
         });
      }
   }
   return warnings;
}

/**
 * The publish warnings a serve verdict carries: `unreachable`, and the
 * deployment facts (a `storage=` destination the environment does not
 * configure, a database source on a connection that cannot create tables).
 * Unservable, not malformed: the source serves live, and a deployment warning
 * clears once the deployment changes, with no reload.
 */
export function verdictWarnings(
   verdicts: readonly ServeVerdict[],
): ApiPackageWarning[] {
   return verdicts.flatMap((v) => {
      const code = v.reason ? VERDICT_WARNING_CODES[v.reason] : undefined;
      if (!code) return [];
      return [
         {
            model: v.modelPath ?? "",
            subject: v.sourceName,
            code,
            ...(v.reason ? { reason: v.reason } : {}),
            severity: "warn" as const,
            message: v.message + " " + (v.fixes ?? []).join(" "),
         },
      ];
   });
}

/**
 * The package manifest's Malloy Persistence policy REJECTION rules: a
 * non-empty result fails a publish and disarms the scheduler (warn-only at
 * load). Cross-package scope rules are not checked: the build plan is
 * intra-package only.
 */
export function persistencePolicyWarnings(
   plan: BuildPlan | null,
   metadata: ApiPackage,
): string[] {
   const warnings: string[] = [];
   const sources = plan?.sources ? Object.values(plan.sources) : [];
   const packageSchedule = metadata.materialization?.schedule ?? null;
   const packageFreshness = metadata.materialization?.freshness ?? null;
   const scope = metadata.scope ?? "package";

   // A package-level cron is legal only with scope: version.
   if (packageSchedule && scope !== "version") {
      warnings.push(
         `materialization.schedule (cron) in ${PACKAGE_MANIFEST_NAME} requires ` +
            `"scope": "version": a package-scoped lineage is reused across ` +
            `versions, so a single per-version cadence is meaningless. Set ` +
            `"scope": "version", or remove the schedule.`,
      );
   }

   // Schedule and freshness are mutually exclusive.
   if (packageSchedule) {
      const freshnessDeclared =
         !!(
            packageFreshness &&
            (packageFreshness.window || packageFreshness.fallback)
         ) ||
         sources.some((s) => s.freshness != null) ||
         // A refused source declares its window all the same (R-06).
         Object.values(plan?.refusedSources ?? {}).some(
            (r) => r.freshness != null,
         );
      if (freshnessDeclared) {
         warnings.push(
            `materialization.schedule and freshness are mutually exclusive in ` +
               `${PACKAGE_MANIFEST_NAME}: declare either a schedule (power tier) ` +
               `or freshness (objective tier), never both.`,
         );
      }
   }

   // A valid 5-field cron, here so publish, PATCH, load and the scheduler all
   // apply one rule and a bad cron cannot silently never arm.
   if (packageSchedule && !new CronEvaluator().isValid(packageSchedule)) {
      warnings.push(
         `materialization.schedule in ${PACKAGE_MANIFEST_NAME} is not a valid ` +
            `5-field UNIX cron: ${JSON.stringify(packageSchedule)}. Use ` +
            `"minute hour day-of-month month day-of-week" (no L/W/#/? ` +
            `extensions).`,
      );
   }

   return warnings;
}

/**
 * REJECTION lines for invalid `#@ preaggregate` declarations, each with its
 * code, strict at publish AND load: an ignored declaration would show correct
 * numbers and no acceleration, with no one told. A valid declaration Publisher
 * cannot build is refused alone (`rollup-not-built`).
 */
export function formatInvalidPreaggregatePolicy(models: Models): string {
   return [...models]
      .flatMap(([modelPath, model]) =>
         preaggregateGateLines(modelPath, model.preaggregateViolations()),
      )
      .join("\n");
}

/**
 * The compiler's own reasons in a compile error, as `: <reason>; <reason>.`:
 * each diagnostic's message without its file and source excerpt, so a package
 * warning names why without the server's paths. A reason that names a
 * served-copy alias is left out: the alias stays in the server log. `.` when
 * there are none.
 */
function compilerReasons(detail: string | undefined): string {
   const reasons = [...(detail ?? "").matchAll(/^line \d+: (.+)$/gm)]
      .map((m) => m[1].trim())
      .filter((reason) => !namesServedCopyAlias(reason));
   return reasons.length > 0 ? `: ${reasons.join("; ")}.` : ".";
}

/** The `database-native-type-missing` sentence for one source. */
function nativeSchemaMissingMessage(n: NativeSchemaNotice): string {
   const names = (columns: string[]) =>
      columns.map((c) => "`" + c + "`").join(", ");
   const parts: string[] = [];
   const types = [...new Set(n.inferred.map((i) => i.as))];
   if (n.inferred.length > 0) {
      parts.push(
         `${names(n.inferred.map((i) => i.column))} ` +
            `${n.inferred.length === 1 ? "is" : "are"} read as ` +
            `${names(types)}, the warehouse's only integer type, so ` +
            `${n.inferred.length === 1 ? "it still serves" : "they still serve"} ` +
            "from the table",
      );
   }
   if (n.leftOut.length > 0) {
      parts.push(
         `${names(n.leftOut)} cannot be read exactly without one, so ` +
            `${n.leftOut.length === 1 ? "it is" : "they are"} left out of the table's read: ` +
            "the source answers live wherever it needs " +
            `${n.leftOut.length === 1 ? "it" : "them"}, and wholly live when its other fields ` +
            `cannot be served without ${n.leftOut.length === 1 ? "it" : "them"} (the ` +
            "`served-copy-not-served` warning on the source says so)",
      );
   }
   return (
      "This source's database table is bound with no captured warehouse " +
      "type for its big-integer columns (its manifest entry records none): " +
      `${parts.join("; ")}. The next full rebuild captures the types.`
   );
}

/**
 * A caller-safe sentence for why a served copy leaves a source unserved. The
 * compiler's own message stays in the logs: it can name the destination alias.
 */
/**
 * A dropped storage table's problem, said of the source the database table
 * now serves: the reason, then that the served copy reads its database table
 * instead of leaving the source as written.
 */
function fellBackMessage(message: string): string {
   const instead = "the served copy reads its database table instead.";
   const at = message.search(/[Tt]he served copy leaves /);
   if (at < 0)
      return `${message} ${instead[0].toUpperCase()}${instead.slice(1)}`;
   const head = message.slice(0, at);
   return head.endsWith(". ") || head === ""
      ? `${head}${instead[0].toUpperCase()}${instead.slice(1)}`
      : `${head}${instead}`;
}

function servedCopyProblemMessage(
   problem: ServedCopyProblem,
   inlinedButKept = false,
   takesInline = true,
): string {
   const message = servedCopyProblemReasonMessage(
      problem,
      inlinedButKept,
      takesInline,
   );
   // A storage table the served copy dropped for a source it then
   // re-rooted on its database table: the reason, said of that table.
   return problem.servedFromDatabase ? fellBackMessage(message) : message;
}

function servedCopyProblemReasonMessage(
   problem: ServedCopyProblem,
   inlinedButKept = false,
   /** Whether the source's declaration takes `inline=` (a `#(materialize)` one). */
   takesInline = true,
): string {
   const { reason, join } = problem;
   switch (reason) {
      case "compile_failed":
         // With a source: one extension table that does not compile over
         // its kept blocks, so the copy is compiled again without them.
         return problem.sourceName
            ? "This source's table does not compile with its kept blocks, so the served copy re-roots it with its own blocks only, or leaves it as written."
            : "This model's served copy does not compile, so its stored sources answer live.";
      case "cross_connection_unbuilt":
         return "This model's served copy re-roots a source onto its storage destination, which moves a join of it across connections, and the join cannot be left out; the copy compiles once both sides are built into one destination, and serves live until then.";
      case "column_types":
         return (
            "This source's stored column types differ from the types the model declares, or one cannot be declared on its table, so the served copy leaves it as written and it answers live" +
            // The columns, named with their declared and stored types.
            (problem.detail ? `: ${problem.detail}.` : ".")
         );
      case "no_declaration":
         return "This source's declaration was not found in the compiled model, so the served copy leaves it as written and it answers live.";
      case "definition_not_parsed":
         if (problem.hiddenUnstored?.length) {
            return (
               "This source's `include` hides " +
               problem.hiddenUnstored.map((c) => "`" + c + "`").join(", ") +
               " (private or internal), which its v1 `storage=` table does not hold (another source " +
               "with the same SQL may have built the table, without that column). The served copy " +
               "leaves the source as written, so it answers live."
            );
         }
         return "This source's declaration could not be re-rooted on its table, so the served copy leaves it as written and it answers live.";
      case "pair_join_not_found":
         return "This source inlines a join_many that is not declared on its own in its own trailing `extend` (a source it is built from declares it, or its `join_many:` statement declares other joins too), so its pair table cannot be read with the join as a record and the served copy leaves it as written.";
      case "pair_not_compiled":
         return "This source's pair table (an inlined join_many) could not be read as the source with its join as a record (its text reads a measure or view of the joined source through the join, which the record does not carry), so the served copy leaves it as written.";
      case "reroot_not_compiled":
         return (
            "This source's re-root on its stored table does not compile in the served copy (the server log has the compiler's message)." +
            " The served copy leaves it as written, so it answers live, and the model's other stored sources still serve. This is a Publisher bug; please report it."
         );
      case "read_terms_not_kept":
         return "This source filters by a given in a place its stored table cannot keep, or is filtered and reached here only through a join, so the served copy leaves it as written rather than answer every caller alike.";
      case "fields_not_carried":
         return "This source inherits fields (a measure, view or join declared on the source it is built from) that its stored table does not carry, so the served copy leaves it as written.";
      case "cross_connection_join":
         return (
            `This source is stored in a destination its join${join ? ` \`${join}\`` : ""} cannot reach: ` +
            "the joined source reads another connection. Queries that use the join are answered live; " +
            "the others read the stored table. To serve them too, " +
            (inlinedButKept
               ? "fix what keeps the inlined join from being answered from its stored columns (the warning on " +
                 "this join says what), materialize the joined source into the same destination, or build this " +
                 "source in its own database (drop `storage=`)."
               : takesInline
                 ? "inline the join (inline=), materialize the joined source into the same destination, or build " +
                   "this source in its own database (drop `storage=`)."
                 : // `#@ persist` takes no `inline=`.
                   "materialize the joined source into the same destination, or build this source in its own database (drop `storage=`).")
         );
      case "inline_join_kept":
         return (
            `This source inlines its join${join ? ` \`${join}\`` : ""} (inline=), but its served copy does not compile ` +
            "with the join answered from the stored columns, so the join is kept as written and every query " +
            "through it reads the joined source through the join" +
            compilerReasons(problem.detail) +
            " A definition that reads the joined source past one hop (a join of the joined source), or reads " +
            "one of its measures or views, is the usual cause."
         );
      case "classification_not_kept":
         return (
            "This source's re-root changed what it exposes or how a field is classified, which the load's " +
            "classification checks flag on the served copy, so its queries are not answered from the " +
            "stored tables (a Publisher bug; please report it): " +
            [...(problem.leaks ?? []), ...(problem.coverage ?? [])].join(" ")
         );
   }
}

/** The code of the warning for a bound database table the bind could not read. */
const DATABASE_TABLE_UNREAD_CODE = "database-table-unread";

/** What a `database-table-unread` warning says of each reason. */
const UNREAD_REASON_TEXT: Readonly<Record<UnreadReason, string>> = {
   connection_failed: "its connection could not be opened",
   timed_out: "the catalog read timed out",
   catalog_query_failed: "the catalog query failed",
   not_found: "it was not found in its database's catalog",
   ambiguous: "its name matches more than one table",
   unparseable: "its name does not name a table the catalog can read",
   not_read: "it was not read at this bind",
};

/**
 * One warning per bound database table whose columns the bind could not read
 * (`database-table-unread`): its sources answer live until a read succeeds.
 * The reason is a fixed class, never the driver's text (the log has that).
 * Named by the table, and by the source a query names (a rollup's base), in
 * the model that declares it.
 */
export function databaseTableUnreadWarnings(
   unread: readonly {
      sourceEntityId: string;
      reason: UnreadReason;
      tableName?: string;
   }[],
   plan: BuildPlan | null,
   retryPending: boolean,
): ApiPackageWarning[] {
   const warnings: ApiPackageWarning[] = [];
   const sources = Object.values(plan?.sources ?? {});
   for (const { sourceEntityId, reason, tableName } of unread) {
      const source = sources.find((s) => s.sourceEntityId === sourceEntityId);
      const name = source?.preaggregate?.baseSourceName ?? source?.name;
      const table = source?.preaggregate
         ? `The rollup \`${source.name}\``
         : tableName
           ? `The table \`${tableName}\``
           : "A table";
      warnings.push({
         ...(source?.modelPath ? { model: source.modelPath } : {}),
         ...(name ? { subject: name } : {}),
         code: DATABASE_TABLE_UNREAD_CODE,
         reason,
         severity: "warn",
         message:
            `${table}${name ? ` of \`${name}\`` : ""} could not be read in its database when it was bound (${UNREAD_REASON_TEXT[reason]}), so its queries are answered live. ` +
            (retryPending
               ? "A retry is pending."
               : "It is read again at the next bind or manifest poll."),
      });
   }
   return warnings;
}
