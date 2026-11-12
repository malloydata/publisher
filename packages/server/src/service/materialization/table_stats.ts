// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Table statistics (materialization v2, T2): for each table a package's build
 * plan materializes, what it costs and how it is used, from data Publisher
 * already holds. No warehouse is queried.
 *
 * - Size and cost come from the runs Publisher recorded (`materializations`):
 *   the newest run that built the table gives its row count, columns, build
 *   time and the bytes its read scanned; every recorded run that built or
 *   failed it gives the build counts.
 * - Use comes from the workload log's local store: each answered query's path
 *   report says which table answered and why each other one was skipped. When
 *   the store is off there is no use to read, and the answer says so rather
 *   than reporting zero.
 *
 * Tables are keyed by `tableId`, the source's content address
 * (`sourceEntityId`), the key the plan, the manifest and the path report share.
 * Pure: the service gathers the inputs.
 */

import type { components } from "../../api";
import type { WorkloadTableUse } from "../workload_log_store";
import type { BuildPlan, ManifestEntry } from "./types";

export type ApiTableStats = components["schemas"]["TableStats"];
export type ApiTableStat = components["schemas"]["TableStat"];

/** One recorded run, as table stats read it. */
export interface RecordedRun {
   id: string;
   status: string;
   completedAt: Date | null;
   manifest: {
      builtAt?: string;
      entries?: Record<string, ManifestEntry>;
      failures?: Record<string, unknown>;
   } | null;
}

export interface TableStatsInput {
   plan: BuildPlan | null;
   /** The package's recorded runs, newest first. */
   runs: readonly RecordedRun[];
   /** The workload window's start and end. */
   since: Date;
   until: Date;
   /** The use the workload log recorded in the window, or why there is none. */
   use: WorkloadTableUse | { unavailable: string };
}

const isNumber = (v: unknown): v is number =>
   typeof v === "number" && Number.isFinite(v);

/** The run's completion time, or its manifest's build time. */
function builtAtOf(run: RecordedRun): string | undefined {
   if (run.completedAt) return run.completedAt.toISOString();
   return run.manifest?.builtAt;
}

/** Assemble the table stats. */
export function computeTableStats(input: TableStatsInput): ApiTableStats {
   const use = "unavailable" in input.use ? undefined : input.use;
   const tables: ApiTableStat[] = [];
   const sources = Object.values(input.plan?.sources ?? {}).sort((a, b) =>
      a.name < b.name ? -1 : a.name > b.name ? 1 : 0,
   );
   for (const source of sources) {
      const tableId = source.sourceEntityId;
      let latest: { run: RecordedRun; entry: ManifestEntry } | undefined;
      let succeeded = 0;
      let failed = 0;
      let durationTotal = 0;
      let durationCount = 0;
      for (const run of input.runs) {
         const entry = run.manifest?.entries?.[tableId];
         // A failure mirrored into `entries` (ManifestEntry.error) is a failure.
         if (entry && !entry.error) {
            succeeded++;
            latest ??= { run, entry };
            if (isNumber(entry.buildDurationMs)) {
               durationTotal += entry.buildDurationMs;
               durationCount++;
            }
         } else if (run.manifest?.failures?.[tableId] !== undefined || entry) {
            failed++;
         }
      }

      const stat: ApiTableStat = {
         tableId,
         sourceName: source.name,
         sourceID: source.sourceID,
         origin: source.origin ?? "persist",
         builds: {
            succeeded,
            failed,
            ...(durationCount > 0
               ? { meanDurationMs: Math.round(durationTotal / durationCount) }
               : {}),
         },
         columns: [],
         columnsFrom: "declared",
      };

      if (latest) {
         const { run, entry } = latest;
         const builtAt = builtAtOf(run);
         stat.lastBuild = {
            materializationId: run.id,
            ...(builtAt ? { builtAt } : {}),
            ...(isNumber(entry.rowCount) ? { rowCount: entry.rowCount } : {}),
            ...(isNumber(entry.buildDurationMs)
               ? { buildDurationMs: entry.buildDurationMs }
               : {}),
            ...(isNumber(entry.queryCostBytes)
               ? { scannedBytes: entry.queryCostBytes }
               : {}),
            ...(entry.storageDestinationName
               ? { storageDestinationName: entry.storageDestinationName }
               : {}),
         };
      }

      // The stored schema where the build captured one (a `storage=` table),
      // else the columns the source declares.
      const stored = latest?.entry.schema;
      if (Array.isArray(stored) && stored.length > 0) {
         stat.columns = stored.flatMap((c) =>
            typeof c?.name === "string" && typeof c?.type === "string"
               ? [{ name: c.name, type: c.type }]
               : [],
         );
         stat.columnsFrom = "stored";
      } else {
         stat.columns = (source.columns ?? []).flatMap((c) =>
            typeof c?.name === "string" && typeof c?.type === "string"
               ? [{ name: c.name, type: c.type }]
               : [],
         );
      }

      if (use) {
         const entry = use.tables[tableId];
         stat.use = {
            considered: entry?.considered ?? 0,
            hits: entry?.hits ?? 0,
            skips: { ...(entry?.skips ?? {}) },
         };
      }
      tables.push(stat);
   }

   return {
      measuredAt: input.until.toISOString(),
      window: {
         since: input.since.toISOString(),
         until: input.until.toISOString(),
      },
      workload: use
         ? {
              available: true,
              answeredQueries: use.answered,
              reportedQueries: use.reported,
           }
         : {
              available: false,
              unavailableReason: (input.use as { unavailable: string })
                 .unavailable,
           },
      runsRead: input.runs.length,
      tables,
   };
}
