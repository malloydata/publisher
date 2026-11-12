// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The workload log's local store (materialization v2, T1): the access records
 * kept, opt-in, in Publisher's own metadata database (`publisher.db`), so
 * table stats, what-if and compare can read the workload by source, shape and
 * window without a log pipeline. Off by default: the record still goes to the
 * log either way, which is where a deployment that ships logs reads it.
 *
 * Writes are buffered and flushed on a timer, so a query never waits on the
 * store, and the store never fails a query: a failed flush drops that batch
 * and logs once per failure. One multi-row INSERT per flush, so the shared
 * metadata connection is held briefly. Bounded by age and by row count,
 * pruned at most once a minute. A graceful shutdown writes what is buffered;
 * a process that dies loses at most the records of one flush interval.
 *
 * The record is stored as the logger writes it, with two exceptions:
 *
 * - The values of `#(secure)` givens are dropped (their names stay in
 *   `givenNames`). Those values are the caller's verified attributes, which an
 *   audit line carries on purpose; a workload store has no use for them, and
 *   keeping them would make it a second copy of identity data.
 * - The error text has every quoted string and number replaced by `?`. A
 *   driver's error quotes the statement back, and a row filter from a
 *   `#(secure)` given is a literal in that statement.
 */

import { logger } from "../logger";
import { settings } from "../settings";
import { lazyCounter } from "../telemetry";
import type { DuckDBConnection } from "../storage/duckdb/DuckDBConnection";
import type { AccessRecord } from "./access_record";
import {
   accessRecordSink,
   setAccessRecordSink,
   type AccessRecordSink,
} from "./access_record_sink";

/** Where the store reads its database from; the storage manager in the server. */
export interface WorkloadLogStorage {
   isInitialized(): boolean;
   getDuckDbConnection(): DuckDBConnection;
}

export interface WorkloadLogStoreOptions {
   /** Days a record is kept. */
   retentionDays: number;
   /** Rows the table may hold; the oldest go first. */
   maxRows: number;
   /** Milliseconds between flushes. */
   flushIntervalMs?: number;
   /** Records buffered before a flush is forced. */
   maxBuffered?: number;
   /** Records held at most; past this the oldest are dropped and counted. */
   maxHeld?: number;
   /** Milliseconds between prunes; a flush prunes only when this has passed. */
   pruneIntervalMs?: number;
   /** The clock, for tests. */
   now?: () => Date;
}

/** One stored row, as `rows()` reads it back. */
export interface WorkloadLogRow {
   recordedAt: string;
   environment: string;
   package: string;
   modelPath: string | null;
   source: string | null;
   shapeKey: string | null;
   servedFrom: string | null;
   outcome: string;
   durationMs: number;
   rowCount: number | null;
   scannedBytes: number | null;
   record: AccessRecord;
}

/** One table's use in {@link WorkloadLogStore.tableUse}. */
export interface WorkloadTableUseEntry {
   /** Answered queries whose path report lists the table. */
   considered: number;
   /** Of those, the queries it answered. */
   hits: number;
   /** The rest, by skip reason (the path report's `SkipReason`). */
   skips: Record<string, number>;
}

/** {@link WorkloadLogStore.tableUse}'s answer. */
export interface WorkloadTableUse {
   /** The package's answered queries in the window. */
   answered: number;
   /** Of those, the ones whose record carries a path report. */
   reported: number;
   tables: Record<string, WorkloadTableUseEntry>;
}

const droppedCounter = lazyCounter(
   "publisher_workload_log_dropped_total",
   "Workload log records the local store could not write (a failed flush, or the buffer full before storage opened). The access record itself is still logged.",
);

const DEFAULT_FLUSH_INTERVAL_MS = 2000;
const DEFAULT_MAX_BUFFERED = 1000;
const DEFAULT_PRUNE_INTERVAL_MS = 60_000;
/**
 * Records held at most, whether storage is still opening or a write is slow;
 * past this the oldest are dropped and counted, so the buffer never grows
 * without bound.
 */
const DEFAULT_MAX_HELD = 10_000;
/** Rows per INSERT statement; a flush writes its batch in as few as fit. */
const ROWS_PER_INSERT = 250;

const COLUMNS = [
   "recorded_at",
   "environment",
   "package",
   "model_path",
   "source",
   "shape_key",
   "served_from",
   "outcome",
   "duration_ms",
   "row_count",
   "scanned_bytes",
   "record",
];

/**
 * An error's text with every quoted string and number replaced by `?`, so a
 * literal the driver quoted back (a secure given's value in a row filter)
 * is not kept.
 */
export function redactErrorLiterals(text: string): string {
   return text
      .replace(/'(?:[^']|'')*'/g, "?")
      .replace(/"(?:[^"\\]|\\.)*"/g, "?")
      .replace(/\b\d+(?:\.\d+)?\b/g, "?");
}

/**
 * The record as the store keeps it: without the `#(secure)` given values, and
 * with the error's literals redacted.
 */
export function storedRecordOf(record: AccessRecord): AccessRecord {
   const { secureGivens: _secure, ...kept } = record;
   return kept.error === undefined
      ? kept
      : { ...kept, error: redactErrorLiterals(kept.error) };
}

export class WorkloadLogStore implements AccessRecordSink {
   private buffer: { at: Date; record: AccessRecord }[] = [];
   private timer: ReturnType<typeof setInterval> | undefined;
   private flushing: Promise<void> | undefined;
   private lastPrunedAt: number | undefined;
   private readonly now: () => Date;
   private readonly maxBuffered: number;
   private readonly maxHeld: number;

   constructor(
      private readonly storage: WorkloadLogStorage,
      private readonly options: WorkloadLogStoreOptions,
   ) {
      this.now = options.now ?? (() => new Date());
      this.maxBuffered = options.maxBuffered ?? DEFAULT_MAX_BUFFERED;
      this.maxHeld = Math.max(
         options.maxHeld ?? DEFAULT_MAX_HELD,
         this.maxBuffered,
      );
   }

   /** Start the flush timer; unref'd, so it never keeps the process alive. */
   start(): void {
      if (this.timer) return;
      this.timer = setInterval(
         () => void this.flush(),
         this.options.flushIntervalMs ?? DEFAULT_FLUSH_INTERVAL_MS,
      );
      this.timer.unref?.();
   }

   /**
    * Stop the timer and write everything buffered, including records that
    * arrive while a write is in flight. Stops early only when storage is not
    * open, since nothing can be written then.
    */
   async stop(): Promise<void> {
      if (this.timer) clearInterval(this.timer);
      this.timer = undefined;
      while (this.flushing || this.buffer.length > 0) {
         if (!this.flushing && !this.storage.isInitialized()) return;
         await this.flush();
      }
   }

   /** Buffer one record. Never throws. */
   add(record: object): void {
      try {
         this.buffer.push({
            at: this.now(),
            record: storedRecordOf(record as AccessRecord),
         });
         if (this.buffer.length > this.maxHeld) {
            const over = this.buffer.length - this.maxHeld;
            this.buffer.splice(0, over);
            droppedCounter().add(over, {
               reason: this.storage.isInitialized()
                  ? "buffer_full"
                  : "not_ready",
            });
         }
         // A write in flight takes the next batch when it finishes.
         if (this.buffer.length >= this.maxBuffered && !this.flushing) {
            void this.flush();
         }
      } catch {
         // Best effort: the record is already in the log.
      }
   }

   /**
    * Write the buffer, then prune when due. Never throws; one write at a time.
    * Called while a write is in flight, it waits for that write and then
    * writes what arrived meanwhile, so a caller that awaits it never leaves a
    * record behind that was buffered when it was called.
    */
   async flush(): Promise<void> {
      if (this.flushing) await this.flushing;
      // Another caller woken by the same write may have started the next one.
      if (this.flushing) return this.flushing;
      if (this.buffer.length === 0 || !this.storage.isInitialized()) return;
      const batch = this.buffer;
      this.buffer = [];
      this.flushing = this.write(batch).finally(() => {
         this.flushing = undefined;
         // Under load the buffer refilled during the write: take it now.
         if (this.buffer.length >= this.maxBuffered) void this.flush();
      });
      return this.flushing;
   }

   private async write(
      batch: { at: Date; record: AccessRecord }[],
   ): Promise<void> {
      try {
         const db = this.storage.getDuckDbConnection();
         const row = `(${COLUMNS.map(() => "?").join(", ")})`;
         await db.transaction(async (tx) => {
            for (let i = 0; i < batch.length; i += ROWS_PER_INSERT) {
               const chunk = batch.slice(i, i + ROWS_PER_INSERT);
               await tx.run(
                  `INSERT INTO workload_log (${COLUMNS.join(", ")}) VALUES ${chunk
                     .map(() => row)
                     .join(", ")}`,
                  chunk.flatMap(({ at, record }) => [
                     at.toISOString(),
                     record.environment,
                     record.package,
                     record.modelPath ?? null,
                     record.source ?? null,
                     record.shape?.key ?? null,
                     record.servedFrom ?? null,
                     record.outcome,
                     record.durationMs,
                     record.rowCount ?? null,
                     record.scannedBytes ?? null,
                     JSON.stringify(record),
                  ]),
               );
            }
         });
         const nowMs = this.now().getTime();
         const pruneEvery =
            this.options.pruneIntervalMs ?? DEFAULT_PRUNE_INTERVAL_MS;
         if (
            this.lastPrunedAt === undefined ||
            nowMs - this.lastPrunedAt >= pruneEvery
         ) {
            this.lastPrunedAt = nowMs;
            await this.prune(db);
         }
      } catch (error) {
         droppedCounter().add(batch.length, { reason: "write_failed" });
         logger.warn("Could not write the workload log batch; dropped it", {
            records: batch.length,
            error: error instanceof Error ? error.message : String(error),
         });
      }
   }

   /** Drop what is older than the retention, then the oldest past the row cap. */
   private async prune(db: DuckDBConnection): Promise<void> {
      const cutoff = new Date(
         this.now().getTime() - this.options.retentionDays * 86_400_000,
      );
      await db.run("DELETE FROM workload_log WHERE recorded_at < ?", [
         cutoff.toISOString(),
      ]);
      const rows = await db.all<{ n: number | bigint }>(
         "SELECT count(*) AS n FROM workload_log",
      );
      const over = Number(rows[0]?.n ?? 0) - this.options.maxRows;
      if (over > 0) {
         await db.run(
            `DELETE FROM workload_log WHERE rowid IN (
               SELECT rowid FROM workload_log ORDER BY recorded_at, rowid LIMIT ?
             )`,
            [over],
         );
      }
   }

   /**
    * How each table was used by one package's answered queries since `since`
    * (table stats, materialization v2 T2): read from the path report each
    * record carries. Keyed by `tableId`, the table's content address, so a
    * table several versions of a package share is counted once. A record
    * from before the path report carries none and is counted only in
    * `answered`.
    */
   async tableUse(params: {
      environment: string;
      package: string;
      since: Date;
   }): Promise<WorkloadTableUse> {
      const db = this.storage.getDuckDbConnection();
      const where =
         "environment = ? AND package = ? AND recorded_at >= ? AND outcome = 'ok'";
      const args = [
         params.environment,
         params.package,
         params.since.toISOString(),
      ];
      const totals = await db.all<Record<string, unknown>>(
         `SELECT count(*) AS answered,
                 count(*) FILTER (WHERE json_extract(record, '$.path') IS NOT NULL) AS reported
          FROM workload_log WHERE ${where}`,
         args,
      );
      const rows = await db.all<Record<string, unknown>>(
         `SELECT json_extract_string(c, '$.tableId') AS table_id,
                 json_extract_string(c, '$.outcome') AS outcome,
                 json_extract_string(c, '$.reason') AS reason,
                 count(*) AS n
          FROM (
             SELECT unnest(json_extract(record, '$.path.considered[*]')) AS c
             FROM workload_log
             WHERE ${where} AND json_extract(record, '$.path') IS NOT NULL
          )
          GROUP BY ALL`,
         args,
      );
      const tables: Record<string, WorkloadTableUseEntry> = {};
      for (const row of rows) {
         const tableId = row.table_id;
         if (typeof tableId !== "string" || tableId === "") continue;
         const entry = (tables[tableId] ??= {
            considered: 0,
            hits: 0,
            skips: {},
         });
         const n = Number(row.n ?? 0);
         entry.considered += n;
         if (row.outcome === "used") {
            entry.hits += n;
         } else {
            const reason = typeof row.reason === "string" ? row.reason : "";
            entry.skips[reason || "UNKNOWN"] =
               (entry.skips[reason || "UNKNOWN"] ?? 0) + n;
         }
      }
      return {
         answered: Number(totals[0]?.answered ?? 0),
         reported: Number(totals[0]?.reported ?? 0),
         tables,
      };
   }

   /** The stored rows, newest first: for tests and the tools that read the log. */
   async rows(limit = 1000): Promise<WorkloadLogRow[]> {
      const db = this.storage.getDuckDbConnection();
      const raw = await db.all<Record<string, unknown>>(
         `SELECT ${COLUMNS.join(", ")} FROM workload_log
          ORDER BY recorded_at DESC, rowid DESC LIMIT ?`,
         [limit],
      );
      const num = (v: unknown) =>
         v === null || v === undefined ? null : Number(v);
      return raw.map((r) => ({
         recordedAt:
            r.recorded_at instanceof Date
               ? r.recorded_at.toISOString()
               : String(r.recorded_at),
         environment: String(r.environment),
         package: String(r.package),
         modelPath: (r.model_path as string | null) ?? null,
         source: (r.source as string | null) ?? null,
         shapeKey: (r.shape_key as string | null) ?? null,
         servedFrom: (r.served_from as string | null) ?? null,
         outcome: String(r.outcome),
         durationMs: Number(r.duration_ms),
         rowCount: num(r.row_count),
         scannedBytes: num(r.scanned_bytes),
         record: (typeof r.record === "string"
            ? JSON.parse(r.record)
            : r.record) as AccessRecord,
      }));
   }
}

/** The store the access record writes to, or none. */
export function workloadLogSink(): WorkloadLogStore | undefined {
   const current = accessRecordSink();
   return current instanceof WorkloadLogStore ? current : undefined;
}

/** Install (or with `undefined`, remove) the store the access record writes to. */
export function setWorkloadLogSink(store: WorkloadLogStore | undefined): void {
   setAccessRecordSink(store);
}

/**
 * Stop the store the access record writes to, writing what it buffered, and
 * remove it. The server's shutdown runs it after the listeners close.
 */
export async function stopWorkloadLogSink(): Promise<void> {
   const store = workloadLogSink();
   if (!store) return;
   setWorkloadLogSink(undefined);
   await store.stop();
}

/**
 * Start the store when `PUBLISHER_WORKLOAD_LOG_STORE` is `on`, installed as the
 * access record's sink; `undefined` when it is off. A store an earlier build
 * installed is stopped and removed either way, so a rebuild with the setting
 * off writes nowhere.
 */
export function startWorkloadLogFromSettings(
   storage: WorkloadLogStorage,
): WorkloadLogStore | undefined {
   const previous = workloadLogSink();
   if (previous) {
      setWorkloadLogSink(undefined);
      void previous.stop();
   }
   if (settings.get("workloadLog.store") !== "on") return undefined;
   const store = new WorkloadLogStore(storage, {
      retentionDays: settings.get("workloadLog.retentionDays"),
      maxRows: settings.get("workloadLog.maxRows"),
   });
   store.start();
   setWorkloadLogSink(store);
   logger.info("Workload log store on", {
      retentionDays: settings.get("workloadLog.retentionDays"),
      maxRows: settings.get("workloadLog.maxRows"),
   });
   return store;
}
