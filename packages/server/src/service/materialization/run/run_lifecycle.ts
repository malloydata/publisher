// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A run's state machine and its life in this process ({@link RunLifecycle}):
 * every status change through the transitions `VALID_TRANSITIONS` admits,
 * cooperative cancellation, and a background run's settle, recording its
 * terminal status and its failure. Read by the materialization service, the
 * manifest committer and the reclaimer.
 *
 * Effects: writes the run store, logs a failed or cancelled run (secrets
 * redacted), counts each run's outcome. In-process only: a run on another
 * process cannot be cancelled from here.
 */

import {
   BuildCancelledError,
   InvalidStateTransitionError,
   MaterializationConflictError,
   MaterializationNotFoundError,
} from "../../../errors";
import { classifyRunFailure } from "./build_failure_class";
import { logger } from "../../../logger";
import {
   MaterializationMode,
   recordMaterializationRun,
} from "../../../materialization_metrics";
import { redactPgSecrets } from "../../../pg_helpers";
import {
   Materialization,
   MaterializationUpdate,
   ResourceRepository,
} from "../../../storage/DatabaseInterface";
import { errMessage } from "../../../utils";
import { MaterializationStatus } from "../domain/types";
import { VALID_TRANSITIONS } from "./materialization_run_support";
import { errorCodeOf } from "./error_code";
import { runFailureReported } from "./run_telemetry";

/**
 * A run's state machine and its life in this process: every status change goes
 * through {@link transition}, which admits only the moves `VALID_TRANSITIONS`
 * lists; a background run is tracked here from start to settle so it can be
 * cancelled, and its terminal status is recorded when it throws.
 *
 * Cancellation is cooperative via AbortController, and in-process only.
 */
export class RunLifecycle {
   /** In-flight runs, so they can be cancelled. In-process only. */
   readonly runningAbortControllers = new Map<string, AbortController>();

   constructor(private readonly repository: () => ResourceRepository) {}

   validateTransition(
      current: MaterializationStatus,
      next: MaterializationStatus,
   ): void {
      if (!VALID_TRANSITIONS[current].includes(next)) {
         throw new InvalidStateTransitionError(
            `Cannot transition from ${current} to ${next}`,
         );
      }
   }

   async transition(
      id: string,
      next: MaterializationStatus,
      extra?: Omit<MaterializationUpdate, "status">,
      // The record as the caller has just read it, so it is not read again.
      read?: Materialization,
   ): Promise<Materialization> {
      const current =
         read ?? (await this.repository().getMaterializationById(id));
      if (!current) {
         throw new MaterializationNotFoundError(
            `Materialization ${id} not found`,
         );
      }
      this.validateTransition(current.status, next);
      // Terminal transitions log at info; the MANIFEST_ROWS_READY hop at debug.
      const terminal =
         next === "MANIFEST_FILE_READY" ||
         next === "FAILED" ||
         next === "CANCELLED";
      logger[terminal ? "info" : "debug"]("Materialization transition", {
         materializationId: id,
         packageName: current.packageName,
         from: current.status,
         to: next,
      });
      // Written only while the status still admits `next`: another writer
      // may have moved it since the read.
      const updated = await this.repository().updateMaterialization(
         id,
         { status: next, ...extra },
         { fromStatuses: statusesInto(next) },
      );
      if (updated) return updated;
      const now = await this.repository().getMaterializationById(id);
      if (!now) {
         throw new MaterializationNotFoundError(
            `Materialization ${id} not found`,
         );
      }
      throw new InvalidStateTransitionError(
         `Cannot transition from ${now.status} to ${next}`,
      );
   }

   /**
    * Cancel a run that has not settled: abort it when it runs in this process,
    * which records CANCELLED when it unwinds, otherwise record CANCELLED now.
    * Returns the updated record, or undefined when the run was aborted.
    */
   async cancel(id: string): Promise<Materialization | undefined> {
      const abortController = this.runningAbortControllers.get(id);
      if (abortController) {
         abortController.abort();
         return undefined;
      }
      return this.transition(id, "CANCELLED", {
         completedAt: new Date(),
         error: "Cancelled",
      });
   }

   /** The single-active conflict error, with the winning run's id when known. */
   activeConflict(
      packageName: string,
      activeId?: string,
      activeVersion?: string | null,
   ): MaterializationConflictError {
      const suffix = activeId
         ? activeVersion
            ? ` (${activeId}, building version ${activeVersion})`
            : ` (${activeId})`
         : "";
      return new MaterializationConflictError(
         `Package ${packageName} already has an active materialization${suffix}`,
      );
   }

   recordRun(
      mode: MaterializationMode,
      outcome: "success" | "partial" | "failed" | "cancelled",
      startedAtMs: number,
   ): void {
      recordMaterializationRun(mode, outcome, Date.now() - startedAtMs);
   }

   runInBackground(
      id: string,
      run: (signal: AbortSignal) => Promise<void>,
      // Called once the run has settled and its final status is recorded.
      onSettled?: () => void,
   ): void {
      const abortController = new AbortController();
      this.runningAbortControllers.set(id, abortController);

      run(abortController.signal)
         .catch(async (err) => {
            // Per-source failures arrive already redacted. A whole-run throw has
            // no connection to redact against but can still echo a DSN, so
            // redactPgSecrets (which needs none) is the floor for log and record.
            const message = redactPgSecrets(errMessage(err));
            const cancelled = abortController.signal.aborted;
            const next = cancelled ? "CANCELLED" : "FAILED";
            const code = errorCodeOf(err);
            // The settle is a transition like any other: a record already
            // terminal (this run's manifest committed before a late throw, or
            // another process stopped it) is kept, and the late throw is not
            // reported as the run's failure. A failed read must never cost the
            // status write, which would leave the run active and block the
            // package's next one, so the run is then settled unchecked.
            let read = false;
            let current: Materialization | null = null;
            try {
               current = await this.repository().getMaterializationById(id);
               read = true;
            } catch (readErr) {
               logger.warn(
                  "Could not read a run's record; recording its status unchecked",
                  { materializationId: id, error: errMessage(readErr) },
               );
            }
            if (current && !VALID_TRANSITIONS[current.status].includes(next)) {
               logger.warn(
                  "Materialization run threw after its status was recorded; keeping it",
                  {
                     materializationId: id,
                     status: current.status,
                     code,
                     stack: stackFrames(err),
                  },
               );
               return;
            }
            // A background throw answers no request, so log it (with the
            // stack's frames) or it is invisible in the server's output,
            // unless the run's own `materialization.run.finished` reported it
            // already. Its code, never its message: a whole-run failure joins
            // its sources' driver errors, which can quote SQL or a value. The
            // message is on the run's record either way.
            if (runFailureReported(err)) {
               // Logged once already, by the run.
            } else if (cancelled) {
               // An abort racing a genuine failure records "Cancelled", with the
               // real message in the record's `errorAtCancel`; this log has its
               // code.
               logger.info("Materialization run cancelled", {
                  materializationId: id,
                  code,
               });
            } else {
               logger.error("Materialization run failed", {
                  materializationId: id,
                  code,
                  stack: stackFrames(err),
               });
            }
            try {
               // Whether retrying the run could help, for a consumer that
               // decides on retries: a failed run commits no manifest, so no
               // per-source `failures` carry it. Omitted when unclassified,
               // and for a cancel, which was asked for.
               const retryable = cancelled
                  ? undefined
                  : classifyRunFailure(err);
               const extra: Record<string, unknown> = {
                  ...(retryable !== undefined
                     ? { errorRetryable: retryable }
                     : {}),
                  // A cancel that raced a genuine failure records "Cancelled"
                  // as the run's error, and the logs carry only its code: the
                  // real message is kept here, on the record.
                  ...(cancelled && !isOwnCancellation(err)
                     ? { errorAtCancel: message }
                     : {}),
               };
               const metadata =
                  read && Object.keys(extra).length > 0
                     ? { ...(current?.metadata ?? {}), ...extra }
                     : undefined;
               // Only over a status that admits it: a record that settled
               // since the read (its manifest committed, another process
               // stopped it) is kept, as one found settled is above.
               const settled = await this.repository().updateMaterialization(
                  id,
                  {
                     status: next,
                     completedAt: new Date(),
                     error: cancelled ? "Cancelled" : message,
                     ...(metadata ? { metadata } : {}),
                  },
                  { fromStatuses: statusesInto(next) },
               );
               if (!settled) {
                  logger.warn(
                     "Materialization run threw after its status was recorded; keeping it",
                     { materializationId: id, code, stack: stackFrames(err) },
                  );
               }
            } catch (transitionErr) {
               logger.error("Failed to record materialization failure", {
                  materializationId: id,
                  code: errorCodeOf(err),
                  transitionError: errMessage(transitionErr),
               });
            }
         })
         .finally(() => {
            this.runningAbortControllers.delete(id);
            onSettled?.();
         });
   }
}

/** The statuses `VALID_TRANSITIONS` admits a move to `next` from. */
function statusesInto(next: MaterializationStatus): MaterializationStatus[] {
   return (Object.keys(VALID_TRANSITIONS) as MaterializationStatus[]).filter(
      (from) => VALID_TRANSITIONS[from].includes(next),
   );
}

/**
 * Whether `err` is the build loop's own answer to the cancel (it checks the
 * signal between sources), rather than a failure the cancel raced.
 */
function isOwnCancellation(err: unknown): boolean {
   return err instanceof BuildCancelledError;
}

/**
 * An error's stack frames without the message it opens with: where a failure
 * was thrown, for a log that may not carry what it said. The message is cut
 * off by its own text, since a line of it (a quoted row value) can look like
 * a frame. A stack whose header is not the message, because the message
 * changed after the stack was read, gives no frames.
 */
export function stackFrames(err: unknown): string | undefined {
   if (!(err instanceof Error) || typeof err.stack !== "string") {
      return undefined;
   }
   const { stack, message } = err;
   // With no message, the header is the error's name alone.
   const at = message === "" ? 0 : stack.indexOf(message);
   const end = message === "" ? stack.indexOf("\n") : at + message.length;
   if (
      at < 0 ||
      end < 0 ||
      stack.slice(0, at).includes("\n") ||
      (end < stack.length && stack[end] !== "\n")
   ) {
      return undefined;
   }
   const frames = stack
      .slice(end)
      .split("\n")
      .filter((line) => /^\s+at /.test(line));
   return frames.length > 0 ? frames.join("\n") : undefined;
}
