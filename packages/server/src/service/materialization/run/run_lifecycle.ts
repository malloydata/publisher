// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   InvalidStateTransitionError,
   MaterializationConflictError,
   MaterializationNotFoundError,
} from "../../../errors";
import { classifyRunFailure } from "./build_failure_class";
import { logger } from "../../../logger";
import { errorCodeOf, stackFrames } from "./error_code";
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
import { MaterializationStatus } from "../types";
import { VALID_TRANSITIONS } from "./materialization_run_support";

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
   ): Promise<Materialization> {
      const current = await this.repository().getMaterializationById(id);
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
      return this.repository().updateMaterialization(id, {
         status: next,
         ...extra,
      });
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
            // A background throw answers no request, so log it (with the
            // stack's frames) or it is invisible in the server's output. Its
            // code, never its message: a whole-run failure joins its sources'
            // driver errors, which can quote SQL or a value. The message is on
            // the run's record.
            const code = errorCodeOf(err);
            if (cancelled) {
               // An abort racing a genuine failure records "Cancelled"; this log
               // is then the only place the real error's code appears.
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
               let metadata: Record<string, unknown> | undefined;
               if (retryable !== undefined) {
                  // Its own try: a failed read must never cost the status
                  // transition, which would leave the run active and block
                  // the package's next one.
                  try {
                     const current =
                        await this.repository().getMaterializationById(id);
                     metadata = {
                        ...(current?.metadata ?? {}),
                        errorRetryable: retryable,
                     };
                  } catch (readErr) {
                     logger.warn(
                        "Could not read a failed run's metadata; recording its status without errorRetryable",
                        { materializationId: id, error: errMessage(readErr) },
                     );
                  }
               }
               await this.repository().updateMaterialization(id, {
                  status: next,
                  completedAt: new Date(),
                  error: cancelled ? "Cancelled" : message,
                  ...(metadata ? { metadata } : {}),
               });
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
