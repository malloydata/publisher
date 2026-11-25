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
 * redacted). In-process only: a run on another
 * process cannot be cancelled from here.
 */

import { BuildCancelledError } from "../domain/build_cancelled";
import {
   InvalidStateTransitionError,
   MaterializationConflictError,
   MaterializationNotFoundError,
} from "../../../errors";
import { classifyRunFailure, failedRunManifestOf } from "./build_failure_class";
import { logger } from "../../../logger";
import { redactPgSecrets, redactStorageUrls } from "../../../pg_helpers";
import {
   Materialization,
   MaterializationUpdate,
   ResourceRepository,
} from "../../../storage/DatabaseInterface";
import { errMessage } from "../../../utils";
import { MaterializationStatus } from "../domain/types";
import { VALID_TRANSITIONS } from "./run_status";
import { errorCodeOf } from "../../../error_code";
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
   /** Each in-flight run's package, and its settle. */
   private readonly runningPackages = new Map<
      string,
      { environmentName: string; packageName: string; settled: Promise<void> }
   >();

   /**
    * The runs {@link abortPackageRuns} aborted: their package is being
    * deleted or unloaded, so the tables they wrote have no run to serve
    * them. Cleared when the run settles.
    */
   private readonly retiring = new Set<string>();

   /** Each create whose run has not started yet, so a retiring package's abort reaches it. */
   private readonly creating = new Set<{
      environmentName: string;
      packageName: string;
      controller: AbortController;
      settled: Promise<void>;
   }>();

   constructor(private readonly repository: () => ResourceRepository) {}

   /**
    * Register a create of `owner`'s run before the create's first await, so
    * {@link abortPackageRuns} aborts it as it aborts a running run: the
    * create reads `signal` once its record is written and, aborted, records
    * CANCELLED rather than start the run. `done` ends the registration, once
    * the create has started its run (which the abort then reaches as a run)
    * or given up; the abort's settle waits for it.
    */
   beginCreate(owner: { environmentName: string; packageName: string }): {
      signal: AbortSignal;
      done: () => void;
   } {
      const controller = new AbortController();
      let done!: () => void;
      const settled = new Promise<void>((resolve) => {
         done = resolve;
      });
      const entry = { ...owner, controller, settled };
      this.creating.add(entry);
      return {
         signal: controller.signal,
         done: () => {
            this.creating.delete(entry);
            done();
         },
      };
   }

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

   /**
    * Move a run to `next`. The write lands only while the run's status
    * admits `next` (one conditional statement); the record is read only on a
    * miss, to say why.
    */
   async transition(
      id: string,
      next: MaterializationStatus,
      extra?: Omit<MaterializationUpdate, "status">,
   ): Promise<Materialization> {
      const updated = await this.repository().updateMaterializationFrom(
         id,
         { status: next, ...extra },
         statusesInto(next),
      );
      if (updated === null) {
         const current = await this.repository().getMaterializationById(id);
         if (!current) {
            throw new MaterializationNotFoundError(
               `Materialization ${id} not found`,
            );
         }
         this.validateTransition(current.status, next);
         // Admitted now, but not when written: it moved meanwhile.
         throw new InvalidStateTransitionError(
            `Cannot transition to ${next}: materialization ${id} changed while it was written`,
         );
      }
      // Debug: `materialization.run.finished` is the run's terminal event.
      logger.debug("Materialization transition", {
         runId: id,
         packageName: updated?.packageName,
         to: next,
      });
      return updated;
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

   /**
    * Abort every run of `packageName` in `environmentName` running in this
    * process, and every create of one not yet started, before this returns, so a package about to be unloaded starts
    * no further build. Resolves once each has settled (CANCELLED, or what it
    * reached first), or after `settleWithinMs`, whichever is sooner: a
    * statement already sent finishes on a database that ignores the abort,
    * and the run stops at its next check.
    */
   abortPackageRuns(
      environmentName: string,
      packageName: string,
      settleWithinMs: number,
   ): Promise<void> {
      const settles: Promise<void>[] = [];
      for (const [id, run] of this.runningPackages) {
         if (
            run.environmentName !== environmentName ||
            run.packageName !== packageName
         ) {
            continue;
         }
         // Before the abort, which the run may read as soon as it lands.
         this.retiring.add(id);
         this.runningAbortControllers.get(id)?.abort();
         settles.push(run.settled);
      }
      for (const create of this.creating) {
         if (
            create.environmentName !== environmentName ||
            create.packageName !== packageName
         ) {
            continue;
         }
         create.controller.abort();
         settles.push(create.settled);
      }
      if (settles.length === 0) return Promise.resolve();
      let timer: ReturnType<typeof setTimeout> | undefined;
      const bound = new Promise<void>((resolve) => {
         timer = setTimeout(resolve, settleWithinMs);
         timer.unref?.();
      });
      return Promise.race([
         Promise.all(settles).then(() => undefined),
         bound,
      ]).finally(() => clearTimeout(timer));
   }

   /**
    * Whether `id` was aborted because its package is being deleted or
    * unloaded ({@link abortPackageRuns}), rather than stopped.
    */
   isRetiring(id: string): boolean {
      return this.retiring.has(id);
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

   runInBackground(
      id: string,
      run: (signal: AbortSignal) => Promise<void>,
      // Called once the run has settled and its final status is recorded.
      onSettled?: () => void,
      // The run's package, so unloading it aborts the run.
      owner?: { environmentName: string; packageName: string },
   ): void {
      const abortController = new AbortController();
      this.runningAbortControllers.set(id, abortController);

      const settled = run(abortController.signal)
         .catch(async (err) => {
            // Per-source failures arrive already redacted. A whole-run throw has
            // no connection to redact against but can still echo a DSN, so
            // redactPgSecrets (which needs none) is the floor for log and record.
            // No storage path (a bucket, an organization's data path) on the
            // record either: it is read back by callers.
            const message = redactStorageUrls(redactPgSecrets(errMessage(err)));
            const cancelled = abortController.signal.aborted;
            const next = cancelled ? "CANCELLED" : "FAILED";
            const code = errorCodeOf(err);
            // Whether retrying the run could help, for a consumer that
            // decides on retries. A run that built nothing also records each
            // source's own answer, in its manifest's `failures`; any other
            // failed run commits no manifest. Omitted when unclassified, and
            // for a cancel, which was asked for.
            const retryable = cancelled ? undefined : classifyRunFailure(err);
            // Publisher never serves or binds it: every reader that binds or
            // reuses a run counts MANIFEST_FILE_READY runs only. A host may
            // still bind the sources it reused, from its own records.
            const failed = cancelled ? undefined : failedRunManifestOf(err);
            const extra: Record<string, unknown> = {
               ...(retryable !== undefined
                  ? { errorRetryable: retryable }
                  : {}),
               // A cancel that raced a genuine failure records "Cancelled" as
               // the run's error, and the logs carry only its code: the real
               // message is kept here, on the record.
               ...(cancelled && !isOwnCancellation(err)
                  ? { errorAtCancel: message }
                  : {}),
               // Refused sources its `failures` does not report (one beside
               // another source's failure on its address), as a committed
               // run records them.
               ...(failed?.refusedSources
                  ? { refusedSources: failed.refusedSources }
                  : {}),
            };
            // A background throw answers no request, so log it (with the
            // stack's frames) or it is invisible in the server's output,
            // unless the run's own `materialization.run.finished` reported it
            // already. Its code, never its message: a whole-run failure joins
            // its sources' driver errors, which can quote SQL or a value. The
            // message is on the run's record either way.
            const report = () => {
               if (runFailureReported(err)) return;
               if (cancelled) {
                  logger.info("Materialization run cancelled", {
                     runId: id,
                     code,
                  });
               } else {
                  logger.error("Materialization run failed", {
                     runId: id,
                     code,
                     stack: stackFrames(err),
                  });
               }
            };
            try {
               // Read only for the metadata the settle merges into. A failed
               // read never costs the status write, which would leave the run
               // active and block the package's next one.
               let metadata: Record<string, unknown> | undefined;
               if (Object.keys(extra).length > 0) {
                  try {
                     const current =
                        await this.repository().getMaterializationById(id);
                     metadata = { ...(current?.metadata ?? {}), ...extra };
                  } catch (readErr) {
                     logger.warn(
                        "Could not read a run's record; recording its status without its metadata",
                        { runId: id, code: errorCodeOf(readErr) },
                     );
                  }
               }
               // The settle is a transition like any other: a record already
               // terminal (this run's manifest committed before a late throw,
               // or another process stopped it) is kept, and the late throw
               // is not reported as the run's failure.
               const settled =
                  await this.repository().updateMaterializationFrom(
                     id,
                     {
                        status: next,
                        completedAt: new Date(),
                        error: cancelled ? "Cancelled" : message,
                        ...(metadata ? { metadata } : {}),
                        ...(failed
                           ? {
                                // No `builtAt`: nothing was built. The
                                // run's `completedAt` says when it ended.
                                manifest: {
                                   entries: {},
                                   failures: failed.failures,
                                   strict: false,
                                },
                             }
                           : {}),
                     },
                     statusesInto(next),
                  );
               if (settled !== null) {
                  report();
               } else {
                  const now = await this.repository()
                     .getMaterializationById(id)
                     .catch(() => undefined);
                  logger.warn(
                     now
                        ? "Materialization run threw after its status was recorded; keeping it"
                        : "Materialization run threw after its record was deleted",
                     {
                        runId: id,
                        ...(now ? { status: now.status } : {}),
                        code,
                        stack: stackFrames(err),
                     },
                  );
               }
            } catch (transitionErr) {
               report();
               logger.error("Failed to record materialization failure", {
                  runId: id,
                  code,
                  transitionErrorCode: errorCodeOf(transitionErr),
               });
            }
         })
         .finally(() => {
            this.runningAbortControllers.delete(id);
            this.runningPackages.delete(id);
            this.retiring.delete(id);
            onSettled?.();
         });
      if (owner) this.runningPackages.set(id, { ...owner, settled });
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
 * signal between sources and after the last, before any whole-run failure is
 * judged, so a cancelled run always ends in this error), rather than a
 * failure the cancel raced.
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
