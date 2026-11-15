// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { MalloyError } from "@malloydata/malloy";
import {
   BadRequestError,
   ChainedShapeNotCarriedError,
   ChainedUpstreamMissingError,
   ConnectionAuthError,
   ConnectionError,
   DestinationNotFoundError,
   MaterializationEligibilityError,
   ModelCompilationError,
   UnconfiguredConnectionError,
   WarehouseRecomputeRefusedError,
} from "../../../errors";
import {
   causeChain,
   isConnectionFailure,
   isCredentialRejection,
} from "../../../driver_errors";
import { errMessage } from "../../../utils";
import {
   HostSuppliedWrongTierError,
   strictMissSourceId,
} from "./materialization_run_support";
import { InlinedJoinNotUniqueError } from "./inline_uniqueness";

/**
 * Whether retrying one source's failed build could succeed, as reported on
 * `SourceFailure.retryable`:
 *
 * - `false`: deterministic. The same model, instructions and bounds fail the
 *   same way on every attempt (a compile or SQL defect, an eligibility
 *   refusal, an upstream that failed deterministically in this run, a
 *   rejected credential). Retrying spends the build again.
 * - `true`: transient. The failure is not a property of the inputs (a network
 *   or connection fault, a timeout, the build session running out of memory, a
 *   cancellation), so a later attempt can succeed.
 * - `undefined`: not recognized. The field is omitted and the consumer keeps
 *   its own judgment, exactly as for a Publisher that predates the field.
 *
 * Kept conservative on purpose: a signature is listed only when it is
 * unambiguous. A missing table or column (a binder error, an unrecognized
 * name) is left unclassified, because a referenced upstream reclaimed
 * mid-build reads the same as a model defect. So is a persisted upstream
 * missing from the manifest on its own: it may be materialized in another
 * destination (fixed configuration), or left out of instructions an
 * orchestrator fixed, and a retry of the same instructions would not supply it.
 */
export type BuildRetryable = boolean | undefined;

// Lower-cased substrings of a message that mark a failure as deterministic.
// Checked before the transient markers, so a defect whose message echoes SQL
// mentioning, say, a `timeout` column is still read as the defect it is.
const DETERMINISTIC_MARKERS: readonly string[] = [
   // Malloy and SQL compile defects.
   "failed to compile",
   "compilation error",
   "syntax error",
   "parser error",
   // A grant the connection lacks; no retry acquires it.
   "permission denied",
   "access denied",
];

// Lower-cased substrings of a message that mark a failure as transient.
const TRANSIENT_MARKERS: readonly string[] = [
   // The build session's DuckDB memory bound. Retryable: a worker's builds
   // share its memory, so a build that ran out beside others can fit alone,
   // and the consumer's retry cap bounds one that never fits.
   "out of memory error",
   "could not allocate block",
   "failed to pin block",
   "build cancelled",
   "operation was aborted",
   // Phrases, not bare words, so an identifier such as `timeout_ms` quoted in
   // a defect's message does not read as transient.
   "timed out",
   "statement timeout",
   "query timeout",
   "deadline exceeded",
   "socket hang up",
   "connection reset",
   "connection refused",
   "connection terminated unexpectedly",
   "connection lost",
   "network error",
   "too many connections",
   "rate limit exceeded",
   "exceeded rate limits",
   "service unavailable",
   "temporarily unavailable",
];

// System codes a driver folds into its message ("connect ECONNREFUSED
// 10.0.0.1:5432") without setting `code`, which isConnectionFailure reads.
const TRANSIENT_CODES_IN_MESSAGES: readonly string[] = [
   "econnreset",
   "econnrefused",
   "econnaborted",
   "etimedout",
   "esockettimedout",
   "eai_again",
   "enetunreach",
   "ehostunreach",
];

/**
 * Whether `err` is the refusal of a build whose persisted upstream no
 * manifest holds: the chained `storage=` refusal, or Malloy's strict manifest
 * miss on a colocated build. On its own it is unclassified (see
 * {@link BuildRetryable}); {@link classifySourceFailure} classifies it when
 * the upstream failed in this run.
 */
function isUpstreamMissingError(err: unknown): boolean {
   return (
      err instanceof ChainedUpstreamMissingError ||
      strictMissSourceId(err) !== undefined
   );
}

/**
 * Classify one source's build error on its own, without reference to the rest
 * of the run. See {@link BuildRetryable} for the three answers.
 */
export function classifyBuildError(err: unknown): BuildRetryable {
   // The host supplied an upstream at the wrong tier: the same instructions
   // fail the same way, so the host must change them.
   if (err instanceof HostSuppliedWrongTierError) return false;
   // The instructions refused the only build there is (warehouseReach:
   // refuse): the same instructions refuse it again.
   if (err instanceof WarehouseRecomputeRefusedError) return false;
   // An upstream no manifest holds: unclassified on its own (see above).
   if (isUpstreamMissingError(err)) return undefined;
   // A property of the model, the package or the environment's configuration.
   if (
      err instanceof MaterializationEligibilityError ||
      err instanceof InlinedJoinNotUniqueError ||
      err instanceof ChainedShapeNotCarriedError ||
      err instanceof ModelCompilationError ||
      err instanceof MalloyError ||
      err instanceof BadRequestError ||
      err instanceof ConnectionAuthError ||
      err instanceof UnconfiguredConnectionError ||
      err instanceof DestinationNotFoundError
   ) {
      return false;
   }
   // Every ConnectionError the server raises means the query never ran: the
   // database was unreachable, or every session was busy.
   if (err instanceof ConnectionError) return true;
   // The drivers' own reading, shared with the query path: a rejected
   // credential stays rejected, an unreachable database may come back.
   if (isCredentialRejection(err)) return false;
   if (isConnectionFailure(err)) return true;
   for (const e of causeChain(err)) {
      if (e.name === "AbortError") return true;
   }

   const message = errMessage(err).toLowerCase();
   if (DETERMINISTIC_MARKERS.some((m) => message.includes(m))) return false;
   if (TRANSIENT_MARKERS.some((m) => message.includes(m))) return true;
   // A driver that folds the system code into its message ("connect
   // ECONNREFUSED 10.0.0.1:5432") rather than setting `code`.
   if (TRANSIENT_CODES_IN_MESSAGES.some((c) => message.includes(c))) {
      return true;
   }
   return undefined;
}

/** A source that failed earlier in the same run, as a dependent sees it. */
export interface FailedUpstream {
   name: string;
   reason: string;
   retryable: BuildRetryable;
}

/**
 * Classify a source's failure in the context of its run. A build refused
 * because a persisted upstream is missing, where that upstream FAILED earlier
 * in this run, fails for the upstream's reason and inherits its
 * classification: deterministic only when every such upstream's failure is,
 * transient when any is (rebuilding that one could unblock this one), and
 * unclassified otherwise. Its reason then names the upstream's own failure,
 * so the dependent does not read as a mere absence.
 *
 * Any other error is the source's own (for instance a non-strict build that
 * recomputed the failed upstream from raw and then failed itself), so it is
 * classified on its own.
 */
export function classifySourceFailure(
   err: unknown,
   failedUpstreams: readonly FailedUpstream[],
): { retryable: BuildRetryable; upstreamDetail?: string } {
   if (!isUpstreamMissingError(err) || failedUpstreams.length === 0) {
      return { retryable: classifyBuildError(err) };
   }
   const retryable = combineRetryable(failedUpstreams.map((u) => u.retryable));
   const upstreamDetail = failedUpstreams
      .map((u) => `upstream '${u.name}' failed in this run: ${u.reason}`)
      .join("; ");
   return { retryable, upstreamDetail };
}

/**
 * The upstream sources of `sourceID` that failed earlier in this run: the
 * ones the missing-upstream error names (by sourceID for Malloy's strict
 * miss; by name among this source's declared upstreams for the chained
 * refusal), or, when it names none, every upstream the plan says it reads.
 */
export function failedUpstreamsOf(
   sourceID: string,
   err: unknown,
   upstreamsBySourceID: ReadonlyMap<string, readonly string[]>,
   failedBySourceID: ReadonlyMap<string, FailedUpstream>,
): FailedUpstream[] {
   const declared = upstreamsBySourceID.get(sourceID) ?? [];
   // The upstreams the error names, when it names any: only those are why
   // this source failed. A strict miss names one by sourceID; the chained
   // refusal names them by bare name, which two models can share, so a name
   // is matched only among this source's own declared upstreams.
   const missedId = strictMissSourceId(err)?.sourceID;
   const missedNames = new Set(
      err instanceof ChainedUpstreamMissingError ? err.missing : [],
   );
   let ids: Set<string>;
   if (missedId) {
      ids = new Set([missedId]);
   } else if (missedNames.size > 0) {
      ids = new Set(
         declared.filter((id) => {
            const failed = failedBySourceID.get(id);
            return failed !== undefined && missedNames.has(failed.name);
         }),
      );
   } else {
      ids = new Set(declared);
   }
   const out: FailedUpstream[] = [];
   for (const id of ids) {
      if (id === sourceID) continue;
      const failed = failedBySourceID.get(id);
      if (failed) out.push(failed);
   }
   return out;
}

/**
 * One answer for several failures: deterministic only when every one is,
 * transient when any is (retrying could clear that one), unclassified
 * otherwise, including when there are none.
 */
export function combineRetryable(
   values: readonly BuildRetryable[],
): BuildRetryable {
   if (values.some((v) => v === true)) return true;
   if (values.length > 0 && values.every((v) => v === false)) return false;
   return undefined;
}

/**
 * The run that built nothing: every source failed or was refused. Thrown in
 * place of the joined message so the run's record can say whether retrying
 * the run could help (`metadata.errorRetryable`), since a run that commits no
 * manifest reports no per-source `failures`.
 */
export class BuildRunFailedError extends Error {
   readonly retryable: BuildRetryable;

   constructor(message: string, retryable: BuildRetryable) {
      super(message);
      this.name = "BuildRunFailedError";
      this.retryable = retryable;
   }
}

/** Whether retrying a whole run that threw `err` could succeed. */
export function classifyRunFailure(err: unknown): BuildRetryable {
   return err instanceof BuildRunFailedError
      ? err.retryable
      : classifyBuildError(err);
}
