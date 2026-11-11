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
} from "../../../errors";
import { errMessage } from "../../../utils";
import { strictMissSourceId } from "./materialization_run_support";

/**
 * Whether retrying one source's failed build could succeed, as reported on
 * `SourceFailure.retryable`:
 *
 * - `false`: deterministic. The same model, instructions and bounds fail the
 *   same way on every attempt (a compile or SQL defect, an eligibility
 *   refusal, the build session's DuckDB memory bound, an upstream that failed
 *   deterministically in this run). Retrying spends the build again.
 * - `true`: transient. The failure is not a property of the inputs (a network
 *   or connection fault, a timeout, a cancellation), so a later attempt can
 *   succeed.
 * - `undefined`: not recognized. The field is omitted and the consumer keeps
 *   its own judgment, exactly as for a Publisher that predates the field.
 *
 * Kept conservative on purpose: a signature is listed only when it is
 * unambiguous. A missing table, for example, is left unclassified, because a
 * referenced upstream reclaimed mid-build reads the same as a model defect.
 */
export type BuildRetryable = boolean | undefined;

// Lower-cased substrings of a message that mark a failure as deterministic.
// Checked before the transient markers, so a defect whose message echoes SQL
// mentioning, say, a `timeout` column is still read as the defect it is.
const DETERMINISTIC_MARKERS: readonly string[] = [
   // The build session's DuckDB memory bound. The input and the bound are the
   // same on every attempt, so the build fails identically until one changes.
   "out of memory error",
   "could not allocate block",
   "failed to pin block",
   // Malloy and SQL compile defects.
   "failed to compile",
   "compilation error",
   "syntax error",
   "parser error",
   "binder error",
   "unrecognized name",
   // A grant the connection lacks; no retry acquires it.
   "permission denied",
   "access denied",
];

// Lower-cased substrings of a message that mark a failure as transient.
const TRANSIENT_MARKERS: readonly string[] = [
   "build cancelled",
   "operation was aborted",
   "timed out",
   "timeout",
   "deadline exceeded",
   "socket hang up",
   "connection reset",
   "connection refused",
   "connection terminated",
   "connection closed",
   "connection lost",
   "network error",
   "too many connections",
   "rate limit",
   "service unavailable",
   "temporarily unavailable",
];

// Node/system error codes of a fault on the way to the database.
const TRANSIENT_CODES: ReadonlySet<string> = new Set([
   "ECONNRESET",
   "ECONNREFUSED",
   "ECONNABORTED",
   "ETIMEDOUT",
   "ESOCKETTIMEDOUT",
   "EPIPE",
   "EAI_AGAIN",
   "ENETUNREACH",
   "EHOSTUNREACH",
   "ENETDOWN",
]);

function errorCode(err: unknown): string | undefined {
   for (let e = err, depth = 0; e && depth < 4; depth++) {
      const code = (e as { code?: unknown }).code;
      if (typeof code === "string") return code;
      e = (e as { cause?: unknown }).cause;
   }
   return undefined;
}

/**
 * Whether `err` is the refusal of a build whose persisted upstream no
 * manifest holds: the chained `storage=` refusal, or Malloy's strict manifest
 * miss on a colocated build. On its own (the upstream merely absent from the
 * instructions) a re-plan can supply it; see {@link classifySourceFailure}
 * for when the upstream failed in this run.
 */
export function isUpstreamMissingError(err: unknown): boolean {
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
   // An upstream nobody supplied: the next plan can supply it.
   if (isUpstreamMissingError(err)) return true;
   // A property of the model, the package or the environment's configuration.
   if (
      err instanceof MaterializationEligibilityError ||
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
   if ((err as { name?: unknown } | null)?.name === "AbortError") return true;
   const code = errorCode(err);
   if (code && TRANSIENT_CODES.has(code)) return true;

   const message = errMessage(err).toLowerCase();
   if (DETERMINISTIC_MARKERS.some((m) => message.includes(m))) return false;
   if (TRANSIENT_MARKERS.some((m) => message.includes(m))) return true;
   // A driver that folds the system code into its message ("connect
   // ECONNREFUSED 10.0.0.1:5432") rather than setting `code`.
   for (const c of TRANSIENT_CODES) {
      if (message.includes(c.toLowerCase())) return true;
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
   const retryable: BuildRetryable = failedUpstreams.some(
      (u) => u.retryable === true,
   )
      ? true
      : failedUpstreams.every((u) => u.retryable === false)
        ? false
        : undefined;
   const upstreamDetail = failedUpstreams
      .map((u) => `upstream '${u.name}' failed in this run: ${u.reason}`)
      .join("; ");
   return { retryable, upstreamDetail };
}

/**
 * The upstream sources of `sourceID` that failed earlier in this run: those
 * the plan says it reads, plus any a missing-upstream error names (by name,
 * for the chained refusal, or by sourceID, for Malloy's strict miss).
 */
export function failedUpstreamsOf(
   sourceID: string,
   err: unknown,
   upstreamsBySourceID: ReadonlyMap<string, readonly string[]>,
   failedBySourceID: ReadonlyMap<string, FailedUpstream>,
): FailedUpstream[] {
   const ids = new Set(upstreamsBySourceID.get(sourceID) ?? []);
   const missedId = strictMissSourceId(err)?.sourceID;
   if (missedId) ids.add(missedId);
   const missedNames = new Set(
      err instanceof ChainedUpstreamMissingError ? err.missing : [],
   );
   const out: FailedUpstream[] = [];
   for (const [id, failed] of failedBySourceID) {
      if (id === sourceID) continue;
      if (ids.has(id) || missedNames.has(failed.name)) out.push(failed);
   }
   return out;
}
