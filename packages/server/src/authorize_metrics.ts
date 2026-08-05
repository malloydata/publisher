/**
 * Telemetry for caller-submitted `#(authorize)` rejections (HTTP 400).
 *
 * `assertNoCallerAuthorizeAnnotation` refuses an authorize annotation in any
 * caller-supplied Malloy text, because a source's own gate replaces the gate it
 * would otherwise inherit and that override is the model author's to make. A
 * rejection is therefore either an author using the wrong door or somebody
 * probing for a forged-gate bypass, and both are worth seeing.
 *
 * Nothing else surfaces them. The query path rethrows a `BadRequestError`
 * straight out of its parse `catch` without touching
 * `malloy_model_query_duration`, so a rejection spike is invisible in the query
 * histogram; on the `/compile` door there is no histogram at all. Undifferentiated
 * `http_server_requests_total{status_code="400"}` cannot separate this from a
 * plain syntax error.
 *
 * `field` names the request field the annotation arrived in, which is what
 * distinguishes the two populations: `query` / `compile_source` are the fields an
 * author would plausibly paste a gate into by mistake, while `source_name` and
 * `query_name` are supposed to be bare identifiers — an annotation there is not a
 * mistake anyone makes by accident.
 *
 * The instrument is created lazily for the same reason as
 * {@link ./query_cap_metrics}: one created before `setGlobalMeterProvider` binds
 * to a NoOp meter (https://github.com/open-telemetry/opentelemetry-js/issues/3505).
 */

import { type Counter } from "@opentelemetry/api";
import { publisherMeter } from "./telemetry";

/** The caller-supplied request field a rejected annotation arrived in. */
export type AuthorizeGuardField =
   | "query"
   | "source_name"
   | "query_name"
   | "compile_source";

let guardRejectionCounter: Counter | null = null;

/**
 * Record one caller-declared-authorize rejection. Call BEFORE throwing, for the
 * same reason as {@link recordQueryCapExceeded}: a downstream `catch` that
 * reshapes or swallows the error must not also lose the metric.
 */
export function recordAuthorizeGuardRejection(
   field: AuthorizeGuardField,
): void {
   guardRejectionCounter ??= publisherMeter().createCounter(
      "publisher_authorize_guard_rejected_total",
      {
         description:
            "Requests rejected with 400 for declaring an `#(authorize)` annotation in caller-submitted Malloy text. Label: field ('query'|'source_name'|'query_name'|'compile_source').",
      },
   );
   guardRejectionCounter.add(1, { field });
}

/**
 * Visible for tests. Drops the cached instrument so a fresh `MeterProvider` can
 * capture future emissions. Do NOT call from production code.
 */
export function resetAuthorizeGuardTelemetryForTesting(): void {
   guardRejectionCounter = null;
}
