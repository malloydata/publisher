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
let bypassCounter: Counter | null = null;

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
 * Which gate entry point skipped its evaluation under an authorize bypass.
 * `source` is {@link Model.assertAuthorized} (a named source, including the
 * early surface-syntax gate); `runnable` is
 * {@link Model.assertAuthorizedForAllSources} (the compiled entry-point walk).
 * A single bypassed query emits one of each — the walk short-circuits before
 * its own nested `assertAuthorized` call, so neither is double-counted.
 */
export type AuthorizeBypassEntryPoint = "source" | "runnable";

/**
 * Record one gate evaluation skipped because the request carried an authorize
 * bypass (the private data-management path).
 *
 * Deliberately unlabelled by org / package / model / source: this counter is
 * the alertable rate signal, and those identifiers are unbounded-cardinality.
 * They are carried on the paired audit log line instead, which is what an
 * investigation reads once the rate signal fires.
 */
export function recordAuthorizeBypass(
   entryPoint: AuthorizeBypassEntryPoint,
): void {
   bypassCounter ??= publisherMeter().createCounter(
      "publisher_authorize_bypass_total",
      {
         description:
            "Gate evaluations skipped because the request carried an authorize bypass (private data-management path). Label: entry_point ('source'|'runnable'). Any nonzero value on a path that should not use the bypass is a finding — see the paired `authorize bypass` audit log line for org/package/model/source.",
      },
   );
   bypassCounter.add(1, { entry_point: entryPoint });
}

/**
 * Visible for tests. Drops the cached instrument so a fresh `MeterProvider` can
 * capture future emissions. Do NOT call from production code.
 */
export function resetAuthorizeGuardTelemetryForTesting(): void {
   guardRejectionCounter = null;
   bypassCounter = null;
}
