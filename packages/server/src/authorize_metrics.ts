// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

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
import {
   ROW_LEVEL_GATE_REJECTION_CAUSES,
   type RowLevelGateRejectionCause,
} from "./service/authorize";

/** The caller-supplied request field a rejected annotation arrived in. */
export type AuthorizeGuardField =
   | "query"
   | "source_name"
   | "query_name"
   | "compile_source";

let guardRejectionCounter: Counter | null = null;
let bypassCounter: Counter | null = null;
let rowLevelDecisionCounter: Counter | null = null;
let rowLevelRejectionCounter: Counter | null = null;

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
 * Neither is ever double-counted: the walk short-circuits before its own nested
 * `assertAuthorized` call. But do NOT read the two as always paired. `runnable`
 * fires on every bypassed query; `source` fires only when a run target was
 * resolvable from surface syntax before compilation, so an ad-hoc query whose
 * target cannot be pinned up front emits `runnable` alone. Alert on the sum, not
 * on a ratio between them.
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
 * How a row-level `#(authorize)` gate resolved a request.
 *
 * `denied_by_gate` is the fail-closed path: a row-level gate is a filter, not
 * a boolean, so there is no whole-source admission decision left to fall back
 * on when the gate cannot be applied (a given that failed to resolve, a shape
 * the compiled IR no longer matches) — "cannot apply the gate" must deny, and
 * this is how an operator sees that happening.
 *
 * `empty_after_filter` is the other side of the same event: the gate applied
 * cleanly and the filter matched no rows, so the caller got a normal 200 with
 * an empty result. That is NOT an error — it is the deliberate
 * readable-but-empty posture (see the row-level design doc) rather than a 403
 * on a source the caller is allowed to query. It is recorded anyway because a
 * filtered-to-nothing response is otherwise indistinguishable from a source
 * that is genuinely empty, and a spike here is how an operator notices a
 * misconfigured given before a support ticket does.
 *
 * The two share one counter rather than splitting into
 * `_denied_total`/`_empty_total` because they are the two mutually exclusive
 * outcomes of the same decision point (apply the gate, then look at what it
 * did), and a dashboard reading "how did row-level gates resolve" wants them
 * side by side as one `decision` label, not two metric names to remember.
 */
export type RowLevelGateDecision = "denied_by_gate" | "empty_after_filter";

/**
 * Record how one row-level `#(authorize)` gate resolved a request.
 */
export function recordRowLevelGateDecision(
   decision: RowLevelGateDecision,
): void {
   rowLevelDecisionCounter ??= publisherMeter().createCounter(
      "publisher_authorize_row_level_total",
      {
         description:
            "How a row-level `#(authorize)` gate resolved a request. Label: decision ('denied_by_gate'|'empty_after_filter'). 'denied_by_gate' is the fail-closed refusal when the gate could not be applied; 'empty_after_filter' is a successful response with zero rows after the filter matched none, which is NOT an error.",
      },
   );
   rowLevelDecisionCounter.add(1, { decision });
}

/**
 * Record one row-level `#(authorize)` gate refused because its compiled
 * condition is not one of the allowed shapes (see
 * {@link RowLevelGateRejectionCause} and the walk in `./service/authorize`).
 *
 * Call BEFORE throwing, for the same reason as
 * {@link recordAuthorizeGuardRejection}: a downstream `catch` that reshapes or
 * swallows the error must not also lose the metric.
 *
 * Fires at package LOAD for a refused gate — `validateAuthorizeProbes`
 * classifies every row-level `#(authorize)` gate's compiled shape at each
 * entry point before the package is servable, so a rejection blocks the whole
 * load and this is a step function on deploy, not a request-rate signal.
 * That is the expected, and by far the more common, call site: the right
 * alert is "nonzero since the last publish", not a rate or a slope.
 *
 * It ALSO fires per REQUEST from `Model.resolveGateShape`, and for three of
 * the seven causes that is the ONLY call site: `unreachable_given`,
 * `given_usage_unresolvable` and `unclassifiable_condition` are produced
 * nowhere but the request-time resolver (`./service/gate_classification`),
 * so no load-time check refuses them first. Read a nonzero value there as
 * "this entry point denies every request against it", not as "load-time
 * validation was bypassed" — see the paired `cause` label and the source in
 * the request's own logs.
 *
 * So of the seven causes exactly ONE — `legacy_string_gate` — fails the whole
 * model load. Three warn at load and leave the model servable; three only
 * ever appear per request, which makes the counter a rate signal for the
 * entry points they affect rather than the pure deploy-time step function the
 * paragraph above describes.
 *
 * The three that warn at load, each leaving the model servable while flagging
 * one specific thing.
 * `cause: 'entry_point_unexpressible'` fires at load for a gate that is
 * valid but unexpressible at one derived entry point (an `extend` that
 * renamed/excluded/projected away the gated field, or a `query_source`
 * projection) — never from `Model.resolveGateShape`, since that path has no
 * `cause` for a compile failure; it warns and leaves that ONE affected entry
 * point denying every request while the rest of the model serves. "ONE
 * derived entry point" is confirmed by `validateAuthorizeProbes` via the
 * gate's own annotation NOTE OBJECT (shared, by reference, with a base that
 * validated, or absent entirely) — not by gate text, which two unrelated
 * sources can share without one deriving from the other.
 * `source_line_gate_no_given_reference` and
 * `source_line_gate_negated_membership` (`./service/gate_dimension`'s
 * `validateSourceLineGateGivenUsage` W1/W2) are the other two — both WARN
 * rather than refuse, and the model still loads with the flagged gate intact
 * (they name a shape that is almost certainly an authoring mistake, not one
 * that is provably wrong the way an unreachable given or a legacy
 * string-form gate is). A nonzero value from any of these three is a
 * per-gate authoring mistake worth a look, not an outage.
 */
export function recordRowLevelGateRejected(
   cause: RowLevelGateRejectionCause,
): void {
   rowLevelRejectionCounter ??= publisherMeter().createCounter(
      "publisher_authorize_row_level_rejected_total",
      {
         description:
            "Row-level `#(authorize)` gates that were refused, warned about, or could not be resolved. Label: cause (" +
            // Derived from the union, not retyped beside it — the retyped
            // version had already drifted a cause behind.
            ROW_LEVEL_GATE_REJECTION_CAUSES.map((c) => `'${c}'`).join("|") +
            "). Only 'legacy_string_gate' fails the whole model load. 'entry_point_unexpressible', 'source_line_gate_no_given_reference' and 'source_line_gate_negated_membership' warn at load and leave the model servable. 'unreachable_given', 'given_usage_unresolvable' and 'unclassifiable_condition' are request-time only — that entry point denies every request. Alert on any nonzero value since the last publish; the request-time causes also make this a rate signal, so see the doc above before writing an alert.",
      },
   );
   rowLevelRejectionCounter.add(1, { cause });
}

/**
 * Visible for tests. Drops the cached instrument so a fresh `MeterProvider` can
 * capture future emissions. Do NOT call from production code.
 */
export function resetAuthorizeGuardTelemetryForTesting(): void {
   guardRejectionCounter = null;
   bypassCounter = null;
   rowLevelDecisionCounter = null;
   rowLevelRejectionCounter = null;
}
