// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Authorize telemetry: caller-submitted `#(authorize)` rejections (HTTP 400),
 * bypass use, and how gates resolved at load and per request. Operator-facing
 * meaning of every counter and label is in docs/authorize.md ("Metrics").
 *
 * Guard rejections get their own counter because nothing else surfaces them:
 * the query path rethrows the `BadRequestError` without touching
 * `malloy_model_query_duration`, `/compile` has no histogram, and a bare
 * `http_server_requests_total{status_code="400"}` cannot separate a forged-gate
 * probe from a syntax error. `field` separates the populations: a gate pasted
 * into `query` / `compile_source` may be an honest mistake, one in
 * `source_name` / `query_name` (bare identifiers) is not.
 *
 * Instruments are created lazily, as in {@link ./query_cap_metrics}: one
 * created before `setGlobalMeterProvider` binds to a NoOp meter
 * (https://github.com/open-telemetry/opentelemetry-js/issues/3505).
 */

import { lazyCounter, resetInstrumentsForTesting } from "./telemetry";
import {
   lastCallerGuardRefusalKind,
   ROW_LEVEL_GATE_REJECTION_CAUSES,
   type RowLevelGateRejectionCause,
} from "./service/authorize";
import { CANONICAL_AUTHORIZE_ROUTES } from "./service/authorize_routes";

/** The caller-supplied request field a rejected annotation arrived in. */
export type AuthorizeGuardField =
   | "query"
   | "source_name"
   | "query_name"
   | "compile_source";

const guardRejectionCounter = lazyCounter(
   "publisher_authorize_guard_rejected_total",
   "Requests rejected with 400 for declaring an `#(authorize)` annotation in caller-submitted Malloy text. Labels: field ('query'|'source_name'|'query_name'|'compile_source'), match ('lexed' when Malloy's lexer placed a tag outside prose, 'whole_text' when the text could not be lexed cleanly or is a name, 'no_lexer' when Malloy's parser failed to load so every prose-bearing text refuses).",
);
const bypassCounter = lazyCounter(
   "publisher_authorize_bypass_total",
   "Gate evaluations skipped because the request carried an authorize bypass (private data-management path). Label: entry_point ('source'|'runnable'). Any nonzero value on a path that should not use the bypass is a finding — see the paired `authorize bypass` audit log line for org/package/model/source.",
);
const rowLevelDecisionCounter = lazyCounter(
   "publisher_authorize_row_level_total",
   "How a row-level `#(access_filter)` gate resolved a request. Labels: decision ('denied_by_gate'|'empty_after_filter'), site ('entry_point'|'caller_join'). 'denied_by_gate' is the fail-closed refusal when the gate could not be applied; 'empty_after_filter' is a successful response with zero rows after the filter matched none, which is NOT an error. site says whether the gate was the query's own run target or a source the caller's text joined.",
);
const rowLevelRejectionCounter = lazyCounter(
   "publisher_authorize_row_level_rejected_total",
   "Row-level `#(access_filter)` gates that were refused, warned about, or could not be resolved. Label: cause (" +
      // Derived from the union, not retyped beside it, so the two cannot drift.
      ROW_LEVEL_GATE_REJECTION_CAUSES.map((c) => `'${c}'`).join("|") +
      "). Only 'legacy_string_gate' fails the whole model load. 'entry_point_unexpressible', 'source_line_gate_no_given_reference' and 'source_line_gate_negated_membership' warn at load and leave the model servable. 'unreachable_given', 'given_usage_unresolvable' and 'unclassifiable_condition' are request-time only — that entry point denies every request. Alert on any nonzero value since the last publish; the request-time causes also make this a rate signal, so see the doc above before writing an alert.",
);
const admitAllCounter = lazyCounter(
   "publisher_authorize_admit_all_total",
   "Sources declaring their OWN unconditional admit-all gate (`#(authorize) true`), counted once each at package load. Label: route (" +
      // Derived from the canonical routes, not retyped, so the two cannot drift.
      CANONICAL_AUTHORIZE_ROUTES.map((r) => `'${r}'`).join("|") +
      "). Not an error — `true` is the only spelling for an extension that deliberately re-opens a gated base — but it is the one declaration that turns a gate off, so a jump since the last publish is worth a look.",
);
const lockDecisionCounter = lazyCounter(
   "publisher_authorize_lock_total",
   "How a `#(authorize)` lock resolved a request. Labels: decision ('admitted'|'denied_by_lock'|'denied_unresolvable'), site ('entry_point'|'caller_join'). Both denials are a 403; 'denied_by_lock' is the gate refusing a caller it does not admit (routine), 'denied_unresolvable' is the fail-closed refusal when the gate could not be decided at all. site says whether the lock was on the query's own run target or on a source the caller's text joined.",
);

/**
 * Record one caller-declared-authorize rejection. Call BEFORE throwing: a
 * downstream `catch` that reshapes or swallows the error must not also lose
 * the metric.
 */
export function recordAuthorizeGuardRejection(
   field: AuthorizeGuardField,
): void {
   guardRejectionCounter().add(1, {
      field,
      match: lastCallerGuardRefusalKind(),
   });
}

/**
 * Which gate entry point skipped its evaluation under an authorize bypass:
 * `source` is {@link Model.assertAuthorized}, `runnable` is
 * {@link Model.assertAuthorizedForAllSources}. Never double-counted, but not
 * always paired: `source` fires only when a run target was resolvable before
 * compilation, so alert on the sum, not a ratio.
 */
export type AuthorizeBypassEntryPoint = "source" | "runnable";

/**
 * Record one gate evaluation skipped under an authorize bypass. Deliberately
 * unlabelled by org / package / model / source (unbounded cardinality); those
 * are on the paired audit log line.
 */
export function recordAuthorizeBypass(
   entryPoint: AuthorizeBypassEntryPoint,
): void {
   bypassCounter().add(1, { entry_point: entryPoint });
}

/**
 * How a row-level `#(access_filter)` gate resolved a request.
 *
 * `denied_by_gate` is fail-closed: a row filter has no whole-source decision
 * to fall back on, so "cannot apply the gate" must deny. `empty_after_filter`
 * is a normal 200 with zero rows — not an error, but otherwise
 * indistinguishable from a genuinely empty source, so a spike flags a
 * misconfigured given. One counter because they are the two exclusive outcomes
 * of one decision.
 */
export type RowLevelGateDecision = "denied_by_gate" | "empty_after_filter";

/**
 * Where a gate was reached: the query's own run target (`entry_point`), or a
 * join the caller's text wrote into a gated source (`caller_join`).
 */
export type AuthorizeGateSite = "entry_point" | "caller_join";

/**
 * Record how one row-level `#(access_filter)` gate resolved a request.
 */
export function recordRowLevelGateDecision(
   decision: RowLevelGateDecision,
   site: AuthorizeGateSite = "entry_point",
): void {
   rowLevelDecisionCounter().add(1, { decision, site });
}

/**
 * Record one row-level `#(access_filter)` gate refused or warned about (see
 * {@link RowLevelGateRejectionCause}). Call BEFORE throwing, so a downstream
 * `catch` cannot lose the metric.
 *
 * Fires mostly at package LOAD from `validateAuthorizeProbes` — a step
 * function on deploy, where the right alert is "nonzero since the last
 * publish". `unreachable_given`, `given_usage_unresolvable` and
 * `unclassifiable_condition` come only from the request-time resolver
 * (`Model.resolveGateShape`), so they are a rate signal meaning "this entry
 * point denies every request", not a bypassed load check. Only
 * `legacy_string_gate` fails the load; the other load-time causes warn and
 * leave the model servable. Per-cause meaning is in docs/authorize.md.
 */
export function recordRowLevelGateRejected(
   cause: RowLevelGateRejectionCause,
): void {
   rowLevelRejectionCounter().add(1, { cause });
}

/**
 * Record one source that declares its OWN unconditional admit-all gate
 * (`#(authorize) true`).
 *
 * `true` is the only body that turns a gate OFF, and on an extension it
 * re-opens a locked base. That is deliberate, not a fault, but its blast
 * radius is invisible from outside, so it is counted. Fires once per own
 * declaration at package load (`assertAuthorizeGrammarValid`); an inherited
 * `true` is not counted, so the number tracks authoring decisions rather than
 * model shape.
 *
 * The `route` label is constant (sentinels are legal only on the lock route)
 * but stays because it is already on the wire.
 */
export function recordAuthorizeAdmitAllGate(route: string): void {
   admitAllCounter().add(1, { route });
}

/**
 * How a `#(authorize)` LOCK resolved a request.
 *
 * Its own counter, not a third `decision` on
 * `publisher_authorize_row_level_total`: that counter's `denied_by_gate` is
 * the fail-closed case operators alert on, and a routine "not admitted" 403
 * would fire that alert. `denied_by_lock` is the feature working;
 * `denied_unresolvable` means the gate could not be decided, the only one
 * signalling something is wrong.
 */
export type LockDecision =
   | "admitted"
   | "denied_by_lock"
   | "denied_unresolvable";

/** Record how one `#(authorize)` lock resolved a request. */
export function recordLockDecision(
   decision: LockDecision,
   site: AuthorizeGateSite = "entry_point",
): void {
   lockDecisionCounter().add(1, { decision, site });
}

/**
 * Visible for tests. Drops the cached instrument so a fresh `MeterProvider` can
 * capture future emissions. Do NOT call from production code.
 */
export function resetAuthorizeGuardTelemetryForTesting(): void {
   resetInstrumentsForTesting();
}
