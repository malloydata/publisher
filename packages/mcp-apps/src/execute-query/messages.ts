// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// User- and agent-facing copy for the cases where a query result cannot be
// displayed. Kept in its own side-effect-free module so it is unit-testable
// without pulling in the DOM or @malloydata/render.
//
// Malloy *renderer* errors are not here: those are passed through verbatim in
// renderer.ts, because the library's own wording is the most accurate
// description of what it rejected. Only the cases below, which are not renderer
// errors, get wording of their own.
//
// Two distinct failures, deliberately worded differently. Telling someone their
// result is too large when the real cause is that no payload arrived sends them
// to shrink a query that was never the problem.

/**
 * A payload was found but did not parse as JSON. In practice this means the
 * host truncated an oversized result, leaving invalid JSON behind.
 */
export const RESULT_TOO_LARGE_HUMAN =
   "This result is too large to display. Add a `limit:` clause or filters to your query to reduce it.";

/** The same case, worded so the agent can recover on its own. */
export const RESULT_TOO_LARGE_AGENT =
   "The malloy_executeQuery result could not be displayed: the tool output exceeded the " +
   "result size limit and was truncated, so it no longer parsed as JSON. Re-run the " +
   "query with a `limit:` clause or additional filters to reduce the number of rows.";

/**
 * No content block carried a payload at all.
 *
 * Distinct from the truncation case above and not the user's fault: the query
 * may have been perfectly fine. Shrinking it would not help, so the copy does
 * not suggest that.
 */
export const NO_PAYLOAD_HUMAN =
   "This result could not be displayed: the tool returned no result payload to render.";

/** The same case, worded so the agent does not retry a smaller query for nothing. */
export const NO_PAYLOAD_AGENT =
   "The malloy_executeQuery result could not be displayed: the tool response carried no " +
   "result payload, in either a resource block or a text block. This is not a size problem, " +
   "so re-running with a smaller limit will not help. Read the tool result directly instead.";

/**
 * A payload arrived and parsed as JSON, but is not an object.
 *
 * A third distinct case, and it needs its own wording for the same reason the
 * two above do. `JSON.parse("123")` and `JSON.parse("null")` both succeed, so
 * the truncation branch never fired: a scalar then failed on the first property
 * access inside the host's notification handler and a null returned early
 * forever, and in both cases the card sat on "Waiting for query result..."
 * saying nothing to anyone. It is not a size problem and there is no missing
 * payload, so neither message above tells the truth about it.
 */
export const MALFORMED_PAYLOAD_HUMAN =
   "This result could not be displayed: the tool returned a result payload that was not in the expected format.";

/** The same case, worded so the agent does not retry a smaller query. */
export const MALFORMED_PAYLOAD_AGENT =
   "The malloy_executeQuery result could not be displayed: the tool response payload parsed as JSON " +
   "but was not an object, so it carried no rows or schema to render. This is not a size problem, so " +
   "re-running with a smaller limit will not help. Read the tool result directly instead.";

/**
 * The `ui/initialize` handshake with the host failed.
 *
 * Human-only: the agent already has the tool result, so nothing here is
 * actionable for it. This failure is between the widget and the host.
 */
export const CONNECT_FAILED_HUMAN =
   "This result could not be displayed: the widget could not connect to the chat client.";
