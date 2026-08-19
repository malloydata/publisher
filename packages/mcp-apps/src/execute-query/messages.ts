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
