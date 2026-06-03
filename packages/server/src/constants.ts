import os from "os";
export const API_PREFIX = "/api/v0";
export const README_NAME = "README.md";
export const PUBLISHER_CONFIG_NAME = "publisher.config.json";
export const PACKAGE_MANIFEST_NAME = "publisher.json";
export const MODEL_FILE_SUFFIX = ".malloy";
export const NOTEBOOK_FILE_SUFFIX = ".malloynb";
/**
 * Default row cap applied to Malloy model queries (the `runnable.run`
 * path used by `getQueryResults` and notebook cell execution) when
 * the user's query doesn't carry its own `LIMIT`. Override at startup
 * via `PUBLISHER_DEFAULT_QUERY_ROW_LIMIT`.
 *
 * This is *the default*, not a hard ceiling. A Malloy query that
 * carries `LIMIT 50000` overrides this. The hard ceiling is
 * {@link DEFAULT_MAX_QUERY_ROWS} (env-tuned via
 * `PUBLISHER_MAX_QUERY_ROWS`), which the model-query path now also
 * enforces — preventing a user `LIMIT 10000000` from blowing up the
 * process.
 *
 * Tuned conservatively (1000) because notebook UIs typically render
 * tabular previews; operators who need larger ad-hoc exports can
 * raise it.
 */
export const DEFAULT_QUERY_ROW_LIMIT = 1000;
/**
 * Maximum number of rows the ad-hoc connection SQL endpoints
 * (`/environments/.../connections/.../sqlQuery`) will return in a
 * single response before failing with HTTP 413. Override at startup
 * via `PUBLISHER_MAX_QUERY_ROWS`.
 *
 * Tuned high enough to handle typical interactive SQL exploration
 * without truncating, while still bounding memory: 100k narrow rows
 * is ~tens of MB of buffered result + a same-order JSON.stringify
 * copy, comfortably under any reasonable pod memory budget. Wide
 * rows (large strings, JSON blobs) are bounded separately by the
 * Step 2 byte budget.
 */
export const DEFAULT_MAX_QUERY_ROWS = 100_000;
/**
 * Maximum aggregate JSON-serialized byte size of an ad-hoc
 * connection SQL response before failing with HTTP 413. Override at
 * startup via `PUBLISHER_MAX_RESPONSE_BYTES`.
 *
 * This is the actual memory bound: row count alone is a poor proxy
 * for memory pressure because a single 10 MB JSON column blows past
 * the 100k-row cap's safe envelope. Enforced only when the
 * underlying connection implements `StreamingConnection` (Postgres,
 * DuckDB, ...) since byte counting requires iterating row-at-a-time
 * — on non-streaming connections the driver has already buffered
 * the whole result by the time we see it, so client-side byte
 * counting gains nothing.
 *
 * 50 MB picked as a middle ground: comfortably accommodates wide
 * exploratory results (50k rows × 1 KB, or 5k rows × 10 KB) while
 * keeping a single pod able to serve a handful of concurrent
 * requests under a typical 1-2 GB memory budget.
 */
export const DEFAULT_MAX_RESPONSE_BYTES = 50_000_000;
/**
 * Default wall-clock timeout (milliseconds) applied to a single query
 * — whether it's an ad-hoc connection SQL call or a Malloy model
 * query. Override at startup via `PUBLISHER_QUERY_TIMEOUT_MS`; `0`
 * disables the timeout entirely.
 *
 * 5 minutes is generous enough to let ad-hoc materializations and
 * larger interactive queries finish, while still well under the
 * 10-minute HTTP socket timeout so the publisher returns a clean
 * `504 Gateway Timeout` instead of letting the load balancer
 * disconnect mid-stream. Operators running long-tail analytical
 * jobs should raise this; operators running a high-RPS API surface
 * may want to lower it.
 */
export const DEFAULT_QUERY_TIMEOUT_MS = 300_000;
/**
 * Default upper bound on the number of inbound query requests a
 * single publisher pod will handle concurrently. Excess requests
 * are rejected with `503 Service Unavailable` so the upstream load
 * balancer can retry / shed load instead of letting them pile up
 * and stampede memory. Override at startup via
 * `PUBLISHER_MAX_CONCURRENT_QUERIES`; `0` disables the limit.
 *
 * 32 picked as a safe out-of-the-box value: it bounds aggregate
 * memory usage at ~32× the per-query byte cap
 * ({@link DEFAULT_MAX_RESPONSE_BYTES}, i.e. ~1.5 GB at the default
 * 50 MB) while leaving substantial headroom for the API surface,
 * notebook execution, and background materializations. Pods sized
 * larger than the default should raise it; pods running on tight
 * memory budgets should lower it.
 */
export const DEFAULT_MAX_CONCURRENT_QUERIES = 32;
export const TEMP_DIR_PATH = os.tmpdir();
export const PUBLISHER_DATA_DIR = "publisher_data";
