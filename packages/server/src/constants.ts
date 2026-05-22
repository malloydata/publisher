import os from "os";
export const API_PREFIX = "/api/v0";
export const README_NAME = "README.md";
export const PUBLISHER_CONFIG_NAME = "publisher.config.json";
export const PACKAGE_MANIFEST_NAME = "publisher.json";
export const MODEL_FILE_SUFFIX = ".malloy";
export const NOTEBOOK_FILE_SUFFIX = ".malloynb";
export const ROW_LIMIT = 1000;
/**
 * Maximum number of rows the ad-hoc connection SQL endpoints
 * (`/environments/.../connections/.../sqlQuery` and the deprecated
 * `queryData` GET) will return in a single response before failing
 * with HTTP 413. Override at startup via `PUBLISHER_MAX_QUERY_ROWS`.
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
export const TEMP_DIR_PATH = os.tmpdir();
export const PUBLISHER_DATA_DIR = "publisher_data";
