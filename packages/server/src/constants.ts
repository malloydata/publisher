// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import os from "os";
import path from "path";
export const API_PREFIX = "/api/v0";
export const README_NAME = "README.md";
export const PUBLISHER_CONFIG_NAME = "publisher.config.json";
export const PACKAGE_MANIFEST_NAME = "publisher.json";
/**
 * `<environment>/.install-records/<package>.json`: where the server recorded
 * installing each package from. Kept outside every package directory so
 * package content cannot plant one, since a reload trusts it as the source to
 * re-fetch (an author's `location` in `publisher.json` is never read as one).
 */
export const PACKAGE_INSTALL_RECORDS_DIR = ".install-records";

export function installRecordPath(
   environmentPath: string,
   packageName: string,
): string {
   return path.join(
      environmentPath,
      PACKAGE_INSTALL_RECORDS_DIR,
      `${packageName}.json`,
   );
}
export const MODEL_FILE_SUFFIX = ".malloy";
export const NOTEBOOK_FILE_SUFFIX = ".malloynb";
/** The guide to migrating from deprecated `#(filter)` annotations to `given:` parameters. */
export const GIVENS_DOCS_URL =
   "https://github.com/malloydata/publisher/blob/main/docs/givens.md";

/**
 * The model file that declares a package's published surface. With no
 * `explores` in publisher.json, what it `export { ... }`s is what the package
 * lists and serves. Fixed rather than configurable, so a reader can predict a
 * package's entry point without opening its manifest. See
 * {@link resolveExplores}.
 */
export const INDEX_MODEL_NAME = "index.malloy";

/**
 * Normalize a package-relative model path (forward slashes, no leading "./")
 * so author-written `explores` compare equal to `listPackageFiles` output,
 * whether the manifest came from disk or the API.
 */
export function normalizeModelPath(p: string): string {
   return p.replace(/\\/g, "/").replace(/^\.\//, "");
}

/**
 * Row limit for a Malloy model query that carries no `LIMIT` of its own
 * (`PUBLISHER_DEFAULT_QUERY_ROW_LIMIT`). A default, not a ceiling: the hard
 * ceiling is {@link DEFAULT_MAX_QUERY_ROWS}. Conservative because UIs mostly
 * render tabular previews.
 */
export const DEFAULT_QUERY_ROW_LIMIT = 1000;
/**
 * Hard row ceiling for one query response before a 413
 * (`PUBLISHER_MAX_QUERY_ROWS`). 100k narrow rows is tens of MB buffered plus
 * a same-order JSON copy; wide rows are bounded by
 * {@link DEFAULT_MAX_RESPONSE_BYTES}.
 */
export const DEFAULT_MAX_QUERY_ROWS = 100_000;
/**
 * Maximum JSON-serialized size of an ad-hoc connection SQL response before a
 * 413 (`PUBLISHER_MAX_RESPONSE_BYTES`). This is the real memory bound, since
 * one wide column defeats a row cap. Enforced only on a `StreamingConnection`:
 * a non-streaming driver has already buffered the whole result.
 */
export const DEFAULT_MAX_RESPONSE_BYTES = 50_000_000;
/**
 * Wall-clock timeout (ms) for one query, SQL or Malloy
 * (`PUBLISHER_QUERY_TIMEOUT_MS`; `0` disables). Kept under the 10-minute
 * socket timeout so the caller gets a clean 504 rather than a dropped
 * connection.
 */
export const DEFAULT_QUERY_TIMEOUT_MS = 300_000;
/**
 * Idle-socket timeout (ms) for the REST and MCP servers while the query
 * timeout is under it. A socket that sends nothing this long is destroyed,
 * even mid-request, so a longer configured query timeout raises it (see
 * `getServerSocketTimeoutMs`).
 */
export const DEFAULT_SOCKET_TIMEOUT_MS = 600_000;
/**
 * How long the socket outlives the query timeout, so the timeout's 504 is
 * written before the socket is torn down.
 */
export const SOCKET_TIMEOUT_GRACE_MS = 60_000;
/**
 * Concurrent query requests per pod; excess requests get a 503 so the load
 * balancer can shed load (`PUBLISHER_MAX_CONCURRENT_QUERIES`; `0` disables).
 * Bounds aggregate memory at ~32x {@link DEFAULT_MAX_RESPONSE_BYTES}.
 */
export const DEFAULT_MAX_CONCURRENT_QUERIES = 32;
/**
 * Default cap on open database sessions for one plain (non-proxied) Postgres
 * connection in one publisher process. Override at startup via
 * `PUBLISHER_POSTGRES_POOL_MAX`. Kept small because a role's CONNECTION LIMIT
 * is shared by every pod serving the environment.
 */
export const DEFAULT_POSTGRES_POOL_MAX = 5;
export const TEMP_DIR_PATH = os.tmpdir();
export const PUBLISHER_DATA_DIR = "publisher_data";
