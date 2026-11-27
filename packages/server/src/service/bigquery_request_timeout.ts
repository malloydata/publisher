// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { Connection } from "@malloydata/malloy";
import { logger } from "../logger";

/**
 * The HTTP timeout Publisher gives a BigQuery connection's SDK client, per
 * request. `@malloydata/db-bigquery` (0.0.438) asks BigQuery to hold each
 * `getQueryResults` poll for up to 120 s, but builds its client with no
 * `timeout`, so the SDK's default of 60 s cuts every held poll ("The
 * operation timed out.", a runtime `TimeoutError`, code 23). The driver
 * counts each cut as a transient error and allows four, never reset, so any
 * query running past about 18 minutes failed while its job ran on. This is
 * above the held poll, with room for the response.
 */
export const BIGQUERY_REQUEST_TIMEOUT_MS = 180_000;

/** The part of the SDK client this reads: `@google-cloud/common`'s `Service`. */
interface BigQueryClient {
   timeout?: number;
   request: unknown;
}

let warnedUnreachable = false;

/**
 * Give `connection`'s SDK client a request timeout of at least
 * {@link BIGQUERY_REQUEST_TIMEOUT_MS}. The driver takes no option for it, so
 * this sets the client's own `timeout`, which the SDK applies to every
 * request. Returns false, and warns once, when the client is not where this
 * driver keeps it (a driver upgrade moved it): the connection then keeps the
 * SDK's default.
 */
export function extendBigQueryRequestTimeout(connection: Connection): boolean {
   const client = (connection as unknown as { bigQuery?: BigQueryClient })
      .bigQuery;
   if (!client || typeof client.request !== "function") {
      if (!warnedUnreachable) {
         warnedUnreachable = true;
         logger.warn(
            "Could not set the BigQuery client's request timeout; a long query's results poll may time out at the SDK's default",
            { connectionName: connection.name },
         );
      }
      return false;
   }
   if (
      typeof client.timeout !== "number" ||
      client.timeout < BIGQUERY_REQUEST_TIMEOUT_MS
   ) {
      client.timeout = BIGQUERY_REQUEST_TIMEOUT_MS;
   }
   return true;
}
