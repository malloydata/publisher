// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { BigQueryConnection } from "@malloydata/db-bigquery";
import type { Connection } from "@malloydata/malloy";
import { describe, expect, it } from "bun:test";
import {
   BIGQUERY_REQUEST_TIMEOUT_MS,
   extendBigQueryRequestTimeout,
} from "./bigquery_request_timeout";

/** A real driver connection; its constructor makes no request. */
function connection(): BigQueryConnection {
   return new BigQueryConnection({
      name: "bq",
      projectId: "p",
      billingProjectId: "p",
   } as never);
}

/** The request options the SDK client sends for one request. */
async function sentOptions(
   conn: BigQueryConnection,
): Promise<{ timeout?: number }> {
   const client = (
      conn as unknown as {
         bigQuery: {
            makeAuthenticatedRequest: (
               opts: unknown,
               cb: (e: unknown) => void,
            ) => void;
            request: (opts: unknown, cb: (e: unknown) => void) => void;
         };
      }
   ).bigQuery;
   let sent: { timeout?: number } | undefined;
   client.makeAuthenticatedRequest = (opts, cb) => {
      sent = opts as { timeout?: number };
      cb(null);
   };
   await new Promise<void>((resolve) =>
      client.request({ uri: "/jobs/j" }, () => resolve()),
   );
   return sent!;
}

describe("a BigQuery connection's request timeout", () => {
   it("is above the driver's held results poll on every request", async () => {
      const conn = connection();
      expect(extendBigQueryRequestTimeout(conn)).toBe(true);
      expect((await sentOptions(conn)).timeout).toBe(
         BIGQUERY_REQUEST_TIMEOUT_MS,
      );
      // The driver holds each poll for up to 120 s.
      expect(BIGQUERY_REQUEST_TIMEOUT_MS).toBeGreaterThan(120_000);
   });

   it("is the SDK's default without it, which a held poll outlasts", async () => {
      expect((await sentOptions(connection())).timeout ?? 60_000).toBe(60_000);
   });

   it("keeps a longer timeout a client already has", () => {
      const conn = connection();
      const client = (conn as unknown as { bigQuery: { timeout?: number } })
         .bigQuery;
      client.timeout = 600_000;
      extendBigQueryRequestTimeout(conn);
      expect(client.timeout).toBe(600_000);
   });

   it("leaves a connection alone, and says so, when the client is not where the driver keeps it", () => {
      expect(
         extendBigQueryRequestTimeout({ name: "x" } as unknown as Connection),
      ).toBe(false);
   });
});
