// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * P6-21 (F-27): an object store refusing a read is answered 403
 * `STORAGE_ACCESS_DENIED` on the query route and the raw `sqlQuery` route
 * alike, naming what the caller read and no storage path; the full driver
 * text, URL included, is logged server-side. Before, the query route
 * answered a 400 quoting the bucket, organization and database paths, and
 * `sqlQuery` an outage's 502.
 */

import type { Connection } from "@malloydata/malloy";
import { afterEach, describe, expect, it, spyOn } from "bun:test";
import sinon from "sinon";
import { ConnectionController } from "./controller/connection.controller";
import { isObjectStoreRefusal } from "./driver_errors";
import {
   BadRequestError,
   internalErrorToHttpError,
   StorageAccessDeniedError,
} from "./errors";
import { logger } from "./logger";
import { redactStorageUrls } from "./pg_helpers";
import { outcomeOf } from "./service/access_record";
import type { ConnectionAdmin } from "./service/environment_ports";
import { mapRunError } from "./service/query_execution";
import { fakeOf } from "./test_helpers/store_fakes";

// DuckDB httpfs's texts, as it raises them (GCS through its https endpoint,
// S3, and gs://), each followed by the statement it echoes.
const GCS =
   "HTTP Error: HTTP GET error on 'https://storage.googleapis.com/credible-dev-ducklake/org_123/db_456/materialized/t/data_0.parquet' (HTTP 403 Forbidden)\n\nLINE 1: SELECT * FROM read_parquet('https://storage.googleapis.com/credible-dev...";
const S3 =
   "HTTP Error: HTTP GET error reading 'https://credible-dev-ducklake.s3.amazonaws.com/org_123/db_456/x.parquet' in region '' (HTTP 403 Forbidden)\n\nAccessDenied: Access Denied";
const GS =
   "HTTP Error: HTTP error on 'https://storage.googleapis.com/credible-dev-ducklake/org_123/x.parquet' (HTTP 401 Unauthorized)\n\nAuthentication Failure - GCS authentication failed.";
const LEAKS =
   /storage\.googleapis\.com|amazonaws|credible-dev-ducklake|org_123|db_456/;

afterEach(() => sinon.restore());

describe("P6-21: an object store's refusal", () => {
   it("is recognised in each scheme's text, and nothing else is", () => {
      for (const text of [GCS, S3, GS]) {
         expect(isObjectStoreRefusal(new Error(text))).toBe(true);
      }
      // On the cause chain too.
      expect(
         isObjectStoreRefusal(new Error("wrapped", { cause: new Error(GCS) })),
      ).toBe(true);
      for (const text of [
         GCS.replace("HTTP 403 Forbidden", "HTTP 404 Not Found"),
         GCS.replace("HTTP 403 Forbidden", "HTTP 0 Internal Server Error"),
         'Binder Error: column "x" not found',
         "the value 'https://x.example/a' (HTTP 403) was quoted",
      ]) {
         expect(isObjectStoreRefusal(new Error(text))).toBe(false);
      }
   });

   it("answers the query route 403 STORAGE_ACCESS_DENIED, naming the source and no path, and logs the full text", () => {
      for (const text of [GCS, S3, GS]) {
         let thrown: unknown;
         try {
            mapRunError(new Error(text), {
               packageName: "pkg",
               modelPath: "m.malloy",
               failurePrefix: "Query execution failed",
               subject: "trips",
               passThrough: [],
               logFailure: () => undefined,
            });
         } catch (error) {
            thrown = error;
         }
         expect(thrown).toBeInstanceOf(StorageAccessDeniedError);
         const warn = spyOn(logger, "warn").mockImplementation(() => logger);
         try {
            const answer = internalErrorToHttpError(thrown as Error);
            expect(answer.status).toBe(403);
            expect(answer.json.reason).toBe("STORAGE_ACCESS_DENIED");
            expect(answer.json.message).toContain('"trips"');
            expect(answer.json.message).not.toMatch(LEAKS);
            const logged = JSON.stringify(warn.mock.calls);
            expect(logged).toContain("credible-dev-ducklake");
         } finally {
            warn.mockRestore();
         }
         expect(outcomeOf(thrown, 403)).toBe("error");
      }
   });

   it("answers the raw sqlQuery route the same, not an outage's 502", async () => {
      const runSQL = sinon.stub().rejects(new Error(GCS));
      const controller = new ConnectionController(
         fakeOf<ConnectionAdmin>({
            getEnvironment: sinon.stub().resolves({
               assertCanAdmitQuery: () => undefined,
               getApiConnection: () => ({ name: "lake", type: "duckdb" }),
            }),
         }),
      );
      sinon
         .stub(
            controller as unknown as {
               getMalloyConnection: (...a: unknown[]) => Promise<Connection>;
            },
            "getMalloyConnection",
         )
         .resolves({ runSQL } as unknown as Connection);
      let thrown: unknown;
      try {
         await controller.getConnectionQueryData(
            "env",
            "lake",
            "SELECT * FROM t",
            "",
            undefined,
            undefined,
            undefined,
            {},
         );
      } catch (error) {
         thrown = error;
      }
      expect(thrown).toBeInstanceOf(StorageAccessDeniedError);
      const answer = internalErrorToHttpError(thrown as Error, { log: false });
      expect([answer.status, answer.json.reason]).toEqual([
         403,
         "STORAGE_ACCESS_DENIED",
      ]);
      expect(answer.json.message).toContain('"lake"');
      expect(answer.json.message).not.toMatch(LEAKS);
   });

   it("leaves no storage path in a query failure's 400 that still carries driver text", () => {
      let thrown: unknown;
      try {
         mapRunError(
            new Error(GCS.replace("HTTP 403 Forbidden", "HTTP 404 Not Found")),
            {
               packageName: "pkg",
               modelPath: "m.malloy",
               failurePrefix: "Query execution failed",
               passThrough: [],
               logFailure: () => undefined,
            },
         );
      } catch (error) {
         thrown = error;
      }
      expect(thrown).toBeInstanceOf(BadRequestError);
      expect((thrown as Error).message).toContain("<storage path>");
      expect((thrown as Error).message).not.toMatch(LEAKS);
   });

   it("redactStorageUrls hides every object-store and HTTP URL", () => {
      expect(
         redactStorageUrls(
            "read 's3://b/k.parquet', gs://b/o and https://h.example/p?q=1 (x)",
         ),
      ).toBe("read '<storage path>', <storage path> and <storage path> (x)");
   });
});
