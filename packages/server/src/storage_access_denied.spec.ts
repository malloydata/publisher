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

import { QueryController } from "./controller/query.controller";
import { QuerySlots } from "./query_concurrency";
import type {
   McpExposureReader,
   PackageLookup,
} from "./service/environment_ports";
import { QueryUseCase } from "./usecases/query";
import type { Connection } from "@malloydata/malloy";
import { afterEach, describe, expect, it, spyOn } from "bun:test";
import sinon from "sinon";
import { ConnectionController } from "./controller/connection.controller";
import { isObjectStoreRefusal, isUnderStorageLocation } from "./driver_errors";
import {
   BadRequestError,
   internalErrorToHttpError,
   StorageAccessDeniedError,
} from "./errors";
import { logger } from "./logger";
import { redactSignedUrlParams, redactStorageUrls } from "./pg_helpers";
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
               // The lake's configured data path.
               isConfiguredStorageUrl: (url: string) =>
                  isUnderStorageLocation(url, [
                     "gs://credible-dev-ducklake/org_123",
                  ]),
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
      // Under the lake's own data path: the query is fine, the credentials
      // are not.
      expect(answer.json.message).toContain("The query and the model are fine");
   });

   it("does not tell a caller the query is fine for a path it is not configured to hold", () => {
      const refusal = new StorageAccessDeniedError(GCS, "trips", "https://x");
      expect(refusal.managed).toBe(false);
      const answer = internalErrorToHttpError(refusal, { log: false });
      expect(answer.json.message).not.toContain("fine");
      expect(answer.json.message).toContain('"trips"');
      expect(refusal.suggestions.join(" ")).not.toContain("Do not rewrite");
      refusal.managed = true;
      expect(refusal.callerMessage).toContain("fine");
   });

   it("matches one location across its spellings", () => {
      const lake = ["gs://credible-dev-ducklake/org_123", "s3://bkt/lake/"];
      for (const url of [
         "https://storage.googleapis.com/credible-dev-ducklake/org_123/db_456/x.parquet",
         "gs://credible-dev-ducklake/org_123/x.parquet",
         "https://bkt.s3.amazonaws.com/lake/a.parquet",
         "https://bkt.s3.us-east-1.amazonaws.com/lake/a.parquet",
         "s3://bkt/lake/a.parquet",
      ]) {
         expect([url, isUnderStorageLocation(url, lake)]).toEqual([url, true]);
      }
      for (const url of [
         "https://storage.googleapis.com/credible-dev-ducklake/org_999/x.parquet",
         "https://storage.googleapis.com/other/org_123/x.parquet",
         "https://bkt.s3.amazonaws.com/lakeside/a.parquet",
         "https://example.com/credible-dev-ducklake/org_123/x",
      ]) {
         expect([url, isUnderStorageLocation(url, lake)]).toEqual([url, false]);
      }
   });

   it("recognises Azure's and the double-quoted forms", () => {
      for (const text of [
         'IO Error: Unable to connect to URL "https://storage.googleapis.com/b/o.parquet": 403 (Forbidden)',
         "IO Error: AzureBlobStorageFileSystem could not open file: 'az://container/org_1/x.parquet', unknown error occurred, this could mean the credentials used were wrong. Original error: 'Reason Phrase: This request is not authorized to perform this operation.' AuthorizationFailure",
         "HTTP Error: GET 'https://acct.blob.core.windows.net/container/x.parquet' failed: 403 AuthorizationPermissionMismatch",
      ]) {
         expect([text, isObjectStoreRefusal(new Error(text))]).toEqual([
            text,
            true,
         ]);
      }
      expect(
         isObjectStoreRefusal(
            new Error(
               'Unable to connect to URL "https://h/x": 404 (Not Found)',
            ),
         ),
      ).toBe(false);
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

describe("P6-21: a signed URL's secrets never reach the log", () => {
   it("masks each credential-bearing parameter and keeps the rest", () => {
      expect(
         redactSignedUrlParams(
            "GET 'https://b.s3.amazonaws.com/k?X-Amz-Algorithm=AWS4&X-Amz-Credential=AKIA%2Fx&X-Amz-Signature=abc123&X-Amz-Security-Token=tok' (HTTP 403)",
         ),
      ).toBe(
         "GET 'https://b.s3.amazonaws.com/k?X-Amz-Algorithm=AWS4&X-Amz-Credential=***&X-Amz-Signature=***&X-Amz-Security-Token=***' (HTTP 403)",
      );
      expect(
         redactSignedUrlParams(
            "https://storage.googleapis.com/b/o?X-Goog-Credential=c&X-Goog-Signature=s https://a.blob.core.windows.net/c/x?sv=1&se=2030&sig=zz https://h/p?token=t&access_token=u",
         ),
      ).toBe(
         "https://storage.googleapis.com/b/o?X-Goog-Credential=***&X-Goog-Signature=*** https://a.blob.core.windows.net/c/x?sv=1&se=***&sig=*** https://h/p?token=***&access_token=***",
      );
   });

   it("is applied to what a storage refusal logs", () => {
      const warn = spyOn(logger, "warn").mockImplementation(() => logger);
      try {
         internalErrorToHttpError(
            new StorageAccessDeniedError(
               "HTTP GET error on 'https://b.s3.amazonaws.com/k.parquet?X-Amz-Signature=deadbeef' (HTTP 403 Forbidden)",
               "trips",
            ),
         );
         const logged = JSON.stringify(warn.mock.calls);
         expect(logged).toContain("b.s3.amazonaws.com/k.parquet");
         expect(logged).not.toContain("deadbeef");
      } finally {
         warn.mockRestore();
      }
   });
});

describe("P6-21: the query route judges a refusal by the environment's data paths", () => {
   async function refusedOn(
      configured: boolean,
   ): Promise<StorageAccessDeniedError> {
      const url =
         "https://storage.googleapis.com/credible-dev-ducklake/org_123/x.parquet";
      const model = {
         carriesReportTag: () => false,
         runQuery: async () => {
            throw new StorageAccessDeniedError(GCS, "trips", url);
         },
      };
      const pkg = {
         getModel: () => model,
         getDeclaredQueryMetadata: () => null,
         getVersionId: () => undefined,
      };
      const environment = {
         assertCanAdmitQuery: () => {},
         getPackage: async () => pkg,
         getApiConnection: () => {
            throw new Error("no such connection");
         },
         isConfiguredStorageUrl: () => configured,
      };
      const store = fakeOf<PackageLookup & McpExposureReader>({
         getEnvironment: async () => environment as never,
         mcpIncludeHiddenFilesAndSources: false,
      });
      const info = spyOn(logger, "info").mockImplementation(() => logger);
      try {
         await new QueryController(
            new QueryUseCase(store, new QuerySlots()),
         ).getQuery({
            environmentName: "env",
            packageName: "pkg",
            modelPath: "m.malloy",
            sourceName: "",
            queryName: "",
            query: "run: trips -> { aggregate: n is count() }",
            compactJson: true,
            filterParams: {},
            givens: undefined,
            bypassAuthorize: false,
            includeHiddenFilesAndSources: false,
            identity: { caller: "test@example.com" },
         });
      } catch (error) {
         return error as StorageAccessDeniedError;
      } finally {
         info.mockRestore();
      }
      throw new Error("the query was not refused");
   }

   it("says the query is fine for a URL under a configured data path", async () => {
      const refusal = await refusedOn(true);
      expect(refusal).toBeInstanceOf(StorageAccessDeniedError);
      expect(refusal.callerMessage).toContain(
         "The query and the model are fine",
      );
   });

   it("does not, for any other URL", async () => {
      const refusal = await refusedOn(false);
      expect(refusal.callerMessage).not.toContain("fine");
   });
});
