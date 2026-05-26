import { describe, expect, it } from "bun:test";
import {
   BadRequestError,
   ConnectionAuthError,
   ConnectionError,
   internalErrorToHttpError,
   PayloadTooLargeError,
   QueryTimeoutError,
   ServiceUnavailableError,
} from "./errors";

describe("internalErrorToHttpError", () => {
   it("maps ConnectionAuthError to 422", () => {
      const { status, json } = internalErrorToHttpError(
         new ConnectionAuthError("creds rejected for db_x"),
      );
      expect(status).toBe(422);
      expect(json).toEqual({
         code: 422,
         message: "creds rejected for db_x",
      });
   });

   it("maps BadRequestError to 400", () => {
      const { status, json } = internalErrorToHttpError(
         new BadRequestError("bad input"),
      );
      expect(status).toBe(400);
      expect(json).toEqual({ code: 400, message: "bad input" });
   });

   it("maps ConnectionError to 502 (distinct from auth, still retryable)", () => {
      const { status, json } = internalErrorToHttpError(
         new ConnectionError("upstream broken"),
      );
      expect(status).toBe(502);
      expect(json).toEqual({ code: 502, message: "upstream broken" });
   });

   it("falls through to 500 for unrecognized errors", () => {
      const { status, json } = internalErrorToHttpError(new Error("boom"));
      expect(status).toBe(500);
      expect(json.message).toBe("boom");
   });

   it("maps PayloadTooLargeError to 413", () => {
      const { status, json } = internalErrorToHttpError(
         new PayloadTooLargeError(
            "Query returned more than 100000 rows; refine the query or raise PUBLISHER_MAX_QUERY_ROWS.",
         ),
      );
      expect(status).toBe(413);
      expect(json).toEqual({
         code: 413,
         message:
            "Query returned more than 100000 rows; refine the query or raise PUBLISHER_MAX_QUERY_ROWS.",
      });
   });

   it("maps ServiceUnavailableError to 503 (load shedding / back-pressure)", () => {
      const { status, json } = internalErrorToHttpError(
         new ServiceUnavailableError(
            "Pod at max concurrent queries (32); retry later.",
         ),
      );
      expect(status).toBe(503);
      expect(json).toEqual({
         code: 503,
         message: "Pod at max concurrent queries (32); retry later.",
      });
   });

   it("maps QueryTimeoutError to 504 (gateway timeout, distinct from 503 back-pressure)", () => {
      const { status, json } = internalErrorToHttpError(
         new QueryTimeoutError(
            "Query exceeded PUBLISHER_QUERY_TIMEOUT_MS (300000ms) and was aborted.",
         ),
      );
      expect(status).toBe(504);
      expect(json).toEqual({
         code: 504,
         message:
            "Query exceeded PUBLISHER_QUERY_TIMEOUT_MS (300000ms) and was aborted.",
      });
   });
});
