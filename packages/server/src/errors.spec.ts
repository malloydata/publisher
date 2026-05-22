import { describe, expect, it } from "bun:test";
import {
   BadRequestError,
   ConnectionAuthError,
   ConnectionError,
   internalErrorToHttpError,
   PayloadTooLargeError,
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
});
