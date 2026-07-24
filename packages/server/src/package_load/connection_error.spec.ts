/**
 * Unit tests for schema-fetch failure classification + diagnostics.
 *
 * These are pure-function tests — no worker, no connection. They pin the
 * behaviour the pool relies on to decide which failures get short-circuited
 * into a single connection diagnostic vs. left as normal compile errors.
 */
import { describe, expect, it } from "bun:test";
import { ConnectionAuthError, ConnectionError } from "../errors";
import {
   buildConnectionDiagnostic,
   classifySchemaFetchError,
   connectionFailureToError,
   isInfrastructureFailure,
} from "./connection_error";

describe("classifySchemaFetchError", () => {
   it("classifies 401 / 403 (status-first) as auth", () => {
      // The exact shape Axios produces and db-publisher passes through.
      expect(
         classifySchemaFetchError("Request failed with status code 401"),
      ).toEqual({ kind: "auth", status: 401 });
      expect(
         classifySchemaFetchError("Request failed with status code 403"),
      ).toEqual({ kind: "auth", status: 403 });
   });

   it("classifies 404 as not_found (NOT short-circuited)", () => {
      const c = classifySchemaFetchError("Request failed with status code 404");
      expect(c).toEqual({ kind: "not_found", status: 404 });
      expect(isInfrastructureFailure(c.kind)).toBe(false);
   });

   it("classifies 5xx and 408 as transport", () => {
      for (const code of [500, 502, 503, 504, 408]) {
         expect(
            classifySchemaFetchError(`Request failed with status code ${code}`),
         ).toEqual({ kind: "transport", status: code });
      }
   });

   it("classifies raw network/timeout errors (no HTTP status) as transport", () => {
      for (const msg of [
         "connect ECONNREFUSED 127.0.0.1:443",
         "getaddrinfo ENOTFOUND warehouse.example.com",
         "socket hang up",
         "timeout of 600000ms exceeded",
         "read ECONNRESET",
      ]) {
         expect(classifySchemaFetchError(msg).kind).toBe("transport");
      }
   });

   it("falls back to auth keywords when no HTTP status is present", () => {
      expect(classifySchemaFetchError("Unauthorized").kind).toBe("auth");
      expect(classifySchemaFetchError("the token is expired").kind).toBe(
         "auth",
      );
      expect(classifySchemaFetchError("invalid credentials").kind).toBe("auth");
   });

   it("treats genuine table-not-found text as not_found", () => {
      expect(classifySchemaFetchError("Table foo does not exist").kind).toBe(
         "not_found",
      );
      expect(classifySchemaFetchError("no such table: bar").kind).toBe(
         "not_found",
      );
   });

   it("leaves anything unrecognized as 'other' (not short-circuited)", () => {
      const c = classifySchemaFetchError("something unexpected happened");
      expect(c.kind).toBe("other");
      expect(isInfrastructureFailure(c.kind)).toBe(false);
   });
});

describe("buildConnectionDiagnostic", () => {
   it("names connection, table, status, and disclaims a model error (auth)", () => {
      const msg = buildConnectionDiagnostic({
         kind: "auth",
         status: 401,
         connection: "prod_warehouse",
         target: "analytics.orders",
         rawMessage: "Request failed with status code 401",
      });
      expect(msg).toContain("prod_warehouse");
      expect(msg).toContain("analytics.orders");
      expect(msg).toContain("401 Unauthorized");
      expect(msg.toLowerCase()).toContain("token");
      expect(msg).toContain("not an error in your Malloy model");
   });

   it("phrases unreachable transport failures without a bogus status", () => {
      const msg = buildConnectionDiagnostic({
         kind: "transport",
         connection: "warehouse",
         target: "schema.tbl",
         rawMessage: "connect ECONNREFUSED 10.0.0.1:443",
      });
      expect(msg).toContain("could not be reached");
      expect(msg).not.toContain("undefined");
   });

   it("uses connection-level phrasing when no target is in hand (resolution failure)", () => {
      // The path real publisher connections take: the token is validated at
      // `lookupConnection` time, before any specific table/SQL is known.
      const msg = buildConnectionDiagnostic({
         kind: "auth",
         status: 401,
         connection: "prod_warehouse",
         rawMessage: "Request failed with status code 401",
      });
      expect(msg).toContain("establishing the connection");
      // No specific target → must not emit a quoted-empty / "undefined" target.
      expect(msg).not.toContain("undefined");
      expect(msg).not.toContain('introspecting ""');
      expect(msg).toContain("401 Unauthorized");
   });
});

describe("connectionFailureToError", () => {
   it("maps auth → ConnectionAuthError (422) and transport → ConnectionError (502)", () => {
      const auth = connectionFailureToError({
         kind: "auth",
         status: 401,
         connection: "c",
         target: "t",
         rawMessage: "Request failed with status code 401",
      });
      const transport = connectionFailureToError({
         kind: "transport",
         status: 503,
         connection: "c",
         target: "t",
         rawMessage: "Request failed with status code 503",
      });
      expect(auth).toBeInstanceOf(ConnectionAuthError);
      expect(transport).toBeInstanceOf(ConnectionError);
   });
});
