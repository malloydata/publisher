/// <reference types="bun-types" />

import { describe, expect, it } from "bun:test";
import { invalidBooleanMessage, parseBooleanParam } from "./query_param_utils";

describe("parseBooleanParam", () => {
   it("accepts the two values the spec declares", () => {
      expect(parseBooleanParam("true")).toEqual({ ok: true, value: true });
      expect(parseBooleanParam("false")).toEqual({ ok: true, value: false });
   });

   it("treats an absent parameter as false", () => {
      expect(parseBooleanParam(undefined)).toEqual({ ok: true, value: false });
      expect(parseBooleanParam(null)).toEqual({ ok: true, value: false });
   });

   it("refuses a value it would otherwise have to guess at", () => {
      // Each of these previously read as `false` under `=== "true"`, so the
      // request answered success without doing the thing. They must be refused,
      // not coerced: a caller who typed one of them meant it.
      for (const value of ["1", "yes", "TRUE", "True", "on", "0", "no", ""]) {
         expect(parseBooleanParam(value)).toEqual({ ok: false });
      }
   });

   it("refuses a repeated parameter", () => {
      // Express hands `?reload=true&reload=1` over as an array; there is no
      // single value to honor, so it cannot be read as either boolean.
      expect(parseBooleanParam(["true", "1"])).toEqual({ ok: false });
      expect(parseBooleanParam(["true"])).toEqual({ ok: false });
   });

   it("refuses a non-string value", () => {
      expect(parseBooleanParam(true)).toEqual({ ok: false });
      expect(parseBooleanParam(1)).toEqual({ ok: false });
      expect(parseBooleanParam({})).toEqual({ ok: false });
   });
});

describe("invalidBooleanMessage", () => {
   it("quotes the value back and names a form that works", () => {
      // Pinned in full: a caller who cannot see what they sent, or what to send
      // instead, is back to guessing -- which is the failure this replaces.
      expect(
         invalidBooleanMessage(
            "reload",
            "yes",
            "GET",
            "/api/v0/environments/e",
         ),
      ).toBe(
         `Invalid reload value "yes": expected "true" or "false". ` +
            `Fix: GET /api/v0/environments/e?reload=true.`,
      );
   });

   it("carries the caller's own param name and method", () => {
      // The same message serves every boolean param, so the destructive one
      // must not suggest a GET.
      expect(
         invalidBooleanMessage("dropTables", "1", "DELETE", "/api/v0/m/1"),
      ).toBe(
         `Invalid dropTables value "1": expected "true" or "false". ` +
            `Fix: DELETE /api/v0/m/1?dropTables=true.`,
      );
   });

   it("renders a repeated parameter as the array it arrived as", () => {
      expect(
         invalidBooleanMessage("reload", ["true", "1"], "GET", "/p"),
      ).toContain(`Invalid reload value ["true","1"]`);
   });
});
