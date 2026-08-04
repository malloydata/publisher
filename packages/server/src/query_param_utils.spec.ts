/// <reference types="bun-types" />

import { describe, expect, it } from "bun:test";
import { parseReloadParam } from "./query_param_utils";

describe("parseReloadParam", () => {
   it("accepts the two values the spec declares", () => {
      expect(parseReloadParam("true")).toEqual({ ok: true, reload: true });
      expect(parseReloadParam("false")).toEqual({ ok: true, reload: false });
   });

   it("treats an absent parameter as no reload", () => {
      expect(parseReloadParam(undefined)).toEqual({ ok: true, reload: false });
      expect(parseReloadParam(null)).toEqual({ ok: true, reload: false });
   });

   it("refuses a value it would otherwise have to guess at", () => {
      // Each of these previously read as `false` under `=== "true"`, so the
      // request answered 200 without recompiling. They must be refused, not
      // coerced: a caller who typed one of them meant to reload.
      for (const value of ["1", "yes", "TRUE", "True", "on", "0", "no", ""]) {
         expect(parseReloadParam(value)).toEqual({ ok: false });
      }
   });

   it("refuses a repeated parameter", () => {
      // Express hands `?reload=true&reload=1` over as an array; there is no
      // single value to honor, so it cannot be read as either boolean.
      expect(parseReloadParam(["true", "1"])).toEqual({ ok: false });
      expect(parseReloadParam(["true"])).toEqual({ ok: false });
   });

   it("refuses a non-string value", () => {
      expect(parseReloadParam(true)).toEqual({ ok: false });
      expect(parseReloadParam(1)).toEqual({ ok: false });
      expect(parseReloadParam({})).toEqual({ ok: false });
   });
});
