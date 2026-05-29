import { describe, expect, it } from "bun:test";
import { setFilterDeprecationHeaders } from "./filter_deprecation";

const makeRes = () => {
   const headers: Record<string, string> = {};
   const res = {
      headers,
      setHeader(name: string, value: string) {
         headers[name] = value;
         return res;
      },
   };
   return res;
};

const DEPRECATION_LINK =
   '<https://github.com/malloydata/publisher/blob/main/docs/givens.md>; rel="deprecation"; type="text/markdown"';

describe("setFilterDeprecationHeaders", () => {
   it("does not set headers when neither filterParams nor bypassFilters is supplied", () => {
      const res = makeRes();
      setFilterDeprecationHeaders(res, {});
      expect(res.headers.Deprecation).toBeUndefined();
      expect(res.headers.Link).toBeUndefined();
   });

   it("does not set headers for an explicit empty filterParams object (no-op opt-out)", () => {
      const res = makeRes();
      setFilterDeprecationHeaders(res, { filterParams: {} });
      expect(res.headers.Deprecation).toBeUndefined();
      expect(res.headers.Link).toBeUndefined();
   });

   it("sets RFC 8594 headers when filterParams carries values", () => {
      const res = makeRes();
      setFilterDeprecationHeaders(res, {
         filterParams: { region: ["US"] },
      });
      expect(res.headers.Deprecation).toBe("true");
      expect(res.headers.Link).toBe(DEPRECATION_LINK);
   });

   it("sets RFC 8594 headers when bypassFilters is true", () => {
      const res = makeRes();
      setFilterDeprecationHeaders(res, { bypassFilters: true });
      expect(res.headers.Deprecation).toBe("true");
      expect(res.headers.Link).toBe(DEPRECATION_LINK);
   });

   it("does not set headers when bypassFilters is undefined and filterParams is undefined", () => {
      const res = makeRes();
      setFilterDeprecationHeaders(res, {
         filterParams: undefined,
         bypassFilters: undefined,
      });
      expect(res.headers.Deprecation).toBeUndefined();
   });

   it("does not set headers when filterParams is null", () => {
      const res = makeRes();
      setFilterDeprecationHeaders(res, { filterParams: null });
      expect(res.headers.Deprecation).toBeUndefined();
   });
});
