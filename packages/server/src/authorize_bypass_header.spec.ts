import { describe, expect, it } from "bun:test";
import { readFileSync } from "fs";
import { resolve } from "path";
import {
   BYPASS_AUTHORIZE_HEADER,
   readBypassAuthorize,
} from "./authorize_bypass_header";

const withHeaders = (
   headers: Record<string, string | string[] | undefined>,
) => ({
   headers,
});

describe("readBypassAuthorize", () => {
   it("reads the bypass from the header", () => {
      expect(
         readBypassAuthorize(
            withHeaders({ [BYPASS_AUTHORIZE_HEADER]: "true" }),
         ),
      ).toBe(true);
   });

   it("tolerates casing and surrounding whitespace", () => {
      expect(
         readBypassAuthorize(
            withHeaders({ [BYPASS_AUTHORIZE_HEADER]: " True " }),
         ),
      ).toBe(true);
   });

   it("returns undefined when the header is absent", () => {
      expect(readBypassAuthorize(withHeaders({}))).toBeUndefined();
   });

   it.each(["false", "1", "yes", "", "TRUEISH"])(
      "returns undefined for the non-opt-in value %p",
      (value) => {
         expect(
            readBypassAuthorize(
               withHeaders({ [BYPASS_AUTHORIZE_HEADER]: value }),
            ),
         ).toBeUndefined();
      },
   );

   // A repeated header arrives as an array. We cannot read it as exactly one
   // `true`, so it leaves gates enforced rather than guessing.
   it("returns undefined for a repeated header", () => {
      expect(
         readBypassAuthorize(
            withHeaders({ [BYPASS_AUTHORIZE_HEADER]: ["true", "true"] }),
         ),
      ).toBeUndefined();
   });

   // THE safety pin for the whole header design. If this ever returns true, a
   // gate-disabling control is settable from the router's public QueryRequest
   // body, which is the `bypassFilters` mistake repeated on #(authorize).
   it("ignores a bypassAuthorize field on the request body", () => {
      const req = {
         headers: {},
         body: { bypassAuthorize: true },
      };
      expect(readBypassAuthorize(req)).toBeUndefined();
   });
});

/**
 * The reader being body-blind is only half the guarantee: the query route has to
 * actually use it. A route that went back to `req.body.bypassAuthorize` would
 * leave every test above green while making the bypass settable from the public
 * request schema, so pin the wiring in the source the same way the data-apps
 * route parity spec pins its path.
 */
describe("query route wiring", () => {
   const serverSource = readFileSync(
      resolve(import.meta.dir, "server.ts"),
      "utf8",
   );

   it("passes the request to the header reader", () => {
      expect(serverSource).toContain("readBypassAuthorize(req)");
   });

   it("never reads bypassAuthorize off a request body", () => {
      const bodyReads = serverSource
         .split("\n")
         .filter(
            (line) =>
               line.includes("bypassAuthorize") && line.includes("req.body"),
         );
      expect(bodyReads).toEqual([]);
   });
});
