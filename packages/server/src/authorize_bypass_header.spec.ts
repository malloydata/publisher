// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

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

   // What Node actually does with a duplicated custom header: joins the values
   // into one comma-separated string. Not `"true"`, so it denies. (The array arm
   // below is reachable only for set-cookie, but the type allows it, so pin it
   // too rather than leave a shape unhandled.)
   it("returns undefined for a duplicated header, as Node joins it", () => {
      expect(
         readBypassAuthorize(
            withHeaders({ [BYPASS_AUTHORIZE_HEADER]: "true, true" }),
         ),
      ).toBeUndefined();
   });

   it("returns undefined for an array-valued header", () => {
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
 * actually use it, and only it. `authorize_bypass_wiring.integration.spec.ts`
 * proves the behaviour through a real HTTP request; this pins the one thing a
 * request cannot see — that the value the route hands the controller is derived
 * from the header and from nothing else.
 *
 * Asserted POSITIVELY, against the extracted argument list with comments
 * stripped. The previous form ("no line contains both `bypassAuthorize` and
 * `req.body`") was blocklist-shaped and a comment could satisfy its other half:
 * `const b = req.body; … b.bypassAuthorize` walked straight past it. An exact
 * match on the argument cannot be satisfied by anything but the right call.
 */
describe("query route wiring", () => {
   const serverSource = readFileSync(
      resolve(import.meta.dir, "server.ts"),
      "utf8",
   );

   /** The `getQuery(...)` argument list on the query route, comments removed. */
   const queryCallArguments = (): string => {
      const withoutComments = serverSource
         .replace(/\/\*[\s\S]*?\*\//g, "")
         .replace(/^\s*\/\/.*$/gm, "");
      const start = withoutComments.indexOf("queryController.getQuery(");
      expect(start).toBeGreaterThan(-1);
      // Balance parens from the opening one so nested calls don't end it early.
      // Bounded by the string length: an unbalanced call must reach the throw
      // below rather than run off the end reading undefined forever.
      const open = withoutComments.indexOf("(", start);
      let depth = 0;
      for (let i = open; i < withoutComments.length; i++) {
         if (withoutComments[i] === "(") depth++;
         if (withoutComments[i] === ")") depth--;
         if (depth === 0) {
            return withoutComments.slice(open + 1, i);
         }
      }
      throw new Error("unbalanced getQuery( call in server.ts");
   };

   it("derives the bypass from the header reader and nothing else", () => {
      const args = queryCallArguments();
      const bypassArguments = args
         .split("\n")
         .map((line) => line.trim())
         .filter((line) => /bypassAuthorize|readBypassAuthorize/i.test(line));
      expect(bypassArguments).toEqual(["readBypassAuthorize(req),"]);
   });
});
