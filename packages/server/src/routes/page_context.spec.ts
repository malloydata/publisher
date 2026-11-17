// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   pageContextScript,
   parsePageContextHeader,
   type PageContext,
} from "./page_context";

const context = (over: Partial<PageContext> = {}): PageContext => ({
   v: 1,
   environment: "env",
   package: "pkg",
   versionId: "1.0.0",
   apiBase: "/api/v0",
   hostOrigins: ["'self'"],
   ...over,
});

/** The JSON text inside the element. */
function jsonOf(script: string): string {
   const match =
      /^<script type="application\/json" id="publisher-context">([\s\S]*)<\/script>$/.exec(
         script,
      );
   if (!match) throw new Error(`not the context element: ${script}`);
   return match[1];
}

describe("pageContextScript", () => {
   it("is one JSON data block that parses back to the context", () => {
      const ctx = context({ versionId: null, hostOrigins: [] });
      expect(JSON.parse(jsonOf(pageContextScript(ctx)))).toEqual(ctx);
   });

   it("is ASCII only, whatever the values hold", () => {
      const ctx = context({
         environment: 'é"日本\u{1f600}\u2028\u2029</script><!--&',
      });
      const script = pageContextScript(ctx);
      // eslint-disable-next-line no-control-regex -- asserting nothing outside printable ASCII
      expect(/^[\x20-\x7e]*$/.test(script)).toBe(true);
      expect(JSON.parse(jsonOf(script))).toEqual(ctx);
   });

   it("escapes the characters that could end the element or open a comment", () => {
      const json = jsonOf(
         pageContextScript(context({ package: "</script><!-- & -->" })),
      );
      expect(json).not.toMatch(/[<>&]/);
      expect(json).toContain("\\u003c/script\\u003e");
   });

   it("writes U+2028 and U+2029 as escapes", () => {
      const json = jsonOf(
         pageContextScript(context({ package: "a\u2028\u2029b" })),
      );
      expect(json).toContain("a\\u2028\\u2029b");
   });
});

describe("parsePageContextHeader", () => {
   const override = (value: unknown) => {
      const parsed = parsePageContextHeader(JSON.stringify(value));
      if (!parsed || !("override" in parsed)) {
         throw new Error(`expected an override: ${JSON.stringify(parsed)}`);
      }
      return parsed.override;
   };
   const invalid = (value: unknown) =>
      expect(
         parsePageContextHeader(
            typeof value === "string" ? value : JSON.stringify(value),
         ),
      ).toHaveProperty("invalid");

   it("is nothing for an absent header", () => {
      expect(parsePageContextHeader(undefined)).toBeUndefined();
   });

   it("reads each overridable field", () => {
      expect(
         override({
            environment: "acme_prod",
            package: "orders",
            apiBase: "/publisher/api/v0",
            hostOrigins: ["https://app.example.com"],
         }),
      ).toEqual({
         environment: "acme_prod",
         package: "orders",
         apiBase: "/publisher/api/v0",
         hostOrigins: ["https://app.example.com"],
      });
   });

   it("reads an empty object as no override, and ignores unknown keys", () => {
      expect(override({})).toEqual({});
      expect(override({ pathPrefix: "/x", other: 1 })).toEqual({});
   });

   it("accepts an empty hostOrigins list, which turns the bus off", () => {
      expect(override({ hostOrigins: [] })).toEqual({ hostOrigins: [] });
   });

   it("applies the frame-ancestors pattern rules to hostOrigins", () => {
      expect(
         override({
            hostOrigins: [
               "'self'",
               "HTTPS://App.Example.com:443",
               "https://*.app.example.com",
               "https://app.example.com",
            ],
         }).hostOrigins,
      ).toEqual([
         "'self'",
         "https://app.example.com",
         "https://*.app.example.com",
      ]);
      for (const bad of [
         "*",
         "https:",
         "https://a.example/x",
         "javascript:1",
      ]) {
         invalid({ hostOrigins: [bad] });
      }
      invalid({ hostOrigins: "https://a.example" });
      invalid({ hostOrigins: [1] });
      invalid({
         hostOrigins: Array.from(
            { length: 33 },
            (_, i) => `https://h${i}.example`,
         ),
      });
   });

   it("refuses the whole header when one field is bad", () => {
      invalid({ environment: "ok", apiBase: "//evil.example" });
   });

   it("refuses names that are not short plain strings", () => {
      invalid({ environment: 5 });
      invalid({ package: "" });
      invalid({ package: "x".repeat(257) });
      invalid({ environment: "a\nb" });
      invalid({ environment: "a\u007fb" });
      expect(override({ package: "x".repeat(256) })).toEqual({
         package: "x".repeat(256),
      });
   });

   it("refuses an apiBase that is not a plain absolute path", () => {
      for (const bad of [
         "api/v0",
         "https://evil.example/api",
         "//evil.example/api",
         "/a\\b",
         "/a\nb",
         "",
         5,
         "/" + "x".repeat(1024),
      ]) {
         invalid({ apiBase: bad });
      }
      expect(override({ apiBase: "/" })).toEqual({ apiBase: "/" });
   });

   it("refuses what is not a JSON object", () => {
      invalid("not json");
      invalid("[]");
      invalid("null");
      invalid("5");
      invalid('"x"');
   });
});
