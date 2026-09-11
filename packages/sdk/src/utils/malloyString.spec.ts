// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { escapeMalloyString } from "./malloyString";

describe("escapeMalloyString", () => {
   it("escapes a quote", () => {
      expect(escapeMalloyString("O'Brien")).toBe("O\\'Brien");
   });

   it("escapes a backslash", () => {
      expect(escapeMalloyString("C:\\temp")).toBe("C:\\\\temp");
   });

   it("escapes backslashes before quotes, so the quote escape is not doubled", () => {
      // Quote-then-backslash order would turn ' into \' and then \\' .
      expect(escapeMalloyString("a'\\b")).toBe("a\\'\\\\b");
   });

   it("leaves a plain string alone", () => {
      expect(escapeMalloyString("plain value")).toBe("plain value");
   });
});
