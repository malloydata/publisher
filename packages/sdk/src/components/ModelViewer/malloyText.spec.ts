// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { escapeRegExp, maskCommentsAndStrings } from "./malloyText";

describe("maskCommentsAndStrings", () => {
   it("blanks line and block comments, keeping the lines", () => {
      const text = "a // $X\nb -- $Y\nc /* $Z\n $W */ d";
      const masked = maskCommentsAndStrings(text);
      expect(masked).not.toMatch(/\$[XYZW]/);
      expect(masked.split("\n")).toHaveLength(4);
      expect(masked).toContain("a ");
      expect(masked).toContain(" d");
      expect(masked).toHaveLength(text.length);
   });

   it("blanks a string's contents, so a comment marker in it ends nothing", () => {
      const masked = maskCommentsAndStrings(
         "where: code = 'A--B' and region = $REGION",
      );
      expect(masked).toContain("$REGION");
      expect(masked).toContain("code = '    '");
   });

   it("blanks escapes, double quotes, prefixes' bodies and triple quotes", () => {
      const masked = maskCommentsAndStrings(
         `x = 'it\\'s $A' y = "q $B" z ~ f'$C%' sql("""\nselect '$D' -- {\n""")`,
      );
      expect(masked).not.toMatch(/\$[ABCD]/);
      expect(masked).not.toContain("{");
      expect(masked).toContain("f'");
   });

   it("keeps a quoted identifier, which is code", () => {
      expect(maskCommentsAndStrings("group_by: `order items`")).toBe(
         "group_by: `order items`",
      );
   });
});

describe("escapeRegExp", () => {
   it("matches its text literally", () => {
      expect(new RegExp(escapeRegExp("a.b(c)*")).test("a.b(c)*")).toBe(true);
      expect(new RegExp(escapeRegExp("a.b")).test("axb")).toBe(false);
   });
});
