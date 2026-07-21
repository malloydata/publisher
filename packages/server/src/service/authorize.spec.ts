import { describe, expect, it } from "bun:test";
import {
   collectAuthorizeExprs,
   isProbeTrue,
   parseAuthorizeAnnotation,
   referencedGivenNames,
} from "./authorize";

describe("referencedGivenNames", () => {
   it("returns the $NAME tokens deduped in first-seen order", () => {
      expect(referencedGivenNames("$ROLE = 'admin' and $ROLE != $PRIOR")).toEqual(
         ["ROLE", "PRIOR"],
      );
   });

   it("ignores a $NAME inside a string literal (not a real reference)", () => {
      // Otherwise a joined gate's referenced-given count is inflated and the
      // full-coverage check wrongly denies a correctly-authorized request.
      expect(referencedGivenNames("$ROLE = 'the $BOSS role'")).toEqual(["ROLE"]);
      expect(referencedGivenNames("'$A $B $C'")).toEqual([]);
      expect(referencedGivenNames("$X = 'it\\'s $Y' or $Z = 1")).toEqual([
         "X",
         "Z",
      ]);
   });
});

describe("isProbeTrue", () => {
   it("grants only on a genuine true / 1 / 'true'", () => {
      expect(isProbeTrue(true)).toBe(true);
      expect(isProbeTrue(1)).toBe(true);
      expect(isProbeTrue("true")).toBe(true);
   });

   it("denies on anything else (fail closed)", () => {
      for (const v of [false, 0, "false", "", null, undefined, {}, "TRUE", 2]) {
         expect(isProbeTrue(v)).toBe(false);
      }
   });
});

describe("parseAuthorizeAnnotation", () => {
   it("parses a source-level #(authorize) expression", () => {
      expect(parseAuthorizeAnnotation(`#(authorize) "$ROLE = 'analyst'"`)).toBe(
         "$ROLE = 'analyst'",
      );
   });

   it("parses a file-level ##(authorize) expression", () => {
      expect(parseAuthorizeAnnotation(`##(authorize) "$ROLE = 'admin'"`)).toBe(
         "$ROLE = 'admin'",
      );
   });

   it("tolerates the trailing newline Malloy keeps on note text", () => {
      expect(
         parseAuthorizeAnnotation(`#(authorize) "$REGION = 'us-west'"\n`),
      ).toBe("$REGION = 'us-west'");
   });

   it("preserves inner single quotes (Malloy string literals)", () => {
      expect(
         parseAuthorizeAnnotation(`#(authorize) "$TENANT in ['a', 'b']"`),
      ).toBe("$TENANT in ['a', 'b']");
   });

   it("unescapes escaped inner double quotes", () => {
      expect(parseAuthorizeAnnotation(`#(authorize) "$NAME = \\"foo\\""`)).toBe(
         `$NAME = "foo"`,
      );
   });

   it("handles a constant false gate", () => {
      expect(parseAuthorizeAnnotation(`#(authorize) "false"`)).toBe("false");
   });

   it("returns null for non-authorize annotations", () => {
      expect(
         parseAuthorizeAnnotation(`#(filter) dimension=x type=equal`),
      ).toBeNull();
      expect(parseAuthorizeAnnotation(`##! experimental.givens`)).toBeNull();
      expect(parseAuthorizeAnnotation(`## just a doc comment`)).toBeNull();
      expect(parseAuthorizeAnnotation(`# plain`)).toBeNull();
      expect(parseAuthorizeAnnotation(``)).toBeNull();
   });

   it("throws when the body is not quoted", () => {
      expect(() =>
         parseAuthorizeAnnotation(`#(authorize) $ROLE = 'analyst'`),
      ).toThrow(/double-quoted/);
   });

   it("throws on mismatched / unterminated quotes", () => {
      expect(() =>
         parseAuthorizeAnnotation(`#(authorize) "$ROLE = 'analyst'`),
      ).toThrow(/mismatched quotes/);
   });

   it("throws on an empty expression body", () => {
      expect(() => parseAuthorizeAnnotation(`#(authorize) ""`)).toThrow(
         /empty expression/,
      );
   });

   it("throws on content after the closing quote", () => {
      expect(() =>
         parseAuthorizeAnnotation(`#(authorize) "$ROLE = 'a'" extra`),
      ).toThrow(/unexpected content/);
   });

   it("throws when the prefix has no body", () => {
      expect(() => parseAuthorizeAnnotation(`#(authorize)`)).toThrow(
         /double-quoted/,
      );
   });
});

describe("collectAuthorizeExprs", () => {
   it("collects authorize expressions in declaration order", () => {
      expect(
         collectAuthorizeExprs([
            `##(authorize) "$ROLE = 'admin'"`,
            `#(filter) dimension=x type=equal`,
            `#(authorize) "$REGION = 'us-west'"`,
         ]),
      ).toEqual(["$ROLE = 'admin'", "$REGION = 'us-west'"]);
   });

   it("returns [] when there are no authorize annotations", () => {
      expect(
         collectAuthorizeExprs([`#(filter) dimension=x type=equal`, `## doc`]),
      ).toEqual([]);
   });

   it("keeps duplicate gates (no dedup — OR semantics)", () => {
      expect(
         collectAuthorizeExprs([
            `#(authorize) "$ROLE = 'admin'"`,
            `#(authorize) "$ROLE = 'admin'"`,
         ]),
      ).toEqual(["$ROLE = 'admin'", "$ROLE = 'admin'"]);
   });

   it("propagates the throw from a malformed authorize annotation", () => {
      expect(() =>
         collectAuthorizeExprs([
            `#(authorize) "$ROLE = 'admin'"`,
            `#(authorize) "unterminated`,
         ]),
      ).toThrow(/mismatched quotes/);
   });
});
