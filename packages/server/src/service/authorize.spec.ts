// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   collectAuthorizeExprs,
   containsAuthorizeAnnotationTag,
   parseAuthorizeAnnotation,
   referencedGivenNames,
   assertNoScalarSecureGivens,
   findScalarSecureGivens,
} from "./authorize";

describe("referencedGivenNames", () => {
   it("returns the $NAME tokens deduped in first-seen order", () => {
      expect(
         referencedGivenNames("$ROLE = 'admin' and $ROLE != $PRIOR"),
      ).toEqual(["ROLE", "PRIOR"]);
   });

   it("ignores a $NAME inside a string literal (not a real reference)", () => {
      // Otherwise a joined gate's referenced-given count is inflated and the
      // full-coverage check wrongly denies a correctly-authorized request.
      expect(referencedGivenNames("$ROLE = 'the $BOSS role'")).toEqual([
         "ROLE",
      ]);
      expect(referencedGivenNames("'$A $B $C'")).toEqual([]);
      expect(referencedGivenNames("$X = 'it\\'s $Y' or $Z = 1")).toEqual([
         "X",
         "Z",
      ]);
   });

   it("ignores a $NAME inside a DOUBLE-quoted literal too", () => {
      // Malloy accepts either quote for a literal, so a double-quoted one must
      // be stripped as well. Reporting a phantom BOSS here makes
      // `filterGivensToModelSurface` drop a caller-supplied BOSS instead of
      // letting the query fail closed on an unknown given.
      expect(referencedGivenNames('$ROLE = "the $BOSS role"')).toEqual([
         "ROLE",
      ]);
      expect(referencedGivenNames('"$A $B $C"')).toEqual([]);
   });
});

describe("parseAuthorizeAnnotation", () => {
   it("returns a source-level legacy-quoted body verbatim, quotes included — never unwrapped", () => {
      // The body is the expression, always, with no exception for the legacy
      // quoted-string form: it is compiled downstream exactly as authored, a
      // STRING LITERAL rather than a boolean, which is what makes that form
      // fail closed on its own (see `resolveGateShape`'s doc) instead of
      // needing to be special-cased here.
      expect(parseAuthorizeAnnotation(`#(authorize) "$ROLE = 'analyst'"`)).toBe(
         `"$ROLE = 'analyst'"`,
      );
   });

   it("returns a file-level ##(authorize) legacy-quoted body verbatim", () => {
      expect(parseAuthorizeAnnotation(`##(authorize) "$ROLE = 'admin'"`)).toBe(
         `"$ROLE = 'admin'"`,
      );
   });

   it("tolerates the trailing newline Malloy keeps on note text", () => {
      expect(
         parseAuthorizeAnnotation(`#(authorize) "$REGION = 'us-west'"\n`),
      ).toBe(`"$REGION = 'us-west'"`);
   });

   it("passes a legacy-quoted body with inner single quotes through untouched", () => {
      expect(
         parseAuthorizeAnnotation(`#(authorize) "$TENANT in ['a', 'b']"`),
      ).toBe(`"$TENANT in ['a', 'b']"`);
   });

   it("does not unescape inner double quotes — no unwrapping happens at all", () => {
      expect(parseAuthorizeAnnotation(`#(authorize) "$NAME = \\"foo\\""`)).toBe(
         `"$NAME = \\"foo\\""`,
      );
   });

   it('returns a quoted "false" body verbatim — a string literal, not the boolean sentinel', () => {
      // `#(authorize) "false"` is the LEGACY form's own quoting of the bare
      // word `false`; it is no longer unwrapped, so this compiles downstream
      // as the Malloy STRING `"false"`, not the boolean literal `false` —
      // `resolveGateShape`'s probe rejects it for that reason, same as any
      // other legacy quoted payload.
      expect(parseAuthorizeAnnotation(`#(authorize) "false"`)).toBe(`"false"`);
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

   it("parses the current unquoted natural-expression form verbatim", () => {
      expect(parseAuthorizeAnnotation(`#(authorize) $ROLE = 'analyst'`)).toBe(
         `$ROLE = 'analyst'`,
      );
      expect(parseAuthorizeAnnotation(`#(authorize) org_id in $GROUPS`)).toBe(
         "org_id in $GROUPS",
      );
   });

   it("an unterminated leading quote is returned verbatim, not thrown on here", () => {
      // Every body is returned verbatim regardless of form (see this
      // function's doc), so an unterminated quote is no exception — it is
      // Malloy's own compiler that rejects the resulting invalid expression
      // downstream, not this function.
      expect(parseAuthorizeAnnotation(`#(authorize) "$ROLE = 'analyst'`)).toBe(
         `"$ROLE = 'analyst'`,
      );
   });

   it("a quoted-but-empty body is NOT an empty expression body — it is returned verbatim", () => {
      // Only a body with nothing in it at all (zero length after trim)
      // throws; `""` has content (two quote characters), so it is returned
      // as-is and left for downstream compilation to reject as a non-boolean
      // string literal.
      expect(parseAuthorizeAnnotation(`#(authorize) ""`)).toBe(`""`);
   });

   it("a quoted string followed by trailing content is returned verbatim, not thrown on here", () => {
      // Same reasoning as the unterminated-quote case above: nothing about
      // this body's shape is special-cased, so the trailing ` extra` comes
      // through untouched too.
      expect(parseAuthorizeAnnotation(`#(authorize) "$ROLE = 'a'" extra`)).toBe(
         `"$ROLE = 'a'" extra`,
      );
   });

   it("throws when the prefix has no body", () => {
      expect(() => parseAuthorizeAnnotation(`#(authorize)`)).toThrow(
         /empty expression/,
      );
   });
});

describe("containsAuthorizeAnnotationTag", () => {
   it("detects the tag at the start of a note, source- or file-level", () => {
      expect(containsAuthorizeAnnotationTag([`#(authorize) "false"`])).toBe(
         true,
      );
      expect(containsAuthorizeAnnotationTag([`##(authorize) "false"`])).toBe(
         true,
      );
   });

   it("tolerates the trailing newline Malloy keeps on note text", () => {
      expect(containsAuthorizeAnnotationTag([`#(authorize) "false"\n`])).toBe(
         true,
      );
   });

   it("does NOT match a note that merely mentions the tag inside its own body", () => {
      // Anchored after trimming, matching `parseAuthorizeAnnotation`: a note
      // is one annotation, so the tag only counts as a declaration at its
      // start. A `##(description)` note that quotes `#(authorize)` in prose
      // is documentation, not a gate, and must not be mistaken for one — see
      // this function's doc for the fail-closed cost of getting this wrong.
      expect(
         containsAuthorizeAnnotationTag([
            `##(description) "see the #(authorize) tag docs"`,
         ]),
      ).toBe(false);
      expect(
         containsAuthorizeAnnotationTag([
            `# note: this used to declare #(authorize) here, now removed`,
         ]),
      ).toBe(false);
   });

   it("still matches when the tag is preceded only by whitespace (the trim() half)", () => {
      // Malloy itself never hands this function leading whitespace before
      // the tag (it strips source indentation when it records note text),
      // so this exercises `.trim()` directly rather than through a
      // compiled fixture — a note object built any other way (e.g. a
      // future caller that assembles notes programmatically) must not
      // let indentation smuggle a gate past detection.
      expect(containsAuthorizeAnnotationTag([`  #(authorize) "false"\n`])).toBe(
         true,
      );
   });

   it("returns false for a non-authorize annotation", () => {
      expect(containsAuthorizeAnnotationTag([`# bar_chart`])).toBe(false);
      expect(containsAuthorizeAnnotationTag([`#(filter) x=1`])).toBe(false);
   });

   it("checks every text in the list", () => {
      expect(
         containsAuthorizeAnnotationTag([`# bar_chart`, `#(authorize) "true"`]),
      ).toBe(true);
   });
});

describe("collectAuthorizeExprs", () => {
   it("collects authorize expressions in declaration order, legacy-quoted bodies verbatim", () => {
      expect(
         collectAuthorizeExprs([
            `##(authorize) "$ROLE = 'admin'"`,
            `#(filter) dimension=x type=equal`,
            `#(authorize) "$REGION = 'us-west'"`,
         ]),
      ).toEqual([`"$ROLE = 'admin'"`, `"$REGION = 'us-west'"`]);
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
      ).toEqual([`"$ROLE = 'admin'"`, `"$ROLE = 'admin'"`]);
   });

   it("propagates the throw from a malformed authorize annotation", () => {
      expect(() =>
         collectAuthorizeExprs([
            `#(authorize) "$ROLE = 'admin'"`,
            `#(authorize)`,
         ]),
      ).toThrow(/empty expression/);
   });
});

describe("scalar #(secure) givens", () => {
   const secure = ["#(secure)\n"];

   it("finds a secure given declared with a scalar type", () => {
      expect(
         findScalarSecureGivens([
            { name: "ROLE", type: "string", annotations: secure },
         ]),
      ).toEqual([{ name: "ROLE", type: "string" }]);
   });

   it("passes a secure given declared filter<string>", () => {
      // The form the dashboard builder writes and docs/givens.md prescribes for
      // passing several values: refusing it would stop every secure given in
      // this repo from loading.
      expect(
         findScalarSecureGivens([
            { name: "CATEGORY", type: "filter<string>", annotations: secure },
         ]),
      ).toEqual([]);
   });

   it.each(["#(secure) keep this server-side", "#(secure)\n", "##(secure)"])(
      "finds a scalar secure given spelled %p",
      (note) => {
         // Malloy routes each of these to `secure`, so the check must too: a
         // rejecter that accepts LESS than the parser is the dangerous direction.
         expect(
            findScalarSecureGivens([
               { name: "ROLE", type: "string", annotations: [note] },
            ]),
         ).toEqual([{ name: "ROLE", type: "string" }]);
      },
   );

   it("passes a secure given declared set-valued", () => {
      expect(
         findScalarSecureGivens([
            { name: "ROLES", type: "string[]", annotations: secure },
         ]),
      ).toEqual([]);
   });

   it("ignores a scalar given that is not marked secure", () => {
      expect(
         findScalarSecureGivens([
            {
               name: "REGION",
               type: "string",
               annotations: ["# label=Region\n"],
            },
         ]),
      ).toEqual([]);
   });

   // The marker is matched on its own line, so a tag that merely contains the
   // word does not arm a refusal the author never asked for.
   it("does not match a note that only mentions the word", () => {
      expect(
         findScalarSecureGivens([
            { name: "NOTE", type: "string", annotations: ["# secure data\n"] },
         ]),
      ).toEqual([]);
   });

   it("names every scalar secure given, not just the first", () => {
      expect(() =>
         assertNoScalarSecureGivens([
            { name: "ROLE", type: "string" },
            { name: "TIER", type: "number" },
         ]),
      ).toThrow(/ROLE.*string[\s\S]*TIER.*number/);
   });

   it("does not throw when nothing is scalar", () => {
      expect(() => assertNoScalarSecureGivens([])).not.toThrow();
   });
});
