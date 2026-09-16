// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   assertNoCallerAuthorizeAnnotation,
   AUTHORIZE_ROUTE,
   collectAuthorizeExprs,
   collectAuthorizeNearMisses,
   containsAuthorizeAnnotationTag,
   parseAuthorizeAnnotation,
   referencedGivenNames,
} from "./authorize";

/** Mirrors `authorize.ts`'s own private `SOURCE_AUTHORIZE_ROUTE` literal —
 *  not imported, since this module's route constant is deliberately kept
 *  private (see `authorize.ts`'s doc on `AUTHORIZE_ROUTE`). */
const SOURCE_AUTHORIZE_ROUTE = "source-authorize";

/** A parsed row-level route result, for `.toEqual` against
 *  `parseAuthorizeAnnotation`'s `{route, expr}` shape. */
const authorized = (expr: string) => ({ route: AUTHORIZE_ROUTE, expr });

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
      expect(
         parseAuthorizeAnnotation(`#(authorize) "$ROLE = 'analyst'"`),
      ).toEqual(authorized(`"$ROLE = 'analyst'"`));
   });

   it("returns a file-level ##(authorize) legacy-quoted body verbatim", () => {
      expect(
         parseAuthorizeAnnotation(`##(authorize) "$ROLE = 'admin'"`),
      ).toEqual(authorized(`"$ROLE = 'admin'"`));
   });

   it("tolerates the trailing newline Malloy keeps on note text", () => {
      expect(
         parseAuthorizeAnnotation(`#(authorize) "$REGION = 'us-west'"\n`),
      ).toEqual(authorized(`"$REGION = 'us-west'"`));
   });

   it("passes a legacy-quoted body with inner single quotes through untouched", () => {
      expect(
         parseAuthorizeAnnotation(`#(authorize) "$TENANT in ['a', 'b']"`),
      ).toEqual(authorized(`"$TENANT in ['a', 'b']"`));
   });

   it("does not unescape inner double quotes — no unwrapping happens at all", () => {
      expect(
         parseAuthorizeAnnotation(`#(authorize) "$NAME = \\"foo\\""`),
      ).toEqual(authorized(`"$NAME = \\"foo\\""`));
   });

   it('returns a quoted "false" body verbatim — a string literal, not the boolean sentinel', () => {
      // `#(authorize) "false"` is the LEGACY form's own quoting of the bare
      // word `false`; it is no longer unwrapped, so this compiles downstream
      // as the Malloy STRING `"false"`, not the boolean literal `false` —
      // `resolveGateShape`'s probe rejects it for that reason, same as any
      // other legacy quoted payload.
      expect(parseAuthorizeAnnotation(`#(authorize) "false"`)).toEqual(
         authorized(`"false"`),
      );
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
      expect(
         parseAuthorizeAnnotation(`#(authorize) $ROLE = 'analyst'`),
      ).toEqual(authorized(`$ROLE = 'analyst'`));
      expect(
         parseAuthorizeAnnotation(`#(authorize) org_id in $GROUPS`),
      ).toEqual(authorized("org_id in $GROUPS"));
   });

   it("an unterminated leading quote is returned verbatim, not thrown on here", () => {
      // Every body is returned verbatim regardless of form (see this
      // function's doc), so an unterminated quote is no exception — it is
      // Malloy's own compiler that rejects the resulting invalid expression
      // downstream, not this function.
      expect(
         parseAuthorizeAnnotation(`#(authorize) "$ROLE = 'analyst'`),
      ).toEqual(authorized(`"$ROLE = 'analyst'`));
   });

   it("a quoted-but-empty body is NOT an empty expression body — it is returned verbatim", () => {
      // Only a body with nothing in it at all (zero length after trim)
      // throws; `""` has content (two quote characters), so it is returned
      // as-is and left for downstream compilation to reject as a non-boolean
      // string literal.
      expect(parseAuthorizeAnnotation(`#(authorize) ""`)).toEqual(
         authorized(`""`),
      );
   });

   it("a quoted string followed by trailing content is returned verbatim, not thrown on here", () => {
      // Same reasoning as the unterminated-quote case above: nothing about
      // this body's shape is special-cased, so the trailing ` extra` comes
      // through untouched too.
      expect(
         parseAuthorizeAnnotation(`#(authorize) "$ROLE = 'a'" extra`),
      ).toEqual(authorized(`"$ROLE = 'a'" extra`));
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
      ).toEqual([
         authorized(`"$ROLE = 'admin'"`),
         authorized(`"$REGION = 'us-west'"`),
      ]);
   });

   it("returns [] when there are no authorize annotations", () => {
      expect(
         collectAuthorizeExprs([`#(filter) dimension=x type=equal`, `## doc`]),
      ).toEqual([]);
   });

   it("keeps duplicate gates (no dedup — every term joins the AND conjunction)", () => {
      expect(
         collectAuthorizeExprs([
            `#(authorize) role = 'admin'`,
            `#(authorize) role = 'admin'`,
         ]),
      ).toEqual([authorized(`role = 'admin'`), authorized(`role = 'admin'`)]);
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

describe("assertNoCallerAuthorizeAnnotation — widened for source-authorize", () => {
   it("rejects caller-submitted #(source-authorize) exactly like #(authorize)", () => {
      // Before the widening, `AUTHORIZE_TAG_LIKE` required `authorize`
      // immediately after the bracket — `#(source-authorize) 'fin' in
      // $GROUPS` tested false, so a caller could mint one past the rejecter
      // even though `noteRoute`-based classification treats it as a real
      // gate.
      expect(() =>
         assertNoCallerAuthorizeAnnotation(
            `#(source-authorize) 'finance' in $GROUPS\nsource: mine is locked extend {}`,
         ),
      ).toThrow(/not permitted in caller-submitted/);
   });

   it("still rejects the block form and every bracket pair for source-authorize", () => {
      for (const spelling of [
         "#(source-authorize)",
         "#[source-authorize]",
         "#<source-authorize>",
         "#{source-authorize}",
         "##(source-authorize)",
         "#|(source-authorize)",
      ]) {
         expect(() =>
            assertNoCallerAuthorizeAnnotation(`${spelling} 'x' in $G`),
         ).toThrow(/not permitted in caller-submitted/);
      }
   });

   it("does not fire on a route that merely mentions authorize as a suffix", () => {
      // The lookahead still stops a false positive on `#(authorize-v2)` /
      // `#(authorize.audit)` — widening the prefix must not widen this.
      expect(() =>
         assertNoCallerAuthorizeAnnotation(`#(authorize-v2) x = 1`),
      ).not.toThrow();
      expect(() =>
         assertNoCallerAuthorizeAnnotation(`#(authorize.audit) x = 1`),
      ).not.toThrow();
   });

   it("still rejects ordinary #(authorize) (no regression from the widening)", () => {
      expect(() =>
         assertNoCallerAuthorizeAnnotation(`#(authorize) org_id in $GROUPS`),
      ).toThrow(/not permitted in caller-submitted/);
   });
});

describe("collectAuthorizeNearMisses — generalized to take a route", () => {
   // Every hyphenation/word-order variant of `source-authorize` must be
   // caught as a near miss for that route, whichever of the three branches
   // (malformed prefix, MOTLY payload, or a real-but-distinct route name) it
   // lands in — none of these load as an inert, unenforced annotation.
   const TYPO_SPELLINGS = [
      `#(source_authorize) 'fin' in $GROUPS`,
      `#(sourceauthorize) 'fin' in $GROUPS`,
      `#(authorize-source) 'fin' in $GROUPS`,
      `#(SOURCE-AUTHORIZE) 'fin' in $GROUPS`,
      `# (source-authorize) 'fin' in $GROUPS`,
   ];

   it.each(TYPO_SPELLINGS)(
      "flags %s as a source-authorize near miss",
      (text) => {
         expect(
            collectAuthorizeNearMisses([text], SOURCE_AUTHORIZE_ROUTE),
         ).toEqual([text]);
      },
   );

   it("does not flag the real spelling as a near miss for its own route", () => {
      expect(
         collectAuthorizeNearMisses(
            [`#(source-authorize) 'fin' in $GROUPS`],
            SOURCE_AUTHORIZE_ROUTE,
         ),
      ).toEqual([]);
   });

   it("does not flag a source-authorize typo against the authorize route (each route's sweep is its own)", () => {
      // `collectAuthorizeNearMissesAllRoutes` is what combines both sweeps;
      // this pins that a single-route call stays scoped to ITS OWN route's
      // spellings rather than accidentally matching the other route's typos.
      expect(
         collectAuthorizeNearMisses(
            [`#(source_authorize) 'fin' in $GROUPS`],
            AUTHORIZE_ROUTE,
         ),
      ).toEqual([]);
   });

   it("defaults to the authorize route, unchanged from before generalization", () => {
      expect(collectAuthorizeNearMisses([`# (authorize) x = 1`])).toEqual([
         `# (authorize) x = 1`,
      ]);
      expect(collectAuthorizeNearMisses([`#(authorize-v2) x = 1`])).toEqual([]);
   });
});
