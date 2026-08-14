import { describe, expect, it } from "bun:test";
import {
   classifyAuthorizeGate,
   collectAuthorizeExprs,
   containsAuthorizeAnnotationTag,
   isProbeTrue,
   parseAuthorizeAnnotation,
   referencedGivenNames,
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

// ---------------------------------------------------------------------------
// classifyAuthorizeGate
// ---------------------------------------------------------------------------
//
// The fixtures below are the compiled shapes Malloy 0.0.427 actually produces
// for a source-level `where:`, transcribed from a spike that dumped them. They are
// hand-built here so the decision table can be exercised without standing up a
// warehouse; `authorize_integration.spec.ts` compiles the real thing, which is
// what pins these fixtures to reality. If a Malloy bump changes the node names,
// the integration test is what fails — treat a green unit suite alone as
// insufficient evidence.

// These are the strings PRODUCTION puts in this map, not a plausible spelling
// of them. `Model.givenDeclaredTypes()` reads `ApiGiven.type`, which
// `malloyGivenToApi` renders from Malloy's type discriminator — so an array
// given is the bare word "array", with the element type dropped. An earlier
// version of this fixture used "number[]", which no code path produces; the
// suite passed while every array gate was misclassified as a scalar in
// production. A fixture is only worth what its fidelity to the real value is.
const TYPES = new Map<string, string>([
   ["GROUPS", "array"],
   ["NAMES", "array"],
   ["BOB", "string"],
   ["LVL", "number"],
   ["ROLE", "string"],
]);

/** `{node:'field', path:[…]}` — a row or joined field reference. */
const field = (...path: string[]) => ({ node: "field", path });
/** `{node:'given', refName}` — a declared given reference. */
const given = (refName: string) => ({
   node: "given",
   refName,
   id: `given/x:${refName}`,
});
const binary = (node: string, left: unknown, right: unknown) => ({
   node,
   kids: { left, right },
});
const inGiven = (e: unknown, refName: string, not = false) => ({
   node: "inGiven",
   not,
   givenRef: given(refName),
   e,
});

/** A compiled condition whose `refSummary.fieldUsage` reports the paths given. */
function condition(e: unknown, fieldPaths: string[][]) {
   return {
      code: "(transcribed fixture)",
      e,
      refSummary: {
         fieldUsage: fieldPaths.map((path) => ({ path })),
         givenUsage: [{ id: "given/x" }],
      },
      isSourceFilter: true,
   };
}

describe("classifyAuthorizeGate", () => {
   it("treats an empty fieldUsage as the pre-existing given-only gate", () => {
      // The whole-source boolean every gate was before row-level gates existed.
      // Misclassifying this would change the enforcement mechanism of every
      // already-published gate on upgrade, so it is the one case that must not
      // depend on any judgement about the expression's shape.
      expect(
         classifyAuthorizeGate(
            {
               e: binary(">", given("LVL"), {
                  node: "numberLiteral",
                  literal: "3",
               }),
               refSummary: { fieldUsage: [], givenUsage: [{ id: "given/x" }] },
            },
            TYPES,
         ),
      ).toEqual({ shape: "given_only" });
   });

   it("treats an absent refSummary as given-only rather than guessing", () => {
      expect(classifyAuthorizeGate({}, TYPES)).toEqual({ shape: "given_only" });
   });

   it("accepts `field in $ARRAY`", () => {
      const result = classifyAuthorizeGate(
         condition(inGiven(field("org_id"), "GROUPS"), [["org_id"]]),
         TYPES,
      );
      expect(result).toEqual({
         shape: "row_level",
         givenNames: ["GROUPS"],
         literalAtoms: [],
      });
   });

   it("accepts a JOINED field path", () => {
      const result = classifyAuthorizeGate(
         condition(binary("=", field("childtable", "name"), given("BOB")), [
            ["childtable", "name"],
         ]),
         TYPES,
      );
      expect(result).toEqual({
         shape: "row_level",
         givenNames: ["BOB"],
         literalAtoms: [],
      });
   });

   it("accepts a boolean combination through and / or / parens", () => {
      const result = classifyAuthorizeGate(
         condition(
            binary(
               "and",
               { node: "()", e: inGiven(field("org_id"), "GROUPS") },
               binary(">", field("val"), given("LVL")),
            ),
            [["org_id"], ["val"]],
         ),
         TYPES,
      );
      expect(result.shape).toBe("row_level");
   });

   it("accepts the given on either side of a comparison", () => {
      expect(
         classifyAuthorizeGate(
            condition(binary("=", given("BOB"), field("owner")), [["owner"]]),
            TYPES,
         ).shape,
      ).toBe("row_level");
   });

   it("classifies the admin-override fold as row_level", () => {
      // `Model.resolveGateShape` folds `#(authorize) "$ROLE = 'admin'"` OR'd
      // with `#(authorize) "org_id in $GROUPS"` into ONE filter,
      // `(org_id in $GROUPS) or ($ROLE = 'admin')`, to keep OR semantics
      // under row-level enforcement. The `$ROLE = 'admin'` disjunct is a
      // given-vs-literal atom: legal, and contributes no field of its own.
      const result = classifyAuthorizeGate(
         condition(
            binary(
               "or",
               inGiven(field("org_id"), "GROUPS"),
               binary("=", given("ROLE"), {
                  node: "stringLiteral",
                  literal: "admin",
               }),
            ),
            [["org_id"]],
         ),
         TYPES,
      );
      expect(result).toEqual({
         shape: "row_level",
         givenNames: ["GROUPS", "ROLE"],
         literalAtoms: ["$ROLE = 'admin'"],
      });
   });

   it("keeps an all-given-only expression given_only, not row_level", () => {
      // `$ROLE = 'admin'` alone (no row field anywhere in the expression)
      // must stay on the pre-existing whole-source-boolean enforcement —
      // driven by Malloy's own `refSummary.fieldUsage` being empty, which is
      // checked before the walk ever runs. Already-published given-only
      // gates must not change enforcement mechanism just because a
      // given-vs-literal atom is now a legal ROW-LEVEL atom too.
      const result = classifyAuthorizeGate(
         condition(
            binary("=", given("ROLE"), {
               node: "stringLiteral",
               literal: "admin",
            }),
            [],
         ),
         TYPES,
      );
      expect(result).toEqual({ shape: "given_only" });
   });

   it("rejects a given-vs-literal atom with a disallowed operator", () => {
      // `$ROLE like 'a%'` — the given-vs-literal allowance only covers the
      // existing scalar comparison set (`=`,`!=`,`>`,`>=`,`<`,`<=`); `like`
      // is not one of them and stays refused, same as a field-vs-literal
      // `like`.
      const result = classifyAuthorizeGate(
         condition(
            binary("like", given("ROLE"), {
               node: "stringLiteral",
               literal: "a%",
            }),
            [["owner"]],
         ),
         TYPES,
      );
      expect(result).toMatchObject({
         shape: "rejected",
         cause: "unsupported_node",
      });
   });

   it("rejects `=` against an ARRAY given — it compiles and then fails in the warehouse", () => {
      // `org_id ? $GROUPS` compiles to this SAME `=` node, so one type check
      // covers both spellings; there is nothing to tell apart by syntax.
      const result = classifyAuthorizeGate(
         condition(binary("=", field("org_id"), given("GROUPS")), [["org_id"]]),
         TYPES,
      );
      expect(result.shape).toBe("rejected");
      expect(result).toMatchObject({ cause: "array_given_needs_in" });
   });

   it("rejects `in` against a SCALAR given", () => {
      const result = classifyAuthorizeGate(
         condition(inGiven(field("owner"), "BOB"), [["owner"]]),
         TYPES,
      );
      expect(result.shape).toBe("rejected");
      expect(result).toMatchObject({ cause: "scalar_given_rejects_in" });
   });

   it("refuses a given that is not on this model's surface, naming the import remedy", () => {
      // Malloy does not flatten a `given:` past one import hop, so a gate whose
      // given is two hops out would silently bind that given's DEFAULT.
      const result = classifyAuthorizeGate(
         condition(inGiven(field("org_id"), "FARAWAY"), [["org_id"]]),
         TYPES,
      );
      expect(result.shape).toBe("rejected");
      expect(result).toMatchObject({ cause: "unreachable_given" });
      expect((result as { detail: string }).detail).toContain(
         "import { FARAWAY }",
      );
   });

   it("rejects a function call", () => {
      const result = classifyAuthorizeGate(
         condition(
            binary("=", { node: "function_call", name: "lower" }, given("BOB")),
            [["name"]],
         ),
         TYPES,
      );
      expect(result).toMatchObject({
         shape: "rejected",
         cause: "unsupported_node",
      });
   });

   it("rejects arithmetic on the field side", () => {
      const result = classifyAuthorizeGate(
         condition(
            binary(
               ">",
               binary("+", field("val"), {
                  node: "numberLiteral",
                  literal: "1",
               }),
               given("LVL"),
            ),
            [["val"]],
         ),
         TYPES,
      );
      expect(result).toMatchObject({
         shape: "rejected",
         cause: "unsupported_node",
      });
   });

   it("rejects a comparison against a constant — a fixed filter, not an access rule", () => {
      const result = classifyAuthorizeGate(
         condition(
            binary("=", field("org_id"), {
               node: "numberLiteral",
               literal: "1",
            }),
            [["org_id"]],
         ),
         TYPES,
      );
      expect(result).toMatchObject({
         shape: "rejected",
         cause: "no_given_reference",
      });
   });

   it("rejects a `not` wrapper — negation makes an EMPTY given match every row", () => {
      // Measured on 0.0.427: `not (org_id in $GROUPS)` with an empty $GROUPS
      // emits `WHERE COALESCE(NOT (FALSE),TRUE)` and returns every row, so a
      // caller with no groups reads the whole table. Negation inverts the one
      // property that makes `in $ARRAY` safe. The COALESCE also admits a NULL
      // on the gated column rather than excluding it.
      const result = classifyAuthorizeGate(
         condition({ node: "not", e: inGiven(field("org_id"), "GROUPS") }, [
            ["org_id"],
         ]),
         TYPES,
      );
      expect(result).toMatchObject({
         shape: "rejected",
         cause: "unsupported_node",
      });
   });

   it("rejects `not in` — a gate states which rows a caller MAY read", () => {
      const result = classifyAuthorizeGate(
         condition(inGiven(field("org_id"), "GROUPS", true), [["org_id"]]),
         TYPES,
      );
      expect(result).toMatchObject({
         shape: "rejected",
         cause: "unsupported_node",
      });
   });

   it("rejects `like`", () => {
      const result = classifyAuthorizeGate(
         condition(
            binary("like", field("name"), {
               node: "stringLiteral",
               literal: "a%",
            }),
            [["name"]],
         ),
         TYPES,
      );
      expect(result).toMatchObject({
         shape: "rejected",
         cause: "unsupported_node",
      });
   });

   it("rejects an unreadable condition rather than passing it", () => {
      // Fail closed: a shape we cannot read is not a gate we can enforce.
      expect(
         classifyAuthorizeGate(condition(null, [["org_id"]]), TYPES),
      ).toMatchObject({ shape: "rejected" });
      expect(
         classifyAuthorizeGate(
            condition({ notANode: true }, [["org_id"]]),
            TYPES,
         ),
      ).toMatchObject({ shape: "rejected" });
   });

   it("rejects a gate nested past the walk bound", () => {
      let deep: unknown = inGiven(field("org_id"), "GROUPS");
      for (let i = 0; i < 80; i++) deep = { node: "()", e: deep };
      expect(
         classifyAuthorizeGate(condition(deep, [["org_id"]]), TYPES),
      ).toMatchObject({ shape: "rejected", cause: "unsupported_node" });
   });
});
