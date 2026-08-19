import { describe, expect, it } from "bun:test";
import {
   assertNoVacuousDefaultAtom,
   classifyAuthorizeGate,
   collectAuthorizeExprs,
   containsAuthorizeAnnotationTag,
   type LiteralAtomDetail,
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

/** No given in this suite's fixtures declares a default; classifyAuthorizeGate's
 *  third argument is exercised separately by the field-given-default tests below. */
const DEFAULTS = new Map<string, string>();

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
   it("classifies an empty fieldUsage as row_level too — a given-vs-literal atom needs no field", () => {
      // `$LVL > 3` references no row field at all (`fieldUsage` is empty), but
      // there is only one gate concept now: every gate is a row filter, and a
      // field-less one is simply constant across every row. `fieldUsage` is
      // therefore not consulted at all any more — the walk below is what
      // decides the shape, uniformly.
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
            DEFAULTS,
         ),
      ).toEqual({
         shape: "row_level",
         givenNames: ["LVL"],
         literalAtoms: ["$LVL > 3"],
         literalAtomDetails: [
            {
               kind: "comparison",
               text: "$LVL > 3",
               given: "LVL",
               op: ">",
               literalText: "3",
               givenOnLeft: true,
               negate: false,
            },
         ],
      });
   });

   it("rejects an absent condition (no `e` at all) as unreadable, fail closed", () => {
      // No compiled expression to walk — this is not "given-only", it is
      // nothing to classify at all.
      expect(classifyAuthorizeGate({}, TYPES, DEFAULTS)).toEqual({
         shape: "rejected",
         cause: "unsupported_node",
         detail: "the gate has an unreadable shape",
      });
   });

   it("accepts `field in $ARRAY`", () => {
      const result = classifyAuthorizeGate(
         condition(inGiven(field("org_id"), "GROUPS"), [["org_id"]]),
         TYPES,
         DEFAULTS,
      );
      expect(result).toEqual({
         shape: "row_level",
         givenNames: ["GROUPS"],
         literalAtoms: [],
         literalAtomDetails: [],
      });
   });

   it("accepts a JOINED field path", () => {
      const result = classifyAuthorizeGate(
         condition(binary("=", field("childtable", "name"), given("BOB")), [
            ["childtable", "name"],
         ]),
         TYPES,
         DEFAULTS,
      );
      expect(result).toEqual({
         shape: "row_level",
         givenNames: ["BOB"],
         literalAtoms: [],
         literalAtomDetails: [],
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
         DEFAULTS,
      );
      expect(result.shape).toBe("row_level");
   });

   it("accepts the given on either side of a comparison", () => {
      expect(
         classifyAuthorizeGate(
            condition(binary("=", given("BOB"), field("owner")), [["owner"]]),
            TYPES,
            DEFAULTS,
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
         DEFAULTS,
      );
      expect(result).toEqual({
         shape: "row_level",
         givenNames: ["GROUPS", "ROLE"],
         literalAtoms: ["$ROLE = 'admin'"],
         literalAtomDetails: [
            {
               kind: "comparison",
               text: "$ROLE = 'admin'",
               given: "ROLE",
               op: "=",
               literalText: "'admin'",
               givenOnLeft: true,
               negate: false,
            },
         ],
      });
   });

   it("classifies an all-given expression as row_level, not a separate given-only shape", () => {
      // `$ROLE = 'admin'` alone (no row field anywhere in the expression) is
      // a given-vs-literal atom — a legal row-level gate whose filter simply
      // doesn't mention a column. There is no separate given-only shape any
      // more: every gate is one concept, `row_level` (or `rejected`).
      const result = classifyAuthorizeGate(
         condition(
            binary("=", given("ROLE"), {
               node: "stringLiteral",
               literal: "admin",
            }),
            [],
         ),
         TYPES,
         DEFAULTS,
      );
      expect(result).toEqual({
         shape: "row_level",
         givenNames: ["ROLE"],
         literalAtoms: ["$ROLE = 'admin'"],
         literalAtomDetails: [
            {
               kind: "comparison",
               text: "$ROLE = 'admin'",
               given: "ROLE",
               op: "=",
               literalText: "'admin'",
               givenOnLeft: true,
               negate: false,
            },
         ],
      });
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
         DEFAULTS,
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
         DEFAULTS,
      );
      expect(result.shape).toBe("rejected");
      expect(result).toMatchObject({ cause: "array_given_needs_in" });
   });

   it("rejects `in` against a SCALAR given", () => {
      const result = classifyAuthorizeGate(
         condition(inGiven(field("owner"), "BOB"), [["owner"]]),
         TYPES,
         DEFAULTS,
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
         DEFAULTS,
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
         DEFAULTS,
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
         DEFAULTS,
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
         DEFAULTS,
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
         DEFAULTS,
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
         DEFAULTS,
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
         DEFAULTS,
      );
      expect(result).toMatchObject({
         shape: "rejected",
         cause: "unsupported_node",
      });
   });

   it("rejects an unreadable condition rather than passing it", () => {
      // Fail closed: a shape we cannot read is not a gate we can enforce.
      expect(
         classifyAuthorizeGate(condition(null, [["org_id"]]), TYPES, DEFAULTS),
      ).toMatchObject({ shape: "rejected" });
      expect(
         classifyAuthorizeGate(
            condition({ notANode: true }, [["org_id"]]),
            TYPES,
            DEFAULTS,
         ),
      ).toMatchObject({ shape: "rejected" });
   });

   it("rejects a gate nested past the walk bound", () => {
      let deep: unknown = inGiven(field("org_id"), "GROUPS");
      for (let i = 0; i < 80; i++) deep = { node: "()", e: deep };
      expect(
         classifyAuthorizeGate(condition(deep, [["org_id"]]), TYPES, DEFAULTS),
      ).toMatchObject({ shape: "rejected", cause: "unsupported_node" });
   });

   it("a bare `true`/`false` literal contributes no `literalAtomDetails` entry — nothing for assertNoVacuousDefaultAtom to evaluate", () => {
      // `literalAtoms` still carries the text (for the field-less-gate
      // grammar tests elsewhere), but there is no given to probe a
      // declared-default hazard against, so it must not reach the evaluator.
      expect(
         classifyAuthorizeGate(condition({ node: "true" }, []), TYPES, DEFAULTS),
      ).toEqual({
         shape: "row_level",
         givenNames: [],
         literalAtoms: ["true"],
         literalAtomDetails: [],
      });
      expect(
         classifyAuthorizeGate(condition({ node: "false" }, []), TYPES, DEFAULTS),
      ).toEqual({
         shape: "row_level",
         givenNames: [],
         literalAtoms: ["false"],
         literalAtomDetails: [],
      });
   });
});

/**
 * `assertNoVacuousDefaultAtom` used to decide these cases by compiling and
 * RUNNING a one-row SQL probe (`runProbe`/`buildAuthorizeProbe`) — which the
 * package-load worker's `ProxyConnection.runSQL` deliberately throws on
 * (`package_load_worker.ts`), failing the load of every package whose gate
 * carried this idiom. It is now a pure, statically-evaluated function, so
 * these are hand-built `LiteralAtomDetail`s — the same idiom
 * `classifyAuthorizeGate`'s own tests above use — rather than a compiled
 * Malloy condition; the end-to-end path (a real gate loading through
 * `Model.create`) is covered by `row_level_authorize.integration.spec.ts`'s
 * "vacuous default atom" describe block.
 */
describe("assertNoVacuousDefaultAtom", () => {
   const TYPES = new Map([
      ["ROLE", "string"],
      ["NUM", "number"],
      ["NO_DEFAULT", "string"],
      ["TENANT", "string"],
      ["ALLOWED", "array"],
   ]);

   function comparisonAtom(
      overrides: Partial<Extract<LiteralAtomDetail, { kind: "comparison" }>>,
   ): LiteralAtomDetail {
      return {
         kind: "comparison",
         text: "",
         given: "ROLE",
         op: "=",
         literalText: "'admin'",
         givenOnLeft: true,
         negate: false,
         ...overrides,
      };
   }

   it("does not refuse `$ROLE = 'admin'` when ROLE defaults to 'guest' — the regression this task fixes", () => {
      const atom = comparisonAtom({ text: "$ROLE = 'admin'" });
      expect(() =>
         assertNoVacuousDefaultAtom(
            "S",
            [atom],
            TYPES,
            new Map([["ROLE", "'guest'"]]),
         ),
      ).not.toThrow();
   });

   it("refuses `$ROLE != 'admin'` when ROLE defaults to '' — vacuously true, same message as before", () => {
      const atom = comparisonAtom({
         text: "$ROLE != 'admin'",
         op: "!=",
      });
      expect(() =>
         assertNoVacuousDefaultAtom("S", [atom], TYPES, new Map([["ROLE", "''"]])),
      ).toThrow(
         /the atom `\$ROLE != 'admin'` evaluates to TRUE when a caller supplies no givens/,
      );
   });

   it("does not refuse when the given carries no declared default", () => {
      const atom = comparisonAtom({
         text: "$NO_DEFAULT = 'admin'",
         given: "NO_DEFAULT",
      });
      // Asserting the absence of a warning too: `not.toThrow()` alone also
      // passes when the atom is treated as UNDECIDABLE, which warns and denies
      // the source for every request — the opposite of "does not refuse".
      const warnings: string[] = [];
      expect(() =>
         assertNoVacuousDefaultAtom("S", [atom], TYPES, new Map(), (_s, d) =>
            warnings.push(d),
         ),
      ).not.toThrow();
      expect(warnings).toEqual([]);
   });

   it("compares a number-typed given numerically, not lexicographically", () => {
      // Lexicographically "9" > "10" (string compare), but 9 > 10 is false —
      // proving the numeric parse actually runs rather than falling back to
      // JS's default `>` on the rendered text.
      const atom = comparisonAtom({
         text: "$NUM > 10",
         given: "NUM",
         op: ">",
         literalText: "10",
      });
      expect(() =>
         assertNoVacuousDefaultAtom("S", [atom], TYPES, new Map([["NUM", "9"]])),
      ).not.toThrow();
   });

   it("still refuses a numeric comparison that IS vacuous at the default", () => {
      const atom = comparisonAtom({
         text: "$NUM >= 5",
         given: "NUM",
         op: ">=",
         literalText: "5",
      });
      expect(() =>
         assertNoVacuousDefaultAtom("S", [atom], TYPES, new Map([["NUM", "9"]])),
      ).toThrow(/evaluates to TRUE/);
   });

   it("negation: `not ($X = 'a')` is false (not vacuous) when X defaults to 'a'", () => {
      const atom = comparisonAtom({
         text: "not ($ROLE = 'a')",
         literalText: "'a'",
         negate: true,
      });
      expect(() =>
         assertNoVacuousDefaultAtom("S", [atom], TYPES, new Map([["ROLE", "'a'"]])),
      ).not.toThrow();
   });

   it("negation: `not ($X = 'a')` is TRUE (vacuous) when X defaults to 'b'", () => {
      const atom = comparisonAtom({
         text: "not ($ROLE = 'a')",
         literalText: "'a'",
         negate: true,
      });
      expect(() =>
         assertNoVacuousDefaultAtom("S", [atom], TYPES, new Map([["ROLE", "'b'"]])),
      ).toThrow(/the atom `not \(\$ROLE = 'a'\)` evaluates to TRUE/);
   });

   it("membership: `$TENANT in $ALLOWED` is not refused when either given lacks a default", () => {
      const atom: LiteralAtomDetail = {
         kind: "membership",
         text: "$TENANT in $ALLOWED",
         element: "TENANT",
         container: "ALLOWED",
      };
      // Neither given has a default.
      expect(() =>
         assertNoVacuousDefaultAtom("S", [atom], TYPES, new Map()),
      ).not.toThrow();
      // Only the element does (an array given cannot declare one at all —
      // see `docs/givens.md` — so this is the only defaulted-container case
      // reachable through a real gate; exercised directly here anyway since
      // this function must not assume which side is missing).
      expect(() =>
         assertNoVacuousDefaultAtom(
            "S",
            [atom],
            TYPES,
            new Map([["TENANT", "'acme'"]]),
         ),
      ).not.toThrow();
   });

   it("membership: refuses when both givens have defaults and the element's default IS a member of the container's", () => {
      const atom: LiteralAtomDetail = {
         kind: "membership",
         text: "$TENANT in $ALLOWED",
         element: "TENANT",
         container: "ALLOWED",
      };
      const defaults = new Map([
         ["TENANT", "'acme'"],
         ["ALLOWED", "['acme', 'other']"],
      ]);
      expect(() =>
         assertNoVacuousDefaultAtom("S", [atom], TYPES, defaults),
      ).toThrow(/the atom `\$TENANT in \$ALLOWED` evaluates to TRUE/);
   });

   it("membership: does not refuse when the element's default is NOT a member of the container's", () => {
      const atom: LiteralAtomDetail = {
         kind: "membership",
         text: "$TENANT in $ALLOWED",
         element: "TENANT",
         container: "ALLOWED",
      };
      const defaults = new Map([
         ["TENANT", "'acme'"],
         ["ALLOWED", "['other']"],
      ]);
      expect(() =>
         assertNoVacuousDefaultAtom("S", [atom], TYPES, defaults),
      ).not.toThrow();
   });

   it("routes an atom it cannot decide statically to onRowLevelGateUnexpressible instead of failing the load", () => {
      // A `date` given: this function only reduces string/number/boolean, so
      // its default cannot be parsed — undecidable, not "vacuous" or "safe".
      const atom = comparisonAtom({
         text: "$SINCE = @2024-01-01",
         given: "SINCE",
         literalText: "@2024-01-01",
      });
      const typesWithDate = new Map([...TYPES, ["SINCE", "date"]]);
      const defaults = new Map([["SINCE", "@2024-01-01"]]);
      const warnings: Array<{ sourceName: string; detail: string }> = [];
      expect(() =>
         assertNoVacuousDefaultAtom(
            "S",
            [atom],
            typesWithDate,
            defaults,
            (sourceName, detail) => warnings.push({ sourceName, detail }),
         ),
      ).not.toThrow();
      expect(warnings).toHaveLength(1);
      expect(warnings[0].sourceName).toBe("S");
   });
});
