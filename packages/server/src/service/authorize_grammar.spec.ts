// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// The authorize grammar as tables: body -> parsed terms, body -> rejection
// cause, and a source's notes -> coherence verdict. Then the two IR-level
// refusals (fan-out join paths, retired-route markers), which need a real
// compiled model. That each refusal is reached from a real `Model.create` is
// pinned in tests/component/authorize/load_validation.spec.ts.
import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
   type ModelDef,
   type SourceDef,
} from "@malloydata/malloy";
import { afterAll, describe, expect, it } from "bun:test";
import {
   assertAuthorizeGrammarTermsCoherent,
   AuthorizeGrammarError,
   parseAuthorizeGrammarBody,
   type AuthorizeGrammarRejectionCause,
} from "./authorize_grammar";
import { ACCESS_FILTER_ROUTE, AUTHORIZE_ROUTE } from "./authorize_routes";
import {
   assertNoFanoutFieldPath,
   assertNoRetiredRouteMarkers,
   collectRetiredRouteMarkers,
} from "./gate_classification";

type Givens = ReadonlyMap<string, string>;
const NONE: Givens = new Map();
const SCALAR: Givens = new Map([["REGION", "string"]]);
const LIST: Givens = new Map([["GROUPS", "array"]]);
const A_B: Givens = new Map([
   ["A", "string"],
   ["B", "string"],
]);
const REGION_ROLE: Givens = new Map([
   ["REGION", "string"],
   ["ROLE", "string"],
]);
const ROLE: Givens = new Map([["ROLE", "string"]]);
const AF = ACCESS_FILTER_ROUTE;
const AUTH = AUTHORIZE_ROUTE;

/** The thrown error's cause, or a failure if nothing (or something else) threw. */
function causeOf(fn: () => unknown): {
   cause: AuthorizeGrammarRejectionCause;
   message: string;
} {
   try {
      fn();
   } catch (err) {
      expect(err).toBeInstanceOf(AuthorizeGrammarError);
      const e = err as AuthorizeGrammarError;
      return { cause: e.rejectionCause, message: e.message };
   }
   throw new Error("expected an AuthorizeGrammarError");
}

const row = (
   fieldPath: string,
   segments: string[],
   given: string,
   op: string,
) => ({
   scope: "row_level",
   fieldPath,
   fieldPathSegments: segments,
   given,
   operator: op,
});
const lit = (literal: string, given: string) => ({
   scope: "source_level",
   literal,
   given,
});

describe("parseAuthorizeGrammarBody — accepted bodies", () => {
   it.each([
      // the sentinels: an exact (trimmed, case-insensitive) `true` / `false`
      ["true", NONE, AUTH, [{ scope: "admit_all" }]],
      ["TRUE", NONE, AUTH, [{ scope: "admit_all" }]],
      ["  true  ", NONE, AUTH, [{ scope: "admit_all" }]],
      ["false", NONE, AUTH, [{ scope: "deny_all" }]],
      // source-level terms read either way round for `=`; `in` is literal-first
      ["'admin' = $ROLE", ROLE, AUTH, [lit("'admin'", "ROLE")]],
      ["$ROLE = 'admin'", ROLE, AUTH, [lit("'admin'", "ROLE")]],
      ["$REGION = 'analyst'", SCALAR, AUTH, [lit("'analyst'", "REGION")]],
      ["'finance' in $GROUPS", LIST, AUTH, [lit("'finance'", "GROUPS")]],
      // tokens inside a string literal are not syntax
      [
         "'research and development' in $GROUPS",
         LIST,
         AUTH,
         [lit("'research and development'", "GROUPS")],
      ],
      [
         "'rock or roll' in $GROUPS",
         LIST,
         AUTH,
         [lit("'rock or roll'", "GROUPS")],
      ],
      ["'a>b' in $GROUPS", LIST, AUTH, [lit("'a>b'", "GROUPS")]],
      ["'a!=b' in $GROUPS", LIST, AUTH, [lit("'a!=b'", "GROUPS")]],
      // row-level terms
      [
         "region = $REGION and org_id in $GROUPS",
         new Map([...SCALAR, ...LIST]),
         AF,
         [
            row("region", ["region"], "REGION", "="),
            row("org_id", ["org_id"], "GROUPS", "in"),
         ],
      ],
      [
         "child.name in $GROUPS",
         LIST,
         AF,
         [row("child.name", ["child", "name"], "GROUPS", "in")],
      ],
      [
         "region = $A and org.region = $B",
         A_B,
         AF,
         [
            row("region", ["region"], "A", "="),
            row("org.region", ["org", "region"], "B", "="),
         ],
      ],
      // a backtick-quoted column hides the tokens (and dots) inside it
      [
         "`org in region` = $REGION",
         SCALAR,
         AF,
         [row("`org in region`", ["org in region"], "REGION", "=")],
      ],
      [
         "`research and development` in $GROUPS",
         LIST,
         AF,
         [
            row(
               "`research and development`",
               ["research and development"],
               "GROUPS",
               "in",
            ),
         ],
      ],
      [
         "`cost.center`.name in $GROUPS",
         LIST,
         AF,
         [row("`cost.center`.name", ["cost.center", "name"], "GROUPS", "in")],
      ],
      // a given absent from the declared-type map skips the arity check
      [
         "region = $UNKNOWN",
         NONE,
         AF,
         [row("region", ["region"], "UNKNOWN", "=")],
      ],
      // `malloyGivenToApi` renders a list given as `array` or `<element>[]`;
      // all three are lists, and a scalar stays a scalar.
      ...["array", "number[]", "string[]"].map((rendering) => [
         "org_id in $GROUPS",
         new Map([["GROUPS", rendering]]),
         AF,
         [row("org_id", ["org_id"], "GROUPS", "in")],
      ]),
      [
         "region = $REGION",
         SCALAR,
         AF,
         [row("region", ["region"], "REGION", "=")],
      ],
   ] as Array<[string, Givens, string, unknown[]]>)(
      "%s",
      (body, givens, route, expected) => {
         expect(parseAuthorizeGrammarBody("X", body, givens, route)).toEqual(
            expected as never,
         );
      },
   );
});

describe("parseAuthorizeGrammarBody — refused bodies", () => {
   it.each([
      ["", NONE, AF, "empty_body"],
      ["   ", NONE, AF, "empty_body"],
      ["region = $REGION or org_id = $REGION", SCALAR, AF, "compound_boolean"],
      ["not region = $REGION", SCALAR, AF, "compound_boolean"],
      ["region != $REGION", SCALAR, AF, "negated_operator"],
      ["region > $REGION", SCALAR, AF, "comparison_operator"],
      ["upper(region) = $REGION", SCALAR, AF, "left_not_field_path"],
      ["1 = 1", NONE, AF, "left_not_field_path"],
      // a row-level term is not reversible: the column stays on the left
      ["$GROUPS = org_id", LIST, AF, "left_not_field_path"],
      ["region = 'east'", SCALAR, AF, "missing_given_reference"],
      ["region like $REGION", SCALAR, AF, "malformed_body"],
      // only an exact `true` is the sentinel; here it is a malformed term
      ["true and org_id = $A", A_B, AF, "malformed_body"],
      ["org_id = $REGION and region = $REGION", SCALAR, AF, "duplicate_given"],
      ["region = $A and region = $B", A_B, AF, "duplicate_field_path"],
      // caught on the unquoted SEGMENTS, not the authored text
      ["`region` = $A and region = $B", A_B, AF, "duplicate_field_path"],
      // arity: `=` is scalar, `in` is list, for every list rendering
      ["org_id = $GROUPS", LIST, AF, "operator_arity_mismatch"],
      ...["number[]", "string[]"].map((rendering) => [
         "org_id = $GROUPS",
         new Map([["GROUPS", rendering]]),
         AF,
         "operator_arity_mismatch",
      ]),
      ["region in $REGION", SCALAR, AF, "operator_arity_mismatch"],
      // each route admits one scope; a mixed body is refused by the other one
      ["region = $REGION", SCALAR, AUTH, "row_level_term_in_authorize"],
      [
         "region = $REGION and 'admin' = $ROLE",
         REGION_ROLE,
         AUTH,
         "row_level_term_in_authorize",
      ],
      [
         "region = $REGION and 'admin' = $ROLE",
         REGION_ROLE,
         AF,
         "source_level_term_in_access_filter",
      ],
      ["'finance' in $GROUPS", LIST, AF, "source_level_term_in_access_filter"],
   ] as Array<[string, Givens, string, AuthorizeGrammarRejectionCause]>)(
      "%p on %s",
      (body, givens, route, cause) => {
         expect(
            causeOf(() => parseAuthorizeGrammarBody("X", body, givens, route))
               .cause,
         ).toBe(cause);
      },
   );

   it.each(["true", "false"])(
      "the sentinel `%s` is refused on #(access_filter), naming the lock form",
      (body) => {
         const { cause, message } = causeOf(() =>
            parseAuthorizeGrammarBody("X", body, NONE, AF),
         );
         expect(cause).toBe("sentinel_in_access_filter");
         expect(message).toContain("#(authorize)");
      },
   );

   it.each([AF, AUTH])(
      "a reversed `in` ($GIVEN in 'literal') is refused on %s, never silently swapped",
      (route) => {
         // The graft compiles the author's original text and Malloy rejects
         // array-in-string membership, so swapping would validate a body
         // that then fails at model compilation.
         const { cause, message } = causeOf(() =>
            parseAuthorizeGrammarBody("X", "$GROUPS in 'finance'", LIST, route),
         );
         expect(cause).toBe("reversed_in_operands");
         expect(message).toMatch(/'finance' in \$GROUPS/);
      },
   );
});

// Repeated notes are legal and AND, so the within-one-body checks must also
// see a source's terms as one set across its notes. Each note is
// [body, givens, parse route, the route it is declared on].
describe("assertAuthorizeGrammarTermsCoherent — across notes", () => {
   type Note = [string, Givens, string, string];
   const check = (notes: Note[]) => () =>
      assertAuthorizeGrammarTermsCoherent(
         "X",
         notes.map(([body, givens, parseRoute, route]) => ({
            term: parseAuthorizeGrammarBody("X", body, givens, parseRoute)[0],
            route,
         })),
      );

   it.each([
      [
         "duplicate_given",
         [
            ["a = $G", SCALAR, AF, AF],
            ["b = $G", SCALAR, AF, AF],
         ],
      ],
      [
         "duplicate_field_path",
         [
            ["region = $A", A_B, AF, AF],
            ["region = $B", A_B, AF, AF],
         ],
      ],
      [
         "duplicate_field_path",
         [
            ["`region` = $A", A_B, AF, AF],
            ["region = $B", A_B, AF, AF],
         ],
      ],
      [
         "deny_all_with_sibling",
         [
            ["false", NONE, AUTH, AF],
            ["region = $REGION", SCALAR, AF, AF],
         ],
      ],
      [
         "admit_all_with_sibling",
         [
            ["true", NONE, AUTH, AF],
            ["region = $REGION", SCALAR, AF, AF],
         ],
      ],
      [
         "admit_all_with_sibling",
         [
            ["true", NONE, AUTH, AUTH],
            ["'finance' in $G", LIST, AUTH, AUTH],
         ],
      ],
      // THE ORDER PIN: admit_all and deny_all together read fail-closed...
      [
         "deny_all_with_sibling",
         [
            ["true", NONE, AUTH, AF],
            ["false", NONE, AUTH, AF],
         ],
      ],
      // ...and the deny check is the one that is not route-scoped.
      [
         "deny_all_with_sibling",
         [
            ["true", NONE, AUTH, AF],
            ["false", NONE, AUTH, AUTH],
         ],
      ],
   ] as Array<[AuthorizeGrammarRejectionCause, Note[]]>)(
      "refuses %s: %j",
      (cause, notes) => {
         expect(causeOf(check(notes)).cause).toBe(cause);
      },
   );

   it.each([
      ["a lone deny_all", [["false", NONE, AUTH, AF]]],
      // coherence is per route: the two routes' terms AND
      [
         "the same given on two different routes",
         [
            ["org_id in $G", LIST, AF, AF],
            ["'finance' in $G", LIST, AUTH, AUTH],
         ],
      ],
      // `true` sheds only its own route's inherited gate, so a term on the
      // other route is live, not dead text
      [
         "admit_all beside a term on the other route",
         [
            ["true", NONE, AUTH, AF],
            ["'finance' in $G", LIST, AUTH, AUTH],
         ],
      ],
      [
         "a row filter beside the lock's own `true`",
         [
            ["region = $REGION", SCALAR, AF, AF],
            ["true", NONE, AUTH, AUTH],
         ],
      ],
   ] as Array<[string, Note[]]>)("admits %s", (_label, notes) => {
      expect(check(notes)).not.toThrow();
   });
});

// ---------------------------------------------------------------------------
// Fan-out and retired-route refusals: IR-level, so real compiled models.
// ---------------------------------------------------------------------------

const ROOT = "file:///authorize-grammar/";
const duckdb = new DuckDBConnection("duckdb", ":memory:");
const connections = new FixedConnectionMap(
   new Map([["duckdb", duckdb]]),
   "duckdb",
);
afterAll(() => duckdb.close());

async function compileModel(
   files: string | Readonly<Record<string, string>>,
   entry = "m.malloy",
): Promise<ModelDef> {
   const fileMap = typeof files === "string" ? { [entry]: files } : files;
   const urlReader = new InMemoryURLReader(
      new Map(
         Object.entries(fileMap).map(([name, text]) => [
            `${ROOT}${name}`,
            text,
         ]),
      ),
   );
   const runtime = new Runtime({ urlReader, connections });
   const compiled = await runtime
      .loadModel(new URL(`${ROOT}${entry}`), { importBaseURL: new URL(ROOT) })
      .getModel();
   return (compiled as unknown as { _modelDef: ModelDef })._modelDef;
}

describe("assertNoFanoutFieldPath", () => {
   async function fanout(
      model: string,
      fieldPath: string,
      segments: readonly string[],
   ): Promise<() => void> {
      const parent = (await compileModel(model)).contents[
         "parent"
      ] as SourceDef;
      return () =>
         assertNoFanoutFieldPath("parent", parent, fieldPath, segments);
   }

   it("refuses a path through a join_many", async () => {
      const assertPath = await fanout(
         `source: child is duckdb.sql("select 1 as parent_id, 'north' as name") extend {}
source: parent is duckdb.sql("select 1 as id") extend {
   join_many: kids is child on id = kids.parent_id
}`,
         "kids.name",
         ["kids", "name"],
      );
      expect(causeOf(assertPath).cause).toBe("fanout_path");
   });

   it("admits a path through a join_one", async () => {
      const assertPath = await fanout(
         `source: child is duckdb.sql("select 1 as id, 'north' as name") extend {}
source: parent is duckdb.sql("select 1 as child_id") extend {
   join_one: kid is child on child_id = kid.id
}`,
         "kid.name",
         ["kid", "name"],
      );
      expect(assertPath).not.toThrow();
   });

   it("refuses a path through a backtick-quoted join_many segment", async () => {
      // A raw "`cost center`" segment would never match the join's real name
      // and read a genuine fan-out as resolved; the parser's unquoted
      // segments are what must reach this check.
      const [term] = parseAuthorizeGrammarBody(
         "parent",
         "`cost center`.name in $GROUPS",
         LIST,
         AF,
      );
      if (term.scope !== "row_level") throw new Error("expected row_level");
      const assertPath = await fanout(
         `source: child is duckdb.sql("select 1 as parent_id, 'north' as name") extend {}
source: parent is duckdb.sql("select 1 as id") extend {
   join_many: \`cost center\` is child on id = \`cost center\`.parent_id
}`,
         term.fieldPath,
         term.fieldPathSegments,
      );
      expect(causeOf(assertPath).cause).toBe("fanout_path");
   });

   it("refuses a path whose segment does not resolve (fail closed, not 'no fan-out')", async () => {
      const assertPath = await fanout(
         `source: parent is duckdb.sql("select 1 as id") extend {}`,
         "nope.name",
         ["nope", "name"],
      );
      expect(assertPath).toThrow(AuthorizeGrammarError);
   });
});

describe("retired-route markers", () => {
   const X = `source: X is duckdb.sql("select 1 as org_id") extend {
   measure: c is count()
}`;

   it.each([
      ["on the source line", `#(partition) org_id = $ORG\n${X}`, '"X"'],
      [
         "one level too low, on a field",
         `source: X is duckdb.sql("select 1 as org_id") extend {
   #(partition) org_id = $ORG
   measure: c is count()
}`,
         '"X"',
      ],
      [
         "on a joined source, through the join",
         `source: Child is duckdb.sql("select 1 as org_id, 1 as k") extend {
   #(partition) org_id = $ORG
   measure: c is count()
}
source: X is duckdb.sql("select 1 as k") extend {
   join_one: Child on k = Child.k
}`,
         '"X"',
      ],
      [
         "on a top-level query:",
         `${X}\n\n#(partition) org_id = $ORG\nquery: secret is X -> { aggregate: c is count() }`,
         '"secret"',
      ],
      [
         "at the file level",
         `##(partition) org_id = $ORG\n${X}`,
         "model itself",
      ],
      // the block form lands on blockNotes, which a `.notes` reader never sees
      [
         "as a file-level ##|(partition)|## block",
         `##|(partition)\norg_id = $ORG\n|##\n${X}`,
         "model itself",
      ],
   ])(
      "catches a leftover #(partition) marker %s",
      async (_label, model, names) => {
         const found = collectRetiredRouteMarkers(await compileModel(model));
         expect(found.some((f) => f.includes(names))).toBe(true);
         expect(() => assertNoRetiredRouteMarkers(found)).toThrow();
      },
   );

   it("catches a marker reachable only through an inherited annotation chain", async () => {
      // `Mid`'s own `#(doc)` demotes the marker to `annotations.inherits`, and
      // the declaring `Base` never enters the entry model's contents.
      const modelDef = await compileModel(
         {
            "base.malloy": `#(partition) org_id = $ORG\n${X.replace("X is", "Base is")}`,
            "mid.malloy": `import "base.malloy"\n#(doc) mid own doc\nsource: Mid is Base extend {\n   measure: c2 is count()\n}`,
            "entry.malloy": `import "mid.malloy"\nsource: Entry is Mid extend {}`,
         },
         "entry.malloy",
      );
      expect(Object.keys(modelDef.contents)).not.toContain("Base");
      const found = collectRetiredRouteMarkers(modelDef);
      expect(found.some((f) => f.includes('"Entry"'))).toBe(true);
      expect(() => assertNoRetiredRouteMarkers(found)).toThrow();
   });

   it("finds nothing in a model with no retired marker", async () => {
      expect(collectRetiredRouteMarkers(await compileModel(X))).toEqual([]);
   });
});
