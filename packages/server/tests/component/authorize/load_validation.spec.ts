// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What `Model.create` accepts, refuses and warns about for `#(authorize)` and
 * `#(access_filter)`: grammar refusals, cross-note coherence, defaulted gate
 * givens (G4), misplaced / file-level / legacy / near-miss annotations, and
 * the shapes that must still load. A refusal here is the whole contract: a
 * gate that loads unvalidated is a source serving rows its author reads as
 * locked. Query-time semantics live in `gate_semantics.spec.ts`.
 *
 * The parser itself is unit-tested in `src/service/authorize_grammar.spec.ts`;
 * the table below proves each shape reaches it from a real load, with the
 * model's given types wired in (the arity check needs them).
 */
import { afterAll, beforeAll, describe, expect, it, spyOn } from "bun:test";
import { ModelCompilationError } from "../../../src/errors";
import { logger } from "../../../src/logger";
import {
   AuthorizeGrammarError,
   type AuthorizeGrammarRejectionCause,
} from "../../../src/service/authorize_grammar";
import {
   countFor,
   expectAccessDenied,
   warnedSources,
   withAuthorizeModel,
} from "../../../src/test_helpers/authorize_fixture";
import { loadTempPackage } from "../../../src/test_helpers/temp_package";

/** A one-source model over `accounts`: `givens` lines, then the gate lines. */
function gateModel(
   givens: string,
   gate: string,
   source = "source: X is duckdb.table('accounts') extend {}",
): string {
   const givenBlock = givens
      ? `given:\n${givens
           .split("\n")
           .map((l) => `  ${l}`)
           .join("\n")}\n\n`
      : "";
   return `##! experimental.givens\n\n${givenBlock}${gate}\n${source}\n`;
}

async function expectLoadError(
   files: string | Record<string, string>,
   entry?: string,
): Promise<Error> {
   return withAuthorizeModel(
      files,
      async ({ model, error }) => {
         expect(error).toBeInstanceOf(ModelCompilationError);
         // A failed load surfaces no sources: the gate is never reported
         // as unrestricted.
         expect(model.getSources()).toBeUndefined();
         return error!;
      },
      entry,
   );
}

async function expectLoadsClean(
   files: string | Record<string, string>,
   entry?: string,
): Promise<void> {
   await withAuthorizeModel(
      files,
      async ({ error }) => expect(error).toBeUndefined(),
      entry,
   );
}

// ---------------------------------------------------------------------------
// The grammar, reached from a real load. Each row is a shape the narrow
// grammar refuses outright; every one used to compile as an arbitrary Malloy
// boolean (or a STRING-form gate) and filter in some surprising way.
// ---------------------------------------------------------------------------

const GRAMMAR_REFUSALS: Array<{
   model: string;
   cause: AuthorizeGrammarRejectionCause;
}> = [
   // compound_boolean: `not`, `or`, `is not null`
   {
      model: gateModel(
         "GROUPS :: string[]",
         "#(authorize) not (org_id in $GROUPS)",
      ),
      cause: "compound_boolean",
   },
   {
      model: gateModel(
         "GROUPS :: string[]\nREGION :: string",
         "#(access_filter) org_id in $GROUPS or region = $REGION",
      ),
      cause: "compound_boolean",
   },
   {
      model: gateModel(
         "ROLE :: string",
         "#(authorize) 'analyst' = $ROLE or 'admin' = $ROLE",
      ),
      cause: "compound_boolean",
   },
   {
      model: gateModel(
         "ROLE :: string\nREGION :: string",
         "#(authorize) ('admin' = $ROLE) or (region = $REGION)",
      ),
      cause: "compound_boolean",
   },
   {
      model: gateModel("UNUSED :: string", "#(authorize) region is not null"),
      cause: "compound_boolean",
   },
   // operators other than `=` / `in`
   {
      model: gateModel(
         "REGION :: string",
         "#(access_filter) region != $REGION",
      ),
      cause: "negated_operator",
   },
   {
      model: gateModel(
         "AMOUNTMIN :: number",
         "#(access_filter) amount > $AMOUNTMIN",
      ),
      cause: "comparison_operator",
   },
   {
      model: gateModel(
         "AMOUNTMIN :: number",
         "#(authorize) amount + 1 > $AMOUNTMIN",
      ),
      cause: "comparison_operator",
   },
   {
      model: gateModel("PAT :: string", "#(authorize) region like $PAT"),
      cause: "malformed_body",
   },
   {
      model: gateModel(
         "GROUPS :: number[]",
         "#(access_filter) org_id ? $GROUPS",
      ),
      cause: "malformed_body",
   },
   // arity: `=` is scalar, `in` is list
   {
      model: gateModel(
         "GROUPS :: string[]",
         "#(access_filter) org_id = $GROUPS",
      ),
      cause: "operator_arity_mismatch",
   },
   {
      model: gateModel("ROLE :: string", "#(access_filter) owner in $ROLE"),
      cause: "operator_arity_mismatch",
   },
   // the left side must be a column or dotted join path
   {
      model: gateModel(
         "REGION :: string",
         "#(authorize) upper(region) = $REGION",
      ),
      cause: "left_not_field_path",
   },
   {
      model: gateModel(
         "REGION :: string",
         "#(access_filter) upper(region) = $REGION",
      ),
      cause: "left_not_field_path",
   },
   // constants: the retired allow-everyone / deny-everyone idioms
   { model: gateModel("", "#(authorize) 1 = 1"), cause: "left_not_field_path" },
   {
      model: gateModel("GROUPS :: number[]", "#(authorize) 1 = 0"),
      cause: "left_not_field_path",
   },
   {
      model: gateModel(
         "GROUPS :: string[]",
         "#(authorize) true and org_id in $GROUPS",
      ),
      cause: "malformed_body",
   },
   // a bare field reference hides its given inside the field's own expression
   {
      model: gateModel(
         "GROUPS :: number[]",
         "#(authorize) authorized",
         `source: gated_by_field_ref is duckdb.table('orgtable') extend {
   dimension: authorized is org_id in $GROUPS
}`,
      ),
      cause: "malformed_body",
   },
   { model: gateModel("", "#(authorize) notquoted"), cause: "malformed_body" },
   // field-less gates: nothing for a row filter to read
   ...(
      [
         ["'a' = 'a'", "missing_given_reference"],
         ["$ROLE like 'ana%'", "malformed_body"],
         ["$ROLE is not null", "compound_boolean"],
         ["$ROLE = 'a' and 1 = 1", "left_not_field_path"],
         ["not false", "compound_boolean"],
         ["$ROLE = $ROLE_D", "left_not_field_path"],
         ["$ROLE_D != 'blocked'", "negated_operator"],
      ] as const
   ).map(([gate, cause]) => ({
      model: gateModel(
         "ROLE :: string\nROLE_D :: string is 'x'",
         `#(authorize) ${gate}`,
      ),
      cause,
   })),
   // route scope: the lock reads literals, the filter reads columns
   {
      model: gateModel("REGION :: string", "#(authorize) region = $REGION"),
      cause: "row_level_term_in_authorize",
   },
   {
      model: gateModel(
         "GROUPS :: string[]",
         "#(access_filter) 'finance' in $GROUPS",
      ),
      cause: "source_level_term_in_access_filter",
   },
   {
      model: gateModel("", "#(access_filter) false"),
      cause: "sentinel_in_access_filter",
   },
   {
      model: gateModel("", "#(access_filter) true"),
      cause: "sentinel_in_access_filter",
   },
   // cross-note coherence: one source's notes are one set of terms
   {
      model: gateModel(
         "G :: string",
         "#(access_filter) region = $G\n#(access_filter) owner = $G",
      ),
      cause: "duplicate_given",
   },
   {
      model: gateModel(
         "A :: string\nB :: string",
         "#(access_filter) region = $A\n#(access_filter) region = $B",
      ),
      cause: "duplicate_field_path",
   },
   {
      model: gateModel(
         "REGION :: string",
         "#(authorize) false\n#(access_filter) region = $REGION",
      ),
      cause: "deny_all_with_sibling",
   },
   {
      model: gateModel(
         "GROUPS :: string[]",
         "#(authorize) true\n#(authorize) 'finance' in $GROUPS",
      ),
      cause: "admit_all_with_sibling",
   },
];

describe("the authorize grammar refuses at load", () => {
   it.each(GRAMMAR_REFUSALS.map((r) => [r.model.split("\n\n").at(-1), r]))(
      "%s",
      async (_label, { model, cause }) => {
         const err = await expectLoadError(model);
         expect(err).toBeInstanceOf(AuthorizeGrammarError);
         expect((err as AuthorizeGrammarError).rejectionCause).toBe(cause);
      },
   );

   it("refuses an `or` before G4's defaulted-given check ever runs, naming the source", async () => {
      const err = await expectLoadError(
         gateModel(
            "ROLE :: string is ''\nGROUPS :: number[]",
            "#(access_filter) org_id in $GROUPS or $ROLE != 'admin'",
         ),
      );
      expect((err as AuthorizeGrammarError).rejectionCause).toBe(
         "compound_boolean",
      );
      expect(err.message).toMatch(/Source "X" declares/);
   });

   it("refuses a grammar-banned gate inherited through an intermediate source with an unrelated annotation", async () => {
      // `# bar_chart` stops Malloy's by-reference note copy, so only the
      // ancestor walk still sees the base's text. Validation must walk that
      // effective text too, or the banned gate loads and grafts unvalidated.
      const err = await expectLoadError(
         {
            "base.malloy": gateModel(
               "GROUPS :: number[]",
               "#(authorize) not (org_id in $GROUPS)",
               "source: deny_locked is duckdb.table('customers') extend {}",
            ),
            "mid.malloy": `import "base.malloy"\n\n# bar_chart\nsource: deny_ext is deny_locked extend {}\n`,
         },
         "mid.malloy",
      );
      expect((err as AuthorizeGrammarError).rejectionCause).toBe(
         "compound_boolean",
      );
   });
});

// ---------------------------------------------------------------------------
// G4: a gate given declared WITH a default lets a caller who supplies nothing
// be served rows off the author's default. Refused outright, whatever the
// operator; keyed on the default, not on the term.
// ---------------------------------------------------------------------------

describe("a gate reading a defaulted given is refused (G4)", () => {
   it.each([
      [
         "region = $REGION, REGION is 'east'",
         gateModel(
            "REGION :: string is 'east'",
            "#(access_filter) region = $REGION",
         ),
         /\$REGION.*declared with a default/,
      ],
      [
         "amount = $FLOOR, FLOOR is 0",
         gateModel("FLOOR :: number is 0", "#(access_filter) amount = $FLOOR"),
         /\$FLOOR.*declared with a default/,
      ],
      [
         "tenant = $EXCLUDED, EXCLUDED is ''",
         gateModel(
            "EXCLUDED :: string is ''",
            "#(access_filter) tenant = $EXCLUDED",
            "source: X is duckdb.table('parent') extend { dimension: tenant is val }",
         ),
         /\$EXCLUDED.*declared with a default/,
      ],
      [
         "the lock's 'admin' = $ROLE, on the declaring source only",
         gateModel(
            "ROLE :: string is ''",
            "#(authorize) 'admin' = $ROLE",
            "source: X is duckdb.table('parent') extend {}\n\nsource: Derived is X -> { group_by: org_id }",
         ),
         /\$ROLE.*declared with a default/,
      ],
   ])("%s", async (_label, model, message) => {
      const err = await expectLoadError(model);
      expect(err).not.toBeInstanceOf(AuthorizeGrammarError);
      expect(err.message).toMatch(message);
   });

   it("refuses a defaulted given two import hops out, where attribution cannot reach the declarer", async () => {
      // `passthru` is two hops from `gated_base`, and `m.malloy` re-declares
      // TENANT with a default. G4 runs at every entry point whose probe
      // compiles, not only where attribution can name the declarer, so this is
      // refused exactly like the one-hop shape (it used to load and admit org 1).
      const err = await expectLoadError(
         {
            "base.malloy": gateModel(
               "TENANT :: number",
               "#(access_filter) org_id = $TENANT",
               "source: gated_base is duckdb.table('orgtable') extend {}",
            ),
            "mid.malloy": `##! experimental.givens\nimport { gated_base } from "base.malloy"\n\nsource: mid_src is gated_base extend {}\n`,
            "m.malloy": `##! experimental.givens\nimport { mid_src } from "mid.malloy"\ngiven: TENANT :: number is 1\n\nsource: passthru is mid_src extend {}\n`,
         },
         "m.malloy",
      );
      expect(err.message).toMatch(/\$TENANT.*declared with a default/);
   });

   it("loads and filters the same term over a given with NO default (the anti-narrowing guard)", async () => {
      await withAuthorizeModel(
         gateModel(
            "MAXLVL :: number[]",
            "#(access_filter) clearance in $MAXLVL",
            "source: X is duckdb.table('parent') extend { dimension: clearance is org_id }",
         ),
         async ({ model, error }) => {
            expect(error).toBeUndefined();
            expect(await countFor(model, "X", { MAXLVL: [1] })).toBe(2);
            expect(await countFor(model, "X", { MAXLVL: [1, 2] })).toBe(4);
         },
      );
   });
});

// ---------------------------------------------------------------------------
// Other gate-content failures, and the shapes that must still load.
// ---------------------------------------------------------------------------

describe("gate content that is not a grammar question", () => {
   it("fails the load when a gate references an undeclared given", async () => {
      const err = await expectLoadError(
         gateModel("ROLE :: string", "#(authorize) 'x' = $NOPE"),
      );
      expect(err.message).toMatch(/NOPE.*not declared/i);
   });

   it("fails the load when a gate's column does not exist on the source", async () => {
      const err = await expectLoadError(
         `#(access_filter) some_field = 1\nsource: gated is duckdb.table('customers') extend {}\n`,
      );
      expect(err.message).toContain("some_field");
   });

   it("fails the load for the source whose column does not resolve, even when a neighbour declares the identical gate", async () => {
      // Each source's probe is validated against its OWN compiled
      // expression; `A`'s success cannot paper over `B`'s.
      const err = await expectLoadError(
         gateModel(
            "GROUPS :: number[]",
            "#(access_filter) org_id in $GROUPS",
            "source: A is duckdb.table('parent') extend {}\n\n#(access_filter) org_id in $GROUPS\nsource: B is duckdb.table('childtable') extend {}",
         ),
      );
      expect(err.message).toMatch(/org_id/);
   });

   // The declaring source's own broken gate aborts the load naming ITSELF,
   // even beside a derivation carrying the by-reference copy of its note
   // (which, if attribution picked it, would turn this into a warning).
   it.each([
      ["alone", ""],
      ["beside a plain extend {}", "source: child is gated_parent extend {}"],
      [
         "beside an except: derivation",
         "source: w_except is gated_parent extend { except: org_id }",
      ],
   ])(
      "the declaring source's own broken gate aborts the load, %s",
      async (_label, derivation) => {
         const err = await expectLoadError(
            gateModel(
               "GROUPS :: number[]",
               "#(access_filter) nonexistent_column in $GROUPS",
               `source: gated_parent is duckdb.table('orgtable') extend {}\n\n${derivation}`,
            ),
         );
         expect(err.message).toContain(
            'Invalid #(access_filter) annotation on source "gated_parent"',
         );
      },
   );

   it.each([
      [
         "a gate over a given with no value (the probe compiles, it does not run)",
         gateModel(
            "ROLE :: string",
            "#(authorize) 'analyst' = $ROLE",
            "source: gated is duckdb.table('customers') extend {}",
         ),
      ],
      [
         "a type-mismatched comparison (the warehouse decides it at request time)",
         gateModel("NUM :: number", "#(access_filter) region = $NUM"),
      ],
      [
         "the same given reused across the two routes (coherence is per route)",
         gateModel(
            "GROUPS :: string[]",
            "#(authorize) 'finance' in $GROUPS\n#(access_filter) org_id in $GROUPS",
         ),
      ],
      [
         "a query-source base gate and its composite member's gate on the same given (two groups, never flattened)",
         `##! experimental { givens composite_sources }

given:
  GROUPS :: string[]

#(access_filter) region in $GROUPS
source: member_a is duckdb.table('accounts') extend {}

source: member_b is duckdb.sql("select id, org_id from accounts") extend {}

#(access_filter) org_id in $GROUPS
source: combo is compose(member_a, member_b)

source: qs is combo -> { group_by: id, region }
`,
      ],
      [
         "a note that merely MENTIONS the tag in its body",
         `##(description) "see the #(authorize) tag docs"\n\nsource: plain is duckdb.table('customers')\n`,
      ],
      [
         "an unannotated join_one: of a gated source (Malloy's by-reference note copy is not misplaced)",
         gateModel(
            "ID :: number",
            "#(access_filter) id = $ID",
            `source: salaries is duckdb.table('parent') extend {}

source: headcount_by_dept is duckdb.table('childtable') extend {
   join_one: salaries on id = salaries.id
}`,
         ),
      ],
   ])("loads %s", async (_label, model) => {
      await expectLoadsClean(model);
   });

   it("reports the authored expression for a gate over a value-less given, and for number and list givens", async () => {
      await withAuthorizeModel(
         gateModel(
            "AGE :: number\nALLOWED :: string[]\nROLE :: string",
            "#(access_filter) age = $AGE and region in $ALLOWED\n#(authorize) 'analyst' = $ROLE",
            "source: gated is duckdb.table('customers') extend { dimension: age is id + 10 }",
         ),
         async ({ model, error }) => {
            expect(error).toBeUndefined();
            expect(model.getAccessFilter("gated")).toEqual([
               "age = $AGE and region in $ALLOWED",
            ]);
            expect(model.getAuthorize("gated")).toEqual(["'analyst' = $ROLE"]);
         },
      );
   });

   // A join whose gated source lies beyond this model's own contents (a
   // selective import of only the joiner, or two hops away) cannot be
   // resolved to a declaration here, so it is an inherited copy, not a
   // misplaced annotation.
   const GATED_SAL = `##! experimental.givens\n\n#(authorize) false\nsource: sal is duckdb.table('parent') extend {}\n`;
   const JOINER = `source: mid is duckdb.table('childtable') extend {\n   join_one: sal on id = sal.id\n}\n`;
   it.each([
      [
         "one hop, importing only the joiner",
         { "a.malloy": `${GATED_SAL}\n${JOINER}` },
         `import { mid } from "a.malloy"`,
      ],
      [
         "two hops, through a full import",
         {
            "a.malloy": GATED_SAL,
            "b.malloy": `##! experimental.givens\nimport "a.malloy"\n\n${JOINER}`,
         },
         `import "b.malloy"`,
      ],
   ])(
      "loads a cross-file join of a gated source, %s",
      async (_label, files, importLine) => {
         await expectLoadsClean(
            {
               ...files,
               "m.malloy": `##! experimental.givens\n${importLine}\n\nsource: top is mid extend {}\n`,
            },
            "m.malloy",
         );
      },
   );

   // Package.create compiles in the worker pool and hydrates with
   // Model.fromSerialized; the worker runs the same validateAuthorizeProbes,
   // and only this path proves it stays in lockstep with Model.create. The
   // worker has its own DuckDB, so the rows travel in the query text.
   it.each([
      [
         "a row-field gate",
         `#(access_filter) org_id in $GROUPS\nsource: X is duckdb.sql("select 1 as id, 1 as org_id") extend {}`,
      ],
      [
         "an unannotated join_one: of a gated source",
         `#(access_filter) id = $GROUPS
source: salaries is duckdb.sql("select 1 as id") extend {}

source: headcount_by_dept is duckdb.sql("select 1 as id") extend {
   join_one: salaries on id = salaries.id
}`,
      ],
   ])(
      "loads %s through the real worker-pool Package.create path",
      async (label, body) => {
         const given = label.startsWith("a row")
            ? "GROUPS :: number[]"
            : "GROUPS :: number";
         const { pkg, cleanup } = await loadTempPackage({
            "m.malloy": `##! experimental.givens\n\ngiven:\n  ${given}\n\n${body}\n`,
         });
         try {
            expect(pkg).toBeDefined();
         } finally {
            await cleanup();
         }
      },
   );
});

// ---------------------------------------------------------------------------
// Where an annotation sits. Only a `source:` line gates; anywhere else nothing
// would enforce it, so the load fails rather than the author reading a
// source as locked while it serves every row.
// ---------------------------------------------------------------------------

describe("a misplaced gate annotation fails the load", () => {
   const GATED_SALARIES = `#(authorize) false\nsource: salaries is duckdb.table('parent') extend {}\n`;
   it.each([
      [
         "on a dimension",
         gateModel(
            "ROLE :: string[]",
            "",
            "source: X is duckdb.table('accounts') extend {\n   #(authorize) 'finance' in $ROLE\n   dimension: d is org_id\n}",
         ),
         /never enforced.*#\(authorize\)/s,
      ],
      [
         "on a measure",
         gateModel(
            "GROUPS :: number[]",
            "",
            "source: X is duckdb.table('parent') extend {\n   #(authorize)\n   measure: n is count()\n}",
         ),
         /never enforced.*field "n" of source "X"/s,
      ],
      [
         "on an array-typed dimension (not a join, despite its IR)",
         `source: s is duckdb.sql("select 1 as id") extend {\n   #(authorize)\n   dimension: arr is [1, 2, 3]\n}\n`,
         /never enforced.*field "arr" of source "s"/s,
      ],
      [
         "on a record-typed dimension",
         `source: s is duckdb.sql("select 1 as id") extend {\n   #(authorize)\n   dimension: rec is {a is 1}\n}\n`,
         /never enforced.*field "rec" of source "s"/s,
      ],
      [
         "on a query: statement",
         gateModel(
            "ROLE :: string[]",
            "",
            "source: X is duckdb.table('accounts') extend {}\n\n#(authorize) 'finance' in $ROLE\nquery: q is X -> { select: id }",
         ),
         /never enforced.*#\(authorize\)/s,
      ],
      [
         "bare, on a query: statement",
         gateModel(
            "",
            "",
            "source: X is duckdb.table('parent') extend {}\n\n#(authorize)\nquery: q is X -> { aggregate: n is count() }",
         ),
         /query "q"/,
      ],
      [
         "in the retired string form, on a query: statement",
         gateModel(
            "GROUPS :: number[]",
            "",
            `source: X is duckdb.table('parent') extend {}\n\n#(authorize) "org_id in $GROUPS"\nquery: q is X -> { aggregate: n is count() }`,
         ),
         /query "q"/,
      ],
      [
         "on the join_one: line of an UNGATED source",
         `source: salaries is duckdb.table('parent') extend {}

source: headcount_by_dept is duckdb.table('childtable') extend {
   #(authorize)
   join_one: salaries on id = salaries.id
}
`,
         /field "salaries" of source "headcount_by_dept"/,
      ],
      [
         "on the join_one: line of a GATED source (Malloy replaces the joined struct's notes, so it is never the source's own)",
         `${GATED_SALARIES}
source: headcount is duckdb.table('childtable') extend {
   #(authorize)
   join_one: salaries on id = salaries.id
}
`,
         /field "salaries" of source "headcount"/,
      ],
      [
         "as a leftover retired #(partition) marker on the source line",
         gateModel(
            "ORG :: string",
            "#(partition) org_id = $ORG",
            "source: gated is duckdb.table('customers') extend {}",
         ),
         /retired.*partition|partition.*retired/s,
      ],
      [
         "as a leftover retired #(partition) marker below the source line",
         gateModel(
            "ORG :: string",
            "",
            "source: gated is duckdb.table('customers') extend {\n  #(partition) org_id = $ORG\n  measure: c is count()\n}",
         ),
         /retired/,
      ],
   ])("%s", async (_label, model, message) => {
      const err = await expectLoadError(model);
      expect(err.message).toMatch(message);
   });

   describe("file-level ##(authorize) is deprecated: refused, naming the remedy", () => {
      it("names the tag actually written and the source: line to move it to", async () => {
         const err = await expectLoadError(
            gateModel(
               "ROLE :: string",
               "##(authorize) \"$ROLE = 'admin'\"",
               "\nsource: plain is duckdb.table('customers')",
            ),
         );
         expect(err.message).toMatch(/at the file level \(`##\(authorize\)`\)/);
         expect(err.message).toMatch(/source:/);
      });

      it("names THAT route, not ##(access_filter), for an unquoted lock body", async () => {
         const err = await expectLoadError(
            gateModel(
               "ROLE :: string",
               "##(authorize) 'admin' = $ROLE",
               "\nsource: plain is duckdb.table('customers')",
            ),
         );
         expect(err.message).toMatch(/at the file level \(`##\(authorize\)`\)/);
         expect(err.message).not.toMatch(
            /at the file level \(`##\(access_filter\)`\)/,
         );
      });

      it("refuses one declared in an IMPORTED model too", async () => {
         // The model-annotation fold has to surface an imported file's
         // `##(authorize)` even when the importer declares no `##` of its own.
         const err = await expectLoadError(
            {
               "auth_base.malloy": gateModel(
                  "ROLE :: string",
                  "##(authorize) \"$ROLE = 'admin'\"",
                  "",
               ),
               "auth_importer.malloy": `import "auth_base.malloy"\n\nsource: inherited_gate is duckdb.table('customers') extend {}\n`,
            },
            "auth_importer.malloy",
         );
         expect(err.message).toMatch(/file level/i);
      });

      it.each(["##(authorize)", "##|(authorize)"])(
         "recognizes %s (Malloy routes it to authorize) and refuses it",
         async (tag) => {
            const closer = tag.startsWith("##|") ? "\n|##" : "";
            const err = await expectLoadError(
               `${tag} "false"${closer}\n\nsource: route_file_plain is duckdb.table('customers')\n`,
            );
            expect(err.message).toMatch(/file level/i);
         },
      );
   });
});

// ---------------------------------------------------------------------------
// The retired quoted-string form: refused with a paste-ready rewrite.
// ---------------------------------------------------------------------------

describe("the legacy quoted-string #(authorize) form", () => {
   it.each([
      [`"org_id = 999"`, "#(authorize) org_id = 999"],
      [`'org_id = 999'`, "#(authorize) org_id = 999"],
      // The payload's escapes are decoded into the rewrite.
      [`"region = \\"east\\""`, '#(authorize) region = "east"'],
   ])(
      "refuses #(authorize) %s, naming it as authored and emitting %s",
      async (payload, rewrite) => {
         const err = await expectLoadError(
            `#(authorize) ${payload}\nsource: locked is duckdb.table('customers') extend {}\n`,
         );
         expect(err.message).toContain("no longer accepted");
         // The rewrite LINE, not a substring the refused text also carries:
         // echoing the payload verbatim once emitted the input as its remedy.
         expect(err.message).toContain(rewrite);
         expect(err.message).toContain(`replace \`#(authorize) ${payload}\``);
      },
   );

   it("refuses it two import hops out too, naming each source that inherits it (detection is presence-based, not attribution-limited)", async () => {
      const err = await expectLoadError(
         {
            "base.malloy": `##! experimental.givens\n\n#(authorize) "org_id = 999"\nsource: gated_base is duckdb.table('orgtable') extend {}\n`,
            "mid.malloy": `##! experimental.givens\nimport { gated_base } from "base.malloy"\n\nsource: mid_src is gated_base extend {}\n`,
            "m.malloy": `##! experimental.givens\nimport { mid_src } from "mid.malloy"\n\nsource: m_src is mid_src extend {}\n`,
         },
         "m.malloy",
      );
      expect(err.message).toContain("no longer accepted");
      expect(err.message).toContain('"mid_src"');
      expect(err.message).toContain('"m_src"');
   });

   it(`loads '"admin" = $ROLE' — a current-form gate that merely starts with a string literal — and enforces it`, async () => {
      await withAuthorizeModel(
         gateModel(
            "ROLE :: string",
            `#(authorize) "admin" = $ROLE`,
            "source: gated is duckdb.table('customers') extend {}",
         ),
         async ({ model, error }) => {
            expect(error).toBeUndefined();
            expect(model.getAuthorize("gated")).toEqual([`"admin" = $ROLE`]);
            expect(await countFor(model, "gated", { ROLE: "admin" })).toBe(2);
            await expectAccessDenied(
               countFor(model, "gated", { ROLE: "someone_else" }),
            );
         },
      );
   });
});

// ---------------------------------------------------------------------------
// Which spellings are gates is Malloy's routing decision. A spelling that
// reads as an attempt at `#(authorize)` but that Malloy routes elsewhere is
// refused, never loaded inert (fail-open) nor enforced (claiming a namespace
// that is not ours).
// ---------------------------------------------------------------------------

describe("a near miss of the lock's spelling fails the load", () => {
   const NEAR_MISSES = [
      "# (authorize)",
      "## (authorize)",
      "#( authorize )",
      "#(authorize )",
      "#(authorize)X",
      "#authorize",
      "#(AUTHORIZE)",
      "#(Authorize)",
      "#(sourceauthorize)",
      "#(authorize-source)",
      "#(SOURCE_AUTHORIZE)",
   ];

   it.each(NEAR_MISSES)("%s", async (tag) => {
      const err = await expectLoadError(
         `##! experimental.givens\n\ngiven:\n  ROLE :: string[]\n\n${tag} 'finance' in $ROLE\nsource: near_locked is duckdb.table('customers') extend {}\n`,
      );
      // The near-miss refusal itself, not some other load failure.
      expect(err).not.toBeInstanceOf(AuthorizeGrammarError);
      expect(err.message).toMatch(/never enforced/i);
      expect(err.message).toContain(tag);
      expect(err.message).toContain("(meant `#(authorize)`?)");
   });

   it("refuses a near miss reached only through a derivation hop", async () => {
      // `near_layer` imports `near_qs` alone, so the misspelled base is only
      // the inline struct on `near_qs`'s query.structRef; the sweep must
      // follow derivation hops to see it.
      const err = await expectLoadError(
         {
            "near_base.malloy": `# (authorize) "false"\nsource: near_base_gated is duckdb.table('customers') extend {}\n`,
            "near_mid.malloy": `import { near_base_gated } from "near_base.malloy"\n\nsource: near_qs is near_base_gated -> { select: * }\n`,
            "near_layer.malloy": `import { near_qs } from "near_mid.malloy"\n`,
         },
         "near_layer.malloy",
      );
      expect(err.message).toContain("# (authorize)");
      expect(err.message).toContain("(meant `#(authorize)`?)");
   });

   it("leaves annotations on other routes alone, including the legitimately different #(authorized)", async () => {
      await withAuthorizeModel(
         `##(description) "see the #(authorize) tag"

#(authorized) "not a gate"
# bar_chart
#(doc) "a doc note"
source: other_routes is duckdb.table('customers') extend {}
`,
         async ({ model, error }) => {
            expect(error).toBeUndefined();
            expect(model.getAccessFilter("other_routes")).toEqual([]);
            expect(model.getAuthorize("other_routes")).toEqual([]);
         },
      );
   });
});

// ---------------------------------------------------------------------------
// What the server SAYS about a source's gates (introspection) must agree with
// what it enforces.
// ---------------------------------------------------------------------------

describe("gate introspection", () => {
   const sourceNamed = (
      model: { getSources(): Array<{ name?: string }> | undefined },
      name: string,
   ) =>
      model.getSources()?.find((s) => s.name === name) as
         | { accessFilter?: string[]; authorize?: string[] }
         | undefined;

   it("reports each route's text under its own field", async () => {
      await withAuthorizeModel(
         gateModel(
            "ROLE :: string[]\nDENY :: number[]",
            "#(authorize) 'finance' in $ROLE",
            "source: fin is duckdb.table('customers') extend {}\n\nsource: open_source is duckdb.table('customers')",
         ),
         async ({ model }) => {
            expect(model.getAccessFilter("fin")).toEqual([]);
            expect(model.getAuthorize("fin")).toEqual(["'finance' in $ROLE"]);
            expect(sourceNamed(model, "fin")?.accessFilter).toBeUndefined();
            expect(sourceNamed(model, "fin")?.authorize).toEqual([
               "'finance' in $ROLE",
            ]);
            // No annotation at all is unrestricted, on both surfaces.
            expect(model.getAccessFilter("open_source")).toEqual([]);
            expect(
               sourceNamed(model, "open_source")?.accessFilter,
            ).toBeUndefined();
         },
      );
   });

   it("splits both routes for a query-source derivation, where extraction and the entry-point walk historically diverged", async () => {
      await withAuthorizeModel(
         gateModel(
            "DENY :: number[]\nROLE :: string[]",
            "#(access_filter) id in $DENY\n#(authorize) 'finance' in $ROLE",
            "source: base is duckdb.table('customers') extend {}\n\nsource: derived is base -> { select: id, region }",
         ),
         async ({ model }) => {
            expect(model.getAccessFilter("derived")).toEqual(["id in $DENY"]);
            expect(model.getAuthorize("derived")).toEqual([
               "'finance' in $ROLE",
            ]);
         },
      );
   });

   it("reports an extension's own gate in place of the base's, not beside it", async () => {
      await withAuthorizeModel(
         gateModel(
            "ALLOW :: number[]\nDENY :: number[]",
            "#(access_filter) id in $DENY",
            "source: customers_raw is duckdb.table('customers') extend {}\n\n#(access_filter) id in $ALLOW\nsource: customers_marketing is customers_raw extend {}",
         ),
         async ({ model }) => {
            expect(model.getAccessFilter("customers_raw")).toEqual([
               "id in $DENY",
            ]);
            expect(model.getAccessFilter("customers_marketing")).toEqual([
               "id in $ALLOW",
            ]);
         },
      );
   });
});

// ---------------------------------------------------------------------------
// A derivation that cannot express the gate it inherits (its own field space
// dropped the column) warns and denies at THAT entry point; it does not abort
// the load. Note-ownership attribution is what tells "inherited and
// unexpressible" from "declared here and broken".
// ---------------------------------------------------------------------------

describe("an inheriting derivation's unexpressible gate scopes to that entry point", () => {
   const UNEXPRESSIBLE = "not expressible at this entry point";
   let warnSpy: ReturnType<typeof spyOn<typeof logger, "warn">>;
   beforeAll(() => {
      warnSpy = spyOn(logger, "warn");
   });
   afterAll(() => warnSpy.mockRestore());

   const GATED_PARENT = gateModel(
      "GROUPS :: number[]",
      "#(access_filter) org_id in $GROUPS",
      "source: gated_parent is duckdb.table('orgtable') extend {}",
   );

   it.each([
      [
         "except:-ing the gated column",
         ["w_except"],
         "extend { except: org_id }",
      ],
      [
         "an accept: list omitting it",
         ["w_accept"],
         "extend { accept: id, val }",
      ],
      [
         "two siblings each except:-ing it",
         ["w_a", "w_b"],
         "extend { except: org_id }",
      ],
   ])(
      "%s: loads, warns naming each derivation, denies there, and leaves the declaring source serving",
      async (_label, names, body) => {
         warnSpy.mockClear();
         const text = `${GATED_PARENT}\n${names.map((n) => `source: ${n} is gated_parent ${body}`).join("\n")}\n`;
         await withAuthorizeModel(text, async ({ model, error }) => {
            expect(error).toBeUndefined();
            const warned = warnedSources(warnSpy.mock.calls, UNEXPRESSIBLE);
            for (const name of names) {
               expect(warned).toContain(name);
               await expectAccessDenied(countFor(model, name, { GROUPS: [1] }));
            }
            expect(warned).not.toContain("gated_parent");
            expect(await countFor(model, "gated_parent", { GROUPS: [1] })).toBe(
               2,
            );
         });
      },
   );

   it("a query-source projecting the gated column away loads and denies", async () => {
      await withAuthorizeModel(
         `${GATED_PARENT}\nsource: Z2 is gated_parent -> { group_by: id, val; aggregate: n is count() }\n`,
         async ({ model, error }) => {
            expect(error).toBeUndefined();
            await expectAccessDenied(countFor(model, "Z2", { GROUPS: [1] }));
         },
      );
   });

   it("a plain extend {} derivation loads with no warning at all and filters", async () => {
      warnSpy.mockClear();
      await withAuthorizeModel(
         `${GATED_PARENT}\nsource: w_plain is gated_parent extend {}\n`,
         async ({ model, error }) => {
            expect(error).toBeUndefined();
            expect(warnedSources(warnSpy.mock.calls, UNEXPRESSIBLE)).toEqual(
               [],
            );
            expect(await countFor(model, "w_plain", { GROUPS: [1] })).toBe(2);
         },
      );
   });

   it("attributes across one import hop to the IMPORTED base, not to the derivation", async () => {
      // `derived` carries `gated_base`'s note by reference, but its location
      // is in m.malloy while the note's is base.malloy, so it is never a
      // candidate declarer: the cross-file comparison is doing real work.
      warnSpy.mockClear();
      await withAuthorizeModel(
         {
            "base.malloy": GATED_PARENT.replace("gated_parent", "gated_base"),
            "m.malloy": `##! experimental.givens\nimport { gated_base, GROUPS } from "base.malloy"\n\nsource: derived is gated_base extend { except: org_id }\n`,
         },
         async ({ model, error }) => {
            expect(error).toBeUndefined();
            const warned = warnedSources(warnSpy.mock.calls, "not expressible");
            expect(warned).toContain("derived");
            expect(warned).not.toContain("gated_base");
            await expectAccessDenied(
               countFor(model, "derived", { GROUPS: [1] }),
            );
            expect(await countFor(model, "gated_base", { GROUPS: [1] })).toBe(
               2,
            );
         },
         "m.malloy",
      );
   });

   it("still loads and denies two import hops out, where attribution cannot name the declarer", async () => {
      await withAuthorizeModel(
         {
            "base.malloy": GATED_PARENT.replace("gated_parent", "gated_base"),
            "mid.malloy": `##! experimental.givens\nimport { gated_base, GROUPS } from "base.malloy"\n\nsource: mid_src is gated_base extend {}\n`,
            "m.malloy": `##! experimental.givens\nimport { mid_src, GROUPS } from "mid.malloy"\n\nsource: m_src is mid_src extend { except: org_id }\n`,
         },
         async ({ model, error }) => {
            expect(error).toBeUndefined();
            await expectAccessDenied(countFor(model, "m_src", { GROUPS: [1] }));
         },
         "m.malloy",
      );
   });

   it("an ordinary gate draws no row-level gate warning (W1/W2 shapes are refused by the grammar instead)", async () => {
      warnSpy.mockClear();
      await withAuthorizeModel(GATED_PARENT, async ({ error }) => {
         expect(error).toBeUndefined();
         expect(
            warnSpy.mock.calls.filter(
               (c) =>
                  String(c[0]) === "Row-level #(access_filter) gate warning",
            ),
         ).toEqual([]);
      });
   });
});
