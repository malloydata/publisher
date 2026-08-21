// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { Connection, GivenValue } from "@malloydata/malloy";
import {
   afterAll,
   afterEach,
   beforeAll,
   beforeEach,
   describe,
   expect,
   it,
} from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import {
   BYPASS_AUTHORIZE_HEADER,
   readBypassAuthorize,
} from "../authorize_bypass_header";
import { resetAuthorizeGuardTelemetryForTesting } from "../authorize_metrics";
import {
   AccessDeniedError,
   BadRequestError,
   ModelCompilationError,
} from "../errors";
import { logger } from "../logger";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "../test_helpers/metrics_harness";
import { Model } from "./model";

// Introspection, compile-time validation, and the runtime gate for
// #(authorize) / ##(authorize).

const TEST_DIR = path.join(os.tmpdir(), "authorize-integration-tests");
const TEST_DB_DIR = path.join(TEST_DIR, "db");
const TEST_DB_PATH = path.join(TEST_DB_DIR, "test.duckdb");
const TEST_PKG_DIR = path.join(TEST_DIR, "pkg");

let duckdbConnection: DuckDBConnection;

const SEED_SQL = `
CREATE TABLE IF NOT EXISTS customers (
   id INTEGER,
   name VARCHAR,
   region VARCHAR
);
INSERT INTO customers VALUES (1, 'a', 'us-west'), (2, 'b', 'us-east');

CREATE TABLE IF NOT EXISTS nested_cols (
   id INTEGER,
   rec STRUCT(a INTEGER),
   arr INTEGER[],
   arr_rec STRUCT(b INTEGER)[]
);
INSERT INTO nested_cols VALUES (1, {'a': 1}, [1, 2, 3], [{'b': 2}]);

CREATE TABLE IF NOT EXISTS salaries (
   id INTEGER,
   department_id INTEGER,
   department VARCHAR,
   salary INTEGER
);
INSERT INTO salaries VALUES (1, 10, 'eng', 100), (2, 20, 'sales', 90);

CREATE TABLE IF NOT EXISTS departments (
   id INTEGER,
   name VARCHAR
);
INSERT INTO departments VALUES (10, 'eng'), (20, 'sales');
`;

function getConnections(): Map<string, Connection> {
   const map = new Map<string, Connection>();
   map.set("duckdb", duckdbConnection);
   return map;
}

async function writeModel(filename: string, content: string): Promise<void> {
   await fs.writeFile(path.join(TEST_PKG_DIR, filename), content, "utf-8");
}

function sourceNamed(model: Model, name: string) {
   return model.getSources()?.find((s) => s.name === name);
}

beforeAll(async () => {
   await fs.mkdir(TEST_DB_DIR, { recursive: true });
   await fs.mkdir(TEST_PKG_DIR, { recursive: true });
   duckdbConnection = new DuckDBConnection("duckdb", TEST_DB_PATH, TEST_DB_DIR);
   for (const stmt of SEED_SQL.trim().split(";").filter(Boolean)) {
      await duckdbConnection.runSQL(stmt.trim() + ";");
   }
});

afterAll(async () => {
   try {
      await duckdbConnection.close();
      await new Promise((resolve) => setTimeout(resolve, 100));
      await fs.rm(TEST_DIR, { recursive: true, force: true });
   } catch {
      // ignore cleanup errors
   }
});

describe("authorize annotation introspection", () => {
   it("does NOT inherit a base source's authorize through extend", async () => {
      await writeModel(
         "extend.malloy",
         `##! experimental.givens

given:
  ROLE :: string

// Locked base.
source: customers_raw is duckdb.table('customers') extend {
  #(authorize)
  internal dimension: authorized is false
}

// Extension with its own gate — must NOT pick up the base's "false". Same
// field name as the base, \`except:\`-ed first (Malloy refuses a bare
// redefinition): re-declaring it is the legal override (own replaces
// inherited); a differently-named second candidate would trip G1's
// at-most-one rule instead.
source: customers_marketing is customers_raw extend {
  except: authorized
  #(authorize)
  internal dimension: authorized is $ROLE = 'analyst'
  measure: customer_count is count()
}
`,
      );
      const model = await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "extend.malloy",
         getConnections(),
      );

      // Base keeps its own lock.
      expect(model.getAuthorize("customers_raw")).toEqual(["false"]);
      // Extension is governed ONLY by its own gate — the base "false" is gone.
      expect(model.getAuthorize("customers_marketing")).toEqual([
         "$ROLE = 'analyst'",
      ]);
   });

   it("refuses to load a file-level ##(authorize) annotation, naming the remedy", async () => {
      // File-level ##(authorize) is deprecated: it is now a load error rather
      // than a model-wide gate, so an author who wrote one must be told what
      // to do instead — never left with a package that silently loses the
      // protection they thought they had.
      await writeModel(
         "file_only.malloy",
         `##! experimental.givens

given:
  ROLE :: string

##(authorize) "$ROLE = 'admin'"

source: plain is duckdb.table('customers')
`,
      );
      const model = await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "file_only.malloy",
         getConnections(),
      );

      const err = model.getNotebookError();
      expect(err).toBeInstanceOf(ModelCompilationError);
      expect(err?.message).toMatch(/file level/i);
      expect(err?.message).toMatch(/source:/);
      expect(model.getSources()).toBeUndefined();
   });

   it("refuses a ##(authorize) declared in an IMPORTED model too, not just the local file", async () => {
      // Regression for the hand-rolled model-annotation fold (malloy 0.0.405+
      // removed ModelDef.annotation): the fold still has to surface an
      // imported file's `##(authorize)` to the importing file's refusal check
      // even when the importer declares no `##` of its own — otherwise a
      // stray file-level gate escapes detection just by living one import hop
      // away, which is the exact fail-open this refusal exists to close.
      await writeModel(
         "auth_base.malloy",
         `##! experimental.givens

given:
  ROLE :: string

##(authorize) "$ROLE = 'admin'"
`,
      );
      await writeModel(
         "auth_importer.malloy",
         `import "auth_base.malloy"

source: inherited_gate is duckdb.table('customers') extend {
  measure: c is count()
}
`,
      );
      const model = await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "auth_importer.malloy",
         getConnections(),
      );

      const err = model.getNotebookError();
      expect(err).toBeInstanceOf(ModelCompilationError);
      expect(err?.message).toMatch(/file level/i);
   });

   it("loads successfully when a note merely MENTIONS the tag rather than declaring it", async () => {
      // `containsAuthorizeAnnotationTag` anchors after trimming, matching
      // `parseAuthorizeAnnotation` — so a note whose body happens to quote
      // the tag (documenting it, not declaring it) is not mistaken for a
      // misplaced gate. An unanchored scan would find "#(authorize)" as a
      // substring of this `##(description)` note's body and refuse the
      // whole load over an annotation the author never wrote.
      await writeModel(
         "mentions_only.malloy",
         `##(description) "see the #(authorize) tag docs"

source: plain is duckdb.table('customers')
`,
      );
      const model = await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "mentions_only.malloy",
         getConnections(),
      );

      expect(model.getNotebookError()).toBeUndefined();
      expect(model.getSources()).toBeDefined();
   });

   it("fails model load on a malformed authorize annotation (no silent drop)", async () => {
      await writeModel(
         "malformed.malloy",
         `#(authorize) notquoted
source: broken is duckdb.table('customers')
`,
      );
      const model = await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "malformed.malloy",
         getConnections(),
      );

      // A malformed gate must surface as a compilation error, not vanish.
      const err = model.getNotebookError();
      expect(err).toBeDefined();
      expect(err?.message).toMatch(/quote/i);
      // No sources surfaced for a failed compile — the gate is not silently
      // reported as unrestricted.
      expect(model.getSources()).toBeUndefined();
   });

   it("treats a source with no authorize annotations as unrestricted", async () => {
      await writeModel(
         "none.malloy",
         `source: open_source is duckdb.table('customers')
`,
      );
      const model = await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "none.malloy",
         getConnections(),
      );

      expect(model.getAuthorize("open_source")).toEqual([]);
      expect(sourceNamed(model, "open_source")?.authorize).toBeUndefined();
   });
});

describe("authorize annotation compile-time validation", () => {
   it("loads a valid expression that references a value-less given", async () => {
      // The probe is compiled, not run, so a given with no default/value does
      // NOT cause a false failure (the original getSQL approach would have).
      await writeModel(
         "valid_valueless.malloy",
         `##! experimental.givens

given:
  ROLE :: string

source: gated is duckdb.table('customers') extend {
  #(authorize)
  internal dimension: authorized is $ROLE = 'analyst'
}
`,
      );
      const model = await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "valid_valueless.malloy",
         getConnections(),
      );

      expect(model.getNotebookError()).toBeUndefined();
      expect(model.getAuthorize("gated")).toEqual(["$ROLE = 'analyst'"]);
   });

   it("fails model load when an expression references an unknown given", async () => {
      await writeModel(
         "unknown_given.malloy",
         `##! experimental.givens

given:
  ROLE :: string

source: gated is duckdb.table('customers') extend {
  #(authorize)
  internal dimension: authorized is $NOPE = 'x'
}
`,
      );
      const model = await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "unknown_given.malloy",
         getConnections(),
      );

      const err = model.getNotebookError();
      expect(err).toBeDefined();
      // Surfaces Malloy's own compile error for the unresolvable given — the
      // dimension form's gate is an ordinary expression, so an unknown
      // reference fails to compile before any gate-specific validation runs.
      expect(err?.message).toMatch(/NOPE.*not declared/i);
   });

   it("fails model load when an expression references a source field", async () => {
      await writeModel(
         "field_ref.malloy",
         `source: gated is duckdb.table('customers') extend {
  #(authorize)
  internal dimension: authorized is some_field = 1
}
`,
      );
      const model = await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "field_ref.malloy",
         getConnections(),
      );

      const err = model.getNotebookError();
      expect(err).toBeDefined();
      expect(err?.message).toContain("some_field");
   });

   it("does not reject a type-mismatched comparison (not a Malloy compile error)", async () => {
      // Documents the boundary: `$ROLE = 5` is not a compile error; such a gate
      // simply evaluates per the warehouse at the runtime gate.
      await writeModel(
         "type_mismatch.malloy",
         `##! experimental.givens

given:
  ROLE :: string

source: gated is duckdb.table('customers') extend {
  #(authorize)
  internal dimension: authorized is $ROLE = 5
}
`,
      );
      const model = await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "type_mismatch.malloy",
         getConnections(),
      );

      expect(model.getNotebookError()).toBeUndefined();
   });

   it("validates expressions over number and list givens, not just strings", async () => {
      await writeModel(
         "given_types.malloy",
         `##! experimental.givens

given:
  AGE :: number
  TENANT :: string
  ALLOWED :: string[]

// A source may declare at most one gate dimension (G1), so both givens are
// exercised in one expression rather than the string form's two OR'd notes.
source: gated is duckdb.table('customers') extend {
  #(authorize)
  internal dimension: authorized is ($AGE > 18) and ($TENANT in $ALLOWED)
}
`,
      );
      const model = await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "given_types.malloy",
         getConnections(),
      );

      expect(model.getNotebookError()).toBeUndefined();
      expect(model.getAuthorize("gated")).toEqual([
         "($AGE > 18) and ($TENANT in $ALLOWED)",
      ]);
   });
});

// Helper: run an ad-hoc query through the full getQueryResults path (which is
// where the gate fires). Returns the result or throws AccessDeniedError.
// Module-scoped (not just inside "authorize runtime gate") so the
// BLOCKING-1/2/3 regression describes below can reuse it too.
async function runGated(
   modelFile: string,
   query: string,
   givens?: Record<string, GivenValue>,
   bypassFilters?: boolean,
   bypassAuthorize?: boolean,
) {
   const model = await Model.create(
      "test-pkg",
      TEST_PKG_DIR,
      modelFile,
      getConnections(),
   );
   return model.getQueryResults(
      undefined,
      undefined,
      query,
      undefined,
      bypassFilters,
      givens,
      undefined,
      undefined,
      "full",
      bypassAuthorize,
   );
}

/**
 * A gate's own VERDICT denial (a caller whose given does not satisfy the
 * gate) is no longer a 403 — see `authorize.ts`'s module doc: every gate is a
 * row filter now, so a denied caller's query still runs and returns zero
 * rows, the same as the source's own `where:` matching nothing. Asserts the
 * query resolves successfully with an empty result, rather than rejecting.
 * Package-level FGA denials and structural gate rejections (unreachable
 * given, unresolvable graft target, …) are UNCHANGED — those still throw
 * `AccessDeniedError` — this helper is only for the gate-verdict case.
 */
async function expectDeniedByFilter(
   modelFile: string,
   query: string,
   givens?: Record<string, GivenValue>,
   bypassFilters?: boolean,
): Promise<void> {
   const { compactResult } = await runGated(
      modelFile,
      query,
      givens,
      bypassFilters,
   );
   // An `aggregate: c` query (this file's convention) matches zero rows as a
   // single row whose count is 0 — not an empty result set.
   const rows = compactResult as unknown as { c: number }[];
   expect(rows).toEqual([{ c: 0 }]);
}

describe("authorize runtime gate", () => {
   const SINGLE_GATE = `##! experimental.givens

given:
  ROLE :: string

source: gated is duckdb.table('customers') extend {
  measure: c is count()
  #(authorize)
  internal dimension: authorized is $ROLE = 'analyst'
}
`;

   it("allows the query when a given satisfies the gate", async () => {
      await writeModel("rt_single.malloy", SINGLE_GATE);
      const { result } = await runGated(
         "rt_single.malloy",
         "run: gated -> { aggregate: c }",
         { ROLE: "analyst" },
      );
      expect(result.data).toBeDefined();
   });

   it("denies (zero rows) when no given satisfies the gate", async () => {
      await writeModel("rt_single.malloy", SINGLE_GATE);
      await expectDeniedByFilter(
         "rt_single.malloy",
         "run: gated -> { aggregate: c }",
         { ROLE: "intern" },
      );
   });

   it("denies when the referenced given has no value (fail closed)", async () => {
      // Every gate is a row filter now: the query recompiles with the filter
      // grafted on, and the filter's own given has no bound value — Malloy
      // fails the run itself (a `getPreparedQuery()` compile does not need a
      // bound value, but actually executing the query does), not a
      // `Model`-issued `AccessDeniedError`. Same "self-triggering" failure
      // mode `row_level_authorize.integration.spec.ts` already pins for a
      // genuinely row-level gate — a clean failure, just not this specific
      // error class.
      await writeModel("rt_single.malloy", SINGLE_GATE);
      await expect(
         runGated("rt_single.malloy", "run: gated -> { aggregate: c }", {}),
      ).rejects.toThrow();
   });

   it("still enforces the gate when bypassFilters is true (authorize is not a filter)", async () => {
      await writeModel("rt_single.malloy", SINGLE_GATE);
      await expectDeniedByFilter(
         "rt_single.malloy",
         "run: gated -> { aggregate: c }",
         { ROLE: "intern" },
         true,
      );
   });

   // Two disjuncts fold into ONE dimension now (G1: at most one gate
   // dimension per source) — the string form's "two OWN #(authorize) notes,
   // OR'd" idiom is expressed as a single `or` expression instead.
   //
   // The string form's OWN OR idiom additionally relied on BOTH givens
   // declaring a default (`is ''`), so a caller supplying only one still
   // compiled — the unsupplied given fell back to its default, which failed
   // its own disjunct, and the OR was decided by whichever given the caller
   // DID supply. G4 refuses that unconditionally now (a referenced given
   // with a declared default can silently admit rows the gate meant to
   // exclude) — see the dedicated G4 test below. So this OR-semantics
   // coverage supplies BOTH givens on every call; "one given omitted,
   // defaults to something that doesn't satisfy it" is not a reproducible
   // shape anymore, not merely untested here.
   const DISJUNCTION = `##! experimental.givens

given:
  ROLE :: string
  REGION :: string

source: regional is duckdb.table('customers') extend {
  measure: c is count()
  #(authorize)
  internal dimension: authorized is ($ROLE = 'admin') or ($REGION = 'us-west')
}
`;

   it("grants on the first disjunct", async () => {
      await writeModel("rt_disj.malloy", DISJUNCTION);
      const { compactResult } = await runGated(
         "rt_disj.malloy",
         "run: regional -> { aggregate: c }",
         { ROLE: "admin", REGION: "nowhere" },
      );
      // `customers` has 2 rows total; a broken `or` (e.g. evaluated as
      // `and`) would deny both, which `.toBeDefined()` cannot distinguish
      // from a genuine grant (both resolve to a defined `{c: 0}`/`{c: 2}`).
      const rows = compactResult as unknown as { c: number }[];
      expect(rows[0].c).toBe(2);
   });

   it("grants on the second disjunct", async () => {
      await writeModel("rt_disj.malloy", DISJUNCTION);
      const { compactResult } = await runGated(
         "rt_disj.malloy",
         "run: regional -> { aggregate: c }",
         { ROLE: "nobody", REGION: "us-west" },
      );
      const rows = compactResult as unknown as { c: number }[];
      expect(rows[0].c).toBe(2);
   });

   it("denies (zero rows) when neither disjunct is satisfied", async () => {
      await writeModel("rt_disj.malloy", DISJUNCTION);
      await expectDeniedByFilter(
         "rt_disj.malloy",
         "run: regional -> { aggregate: c }",
         { ROLE: "nobody", REGION: "nowhere" },
      );
   });

   it("G4 refuses the retired idiom: a disjunct's given declared with a default", async () => {
      // Pins the guarantee that does NOT survive from the string form: a
      // referenced given with a default is refused unconditionally, even
      // one whose default value fails its own disjunct — an unsupplied
      // given resolving to a default is exactly the shape G4 exists to
      // close, regardless of which disjunct the caller relies on instead.
      await writeModel(
         "rt_disj_default.malloy",
         `##! experimental.givens

given:
  ROLE :: string
  REGION :: string is ''

source: regional is duckdb.table('customers') extend {
  measure: c is count()
  #(authorize)
  internal dimension: authorized is ($ROLE = 'admin') or ($REGION = 'us-west')
}
`,
      );
      const model = await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "rt_disj_default.malloy",
         getConnections(),
      );
      expect(model.getNotebookError()?.message).toMatch(
         /declared with a default/,
      );
   });

   it("gates a named query that targets a gated source (no sourceName supplied)", async () => {
      await writeModel(
         "rt_namedq.malloy",
         `##! experimental.givens

given:
  ROLE :: string

source: gated is duckdb.table('customers') extend {
  measure: c is count()
  #(authorize)
  internal dimension: authorized is $ROLE = 'analyst'
}

query: secret is gated -> { aggregate: c }
`,
      );
      const model = await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "rt_namedq.malloy",
         getConnections(),
      );
      // Named query, no sourceName — must still resolve to `gated` and gate
      // it. A mismatched given is a filter-verdict denial now (zero rows),
      // not a 403 — see `expectDeniedByFilter`'s doc.
      const denied = await model.getQueryResults(
         undefined,
         "secret",
         undefined,
         undefined,
         false,
         {
            ROLE: "intern",
         },
      );
      expect(denied.compactResult).toEqual([{ c: 0 }]);
      // And it runs when the gate passes.
      const { result } = await model.getQueryResults(
         undefined,
         "secret",
         undefined,
         undefined,
         false,
         { ROLE: "analyst" },
      );
      expect(result.data).toBeDefined();
   });

   it("gates an ad-hoc query even when a blank sourceName is supplied", async () => {
      await writeModel("rt_single.malloy", SINGLE_GATE);
      const model = await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "rt_single.malloy",
         getConnections(),
      );
      // Blank sourceName must not skip the gate while the query-builder treats
      // it as absent and runs the ad-hoc query. A mismatched given is a
      // filter-verdict denial now (zero rows), not a 403.
      const denied = await model.getQueryResults(
         "",
         undefined,
         "run: gated -> { aggregate: c }",
         undefined,
         false,
         { ROLE: "intern" },
      );
      expect(denied.compactResult).toEqual([{ c: 0 }]);
   });

   it("gates the source the query actually RUNS, not a decoy leading statement", async () => {
      // Malloy runs the LAST `run:`. A multi-statement ad-hoc query that names
      // an ungated source first and the gated source last must be gated on the
      // gated source (the one that executes), not fooled by the leading one.
      await writeModel(
         "rt_multi.malloy",
         `##! experimental.givens

given:
  ROLE :: string

source: ungated is duckdb.table('customers') extend { measure: c is count() }

source: gated is duckdb.table('customers') extend {
  measure: c is count()
  #(authorize)
  internal dimension: authorized is $ROLE = 'analyst'
}
`,
      );
      await expectDeniedByFilter(
         "rt_multi.malloy",
         "run: ungated -> { aggregate: c }\nrun: gated -> { aggregate: c }",
         { ROLE: "intern" },
      );
   });

   it("leaves a source with no authorize annotations unrestricted", async () => {
      await writeModel(
         "rt_open.malloy",
         `source: open_src is duckdb.table('customers') extend { measure: c is count() }
`,
      );
      const { result } = await runGated(
         "rt_open.malloy",
         "run: open_src -> { aggregate: c }",
      );
      expect(result.data).toBeDefined();
   });

   it("rejects an ad-hoc inline-SQL query (restricted mode closes the raw-warehouse path)", async () => {
      // Restricted mode rejects inline raw SQL outright while resolving the
      // compiled source — before any `#(authorize)` gate is reached — so the
      // raw-warehouse path is closed at the compile layer regardless of
      // givens (even for a caller who satisfies a gate declared elsewhere in
      // the model).
      await writeModel("rt_single.malloy", SINGLE_GATE);
      await expect(
         runGated(
            "rt_single.malloy",
            `run: duckdb.sql("SELECT 1 AS id") -> { aggregate: c is count() }`,
            { ROLE: "analyst" },
         ),
      ).rejects.toThrow(/raw SQL is not permitted/);
   });

   const MIXED_SOURCE_GATE = `##! experimental.givens

given:
  ROLE :: string

source: gated is duckdb.table('customers') extend {
  measure: c is count()
  #(authorize)
  internal dimension: authorized is $ROLE = 'analyst'
}

source: open_src is duckdb.table('customers') extend { measure: c is count() }
`;

   it("does not over-gate: a source-level gate is not model-wide", async () => {
      // Control: a per-source gate applies only to that source, not the whole
      // model. An ad-hoc query against an ungated declared source in the same
      // model runs without any given.
      await writeModel("rt_mixed.malloy", MIXED_SOURCE_GATE);
      const { result } = await runGated(
         "rt_mixed.malloy",
         "run: open_src -> { aggregate: c }",
      );
      expect(result.data).toBeDefined();
   });

   it("recognizes a quoted-identifier source in the early gate (identifier resolution, not the schema-oracle guarantee)", async () => {
      // A gated source whose Malloy name must be quoted (here, a hyphen) must
      // still be recognized by the early gate — `extractRunTargetSourceName`
      // resolving `` `gated-source` `` correctly is what this pins.
      //
      // The no-schema-oracle guarantee this test used to assert (a denied
      // caller probing a non-existent field gets a clean 403, not a Malloy
      // field error) held only for the PRE-EXISTING given-only fast path,
      // which could evaluate a whole-source boolean synchronously with no
      // compile at all. Every gate is a row filter now — enforcement is
      // deferred until the caller's own query compiles and a graft can be
      // attempted — so a field that does not exist fails to compile before
      // the gate ever gets a chance to run, the same as it already did for a
      // genuinely row-level gate before this change (see
      // `row_level_authorize.integration.spec.ts`'s "self-triggering" case).
      // This is a real, accepted narrowing of that guarantee, not a bug: no
      // query ever executes either way, so no ROW data leaks — only the
      // fact that the field name is unrecognized, which is not gate-specific
      // information.
      await writeModel(
         "rt_quoted.malloy",
         `##! experimental.givens

given:
  ROLE :: string

source: \`gated-source\` is duckdb.table('customers') extend {
  measure: c is count()
  #(authorize)
  internal dimension: authorized is $ROLE = 'analyst'
}
`,
      );
      await expect(
         runGated(
            "rt_quoted.malloy",
            "run: `gated-source` -> { group_by: no_such_field }",
            { ROLE: "viewer" },
         ),
      ).rejects.toThrow(/no_such_field/);
   });

   it("gates a notebook cell that runs a NAMED QUERY targeting a gated source", async () => {
      // `run: secret` has no `->`, so source resolution must come from the
      // compiled query, not a text regex — otherwise the gate is bypassed.
      await writeModel(
         "rt_nb.malloynb",
         `>>>malloy
##! experimental.givens

given:
  ROLE :: string

source: gated is duckdb.table('customers') extend {
  measure: c is count()
  #(authorize)
  internal dimension: authorized is $ROLE = 'analyst'
}
query: secret is gated -> { aggregate: c }

>>>malloy
run: secret
`,
      );
      const model = await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "rt_nb.malloynb",
         getConnections(),
      );
      // Cell 1 is `run: secret` — a mismatched given is a filter-verdict
      // denial now (zero-count aggregate), not a rejected cell.
      const denied = await model.executeNotebookCell(1, undefined, false, {
         ROLE: "intern",
      });
      const deniedRow = JSON.parse(denied.result!)?.data?.array_value?.[0]
         ?.record_value?.[0];
      expect(deniedRow?.number_value).toBe(0);
      // ...and allowed when the gate passes.
      const ok = await model.executeNotebookCell(1, undefined, false, {
         ROLE: "analyst",
      });
      expect(ok.result).toBeDefined();
   });

   const LOCKED_BASE = `##! experimental.givens

given:
  ROLE :: string

source: base_locked is duckdb.table('customers') extend {
  measure: c is count()
  #(authorize)
  internal dimension: authorized is false
}

// Same field name as the base, \`except:\`-ed first (Malloy refuses a bare
// redefinition): re-declaring it is the legal override that makes the
// locked-base idiom work.
source: ext_gated is base_locked extend {
  except: authorized
  #(authorize)
  internal dimension: authorized is $ROLE = 'analyst'
}

source: ext_nogate is base_locked extend {}

source: joiner is duckdb.table('customers') extend {
  join_one: base_locked on id = base_locked.id
  measure: c is count()
}

source: joiner_aliased is duckdb.table('customers') extend {
  join_one: b is base_locked on id = b.id
  measure: c is count()
}

source: plain is duckdb.table('customers') extend { measure: c is count() }

source: mixed_joiner is duckdb.table('customers') extend {
  join_one: base_locked on id = base_locked.id
  join_one: plain on id = plain.id
  measure: c is count()
}

source: inline_joiner is duckdb.table('customers') extend {
  join_one: t is duckdb.table('customers') on id = t.id
  measure: c is count()
}

source: mid_join is duckdb.table('customers') extend {
  join_one: base_locked on id = base_locked.id
  measure: c is count()
}

source: top_join is duckdb.table('customers') extend {
  join_one: mid_join on id = mid_join.id
  measure: c is count()
}
`;

   it('denies (zero rows) a direct query against a base locked with #(authorize) "false"', async () => {
      // `false` is a constant row filter now — a bare literal is a legal
      // gate condition (see `gate_classification.ts`'s `resolveGateShape`),
      // so this admits the request into a query that matches zero rows (a
      // `count()` of 0), rather than throwing a 403.
      await writeModel("rt_locked.malloy", LOCKED_BASE);
      const { compactResult } = await runGated(
         "rt_locked.malloy",
         "run: base_locked -> { aggregate: c }",
         { ROLE: "analyst" },
      );
      const rows = compactResult as unknown as { c: number }[];
      expect(rows[0].c).toBe(0);
   });

   it("allows an extension of a locked base when the extension's own gate passes", async () => {
      await writeModel("rt_locked.malloy", LOCKED_BASE);
      const { result } = await runGated(
         "rt_locked.malloy",
         "run: ext_gated -> { aggregate: c }",
         { ROLE: "analyst" },
      );
      expect(result.data).toBeDefined();
   });

   it("denies (zero rows) an extension that declares no own gate — it inherits the base lock (safe default)", async () => {
      // Malloy carries the base's #(authorize) onto an extension UNLESS the
      // extension declares its own. So a bare `is base_locked extend {}` with
      // no own gate stays locked by the base's "false". An extension escapes
      // the base gate only by declaring its own #(authorize) (see ext_gated).
      await writeModel("rt_locked.malloy", LOCKED_BASE);
      const { compactResult } = await runGated(
         "rt_locked.malloy",
         "run: ext_nogate -> { aggregate: c }",
         { ROLE: "analyst" },
      );
      expect((compactResult as unknown as { c: number }[])[0].c).toBe(0);
   });

   // Q16: authorization is evaluated at the ENTRY POINT only. A gate is a
   // statement about who may query the source it is declared on, not about
   // everything reachable beneath that source, so joining a locked base into an
   // ungated source does NOT carry the base's gate along. These are the shapes
   // that used to deny and now do not — asserted positively, because the whole
   // point is that this is the intended contract and not an oversight. The
   // author-facing rule: joining sensitive data into an ungated source publishes
   // it; put the gate on the source callers enter through.
   describe("joins are not gated (Q16 — entry-point-only evaluation)", () => {
      const joinShapes: [string, string][] = [
         [
            "a direct join of the locked base",
            "run: joiner -> { aggregate: c }",
         ],
         ["an aliased join", "run: joiner_aliased -> { aggregate: c }"],
         [
            "one gated plus one ungated join",
            "run: mixed_joiner -> { aggregate: c }",
         ],
         ["a transitive A→B→C join", "run: top_join -> { aggregate: c }"],
         [
            "an annotated join_one of the locked base",
            `source: j is plain extend {
               # some_render_tag
               join_one: base_locked on id = base_locked.id
             }
             run: j -> { aggregate: c }`,
         ],
         [
            "an annotated join_many of the locked base",
            `source: j is plain extend {
               # some_render_tag
               join_many: base_locked on id = base_locked.id
             }
             run: j -> { aggregate: c }`,
         ],
         [
            "a query-local join inside the query's own pipeline extend",
            `run: plain -> {
               extend: {
                 join_one: base_locked on id = base_locked.id
               }
               aggregate: c
             }`,
         ],
      ];

      for (const [label, query] of joinShapes) {
         it(`allows ${label}`, async () => {
            await writeModel("rt_locked.malloy", LOCKED_BASE);
            const { compactResult } = await runGated(
               "rt_locked.malloy",
               query,
               {},
            );
            // `customers` has 2 rows; the entry point's own `count()` is
            // unaffected by a join to the locked `base_locked` (Q16), so a
            // wrongly-fired join gate (which would zero this out) is
            // distinguishable from a genuine allow — unlike `.toBeDefined()`.
            const rows = compactResult as unknown as { c: number }[];
            expect(rows[0].c).toBe(2);
         });
      }

      it("still denies (zero rows) the locked base when it IS the entry point", async () => {
         // The control that keeps the block above meaningful: the gate is real,
         // it just applies where the query enters.
         await writeModel("rt_locked.malloy", LOCKED_BASE);
         const { compactResult } = await runGated(
            "rt_locked.malloy",
            "run: base_locked -> { aggregate: c }",
            {
               ROLE: "analyst",
            },
         );
         expect((compactResult as unknown as { c: number }[])[0].c).toBe(0);
      });
   });

   // An extension escapes its base's gate by declaring its own #(authorize) —
   // that override is what makes the locked-base idiom work, and it is only safe
   // while the declaration is the model AUTHOR's. Query text is not the author's.
   //
   // Measured against @malloydata/* 0.0.427 by stubbing the guard out: FOUR of
   // the six shapes below then resolve and return every row of a base locked
   // with `#(authorize) "false"` — the first three and the whitespace variant.
   // The other two are already stopped by a DIFFERENT control, and the guard is
   // defense in depth for them rather than the only thing standing there:
   //   - "file-level annotation" denies on its own (`Access denied for source
   //     "mine"`) — a caller `##(authorize)` does not become the file-level gate.
   //   - "laundering through a join" is refused by restricted mode, which bans
   //     `duckdb.table(...)` in caller text before authorization is reached.
   // Both are kept as tests precisely so that if either of those controls is
   // ever relaxed, this guard is already the backstop and the shape does not
   // silently become exploitable. Do not restate them as live bypasses.
   describe("an authorize annotation in query text is rejected", () => {
      const attacks: [string, string][] = [
         [
            "extension declaring its own passing gate",
            `#(authorize) "true"
             source: mine is base_locked extend { measure: cc is count() }
             run: mine -> { aggregate: cc }`,
         ],
         [
            "bare rename declaring its own passing gate",
            `#(authorize) "true"
             source: mine is base_locked
             run: mine -> { aggregate: c }`,
         ],
         [
            "extension projecting row values, not just an aggregate",
            `#(authorize) "true"
             source: mine is base_locked extend {}
             run: mine -> { group_by: id }`,
         ],
         [
            "file-level annotation",
            `##(authorize) "true"
             source: mine is base_locked extend {}
             run: mine -> { aggregate: c }`,
         ],
         [
            "whitespace inside the parens",
            `#( authorize ) "true"
             source: mine is base_locked extend {}
             run: mine -> { aggregate: c }`,
         ],
         [
            "laundering through a join of a self-gated extension",
            `#(authorize) "true"
             source: mine is base_locked extend {}
             source: j is duckdb.table('customers') extend {
               join_one: mine on id = mine.id
               measure: cc is count()
             }
             run: j -> { aggregate: cc }`,
         ],
      ];

      for (const [label, query] of attacks) {
         it(`rejects: ${label}`, async () => {
            await writeModel("rt_locked.malloy", LOCKED_BASE);
            // Rejected, and specifically NOT resolved as a granted query.
            await expect(
               runGated("rt_locked.malloy", query, { ROLE: "nobody" }),
            ).rejects.toThrow(/authorize` annotation is not permitted/);
         });
      }
   });

   // The other half of the same hole, which the text guard above cannot see:
   // Malloy moves a base's annotations off the deriving struct as soon as that
   // statement carries ANY annotation of its own, authorize or not — demoted to
   // `annotations.inherits` for a `source:` (define-source.ts), REPLACED with no
   // inherits at all for a `join_*` (join.ts). A render tag or a doc comment
   // therefore used to launder the gate away with no "authorize" byte anywhere
   // in the request. Every shape below returned every row of `base_locked`
   // before `Model.ancestorGateExprs`; each is a real bypass, not defense in
   // depth, so do not relax them to "rejected somehow".
   describe("an unrelated caller annotation cannot launder the base's gate", () => {
      const launderings: [string, string][] = [
         [
            "block annotation on a caller extension",
            `# some_render_tag
             source: mine is base_locked extend { measure: cc is count() }
             run: mine -> { aggregate: cc }`,
         ],
         [
            "doc-comment annotation on a caller extension",
            `#" just a doc comment
             source: mine is base_locked extend { measure: cc is count() }
             run: mine -> { aggregate: cc }`,
         ],
         [
            "annotation whose route merely looks like authorize",
            `#(authorized) "true"
             source: mine is base_locked extend {}
             run: mine -> { aggregate: c }`,
         ],
      ];

      for (const [label, query] of launderings) {
         it(`denies: ${label}`, async () => {
            await writeModel("rt_locked.malloy", LOCKED_BASE);
            await expect(
               runGated("rt_locked.malloy", query, { ROLE: "analyst" }),
            ).rejects.toBeInstanceOf(AccessDeniedError);
         });
      }

      it("denies (zero rows): block annotation on a caller bare rename", async () => {
         // Unlike the three shapes above, `source: mine is base_locked` (a
         // BARE rename with no `extend` at all) resolves a graft target for
         // `mine` — `resolveGraftTarget`'s `sourceRegistry`/annotation-identity
         // fallback links it back to `base_locked` — so the inherited "false"
         // gate is enforced as a row filter (zero rows), not a rejected
         // classification.
         await writeModel("rt_locked.malloy", LOCKED_BASE);
         const { compactResult } = await runGated(
            "rt_locked.malloy",
            `# some_render_tag
             source: mine is base_locked
             run: mine -> { aggregate: c }`,
            { ROLE: "analyst" },
         );
         expect((compactResult as unknown as { c: number }[])[0].c).toBe(0);
      });
   });

   it("still allows a render tag in ordinary query text", async () => {
      // The gate resolution above must not turn an annotation anywhere in caller
      // text into a refusal — only into "read the ancestor's gate".
      await writeModel("rt_locked.malloy", LOCKED_BASE);
      const { result } = await runGated(
         "rt_locked.malloy",
         `run: plain -> {
            # bar_chart
            aggregate: c
          }`,
         {},
      );
      expect(result.data).toBeDefined();
   });

   it("keeps the base's gate on an AUTHOR extension that carries only a render tag", async () => {
      // The same demotion reachable without any caller at all: an author who
      // decorates an extension of a gated base loses the gate unless the walk
      // reads the ancestor. Only an own #(authorize) may replace it.
      await writeModel(
         "rt_tagged_ext.malloy",
         `##! experimental.givens

given:
  ROLE :: string

source: base_locked is duckdb.table('customers') extend {
  measure: c is count()
  #(authorize)
  internal dimension: authorized is false
}

# bar_chart
source: ext_tagged is base_locked extend {}
`,
      );
      const { compactResult } = await runGated(
         "rt_tagged_ext.malloy",
         "run: ext_tagged -> { aggregate: c }",
         {
            ROLE: "analyst",
         },
      );
      expect((compactResult as unknown as { c: number }[])[0].c).toBe(0);
   });

   it("still allows an author-declared curated extension of a locked base", async () => {
      // The regression guard for the rejection above: the locked-base idiom is a
      // documented pattern and the fix must not touch it. `ext_gated` declares
      // its own gate in the PACKAGE, which stays authoritative.
      await writeModel("rt_locked.malloy", LOCKED_BASE);
      const { result } = await runGated(
         "rt_locked.malloy",
         "run: ext_gated -> { aggregate: c }",
         { ROLE: "analyst" },
      );
      expect(result.data).toBeDefined();
   });

   it("still allows ordinary query text that declares a source with no gate", async () => {
      // The rejection is scoped to the annotation, not to declaring sources.
      await writeModel("rt_locked.malloy", LOCKED_BASE);
      const { result } = await runGated(
         "rt_locked.malloy",
         `source: mine is plain extend { measure: cc is count() }
          run: mine -> { aggregate: cc }`,
         {},
      );
      expect(result.data).toBeDefined();
   });

   // Q21's third door, pinned deliberately. The guard covers REQUEST text; a
   // notebook cell is PACKAGE content and is intentionally left unguarded (it
   // also compiles unrestricted, via `loadQuery` rather than
   // `loadRestrictedQuery`). The asymmetry is structural, not an oversight:
   // `executeNotebookCell` takes a cell INDEX and no query text, so a caller
   // cannot author a cell — only choose one the package author already wrote.
   // The bytes below are byte-for-byte the first attack shape in the describe
   // above; through the notebook channel they are the author's own curated
   // extension and must still run. Do not "fix" this by extending the guard to
   // notebook cells: that would break authors declaring gates in notebooks.
   it("does NOT reject an authorize annotation in a NOTEBOOK cell (author content, not request text)", async () => {
      await writeModel(
         "rt_nb_authorize.malloynb",
         `>>>malloy
${LOCKED_BASE}
>>>malloy
source: mine is base_locked extend {
  except: authorized
  #(authorize)
  internal dimension: authorized is $ROLE = 'analyst'
  measure: cc is count()
}
run: mine -> { aggregate: cc }
`,
      );
      const model = await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "rt_nb_authorize.malloynb",
         getConnections(),
      );
      // Not rejected as forged caller policy — the author's gate is honored, so
      // it grants on the satisfying given...
      const ok = await model.executeNotebookCell(1, undefined, false, {
         ROLE: "analyst",
      });
      expect(ok.result).toBeDefined();
      // ...and still DENIES (zero rows) on a non-satisfying one. The cell's
      // gate is really enforced, not merely tolerated — this is a gate, not
      // a hole.
      const denied = await model.executeNotebookCell(1, undefined, false, {
         ROLE: "nobody",
      });
      const deniedRow = JSON.parse(denied.result!)?.data?.array_value?.[0]
         ?.record_value?.[0];
      expect(deniedRow?.number_value).toBe(0);
   });

   it("allows a query joining only an inline duckdb.table(...) source (no annotations to gate)", async () => {
      await writeModel("rt_locked.malloy", LOCKED_BASE);
      const { result } = await runGated(
         "rt_locked.malloy",
         "run: inline_joiner -> { aggregate: c }",
         {},
      );
      expect(result.data).toBeDefined();
   });

   it("does not gate a notebook cell whose run target merely JOINS a locked base", async () => {
      // The notebook path uses the same entry-point rule as every other path:
      // `secret_join`'s entry point is `joiner`, which is ungated.
      await writeModel(
         "rt_locked_join.malloynb",
         `>>>malloy
${LOCKED_BASE}
query: secret_join is joiner -> { aggregate: c }

>>>malloy
run: secret_join
`,
      );
      const model = await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "rt_locked_join.malloynb",
         getConnections(),
      );
      const ok = await model.executeNotebookCell(1, undefined, false, {});
      expect(ok.result).toBeDefined();
   });
});

// A locked base reached only through a two-hop CROSS-FILE join. Under
// entry-point-only evaluation the file boundary changes nothing: the entry point
// is `top`, which declares no gate, so nothing is enforced. Kept as an explicit
// case because the cross-file shape is the one most likely to be assumed
// "surely that still denies".
describe("cross-file joins are not gated either (Q16)", () => {
   it("allows a query whose run target reaches a locked source only via a two-hop cross-file join", async () => {
      await writeModel(
         "c_base.malloy",
         `#(authorize) "false"
source: base_locked is duckdb.table('customers') extend { measure: c is count() }
`,
      );
      await writeModel(
         "c_mid.malloy",
         `import "c_base.malloy"

source: c_mid is duckdb.table('customers') extend {
  join_one: base_locked on id = base_locked.id
  measure: c is count()
}
`,
      );
      await writeModel(
         "c_entry.malloy",
         `import "c_mid.malloy"

source: top is duckdb.table('customers') extend {
  join_one: c_mid on id = c_mid.id
  measure: c is count()
}
`,
      );
      const { result } = await runGated(
         "c_entry.malloy",
         "run: top -> { aggregate: c }",
         {},
      );
      expect(result.data).toBeDefined();
   });
});

// `extractSourcesFromModelDef` only validates #(authorize) syntax for TOP-LEVEL
// `modelDef.contents` sources at model load, so a malformed gate on a join-only
// source does not fail model load. It is also never read at request time, since
// joins are not walked — so it has no effect at all. A malformed gate on an
// ENTRY POINT does still fail closed (`gateExprsForOwnAnnotations` catches the
// parse error and substitutes `"false"`); the "fails model load on a malformed
// authorize annotation" test above covers the top-level case.
describe("a malformed gate on a join-only source is never read", () => {
   it("loads successfully despite the malformed gate (never validated — it's join-only, not top-level)", async () => {
      await writeModel(
         "d_base.malloy",
         `#(authorize) notquoted
source: base_locked is duckdb.table('customers') extend { measure: c is count() }
`,
      );
      await writeModel(
         "d_mid.malloy",
         `import "d_base.malloy"

source: d_mid is duckdb.table('customers') extend {
  join_one: base_locked on id = base_locked.id
  measure: c is count()
}
`,
      );
      await writeModel(
         "d_entry.malloy",
         `import "d_mid.malloy"

source: top is duckdb.table('customers') extend {
  join_one: d_mid on id = d_mid.id
  measure: c is count()
}
`,
      );
      const model = await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "d_entry.malloy",
         getConnections(),
      );
      // Confirms the premise: the malformed gate did NOT fail model load, and
      // `base_locked` is not even in the entry model's own discovery surface.
      expect(model.getNotebookError()).toBeUndefined();
      expect(model.getSources()?.map((s) => s.name)).not.toContain(
         "base_locked",
      );
   });

   it("runs the query — the join-only gate is neither validated nor evaluated", async () => {
      await writeModel(
         "d_base.malloy",
         `#(authorize) notquoted
source: base_locked is duckdb.table('customers') extend { measure: c is count() }
`,
      );
      await writeModel(
         "d_mid.malloy",
         `import "d_base.malloy"

source: d_mid is duckdb.table('customers') extend {
  join_one: base_locked on id = base_locked.id
  measure: c is count()
}
`,
      );
      await writeModel(
         "d_entry.malloy",
         `import "d_mid.malloy"

source: top is duckdb.table('customers') extend {
  join_one: d_mid on id = d_mid.id
  measure: c is count()
}
`,
      );
      const { result } = await runGated(
         "d_entry.malloy",
         "run: top -> { aggregate: c }",
         {},
      );
      expect(result.data).toBeDefined();
   });

   it("still fails closed on a malformed gate when the source IS the entry point", async () => {
      await writeModel(
         "d_direct.malloy",
         `#(authorize) notquoted
source: gated_direct is duckdb.table('customers') extend { measure: c is count() }
`,
      );
      // Top-level, so this one DOES fail model load — the gate never silently
      // becomes "unrestricted".
      const model = await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "d_direct.malloy",
         getConnections(),
      );
      expect(model.getNotebookError()).toBeDefined();
   });
});

// BLOCKING-2: composite sources (`compose(a, b)`) resolve to ONE concrete
// member branch per query (`Query.compositeResolvedSourceDef`), based on
// which fields are referenced. The pre-fix code resolved only
// `_query.structRef` (the composite's own merged shape) and scanned
// `combo.fields`, never the composite's member branches — so a query whose
// resolution picked the locked branch was never gated.
describe("authorize composite source enforcement (BLOCKING-2)", () => {
   const COMPOSITE_MODEL = `##! experimental.composite_sources

source: open_src is duckdb.table('customers') extend {
  measure: c is count()
  dimension: open_flag is 1
}

source: locked_src is duckdb.table('customers') extend {
  measure: c is count()
  dimension: locked_flag is 1
  #(authorize)
  internal dimension: authorized is false
}

source: combo is compose(open_src, locked_src)
`;

   it("denies (zero rows) a query that resolves the composite to the locked member branch", async () => {
      // A row-filter graft now, not a classify-time AccessDeniedError: the
      // composite resolves toward the branch the query needs (`locked_flag`,
      // only on locked_src), and the appended `where: authorized` filters
      // that branch's rows to zero rather than refusing the request. A
      // `group_by:` query with no matching rows is an EMPTY result set, not
      // `expectDeniedByFilter`'s single zero-count aggregate row.
      await writeModel("c_composite.malloy", COMPOSITE_MODEL);
      const { compactResult } = await runGated(
         "c_composite.malloy",
         "run: combo -> { aggregate: c; group_by: locked_flag }",
         {},
      );
      expect(compactResult).toEqual([]);
   });

   it("denies (AccessDeniedError) a query resolving to the OPEN member branch too — guarantee changed from the string form", async () => {
      // Under the string form this allowed: the boolean probe evaluated
      // ONLY against the branch the query actually resolved to (open_src),
      // so an ungated member stayed queryable regardless of a sibling
      // member's gate. The dimension form grafts `where: authorized` onto
      // the composite unconditionally (enforcement does not know which
      // branch the query will resolve to before attaching the filter), and
      // `authorized` exists on ONLY locked_src's branch — so the composite
      // resolver can no longer satisfy both "the query's own field
      // (open_flag, locked_src-absent)" and "the filter's field (authorized,
      // open_src-absent)" in one branch, and the query fails to compile.
      // Confirmed safe (no fail-open: this still denies), but it is a real
      // usability regression — an ungated composite member becomes
      // unqueryable merely because a SIBLING member carries a gate.
      await writeModel("c_composite.malloy", COMPOSITE_MODEL);
      await expect(
         runGated(
            "c_composite.malloy",
            "run: combo -> { aggregate: c; group_by: open_flag }",
            {},
         ),
      ).rejects.toBeInstanceOf(AccessDeniedError);
   });

   // `Query.compositeResolvedSourceDef` is what lets assertAuthorizedForAllSources
   // gate the ONE concrete branch a composite run target resolved to (see
   // model.ts's resolveRunTargetStruct/assertAuthorizedForAllSources) instead
   // of every member. This confirms it's populated even when the query
   // references NO field that discriminates between members (just the shared
   // `c` measure) — Malloy's composite resolver still picks a concrete first
   // candidate rather than leaving the resolution unset, so the gate still
   // fires on whichever member happens to resolve first.
   it("still denies (zero rows) when the composite resolves with no field forcing a choice (locked member listed first)", async () => {
      await writeModel(
         "c_composite_no_disambiguator.malloy",
         `##! experimental.composite_sources

source: open_src is duckdb.table('customers') extend { measure: c is count() }

source: locked_src is duckdb.table('customers') extend {
  measure: c is count()
  #(authorize)
  internal dimension: authorized is false
}

source: combo_locked_first is compose(locked_src, open_src)
`,
      );
      // The appended `where: authorized` filter is itself now what forces
      // the composite resolver toward locked_src (the only branch with that
      // field) — a row-filter denial (zero rows), not the classify-time
      // AccessDeniedError the string form's whole-source boolean produced.
      await expectDeniedByFilter(
         "c_composite_no_disambiguator.malloy",
         "run: combo_locked_first -> { aggregate: c }",
         {},
      );
   });

   // GUARANTEE LOST, structural (not a test-authoring choice): under the
   // string form, `combo`'s own note and `locked_src`'s note lived on two
   // DIFFERENT sources' annotation lists, so each classified independently
   // and the two composed by AND. The dimension form cannot reproduce this
   // shape at all — a composite source's OWN `struct.fields` transparently
   // includes every member's fields (that is what lets a query reference a
   // member's column through the composite in the first place), so
   // `locked_src`'s annotated "authorized" dimension is ALREADY a candidate
   // on `combo`'s own struct before `combo` declares anything of its own.
   // Adding a second, independently-named annotated dimension directly on
   // `combo` therefore always trips G1 ("at most one gate dimension per
   // source") — there is no field name that avoids this, since the conflict
   // is with the INHERITED member field, not with whatever name `combo`
   // picks. A composite can carry AT MOST one gate total post-migration:
   // either its own, or (by inheriting the sole candidate) a single
   // member's — never both independently ANDed. Confirmed empirically
   // (see the refusal test below); worth a human decision on whether
   // composites need first-class multi-gate support, but that is a product
   // question, not a test-authoring one.
   it("G1 refuses a composite's own gate dimension when a member already carries one (structural — the AND-composition shape above no longer exists)", async () => {
      await writeModel(
         "c_composite_qs_refused.malloy",
         `##! experimental.composite_sources
##! experimental.givens

given:
  ROLE :: string
  REGION :: string

source: open_src is duckdb.table('customers') extend {
  measure: c is count()
  dimension: open_flag is 1
}

source: locked_src is duckdb.table('customers') extend {
  measure: c is count()
  dimension: locked_flag is 1
  #(authorize)
  internal dimension: authorized is region = $REGION
}

source: combo is compose(open_src, locked_src) extend {
  #(authorize)
  internal dimension: permitted is not ($ROLE = 'blocked')
}
`,
      );
      const model = await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "c_composite_qs_refused.malloy",
         getConnections(),
      );
      expect(model.getNotebookError()?.message).toMatch(
         /more than one #\(authorize\) gate dimension/,
      );
   });

   it("a query source over a composite reports TWO authorize elements for one gate — the base and the resolved member each contribute one", async () => {
      // `collectEntryPointGates` walks a query_source's own base AND its
      // `query.compositeResolvedSourceDef` separately. A composite's struct
      // transparently carries its members' fields, so the member's annotated
      // gate dimension is a candidate on BOTH — and the base's
      // `excludeNotes` identity-subtraction only covers STRUCT-level notes,
      // not a field annotation. So one authored gate yields two carried-in
      // entries, and `Model`'s introspection override flatMaps both.
      // Documented in api-doc.yaml's `authorize` description: the field is
      // not a per-source-count of one, and its elements can be the opaque
      // graft identifier rather than the author's expression text.
      await writeModel(
         "c_composite_qs_two_elements.malloy",
         `##! experimental { composite_sources givens }

given:
  GROUPS :: number[]

source: member_a is duckdb.sql("select 7 as org_id, 1 as amount") extend {
   #(authorize)
   internal dimension: authorized is org_id in $GROUPS
}

source: member_b is duckdb.sql("select 99 as org_id, 2 as amount")

source: combo is compose(member_a, member_b)

source: qs is combo -> { group_by: org_id }
`,
      );
      const model = await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "c_composite_qs_two_elements.malloy",
         getConnections(),
      );
      expect(model.getNotebookError()).toBeUndefined();
      expect(model.getAuthorize("qs")).toEqual([
         "`authorized`",
         "`authorized`",
      ]);
      expect(sourceNamed(model, "qs")?.authorize).toEqual([
         "`authorized`",
         "`authorized`",
      ]);
      // The source that DECLARED the gate still reports the authored text.
      expect(sourceNamed(model, "member_a")?.authorize).toEqual([
         "org_id in $GROUPS",
      ]);
   });
});

// The sharpest consequence of entry-point-only evaluation, pinned deliberately
// because it is the one a reviewer will want to see stated: a caller may join a
// locked source inside its OWN query refinement and project that source's
// columns. The entry point is `open_src`, which is ungated, so nothing denies.
// This is the shape that makes "the gate belongs on the source callers enter
// through" a real obligation on authors rather than advice.
describe("a query-local join to a locked source is not gated (Q16)", () => {
   it("allows projecting a locked source's column through a query-local join", async () => {
      await writeModel(
         "c_query_local_join.malloy",
         `source: open_src is duckdb.table('customers') extend { measure: c is count() }

source: locked_src is duckdb.table('customers') extend {
  measure: c is count()
  dimension: secret is name
  #(authorize)
  internal dimension: authorized is false
}
`,
      );
      const { result } = await runGated(
         "c_query_local_join.malloy",
         `run: open_src -> {
  join_one: locked_src on id = locked_src.id
  group_by: locked_src.secret
}`,
         {},
      );
      expect(result.data).toBeDefined();
   });
});

// The /compile path gates via Model.assertAuthorizedForText (early,
// surface-syntax) and Model.assertAuthorizedForRunnable (compiled-source
// backstop). These are the enforcement primitives environment.compileSource
// calls; exercise them directly here.
describe("authorize compile-path gate", () => {
   const CP_GATE = `##! experimental.givens

given:
  ROLE :: string

source: gated is duckdb.table('customers') extend {
  measure: c is count()
  #(authorize)
  internal dimension: authorized is $ROLE = 'analyst'
}

source: open_src is duckdb.table('customers') extend { measure: c is count() }
`;
   const CP_JOIN = `##! experimental.givens

#(authorize) "false"
source: cp_locked is duckdb.table('customers') extend { measure: c is count() }

source: cp_joiner is duckdb.table('customers') extend {
  join_one: cp_locked on id = cp_locked.id
  measure: c is count()
}
`;

   async function cpModel(file: string, src: string): Promise<Model> {
      await writeModel(file, src);
      return Model.create("test-pkg", TEST_PKG_DIR, file, getConnections());
   }

   it("assertAuthorizedForText defers a row-level gate's verdict to the authoritative backstop", async () => {
      // `assertAuthorizedForText` is a best-effort PRE-compile check (see
      // `Model.assertAuthorized`'s doc): it denies a gate it cannot even
      // classify/graft, but a gate that CAN be expressed as a row filter is
      // deferred rather than evaluated here — it has no whole-source
      // admit/deny answer on its own. Both calls resolve regardless of
      // whether the given actually satisfies `$ROLE = 'analyst'`; the
      // authoritative backstop (`authorizeAndBindRunnable`, exercised via
      // `assertAuthorizedForRunnable` below) is what actually enforces it.
      const model = await cpModel("cp_gate.malloy", CP_GATE);
      await expect(
         model.assertAuthorizedForText("run: gated -> { aggregate: c }", {}),
      ).resolves.toBeUndefined();
      await expect(
         model.assertAuthorizedForText("run: gated -> { aggregate: c }", {
            ROLE: "analyst",
         }),
      ).resolves.toBeUndefined();
   });

   it("assertAuthorizedForText leaves an ungated source unrestricted", async () => {
      const model = await cpModel("cp_gate.malloy", CP_GATE);
      await expect(
         model.assertAuthorizedForText("run: open_src -> { aggregate: c }", {}),
      ).resolves.toBeUndefined();
   });

   it("assertAuthorizedForText leaves inline/unnamed text unrestricted (nothing to name a gate on)", async () => {
      const model = await cpModel("cp_gate.malloy", CP_GATE);
      // No named source the regex recognizes -> undefined -> nothing gates
      // it here; `#(authorize)` is declared only on a `source:`.
      await expect(
         model.assertAuthorizedForText(
            `run: duckdb.sql("SELECT 1 AS x") -> { aggregate: n is count() }`,
            {},
         ),
      ).resolves.toBeUndefined();
   });

   it("assertAuthorizedForRunnable gates the compiled-source structRef (alias backstop)", async () => {
      const model = await cpModel("cp_gate.malloy", CP_GATE);
      // Stub a runnable whose compiled query reads `gated` (e.g. via an alias
      // the surface-syntax gate would miss). This is the AUTHORITATIVE path
      // (`authorizeAndBindRunnable`), and on `/compile` it has no `recompile`
      // hook to attach the row filter with. A caller who supplied nothing for
      // the gate's given is refused; one who supplied every given the gate
      // reads is compiling their own authoring loop and is admitted — see
      // `authorizeAndBindRunnable`'s `checkOnly` branch.
      const gatedRunnable = {
         getPreparedQuery: async () => ({ _query: { structRef: "gated" } }),
      };
      await expect(
         model.assertAuthorizedForRunnable(gatedRunnable, {}),
      ).rejects.toBeInstanceOf(AccessDeniedError);
      await expect(
         model.assertAuthorizedForRunnable(gatedRunnable, { ROLE: "analyst" }),
      ).resolves.toBeUndefined();
      // Ungated compiled source -> unrestricted.
      const openRunnable = {
         getPreparedQuery: async () => ({ _query: { structRef: "open_src" } }),
      };
      await expect(
         model.assertAuthorizedForRunnable(openRunnable, {}),
      ).resolves.toBeUndefined();
   });

   it("assertAuthorizedForRunnable does NOT gate a joined source on the /compile path", async () => {
      const model = await cpModel("cp_join.malloy", CP_JOIN);
      // Same entry-point rule as the query path: `cp_joiner` declares no gate,
      // so the locked source it joins is not gated here either.
      const joinerRunnable = {
         getPreparedQuery: async () => ({
            _query: { structRef: "cp_joiner" },
         }),
      };
      await expect(
         model.assertAuthorizedForRunnable(joinerRunnable, {}),
      ).resolves.toBeUndefined();
   });
});

// A given can be both a runtime query parameter (substituted into a view's
// `where`) AND the subject of an `#(authorize)` gate on the same source. These
// two layers compose: the gate fires first (before any filtering), and once it
// passes the parameterized filter applies to the result. This is the layered
// case the end-to-end story rests on.
describe("authorize composes with given parameters", () => {
   // `ROLE` gates access; `REGION` is a runtime filter parameter on the view.
   const COMPOSED = `##! experimental.givens

given:
  ROLE :: string
  REGION :: string

source: regional is duckdb.table('customers') extend {
  measure: c is count()
  view: in_region is {
    where: region = $REGION
    aggregate: c
  }
  #(authorize)
  internal dimension: authorized is $ROLE = 'analyst'
}
`;

   async function runComposed(givens: Record<string, GivenValue>) {
      await writeModel("compose.malloy", COMPOSED);
      const model = await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "compose.malloy",
         getConnections(),
      );
      return model.getQueryResults(
         undefined,
         undefined,
         "run: regional -> in_region",
         undefined,
         undefined,
         givens,
      );
   }

   function countOf(compactResult: unknown): number {
      const rows = compactResult as Array<Record<string, unknown>>;
      return Number(rows[0]?.c);
   }

   it("denies before filtering when the gate fails, even with the filter given supplied", async () => {
      // `ROLE` has no default (deliberately), so an unsupplied `ROLE` surfaces
      // as a MalloyError, not `Model`-issued `AccessDeniedError` — every gate
      // is a row filter now, and this one can't even resolve to a value.
      await expect(runComposed({ REGION: "us-west" })).rejects.toThrow();
   });

   it("applies the parameterized filter once the gate passes", async () => {
      // Seed: one 'us-west' customer, one 'us-east'.
      const west = await runComposed({ ROLE: "analyst", REGION: "us-west" });
      expect(countOf(west.compactResult)).toBe(1);

      const east = await runComposed({ ROLE: "analyst", REGION: "us-east" });
      expect(countOf(east.compactResult)).toBe(1);

      // A region matching no rows proves the given genuinely filters (not a
      // gate-only pass-through).
      const none = await runComposed({ ROLE: "analyst", REGION: "nowhere" });
      expect(countOf(none.compactResult)).toBe(0);
   });
});

// A GIVEN-based gate on a source reached only through a transitive import +
// join. Under entry-point-only evaluation the joined gate is not evaluated at
// all, so the elaborate probe machinery this block used to exercise (ambient vs
// self-contained probes, cross-hop given namespaces, name-collision isolation
// between an entry model's own `$LEVEL` and a joined source's) has no joined
// gate left to apply to. What remains worth pinning is the caller-visible
// consequence, which is NOT simply "it is allowed".
describe("a joined given-based gate is not evaluated (Q16)", () => {
   const G_BASE = `##! experimental.givens

given:
  ROLE :: string

source: base_gated is duckdb.table('customers') extend {
  measure: c is count()
  #(authorize)
  internal dimension: authorized is $ROLE = 'analyst'
}
`;
   const G_MID = `import "g_base.malloy"

source: g_mid is duckdb.table('customers') extend {
  join_one: base_gated on id = base_gated.id
  measure: c is count()
}
`;
   const G_ENTRY = `import "g_mid.malloy"

source: g_top is duckdb.table('customers') extend {
  join_one: g_mid on id = g_mid.id
  measure: c is count()
}
`;

   async function writeTwoHop() {
      await writeModel("g_base.malloy", G_BASE);
      await writeModel("g_mid.malloy", G_MID);
      await writeModel("g_entry.malloy", G_ENTRY);
   }

   it("runs with no given at all — the two-hop joined gate never fires", async () => {
      await writeTwoHop();
      const { result } = await runGated(
         "g_entry.malloy",
         "run: g_top -> { aggregate: c }",
         {},
      );
      expect(result.data).toBeDefined();
   });

   it("rejects a given supplied for that gate as an unknown given, rather than ignoring it", async () => {
      // The entry model surfaces no givens, and with the joined gate no longer
      // collected, `ROLE` is no longer authorize-referenced either — so
      // `filterGivensToModelSurface` stops dropping it and Malloy's own
      // `resolveSuppliedGivens` refuses it. That is the pre-existing deliberate
      // behavior for an unreferenced, unsurfaced given (fail closed rather than
      // silently swallow it and fall back to a default), and it is why removing
      // the join walk is caller-VISIBLE and not silent: a caller who used to
      // pass `ROLE` to satisfy this gate now gets an error, not a quiet allow.
      await writeTwoHop();
      await expect(
         runGated("g_entry.malloy", "run: g_top -> { aggregate: c }", {
            ROLE: "analyst",
         }),
      ).rejects.toThrow(/unknown given/i);
   });

   it("still evaluates the gate when the gated source IS the entry point", async () => {
      // The control: the same given-based gate is fully enforced one hop in,
      // where the caller enters through the gated source itself.
      await writeTwoHop();
      const { result } = await runGated(
         "g_base.malloy",
         "run: base_gated -> { aggregate: c }",
         { ROLE: "analyst" },
      );
      expect(result.data).toBeDefined();
      await expectDeniedByFilter(
         "g_base.malloy",
         "run: base_gated -> { aggregate: c }",
         { ROLE: "intern" },
      );
   });
});

// BLOCKING-5: a query-source derivation (`source: laundered is locked_src ->
// { ... }`) laundered a locked base's gate away — the run target resolves to
// a `QuerySourceDef` whose own `.fields`/`.annotations` reflect the DERIVED
// shape, not `locked_src`'s #(authorize), so the walk found nothing. Fixed
// by resolveQuerySourceBases/collectQuerySourceBaseGates: a `QuerySourceDef`
// keeps its base reachable via `query.structRef` (an inline SourceDef or a
// string name resolving through modelDef.contents — the same StructRef
// shape resolveRunTargetStruct already resolves for a run target), so the
// walk now gates that base too, recursing for a chained derivation and for a
// query-source reached only via a join.
describe("authorize query-source derivation enforcement (BLOCKING-5)", () => {
   const QS_MODEL = `source: locked_src is duckdb.table('customers') extend {
  measure: c is count()
  dimension: secret is name
  #(authorize)
  internal dimension: authorized is false
}

source: laundered is locked_src -> { select: id, secret }

source: open_src is duckdb.table('customers') extend { measure: c is count() }

source: open_laundered is open_src -> { select: id }

source: qs_joiner is duckdb.table('customers') extend {
  join_one: laundered on id = laundered.id
  measure: c is count()
}

source: double_laundered is laundered -> { select: id, secret }
`;

   it("denies (AccessDeniedError) a query against a query-source derived from a locked base — guarantee changed from the string form", async () => {
      // Under the string form this was a zero-rows filter denial: the
      // walk discovers the gate fine (`query.structRef` reaches
      // `locked_src` directly), but `laundered`'s own projection
      // (`-> { select: id, secret }`) does not carry `locked_src`'s
      // "authorized" field forward, so the by-NAME graft has nothing to
      // attach `where: authorized` to on `laundered`'s own compiled
      // struct. Fails CLOSED (denied outright), not open — same root
      // cause the brief's `except:`/`accept:` KNOWN GAP names, reached via
      // a projecting pipeline instead of a field-dropping extend.
      await writeModel("qs.malloy", QS_MODEL);
      await expect(
         runGated("qs.malloy", "run: laundered -> { select: id, secret }", {}),
      ).rejects.toBeInstanceOf(AccessDeniedError);
   });

   it("allows a query-source derived from an ungated base", async () => {
      await writeModel("qs.malloy", QS_MODEL);
      const { result } = await runGated(
         "qs.malloy",
         "run: open_laundered -> { select: id }",
         {},
      );
      expect(result.data).toBeDefined();
   });

   it("does NOT gate a query-source reached only via a join", async () => {
      // The line between the two rules, on one model: derivation carries the
      // gate to the entry point (the tests above), a join does not (this one).
      await writeModel("qs.malloy", QS_MODEL);
      const { result } = await runGated(
         "qs.malloy",
         "run: qs_joiner -> { aggregate: c }",
         {},
      );
      expect(result.data).toBeDefined();
   });

   it("denies (AccessDeniedError) a CHAINED query-source (derived from a derivation of a locked base) — same guarantee change", async () => {
      await writeModel("qs.malloy", QS_MODEL);
      await expect(
         runGated(
            "qs.malloy",
            "run: double_laundered -> { select: id, secret }",
            {},
         ),
      ).rejects.toBeInstanceOf(AccessDeniedError);
   });
});

// BLOCKING-6: a query-source's OWN inner pipeline segment can declare a
// `join_one` INSIDE the derivation (`-> { extend: { join_one: locked ... }
// ... }`). That join lives on the pipeline segment's `extendSource`, not on
// `struct.fields` and not reachable via `query.structRef` (the base chain
// resolveQuerySourceBases/collectQuerySourceBaseGates already walk) — so it
// was never gated, laundering a locked source's #(authorize) through the
// query-source's own derivation. The identical join declared inline in a
// RUN query (rather than inside a `source: x is y -> {...}` derivation) was
// already correctly denied via assertAuthorizedForAllSources's own
// `extendSources` handling.
describe("a query-source's own inner-pipeline join is not gated (Q16)", () => {
   const QS_INNER_JOIN_MODEL = `source: locked9 is duckdb.table('customers') extend {
  dimension: secret is name
  #(authorize)
  internal dimension: authorized is false
}

source: open9 is duckdb.table('customers') extend { measure: c is count() }

source: sneaky is open9 -> {
  extend: { join_one: locked9 on id = locked9.id }
  select: id, leak is locked9.secret
}

source: qs_joiner is duckdb.table('customers') extend {
  join_one: sneaky on id = sneaky.id
  measure: c is count()
}

source: open_inner_join is open9 -> {
  extend: { join_one: open9_b is duckdb.table('customers') on id = open9_b.id }
  select: id
}
`;

   it("allows a query-source whose own inner pipeline joins a locked source", async () => {
      // `sneaky` selects `locked9.secret` through a join inside its own
      // pipeline. Its entry point is `open9`, which is ungated, so the locked
      // column is readable. This is the author-side twin of the query-local
      // join case: an AUTHOR can publish a gated source's column this way, so
      // the gate has to sit on what callers enter through.
      await writeModel("qs_inner.malloy", QS_INNER_JOIN_MODEL);
      const { result } = await runGated(
         "qs_inner.malloy",
         "run: sneaky -> { select: id, leak }",
         {},
      );
      expect(result.data).toBeDefined();
   });

   it("allows a query joining such a query-source", async () => {
      await writeModel("qs_inner.malloy", QS_INNER_JOIN_MODEL);
      const { result } = await runGated(
         "qs_inner.malloy",
         "run: qs_joiner -> { group_by: sneaky.leak }",
         {},
      );
      expect(result.data).toBeDefined();
   });

   it("allows a query-source whose own inner pipeline joins an UNGATED source", async () => {
      await writeModel("qs_inner.malloy", QS_INNER_JOIN_MODEL);
      const { result } = await runGated(
         "qs_inner.malloy",
         "run: open_inner_join -> { select: id }",
         {},
      );
      expect(result.data).toBeDefined();
   });
});

// SHOULD-2: filterGivensToModelSurface dropped EVERY given name absent from
// the entry model's own surface, not just an authorize-only given. That
// over-drops a genuine `where:` filter given declared two-plus import hops
// away (never on the entry surface, per Malloy's one-hop given-namespace
// merge — see BLOCKING-4): the caller's value is silently discarded and the
// query falls back to the given's declared default, returning MORE rows
// than the caller intended. It also silently swallows a typo'd given name
// instead of raising Malloy's "unknown given" error. Fix: only drop a name
// that is BOTH referenced by an authorize gate reachable in this model AND
// absent from the surface -- a name no gate references is left alone so it
// either reaches the real query (honoring the caller's value) or fails
// closed with a compile/runtime error, never a silent default.
describe("filterGivensToModelSurface no longer over-drops a non-gate given (SHOULD-2)", () => {
   const HB_BASE = `##! experimental.givens

given:
  HIDE :: string is 'none'

source: hb_base is duckdb.table('customers') extend {
  where: region != $HIDE
  measure: c is count()
}
`;
   const HB_MID = `import "hb_base.malloy"

source: hb_mid is duckdb.table('customers') extend {
  join_one: hb_base on id = hb_base.id
  measure: c is count()
}
`;
   const HB_ENTRY = `import "hb_mid.malloy"

source: hb_top is duckdb.table('customers') extend {
  join_one: hb_mid on id = hb_mid.id
  measure: c is count()
}
`;

   it("does not silently over-return when a two-hop where-given is supplied", async () => {
      await writeModel("hb_base.malloy", HB_BASE);
      await writeModel("hb_mid.malloy", HB_MID);
      await writeModel("hb_entry.malloy", HB_ENTRY);
      // Seed has one 'us-west' row and one 'us-east' row. The caller intends
      // to exclude 'us-west'. Silently falling back to the default ('none',
      // which excludes nothing) would return BOTH rows via hb_base.c.
      let overExposed = false;
      try {
         const { compactResult } = await runGated(
            "hb_entry.malloy",
            "run: hb_top -> { aggregate: c is hb_mid.hb_base.c }",
            { HIDE: "us-west" },
         );
         const rows = compactResult as unknown as Array<
            Record<string, unknown>
         >;
         overExposed = Number(rows[0]?.c) === 2;
      } catch {
         // Erroring instead of over-returning is an acceptable outcome too
         // (fail closed on a name the entry model can't resolve).
      }
      expect(overExposed).toBe(false);
   });

   it("does not silently swallow a typo'd given name", async () => {
      await writeModel("hb_base.malloy", HB_BASE);
      await writeModel("hb_mid.malloy", HB_MID);
      await writeModel("hb_entry.malloy", HB_ENTRY);
      await expect(
         runGated(
            "hb_entry.malloy",
            "run: hb_top -> { aggregate: c is hb_mid.hb_base.c }",
            { HIDEE: "us-west" }, // typo for HIDE
         ),
      ).rejects.toBeTruthy();
   });
});

// Where the two surviving rules meet. A composite's RESOLVED branch stands in
// for the entry point, so a branch that is itself a query-source derived from a
// locked base still carries that gate. The same derived source reached via a
// query-local JOIN does not. Both directions are pinned here so the boundary
// between "derivation carries" and "joins do not" is explicit.
describe("derivation carries the gate; joins do not", () => {
   const UNIFIED_MODEL = `##! experimental.composite_sources
source: open_src is duckdb.table('customers') extend {
  measure: c is count()
}

source: locked is duckdb.table('customers') extend {
  measure: c is count()
  dimension: locked_region is region
  #(authorize)
  internal dimension: authorized is false
}

source: laundered is locked -> { group_by: id, locked_region }

source: combo is compose(open_src, laundered)

source: open_laundered is open_src -> { group_by: id }
`;

   it("denies a composite query resolving to a query-source branch derived from a locked base", async () => {
      await writeModel("c_unified.malloy", UNIFIED_MODEL);
      await expect(
         runGated(
            "c_unified.malloy",
            "run: combo -> { group_by: locked_region }",
            {},
         ),
      ).rejects.toBeInstanceOf(AccessDeniedError);
   });

   it("allows a run target's own query-local join to a query-source derived from a locked base", async () => {
      // Derivation carries the gate, but only to the derived source's OWN entry
      // point. Reached via a query-local join it is not gated — the join rule
      // wins over the derivation rule when the two meet.
      await writeModel("c_unified.malloy", UNIFIED_MODEL);
      const { result } = await runGated(
         "c_unified.malloy",
         `run: open_src -> {
  extend: { join_one: laundered on id = laundered.id }
  group_by: leak is laundered.locked_region
}`,
         {},
      );
      expect(result.data).toBeDefined();
   });

   it("allows a composite query resolving to the ungated open branch", async () => {
      await writeModel("c_unified.malloy", UNIFIED_MODEL);
      const { result } = await runGated(
         "c_unified.malloy",
         "run: combo -> { aggregate: c }",
         {},
      );
      expect(result.data).toBeDefined();
   });

   it("allows a run target's own query-local join to an UNGATED query-source", async () => {
      await writeModel("c_unified.malloy", UNIFIED_MODEL);
      const { result } = await runGated(
         "c_unified.malloy",
         `run: open_src -> {
  extend: { join_one: open_laundered on id = open_laundered.id }
  group_by: ok is open_laundered.id
}`,
         {},
      );
      expect(result.data).toBeDefined();
   });
});

// query_source's OWN base is a composite (not the run target itself): the
// query_source branch of collectEntryPointGates resolved the base via
// query.structRef (the raw composite) and walked query.pipeline, but never
// walked the query-source's own compositeResolvedSourceDef — the RESOLVED
// branch Malloy picked for that query-source's derivation. A query-source
// derived from a locked composite member laundered that member's gate away.
describe("query-source over composite: resolved-branch gate", () => {
   const QS_OVER_COMPOSITE_MODEL = `##! experimental.composite_sources
source: open_src is duckdb.table('customers') extend {
  measure: c is count()
  dimension: region_d is region
}

source: locked is duckdb.table('customers') extend {
  measure: c is count()
  dimension: locked_region is region
  #(authorize)
  internal dimension: authorized is false
}

source: laundered is locked -> { group_by: id, locked_region }

source: inner_combo is compose(open_src, laundered)

source: qs_over_combo is inner_combo -> { group_by: id, locked_region }

source: open_qs_over_combo is inner_combo -> { group_by: id, region_d }

source: outer_qs is qs_over_combo -> { group_by: locked_region }
`;

   it("denies (AccessDeniedError) a query-source whose base composite resolves to a locked member — guarantee changed from the string form", async () => {
      // Same root cause as BLOCKING-5's derivation tests: `qs_over_combo`'s
      // own projection (`-> { group_by: id, locked_region }`) does not carry
      // `locked`'s "authorized" field forward, so the by-name graft fails
      // to attach and denies outright rather than filtering to zero rows.
      await writeModel("qsc_unified.malloy", QS_OVER_COMPOSITE_MODEL);
      await expect(
         runGated(
            "qsc_unified.malloy",
            "run: qs_over_combo -> { group_by: locked_region }",
            {},
         ),
      ).rejects.toBeInstanceOf(AccessDeniedError);
   });

   it("allows its open-branch twin: query-source whose base composite resolves to the ungated member", async () => {
      await writeModel("qsc_unified.malloy", QS_OVER_COMPOSITE_MODEL);
      const { result } = await runGated(
         "qsc_unified.malloy",
         "run: open_qs_over_combo -> { group_by: region_d }",
         {},
      );
      expect(result.data).toBeDefined();
   });

   it("denies (AccessDeniedError) at nesting depth 2: a query-source over a query-source over a composite whose resolved branch hits a locked base — same guarantee change", async () => {
      await writeModel("qsc_unified.malloy", QS_OVER_COMPOSITE_MODEL);
      await expect(
         runGated(
            "qsc_unified.malloy",
            "run: outer_qs -> { group_by: locked_region }",
            {},
         ),
      ).rejects.toBeInstanceOf(AccessDeniedError);
   });
});

// MUST-FIX 1 (review regression): Malloy's RecordDef/RepeatedRecordDef/
// BasicArrayDef (STRUCT/ARRAY/JSON-shaped nested columns) extend JoinBase,
// so `isJoined()` is true for them even though they are ordinary columns,
// not joins to another authorize-gated source. `isSourceDef()` correctly
// excludes them ('record'/'array' aren't source kinds), which used to trip
// the "isJoined but not isSourceDef" drift invariant and deny a query
// against any table with a nested column — even with zero authorize
// annotations anywhere in the model.
describe("authorize tolerates record/array-typed columns (MUST-FIX 1)", () => {
   it("allows a query against a table with STRUCT/ARRAY/nested-STRUCT-ARRAY columns and no gates", async () => {
      await writeModel(
         "rt_nested.malloy",
         `source: nested_src is duckdb.table('nested_cols') extend {
  measure: c is count()
}
`,
      );
      const { result } = await runGated(
         "rt_nested.malloy",
         "run: nested_src -> { aggregate: c }",
         {},
      );
      expect(result.data).toBeDefined();
   });
});

// MUST-FIX 2 (review regression): evaluateSelfContainedFirst treated
// `decls.length === 0` as "unsatisfiable, deny" unconditionally — but that
// conflates "the expression references givens the caller can't supply"
// (correct deny) with "the expression references NO givens at all" (e.g. a
// constant/public gate like `#(authorize) "true"` — there's nothing ambient
// to isolate from, so there's nothing wrong with running it with no decls).
describe("authorize allows a givens-free joined gate (MUST-FIX 2)", () => {
   it('allows a same-file `#(authorize) "true"` source joined by an ungated top', async () => {
      await writeModel(
         "rt_pub.malloy",
         `source: pub_gated is duckdb.table('customers') extend {
  measure: c is count()
  #(authorize)
  internal dimension: authorized is true
}

source: pub_joiner is duckdb.table('customers') extend {
  join_one: pub_gated on id = pub_gated.id
  measure: c is count()
}
`,
      );
      const { result } = await runGated(
         "rt_pub.malloy",
         "run: pub_joiner -> { aggregate: c }",
         {},
      );
      expect(result.data).toBeDefined();
   });
});

// The `query` body is not the only caller-supplied Malloy text the query path
// compiles: `sourceName`/`queryName` are interpolated verbatim into the
// `run: <source>-><query>` statement it builds, so an annotation smuggled
// through either one lands in the compiled text exactly as one in `query` does.
// Verified against @malloydata/* 0.0.427: with the guard applied only to
// `query`, the request below returned every row of a base locked with
// `#(authorize) "false"`.
describe("the caller-text guard covers sourceName/queryName too", () => {
   const LOCKED = `source: inj_locked is duckdb.table('customers') extend {
  measure: c is count()
  #(authorize)
  internal dimension: authorized is false
}

source: inj_plain is duckdb.table('customers') extend { measure: c is count() }
`;

   it("rejects a forged gate smuggled through sourceName", async () => {
      await writeModel("inj.malloy", LOCKED);
      const model = await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "inj.malloy",
         getConnections(),
      );
      await expect(
         model.getQueryResults(
            `inj_plain -> { aggregate: c }
             #(authorize) "true"
             source: mine is inj_locked extend { measure: cc is count() }
             run: mine`,
            "{ aggregate: cc }",
            undefined,
            undefined,
            undefined,
            { ROLE: "nobody" },
         ),
      ).rejects.toBeInstanceOf(BadRequestError);
   });

   it("rejects a forged gate smuggled through queryName", async () => {
      await writeModel("inj.malloy", LOCKED);
      const model = await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "inj.malloy",
         getConnections(),
      );
      await expect(
         model.getQueryResults(
            undefined,
            `{ aggregate: c }
             #(authorize) "true"
             source: mine is inj_locked extend { measure: cc is count() }
             run: mine -> { aggregate: cc }`,
            undefined,
            undefined,
            undefined,
            { ROLE: "nobody" },
         ),
      ).rejects.toBeInstanceOf(BadRequestError);
   });
});

// A prior version of this suite pinned "an inherited gate is isolated from a
// colliding entry-model given" here: a STRING-FORM gate (`$LEVEL > 3`)
// inherited into an entry model that redeclares `LEVEL` WITH a default whose
// value trivially satisfies the gate. That guarantee was enforced by
// `assertNoVacuousDefaultAtom`, a static-literal evaluator that walked the
// STRING form's compiled comparison tree looking for exactly this hazard.
// Task 4 (authorize-natural-malloy) deleted that evaluator along with the
// rest of the string-form classification machinery it depended on
// (`classifyAuthorizeGate`) — the dimension form has no equivalent check, and
// none is planned: a gate dimension's default-satisfies-vacuously hazard is a
// property of how the AUTHOR wrote the expression, not something publisher
// can detect generically without re-implementing an evaluator against
// `@malloydata/malloy`'s compiled IR. This is an accepted coverage reduction,
// not a silently dropped guarantee — the dimension form's own gates still
// fail closed (see `row_level_authorize.integration.spec.ts`'s "constant gate
// dimensions" and `gate_dimension_integration.spec.ts`) if authored to do so
// deliberately; only the built-in "warn me if a NON-constant expression
// happens to be trivially satisfiable" detection is gone.

// Which annotation SPELLINGS are gates is Malloy's decision, not publisher's: a
// note is a gate iff Malloy routes it to `authorize`. Every row below was
// confirmed against `@malloydata/malloy` 0.0.427's own prefix parser, and the
// three outcomes are exhaustive — a gate, a refused near miss, or a note on
// somebody else's route that this must leave completely alone.
//
// The two directions this pins are not symmetric in cost. A spelling Malloy
// routes here that publisher misses (`#|(authorize)`, the block form) is a source
// serving every row while its author reads it as locked — the fail-open a prefix
// regex left open. A spelling publisher honours that Malloy routes elsewhere
// (`# (authorize)`, route `''` — Malloy's reserved MOTLY/render namespace) means
// publisher assigning meaning inside that namespace, and silently starting to
// enforce a filter on packages that served every row yesterday.
describe("authorize is classified by Malloy's annotation route", () => {
   // Source-level (`#`): routed to `authorize` AND enforced on the source below.
   const GATE_SPELLINGS = [
      "#(authorize)",
      // The block form. Malloy routes it to `authorize`, and the deprecated
      // RegExp readers cannot see it at all — its own type docs say so.
      "#|(authorize)",
      // Malloy's bracket pairs are `()`, `<>`, `[]`, `{}`, all equivalent.
      "#[authorize]",
      "#<authorize>",
      "#{authorize}",
   ];

   // File-level (`##`): routed to `authorize` too, which is exactly why the
   // deprecation refusal reaches them — recognition is the precondition for
   // refusing. A spelling that routed nowhere would load silently unenforced,
   // which is the fail-open that refusal exists to close.
   const FILE_LEVEL_GATE_SPELLINGS = ["##(authorize)", "##|(authorize)"];

   // Refused — neither honoured nor ignored. Each is route `''` (a plain MOTLY
   // tag), `malformed-route`, or a case variant of our own name, so nothing
   // would ever enforce it. The case variants are the one place the rule refuses
   // a note Malloy gives a perfectly good route of its own: honouring
   // `#(AUTHORIZE)` would mean claiming a namespace that isn't ours, and leaving
   // it silent is the fail-open — an author reads the source as locked while
   // every row serves.
   const NEAR_MISS_SPELLINGS = [
      "# (authorize)",
      "## (authorize)",
      "#( authorize )",
      "#(authorize )",
      "#(authorize)X",
      "#authorize",
      "#(AUTHORIZE)",
      "#(Authorize)",
   ];

   // A block annotation's closer mirrors its sigil: `#|` closes with `|#`,
   // `##|` with `|##`.
   const blockCloser = (tag: string) =>
      tag.startsWith("##|") ? "\n|##" : tag.startsWith("#|") ? "\n|#" : "";

   for (const tag of GATE_SPELLINGS) {
      it(`enforces (zero rows) a gate written ${tag}`, async () => {
         // The block closer's indentation must match the tag's (field
         // position, inside `extend {}`) or Malloy fails to close the
         // annotation and reports a spurious "Missing '}'" for the source.
         const closer = blockCloser(tag).replace("\n|", "\n  |");
         await writeModel(
            "route.malloy",
            `source: route_locked is duckdb.table('customers') extend {
  measure: c is count()
  ${tag}${closer}
  internal dimension: authorized is false
}
`,
         );
         await expectDeniedByFilter(
            "route.malloy",
            "run: route_locked -> { aggregate: c }",
         );
      });

      it(`rejects ${tag} in caller-submitted text`, async () => {
         // The rejecter reads raw pre-compile text where no route exists yet, so
         // it stays a regex — and it must be a SUPERSET of the spellings the
         // parser honours, or a caller mints a gate in a spelling only the
         // compiler recognizes.
         await writeModel(
            "route_open.malloy",
            `source: route_open is duckdb.table('customers') extend { measure: c is count() }
`,
         );
         await expect(
            runGated(
               "route_open.malloy",
               `${tag} "true"\nrun: route_open -> { aggregate: c }`,
               {},
            ),
         ).rejects.toBeInstanceOf(BadRequestError);
      });
   }

   for (const tag of FILE_LEVEL_GATE_SPELLINGS) {
      it(`recognizes ${tag} at the file level and refuses it as deprecated`, async () => {
         const closer = blockCloser(tag);
         await writeModel(
            "route_file.malloy",
            `${tag} "false"${closer}

source: route_file_plain is duckdb.table('customers')
`,
         );
         const model = await Model.create(
            "test-pkg",
            TEST_PKG_DIR,
            "route_file.malloy",
            getConnections(),
         );
         const err = model.getNotebookError();
         expect(err).toBeInstanceOf(ModelCompilationError);
         expect(err?.message).toMatch(/file level/i);
      });

      it(`rejects ${tag} in caller-submitted text`, async () => {
         await writeModel(
            "route_file_open.malloy",
            `source: route_file_open is duckdb.table('customers') extend { measure: c is count() }
`,
         );
         await expect(
            runGated(
               "route_file_open.malloy",
               `${tag} "true"\nrun: route_file_open -> { aggregate: c }`,
               {},
            ),
         ).rejects.toBeInstanceOf(BadRequestError);
      });
   }

   for (const tag of NEAR_MISS_SPELLINGS) {
      it(`refuses the load for the near miss ${tag}`, async () => {
         await writeModel(
            "near.malloy",
            `${tag} "false"
source: near_locked is duckdb.table('customers') extend { measure: c is count() }
`,
         );
         const model = await Model.create(
            "test-pkg",
            TEST_PKG_DIR,
            "near.malloy",
            getConnections(),
         );
         const err = model.getNotebookError();
         expect(err).toBeInstanceOf(ModelCompilationError);
         // Names the spelling, and what to write instead.
         expect(err?.message).toContain(tag);
         expect(err?.message).toContain('#(authorize) "<expression>"');
         // Refused, never silently enforced as if it had been spelled right.
         expect(model.getSources()).toBeUndefined();
      });

      it(`rejects the near miss ${tag} in caller-submitted text too`, async () => {
         await writeModel(
            "near_open.malloy",
            `source: near_open is duckdb.table('customers') extend { measure: c is count() }
`,
         );
         await expect(
            runGated(
               "near_open.malloy",
               `${tag} "true"\nrun: near_open -> { aggregate: c }`,
               {},
            ),
         ).rejects.toBeInstanceOf(BadRequestError);
      });
   }

   it("refuses a near miss reached only through a derivation hop", async () => {
      // The spelling is on a source the entry model never names: `near_layer`
      // imports `near_qs` alone, so `near_base_gated` reaches the sweep only as
      // the inline struct on `near_qs`'s `query.structRef`. The near-miss sweep
      // reads `contents` u `sourceRegistry`, which is not where that struct
      // lives — without following the derivation hops, this is the one place a
      // misspelled gate loads clean.
      await writeModel(
         "near_base.malloy",
         `# (authorize) "false"
source: near_base_gated is duckdb.table('customers') extend { measure: c is count() }
`,
      );
      await writeModel(
         "near_mid.malloy",
         `import { near_base_gated } from "near_base.malloy"

source: near_qs is near_base_gated -> { select: * }
`,
      );
      await writeModel(
         "near_layer.malloy",
         `import { near_qs } from "near_mid.malloy"
`,
      );
      const model = await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "near_layer.malloy",
         getConnections(),
      );
      const err = model.getNotebookError();
      expect(err).toBeInstanceOf(ModelCompilationError);
      expect(err?.message).toContain("# (authorize)");
      expect(err?.message).toContain('#(authorize) "<expression>"');
   });

   // The other side of the near-miss detector: it is anchored at each note's own
   // prefix and requires `authorize` to end at a non-word character, so ordinary
   // annotations on other routes load untouched. `#(authorized)` is the sharp one
   // — a legitimately different app route that a substring test would refuse.
   it("leaves annotations on other routes alone", async () => {
      await writeModel(
         "other_routes.malloy",
         `##(description) "see the #(authorize) tag"

#(authorized) "not a gate"
# bar_chart
#(doc) "a doc note"
source: other_routes is duckdb.table('customers') extend { measure: c is count() }
`,
      );
      const model = await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "other_routes.malloy",
         getConnections(),
      );
      expect(model.getNotebookError()).toBeUndefined();
      expect(sourceNamed(model, "other_routes")?.authorize).toBeUndefined();
   });
});

// The IR walk is what ENFORCES an inherited gate, and has since
// `ancestorGateExprs` landed. `sources[].authorize` is the separate question of
// what the server SAYS about a source, and it read own-blockNotes only — so a
// locked-by-inheritance source introspected as ungated. Two things follow from
// fixing it, and the second is the security half: `getAuthorize` feeds the EARLY
// gate, which runs before compilation specifically so a denied caller cannot read
// a gated source's schema out of compile diagnostics.
describe("an inherited gate is reported, not just enforced", () => {
   const INHERITED = `##! experimental.givens

given:
  ROLE :: string

source: rep_locked is duckdb.table('customers') extend {
  measure: c is count()
  #(authorize)
  internal dimension: authorized is false
}

// Any annotation at all demotes the base's to \`annotations.inherits\`; a render
// tag is the most innocent way an author trips this.
# bar_chart
source: rep_ext is rep_locked extend {}
`;

   it("reports the base's gate on an extension that declares none", async () => {
      await writeModel("rep.malloy", INHERITED);
      const model = await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "rep.malloy",
         getConnections(),
      );
      expect(sourceNamed(model, "rep_ext")?.authorize).toEqual(["false"]);
      expect(model.getAuthorize("rep_ext")).toEqual(["false"]);
      // The base is unchanged — this is not double-counting.
      expect(model.getAuthorize("rep_locked")).toEqual(["false"]);
   });

   it("surfaces the caller's own compile error, not a schema oracle bypass", async () => {
      await writeModel("rep.malloy", INHERITED);
      // `no_such_field` does not exist. This used to deny (403) before
      // compiling, because the inherited gate resolved to given-only and
      // could be evaluated with no compile at all. Every gate is a row
      // filter now — enforcement is deferred until the caller's own query
      // compiles and a graft can be attempted — so the bad field name
      // surfaces first, the same accepted narrowing as the quoted-identifier
      // test above. No query ever executes either way, so no row data leaks.
      await expect(
         runGated("rep.malloy", "run: rep_ext -> { group_by: no_such_field }", {
            ROLE: "intern",
         }),
      ).rejects.toThrow(/no_such_field/);
   });

   it("still lets an extension's own gate replace the base's", async () => {
      // The paired control: reporting the inherited gate must not resurrect a
      // base gate the author deliberately overrode (the locked-base +
      // curated-extension idiom).
      await writeModel(
         "rep_override.malloy",
         `##! experimental.givens

given:
  ROLE :: string

source: ro_locked is duckdb.table('customers') extend {
  measure: c is count()
  #(authorize)
  internal dimension: authorized is false
}

source: ro_ext is ro_locked extend {
  except: authorized
  #(authorize)
  internal dimension: authorized is $ROLE = 'analyst'
}
`,
      );
      const model = await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "rep_override.malloy",
         getConnections(),
      );
      expect(model.getAuthorize("ro_ext")).toEqual(["$ROLE = 'analyst'"]);
      const { result } = await runGated(
         "rep_override.malloy",
         "run: ro_ext -> { aggregate: c }",
         { ROLE: "analyst" },
      );
      expect(result.data).toBeDefined();
   });

   it("loads a package whose inherited gate references an out-of-reach given", async () => {
      // Load-time validation stays scoped to DECLARED gates for this shape. The
      // entry model cannot see `DEEP` (Malloy merges one import level, and the
      // gate's own declaration is two hops away), so probing the inherited gate
      // here would fail the load with a 424 blaming a valid annotation. It has to
      // load — and still deny at request time.
      await writeModel(
         "deep_base.malloy",
         `##! experimental.givens

given:
  DEEP :: string

#(authorize) "$DEEP = 'yes'"
source: deep_locked is duckdb.table('customers') extend { measure: c is count() }
`,
      );
      await writeModel(
         "deep_mid.malloy",
         `import "deep_base.malloy"

# bar_chart
source: deep_ext is deep_locked extend {}
`,
      );
      await writeModel("deep_entry.malloy", `import "deep_mid.malloy"\n`);
      const model = await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "deep_entry.malloy",
         getConnections(),
      );
      // Loaded, not 424 — and the gate is reported rather than silently absent.
      expect(model.getAuthorize("deep_ext")).toEqual(["$DEEP = 'yes'"]);
      await expect(
         runGated("deep_entry.malloy", "run: deep_ext -> { aggregate: c }", {}),
      ).rejects.toBeInstanceOf(AccessDeniedError);
   });
});

// The finding this pins: the guard's BadRequestError is rethrown untouched by the
// query path's parse `catch` and never reaches `malloy_model_query_duration`, so
// without a counter of its own a forged-gate rejection is invisible — a probing
// spike looks like ordinary 400 syntax noise.
describe("a rejected caller-declared gate is counted", () => {
   let harness: MetricsHarness;

   const COUNTER = "publisher_authorize_guard_rejected_total";
   const FORGED =
      "#(authorize) \"true\"\nsource: forged is duckdb.table('customers') extend { measure: c is count() }\nrun: forged -> { aggregate: c }";

   beforeEach(async () => {
      harness = await startMetricsHarness();
      // Drop the cached instrument so it re-binds to this test's provider.
      resetAuthorizeGuardTelemetryForTesting();
   });

   afterEach(async () => {
      resetAuthorizeGuardTelemetryForTesting();
      await harness.shutdown();
   });

   it("labels a rejection in the ad-hoc query text as field=query", async () => {
      await writeModel(
         "gm.malloy",
         "source: gm_base is duckdb.table('customers')\n",
      );
      await expect(runGated("gm.malloy", FORGED, {})).rejects.toBeInstanceOf(
         BadRequestError,
      );
      expect(await harness.collectCounter(COUNTER, { field: "query" })).toBe(1);
   });

   it("labels a rejection smuggled through sourceName as field=source_name", async () => {
      // sourceName is supposed to be a bare identifier, so this label is the one
      // that separates a probe from an author pasting a gate into the wrong field.
      await writeModel(
         "gm.malloy",
         "source: gm_base is duckdb.table('customers')\n",
      );
      const model = await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "gm.malloy",
         getConnections(),
      );
      await expect(
         model.getQueryResults(
            '#(authorize) "true"\nsource: sneaky is gm_base extend {}\nrun: sneaky',
            "v",
            undefined,
            undefined,
            undefined,
            {},
         ),
      ).rejects.toBeInstanceOf(BadRequestError);
      expect(
         await harness.collectCounter(COUNTER, { field: "source_name" }),
      ).toBe(1);
   });
});

// The worked example in docs/authorize.md, verbatim, with all six verdicts
// asserted. The entry-point rule is subtle enough that a reader will copy this
// model; if the doc and the compiler ever disagree, that is a doc bug shipped to
// authors, so the example is pinned rather than trusted.
describe("docs/authorize.md worked example", () => {
   const DOC_EXAMPLE = `##! experimental.givens

given:
  ROLE :: string

source: salaries is duckdb.table('salaries') extend {
  #(authorize)
  internal dimension: authorized is false
}

source: salaries_plain is salaries extend {
  measure: headcount is count()
}

# bar_chart
source: salaries_tagged is salaries extend {
  measure: headcount is count()
}

source: salaries_hr is salaries extend {
  except: authorized
  #(authorize)
  internal dimension: authorized is $ROLE = 'hr'
  measure: avg_salary is avg(salary)
}

source: salaries_derived is salaries -> { group_by: department }

source: headcount_by_dept is duckdb.table('departments') extend {
  join_one: salaries on id = salaries.department_id
  measure: headcount is count()
}
`;

   const denied: [string, string][] = [
      ["salaries", "run: salaries -> { aggregate: n is count() }"],
      ["salaries_plain", "run: salaries_plain -> { aggregate: headcount }"],
      ["salaries_tagged", "run: salaries_tagged -> { aggregate: headcount }"],
   ];

   for (const [name, query] of denied) {
      it(`denies (zero rows) ${name} regardless of givens`, async () => {
         await writeModel("doc_example.malloy", DOC_EXAMPLE);
         const { compactResult } = await runGated("doc_example.malloy", query, {
            ROLE: "hr",
         });
         const rows = compactResult as unknown as Record<string, number>[];
         expect(Object.values(rows[0])[0]).toBe(0);
      });
   }

   it("denies (AccessDeniedError) salaries_derived regardless of givens — guarantee changed from the string form, worth a docs/authorize.md update", async () => {
      // `salaries_derived is salaries -> { group_by: department }` does not
      // select `salaries`'s "authorized" field forward, so the by-name graft
      // has nothing on `salaries_derived`'s own struct to attach
      // `where: authorized` to — same fails-CLOSED shape BLOCKING-5 pins.
      // The doc's own worked example currently reads "denies (zero rows)"
      // for this row; that prose needs updating alongside a real migration
      // of docs/authorize.md (out of this file's scope).
      await writeModel("doc_example.malloy", DOC_EXAMPLE);
      await expect(
         runGated(
            "doc_example.malloy",
            "run: salaries_derived -> { aggregate: n is count() }",
            { ROLE: "hr" },
         ),
      ).rejects.toBeInstanceOf(AccessDeniedError);
   });

   it("gates salaries_hr on its OWN gate, not the base's", async () => {
      await writeModel("doc_example.malloy", DOC_EXAMPLE);
      const { result } = await runGated(
         "doc_example.malloy",
         "run: salaries_hr -> { aggregate: avg_salary }",
         { ROLE: "hr" },
      );
      expect(result.data).toBeDefined();
      // `avg_salary` on zero rows: the base's own gate denies (zero rows),
      // not an AccessDeniedError — salaries_hr's gate is a row filter now.
      const { compactResult } = await runGated(
         "doc_example.malloy",
         "run: salaries_hr -> { aggregate: avg_salary }",
         { ROLE: "intern" },
      );
      const rows = compactResult as unknown as { avg_salary: number | null }[];
      expect(rows[0].avg_salary == null).toBe(true);
   });

   it("returns rows for headcount_by_dept — the join is not traced", async () => {
      // The verdict authors most need to internalize, and the one this PR
      // deliberately opened. If this ever starts denying, the doc is wrong.
      await writeModel("doc_example.malloy", DOC_EXAMPLE);
      const { result } = await runGated(
         "doc_example.malloy",
         "run: headcount_by_dept -> { aggregate: headcount }",
         {},
      );
      expect(result.data).toBeDefined();
   });
});

// Two schema-oracle holes that survived the first pass at making introspection
// agree with enforcement, both found in review. Both used to assert
// AccessDeniedError and NOT MalloyError — the compiled backstop always denied
// these, so the data path was never open, but the compile error on the way
// there leaked whether a column existed on a source the caller cannot read.
// Now every gate defers to the compiled backstop (a graft target), so the
// caller's own bad field reference surfaces FIRST, the same accepted
// narrowing as the quoted-identifier test above: no query ever executes
// either way, so no row data leaks, only whether a field name is recognized.
describe("the early gate agrees with the compiled backstop (no schema oracle)", () => {
   it("denies a derived source whose base is locked", async () => {
      // `laundered` reports its gate only if the resolution behind the early gate
      // follows `query.structRef`. Walking the `inherits` chain alone — which is
      // all the source extractor can do — leaves it looking unrestricted.
      await writeModel(
         "oracle_derived.malloy",
         `source: locked_src is duckdb.table('customers') extend {
  measure: c is count()
  #(authorize)
  internal dimension: authorized is false
}

source: laundered is locked_src -> { group_by: region }
`,
      );
      const model = await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "oracle_derived.malloy",
         getConnections(),
      );
      // A CARRIED-IN dimension gate (selfContained — declared on the base,
      // not on `laundered`'s own struct) is not the "own struct's code"
      // case `getAuthorize` special-cases; it reports the graft's own
      // quoted-identifier reference instead of re-deriving the base's
      // expression text.
      expect(model.getAuthorize("laundered")).toEqual(["`authorized`"]);
      expect(sourceNamed(model, "laundered")?.authorize).toEqual([
         "`authorized`",
      ]);
      // Guarantee changed from the string form: `laundered`'s own projection
      // (`-> { group_by: region }`) does not carry `locked_src`'s
      // "authorized" field forward, so the by-name graft fails to attach —
      // same fails-CLOSED shape BLOCKING-5 pins. That denies OUTRIGHT,
      // before the caller's own bad field reference is ever reached, so the
      // "no schema oracle" question this test used to isolate is moot here:
      // no compile of the caller's query happens at all, satisfying it more
      // strongly (nothing about the caller's query is even evaluated) but
      // via AccessDeniedError, not a surfaced compile error.
      await expect(
         runGated(
            "oracle_derived.malloy",
            "run: laundered -> { group_by: no_such_field }",
            {},
         ),
      ).rejects.toBeInstanceOf(AccessDeniedError);
   });

   // A prior version of this suite pinned "refuses an inherited gate whose
   // given collides with an entry default that would satisfy it" here — the
   // same STRING-FORM vacuous-default hazard as the deleted describe block
   // above ("an inherited gate is isolated from a colliding entry-model
   // given"), enforced by the now-deleted `assertNoVacuousDefaultAtom`. See
   // that block's comment for why this is an accepted coverage reduction,
   // not a silently dropped guarantee.
});

// Two more oracle/consistency holes, found by an independent review pass. Both
// concern a source the PACKAGE declares — no caller-declared alias involved — so
// neither is covered by the known limitation about caller-declared sources.
describe("run-target expressions do not skip the early gate", () => {
   const DECLARED = `source: rt_locked is duckdb.table('customers') extend {
  measure: cc is count()
  #(authorize)
  internal dimension: authorized is false
}

query: rt_locked_q is rt_locked -> { group_by: region }

source: rt_open is duckdb.table('customers') extend { measure: oc is count() }
`;

   // The early gate resolved a run target only as `NAME ->`. A run target can be
   // an expression over the name, and each of these skipped the pre-compile gate
   // and returned Malloy's error instead — leaking column names AND column types
   // on a locked source. That leak is real regardless of gate shape; what changed
   // is that EVERY gate now defers to the compiled backstop, so the query's own
   // bad reference (an unknown field, a type error) surfaces before the gate gets
   // a chance to run — the same accepted schema-oracle narrowing as above. No
   // query ever executes either way.
   const shapes: [string, string, RegExp][] = [
      [
         "extend {} with an unknown field",
         "run: rt_locked extend {} -> { group_by: salary }",
         /salary/,
      ],
      [
         "extend {} with a type error",
         "run: rt_locked extend {} -> { aggregate: x is sum(name) }",
         /Can't use type string/,
      ],
      [
         "a refinement of the author's named query",
         "run: rt_locked_q + { group_by: salary }",
         /salary/,
      ],
   ];
   for (const [name, query, expectedError] of shapes) {
      it(`surfaces the caller's own compile error for ${name}`, async () => {
         await writeModel("rt_expr.malloy", DECLARED);
         await expect(runGated("rt_expr.malloy", query, {})).rejects.toThrow(
            expectedError,
         );
      });
   }

   it("still reports compile errors for an UNGATED source", async () => {
      // The paired control, and the reason this is not just "deny more": widening
      // the resolver must not turn every typo into a 403 on a source anyone may
      // read. Hiding schema is only correct where the gate says so.
      await writeModel("rt_expr.malloy", DECLARED);
      await expect(
         runGated("rt_expr.malloy", "run: rt_open -> { group_by: nope }", {}),
      ).rejects.not.toBeInstanceOf(AccessDeniedError);
      const { result } = await runGated(
         "rt_expr.malloy",
         "run: rt_open extend {} -> { aggregate: oc }",
         {},
      );
      expect(result.data).toBeDefined();
   });
});

// A gate written above a standalone `source:` lands in `annotations.blockNotes`.
// The SAME gate written inside a multi-definition `source:` block
// (`source:\n  #(authorize)\n  name is ...`) lands in `annotations.notes`
// instead — same declaration slot, different key depending only on which
// syntax the author used (confirmed by compiling both and inspecting the
// resulting SourceDef). Reading `blockNotes` only reported the block form as
// ungated and served every query against it — a real access-control bypass an
// author could not detect from Publisher's own introspection response.
//
// The dimension form's gate lives in FIELD position inside `extend {}`,
// unaffected by which of these syntaxes declares the enclosing SOURCE — so
// this block's original distinguishing question (does `blockNotes` vs
// `notes` placement matter for GATE discovery) no longer applies. Kept as
// regression coverage that block-form/after-is source declarations still
// compile and enforce a field-position gate the same as the line-above form.
describe("a gate declared in a multi-definition source: block is not silently dropped", () => {
   it("gates a source declared with its own #(authorize) inside a source: block", async () => {
      await writeModel(
         "block.malloy",
         `source:
  bf_locked is duckdb.table('customers') extend {
    measure: c is count()
    #(authorize)
    internal dimension: authorized is false
  }
`,
      );
      const model = await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "block.malloy",
         getConnections(),
      );
      expect(sourceNamed(model, "bf_locked")?.authorize).toEqual(["false"]);
      expect(model.getAuthorize("bf_locked")).toEqual(["false"]);
      await expectDeniedByFilter(
         "block.malloy",
         "run: bf_locked -> { aggregate: c }",
      );
   });

   it("still gates the line-above form the same block-form model also uses", async () => {
      // Guards against a fix that special-cases the block form and stops
      // reading `blockNotes` — both forms must keep working side by side.
      await writeModel(
         "block_mixed.malloy",
         `source: bf_above is duckdb.table('customers') extend {
  measure: c is count()
  #(authorize)
  internal dimension: authorized is false
}

source:
  bf_block is duckdb.table('customers') extend {
    measure: c is count()
    #(authorize)
    internal dimension: authorized is false
  }
`,
      );
      const model = await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "block_mixed.malloy",
         getConnections(),
      );
      expect(model.getAuthorize("bf_above")).toEqual(["false"]);
      expect(model.getAuthorize("bf_block")).toEqual(["false"]);
   });

   it("does not over-gate a sibling in the same block that declares no annotation", async () => {
      // The narrowest-safe-reading control: `notes` only ever carries the
      // annotation directly above ONE block item, never a sibling's, so an
      // undecorated definition in the same block must stay unrestricted.
      await writeModel(
         "block_sibling.malloy",
         `source:
  bf_open is duckdb.table('customers') extend { measure: c is count() }
  bf_sibling_locked is duckdb.table('customers') extend {
    measure: c is count()
    #(authorize)
    internal dimension: authorized is false
  }
`,
      );
      const model = await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "block_sibling.malloy",
         getConnections(),
      );
      expect(sourceNamed(model, "bf_open")?.authorize).toBeUndefined();
      expect(model.getAuthorize("bf_open")).toEqual([]);
      // Positive half of the control: the gate has to have landed on the item
      // it precedes. Without this, an off-by-one that attributed a block item's
      // note to the PREVIOUS definition would still satisfy the assertions
      // above while leaving nothing gated at all.
      expect(model.getAuthorize("bf_sibling_locked")).toEqual(["false"]);
      const { result } = await runGated(
         "block_sibling.malloy",
         "run: bf_open -> { aggregate: c }",
         {},
      );
      expect(result.data).toBeDefined();
   });

   it("fails the load for a field/view-level annotation that merely mentions authorize-shaped text", async () => {
      // `notes`/`blockNotes` are populated only by SOURCE-level (block-item)
      // annotations. A dimension- or view-level annotation never lands on the
      // source struct's own annotations at all, so `extractSourcesFromModelDef`
      // cannot mistake it for a source gate. It IS now collected as a gate
      // -dimension candidate (see `source_extraction.ts`), so this fails via
      // `validateGateDimension`'s G1 instead: `region_copy` is a STRING
      // dimension, not a scalar boolean one.
      await writeModel(
         "block_field_level.malloy",
         `source: bf_field is duckdb.table('customers') extend {
  #(authorize) "false"
  dimension: region_copy is region
  measure: c is count()
}
`,
      );
      // `Model.create` itself does not reject here — a compile-time failure is
      // caught inside it and stashed on `compilationError`, surfaced only when
      // something asks the model for its compiled shape (`getModel()`).
      const model = await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "block_field_level.malloy",
         getConnections(),
      );
      await expect(model.getModel()).rejects.toBeInstanceOf(
         ModelCompilationError,
      );
      await expect(model.getModel()).rejects.toThrow(
         /"bf_field\.region_copy".*scalar boolean dimension/,
      );
   });

   it("carries a block-form base's gate to an extension through annotations.inherits", async () => {
      // The base's gate is filed under `annotations.inherits.notes` once the
      // extension adds its own (unrelated) annotation — the same demotion
      // `ancestorGateExprs` already follows for the line-above form.
      await writeModel(
         "block_inherit.malloy",
         `source:
  bf_base is duckdb.table('customers') extend {
    measure: c is count()
    #(authorize)
    internal dimension: authorized is false
  }

# bar_chart
source: bf_ext is bf_base extend {}
`,
      );
      const model = await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "block_inherit.malloy",
         getConnections(),
      );
      expect(model.getAuthorize("bf_ext")).toEqual(["false"]);
      await expectDeniedByFilter(
         "block_inherit.malloy",
         "run: bf_ext -> { aggregate: c }",
      );
   });

   it("carries a block-form base's gate through a query-source derivation", async () => {
      // `collectEntryPointGates` resolves a `query_source`'s base through
      // `query.structRef`, so the gate has to be readable off the BASE's own
      // block-form notes. (This does not reach `ancestorGateExprs`'s
      // `resolveDeclaredSource` branch — that one is only entered when no
      // `inherits` link survives, and no fixture here produces a declared
      // source that both carries notes and is reached that way, so the
      // sourceRegistry read stays defensive rather than covered.)
      await writeModel(
         "block_registry.malloy",
         `source:
  bf_reg_base is duckdb.table('customers') extend {
    measure: c is count()
    dimension: secret is name
    #(authorize)
    internal dimension: authorized is false
  }

source: bf_reg_derived is bf_reg_base -> { select: id, secret }
`,
      );
      // Same fails-CLOSED shape BLOCKING-5 pins: `bf_reg_derived`'s own
      // projection doesn't select "authorized" forward, so the by-name graft
      // can't attach and denies outright rather than filtering to zero rows.
      await expect(
         runGated(
            "block_registry.malloy",
            "run: bf_reg_derived -> { select: id, secret }",
            {},
         ),
      ).rejects.toBeInstanceOf(AccessDeniedError);
   });

   it("evaluates a block-form gate that references a given", async () => {
      // Every other test here uses a constant gate, which never reaches
      // `validateAuthorizeProbes` with a real `$NAME`. The realistic
      // block-form gate does, so pin both directions of the gate.
      await writeModel(
         "block_given.malloy",
         `##! experimental.givens

given:
  ROLE :: string

source:
  bf_given is duckdb.table('customers') extend {
    measure: c is count()
    #(authorize)
    internal dimension: authorized is $ROLE = 'analyst'
  }
`,
      );
      const model = await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "block_given.malloy",
         getConnections(),
      );
      expect(model.getAuthorize("bf_given")).toEqual(["$ROLE = 'analyst'"]);
      const { result } = await runGated(
         "block_given.malloy",
         "run: bf_given -> { aggregate: c }",
         { ROLE: "analyst" },
      );
      expect(result.data).toBeDefined();
      await expectDeniedByFilter(
         "block_given.malloy",
         "run: bf_given -> { aggregate: c }",
         { ROLE: "intern" },
      );
   });

   it("gates a source whose #(authorize) sits after the `is`", async () => {
      // The third producer of source-level `notes`: malloy's `getIsNotes`
      // collects annotations on either side of the `is`, so this placement was
      // dropped by the `blockNotes`-only read exactly like the block form.
      await writeModel(
         "block_afteris.malloy",
         `source: bf_afteris is
  duckdb.table('customers') extend {
    measure: c is count()
    #(authorize)
    internal dimension: authorized is false
  }
`,
      );
      const model = await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "block_afteris.malloy",
         getConnections(),
      );
      expect(model.getAuthorize("bf_afteris")).toEqual(["false"]);
      await expectDeniedByFilter(
         "block_afteris.malloy",
         "run: bf_afteris -> { aggregate: c }",
      );
   });

   it("fails model load for a block-form gate naming an undeclared given", async () => {
      // The `source_extraction.ts` half of the read, isolated. Load-time
      // validation works from the extractor's `authorizeMap`, and the Model
      // constructor later overwrites `sources[].authorize` from the
      // enforcement walk in model.ts — so this test is what pins the reporting
      // half: with a `blockNotes`-only extractor the bad gate is invisible at
      // load and the model compiles clean.
      await writeModel(
         "block_validate.malloy",
         `##! experimental.givens

given:
  ROLE :: string

source:
  bf_validate is duckdb.table('customers') extend {
    measure: c is count()
    #(authorize)
    internal dimension: authorized is $NO_SUCH_GIVEN = 'x'
  }
`,
      );
      const model = await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "block_validate.malloy",
         getConnections(),
      );
      const err = model.getNotebookError();
      expect(err).toBeDefined();
      // Surfaces Malloy's own compile error for the unresolvable given, the
      // same as the top-level "fails model load" test above — the gate's
      // expression is an ordinary dimension body now, block-declared or not.
      expect(err?.message).toMatch(/NO_SUCH_GIVEN.*not declared/i);
   });
});

// The private data-management bypass (router `dataManagementQuery`): a trusted
// internal caller runs a query with `#(authorize)` gates skipped, so indexing can
// scan a source the author tagged `#(index)` but gated against the machine
// identity that does the scanning.
//
// What is disabled is ONLY expressions collected from `#(authorize)`
// annotations. The author's own `where:` is part of the source's definition
// and is never bypassed — that is the whole authoring contract, and the
// `where:`-still-applies test below is the one that pins it.
describe("authorize bypass (private data-management path)", () => {
   const GATED = `##! experimental.givens

given:
  ROLE :: string

source: dm_gated is duckdb.table('customers') extend {
  measure: c is count()
  #(authorize)
  internal dimension: authorized is $ROLE = 'analyst'
}
`;

   // The canonical hard case from the design doc: an access GATE and an ordinary
   // analytical `where:` on the same source. The gate is bypassable; the `where:`
   // is not.
   const GATED_PLUS_WHERE = `##! experimental.givens

given:
  ROLE :: string

source: dm_mixed is duckdb.table('customers') extend {
  where: region = 'us-west'
  measure: c is count()
  #(authorize)
  internal dimension: authorized is $ROLE = 'analyst'
}
`;

   // `compactResult` is the row array; the aggregate lands in `c`.
   function countOf(compactResult: unknown): number {
      const rows = compactResult as Array<Record<string, unknown>>;
      return Number(rows[0]?.c);
   }

   it("returns rows for a gated source that would otherwise 403", async () => {
      await writeModel("dm_gated.malloy", GATED);
      // No ROLE supplied at all — exactly the indexer's position.
      const result = await runGated(
         "dm_gated.malloy",
         "run: dm_gated -> { aggregate: c }",
         {},
         undefined,
         true,
      );
      expect(countOf(result.compactResult)).toBe(2);
   });

   // Regression pin. If this ever stops throwing, the bypass has stopped being
   // opt-in and every gate in the product is off.
   it("still denies the same query without the bypass", async () => {
      await writeModel("dm_gated.malloy", GATED);
      // `ROLE` has no default (deliberately — a caller must supply one), so
      // enforced-and-unsatisfied surfaces as a MalloyError, not
      // `Model`-issued `AccessDeniedError` — every gate is a row filter now.
      await expect(
         runGated("dm_gated.malloy", "run: dm_gated -> { aggregate: c }", {}),
      ).rejects.toThrow();
   });

   it("defaults to enforcing when the flag is explicitly false", async () => {
      await writeModel("dm_gated.malloy", GATED);
      await expect(
         runGated(
            "dm_gated.malloy",
            "run: dm_gated -> { aggregate: c }",
            {},
            undefined,
            false,
         ),
      ).rejects.toThrow();
   });

   // CRITICAL. A bypass that also dropped the author's `where:` would silently
   // widen every scan past the source's own definition — and would break the
   // v2 rule that the footprint covers gates only, never analytical narrowing.
   it("still applies the author's own where: under the bypass", async () => {
      await writeModel("dm_mixed.malloy", GATED_PLUS_WHERE);
      const result = await runGated(
         "dm_mixed.malloy",
         "run: dm_mixed -> { aggregate: c }",
         {},
         undefined,
         true,
      );
      // 2 rows exist; `where: region = 'us-west'` keeps exactly 1. Seeing 2 here
      // would mean the bypass reached past the gate into the source definition.
      expect(countOf(result.compactResult)).toBe(1);
   });

   // Isolation, forward direction. The reverse ("bypassFilters does not disable
   // the gate") is pinned separately in the runtime-gate describe above.
   it("does not imply bypassFilters", async () => {
      await writeModel("dm_mixed.malloy", GATED_PLUS_WHERE);
      const result = await runGated(
         "dm_mixed.malloy",
         "run: dm_mixed -> { aggregate: c }",
         {},
         undefined, // bypassFilters deliberately NOT set
         true,
      );
      expect(countOf(result.compactResult)).toBe(1);
   });

   describe("emits telemetry on every use", () => {
      let harness: MetricsHarness;
      const COUNTER = "publisher_authorize_bypass_total";

      beforeEach(async () => {
         harness = await startMetricsHarness();
         resetAuthorizeGuardTelemetryForTesting();
      });

      afterEach(async () => {
         resetAuthorizeGuardTelemetryForTesting();
         await harness.shutdown();
      });

      // Asserts the counter, not the status code: a bypass that ran but went
      // uncounted is exactly the failure that leaves an audit with no rate
      // signal behind it.
      it("counts both gate entry points once each", async () => {
         await writeModel("dm_gated.malloy", GATED);
         await runGated(
            "dm_gated.malloy",
            "run: dm_gated -> { aggregate: c }",
            {},
            undefined,
            true,
         );
         // The early surface-syntax gate...
         expect(
            await harness.collectCounter(COUNTER, { entry_point: "source" }),
         ).toBe(1);
         // ...and the compiled backstop. Exactly one each: the walk
         // short-circuits before its own nested assertAuthorized call.
         expect(
            await harness.collectCounter(COUNTER, { entry_point: "runnable" }),
         ).toBe(1);
      });

      /**
       * The audit line is the compensating control for a bypass, so the source it
       * names is the whole point. `assertAuthorizedForAllSources` used to return
       * before resolving it and log `"(query)"` — and it did that on exactly the
       * request where nothing else records the target: an ad-hoc query whose run
       * target cannot be pinned from surface syntax before compilation, so the
       * `source` entry point never fires either. That left the one case an
       * investigator most needs with package and model but no source at all.
       */
      it('names the source on the runnable audit, not "(query)"', async () => {
         await writeModel("dm_gated.malloy", GATED);
         const lines: Array<Record<string, unknown>> = [];
         const info = logger.info;
         (logger as { info: unknown }).info = (
            message: string,
            meta?: Record<string, unknown>,
         ) => {
            if (message === "authorize bypass" && meta) lines.push(meta);
            return undefined as never;
         };
         try {
            // Declared in the caller's own text, so `earlySource` resolves to
            // nothing and `runnable` is the only emission there is.
            await runGated(
               "dm_gated.malloy",
               "source: mine is dm_gated extend {}\nrun: mine -> { aggregate: c }",
               {},
               undefined,
               true,
            );
         } finally {
            (logger as { info: unknown }).info = info;
         }

         const runnable = lines.filter((l) => l.entryPoint === "runnable");
         expect(runnable.length).toBe(1);
         expect(runnable[0]?.sourceName).not.toBe("(query)");
         expect(runnable[0]).toMatchObject({
            modelPath: "dm_gated.malloy",
            packageName: "test-pkg",
         });
      });

      it("counts nothing when the bypass is not used", async () => {
         await writeModel(
            "dm_open.malloy",
            "source: dm_open is duckdb.table('customers') extend { measure: c is count() }\n",
         );
         await runGated(
            "dm_open.malloy",
            "run: dm_open -> { aggregate: c }",
            {},
         );
         expect(await harness.collectCounter(COUNTER)).toBe(0);
      });
   });

   // How the flag actually arrives in production: off the request header, through
   // the same reader server.ts calls. The reader's own edge cases live in
   // authorize_bypass_header.spec.ts; what these two add is the composition —
   // that a header reaches the gate and that a body field does not.
   describe("arrives on the header, not the body", () => {
      it("honours the bypass when the header carries it", async () => {
         await writeModel("dm_gated.malloy", GATED);
         const result = await runGated(
            "dm_gated.malloy",
            "run: dm_gated -> { aggregate: c }",
            {},
            undefined,
            readBypassAuthorize({
               headers: { [BYPASS_AUTHORIZE_HEADER]: "true" },
            }),
         );
         expect(countOf(result.compactResult)).toBe(2);
      });

      // The whole safety argument for putting this on a header rather than in
      // `QueryRequest`. Without this test a refactor that reads the body again
      // reopens the hole with every other test still green.
      it("leaves gates enforced for a bypassAuthorize field in the body", async () => {
         await writeModel("dm_gated.malloy", GATED);
         // Held in a variable so `body` reaches the reader at all: passed inline,
         // excess-property checking rejects it, which is itself the type-level
         // half of this guarantee.
         const bodyOnlyRequest = {
            headers: {},
            body: { bypassAuthorize: true },
         };
         await expect(
            runGated(
               "dm_gated.malloy",
               "run: dm_gated -> { aggregate: c }",
               {},
               undefined,
               readBypassAuthorize(bodyOnlyRequest),
            ),
         ).rejects.toThrow();
      });
   });
});
