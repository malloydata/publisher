import { DuckDBConnection } from "@malloydata/db-duckdb";
import { Connection, GivenValue } from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { AccessDeniedError } from "../errors";
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
   it("collects file-level then source-level expressions as one list", async () => {
      await writeModel(
         "disjunction.malloy",
         `##! experimental.givens

given:
  ROLE :: string
  REGION :: string

##(authorize) "$ROLE = 'admin'"

#(authorize) "$REGION = 'us-west'"
source: regional is duckdb.table('customers')
`,
      );
      const model = await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "disjunction.malloy",
         getConnections(),
      );

      // File-level first, then the source's own.
      expect(model.getAuthorize("regional")).toEqual([
         "$ROLE = 'admin'",
         "$REGION = 'us-west'",
      ]);
      expect(sourceNamed(model, "regional")?.authorize).toEqual([
         "$ROLE = 'admin'",
         "$REGION = 'us-west'",
      ]);
   });

   it("does NOT inherit a base source's authorize through extend", async () => {
      await writeModel(
         "extend.malloy",
         `##! experimental.givens

given:
  ROLE :: string

// Locked base.
#(authorize) "false"
source: customers_raw is duckdb.table('customers')

// Extension with its own gate — must NOT pick up the base's "false".
#(authorize) "$ROLE = 'analyst'"
source: customers_marketing is customers_raw extend {
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

   it("applies a file-level gate to a source with no own authorize", async () => {
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

      expect(model.getAuthorize("plain")).toEqual(["$ROLE = 'admin'"]);
   });

   it("applies a file-level gate inherited from an imported model", async () => {
      // Regression for the hand-rolled model-annotation fold (malloy 0.0.405+
      // removed ModelDef.annotation): a `##(authorize)` declared in an
      // imported file must flow into the importing file's file-level gate even
      // when the importer declares no `##` of its own. The fold must match
      // malloy's getModelAnnotations (skip empty-ownNotes links) or `.notes`
      // returns [] here and the gate silently drops — fail-open.
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

      expect(model.getAuthorize("inherited_gate")).toEqual(["$ROLE = 'admin'"]);
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

#(authorize) "$ROLE = 'analyst'"
source: gated is duckdb.table('customers')
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

#(authorize) "$NOPE = 'x'"
source: gated is duckdb.table('customers')
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
      // Names the source and surfaces the underlying Malloy reason.
      expect(err?.message).toContain("gated");
      expect(err?.message).toMatch(/NOPE|not declared/i);
      // Redaction policy (pinned): the model-load 424 is author-facing, so it
      // KEEPS the full expression text (needed to fix a malformed annotation).
      // Only the runtime 403 redacts to the source name. If this assertion ever
      // flips, the redaction split was changed — make it a conscious decision.
      expect(err?.message).toContain("$NOPE = 'x'");
   });

   it("fails model load when an expression references a source field", async () => {
      await writeModel(
         "field_ref.malloy",
         `#(authorize) "some_field = 1"
source: gated is duckdb.table('customers')
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
      expect(err?.message).toContain("gated");
   });

   it("does not reject a type-mismatched comparison (not a Malloy compile error)", async () => {
      // Documents the boundary: `$ROLE = 5` is not a compile error; such a gate
      // simply evaluates per the warehouse at the runtime gate.
      await writeModel(
         "type_mismatch.malloy",
         `##! experimental.givens

given:
  ROLE :: string

#(authorize) "$ROLE = 5"
source: gated is duckdb.table('customers')
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

   it("fails model load when a file-level ##(authorize) references an unknown given", async () => {
      await writeModel(
         "file_unknown_given.malloy",
         `##! experimental.givens

given:
  ROLE :: string

##(authorize) "$NOPE = 'x'"

source: gated is duckdb.table('customers')
`,
      );
      const model = await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "file_unknown_given.malloy",
         getConnections(),
      );

      const err = model.getNotebookError();
      expect(err).toBeDefined();
      expect(err?.message).toContain("gated");
      expect(err?.message).toMatch(/NOPE|not declared/i);
   });

   it("validates expressions over number and list givens, not just strings", async () => {
      await writeModel(
         "given_types.malloy",
         `##! experimental.givens

given:
  AGE :: number
  TENANT :: string
  ALLOWED :: string[]

#(authorize) "$AGE > 18"
#(authorize) "$TENANT in $ALLOWED"
source: gated is duckdb.table('customers')
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
         "$AGE > 18",
         "$TENANT in $ALLOWED",
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
   );
}

describe("authorize runtime gate", () => {
   const SINGLE_GATE = `##! experimental.givens

given:
  ROLE :: string

#(authorize) "$ROLE = 'analyst'"
source: gated is duckdb.table('customers') extend { measure: c is count() }
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

   it("denies (403) when no given satisfies the gate", async () => {
      await writeModel("rt_single.malloy", SINGLE_GATE);
      await expect(
         runGated("rt_single.malloy", "run: gated -> { aggregate: c }", {
            ROLE: "intern",
         }),
      ).rejects.toBeInstanceOf(AccessDeniedError);
   });

   it("denies when the referenced given has no value (fail closed)", async () => {
      await writeModel("rt_single.malloy", SINGLE_GATE);
      await expect(
         runGated("rt_single.malloy", "run: gated -> { aggregate: c }", {}),
      ).rejects.toBeInstanceOf(AccessDeniedError);
   });

   it("still enforces the gate when bypassFilters is true (authorize is not a filter)", async () => {
      await writeModel("rt_single.malloy", SINGLE_GATE);
      await expect(
         runGated(
            "rt_single.malloy",
            "run: gated -> { aggregate: c }",
            { ROLE: "intern" },
            true,
         ),
      ).rejects.toBeInstanceOf(AccessDeniedError);
   });

   // A model that declares no `##` of its own and inherits its file-level gate
   // entirely from an imported model. Exercises the model-annotation fold end
   // to end: parse → fold → fileLevelAuthorize → runtime gate.
   const IMPORT_BASE = `##! experimental.givens

given:
  ROLE :: string

##(authorize) "$ROLE = 'admin'"
`;
   const IMPORT_GATED = `import "rt_auth_base.malloy"

source: inherited_gate is duckdb.table('customers') extend {
  measure: c is count()
}
`;

   it("enforces a file-level gate inherited from an imported model (allow with role)", async () => {
      await writeModel("rt_auth_base.malloy", IMPORT_BASE);
      await writeModel("rt_import.malloy", IMPORT_GATED);
      const { result } = await runGated(
         "rt_import.malloy",
         "run: inherited_gate -> { aggregate: c }",
         { ROLE: "admin" },
      );
      expect(result.data).toBeDefined();
   });

   it("enforces a file-level gate inherited from an imported model (deny without role — not fail-open)", async () => {
      await writeModel("rt_auth_base.malloy", IMPORT_BASE);
      await writeModel("rt_import.malloy", IMPORT_GATED);
      await expect(
         runGated(
            "rt_import.malloy",
            "run: inherited_gate -> { aggregate: c }",
            {
               ROLE: "intern",
            },
         ),
      ).rejects.toBeInstanceOf(AccessDeniedError);
   });

   const DISJUNCTION = `##! experimental.givens

given:
  ROLE :: string
  REGION :: string

##(authorize) "$ROLE = 'admin'"

#(authorize) "$REGION = 'us-west'"
source: regional is duckdb.table('customers') extend { measure: c is count() }
`;

   it("grants on the file-level gate even when the OTHER disjunct's given is missing", async () => {
      // The key OR-semantics case: admin supplies only ROLE; REGION is absent.
      // A disjunct that can't evaluate (missing given) must not sink the whole
      // request — the satisfied $ROLE='admin' branch still grants.
      await writeModel("rt_disj.malloy", DISJUNCTION);
      const { result } = await runGated(
         "rt_disj.malloy",
         "run: regional -> { aggregate: c }",
         { ROLE: "admin" },
      );
      expect(result.data).toBeDefined();
   });

   it("grants on the source-level gate even when the file-level disjunct's given is missing", async () => {
      await writeModel("rt_disj.malloy", DISJUNCTION);
      const { result } = await runGated(
         "rt_disj.malloy",
         "run: regional -> { aggregate: c }",
         { REGION: "us-west" },
      );
      expect(result.data).toBeDefined();
   });

   it("denies when neither disjunct is satisfied", async () => {
      await writeModel("rt_disj.malloy", DISJUNCTION);
      await expect(
         runGated("rt_disj.malloy", "run: regional -> { aggregate: c }", {
            ROLE: "nobody",
            REGION: "nowhere",
         }),
      ).rejects.toBeInstanceOf(AccessDeniedError);
   });

   it("gates a named query that targets a gated source (no sourceName supplied)", async () => {
      await writeModel(
         "rt_namedq.malloy",
         `##! experimental.givens

given:
  ROLE :: string

#(authorize) "$ROLE = 'analyst'"
source: gated is duckdb.table('customers') extend { measure: c is count() }

query: secret is gated -> { aggregate: c }
`,
      );
      const model = await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "rt_namedq.malloy",
         getConnections(),
      );
      // Named query, no sourceName — must still resolve to `gated` and gate it.
      await expect(
         model.getQueryResults(
            undefined,
            "secret",
            undefined,
            undefined,
            false,
            {
               ROLE: "intern",
            },
         ),
      ).rejects.toBeInstanceOf(AccessDeniedError);
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
      // it as absent and runs the ad-hoc query.
      await expect(
         model.getQueryResults(
            "",
            undefined,
            "run: gated -> { aggregate: c }",
            undefined,
            false,
            { ROLE: "intern" },
         ),
      ).rejects.toBeInstanceOf(AccessDeniedError);
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

#(authorize) "$ROLE = 'analyst'"
source: gated is duckdb.table('customers') extend { measure: c is count() }
`,
      );
      await expect(
         runGated(
            "rt_multi.malloy",
            "run: ungated -> { aggregate: c }\nrun: gated -> { aggregate: c }",
            { ROLE: "intern" },
         ),
      ).rejects.toBeInstanceOf(AccessDeniedError);
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

   const FILE_LEVEL = `##! experimental.givens

given:
  ROLE :: string

##(authorize) "$ROLE = 'admin'"

source: declared is duckdb.table('customers') extend { measure: c is count() }
`;

   it("applies a file-level gate to an ad-hoc query (no gate bypass)", async () => {
      await writeModel("rt_filelevel.malloy", FILE_LEVEL);
      // The model-wide file-level gate must apply to any ad-hoc query against
      // the model, denying a non-admin with a 403.
      await expect(
         runGated("rt_filelevel.malloy", "run: declared -> { aggregate: c }", {
            ROLE: "user",
         }),
      ).rejects.toBeInstanceOf(AccessDeniedError);
      // An admin (file-level gate satisfied) can run it.
      const { result } = await runGated(
         "rt_filelevel.malloy",
         "run: declared -> { aggregate: c }",
         { ROLE: "admin" },
      );
      expect(result.data).toBeDefined();
   });

   it("rejects an ad-hoc inline-SQL query (restricted mode closes the raw-warehouse path)", async () => {
      // Pre-#807 the file-level #(authorize) gate was the only thing between a
      // caller and raw `duckdb.sql(...)`. #807's restricted mode now rejects
      // inline raw SQL outright while resolving the compiled source — before the
      // gate is reached — so the raw-warehouse path is closed at the compile
      // layer regardless of givens (even for an admin who satisfies the gate).
      await writeModel("rt_filelevel.malloy", FILE_LEVEL);
      await expect(
         runGated(
            "rt_filelevel.malloy",
            `run: duckdb.sql("SELECT 1 AS id") -> { aggregate: c is count() }`,
            { ROLE: "admin" },
         ),
      ).rejects.toThrow(/raw SQL is not permitted/);
   });

   const MIXED_SOURCE_GATE = `##! experimental.givens

given:
  ROLE :: string

#(authorize) "$ROLE = 'analyst'"
source: gated is duckdb.table('customers') extend { measure: c is count() }

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

   it("gates a quoted-identifier source BEFORE compilation (no schema oracle)", async () => {
      // A gated source whose Malloy name must be quoted (here, a hyphen) must be
      // recognized by the early gate too. Otherwise a denied caller could probe
      // a non-existent field and learn the schema from a pre-compilation Malloy
      // field error instead of a clean 403.
      await writeModel(
         "rt_quoted.malloy",
         `##! experimental.givens

given:
  ROLE :: string

#(authorize) "$ROLE = 'analyst'"
source: \`gated-source\` is duckdb.table('customers') extend {
  measure: c is count()
}
`,
      );
      // Probing a field that doesn't exist must deny (403) before compilation,
      // not surface a Malloy "field not found" error.
      await expect(
         runGated(
            "rt_quoted.malloy",
            "run: `gated-source` -> { group_by: no_such_field }",
            { ROLE: "viewer" },
         ),
      ).rejects.toBeInstanceOf(AccessDeniedError);
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

#(authorize) "$ROLE = 'analyst'"
source: gated is duckdb.table('customers') extend { measure: c is count() }
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
      // Cell 1 is `run: secret` — must be denied without the gate-passing given.
      await expect(
         model.executeNotebookCell(1, undefined, false, { ROLE: "intern" }),
      ).rejects.toBeInstanceOf(AccessDeniedError);
      // ...and allowed when the gate passes.
      const ok = await model.executeNotebookCell(1, undefined, false, {
         ROLE: "analyst",
      });
      expect(ok.result).toBeDefined();
   });

   const LOCKED_BASE = `##! experimental.givens

given:
  ROLE :: string

#(authorize) "false"
source: base_locked is duckdb.table('customers') extend { measure: c is count() }

#(authorize) "$ROLE = 'analyst'"
source: ext_gated is base_locked extend {}

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

   it('denies a direct query against a base locked with #(authorize) "false"', async () => {
      await writeModel("rt_locked.malloy", LOCKED_BASE);
      await expect(
         runGated("rt_locked.malloy", "run: base_locked -> { aggregate: c }", {
            ROLE: "analyst",
         }),
      ).rejects.toBeInstanceOf(AccessDeniedError);
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

   it("denies an extension that declares no own gate — it inherits the base lock (safe default)", async () => {
      // Malloy carries the base's #(authorize) onto an extension UNLESS the
      // extension declares its own. So a bare `is base_locked extend {}` with
      // no own gate stays locked by the base's "false". An extension escapes
      // the base gate only by declaring its own #(authorize) (see ext_gated).
      await writeModel("rt_locked.malloy", LOCKED_BASE);
      await expect(
         runGated("rt_locked.malloy", "run: ext_nogate -> { aggregate: c }", {
            ROLE: "analyst",
         }),
      ).rejects.toBeInstanceOf(AccessDeniedError);
   });

   it("a query joining a locked base via an ungated source is denied", async () => {
      await writeModel("rt_locked.malloy", LOCKED_BASE);
      await expect(
         runGated("rt_locked.malloy", "run: joiner -> { aggregate: c }", {}),
      ).rejects.toBeInstanceOf(AccessDeniedError);
   });

   it("denies a transitive A→B→C join where C is locked", async () => {
      await writeModel("rt_locked.malloy", LOCKED_BASE);
      await expect(
         runGated("rt_locked.malloy", "run: top_join -> { aggregate: c }", {}),
      ).rejects.toBeInstanceOf(AccessDeniedError);
   });

   it("denies an aliased join (join_one: b is base_locked) — alias resolution can't dodge the gate", async () => {
      await writeModel("rt_locked.malloy", LOCKED_BASE);
      await expect(
         runGated(
            "rt_locked.malloy",
            "run: joiner_aliased -> { aggregate: c }",
            {},
         ),
      ).rejects.toBeInstanceOf(AccessDeniedError);
   });

   it("denies a query joining one gated and one ungated source (AND across sources)", async () => {
      await writeModel("rt_locked.malloy", LOCKED_BASE);
      await expect(
         runGated(
            "rt_locked.malloy",
            "run: mixed_joiner -> { aggregate: c }",
            {},
         ),
      ).rejects.toBeInstanceOf(AccessDeniedError);
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

   const FILE_LEVEL_OVERRIDE_WITH_JOIN = `##! experimental.givens

given:
  ROLE :: string

##(authorize) "$ROLE = 'admin'"

#(authorize) "false"
source: fl_locked is duckdb.table('customers') extend { measure: c is count() }

source: fl_joiner is duckdb.table('customers') extend {
  join_one: fl_locked on id = fl_locked.id
  measure: c is count()
}
`;

   it("allows a locked join for admin givens via the file-level ##(authorize) override", async () => {
      await writeModel("rt_fl_override.malloy", FILE_LEVEL_OVERRIDE_WITH_JOIN);
      const { result } = await runGated(
         "rt_fl_override.malloy",
         "run: fl_joiner -> { aggregate: c }",
         { ROLE: "admin" },
      );
      expect(result.data).toBeDefined();
   });

   it("denies a locked join for a non-admin given despite the file-level override existing", async () => {
      await writeModel("rt_fl_override.malloy", FILE_LEVEL_OVERRIDE_WITH_JOIN);
      await expect(
         runGated(
            "rt_fl_override.malloy",
            "run: fl_joiner -> { aggregate: c }",
            { ROLE: "analyst" },
         ),
      ).rejects.toBeInstanceOf(AccessDeniedError);
   });

   it("denies a notebook cell whose run target joins a locked base", async () => {
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
      await expect(
         model.executeNotebookCell(1, undefined, false, {}),
      ).rejects.toBeInstanceOf(AccessDeniedError);
   });
});

// BLOCKING-1: `hasAuthorize()` only inspects `this.sources` (built from
// entry-file `modelDef.contents`), so a gated source reached ONLY via a
// cross-file/deep-transitive join is invisible to it and the joined-gate
// walk was skipped entirely. Three files: a locked base, a mid file that
// imports it and joins it, and an entry file that imports the mid file and
// joins IT — never naming (or importing directly) the locked base, and
// declaring no gate of its own.
describe("authorize deep cross-file join enforcement (BLOCKING-1)", () => {
   it("denies a query whose run target reaches a locked source only via a two-hop cross-file join", async () => {
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
      await expect(
         runGated("c_entry.malloy", "run: top -> { aggregate: c }", {}),
      ).rejects.toBeInstanceOf(AccessDeniedError);
   });
});

// SHOULD-1: `extractSourcesFromModelDef` only validates #(authorize) syntax
// for TOP-LEVEL `modelDef.contents` sources at model load — a source reached
// ONLY via a join (never itself imported/named at the entry file, same shape
// as the BLOCKING-1 repro) is never probed there, so a malformed gate on it
// does NOT fail model load. The runtime join walk must still fail CLOSED when
// it hits that malformed annotation, not silently treat it as ungated.
describe("authorize fail-closed on a malformed join-only gate (SHOULD-1)", () => {
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

   it("denies (fail closed) rather than silently admitting through the malformed gate", async () => {
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
      await expect(
         runGated("d_entry.malloy", "run: top -> { aggregate: c }", {}),
      ).rejects.toBeInstanceOf(AccessDeniedError);
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

#(authorize) "false"
source: locked_src is duckdb.table('customers') extend {
  measure: c is count()
  dimension: locked_flag is 1
}

source: combo is compose(open_src, locked_src)
`;

   it("denies a query that resolves the composite to the locked member branch", async () => {
      await writeModel("c_composite.malloy", COMPOSITE_MODEL);
      await expect(
         runGated(
            "c_composite.malloy",
            "run: combo -> { aggregate: c; group_by: locked_flag }",
            {},
         ),
      ).rejects.toBeInstanceOf(AccessDeniedError);
   });

   it("allows a query that resolves the composite to the open member branch", async () => {
      await writeModel("c_composite.malloy", COMPOSITE_MODEL);
      const { result } = await runGated(
         "c_composite.malloy",
         "run: combo -> { aggregate: c; group_by: open_flag }",
         {},
      );
      expect(result.data).toBeDefined();
   });

   // `Query.compositeResolvedSourceDef` is what lets assertAuthorizedForAllSources
   // gate the ONE concrete branch a composite run target resolved to (see
   // model.ts's resolveRunTargetStruct/assertAuthorizedForAllSources) instead
   // of every member. This confirms it's populated even when the query
   // references NO field that discriminates between members (just the shared
   // `c` measure) — Malloy's composite resolver still picks a concrete first
   // candidate rather than leaving the resolution unset, so the gate still
   // fires on whichever member happens to resolve first.
   it("still denies when the composite resolves with no field forcing a choice (locked member listed first)", async () => {
      await writeModel(
         "c_composite_no_disambiguator.malloy",
         `##! experimental.composite_sources

source: open_src is duckdb.table('customers') extend { measure: c is count() }

#(authorize) "false"
source: locked_src is duckdb.table('customers') extend { measure: c is count() }

source: combo_locked_first is compose(locked_src, open_src)
`,
      );
      await expect(
         runGated(
            "c_composite_no_disambiguator.malloy",
            "run: combo_locked_first -> { aggregate: c }",
            {},
         ),
      ).rejects.toBeInstanceOf(AccessDeniedError);
   });
});

// BLOCKING-3: codex vs. Fable disagreed on whether a QUERY-LOCAL `join_one`
// declared inside a `-> { ... }` refinement (rather than on the source
// itself) bypasses the gate — the join then lives on the query pipeline's
// segment/extendSource, not on `open_src.fields`. Exercise codex's exact
// shape and report which way it actually goes.
describe("authorize query-local join_one enforcement (BLOCKING-3)", () => {
   it("gates a source joined ONLY inside a query's own -> { ... } refinement (not on the source definition)", async () => {
      await writeModel(
         "c_query_local_join.malloy",
         `source: open_src is duckdb.table('customers') extend { measure: c is count() }

#(authorize) "false"
source: locked_src is duckdb.table('customers') extend {
  measure: c is count()
  dimension: secret is name
}
`,
      );
      await expect(
         runGated(
            "c_query_local_join.malloy",
            `run: open_src -> {
  join_one: locked_src on id = locked_src.id
  group_by: locked_src.secret
}`,
            {},
         ),
      ).rejects.toBeInstanceOf(AccessDeniedError);
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

#(authorize) "$ROLE = 'analyst'"
source: gated is duckdb.table('customers') extend { measure: c is count() }

source: open_src is duckdb.table('customers') extend { measure: c is count() }
`;
   const CP_FILE_LEVEL = `##! experimental.givens

given:
  ROLE :: string

##(authorize) "$ROLE = 'admin'"

source: declared is duckdb.table('customers') extend { measure: c is count() }
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

   it("assertAuthorizedForText denies/allows a gated named source by its given", async () => {
      const model = await cpModel("cp_gate.malloy", CP_GATE);
      await expect(
         model.assertAuthorizedForText("run: gated -> { aggregate: c }", {}),
      ).rejects.toBeInstanceOf(AccessDeniedError);
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

   it("assertAuthorizedForText applies the model-wide file-level gate to inline/unnamed text", async () => {
      const model = await cpModel("cp_file.malloy", CP_FILE_LEVEL);
      // No named source the regex recognizes -> undefined -> file-level gate.
      await expect(
         model.assertAuthorizedForText(
            `run: duckdb.sql("SELECT 1 AS x") -> { aggregate: n is count() }`,
            {},
         ),
      ).rejects.toBeInstanceOf(AccessDeniedError);
      await expect(
         model.assertAuthorizedForText(
            `run: duckdb.sql("SELECT 1 AS x") -> { aggregate: n is count() }`,
            { ROLE: "admin" },
         ),
      ).resolves.toBeUndefined();
   });

   it("assertAuthorizedForRunnable gates the compiled-source structRef (alias backstop)", async () => {
      const model = await cpModel("cp_gate.malloy", CP_GATE);
      // Stub a runnable whose compiled query reads `gated` (e.g. via an alias
      // the surface-syntax gate would miss).
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

   it("assertAuthorizedForRunnable walks joined sources on the /compile path", async () => {
      const model = await cpModel("cp_join.malloy", CP_JOIN);
      // structRef "cp_joiner" resolves to its real SourceDef (with the join
      // field), so the /compile backstop must gate the locked joined source
      // too — not just the ungated run target.
      const joinerRunnable = {
         getPreparedQuery: async () => ({
            _query: { structRef: "cp_joiner" },
         }),
      };
      await expect(
         model.assertAuthorizedForRunnable(joinerRunnable, {}),
      ).rejects.toBeInstanceOf(AccessDeniedError);
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

#(authorize) "$ROLE = 'analyst'"
source: regional is duckdb.table('customers') extend {
  measure: c is count()
  view: in_region is {
    where: region = $REGION
    aggregate: c
  }
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
      await expect(runComposed({ REGION: "us-west" })).rejects.toBeInstanceOf(
         AccessDeniedError,
      );
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

// BLOCKING-4: a GIVEN-based #(authorize) gate on a source reached only
// through a TWO-HOP transitive import over-denied every caller regardless of
// the supplied given's value. Root cause: evaluateAuthorize's probe compiled
// against the ENTRY model's modelMaterializer, but `$ROLE` is declared in
// g_base — two import hops away — and Malloy's given-namespace merge covers
// only one hop of import, so the probe failed to compile and was swallowed
// by the catch-and-deny in evaluateAuthorize, fail-closed regardless of
// value. Fixed by making each probe self-contained (bindProbeGivens):
// declare + bind just the givens an expression references, inferring type
// from the supplied value, as a fallback when the ambient-namespace probe
// fails to compile. (One-hop cases still resolve ambiently on the first
// try — ambient-declares-ROLE is the common case and is left unchanged, see
// `evaluateAuthorize`.) The self-contained probe fix alone isn't sufficient
// end to end: the REAL query also has to accept a `ROLE` given the entry
// model doesn't itself surface, which required filterGivensToModelSurface to
// stop forwarding an authorize-only given to the real query (Malloy's
// `resolveSuppliedGivens` rejects any given a model doesn't surface).
describe("authorize given-based gate across a two-hop transitive import (BLOCKING-4)", () => {
   const G_BASE = `##! experimental.givens

given:
  ROLE :: string

#(authorize) "$ROLE = 'analyst'"
source: base_gated is duckdb.table('customers') extend { measure: c is count() }
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

   it("allows a two-hop transitively-imported given-based gate when the correct given is supplied", async () => {
      await writeModel("g_base.malloy", G_BASE);
      await writeModel("g_mid.malloy", G_MID);
      await writeModel("g_entry.malloy", G_ENTRY);
      const { result } = await runGated(
         "g_entry.malloy",
         "run: g_top -> { aggregate: c }",
         { ROLE: "analyst" },
      );
      expect(result.data).toBeDefined();
   });

   it("still denies the two-hop gate when the wrong given is supplied", async () => {
      await writeModel("g_base.malloy", G_BASE);
      await writeModel("g_mid.malloy", G_MID);
      await writeModel("g_entry.malloy", G_ENTRY);
      await expect(
         runGated("g_entry.malloy", "run: g_top -> { aggregate: c }", {
            ROLE: "intern",
         }),
      ).rejects.toBeInstanceOf(AccessDeniedError);
   });

   it("still denies a two-hop transitive join to a constant-`false`-gated source", async () => {
      // Control: the pre-existing BLOCKING-1 case (a constant gate, no given
      // involved) must still deny — the self-contained-probe fallback must
      // not accidentally turn an unconditional deny into an allow.
      await writeModel(
         "cf_base.malloy",
         `#(authorize) "false"
source: base_locked is duckdb.table('customers') extend { measure: c is count() }
`,
      );
      await writeModel(
         "cf_mid.malloy",
         `import "cf_base.malloy"

source: cf_mid is duckdb.table('customers') extend {
  join_one: base_locked on id = base_locked.id
  measure: c is count()
}
`,
      );
      await writeModel(
         "cf_entry.malloy",
         `import "cf_mid.malloy"

source: cf_top is duckdb.table('customers') extend {
  join_one: cf_mid on id = cf_mid.id
  measure: c is count()
}
`,
      );
      await expect(
         runGated("cf_entry.malloy", "run: cf_top -> { aggregate: c }", {}),
      ).rejects.toBeInstanceOf(AccessDeniedError);
   });

   it("still allows a ONE-hop given-based gate on a joined source (no regression)", async () => {
      // Control: a joined source whose gate's given is declared in a
      // DIRECTLY imported file (one hop) already resolved ambiently before
      // this fix — confirm the self-contained fallback didn't change that.
      await writeModel(
         "oh_base.malloy",
         `##! experimental.givens

given:
  ROLE :: string

#(authorize) "$ROLE = 'analyst'"
source: base_gated is duckdb.table('customers') extend { measure: c is count() }
`,
      );
      await writeModel(
         "oh_entry.malloy",
         `import "oh_base.malloy"

source: oh_top is duckdb.table('customers') extend {
  join_one: base_gated on id = base_gated.id
  measure: c is count()
}
`,
      );
      const { result } = await runGated(
         "oh_entry.malloy",
         "run: oh_top -> { aggregate: c }",
         { ROLE: "analyst" },
      );
      expect(result.data).toBeDefined();
      await expect(
         runGated("oh_entry.malloy", "run: oh_top -> { aggregate: c }", {
            ROLE: "intern",
         }),
      ).rejects.toBeInstanceOf(AccessDeniedError);
   });

   // Name-collision isolation: the entry model declares its OWN `$LEVEL`
   // given (default 99), and a deep-joined source's gate ALSO references
   // `$LEVEL` but means its own, unrelated given. Root cause: evaluateAuthorize
   // tried the AMBIENT probe first — compiled and evaluated against the
   // ENTRY model's `modelMaterializer` — so on a name collision it happily
   // compiled against the entry model's OWN `$LEVEL` default and granted a
   // caller who supplied no `LEVEL` at all, never falling back to the
   // isolating self-contained probe (that fallback only fires when the
   // ambient probe THROWS, and here it didn't). Fixed by evaluating a
   // joined-source's gate self-contained FIRST, so a caller supplying no
   // `LEVEL` fails to build a self-contained probe for the joined gate and is
   // denied, regardless of what the entry model's own `$LEVEL` default is.
   it("denies a joined source's $LEVEL gate on a name collision with the entry model's own $LEVEL given, when no LEVEL is supplied", async () => {
      // coll_base's $LEVEL is TWO hops from the entry (via coll_mid), so it is
      // never merged into the entry's own ambient given namespace (only a
      // one-hop import merges) — this is what lets the entry separately
      // declare its OWN, unrelated $LEVEL without a "Cannot redefine"
      // compile error, setting up the name collision.
      await writeModel(
         "coll_base.malloy",
         `##! experimental.givens

given:
  LEVEL :: number

#(authorize) "$LEVEL > 3"
source: coll_base_gated is duckdb.table('customers') extend { measure: c is count() }
`,
      );
      await writeModel(
         "coll_mid.malloy",
         `import "coll_base.malloy"

source: coll_mid is duckdb.table('customers') extend {
  join_one: coll_base_gated on id = coll_base_gated.id
  measure: c is count()
}
`,
      );
      await writeModel(
         "coll_entry.malloy",
         `import "coll_mid.malloy"

##! experimental.givens

given:
  LEVEL :: number is 99

source: coll_top is duckdb.table('customers') extend {
  join_one: coll_mid on id = coll_mid.id
  measure: c is count()
}
`,
      );
      await expect(
         runGated("coll_entry.malloy", "run: coll_top -> { aggregate: c }", {}),
      ).rejects.toBeInstanceOf(AccessDeniedError);
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
   const QS_MODEL = `#(authorize) "false"
source: locked_src is duckdb.table('customers') extend {
  measure: c is count()
  dimension: secret is name
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

   it("denies a query against a query-source derived from a locked base", async () => {
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

   it("denies a query joining a query-source that derives from a locked base", async () => {
      await writeModel("qs.malloy", QS_MODEL);
      await expect(
         runGated("qs.malloy", "run: qs_joiner -> { aggregate: c }", {}),
      ).rejects.toBeInstanceOf(AccessDeniedError);
   });

   it("denies a CHAINED query-source (derived from a derivation of a locked base)", async () => {
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
describe("authorize query-source's own inner-pipeline join enforcement (BLOCKING-6)", () => {
   const QS_INNER_JOIN_MODEL = `#(authorize) "false"
source: locked9 is duckdb.table('customers') extend { dimension: secret is name }

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

   it("denies a direct query against a query-source whose own inner pipeline joins a locked source", async () => {
      await writeModel("qs_inner.malloy", QS_INNER_JOIN_MODEL);
      await expect(
         runGated("qs_inner.malloy", "run: sneaky -> { select: id, leak }", {}),
      ).rejects.toBeInstanceOf(AccessDeniedError);
   });

   it("denies a query joining a query-source whose own inner pipeline joins a locked source", async () => {
      await writeModel("qs_inner.malloy", QS_INNER_JOIN_MODEL);
      await expect(
         runGated(
            "qs_inner.malloy",
            "run: qs_joiner -> { group_by: sneaky.leak }",
            {},
         ),
      ).rejects.toBeInstanceOf(AccessDeniedError);
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

// BLOCKING-7: two call sites in assertAuthorizedForAllSources hand-composed
// gateExprsForOwnAnnotations + collectJoinedAuthorizeGates for a struct
// WITHOUT also calling collectQuerySourceBaseGates on it, unlike every other
// call site in this file — so specifically when that struct is ITSELF a
// query-source derived from a locked base, the derivation laundered the
// locked base's gate away:
//  (1) a composite's RESOLVED branch (compositeResolvedSourceDef);
//  (2) a run target's own query-local join (extendSources).
// Fixed by routing every reachable-gate call site through one unified
// collectAllReachableGates walk (own annotations ++ joined sources ++
// query-source bases ++ a query-source's own inner-pipeline joins, all
// recursed through itself) instead of a hand-picked subset per call site.
describe("BLOCKING-7 / unified walk", () => {
   const UNIFIED_MODEL = `##! experimental.composite_sources
source: open_src is duckdb.table('customers') extend {
  measure: c is count()
}

#(authorize) "false"
source: locked is duckdb.table('customers') extend {
  measure: c is count()
  dimension: locked_region is region
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

   it("denies a run target's own query-local join to a query-source derived from a locked base", async () => {
      await writeModel("c_unified.malloy", UNIFIED_MODEL);
      await expect(
         runGated(
            "c_unified.malloy",
            `run: open_src -> {
  extend: { join_one: laundered on id = laundered.id }
  group_by: leak is laundered.locked_region
}`,
            {},
         ),
      ).rejects.toBeInstanceOf(AccessDeniedError);
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
// query_source branch of collectAllReachableGates resolved the base via
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

#(authorize) "false"
source: locked is duckdb.table('customers') extend {
  measure: c is count()
  dimension: locked_region is region
}

source: laundered is locked -> { group_by: id, locked_region }

source: inner_combo is compose(open_src, laundered)

source: qs_over_combo is inner_combo -> { group_by: id, locked_region }

source: open_qs_over_combo is inner_combo -> { group_by: id, region_d }

source: outer_qs is qs_over_combo -> { group_by: locked_region }
`;

   it("denies a query-source whose base composite resolves to a locked member", async () => {
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

   it("denies at nesting depth 2: a query-source over a query-source over a composite whose resolved branch hits a locked base", async () => {
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
         `#(authorize) "true"
source: pub_gated is duckdb.table('customers') extend { measure: c is count() }

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

// MUST-FIX 3 (review regression): when a joined gate references TWO givens
// and the caller supplies only ONE of them, the self-contained probe throws
// (the other given is unbound) and the old code fell back to the AMBIENT
// probe — which compiles against the ENTRY model's own given namespace. If
// the entry model happens to declare its OWN given of the SAME NAME as the
// unsupplied one (a two-hop-away collision, same setup as the existing
// BLOCKING-4 $LEVEL-collision test), the entry's default silently decided
// the outcome — reopening exactly the name-collision hole the
// self-contained-first fix exists to close. Fix: only fall back to ambient
// when EVERY referenced given was covered by the self-contained decls.
describe("authorize multi-given joined gate stays fail-closed on partial given supply (MUST-FIX 3)", () => {
   it("denies when only one of two referenced givens is supplied and the entry model has a colliding default for the other", async () => {
      await writeModel(
         "mg_base.malloy",
         `##! experimental.givens

given:
  LEVEL :: number
  REGION :: string

#(authorize) "$LEVEL > 3 and $REGION = 'us-west'"
source: mg_base_gated is duckdb.table('customers') extend { measure: c is count() }
`,
      );
      await writeModel(
         "mg_mid.malloy",
         `import "mg_base.malloy"

source: mg_mid is duckdb.table('customers') extend {
  join_one: mg_base_gated on id = mg_base_gated.id
  measure: c is count()
}
`,
      );
      await writeModel(
         "mg_entry.malloy",
         `import "mg_mid.malloy"

##! experimental.givens

given:
  LEVEL :: number is 99

source: mg_top is duckdb.table('customers') extend {
  join_one: mg_mid on id = mg_mid.id
  measure: c is count()
}
`,
      );
      await expect(
         runGated("mg_entry.malloy", "run: mg_top -> { aggregate: c }", {
            REGION: "us-west",
         }),
      ).rejects.toBeInstanceOf(AccessDeniedError);
   });
});
