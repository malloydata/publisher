import { DuckDBConnection } from "@malloydata/db-duckdb";
import { Connection, GivenValue } from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { AccessDeniedError } from "../errors";
import { Model } from "./model";

// Introspection (PR1), compile-time validation (PR2), and the runtime gate
// (PR3) for #(authorize) / ##(authorize).

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

describe("authorize runtime gate", () => {
   // Helper: run an ad-hoc query through the full getQueryResults path (which
   // is where the gate fires). Returns the result or throws AccessDeniedError.
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

   it("applies a file-level gate to an ad-hoc inline duckdb.sql query (no raw-SQL bypass)", async () => {
      await writeModel("rt_filelevel.malloy", FILE_LEVEL);
      // Inline SQL source is not a declared model source; the model-wide
      // file-level gate must still apply, or it's a raw-warehouse bypass.
      await expect(
         runGated(
            "rt_filelevel.malloy",
            `run: duckdb.sql("SELECT 1 AS id") -> { aggregate: c is count() }`,
            { ROLE: "user" },
         ),
      ).rejects.toBeInstanceOf(AccessDeniedError);
      // An admin (file-level gate satisfied) can run it.
      const { result } = await runGated(
         "rt_filelevel.malloy",
         `run: duckdb.sql("SELECT 1 AS id") -> { aggregate: c is count() }`,
         { ROLE: "admin" },
      );
      expect(result.data).toBeDefined();
   });

   it("does not over-gate ad-hoc inline SQL when the model has no file-level gate", async () => {
      // Control: a per-source gate is NOT model-wide, so an inline ad-hoc query
      // in a model whose only gate is source-level stays unrestricted.
      await writeModel("rt_single.malloy", SINGLE_GATE);
      const { result } = await runGated(
         "rt_single.malloy",
         `run: duckdb.sql("SELECT 1 AS id") -> { aggregate: c is count() }`,
      );
      expect(result.data).toBeDefined();
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

   it("[documented limitation] a query joining a locked base via an ungated source is allowed (top-level-source only)", async () => {
      await writeModel("rt_locked.malloy", LOCKED_BASE);
      const { result } = await runGated(
         "rt_locked.malloy",
         "run: joiner -> { aggregate: c }",
         {},
      );
      expect(result.data).toBeDefined();
   });
});
