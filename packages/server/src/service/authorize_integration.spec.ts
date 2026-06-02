import { DuckDBConnection } from "@malloydata/db-duckdb";
import { Connection } from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { Model } from "./model";

// Introspection-level tests for #(authorize) / ##(authorize) collection (PR1).
// Runtime enforcement is added in a later PR; here we only assert that the
// effective expression lists are collected correctly and surfaced via
// getAuthorize() / Source.authorize.

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
});
