/**
 * Restricted-mode containment for untrusted ad-hoc query text.
 *
 * The `query` text that reaches `execute_query` is authored by an untrusted
 * caller (an MCP/LLM client, a UI field, an HTTP body), but it runs against a
 * warehouse connection that can see far more than any one model curates.
 * Compiling that text with `loadRestrictedQuery` keeps the caller inside the
 * model's published surface — its sources, views, dimensions and measures —
 * and stops it from using Malloy as a general-purpose handle to the underlying
 * database or filesystem.
 *
 * These tests are written from the publisher's threat model: each is a way an
 * untrusted query could try to reach data or compute the model never exposed,
 * paired with the assertion that restricted mode blocks it. They are not a
 * re-test of Malloy's per-construct rejection logic — the point is the misuse
 * scenario, not the grammar.
 *
 * The setup: the connection holds a `secrets` table that the `catalog` model
 * never references. The model only exposes `widgets`. Any query that manages to
 * return a row of `secrets` has escaped the curated surface.
 */

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { Connection } from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { Model } from "./model";

const TEST_DIR = path.join(os.tmpdir(), "restricted-mode-tests");
const TEST_DB_DIR = path.join(TEST_DIR, "db");
const TEST_DB_PATH = path.join(TEST_DB_DIR, "test.duckdb");
const TEST_PKG_DIR = path.join(TEST_DIR, "pkg");

let duckdbConnection: DuckDBConnection;

// `widgets` is the curated, model-exposed table. `secrets` lives in the same
// connection but is never referenced by the model the caller queries — it
// stands in for any table the deployment did not mean to publish.
const SEED_SQL = `
CREATE TABLE IF NOT EXISTS widgets (
   region VARCHAR,
   name VARCHAR
);
INSERT INTO widgets VALUES
   ('US', 'Alpha'),
   ('EU', 'Beta'),
   ('APAC', 'Gamma');

CREATE TABLE IF NOT EXISTS secrets (
   id VARCHAR,
   ssn VARCHAR
);
INSERT INTO secrets VALUES
   ('1', '111-11-1111'),
   ('2', '222-22-2222');
`;

// The model the ad-hoc queries are issued against. It publishes `widgets` and
// nothing else — `secrets` is deliberately absent.
const CATALOG_MODEL = `
source: widgets is duckdb.table('widgets') extend {
   measure: n is count()
   view: by_region is {
      group_by: region
      aggregate: n
   }
}
`;

// A second model that DOES expose secrets. It is never loaded by the caller;
// it exists only as the target of an `import` escalation attempt.
const VAULT_MODEL = `
source: vault is duckdb.table('secrets') extend {
   measure: n is count()
}
`;

beforeAll(async () => {
   await fs.mkdir(TEST_DB_DIR, { recursive: true });
   await fs.mkdir(TEST_PKG_DIR, { recursive: true });
   duckdbConnection = new DuckDBConnection("duckdb", TEST_DB_PATH, TEST_DB_DIR);
   for (const stmt of SEED_SQL.trim().split(";").filter(Boolean)) {
      await duckdbConnection.runSQL(stmt.trim() + ";");
   }
   await fs.writeFile(
      path.join(TEST_PKG_DIR, "catalog.malloy"),
      CATALOG_MODEL,
      "utf-8",
   );
   await fs.writeFile(
      path.join(TEST_PKG_DIR, "vault.malloy"),
      VAULT_MODEL,
      "utf-8",
   );
});

afterAll(async () => {
   try {
      await duckdbConnection.close();
      await new Promise((resolve) => setTimeout(resolve, 100));
      await fs.rm(TEST_DIR, { recursive: true, force: true });
   } catch {
      // Ignore cleanup errors
   }
});

function getConnections(): Map<string, Connection> {
   const map = new Map<string, Connection>();
   map.set("duckdb", duckdbConnection);
   return map;
}

type Row = Record<string, unknown>;

function asRows(compactResult: unknown): Row[] {
   return compactResult as Row[];
}

async function makeModel(modelPath: string): Promise<Model> {
   return Model.create("test-pkg", TEST_PKG_DIR, modelPath, getConnections());
}

/** Run an ad-hoc query string and return the result rows. */
async function runAdHoc(model: Model, query: string): Promise<Row[]> {
   const { compactResult } = await model.getQueryResults(
      undefined,
      undefined,
      query,
   );
   return asRows(compactResult);
}

/**
 * Restricted-mode rejections surface as a Malloy compile error: the message
 * quotes the offending source text and states the rule, and the underlying
 * `problems` carry `code: 'restricted-construct-forbidden'`. We accept either
 * signal so the assertion is robust to how `model.ts` re-wraps the error.
 */
function looksRestricted(error: unknown): boolean {
   const message = ((error as Error)?.message ?? String(error)).toLowerCase();
   if (message.includes("restricted")) return true;
   const problems = (error as { problems?: Array<{ code?: string }> })
      ?.problems;
   return (
      Array.isArray(problems) &&
      problems.some((p) => (p.code ?? "").includes("restricted"))
   );
}

/**
 * Assert an untrusted ad-hoc query is blocked before it can reach unpublished
 * data. If it instead succeeds, report the row count — the caller escaped the
 * curated surface and that is the leak we are guarding against.
 */
async function expectBlocked(model: Model, query: string): Promise<void> {
   let leakedRows: number | undefined;
   try {
      const rows = await runAdHoc(model, query);
      leakedRows = rows.length;
   } catch (error) {
      expect(looksRestricted(error)).toBe(true);
      return;
   }
   throw new Error(
      `Expected the query to be blocked by restricted mode, but it succeeded ` +
         `and returned ${leakedRows} rows (escaped the curated surface).`,
   );
}

// ===========================================================================
// The published surface stays fully usable — restriction must not break the
// legitimate path it is wrapped around.
// ===========================================================================

describe("the curated model surface stays usable under restriction", () => {
   it("runs an ad-hoc query over a published source", async () => {
      const model = await makeModel("catalog.malloy");
      const rows = await runAdHoc(
         model,
         "run: widgets -> { group_by: region; aggregate: n is count() }",
      );
      expect(rows.length).toBe(3); // US, EU, APAC
   });

   it("runs a published named view", async () => {
      const model = await makeModel("catalog.malloy");
      const rows = await runAdHoc(model, "run: widgets -> by_region");
      expect(rows.length).toBe(3);
   });
});

// ===========================================================================
// Misuse vectors: an untrusted query trying to read `secrets`, which the
// catalog model never published. Each must be blocked.
// ===========================================================================

describe("an untrusted query cannot reach data the model never published", () => {
   // The connection can see every table; the model curated only `widgets`.
   // Naming another table directly would turn the model into a handle on the
   // whole warehouse.
   it("cannot point a source at an arbitrary warehouse table", async () => {
      const model = await makeModel("catalog.malloy");
      await expectBlocked(
         model,
         "run: duckdb.table('secrets') -> { aggregate: c is count() }",
      );
   });

   // Raw SQL would let the caller run anything the connection's credentials
   // allow — arbitrary reads, cross-table joins, even writes on a writable role.
   it("cannot execute arbitrary SQL against the connection", async () => {
      const model = await makeModel("catalog.malloy");
      await expectBlocked(
         model,
         'run: duckdb.sql("SELECT id, ssn FROM secrets") -> { group_by: ssn }',
      );
   });

   // Importing another model would pull in surfaces the queried model chose not
   // to expose (and the file path is caller-controlled).
   it("cannot import another model to borrow its surface", async () => {
      const model = await makeModel("catalog.malloy");
      await expectBlocked(
         model,
         'import "vault.malloy"\n' +
            "run: vault -> { aggregate: c is count() }",
      );
   });

   // Combining the curated surface with a raw table — joining `secrets` onto the
   // published `widgets` — must not slip a raw table past the restriction.
   it("cannot smuggle a raw table in through a join on a published source", async () => {
      const model = await makeModel("catalog.malloy");
      await expectBlocked(
         model,
         "source: x is widgets extend {\n" +
            "   join_cross: s is duckdb.table('secrets')\n" +
            "}\n" +
            "run: x -> { group_by: s.ssn }",
      );
   });
});
