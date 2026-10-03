// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * An ad-hoc query runs exactly one `run:` statement.
 *
 * Malloy executes only the LAST `run:` of a text and drops the others without a
 * word, so a caller that sends several gets one answer back and no sign the
 * rest were lost. `getQueryResults` refuses such text with a 400 instead. The
 * definitions that commonly come before a single `run:` (`source:`, `query:`)
 * must keep working, and so must the named `sourceName`/`queryName` path.
 */

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { Connection } from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { BadRequestError } from "../errors";
import { Model } from "./model";

const TEST_DIR = path.join(os.tmpdir(), "single-run-statement-tests");
const TEST_DB_DIR = path.join(TEST_DIR, "db");
const TEST_DB_PATH = path.join(TEST_DB_DIR, "test.duckdb");
const TEST_PKG_DIR = path.join(TEST_DIR, "pkg");

let duckdbConnection: DuckDBConnection;

const SEED_SQL = `
CREATE TABLE IF NOT EXISTS widgets (
   region VARCHAR,
   name VARCHAR
);
INSERT INTO widgets VALUES
   ('US', 'Alpha'),
   ('EU', 'Beta'),
   ('APAC', 'Gamma');
`;

// The model carries `run:` statements of its own, so the tests also show that
// those are not counted against the caller's text.
const CATALOG_MODEL = `
source: widgets is duckdb.table('widgets') extend {
   measure: n is count()
   view: by_region is {
      group_by: region
      aggregate: n
   }
}

query: widget_count is widgets -> { aggregate: n }

run: widgets -> { aggregate: n }
run: widgets -> by_region
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

async function makeModel(): Promise<Model> {
   return Model.create(
      "test-pkg",
      TEST_PKG_DIR,
      "catalog.malloy",
      getConnections(),
   );
}

async function runAdHoc(model: Model, query: string): Promise<Row[]> {
   const { compactResult } = await model.getQueryResults(
      undefined,
      undefined,
      query,
   );
   return compactResult as Row[];
}

async function refusal(model: Model, query: string): Promise<unknown> {
   try {
      await runAdHoc(model, query);
   } catch (error) {
      return error;
   }
   throw new Error(`"${query}" was run`);
}

describe("an ad-hoc query with more than one run: statement", () => {
   it("is refused with a 400 that says how many there were", async () => {
      const model = await makeModel();
      const error = await refusal(
         model,
         "run: widgets -> { aggregate: n }\n" +
            "run: widgets -> { group_by: region }\n" +
            "run: widgets -> { group_by: name }",
      );
      expect(error).toBeInstanceOf(BadRequestError);
      expect((error as Error).message).toBe(
         "The query has 3 run: statements; only one runs per call, so the " +
            "others would be ignored. Send each as its own request. (source: " +
            "and query: definitions before a single run: are fine.)",
      );
   });

   it("is refused when the statements share one line", async () => {
      const model = await makeModel();
      const error = await refusal(
         model,
         "run: widgets -> { aggregate: n } run: widgets -> by_region",
      );
      expect(error).toBeInstanceOf(BadRequestError);
      expect((error as Error).message).toStartWith(
         "The query has 2 run: statements",
      );
   });

   it("is refused when a definition sits between the statements", async () => {
      const model = await makeModel();
      const error = await refusal(
         model,
         "run: widgets -> { aggregate: n }\n" +
            "source: w2 is widgets extend { dimension: r is region }\n" +
            "run: w2 -> { group_by: r }",
      );
      expect(error).toBeInstanceOf(BadRequestError);
      expect((error as Error).message).toStartWith(
         "The query has 2 run: statements",
      );
   });
});

describe("text with exactly one run: statement keeps running", () => {
   it("runs a source: definition followed by one run:", async () => {
      const model = await makeModel();
      const rows = await runAdHoc(
         model,
         "source: x is widgets extend { dimension: r is region }\n" +
            "run: x -> { group_by: r }",
      );
      expect(rows.length).toBe(3);
   });

   it("runs a query: definition followed by a run: of it", async () => {
      const model = await makeModel();
      const rows = await runAdHoc(
         model,
         "query: q is widgets -> { group_by: region }\nrun: q",
      );
      expect(rows.length).toBe(3);
   });

   it("runs several definitions followed by one run:", async () => {
      const model = await makeModel();
      const rows = await runAdHoc(
         model,
         "source: a is widgets extend { dimension: r is region }\n" +
            "source: b is a extend { measure: c is count() }\n" +
            "query: q is b -> { group_by: r; aggregate: c }\n" +
            "query: unused is b -> { aggregate: c }\n" +
            "run: q",
      );
      expect(rows.length).toBe(3);
   });

   it("runs a single run: although the model has run: statements of its own", async () => {
      const model = await makeModel();
      const rows = await runAdHoc(model, "run: widgets -> by_region");
      expect(rows.length).toBe(3);
   });

   it("runs a named view and a named query by name", async () => {
      const model = await makeModel();
      const view = await model.getQueryResults("widgets", "by_region");
      expect((view.compactResult as Row[]).length).toBe(3);
      const query = await model.getQueryResults(undefined, "widget_count");
      expect(query.compactResult as Row[]).toEqual([{ n: 3 }]);
   });
});
