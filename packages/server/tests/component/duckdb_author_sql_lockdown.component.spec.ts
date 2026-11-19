// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Author SQL on a locked DuckDB connection cannot end its statement and start
 * another, through a real database-tier build (duckdb_lockdown.ts): a raw SQL
 * expression is checked at load with nonce-named placeholders for its
 * arguments, and the build's CTAS of the author's query runs under the
 * one-statement rule, which only Publisher's own swap transaction is admitted
 * past, by its exact text. Run on the environment tier and the sandbox tier.
 */

import { DuckDBConnection as MalloyDuckDB } from "@malloydata/db-duckdb";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import {
   assertStatementAllowed,
   authorSqlRefusals,
   runAsInternalStatement,
   sandboxedDuckDBOptions,
} from "../../src/service/duckdb_lockdown";
import { Environment } from "../../src/service/environment";
import { parsePackageName } from "../../src/service/identity";
import { DuckDBConnection } from "../../src/storage/duckdb/DuckDBConnection";
import { createDuckDBRepository } from "../../src/storage/duckdb/DuckDBRepository";
import { initializeSchema } from "../../src/storage/duckdb/schema";
import { freshLoadPool } from "../../src/test_helpers/conformance/package_runner";
import { serviceOverEnvironment } from "../../src/test_helpers/materialization_service_harness";
import { installSettings } from "../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../src/test_helpers/temp_package";

let restore: () => void;

beforeAll(async () => {
   restore = installSettings({
      env: {
         PUBLISHER_MATERIALIZE: "true",
         PERSIST_STORAGE_MODE: "off",
         PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true",
      },
   }).restore;
   await freshLoadPool();
});

afterAll(async () => {
   restore();
   await freshLoadPool();
});

/** A compiled model holding one DuckDB raw SQL expression. */
function expression(src: string[], args: number) {
   return {
      dialect: "duckdb",
      fields: [
         {
            e: {
               node: "genericSQLExpr",
               src,
               kids: {
                  args: Array.from({ length: args }, () => ({ node: "field" })),
               },
            },
         },
      ],
   };
}

describe("the load check reads each argument back as a value", () => {
   it("refuses an argument a fragment hides, however the fragment spells a decoy", async () => {
      for (const src of [
         ["$q$", "$q$"],
         ["__arg_0__ || $q$", "$q$"],
         ['"__arg_0__" || $q$', "$q$"],
         ["list_transform([1], __arg_0__ -> __arg_0__) || $q$", "$q$"],
         ["1 -", " + 1"],
         ["1 -- ", "\n"],
      ]) {
         expect(
            await authorSqlRefusals("m.malloy", expression(src, 1)),
         ).toHaveLength(1);
      }
   });

   it("loads an argument that reads as a value", async () => {
      for (const [src, args] of [
         [["", " + 1"], 1],
         [["upper(lower(", "))"], 1],
         [["CASE WHEN ", " > 1 THEN 1 END"], 1],
         [["list_transform([1], y -> y + ", ")"], 1],
         [["", " + ", ""], 2],
         [["'a' || ", ""], 1],
         [["1 - ", ""], 1],
      ] as const) {
         expect(
            await authorSqlRefusals("m.malloy", expression([...src], args)),
         ).toEqual([]);
      }
   });
});

describe("the run gate admits Publisher's own script by its exact text only", () => {
   it("refuses several statements unless that exact text was marked", async () => {
      const dir = fs.realpathSync(
         fs.mkdtempSync(path.join(os.tmpdir(), "lockdown-script-")),
      );
      const connection = new MalloyDuckDB({
         name: "duckdb",
         databasePath: ":memory:",
         ...sandboxedDuckDBOptions(dir),
      });
      try {
         await connection.runSQL("SELECT 1");
         const script = "BEGIN TRANSACTION;\nSELECT 1;\nCOMMIT;";
         await expect(
            assertStatementAllowed(connection, script),
         ).rejects.toThrow("statements in one call");
         await runAsInternalStatement(script, () =>
            assertStatementAllowed(connection, script),
         );
         await runAsInternalStatement(script, () =>
            assertStatementAllowed(connection, `-- run="x"\n${script}`),
         );
         await expect(
            runAsInternalStatement(script, () =>
               assertStatementAllowed(
                  connection,
                  `CREATE TABLE t AS SELECT 1; ${script}`,
               ),
            ),
         ).rejects.toThrow("statements in one call");
      } finally {
         await connection.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });
});

/** The database-tier vectors, through a real build. */
const VECTORS = {
   "a decoy placeholder in the fragment": (target: string) => [
      `  dimension: inj is '$q$ AS d FROM (SELECT 2 AS __arg_0__) t); COPY (SELECT 42 AS x) TO $w$${target}$w$; CREATE TABLE tail_t AS (SELECT $q$'`,
      '  dimension: d is sql_string("__arg_0__ || $q$${inj}$q$")',
   ],
   "an argument after a trailing minus": (target: string) => [
      "  dimension: neg is -a",
      `  dimension: d is sql_number("""1 -\${neg} + length($q$\n; COPY (SELECT 42 AS x) TO $w$${target}$w$; SELECT 1 -- $q$)""")`,
   ],
};

for (const tier of ["env", "sandbox"] as const) {
   describe(`a database-tier build on the ${tier} tier`, () => {
      for (const [label, body] of Object.entries(VECTORS)) {
         it(`never runs the statements ${label} would add`, async () => {
            const root = fs.realpathSync(
               fs.mkdtempSync(path.join(os.tmpdir(), "lockdown-build-")),
            );
            const envPath = path.join(root, "env");
            const target = path.join(envPath, "pkg", "written.csv");
            const connection = tier === "env" ? "warehouse" : "duckdb";
            const model = [
               "##! experimental { persistence sql_functions }",
               `source: raw is ${connection}.sql("SELECT 1 AS a, 2 AS __arg_0__") extend {`,
               ...body(target),
               "}",
               '#@ persist name="probe_tbl"',
               "source: built is raw -> { group_by: d }",
               "",
            ].join("\n");
            fs.mkdirSync(envPath, { recursive: true });
            writePackageFiles(
               path.join(envPath, "pkg"),
               { "m.malloy": model },
               { manifest: { name: "pkg", description: "lockdown" } },
            );
            const db = new DuckDBConnection(":memory:");
            await db.initialize();
            await initializeSchema(db);
            await db.run(
               "INSERT INTO environments (id, name, path, created_at, updated_at) VALUES (?, ?, ?, now(), now())",
               ["env-1", "e1", envPath],
            );
            const repo = createDuckDBRepository(db);
            const env = await Environment.create(
               "e1",
               envPath,
               [
                  {
                     name: "warehouse",
                     type: "duckdb",
                     duckdbConnection: { setupSQL: "SELECT 1" },
                  } as never,
               ],
               [],
            );
            try {
               // The load refuses it, so no build can run it; were it loaded,
               // the build would run it.
               let loaded = true;
               try {
                  await env.addPackage("pkg");
                  await env.getPackage(parsePackageName("pkg"), false);
               } catch (error) {
                  loaded = false;
                  expect(String(error)).toMatch(/one SELECT/);
               }
               if (loaded) {
                  const service = serviceOverEnvironment(
                     () => repo,
                     () => env,
                  );
                  const created = await service.createMaterialization(
                     "e1",
                     "pkg",
                     {},
                  );
                  const deadline = Date.now() + 120_000;
                  for (;;) {
                     const run = await repo.getMaterializationById(created.id);
                     if (
                        ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"].includes(
                           run?.status ?? "",
                        ) ||
                        Date.now() > deadline
                     ) {
                        break;
                     }
                     await new Promise((resolve) => setTimeout(resolve, 50));
                  }
               }
               expect(loaded).toBe(false);
               expect(fs.existsSync(target)).toBe(false);
            } finally {
               await env.closeAllConnections().catch(() => undefined);
               await db.close();
               fs.rmSync(root, { recursive: true, force: true });
            }
         }, 240_000);
      }
   });
}
