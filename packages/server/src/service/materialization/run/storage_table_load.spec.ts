// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterAll, afterEach, describe, expect, it } from "bun:test";
import fs from "fs";
import os from "os";
import path, { join } from "path";
import {
   openTempDuckDB,
   type TempDuckDB,
} from "../../../test_helpers/temp_storage";
import type { components } from "../../../api";
import { BadRequestError, ConflictError } from "../../../errors";
import {
   LoadRefusalError,
   loadLayout,
   loadStorageTable,
   mergeStatements,
   objectUnreadable,
   packColumnGroups,
   typeBytes,
   parquetRowBytes,
   readerRefusal,
   readerSql,
   sourceColumns,
   typeVariants,
   writeTableAs,
} from "./storage_table_load";

type ApiConnection = components["schemas"]["Connection"];

const dest = {
   name: "sales",
   type: "ducklake",
   ducklakeConnection: {
      storage: { bucketUrl: "gs://bucket/ds_x" },
      catalog: {
         postgresConnection: { host: "h", databaseName: "d", userName: "u" },
         metadataSchema: "ds_x",
      },
   },
} as ApiConnection;

describe("readerSql renders only allowlisted, typed options", () => {
   it("reads each format with its own reader", () => {
      expect(readerSql("gs://b/x.parquet", "parquet")).toBe(
         "read_parquet('gs://b/x.parquet', hive_partitioning = false)",
      );
      expect(readerSql("gs://b/x.json", "json")).toBe(
         "read_json_auto('gs://b/x.json', hive_partitioning = false)",
      );
      expect(
         readerSql("gs://b/x.csv", "csv", {
            header: true,
            delimiter: ";",
            skip: 2,
         }),
      ).toBe(
         "read_csv('gs://b/x.csv', header = true, delim = ';', skip = 2, hive_partitioning = false)",
      );
      expect(readerSql("gs://b/x.xlsx", "xlsx", { sheet: "Q3 '24" })).toBe(
         "read_xlsx('gs://b/x.xlsx', sheet = 'Q3 ''24')",
      );
   });

   it("refuses an option that could carry text into SQL", () => {
      expect(() =>
         readerSql("u", "csv", { delimiter: "', x := 1, '" }),
      ).toThrow(BadRequestError);
      expect(() =>
         readerSql("u", "csv", { skip: "1; DROP" as unknown as number }),
      ).toThrow(BadRequestError);
   });

   it("escapes a quote in the object path", () => {
      expect(readerSql("gs://b/o'brien.csv", "csv")).toBe(
         "read_csv('gs://b/o''brien.csv', hive_partitioning = false)",
      );
   });

   it("keeps an object's own column k when it sits under a k=v directory", async () => {
      const lake = openTempDuckDB("reader-hive-", "reader_hive");
      try {
         const dir = path.join(lake.dir, "k=v").replaceAll(path.sep, "/");
         fs.mkdirSync(dir);
         fs.writeFileSync(`${dir}/x.csv`, "k,a\n1,2\n");
         fs.writeFileSync(`${dir}/x.json`, '{"k":1,"a":2}\n');
         await lake.connection.runSQL(
            `COPY (SELECT 1 AS k, 2 AS a) TO '${dir}/x.parquet' (FORMAT parquet)`,
         );
         for (const format of ["csv", "json", "parquet"] as const) {
            const { rows } = await lake.connection.runSQL(
               `SELECT CAST(k AS VARCHAR) AS k FROM ${readerSql(`${dir}/x.${format}`, format)}`,
            );
            expect(rows).toEqual([{ k: "1" }]);
         }
      } finally {
         await lake.dispose();
      }
   });
});

describe("loadStorageTable refuses before attaching", () => {
   const base = {
      destinationName: "sales",
      destinationConnection: dest,
      schemaName: "main",
      tableName: "orders",
      format: "csv" as const,
      mode: "create" as const,
   };

   it("reads nothing outside the destination's own prefix, a sibling prefix included", async () => {
      for (const uri of [
         "gs://bucket/other/x.csv",
         "gs://bucket/ds_x_evil/x.csv",
         "gs://bucket/ds_x",
      ]) {
         await expect(
            loadStorageTable({ ...base, uri }),
         ).rejects.toBeInstanceOf(BadRequestError);
      }
   });

   it("takes plain identifiers only", async () => {
      await expect(
         loadStorageTable({
            ...base,
            tableName: "x; DROP",
            uri: "gs://bucket/ds_x/a.csv",
         }),
      ).rejects.toBeInstanceOf(BadRequestError);
   });

   it("loads only into DuckLake", async () => {
      await expect(
         loadStorageTable({
            ...base,
            destinationConnection: {
               name: "sales",
               type: "duckdb",
            } as ApiConnection,
            uri: "gs://bucket/ds_x/a.csv",
         }),
      ).rejects.toBeInstanceOf(BadRequestError);
   });
});

describe("readerRefusal: what a person sent and the reader refused", () => {
   const uri = "s3://lake/org_1/db_2/staging/orders/0e1f/broken.parquet";

   it("is a 400 in the reader's own words, naming the file rather than the staging path, without the SQL", () => {
      const refusal = readerRefusal(
         new Error(
            `Invalid Input Error: No magic bytes found at end of file '${uri}'\n\nLINE 1: ...CREATE OR REPLACE TABLE "db_2"."main"."orders" AS SELECT * FROM read_parquet('${uri}')`,
         ),
         uri,
      );
      expect(refusal).toBeInstanceOf(BadRequestError);
      expect(refusal?.message).toBe(
         "Invalid Input Error: No magic bytes found at end of file 'broken.parquet'",
      );
   });

   it("leaves a storage or catalog failure alone, to answer 500", () => {
      expect(
         readerRefusal(
            new Error("IO Error: Connection error for HTTP HEAD"),
            uri,
         ),
      ).toBeNull();
      // Arithmetic, which a `SELECT *` from a reader never does.
      expect(
         readerRefusal(
            new Error(
               "Out of Range Error: Overflow in multiplication of INT8 (100 * 2)!",
            ),
            uri,
         ),
      ).toBeNull();
      expect(readerRefusal("not an error", uri)).toBeNull();
   });

   // The message shapes the patterns above are written against, read from DuckDB
   // itself, so a DuckDB upgrade that rewords one fails here rather than quietly
   // answering 500 again.
   describe("matches what DuckDB says", () => {
      /**
       * The error DuckDB throws for the load's own statement shape, `CREATE TABLE ... AS SELECT *
       * FROM <reader>`. Every default in-memory DuckDBConnection in this process shares one
       * instance (the driver pools them by path and options, not by name), so the table is
       * named for this spec: a plain `t` collides with one another spec leaves behind, in an
       * order that differs by platform, and the collision's Catalog Error is not the reader's.
       */
      async function readerError(reader: string): Promise<Error> {
         const db = new DuckDBConnection("reader-refusal");
         try {
            await db.runSQL(
               `CREATE TABLE reader_refusal_probe AS SELECT * FROM ${reader}`,
            );
         } catch (error) {
            expect(error).toBeInstanceOf(Error);
            return error as Error;
         } finally {
            try {
               await db.runSQL("DROP TABLE IF EXISTS reader_refusal_probe");
            } finally {
               await db.close();
            }
         }
         throw new Error(`DuckDB read ${reader} without complaint`);
      }

      // Forward slashes, as an object URI has: DuckDB reads them on Windows too.
      const dir = fs
         .mkdtempSync(path.join(os.tmpdir(), "reader-refusal-"))
         .replaceAll(path.sep, "/");
      afterAll(() => fs.rmSync(dir, { recursive: true, force: true }));

      it("for a file that is not Parquet", async () => {
         const file = `${dir}/broken.parquet`;
         fs.writeFileSync(file, "this is not a parquet file\n".repeat(64));
         const thrown = await readerError(`read_parquet('${file}')`);
         // On the error itself first, so a reworded message is shown rather than a null refusal.
         expect(thrown.message).toMatch(
            /^Invalid Input Error: No magic bytes found/,
         );
         const refusal = readerRefusal(thrown, file);
         expect(refusal).toBeInstanceOf(BadRequestError);
         expect(refusal?.message).toContain("'broken.parquet'");
         expect(refusal?.message).not.toContain(dir);
         // The SQL DuckDB echoes after `LINE 1:` names the staging path again.
         expect(refusal?.message).not.toContain("CREATE TABLE");
      });

      it("for an object that does not exist", async () => {
         const file = `${dir}/absent.parquet`;
         const thrown = await readerError(`read_parquet('${file}')`);
         expect(thrown.message).toMatch(
            /^IO Error: No files found that match the pattern/,
         );
         const refusal = readerRefusal(thrown, file);
         expect(refusal).toBeInstanceOf(BadRequestError);
         expect(refusal?.message).toContain('"absent.parquet"');
         expect(refusal?.message).not.toContain(dir);
      });

      it("for a CSV value the column's type cannot hold", async () => {
         const file = `${dir}/orders.csv`;
         fs.writeFileSync(file, "id,amount\n1,10.5\n2,N/A\n3,7\n");
         // A type the reader is told, as a column already in the table is on a
         // replace; a sniffed three-line file would read the column as text.
         const thrown = await readerError(
            `read_csv('${file}', types = {'amount': 'DOUBLE'})`,
         );
         expect(thrown.message).toMatch(
            /^Conversion Error: CSV Error on Line: 3\n/,
         );
         const refusal = readerRefusal(thrown, file);
         expect(refusal).toBeInstanceOf(BadRequestError);
         expect(refusal?.message).toContain(
            `Could not convert string "N/A" to 'DOUBLE'`,
         );
         // The reader's own report names the file: by name, not by staging path.
         expect(refusal?.message).toContain("file = orders.csv");
         expect(refusal?.message).not.toContain(dir);
         expect(refusal?.message).not.toContain("CREATE TABLE");
      });
   });
});

describe("objectUnreadable: the reader failures a retry cannot cure", () => {
   const uris = [
      "gs://lake/org_1/staging/a.parquet",
      "gs://lake/org_1/staging/b.parquet",
   ];

   it.each([
      [
         "a missing object",
         `IO Error: No files found that match the pattern "${uris[1]}"`,
      ],
      [
         "a local file that is not there",
         `IO Error: Cannot open file "${uris[1]}": No such file or directory`,
      ],
      [
         "an object storage answers 404 for",
         `HTTP Error: HTTP GET error on '${uris[1]}' (HTTP 404)`,
      ],
      [
         "a file that is not Parquet",
         `Invalid Input Error: No magic bytes found at end of file '${uris[1]}'`,
      ],
      [
         "a Parquet footer that does not parse",
         "Invalid Error: TProtocolException: Invalid data",
      ],
      [
         "a value its column cannot hold",
         `Conversion Error: Could not convert string "N/A" to 'DOUBLE' in '${uris[1]}'`,
      ],
      [
         "any other invalid input",
         `Invalid Input Error: File '${uris[1]}' too small to be a Parquet file`,
      ],
   ])("is a 422 for %s, naming the object by its place", (_, message) => {
      const refusal = objectUnreadable(
         new Error(
            `${message}\n\nLINE 1: SELECT * FROM read_parquet(['${uris[0]}'])`,
         ),
         uris,
      );
      expect(refusal).toBeInstanceOf(LoadRefusalError);
      expect(refusal?.reason).toBe("LOAD_OBJECT_UNREADABLE");
      expect(refusal?.message).not.toContain("gs://");
      expect(refusal?.message).not.toContain("LINE 1");
      if (message.includes(uris[1])) {
         expect(refusal?.message).toStartWith("objects[1] could not be read: ");
         expect(refusal?.message).toEndWith(
            message.split("\n")[0].split(uris[1]).join("objects[1]"),
         );
         expect(refusal?.detail).toEqual({ object: 1 });
      } else {
         expect(refusal?.detail).toBeUndefined();
      }
   });

   it.each([
      [
         "an https spelling",
         "https://storage.googleapis.com/lake/org_1/staging/b.parquet",
      ],
      ["a percent-encoded spelling", "gs://lake/org_1/staging/b%2Eparquet%20x"],
   ])(
      "names no object it was not sent when the reader respells a URI as %s",
      (_, respelled) => {
         const refusal = objectUnreadable(
            new Error(
               `HTTP Error: HTTP GET error on '${respelled}' (HTTP 404)\n\nLINE 1: SELECT * FROM read_parquet(['${uris[0]}'])`,
            ),
            uris,
         );
         expect(refusal?.message).toBe(
            "An object of the load could not be read: HTTP Error: HTTP GET error on 'the object' (HTTP 404)",
         );
         expect(refusal?.detail).toBeUndefined();
      },
   );

   it.each([
      [
         "storage that is unavailable",
         `IO Error: HTTP GET error on '${uris[0]}' (HTTP 503)`,
      ],
      [
         "storage that throttles",
         `HTTP Error: HTTP GET error on '${uris[0]}' (HTTP 429)`,
      ],
      [
         "a credential storage refuses",
         `HTTP Error: HTTP GET error on '${uris[0]}' (HTTP 403)`,
      ],
      [
         "a full spill disk",
         'IO Error: Could not write file "/tmp/duckdb_temp_storage-0.tmp": No space left on device',
      ],
      [
         "a spill file that went missing",
         'IO Error: Cannot open file "/tmp/duckdb_temp_storage-0.tmp": No such file or directory',
      ],
      ["a catalog failure", "Catalog Error: Table with name t does not exist!"],
   ])("is null for %s, to answer a 500 the caller retries", (_, message) => {
      expect(objectUnreadable(new Error(message), uris)).toBeNull();
   });

   it("is null for what is not an error", () => {
      expect(objectUnreadable("IO Error: No files found", uris)).toBeNull();
   });
});

/**
 * The write every load and copy ends in, on a file-catalog DuckLake attached
 * under the destination's name: the one part of either that runs without a
 * Postgres catalog.
 */
describe("writeTableAs: the load's and the copy's write", () => {
   let lake: TempDuckDB | undefined;
   afterEach(async () => {
      if (!lake) return;
      await lake.connection.runSQL("DETACH lake");
      await lake.dispose();
      lake = undefined;
   });

   /** `inliningLimit`: a limit the catalog records itself, as a POST can set. */
   async function attached(inliningLimit?: number): Promise<DuckDBConnection> {
      lake = openTempDuckDB("write-table-as-", "write_as", {
         file: "host.duckdb",
      });
      const { connection: conn, dir } = lake;
      await conn.runSQL("INSTALL ducklake");
      await conn.runSQL("LOAD ducklake");
      const limit =
         inliningLimit === undefined
            ? ""
            : `, DATA_INLINING_ROW_LIMIT ${inliningLimit}`;
      await conn.runSQL(
         `ATTACH 'ducklake:${join(dir, "catalog.ducklake")}' AS lake ` +
            `(DATA_PATH '${join(dir, "data")}/'${limit})`,
      );
      await conn.runSQL("CREATE SCHEMA lake.s");
      return conn;
   }

   const params = (mode: "create" | "replace") => ({
      destinationName: "lake",
      schemaName: "s",
      tableName: "t",
      mode,
   });
   const ROWS = "(SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS v(id, label))";

   it("creates the table as Parquet, never inlined rows, and reports its count and columns", async () => {
      const conn = await attached();
      expect(await writeTableAs(conn, params("create"), ROWS)).toEqual({
         rowCount: 2,
         columns: [
            { name: "id", type: "INTEGER" },
            { name: "label", type: "VARCHAR" },
         ],
      });
      const files = await conn.runSQL(
         "SELECT data_file FROM ducklake_list_files('lake', 't', schema => 's')",
      );
      expect(files.rows.length).toBeGreaterThan(0);
   });

   it("flushes to Parquet the rows a catalog's own inlining limit would keep in it", async () => {
      const conn = await attached(10);
      expect((await writeTableAs(conn, params("create"), ROWS)).rowCount).toBe(
         2,
      );
      const files = await conn.runSQL(
         "SELECT data_file FROM ducklake_list_files('lake', 't', schema => 's')",
      );
      expect(files.rows.length).toBeGreaterThan(0);
   });

   it("refuses to create over a table that exists, as a 409, and replaces it when asked", async () => {
      const conn = await attached();
      await writeTableAs(conn, params("create"), ROWS);
      const conflict = await writeTableAs(conn, params("create"), ROWS).then(
         () => undefined,
         (e: unknown) => e,
      );
      expect(conflict).toBeInstanceOf(ConflictError);
      expect((conflict as Error).message).toBe(
         "Table 's.t' already exists in 'lake'.",
      );
      const replaced = await writeTableAs(
         conn,
         params("replace"),
         "(SELECT 9 AS id)",
      );
      expect(replaced).toEqual({
         rowCount: 1,
         columns: [{ name: "id", type: "INTEGER" }],
      });
   });

   it("throws what `refused` returns for a failed write, and the failure itself otherwise", async () => {
      const conn = await attached();
      const bad = "(SELECT CAST('x' AS INTEGER) AS id)";
      const mapped = new BadRequestError("mapped");
      await expect(
         writeTableAs(conn, params("create"), bad, () => mapped),
      ).rejects.toBe(mapped);
      await expect(
         writeTableAs(conn, params("create"), bad, () => null),
      ).rejects.toThrow(/Conversion Error/);
      await expect(writeTableAs(conn, params("create"), bad)).rejects.toThrow(
         /Conversion Error/,
      );
   });

   it("writes a partitioned table with its partition spec", async () => {
      const conn = await attached();
      const result = await writeTableAs(
         conn,
         params("create"),
         ROWS,
         undefined,
         [{ column: "label", transform: "identity" }],
      );
      expect(result.rowCount).toBe(2);
      const files = await conn.runSQL(
         "SELECT data_file FROM ducklake_list_files('lake', 't', schema => 's')",
      );
      const dirs = (files.rows as { data_file: string }[])
         .map(
            (r) =>
               /\/(label=[^/]+)\//.exec(r.data_file.replaceAll("\\", "/"))?.[1],
         )
         .sort();
      expect(dirs).toEqual(["label=a", "label=b"]);
   });
});

describe("loadLayout: the objects' columns against the table's", () => {
   const stage = [
      { name: "id", type: "BIGINT" },
      { name: "v", type: "VARCHAR" },
   ];
   const table = [
      { name: "ID", type: "BIGINT" },
      { name: "v", type: "VARCHAR" },
      { name: "extra", type: "INTEGER" },
   ];
   const contract = (run: () => unknown): LoadRefusalError => {
      try {
         run();
      } catch (error) {
         expect(error).toBeInstanceOf(LoadRefusalError);
         expect((error as LoadRefusalError).reason).toBe(
            "LOAD_SCHEMA_CONTRACT",
         );
         return error as LoadRefusalError;
      }
      throw new Error("expected a refusal");
   };

   it("matches columns and keys by case, and keeps the table's spelling", () => {
      const layout = loadLayout({ key: ["id"] }, stage, table);
      expect(layout.keys).toEqual(["ID"]);
      expect(layout.columns).toEqual([
         { name: "v", type: "VARCHAR", inStage: true, inTable: true },
         { name: "extra", type: "INTEGER", inStage: false, inTable: true },
      ]);
      expect(layout.added).toEqual([]);
   });

   it("creates every column of a table that does not exist, and adds a column an existing one lacks", () => {
      expect(loadLayout({ key: ["id"] }, stage, null).added).toEqual([]);
      const wider = [...stage, { name: "w", type: 'STRUCT("a b" INTEGER)' }];
      const layout = loadLayout({ key: ["id"] }, wider, table);
      expect(layout.added).toEqual([
         { name: "w", type: 'STRUCT("a b" INTEGER)' },
      ]);
      expect(layout.columns).toContainEqual({
         name: "w",
         type: 'STRUCT("a b" INTEGER)',
         inStage: true,
         inTable: false,
      });
   });

   it.each([
      ["TINYINT", "INTEGER", "kept"],
      ["INTEGER", "INTEGER", "kept"],
      ["BIGINT", "INTEGER", "widened"],
      ["UTINYINT", "UBIGINT", "kept"],
      ["UBIGINT", "UTINYINT", "widened"],
      ["UBIGINT", "INTEGER", "refused"],
      ["BIGINT", "UINTEGER", "widened"],
      ["FLOAT", "DOUBLE", "kept"],
      ["DOUBLE", "FLOAT", "widened"],
      ["DECIMAL(18,3)", "DECIMAL(18,3)", "kept"],
      ["DECIMAL(3,2)", "DECIMAL(7,2)", "kept"],
      ["DECIMAL(10,2)", "DECIMAL(18,3)", "kept"],
      ["DECIMAL(7,2)", "DECIMAL(3,2)", "widened"],
      ["DECIMAL(10,2)", "DECIMAL(10,3)", "refused"],
      ["INTEGER", "DOUBLE", "kept"],
      ["BIGINT", "DOUBLE", "refused"],
      ["UTINYINT", "INTEGER", "kept"],
      ["DOUBLE", "INTEGER", "widened"],
      ["INTEGER", "VARCHAR", "refused"],
      ["TIMESTAMP WITH TIME ZONE", "TIMESTAMP", "refused"],
      ["struct(A INTEGER)", "STRUCT(a INTEGER)", "kept"],
      ["STRUCT(a BIGINT)", "STRUCT(a INTEGER)", "refused"],
   ])("takes a %s load into a %s column: %s", (load, tableType, outcome) => {
      const run = (key = "id") =>
         loadLayout(
            { key: [key] },
            [
               { name: "id", type: "BIGINT" },
               { name: "x", type: load },
            ],
            [
               { name: "id", type: "BIGINT" },
               { name: "X", type: tableType },
            ],
         );
      if (outcome === "refused") {
         expect(contract(run).detail).toEqual({
            column: "X",
            tableType,
            loadType: load,
            detail: "type_change",
         });
         return;
      }
      const widened = outcome === "widened";
      const layout = run();
      expect(layout.retyped).toEqual(
         widened ? [{ name: "X", type: load }] : [],
      );
      expect(layout.columns).toContainEqual({
         name: "X",
         type: widened ? load : tableType,
         inStage: true,
         inTable: true,
      });
      // A widened key is compared at its new type too.
      expect(run("x").keyTypes).toEqual([widened ? load : tableType]);
   });

   it("refuses a key that is not a column of both sides", () => {
      expect(
         contract(() => loadLayout({ key: ["nope"] }, stage, table)).detail,
      ).toMatchObject({ column: "nope", detail: "missing_key" });
      expect(
         contract(() => loadLayout({ key: ["extra"] }, stage, table)).detail,
      ).toMatchObject({
         column: "extra",
         detail: "missing_key",
         tableType: "INTEGER",
      });
      expect(
         contract(() =>
            loadLayout(
               { key: ["w"] },
               [...stage, { name: "w", type: "INTEGER" }],
               table,
            ),
         ).detail,
      ).toMatchObject({
         column: "w",
         detail: "missing_key",
         loadType: "INTEGER",
      });
   });

   it("takes an integer, DATE or TIMESTAMP column as the cursor, and refuses anything else", () => {
      for (const [type, sql] of [
         ["BIGINT", 'max("c")::VARCHAR'],
         ["UBIGINT", 'max("c")::VARCHAR'],
         ["DATE", `strftime(max("c"), '%Y-%m-%d')`],
         ["TIMESTAMP", "%S.%f'"],
         ["TIMESTAMP_MS", "%S.%f'"],
         ["TIMESTAMP_NS", "%S.%n'"],
         ["TIMESTAMP WITH TIME ZONE", "AT TIME ZONE 'UTC'"],
      ]) {
         const layout = loadLayout(
            { key: ["id"], cursor: "c" },
            [...stage, { name: "c", type }],
            null,
         );
         expect(layout.cursor).toMatchObject({ name: "c", type });
         expect(layout.cursor!.maxSql).toContain(sql);
      }
      for (const type of [
         "VARCHAR",
         "DOUBLE",
         "DECIMAL(18,3)",
         "TIME",
         "BOOLEAN",
      ]) {
         expect(
            contract(() =>
               loadLayout(
                  { key: ["id"], cursor: "c" },
                  [...stage, { name: "c", type }],
                  null,
               ),
            ).detail,
         ).toMatchObject({
            column: "c",
            loadType: type,
            detail: "cursor_type",
         });
      }
      expect(
         contract(() => loadLayout({ key: ["id"], cursor: "c" }, stage, null))
            .detail,
      ).toMatchObject({ detail: "missing_cursor" });
   });

   it("refuses a table column named like the load's own helper columns, whatever its case", () => {
      for (const name of ["__publisher_present", "__PUBLISHER_present"]) {
         expect(
            contract(() =>
               loadLayout({ key: ["id"] }, stage, [
                  ...table,
                  { name, type: "INTEGER" },
               ]),
            ).detail,
         ).toMatchObject({ column: name, detail: "reserved_name" });
      }
   });
});

/** A schema of the object at `uri` from `[name, childCount, logicalType?, physicalType?]` rows, root first. */
const shapeAt = (
   uri: string,
   ...elements: [string, number, string?, string?][]
) => ({
   names: elements.map(([name]) => name),
   kids: elements.map(([, kids]) => kids),
   kinds: elements.map(([, , kind]) => kind ?? null),
   types: elements.map(([, , , type]) => type ?? ""),
   uri,
});

describe("sourceColumns: the columns the objects' schemas hold", () => {
   const shape = (...elements: [string, number, string?, string?][]) =>
      shapeAt("o.parquet", ...elements);
   const contract = (run: () => unknown) => {
      try {
         run();
      } catch (error) {
         expect(error).toBeInstanceOf(LoadRefusalError);
         return (error as LoadRefusalError).detail;
      }
      throw new Error("expected a refusal");
   };

   it("skips the fields of a struct and the elements of a list", () => {
      // root, id, st { x, y { z } }, l [ element ], last
      expect(
         sourceColumns([
            shape(
               ["root", 4],
               ["id", 0],
               ["st", 2],
               ["x", 0],
               ["y", 1],
               ["z", 0],
               ["l", 1],
               ["list", 1],
               ["element", 0],
               ["last", 0],
            ),
         ]),
      ).toEqual(["id", "st", "l", "last"]);
   });

   // DuckDB spells a column of no type `UNKNOWN`; pyarrow's `null()` is
   // written as a NullType() logical type, which `parquet_schema` reports so.
   it.each(["UNKNOWN", "NullType()"])(
      "joins the columns of every schema, and drops one typed NULL (%s) in all of them",
      (nullType) => {
         expect(
            sourceColumns([
               shape(["root", 2], ["id", 0], ["gone", 0, nullType]),
               shape(["root", 3], ["id", 0], ["gone", 0, nullType], ["v", 0]),
            ]),
         ).toEqual(["id", "v"]);
         // Typed in one object, NULL in another: the column stays.
         expect(
            sourceColumns([
               shape(["root", 2], ["id", 0], ["x", 0, nullType]),
               shape(["root", 2], ["id", 0], ["x", 0]),
            ]),
         ).toEqual(["id", "x"]);
      },
   );

   it("refuses objects that leave no column: none at all, or only ones typed NULL", () => {
      expect(contract(() => sourceColumns([shape(["root", 0])]))).toEqual({
         column: "",
         tableType: null,
         loadType: null,
         detail: "no_columns",
      });
      expect(
         contract(() =>
            sourceColumns([shape(["root", 1], ["x", 0, "NullType()"])]),
         ),
      ).toMatchObject({ detail: "no_columns" });
      expect(contract(() => sourceColumns([]))).toMatchObject({
         detail: "no_columns",
      });
   });

   it("refuses names the load reserves, a NUL, and names that differ only by case", () => {
      for (const name of [
         "__publisher_x",
         "__PUBLISHER_x",
         "file_row_number",
         "ROWID",
         "a\0b",
      ]) {
         expect(
            contract(() => sourceColumns([shape(["root", 1], [name, 0])])),
         ).toMatchObject({ column: name, detail: "reserved_name" });
      }
      // Within one schema, and across two.
      expect(
         contract(() =>
            sourceColumns([shape(["root", 2], ["a", 0], ["A", 0])]),
         ),
      ).toMatchObject({ column: "a", detail: "case_collision" });
      expect(
         contract(() =>
            sourceColumns([
               shape(["root", 1], ["Id", 0]),
               shape(["root", 1], ["id", 0]),
            ]),
         ),
      ).toMatchObject({ column: "id", detail: "case_collision" });
      // A name repeated within one schema.
      expect(
         contract(() =>
            sourceColumns([shape(["root", 2], ["a", 0], ["a", 0])]),
         ),
      ).toMatchObject({ detail: "case_collision" });
   });

   it.each(["UNKNOWN", "NullType()"])(
      "refuses a column typed NULL (%s) that differs only by case from a populated one",
      (nullType) => {
         expect(
            contract(() =>
               sourceColumns([
                  shape(["root", 2], ["a", 0, nullType], ["A", 0]),
               ]),
            ),
         ).toMatchObject({ column: "a", detail: "case_collision" });
      },
   );

   it("does not take a nested field's name for a column's", () => {
      expect(
         sourceColumns([shape(["root", 2], ["id", 0], ["st", 1], ["ID", 0])]),
      ).toEqual(["id", "st"]);
   });
});

describe("typeVariants: the columns the objects type in more than one way", () => {
   const objects = (type: string | undefined, uri: string, kind?: string) =>
      shapeAt(
         uri,
         ["root", 2],
         ["id", 0, undefined, "INT64"],
         ["k", 0, kind, type],
      );

   it("names one object of each distinct type of a column", () => {
      expect(
         typeVariants([
            objects("INT64", "a.parquet"),
            objects("DOUBLE", "b.parquet"),
            objects("DOUBLE", "c.parquet"),
         ]),
      ).toEqual(new Map([["k", ["a.parquet", "b.parquet"]]]));
   });

   it("names nothing when every object types its columns alike", () => {
      expect(
         typeVariants([
            objects("INT64", "a.parquet"),
            objects("INT64", "b.parquet"),
         ]),
      ).toEqual(new Map());
   });

   it("does not count a column typed NULL as a type of its own", () => {
      expect(
         typeVariants([
            objects("INT64", "a.parquet"),
            objects("INT32", "b.parquet", "NullType()"),
            objects(undefined, "c.parquet", "UNKNOWN"),
         ]),
      ).toEqual(new Map());
   });

   it("tells a nested column's types apart by what is inside it", () => {
      const nested = (inner: string, uri: string) =>
         shapeAt(uri, ["root", 1], ["st", 1], ["x", 0, undefined, inner]);
      expect(
         typeVariants([
            nested("INT32", "a.parquet"),
            nested("INT64", "b.parquet"),
         ]),
      ).toEqual(new Map([["st", ["a.parquet", "b.parquet"]]]));
   });
});

describe("mergeStatements: the plan joins on plain equality", () => {
   const v = { name: "v", type: "VARCHAR", inStage: true, inTable: true };
   const columns = [v];
   const statements = (mode: "merge" | "replace") =>
      mergeStatements({ target: "t", mode, keys: ["id"], columns });

   it("joins the stage to the table with `=` alone, and matches NULLs only in the delete", () => {
      for (const mode of ["merge", "replace"] as const) {
         const { changes, delete: del, insert } = statements(mode);
         const text = changes.join("\n");
         expect(text).toContain('__s."id" = __t."id"');
         expect(text).not.toMatch(/IS NULL/);
         expect(insert).not.toMatch(/IS NULL/);
         expect(del).toContain('__t."id" IS NULL AND __s."id" IS NULL');
      }
   });

   it("inserts through a semi join over the changed keys, not an EXISTS that DuckDB decorrelates into a delim join", () => {
      for (const mode of ["merge", "replace"] as const) {
         const { insert } = statements(mode);
         expect(insert).toContain("SEMI JOIN");
         expect(insert).not.toContain("EXISTS");
      }
   });

   it("plans a merge from the stage's side and a replace over both sides", () => {
      const [merge, replace] = [statements("merge"), statements("replace")];
      expect(merge.changes[0]).toContain("LEFT JOIN");
      expect(merge.changes[0]).not.toContain("'deleted'");
      expect(replace.changes[0]).toContain("FULL JOIN");
      expect(replace.changes[0]).toContain("'deleted'");
   });

   it("compares a column the objects lack, and one the table lacks, as NULL", () => {
      const { changes } = mergeStatements({
         target: "t",
         mode: "merge",
         keys: ["id"],
         columns: [
            { name: "old", type: "INTEGER", inStage: false, inTable: true },
            { name: "new", type: "INTEGER", inStage: true, inTable: false },
         ],
      });
      const text = changes.join("\n");
      expect(text).toContain('__t."old" IS DISTINCT FROM NULL');
      expect(text).toContain('NULL IS DISTINCT FROM __s."new"');
   });

   it("settles the keys in a pass that names no compared column, and adds each group's changed keys straight to the changes unless already there", () => {
      const [a, b, c] = ["a", "b", "c"].map((name) => ({
         ...v,
         name,
      }));
      const { changes } = mergeStatements({
         target: "t",
         mode: "replace",
         keys: ["id"],
         columns: [a, b, c],
         groups: [[a, b], [c]],
      });
      const [keys, first, second] = changes;
      expect(changes).toHaveLength(3);
      expect(keys).toStartWith('CREATE TEMP TABLE "__publisher_changes"');
      expect(keys).not.toMatch(/"[abc]"/);
      expect(first).toStartWith('INSERT INTO "__publisher_changes"');
      expect(first).toContain('__t."a" IS DISTINCT FROM __s."a" OR __t."b"');
      expect(first).not.toContain('"c"');
      expect(second).toStartWith('INSERT INTO "__publisher_changes"');
      expect(second).toContain('__t."c" IS DISTINCT FROM __s."c"');
      expect(second).not.toMatch(/"[ab]"/);
      for (const pass of [first, second]) {
         expect(pass).toContain(
            'AND NOT EXISTS (SELECT 1 FROM "__publisher_changes" AS __c WHERE __c."id" = __s."id")',
         );
         expect(pass).not.toContain("__publisher_diff");
      }
   });

   it("compares every column in one group by default, and none when there is no column", () => {
      expect(statements("merge").changes).toHaveLength(2);
      const keysOnly = mergeStatements({
         target: "t",
         mode: "merge",
         keys: ["id"],
         columns: [],
      });
      expect(keysOnly.changes).toHaveLength(1);
   });
});

describe("packColumnGroups: columns into compare passes within a budget", () => {
   const sized = (...bytes: number[]) =>
      bytes.map((b, i) => ({ column: `c${i}`, bytes: b }));

   it("keeps every column in one group when both sides of the rows fit", () => {
      expect(packColumnGroups(sized(8, 8, 8), 8, 100, 7200)).toEqual([
         ["c0", "c1", "c2"],
      ]);
   });

   it("counts the keys and both sides of every row against each group, and the changes' keys once", () => {
      // 100 rows × 2 sides × (8 key + 16) + 100 × 8 key = 5600 bytes a group of two 8-byte columns.
      expect(packColumnGroups(sized(8, 8, 8, 8), 8, 100, 5600)).toEqual([
         ["c0", "c1"],
         ["c2", "c3"],
      ]);
      expect(packColumnGroups(sized(8, 8, 8, 8), 8, 100, 5599)).toEqual([
         ["c0"],
         ["c1"],
         ["c2"],
         ["c3"],
      ]);
   });

   it("fills the widest columns first, so mixed widths take few passes, and keeps each group in the columns' order", () => {
      // Budget for a key and 24 bytes: the two 16s cannot share a group, and each takes an 8.
      expect(packColumnGroups(sized(8, 16, 8, 16), 0, 10, 10 * 2 * 24)).toEqual(
         [
            ["c0", "c1"],
            ["c2", "c3"],
         ],
      );
   });

   it("gives a column that exceeds the budget alone a group of its own, and packs the rest around it", () => {
      expect(packColumnGroups(sized(8, 500, 8), 8, 100, 5600)).toEqual([
         ["c1"],
         ["c0", "c2"],
      ]);
   });

   it("takes no group for no column, and one group when there are no rows", () => {
      expect(packColumnGroups([], 8, 100, 1)).toEqual([]);
      expect(packColumnGroups(sized(8, 8), 8, 0, 1)).toEqual([["c0", "c1"]]);
   });
});

describe("typeBytes: a column's width in a join's hash table", () => {
   it.each([
      ["BIGINT", 8],
      ["TIMESTAMP WITH TIME ZONE", 8],
      ["DECIMAL(18,3)", 8],
      ["HUGEINT", 16],
      ["UHUGEINT", 16],
      ["UUID", 16],
      ["INTERVAL", 16],
      ["DECIMAL(19,2)", 16],
      ["DECIMAL(38,10)", 16],
      ["VARCHAR", 32],
      ["STRUCT(a INTEGER)", 32],
   ])("counts %s at %d bytes", (type, bytes) => {
      expect(typeBytes(type)).toBe(bytes);
   });
});

describe("parquetRowBytes: a column's bytes per row from the footers", () => {
   let db: TempDuckDB | undefined;
   afterEach(async () => {
      await db?.dispose();
      db = undefined;
   });

   it("sums a nested column's leaves, folds names, and does not hand a column the leaves of one whose name starts with its own", async () => {
      db = openTempDuckDB("parquet-row-bytes-", "row_bytes");
      const file = join(db.dir, "o.parquet");
      await db.connection.runSQL(
         `COPY (SELECT i AS a, 'abc' || i AS "a, b", {'x': i, 'y': i} AS st, i AS "Mixed" FROM range(1000) t(i)) TO '${file}' (FORMAT parquet)`,
      );
      const bytes = await parquetRowBytes(
         db.connection,
         [file],
         ["a", "a, b", "st", "mixed", "absent"],
         1000,
      );
      expect([...bytes.keys()].sort()).toEqual(["a", "a, b", "mixed", "st"]);
      // A BIGINT is about 8 bytes a row, and `st` holds two of them.
      expect(bytes.get("a")).toBeGreaterThan(7);
      expect(bytes.get("a")).toBeLessThan(9);
      expect(bytes.get("mixed")).toBeCloseTo(bytes.get("a")!, 0);
      expect(bytes.get("st")).toBeGreaterThan(15);
      expect(bytes.get("a, b")).toBeGreaterThan(9);
   });
});
