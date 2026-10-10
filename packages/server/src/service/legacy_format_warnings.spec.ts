// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A package on a deprecated format reports it in `getPackageMetadata().warnings`:
 * one warning per `.malloynb` notebook, and one per source declaring `#(filter)`
 * annotations however many models import it. Advisory: the package still loads.
 * Drives `Package.create` on the worker path, as the other warnings specs do.
 */
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { FixedConnectionMap, MalloyConfig } from "@malloydata/malloy";
import {
   afterAll,
   afterEach,
   beforeAll,
   beforeEach,
   describe,
   expect,
   it,
} from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import {
   PackageLoadPool,
   __setPackageLoadPoolForTests,
} from "../package_load/package_load_pool";
import { Package } from "./package";

const ORIGINAL_ENV = process.env.PACKAGE_LOAD_WORKERS;
const ROWS = `duckdb.sql("select * from (values (1,'east'),(2,'west')) as t(id, region)")`;

const FILTERED_MODEL = `#(filter) dimension=region type=in
source: filtered is ${ROWS} extend { measure: n is count() }

source: plain is ${ROWS} extend { measure: n is count() }
`;

const GIVENS_MODEL = `##! experimental.givens
given: REGION :: filter<string> is f''
source: regional is ${ROWS} extend {
   where: region ~ $REGION
   measure: n is count()
}
`;

describe("warnings for content on a deprecated format", () => {
   let tempDir: string;
   let pool: PackageLoadPool;
   let duckdb: DuckDBConnection;

   beforeAll(async () => {
      process.env.PACKAGE_LOAD_WORKERS = "1";
      pool = new PackageLoadPool(1);
      await __setPackageLoadPoolForTests(pool);
   });

   afterAll(async () => {
      await __setPackageLoadPoolForTests(null);
      if (ORIGINAL_ENV === undefined) delete process.env.PACKAGE_LOAD_WORKERS;
      else process.env.PACKAGE_LOAD_WORKERS = ORIGINAL_ENV;
   });

   beforeEach(() => {
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "publisher-legacy-"));
      duckdb = new DuckDBConnection("duckdb", ":memory:");
      fs.writeFileSync(
         path.join(tempDir, "publisher.json"),
         JSON.stringify({ name: "pkg", description: "legacy formats" }),
      );
   });

   afterEach(async () => {
      await duckdb.close();
      fs.rmSync(tempDir, { recursive: true, force: true });
   });

   function write(file: string, text: string): void {
      const target = path.join(tempDir, file);
      fs.mkdirSync(path.dirname(target), { recursive: true });
      fs.writeFileSync(target, text);
   }

   async function warnings() {
      const config = new MalloyConfig({ connections: {} });
      config.wrapConnections(
         () => new FixedConnectionMap(new Map([["duckdb", duckdb]]), "duckdb"),
      );
      const pkg = await Package.create("env", "pkg", tempDir, config);
      return pkg.getPackageMetadata().warnings ?? [];
   }

   const filterWarnings = <T extends { message?: string }>(
      found: readonly T[],
   ) => found.filter((w) => w.message?.includes("#(filter)"));

   it("reports each .malloynb, and each #(filter) declaration once, in the file that declares it", async () => {
      write("models/filtered.malloy", FILTERED_MODEL);
      // An importer (aliasing the source, too) and an `extend` of the filtered
      // source resolve its filter but declare none: neither is reported.
      write(
         "models/uses.malloy",
         `import { f is filtered } from "filtered.malloy"\n\nsource: east is f extend { where: region = 'east' }\n\nquery: q is east -> { aggregate: n }\n`,
      );
      write(
         "README.malloynb",
         `>>>markdown\n# Legacy\n>>>malloy\nimport "models/filtered.malloy"\n>>>malloy\nrun: plain -> { aggregate: n }`,
      );

      const found = await warnings();

      const notebook = found.filter((w) => w.model === "README.malloynb");
      expect(notebook).toHaveLength(1);
      expect(notebook[0]).toMatchObject({
         severity: "warn",
         message:
            "README.malloynb is a .malloynb notebook, a deprecated format. Convert it to a .malloy notebook under notebooks/.",
      });

      const filters = filterWarnings(found);
      expect(filters).toHaveLength(1);
      expect(filters[0]).toMatchObject({
         model: "models/filtered.malloy",
         subject: "filtered",
         severity: "warn",
      });
      expect(filters[0].message).toContain(
         "Source filtered uses deprecated #(filter) annotations. Replace them with given: runtime parameters",
      );
   });

   it("reports two same-named filtered sources once each, naming each file", async () => {
      write("models/a.malloy", FILTERED_MODEL);
      write("models/b.malloy", FILTERED_MODEL);

      const filters = filterWarnings(await warnings());

      expect(filters.map((w) => [w.model, w.subject])).toEqual([
         ["models/a.malloy", "filtered"],
         ["models/b.malloy", "filtered"],
      ]);
   });

   it("reports nothing for a package on givens and .malloy notebooks", async () => {
      write("models/regional.malloy", GIVENS_MODEL);
      write(
         "notebooks/overview.malloy",
         `##! experimental.givens\n## artifact { kind=notebook title="Overview" }\nimport "../models/regional.malloy"\n\nrun: regional -> { aggregate: n }\n`,
      );

      const found = await warnings();

      expect(
         found.filter(
            (w) =>
               w.message?.includes("#(filter)") ||
               w.message?.includes(".malloynb"),
         ),
      ).toEqual([]);
   });
});
