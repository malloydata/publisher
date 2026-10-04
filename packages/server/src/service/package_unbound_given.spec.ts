// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A query that bakes a default-less given (a `$G` in a `where:`, or the
 * `#(access_filter)` an operator may not give a default) used to throw while the
 * schema of the model's queries was read, failing the file and then the whole
 * package load. Each file shape below carries such a query and must load, on the
 * worker path (`Package.create`) and the in-process path (`Model.create`).
 *
 * One `PackageLoadPool` is shared across the cases for the reason given in
 * `package_worker_path.spec.ts`.
 */
import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   MalloyConfig,
   type Connection,
} from "@malloydata/malloy";
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
import { Model } from "./model";
import { Package } from "./package";

const ROWS = `duckdb.sql("select * from (values (1,1),(2,2),(3,2),(4,1)) as t(id, org_id)")`;

const GIVEN_AND_SOURCE = `##! experimental.givens
given: ORG :: number
source: orders is ${ROWS} extend { measure: c is count() }
`;

const MODEL_WITH_QUERY = `${GIVEN_AND_SOURCE}
query: for_org is orders -> { where: org_id = $ORG; aggregate: c }
`;

const LAYOUT_NOTEBOOK = `##! experimental.givens
## artifact { kind=notebook title="Unbound" }
given: ORG :: number
source: orders is ${ROWS} extend { measure: c is count() }

run: orders -> { where: org_id = $ORG; aggregate: c }
`;

const LEGACY_NOTEBOOK = `>>>markdown
# Unbound
>>>malloy
${GIVEN_AND_SOURCE}
run: orders -> { where: org_id = $ORG; aggregate: c }`;

const DASHBOARD = `##! experimental.givens
given: ORG :: number
source: orders is ${ROWS} extend { measure: c is count() }

# artifact { title="Unbound" } dashboard {columns=12}
query: tile is orders -> { where: org_id = $ORG; aggregate: c }
`;

const ACCESS_FILTERED = `##! experimental.givens
given: GROUPS :: number[]

#(access_filter) org_id in $GROUPS
source: gated is ${ROWS} extend { measure: c is count() }

query: all_gated is gated -> { aggregate: c }
`;

const AUTHORIZED = `##! experimental.givens
given: ROLE :: string

#(authorize) 'analyst' = $ROLE
source: gated is ${ROWS} extend { measure: c is count() }

query: all_gated is gated -> { aggregate: c }
`;

type Shape = { name: string; file: string; text: string };

const SHAPES: Shape[] = [
   { name: "an exported query", file: "m.malloy", text: MODEL_WITH_QUERY },
   {
      name: "a layout notebook run:",
      file: "notebooks/nb.malloy",
      text: LAYOUT_NOTEBOOK,
   },
   { name: "a legacy .malloynb", file: "nb.malloynb", text: LEGACY_NOTEBOOK },
   { name: "a dashboard", file: "dashboards/d.malloy", text: DASHBOARD },
   {
      name: "an #(access_filter) source",
      file: "m.malloy",
      text: ACCESS_FILTERED,
   },
   { name: "an #(authorize) source", file: "m.malloy", text: AUTHORIZED },
];

function count(result: unknown): number | undefined {
   const rows = (typeof result === "string" ? JSON.parse(result) : result) as {
      data?: {
         array_value?: Array<{
            record_value?: Array<{ number_value?: number }>;
         }>;
      };
   };
   return rows.data?.array_value?.[0]?.record_value?.[0]?.number_value;
}

describe("a query that bakes a default-less given loads", () => {
   let tempDir: string;
   let pool: PackageLoadPool;
   let duckdb: DuckDBConnection;
   const originalWorkers = process.env.PACKAGE_LOAD_WORKERS;

   beforeAll(async () => {
      process.env.PACKAGE_LOAD_WORKERS = "1";
      pool = new PackageLoadPool(1);
      await __setPackageLoadPoolForTests(pool);
   });

   afterAll(async () => {
      await __setPackageLoadPoolForTests(null);
      if (originalWorkers === undefined)
         delete process.env.PACKAGE_LOAD_WORKERS;
      else process.env.PACKAGE_LOAD_WORKERS = originalWorkers;
   });

   beforeEach(() => {
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "publisher-unbound-"));
      duckdb = new DuckDBConnection("duckdb", ":memory:");
      fs.writeFileSync(
         path.join(tempDir, "publisher.json"),
         JSON.stringify({ name: "pkg", description: "unbound given" }),
      );
   });

   afterEach(async () => {
      await duckdb.close();
      fs.rmSync(tempDir, { recursive: true, force: true });
   });

   function write(shape: Shape): void {
      const target = path.join(tempDir, shape.file);
      fs.mkdirSync(path.dirname(target), { recursive: true });
      fs.writeFileSync(target, shape.text);
   }

   function malloyConfig(): MalloyConfig {
      const config = new MalloyConfig({ connections: {} });
      config.wrapConnections(
         () => new FixedConnectionMap(new Map([["duckdb", duckdb]]), "duckdb"),
      );
      return config;
   }

   for (const shape of SHAPES) {
      it(`on the worker path: ${shape.name}`, async () => {
         write(shape);
         const pkg = await Package.create(
            "env",
            "pkg",
            tempDir,
            malloyConfig(),
         );
         const model = pkg.getModel(shape.file);
         expect(model).toBeDefined();
         expect(model!.getCompilationError()).toBeUndefined();
      });

      it(`on the in-process path: ${shape.name}`, async () => {
         write(shape);
         const model = await Model.create(
            "pkg",
            tempDir,
            shape.file,
            new Map<string, Connection>([["duckdb", duckdb]]),
         );
         expect(model.getCompilationError()).toBeUndefined();
      });
   }

   it("lists the legacy notebook's cell and runs it with the given", async () => {
      write(SHAPES[2]);
      const pkg = await Package.create("env", "pkg", tempDir, malloyConfig());
      const model = pkg.getModel("nb.malloynb")!;
      const notebook = await model.getNotebook();
      expect(notebook.notebookCells?.some((c) => c.type === "code")).toBe(true);
      const run = await model.executeNotebookCell(1, undefined, false, {
         ORG: 2,
      });
      expect(count(run.result)).toBe(2);
   });

   it("lists the layout notebook's query cell", async () => {
      write(SHAPES[1]);
      const pkg = await Package.create("env", "pkg", tempDir, malloyConfig());
      const notebook = await pkg.getModel("notebooks/nb.malloy")!.getNotebook();
      expect(notebook.notebookCells?.some((c) => c.kind === "query")).toBe(
         true,
      );
   });

   it("runs an exported query with the given, and refuses it without", async () => {
      write(SHAPES[0]);
      const pkg = await Package.create("env", "pkg", tempDir, malloyConfig());
      const model = pkg.getModel("m.malloy")!;
      const withGiven = await model.getQueryResults(
         undefined,
         "for_org",
         undefined,
         undefined,
         undefined,
         { ORG: 1 },
      );
      expect(count(withGiven.result)).toBe(2);
      await expect(
         model.getQueryResults(undefined, "for_org", undefined),
      ).rejects.toThrow();
   });

   it("still applies #(access_filter) with the caller's given", async () => {
      write(SHAPES[4]);
      const pkg = await Package.create("env", "pkg", tempDir, malloyConfig());
      const model = pkg.getModel("m.malloy")!;
      const run = await model.getQueryResults(
         undefined,
         "all_gated",
         undefined,
         undefined,
         undefined,
         { GROUPS: [1] },
      );
      expect(count(run.result)).toBe(2);
   });

   it("still surfaces the #(authorize) gate", async () => {
      write(SHAPES[5]);
      const pkg = await Package.create("env", "pkg", tempDir, malloyConfig());
      expect(pkg.getModel("m.malloy")!.getAuthorize("gated")).toEqual([
         "'analyst' = $ROLE",
      ]);
   });
});
