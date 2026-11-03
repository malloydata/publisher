// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `gate-insecure-given`: a published source gated on a plain given warns, one
 * gated on `#(secure)` givens does not, and a hidden source is not read.
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
import { GATE_INSECURE_GIVEN } from "./gate_given_warnings";
import { Package } from "./package";

const ORIGINAL_ENV = process.env.PACKAGE_LOAD_WORKERS;
const ROWS = `duckdb.sql("select * from (values (1,'org-a'),(2,'org-b')) as t(id, org)")`;

describe("gate-insecure-given on package load", () => {
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
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "publisher-gategiven-"));
      duckdb = new DuckDBConnection("duckdb", ":memory:");
      fs.writeFileSync(
         path.join(tempDir, "publisher.json"),
         JSON.stringify({ name: "pkg", description: "gate givens" }),
      );
   });

   afterEach(async () => {
      await duckdb.close();
      fs.rmSync(tempDir, { recursive: true, force: true });
   });

   function write(file: string, text: string): void {
      fs.writeFileSync(path.join(tempDir, file), text);
   }

   async function warnings() {
      const config = new MalloyConfig({ connections: {} });
      config.wrapConnections(
         () => new FixedConnectionMap(new Map([["duckdb", duckdb]]), "duckdb"),
      );
      const pkg = await Package.create("env", "pkg", tempDir, config);
      return (pkg.getPackageMetadata().warnings ?? []).filter(
         (w) => w.code === GATE_INSECURE_GIVEN,
      );
   }

   it("warns once per published source gated on a plain given, naming it", async () => {
      write(
         "m.malloy",
         `##! experimental.givens
given: ORGS :: string[]
given: ROLES :: string[]
#(secure)
given: TENANTS :: string[]

#(authorize) 'admin' in $ROLES
#(access_filter) org in $ORGS
source: a is ${ROWS}

#(access_filter) org in $TENANTS
source: b is ${ROWS}

#(authorize) false
source: c is ${ROWS}
`,
      );
      const found = await warnings();
      expect(found.map((w) => [w.model, w.subject, w.severity])).toEqual([
         ["m.malloy", "a", "warn"],
      ]);
      expect(found[0].message).toContain("gated on $ORGS, $ROLES");
      expect(found[0].message).toContain("#(secure)");
   });

   it("reads only the surface", async () => {
      write(
         "hidden.malloy",
         `##! experimental.givens
given: ORGS :: string[]
#(access_filter) org in $ORGS
source: a is ${ROWS}
`,
      );
      write(
         "index.malloy",
         `##! experimental.givens
import "hidden.malloy"
source: open is ${ROWS}
export { open }
`,
      );
      expect(await warnings()).toEqual([]);
   });
});
