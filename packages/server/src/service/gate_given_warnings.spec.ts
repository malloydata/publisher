// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A published source gated on a plain given refuses the package load, naming
 * the givens to mark; one gated on `#(secure)` givens loads, and a hidden
 * source is not read.
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
import { BadRequestError } from "../errors";
import { Package } from "./package";

const ROWS = `duckdb.sql("select * from (values (1,'org-a'),(2,'org-b')) as t(id, org)")`;

describe("gate-insecure-given on package load", () => {
   let tempDir: string;
   let pool: PackageLoadPool;
   let duckdb: DuckDBConnection;

   beforeAll(async () => {
      pool = new PackageLoadPool(1);
      await __setPackageLoadPoolForTests(pool);
   });

   afterAll(async () => {
      await __setPackageLoadPoolForTests(null);
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

   async function load() {
      const config = new MalloyConfig({ connections: {} });
      config.wrapConnections(
         () => new FixedConnectionMap(new Map([["duckdb", duckdb]]), "duckdb"),
      );
      return Package.create("env", "pkg", tempDir, config);
   }

   async function refusal(): Promise<string> {
      try {
         await load();
      } catch (error) {
         expect(error).toBeInstanceOf(BadRequestError);
         return (error as Error).message;
      }
      throw new Error(
         "expected the load to be refused, but the package loaded",
      );
   }

   it("refuses a published source gated on a plain given, naming the givens", async () => {
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
      const message = await refusal();
      expect(message).toContain(
         "source a (m.malloy) is gated on $ORGS, $ROLES",
      );
      expect(message).toContain("#(secure)");
      expect(message).not.toContain("source b ");
      expect(message).not.toContain("source c ");
   });

   it("reads one #(secure) above a given: block as marking every item in it", async () => {
      write(
         "m.malloy",
         `##! experimental.givens
#(secure)
given:
  ORGS :: string[]
  ROLES :: string[]

#(authorize) 'admin' in $ROLES
#(access_filter) org in $ORGS
source: a is ${ROWS}
`,
      );
      const pkg = await load();
      expect(pkg.getPackageMetadata().name).toBe("pkg");
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
      const pkg = await load();
      expect(pkg.getPackageMetadata().name).toBe("pkg");
   });
});
