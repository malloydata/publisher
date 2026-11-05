// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `Package.classification` over a real package load: published entry points
 * only, each with its `#(carries)` and its field count by class.
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
const ROWS = `duckdb.sql("select * from (values (1,'east','999-11-2222',10,'org-a'),(2,'west','999-33-4444',20,'org-b')) as t(id, region, ssn, amount, org)")`;

const BASE = `##! experimental.access_modifiers
#(authorize) false
source: people is ${ROWS} include { internal: id, ssn, region, amount, org } extend {
   #(classification) ["phi.direct_identifier", "gdpr.personal"]
   dimension: social is ssn
   #(classification) "phi.financial"
   dimension: spend is amount
   #(classification) "none"
   dimension: area is region
   #(classification) "phi.quasi_identifier"
   dimension: org_id is org
}
`;

const INDEX = `##! experimental.givens
##! experimental.access_modifiers
import "base.malloy"

#(secure)
given: ROLES :: string[]
#(secure)
given: ORGS :: string[]

#(authorize) 'clinician' in $ROLES
#(access_filter) org_id in $ORGS
#(carries) ["phi.direct_identifier", "gdpr.personal", "phi.financial", "phi.quasi_identifier", "none"]
source: chart is people extend {
   dimension: masked is concat('***', social)
   #(classification) "none"
   dimension: initial is substr(area, 1, 1)
   measure: total is spend.sum()
   #(classification) "none"
   measure: n is count()
}

export { chart }
`;

const PLAIN = `source: plain is duckdb.sql("select 1 as a, 2 as b") extend {
   dimension: c is a + b
}
`;

describe("Package.getClassificationSummary", () => {
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
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "publisher-classsum-"));
      duckdb = new DuckDBConnection("duckdb", ":memory:");
      fs.writeFileSync(
         path.join(tempDir, "publisher.json"),
         JSON.stringify({ name: "pkg", description: "classification summary" }),
      );
   });

   afterEach(async () => {
      await duckdb.close();
      fs.rmSync(tempDir, { recursive: true, force: true });
   });

   async function load(files: Record<string, string>) {
      for (const [name, text] of Object.entries(files)) {
         fs.writeFileSync(path.join(tempDir, name), text);
      }
      const config = new MalloyConfig({ connections: {} });
      config.wrapConnections(
         () => new FixedConnectionMap(new Map([["duckdb", duckdb]]), "duckdb"),
      );
      return Package.create("env", "pkg", tempDir, config);
   }

   it("lists each published entry point with its carries and its fields by class, and no hidden source", async () => {
      const pkg = await load({ "base.malloy": BASE, "index.malloy": INDEX });
      expect(pkg.getClassificationSummary()).toEqual({
         sources: [
            {
               modelPath: "index.malloy",
               name: "chart",
               carries: [
                  "gdpr.personal",
                  "none",
                  "phi.direct_identifier",
                  "phi.financial",
                  "phi.quasi_identifier",
               ],
               // area, org_id, social, spend, and masked, initial, total, n.
               fields: 8,
               unclassified: 0,
               classCounts: {
                  // social, and masked by lineage.
                  "gdpr.personal": 2,
                  none: 3,
                  "phi.direct_identifier": 2,
                  // spend, and total by lineage.
                  "phi.financial": 2,
                  "phi.quasi_identifier": 1,
               },
            },
         ],
      });
   });

   it("is computed once per load and returned by reference", async () => {
      const pkg = await load({ "base.malloy": BASE, "index.malloy": INDEX });
      expect(pkg.getClassificationSummary()).toBe(
         pkg.getClassificationSummary(),
      );
   });

   it("is absent for a package that classifies nothing", async () => {
      const pkg = await load({ "plain.malloy": PLAIN });
      expect(pkg.getClassificationSummary()).toBeUndefined();
   });

   it("stays off the package metadata the status poll reads, and a PATCH body cannot store it", async () => {
      const pkg = await load({ "base.malloy": BASE, "index.malloy": INDEX });
      expect(pkg.getPackageMetadata().classification).toBeUndefined();
      pkg.setPackageMetadata({
         ...pkg.getPackageMetadata(),
         classification: { sources: [] },
      });
      expect(pkg.getPackageMetadata().classification).toBeUndefined();
   });
});
