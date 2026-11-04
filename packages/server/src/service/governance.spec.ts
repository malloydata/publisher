// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The governance export over a real package load: every source of every
 * model, hidden ones marked, with gates, `#(carries)` and classified
 * fields, sorted so the document is the same on every server.
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
import { packageGovernance } from "./governance";
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

describe("packageGovernance", () => {
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
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "publisher-governance-"));
      duckdb = new DuckDBConnection("duckdb", ":memory:");
      fs.writeFileSync(
         path.join(tempDir, "publisher.json"),
         JSON.stringify({ name: "pkg", description: "governance" }),
      );
      fs.writeFileSync(path.join(tempDir, "base.malloy"), BASE);
      fs.writeFileSync(path.join(tempDir, "index.malloy"), INDEX);
   });

   afterEach(async () => {
      await duckdb.close();
      fs.rmSync(tempDir, { recursive: true, force: true });
   });

   async function governance() {
      const config = new MalloyConfig({ connections: {} });
      config.wrapConnections(
         () => new FixedConnectionMap(new Map([["duckdb", duckdb]]), "duckdb"),
      );
      const pkg = await Package.create("env", "pkg", tempDir, config);
      return packageGovernance(pkg, "env", "pkg");
   }

   it("describes every source, hidden and published, with gates, carries and classified fields", async () => {
      const doc = await governance();
      expect(doc.environmentName).toBe("env");
      expect(doc.packageName).toBe("pkg");
      expect(doc.classifiesFields).toBe(true);
      expect(doc.unreadModels).toBeUndefined();
      expect(doc.secureGivens).toEqual(["ORGS", "ROLES"]);
      expect(
         doc.sources.map((s) => [s.modelPath, s.name, s.published]),
      ).toEqual([
         ["base.malloy", "people", false],
         ["index.malloy", "chart", true],
      ]);

      const people = doc.sources[0];
      expect(people.deniedToAll).toBe(true);
      expect(people.authorize).toEqual([{ expression: "false", givens: [] }]);
      expect(people.carries).toBeUndefined();
      expect(
         people.fields.map((f) => [f.path, f.kind, f.classifications]),
      ).toEqual([
         ["area", "dimension", ["none"]],
         ["org_id", "dimension", ["phi.quasi_identifier"]],
         ["social", "dimension", ["gdpr.personal", "phi.direct_identifier"]],
         ["spend", "dimension", ["phi.financial"]],
      ]);

      const chart = doc.sources[1];
      expect(chart.deniedToAll).toBe(false);
      expect(chart.authorize).toEqual([
         {
            expression: "'clinician' in $ROLES",
            givens: [{ name: "ROLES", secure: true }],
         },
      ]);
      expect(chart.accessFilter).toEqual([
         {
            expression: "org_id in $ORGS",
            givens: [{ name: "ORGS", secure: true }],
         },
      ]);
      expect(chart.carries).toEqual([
         "gdpr.personal",
         "none",
         "phi.direct_identifier",
         "phi.financial",
         "phi.quasi_identifier",
      ]);

      const field = (p: string) => chart.fields.find((f) => f.path === p);
      expect(field("masked")).toEqual({
         path: "masked",
         kind: "dimension",
         classifications: ["gdpr.personal", "phi.direct_identifier"],
         inferred: true,
         inputs: [
            {
               field: "social",
               classifications: ["gdpr.personal", "phi.direct_identifier"],
            },
         ],
      });
      expect(field("initial")).toEqual({
         path: "initial",
         kind: "dimension",
         classifications: ["none"],
         inputs: [{ field: "area", classifications: ["none"] }],
      });
      expect(field("total")).toMatchObject({
         kind: "measure",
         classifications: ["phi.financial"],
         inferred: true,
      });
      expect(field("n")).toEqual({
         path: "n",
         kind: "measure",
         classifications: ["none"],
      });
      // Hidden columns are not reachable, so not listed.
      expect(field("ssn")).toBeUndefined();
      expect(field("id")).toBeUndefined();
   });

   it("writes the same document twice", async () => {
      const a = JSON.stringify(await governance());
      const b = JSON.stringify(await governance());
      expect(a).toBe(b);
   });
});
