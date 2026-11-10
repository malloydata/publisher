// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A source whose `#(authorize)` and `#(access_filter)` read a given that is
 * not `#(secure)` loads and serves: an API customer sets its givens from its
 * own trusted tier, so the gate is a real control there. Drives
 * `Package.create` on the worker path.
 */
import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   MalloyConfig,
   type GivenValue,
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
import { AccessDeniedError } from "../errors";
import {
   PackageLoadPool,
   __setPackageLoadPoolForTests,
} from "../package_load/package_load_pool";
import { unrecordedAudit } from "./access_record";
import { Package } from "./package";
import { writeTempPackage } from "../test_helpers/temp_package";
import { QueryPrivileges } from "./model_query_request";

const ROWS = `duckdb.sql("select * from (values (1,'org-a'),(2,'org-b'),(3,'org-b')) as t(id, org)")`;

const MODEL = `##! experimental.givens
given: ROLES :: string[]
given: ORGS :: string[]

#(authorize) 'clinician' in $ROLES
#(access_filter) org in $ORGS
source: chart is ${ROWS}
`;

describe("a gate on a given that is not #(secure)", () => {
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
      tempDir = writeTempPackage(
         { "m.malloy": MODEL },
         {
            prefix: "publisher-plaingate-",
            manifest: { name: "pkg", description: "plain given gate" },
         },
      ).dir;
      duckdb = new DuckDBConnection("duckdb", ":memory:");
   });

   afterEach(async () => {
      await duckdb.close();
      fs.rmSync(tempDir, { recursive: true, force: true });
   });

   it("loads, admits the caller its lock names, and filters to its rows", async () => {
      const config = new MalloyConfig({ connections: {} });
      config.wrapConnections(
         () => new FixedConnectionMap(new Map([["duckdb", duckdb]]), "duckdb"),
      );
      const pkg = await Package.create("env", "pkg", tempDir, config);
      const run = (givens: Record<string, unknown>) =>
         pkg.getModel("m.malloy")!.runQuery(
            unrecordedAudit(),
            {
               query: "run: chart -> { aggregate: n is count() }",
               filterParams: {},
               givens: givens as Record<string, GivenValue>,
            },
            QueryPrivileges.of({ bypassFilters: true }),
         );
      const admitted = await run({ ROLES: ["clinician"], ORGS: ["org-b"] });
      expect(admitted.compactResult).toEqual([{ n: 2 }]);
      await expect(
         run({ ROLES: ["billing"], ORGS: ["org-b"] }),
      ).rejects.toBeInstanceOf(AccessDeniedError);
   });
});
