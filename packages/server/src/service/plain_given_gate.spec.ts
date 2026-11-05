// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A source whose `#(authorize)` and `#(access_filter)` read a given that is
 * not `#(secure)`: refused at load by default, and loaded and gated under
 * `PUBLISHER_REQUIRE_SECURE_GATE_GIVENS=false`, the setting for a deployment
 * whose callers set their givens from their own trusted tier. Drives
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
import * as os from "os";
import * as path from "path";
import { REQUIRE_SECURE_GATE_GIVENS_ENV } from "../config";
import { AccessDeniedError, BadRequestError } from "../errors";
import {
   PackageLoadPool,
   __setPackageLoadPoolForTests,
} from "../package_load/package_load_pool";
import { unrecordedAudit } from "./access_record";
import { Package } from "./package";

const ORIGINAL_ENV = process.env.PACKAGE_LOAD_WORKERS;
const ORIGINAL_REQUIRE = process.env[REQUIRE_SECURE_GATE_GIVENS_ENV];
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
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "publisher-plaingate-"));
      duckdb = new DuckDBConnection("duckdb", ":memory:");
      fs.writeFileSync(
         path.join(tempDir, "publisher.json"),
         JSON.stringify({ name: "pkg", description: "plain given gate" }),
      );
      fs.writeFileSync(path.join(tempDir, "m.malloy"), MODEL);
   });

   afterEach(async () => {
      if (ORIGINAL_REQUIRE === undefined) {
         delete process.env[REQUIRE_SECURE_GATE_GIVENS_ENV];
      } else {
         process.env[REQUIRE_SECURE_GATE_GIVENS_ENV] = ORIGINAL_REQUIRE;
      }
      await duckdb.close();
      fs.rmSync(tempDir, { recursive: true, force: true });
   });

   const malloyConfig = () => {
      const config = new MalloyConfig({ connections: {} });
      config.wrapConnections(
         () => new FixedConnectionMap(new Map([["duckdb", duckdb]]), "duckdb"),
      );
      return config;
   };

   it("is refused at load by default, naming the source and the given", async () => {
      delete process.env[REQUIRE_SECURE_GATE_GIVENS_ENV];
      let error: unknown;
      try {
         await Package.create("env", "pkg", tempDir, malloyConfig());
      } catch (e) {
         error = e;
      }
      expect(error).toBeInstanceOf(BadRequestError);
      expect((error as Error).message).toContain(
         "source chart (m.malloy) is gated on",
      );
   });

   it("with the requirement off, loads, admits the caller its lock names, and filters to its rows", async () => {
      process.env[REQUIRE_SECURE_GATE_GIVENS_ENV] = "false";
      const pkg = await Package.create("env", "pkg", tempDir, malloyConfig());
      const run = (givens: Record<string, unknown>) =>
         pkg
            .getModel("m.malloy")!
            .getQueryResults(
               unrecordedAudit(),
               undefined,
               undefined,
               "run: chart -> { aggregate: n is count() }",
               {},
               true,
               givens as Record<string, GivenValue>,
            );
      const admitted = await run({ ROLES: ["clinician"], ORGS: ["org-b"] });
      expect(admitted.compactResult).toEqual([{ n: 2 }]);
      await expect(
         run({ ROLES: ["billing"], ORGS: ["org-b"] }),
      ).rejects.toBeInstanceOf(AccessDeniedError);
   });
});
