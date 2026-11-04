// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * An author join from a published source that bypasses a gate on what it
 * joins refuses the package load; one gated on the same givens, or a hidden
 * entry nobody can enter, loads. Drives `Package.create` on the worker path.
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
import { BadRequestError } from "../errors";
import {
   PackageLoadPool,
   __setPackageLoadPoolForTests,
} from "../package_load/package_load_pool";
import { Package } from "./package";

const PEOPLE = `duckdb.sql("select * from (values (1,'org-a','999'),(2,'org-b','888')) as t(id, org, ssn)")`;
const VISITS = `duckdb.sql("select * from (values (1,1,'org-a'),(2,2,'org-b')) as t(id, person_id, org)")`;

const GIVENS = `##! experimental.givens
#(secure)
given: ROLES :: string[]
#(secure)
given: ORGS :: string[]
`;

describe("join bypass refusal on package load", () => {
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
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "publisher-joinbypass-"));
      duckdb = new DuckDBConnection("duckdb", ":memory:");
      fs.writeFileSync(
         path.join(tempDir, "publisher.json"),
         JSON.stringify({ name: "pkg", description: "join bypass" }),
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

   it("refuses an open source that joins a locked base", async () => {
      write(
         "m.malloy",
         `${GIVENS}
#(authorize) false
source: people is ${PEOPLE}

source: visits is ${VISITS} extend {
   join_one: person is people on person.id = person_id
}
`,
      );
      const message = await refusal();
      expect(message).toContain(
         "source visits (m.malloy) is open to every caller and joins person, which is locked",
      );
   });

   it("refuses a source re-opened with true over a join to a source that inherits its lock through extend", async () => {
      write(
         "m.malloy",
         `${GIVENS}
#(authorize) false
source: people_base is ${PEOPLE}

source: people is people_base extend { dimension: secret is ssn }

#(authorize) false
source: visits_base is ${VISITS}

#(authorize) true
source: visits is visits_base extend {
   join_one: person is people on person.id = person_id
}
`,
      );
      const message = await refusal();
      expect(message).toContain(
         "source visits (m.malloy) is open to every caller and joins person, which is locked",
      );
   });

   it("refuses an entry gated on other givens than the joined filter reads", async () => {
      write(
         "m.malloy",
         `${GIVENS}
#(access_filter) org in $ORGS
source: people is ${PEOPLE}

#(authorize) 'clinician' in $ROLES
source: visits is ${VISITS} extend {
   join_one: person is people on person.id = person_id
}
`,
      );
      const message = await refusal();
      expect(message).toContain(
         "source visits (m.malloy) joins person, whose gate reads $ORGS, which no gate on visits reads",
      );
      expect(message).toContain("#(access_filter) person.<column> in $ORGS");
   });

   it("loads an entry scoped on the joined filter's given, and one gated over a locked base", async () => {
      write(
         "m.malloy",
         `${GIVENS}
#(access_filter) org in $ORGS
source: people is ${PEOPLE}

#(authorize) false
source: vault is ${PEOPLE}

#(authorize) 'clinician' in $ROLES
#(access_filter) person.org in $ORGS
source: visits is ${VISITS} extend {
   join_one: person is people on person.id = person_id
   join_one: locked_person is vault on locked_person.id = person_id
}
`,
      );
      const pkg = await load();
      expect(pkg.getPackageMetadata().name).toBe("pkg");
   });

   it("follows nested joins from the entry, and skips an entry nobody can enter", async () => {
      write(
         "base.malloy",
         `${GIVENS}
#(access_filter) org in $ORGS
source: people is ${PEOPLE}

source: visits is ${VISITS} extend {
   join_one: person is people on person.id = person_id
}
`,
      );
      write(
         "index.malloy",
         `##! experimental.givens
import "base.malloy"

#(authorize) 'auditor' in $ROLES
source: audit is duckdb.sql("select 1 as id, 1 as visit_id") extend {
   join_one: visit is visits on visit.id = visit_id
}

export { audit }
`,
      );
      // `visits` is hidden (not exported), so its own bypass is not an entry;
      // `audit` is published and reaches people through visit.person.
      const message = await refusal();
      expect(message).toContain(
         "source audit (index.malloy) joins visit.person",
      );
      expect(message).not.toContain("source visits");
   });
});
