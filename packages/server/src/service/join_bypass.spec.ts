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
import { lockTermsOf, missingLockTerm } from "./join_bypass";
import { Package } from "./package";

const ORIGINAL_ENV = process.env.PACKAGE_LOAD_WORKERS;
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

   const CHART = `#(authorize) 'clinician' in $ROLES
source: chart is ${PEOPLE}
`;

   it("refuses an entry locked to one role that joins a source locked to another, naming the missing term", async () => {
      write(
         "m.malloy",
         `${GIVENS}${CHART}
#(authorize) 'billing' in $ROLES
source: invoices is ${VISITS} extend {
   join_one: c is chart on c.id = person_id
}
`,
      );
      const message = await refusal();
      expect(message).toContain("source invoices (m.malloy)");
      expect(message).toContain("'clinician' in $ROLES");
      expect(message).toContain("join an extension of the source joined as c");
   });

   it("loads an entry whose lock carries the joined lock's term, however the literal is quoted", async () => {
      write(
         "m.malloy",
         `${GIVENS}${CHART}
#(authorize) "clinician" in $ROLES
source: visits_view is ${VISITS} extend {
   join_one: c is chart on c.id = person_id
}
`,
      );
      expect((await load()).getPackageMetadata().name).toBe("pkg");
   });

   it("loads an entry whose lock carries every joined term plus its own", async () => {
      write(
         "m.malloy",
         `${GIVENS}${CHART}
#(authorize) 'clinician' in $ROLES and 'north' in $ORGS
source: visits_view is ${VISITS} extend {
   join_one: c is chart on c.id = person_id
}
`,
      );
      expect((await load()).getPackageMetadata().name).toBe("pkg");
   });

   it("refuses an open entry that joins a locked source inside a -> pipeline stage", async () => {
      write(
         "m.malloy",
         `${GIVENS}
#(authorize) false
source: vault is ${PEOPLE}

source: open_visits is ${VISITS}

source: sneaky is open_visits -> {
   extend: { join_one: vault on vault.id = person_id }
   select: id, leak is vault.ssn
}
`,
      );
      const message = await refusal();
      expect(message).toContain(
         "source sneaky (m.malloy) is open to every caller and joins vault, which is locked",
      );
   });

   it("loads an entry that joins a curated extension of a locked base under the same lock", async () => {
      write(
         "m.malloy",
         `${GIVENS}
#(authorize) false
source: people_base is ${PEOPLE}

#(authorize) 'clinician' in $ROLES
source: people is people_base extend { dimension: shown is org }

#(authorize) 'clinician' in $ROLES
source: visits is ${VISITS} extend {
   join_one: person is people on person.id = person_id
}
`,
      );
      expect((await load()).getPackageMetadata().name).toBe("pkg");
   });

   it("refuses a locked source joined inside a -> pipeline stage", async () => {
      write(
         "m.malloy",
         `${GIVENS}${CHART}
source: open_visits is ${VISITS}

source: sneaky is open_visits -> {
   extend: { join_one: chart on chart.id = person_id }
   select: id, leak is chart.ssn
}
`,
      );
      const message = await refusal();
      expect(message).toContain("source sneaky (m.malloy)");
      expect(message).toContain("'clinician' in $ROLES");
   });

   const SECRETS = `#(authorize) false
source: secrets is duckdb.sql("select 1 as id, 'x' as secret")
`;
   const STAGING = `source: staging is ${VISITS} extend {
   join_one: secrets on secrets.id = person_id
}
`;

   it("refuses an open entry derived with -> from a hidden source that joins a locked one", async () => {
      write("base.malloy", `${GIVENS}${SECRETS}${STAGING}`);
      write(
         "index.malloy",
         `##! experimental.givens
import "base.malloy"

source: pub is staging -> { group_by: id, leak is secrets.secret }

export { pub }
`,
      );
      expect(await refusal()).toContain(
         "source pub (index.malloy) is open to every caller and joins secrets, which is locked",
      );
   });

   it("loads an entry derived with -> from a hidden source whose joins are not gated", async () => {
      write(
         "base.malloy",
         `${GIVENS}
source: lookup is duckdb.sql("select 1 as id, 'x' as label")
source: staging is ${VISITS} extend {
   join_one: lookup on lookup.id = person_id
}
`,
      );
      write(
         "index.malloy",
         `##! experimental.givens
import "base.malloy"

source: pub is staging -> { group_by: id, label is lookup.label }

export { pub }
`,
      );
      expect((await load()).getPackageMetadata().name).toBe("pkg");
   });

   it("refuses an open compose entry whose member joins a locked source, and loads one over open sources", async () => {
      const composed = (
         join: string,
      ) => `##! experimental { composite_sources givens }
${GIVENS.split("\n").slice(1).join("\n")}
${SECRETS}
source: a is ${VISITS} extend {
   ${join}
   dimension: k is id
}
source: b is ${VISITS} extend { dimension: k is id }
source: pub is compose(a, b) -> { group_by: k }
`;
      write(
         "m.malloy",
         composed("join_one: secrets on secrets.id = person_id"),
      );
      expect(await refusal()).toContain(
         "source pub (m.malloy) is open to every caller and joins secrets, which is locked",
      );
      write(
         "m.malloy",
         composed(
            'join_one: lookup is duckdb.sql("select 1 as id") on lookup.id = person_id',
         ).replace(SECRETS, ""),
      );
      expect((await load()).getPackageMetadata().name).toBe("pkg");
   });

   it("compares row-level terms by given name only: a filter on the same given over a column the join does not tie still loads", async () => {
      write(
         "m.malloy",
         `${GIVENS}
#(access_filter) org in $ORGS
source: people is ${PEOPLE}

#(access_filter) org in $ORGS
source: visits is ${VISITS} extend {
   join_one: person is people on person.id = person_id
}
`,
      );
      expect((await load()).getPackageMetadata().name).toBe("pkg");
   });

   it("counts a gate on a source hidden from discovery when a walk is cut short", async () => {
      // Only s0 is an entry; s17, the sole gate in the package, is a lock nobody can list.
      let base = `${GIVENS}#(authorize) false\nsource: s17 is ${PEOPLE}\n`;
      for (let i = 16; i >= 1; i--) {
         base += `source: s${i} is ${PEOPLE} extend {
   join_one: n is s${i + 1} on n.id = id
}
`;
      }
      write("base.malloy", `${base}\nexport { s1 }\n`);
      write(
         "index.malloy",
         `##! experimental.givens
import "base.malloy"

source: s0 is ${PEOPLE} extend {
   join_one: n is s1 on n.id = id
}

export { s0 }
`,
      );
      expect(await refusal()).toContain(
         "source s0 (index.malloy) joins more than 16 deep",
      );
   });

   /** `s0 -> s1 -> ... -> s<joins>`, s0 gated unless `gated` is false. */
   function chain(joins: number, gated: boolean): string {
      let out = `${GIVENS}source: s${joins} is ${PEOPLE}\n`;
      for (let i = joins - 1; i >= 0; i--) {
         const gate =
            i === 0 && gated ? "#(authorize) 'auditor' in $ROLES\n" : "";
         out += `${gate}source: s${i} is ${PEOPLE} extend {
   join_one: n is s${i + 1} on n.id = id
}
`;
      }
      return `${out}\nexport { s0 }\n`;
   }

   it("reads 16 joins from the entry and refuses a 17th in a package that has a gate", async () => {
      write("m.malloy", chain(16, true));
      expect((await load()).getPackageMetadata().name).toBe("pkg");
      write("m.malloy", chain(17, true));
      const message = await refusal();
      expect(message).toContain("source s0 (m.malloy)");
      expect(message).toContain("joins more than 16 deep");
   });

   it("refuses a fan-out past the join-path budget in a package that has a gate", async () => {
      let out = `${GIVENS}source: f10 is ${PEOPLE}\n`;
      for (let i = 9; i >= 0; i--) {
         const gate = i === 0 ? "#(authorize) 'auditor' in $ROLES\n" : "";
         out += `${gate}source: f${i} is ${PEOPLE} extend {
   join_one: a is f${i + 1} on a.id = id
   join_one: b is f${i + 1} on b.id = id
}
`;
      }
      write("m.malloy", `${out}\nexport { f0 }\n`);
      expect(await refusal()).toContain("join paths");
   });

   it("loads a source past the bound when no source in the package has a gate", async () => {
      write("m.malloy", chain(17, false));
      expect((await load()).getPackageMetadata().name).toBe("pkg");
   });
});

describe("lock term comparison", () => {
   const terms = (...exprs: string[]) => {
      const out = lockTermsOf(exprs);
      if (out === "false") throw new Error("unexpected deny-all");
      return out;
   };

   it("treats the same literal under a list given and a scalar given as different terms", () => {
      expect(missingLockTerm(terms("'x' = $R"), terms("'x' in $R"))).toBe(
         "'x' = $R",
      );
      expect(
         missingLockTerm(terms("'x' in $R"), terms("'x' in $R")),
      ).toBeUndefined();
   });

   it("compares decoded literals, so either quote style is the same term", () => {
      expect(
         missingLockTerm(terms("'x' in $R"), terms('"x" in $R')),
      ).toBeUndefined();
      expect(
         missingLockTerm(terms("'x' in $R"), terms("'X' in $R")),
      ).toBeDefined();
   });

   it("reads an unparseable or deny-all lock as false", () => {
      expect(lockTermsOf(["false"])).toBe("false");
      expect(lockTermsOf(["not a term"])).toBe("false");
   });
});
