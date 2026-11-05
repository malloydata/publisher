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
import type { Model } from "./model";
import { insecureGateGivens } from "./gate_given_warnings";

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

   const HIDDEN_GATE = (secure: boolean) => `##! experimental.givens
${secure ? "#(secure)\n" : ""}given: ORGS :: string[]
#(access_filter) org in $ORGS
source: a is ${ROWS}
`;
   const writeSurface = (queryableSources: "all" | "declared") => {
      write(
         "publisher.json",
         JSON.stringify({ name: "pkg", description: "g", queryableSources }),
      );
      write(
         "index.malloy",
         `##! experimental.givens
import "hidden.malloy"
source: open is ${ROWS}
export { open }
`,
      );
   };

   it("refuses a hidden source gated on a plain given when every source is queryable", async () => {
      writeSurface("all");
      write("hidden.malloy", HIDDEN_GATE(false));
      const message = await refusal();
      expect(message).toContain("source a (hidden.malloy) is gated on $ORGS");
   });

   it("names a source imported into several models once", async () => {
      writeSurface("all");
      write("hidden.malloy", HIDDEN_GATE(false));
      write("other.malloy", `import "hidden.malloy"\n`);
      const message = await refusal();
      expect(message.match(/source a \(/g)).toHaveLength(1);
   });

   it("loads the same hidden source when its given is #(secure)", async () => {
      writeSurface("all");
      write("hidden.malloy", HIDDEN_GATE(true));
      const pkg = await load();
      expect(pkg.getPackageMetadata().name).toBe("pkg");
   });

   it("still loads the hidden plain-given source when only declared sources are queryable", async () => {
      writeSurface("declared");
      write("hidden.malloy", HIDDEN_GATE(false));
      const pkg = await load();
      expect(pkg.getPackageMetadata().name).toBe("pkg");
   });

   it("refuses a gate on a plain given when another model declares the same name #(secure)", async () => {
      write(
         "a.malloy",
         `##! experimental.givens
#(secure)
given: ROLES :: string[]
source: a is ${ROWS}
`,
      );
      write(
         "b.malloy",
         `##! experimental.givens
given: ROLES :: string[]
#(authorize) 'admin' in $ROLES
source: b is ${ROWS}
`,
      );
      const message = await refusal();
      expect(message).toContain("source b (b.malloy) is gated on $ROLES");
   });

   it("loads a gate on a #(secure) given its model imports from another", async () => {
      write(
         "a.malloy",
         `##! experimental.givens
#(secure)
given: ROLES :: string[]
source: a is ${ROWS}
`,
      );
      write(
         "b.malloy",
         `##! experimental.givens
import "a.malloy"
#(authorize) 'admin' in $ROLES
source: b is ${ROWS}
`,
      );
      const pkg = await load();
      expect(pkg.getPackageMetadata().name).toBe("pkg");
   });

   it("loads a gate on a #(secure) given declared where the gate is written, when the entry model does not surface it", async () => {
      write(
         "a.malloy",
         `##! experimental.givens
#(secure)
given: ROLES :: string[]

#(authorize) 'admin' in $ROLES
source: g is ${ROWS}
`,
      );
      write(
         "hub.malloy",
         `import "a.malloy"
source: mid is g extend {}
`,
      );
      write("entry.malloy", `import "hub.malloy"\n`);
      const pkg = await load();
      expect(pkg.getPackageMetadata().name).toBe("pkg");
   });
});

describe("insecureGateGivens fails closed", () => {
   const stubModel = (note: { text: string; at?: { url: string } }) =>
      ({
         getSources: () => [{ name: "s" }],
         getGivens: () => [{ name: "ORGS", secure: true }],
         getModelDef: () => ({
            contents: {
               s: { type: "table", annotations: { notes: [note] } },
            },
         }),
      }) as unknown as Model;
   const gate = "#(access_filter) org in $ORGS\n";

   it("refuses when the gate note has no position", () => {
      const models = new Map([["m.malloy", stubModel({ text: gate })]]);
      expect(insecureGateGivens(models, () => true)).toHaveLength(1);
   });

   it("refuses when the gate is written in a file that is not a package model", () => {
      const models = new Map([
         [
            "m.malloy",
            stubModel({
               text: gate,
               at: { url: "file:///elsewhere/x.malloy" },
            }),
         ],
      ]);
      expect(insecureGateGivens(models, () => true)).toHaveLength(1);
   });

   it("admits it when the declaring model is in the package and the given is secure there", () => {
      const models = new Map([
         [
            "m.malloy",
            stubModel({ text: gate, at: { url: "file:///pkg/m.malloy" } }),
         ],
      ]);
      expect(insecureGateGivens(models, () => true)).toEqual([]);
   });
});
