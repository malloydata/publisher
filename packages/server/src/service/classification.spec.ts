// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `#(classification) <class>` on a field, and the two package warnings it
 * drives: `classification-coverage` on a published source with unclassified
 * fields once any field in the package carries a class, and
 * `classification-malformed` on an annotation that names no single class.
 * Drives `Package.create` on the worker path, as the other warnings specs do.
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
import {
   CLASSIFICATION_COVERAGE,
   CLASSIFICATION_MALFORMED,
   isClassificationLine,
   readClassification,
} from "./classification";
import { Package } from "./package";

describe("readClassification", () => {
   it("reads the one token after the route, from a string or a {value} note", () => {
      expect(
         readClassification(["#(classification) phi.direct_identifier"]),
      ).toEqual({ value: "phi.direct_identifier" });
      expect(
         readClassification([
            { value: "#(doc) The patient's SSN.\n" },
            { value: "#(classification)   confidential \n" },
         ]),
      ).toEqual({ value: "confidential" });
   });

   it("is absent on a field with no classification note", () => {
      expect(readClassification(undefined)).toEqual({});
      expect(readClassification(["#(doc) Nothing here."])).toEqual({});
   });

   it("names the fault of a note with no class, several words, or a twin", () => {
      expect(readClassification(["#(classification)"]).malformed).toContain(
         "names no class",
      );
      expect(
         readClassification(["#(classification) phi direct identifier"])
            .malformed,
      ).toContain("3 words");
      expect(
         readClassification(["#(classification) a", "#(classification) b"])
            .malformed,
      ).toContain("2 #(classification) annotations");
   });

   it("tells a classification line from any other annotation", () => {
      expect(isClassificationLine("#(classification) x")).toBe(true);
      expect(isClassificationLine("  #(classification)\n")).toBe(true);
      expect(isClassificationLine("#(doc) classification of things")).toBe(
         false,
      );
      expect(isClassificationLine("#(classification x)")).toBe(false);
   });
});

const ORIGINAL_ENV = process.env.PACKAGE_LOAD_WORKERS;
const ROWS = `duckdb.sql("select * from (values (1,'east','999-11-2222',10),(2,'west','999-33-4444',20)) as t(id, region, ssn, amount)")`;

describe("classification warnings on package load", () => {
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
      tempDir = fs.mkdtempSync(
         path.join(os.tmpdir(), "publisher-classification-"),
      );
      duckdb = new DuckDBConnection("duckdb", ":memory:");
      fs.writeFileSync(
         path.join(tempDir, "publisher.json"),
         JSON.stringify({ name: "pkg", description: "classification" }),
      );
   });

   afterEach(async () => {
      await duckdb.close();
      fs.rmSync(tempDir, { recursive: true, force: true });
   });

   function write(file: string, text: string): void {
      const target = path.join(tempDir, file);
      fs.mkdirSync(path.dirname(target), { recursive: true });
      fs.writeFileSync(target, text);
   }

   async function warnings() {
      const config = new MalloyConfig({ connections: {} });
      config.wrapConnections(
         () => new FixedConnectionMap(new Map([["duckdb", duckdb]]), "duckdb"),
      );
      const pkg = await Package.create("env", "pkg", tempDir, config);
      return (pkg.getPackageMetadata().warnings ?? []).filter((w) =>
         (w.code ?? "").startsWith("classification-"),
      );
   }

   it("says nothing about a package that classifies nothing", async () => {
      write(
         "orders.malloy",
         `source: orders is ${ROWS} extend {
   dimension: area is region
   measure: n is count()
}
`,
      );
      expect(await warnings()).toEqual([]);
   });

   it("once one field is classified, names every published field that is not, per source", async () => {
      write(
         "orders.malloy",
         `source: orders is ${ROWS} extend {
   #(classification) pii.direct_identifier
   dimension: social is ssn
   dimension: area is region
   measure: n is count()
}

source: totals is ${ROWS} extend {
   measure: total is amount.sum()
}
`,
      );
      const found = await warnings();
      expect(found.map((w) => [w.code, w.model, w.subject])).toEqual([
         [CLASSIFICATION_COVERAGE, "orders.malloy", "orders"],
         [CLASSIFICATION_COVERAGE, "orders.malloy", "totals"],
      ]);
      expect(found[0].severity).toBe("warn");
      // The classified field is not named; the raw columns (which are
      // published dimensions too), the derived one and the measure are.
      expect(found[0].message).toContain("publishes 6 fields");
      expect(found[0].message).toContain("id, region, ssn, amount, area, n");
      expect(found[0].message).not.toContain("social");
      expect(found[1].message).toContain("total");
   });

   it("reports a malformed note whether or not coverage is in force", async () => {
      write(
         "orders.malloy",
         `source: orders is ${ROWS} extend {
   #(classification)
   dimension: social is ssn
   dimension: area is region
}
`,
      );
      const found = await warnings();
      expect(found).toHaveLength(1);
      expect(found[0]).toMatchObject({
         code: CLASSIFICATION_MALFORMED,
         model: "orders.malloy",
         subject: "orders",
         severity: "warn",
      });
      expect(found[0].message).toContain("Field social of source orders");
      expect(found[0].message).toContain("names no class");
   });

   it("reads only the surface: a hidden base is reported where a published source reaches it, and a locked source not at all", async () => {
      write(
         "base.malloy",
         `source: people is ${ROWS} extend {
   #(classification) pii.direct_identifier
   dimension: social is ssn
   dimension: area is region
}
`,
      );
      write(
         "index.malloy",
         `##! experimental.givens
import "base.malloy"

#(authorize) false
source: locked is people extend {
   dimension: hidden_area is region
}

source: directory is ${ROWS} extend {
   #(classification) none
   dimension: area is region
   join_one: person is people on person.id = id
}

export { locked, directory }
`,
      );
      const found = await warnings();
      expect(found.map((w) => [w.model, w.subject])).toEqual([
         ["index.malloy", "directory"],
      ]);
      const message = found[0].message ?? "";
      // Its own raw columns, and the base's unclassified fields by join path.
      expect(message).toContain("person.area");
      expect(message).not.toContain("person.social");
      expect(message).not.toContain("hidden_area");
   });
});
