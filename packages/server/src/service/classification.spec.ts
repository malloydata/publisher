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
import { BadRequestError } from "../errors";
import {
   CLASSIFICATION_COVERAGE,
   CLASSIFICATION_MALFORMED,
   isClassificationLine,
   readClassification,
   readSourceClassifications,
} from "./classification";
import { compileModelFiles } from "../test_helpers/get_context_join_fixture";
import type { ModelDef } from "@malloydata/malloy";
import { Package } from "./package";

describe("readClassification", () => {
   it("reads one quoted class, or a quoted list, from a string or a {value} note", () => {
      expect(
         readClassification(['#(classification) "phi.direct_identifier"']),
      ).toEqual({ values: ["phi.direct_identifier"] });
      expect(
         readClassification([
            { value: "#(doc) The patient's SSN.\n" },
            {
               value: '#(classification) ["phi.direct_identifier", "gdpr.personal"]\n',
            },
         ]),
      ).toEqual({ values: ["gdpr.personal", "phi.direct_identifier"] });
   });

   it("is empty on a field with no classification note", () => {
      expect(readClassification(undefined)).toEqual({ values: [] });
      expect(readClassification(["#(doc) Nothing here."])).toEqual({
         values: [],
      });
   });

   it("refuses an unquoted class, an empty note, an empty list, and a second note", () => {
      expect(
         readClassification(["#(classification) phi.direct_identifier"])
            .malformed,
      ).toContain("is not quoted");
      expect(readClassification(["#(classification)"]).malformed).toContain(
         "names nothing",
      );
      expect(readClassification(["#(classification) []"]).malformed).toContain(
         "names nothing",
      );
      expect(
         readClassification(['#(classification) "a"', '#(classification) "b"'])
            .malformed,
      ).toContain("2 #(classification) annotations");
   });

   it("tells a classification or carries line from any other annotation", () => {
      expect(isClassificationLine('#(classification) "x"')).toBe(true);
      expect(isClassificationLine("  #(classification)\n")).toBe(true);
      expect(isClassificationLine('#(carries) ["a", "b"]')).toBe(true);
      // A report's filter-control note is not a classification line.
      expect(isClassificationLine('#(control) label="Since"')).toBe(false);
      expect(isClassificationLine("#(doc) classification of things")).toBe(
         false,
      );
      expect(isClassificationLine("#(classification x)")).toBe(false);
   });
});

describe("readSourceClassifications (lineage)", () => {
   const MODEL = `
##! experimental.access_modifiers
source: people is duckdb.sql("select 1 as id, '999' as ssn, 'A' as first, 'B' as last, 10 as spend, 'MA' as st") include { internal: ssn public: * } extend {
  #(classification) "phi.direct_identifier"
  dimension: social is ssn
  #(classification) "phi.direct_identifier"
  dimension: first_name is first
  #(classification) "phi.financial"
  dimension: spent is spend
  #(classification) "phi.quasi_identifier"
  dimension: state_code is st
}

source: chart is people extend {
  rename: ssn_renamed is social
  dimension: copied is ssn_renamed
  dimension: masked is concat('***', copied)
  #(classification) "none"
  dimension: initials is substr(first_name, 1, 1)
  dimension: mixed is concat(first_name, spent)
  dimension: plain is upper(st)
  measure: socials is count(copied)
  measure: n is count()
}

source: visits is duckdb.sql("select 1 as id, 1 as person_id") extend {
  join_one: person is chart on person.id = person_id
  dimension: who is person.masked
  dimension: where_from is concat(person.state_code, '!')
}
`;
   let chart: Map<
      string,
      ReturnType<typeof readSourceClassifications> extends Map<string, infer V>
         ? V
         : never
   >;
   let visits: typeof chart;
   beforeAll(async () => {
      const { modelDefs } = await compileModelFiles({ "m.malloy": MODEL });
      const md = modelDefs["m.malloy"] as ModelDef;
      const fieldsOf = (name: string) =>
         (md.contents[name] as { fields?: unknown[] }).fields as never;
      chart = readSourceClassifications(fieldsOf("chart"));
      visits = readSourceClassifications(fieldsOf("visits"));
   });

   it("keeps a declared class through a rename", () => {
      expect(chart.get("ssn_renamed")?.declared).toEqual([
         "phi.direct_identifier",
      ]);
   });

   it("infers through a copy, a function, an intermediate, and an aggregate", () => {
      expect(chart.get("copied")).toMatchObject({
         inferred: ["phi.direct_identifier"],
         classifications: ["phi.direct_identifier"],
         inputs: [
            {
               field: "ssn_renamed",
               classifications: ["phi.direct_identifier"],
            },
         ],
      });
      // `masked` reads `copied`, which carries the class, so copied is the input.
      expect(chart.get("masked")).toMatchObject({
         inferred: ["phi.direct_identifier"],
         inputs: [
            { field: "copied", classifications: ["phi.direct_identifier"] },
         ],
      });
      expect(chart.get("socials")?.inferred).toEqual(["phi.direct_identifier"]);
   });

   it("lets a declared class win and still reports the inputs", () => {
      expect(chart.get("initials")).toMatchObject({
         declared: ["none"],
         classifications: ["none"],
         inputs: [
            { field: "first_name", classifications: ["phi.direct_identifier"] },
         ],
      });
      expect(chart.get("initials")?.inferred).toEqual([]);
   });

   it("unions inputs of differing classes, the fail-closed reading", () => {
      const mixed = chart.get("mixed");
      expect(mixed?.classifications).toEqual([
         "phi.direct_identifier",
         "phi.financial",
      ]);
      expect(mixed?.inferred).toEqual([
         "phi.direct_identifier",
         "phi.financial",
      ]);
      expect(mixed?.malformed).toBeUndefined();
   });

   it("leaves a field that reads nothing classified unclassified", () => {
      expect(chart.get("plain")?.classifications).toEqual([]);
      expect(chart.get("plain")?.inputs).toEqual([]);
      expect(chart.get("n")?.classifications).toEqual([]);
   });

   it("marks a hidden field and keeps it as a possible input", () => {
      expect(chart.get("ssn")?.hidden).toBe(true);
      // `social` was renamed, so it lives under its active name.
      expect(chart.get("ssn_renamed")?.hidden).toBe(false);
   });

   it("follows a join: joined fields by path, and references through the join", () => {
      expect(visits.get("person.masked")?.classifications).toEqual([
         "phi.direct_identifier",
      ]);
      expect(visits.get("who")).toMatchObject({
         inferred: ["phi.direct_identifier"],
         inputs: [
            {
               field: "person.masked",
               classifications: ["phi.direct_identifier"],
            },
         ],
      });
      expect(visits.get("where_from")?.inferred).toEqual([
         "phi.quasi_identifier",
      ]);
   });
});

const ROWS = `duckdb.sql("select * from (values (1,'east','999-11-2222',10),(2,'west','999-33-4444',20)) as t(id, region, ssn, amount)")`;

describe("classification warnings on package load", () => {
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
   #(classification) "pii.direct_identifier"
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
      expect(found[0].message).toContain("names nothing");
   });

   it("reads only the surface: a hidden base is reported where a published source reaches it, and a locked source not at all", async () => {
      write(
         "base.malloy",
         `source: people is ${ROWS} extend {
   #(classification) "pii.direct_identifier"
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
   #(classification) "none"
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

   it("counts an inferred class as classified", async () => {
      write(
         "orders.malloy",
         `##! experimental.access_modifiers
source: orders is ${ROWS} include { internal: id, region, ssn, amount } extend {
   #(classification) "pii.direct_identifier"
   dimension: social is ssn
   dimension: social_copy is social
   dimension: area is region
}
`,
      );
      const found = await warnings();
      expect(found).toHaveLength(1);
      expect(found[0].message).toContain(
         "publishes 1 field with no #(classification): area.",
      );
   });

   async function loadError(): Promise<string> {
      try {
         await warnings();
      } catch (error) {
         expect(error).toBeInstanceOf(BadRequestError);
         return (error as Error).message;
      }
      throw new Error(
         "expected the load to be refused, but the package loaded",
      );
   }

   it("refuses the load when a #(carries) surface exposes a class off its list, naming the lineage", async () => {
      write(
         "base.malloy",
         `##! experimental.access_modifiers
source: people is ${ROWS} include { internal: id, region, ssn, amount } extend {
   #(classification) "pii.direct_identifier"
   dimension: social is ssn
   #(classification) "financial"
   dimension: spend is amount
   #(classification) "none"
   dimension: area is region
}
`,
      );
      write(
         "index.malloy",
         `##! experimental.access_modifiers
import "base.malloy"

#(carries) ["financial", "none"]
source: research is people include { internal: social } extend {
   dimension: masked is concat('***', social)
}

export { research }
`,
      );
      const message = await loadError();
      expect(message).toContain(
         "source research (index.malloy) carries [financial, none] but exposes masked, classified pii.direct_identifier, inferred from social (pii.direct_identifier)",
      );
      expect(message).not.toContain("exposes spend");
      expect(message).not.toContain("exposes area");
   });

   it("refuses the load when a #(carries) surface exposes an unclassified field", async () => {
      write(
         "orders.malloy",
         `##! experimental.access_modifiers
#(carries) ["none"]
source: orders is ${ROWS} include { internal: id, region, ssn public: amount } extend {
   #(classification) "none"
   dimension: area is region
}
`,
      );
      const message = await loadError();
      expect(message).toContain(
         "carries [none] but exposes amount, which has no class",
      );
   });

   it("inherits #(carries) through extend, and refuses one that names nothing", async () => {
      write(
         "orders.malloy",
         `##! experimental.access_modifiers
#(carries) ["none"]
source: orders is ${ROWS} include { internal: id, region, ssn, amount } extend {
   #(classification) "none"
   dimension: area is region
}

source: fine is orders extend { measure: n is count() }

#(carries)
source: empty_list is orders extend {}
`,
      );
      const message = await loadError();
      // `fine` inherits the list; `n` reads nothing classified, so it leaks.
      expect(message).toContain(
         "source fine (orders.malloy) carries [none] but exposes n, which has no class",
      );
      expect(message).toContain(
         "source empty_list (orders.malloy) has a #(carries) annotation that names nothing",
      );
   });

   it("loads a surface whose every field is on its list", async () => {
      write(
         "orders.malloy",
         `##! experimental.access_modifiers
#(carries) ["none", "financial"]
source: orders is ${ROWS} include { internal: id, region, ssn, amount } extend {
   #(classification) "none"
   dimension: area is region
   #(classification) "financial"
   measure: total is amount.sum()
   measure: total_again is total * 2
}
`,
      );
      expect(await warnings()).toEqual([]);
   });
});
