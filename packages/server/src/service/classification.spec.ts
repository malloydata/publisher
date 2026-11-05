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
   spyOn,
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
   CLASSIFICATION_DOWNGRADE,
   CLASSIFICATION_MALFORMED,
   isClassificationLine,
   readCarries,
   readClassification,
   readSourceClassifications,
} from "./classification";
import * as classificationModule from "./classification";
import type { AnnotationsDef } from "./annotations";
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

   const PEOPLE = `##! experimental.access_modifiers
source: people is ${ROWS} include { internal: id, region, ssn, amount } extend {
   #(classification) "pii.direct_identifier"
   dimension: social is ssn
   #(classification) "none"
   dimension: area is region
}
`;

   it("refuses a view that groups by, or filters on, a field class off the #(carries) list, and names the way out", async () => {
      write("base.malloy", PEOPLE);
      write(
         "index.malloy",
         `##! experimental.access_modifiers
import "base.malloy"

#(carries) ["none"]
source: research is people include { internal: social } extend {
   view: by_social is { group_by: social }
   view: filtered is {
      group_by: area
      where: social = 'x'
   }
   view: fine is { group_by: area }
}

export { research }
`,
      );
      const message = await loadError();
      expect(message).toContain(
         "source research (index.malloy) carries [none] but its view by_social reads social, classified pii.direct_identifier",
      );
      expect(message).toContain("view filtered reads social");
      expect(message).toContain("move the filter into a declared measure");
      expect(message).not.toContain("view fine");
   });

   it("loads the same surface without the leaking views", async () => {
      write("base.malloy", PEOPLE);
      write(
         "index.malloy",
         `##! experimental.access_modifiers
import "base.malloy"

#(carries) ["none"]
source: research is people include { internal: social } extend {
   view: fine is { group_by: area }
}

export { research }
`,
      );
      expect(await warnings()).toEqual([]);
   });

   it("fails closed on a view that reads a field the source does not declare", async () => {
      write("base.malloy", PEOPLE);
      write(
         "index.malloy",
         `##! experimental.access_modifiers
import "base.malloy"

#(carries) ["none"]
source: research is people include { internal: social } extend {
   view: sneaky is {
      extend: { dimension: z is social }
      group_by: z
   }
}

export { research }
`,
      );
      expect(await loadError()).toContain(
         "its view sneaky reads z, which has no class or cannot be resolved against the source",
      );
   });

   it("refuses a plain copy that declares a lower class than its input, and warns on a computed one", async () => {
      write("base.malloy", PEOPLE);
      write(
         "index.malloy",
         `##! experimental.access_modifiers
import "base.malloy"

#(carries) ["none"]
source: research is people include { internal: social } extend {
   #(classification) "none"
   dimension: copied is social
   #(classification) "none"
   dimension: casted is social::string
}

export { research }
`,
      );
      const message = await loadError();
      expect(message).toContain(
         "exposes copied, declared none over a plain copy of social (pii.direct_identifier)",
      );
      expect(message).toContain(
         "exposes casted, declared none over a plain copy",
      );
   });

   it("warns classification-downgrade, once per field, when a computed field declares a lower class", async () => {
      write("base.malloy", PEOPLE);
      write(
         "index.malloy",
         `##! experimental.access_modifiers
import "base.malloy"

source: research is people include { internal: social } extend {
   #(classification) "none"
   dimension: padded is concat(social, '')
}

export { research }
`,
      );
      const found = (await warnings()).filter(
         (w) => w.code === CLASSIFICATION_DOWNGRADE,
      );
      expect(found).toHaveLength(1);
      expect(found[0]).toMatchObject({
         model: "index.malloy",
         subject: "research",
         severity: "warn",
      });
      expect(found[0].message).toContain("padded");
      expect(found[0].message).toContain("pii.direct_identifier");
   });

   it("refuses a query source whose output declares a lower class over a plain copy, and warns on a computed one", async () => {
      write("base.malloy", PEOPLE);
      write(
         "index.malloy",
         `##! experimental.access_modifiers
import "base.malloy"

#(carries) ["none"]
source: copy_q is people -> {
   group_by:
      #(classification) "none"
      x is social
}

export { copy_q }
`,
      );
      expect(await loadError()).toContain(
         "source copy_q (index.malloy) carries [none] but exposes x, declared none over a plain copy of social",
      );
      write(
         "index.malloy",
         `##! experimental.access_modifiers
import "base.malloy"

#(carries) ["none"]
source: computed_q is people -> {
   group_by:
      #(classification) "none"
      x is concat(social, '')
}

export { computed_q }
`,
      );
      const found = await warnings();
      expect(found.map((w) => [w.code, w.subject])).toEqual([
         [CLASSIFICATION_DOWNGRADE, "computed_q"],
      ]);
   });

   it("gives a query source's output the classes its stage reads, filters included", async () => {
      write("base.malloy", PEOPLE);
      write(
         "index.malloy",
         `##! experimental.access_modifiers
import "base.malloy"

#(carries) ["none"]
source: leaky_q is people -> {
   group_by: area, shown is concat(social, '!')
   where: social = 'x'
}

export { leaky_q }
`,
      );
      const message = await loadError();
      expect(message).toContain(
         "exposes shown, classified pii.direct_identifier",
      );
      expect(message).toContain(
         "exposes area, declared none over a plain copy of social (pii.direct_identifier)",
      );
   });

   it("treats everything under a private join as hidden", async () => {
      write("base.malloy", PEOPLE);
      write(
         "index.malloy",
         `##! experimental.access_modifiers
import "base.malloy"

#(carries) ["none"]
source: research is ${ROWS} include { internal: id, region, ssn, amount } extend {
   #(classification) "none"
   dimension: area is region
   private join_one: owner is people on owner.id = id
}

export { research }
`,
      );
      expect(await warnings()).toEqual([]);
   });

   it("refuses a #(carries) source whose PHI sits four joins deep, and loads its twin declared none", async () => {
      const entry = (leafClass: string) =>
         `##! experimental.access_modifiers
${joinChain(4, leafClass)}
export { s0 }
`.replace("source: s0", '#(carries) ["none"]\nsource: s0');
      write("m.malloy", entry("phi.direct_identifier"));
      expect(await loadError()).toContain(
         "source s0 (m.malloy) carries [none] but exposes n.n.n.n.ssn, classified phi.direct_identifier",
      );
      write("m.malloy", entry("none"));
      expect(await warnings()).toEqual([]);
   });

   it("refuses a #(carries) source past 16 joins or past the join-path budget, and loads one inside them", async () => {
      const carries = (model: string, name: string) =>
         `##! experimental.access_modifiers
${model}
export { ${name} }
`.replace(`source: ${name}`, `#(carries) ["none"]\nsource: ${name}`);
      write("m.malloy", carries(joinChain(16, "none"), "s0"));
      expect(await warnings()).toEqual([]);
      write("m.malloy", carries(joinChain(17, "none"), "s0"));
      expect(await loadError()).toContain(
         "source s0 (m.malloy) carries [none]",
      );
      write("m.malloy", carries(joinFanOut(10), "f0"));
      expect(await loadError()).toContain("join paths");
   });

   it("says nothing about a truncated source that declares no #(carries)", async () => {
      write(
         "m.malloy",
         `##! experimental.access_modifiers
${joinChain(17, "phi.direct_identifier")}
export { s0 }
`,
      );
      expect(
         (await warnings()).filter((w) => w.code === CLASSIFICATION_MALFORMED),
      ).toEqual([]);
   });

   it("walks the classification warnings once per models map, however often /status reads them", async () => {
      write(
         "m.malloy",
         `##! experimental.access_modifiers
${joinChain(1, "none")}
export { s0 }
`,
      );
      const config = new MalloyConfig({ connections: {} });
      config.wrapConnections(
         () => new FixedConnectionMap(new Map([["duckdb", duckdb]]), "duckdb"),
      );
      const pkg = await Package.create("env", "pkg", tempDir, config);
      pkg.getPackageMetadata();
      const walk = spyOn(classificationModule, "classificationWarnings");
      try {
         pkg.getPackageMetadata();
         pkg.getPackageMetadata();
         expect(walk).toHaveBeenCalledTimes(0);
         pkg.setPackageMetadata({
            ...pkg.getPackageMetadata(),
            explores: ["m.malloy"],
         });
         pkg.getPackageMetadata();
         pkg.getPackageMetadata();
         expect(walk).toHaveBeenCalledTimes(1);
      } finally {
         walk.mockRestore();
      }
   });
});

/** `s0 -> s1 -> ... -> s<joins>`, each hop one join; the leaf's `ssn` carries `leafClass`. */
function joinChain(joins: number, leafClass: string): string {
   const base = `duckdb.sql("select 1 as id") include { internal: id } extend {
   #(classification) "none"
   dimension: k is id`;
   let out = `source: s${joins} is ${base}
   #(classification) "${leafClass}"
   dimension: ssn is '999'
}
`;
   for (let i = joins - 1; i >= 0; i--) {
      out += `source: s${i} is ${base}
   join_one: n is s${i + 1} on n.k = k
}
`;
   }
   return out;
}

/** A tree `levels` joins deep where every source joins the next level twice. */
function joinFanOut(levels: number): string {
   const base = `duckdb.sql("select 1 as id") include { internal: id } extend {
   #(classification) "none"
   dimension: k is id`;
   let out = `source: f${levels} is ${base}
}
`;
   for (let i = levels - 1; i >= 0; i--) {
      out += `source: f${i} is ${base}
   join_one: a is f${i + 1} on a.k = k
   join_one: b is f${i + 1} on b.k = k
}
`;
   }
   return out;
}

describe("the join walk's bounds", () => {
   const fieldsOf = async (model: string, name: string) => {
      const { modelDefs } = await compileModelFiles({
         "m.malloy": `##! experimental.access_modifiers\n${model}`,
      });
      const md = modelDefs["m.malloy"] as ModelDef;
      return (md.contents[name] as { fields?: unknown[] }).fields as never;
   };

   it("counts joins from the entry source: 16 are read, a 17th is a truncation", async () => {
      const sixteen = readSourceClassifications(
         await fieldsOf(joinChain(16, "phi.direct_identifier"), "s0"),
      );
      expect(sixteen.truncated).toBe(false);
      expect(sixteen.get(`${"n.".repeat(16)}ssn`)?.classifications).toEqual([
         "phi.direct_identifier",
      ]);
      const seventeen = readSourceClassifications(
         await fieldsOf(joinChain(17, "phi.direct_identifier"), "s0"),
      );
      expect(seventeen.truncated).toBe(true);
   });

   it("is truncated by a fan-out of join paths, not by depth", async () => {
      const fields = await fieldsOf(joinFanOut(10), "f0");
      expect(readSourceClassifications(fields).truncated).toBe(true);
      const small = await fieldsOf(joinFanOut(6), "f0");
      expect(readSourceClassifications(small).truncated).toBe(false);
   });

   it("reads a compiled source once: the same fields give the same lineage", async () => {
      const fields = await fieldsOf(joinChain(2, "none"), "s0");
      expect(readSourceClassifications(fields)).toBe(
         readSourceClassifications(fields),
      );
   });

   it("refuses an annotation chain past the hop bound instead of reading none", () => {
      let node = {
         notes: [{ text: '#(carries) ["none"]' }],
      } as unknown as AnnotationsDef;
      for (let i = 0; i < 70; i++) node = { inherits: node };
      expect(readCarries(node)).toMatchObject({
         declared: true,
         malformed: expect.stringContaining("deeper than"),
      });
   });
});

describe("readSourceClassifications over a query source", () => {
   it("reads each output's inputs and its stage's filters, and fails closed on what it cannot resolve", async () => {
      const { modelDefs } = await compileModelFiles({
         "m.malloy": `##! experimental.access_modifiers
source: p is duckdb.sql("select 1 as id, '9' as ssn, 'a' as n") include { internal: ssn public: * } extend {
   #(classification) "phi"
   dimension: social is ssn
}
source: q is p -> {
   group_by: social, shown is concat(social, '!'), n
   where: social = 'x'
}
source: two_stage is p -> {
   group_by: social
} -> {
   select: social
}
`,
      });
      const md = modelDefs["m.malloy"] as ModelDef;
      const ir = (name: string) =>
         md.contents[name] as unknown as {
            fields: never;
            query: never;
         };
      const q = readSourceClassifications(ir("q").fields, ir("q").query);
      expect(q.get("shown")).toMatchObject({
         inferred: ["phi"],
         passthrough: false,
      });
      expect(q.get("social")?.passthrough).toBe(true);
      expect(q.get("n")?.inferred).toEqual(["phi"]);
      const two = readSourceClassifications(
         ir("two_stage").fields,
         ir("two_stage").query,
      );
      expect(two.get("social")?.unresolved).toEqual(["social"]);
   });
});
