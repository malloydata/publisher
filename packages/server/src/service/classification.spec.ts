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
import { classificationLeaks } from "./classification_report";
import * as classificationReportModule from "./classification_report";
import type { AnnotationsDef } from "./annotations";
import { compileModelFiles } from "../test_helpers/get_context_join_fixture";
import type { ModelDef } from "@malloydata/malloy";
import { Package } from "./package";
import { writeTempPackage } from "../test_helpers/temp_package";

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
      tempDir = writeTempPackage(
         {},
         {
            prefix: "publisher-classification-",
            manifest: { name: "pkg", description: "classification" },
         },
      ).dir;
      duckdb = new DuckDBConnection("duckdb", ":memory:");
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

   async function allWarnings() {
      const config = new MalloyConfig({ connections: {} });
      config.wrapConnections(
         () => new FixedConnectionMap(new Map([["duckdb", duckdb]]), "duckdb"),
      );
      const pkg = await Package.create("env", "pkg", tempDir, config);
      return pkg.getPackageMetadata().warnings ?? [];
   }

   async function warnings() {
      return (await allWarnings()).filter((w) =>
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

   it("reports a source's malformed notes as one warning, however many fields have one", async () => {
      write(
         "orders.malloy",
         `source: orders is ${ROWS} extend {
   #(classification)
   dimension: social is ssn
   #(classification)
   dimension: area is region
}
`,
      );
      const found = await warnings();
      expect(found).toHaveLength(1);
      expect(found[0]).toMatchObject({
         code: CLASSIFICATION_MALFORMED,
         subject: "orders",
      });
      expect(found[0].message).toContain("2 malformed");
      expect(found[0].message).toContain("Field social of source orders");
      expect(found[0].message).toContain("Field area of source orders");
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

   const recordSurface = (body: string, carries = '#(carries) ["none"]\n') => {
      write("base.malloy", PEOPLE);
      write(
         "index.malloy",
         `##! experimental.access_modifiers
import "base.malloy"

${carries}source: research is people include { internal: social } extend {
${body}
}

export { research }
`,
      );
   };

   it("refuses a record or array dimension over a classified field, exposed through a view or directly", async () => {
      for (const [body, message] of [
         [
            "   internal dimension: addr is { v is social }\n   view: v is { select: addr }",
            "carries [none] but its view v reads addr, classified pii.direct_identifier",
         ],
         [
            "   internal dimension: addr is [social]\n   view: v is { select: addr }",
            "carries [none] but its view v reads addr, classified pii.direct_identifier",
         ],
         [
            "   internal dimension: addr is { v is social }\n   view: v is { group_by: addr }",
            "carries [none] but its view v reads addr, classified pii.direct_identifier",
         ],
         [
            "   dimension: r is { v is social }",
            "carries [none] but exposes r, classified pii.direct_identifier, inferred from social (pii.direct_identifier)",
         ],
         [
            "   dimension: a is [social]",
            "carries [none] but exposes a, classified pii.direct_identifier, inferred from social (pii.direct_identifier)",
         ],
         [
            '   #(classification) "none"\n   internal dimension: copy is social\n   dimension: r is { v is copy }',
            "carries [none] but exposes r, classified none pii.direct_identifier, inferred from copy (none pii.direct_identifier)",
         ],
      ]) {
         recordSurface(body);
         expect(await loadError()).toContain(message);
      }
   });

   it("refuses a record a query source returns whose child it cannot follow", async () => {
      write("base.malloy", PEOPLE);
      write(
         "index.malloy",
         `##! experimental.access_modifiers
import "base.malloy"

#(carries) ["none"]
source: q is people -> {
   group_by: area
   nest: n is {
      extend: { dimension: z is social }
      group_by: r is { v is z }
   }
}

export { q }
`,
      );
      expect(await loadError()).toContain(
         "source q (index.malloy) carries [none] but exposes n.r, which has no class (it reads z, which cannot be resolved against the source)",
      );
   });

   it("holds a record on a carried source an open one joins to the joined source's ceiling", async () => {
      write(
         "base.malloy",
         `##! experimental.access_modifiers
#(carries) ["none"]
source: s is ${ROWS} include { internal: id, region, ssn, amount } extend {
   #(classification) "none"
   dimension: sid is id
   #(classification) "pii.direct_identifier"
   internal dimension: social is ssn
   dimension: r is { v is social }
}
source: hub is ${ROWS} include { internal: id, region, ssn, amount } extend {
   join_one: rr is s on id = rr.sid
   #(classification) "none"
   measure: hc is count()
}
`,
      );
      write(
         "index.malloy",
         `import { hub, s } from "base.malloy"
export { hub }
`,
      );
      expect(await loadError()).toContain(
         "source hub (index.malloy) exposes rr.r, classified pii.direct_identifier, through rr, which carries [none]",
      );
   });

   it("twin: loads a record or array built from fields on the list, directly or through a view, and a record under no #(carries)", async () => {
      recordSurface(
         "   dimension: r is { v is area }\n   dimension: a is [area]\n   dimension: ar is [{ v is area }]\n   view: v is { select: r, a }",
      );
      expect(await warnings()).toEqual([]);
      recordSurface("   dimension: r is { v is social }", "");
      expect(await warnings()).toEqual([]);
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

   it("follows a query source's output through every stage, so a copy two stages down is still a copy", async () => {
      write("base.malloy", PEOPLE);
      write(
         "index.malloy",
         `##! experimental.access_modifiers
import "base.malloy"

#(carries) ["none"]
source: copy_q is people -> { group_by: y is social } -> {
   group_by:
      #(classification) "none"
      x is y
}

export { copy_q }
`,
      );
      expect(await loadError()).toContain(
         "source copy_q (index.malloy) carries [none] but exposes x, declared none over a plain copy of y (pii.direct_identifier)",
      );
   });

   it("twin: a computed output two stages down declared lower still loads, with a downgrade warning", async () => {
      write("base.malloy", PEOPLE);
      write(
         "index.malloy",
         `##! experimental.access_modifiers
import "base.malloy"

#(carries) ["none"]
source: computed_q is people -> { group_by: y is social } -> {
   group_by:
      #(classification) "none"
      x is concat(y, '')
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
         "its query filters on social (pii.direct_identifier)",
      );
      expect(message).not.toContain("plain copy");
   });

   it("charges a view that reads a hidden relabel of a classified field the class the relabel hides", async () => {
      write("base.malloy", PEOPLE);
      write(
         "index.malloy",
         `##! experimental.access_modifiers
import "base.malloy"

#(carries) ["none"]
source: research is people include { internal: social } extend {
   #(classification) "none"
   internal dimension: s2 is social
   view: v is { group_by: s2 }
}

export { research }
`,
      );
      const message = await loadError();
      expect(message).toContain(
         "its view v reads s2, classified none pii.direct_identifier",
      );
      expect(message).toContain(
         "exposes s2, declared none over a plain copy of social (pii.direct_identifier)",
      );
   });

   it("loads a hidden relabel no view reads, and warns on a hidden computed field a view reads", async () => {
      write("base.malloy", PEOPLE);
      write(
         "index.malloy",
         `##! experimental.access_modifiers
import "base.malloy"

#(carries) ["none"]
source: research is people include { internal: social } extend {
   #(classification) "none"
   internal dimension: s2 is social
   #(classification) "none"
   internal dimension: s3 is concat(social, '')
   view: v is { group_by: s3 }
}

export { research }
`,
      );
      const found = await warnings();
      expect(found.map((w) => [w.code, w.message?.includes("s3")])).toEqual([
         [CLASSIFICATION_DOWNGRADE, true],
      ]);
   });

   it("passes a relabel's input classes on to a field that reads it, so a copy of the relabel cannot launder them", async () => {
      write("base.malloy", PEOPLE);
      const source = (body: string) =>
         `##! experimental.access_modifiers
import "base.malloy"

#(carries) ["none"]
source: research is people include { internal: social } extend {
${body}
}

export { research }
`;
      write(
         "index.malloy",
         source(`   #(classification) "none"
   internal dimension: s2 is social
   dimension: s3 is s2`),
      );
      expect(await loadError()).toContain(
         "exposes s3, classified none pii.direct_identifier",
      );
      write(
         "index.malloy",
         source(`   #(classification) "none"
   internal dimension: s2 is social
   internal dimension: s3 is s2
   view: v is { group_by: s3 }`),
      );
      expect(await loadError()).toContain(
         "its view v reads s3, classified none pii.direct_identifier",
      );
      write(
         "index.malloy",
         source(`   #(classification) "none"
   internal dimension: s2 is social
   internal dimension: s3 is s2
   internal dimension: s4 is s3
   dimension: s5 is s4`),
      );
      expect(await loadError()).toContain(
         "exposes s5, classified none pii.direct_identifier",
      );
   });

   it("loads a copy of a relabel declared over a computed expression, warning on the relabel only", async () => {
      write("base.malloy", PEOPLE);
      write(
         "index.malloy",
         `##! experimental.access_modifiers
import "base.malloy"

#(carries) ["none"]
source: research is people include { internal: social } extend {
   #(classification) "none"
   dimension: s2 is concat(social, '')
   dimension: s3 is s2
}

export { research }
`,
      );
      const found = await warnings();
      expect(found.map((w) => [w.code, w.message?.includes("s2")])).toEqual([
         [CLASSIFICATION_DOWNGRADE, true],
      ]);
   });

   it("refuses a declared plain copy of a read it cannot resolve, and warns on a declared computed field over one", async () => {
      write("base.malloy", PEOPLE);
      write(
         "index.malloy",
         `##! experimental.access_modifiers
import "base.malloy"

#(carries) ["none"]
source: copy_q is people -> {
   nest: n is {
      extend: { dimension: z is social }
      group_by:
         #(classification) "none"
         x is z
   }
}

export { copy_q }
`,
      );
      expect(await loadError()).toContain(
         "exposes n.x, declared none over a plain copy of z, which cannot be resolved",
      );
      write(
         "index.malloy",
         `##! experimental.access_modifiers
import "base.malloy"

#(carries) ["none"]
source: computed_q is people -> {
   nest: n is {
      extend: { dimension: z is social }
      group_by:
         #(classification) "none"
         x is concat(z, '')
   }
}

export { computed_q }
`,
      );
      const found = await warnings();
      expect(found.map((w) => [w.code, w.subject])).toEqual([
         [CLASSIFICATION_DOWNGRADE, "computed_q"],
      ]);
      expect(found[0].message).toContain("cannot be resolved: z");
   });

   it("refuses an output under a nest with stages of its own, naming the nest's stages rather than the declaration", async () => {
      write("base.malloy", PEOPLE);
      const nested = (view: string) =>
         write(
            "index.malloy",
            `##! experimental.access_modifiers
import "base.malloy"

#(carries) ["none"]
source: q is people -> {
   group_by: area
   nest: n is ${view}
}

export { q }
`,
         );
      for (const declare of ['#(classification) "none"', ""]) {
         nested(`{ group_by:
      ${declare}
      a2 is area } -> { group_by:
      ${declare}
      a3 is a2 }`);
         const message = await loadError();
         expect(message).toContain(
            "source q (index.malloy) carries [none] but exposes n.a3, an output Publisher cannot follow to what it reads",
         );
         expect(message).toContain("Flatten the nest into one stage");
         expect(message).not.toContain("remove the declaration");
         expect(message).not.toContain("reads no classified field");
      }
      nested(`{ group_by:
      #(classification) "none"
      a3 is area }`);
      expect(await warnings()).toEqual([]);
   });

   it("says nothing about a declared computed field whose reads all resolve", async () => {
      write("base.malloy", PEOPLE);
      write(
         "index.malloy",
         `##! experimental.access_modifiers
import "base.malloy"

source: ok_q is people -> {
   group_by:
      #(classification) "pii.direct_identifier"
      x is concat(social, '')
}

export { ok_q }
`,
      );
      expect(await warnings()).toEqual([]);
   });

   it("charges a view that nests another view the leaf fields the nested view reads", async () => {
      write("base.malloy", PEOPLE);
      write(
         "index.malloy",
         `##! experimental.access_modifiers
import "base.malloy"

#(carries) ["none"]
source: research is people include { internal: social } extend {
   view: leaf is { group_by: social }
   view: outer is { nest: leaf }
   view: inline is { nest: deep is { group_by: social } }
}

export { research }
`,
      );
      const message = await loadError();
      expect(message).toContain("its view outer reads social");
      expect(message).toContain("its view inline reads social");
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
   private join_one: owner is people on owner.area = area
}

export { research }
`,
      );
      expect(await warnings()).toEqual([]);
   });

   it("refuses at load a named query or notebook cell that reads past its source's ceiling, and loads one that stays within it", async () => {
      write("base.malloy", PEOPLE);
      const model = (queries: string) =>
         write(
            "index.malloy",
            `##! experimental.access_modifiers
import "base.malloy"

#(carries) ["none"]
source: research is people include { internal: social public: area }
${queries}
export { research }
`,
         );
      model(`query: fine is research -> { group_by: area }`);
      expect(await warnings()).toEqual([]);
      model(
         `query: leaky is research extend { dimension: x is social } -> { group_by: x }`,
      );
      const named = await loadError();
      expect(named).toContain("index.malloy: query leaky");
      expect(named).toContain("x, which is classified pii.direct_identifier");
      model(`query: fine is research -> { group_by: area }`);
      write(
         "tour.malloynb",
         `>>>malloy
import "index.malloy"
>>>malloy
run: research extend { dimension: x is social } -> { group_by: x }
`,
      );
      expect(await loadError()).toContain("tour.malloynb: notebook cell");
   });

   it("carries a query source's filter classes to every field and view that reads its hidden output", async () => {
      const hidden = (tail: string, carries = '["none"]') =>
         write(
            "index.malloy",
            `##! experimental.access_modifiers
import "base.malloy"

#(carries) ${carries}
source: s is people -> {
   group_by:
      #(classification) "none"
      a is area
   where: social = 'x'
} include { internal: a } extend {
${tail}
}

export { s }
`,
         );
      write("base.malloy", PEOPLE);
      hidden(`   dimension: y is upper(a)`);
      expect(await loadError()).toContain(
         "exposes y, classified none pii.direct_identifier",
      );
      hidden(`   view: v is { group_by: a }`);
      expect(await loadError()).toContain(
         "its view v reads social, classified pii.direct_identifier",
      );
      hidden(
         `   dimension: y is upper(a)\n   view: v is { group_by: a }`,
         '["none", "pii.direct_identifier"]',
      );
      expect(await warnings()).toEqual([]);
   });

   it("holds a query source to the ceiling of the source its query runs on", async () => {
      write("base.malloy", PEOPLE);
      const model = (wide: string) =>
         write(
            "index.malloy",
            `##! experimental.access_modifiers
import "base.malloy"

#(carries) ["none"]
source: narrow is people

#(carries) ${wide}
source: wide is narrow -> { group_by: social }

export { wide }
`,
         );
      model('["none", "pii.direct_identifier"]');
      const message = await loadError();
      expect(message).toContain("source wide (index.malloy) carries [none]");
      expect(message).toContain("exposes social");
      write(
         "index.malloy",
         `##! experimental.access_modifiers
import "base.malloy"

#(carries) ["none", "pii.direct_identifier"]
source: narrow is people

#(carries) ["none", "pii.direct_identifier"]
source: wide is narrow -> { group_by: social }

export { wide }
`,
      );
      expect(await warnings()).toEqual([]);
   });

   it("holds a source that carries nothing to the list of a carried source it joins", async () => {
      write("base.malloy", PEOPLE);
      const model = (carries: string) =>
         write(
            "index.malloy",
            `##! experimental.access_modifiers
import "base.malloy"

#(carries) ${carries}
source: tight is people

source: hub is ${ROWS} include { internal: id, region, ssn, amount } extend {
   #(classification) "none"
   dimension: area is region
   join_one: t is tight on t.area = area
}

export { hub }
`,
         );
      model('["none"]');
      const message = await loadError();
      expect(message).toContain("source hub (index.malloy) exposes t.social");
      expect(message).toContain("through t, which carries [none]");
      model('["none", "pii.direct_identifier", "financial"]');
      expect(await warnings()).toEqual([]);
   });

   it("refuses a plain copy declared lower in a carried source that only an open source joins", async () => {
      const joined = (yr: string) => {
         write(
            "base.malloy",
            `##! experimental.access_modifiers
#(carries) ["none"]
source: s is ${ROWS} include { internal: id, region, ssn, amount } extend {
   #(classification) "none"
   dimension: sid is id
   #(classification) "pii.direct_identifier"
   internal dimension: social is ssn
   #(classification) "none"
   dimension: yr is ${yr}
}
source: hub is ${ROWS} include { internal: id, region, ssn, amount } extend {
   join_one: r is s on id = r.sid
   #(classification) "none"
   measure: hc is count()
}
`,
         );
         write(
            "index.malloy",
            `import { hub, s } from "base.malloy"
export { hub }
`,
         );
      };
      joined("social");
      expect(await loadError()).toContain(
         "source hub (index.malloy) joins r, which carries [none] but exposes r.yr, declared none over a plain copy of r.social (pii.direct_identifier)",
      );
      joined("concat(social, '')");
      expect((await warnings()).map((w) => [w.code, w.subject])).toEqual([
         [CLASSIFICATION_DOWNGRADE, "hub"],
      ]);
   });

   const leaksOf = async (source: string, carries = '["none"]') => {
      write("base.malloy", PEOPLE);
      write(
         "index.malloy",
         `##! experimental.access_modifiers
import "base.malloy"

#(carries) ${carries}
${source}

export { s }
`,
      );
   };

   it("follows an extend field of a query source: a relabel of a hidden output is refused, a copy of a carried one loads", async () => {
      await leaksOf(`source: s is people -> { group_by: social, area } include { internal: social } extend {
   #(classification) "none"
   dimension: x is social
}`);
      expect(await loadError()).toContain(
         "exposes x, declared none over a plain copy of social",
      );
      await leaksOf(`source: s is people -> { group_by: social, area } include { internal: social } extend {
   dimension: z is upper(social)
}`);
      expect(await loadError()).toContain(
         "exposes z, classified pii.direct_identifier",
      );
      await leaksOf(`source: s is people -> { group_by: social, area } include { internal: social } extend {
   dimension: y is upper(area)
   #(classification) "none"
   dimension: x is area
}`);
      expect(await warnings()).toEqual([]);
   });

   it("follows the outputs of a query source that is joined, and loads the join of a carried one", async () => {
      write(
         "base.malloy",
         `${PEOPLE}
source: leaky_q is people -> {
   group_by:
      #(classification) "none"
      x is social
}
source: tidy_q is people -> { group_by: area }
`,
      );
      const joined = (q: string, key: string) =>
         write(
            "index.malloy",
            `##! experimental.access_modifiers
import "base.malloy"

#(carries) ["none"]
source: s is ${ROWS} include { internal: id, region, ssn, amount } extend {
   #(classification) "none"
   dimension: area is region
   join_one: j is ${q} on j.${key} = area
}

export { s }
`,
         );
      joined("leaky_q", "x");
      expect(await loadError()).toContain(
         "exposes j.x, declared none over a plain copy of social",
      );
      joined("tidy_q", "area");
      expect(await warnings()).toEqual([]);
   });

   it("follows nest outputs and nest filters of a query source", async () => {
      await leaksOf(`source: s is people -> {
   nest: n is {
      group_by:
         #(classification) "none"
         r is social
   }
   group_by: area
}`);
      expect(await loadError()).toContain(
         "exposes n.r, declared none over a plain copy of social",
      );
      await leaksOf(`source: s is people -> {
   nest: n is { group_by: area; where: social = 'x' }
}`);
      expect(await loadError()).toContain("filters on social");
      await leaksOf(`source: s is people -> {
   nest: n is { group_by: r is area; where: area = 'east' }
   group_by: area
}`);
      expect(await warnings()).toEqual([]);
   });

   it("charges a query source's filter its class without calling the output a copy of it", async () => {
      const query = `source: s is people -> { group_by: area; where: social = 'x' }`;
      await leaksOf(query);
      const message = await loadError();
      expect(message).toContain("filters on social");
      expect(message).not.toContain("plain copy");
      await leaksOf(query, '["none", "pii.direct_identifier"]');
      expect(await warnings()).toEqual([]);
   });

   it("keeps an earlier stage's filter a filter of the stage after it, never a value it copies", async () => {
      const query = `source: s is people -> { where: social = 'x'; group_by: area } -> { group_by: area }`;
      await leaksOf(query);
      const message = await loadError();
      expect(message).toContain(
         "source s (index.malloy) carries [none] but its query filters on social (pii.direct_identifier)",
      );
      expect(message).not.toContain("plain copy");
      await leaksOf(query, '["none", "pii.direct_identifier"]');
      expect(await warnings()).toEqual([]);
   });

   it("charges an output that reads nothing, like a count, the filters of the stages before it", async () => {
      const counted = (
         where: string,
      ) => `source: s is people -> { where: ${where}; group_by: area } -> {
   aggregate:
      #(classification) "none"
      n is count()
}`;
      await leaksOf(counted("social = 'x'"));
      expect(await loadError()).toContain(
         "source s (index.malloy) carries [none] but its query filters on social (pii.direct_identifier)",
      );
      await leaksOf(counted("area = 'east'"));
      expect(await warnings()).toEqual([]);
   });

   it("warns on a hidden computed downgrade a published field reads, and not on one nothing reads", async () => {
      const research = (extra: string) =>
         leaksOf(`source: s is people include { internal: social } extend {
   #(classification) "none"
   internal dimension: s3 is concat(social, '')
${extra}
}`);
      await research(`   #(classification) "none"
   dimension: shown is upper(s3)`);
      const found = await warnings();
      expect(found.map((w) => [w.code, w.message?.includes("s3")])).toEqual([
         [CLASSIFICATION_DOWNGRADE, true],
      ]);
      await research("");
      expect(
         (await warnings()).filter((w) => w.code === CLASSIFICATION_DOWNGRADE),
      ).toEqual([]);
   });

   it("runs the leak gate and the join-bypass warning over a source the surface hides under queryableSources all", async () => {
      write("base.malloy", PEOPLE);
      write(
         "index.malloy",
         `source: shown is ${ROWS} extend { dimension: n is 1 }\n`,
      );
      const hidden = (source: string) =>
         write(
            "hidden.malloy",
            `##! experimental.access_modifiers
import "base.malloy"

${source}
`,
         );
      const leak = `#(carries) ["none"]
source: hid is people extend { view: v is { group_by: social } }`;
      const bypass = `#(authorize) false
source: vault is ${ROWS}

source: bridge is ${ROWS} extend { join_one: locked is vault on locked.id = id }`;
      const manifest = (extra: object) =>
         write("publisher.json", JSON.stringify({ name: "pkg", ...extra }));
      manifest({ explores: ["index.malloy"] });
      hidden(leak);
      expect(await warnings()).toEqual([]);
      hidden(bypass);
      expect(
         (await allWarnings()).filter((w) => w.code === "join-bypass"),
      ).toEqual([]);
      manifest({ queryableSources: "all" });
      hidden(leak);
      expect(await loadError()).toContain(
         "source hid (hidden.malloy) carries [none]",
      );
      hidden(bypass);
      const bypasses = (await allWarnings()).filter(
         (w) => w.code === "join-bypass",
      );
      expect(bypasses).toHaveLength(1);
      expect(bypasses[0].message).toContain("source bridge (hidden.malloy)");
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
      const walk = spyOn(classificationReportModule, "classificationWarnings");
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
source: nested_two is p -> {
   nest: m is { group_by: social } -> { select: social }
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
      expect(two.get("social")).toMatchObject({
         classifications: ["phi"],
         unresolved: [],
         passthrough: true,
      });
      const nested = readSourceClassifications(
         ir("nested_two").fields,
         ir("nested_two").query,
      );
      expect(nested.get("m.social")).toMatchObject({
         unresolved: ["m.social"],
         passthrough: true,
      });
   });

   it("passes an unresolved read up through an undeclared intermediate, and reads an extend field against the outputs", async () => {
      const { modelDefs } = await compileModelFiles({
         "m.malloy": `##! experimental.access_modifiers
source: p is duckdb.sql("select 1 as id, '9' as ssn, 'a' as n") include { internal: ssn public: * } extend {
   #(classification) "phi"
   dimension: social is ssn
}
source: two_stage is p -> { group_by: social, n } -> { select: social, n } extend {
   dimension: y is n
   dimension: z is upper(y)
}
source: one_stage is p -> { group_by: social, n } extend {
   dimension: y is social
   dimension: z is upper(y)
}
`,
      });
      const md = modelDefs["m.malloy"] as ModelDef;
      const lineage = (name: string) => {
         const ir = md.contents[name] as unknown as {
            fields: never;
            query: never;
         };
         return readSourceClassifications(ir.fields, ir.query);
      };
      expect(lineage("two_stage").get("y")?.unresolved).toEqual([]);
      // A last stage that does not say what its `n` reads leaves `n` unresolved.
      const ir = md.contents["two_stage"] as unknown as {
         fields: never;
         query: {
            pipeline: Array<{ queryFields: Array<{ path?: string[] }> }>;
         };
      };
      const last = ir.query.pipeline[1];
      const two = readSourceClassifications(ir.fields, {
         ...ir.query,
         pipeline: [
            ir.query.pipeline[0],
            {
               ...last,
               queryFields: last.queryFields.filter(
                  (qf) => qf.path?.at(-1) !== "n",
               ),
            },
         ],
      } as never);
      expect(two.get("y")?.unresolved).toEqual(["n"]);
      expect(two.get("z")?.unresolved).toEqual(["y"]);
      const one = lineage("one_stage");
      expect(one.get("y")).toMatchObject({
         inferred: ["phi"],
         unresolved: [],
      });
      expect(one.get("z")?.inferred).toEqual(["phi"]);
   });

   it("keys its memo on the query as well as the fields", async () => {
      const { modelDefs } = await compileModelFiles({
         "m.malloy": `##! experimental.access_modifiers
source: p is duckdb.sql("select 1 as id, '9' as ssn") include { internal: ssn public: * } extend {
   #(classification) "phi"
   dimension: social is ssn
}
source: q is p -> {
   group_by:
      #(classification) "none"
      x is social
}
`,
      });
      const q = (modelDefs["m.malloy"] as ModelDef).contents[
         "q"
      ] as unknown as {
         fields: never;
         query: never;
      };
      readSourceClassifications(q.fields);
      const withQuery = readSourceClassifications(q.fields, q.query);
      expect(withQuery.get("x")).toMatchObject({
         passthrough: true,
         dropped: ["phi"],
      });
      expect(readSourceClassifications(q.fields, q.query)).toBe(withQuery);
      expect(readSourceClassifications(q.fields).get("x")?.passthrough).toBe(
         false,
      );
   });
});

describe("classificationLeaks over a view with no reference summary", () => {
   const sourceWith = (views: unknown[]) =>
      ({
         getSourceInfos: () => [{ name: "s" }],
         getSources: () => [{ name: "s" }],
         getModelDef: () => ({
            contents: {
               s: {
                  name: "s",
                  annotations: { notes: [{ text: '#(carries) ["none"]' }] },
                  fields: [
                     {
                        name: "a",
                        type: "string",
                        annotations: {
                           notes: [{ text: '#(classification) "none"' }],
                        },
                     },
                     ...views,
                  ],
               },
            },
         }),
      }) as never;

   it("refuses a view neither its turtle nor its first stage summarises, and loads one that does", () => {
      const leaks = (view: unknown) =>
         classificationLeaks(
            new Map([["m.malloy", sourceWith([view])]]),
            () => true,
         );
      expect(leaks({ name: "bare", type: "turtle", pipeline: [{}] })).toEqual([
         expect.stringContaining("its view bare has no reference summary"),
      ]);
      expect(
         leaks({
            name: "fine",
            type: "turtle",
            pipeline: [{ refSummary: { fieldUsage: [{ path: ["a"] }] } }],
         }),
      ).toEqual([]);
   });
});
