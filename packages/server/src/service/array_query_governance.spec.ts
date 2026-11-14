// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Malloy's experimental query on an array (`items -> { ... }`, enabled per
 * model by `##! experimental.array_queries`) against every access check.
 *
 * What an array query reads is the array column of the row, which Malloy
 * reports as an ordinary read, and the fields of its elements, which it
 * reports re-rooted under the array (`inPlaceDepth`); the gates, the row
 * filters, the `#(carries)` ceiling and the given checks see both. Two things
 * did not hold, and are pinned here:
 *
 *  - A join written inside the array query's pipeline (`items -> { extend: {
 *    join_one: ... } }`) reads another source, which neither Malloy's field
 *    usage nor Publisher's join walks follow. An author's such join read a
 *    `#(carries)` source's field past its ceiling, at load and per query, and
 *    one into a gated source raised no `join-bypass` warning. A model that
 *    governs access now refuses it, at load and per query.
 *  - A field path inside the pipeline is relative to the elements; the IR
 *    walkers read it as a field of the row, so the access record listed a
 *    field the query never read and `#(carries)` refused a field it could
 *    resolve.
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
   spyOn,
} from "bun:test";
import * as fs from "fs";
import * as path from "path";
import { BadRequestError } from "../errors";
import { logger } from "../logger";
import {
   PackageLoadPool,
   __setPackageLoadPoolForTests,
} from "../package_load/package_load_pool";
import {
   expectAccessDenied,
   loadAuthorizeModel,
   type LoadedAuthorizeModel,
} from "../test_helpers/authorize_fixture";
import { seededDuckdb, writeTempPackage } from "../test_helpers/temp_package";
import { withAccessRecord, type AccessRecord } from "./access_record";
import type { Model } from "./model";
import { QueryPrivileges } from "./model_query_request";
import { Package } from "./package";

/** Two people, each with an array of purchases; a purchase has tags. */
const SEED = `
CREATE OR REPLACE TABLE people (id INTEGER, org_id VARCHAR, ssn VARCHAR,
  items STRUCT(sku VARCHAR, price INTEGER, ssn VARCHAR, tags STRUCT(t VARCHAR, w INTEGER)[])[]);
INSERT INTO people VALUES
 (1, 'org1', '111', [{'sku': 'org1', 'price': 10, 'ssn': 's1', 'tags': [{'t': 'x', 'w': 1}, {'t': 'y', 'w': 2}]},
                     {'sku': 'b', 'price': 20, 'ssn': 's2', 'tags': []}]),
 (2, 'org2', '222', [{'sku': 'org2', 'price': 100, 'ssn': 's3', 'tags': [{'t': 'z', 'w': 5}]}]);
`;

const FLAGS = "##! experimental { array_queries givens access_modifiers }";

let info: ReturnType<typeof spyOn>;
beforeEach(() => {
   info = spyOn(logger, "info").mockImplementation(() => logger);
});
afterEach(() => {
   info.mockRestore();
});

async function withModel<T>(
   text: string,
   body: (loaded: LoadedAuthorizeModel) => Promise<T>,
): Promise<T> {
   const duckdb = await seededDuckdb(SEED);
   const loaded = await loadAuthorizeModel(`${FLAGS}\n${text}`, { duckdb });
   try {
      expect(loaded.error).toBeUndefined();
      return await body(loaded);
   } finally {
      await loaded.cleanup();
      await duckdb.close();
   }
}

async function rows(
   model: Model,
   query: string,
   givens: Record<string, unknown> = {},
): Promise<Array<Record<string, unknown>>> {
   const result = await withAccessRecord(
      {
         environment: "env",
         package: "pkg",
         modelPath: "m.malloy",
         adHoc: true,
         caller: "c",
      },
      undefined,
      (audit) =>
         model.runQuery(
            audit,
            {
               query,
               filterParams: {},
               givens: givens as Record<string, GivenValue>,
               responseShape: "compact",
            },
            QueryPrivileges.of({ bypassFilters: true }),
         ),
   );
   return result.compactResult as unknown as Array<Record<string, unknown>>;
}

/** The one access record the last query wrote. */
function lastRecord(): AccessRecord {
   const records = (info.mock.calls as unknown[][])
      .filter(([message]) => message === "Access record")
      .map(([, record]) => record as AccessRecord);
   expect(records.length).toBeGreaterThan(0);
   return records[records.length - 1];
}

async function refusal(promise: Promise<unknown>): Promise<string> {
   const err = await promise.then(
      () => undefined,
      (e: unknown) => e,
   );
   expect(err).toBeInstanceOf(BadRequestError);
   return (err as Error).message;
}

const ARRAY_JOIN_REFUSAL = "joins a source inside a query on an array";

describe("array queries: what the access checks already see", () => {
   const GATED = `
#(secure)
given: ROLES :: string[]
#(secure)
given: ORGS :: string[]
#(secure)
given: ORGS_ONE :: string

#(authorize) 'clinician' in $ROLES
source: locked is duckdb.table('people')

#(access_filter) org_id in $ORGS
source: rowf is duckdb.table('people')

#(access_filter) top = $ORGS_ONE
source: by_top is duckdb.table('people') extend {
  dimension: top is items -> { aggregate: m is sku.max() }
}

source: hid is duckdb.table('people') include { public: *; private: items }
source: exc is duckdb.table('people') extend { except: items }
`;
   const SUM = "s is items -> { aggregate: t is price.sum() }";

   it("a lock decides a query that reads only through an array query", async () => {
      await withModel(GATED, async ({ model }) => {
         await expectAccessDenied(
            rows(model, `run: locked -> { select: id, ${SUM} }`, {}),
         );
         expect(
            await rows(model, `run: locked -> { select: id, ${SUM} }`, {
               ROLES: ["clinician"],
            }),
         ).toEqual([
            { id: 1, s: 30 },
            { id: 2, s: 100 },
         ]);
      });
   });

   it("a row filter scopes each caller's array query, on the source and through a caller join", async () => {
      await withModel(GATED, async ({ model }) => {
         for (const [org, id, s] of [
            ["org1", 1, 30],
            ["org2", 2, 100],
         ] as const) {
            expect(
               await rows(model, `run: rowf -> { select: id, ${SUM} }`, {
                  ORGS: [org],
               }),
            ).toEqual([{ id, s }]);
         }
         expect(
            await rows(
               model,
               `run: hid extend { join_one: r is rowf on id = r.id } -> { select: id, s is r.items -> { aggregate: t is price.sum() } }`,
               { ORGS: ["org1"] },
            ),
         ).toEqual([
            { id: 1, s: 30 },
            { id: 2, s: 0 },
         ]);
      });
   });

   it("a row filter on a dimension that is an array query admits each caller its own rows", async () => {
      await withModel(GATED, async ({ model }) => {
         expect(
            await rows(model, "run: by_top -> { select: id }", {
               ORGS_ONE: "org2",
            }),
         ).toEqual([{ id: 2 }]);
         expect(
            await rows(model, "run: by_top -> { select: id }", {
               ORGS_ONE: "org1",
            }),
         ).toEqual([{ id: 1 }]);
      });
   });

   it("a hidden or excepted array is not readable through an array query", async () => {
      await withModel(GATED, async ({ model }) => {
         for (const query of [
            `run: hid -> { select: id, ${SUM} }`,
            `run: hid extend { dimension: x is items -> { aggregate: t is price.sum() } } -> { select: x }`,
            `run: exc -> { select: id, ${SUM} }`,
         ]) {
            await expect(rows(model, query)).rejects.toThrow(
               /is private|is not defined/,
            );
         }
      });
   });

   it("a row filter that reads a defaulted given inside an array query refuses the load", async () => {
      const duckdb = await seededDuckdb(SEED);
      const loaded = await loadAuthorizeModel(
         `${FLAGS}
#(secure)
given: SKU :: string
given: DEF :: string is 's1'
#(access_filter) top = $SKU
source: g is duckdb.table('people') extend {
  dimension: top is items -> { select: sku, tw is tags -> { where: t = $DEF; aggregate: c is count() } } -> { where: tw > 0; aggregate: m is sku.max() }
}`,
         { duckdb },
      );
      try {
         expect(loaded.error?.message).toContain("`$DEF`");
      } finally {
         await loaded.cleanup();
         await duckdb.close();
      }
   });
});

describe("array queries: the access record and #(carries) read element fields under their array", () => {
   const CLASSIFIED = `
source: cls is duckdb.table('people') extend {
  #(classification) "phi.direct_identifier"
  rename: purchases is items
  #(classification) "none"
  dimension: pid is id
}
#(carries) ["none", "phi.direct_identifier"]
source: car is cls include { internal: *; public: pid } extend {
  dimension: spend is purchases -> { aggregate: t is price.sum() }
}
`;

   it("lists the array and its element fields, and no field of the row the query never read", async () => {
      await withModel(CLASSIFIED, async ({ model }) => {
         await rows(
            model,
            "run: cls -> { select: pid, s is purchases -> { select: ssn, tw is tags -> { aggregate: w2 is w.sum() } } }",
         );
         const record = lastRecord();
         // `ssn` of the row is classified nowhere and was not read: the
         // query read `purchases.ssn`, an element's.
         expect((record.fieldsRead ?? []).map((f) => f.path)).toEqual([
            "pid",
            "purchases",
            "purchases.ssn",
            "purchases.tags",
            "purchases.tags.w",
         ]);
         expect(
            (record.fieldsRead ?? []).find((f) => f.path === "purchases")
               ?.classifications,
         ).toEqual(["phi.direct_identifier"]);
         expect(record.fieldsReadComplete).toBe(true);
      });
   });

   it("classifies a dimension that is an array query by its array, and serves it under a ceiling that lists that class", async () => {
      await withModel(CLASSIFIED, async ({ model }) => {
         expect(
            await rows(model, "run: car -> { select: pid, spend }"),
         ).toEqual([
            { pid: 1, spend: 30 },
            { pid: 2, spend: 100 },
         ]);
         const spend = (lastRecord().fieldsRead ?? []).find(
            (f) => f.path === "spend",
         );
         expect(spend?.classifications).toEqual(["phi.direct_identifier"]);
      });
   });

   it("refuses a caller's array query over an array the ceiling does not list", async () => {
      await withModel(
         `${CLASSIFIED}
#(carries) ["none"]
source: narrow is cls include { internal: *; public: pid }
`,
         async ({ model }) => {
            expect(
               await refusal(
                  rows(
                     model,
                     "run: narrow extend { dimension: x is purchases -> { aggregate: c is count() } } -> { select: pid, x }",
                  ),
               ),
            ).toContain("classified phi.direct_identifier, not on the list");
         },
      );
   });
});

describe("array queries: a join inside the array query", () => {
   const CARRIED = `
source: base is duckdb.table('people') extend {
  #(classification) "phi.direct_identifier"
  dimension: social is ssn
}
#(carries) ["none"]
source: car is base include { internal: * } extend {
  #(classification) "none"
  dimension: pid is id
  #(classification) "none"
  dimension: k is org_id
}
`;
   const LEAK =
      "items -> { extend: { join_one: c is car extend { dimension: z is social } on sku = c.k } aggregate: x is c.z.max() }";

   it("refuses a query on a model field that reads past a #(carries) through it", async () => {
      // The load check refuses this model (below); a model loaded without it
      // still never serves the field.
      await withModel(
         `${CARRIED}
source: open_a is duckdb.table('people') extend { dimension: leak is ${LEAK} }
`,
         async ({ model }) => {
            expect(
               await refusal(
                  rows(model, "run: open_a -> { select: id, leak }"),
               ),
            ).toContain(ARRAY_JOIN_REFUSAL);
         },
      );
   });

   it("refuses a caller's in a model that only classifies, where nothing else would", async () => {
      await withModel(
         `
source: base is duckdb.table('people') extend {
  #(classification) "phi.direct_identifier"
  dimension: social is ssn
}
source: open_src is duckdb.table('people')
`,
         async ({ model }) => {
            expect(
               await refusal(
                  rows(
                     model,
                     "run: open_src -> { select: id, s is items -> { extend: { join_one: b is base on sku = b.org_id } select: x is b.social } }",
                  ),
               ),
            ).toContain(ARRAY_JOIN_REFUSAL);
         },
      );
   });

   it("denies a caller's into a gated source, for a caller the gate admits and one it does not", async () => {
      await withModel(
         `
#(secure)
given: ROLES :: string[]
#(authorize) 'clinician' in $ROLES
source: locked is duckdb.table('people')
source: open_src is duckdb.table('people')
`,
         async ({ model }) => {
            const query =
               "run: open_src -> { select: id, s is items -> { extend: { join_one: l is locked on sku = l.org_id } select: x is l.ssn } }";
            await expectAccessDenied(rows(model, query, {}));
            await expect(
               rows(model, query, { ROLES: ["clinician"] }),
            ).rejects.toThrow();
         },
      );
   });

   it("is served in a model that governs nothing", async () => {
      await withModel(
         `
source: other is duckdb.table('people')
source: open_src is duckdb.table('people')
`,
         async ({ model }) => {
            expect(
               await rows(
                  model,
                  "run: open_src -> { select: id, s is items -> { extend: { join_one: o is other on sku = o.org_id } aggregate: x is o.ssn.max() } }",
               ),
            ).toEqual([
               { id: 1, s: "111" },
               { id: 2, s: "222" },
            ]);
         },
      );
   });
});

describe("array queries: a caller's join inside an array query, wherever the query holds it", () => {
   const CARRIED = `
source: base is duckdb.table('people') extend {
  #(classification) "phi.direct_identifier"
  dimension: social is ssn
}
#(carries) ["none"]
source: car is base include { internal: * } extend {
  #(classification) "none"
  dimension: k is org_id
}
source: open_a is duckdb.table('people') extend { view: v is { select: id } }
`;
   const JOIN = "join_one: c is car extend { dimension: z is social } on sku = c.k";
   const READ = `items -> { extend: { ${JOIN} } aggregate: m is c.z.max() }`;
   const FORMS: Record<string, string> = {
      "a stage's extend": `run: open_a -> { extend: { dimension: d is ${READ} } select: d }`,
      "a source extend": `run: open_a extend { dimension: d is ${READ} } -> { select: d }`,
      "a view refinement": `run: open_a -> v + { select: x is ${READ} }`,
      "a where": `run: open_a -> { where: ${READ} = '111'; select: id }`,
      "a having": `run: open_a -> { group_by: id; aggregate: n is count(); having: max(${READ}) = '111' }`,
      "a pick": `run: open_a -> { select: x is pick ${READ} when id = 1 else 'n' }`,
      "a nest": `run: open_a -> { group_by: id; nest: n is { select: x is ${READ} } }`,
      "an array query nested in another": `run: open_a -> { select: x is items -> { select: sku, y is tags -> { extend: { join_one: c is car extend { dimension: z is social } on t = c.k } aggregate: m is c.z.max() } } }`,
      "a nest inside the array query": `run: open_a -> { select: x is items -> { group_by: sku; nest: n is { extend: { ${JOIN} } aggregate: m is c.z.max() } } }`,
      "a later stage of the array query": `run: open_a -> { select: x is items -> { select: sku } -> { extend: { ${JOIN} } aggregate: m is c.z.max() } }`,
      "a named query": `query: qq is open_a -> { select: id, x is ${READ} }\nrun: qq -> { select: * }`,
      "a source the query declares": `source: s2 is open_a extend { dimension: d is ${READ} }\nrun: s2 -> { select: d }`,
      "a source the query declares and joins": `source: s2 is open_a extend { dimension: d is ${READ} }\nrun: open_a extend { join_one: j is s2 on id = j.id } -> { select: j.d }`,
      "a query source the query declares and joins": `source: s2 is open_a -> { select: id, x is ${READ} }\nrun: open_a extend { join_one: j is s2 on id = j.id } -> { select: j.x }`,
   };

   for (const [where, query] of Object.entries(FORMS)) {
      it(`is refused in ${where}`, async () => {
         await withModel(CARRIED, async ({ model }) => {
            expect(await refusal(rows(model, query))).toContain(
               ARRAY_JOIN_REFUSAL,
            );
         });
      });
   }
});

describe("array queries: a join inside the array query refuses a governed package's load", () => {
   let pool: PackageLoadPool;
   let dir: string;
   let duckdb: DuckDBConnection;

   beforeAll(async () => {
      pool = new PackageLoadPool(1);
      await __setPackageLoadPoolForTests(pool);
   });
   afterAll(async () => {
      await __setPackageLoadPoolForTests(null);
   });
   beforeEach(async () => {
      dir = writeTempPackage(
         {},
         {
            prefix: "publisher-arrayq-",
            manifest: { name: "pkg", description: "array queries" },
         },
      ).dir;
      duckdb = await seededDuckdb(SEED);
      await duckdb.runSQL(
         `COPY people TO '${path.join(dir, "people.parquet")}' (FORMAT parquet)`,
      );
   });
   afterEach(async () => {
      await duckdb.close();
      fs.rmSync(dir, { recursive: true, force: true });
   });

   async function load(text: string): Promise<Package> {
      fs.writeFileSync(
         path.join(dir, "m.malloy"),
         `${FLAGS}\n${text}`.replaceAll(
            "duckdb.table('people')",
            "duckdb.table('people.parquet')",
         ),
      );
      const config = new MalloyConfig({ connections: {} });
      config.wrapConnections(
         () => new FixedConnectionMap(new Map([["duckdb", duckdb]]), "duckdb"),
      );
      return Package.create("env", "pkg", dir, config);
   }

   async function loadRefusal(text: string): Promise<string> {
      const err = await load(text).then(
         () => undefined,
         (e: unknown) => e,
      );
      expect(err).toBeInstanceOf(Error);
      return (err as Error).message;
   }

   it("refuses a model field that reads a #(carries) source past its list", async () => {
      expect(
         await loadRefusal(`
source: base is duckdb.table('people') extend {
  #(classification) "phi.direct_identifier"
  dimension: social is ssn
}
#(carries) ["none"]
source: car is base include { internal: * } extend {
  #(classification) "none"
  dimension: pid is id
  #(classification) "none"
  dimension: k is org_id
}
source: open_a is duckdb.table('people') extend {
  dimension: leak is items -> { extend: { join_one: c is car extend { dimension: z is social } on sku = c.k } aggregate: x is c.z.max() }
}
`),
      ).toContain(`Model m.malloy ${ARRAY_JOIN_REFUSAL}`);
   });

   it("refuses a view that joins a gated source no join-bypass check can see", async () => {
      expect(
         await loadRefusal(`
#(secure)
given: ROLES :: string[]
#(authorize) 'clinician' in $ROLES
source: locked is duckdb.table('people')
source: open_a is duckdb.table('people') extend {
  view: v is { select: id, s is items -> { select: sku, tw is tags -> { extend: { join_one: l is locked on t = l.org_id } aggregate: x is l.ssn.max() } } }
}
`),
      ).toContain("`l`");
   });

   it("refuses a notebook, or a model, whose own join reads a governed model it imports", async () => {
      const governed = `${FLAGS}
source: base is duckdb.table('people.parquet') extend {
  #(classification) "phi.direct_identifier"
  dimension: social is ssn
}
#(carries) ["none"]
source: car is base include { internal: * } extend {
  #(classification) "none"
  dimension: k is org_id
}
source: open_a is duckdb.table('people.parquet')
`;
      const join =
         "items -> { extend: { join_one: c is car extend { dimension: z is social } on sku = c.k } aggregate: m is c.z.max() }";
      for (const [file, text] of [
         [
            "n.malloynb",
            `>>>malloy\n${FLAGS}\nimport 'g.malloy'\n>>>malloy\nrun: open_a -> { select: id, x is ${join} }\n`,
         ],
         [
            "u.malloy",
            `${FLAGS}\nimport { car } from 'g.malloy'\nsource: open_u is duckdb.table('people.parquet') extend { dimension: leak is ${join} }\n`,
         ],
      ]) {
         fs.writeFileSync(path.join(dir, "g.malloy"), governed);
         fs.writeFileSync(path.join(dir, file), text);
         try {
            expect(await loadRefusal("")).toContain(
               `Model ${file} ${ARRAY_JOIN_REFUSAL}`,
            );
         } finally {
            fs.rmSync(path.join(dir, file));
         }
      }
   });

   it("loads the same join where the package governs nothing", async () => {
      const pkg = await load(`
source: other is duckdb.table('people')
source: open_a is duckdb.table('people') extend {
  dimension: leak is items -> { extend: { join_one: o is other on sku = o.org_id } aggregate: x is o.ssn.max() }
}
`);
      expect(pkg.getModel("m.malloy")).toBeDefined();
   });

   it("loads a governed package whose array queries read only their elements", async () => {
      const pkg = await load(`
#(secure)
given: ROLES :: string[]
source: base is duckdb.table('people') extend {
  #(classification) "phi.direct_identifier"
  rename: purchases is items
}
#(authorize) 'clinician' in $ROLES
#(carries) ["none", "phi.direct_identifier"]
source: chart is base include { internal: * } extend {
  #(classification) "none"
  dimension: pid is id
  dimension: spend is purchases -> { aggregate: t is price.sum() }
}
`);
      expect(pkg.getModel("m.malloy")).toBeDefined();
   });
});
