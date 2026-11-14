// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A field read inside a record or array column (`purchases.ssn`, `r.a.b`)
 * carries the classes of the column it is read through, unless it declares
 * its own: a part of a classified column is a piece of that column's value.
 * Before, a part read with no classes at all, so the access record
 * under-reported it (`classifications: []`) and a `#(carries)` ceiling
 * refused it as unclassified even where the ceiling lists the column's class.
 */
import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import type { ModelDef } from "@malloydata/malloy";
import { BadRequestError } from "../errors";
import { logger } from "../logger";
import {
   loadAuthorizeModel,
   type LoadedAuthorizeModel,
} from "../test_helpers/authorize_fixture";
import { compileModelFiles } from "../test_helpers/get_context_join_fixture";
import { seededDuckdb } from "../test_helpers/temp_package";
import { withAccessRecord, type AccessRecord } from "./access_record";
import { readSourceClassifications } from "./classification";
import type { Model } from "./model";
import { QueryPrivileges } from "./model_query_request";

const SEED = `
CREATE OR REPLACE TABLE people (id INTEGER, ssn VARCHAR,
  items STRUCT(sku VARCHAR, ssn VARCHAR)[],
  rec STRUCT(a VARCHAR, inr STRUCT(b VARCHAR)));
INSERT INTO people VALUES
 (1, '111', [{'sku': 'x', 'ssn': 's1'}], {'a': 'a1', 'inr': {'b': 'b1'}}),
 (2, '222', [{'sku': 'y', 'ssn': 's2'}], {'a': 'a2', 'inr': {'b': 'b2'}});
`;

/** `purchases` and `r` are the classified columns; `plain` is not. */
const MODEL = `
source: cls is duckdb.table('people') extend {
  #(classification) "phi.direct_identifier"
  rename: purchases is items
  #(classification) "pii"
  rename: r is rec
  #(classification) "none"
  dimension: pid is id
}
`;

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
   const loaded = await loadAuthorizeModel(text, { duckdb });
   try {
      expect(loaded.error).toBeUndefined();
      return await body(loaded);
   } finally {
      await loaded.cleanup();
      await duckdb.close();
   }
}

function run(model: Model, query: string): Promise<unknown> {
   return withAccessRecord(
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
            { query, filterParams: {}, givens: {}, responseShape: "compact" },
            QueryPrivileges.of({ bypassFilters: true }),
         ),
   );
}

/** The one access record the last query wrote. */
function lastRecord(): AccessRecord {
   const records = (info.mock.calls as unknown[][])
      .filter(([message]) => message === "Access record")
      .map(([, record]) => record as AccessRecord);
   expect(records.length).toBeGreaterThan(0);
   return records[records.length - 1];
}

function classesOf(record: AccessRecord): Record<string, string[]> {
   return Object.fromEntries(
      (record.fieldsRead ?? []).map((f) => [f.path, f.classifications]),
   );
}

describe("a field read inside a record or array column", () => {
   it("carries the column's classes in the source's lineage, at any depth", async () => {
      const { modelDefs } = await compileModelFiles({
         "m.malloy": MODEL.replaceAll(
            "duckdb.table('people')",
            `duckdb.sql("select [{'sku': 'x', 'ssn': 's'}] as items, {'a': 'a', 'inr': {'b': 'b'}} as rec, 1 as id")`,
         ),
      });
      const md = modelDefs["m.malloy"] as ModelDef;
      const lineage = readSourceClassifications(
         (md.contents.cls as { fields?: unknown[] }).fields as never,
      );
      expect(lineage.get("purchases.ssn")?.classifications).toEqual([
         "phi.direct_identifier",
      ]);
      expect(lineage.get("r.a")?.classifications).toEqual(["pii"]);
      expect(lineage.get("r.inr.b")?.classifications).toEqual(["pii"]);
      // The column itself is unchanged.
      expect(lineage.get("purchases")?.classifications).toEqual([
         "phi.direct_identifier",
      ]);
   });

   it("is recorded in the access record with the column's classes", async () => {
      await withModel(MODEL, async ({ model }) => {
         // A renamed record column does not run (Malloy reads it under its
         // new name), so the record case is pinned in the lineage above.
         await run(model, "run: cls -> { group_by: pid, purchases.ssn }");
         const record = lastRecord();
         expect(classesOf(record)).toEqual({
            pid: ["none"],
            "purchases.ssn": ["phi.direct_identifier"],
         });
         expect(record.fieldsReadComplete).toBe(true);
      });
   });

   it("leaves a whole column read as one read, not a read of every part", async () => {
      await withModel(MODEL, async ({ model }) => {
         await run(model, "run: cls -> { select: pid, purchases }");
         const record = lastRecord();
         // The parts only carry the column's own class, so they are not inputs to it.
         expect(classesOf(record)).toEqual({
            pid: ["none"],
            purchases: ["phi.direct_identifier"],
         });
         expect(record.fieldsReadComplete).toBe(true);
      });
   });

   it("classifies an element read inside a query on the array the same way", async () => {
      await withModel(
         `##! experimental { array_queries }\n${MODEL}`,
         async ({ model }) => {
            await run(
               model,
               "run: cls -> { select: pid, s is purchases -> { select: ssn } }",
            );
            const record = lastRecord();
            expect(classesOf(record)).toEqual({
               pid: ["none"],
               purchases: ["phi.direct_identifier"],
               "purchases.ssn": ["phi.direct_identifier"],
            });
            expect(record.fieldsReadComplete).toBe(true);
         },
      );
   });

   it("leaves parts of computed records, record literals and query outputs as they were", async () => {
      const { modelDefs } = await compileModelFiles({
         "m.malloy": `
source: cls is duckdb.sql("select [{'sku': 'x', 'ssn': 's'}] as items, {'a': 'a'} as rec, 1 as id") extend {
  #(classification) "phi.direct_identifier"
  rename: purchases is items
  #(classification) "pii"
  rename: r is rec
  #(classification) "none"
  dimension: pid is id
  #(classification) "pii"
  dimension: lit is { a is pid }
  #(classification) "gdpr.personal"
  dimension: cr is r
  #(classification) "gdpr.personal"
  dimension: arr is [pid]
}
source: q is cls -> { select: pid, purchases, lit }
`,
      });
      const md = modelDefs["m.malloy"] as ModelDef;
      const lineageOf = (name: string) => {
         const s = md.contents[name] as { fields?: unknown[]; query?: unknown };
         return readSourceClassifications(s.fields as never, s.query as never);
      };
      const classes = (name: string, path: string) =>
         lineageOf(name).get(path)?.classifications;
      // A literal's part reads its own element, not the record's class.
      expect(classes("cls", "lit.a")).toEqual(["none"]);
      // A computed record's or array's part takes what the value reads.
      expect(classes("cls", "cr.a")).toEqual(["gdpr.personal", "pii"]);
      expect(classes("cls", "arr.each")).toEqual(["gdpr.personal"]);
      // A query output's part reads the output it is part of.
      expect(classes("q", "purchases.ssn")).toEqual(["phi.direct_identifier"]);
      expect(lineageOf("q").get("purchases.ssn")?.inputs).toEqual([
         { field: "purchases", classifications: ["phi.direct_identifier"] },
      ]);
      expect(classes("q", "lit.a")).toEqual(["pii"]);
   });

   it("is served under a #(carries) ceiling that lists the column's class, and refused by one that does not", async () => {
      await withModel(
         `${MODEL}
#(carries) ["none", "phi.direct_identifier"]
source: wide is cls
#(carries) ["none"]
source: narrow is cls
`,
         async ({ model }) => {
            const served = (await run(
               model,
               "run: wide -> { group_by: purchases.ssn }",
            )) as { compactResult: unknown };
            expect(served.compactResult).toEqual([
               { ssn: "s1" },
               { ssn: "s2" },
            ]);
            const err = await run(
               model,
               "run: narrow -> { group_by: purchases.sku }",
            ).then(
               () => undefined,
               (e: unknown) => e,
            );
            expect(err).toBeInstanceOf(BadRequestError);
            expect((err as Error).message).toContain("phi.direct_identifier");
         },
      );
   });
});
