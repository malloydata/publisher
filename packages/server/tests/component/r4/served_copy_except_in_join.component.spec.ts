// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Under the v1 cutover a re-root deletes an `except:` item that names no
 * stored column (`dropExceptItems`): the table never held it, so the item
 * excludes nothing. Only a statement of the re-rooted source's own top-level
 * blocks qualifies. An `except:` inside a join's own `extend` hides a field
 * of the JOINED source, which the table never stores either; deleting it
 * would expose that field through the join. Served must refuse such a field
 * exactly as live does, and still answer the rest from the table.
 */
import { afterAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import { DATA_FILES, RAW_MODEL } from "./conformance/data";
import { freshLoadPool, loadFixture } from "./conformance/package_harness";
import { rowDifference } from "./harness";

const SQL = `duckdb.sql("SELECT CAST(id AS BIGINT) AS id, region, CAST(amount AS DOUBLE) AS amount, CAST(customer_id AS BIGINT) AS customer_id FROM read_csv('data/orders.csv', header=true, all_varchar=true)")`;

function model(storage: string): string {
   return `##! experimental { persistence access_modifiers }
import "raw.malloy"
#@ persist name="ej_conn"${storage}
source: ej_conn is ${SQL} extend {
  join_one: c is customers_raw extend { except: name } on c.id = customer_id
}
#@ persist name="ej_query"${storage}
source: ej_query is orders_raw -> { group_by: id, customer_id, amount } extend {
  join_one: c is customers_raw extend { except: name } on c.id = customer_id
}
`;
}

const HIDDEN = [
   "run: ej_conn -> { group_by: c.name; aggregate: n is count() }",
   "run: ej_query -> { group_by: c.name; aggregate: n is count() }",
];
const VISIBLE = [
   "run: ej_conn -> { group_by: c.segment; aggregate: n is count() }",
   "run: ej_query -> { group_by: c.segment; aggregate: n is count() }",
];

describe("an except: inside a join's own extend on a v1 re-root", () => {
   afterAll(async () => {
      await freshLoadPool();
   });

   for (const [label, storage] of [
      ["colocated", ""],
      ["storage=", ` storage="lake"`],
   ] as const) {
      it(`keeps the joined field hidden (${label})`, async () => {
         const restore = installSettings({
            env: { PERSIST_STORAGE_MODE: "on" },
         }).restore;
         await freshLoadPool();
         const loader = await loadFixture({
            ...DATA_FILES,
            "raw.malloy": RAW_MODEL,
            "m.malloy": model(storage),
         });
         try {
            for (const q of HIDDEN) {
               const live = await loader.run("m.malloy", q);
               expect("error" in live && live.error).toContain(
                  "'name' is not defined",
               );
            }
            const fixture = await loader.bind();
            try {
               let fromTable = 0;
               for (const q of VISIBLE) {
                  const live = await loader.run("m.malloy", q);
                  const served = await fixture.run("m.malloy", q);
                  if ("error" in live) throw new Error(live.error);
                  if ("error" in served) throw new Error(served.error);
                  expect(
                     rowDifference(live.answer.rows, served.answer.rows),
                  ).toBeUndefined();
                  if (served.answer.via === "served_copy") fromTable++;
               }
               for (const q of HIDDEN) {
                  const served = await fixture.run("m.malloy", q);
                  expect({ q, served }).toEqual({
                     q,
                     served: { error: expect.stringContaining("'name'") },
                  });
               }
               // The colocated re-roots still answer from their tables.
               if (storage === "") expect(fromTable).toBe(VISIBLE.length);
            } finally {
               await fixture.dispose();
            }
         } finally {
            restore();
         }
      }, 180_000);
   }
});
