// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * #2048: a dimension defined as exactly a one-hop join path
 * (`region is account.region`) with no `#(doc)` of its own reports the
 * joined field's doc, marked as inherited from that path, wherever the
 * model's fields are described: the package index `get_context` reads, its
 * entity cards, and `GET …/entities`. A doc written on the dimension wins,
 * and an expression over the path inherits nothing.
 */

import { describe, expect, it } from "bun:test";
import { compileFixture } from "../../service/entities/test_support";
import { toApiEntities } from "../../service/entities/to_api";
import {
   compileJoinFixture,
   storeServing,
} from "../../test_helpers/get_context_join_fixture";
import { projectEntity, toSourceResults } from "../search/response";
import { getPackageIndex } from "./package_entities";

const REGION_DOC = "The sales region the account belongs to.";

const MODEL_TEXT = `
source: accounts is duckdb.sql("select 1 as account_id, 'EU' as region") extend {
  primary_key: account_id
  #(doc) ${REGION_DOC}
  dimension: sales_region is region
}

source: subs is duckdb.sql("select 10 as sub_id, 1 as account_id") extend {
  primary_key: sub_id
  join_one: account is accounts with account_id
  dimension:
    region is account.sales_region
    #(doc) The region, as the subscription reports it.
    own_region is account.sales_region
    up_region is upper(account.sales_region)
    a_res is account.sales_region
    a_chain is a_res
  join_one: \`source\` is accounts with account_id
  dimension: via_reserved is \`source\`.sales_region
}
`;

describe("an inherited #(doc) in the package index (get_context)", () => {
   async function find(name: string) {
      const { pkg } = await compileJoinFixture("m.malloy", {
         modelText: MODEL_TEXT,
      });
      const index = await getPackageIndex(
         new WeakMap(),
         storeServing(pkg),
         "env",
         "pkg",
      );
      const entity = [...index.byId.values()].find(
         (e) =>
            e.kind === "dimension" && e.source === "subs" && e.name === name,
      );
      return { index, entity };
   }

   it("reports the joined field's doc on a dimension that is exactly the path, marked inherited", async () => {
      const { entity } = await find("region");
      expect(entity).toMatchObject({
         doc: REGION_DOC,
         embedDoc: REGION_DOC,
         docInheritedFrom: "account.sales_region",
      });
   });

   it("carries the mark onto the entity card", async () => {
      const { index, entity } = await find("region");
      const cards = toSourceResults(
         [projectEntity(entity!, "env", "pkg")],
         index.sourceContext,
         "env",
         "pkg",
      );
      const card = cards
         .flatMap((c) => c.entities ?? [])
         .find((e) => e.name === "region");
      expect(card).toMatchObject({
         description: REGION_DOC,
         description_inherited_from: "account.sales_region",
      });
   });

   it("keeps a doc written on the dimension, unmarked", async () => {
      const { entity } = await find("own_region");
      expect(entity?.doc).toBe("The region, as the subscription reports it.");
      expect(entity?.docInheritedFrom).toBeUndefined();
   });

   it("inherits nothing for an expression over the path", async () => {
      const { entity } = await find("up_region");
      expect(entity?.embedDoc).toBe("");
      expect(entity?.docInheritedFrom).toBeUndefined();
   });
});

describe("an inherited #(doc) through an alias chain and a reserved-word join (get_context)", () => {
   it("keeps the mark on a chained alias, which folds its link into it", async () => {
      const { pkg } = await compileJoinFixture("m.malloy", {
         modelText: MODEL_TEXT,
      });
      const index = await getPackageIndex(
         new WeakMap(),
         storeServing(pkg),
         "env",
         "pkg",
      );
      const dims = [...index.byId.values()].filter(
         (e) => e.kind === "dimension" && e.source === "subs",
      );
      const chain = dims.find((e) => e.name === "a_chain");
      expect(chain).toMatchObject({
         doc: REGION_DOC,
         docInheritedFrom: "account.sales_region",
         aliases: ["a_res"],
      });
      expect(dims.find((e) => e.name === "via_reserved")).toMatchObject({
         doc: REGION_DOC,
         docInheritedFrom: "source.sales_region",
      });
   });
});

describe("an inherited #(doc) in GET …/entities", () => {
   async function field(name: string) {
      const results = await compileFixture({ "m.malloy": MODEL_TEXT });
      const [model] = toApiEntities(results);
      return model?.fields?.find((f) => f.source === "subs" && f.name === name);
   }

   it("reports the joined field's doc, naming the path in docInheritedFrom", async () => {
      expect(await field("region")).toMatchObject({
         doc: REGION_DOC,
         docInheritedFrom: "account.sales_region",
      });
   });

   it("keeps a doc written on the dimension, with no docInheritedFrom", async () => {
      const f = await field("own_region");
      expect(f?.doc).toBe("The region, as the subscription reports it.");
      expect(f?.docInheritedFrom).toBeUndefined();
   });

   it("reports it on each link of an alias chain, with the same mark", async () => {
      for (const name of ["a_res", "a_chain"]) {
         expect(await field(name)).toMatchObject({
            doc: REGION_DOC,
            docInheritedFrom: "account.sales_region",
         });
      }
   });

   it("names a reserved-word join in plain names, as the index does", async () => {
      expect(await field("via_reserved")).toMatchObject({
         doc: REGION_DOC,
         docInheritedFrom: "source.sales_region",
      });
   });

   it("inherits nothing for an expression over the path", async () => {
      const f = await field("up_region");
      expect(f?.doc).toBeUndefined();
      expect(f?.docInheritedFrom).toBeUndefined();
   });
});
