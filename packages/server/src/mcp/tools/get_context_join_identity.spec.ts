// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Two things about which fields the semantic index embeds when joined copies
 * are made at assembly:
 *
 * 1. A field reached through a join that assembly cannot rebuild stays in the
 *    index, so it stays searchable. That is a join to an inline table or SQL
 *    (there is no source to copy from) and a field a join adds to its target.
 * 2. A source is identified by the file that defines it as well as its name.
 *    Two files that each define `cust` do not borrow each other's joins.
 *
 * Both run over the real compiler, so the join topology under test is the one
 * the server builds.
 */

import { describe, expect, it } from "bun:test";
import {
   compileModelFiles,
   storeServing,
} from "../../test_helpers/get_context_join_fixture";
import { assembleCards } from "./get_context_assembly";
import type { PipelineContext, PipelineSettings } from "./get_context_pipeline";
import {
   getPackageIndex,
   projectEntity,
   type ResolvedRequest,
   type ResultEntity,
} from "./get_context_tool";

const ASSEMBLY: PipelineSettings = {
   joins: "assembly",
   entityWindow: { perSourcePerTarget: 10 },
   joinMaxDepth: 10,
   joinDamping: 0.9,
   scoring: "cosine",
   maxChars: null,
   reserveChars: 1_000,
};

function request(extra: Partial<ResolvedRequest> = {}): ResolvedRequest {
   return {
      pureSourceListing: false,
      environmentName: "env",
      packageName: "pkg",
      kinds: new Set(["dimension", "measure"]),
      searches: [],
      listingOnly: false,
      includeCode: false,
      unsupported: [],
      limit: 150,
      offset: 0,
      ...extra,
   };
}

const names = (
   entities: readonly { source?: string; name: string }[],
   source: string,
) =>
   entities
      .filter((e) => e.source === source)
      .map((e) => e.name)
      .sort();

describe("joined fields assembly cannot rebuild stay in the semantic index", () => {
   const MODEL = `
source: cust is duckdb.sql("select 1 as id, 'a' as nm") extend {
  #(doc) Name of the customer.
  dimension: name is nm
}

source: ord is duckdb.sql("select 1 as id, 1 as cust_id, 1 as zone_id") extend {
  #(doc) Order status.
  dimension: status is 's'
  join_one: c is cust on c.id = cust_id
  join_one: z is duckdb.sql("select 1 as id, 'x' as zone_name") extend {
    #(doc) Name of the delivery zone.
    dimension: zone is zone_name
  } on z.id = zone_id
}
`;

   it("keeps a field joined from an inline table, and drops a copy it can rebuild", async () => {
      const { pkg } = await compileModelFiles({ "m.malloy": MODEL });
      const index = await getPackageIndex(storeServing(pkg), "env", "pkg");
      const direct = names(index.directEntities, "ord");
      // The inline join has no source to copy from, so its field is indexed.
      expect(direct).toContain("z.zone");
      // `cust` is a source of its own: its field is copied at assembly.
      expect(direct).not.toContain("c.name");
      // Nothing else changes: the lexical index still has every copy.
      expect(names(index.retrievalEntities, "ord")).toEqual(
         expect.arrayContaining(["z.zone", "c.name"]),
      );
   });

   it("keeps the fields of a join that extends its target", async () => {
      const { pkg } = await compileModelFiles({
         "m.malloy": `
source: cust is duckdb.sql("select 1 as id, 'a' as nm") extend {
  dimension: name is nm
}
source: ord is duckdb.sql("select 1 as id, 1 as cust_id") extend {
  join_one: c is cust extend {
    #(doc) A short form of the name.
    dimension: nick is nm
  } on c.id = cust_id
}
`,
      });
      const index = await getPackageIndex(storeServing(pkg), "env", "pkg");
      const direct = names(index.directEntities, "ord");
      // `nick` exists only through the join, so nothing can rebuild it.
      expect(direct).toContain("c.nick");
      // The compiler records no source for a join that extends its target,
      // so `c.name` has no route back to `cust.name` either: it stays too.
      expect(direct).toContain("c.name");
   });
});

describe("a source is identified by its file as well as its name", () => {
   const FILES = {
      "a.malloy": `
source: cust is duckdb.sql("select 1 as id, 'a' as nm") extend {
  dimension: name is nm
}
source: ord is duckdb.sql("select 1 as id, 1 as cust_id") extend {
  join_one: buyer is cust on buyer.id = cust_id
}
`,
      "b.malloy": `
source: cust is duckdb.sql("select 1 as id, 'b' as nm") extend {
  dimension: name is nm
}
source: invoice is duckdb.sql("select 1 as id, 1 as cust_id") extend {
  join_one: payer is cust on payer.id = cust_id
}
`,
   };

   it("makes a joined copy only in the file whose source it was ranked from", async () => {
      const { pkg } = await compileModelFiles(FILES);
      const index = await getPackageIndex(storeServing(pkg), "env", "pkg");
      const direct = index.directEntities.find(
         (e) =>
            e.modelPath === "a.malloy" &&
            e.source === "cust" &&
            e.name === "name",
      );
      if (!direct) throw new Error("no direct cust.name in a.malloy");
      const row: ResultEntity = {
         ...projectEntity(direct, "env", "pkg"),
         score: 0.5,
         targetScores: new Map([[0, 0.5]]),
         bestTarget: 0,
      };
      const ctx = {
         request: request(),
         pkgIndex: index,
         settings: ASSEMBLY,
      } as unknown as PipelineContext;
      const state = assembleCards(
         {
            rows: [row],
            retrieval: "semantic",
            belowCutoffCount: 0,
         },
         ctx,
      );
      const cards = state.cards.map((c) => `${c.modelPath}:${c.source}`);
      // a.malloy's `ord` joins a.malloy's `cust`. b.malloy's `invoice` joins a
      // different `cust`, so it is not a place this field is reached.
      expect(cards).toContain("a.malloy:ord");
      expect(cards).not.toContain("b.malloy:invoice");
   });
});
