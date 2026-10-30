// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A connection with `dbmeta` on annotates each described table and column
 * with `#(dbmeta) description="..."`. A source, join or field the model does
 * not document itself, with `#(doc)` or a `#"` doc string, takes that
 * description, marked as the database's.
 */

import { describe, expect, it } from "bun:test";
import {
   compileJoinFixture,
   storeServing,
} from "../../test_helpers/get_context_join_fixture";
import { projectEntity, toSourceResults } from "./get_context_response";
import { getPackageIndex, sourceContextKey } from "./package_entities";

const ORDERS_SQL = "select 1 as id, 2 as amount";
const INVOICES_SQL = "select 1 as id, 2 as amount, 3 as tax";
const NET_REVENUE =
   "Net revenue. Document fields with #(doc) per the style guide.";

const MODEL_TEXT = `
##! experimental { access_modifiers }
source: bare is duckdb.sql("${ORDERS_SQL}")

#(doc) Authored orders.
source: documented is duckdb.sql("${ORDERS_SQL}") extend {
  dimension: one is 1
}

source: child is bare extend {
  dimension: two is 2
}

source: invoices is duckdb.sql("${INVOICES_SQL}") include {
  #" Net of refunds.
  amount
  #(doc) Tax charged.
  tax
  id
}

source: orders is duckdb.sql("select 1 as id, 1 as bare_id") extend {
  join_one: b is bare on b.id = bare_id
}
`;

async function indexWithDbmeta() {
   const { pkg } = await compileJoinFixture("m.malloy", {
      modelText: MODEL_TEXT,
      dbmeta: {
         [ORDERS_SQL]: {
            table: "Orders table.",
            columns: { amount: NET_REVENUE },
         },
         [INVOICES_SQL]: {
            table: "Invoices table.",
            columns: {
               amount: "Warehouse amount.",
               tax: "See #(doc) on the model; this column is net.",
            },
         },
      },
   });
   const index = await getPackageIndex(storeServing(pkg), "env", "pkg");
   const find = (kind: string, source: string, name: string) =>
      [...index.byId.values()].find(
         (e) => e.kind === kind && e.source === source && e.name === name,
      );
   return { index, find };
}

describe("database descriptions on a source", () => {
   it("document a bare table source", async () => {
      const { find } = await indexWithDbmeta();
      expect(find("source", "bare", "bare")).toMatchObject({
         doc: "Orders table.",
         embedDoc: "Orders table.",
         docSource: "dbmeta",
      });
   });

   it("document a source that extends a table without documenting itself", async () => {
      const { find } = await indexWithDbmeta();
      expect(find("source", "child", "child")).toMatchObject({
         doc: "Orders table.",
         docSource: "dbmeta",
      });
   });

   it("give way to the source's own #(doc)", async () => {
      const { find } = await indexWithDbmeta();
      const source = find("source", "documented", "documented");
      expect(source?.doc).toBe("Authored orders.");
      expect(source?.docSource).toBeUndefined();
   });

   it("reach the source context with their marker", async () => {
      const { index } = await indexWithDbmeta();
      expect(
         index.sourceContext.get(sourceContextKey("m.malloy", "bare")),
      ).toMatchObject({ doc: "Orders table.", docSource: "dbmeta" });
   });
});

describe("database descriptions on a join", () => {
   it("document a join the model does not document, like its target source", async () => {
      const { find } = await indexWithDbmeta();
      expect(find("join", "orders", "b")).toMatchObject({
         doc: "Orders table.",
         docSource: "dbmeta",
      });
   });

   it("document the fields reached through it", async () => {
      const { find } = await indexWithDbmeta();
      expect(find("dimension", "orders", "b.amount")).toMatchObject({
         doc: NET_REVENUE,
         docSource: "dbmeta",
      });
   });
});

describe("database descriptions on a field", () => {
   it("are read whole when they mention #(doc)", async () => {
      const { find } = await indexWithDbmeta();
      expect(find("dimension", "bare", "amount")).toMatchObject({
         doc: NET_REVENUE,
         embedDoc: NET_REVENUE,
         docSource: "dbmeta",
      });
   });

   it("never add to an authored #(doc), even when they mention #(doc)", async () => {
      const { find } = await indexWithDbmeta();
      const tax = find("dimension", "invoices", "tax");
      expect(tax?.doc).toBe("Tax charged.");
      expect(tax?.docSource).toBeUndefined();
   });

   it('give way to a #" doc string', async () => {
      const { find } = await indexWithDbmeta();
      const amount = find("dimension", "invoices", "amount");
      expect(amount?.doc).toBe("Net of refunds.");
      expect(amount?.embedDoc).toBe("Net of refunds.");
      expect(amount?.docSource).toBeUndefined();
   });

   it("leave a column with no description undocumented", async () => {
      const { find } = await indexWithDbmeta();
      const id = find("dimension", "invoices", "id");
      expect(id?.doc).toBe("");
      expect(id?.docSource).toBeUndefined();
   });
});

describe("the response", () => {
   it("marks a description and a source's docs that came from the database", async () => {
      const { index, find } = await indexWithDbmeta();
      const rows = [
         find("source", "bare", "bare")!,
         find("dimension", "bare", "amount")!,
         find("dimension", "invoices", "tax")!,
      ].map((e) => projectEntity(e, "env", "pkg"));
      const cards = toSourceResults(rows, index.sourceContext, "env", "pkg");
      const bare = cards.find(
         (c) => c.source_info.resource_id.source === "bare",
      );
      expect(bare?.source_info).toMatchObject({
         docs: "Orders table.",
         docs_source: "dbmeta",
      });
      expect(bare?.entities?.[0]).toMatchObject({
         name: "amount",
         description: NET_REVENUE,
         description_source: "dbmeta",
      });
      const invoices = cards.find(
         (c) => c.source_info.resource_id.source === "invoices",
      );
      expect(invoices?.source_info.docs).toBe("Invoices table.");
      expect(invoices?.entities?.[0].description).toBe("Tax charged.");
      expect(invoices?.entities?.[0]).not.toHaveProperty("description_source");
   });
});
