// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The find, list, judge catalog reads database descriptions by the same rule
 * as the entity index: a source, join or field with no doc of its own takes
 * its table's or column's `#(dbmeta)` description, marked as the database's,
 * and nothing a deny-all source documents is copied through a join to it.
 */

import { describe, expect, it } from "bun:test";
import {
   compileJoinFixture,
   storeServing,
} from "../../test_helpers/get_context_join_fixture";
import { getPackageIndex } from "../tools/package_entities";
import { buildCatalog, catalogInputOf } from "./catalog";

const ORDERS_SQL = "select 1 as id, 2 as amount";
const VAULT_SQL = "select 1 as id, 9 as salary";
const SECRETS = [
   "Payroll vault",
   "Employee salaries",
   "Gross salary",
   "Salaries",
];

const MODEL_TEXT = `
source: bare is duckdb.sql("${ORDERS_SQL}")

source: child is bare extend {
  dimension: two is 2
}

#(doc) Authored child.
source: documented_child is bare extend {
  dimension: three is 3
}

#(doc) Payroll vault.
source: vault_src is duckdb.sql("${VAULT_SQL}") extend {
  #(doc) Salaries.
  dimension: pay is salary
}

source: orders is duckdb.sql("select 1 as id, 1 as vault_id") extend {
  join_one: vault is vault_src on vault.id = vault_id
}

source: hop2 is duckdb.sql("select 1 as id, 1 as vault_id") extend {
  join_one: vault is vault_src on vault.id = vault_id
}

source: hop1 is duckdb.sql("select 1 as id") extend {
  join_one: hop2 is hop2 on hop2.id = id
}

source: deep is duckdb.sql("select 1 as id") extend {
  join_one: hop1 is hop1 on hop1.id = id
}
`;

async function catalogWithDbmeta() {
   const { pkg } = await compileJoinFixture("m.malloy", {
      modelText: MODEL_TEXT,
      apiSources: [
         { name: "bare" },
         { name: "child" },
         { name: "documented_child" },
         { name: "vault_src", authorize: ["false"] },
         { name: "orders" },
         { name: "hop2" },
         { name: "hop1" },
         { name: "deep" },
      ],
      dbmeta: {
         [ORDERS_SQL]: {
            table: "Orders table.",
            columns: { amount: "Amount in cents." },
         },
         [VAULT_SQL]: {
            table: "Employee salaries, one row per pay period.",
            columns: { salary: "Gross salary." },
         },
      },
   });
   const pkgIndex = await getPackageIndex(storeServing(pkg), "env", "pkg");
   return buildCatalog({
      ...(await catalogInputOf(pkgIndex)),
      unservedCopies: 10,
   });
}

const sourceNode = (
   catalog: Awaited<ReturnType<typeof catalogWithDbmeta>>,
   name: string,
) => [...catalog.sources.values()].find((s) => s.name === name)?.node;

describe("the catalog with database descriptions", () => {
   it("documents a bare table source with its table description", async () => {
      const catalog = await catalogWithDbmeta();
      expect(sourceNode(catalog, "bare")).toMatchObject({
         doc: "Orders table.",
         displayDoc: "Orders table.",
         docSource: "dbmeta",
      });
   });

   it("documents an extension with no doc of its own with its table description", async () => {
      const catalog = await catalogWithDbmeta();
      expect(sourceNode(catalog, "child")).toMatchObject({
         doc: "Orders table.",
         docSource: "dbmeta",
      });
   });

   it("keeps an extension's own #(doc), unmarked", async () => {
      const catalog = await catalogWithDbmeta();
      const node = sourceNode(catalog, "documented_child");
      expect(node?.doc).toBe("Authored child.");
      expect(node?.docSource).toBeUndefined();
   });

   it("marks a field documented by its column description", async () => {
      const catalog = await catalogWithDbmeta();
      const amount = [...catalog.nodes.values()].find(
         (n) => n.source === "bare" && n.name === "amount",
      );
      expect(amount).toMatchObject({
         doc: "Amount in cents.",
         docSource: "dbmeta",
      });
   });

   it("copies nothing a deny-all source documents through a join to it", async () => {
      const catalog = await catalogWithDbmeta();
      const nodes = [...catalog.nodes.values()];
      expect(
         nodes.some((n) => n.source === "orders" && n.name === "vault.pay"),
      ).toBe(true);
      expect(
         nodes.some(
            (n) => n.source === "deep" && n.name === "hop1.hop2.vault.pay",
         ),
      ).toBe(true);
      for (const n of nodes) {
         for (const secret of SECRETS) {
            expect(`${n.doc} ${n.displayDoc}`).not.toContain(secret);
         }
      }
   });
});
