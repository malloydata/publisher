// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A source with an unconditional deny-all gate is dropped from the index, and
 * its name must not reach a model through another source that joins it. The
 * join is written under an alias, so the visible source's own field list names
 * the alias; the TARGET's real name is what has to stay out of the source
 * summary prompt.
 */

import { describe, expect, it } from "bun:test";
import {
   compileJoinFixture,
   storeServing,
} from "../../test_helpers/get_context_join_fixture";
import { buildSourceSummaryInputs } from "./source_summaries";
import { getPackageIndex } from "./package_entities";

const HIDDEN = "payroll_vault_tbl";

const MODEL_TEXT = `
source: ${HIDDEN} is duckdb.sql("select 1 as id, 9 as salary") extend {
  #(doc) Salaries.
  dimension: pay is salary
}

source: ok_country is duckdb.sql("select 1 as id, 'US' as code") extend {
  dimension: code_name is code
}

source: orders is duckdb.sql("select 1 as id, 1 as vault_id, 1 as c_id") extend {
  dimension: status is 's'
  join_one: vault is ${HIDDEN} on vault.id = vault_id
  join_one: shipping is ok_country on shipping.id = c_id
}
`;

async function indexWithHiddenJoinTarget() {
   const { pkg } = await compileJoinFixture("m.malloy", {
      modelText: MODEL_TEXT,
      apiSources: [{ name: HIDDEN, authorize: ["false"] }, { name: "orders" }],
   });
   return getPackageIndex(storeServing(pkg), "env", "pkg");
}

describe("a deny-all source joined under an alias", () => {
   it("is dropped from the index, and its join keeps the alias but loses the target name", async () => {
      const index = await indexWithHiddenJoinTarget();
      expect(index.directEntities.some((e) => e.name === HIDDEN)).toBe(false);
      const vault = index.directEntities.find(
         (e) => e.kind === "join" && e.name === "vault",
      );
      expect(vault).toBeDefined();
      expect(vault?.joinTarget).toBeUndefined();
      // A join to a visible source still names its target.
      const shipping = index.directEntities.find(
         (e) => e.kind === "join" && e.name === "shipping",
      );
      expect(shipping?.joinTarget).toBe("ok_country");
   });

   it("is not named in the source summary prompt", async () => {
      const index = await indexWithHiddenJoinTarget();
      const inputs = buildSourceSummaryInputs(index.directEntities);
      const orders = inputs.find((i) => i.source === "orders");
      expect(orders).toBeDefined();
      expect(orders?.prompt).not.toContain(HIDDEN);
      // The visible target and both aliases are still there.
      expect(orders?.prompt).toContain("source ok_country");
      expect(orders?.prompt).toContain("vault");
      expect(orders?.prompt).toContain("shipping");
   });
});

describe("a source the package does not export, joined under an alias", () => {
   // Not a deny-all source: it has no gate at all. It is simply not listed, as
   // a source outside a package's exports is not. Its name is no more the
   // model's to see than a gated one's.
   const UNLISTED = "raw_ledger_tbl";
   const MODEL = `
source: ${UNLISTED} is duckdb.sql("select 1 as id, 9 as amount") extend {
  dimension: amt is amount
}

source: ok_country is duckdb.sql("select 1 as id, 'US' as code") extend {
  dimension: code_name is code
}

source: orders is duckdb.sql("select 1 as id, 1 as ledger_id, 1 as c_id") extend {
  dimension: status is 's'
  join_one: ledger is ${UNLISTED} on ledger.id = ledger_id
  join_one: shipping is ok_country on shipping.id = c_id
}
`;

   async function index() {
      const { pkg } = await compileJoinFixture("m.malloy", {
         modelText: MODEL,
         hiddenSources: [UNLISTED],
      });
      return getPackageIndex(storeServing(pkg), "env", "pkg");
   }

   it("is not named by the join entity or the summary prompt", async () => {
      const idx = await index();
      expect(idx.directEntities.some((e) => e.name === UNLISTED)).toBe(false);
      const ledger = idx.directEntities.find(
         (e) => e.kind === "join" && e.name === "ledger",
      );
      expect(ledger).toBeDefined();
      expect(ledger?.joinTarget).toBeUndefined();

      const inputs = buildSourceSummaryInputs(idx.directEntities);
      expect(inputs.find((i) => i.source === "orders")?.prompt).not.toContain(
         UNLISTED,
      );
   });
});
