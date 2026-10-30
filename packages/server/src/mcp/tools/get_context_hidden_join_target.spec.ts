// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A source with an unconditional deny-all gate is dropped from the index, and
 * its name must not reach a model through another source that joins it. The
 * join is written under an alias, so the visible source's own field list names
 * the alias; the TARGET's real name is what has to stay out of the source
 * summary prompt and the source-match description.
 */

import { describe, expect, it } from "bun:test";
import {
   compileJoinFixture,
   storeServing,
} from "../../test_helpers/get_context_join_fixture";
import { buildSourceSummaryInputs } from "./source_summaries";
import { sourceDescription } from "./get_context_source_match";
import { getPackageIndex } from "./package_entities";
import type { PipelineContext } from "./get_context_pipeline";

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

   it("is not named in the source-match description, even if the topology reaches it", async () => {
      const index = await indexWithHiddenJoinTarget();
      const orders = index.directEntities.find(
         (e) => e.kind === "source" && e.name === "orders",
      )!;
      const ctx = { pkgIndex: index } as unknown as PipelineContext;
      expect(sourceDescription(ctx, orders)).not.toContain(HIDDEN);
      expect(sourceDescription(ctx, orders)).toContain("ok_country");

      // The topology is built before the drop, so a path to the hidden source
      // can still be there; the description must filter it by itself.
      const withReach = {
         pkgIndex: {
            ...index,
            topology: new Map([
               [
                  `m.malloy\u0000orders`,
                  [
                     {
                        targetSource: HIDDEN,
                        targetModelPath: "m.malloy",
                        path: ["vault"],
                        fanout: "one",
                     },
                     {
                        targetSource: "ok_country",
                        targetModelPath: "m.malloy",
                        path: ["shipping"],
                     },
                  ],
               ],
            ]),
         },
      } as unknown as PipelineContext;
      // The hidden target is named with its model path, so only the index
      // check can remove it; the visible target, named the same way, stays.
      expect(sourceDescription(withReach, orders)).not.toContain(HIDDEN);
      expect(sourceDescription(withReach, orders)).toContain("ok_country");
   });

   it("is not sent when the topology reaches a source that is not in the index", async () => {
      // The same belt-and-braces check for a name the topology records but no
      // source entity backs, whatever the reason the source is not indexed.
      const idx = await indexWithHiddenJoinTarget();
      const orders = idx.directEntities.find(
         (e) => e.kind === "source" && e.name === "orders",
      )!;
      const withReach = {
         pkgIndex: {
            ...idx,
            topology: new Map([
               [
                  `m.malloy\u0000orders`,
                  [
                     {
                        targetSource: "not_in_the_index",
                        targetModelPath: "m.malloy",
                        path: ["vault"],
                        fanout: "one",
                     },
                  ],
               ],
            ]),
         },
      } as unknown as PipelineContext;
      expect(sourceDescription(withReach, orders)).not.toContain(
         "not_in_the_index",
      );
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

   it("is not named by the join entity, the summary prompt or the match description", async () => {
      const idx = await index();
      expect(idx.directEntities.some((e) => e.name === UNLISTED)).toBe(false);
      const ledger = idx.directEntities.find(
         (e) => e.kind === "join" && e.name === "ledger",
      );
      expect(ledger).toBeDefined();
      expect(ledger?.joinTarget).toBeUndefined();

      const orders = idx.directEntities.find(
         (e) => e.kind === "source" && e.name === "orders",
      )!;
      const inputs = buildSourceSummaryInputs(idx.directEntities);
      expect(inputs.find((i) => i.source === "orders")?.prompt).not.toContain(
         UNLISTED,
      );
      const ctx = { pkgIndex: idx } as unknown as PipelineContext;
      expect(sourceDescription(ctx, orders)).not.toContain(UNLISTED);
      expect(sourceDescription(ctx, orders)).toContain("ok_country");
   });

   it("is not named by the description even if the topology reaches it", async () => {
      const idx = await index();
      const orders = idx.directEntities.find(
         (e) => e.kind === "source" && e.name === "orders",
      )!;
      const withReach = {
         pkgIndex: {
            ...idx,
            topology: new Map([
               [
                  `m.malloy\u0000orders`,
                  [
                     {
                        targetSource: UNLISTED,
                        targetModelPath: "m.malloy",
                        path: ["ledger"],
                        fanout: "one",
                     },
                     {
                        targetSource: "ok_country",
                        targetModelPath: "m.malloy",
                        path: ["shipping"],
                        fanout: "one",
                     },
                  ],
               ],
            ]),
         },
      } as unknown as PipelineContext;
      expect(sourceDescription(withReach, orders)).not.toContain(UNLISTED);
      expect(sourceDescription(withReach, orders)).toContain("ok_country");
   });
});

const VAULT_SQL = "select 1 as id, 9 as salary";
const COUNTRY_SQL = "select 1 as id, 'US' as code";
const SECRETS = [
   "Payroll vault",
   "Employee salaries",
   "Gross salary",
   "Salaries",
] as const;

const DOCUMENTED_MODEL_TEXT = `
#(doc) Payroll vault.
source: ${HIDDEN} is duckdb.sql("${VAULT_SQL}") extend {
  #(doc) Salaries.
  dimension: pay is salary
}

source: ok_country is duckdb.sql("${COUNTRY_SQL}") extend {
  join_one: inner_vault is ${HIDDEN} on inner_vault.id = id
}

source: orders is duckdb.sql("select 1 as id, 1 as vault_id, 1 as c_id") extend {
  join_one: vault is ${HIDDEN} on vault.id = vault_id
  #(doc) Payroll lookup.
  join_one: vault_own is ${HIDDEN} on vault_own.id = vault_id
  join_one: shipping is ok_country on shipping.id = c_id
}
`;

async function indexWithDocumentedHiddenTarget() {
   const { pkg } = await compileJoinFixture("m.malloy", {
      modelText: DOCUMENTED_MODEL_TEXT,
      apiSources: [
         { name: HIDDEN, authorize: ["false"] },
         { name: "ok_country" },
         { name: "orders" },
      ],
      dbmeta: {
         [VAULT_SQL]: {
            table: "Employee salaries, one row per pay period.",
            columns: { salary: "Gross salary." },
         },
         [COUNTRY_SQL]: { table: "Country codes." },
      },
   });
   return getPackageIndex(storeServing(pkg), "env", "pkg");
}

describe("a deny-all source with a #(doc) and database descriptions, joined", () => {
   it("documents no join to it, and no field reached through it", async () => {
      const index = await indexWithDocumentedHiddenTarget();
      const entities = [...index.byId.values()];
      for (const e of entities) {
         for (const secret of SECRETS) {
            expect(`${e.name}: ${e.doc} ${e.embedDoc}`).not.toContain(secret);
         }
      }
      const vault = entities.find(
         (e) => e.kind === "join" && e.name === "vault",
      );
      expect(vault).toMatchObject({ doc: "", embedDoc: "" });
      expect(vault?.docSource).toBeUndefined();
      expect(entities.find((e) => e.name === "vault.pay")?.embedDoc).toBe("");
      expect(
         entities.find((e) => e.name === "shipping.inner_vault.pay"),
      ).toMatchObject({ doc: "", embedDoc: "" });
   });

   it("keeps the doc a join writes itself", async () => {
      const index = await indexWithDocumentedHiddenTarget();
      const own = [...index.byId.values()].find(
         (e) => e.kind === "join" && e.name === "vault_own",
      );
      expect(own).toMatchObject({
         doc: "Payroll lookup.",
         embedDoc: "Payroll lookup.",
      });
   });

   it("still documents a join to a visible source from the database", async () => {
      const index = await indexWithDocumentedHiddenTarget();
      const shipping = [...index.byId.values()].find(
         (e) => e.kind === "join" && e.name === "shipping",
      );
      expect(shipping).toMatchObject({
         doc: "Country codes.",
         docSource: "dbmeta",
      });
   });

   it("is not described in the summary or source-match prompts", async () => {
      const index = await indexWithDocumentedHiddenTarget();
      const inputs = buildSourceSummaryInputs(index.directEntities);
      const ctx = { pkgIndex: index } as unknown as PipelineContext;
      for (const name of ["orders", "ok_country"]) {
         const prompt = inputs.find((i) => i.source === name)?.prompt;
         expect(prompt).toBeDefined();
         const source = index.directEntities.find(
            (e) => e.kind === "source" && e.name === name,
         )!;
         for (const secret of SECRETS) {
            expect(prompt).not.toContain(secret);
            expect(sourceDescription(ctx, source)).not.toContain(secret);
         }
      }
   });
});
