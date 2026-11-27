// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { materializationFixture } from "../../../test_helpers/materialization_fixture";
import { testFileURL } from "../../../test_helpers/test_file_url";
import { generateRollupBuildFile } from "../text/read_text";
import { SourceMap } from "../text/source_map";
import { grainColumnOf, grainPathOf } from "../domain/rollup_grain";
import { planModelRollups } from "./rollup_plan";

/**
 * #2048: a `#(rollup)` grain that names a one-hop join path, as planned:
 * what is accepted, how it is stored, which grain dimension is exactly a
 * path (matched on the compiled definition), and each refusal's message.
 */

const mz = materializationFixture();
const ROOT = testFileURL("pkg/");

const ACCOUNTS = `source: accounts is duckdb.sql("""select * from (values (1,'EU','SE',TIMESTAMP '2024-01-01 08:00:00')) t(account_id, region, country, created_at)""") extend {
  primary_key: account_id
  dimension: created_month is created_at.month
  internal dimension: secret is country
  join_one: owner is owners with account_id
}`;
const OWNERS = `source: owners is duckdb.sql("""select 1 as account_id, 'Ann' as name""") extend { primary_key: account_id }`;
const ITEMS = `source: items is duckdb.sql("""select 1 as item_id, 10 as sub_id, 'A' as sku""") extend { primary_key: item_id }`;

function model(subsBody: string): string {
   return `${OWNERS}
${ACCOUNTS}
${ITEMS}
source: subs is duckdb.sql("""select * from (values (10,1,DATE '2026-01-01',100.0)) t(sub_id, account_id, period, amount)""") extend {
  primary_key: sub_id
  join_one: account is accounts with account_id
  join_many: items on items.sub_id = sub_id
  join_cross: everyone is owners on true
  internal join_one: hidden_account is accounts with account_id
${subsBody}
}`;
}

async function plan(subsBody: string) {
   const modelDef = await mz.compile(
      { "m.malloy": model(subsBody) },
      { root: ROOT },
   );
   return planModelRollups(SourceMap.of(modelDef), {
      packageRelativeSourceID: (id) => id,
   });
}

describe("grain entries and their stored columns", () => {
   it("stores a path under the inline= naming and a dimension under its name", () => {
      expect(grainPathOf("account.region")).toEqual({
         join: "account",
         field: "region",
      });
      expect(grainPathOf("region")).toBeUndefined();
      expect(grainColumnOf("account.region")).toBe("account__region");
      expect(grainColumnOf("region")).toBe("region");
   });
});

describe("#(rollup) path grains (#2048)", () => {
   it("plans a path through a join_one, stored as j__f, and writes it in the build", async () => {
      const { plans, refused } =
         await plan(`  dimension: period_month is period.month
  #(rollup) grain=["account.region", "period_month"]
  measure: mrr is amount.sum()`);
      expect(refused).toEqual([]);
      const [p] = plans;
      expect(p?.grainDimensions).toEqual(["account.region", "period_month"]);
      expect(p?.pathColumns).toEqual([
         {
            grain: "account.region",
            column: "account__region",
            join: "account",
            field: "region",
         },
      ]);
      expect(p?.fanOutDimensions).toEqual([]);
      expect(p?.rollupSourceName).toStartWith(
         "subs__rollup__account_region_period_month__",
      );
      const build = generateRollupBuildFile(plans, "m.malloy") ?? "";
      expect(build).toContain("    account__region is account.region\n");
      expect(build).toContain("    period_month\n");
   });

   it("records a path through a join_many as a fan-out entry, and a joined truncation's timeframe", async () => {
      const { plans, refused } =
         await plan(`  #(rollup) grain=["items.sku", "account.created_month"]
  measure: mrr is amount.sum()`);
      expect(refused).toEqual([]);
      expect(plans[0]?.fanOutDimensions).toEqual(["items.sku"]);
      expect(plans[0]?.pathColumns).toContainEqual({
         grain: "account.created_month",
         column: "account__created_month",
         join: "account",
         field: "created_month",
         timeframe: "month",
      });
   });

   it("records a grain dimension that is exactly a path, never one over it", async () => {
      const { plans, refused } = await plan(`  dimension:
    region is account.region
    up_region is upper(account.region)
  #(rollup) grain=["region", "up_region"]
  measure: mrr is amount.sum()`);
      expect(refused).toEqual([]);
      expect(plans[0]?.pathColumns).toEqual([
         {
            grain: "region",
            column: "region",
            join: "account",
            field: "region",
         },
      ]);
   });

   for (const [label, grain, message] of [
      [
         "a multi-hop path",
         "account.owner.name",
         "the grain names `account.owner.name`, a path through more than one join; a grain reads one join, so declare a dimension on `account`'s source that reads the rest, and name `account.<that dimension>`",
      ],
      [
         "a truncation through a path",
         "account.created_at.month",
         "the grain names `account.created_at.month`, a truncation through a join; declare the truncated dimension on `account`'s source, and name `account.<that dimension>`",
      ],
      [
         "a path through no join",
         "period.month",
         "the grain names `period.month`, which reads no join of the source; a truncation or a field of a record is named through a dimension on the source that reads it",
      ],
      [
         "a path through a join_cross",
         "everyone.name",
         "the grain names `everyone.name`, through `everyone`, a join_cross; a path grain reads a join_one or a join_many",
      ],
      [
         "a path to a field the joined source does not have",
         "account.nope",
         "the grain names `account.nope`, but `nope` is not a dimension of `account`'s source",
      ],
      [
         "a path to a hidden field",
         "account.secret",
         "the grain names `account.secret`, which is not public (`secret` on its source is not), so a rollup's build cannot read it",
      ],
      [
         "a path through a hidden join",
         "hidden_account.region",
         "the grain names `hidden_account.region`, which is not public (the join `hidden_account` is not), so a rollup's build cannot read it",
      ],
   ] as const) {
      it(`refuses ${label}, rollup-unknown-grain, with its own message`, async () => {
         const { plans, refused } = await plan(`  #(rollup) grain=["${grain}"]
  measure: mrr is amount.sum()`);
         expect(plans).toEqual([]);
         expect(refused.map((r) => [r.tier, r.code, r.message])).toEqual([
            [
               "malformed",
               "rollup-unknown-grain",
               `#(rollup) on measure "mrr" of source "subs": ${message}`,
            ],
         ]);
      });
   }

   it("refuses a path whose stored column is a field the source already has", async () => {
      const { refused } = await plan(`  dimension: account__region is 'x'
  #(rollup) grain=["account.region"]
  measure: mrr is amount.sum()`);
      expect(refused.map((r) => [r.code, r.message])).toEqual([
         [
            "rollup-unknown-grain",
            '#(rollup) on measure "mrr" of source "subs": the grain names `account.region`, stored as `account__region`, which is already the name of a field of the source',
         ],
      ]);
   });
});

describe("#(rollup) path grains through a gated joined source (#2048)", () => {
   it("is not built when the base is gated and the joined source is too", async () => {
      const modelDef = await mz.compile(
         {
            "m.malloy": `##! experimental { givens }
given: REGION :: string
#(access_filter) region = $REGION
source: accounts is duckdb.sql("""select 1 as account_id, 'EU' as region""") extend { primary_key: account_id }
#(access_filter) sub_id > 0
source: subs is duckdb.sql("""select 10 as sub_id, 1 as account_id, 5.0 as amount""") extend {
  primary_key: sub_id
  join_one: account is accounts with account_id
  #(rollup) grain=["account.region", "sub_id"]
  measure: mrr is amount.sum()
}`,
         },
         { root: ROOT },
      );
      const { plans, refused } = planModelRollups(SourceMap.of(modelDef), {
         packageRelativeSourceID: (id) => id,
         readUnderGates: true,
      });
      expect(plans).toEqual([]);
      expect(
         refused.map((r) => [
            r.tier,
            "refused" in r ? r.refused.reason : undefined,
         ]),
      ).toEqual([["unservable", "access_filter"]]);
      expect(refused[0]?.message).toContain(
         "a source it joins has a row-level gate",
      );
   });
});
