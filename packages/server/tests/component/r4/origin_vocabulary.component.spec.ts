// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * One mechanism word per table, everywhere a table is reported: the build
 * plan's `origin`, a refusal record's `origin`, a storage serve binding's
 * `origin` and the path report's. Each is the annotation that declared the
 * table: `persist`, `materialize`, `view`, `query` or `preaggregate`. Before,
 * every table that was not a rollup said `persist` on the plan and the
 * binding, a refusal record said nothing, and the path report's
 * `materialized` kind could not tell `#@ persist` from `#(materialize)`.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { DATA_FILES, RAW_MODEL } from "./conformance/data";
import { unrecordedAudit } from "../../../src/service/access_record";
import { QueryPrivileges } from "../../../src/service/model_query_request";
import {
   freshLoadPool,
   type LoadedFixture,
   loadFixture,
   withSwitch,
} from "../../../src/test_helpers/conformance/package_runner";

const MODEL = `##! experimental { persistence givens access_modifiers }
import "raw.malloy"
given: ROLE :: string

#@ persist name="p_db"
source: p_db is orders_raw extend {}

#@ persist name="p_lake" storage="lake"
source: p_lake is orders_raw extend {}

#(materialize) storage="lake"
source: m_lake is orders_raw extend {
  measure: n is count()
}

source: v_base is orders_raw extend {
  #@ persist storage="lake"
  view: by_region is { group_by: region; aggregate: n is count() }
}

#@ persist storage="lake"
query: q_lake is orders_raw -> { group_by: status; aggregate: n is count() }

source: rolled is orders_raw extend {
  #(rollup) grain=["region"] storage="lake"
  measure: total is amount.sum()
  #(rollup) grain=["region"]
  private measure: secret is amount.sum()
}

// Refused: a stored table holds one given value's rows.
#@ persist name="p_given" storage="lake"
source: p_given is orders_raw -> { where: status = $ROLE; select: * }

#(materialize) storage="lake"
source: m_given is orders_raw -> { where: status = $ROLE; select: * }

source: v_given is orders_raw extend {
  #@ persist storage="lake"
  view: by_role is { where: status = $ROLE; group_by: region }
}
`;

describe("the origin of every reported table", () => {
   let restore: () => void = () => {};
   let fixture: LoadedFixture | undefined;
   let dispose: () => Promise<void> = async () => {};

   beforeAll(async () => {
      restore = withSwitch("cutover");
      await freshLoadPool();
      const loader = await loadFixture({
         ...DATA_FILES,
         "raw.malloy": RAW_MODEL,
         "m.malloy": MODEL,
      });
      dispose = loader.dispose;
      fixture = await loader.bind({ includeRollups: true });
   });

   afterAll(async () => {
      await dispose();
      restore();
      await freshLoadPool();
   });

   it("names each plan entry by its mechanism", () => {
      const plan = fixture!.pkg.getBuildPlan();
      const byName = Object.fromEntries(
         Object.values(plan?.sources ?? {}).map((s) => [s.name, s.origin]),
      );
      expect(byName).toMatchObject({
         p_db: "persist",
         p_lake: "persist",
         m_lake: "materialize",
         v_base__by_region__view: "view",
         q_lake__query: "query",
      });
      const rollups = Object.values(plan?.sources ?? {}).filter(
         (s) => s.preaggregate,
      );
      expect(rollups.length).toBeGreaterThan(0);
      for (const r of rollups) expect(r.origin).toBe("rollup");
   });

   it("names each refusal record by its mechanism, a rollup's by its own tier", () => {
      const refused = Object.values(
         fixture!.pkg.getBuildPlan()?.refusedSources ?? {},
      );
      const of = (pred: (r: (typeof refused)[number]) => boolean) =>
         refused.find(pred);
      expect(of((r) => r.name === "p_given")).toMatchObject({
         origin: "persist",
         tier: "storage",
      });
      expect(of((r) => r.name === "m_given")).toMatchObject({
         origin: "materialize",
         tier: "storage",
      });
      expect(of((r) => r.name === "v_given__by_role__view")).toMatchObject({
         origin: "view",
         tier: "storage",
      });
      expect(of((r) => r.reason === "hidden_field")).toMatchObject({
         origin: "rollup",
         tier: "database",
      });
      for (const r of refused) expect(r.origin).toBeDefined();
   });

   it("names each storage serve binding by its mechanism", () => {
      const bindings = fixture!.pkg
         .materialization()
         .bindingState().storageServeBindings;
      const origins = new Set(bindings.map((b) => b.origin));
      expect(origins).toContain("persist");
      expect(origins).toContain("materialize");
      expect(origins).toContain("rollup");
   });

   it("names the answering table's mechanism in the path report", async () => {
      const cases: [string, string, string][] = [
         ["run: p_lake -> { group_by: region }", "materialized", "persist"],
         ["run: m_lake -> { aggregate: n }", "materialized", "materialize"],
         ["run: v_base -> by_region", "view", "view"],
         ["run: q_lake", "query", "query"],
         [
            "run: rolled -> { group_by: region; aggregate: total }",
            "rollup",
            "rollup",
         ],
      ];
      const model = fixture!.pkg.getModel("m.malloy")!;
      for (const [query, kind, origin] of cases) {
         const result = await model.runQuery(
            unrecordedAudit(),
            { query } as never,
            QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
         );
         expect([query, result.path?.servedBy]).toMatchObject([
            query,
            { kind, origin },
         ]);
      }
   });
});
