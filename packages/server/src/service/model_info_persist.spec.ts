// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { ModelDef } from "@malloydata/malloy";
import { describe, expect, it } from "bun:test";
import {
   duckdbTestConnections,
   loadTestRuntime,
} from "./incremental_test_harness";
import { modelInfoOf } from "./model_info";

// `ORG` has no default and a query bakes it, so reading the model's schemas
// takes the placeholder retry; `FLOOR` has one and the persisted source bakes it.
const MODEL = `##! experimental.persistence
##! experimental.givens
given: ORG :: number
given: FLOOR :: number is 1

source: base is duckdb.sql("select 1 as id, 2 as org_id, 5 as amount")

#@ persist
source: rollup is base -> {
  where: amount >= $FLOOR
  group_by: org_id
  aggregate: total is amount.sum()
}

query: for_org is base -> { where: org_id = $ORG; aggregate: n is count() }
`;

describe("modelInfoOf leaves what persistence builds from untouched", () => {
   it("keeps the persisted source's SQL and BuildID", async () => {
      const { connections } = duckdbTestConnections();
      const { runtime, materializer } = loadTestRuntime(connections, MODEL);
      const model = await materializer.getModel();
      const targets = async () =>
         (await runtime.getBuildTargets(model)).connections.flatMap((c) =>
            c.targets.map((t) => ({ sql: t.sql, buildId: t.buildId })),
         );

      const before = await targets();
      expect(before).toHaveLength(1);

      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const modelDef = (model as any)._modelDef as ModelDef;
      modelInfoOf(modelDef);

      expect(await targets()).toEqual(before);
      expect(before[0].sql).not.toContain("$");
      const orgGiven = Object.values(modelDef.givens ?? {}).find(
         (g) => g.name === "ORG",
      );
      expect(orgGiven?.default).toBeUndefined();
   });
});
