// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// What a build target's SQL is generated under, against the deprecated
// `getSQL()` it replaced, for a source that reads a given.
//
// `Runtime.getBuildTargets()` binds the runtime's own givens (its config file's
// `givensPath` and its constructor `givens:`) and the runtime's `virtualMap`.
// The no-argument `getSQL()` bound each given's declared default and no
// `virtualMap`. The two therefore agree exactly when the runtime carries
// neither, and a source that reads a given moves address on a runtime that
// does.
//
// Publisher's runtimes carry neither: the environment, package and load-worker
// `MalloyConfig`s are built from `connections` alone (no `givensPath`), no
// runtime is constructed with `givens:`, and only the chained build passes a
// `virtualMap`, which it passed to `getSQL` before too. So no address moved.
// This file pins both halves, so the day a runtime gains givens the address
// question (Q13) is decided on purpose rather than by accident.
import {
   InMemoryURLReader,
   MalloyConfig,
   Runtime,
   type FixedConnectionMap,
} from "@malloydata/malloy";
import type { DuckDBConnection } from "@malloydata/db-duckdb";
import { beforeAll, describe, expect, it } from "bun:test";
import {
   addressSQL,
   buildTargetOf,
   plannedSources,
} from "./materialization/plan/build_plan";
import { duckdbTestConnections } from "../test_helpers/incremental_test_harness";
import { assembleEnvironmentConnections } from "./connection_config";

const MODEL = `##! experimental { persistence givens }
given: REGION :: string is 'A'

source: base is duckdb.sql("""SELECT * FROM (VALUES (10, 'A'), (20, 'B')) AS t(amount, category)""")

#@ persist
source: by_region is base -> {
  where: category = $REGION
  aggregate: total is amount.sum()
}
`;

let connections: FixedConnectionMap;
let duckdb: DuckDBConnection;

beforeAll(() => {
   ({ duckdb, connections } = duckdbTestConnections());
   void duckdb;
});

async function targetAndLegacySQL(givens?: Record<string, string>) {
   const url = "file:///given_binding.malloy";
   const runtime = new Runtime({
      urlReader: new InMemoryURLReader(new Map([[url, MODEL]])),
      connections,
      ...(givens ? { givens } : {}),
   });
   const model = await runtime.loadModel(new URL(url)).getModel();
   const [source] = plannedSources(await runtime.getBuildTargets(model));
   return { target: addressSQL(source), legacy: source.getSQL() };
}

describe("build target SQL against the deprecated getSQL()", () => {
   it("is identical on a runtime with no givens of its own (Publisher's case)", async () => {
      const { target, legacy } = await targetAndLegacySQL();
      expect(target).toBe(legacy);
      expect(target).toContain("'A'");
   });

   it("differs on a runtime that supplies a given the source reads", async () => {
      const { target, legacy } = await targetAndLegacySQL({ REGION: "B" });
      // getSQL() kept the declared default; the target took the runtime's.
      expect(legacy).toContain("'A'");
      expect(target).toContain("'B'");
      expect(target).not.toBe(legacy);
   });
});

describe("Publisher's runtimes carry no givens of their own", () => {
   it("builds the environment MalloyConfig from connections alone", () => {
      const assembled = assembleEnvironmentConnections([]);
      expect(Object.keys(assembled.pojo)).toEqual(["connections"]);
      const config = new MalloyConfig(assembled.pojo);
      expect(config.givensURL).toBeUndefined();
   });
});

describe("a source the target walk cannot key", () => {
   it("says it cannot be keyed, with the compiler's reason", async () => {
      const url = "file:///unkeyable.malloy";
      const model = MODEL.replace(
         "given: REGION :: string is 'A'",
         "given: REGION :: string",
      );
      const runtime = new Runtime({
         urlReader: new InMemoryURLReader(new Map([[url, model]])),
         connections,
      });
      const compiled = await runtime.loadModel(new URL(url)).getModel();
      const targets = await runtime.getBuildTargets(compiled);
      const [source] = plannedSources(targets);
      expect(targets.unbuildable.map((u) => u.source)).toContain(source);
      expect(() => buildTargetOf(source)).toThrow(/cannot be keyed/);
   });
});
