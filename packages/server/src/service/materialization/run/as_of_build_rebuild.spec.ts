// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A table stored as of its build (a v1 `#@ persist` whose build reads the
 * current time) is rebuilt on every run: a run without `forceRefresh` that
 * finds its table unchanged since the last run still builds it, while a
 * neighbour that reads no clock (or reads it only in its own extend block)
 * is carried forward as before. Compiled from a real package, so the plan's
 * own `asOfBuild` decides it.
 */

import { afterEach, describe, expect, it } from "bun:test";
import { tempBuildPlanPackages } from "../../../test_helpers/incremental_test_harness";
import type { ManifestEntry } from "../domain/types";
import { deriveSelfInstructions } from "./run_instructions";
import { compileRunPlan } from "./run_plan";

const packages = tempBuildPlanPackages();
afterEach(async () => {
   await packages.cleanup();
});

const MODEL = `##! experimental.persistence
source: ev is duckdb.sql("SELECT 1 AS id, TIMESTAMP '2001-01-01' AS ts")

#@ persist name="last7"
source: last7 is ev -> { where: ts ~ f'last 7 days'; select: * }

#@ persist name="past"
source: past is ev -> { where: ts < now; select: * }

#@ persist name="own"
source: own is ev -> { select: * } extend { where: ts < now }

#@ persist name="plain"
source: plain is ev -> { select: * }
`;

describe("a run over a table stored as of its build", () => {
   it("rebuilds it on every run and carries the rest forward", async () => {
      const pkg = await packages.create({ "m.malloy": MODEL });
      const { runPlan: compiled } = await compileRunPlan(pkg);
      const byName = Object.fromEntries(
         Object.entries(compiled.tables).map(([sourceID, t]) => [
            t.source.name,
            sourceID,
         ]),
      );
      expect(Object.keys(compiled.asOfBuild ?? {}).sort()).toEqual(
         [byName.last7, byName.past].sort(),
      );
      // The first run: everything builds.
      const first = deriveSelfInstructions(compiled, undefined, {});
      expect(first.instructions.length).toBe(4);
      const prior: Record<string, ManifestEntry> = Object.fromEntries(
         first.instructions.map((i) => [
            i.sourceEntityId,
            {
               sourceEntityId: i.sourceEntityId,
               physicalTableName: i.physicalTableName,
               connectionName: "duckdb",
            } as ManifestEntry,
         ]),
      );
      // The next run, nothing changed: only the as-of-build tables rebuild.
      const next = deriveSelfInstructions(compiled, undefined, prior);
      const rebuilt = next.instructions.map((i) => i.physicalTableName).sort();
      const carried = Object.values(next.carried)
         .map((e) => e.physicalTableName)
         .sort();
      expect(rebuilt).toEqual(["last7", "past"]);
      expect(carried).toEqual(["own", "plain"]);
   });
});
