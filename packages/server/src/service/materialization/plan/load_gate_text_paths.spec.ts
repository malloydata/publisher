// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The load gate's text on each path that reads it: the load itself (through
 * `Package`), and the plan a run guard, a re-derivation and a package
 * compile derive from the files (`loadGateText`). Pinned on the two inputs
 * where they are known to part: a persisted view's invalid incremental
 * declaration, and a `name=` only the raw text reads.
 */

import {
   afterAll,
   afterEach,
   beforeEach,
   describe,
   expect,
   it,
} from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { installSettings } from "../../../test_helpers/settings_env";
import {
   usePackageLoadPool,
   writePackageFiles,
} from "../../../test_helpers/temp_package";
import { tempBuildPlanPackages } from "../../../test_helpers/incremental_test_harness";
import { Environment } from "../../environment";
import { computePackageBuildPlan } from "./build_plan_compile";
import { loadGateText } from "./load_gate_text";

const HEADER = `##! experimental { persistence composite_sources }\n`;
const ROWS = `duckdb.sql("""SELECT * FROM (VALUES (1, 'A', 10), (2, 'B', 20)) AS t(id, category, amount)""")`;

/** A persisted view whose incremental declaration is invalid: no watermark=. */
const VIEW_INCREMENTAL = `${HEADER}source: orders is ${ROWS} extend {
  measure: total is amount.sum()
  #@ persist refresh="incremental"
  view: by_category is { group_by: category; aggregate: total }
}
`;

/** A `#@ persist` whose `name=` is unsafe, which the raw text reads. */
const UNSAFE_NAME = `${HEADER}source: raw is ${ROWS}
#@ persist name="t; drop"
source: daily is raw -> { group_by: category }
`;

/** A `#@ persist` whose `name=` is not quoted, which only the raw text reads. */
const UNQUOTED_NAME = `${HEADER}source: raw is ${ROWS}
#@ persist name=t
source: daily is raw -> { group_by: category }
`;

let root: string;

beforeEach(() => {
   root = fs.realpathSync(
      fs.mkdtempSync(path.join(os.tmpdir(), "gate-text-paths-")),
   );
});

afterEach(() => {
   fs.rmSync(root, { recursive: true, force: true });
});

/** The text a load of `model` fails with. */
async function loadText(model: string): Promise<string> {
   const envPath = path.join(root, "env");
   writePackageFiles(
      path.join(envPath, "pkg"),
      { "model.malloy": model },
      { manifest: { name: "pkg", description: "fixture" } },
   );
   const env = await Environment.create("testEnv", envPath, []);
   try {
      await env.addPackage("pkg");
   } catch (error) {
      return (error as Error).message;
   }
   throw new Error("expected the load to fail");
}

/** The text a plan derived from `model`'s files reads. */
async function planText(model: string): Promise<string> {
   const packages = tempBuildPlanPackages();
   try {
      const outcome = await computePackageBuildPlan(
         await packages.create({ "model.malloy": model }),
      );
      return loadGateText(outcome, {
         enforceCollisions: true,
         versionScoped: false,
      });
   } finally {
      await packages.cleanup();
   }
}

describe("the load gate's text on the load and on a derived plan", () => {
   const settings = installSettings({ env: { PUBLISHER_MATERIALIZE: "true" } });
   usePackageLoadPool();
   afterAll(() => settings.restore());

   it("CURRENT: a persisted view's invalid incremental line names the view on the load, the generated source on a derived plan", async () => {
      const load = await loadText(VIEW_INCREMENTAL);
      const plan = await planText(VIEW_INCREMENTAL);
      expect(load).toContain("[persist-view-invalid]");
      expect(load).toContain("by_category");
      expect(load).not.toContain("orders__by_category__view");
      expect(plan).toContain("[persist-invalid]");
      expect(plan).toContain("orders__by_category__view");
      expect(plan).not.toBe(load);
   }, 60000);

   it("CURRENT: an unsafe name= reads as the raw text's line on the load, and the plan's own line on a derived plan", async () => {
      const load = await loadText(UNSAFE_NAME);
      const plan = await planText(UNSAFE_NAME);
      expect(load.split("\n")).toHaveLength(1);
      expect(load).toContain('Offending annotation: #@ persist name="t; drop"');
      expect(plan.split("\n")).toHaveLength(1);
      expect(plan).toContain(
         'its name= "t; drop" is not a plain identifier path',
      );
      expect(plan).not.toBe(load);
   }, 60000);

   it("CURRENT: an unquoted name= is on the load's text alone", async () => {
      const load = await loadText(UNQUOTED_NAME);
      const plan = await planText(UNQUOTED_NAME);
      expect(load).toContain("[persist-invalid]");
      expect(plan).toBe("");
   }, 60000);
});
