// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A refused serve binding is warned once per plan, by its code, then logged
 * at debug: a status poll rebinds an unchanged manifest, which declines the
 * same entries again. A new plan (a reload, a refresh) warns again.
 */

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as sinon from "sinon";
import { logger } from "../../logger";
import {
   makeBuildPlan,
   makePlanOutcome,
   okWorkerPlan,
} from "../../test_helpers/materialization_test_fixtures";
import type { FreshnessManifest } from "./domain/types";
import { PackageMaterialization } from "./package_materialization";

const MESSAGE =
   "Refusing a database serve binding: the source is not eligible to be served from a materialized table";

/** A database entry the plan never examined, so the bind declines it. */
const stranger = (): FreshnessManifest =>
   ({
      "not-in-the-plan": {
         tableName: "public.secret_host_table",
         connectionName: "duckdb",
      },
   }) as unknown as FreshnessManifest;

function facade(): PackageMaterialization {
   return new PackageMaterialization({
      packageName: () => "pkg",
      versionId: () => undefined,
      metadata: () => ({}) as never,
      models: () => new Map(),
      modelPolicy: () => ({ setServingResolvers: () => {} }) as never,
      packagePath: () => "/pkg",
      malloyConfig: () => ({}) as never,
      connection: async () => ({}) as never,
      buildPlanPackage: {} as never,
      publishedSurface: () => undefined,
   });
}

const loadPlan = (m: PackageMaterialization) =>
   m.applyLoadBuildPlan(
      okWorkerPlan(makePlanOutcome({ plan: makeBuildPlan() })),
   );

describe("a refused serve binding's log", () => {
   let warn: sinon.SinonStub;
   let debug: sinon.SinonStub;
   beforeEach(() => {
      warn = sinon.stub(logger, "warn");
      debug = sinon.stub(logger, "debug");
   });
   afterEach(() => {
      warn.restore();
      debug.restore();
   });
   const declines = (s: sinon.SinonStub) =>
      s.getCalls().filter((c) => c.args[0] === MESSAGE);

   it("warns once per plan, by its code, then logs at debug; a new plan warns again", async () => {
      const m = facade();
      await loadPlan(m);
      m.bind({ database: stranger() });
      m.bind({ database: stranger() });
      expect(declines(warn)).toHaveLength(1);
      expect(declines(debug)).toHaveLength(1);
      // Its reason is a code, never text that can name the host's table.
      const fields = declines(warn)[0].args[1] as Record<string, unknown>;
      expect(String(fields.reason)).toMatch(/^[a-z_]+$/);
      expect(JSON.stringify(fields)).not.toContain("secret_host_table");

      await loadPlan(m);
      m.bind({ database: stranger() });
      expect(declines(warn)).toHaveLength(2);
   });
});
