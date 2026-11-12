// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { DuckDBConnection } from "../storage/duckdb/DuckDBConnection";
import { createDuckDBRepository } from "../storage/duckdb/DuckDBRepository";
import type { ResourceRepository } from "../storage/DatabaseInterface";
import { initializeSchema } from "../storage/duckdb/schema";
import { Environment } from "./environment";
import type { Package } from "./package";
import { compilePackageBuildPlan } from "./materialization/plan/build_plan_compile";
import { computeSourceEntityId } from "./materialization/plan/build_plan";
import { installSettings } from "../test_helpers/settings_env";
import { writePackageFiles } from "../test_helpers/temp_package";

// Build plans of real packages loaded through a real Environment, around an
// edit of the connection their persisted source reads.

const ENV_ID = "env-1";
const PKG = "sales";
const MODEL = `##! experimental.persistence
#@ persist name="daily"
source: daily is duck.sql("SELECT 1 AS id, 2 AS amount") -> { select: id, amount }
`;
/** An environment DuckDB connection; its setupSQL is part of its digest. */
function duck(setupSQL: string) {
   return { name: "duck", type: "duckdb", duckdbConnection: { setupSQL } };
}

let root: string;
let envPath: string;
let sourcesPath: string;
let db: DuckDBConnection;
let repo: ResourceRepository;
let restoreSettings: () => void;

/** A package tree with one persisted source over the `duck` connection. */
function writePackage(dir: string, manifest: Record<string, unknown>): string {
   writePackageFiles(
      dir,
      {
         "model.malloy": MODEL,
      },
      { manifest: { name: PKG, ...manifest } },
   );
   return dir;
}

function source(version: string): string {
   return writePackage(path.join(sourcesPath, version), {
      version,
      description: `release ${version}`,
   });
}

async function newEnvironment(): Promise<Environment> {
   const env = await Environment.create("testEnv", envPath, [
      duck("SELECT 1") as never,
   ]);
   env.bindVersions(repo, ENV_ID, {
      downloaderFor: (_packageName, location) => async (stagingPath) => {
         await fs.promises.cp(location, stagingPath, { recursive: true });
      },
      ensurePackageRecord: async (packageName, description) => {
         if (await repo.getPackageByName(ENV_ID, packageName)) return false;
         await repo.createPackage({
            environmentId: ENV_ID,
            name: packageName,
            description,
            manifestPath: "",
         });
         return true;
      },
      removePackageRecord: async (packageName) => {
         const row = await repo.getPackageByName(ENV_ID, packageName);
         if (row) await repo.deletePackage(row.id);
      },
   });
   return env;
}

async function publish(
   env: Environment,
   location: string,
   promotion: "on-publish" | "explicit" = "on-publish",
) {
   return env.getVersionService()!.publish(
      PKG,
      async (stagingPath) => {
         await fs.promises.cp(location, stagingPath, { recursive: true });
      },
      { sourceLocation: location, promotion },
   );
}

beforeEach(async () => {
   // The edit under test changes a DuckDB connection's setupSQL.
   restoreSettings = installSettings({
      env: { PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true" },
   }).restore;
   root = fs.realpathSync(
      fs.mkdtempSync(path.join(os.tmpdir(), "environment-versions-")),
   );
   envPath = path.join(root, "env");
   sourcesPath = path.join(root, "sources");
   fs.mkdirSync(envPath);
   db = new DuckDBConnection(":memory:");
   await db.initialize();
   await initializeSchema(db);
   await db.run(
      "INSERT INTO environments (id, name, path, created_at, updated_at) VALUES (?, 'testEnv', ?, now(), now())",
      [ENV_ID, envPath],
   );
   repo = createDuckDBRepository(db);
});

afterEach(async () => {
   restoreSettings();
   await db.close();
   fs.rmSync(root, { recursive: true, force: true });
});

/** The addresses the loaded package hands out (GET build-plan, validation). */
function planned(pkg: Package): string[] {
   return Object.values(pkg.getBuildPlan()?.sources ?? {}).map(
      (s) => s.sourceEntityId,
   );
}

/** The addresses a run computes: a fresh compile with the connections as they are now. */
async function compiled(pkg: Package): Promise<string[]> {
   const plan = await compilePackageBuildPlan(pkg);
   return Object.values(plan.sources).map((s) =>
      computeSourceEntityId(
         s,
         plan.connectionDigests,
         plan.targets?.[s.sourceID],
      ),
   );
}

async function editConnection(env: Environment, setupSQL: string) {
   await env.update({
      name: "testEnv",
      connections: [duck(setupSQL)],
   } as never);
}

// ms2data/service#6145. A content address folds in the connection's digest,
// which reads connection config, so a connection edit moves it. The plan
// computed at load used to keep the old address: a host echoed ledger entries
// recorded under it, create-time validation passed against that same stale
// plan, and the run's own compile refused them as "measured under a different
// definition", failing every refresh until the package reloaded.
describe("a connection edit re-derives the loaded build plans", () => {
   it("a published version hands out the address a run computes", async () => {
      const env = await newEnvironment();
      const v1 = (await publish(env, source("1.0.0"))).loaded;
      const before = planned(v1);
      expect(before).toHaveLength(1);
      expect(await compiled(v1)).toEqual(before);

      await editConnection(env, "SELECT 2");
      await v1.whenBuildPlanCurrent();

      const after = await compiled(v1);
      expect(after).not.toEqual(before);
      expect(planned(v1)).toEqual(after);
   });

   it("every loaded version follows, and identical content keeps one address across versions", async () => {
      const env = await newEnvironment();
      const v1 = (await publish(env, source("1.0.0"))).loaded;
      const v2 = (await publish(env, source("2.0.0"))).loaded;
      expect(planned(v2)).toEqual(planned(v1));

      await editConnection(env, "SELECT 3");
      await Promise.all([v1.whenBuildPlanCurrent(), v2.whenBuildPlanCurrent()]);

      expect(planned(v1)).toEqual(await compiled(v1));
      expect(planned(v2)).toEqual(planned(v1));
   });

   it("an edit that changes nothing in the digest keeps the address", async () => {
      const env = await newEnvironment();
      const v1 = (await publish(env, source("1.0.0"))).loaded;
      const before = planned(v1);

      await editConnection(env, "SELECT 1");
      await v1.whenBuildPlanCurrent();

      expect(planned(v1)).toEqual(before);
   });
});
