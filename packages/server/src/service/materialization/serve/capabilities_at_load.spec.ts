// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * How a model's sources compare strings is read from load, before any
 * bind, and a lookup waits for a read still running: read admission only
 * looks it up, so a query over a table bound later (a spec's direct bind
 * publishes its compile in the background) never finds it unread and
 * declines the table as of an unknown collation.
 */
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { writePackageFiles } from "../../../test_helpers/temp_package";
import { Environment } from "../../environment";
import { parsePackageName } from "../../identity";
import { stringCapabilitiesOf } from "./source_capability";

const MODEL = `##! experimental.persistence
source: orders is duckdb.sql("SELECT 1 AS id, 'east' AS region")

#@ persist name="by_region"
source: by_region is orders -> { group_by: region; aggregate: n is count() }
`;

describe("string capabilities at load", () => {
   let rootDir: string;
   let env: Environment;

   beforeEach(async () => {
      rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-caps-"));
      const envPath = path.join(rootDir, "env");
      writePackageFiles(
         path.join(envPath, "pkg"),
         { "model.malloy": MODEL },
         { manifest: { name: "pkg", description: "fixture" } },
      );
      env = await Environment.create("testEnv", envPath, []);
   });

   afterEach(async () => {
      await env.closeAllConnections().catch(() => {});
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
   });

   it("are read for each model's connections from load, before anything is bound", async () => {
      await env.addPackage("pkg");
      const pkg = await env.getPackage(parsePackageName("pkg"), false);
      const model = pkg.getModel("model.malloy");
      const runtime = (
         model as unknown as { modelMaterializer?: { runtime?: object } }
      ).modelMaterializer?.runtime;
      expect(runtime).toBeDefined();
      const capabilities = await stringCapabilitiesOf(runtime);
      expect([...(capabilities?.connections.keys() ?? [])]).toEqual(["duckdb"]);
   });
});
