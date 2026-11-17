// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Whether a `storage=` source's connection can be read by the build, decided
 * at plan from the type each environment connection's configuration records
 * (service/connection_type.ts), through a real Environment and package load
 * with no test seam: an environment DuckDB connection is refused
 * (`storage_source_unsupported`), with its `persist-not-built` warning, and
 * answers live.
 */

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs/promises";
import * as os from "os";
import * as path from "path";
import { Environment } from "../../src/service/environment";
import { parsePackageName } from "../../src/service/identity";
import { installSettings } from "../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../src/test_helpers/temp_package";

const MODEL = `##! experimental.persistence
source: orders is shared_duckdb.sql("SELECT 1 AS id, 'east' AS region")

#@ persist name="by_region" storage=lake
source: by_region is orders -> { group_by: region; aggregate: n is count() }

#@ persist name="colocated"
source: colocated is orders -> { group_by: region }
`;

/** A destination of the one declarable type; nothing connects to it at load. */
const LAKE = {
   name: "lake",
   type: "ducklake",
   ducklakeConnection: {
      catalog: {
         postgresConnection: { host: "catalog.internal", password: "x" },
      },
      storage: { bucketUrl: "gs://bucket/lake" },
   },
};

let rootDir: string;
let restoreSettings: () => void;
let env: Environment | undefined;

beforeEach(async () => {
   restoreSettings = installSettings({
      env: {
         PERSIST_STORAGE_MODE: "on",
         PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true",
      },
   }).restore;
   rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "storage-conn-"));
});

afterEach(async () => {
   await env?.closeAllConnections().catch(() => {});
   env = undefined;
   restoreSettings();
   await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
});

describe("a storage= source on an environment DuckDB connection", () => {
   it("is refused at plan, warns, and answers live; its colocated sibling plans", async () => {
      const envPath = path.join(rootDir, "env");
      writePackageFiles(
         path.join(envPath, "pkg"),
         { "model.malloy": MODEL },
         { manifest: { name: "pkg", description: "fixture" } },
      );
      env = await Environment.create(
         "testEnv",
         envPath,
         [
            {
               name: "shared_duckdb",
               type: "duckdb",
               duckdbConnection: { setupSQL: "SELECT 1" },
            } as never,
         ],
         [LAKE] as never,
      );
      await env.addPackage("pkg");
      const pkg = await env.getPackage(parsePackageName("pkg"), false);

      const plan = pkg.getBuildPlan();
      expect(Object.values(plan?.sources ?? {}).map((s) => s.name)).toEqual([
         "colocated",
      ]);
      expect(
         Object.values(plan?.refusedSources ?? {}).map((r) => [
            r.name,
            r.reason,
         ]),
      ).toEqual([["by_region", "storage_source_unsupported"]]);

      const warning = (pkg.getPackageMetadata().warnings ?? []).find(
         (w) => w.code === "persist-not-built" && w.subject === "by_region",
      );
      expect(warning?.severity).toBe("warn");
      expect(warning?.message).toContain(
         "its connection 'shared_duckdb' is a 'duckdb' connection",
      );
      // A refusal no earlier release raised is not repeated under the former
      // `persist-source-refused`.
      expect(
         (pkg.getPackageMetadata().warnings ?? []).filter(
            (w) => w.code === "persist-source-refused",
         ),
      ).toEqual([]);
      expect(
         pkg
            .materialization()
            .serveVerdicts()
            .find((v) => v.sourceName === "by_region")?.verdict,
      ).toBe("live");
   });
});
