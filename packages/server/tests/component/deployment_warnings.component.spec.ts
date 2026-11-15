// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// P13's publish warnings over a real Environment and Package: a `storage=`
// source naming a destination the environment does not configure, and a
// colocated source on a connection that cannot hold a table. Each is a coded
// package warning and a `live` serve verdict; the package still loads, and the
// warning is read live, so fixing the deployment clears it with no reload.
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs/promises";
import * as os from "os";
import * as path from "path";
import { Environment } from "../../src/service/environment";
import { parsePackageName } from "../../src/service/identity";
import type { Package } from "../../src/service/package";
import { installSettings } from "../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../src/test_helpers/temp_package";

const DESTINATION_MISSING = "materialization-destination-missing";
const CANNOT_HOLD_TABLE = "materialization-connection-cannot-hold-table";

const MODEL = `##! experimental.persistence
source: orders is duckdb.sql("SELECT 1 AS id, 'east' AS region")

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
let envPath: string;
let restoreSettings: () => void;

function useStorageMode(mode: "on" | "off" | "write-only"): void {
   restoreSettings();
   restoreSettings = installSettings({
      env: { PERSIST_STORAGE_MODE: mode },
   }).restore;
}

beforeEach(async () => {
   restoreSettings = installSettings({
      env: { PERSIST_STORAGE_MODE: "on" },
   }).restore;
   rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-p13-"));
   envPath = path.join(rootDir, "env");
   await fs.mkdir(envPath, { recursive: true });
});

afterEach(async () => {
   restoreSettings();
   await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
});

async function load(
   destinations: unknown[],
): Promise<{ env: Environment; pkg: Package }> {
   writePackageFiles(
      path.join(envPath, "pkg"),
      { "model.malloy": MODEL },
      { manifest: { name: "pkg", description: "fixture" } },
   );
   const env = await Environment.create(
      "testEnv",
      envPath,
      [],
      destinations as never,
   );
   await env.addPackage("pkg");
   return { env, pkg: await env.getPackage(parsePackageName("pkg"), false) };
}

function warningsWith(pkg: Package, code: string) {
   return (pkg.getPackageMetadata().warnings ?? []).filter(
      (w) => w.code === code,
   );
}

function verdictOf(pkg: Package, sourceName: string) {
   return pkg.getServeVerdicts().find((v) => v.sourceName === sourceName);
}

describe("a storage= destination the environment does not configure (P13)", () => {
   it("loads, warns with a code naming the source and destination, and serves live", async () => {
      const { pkg } = await load([]);
      // Unservable, not malformed: the source is still planned.
      expect(
         Object.values(pkg.getBuildPlan()?.sources ?? {}).map((s) => s.name),
      ).toContain("by_region");
      const [warning] = warningsWith(pkg, DESTINATION_MISSING);
      expect(warning).toMatchObject({
         subject: "by_region",
         model: "model.malloy",
         severity: "warn",
      });
      expect(warning.message).toContain('storage="lake"');
      expect(verdictOf(pkg, "by_region")).toMatchObject({
         verdict: "live",
         reason: "destination_missing",
      });
      // The colocated source on a writable connection warns nothing.
      expect(warningsWith(pkg, CANNOT_HOLD_TABLE)).toEqual([]);
   });

   it("warns nothing when the destination is configured", async () => {
      const { pkg } = await load([LAKE]);
      expect(warningsWith(pkg, DESTINATION_MISSING)).toEqual([]);
      expect(verdictOf(pkg, "by_region")?.verdict).toBe("not_built");
   });

   it("clears once the destination is configured, with no reload, and returns when it is removed", async () => {
      const { env, pkg } = await load([]);
      expect(warningsWith(pkg, DESTINATION_MISSING)).toHaveLength(1);
      env.setStorageDestinations([LAKE as never]);
      expect(warningsWith(pkg, DESTINATION_MISSING)).toEqual([]);
      env.setStorageDestinations([]);
      expect(warningsWith(pkg, DESTINATION_MISSING)).toHaveLength(1);
   });

   it("warns nothing while the destination list is not known", async () => {
      const { env, pkg } = await load([]);
      env.markStorageDestinationsUnknown();
      expect(warningsWith(pkg, DESTINATION_MISSING)).toEqual([]);
   });

   it("is not reported under PERSIST_STORAGE_MODE=off, which ignores storage=", async () => {
      useStorageMode("off");
      const { pkg } = await load([]);
      expect(warningsWith(pkg, DESTINATION_MISSING)).toEqual([]);
      expect(verdictOf(pkg, "by_region")?.reason).toBe("storage_mode");
   });

   it("is reported under write-only, where the build would fail on it", async () => {
      useStorageMode("write-only");
      const { pkg } = await load([]);
      expect(warningsWith(pkg, DESTINATION_MISSING)).toHaveLength(1);
   });
});

describe("a colocated source on a connection that cannot hold a table (P13)", () => {
   it("warns with a code naming the connection, serves live, and clears when the capability changes", async () => {
      const { pkg } = await load([LAKE]);
      // The environment answers from each connection's configured type; no
      // connection type this test can reach is read-only, so the package's
      // fact source is replaced with one that answers as a read-only type does.
      let canHold = false;
      pkg.setDeploymentFacts({
         destinationNames: () => ["lake"],
         connectionCanHoldTable: (name) => (name === "duckdb" ? canHold : true),
      });
      const [warning] = warningsWith(pkg, CANNOT_HOLD_TABLE);
      expect(warning).toMatchObject({
         subject: "colocated",
         severity: "warn",
      });
      expect(warning.message).toContain("'duckdb'");
      expect(verdictOf(pkg, "colocated")).toMatchObject({
         verdict: "live",
         reason: "connection_cannot_hold_table",
      });
      // A storage= source only reads its connection: not warned.
      expect(
         warningsWith(pkg, CANNOT_HOLD_TABLE).map((w) => w.subject),
      ).toEqual(["colocated"]);
      canHold = true;
      expect(warningsWith(pkg, CANNOT_HOLD_TABLE)).toEqual([]);
   });

   it("reads a connection's configured type from the environment: a writable type warns nothing", async () => {
      const { pkg } = await load([LAKE]);
      expect(warningsWith(pkg, CANNOT_HOLD_TABLE)).toEqual([]);
      expect(verdictOf(pkg, "colocated")?.verdict).toBe("not_built");
   });
});
