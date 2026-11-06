// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * A versioned package's sub-resources over HTTP: each answers for the version
 * `versionId` names (its `latest` when none), an unknown version is a 404 and
 * a malformed one a 400, and with inline fields off `GET /status` lists one
 * slim entry per version held.
 */

import { afterAll, afterEach, beforeAll, describe, expect, it } from "bun:test";
import type { components } from "../../../src/api";
import { installVariables } from "../../../src/test_helpers/settings_env";
import { versioningOnForSuite } from "../../harness/versioning";
import { type ResourcesEnv, startResourcesEnv } from "./resources_harness";

type ModelEntities = components["schemas"]["ModelEntities"];

const ENV_NAME = "versioned-package-resources-env";

let restoreInline: (() => void) | undefined;

const ROWS = `duckdb.sql("select * from (values (1, 'web'), (2, 'store')) as t(id, channel)")`;

/** The model at version `n`: version 2 adds a dimension. */
function orders(n: number): string {
   return (
      `source: orders is ${ROWS} extend {\n` +
      `  dimension: channel_upper is upper(channel)\n` +
      (n > 1 ? `  dimension: channel_lower is lower(channel)\n` : "") +
      `}\n`
   );
}

describe("a versioned package's sub-resources", () => {
   versioningOnForSuite();
   let r: ResourcesEnv;
   const at = (sub: string, versionId?: string) =>
      r.get(
         r.pkgUrl("shop", sub) +
            (versionId === undefined ? "" : `?versionId=${versionId}`),
      );

   beforeAll(async () => {
      r = await startResourcesEnv(ENV_NAME);
      for (const n of [1, 2]) {
         const location = r.writePackage(
            "shop",
            { "orders.malloy": orders(n) },
            { version: `${n}.0.0` },
         );
         expect((await r.publish("shop", location)).status).toBe(200);
      }
   }, 180_000);

   afterEach(() => {
      restoreInline?.();
      restoreInline = undefined;
   });

   afterAll(() => r.stop());

   it("GET …/entities answers for the version named, and for latest by default", async () => {
      const fieldsOf = async (versionId?: string) => {
         const { status, json } = await at("/entities", versionId);
         expect(status).toBe(200);
         return (json as unknown as ModelEntities[])[0]!.fields!.map(
            (f) => f.name,
         );
      };
      expect(await fieldsOf("1.0.0")).not.toContain("channel_lower");
      expect(await fieldsOf("2.0.0")).toContain("channel_lower");
      expect(await fieldsOf()).toContain("channel_lower");
   }, 60_000);

   it("GET …/build-plan and GET …/classification answer for a named version", async () => {
      expect((await at("/build-plan", "1.0.0")).status).toBe(200);
      expect((await at("/classification", "1.0.0")).json).toEqual({
         sources: [],
      });
   }, 60_000);

   it("GET …/status describes latest by default and the version named", async () => {
      expect((await at("/status")).json).toMatchObject({
         versionId: "2.0.0",
         loaded: true,
         archiveStatus: "unarchive",
      });
      expect((await at("/status", "1.0.0")).json).toMatchObject({
         versionId: "1.0.0",
         loaded: true,
      });
   }, 60_000);

   it("an unknown version is a 404 and a malformed one a 400, on every sub-resource", async () => {
      for (const sub of [
         "/status",
         "/entities",
         "/build-plan",
         "/classification",
         "/embedding-index",
      ]) {
         expect((await at(sub, "9.9.9")).status).toBe(404);
         expect((await at(sub, "two")).status).toBe(400);
      }
   }, 60_000);

   it("with inline fields off, GET /status lists one slim entry per version", async () => {
      // Installed over the process's variables, which do not hold the suite's
      // versioning, so this keeps it on beside inline fields off.
      restoreInline = installVariables({
         PUBLISHER_PACKAGE_VERSIONING: "on",
         PUBLISHER_PACKAGE_INLINE_FIELDS: "off",
      });
      const entries = await r.statusEntries("shop");
      expect(entries.map((e) => e.versionId).sort()).toEqual([
         "1.0.0",
         "2.0.0",
      ]);
      for (const entry of entries) {
         expect(Object.keys(entry).sort()).toEqual([
            "archiveStatus",
            "boundManifestUri",
            "latestVersion",
            "loaded",
            "manifestBindingStatus",
            "name",
            "resource",
            "status",
            "versionId",
         ]);
      }
   }, 60_000);
});
