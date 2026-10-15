// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import express from "express";
import request from "supertest";
import type { EnvironmentStore } from "../service/environment_store";
import type { Version } from "../storage/DatabaseInterface";
import { VersionController, versionsRouter } from "./version.controller";

// A frozen config (`frozenConfig: true` in publisher.config.json) refuses
// every write to a package's version lifecycle with 403, before the version
// rules are reached; reads stay open. The routes are mounted as server.ts
// mounts them, over a version service that would carry out every write.

const BASE = "/api/v0/environments/env/packages/pkg";

function row(versionId: string): Version {
   const at = new Date("2026-01-01T00:00:00Z");
   return {
      versionId,
      environmentId: "env-1",
      packageName: "pkg",
      dirName: versionId,
      contentHash: "hash",
      sourceLocation: null,
      manifestPath: null,
      archiveStatus: "unarchive",
      archivedAt: null,
      promotedAt: null,
      demotedAt: null,
      description: null,
      gitCommitSha: null,
      gitRef: null,
      createdAt: at,
      updatedAt: at,
   };
}

function serverWith(frozen: boolean) {
   /** The version-rule writes that were reached. */
   const writes: string[] = [];
   const versions = {
      listVersions: async () => ({
         latest: "2.0.0",
         versions: [row("2.0.0"), row("1.0.0")],
      }),
      getVersion: async (_pkg: string, version: string) => row(version),
      latestOf: async () => "2.0.0",
      setArchiveStatus: async (_pkg: string, version: string) => {
         writes.push(`archive ${version}`);
         return row(version);
      },
      setManifest: async (_pkg: string, version: string) => {
         writes.push(`manifest ${version}`);
         return { name: "pkg" };
      },
      setLatest: async (_pkg: string, version: string) => {
         writes.push(`latest ${version}`);
         return row(version);
      },
   };
   const environment = {
      getVersionService: () => versions,
      getPackageStatus: () => ({}),
      describePackage: async () => ({ name: "pkg", versionId: "1.0.0" }),
   };
   const environmentStore = {
      publisherConfigIsFrozen: frozen,
      getEnvironment: async () => environment,
   } as unknown as EnvironmentStore;
   const app = express();
   app.use(express.json());
   app.use(versionsRouter(new VersionController(environmentStore)));
   return { app, writes };
}

/** The three lifecycle writes, each a request the version service accepts. */
const lifecycleWrites = [
   {
      name: "PATCH .../versions/{v} (archive)",
      send: (app: express.Express) =>
         request(app)
            .patch(`${BASE}/versions/1.0.0`)
            .send({ archiveStatus: "archive" }),
   },
   {
      name: "PATCH .../versions/{v} (unarchive)",
      send: (app: express.Express) =>
         request(app)
            .patch(`${BASE}/versions/1.0.0`)
            .send({ archiveStatus: "unarchive" }),
   },
   {
      name: "PUT .../versions/{v}/manifest",
      send: (app: express.Express) =>
         request(app)
            .put(`${BASE}/versions/1.0.0/manifest`)
            .send({ manifestLocation: "gs://bucket/pkg-1.0.0.manifest.json" }),
   },
   {
      name: "PUT .../latest",
      send: (app: express.Express) =>
         request(app).put(`${BASE}/latest`).send({ version: "1.0.0" }),
   },
];

describe("version lifecycle writes under a frozen config", () => {
   for (const write of lifecycleWrites) {
      it(`${write.name} is refused with 403, and no version rule runs`, async () => {
         const { app, writes } = serverWith(true);
         const res = await write.send(app);
         expect(res.status).toBe(403);
         expect(res.body.message).toMatch(/frozenConfig/);
         expect(writes).toEqual([]);
      });
   }

   it("the same writes succeed when the config is not frozen", async () => {
      // So the 403s above are the frozen gate, not a malformed request.
      const { app, writes } = serverWith(false);
      for (const write of lifecycleWrites) {
         const res = await write.send(app);
         expect(res.status).toBe(200);
      }
      expect(writes).toEqual([
         "archive 1.0.0",
         "archive 1.0.0",
         "manifest 1.0.0",
         "latest 1.0.0",
      ]);
   });

   it("still answers reads of the versions", async () => {
      const { app } = serverWith(true);
      const list = await request(app).get(`${BASE}/versions`);
      expect(list.status).toBe(200);
      expect(
         (list.body as { id: string; latest: boolean }[]).map((v) => [
            v.id,
            v.latest,
         ]),
      ).toEqual([
         ["2.0.0", true],
         ["1.0.0", false],
      ]);
      const one = await request(app).get(`${BASE}/versions/1.0.0`);
      expect(one.status).toBe(200);
      expect(one.body.id).toBe("1.0.0");
   });
});
