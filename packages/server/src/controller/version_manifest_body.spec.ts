// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import express from "express";
import request from "supertest";
import { registerVersionRoutes } from "../routes/versions";
import { VersionController } from "./version.controller";
import { fakeOf } from "../test_helpers/store_fakes";
import type {
   ConfigFreezeReader,
   PackageLookup,
} from "../service/environment_ports";

// `PUT …/versions/{v}/manifest` takes one field. An empty body clears the
// binding, as null does: generated clients drop a null field from the JSON
// they send, so `{}` is how they say null. Any other field is refused, so a
// typo is never read as a clear.

const URL = "/api/v0/environments/env/packages/pkg/versions/1.0.0/manifest";

function serverRecordingBinds() {
   /** Each location the version service was asked to bind (null: clear). */
   const bound: (string | null)[] = [];
   const versions = {
      setManifest: async (
         _pkg: string,
         _version: string,
         location: string | null,
      ) => {
         bound.push(location);
         return { name: "pkg" };
      },
   };
   const environment = {
      getVersionService: () => versions,
      describePackage: async () => ({ name: "pkg", versionId: "1.0.0" }),
   };
   const environmentStore = fakeOf<PackageLookup & ConfigFreezeReader>({
      publisherConfigIsFrozen: false,
      getEnvironment: async () => environment,
   });
   const app = express();
   app.use(express.json());
   registerVersionRoutes(
      app,
      "/api/v0/environments/:environmentName/packages/:packageName",
      new VersionController(environmentStore),
   );
   return { app, bound };
}

describe("PUT …/versions/{v}/manifest body", () => {
   it("clears the binding for an empty body, as for an explicit null", async () => {
      const { app, bound } = serverRecordingBinds();
      expect((await request(app).put(URL).send({})).status).toBe(200);
      expect(
         (await request(app).put(URL).send({ manifestLocation: null })).status,
      ).toBe(200);
      expect(bound).toEqual([null, null]);
   });

   it("refuses a body not sent as JSON, and binds nothing", async () => {
      const { app, bound } = serverRecordingBinds();
      const text = await request(app)
         .put(URL)
         .set("content-type", "text/plain")
         .send('{"manifestLocation":"gs://bucket/pkg.manifest.json"}');
      expect(text.status).toBe(400);
      expect(JSON.stringify(text.body)).toContain("application/json");
      const none = await request(app).put(URL);
      expect(none.status).toBe(400);
      expect(bound).toEqual([]);
   });

   it("binds a location the loader reads", async () => {
      const { app, bound } = serverRecordingBinds();
      const res = await request(app)
         .put(URL)
         .send({ manifestLocation: "gs://bucket/pkg.manifest.json" });
      expect(res.status).toBe(200);
      expect(bound).toEqual(["gs://bucket/pkg.manifest.json"]);
   });

   it("refuses another field, naming it, and binds nothing", async () => {
      const { app, bound } = serverRecordingBinds();
      const typo = await request(app)
         .put(URL)
         .send({ manifestUri: "gs://bucket/pkg.manifest.json" });
      expect(typo.status).toBe(400);
      expect(JSON.stringify(typo.body)).toContain("manifestUri");
      const extra = await request(app)
         .put(URL)
         .send({ manifestLocation: null, other: 1 });
      expect(extra.status).toBe(400);
      expect(JSON.stringify(extra.body)).toContain("other");
      expect(bound).toEqual([]);
   });

   it("refuses a location of the wrong type or form", async () => {
      const { app, bound } = serverRecordingBinds();
      for (const manifestLocation of [5, [], {}, "", "relative/path.json"]) {
         const res = await request(app).put(URL).send({ manifestLocation });
         expect([manifestLocation, res.status]).toEqual([
            manifestLocation,
            400,
         ]);
      }
      expect(bound).toEqual([]);
   });

   it("refuses a body that is not an object", async () => {
      const { app, bound } = serverRecordingBinds();
      const res = await request(app)
         .put(URL)
         .set("content-type", "application/json")
         .send("[]");
      expect(res.status).toBe(400);
      expect(bound).toEqual([]);
   });
});
