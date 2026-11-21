// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import request from "supertest";
import { API_PREFIX, buildRouteApp } from "./test_helpers/route_app";

// A materialization create names its version in the body, or in the URL as
// every other materialization route does; both must agree.

const MATERIALIZATIONS = `${API_PREFIX}/environments/acme/packages/sales/materializations`;

function appRecordingBodies(): {
   app: ReturnType<typeof buildRouteApp>;
   bodies: unknown[];
} {
   const bodies: unknown[] = [];
   const app = buildRouteApp({
      materializationController: {
         createMaterialization: async (
            _environment: string,
            _pkg: string,
            body: unknown,
         ) => {
            bodies.push(body);
            return {} as never;
         },
      },
   });
   return { app, bodies };
}

describe("POST .../materializations versionId", () => {
   it("builds the version the URL names when the body names none", async () => {
      const { app, bodies } = appRecordingBodies();
      await request(app)
         .post(`${MATERIALIZATIONS}?versionId=1.2.0`)
         .send({ forceRefresh: true })
         .expect(201);
      expect(bodies).toEqual([{ forceRefresh: true, versionId: "1.2.0" }]);
   });

   it("takes the version both name when they agree", async () => {
      const { app, bodies } = appRecordingBodies();
      await request(app)
         .post(`${MATERIALIZATIONS}?versionId=1.2.0`)
         .send({ versionId: "1.2.0" })
         .expect(201);
      expect(bodies).toEqual([{ versionId: "1.2.0" }]);
   });

   it("answers 400 VERSION_ID_INVALID when the body and the URL differ", async () => {
      const { app, bodies } = appRecordingBodies();
      const res = await request(app)
         .post(`${MATERIALIZATIONS}?versionId=1.2.0`)
         .send({ versionId: "1.3.0" })
         .expect(400);
      expect(res.body.reason).toBe("VERSION_ID_INVALID");
      expect(bodies).toEqual([]);
   });

   it("reads an empty URL value as naming none", async () => {
      const { app, bodies } = appRecordingBodies();
      await request(app)
         .post(`${MATERIALIZATIONS}?versionId=`)
         .send({ versionId: "1.3.0" })
         .expect(201);
      await request(app).post(`${MATERIALIZATIONS}?versionId=`).expect(201);
      expect(bodies).toEqual([{ versionId: "1.3.0" }, {}]);
   });
});
