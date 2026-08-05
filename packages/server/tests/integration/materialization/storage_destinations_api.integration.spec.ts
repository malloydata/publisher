/// <reference types="bun-types" />

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const PROJECT_NAME = "storage-destinations-api";

function ducklakeDestination(name: string, bucketPath: string) {
   return {
      name,
      type: "ducklake",
      ducklakeConnection: {
         catalog: {
            postgresConnection: {
               host: "catalog.internal",
               port: 5432,
               databaseName: "ducklake",
               userName: "publisher",
               password: "catalog-secret",
            },
         },
         storage: { bucketUrl: `gs://managed-tier/${bucketPath}` },
      },
   };
}

// The `storageDestinations` write contract, over HTTP, through the real Express
// error handling: a list the server cannot read is refused with 400 and applies
// none of itself, on create as well as on update. Asserted here rather than only
// against Environment because the two entry points reach validation by different
// routes — the create handler gates the body itself, the update path validates
// inside Environment so an entry can resolve against the stored list — and a unit
// test of either one cannot show that both are strict.
describe("storageDestinations write contract (REST)", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;

   const post = (body: unknown) =>
      fetch(`${baseUrl}/api/v0/environments`, {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify(body),
      });

   const patch = (body: unknown) =>
      fetch(`${baseUrl}/api/v0/environments/${PROJECT_NAME}`, {
         method: "PATCH",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify(body),
      });

   beforeAll(async () => {
      env = await startRestE2E();
      baseUrl = env.baseUrl;
   });

   afterAll(async () => {
      if (baseUrl) {
         try {
            await fetch(`${baseUrl}/api/v0/environments/${PROJECT_NAME}`, {
               method: "DELETE",
            });
         } catch {
            // best effort
         }
      }
      await env?.stop();
   });

   it("refuses a create whose destination cannot be used, and creates nothing", async () => {
      const res = await post({
         name: PROJECT_NAME,
         connections: [],
         // A postgres connection is not a declarable destination type.
         storageDestinations: [{ name: "lake", type: "postgres" }],
      });

      expect(res.status).toBe(400);
      const body = (await res.json()) as { message?: string };
      expect(body.message).toContain("lake");

      // Refused before the environment was created, not after.
      const lookup = await fetch(
         `${baseUrl}/api/v0/environments/${PROJECT_NAME}`,
      );
      expect(lookup.status).toBe(404);
   });

   it("refuses the same create through the legacy projects alias", async () => {
      // An alias is still a create. Covered separately because it is a second
      // route to the same store method, and gating one says nothing about the
      // other.
      const res = await fetch(`${baseUrl}/api/v0/projects`, {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            name: PROJECT_NAME,
            connections: [],
            storageDestinations: [{ name: "lake", type: "postgres" }],
         }),
      });

      expect(res.status).toBe(400);
      const lookup = await fetch(
         `${baseUrl}/api/v0/environments/${PROJECT_NAME}`,
      );
      expect(lookup.status).toBe(404);
   });

   it("creates an environment with a usable destination, reporting name and type only", async () => {
      const res = await post({
         name: PROJECT_NAME,
         connections: [],
         storageDestinations: [ducklakeDestination("lake", "org_a")],
      });

      expect(res.status).toBe(200);
      const created = (await res.json()) as {
         storageDestinations?: { name: string; type: string }[];
      };
      expect(created.storageDestinations).toEqual([
         { name: "lake", type: "ducklake" },
      ]);
      // The config carries warehouse credentials and is never echoed back.
      expect(JSON.stringify(created)).not.toContain("catalog-secret");
   });

   it("refuses an update whose destination list cannot be read, and keeps the stored one", async () => {
      const res = await patch({
         name: PROJECT_NAME,
         storageDestinations: "lake",
      });

      expect(res.status).toBe(400);

      const after = await fetch(
         `${baseUrl}/api/v0/environments/${PROJECT_NAME}`,
      );
      const body = (await after.json()) as {
         storageDestinations?: { name: string; type: string }[];
      };
      expect(body.storageDestinations).toEqual([
         { name: "lake", type: "ducklake" },
      ]);
   });

   it("keeps the stored config when an update echoes back what a read reported", async () => {
      const res = await patch({
         name: PROJECT_NAME,
         storageDestinations: [{ name: "lake", type: "ducklake" }],
      });

      expect(res.status).toBe(200);
      const body = (await res.json()) as {
         storageDestinations?: { name: string; type: string }[];
      };
      expect(body.storageDestinations).toEqual([
         { name: "lake", type: "ducklake" },
      ]);
   });
});
