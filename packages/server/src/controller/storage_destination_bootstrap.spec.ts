// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { components } from "../api";
import { BadRequestError } from "../errors";
import { firstAttachClauses } from "../service/connection";
import type { EnvironmentStore } from "../service/environment_store";
import { StorageDestinationController } from "./storage_destination.controller";

type ApiConnection = components["schemas"]["Connection"];

const lake = (name = "sales"): ApiConnection =>
   ({
      name,
      type: "ducklake",
      ducklakeConnection: {
         storage: { bucketUrl: "gs://b/ds_x" },
         catalog: {
            postgresConnection: {
               host: "h",
               databaseName: "ducklake",
               userName: "ds_x",
               password: "p",
            },
            metadataSchema: "ds_x",
         },
      },
   }) as ApiConnection;

function controller(): StorageDestinationController {
   const store = {
      getEnvironment: async () => ({}),
   } as unknown as EnvironmentStore;
   return new StorageDestinationController(store);
}

/**
 * The refusals all happen before anything attaches, so they are proven without
 * a catalog. The attach itself runs against a real DuckLake in the hammer.
 */
describe("createBootstrap refuses a request it cannot trust", () => {
   const refused = async (body: Record<string, unknown>, dest = "sales") => {
      const err = await controller()
         .createBootstrap("env", dest, body)
         .catch((e) => e);
      expect(err).toBeInstanceOf(BadRequestError);
      return (err as Error).message;
   };

   it("needs the destination in the body", async () => {
      expect(await refused({})).toContain("'destination' is required");
   });

   it("refuses a body destination named other than the path", async () => {
      expect(await refused({ destination: lake("other") })).toContain(
         "named 'other', not 'sales'",
      );
   });

   it("bootstraps only DuckLake", async () => {
      expect(
         await refused({ destination: { name: "sales", type: "duckdb" } }),
      ).toContain("Only a DuckLake destination is taken here");
   });

   it("refuses a schema that is not a plain identifier, and a bad limit", async () => {
      expect(
         await refused({ destination: lake(), schemas: ["ok", "x; DROP"] }),
      ).toContain("Invalid schema name 'x; DROP'");
      expect(
         await refused({ destination: lake(), dataInliningRowLimit: -1 }),
      ).toContain("non-negative integer");
      expect(
         await refused({ destination: lake(), encrypted: "yes" }),
      ).toContain("'encrypted' must be a boolean");
   });
});

describe("firstAttachClauses", () => {
   it("adds ENCRYPTED and the inlining limit only when asked", () => {
      expect(firstAttachClauses({})).toBe("");
      expect(firstAttachClauses({ encrypted: true })).toBe(", ENCRYPTED true");
      expect(firstAttachClauses({ encrypted: true, dataInliningRowLimit: 10 })).toBe(
         ", ENCRYPTED true, DATA_INLINING_ROW_LIMIT 10",
      );
   });

   it("splices no text: a non-integer limit is dropped", () => {
      expect(
         firstAttachClauses({ dataInliningRowLimit: "1; DROP" as unknown as number }),
      ).toBe("");
      expect(firstAttachClauses({ dataInliningRowLimit: 1.5 })).toBe("");
   });
});
