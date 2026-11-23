// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { referenceManifestToEntries } from "./manifest_committer";

// ms2data/service#7087: a stored table's reference names the source's
// WAREHOUSE connection, so only an explicit destination says it is stored.
describe("referenceManifestToEntries", () => {
   it("carries a reference's storage destination onto the seeded entry", () => {
      const entries = referenceManifestToEntries([
         {
            sourceEntityId: "up",
            physicalTableName: "up__g004__e9c3ad40",
            connectionName: "warehouse",
            storageDestinationName: "lake",
         },
         {
            sourceEntityId: "co",
            physicalTableName: "analytics.co_v1",
            connectionName: "warehouse",
         },
      ]);
      expect(entries.up).toEqual({
         sourceEntityId: "up",
         physicalTableName: "up__g004__e9c3ad40",
         connectionName: "warehouse",
         storageDestinationName: "lake",
      });
      // A reference without one is a warehouse table, as before.
      expect(entries.co).toEqual({
         sourceEntityId: "co",
         physicalTableName: "analytics.co_v1",
         connectionName: "warehouse",
      });
   });

   it("carries a reference's nativeSchema onto the seeded entry, for the altered-upstream check", () => {
      const nativeSchema = [{ name: "amt", type: "DECIMAL(10,1)" }];
      const entries = referenceManifestToEntries([
         { sourceEntityId: "up", physicalTableName: "up_t", nativeSchema },
      ]);
      expect(entries.up.nativeSchema).toEqual(nativeSchema);
   });

   it("carries a reference's schema, dataAsOf, builtAt and contract onto the seeded entry", () => {
      const ref = {
         sourceEntityId: "up",
         physicalTableName: "up_t",
         storageDestinationName: "lake",
         schema: [{ name: "cat", type: "VARCHAR" }],
         dataAsOf: "2026-11-20T00:00:00.000Z",
         builtAt: "2026-11-20T00:05:00.000Z",
         contract: { v: 1, dialect: "duckdb", columns: [] },
      };
      const { up } = referenceManifestToEntries([ref]);
      expect(up).toEqual(ref);
   });
});
