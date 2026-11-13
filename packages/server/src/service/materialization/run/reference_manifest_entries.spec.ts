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
});
