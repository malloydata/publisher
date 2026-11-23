// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { ManifestEntry } from "../domain/types";
import {
   ManifestCommitter,
   referenceManifestToEntries,
} from "./manifest_committer";

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

   it("carries a reference's ledger entry whole, for an incremental extension over it", () => {
      const ledger = {
         connectionName: "warehouse",
         physicalTableName: "up_t",
         coveredThrough: "2026-11-20 00:00:00",
         coveredThroughType: "timestamp",
         watermark: "ts",
         strategy: "merge" as const,
         sourceEntityId: "up",
         advancedBy: "run-2",
         seededBy: "run-1",
         advances: [{ from: "run-1", advancedBy: "run-2" }],
         later: { x: 1 },
      };
      const { up } = referenceManifestToEntries([
         {
            sourceEntityId: "up",
            physicalTableName: "up_t",
            connectionName: "warehouse",
            ledger,
         },
      ]);
      expect(up.ledger).toEqual(ledger);
   });
});

describe("a reference's ledger over this worker's own entry", () => {
   /** A committer whose newest local manifest holds `local`. */
   const committerWith = (local: Record<string, ManifestEntry>) => {
      const committer = new ManifestCommitter({} as never);
      committer.mostRecentEntries = async () => local;
      return committer;
   };
   const ledgerOf = (advancedBy: string) => ({
      connectionName: "warehouse",
      physicalTableName: "up_t",
      coveredThrough: "2026-11-20 00:00:00",
      coveredThroughType: "timestamp",
      watermark: "ts",
      strategy: "merge" as const,
      sourceEntityId: "up",
      advancedBy,
   });
   const local: ManifestEntry = {
      sourceEntityId: "up",
      physicalTableName: "up_t",
      connectionName: "warehouse",
      builtAt: "2026-11-19T00:00:00.000Z",
      ledger: ledgerOf("run-1"),
   };

   it("wins, and keeps no local builtAt of another build beside it", async () => {
      const carried = referenceManifestToEntries([
         {
            sourceEntityId: "up",
            physicalTableName: "up_t",
            connectionName: "warehouse",
            ledger: ledgerOf("run-2"),
         },
      ]);
      await committerWith({ up: local }).resolveReferencesFromStore(
         carried,
         "env-1",
         "pkg",
         "this-run",
      );
      expect(carried.up.ledger?.advancedBy).toBe("run-2");
      expect(carried.up.builtAt).toBeUndefined();
   });

   it("keeps the local builtAt when the reference's ledger names the same build", async () => {
      const carried = referenceManifestToEntries([
         {
            sourceEntityId: "up",
            physicalTableName: "up_t",
            connectionName: "warehouse",
            ledger: ledgerOf("run-1"),
         },
      ]);
      await committerWith({ up: local }).resolveReferencesFromStore(
         carried,
         "env-1",
         "pkg",
         "this-run",
      );
      expect(carried.up.builtAt).toBe(local.builtAt);
   });
});
