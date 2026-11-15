// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// The window an auto-run spends loading its manifest (MANIFEST_ROWS_READY with
// the manifest recorded): the tables it just built must stay claimed, so an
// orchestrated run of another version cannot overwrite one meanwhile.
import { describe, expect, it } from "bun:test";
import type { Materialization } from "../../../storage/DatabaseInterface";
import { TableClaims } from "./table_claims";

const SHARED = "summary";

/** A `scope: package` auto-run, loading the manifest of the table it built. */
function loadingAutoRun(metadata: Record<string, unknown>): Materialization {
   return {
      id: "a",
      environmentId: "env",
      packageName: "pkg",
      status: "MANIFEST_ROWS_READY",
      version: "1.0.0",
      manifest: {
         strict: false,
         builtAt: "2026-11-15T00:00:00Z",
         entries: {
            s: { sourceEntityId: "s", physicalTableName: SHARED },
         },
      },
      metadata: {
         mode: "auto",
         scope: "package",
         versionId: "1.0.0",
         ...metadata,
      },
      startedAt: null,
      completedAt: null,
      error: null,
      createdAt: new Date(),
      updatedAt: new Date(),
   } as unknown as Materialization;
}

function claims(runs: Materialization[]): TableClaims {
   return new TableClaims(
      () =>
         ({
            listMaterializations: async () => runs,
         }) as never,
   );
}

describe("assertTablesFree while an auto-run loads its manifest", () => {
   it("refuses an orchestrated run's table the loading run still claims", async () => {
      const a = loadingAutoRun({ writesTables: [SHARED] });
      await expect(
         claims([a]).assertTablesFree("env", "pkg", [SHARED], "b", true),
      ).rejects.toThrow(/being written by materialization a/);
   });

   it("refuses it as a shared table too, since the loading run recorded its manifest", async () => {
      const a = loadingAutoRun({});
      await expect(
         claims([a]).assertTablesFree("env", "pkg", [SHARED], "b", true),
      ).rejects.toThrow(/one the versions of package pkg share/);
   });
});
