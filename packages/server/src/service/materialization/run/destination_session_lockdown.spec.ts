// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Every session on a storage destination is confined to that destination by
 * default (duckdb_lockdown.ts): external access off, so its work reads and
 * writes only the destination's own paths. A caller that must set up more
 * first (a build that federates its source) opts out and locks it itself.
 */

import { afterAll, describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { DuckDBConnection } from "@malloydata/db-duckdb";
import { withDestinationSession } from "./materialization_build_session";

const environmentPath = mkdtempSync(join(tmpdir(), "dest-lockdown-"));
afterAll(() => rmSync(environmentPath, { recursive: true, force: true }));

const destination = {
   destinationName: "lake",
   destinationConnection: { name: "lake", type: "duckdb" } as never,
   environmentPath,
};

async function externalAccess(session: DuckDBConnection): Promise<unknown> {
   const result = (await session.runSQL(
      "SELECT current_setting('enable_external_access') AS on",
   )) as unknown as { rows: { on: unknown }[] };
   return result.rows[0]?.on;
}

describe("a destination session", () => {
   it("is confined to its destination before its work runs", async () => {
      expect(
         await withDestinationSession(
            destination,
            { purpose: "view" },
            externalAccess,
         ),
      ).toBe(false);
   });

   it("reads no file outside its destination", async () => {
      const outside = join(mkdtempSync(join(tmpdir(), "outside-")), "t.csv");
      writeFileSync(outside, "a\n1\n");
      const read = withDestinationSession(
         destination,
         { purpose: "view" },
         (session) => session.runSQL(`SELECT * FROM read_csv('${outside}')`),
      );
      await expect(read).rejects.toThrow();
      rmSync(outside, { force: true });
   });

   it("is left to its caller to confine, when the caller says so", async () => {
      expect(
         await withDestinationSession(
            destination,
            { purpose: "view", lockdown: "caller" },
            externalAccess,
         ),
      ).toBe(true);
   });
});
