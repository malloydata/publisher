// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A run's status is written only over a status that admits the move, in one
 * statement, so a writer that read the record before another changed it
 * cannot undo that change: a transition, and a background run's settle. Over
 * the real run store (DuckDB, the store's only backend).
 */

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { InvalidStateTransitionError } from "../../../errors";
import type { ResourceRepository } from "../../../storage/DatabaseInterface";
import { createDuckDBRepository } from "../../../storage/duckdb/DuckDBRepository";
import { DuckDBConnection } from "../../../storage/duckdb/DuckDBConnection";
import { initializeSchema } from "../../../storage/duckdb/schema";
import { RunLifecycle } from "./run_lifecycle";

const ENV_ID = "env-race";

let db: DuckDBConnection;
let repository: ResourceRepository;

beforeEach(async () => {
   db = new DuckDBConnection(":memory:");
   await db.initialize();
   await initializeSchema(db);
   await db.run(
      `INSERT INTO environments (id, name, path, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?)`,
      [
         ENV_ID,
         "race-env",
         "/tmp/race-env",
         "2026-01-01T00:00:00.000Z",
         "2026-01-01T00:00:00.000Z",
      ],
   );
   repository = createDuckDBRepository(db);
});

afterEach(async () => {
   await db.close();
});

async function statusOf(id: string): Promise<string | undefined> {
   return (await repository.getMaterializationById(id))?.status;
}

describe("the run store's conditional status write", () => {
   it("writes only over a listed status, and answers null otherwise", async () => {
      const run = await repository.createMaterialization(ENV_ID, "pkg");
      expect(
         await repository.updateMaterialization(
            run.id,
            { status: "CANCELLED", error: "Cancelled" },
            { fromStatuses: ["MANIFEST_ROWS_READY"] },
         ),
      ).toBeNull();
      expect(await statusOf(run.id)).toBe("PENDING");
      const moved = await repository.updateMaterialization(
         run.id,
         { status: "CANCELLED", error: "Cancelled" },
         { fromStatuses: ["PENDING", "MANIFEST_ROWS_READY"] },
      );
      expect(moved?.status).toBe("CANCELLED");
      expect(moved?.error).toBe("Cancelled");
   });

   it("frees the run's slot when it settles, as the unconditional write does", async () => {
      const run = await repository.createMaterialization(ENV_ID, "pkg");
      await repository.updateMaterialization(
         run.id,
         { status: "FAILED" },
         { fromStatuses: ["PENDING"] },
      );
      expect(
         await repository.getActiveMaterialization(ENV_ID, "pkg"),
      ).toBeNull();
      // The slot is free for the next run.
      await repository.createMaterialization(ENV_ID, "pkg");
   });

   it("lets exactly one of two racing settles land", async () => {
      const run = await repository.createMaterialization(ENV_ID, "pkg");
      const [cancel, fail] = await Promise.all([
         repository.updateMaterialization(
            run.id,
            { status: "CANCELLED" },
            { fromStatuses: ["PENDING", "MANIFEST_ROWS_READY"] },
         ),
         repository.updateMaterialization(
            run.id,
            { status: "FAILED" },
            { fromStatuses: ["PENDING", "MANIFEST_ROWS_READY"] },
         ),
      ]);
      expect([cancel, fail].filter((r) => r !== null)).toHaveLength(1);
      expect(["CANCELLED", "FAILED"]).toContain(
         (await statusOf(run.id)) as string,
      );
   });

   it("answers null for a run that is gone", async () => {
      expect(
         await repository.updateMaterialization(
            "no-such-run",
            { status: "FAILED" },
            { fromStatuses: ["PENDING"] },
         ),
      ).toBeNull();
   });
});

describe("a transition over a status that changed after it was read", () => {
   it("refuses, and leaves the newer status, where it would have overwritten it", async () => {
      const lifecycle = new RunLifecycle(() => repository);
      const run = await repository.createMaterialization(ENV_ID, "pkg");
      // Read while PENDING; another writer cancels the run before the write.
      const read = (await repository.getMaterializationById(run.id))!;
      await repository.updateMaterialization(run.id, { status: "CANCELLED" });
      await expect(
         lifecycle.transition(run.id, "MANIFEST_ROWS_READY", {}, read),
      ).rejects.toThrow(InvalidStateTransitionError);
      expect(await statusOf(run.id)).toBe("CANCELLED");
   });

   it("still lands when the status moved to another that admits it", async () => {
      const lifecycle = new RunLifecycle(() => repository);
      const run = await repository.createMaterialization(ENV_ID, "pkg");
      const read = (await repository.getMaterializationById(run.id))!;
      await repository.updateMaterialization(run.id, {
         status: "MANIFEST_ROWS_READY",
      });
      const cancelled = await lifecycle.transition(
         run.id,
         "CANCELLED",
         { error: "Cancelled" },
         read,
      );
      expect(cancelled.status).toBe("CANCELLED");
   });
});

describe("a background run's settle after its record settled", () => {
   it("keeps a status recorded between the settle's read and its write", async () => {
      const run = await repository.createMaterialization(ENV_ID, "pkg");
      // The record commits between the settle's read and its write.
      const racing: ResourceRepository = {
         ...repository,
         getMaterializationById: async (id: string) => {
            const current = await repository.getMaterializationById(id);
            await repository.updateMaterialization(id, {
               status: "MANIFEST_ROWS_READY",
            });
            await repository.updateMaterialization(id, {
               status: "MANIFEST_FILE_READY",
            });
            return current;
         },
      };
      const lifecycle = new RunLifecycle(() => racing);
      const settled = new Promise<void>((resolve) =>
         lifecycle.runInBackground(
            run.id,
            async () => {
               throw new Error("a late failure");
            },
            resolve,
         ),
      );
      await settled;
      const record = await repository.getMaterializationById(run.id);
      expect(record?.status).toBe("MANIFEST_FILE_READY");
      expect(record?.error).toBeNull();
   });
});
