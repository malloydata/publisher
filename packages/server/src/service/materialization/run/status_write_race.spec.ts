// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A run's status is written only over a status that admits the move, in one
 * statement, so a writer that read the record before another changed it
 * cannot undo that change: a transition, and a background run's settle. Over
 * the real run store (DuckDB, the store's only backend).
 */

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import {
   InvalidStateTransitionError,
   MaterializationNotFoundError,
} from "../../../errors";
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
         await repository.updateMaterializationFrom(
            run.id,
            { status: "CANCELLED", error: "Cancelled" },
            ["MANIFEST_ROWS_READY"],
         ),
      ).toBeNull();
      expect(await statusOf(run.id)).toBe("PENDING");
      const moved = await repository.updateMaterializationFrom(
         run.id,
         { status: "CANCELLED", error: "Cancelled" },
         ["PENDING", "MANIFEST_ROWS_READY"],
      );
      expect(moved?.status).toBe("CANCELLED");
      expect(moved?.error).toBe("Cancelled");
   });

   it("frees the run's slot when it settles, as the unconditional write does", async () => {
      const run = await repository.createMaterialization(ENV_ID, "pkg");
      await repository.updateMaterializationFrom(run.id, { status: "FAILED" }, [
         "PENDING",
      ]);
      expect(
         await repository.getActiveMaterialization(ENV_ID, "pkg"),
      ).toBeNull();
      // The slot is free for the next run.
      await repository.createMaterialization(ENV_ID, "pkg");
   });

   it("lets exactly one of two racing settles land", async () => {
      const run = await repository.createMaterialization(ENV_ID, "pkg");
      const active = ["PENDING", "MANIFEST_ROWS_READY"] as const;
      const [cancel, fail] = await Promise.all([
         repository.updateMaterializationFrom(
            run.id,
            { status: "CANCELLED" },
            active,
         ),
         repository.updateMaterializationFrom(
            run.id,
            { status: "FAILED" },
            active,
         ),
      ]);
      expect([cancel, fail].filter((r) => r !== null)).toHaveLength(1);
      expect(["CANCELLED", "FAILED"]).toContain(
         (await statusOf(run.id)) as string,
      );
   });

   it("answers null for a run that is gone", async () => {
      expect(
         await repository.updateMaterializationFrom(
            "no-such-run",
            { status: "FAILED" },
            ["PENDING"],
         ),
      ).toBeNull();
   });
});

describe("a transition", () => {
   it("refuses, and leaves the newer status, when another writer moved it first", async () => {
      const run = await repository.createMaterialization(ENV_ID, "pkg");
      // Another writer cancels the run between the caller's decision and
      // its write.
      const racing: ResourceRepository = {
         ...repository,
         updateMaterializationFrom: async (id, updates, fromStatuses) => {
            await repository.updateMaterialization(id, { status: "CANCELLED" });
            return repository.updateMaterializationFrom(
               id,
               updates,
               fromStatuses,
            );
         },
      };
      await expect(
         new RunLifecycle(() => racing).transition(
            run.id,
            "MANIFEST_ROWS_READY",
         ),
      ).rejects.toThrow(InvalidStateTransitionError);
      expect(await statusOf(run.id)).toBe("CANCELLED");
   });

   it("lands from any status that admits it", async () => {
      const lifecycle = new RunLifecycle(() => repository);
      const run = await repository.createMaterialization(ENV_ID, "pkg");
      await repository.updateMaterialization(run.id, {
         status: "MANIFEST_ROWS_READY",
      });
      const cancelled = await lifecycle.transition(run.id, "CANCELLED", {
         error: "Cancelled",
      });
      expect(cancelled.status).toBe("CANCELLED");
   });

   it("answers not found for a run that is gone", async () => {
      const lifecycle = new RunLifecycle(() => repository);
      const run = await repository.createMaterialization(ENV_ID, "pkg");
      await repository.deleteMaterialization(run.id);
      await expect(lifecycle.transition(run.id, "CANCELLED")).rejects.toThrow(
         MaterializationNotFoundError,
      );
   });
});

/** Settle a background run of `id` that throws, its abort first when `cancel`. */
async function settle(
   lifecycle: RunLifecycle,
   id: string,
   cancel = false,
): Promise<void> {
   await new Promise<void>((resolve) =>
      lifecycle.runInBackground(
         id,
         async () => {
            if (cancel) lifecycle.runningAbortControllers.get(id)!.abort();
            throw new Error("a late failure");
         },
         resolve,
      ),
   );
}

describe("a background run's settle", () => {
   it("keeps a status another writer recorded first", async () => {
      const run = await repository.createMaterialization(ENV_ID, "pkg");
      // The record commits just before the settle's write.
      const racing: ResourceRepository = {
         ...repository,
         updateMaterializationFrom: async (id, updates, fromStatuses) => {
            await repository.updateMaterialization(id, {
               status: "MANIFEST_ROWS_READY",
            });
            await repository.updateMaterialization(id, {
               status: "MANIFEST_FILE_READY",
            });
            return repository.updateMaterializationFrom(
               id,
               updates,
               fromStatuses,
            );
         },
      };
      await settle(new RunLifecycle(() => racing), run.id);
      const record = await repository.getMaterializationById(run.id);
      expect(record?.status).toBe("MANIFEST_FILE_READY");
      expect(record?.error).toBeNull();
   });

   it("still records the status when its record cannot be read", async () => {
      const run = await repository.createMaterialization(ENV_ID, "pkg");
      const unreadable: ResourceRepository = {
         ...repository,
         getMaterializationById: async () => {
            throw new Error("store unavailable");
         },
      };
      // A cancel that raced a failure has metadata to merge, so it reads.
      await settle(new RunLifecycle(() => unreadable), run.id, true);
      const record = await repository.getMaterializationById(run.id);
      expect(record?.status).toBe("CANCELLED");
      expect(record?.error).toBe("Cancelled");
   });

   it("writes nothing for a run that is gone", async () => {
      const run = await repository.createMaterialization(ENV_ID, "pkg");
      await repository.deleteMaterialization(run.id);
      await settle(new RunLifecycle(() => repository), run.id);
      expect(await repository.getMaterializationById(run.id)).toBeNull();
   });
});
