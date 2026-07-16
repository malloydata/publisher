import { afterEach, describe, expect, it } from "bun:test";
import { DuckDBConnection } from "./DuckDBConnection";
import { MaterializationRepository } from "./MaterializationRepository";
import { initializeSchema } from "./schema";

// Capture the SQL + params of reads (all) and writes (run) without touching a
// real DuckDB.
function fakeRepo() {
   const calls: Array<{ sql: string; params: unknown[] }> = [];
   const runCalls: Array<{ sql: string; params: unknown[] }> = [];
   const db = {
      all: async (sql: string, params: unknown[]) => {
         calls.push({ sql, params });
         return [];
      },
      run: async (sql: string, params: unknown[]) => {
         runCalls.push({ sql, params });
      },
   } as unknown as DuckDBConnection;
   return { repo: new MaterializationRepository(db), calls, runCalls };
}

describe("MaterializationRepository list bounds", () => {
   it("per-package list is unbounded when no limit is given (no LIMIT clause)", async () => {
      const { repo, calls } = fakeRepo();
      await repo.list("env-1", "pkg-a");
      expect(calls[0].sql).not.toContain("LIMIT");
      expect(calls[0].sql).not.toContain("OFFSET");
      expect(calls[0].params).toEqual(["env-1", "pkg-a"]);
   });

   it("per-package list honors an explicit limit/offset", async () => {
      const { repo, calls } = fakeRepo();
      await repo.list("env-1", "pkg-a", { limit: 5, offset: 10 });
      expect(calls[0].sql).toContain("LIMIT ?");
      expect(calls[0].sql).toContain("OFFSET ?");
      expect(calls[0].params).toEqual(["env-1", "pkg-a", 5, 10]);
   });

   it("environment-scoped list always applies a default cap (500)", async () => {
      const { repo, calls } = fakeRepo();
      await repo.listByEnvironment("env-1");
      expect(calls[0].sql).toContain("LIMIT ?");
      expect(calls[0].params).toEqual(["env-1", 500]);
   });

   it("environment-scoped list honors an explicit limit over the default", async () => {
      const { repo, calls } = fakeRepo();
      await repo.listByEnvironment("env-1", { limit: 25 });
      expect(calls[0].params).toEqual(["env-1", 25]);
   });
});

// The restart-recovery anchor (MaterializationScheduler.recoveryAnchor) hangs on
// getLatestScheduledFireAt's `json_extract_string(metadata,'$.trigger')` filter,
// which a fake db can't exercise. Drive the real query against an in-memory
// DuckDB so the trigger filter and ORDER BY are covered, not just the SQL shape.
describe("MaterializationRepository.getLatestScheduledFireAt (real DuckDB)", () => {
   const ENV_ID = "env-sched";
   const PKG = "pkg-a";
   const dbs: DuckDBConnection[] = [];

   afterEach(async () => {
      while (dbs.length) await dbs.pop()!.close();
   });

   async function freshRepo(): Promise<{
      db: DuckDBConnection;
      repo: MaterializationRepository;
   }> {
      const db = new DuckDBConnection(":memory:");
      dbs.push(db);
      await db.initialize();
      await initializeSchema(db);
      // materializations.environment_id is a FK to environments(id).
      await db.run(
         `INSERT INTO environments (id, name, path, created_at, updated_at)
          VALUES (?, ?, ?, ?, ?)`,
         [
            ENV_ID,
            "sched-env",
            "/tmp/sched-env",
            "2026-01-01T00:00:00.000Z",
            "2026-01-01T00:00:00.000Z",
         ],
      );
      return { db, repo: new MaterializationRepository(db) };
   }

   // Insert a terminal run directly (active_key NULL so repeated inserts for the
   // same package don't trip the unique active-key index). `trigger === undefined`
   // stores metadata NULL; `null` stores a trigger-less object; a string stores
   // that trigger.
   async function insertRun(
      db: DuckDBConnection,
      id: string,
      createdAt: string,
      trigger: string | null | undefined,
   ): Promise<void> {
      const metadata =
         trigger === undefined
            ? null
            : JSON.stringify(
                 trigger === null ? { note: "manual" } : { trigger },
              );
      await db.run(
         `INSERT INTO materializations
            (id, environment_id, package_name, status, active_key, metadata, manifest, created_at, updated_at)
          VALUES (?, ?, ?, 'MANIFEST_FILE_READY', NULL, ?, NULL, ?, ?)`,
         [id, ENV_ID, PKG, metadata, createdAt, createdAt],
      );
   }

   const day = (d: string) => `2026-${d}T12:00:00.000Z`;

   it("returns the created_at of the newest SCHEDULER run", async () => {
      const { db, repo } = await freshRepo();
      await insertRun(db, "s1", day("01-15"), "SCHEDULER");
      await insertRun(db, "s2", day("03-15"), "SCHEDULER");
      const at = await repo.getLatestScheduledFireAt(ENV_ID, PKG);
      expect(at).not.toBeNull();
      expect(at!.toISOString().slice(0, 10)).toBe("2026-03-15");
   });

   it("ignores ON_DEMAND and trigger-less runs even when they are newer", async () => {
      const { db, repo } = await freshRepo();
      await insertRun(db, "s1", day("02-01"), "SCHEDULER"); // the only match
      await insertRun(db, "d1", day("06-01"), "ON_DEMAND"); // newer, wrong trigger
      await insertRun(db, "n1", day("07-01"), null); // newer, no trigger key
      await insertRun(db, "m1", day("08-01"), undefined); // newer, metadata NULL
      const at = await repo.getLatestScheduledFireAt(ENV_ID, PKG);
      expect(at).not.toBeNull();
      expect(at!.toISOString().slice(0, 10)).toBe("2026-02-01");
   });

   it("returns null when the package has no SCHEDULER run", async () => {
      const { db, repo } = await freshRepo();
      await insertRun(db, "d1", day("06-01"), "ON_DEMAND");
      await insertRun(db, "m1", day("07-01"), undefined);
      expect(await repo.getLatestScheduledFireAt(ENV_ID, PKG)).toBeNull();
   });

   it("scopes to the requested package (ignores another package's SCHEDULER run)", async () => {
      const { db, repo } = await freshRepo();
      await db.run(
         `INSERT INTO materializations
            (id, environment_id, package_name, status, active_key, metadata, manifest, created_at, updated_at)
          VALUES (?, ?, 'other-pkg', 'MANIFEST_FILE_READY', NULL, ?, NULL, ?, ?)`,
         [
            "x1",
            ENV_ID,
            JSON.stringify({ trigger: "SCHEDULER" }),
            day("09-01"),
            day("09-01"),
         ],
      );
      expect(await repo.getLatestScheduledFireAt(ENV_ID, PKG)).toBeNull();
   });
});

// The publisher tracks materialization *records*; dropping the physical tables
// is opt-in (deleteMaterialization({ dropTables })) and otherwise the caller's
// responsibility. These cascade deletes therefore remove records only — they
// must never issue DDL — so a materialized table intentionally outlives the
// record when an environment or package is deleted. Locking that in guards the
// contract against a future refactor that "helpfully" adds a DROP here (which
// would fire against a possibly-already-torn-down connection on env delete).
describe("MaterializationRepository cascade deletes are records-only", () => {
   it("deleteByEnvironmentId issues a single DELETE and no DDL", async () => {
      const { repo, runCalls } = fakeRepo();
      await repo.deleteByEnvironmentId("env-1");
      expect(runCalls.length).toBe(1);
      expect(runCalls[0].sql).toContain("DELETE FROM materializations");
      expect(runCalls[0].sql).not.toContain("DROP");
      expect(runCalls[0].params).toEqual(["env-1"]);
   });

   it("deleteByPackage issues a single DELETE and no DDL", async () => {
      const { repo, runCalls } = fakeRepo();
      await repo.deleteByPackage("env-1", "pkg-a");
      expect(runCalls.length).toBe(1);
      expect(runCalls[0].sql).toContain("DELETE FROM materializations");
      expect(runCalls[0].sql).not.toContain("DROP");
      expect(runCalls[0].params).toEqual(["env-1", "pkg-a"]);
   });
});
