import { describe, expect, it } from "bun:test";
import type { DuckDBConnection } from "./DuckDBConnection";
import { MaterializationRepository } from "./MaterializationRepository";

// Capture the SQL + params of the last query without touching a real DuckDB.
function fakeRepo() {
   const calls: Array<{ sql: string; params: unknown[] }> = [];
   const db = {
      all: async (sql: string, params: unknown[]) => {
         calls.push({ sql, params });
         return [];
      },
   } as unknown as DuckDBConnection;
   return { repo: new MaterializationRepository(db), calls };
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
