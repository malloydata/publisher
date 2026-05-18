// Unit tests for the catch-block wiring in
// StorageManager.attachDuckLakeCatalog. The pure helpers from
// `pg_helpers.ts` are tested directly in `pg_helpers.spec.ts`; this file
// covers the integration — that the helpers are invoked in the right
// order and the right places inside `initializeDuckLakeForEnvironment`.
//
// Stubs DuckDBConnection.run instead of using a real DuckDB so we can
// inject libpq-style errors at the ATTACH boundary without standing up a
// real Postgres.
//
// Lives under `tests/unit/` (not `src/`) on purpose: the `src/` unit-spec
// process imports `service/environment_store.spec.ts`, which calls
// `mock.module("../storage/StorageManager", ...)`. Bun's module mocks
// persist process-wide across spec files, so a sibling spec in `src/` that
// `import`s the real StorageManager would get the mock instead. Running
// here puts us in the separate `test:integration` process with a clean
// module cache.
import { describe, expect, it } from "bun:test";
import { ConnectionAuthError } from "../../../src/errors";
import { StorageManager } from "../../../src/storage/StorageManager";

interface PrivateStorageManager {
   duckDbConnection: { run: (sql: string) => Promise<unknown> } | null;
}

const PG_CONFIG = {
   catalogUrl: "postgres:host=h user=u password=hunter2 dbname=catalog",
   dataPath: "gs://bucket/path",
};

function setupWithStubbedConn(runHandler: (sql: string) => Promise<unknown>): {
   sm: StorageManager;
   calls: string[];
} {
   const sm = new StorageManager({ type: "duckdb" });
   const calls: string[] = [];
   const stub = {
      run: async (sql: string): Promise<unknown> => {
         calls.push(sql);
         return runHandler(sql);
      },
   };
   (sm as unknown as PrivateStorageManager).duckDbConnection = stub;
   return { sm, calls };
}

describe("StorageManager.attachDuckLakeCatalog wiring", () => {
   it("classifies libpq auth failure on ATTACH as ConnectionAuthError", async () => {
      const { sm } = setupWithStubbedConn(async (sql) => {
         if (sql.startsWith("ATTACH")) {
            throw new Error(
               'IO Error: Unable to connect to Postgres at "host=h user=u password=hunter2 ...": FATAL:  password authentication failed for user "u"',
            );
         }
         return undefined;
      });

      await expect(
         sm.initializeDuckLakeForEnvironment("env-1", "env-name", PG_CONFIG),
      ).rejects.toBeInstanceOf(ConnectionAuthError);
   });

   it("redacts the embedded password in the classified error", async () => {
      const { sm } = setupWithStubbedConn(async (sql) => {
         if (sql.startsWith("ATTACH")) {
            throw new Error(
               "password authentication failed: tried host=h password=hunter2",
            );
         }
         return undefined;
      });

      try {
         await sm.initializeDuckLakeForEnvironment(
            "env-1",
            "env-name",
            PG_CONFIG,
         );
         throw new Error("expected ATTACH to throw");
      } catch (e) {
         expect(e).toBeInstanceOf(ConnectionAuthError);
         expect((e as Error).message).toContain("password=***");
         expect((e as Error).message).not.toContain("hunter2");
      }
   });

   it("injects connect_timeout into PG catalogUrl before ATTACH", async () => {
      const { sm, calls } = setupWithStubbedConn(async () => undefined);

      await sm.initializeDuckLakeForEnvironment("env-1", "env-name", PG_CONFIG);

      const attachSql = calls.find((s) => s.startsWith("ATTACH"));
      expect(attachSql).toBeDefined();
      expect(attachSql).toContain("connect_timeout=");
   });

   it("honors PG_CONNECT_TIMEOUT_SECONDS env override in the emitted SQL", async () => {
      const original = process.env.PG_CONNECT_TIMEOUT_SECONDS;
      process.env.PG_CONNECT_TIMEOUT_SECONDS = "17";
      try {
         const { sm, calls } = setupWithStubbedConn(async () => undefined);
         await sm.initializeDuckLakeForEnvironment(
            "env-1",
            "env-name",
            PG_CONFIG,
         );
         const attachSql = calls.find((s) => s.startsWith("ATTACH"));
         expect(attachSql).toContain("connect_timeout=17");
      } finally {
         if (original === undefined) {
            delete process.env.PG_CONNECT_TIMEOUT_SECONDS;
         } else {
            process.env.PG_CONNECT_TIMEOUT_SECONDS = original;
         }
      }
   });

   it("leaves a non-PG catalogUrl untouched (no connect_timeout)", async () => {
      const { sm, calls } = setupWithStubbedConn(async () => undefined);
      const sqliteConfig = {
         catalogUrl: "sqlite:/tmp/x.db",
         dataPath: "/tmp/data",
      };

      await sm.initializeDuckLakeForEnvironment(
         "env-1",
         "env-name",
         sqliteConfig,
      );

      const attachSql = calls.find((s) => s.startsWith("ATTACH"));
      expect(attachSql).toBeDefined();
      expect(attachSql).not.toContain("connect_timeout");
   });

   it("rethrows non-auth errors from ATTACH unchanged (preserves cause)", async () => {
      const original = new Error("disk I/O error: read failed");
      const { sm } = setupWithStubbedConn(async (sql) => {
         if (sql.startsWith("ATTACH")) throw original;
         return undefined;
      });

      await expect(
         sm.initializeDuckLakeForEnvironment("env-1", "env-name", PG_CONFIG),
      ).rejects.toBe(original);
   });

   it("does not call connect_timeout injection when catalogUrl lacks postgres: prefix", async () => {
      // Sanity: the isPostgres branch is detected purely by string prefix.
      // A keyword-form string without the prefix shouldn't be misclassified.
      const { sm, calls } = setupWithStubbedConn(async () => undefined);
      const ambiguousConfig = {
         catalogUrl: "host=h dbname=d", // no scheme prefix at all
         dataPath: "/tmp/data",
      };

      await sm.initializeDuckLakeForEnvironment(
         "env-1",
         "env-name",
         ambiguousConfig,
      );

      const attachSql = calls.find((s) => s.startsWith("ATTACH"));
      expect(attachSql).not.toContain("connect_timeout");
   });
});
