import { afterEach, describe, expect, it } from "bun:test";
import { DuckDBConnection } from "./DuckDBConnection";
import { DuckDBRepository } from "./DuckDBRepository";
import { MaterializationDestinationRepository } from "./MaterializationDestinationRepository";
import { initializeSchema } from "./schema";

/**
 * Driven against a real in-memory DuckDB rather than a fake, so the schema, the
 * `(environment_id, name)` uniqueness, and the JSON round-trip of a config are
 * all covered — the parts a SQL-shape assertion would miss.
 */
describe("MaterializationDestinationRepository", () => {
   const dbs: DuckDBConnection[] = [];
   const ENV_ID = "env-1";
   const OTHER_ENV_ID = "env-2";

   afterEach(async () => {
      while (dbs.length) await dbs.pop()!.close();
   });

   const destinationConfig = (bucket: string) => ({
      name: "managed",
      type: "ducklake",
      ducklakeConnection: {
         catalog: {
            postgresConnection: {
               host: "catalog.internal",
               password: "catalog-secret",
            },
         },
         storage: { bucketUrl: bucket },
      },
   });

   async function freshDb(): Promise<DuckDBConnection> {
      const db = new DuckDBConnection(":memory:");
      dbs.push(db);
      await db.initialize();
      await initializeSchema(db);
      for (const [id, name] of [
         [ENV_ID, "env-one"],
         [OTHER_ENV_ID, "env-two"],
      ]) {
         await db.run(
            `INSERT INTO environments (id, name, path, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?)`,
            [
               id,
               name,
               `/tmp/${name}`,
               "2026-01-01T00:00:00.000Z",
               "2026-01-01T00:00:00.000Z",
            ],
         );
      }
      return db;
   }

   it("round-trips a destination config through the store", async () => {
      const repo = new MaterializationDestinationRepository(await freshDb());

      await repo.upsert({
         environmentId: ENV_ID,
         name: "managed",
         type: "ducklake",
         config: destinationConfig("gs://managed-tier/org_a"),
      });

      const [stored] = await repo.list(ENV_ID);
      expect(stored.name).toBe("managed");
      expect(stored.type).toBe("ducklake");
      expect(stored.config).toEqual(
         destinationConfig("gs://managed-tier/org_a"),
      );
   });

   it("replaces the row for a name instead of duplicating it", async () => {
      const repo = new MaterializationDestinationRepository(await freshDb());

      await repo.upsert({
         environmentId: ENV_ID,
         name: "managed",
         type: "ducklake",
         config: destinationConfig("gs://managed-tier/org_a"),
      });
      await repo.upsert({
         environmentId: ENV_ID,
         name: "managed",
         type: "ducklake",
         config: destinationConfig("gs://managed-tier/org_b"),
      });

      const rows = await repo.list(ENV_ID);
      expect(rows).toHaveLength(1);
      expect(
         (rows[0].config as ReturnType<typeof destinationConfig>)
            .ducklakeConnection.storage.bucketUrl,
      ).toBe("gs://managed-tier/org_b");
   });

   it("scopes a destination to its environment", async () => {
      const repo = new MaterializationDestinationRepository(await freshDb());

      await repo.upsert({
         environmentId: ENV_ID,
         name: "managed",
         type: "ducklake",
         config: destinationConfig("gs://managed-tier/org_a"),
      });

      expect(await repo.list(OTHER_ENV_ID)).toEqual([]);
      expect(await repo.getByName(OTHER_ENV_ID, "managed")).toBeNull();
      expect((await repo.getByName(ENV_ID, "managed"))?.name).toBe("managed");
   });

   it("deletes a destination by id and by environment", async () => {
      const repo = new MaterializationDestinationRepository(await freshDb());

      const first = await repo.upsert({
         environmentId: ENV_ID,
         name: "managed",
         type: "ducklake",
         config: destinationConfig("gs://managed-tier/org_a"),
      });
      await repo.upsert({
         environmentId: ENV_ID,
         name: "other",
         type: "ducklake",
         config: destinationConfig("gs://managed-tier/org_b"),
      });

      await repo.deleteById(first.id);
      expect((await repo.list(ENV_ID)).map((row) => row.name)).toEqual([
         "other",
      ]);

      await repo.deleteByEnvironmentId(ENV_ID);
      expect(await repo.list(ENV_ID)).toEqual([]);
   });

   it("takes destinations with the environment when it is deleted", async () => {
      // The rows carry a foreign key to environments(id), so a delete that
      // skipped them would fail the constraint and leave the environment behind.
      const db = await freshDb();
      const repository = new DuckDBRepository(db);

      await repository.upsertMaterializationDestination({
         environmentId: ENV_ID,
         name: "managed",
         type: "ducklake",
         config: destinationConfig("gs://managed-tier/org_a"),
      });

      await repository.deleteEnvironment(ENV_ID);

      expect(await repository.listMaterializationDestinations(ENV_ID)).toEqual(
         [],
      );
      expect(await repository.getEnvironmentById(ENV_ID)).toBeNull();
   });
});
