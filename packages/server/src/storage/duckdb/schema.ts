import { logger } from "../../logger";
import { DuckDBConnection } from "./DuckDBConnection";

export async function initializeSchema(
   db: DuckDBConnection,
   force: boolean = false,
): Promise<void> {
   const initialized = await db.isInitialized();

   if (initialized && !force) {
      return;
   }

   if (force) {
      logger.info(
         "Reinitializing database schema dropping and recreating all tables",
      );
      await dropAllTables(db);
   } else {
      logger.info("Creating database schema for the first time...");
   }

   // Environments table
   await db.run(`
    CREATE TABLE IF NOT EXISTS environments (
      id VARCHAR PRIMARY KEY,
      name VARCHAR NOT NULL UNIQUE,
      path VARCHAR NOT NULL,
      description VARCHAR,
      metadata JSON,
      created_at TIMESTAMP NOT NULL,
      updated_at TIMESTAMP NOT NULL
    )
  `);

   // Packages table
   await db.run(`
    CREATE TABLE IF NOT EXISTS packages (
      id VARCHAR PRIMARY KEY,
      environment_id VARCHAR NOT NULL,
      name VARCHAR NOT NULL,
      description VARCHAR,
      manifest_path VARCHAR NOT NULL,
      metadata JSON,
      created_at TIMESTAMP NOT NULL,
      updated_at TIMESTAMP NOT NULL,
      FOREIGN KEY (environment_id) REFERENCES environments(id),
      UNIQUE (environment_id, name)
    )
  `);

   // Connections table
   await db.run(`
    CREATE TABLE IF NOT EXISTS connections (
      id VARCHAR PRIMARY KEY,
      environment_id VARCHAR NOT NULL,
      name VARCHAR NOT NULL,
      type VARCHAR NOT NULL,
      config JSON NOT NULL,
      created_at TIMESTAMP NOT NULL,
      updated_at TIMESTAMP NOT NULL,
      FOREIGN KEY (environment_id) REFERENCES environments(id),
      UNIQUE (environment_id, name)
    )
  `);

   // Materializations table.
   //
   // `active_key` enforces at-most-one active (PENDING or RUNNING)
   // materialization per (environment, package) at the DB layer. It is set to
   // `{environment_id}|{package_name}` while the row is active and cleared
   // to NULL on transition to any terminal state. A unique index on
   // `active_key` (see below) makes the insert-then-check race impossible —
   // a second concurrent create fails with a constraint violation, which the
   // service layer translates to `MaterializationConflictError`.
   await db.run(`
    CREATE TABLE IF NOT EXISTS materializations (
      id VARCHAR PRIMARY KEY,
      environment_id VARCHAR NOT NULL,
      package_name VARCHAR NOT NULL,
      status VARCHAR NOT NULL,
      active_key VARCHAR,
      started_at TIMESTAMP,
      completed_at TIMESTAMP,
      error TEXT,
      metadata JSON,
      created_at TIMESTAMP NOT NULL,
      updated_at TIMESTAMP NOT NULL,
      FOREIGN KEY (environment_id) REFERENCES environments(id)
    )
  `);

   // Build manifests table
   await db.run(`
    CREATE TABLE IF NOT EXISTS build_manifests (
      id VARCHAR PRIMARY KEY,
      environment_id VARCHAR NOT NULL,
      package_name VARCHAR NOT NULL,
      build_id VARCHAR NOT NULL,
      table_name VARCHAR NOT NULL,
      source_name VARCHAR NOT NULL,
      connection_name VARCHAR NOT NULL,
      created_at TIMESTAMP NOT NULL,
      updated_at TIMESTAMP NOT NULL,
      FOREIGN KEY (environment_id) REFERENCES environments(id),
      UNIQUE (environment_id, package_name, build_id)
    )
  `);

   // Create indexes for better query performance
   await db.run(
      "CREATE INDEX IF NOT EXISTS idx_packages_environment_id ON packages(environment_id)",
   );
   await db.run(
      "CREATE INDEX IF NOT EXISTS idx_connections_environment_id ON connections(environment_id)",
   );
   await db.run(
      "CREATE INDEX IF NOT EXISTS idx_materializations_environment_package ON materializations(environment_id, package_name)",
   );
   await db.run(
      "CREATE UNIQUE INDEX IF NOT EXISTS idx_materializations_active_key ON materializations(active_key)",
   );
   await db.run(
      "CREATE INDEX IF NOT EXISTS idx_build_manifests_environment_package ON build_manifests(environment_id, package_name)",
   );
}

async function dropAllTables(db: DuckDBConnection): Promise<void> {
   const tables = [
      "build_manifests",
      "materializations",
      "packages",
      "connections",
      "environments",
   ];

   logger.info("Dropping tables:", tables.join(", "));

   for (const table of tables) {
      try {
         await db.run(`DROP TABLE IF EXISTS ${table} `);
         logger.info(`Dropped table: ${table}`);
      } catch (err) {
         logger.warn(` Warning: Could not drop table ${table}:`, err);
      }
   }
}
