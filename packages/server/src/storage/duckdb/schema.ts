import { logger } from "../../logger";
import { DuckDBConnection } from "./DuckDBConnection";

export async function initializeSchema(
   db: DuckDBConnection,
   force: boolean = false,
): Promise<void> {
   const initialized = await db.isInitialized();

   if (force) {
      logger.info(
         "Reinitializing database schema dropping and recreating all tables",
      );
      await dropAllTables(db);
   } else if (!initialized) {
      // TODO: Remove this during projects cleanup
      // If a pre-rename `projects` schema is on disk, the new
      // CREATE TABLE IF NOT EXISTS pass below would silently leave child
      // tables on the old `project_id` column and the first query against
      // `environment_id` would crash. Drop the legacy tables (with a loud
      // warning) so the fresh schema can be created cleanly. This is
      // destructive: operators upgrading should re-create their environments
      // and packages via the API after the upgrade.
      await dropLegacyProjectSchema(db);
      logger.info("Creating database schema for the first time...");
   }
   // Always fall through to the CREATE TABLE IF NOT EXISTS pass below.
   // The statements are idempotent and let an already-initialized DB pick
   // up new tables added in later builds (e.g., the `themes` table for
   // the in-app Theme Editor) without forcing operators to run --init
   // and lose their existing environments / packages / materializations.

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
   // `active_key` enforces at-most-one active (non-terminal) materialization
   // per (environment, package) at the DB layer. It is set to
   // `{environment_id}|{package_name}` while the row is active and cleared
   // to NULL on transition to any terminal state. A unique index on
   // `active_key` (see below) makes the insert-then-check race impossible —
   // a second concurrent create fails with a constraint violation, which the
   // service layer translates to `MaterializationConflictError`.
   // `manifest` is a JSON blob holding the build output returned inline on the
   // resource.
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
      manifest JSON,
      created_at TIMESTAMP NOT NULL,
      updated_at TIMESTAMP NOT NULL,
      FOREIGN KEY (environment_id) REFERENCES environments(id)
    )
  `);

   // Themes table.
   //
   // Singleton storage for the instance-wide theme edited by the in-app
   // Theme Editor. At most one row (id = "default"); we use INSERT ON
   // CONFLICT to keep the table effectively a key/value cell.
   //
   // publisher.config.json's optional `theme` block is a BOOT SEED only —
   // on first run, if this table is empty, ThemeStore writes the seed
   // here. Subsequent reads come from this table so the editor's saves
   // win over the file.
   await db.run(`
    CREATE TABLE IF NOT EXISTS themes (
      id VARCHAR PRIMARY KEY,
      payload JSON NOT NULL,
      created_at TIMESTAMP NOT NULL,
      updated_at TIMESTAMP NOT NULL
    )
  `);

   await createEntityEmbeddingsTable(db);

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
}

/**
 * Vector cache for semantic `malloy_getContext` retrieval (see
 * mcp/tools/embedding_index.ts). One row per discoverable model entity;
 * the primary key mirrors the tool's in-memory dedup key. Rows are
 * content-addressed (`content_hash` over the embedded text, compared
 * together with `embedding_model` + `dims`), so staleness self-heals on
 * the next index sync and wiping the table only ever costs re-embedding.
 * `embedding` is a LIST column searched with list_cosine_similarity; at
 * the entity counts a single Publisher serves, a brute-force scan is
 * faster and simpler than a vector-index extension.
 */
export async function createEntityEmbeddingsTable(
   db: DuckDBConnection,
): Promise<void> {
   await db.run(`
    CREATE TABLE IF NOT EXISTS entity_embeddings (
      environment_name VARCHAR NOT NULL,
      package_name VARCHAR NOT NULL,
      entity_kind VARCHAR NOT NULL,
      entity_source VARCHAR NOT NULL,
      entity_name VARCHAR NOT NULL,
      model_path VARCHAR NOT NULL,
      content_hash VARCHAR NOT NULL,
      embedding_model VARCHAR NOT NULL,
      dims INTEGER NOT NULL,
      embedding FLOAT[] NOT NULL,
      updated_at TIMESTAMP NOT NULL,
      PRIMARY KEY (environment_name, package_name, entity_kind, entity_source, entity_name)
    )
  `);
}

// TODO: Remove this during projects cleanup
// Tables in the pre-rename schema, listed children-first so DROP order
// satisfies foreign-key dependencies on the legacy `projects` table.
const LEGACY_TABLES_DROP_ORDER = [
   "build_manifests",
   "materializations",
   "packages",
   "connections",
   "projects",
] as const;

async function dropLegacyProjectSchema(db: DuckDBConnection): Promise<void> {
   const legacy = await db.all<{ name: string }>(
      "SELECT name FROM sqlite_master WHERE type='table' AND name='projects'",
   );
   if (!legacy || legacy.length === 0) {
      return;
   }

   logger.warn(
      "Detected legacy 'projects' schema. Dropping legacy tables; existing environments/packages/connections/materializations data will be lost. Re-create them via the API after upgrade.",
   );

   for (const table of LEGACY_TABLES_DROP_ORDER) {
      try {
         await db.run(`DROP TABLE IF EXISTS ${table}`);
      } catch (err) {
         logger.warn(`Failed to drop legacy table ${table}:`, err);
      }
   }
}

async function dropAllTables(db: DuckDBConnection): Promise<void> {
   const tables = [
      "build_manifests",
      "materializations",
      "packages",
      "connections",
      "environments",
      "themes",
      "entity_embeddings",
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
