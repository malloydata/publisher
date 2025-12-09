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
      console.log(
         "Reinitializing database schema dropping and recreating all tables",
      );
      await dropAllTables(db);
   } else {
      console.log("Creating database schema for the first time...");
   }

   // Projects table
   await db.run(`
    CREATE TABLE IF NOT EXISTS projects (
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
      project_id VARCHAR NOT NULL,
      name VARCHAR NOT NULL,
      description VARCHAR,
      manifest_path VARCHAR NOT NULL,
      metadata JSON,
      created_at TIMESTAMP NOT NULL,
      updated_at TIMESTAMP NOT NULL,
      FOREIGN KEY (project_id) REFERENCES projects(id),
      UNIQUE (project_id, name)
    )
  `);

   // Connections table
   await db.run(`
    CREATE TABLE IF NOT EXISTS connections (
      id VARCHAR PRIMARY KEY,
      project_id VARCHAR NOT NULL,
      name VARCHAR NOT NULL,
      type VARCHAR NOT NULL,
      config JSON NOT NULL,
      created_at TIMESTAMP NOT NULL,
      updated_at TIMESTAMP NOT NULL,
      FOREIGN KEY (project_id) REFERENCES projects(id),
      UNIQUE (project_id, name)
    )
  `);

   // Create indexes for better query performance
   await db.run(
      "CREATE INDEX IF NOT EXISTS idx_packages_project_id ON packages(project_id)",
   );
   await db.run(
      "CREATE INDEX IF NOT EXISTS idx_connections_project_id ON connections(project_id)",
   );
}

async function dropAllTables(db: DuckDBConnection): Promise<void> {
   const tables = ["connections", "packages", "projects"];

   console.log("Dropping tables:", tables.join(", "));

   for (const table of tables) {
      try {
         await db.run(`DROP TABLE IF EXISTS ${table} `);
         console.log(`Dropped table: ${table}`);
      } catch (err) {
         console.warn(` Warning: Could not drop table ${table}:`, err);
      }
   }
}
