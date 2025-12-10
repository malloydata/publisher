import { DuckDBConnection } from "./DuckDBConnection";

export async function initializeSchema(
   db: DuckDBConnection,
   reInit: boolean = false,
): Promise<void> {
   const initialized = await db.isInitialized();

   if (initialized && !reInit) {
      return;
   }

   if (reInit) {
      console.log(
         "Reinitializing database schema dropping and recreating all tables"
      );
      await dropAllTables(db);
      await createTables(db);
   } else {
      console.log(
         "Database not initialized. Run: bun run start:dev:init"
      );
      // throw new Error(
      //    "Database not initialized. Run: bun run start:dev:init"
      // );
   }

}

async function createTables(db: DuckDBConnection): Promise<void> {
   await db.run(`
      CREATE TABLE projects (
         id VARCHAR PRIMARY KEY,
         name VARCHAR NOT NULL UNIQUE,
         path VARCHAR NOT NULL,
         description VARCHAR,
         metadata JSON,
         created_at TIMESTAMP NOT NULL,
         updated_at TIMESTAMP NOT NULL
      )
   `);

   await db.run(`
      CREATE TABLE packages (
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

   await db.run(`
      CREATE TABLE connections (
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

   await db.run("CREATE INDEX idx_packages_project_id ON packages(project_id)");
   await db.run("CREATE INDEX idx_connections_project_id ON connections(project_id)");
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
