// Generates the publisher.config.json the spawned server boots from: one
// environment, its connections (a Postgres source warehouse + a DuckLake
// destination, both on the harness Postgres), and the package locations.

import path from "path";
import type { PostgresHandle } from "./postgres";

/** A registered connection in the generated config (subset we use). */
export type ConnectionConfig =
   | {
        name: string;
        type: "postgres";
        postgresConnection: {
           host: string;
           port: number;
           databaseName: string;
           userName: string;
           password: string;
        };
     }
   | {
        name: string;
        type: "ducklake";
        ducklakeConnection: {
           catalog: {
              postgresConnection: {
                 host: string;
                 port: number;
                 databaseName: string;
                 userName: string;
                 password: string;
              };
           };
           storage: { bucketUrl: string };
        };
     }
   | {
        name: string;
        type: "duckdb";
        duckdbConnection: {
           attachedDatabases: {
              name: string;
              type: "postgres";
              postgresConnection: {
                 host: string;
                 port: number;
                 databaseName: string;
                 userName: string;
                 password: string;
              };
           }[];
        };
     };

export interface PackageRef {
   name: string;
   location: string;
}

export interface GeneratedConfig {
   configPath: string;
   environmentName: string;
   connectionNames: string[];
}

/** The Postgres source warehouse connection (`orders_pg`-style). */
export function postgresSource(
   name: string,
   pg: PostgresHandle,
   databaseName: string,
): ConnectionConfig {
   return {
      name,
      type: "postgres",
      postgresConnection: {
         host: pg.host,
         port: pg.hostPort,
         databaseName,
         userName: pg.user,
         password: pg.password,
      },
   };
}

/** The DuckLake destination connection (`lake`): Postgres catalog + local storage dir. */
export function ducklakeDest(
   name: string,
   pg: PostgresHandle,
   catalogDb: string,
   storageDir: string,
): ConnectionConfig {
   return {
      name,
      type: "ducklake",
      ducklakeConnection: {
         catalog: {
            postgresConnection: {
               host: pg.host,
               port: pg.hostPort,
               databaseName: catalogDb,
               userName: pg.user,
               password: pg.password,
            },
         },
         storage: { bucketUrl: storageDir },
      },
   };
}

/**
 * A DuckDB source connection. An env-level DuckDB connection must declare at
 * least one attached database, so we attach the run's Postgres source (a valid
 * attach it need not read) — inline `<name>.sql(...)` still runs natively in
 * DuckDB, giving a source whose dialect is `duckdb`, distinct from `postgres`.
 */
export function duckdbConn(
   name: string,
   pg: PostgresHandle,
   databaseName: string,
): ConnectionConfig {
   return {
      name,
      type: "duckdb",
      duckdbConnection: {
         attachedDatabases: [
            {
               name: `${name}_attach`,
               type: "postgres",
               postgresConnection: {
                  host: pg.host,
                  port: pg.hostPort,
                  databaseName,
                  userName: pg.user,
                  password: pg.password,
               },
            },
         ],
      },
   };
}

/** One environment in the generated config: its own connections + packages. */
export interface EnvSpec {
   name: string;
   connections: ConnectionConfig[];
   packages: PackageRef[];
}

export async function writeConfig(opts: {
   configPath: string;
   environments: EnvSpec[];
}): Promise<GeneratedConfig> {
   const config = {
      frozenConfig: false,
      environments: opts.environments.map((e) => ({
         name: e.name,
         packages: e.packages.map((p) => ({
            name: p.name,
            location: path.resolve(p.location),
         })),
         connections: e.connections,
      })),
   };
   await Bun.write(opts.configPath, JSON.stringify(config, null, 2));
   return {
      configPath: opts.configPath,
      environmentName: opts.environments[0]?.name ?? "",
      connectionNames:
         opts.environments[0]?.connections.map((c) => c.name) ?? [],
   };
}
