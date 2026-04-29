import path from "path";
import { components } from "../api";

type ApiConnection = components["schemas"]["Connection"];
type AttachedDatabase = components["schemas"]["AttachedDatabase"];

export type CoreConnectionEntry = {
   is: string;
   [key: string]: unknown;
};

export type CoreConnectionsPojo = {
   connections: Record<string, CoreConnectionEntry>;
};

export type ProjectConnectionMetadata = {
   apiConnection: ApiConnection;
   attachedDatabases: AttachedDatabase[];
   hasAzureAttachment: boolean;
   hasSnowflakePrivateKey: boolean;
   isDuckLake: boolean;
   databasePath?: string;
   workingDirectory: string;
};

export type AssembledProjectConnections = {
   pojo: CoreConnectionsPojo;
   metadata: Map<string, ProjectConnectionMetadata>;
   apiConnections: ApiConnection[];
};

const PUBLISHER_DUCKDB_API_FIELDS = new Set<string>(["attachedDatabases"]);

export function normalizeSnowflakePrivateKey(privateKey: string): string {
   let privateKeyContent = privateKey.trim();

   if (!privateKeyContent.includes("\n")) {
      const keyPatterns = [
         {
            beginRegex: /-----BEGIN\s+ENCRYPTED\s+PRIVATE\s+KEY-----/i,
            endRegex: /-----END\s+ENCRYPTED\s+PRIVATE\s+KEY-----/i,
            beginMarker: "-----BEGIN ENCRYPTED PRIVATE KEY-----",
            endMarker: "-----END ENCRYPTED PRIVATE KEY-----",
         },
         {
            beginRegex: /-----BEGIN\s+PRIVATE\s+KEY-----/i,
            endRegex: /-----END\s+PRIVATE\s+KEY-----/i,
            beginMarker: "-----BEGIN PRIVATE KEY-----",
            endMarker: "-----END PRIVATE KEY-----",
         },
      ];

      for (const pattern of keyPatterns) {
         const beginMatch = privateKeyContent.match(pattern.beginRegex);
         const endMatch = privateKeyContent.match(pattern.endRegex);

         if (beginMatch && endMatch) {
            const beginPos = beginMatch.index! + beginMatch[0].length;
            const endPos = endMatch.index!;
            const keyData = privateKeyContent
               .substring(beginPos, endPos)
               .replace(/\s+/g, "");

            const lines: string[] = [];
            for (let i = 0; i < keyData.length; i += 64) {
               lines.push(keyData.slice(i, i + 64));
            }
            privateKeyContent = `${pattern.beginMarker}\n${lines.join("\n")}\n${pattern.endMarker}\n`;
            break;
         }
      }
   } else if (!privateKeyContent.endsWith("\n")) {
      privateKeyContent += "\n";
   }

   return privateKeyContent;
}

// NOTE: This narrows the project-author API surface (it rejects securityPolicy,
// allowedDirectories, setupSQL, etc.). It is NOT a filesystem isolation
// boundary: attachedDatabases[].path is not normalized or constrained to stay
// under the project root, and DuckDB's local-file access is unchanged.
// Adversarial filesystem isolation is explicit non-goal of the MalloyConfig
// adoption — see PR #682 release notes ("DuckDB hardening knobs are not
// exposed", "no adversarial DuckDB filesystem isolation"). Future work owns
// any path-traversal/allowlist enforcement.
export function validateDuckdbApiSurface(connection: ApiConnection): void {
   if (connection.type !== "duckdb" || !connection.duckdbConnection) return;

   const unsupportedFields = Object.keys(connection.duckdbConnection).filter(
      (field) =>
         !PUBLISHER_DUCKDB_API_FIELDS.has(field) &&
         (connection.duckdbConnection as Record<string, unknown>)[field] !==
            undefined,
   );

   if (unsupportedFields.length > 0) {
      throw new Error(
         `Unsupported DuckDB connection field(s): ${unsupportedFields.join(
            ", ",
         )}. Publisher only supports attachedDatabases for project-authored DuckDB connections.`,
      );
   }
}

function cloneApiConnection(connection: ApiConnection): ApiConnection {
   return { ...connection };
}

function getStaticConnectionAttributes(
   type: ApiConnection["type"],
): components["schemas"]["ConnectionAttributes"] | undefined {
   switch (type) {
      case "postgres":
         return {
            dialectName: "postgres",
            isPool: false,
            canPersist: true,
            canStream: true,
         };
      case "bigquery":
         return {
            dialectName: "standardsql",
            isPool: false,
            canPersist: true,
            canStream: true,
         };
      case "snowflake":
         return {
            dialectName: "snowflake",
            isPool: true,
            canPersist: true,
            canStream: true,
         };
      case "trino":
         return {
            dialectName: "trino",
            isPool: false,
            canPersist: true,
            canStream: false,
         };
      case "mysql":
         return {
            dialectName: "mysql",
            isPool: false,
            canPersist: true,
            canStream: false,
         };
      case "duckdb":
      case "motherduck":
      case "ducklake":
         return {
            dialectName: "duckdb",
            isPool: false,
            canPersist: true,
            canStream: true,
         };
      default:
         return undefined;
   }
}

type ServiceAccountKey = {
   type?: string;
   project_id?: string;
   private_key?: string;
   client_email?: string;
   [key: string]: unknown;
};

function parseServiceAccountKey(json?: string): ServiceAccountKey | undefined {
   if (!json) return undefined;
   const keyData = JSON.parse(json) as ServiceAccountKey;
   const requiredFields = ["type", "project_id", "private_key", "client_email"];
   for (const field of requiredFields) {
      if (!keyData[field]) {
         throw new Error(
            `Invalid service account key: missing "${field}" field`,
         );
      }
   }
   if (keyData.type !== "service_account") {
      throw new Error('Invalid service account key: incorrect "type" field');
   }
   return keyData;
}

function buildPostgresConnectionString(
   config: components["schemas"]["PostgresConnection"],
): string | undefined {
   if (config.connectionString || !process.env.PGSSLMODE) {
      return config.connectionString;
   }

   const params = new URLSearchParams();
   params.set("sslmode", process.env.PGSSLMODE);
   const auth =
      config.userName && config.password
         ? `${encodeURIComponent(config.userName)}:${encodeURIComponent(
              config.password,
           )}@`
         : config.userName
           ? `${encodeURIComponent(config.userName)}@`
           : "";
   const host = config.host ?? "localhost";
   const port = config.port ? `:${config.port}` : "";
   const database = config.databaseName
      ? `/${encodeURIComponent(config.databaseName)}`
      : "";
   return `postgresql://${auth}${host}${port}${database}?${params.toString()}`;
}

function buildDuckdbEntry(
   name: string,
   projectPath: string,
   databaseFilename = `${name}.duckdb`,
): CoreConnectionEntry {
   return {
      is: "duckdb",
      databasePath: path.join(projectPath, databaseFilename),
   };
}

function validateConnectionShape(connection: ApiConnection): void {
   switch (connection.type) {
      case "postgres":
      case "mysql":
      case "bigquery":
         break;
      case "duckdb":
         if (!connection.duckdbConnection) {
            throw new Error("DuckDB connection configuration is missing.");
         }
         break;
      case "motherduck":
         if (!connection.motherduckConnection) {
            throw new Error("MotherDuck connection configuration is missing.");
         }
         if (!connection.motherduckConnection.accessToken) {
            throw new Error("MotherDuck access token is required.");
         }
         break;
      case "trino":
         if (!connection.trinoConnection) {
            throw new Error("Trino connection configuration is missing.");
         }
         break;
      case "snowflake": {
         const snowflakeConnection = connection.snowflakeConnection;
         if (!snowflakeConnection) {
            throw new Error("Snowflake connection configuration is missing.");
         }
         if (!snowflakeConnection.account) {
            throw new Error("Snowflake account is required.");
         }
         if (!snowflakeConnection.username) {
            throw new Error("Snowflake username is required.");
         }
         if (!snowflakeConnection.password && !snowflakeConnection.privateKey) {
            throw new Error(
               "Snowflake password or private key or private key path is required.",
            );
         }
         if (!snowflakeConnection.warehouse) {
            throw new Error("Snowflake warehouse is required.");
         }
         break;
      }
   }
}

export function assembleProjectConnections(
   connections: ApiConnection[] = [],
   projectPath = "",
): AssembledProjectConnections {
   const pojo: CoreConnectionsPojo = { connections: {} };
   const metadata = new Map<string, ProjectConnectionMetadata>();
   const apiConnections: ApiConnection[] = [];
   const processedConnections = new Set<string>();

   for (const connection of connections) {
      if (!connection.name) {
         throw new Error("Invalid connection configuration. No name.");
      }

      if (processedConnections.has(connection.name)) {
         continue;
      }

      if (connection.name === "duckdb") {
         throw new Error(
            "DuckDB connection name cannot be 'duckdb'; it is reserved for Publisher package sandboxes.",
         );
      }

      processedConnections.add(connection.name);
      validateDuckdbApiSurface(connection);
      validateConnectionShape(connection);

      const apiConnection = cloneApiConnection(connection);
      apiConnection.attributes = getStaticConnectionAttributes(connection.type);
      const attachedDatabases =
         connection.duckdbConnection?.attachedDatabases ?? [];
      const isDuckLake = connection.type === "ducklake";
      const isDuckdb = connection.type === "duckdb";
      const databasePath = isDuckLake
         ? path.join(projectPath, `${connection.name}_ducklake.duckdb`)
         : isDuckdb
           ? path.join(projectPath, `${connection.name}.duckdb`)
           : undefined;

      metadata.set(connection.name, {
         apiConnection,
         attachedDatabases,
         hasAzureAttachment: attachedDatabases.some(
            (database) => database.type === "azure",
         ),
         hasSnowflakePrivateKey:
            connection.type === "snowflake" &&
            !!connection.snowflakeConnection?.privateKey,
         isDuckLake,
         databasePath,
         workingDirectory: projectPath,
      });

      switch (connection.type) {
         case "postgres": {
            const postgresConnection = connection.postgresConnection;
            pojo.connections[connection.name] = {
               is: "postgres",
               host: postgresConnection?.host,
               port: postgresConnection?.port,
               username: postgresConnection?.userName,
               password: postgresConnection?.password,
               databaseName: postgresConnection?.databaseName,
               connectionString: postgresConnection
                  ? buildPostgresConnectionString(postgresConnection)
                  : undefined,
            };
            break;
         }

         case "mysql": {
            pojo.connections[connection.name] = {
               is: "mysql",
               host: connection.mysqlConnection?.host,
               port: connection.mysqlConnection?.port,
               user: connection.mysqlConnection?.user,
               password: connection.mysqlConnection?.password,
               database: connection.mysqlConnection?.database,
            };
            break;
         }

         case "bigquery": {
            const serviceAccountKey = parseServiceAccountKey(
               connection.bigqueryConnection?.serviceAccountKeyJson as
                  | string
                  | undefined,
            );
            pojo.connections[connection.name] = {
               is: "bigquery",
               projectId:
                  connection.bigqueryConnection?.defaultProjectId ??
                  serviceAccountKey?.project_id,
               serviceAccountKey,
               location: connection.bigqueryConnection?.location,
               maximumBytesBilled:
                  connection.bigqueryConnection?.maximumBytesBilled,
               timeoutMs:
                  connection.bigqueryConnection?.queryTimeoutMilliseconds,
               billingProjectId:
                  connection.bigqueryConnection?.billingProjectId,
            };
            break;
         }

         case "snowflake": {
            pojo.connections[connection.name] = {
               is: "snowflake",
               account: connection.snowflakeConnection?.account,
               username: connection.snowflakeConnection?.username,
               password: connection.snowflakeConnection?.password,
               privateKey: connection.snowflakeConnection?.privateKey
                  ? normalizeSnowflakePrivateKey(
                       connection.snowflakeConnection.privateKey,
                    )
                  : undefined,
               privateKeyPass: connection.snowflakeConnection?.privateKeyPass,
               warehouse: connection.snowflakeConnection?.warehouse,
               database: connection.snowflakeConnection?.database,
               schema: connection.snowflakeConnection?.schema,
               role: connection.snowflakeConnection?.role,
               timeoutMs:
                  connection.snowflakeConnection?.responseTimeoutMilliseconds,
               // Pool sizing is server-owned policy (matches the values
               // main's deleted switch passed pre-MalloyConfig adoption).
               // Not exposed through the public API.
               poolMin: 1,
               poolMax: 20,
            };
            break;
         }

         case "trino": {
            pojo.connections[connection.name] = {
               is: "trino",
               ...validateAndBuildTrinoCoreConfig(connection.trinoConnection),
            };
            break;
         }

         case "duckdb": {
            if (
               attachedDatabases.some(
                  (database) => database.name === connection.name,
               )
            ) {
               throw new Error(
                  `DuckDB attached database names cannot conflict with connection name ${connection.name}`,
               );
            }
            pojo.connections[connection.name] = buildDuckdbEntry(
               connection.name,
               projectPath,
               `${connection.name}.duckdb`,
            );
            break;
         }

         case "motherduck": {
            if (!connection.motherduckConnection?.accessToken) {
               throw new Error("MotherDuck access token is required.");
            }

            pojo.connections[connection.name] = {
               is: "duckdb",
               databasePath: connection.motherduckConnection.database
                  ? `md:${connection.motherduckConnection.database}?attach_mode=single`
                  : "md:",
               motherDuckToken: connection.motherduckConnection.accessToken,
            };
            break;
         }

         case "ducklake": {
            if (!connection.ducklakeConnection) {
               throw new Error("DuckLake connection configuration is missing.");
            }
            if (!connection.ducklakeConnection.catalog?.postgresConnection) {
               throw new Error(
                  `PostgreSQL connection configuration is required for DuckLake catalog: ${connection.name}`,
               );
            }
            pojo.connections[connection.name] = buildDuckdbEntry(
               connection.name,
               projectPath,
               `${connection.name}_ducklake.duckdb`,
            );
            break;
         }

         default: {
            throw new Error(`Unsupported connection type: ${connection.type}`);
         }
      }

      apiConnections.push(apiConnection);
   }

   return { pojo, metadata, apiConnections };
}

function validateAndBuildTrinoCoreConfig(
   trinoConfig: components["schemas"]["TrinoConnection"] | undefined,
): Record<string, unknown> {
   if (!trinoConfig) {
      return {};
   }

   const server =
      trinoConfig.server && trinoConfig.port
         ? trinoConfig.server.includes(trinoConfig.port.toString())
            ? trinoConfig.server
            : `${trinoConfig.server}:${trinoConfig.port}`
         : trinoConfig.server;

   const baseConfig: Record<string, unknown> = {
      server,
      port: trinoConfig.port,
      catalog: trinoConfig.catalog,
      schema: trinoConfig.schema,
      user: trinoConfig.user,
   };

   if (trinoConfig.peakaKey) {
      baseConfig.extraCredential = {
         peakaKey: trinoConfig.peakaKey,
      };
      return baseConfig;
   }

   if (server?.startsWith("https://") && trinoConfig.password) {
      baseConfig.password = trinoConfig.password;
   }

   if (server?.startsWith("http://") || server?.startsWith("https://")) {
      return baseConfig;
   }

   throw new Error(
      `Invalid Trino connection: expected "http://server:port" or "https://server:port".`,
   );
}
