import { DuckDBConnection } from "@malloydata/db-duckdb";
import { PostgresConnection } from "@malloydata/db-postgres";
import { BigQueryConnection } from "@malloydata/db-bigquery";
import { SnowflakeConnection } from "@malloydata/db-snowflake";
import { TrinoConnection } from "@malloydata/db-trino";
import { v4 as uuidv4 } from "uuid";
import {
    Connection,
    FixedConnectionMap,
} from "@malloydata/malloy";
import { components } from "../api";
import path from "path";
import fs from "fs/promises";

type ApiConnection = components["schemas"]["Connection"];

export async function createConnections(
    modelDirectory: string,
    connectionConfig: ApiConnection[] | undefined,
): Promise<FixedConnectionMap> {
    const connectionMap = new Map<string, Connection>();
    connectionMap.set(
        "duckdb",
        new DuckDBConnection("duckdb", ":memory:", modelDirectory),
    );

    if (connectionConfig) {
        connectionConfig.map(async (connection) => {
            // This case shouldn't happen.  The package validation logic should
            // catch it.
            if (!connection.name) {
                throw "Invalid connection configuration.  No name.";
            }

            switch (connection.type) {
                case "postgres": {
                    const configReader = async () => {
                        if (!connection.postgresConnection) {
                            throw "Invalid connection configuration.  No postgres connection.";
                        }
                        return {
                            host: connection.postgresConnection.host,
                            port: connection.postgresConnection.port,
                            username: connection.postgresConnection.userName,
                            password: connection.postgresConnection.password,
                            databaseName:
                                connection.postgresConnection.databaseName,
                            connectionString:
                                connection.postgresConnection.connectionString,
                        };
                    };
                    const postgresConnection = new PostgresConnection(
                        connection.name,
                        () => ({}),
                        configReader,
                    );
                    connectionMap.set(connection.name, postgresConnection);
                    break;
                }

                case "bigquery": {
                    if (!connection.bigqueryConnection) {
                        throw "Invalid connection configuration.  No bigquery connection.";
                    }

                    // If a service account key file is provided, we persist it to disk
                    // and pass the path to the BigQueryConnection.
                    let serviceAccountKeyPath = undefined;
                    if (connection.bigqueryConnection.serviceAccountKeyJson) {
                        serviceAccountKeyPath = path.join(
                            "/tmp",
                            `${connection.name}-${uuidv4()}-service-account-key.json`,
                        );
                        await fs.writeFile(
                            serviceAccountKeyPath,
                            connection.bigqueryConnection
                                .serviceAccountKeyJson as string,
                        );
                    }

                    const bigqueryConnectionOptions = {
                        projectId: connection.bigqueryConnection.defaultProjectId,
                        serviceAccountKeyPath: serviceAccountKeyPath,
                        location: connection.bigqueryConnection.location,
                        maximumBytesBilled:
                            connection.bigqueryConnection.maximumBytesBilled,
                        timeoutMs:
                            connection.bigqueryConnection.queryTimeoutMilliseconds,
                        billingProjectId:
                            connection.bigqueryConnection.billingProjectId,
                    };
                    const bigqueryConnection = new BigQueryConnection(
                        connection.name,
                        () => ({}),
                        bigqueryConnectionOptions,
                    );
                    connectionMap.set(connection.name, bigqueryConnection);
                    break;
                }

                case "snowflake": {
                    if (!connection.snowflakeConnection) {
                        throw new Error(
                            "Snowflake connection configuration is missing.",
                        );
                    }
                    if (!connection.snowflakeConnection.account) {
                        throw new Error("Snowflake account is required.");
                    }

                    if (!connection.snowflakeConnection.username) {
                        throw new Error("Snowflake username is required.");
                    }

                    if (!connection.snowflakeConnection.password) {
                        throw new Error("Snowflake password is required.");
                    }

                    if (!connection.snowflakeConnection.warehouse) {
                        throw new Error("Snowflake warehouse is required.");
                    }

                    const snowflakeConnectionOptions = {
                        connOptions: {
                            account: connection.snowflakeConnection.account,
                            username: connection.snowflakeConnection.username,
                            password: connection.snowflakeConnection.password,
                            warehouse: connection.snowflakeConnection.warehouse,
                            database: connection.snowflakeConnection.database,
                            schema: connection.snowflakeConnection.schema,
                            timeout:
                                connection.snowflakeConnection
                                    .responseTimeoutMilliseconds,
                        },
                    };
                    const snowflakeConnection = new SnowflakeConnection(
                        connection.name,
                        snowflakeConnectionOptions,
                    );
                    connectionMap.set(connection.name, snowflakeConnection);
                    break;
                }

                case "trino": {
                    if (!connection.trinoConnection) {
                        throw new Error(
                            "Trino connection configuration is missing.",
                        );
                    }
                    const trinoConnectionOptions = {
                        server: connection.trinoConnection.server,
                        port: connection.trinoConnection.port,
                        catalog: connection.trinoConnection.catalog,
                        schema: connection.trinoConnection.schema,
                        user: connection.trinoConnection.user,
                        password: connection.trinoConnection.password,
                    };
                    const trinoConnection: Connection = new TrinoConnection(
                        connection.name,
                        {},
                        trinoConnectionOptions,
                    );
                    connectionMap.set(connection.name, trinoConnection);
                    break;
                }

                default: {
                    throw new Error(
                        `Unsupported connection type: ${connection.type}`,
                    );
                }
            }
        });
    }

    return new FixedConnectionMap(connectionMap, "duckdb");
}
