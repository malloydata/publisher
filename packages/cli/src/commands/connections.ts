import { PublisherClient } from "../api/client.js";
import Table from "cli-table3";
import * as fs from "fs-extra";
import { logSuccess, logInfo, logOutput } from "../utils/logger.js";

export async function listConnections(
  client: PublisherClient,
  projectName: string,
): Promise<void> {
  const connections = await client.listConnections(projectName);

  if (connections.length === 0) {
    logInfo(`No connections in project: ${projectName}`);
    return;
  }

  const table = new Table({
    head: ["Name", "Type"],
  });

  connections.forEach((c: any) => {
    table.push([c.name, c.type]);
  });

  logOutput(table.toString());
}

export async function getConnection(
  client: PublisherClient,
  projectName: string,
  connectionName: string,
): Promise<void> {
  const conn = await client.getConnection(projectName, connectionName);
  logOutput(JSON.stringify(conn, null, 2));
}

export async function createConnection(
  client: PublisherClient,
  projectName: string,
  options: { file?: string; json?: string; name?: string },
): Promise<void> {
  let connection;

  if (options.file) {
    const fileContent = await fs.readJSON(options.file);

    if (fileContent.connections && Array.isArray(fileContent.connections)) {
      if (options.name) {
        connection = fileContent.connections.find(
          (c: any) => c.name === options.name,
        );
        if (!connection) {
          throw new Error(`Connection '${options.name}' not found in file`);
        }
      } else {
        // Bulk create
        for (const conn of fileContent.connections) {
          await client.createConnection(projectName, conn);
          logSuccess(`Created connection: ${conn.name}`);
        }
        return;
      }
    } else {
      connection = fileContent;
    }
  } else if (options.json) {
    connection = JSON.parse(options.json);
  } else {
    throw new Error("Either --file or --json is required");
  }

  await client.createConnection(projectName, connection);
  logSuccess(`Created connection: ${connection.name}`);
}

export async function updateConnection(
  client: PublisherClient,
  projectName: string,
  connectionName: string,
  options: { file?: string; json?: string },
): Promise<void> {
  let connection;

  if (options.file) {
    const fileContent = await fs.readJSON(options.file);
    connection =
      fileContent.connections?.find((c: any) => c.name === connectionName) ||
      fileContent;
  } else if (options.json) {
    connection = JSON.parse(options.json);
  } else {
    throw new Error("Either --file or --json is required");
  }

  await client.updateConnection(projectName, connectionName, connection);
  logSuccess(`Updated connection: ${connectionName}`);
}

export async function deleteConnection(
  client: PublisherClient,
  projectName: string,
  connectionName: string,
): Promise<void> {
  await client.deleteConnection(projectName, connectionName);
  logSuccess(`Deleted connection: ${connectionName}`);
}
