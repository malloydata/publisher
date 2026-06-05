import { PublisherClient } from "../api/client.js";
import Table from "cli-table3";
import { logInfo, logOutput, truncate } from "../utils/logger.js";

export async function listNotebooks(
  client: PublisherClient,
  environmentName: string,
  packageName: string,
): Promise<void> {
  const notebooks = await client.listNotebooks(environmentName, packageName);

  if (notebooks.length === 0) {
    logInfo(`No notebooks in package: ${packageName}`);
    return;
  }

  const table = new Table({
    head: ["Path", "Error"],
  });

  notebooks.forEach((n: any) => {
    table.push([n.path ?? "", n.error ? truncate(n.error) : ""]);
  });

  logOutput(table.toString());
}

export async function getNotebook(
  client: PublisherClient,
  environmentName: string,
  packageName: string,
  path: string,
): Promise<void> {
  const notebook = await client.getNotebook(environmentName, packageName, path);
  logOutput(JSON.stringify(notebook, null, 2));
}
