import { PublisherClient } from "../api/client.js";
import Table from "cli-table3";
import { logInfo, logOutput } from "../utils/logger.js";

export async function listDatabases(
  client: PublisherClient,
  environmentName: string,
  packageName: string,
): Promise<void> {
  const databases = await client.listDatabases(environmentName, packageName);

  if (databases.length === 0) {
    logInfo(`No databases in package: ${packageName}`);
    return;
  }

  const table = new Table({
    head: ["Path", "Type"],
  });

  databases.forEach((d: any) => {
    table.push([d.path ?? "", d.type ?? ""]);
  });

  logOutput(table.toString());
}
