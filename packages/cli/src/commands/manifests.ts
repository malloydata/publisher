import { PublisherClient } from "../api/client.js";
import Table from "cli-table3";
import { logSuccess, logInfo, logOutput } from "../utils/logger.js";

export async function getManifest(
  client: PublisherClient,
  environmentName: string,
  packageName: string,
): Promise<void> {
  const manifest = await client.getManifest(environmentName, packageName);
  const entries = (manifest && manifest.entries) || {};
  const buildIds = Object.keys(entries);

  if (buildIds.length === 0) {
    logInfo(`No materialized tables in manifest for package: ${packageName}`);
    return;
  }

  const table = new Table({
    head: ["Build ID", "Table"],
  });

  buildIds.forEach((buildId) => {
    table.push([buildId, entries[buildId]?.tableName ?? ""]);
  });

  logOutput(table.toString());
}

export async function reloadManifest(
  client: PublisherClient,
  environmentName: string,
  packageName: string,
): Promise<void> {
  await client.reloadManifest(environmentName, packageName);
  logSuccess(`Reloaded manifest for package: ${packageName}`);
}
