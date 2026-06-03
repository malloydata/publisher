import { PublisherClient } from "../api/client.js";
import Table from "cli-table3";
import { logInfo, logOutput, truncate } from "../utils/logger.js";

export async function listModels(
  client: PublisherClient,
  environmentName: string,
  packageName: string,
): Promise<void> {
  const models = await client.listModels(environmentName, packageName);

  if (models.length === 0) {
    logInfo(`No models in package: ${packageName}`);
    return;
  }

  const table = new Table({
    head: ["Path", "Error"],
  });

  models.forEach((m: any) => {
    table.push([m.path ?? "", m.error ? truncate(m.error) : ""]);
  });

  logOutput(table.toString());
}

export async function getModel(
  client: PublisherClient,
  environmentName: string,
  packageName: string,
  path: string,
): Promise<void> {
  const model = await client.getModel(environmentName, packageName, path);
  logOutput(JSON.stringify(model, null, 2));
}
