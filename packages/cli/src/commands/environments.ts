import { PublisherClient } from "../api/client.js";
import Table from "cli-table3";
import { logSuccess, logInfo, logWarning, logOutput } from "../utils/logger.js";

export async function listEnvironments(client: PublisherClient): Promise<void> {
  logInfo(`Fetching environments from ${client.getBaseURL()}...`);
  const environments = await client.listEnvironments();

  if (environments.length === 0) {
    logInfo("No environments found.");
    return;
  }

  const table = new Table({
    head: ["Name", "Packages", "Connections"],
  });

  environments.forEach((e: any) => {
    table.push([e.name, e.packages?.length || 0, e.connections?.length || 0]);
  });

  logOutput(table.toString());
  logInfo(`Total: ${environments.length} environment(s)`);
}

export async function getEnvironment(
  client: PublisherClient,
  name: string,
): Promise<void> {
  const environment = await client.getEnvironment(name);
  logOutput(JSON.stringify(environment, null, 2));
}

export async function createEnvironment(
  client: PublisherClient,
  name: string,
): Promise<void> {
  await client.createEnvironment(name);
  logSuccess(`Created environment: ${name}`);
}

export async function updateEnvironment(
  client: PublisherClient,
  name: string,
  options: { readme?: string; location?: string },
): Promise<void> {
  const updates: any = { name };
  if (options.readme) updates.readme = options.readme;
  if (options.location) updates.location = options.location;

  if (Object.keys(updates).length === 1) {
    logWarning("No updates specified");
    return;
  }

  await client.updateEnvironment(name, updates);
  logSuccess(`Updated environment: ${name}`);
}

export async function deleteEnvironment(
  client: PublisherClient,
  name: string,
): Promise<void> {
  await client.deleteEnvironment(name);
  logSuccess(`Deleted environment: ${name}`);
}
