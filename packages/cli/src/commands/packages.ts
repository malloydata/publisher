import { PublisherClient } from "../api/client.js";
import Table from "cli-table3";
import { logSuccess, logInfo, logOutput } from "../utils/logger.js";

export async function listPackages(
  client: PublisherClient,
  environmentName: string,
): Promise<void> {
  const packages = await client.listPackages(environmentName);

  if (packages.length === 0) {
    logInfo(`No packages in environment: ${environmentName}`);
    return;
  }

  const table = new Table({
    head: ["Name", "Location"],
  });

  packages.forEach((p: any) => {
    table.push([p.name, p.location]);
  });

  logOutput(table.toString());
}

export async function getPackage(
  client: PublisherClient,
  environmentName: string,
  packageName: string,
): Promise<void> {
  const pkg = await client.getPackage(environmentName, packageName);
  logOutput(JSON.stringify(pkg, null, 2));
}

export async function createPackage(
  client: PublisherClient,
  environmentName: string,
  packageName: string,
  location: string,
  description?: string,
): Promise<void> {
  await client.createPackage(environmentName, packageName, location, description);
  logSuccess(`Created package: ${packageName}`);
}

export async function updatePackage(
  client: PublisherClient,
  environmentName: string,
  packageName: string,
  options: { location?: string; description?: string },
): Promise<void> {
  const updates: any = { name: packageName };
  if (options.location) updates.location = options.location;
  if (options.description) updates.description = options.description;

  await client.updatePackage(environmentName, packageName, updates);
  logSuccess(`Updated package: ${packageName}`);
}

export async function deletePackage(
  client: PublisherClient,
  environmentName: string,
  packageName: string,
): Promise<void> {
  await client.deletePackage(environmentName, packageName);
  logSuccess(`Deleted package: ${packageName}`);
}
