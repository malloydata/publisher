import { PublisherClient } from "../api/client.js";
import Table from "cli-table3";
import { logSuccess, logInfo, logOutput } from "../utils/logger.js";

export async function listPackages(
  client: PublisherClient,
  projectName: string,
): Promise<void> {
  const packages = await client.listPackages(projectName);

  if (packages.length === 0) {
    logInfo(`No packages in project: ${projectName}`);
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
  projectName: string,
  packageName: string,
): Promise<void> {
  const pkg = await client.getPackage(projectName, packageName);
  logOutput(JSON.stringify(pkg, null, 2));
}

export async function createPackage(
  client: PublisherClient,
  projectName: string,
  packageName: string,
  location: string,
  description?: string,
): Promise<void> {
  await client.createPackage(projectName, packageName, location, description);
  logSuccess(`Created package: ${packageName}`);
}

export async function updatePackage(
  client: PublisherClient,
  projectName: string,
  packageName: string,
  options: { location?: string; description?: string },
): Promise<void> {
  const updates: any = { name: packageName };
  if (options.location) updates.location = options.location;
  if (options.description) updates.description = options.description;

  await client.updatePackage(projectName, packageName, updates);
  logSuccess(`Updated package: ${packageName}`);
}

export async function deletePackage(
  client: PublisherClient,
  projectName: string,
  packageName: string,
): Promise<void> {
  await client.deletePackage(projectName, packageName);
  logSuccess(`Deleted package: ${packageName}`);
}
