import { PublisherClient } from "../api/client.js";
import Table from "cli-table3";
import { logSuccess, logInfo, logWarning, logOutput } from "../utils/logger.js";

export async function listProjects(client: PublisherClient): Promise<void> {
  logInfo(`Fetching projects from ${client.getBaseURL()}...`);
  const projects = await client.listProjects();

  if (projects.length === 0) {
    logInfo("No projects found.");
    return;
  }

  const table = new Table({
    head: ["Name", "Packages", "Connections"],
  });

  projects.forEach((p: any) => {
    table.push([p.name, p.packages?.length || 0, p.connections?.length || 0]);
  });

  logOutput(table.toString());
  logInfo(`Total: ${projects.length} project(s)`);
}

export async function getProject(
  client: PublisherClient,
  name: string,
): Promise<void> {
  const project = await client.getProject(name);
  logOutput(JSON.stringify(project, null, 2));
}

export async function createProject(
  client: PublisherClient,
  name: string,
): Promise<void> {
  await client.createProject(name);
  logSuccess(`Created project: ${name}`);
}

export async function updateProject(
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

  await client.updateProject(name, updates);
  logSuccess(`Updated project: ${name}`);
}

export async function deleteProject(
  client: PublisherClient,
  name: string,
): Promise<void> {
  await client.deleteProject(name);
  logSuccess(`Deleted project: ${name}`);
}
