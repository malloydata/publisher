import { Command } from "commander";
import chalk from "chalk";
import * as fs from "fs-extra";
import Table from "cli-table3";

import { PublisherClient } from "./api/client";

const program = new Command();

program
  .name("malloy-pub")
  .description("Malloy Publisher CLI - Manage Publisher resources")
  .version("0.0.50")
  .option(
    "--url <server>",
    "Publisher server URL (overrides MALLOY_PUBLISHER_URL)",
  );

let globalUrl: string | undefined;
program.hook("preAction", (thisCommand) => {
  const opts = thisCommand.opts();
  if (opts.url) {
    globalUrl = opts.url;
  }
});

function getClient(): PublisherClient {
  return new PublisherClient(globalUrl);
}

// LIST COMMAND
program
  .command("list <noun>")
  .description("List resources (project, package, connection)")
  .option("--project <n>", "Project name (required for package/connection)")
  .action(async (noun, options) => {
    try {
      const client = getClient();

      switch (noun) {
        case "project":
          await listProjects(client);
          break;
        case "package":
          if (!options.project) {
            console.error(chalk.red("Error: --project is required"));
            process.exit(1);
          }
          await listPackages(client, options.project);
          break;
        case "connection":
          if (!options.project) {
            console.error(chalk.red("Error: --project is required"));
            process.exit(1);
          }
          await listConnections(client, options.project);
          break;
        default:
          console.error(chalk.red(`Unknown resource: ${noun}`));
          console.log("Valid types: project, package, connection");
          process.exit(1);
      }
    } catch (error: any) {
      console.error(chalk.red("Error:"), error.message);
      process.exit(1);
    }
  });

// GET COMMAND
program
  .command("get <noun> [name]")
  .description("Get resource details")
  .option("--project <n>", "Project name (required for package/connection)")
  .option("--package <n>", "Package name")
  .action(async (noun, name, options) => {
    try {
      const client = getClient();

      switch (noun) {
        case "project":
          await getProject(client, name);
          break;
        case "package":
          if (!options.project) {
            console.error(chalk.red("Error: --project is required"));
            process.exit(1);
          }
          await getPackage(client, options.project, options.package);
          break;
        case "connection":
          if (!options.project) {
            console.error(chalk.red("Error: --project is required"));
            process.exit(1);
          }
          await getConnection(client, options.project, name);
          break;
        default:
          console.error(chalk.red(`Unknown resource: ${noun}`));
          process.exit(1);
      }
    } catch (error: any) {
      console.error(chalk.red("Error:"), error.message);
      process.exit(1);
    }
  });

// CREATE COMMAND
program
  .command("create <noun> [name]")
  .description("Create a resource")
  .option("--project <n>", "Project name")
  .option("--package <n>", "Package name")
  .option("--location <path>", "Package location")
  .option("--description <text>", "Description")
  .option("--file <path>", "JSON file (for connections)")
  .option("--json <string>", "JSON string (for connections)")
  .option("--name <n>", "Connection name from file (optional)")
  .action(async (noun, name, options) => {
    try {
      const client = getClient();

      switch (noun) {
        case "project":
          if (!name) {
            console.error(chalk.red("Error: Project name is required"));
            process.exit(1);
          }
          await createProject(client, name);
          break;
        case "package":
          if (!options.project || !options.package || !options.location) {
            console.error(
              chalk.red("Error: --project, --package, and --location required"),
            );
            process.exit(1);
          }
          await createPackage(
            client,
            options.project,
            options.package,
            options.location,
            options.description,
          );
          break;
        case "connection":
          if (!options.project) {
            console.error(chalk.red("Error: --project is required"));
            process.exit(1);
          }
          await createConnection(client, options.project, options);
          break;
        default:
          console.error(chalk.red(`Unknown resource: ${noun}`));
          process.exit(1);
      }
    } catch (error: any) {
      console.error(chalk.red("Error:"), error.message);
      process.exit(1);
    }
  });

// UPDATE COMMAND
program
  .command("update <noun> [name]")
  .description("Update a resource")
  .option("--project <n>", "Project name")
  .option("--package <n>", "Package name")
  .option("--readme <text>", "Project readme")
  .option("--location <path>", "Location")
  .option("--description <text>", "Description")
  .option("--file <path>", "JSON file (for connections)")
  .option("--json <string>", "JSON string (for connections)")
  .action(async (noun, name, options) => {
    try {
      const client = getClient();

      switch (noun) {
        case "project":
          await updateProject(client, name, options);
          break;
        case "package":
          if (!options.project) {
            console.error(chalk.red("Error: --project is required"));
            process.exit(1);
          }
          await updatePackage(
            client,
            options.project,
            options.package,
            options,
          );
          break;
        case "connection":
          if (!options.project) {
            console.error(chalk.red("Error: --project is required"));
            process.exit(1);
          }
          await updateConnection(client, options.project, name, options);
          break;
        default:
          console.error(chalk.red(`Unknown resource: ${noun}`));
          process.exit(1);
      }
    } catch (error: any) {
      console.error(chalk.red("Error:"), error.message);
      process.exit(1);
    }
  });

// DELETE COMMAND
program
  .command("delete <noun> [name]")
  .description("Delete a resource")
  .option("--project <n>", "Project name (required for package/connection)")
  .option("--package <n>", "Package name")
  .action(async (noun, name, options) => {
    try {
      const client = getClient();

      switch (noun) {
        case "project":
          await deleteProject(client, name);
          break;
        case "package":
          if (!options.project) {
            console.error(chalk.red("Error: --project is required"));
            process.exit(1);
          }
          await deletePackage(client, options.project, options.package);
          break;
        case "connection":
          if (!options.project) {
            console.error(chalk.red("Error: --project is required"));
            process.exit(1);
          }
          await deleteConnection(client, options.project, name);
          break;
        default:
          console.error(chalk.red(`Unknown resource: ${noun}`));
          process.exit(1);
      }
    } catch (error: any) {
      console.error(chalk.red("Error:"), error.message);
      process.exit(1);
    }
  });

// IMPLEMENTATIONS
async function listProjects(client: PublisherClient) {
  console.log(chalk.dim(`Fetching projects from ${client.getBaseURL()}...`));
  const projects = await client.listProjects();

  if (projects.length === 0) {
    console.log("No projects found.");
    return;
  }

  const table = new Table({
    head: ["Name", "Packages", "Connections"],
  });

  projects.forEach((p: any) => {
    table.push([p.name, p.packages?.length || 0, p.connections?.length || 0]);
  });

  console.log(table.toString());
  console.log(`\nTotal: ${projects.length} project(s)`);
}

async function getProject(client: PublisherClient, name: string) {
  const project = await client.getProject(name);
  console.log(JSON.stringify(project, null, 2));
}

async function createProject(client: PublisherClient, name: string) {
  await client.createProject(name);
  console.log(chalk.green(`✓ Created project: ${name}`));
}

async function updateProject(
  client: PublisherClient,
  name: string,
  options: any,
) {
  const updates: any = { name };
  if (options.readme) updates.readme = options.readme;
  if (options.location) updates.location = options.location;

  if (Object.keys(updates).length === 1) {
    console.log(chalk.yellow("No updates specified"));
    return;
  }

  await client.updateProject(name, updates);
  console.log(chalk.green(`✓ Updated project: ${name}`));
}

async function deleteProject(client: PublisherClient, name: string) {
  await client.deleteProject(name);
  console.log(chalk.green(`✓ Deleted project: ${name}`));
}

async function listPackages(client: PublisherClient, projectName: string) {
  const packages = await client.listPackages(projectName);

  if (packages.length === 0) {
    console.log(`No packages in project: ${projectName}`);
    return;
  }

  const table = new Table({
    head: ["Name", "Location"],
  });

  packages.forEach((p: any) => {
    table.push([p.name, p.location]);
  });

  console.log(table.toString());
}

async function getPackage(
  client: PublisherClient,
  projectName: string,
  packageName: string,
) {
  const pkg = await client.getPackage(projectName, packageName);
  console.log(JSON.stringify(pkg, null, 2));
}

async function createPackage(
  client: PublisherClient,
  projectName: string,
  packageName: string,
  location: string,
  description?: string,
) {
  await client.createPackage(projectName, packageName, location, description);
  console.log(chalk.green(`✓ Created package: ${packageName}`));
}

async function updatePackage(
  client: PublisherClient,
  projectName: string,
  packageName: string,
  options: any,
) {
  const updates: any = { name: packageName };
  if (options.location) updates.location = options.location;
  if (options.description) updates.description = options.description;

  await client.updatePackage(projectName, packageName, updates);
  console.log(chalk.green(`✓ Updated package: ${packageName}`));
}

async function deletePackage(
  client: PublisherClient,
  projectName: string,
  packageName: string,
) {
  await client.deletePackage(projectName, packageName);
  console.log(chalk.green(`✓ Deleted package: ${packageName}`));
}

async function listConnections(client: PublisherClient, projectName: string) {
  const connections = await client.listConnections(projectName);

  if (connections.length === 0) {
    console.log(`No connections in project: ${projectName}`);
    return;
  }

  const table = new Table({
    head: ["Name", "Type"],
  });

  connections.forEach((c: any) => {
    table.push([c.name, c.type]);
  });

  console.log(table.toString());
}

async function getConnection(
  client: PublisherClient,
  projectName: string,
  connectionName: string,
) {
  const conn = await client.getConnection(projectName, connectionName);
  console.log(JSON.stringify(conn, null, 2));
}

async function createConnection(
  client: PublisherClient,
  projectName: string,
  options: any,
) {
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
        for (const conn of fileContent.connections) {
          await client.createConnection(projectName, conn);
          console.log(chalk.green(`✓ Created connection: ${conn.name}`));
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
  console.log(chalk.green(`✓ Created connection: ${connection.name}`));
}

async function updateConnection(
  client: PublisherClient,
  projectName: string,
  connectionName: string,
  options: any,
) {
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
  console.log(chalk.green(`✓ Updated connection: ${connectionName}`));
}

async function deleteConnection(
  client: PublisherClient,
  projectName: string,
  connectionName: string,
) {
  await client.deleteConnection(projectName, connectionName);
  console.log(chalk.green(`✓ Deleted connection: ${connectionName}`));
}

program.parse();
