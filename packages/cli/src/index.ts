#!/usr/bin/env node
import { Command } from "commander";
import { PublisherClient } from "./api/client";
import { logError } from "./utils/logger.js";
import * as projectCommands from "./commands/projects.js";
import * as packageCommands from "./commands/packages.js";
import * as connectionCommands from "./commands/connections.js";

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
          await projectCommands.listProjects(client);
          break;
        case "package":
          if (!options.project) {
            logError("--project is required");
            process.exit(1);
          }
          await packageCommands.listPackages(client, options.project);
          break;
        case "connection":
          if (!options.project) {
            logError("--project is required");
            process.exit(1);
          }
          await connectionCommands.listConnections(client, options.project);
          break;
        default:
          logError(`Unknown resource: ${noun}`);
          console.log("Valid types: project, package, connection");
          process.exit(1);
      }
    } catch (error: any) {
      logError("Command failed", error);
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
          await projectCommands.getProject(client, name);
          break;
        case "package":
          if (!options.project) {
            logError("--project is required");
            process.exit(1);
          }
          await packageCommands.getPackage(
            client,
            options.project,
            options.package,
          );
          break;
        case "connection":
          if (!options.project) {
            logError("--project is required");
            process.exit(1);
          }
          await connectionCommands.getConnection(client, options.project, name);
          break;
        default:
          logError(`Unknown resource: ${noun}`);
          process.exit(1);
      }
    } catch (error: any) {
      logError("Command failed", error);
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
            logError("Project name is required");
            process.exit(1);
          }
          await projectCommands.createProject(client, name);
          break;
        case "package":
          if (!options.project || !options.package || !options.location) {
            logError("--project, --package, and --location required");
            process.exit(1);
          }
          await packageCommands.createPackage(
            client,
            options.project,
            options.package,
            options.location,
            options.description,
          );
          break;
        case "connection":
          if (!options.project) {
            logError("--project is required");
            process.exit(1);
          }
          await connectionCommands.createConnection(
            client,
            options.project,
            options,
          );
          break;
        default:
          logError(`Unknown resource: ${noun}`);
          process.exit(1);
      }
    } catch (error: any) {
      logError("Command failed", error);
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
          await projectCommands.updateProject(client, name, options);
          break;
        case "package":
          if (!options.project) {
            logError("--project is required");
            process.exit(1);
          }
          await packageCommands.updatePackage(
            client,
            options.project,
            options.package,
            options,
          );
          break;
        case "connection":
          if (!options.project) {
            logError("--project is required");
            process.exit(1);
          }
          await connectionCommands.updateConnection(
            client,
            options.project,
            name,
            options,
          );
          break;
        default:
          logError(`Unknown resource: ${noun}`);
          process.exit(1);
      }
    } catch (error: any) {
      logError("Command failed", error);
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
          await projectCommands.deleteProject(client, name);
          break;
        case "package":
          if (!options.project) {
            logError("--project is required");
            process.exit(1);
          }
          await packageCommands.deletePackage(
            client,
            options.project,
            options.package,
          );
          break;
        case "connection":
          if (!options.project) {
            logError("--project is required");
            process.exit(1);
          }
          await connectionCommands.deleteConnection(
            client,
            options.project,
            name,
          );
          break;
        default:
          logError(`Unknown resource: ${noun}`);
          process.exit(1);
      }
    } catch (error: any) {
      logError("Command failed", error);
      process.exit(1);
    }
  });

program.parse();
