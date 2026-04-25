#!/usr/bin/env node

process.removeAllListeners("warning");
process.on("warning", (warning) => {
  if (
    warning.name === "DeprecationWarning" &&
    warning.message.includes("url.parse")
  ) {
    return;
  }
  logWarning(`${warning.name}: ${warning.message}`);
});

import { Command } from "commander";
import { PublisherClient } from "./api/client";
import { logError, logOutput, logWarning } from "./utils/logger.js";
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
  .command("list <resource>")
  .description("List resources (project, package, connection)")
  .option("--project <n>", "Project name (required for package/connection)")
  .action(async (resource, options) => {
    try {
      const client = getClient();

      switch (resource) {
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
          logError(`Unknown resource: ${resource}`);
          logOutput("Valid types: project, package, connection");
          process.exit(1);
      }
    } catch (error: any) {
      logError("Command failed", error);
      process.exit(1);
    }
  });

// GET COMMAND
program
  .command("get <resource> [name]")
  .description("Get resource details")
  .option("--project <n>", "Project name (required for package/connection)")
  .option("--package <n>", "Package name")
  .action(async (resource, name, options) => {
    try {
      const client = getClient();

      switch (resource) {
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
          logError(`Unknown resource: ${resource}`);
          process.exit(1);
      }
    } catch (error: any) {
      logError("Command failed", error);
      process.exit(1);
    }
  });

// CREATE COMMAND
program
  .command("create <resource> [name]")
  .description("Create a resource")
  .option("--project <n>", "Project name")
  .option("--package <n>", "Package name")
  .option("--location <path>", "Package location")
  .option("--description <text>", "Description")
  .option("--file <path>", "JSON file (for connections)")
  .option("--json <string>", "JSON string (for connections)")
  .option("--name <n>", "Connection name from file (optional)")
  .action(async (resource, name, options) => {
    try {
      const client = getClient();

      switch (resource) {
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
          logError(`Unknown resource: ${resource}`);
          process.exit(1);
      }
    } catch (error: any) {
      logError("Command failed", error);
      process.exit(1);
    }
  });

// UPDATE COMMAND
program
  .command("update <resource> [name]")
  .description("Update a resource")
  .option("--project <n>", "Project name")
  .option("--package <n>", "Package name")
  .option("--readme <text>", "Project readme")
  .option("--location <path>", "Location")
  .option("--description <text>", "Description")
  .option("--file <path>", "JSON file (for connections)")
  .option("--json <string>", "JSON string (for connections)")
  .action(async (resource, name, options) => {
    try {
      const client = getClient();

      switch (resource) {
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
          logError(`Unknown resource: ${resource}`);
          process.exit(1);
      }
    } catch (error: any) {
      logError("Command failed", error);
      process.exit(1);
    }
  });

// DELETE COMMAND
program
  .command("delete <resource> [name]")
  .description("Delete a resource")
  .option("--project <n>", "Project name (required for package/connection)")
  .option("--package <n>", "Package name")
  .action(async (resource, name, options) => {
    try {
      const client = getClient();

      switch (resource) {
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
          logError(`Unknown resource: ${resource}`);
          process.exit(1);
      }
    } catch (error: any) {
      logError("Command failed", error);
      process.exit(1);
    }
  });

program.parse();
