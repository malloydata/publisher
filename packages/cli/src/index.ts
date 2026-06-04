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
import * as environmentCommands from "./commands/environments.js";
import * as packageCommands from "./commands/packages.js";
import * as connectionCommands from "./commands/connections.js";
import * as materializationCommands from "./commands/materializations.js";
import * as manifestCommands from "./commands/manifests.js";
import * as modelCommands from "./commands/models.js";
import * as notebookCommands from "./commands/notebooks.js";
import * as databaseCommands from "./commands/databases.js";

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

// Parse an optional non-negative integer flag (e.g. --limit / --offset),
// erroring clearly instead of forwarding NaN to the server.
function parseOptionalCount(
  value: string | undefined,
  flag: string,
): number | undefined {
  if (value === undefined) {
    return undefined;
  }
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 0) {
    logError(`${flag} must be a non-negative integer`);
    process.exit(1);
  }
  return parsed;
}

// LIST COMMAND
program
  .command("list <resource>")
  .description(
    "List resources (environment, package, connection, materialization, model, notebook, database)",
  )
  .option(
    "--environment <n>",
    "Environment name (required for all but environment)",
  )
  .option(
    "--package <n>",
    "Package name (required for materialization/model/notebook/database)",
  )
  .option("--limit <n>", "Max materializations to return")
  .option("--offset <n>", "Materializations to skip")
  .action(async (resource, options) => {
    try {
      const client = getClient();

      switch (resource) {
        case "environment":
          await environmentCommands.listEnvironments(client);
          break;
        case "package":
          if (!options.environment) {
            logError("--environment is required");
            process.exit(1);
          }
          await packageCommands.listPackages(client, options.environment);
          break;
        case "connection":
          if (!options.environment) {
            logError("--environment is required");
            process.exit(1);
          }
          await connectionCommands.listConnections(client, options.environment);
          break;
        case "materialization":
          if (!options.environment || !options.package) {
            logError("--environment and --package are required");
            process.exit(1);
          }
          await materializationCommands.listMaterializations(
            client,
            options.environment,
            options.package,
            {
              limit: parseOptionalCount(options.limit, "--limit"),
              offset: parseOptionalCount(options.offset, "--offset"),
            },
          );
          break;
        case "model":
          if (!options.environment || !options.package) {
            logError("--environment and --package are required");
            process.exit(1);
          }
          await modelCommands.listModels(
            client,
            options.environment,
            options.package,
          );
          break;
        case "notebook":
          if (!options.environment || !options.package) {
            logError("--environment and --package are required");
            process.exit(1);
          }
          await notebookCommands.listNotebooks(
            client,
            options.environment,
            options.package,
          );
          break;
        case "database":
          if (!options.environment || !options.package) {
            logError("--environment and --package are required");
            process.exit(1);
          }
          await databaseCommands.listDatabases(
            client,
            options.environment,
            options.package,
          );
          break;
        default:
          logError(`Unknown resource: ${resource}`);
          logOutput(
            "Valid types: environment, package, connection, materialization, model, notebook, database",
          );
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
  .option(
    "--environment <n>",
    "Environment name (required for package/connection)",
  )
  .option("--package <n>", "Package name")
  .action(async (resource, name, options) => {
    try {
      const client = getClient();

      switch (resource) {
        case "environment":
          await environmentCommands.getEnvironment(client, name);
          break;
        case "package":
          if (!options.environment) {
            logError("--environment is required");
            process.exit(1);
          }
          await packageCommands.getPackage(
            client,
            options.environment,
            options.package,
          );
          break;
        case "connection":
          if (!options.environment) {
            logError("--environment is required");
            process.exit(1);
          }
          await connectionCommands.getConnection(
            client,
            options.environment,
            name,
          );
          break;
        case "materialization":
          if (!options.environment || !options.package) {
            logError("--environment and --package are required");
            process.exit(1);
          }
          await materializationCommands.getMaterialization(
            client,
            options.environment,
            options.package,
            name,
          );
          break;
        case "model":
          if (!options.environment || !options.package) {
            logError("--environment and --package are required");
            process.exit(1);
          }
          await modelCommands.getModel(
            client,
            options.environment,
            options.package,
            name,
          );
          break;
        case "notebook":
          if (!options.environment || !options.package) {
            logError("--environment and --package are required");
            process.exit(1);
          }
          await notebookCommands.getNotebook(
            client,
            options.environment,
            options.package,
            name,
          );
          break;
        case "manifest":
          if (!options.environment || !options.package) {
            logError("--environment and --package are required");
            process.exit(1);
          }
          await manifestCommands.getManifest(
            client,
            options.environment,
            options.package,
          );
          break;
        default:
          logError(`Unknown resource: ${resource}`);
          logOutput(
            "Valid types: environment, package, connection, materialization, model, notebook, manifest",
          );
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
  .option("--environment <n>", "Environment name")
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
        case "environment":
          if (!name) {
            logError("Environment name is required");
            process.exit(1);
          }
          await environmentCommands.createEnvironment(client, name);
          break;
        case "package":
          if (!options.environment || !options.package || !options.location) {
            logError("--environment, --package, and --location required");
            process.exit(1);
          }
          await packageCommands.createPackage(
            client,
            options.environment,
            options.package,
            options.location,
            options.description,
          );
          break;
        case "connection":
          if (!options.environment) {
            logError("--environment is required");
            process.exit(1);
          }
          await connectionCommands.createConnection(
            client,
            options.environment,
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
  .option("--environment <n>", "Environment name")
  .option("--package <n>", "Package name")
  .option("--readme <text>", "Environment readme")
  .option("--location <path>", "Location")
  .option("--description <text>", "Description")
  .option("--file <path>", "JSON file (for connections)")
  .option("--json <string>", "JSON string (for connections)")
  .action(async (resource, name, options) => {
    try {
      const client = getClient();

      switch (resource) {
        case "environment":
          await environmentCommands.updateEnvironment(client, name, options);
          break;
        case "package":
          if (!options.environment) {
            logError("--environment is required");
            process.exit(1);
          }
          await packageCommands.updatePackage(
            client,
            options.environment,
            options.package,
            options,
          );
          break;
        case "connection":
          if (!options.environment) {
            logError("--environment is required");
            process.exit(1);
          }
          await connectionCommands.updateConnection(
            client,
            options.environment,
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
  .option(
    "--environment <n>",
    "Environment name (required for package/connection)",
  )
  .option("--package <n>", "Package name")
  .action(async (resource, name, options) => {
    try {
      const client = getClient();

      switch (resource) {
        case "environment":
          await environmentCommands.deleteEnvironment(client, name);
          break;
        case "package":
          if (!options.environment) {
            logError("--environment is required");
            process.exit(1);
          }
          await packageCommands.deletePackage(
            client,
            options.environment,
            options.package,
          );
          break;
        case "connection":
          if (!options.environment) {
            logError("--environment is required");
            process.exit(1);
          }
          await connectionCommands.deleteConnection(
            client,
            options.environment,
            name,
          );
          break;
        case "materialization":
          if (!options.environment || !options.package) {
            logError("--environment and --package are required");
            process.exit(1);
          }
          await materializationCommands.deleteMaterialization(
            client,
            options.environment,
            options.package,
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

// The standalone action verbs below (materialize / stop-materialization /
// reload-manifest) always require --environment and --package, so they use
// Commander's requiredOption(). The verb-first switch commands above multiplex
// resources with differing requirements (e.g. `list environment` needs no
// --package), so they validate manually inside each case instead.

// MATERIALIZE COMMAND (create + start a materialization build)
program
  .command("materialize")
  .description("Create and start a materialization build for a package")
  .requiredOption("--environment <n>", "Environment name")
  .requiredOption("--package <n>", "Package name")
  .option("--force-refresh", "Rebuild all sources, ignoring existing build IDs")
  .option(
    "--auto-load-manifest",
    "Reload the manifest after a successful build",
  )
  .option("--wait", "Poll until the build reaches a terminal state")
  .option(
    "--timeout <seconds>",
    "With --wait, seconds to wait before giving up (default 120)",
  )
  .option(
    "--poll-interval <seconds>",
    "With --wait, seconds between status checks (default 2)",
  )
  .action(async (options) => {
    try {
      const client = getClient();
      const timeoutSec = parseOptionalCount(options.timeout, "--timeout");
      const pollSec = parseOptionalCount(
        options.pollInterval,
        "--poll-interval",
      );
      await materializationCommands.materialize(
        client,
        options.environment,
        options.package,
        {
          forceRefresh: options.forceRefresh,
          autoLoadManifest: options.autoLoadManifest,
          wait: options.wait,
          timeoutMs: timeoutSec !== undefined ? timeoutSec * 1000 : undefined,
          pollIntervalMs: pollSec !== undefined ? pollSec * 1000 : undefined,
        },
      );
    } catch (error: any) {
      logError("Command failed", error);
      process.exit(1);
    }
  });

// STOP-MATERIALIZATION COMMAND
program
  .command("stop-materialization <id>")
  .description("Stop a pending or running materialization")
  .requiredOption("--environment <n>", "Environment name")
  .requiredOption("--package <n>", "Package name")
  .action(async (id, options) => {
    try {
      const client = getClient();
      await materializationCommands.stopMaterialization(
        client,
        options.environment,
        options.package,
        id,
      );
    } catch (error: any) {
      logError("Command failed", error);
      process.exit(1);
    }
  });

// RELOAD-MANIFEST COMMAND
program
  .command("reload-manifest")
  .description("Reload the build manifest and recompile a package's models")
  .requiredOption("--environment <n>", "Environment name")
  .requiredOption("--package <n>", "Package name")
  .action(async (options) => {
    try {
      const client = getClient();
      await manifestCommands.reloadManifest(
        client,
        options.environment,
        options.package,
      );
    } catch (error: any) {
      logError("Command failed", error);
      process.exit(1);
    }
  });

program.parse();
