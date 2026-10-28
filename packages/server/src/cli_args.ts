// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `--help`, `-h`: print the flags and exit. Every other flag is a setting's,
 * read with the variables and the config file by the settings loader
 * (settings/settings.ts), so nothing here writes `process.env`.
 */
export function printHelpIfAsked(args: readonly string[]): void {
   if (!args.some((arg) => arg === "--help" || arg === "-h")) return;
   console.log("Malloy Publisher Server");
   console.log("");
   console.log("Usage: malloy-publisher [options]");
   console.log("");
   console.log("Options:");
   console.log(
      "  --port <number>        Port to run the server on (default: 4000)",
   );
   console.log(
      "  --host <string>        Host to bind the REST server to, and the MCP server unless --mcp_host is given (default: 0.0.0.0)",
   );
   console.log(
      "  --mcp_host <string>    Host to bind the MCP server to (default: 127.0.0.1, or --host when that is set)",
   );
   console.log(
      "  --server_root <path>   Root directory to serve files from (default: .)",
   );
   console.log(
      "  --config <path>        Path to publisher.config.json (default: <server_root>/publisher.config.json; falls back to bundled DuckDB-only sample config if missing)",
   );
   console.log("  --mcp_port <number>    Port for MCP server (default: 4040)");
   console.log(
      "  --admin_port <number>  Port for the admin API, an orchestrator's catalog operations (default: off)",
   );
   console.log(
      "  --admin_host <string>  Host to bind the admin API to (default: 127.0.0.1; not moved by --host)",
   );
   console.log(
      "  --[no-]serving-api-enabled  Run the main API and MCP (default: on; off runs only the admin API)",
   );
   console.log(
      "  --[no-]admin-api-enabled    Allow the admin API (default: on; off refuses an admin setting)",
   );
   console.log(
      "  --shutdown_drain_duration_seconds <number>  Time in seconds to keep service in draining state before closing servers (default: 0)",
   );
   console.log(
      "  --shutdown_graceful_close_timeout_seconds <number>  Time in seconds to wait after closing servers before exit (default: 0)",
   );
   console.log(
      "  --init                 Wipe persisted storage and re-sync it from the config (default: false)",
   );
   console.log(
      "  --no-mcp-config        Do not write .mcp.json into the working directory (default: it is written, so an agent opened here finds this server; skipped when the directory already has one, is your home directory or the filesystem root, is inside a git working tree, or the MCP port bound is not the one requested)",
   );
   console.log(
      "  --watch-env <name>     Enable dev-mode watch for the named environment.",
   );
   console.log(
      "                         Mounts local-dir packages in-place (symlink, not",
   );
   console.log(
      "                         copy) so source-edit live reload works. A comma-",
   );
   console.log(
      "                         separated PUBLISHER_WATCH mounts all listed envs in",
   );
   console.log(
      "                         place, but only the first one auto-reloads.",
   );
   console.log("  --help, -h             Show this help message");
   process.exit(0);
}
