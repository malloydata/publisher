// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Command-line flags. Each flag sets the environment variable of the same
 * meaning, so a flag and its env var are interchangeable and every module reads
 * one place.
 */
export function applyCliArgs(
   args: string[],
   env: NodeJS.ProcessEnv = process.env,
): void {
   let sawServerRoot = false;
   let sawConfig = false;
   for (let i = 0; i < args.length; i++) {
      const arg = args[i];
      if (arg === "--port" && args[i + 1]) {
         env.PUBLISHER_PORT = args[i + 1];
         i++;
      } else if (arg === "--host" && args[i + 1]) {
         env.PUBLISHER_HOST = args[i + 1];
         i++;
      } else if (arg === "--server_root" && args[i + 1]) {
         sawServerRoot = true;
         env.SERVER_ROOT = args[i + 1];
         i++;
      } else if (arg === "--config" && args[i + 1]) {
         sawConfig = true;
         env.PUBLISHER_CONFIG_PATH = args[i + 1];
         i++;
      } else if (arg === "--mcp_port" && args[i + 1]) {
         env.MCP_PORT = args[i + 1];
         i++;
      } else if (arg === "--mcp_host" && args[i + 1]) {
         env.MCP_HOST = args[i + 1];
         i++;
      } else if (arg === "--admin_port" && args[i + 1]) {
         env.PUBLISHER_ADMIN_PORT = args[i + 1];
         i++;
      } else if (arg === "--admin_host" && args[i + 1]) {
         env.PUBLISHER_ADMIN_HOST = args[i + 1];
         i++;
      } else if (arg === "--shutdown_drain_duration_seconds" && args[i + 1]) {
         env.SHUTDOWN_DRAIN_DURATION_SECONDS = args[i + 1];
         i++;
      } else if (
         arg === "--shutdown_graceful_close_timeout_seconds" &&
         args[i + 1]
      ) {
         env.SHUTDOWN_GRACEFUL_CLOSE_TIMEOUT_SECONDS = args[i + 1];
         i++;
      } else if (arg === "--init") {
         env.INITIALIZE_STORAGE = "true";
      } else if (arg === "--no-mcp-config") {
         env.PUBLISHER_NO_MCP_CONFIG = "true";
      } else if (arg === "--watch-env" && args[i + 1]) {
         // Append (don't overwrite) so multiple --watch-env flags compose
         // and so an explicit env var pre-set still wins.
         const existing = env.PUBLISHER_WATCH || "";
         env.PUBLISHER_WATCH = existing
            ? `${existing},${args[i + 1]}`
            : args[i + 1];
         i++;
      } else if (arg === "--help" || arg === "-h") {
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
         console.log(
            "  --mcp_port <number>    Port for MCP server (default: 4040)",
         );
         console.log(
            "  --admin_port <number>  Port for the admin API, an orchestrator's catalog operations (default: off)",
         );
         console.log(
            "  --admin_host <string>  Host to bind the admin API to (default: 127.0.0.1; not moved by --host)",
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
   }
   // A zero-config invocation serves the bundled DuckDB-only sample config;
   // an explicit --server_root or --config says where to look instead. Never
   // in tests, so a spec cannot pin the EnvironmentStore to the samples.
   if (!sawServerRoot && !sawConfig && env.NODE_ENV !== "test") {
      env.PUBLISHER_USE_BUNDLED_DEFAULT = "true";
   }
}
