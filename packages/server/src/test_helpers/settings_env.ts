// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import fs from "fs";
import os from "os";
import path from "path";
import { applyCliArgs } from "../cli_args";
import { PUBLISHER_CONFIG_NAME } from "../constants";

/**
 * One startup's worth of settings sources: variables, flags and the config
 * file. `file` is written to a fresh server root's `publisher.config.json`, as
 * JSON or, given a string, as raw text (so a case can be unreadable).
 */
export interface SettingsInput {
   env?: Record<string, string>;
   argv?: string[];
   file?: Record<string, unknown> | string;
}

export interface SettingsContext {
   /** A fresh directory holding `file`, if the input gave one. */
   serverRoot: string;
}

/**
 * Every variable Publisher reads its own settings from today. Cleared around
 * each case, so a variable set in the developer's shell never reaches one.
 */
export const TODAY_VARIABLES = [
   "PUBLISHER_SERVING_API_ENABLED",
   "PUBLISHER_ADMIN_API_ENABLED",
   "PUBLISHER_PORT",
   "PUBLISHER_HOST",
   "PUBLISHER_RATE_LIMIT",
   "PUBLISHER_FRAME_ANCESTORS",
   "SERVER_ROOT",
   "PUBLISHER_CONFIG_PATH",
   "PUBLISHER_USE_BUNDLED_DEFAULT",
   "INITIALIZE_STORAGE",
   "PUBLISHER_WATCH",
   "MCP_PORT",
   "MCP_HOST",
   "MCP_CORS_ORIGINS",
   "PUBLISHER_NO_MCP_CONFIG",
   "PUBLISHER_MCP_TRACE",
   "PUBLISHER_ADMIN_PORT",
   "PUBLISHER_ADMIN_HOST",
   "PUBLISHER_ADMIN_CATALOG_DATABASE",
   "PUBLISHER_ADMIN_STORAGE_PROVIDER",
   "PUBLISHER_ADMIN_STORAGE_KEY_ID",
   "PUBLISHER_ADMIN_STORAGE_SECRET",
   "PUBLISHER_ADMIN_STORAGE_REGION",
   "PUBLISHER_ADMIN_STORAGE_ENDPOINT",
   "PUBLISHER_MAX_QUERY_ROWS",
   "PUBLISHER_MAX_RESPONSE_BYTES",
   "PUBLISHER_DEFAULT_QUERY_ROW_LIMIT",
   "PUBLISHER_QUERY_TIMEOUT_MS",
   "PUBLISHER_MAX_CONCURRENT_QUERIES",
   "PUBLISHER_QUERY_METADATA",
   "PUBLISHER_POSTGRES_POOL_MAX",
   "PUBLISHER_MAX_MEMORY_BYTES",
   "PUBLISHER_MEMORY_HIGH_WATER_FRACTION",
   "PUBLISHER_MEMORY_LOW_WATER_FRACTION",
   "PUBLISHER_MEMORY_CHECK_INTERVAL_MS",
   "PUBLISHER_MEMORY_BACKPRESSURE",
   "PACKAGE_LOAD_WORKERS",
   "PACKAGE_LOAD_JOB_TIMEOUT_MS",
   "PACKAGE_LOAD_SCHEMA_CACHE_BYTES",
   "EXTENSION_FETCH_POLICY",
   "PUBLISHER_DUCKDB_MEMORY_LIMIT",
   "PUBLISHER_DUCKDB_TEMP_DIRECTORY",
   "PUBLISHER_PARTITIONED_WRITE_FLUSH_THRESHOLD",
   "PUBLISHER_DUCKLAKE_ROW_GROUP_SIZE_BYTES",
   "PUBLISHER_DUCKLAKE_TARGET_FILE_SIZE_BYTES",
   "PERSIST_STORAGE_MODE",
   "PERSIST_COLLISION_ENFORCE",
   "PUBLISHER_LOCAL_MATERIALIZATION_SCHEDULER",
   "PUBLISHER_MATERIALIZATION_SCHEDULER_INTERVAL_MS",
   "PUBLISHER_MATERIALIZATION_SCHEDULER_MAX_FIRES_PER_TICK",
   "PUBLISHER_ALLOW_PROXY_CONNECTIONS",
   "PUBLISHER_ALLOW_DUCKDB_SETUP_SQL",
   "PUBLISHER_ALLOW_UNVERIFIED_SSH_HOST_KEY",
   "S3_CREDENTIAL_CHAIN_POLICY",
   "PUBLISHER_BYPASS_AUTHORIZE_SECRET",
   "LOG_LEVEL",
   "DISABLE_RESPONSE_LOGGING",
   "SHUTDOWN_DRAIN_DURATION_SECONDS",
   "SHUTDOWN_GRACEFUL_CLOSE_TIMEOUT_SECONDS",
   "EMBEDDING_API_KEY",
   "EMBEDDING_MODEL",
   "EMBEDDING_API_BASE",
   "EMBEDDING_DIMENSIONS",
   "EMBEDDING_MIN_SIMILARITY",
   "EMBEDDING_INDEX_CONNECTION_SCHEMA",
   "LLM_API_KEY",
] as const;

/**
 * Run `fn` as if this process had started with `input`: the variables set,
 * the flags parsed as `server.ts` parses them, and the file in a fresh server
 * root. `process.env` is restored afterwards, so no case leaks into the next.
 */
export function withEnv<T>(
   input: SettingsInput,
   fn: (context: SettingsContext) => T,
): T {
   const saved = { ...process.env };
   const serverRoot = fs.mkdtempSync(path.join(os.tmpdir(), "settings-"));
   try {
      for (const name of TODAY_VARIABLES) delete process.env[name];
      Object.assign(process.env, input.env ?? {});
      if (input.file !== undefined) {
         fs.writeFileSync(
            path.join(serverRoot, PUBLISHER_CONFIG_NAME),
            typeof input.file === "string"
               ? input.file
               : JSON.stringify(input.file),
         );
      }
      applyCliArgs(input.argv ?? [], process.env);
      return fn({ serverRoot });
   } finally {
      for (const name of Object.keys(process.env)) {
         if (!(name in saved)) delete process.env[name];
      }
      Object.assign(process.env, saved);
      fs.rmSync(serverRoot, { recursive: true, force: true });
   }
}
