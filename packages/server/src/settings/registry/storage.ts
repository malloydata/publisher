// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { setting } from "../setting";
import { boolean, integer, oneOf, string } from "../types";

/** The configuration design's `duckdb`, `ducklake`, `persist`, `materialization` table. */
export const storage = [
   setting({
      key: "duckdb.extensionFetchPolicy",
      type: oneOf(["on-demand", "local-only"]),
      default: "on-demand",
      description:
         "on-demand installs a missing DuckDB extension on first use; local-only never reaches the network.",
      aliases: { env: ["EXTENSION_FETCH_POLICY"] },
   }),
   // The sizes below are strings because DuckDB parses them (`1GB`);
   // assertDuckDBResourceConfig checks them at boot.
   setting({
      key: "duckdb.memoryLimit",
      type: string(),
      description:
         "memory_limit of every DuckDB session, as a DuckDB size (1GB), or off; unset warns at boot.",
      aliases: { env: ["PUBLISHER_DUCKDB_MEMORY_LIMIT"] },
   }),
   setting({
      key: "duckdb.tempDirectory",
      type: string(),
      description: "Directory DuckDB spills to.",
      aliases: { env: ["PUBLISHER_DUCKDB_TEMP_DIRECTORY"] },
   }),
   setting({
      key: "duckdb.partitionedWriteFlushThreshold",
      // A row count or `off`, which turns the partitioned-insert bounds off.
      type: string(),
      default: "8192",
      description:
         "Rows a partitioned storage build buffers before flushing, or off.",
      aliases: { env: ["PUBLISHER_PARTITIONED_WRITE_FLUSH_THRESHOLD"] },
   }),
   setting({
      key: "ducklake.rowGroupSizeBytes",
      type: string(),
      description:
         "Bytes DuckLake buffers per column before flushing a Parquet row group (16MB).",
      aliases: { env: ["PUBLISHER_DUCKLAKE_ROW_GROUP_SIZE_BYTES"] },
   }),
   setting({
      key: "ducklake.targetFileSizeBytes",
      type: string(),
      description:
         "Size a DuckLake Parquet file grows to before rotating (256MB).",
      aliases: { env: ["PUBLISHER_DUCKLAKE_TARGET_FILE_SIZE_BYTES"] },
   }),
   setting({
      key: "persist.storageMode",
      type: oneOf(["off", "write-only", "on"]),
      default: "off",
      description:
         "How #@ persist storage= is honored: off, write-only (build only) or on (build and serve).",
      aliases: { env: ["PERSIST_STORAGE_MODE"] },
   }),
   setting({
      key: "persist.collisionEnforcementEnabled",
      type: boolean(),
      default: false,
      description:
         "Refuse a publish where two persist sources resolve to the same table.",
      aliases: { env: ["PERSIST_COLLISION_ENFORCE"] },
   }),
   setting({
      key: "materialization.schedulerEnabled",
      type: boolean(),
      default: false,
      description:
         "Fire packages' materialization schedules from this process; never on an orchestrated worker.",
      aliases: { env: ["PUBLISHER_LOCAL_MATERIALIZATION_SCHEDULER"] },
   }),
   setting({
      key: "materialization.schedulerIntervalMs",
      type: integer({ min: 1000 }),
      default: 60_000,
      description: "How often the scheduler looks for due schedules, in ms.",
      aliases: { env: ["PUBLISHER_MATERIALIZATION_SCHEDULER_INTERVAL_MS"] },
   }),
   setting({
      key: "materialization.schedulerMaxFiresPerTick",
      type: integer({ min: 1 }),
      default: 10,
      description: "Packages the scheduler fires per look.",
      aliases: {
         env: ["PUBLISHER_MATERIALIZATION_SCHEDULER_MAX_FIRES_PER_TICK"],
      },
   }),
];
