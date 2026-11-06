// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   DEFAULT_MAX_CONCURRENT_QUERIES,
   DEFAULT_MAX_QUERY_ROWS,
   DEFAULT_MAX_RESPONSE_BYTES,
   DEFAULT_PACKAGE_LOAD_SCHEMA_CACHE_BYTES,
   DEFAULT_POSTGRES_POOL_MAX,
   DEFAULT_QUERY_ROW_LIMIT,
   DEFAULT_QUERY_TIMEOUT_MS,
} from "../../constants";
import { setting } from "../setting";
import { boolean, integer, number } from "../types";

/** The configuration design's `query`, `postgres`, `memory`, `packageLoad` table. */
export const query = [
   setting({
      key: "query.maxRows",
      type: integer({ min: 0 }),
      default: DEFAULT_MAX_QUERY_ROWS,
      description:
         "Row cap on ad-hoc connection SQL; 0 leaves only the byte cap.",
      aliases: { env: ["PUBLISHER_MAX_QUERY_ROWS"] },
   }),
   setting({
      key: "query.maxResponseBytes",
      type: integer({ min: 0 }),
      default: DEFAULT_MAX_RESPONSE_BYTES,
      description:
         "Byte cap on ad-hoc connection SQL responses; 0 leaves only the row cap.",
      aliases: { env: ["PUBLISHER_MAX_RESPONSE_BYTES"] },
   }),
   setting({
      key: "query.defaultRowLimit",
      type: integer({ min: 1 }),
      default: DEFAULT_QUERY_ROW_LIMIT,
      description: "Row limit of a model query that sets none of its own.",
      aliases: { env: ["PUBLISHER_DEFAULT_QUERY_ROW_LIMIT"] },
   }),
   setting({
      key: "query.timeoutMs",
      type: integer({ min: 0 }),
      default: DEFAULT_QUERY_TIMEOUT_MS,
      description: "Per-query wall-clock timeout in ms; 0 turns it off.",
      aliases: { env: ["PUBLISHER_QUERY_TIMEOUT_MS"] },
   }),
   setting({
      key: "query.maxConcurrent",
      type: integer({ min: 0 }),
      default: DEFAULT_MAX_CONCURRENT_QUERIES,
      description: "Queries this process runs at once; 0 turns the cap off.",
      aliases: { env: ["PUBLISHER_MAX_CONCURRENT_QUERIES"] },
   }),
   setting({
      key: "query.metadataEnabled",
      type: boolean(),
      default: false,
      description: "Attach per-query metadata to every statement.",
      aliases: {
         env: ["PUBLISHER_QUERY_METADATA"],
         today: { reads: "on-off" },
      },
   }),
   setting({
      key: "postgres.poolMax",
      type: integer({ min: 1 }),
      default: DEFAULT_POSTGRES_POOL_MAX,
      description: "Open sessions per plain Postgres connection.",
      aliases: { env: ["PUBLISHER_POSTGRES_POOL_MAX"] },
   }),
   setting({
      key: "memory.maxBytes",
      type: integer({ min: 0 }),
      default: 0,
      description:
         "Process RSS ceiling for the memory governor, in bytes; 0 turns the governor off.",
      aliases: { env: ["PUBLISHER_MAX_MEMORY_BYTES"] },
   }),
   setting({
      key: "memory.highWaterFraction",
      type: number({ above: 0, below: 1 }),
      default: 0.8,
      description:
         "Share of memory.maxBytes at which new package loads are refused.",
      aliases: { env: ["PUBLISHER_MEMORY_HIGH_WATER_FRACTION"] },
   }),
   setting({
      key: "memory.lowWaterFraction",
      type: number({ above: 0, below: 1 }),
      default: 0.7,
      description:
         "Share of memory.maxBytes at which loads are admitted again; below the high-water mark.",
      aliases: { env: ["PUBLISHER_MEMORY_LOW_WATER_FRACTION"] },
   }),
   setting({
      key: "memory.checkIntervalMs",
      type: integer({ min: 100 }),
      default: 5000,
      description: "How often the memory governor samples RSS, in ms.",
      aliases: { env: ["PUBLISHER_MEMORY_CHECK_INTERVAL_MS"] },
   }),
   setting({
      key: "memory.backpressureEnabled",
      type: boolean(),
      default: true,
      description:
         "Refuse loads above the high-water mark; off only samples and reports.",
      aliases: { env: ["PUBLISHER_MEMORY_BACKPRESSURE"] },
   }),
   setting({
      key: "packageLoad.workers",
      type: integer({ min: 1 }),
      default: 1,
      description: "Package-load worker threads.",
      aliases: { env: ["PACKAGE_LOAD_WORKERS"] },
   }),
   setting({
      key: "packageLoad.jobTimeoutMs",
      type: integer({ min: 1 }),
      default: 120_000,
      description: "Compile budget of one package load, in ms.",
      aliases: { env: ["PACKAGE_LOAD_JOB_TIMEOUT_MS"] },
   }),
   setting({
      key: "packageLoad.schemaCacheBytes",
      type: integer({ min: 0 }),
      default: DEFAULT_PACKAGE_LOAD_SCHEMA_CACHE_BYTES,
      description:
         "Serialized schema each package-load worker caches across loads, in bytes; 0 turns it off.",
      aliases: { env: ["PACKAGE_LOAD_SCHEMA_CACHE_BYTES"] },
   }),
];
