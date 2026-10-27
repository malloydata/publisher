// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { Settings } from ".";
import { assertServerApis } from "../server_apis";
import { getServerApiSettings } from "../config";
import { withSettings, type SettingsInput } from "../test_helpers/settings_env";
import { PROBES } from "../test_helpers/settings_probes";

/**
 * What each setting gives for each way an operator can set it, today. One
 * entry per setting, under its key in the configuration design
 * (docs/configuration-design.md), with today's variable, flag and file key as
 * the sources. Every row is a case a deployment can hit: unset, each source
 * read today, a blank variable, and values that are wrong. The expected result
 * is today's, quirks included, so a change to how a setting is read shows up
 * here as a changed row. Step 1's rules (#1477) are such rows: a value of
 * another type fails startup, a blank variable is unset, a boolean today
 * reads its own way keeps its meaning with a warning, and an unreadable file
 * fails startup.
 *
 * A row expects a value, with the warning startup gives if any, or a failure
 * whose message names the setting. A failure is at startup, as server.ts
 * reports it, or where the server reads the setting.
 */
type Expected = { value: unknown; warns?: string } | { fails: string };

interface Case extends SettingsInput {
   name: string;
   expect: Expected;
}

const value = (v: unknown): Expected => ({ value: v });
const warned = (v: unknown, warning: string): Expected => ({
   value: v,
   warns: warning,
});
const fails = (name: string): Expected => ({ fails: name });

/** Embedding settings are off without a key, so their rows set one. */
const KEY = { EMBEDDING_API_KEY: "sk-test" };
/** The memory governor's tunables are read only with a ceiling set. */
const CEILING = { PUBLISHER_MAX_MEMORY_BYTES: "1000000" };
/** The scheduler's tunables are read only with the scheduler on. */
const SCHEDULER = { PUBLISHER_LOCAL_MATERIALIZATION_SCHEDULER: "true" };
/** The admin API's storage is read only with a catalog database set. */
const CATALOG = { PUBLISHER_ADMIN_CATALOG_DATABASE: "postgresql://h/catalog" };
const S3 = {
   ...CATALOG,
   PUBLISHER_ADMIN_STORAGE_PROVIDER: "s3",
   PUBLISHER_ADMIN_STORAGE_KEY_ID: "AKID",
   PUBLISHER_ADMIN_STORAGE_SECRET: "SECRET",
};
const ONE_ENVIRONMENT = [
   { name: "e", packages: [{ name: "p", location: "./p" }] },
];

export const CONTRACT: Record<string, Case[]> = {
   // ── Server (top level) ────────────────────────────────────────────────
   frozenConfig: [
      { name: "unset", expect: value(false) },
      {
         name: "true in the file",
         file: { frozenConfig: true },
         expect: value(true),
      },
      {
         name: "false in the file",
         file: { frozenConfig: false },
         expect: value(false),
      },
      {
         name: 'the string "false" still freezes the server, with a warning',
         file: { frozenConfig: "false" },
         expect: warned(true, '"frozenConfig": "false"'),
      },
      {
         name: "a ${VAR} still reads as a string, so any value freezes, with a warning",
         env: { FROZEN: "false" },
         file: { frozenConfig: "${FROZEN}" },
         expect: warned(true, '"frozenConfig": "${FROZEN}"'),
      },
      {
         name: "an unreadable file fails startup",
         file: "{",
         expect: fails("Failed to parse"),
      },
   ],
   servingApiEnabled: [
      { name: "unset", expect: value(true) },
      {
         name: "PUBLISHER_SERVING_API_ENABLED",
         env: { PUBLISHER_SERVING_API_ENABLED: "false" },
         expect: value(false),
      },
      {
         name: "--no-serving-api-enabled",
         argv: ["--no-serving-api-enabled"],
         expect: value(false),
      },
      {
         name: "--serving-api-enabled wins over the variable",
         env: { PUBLISHER_SERVING_API_ENABLED: "false" },
         argv: ["--serving-api-enabled"],
         expect: value(true),
      },
      {
         name: "the file",
         file: { servingApiEnabled: false },
         expect: value(false),
      },
      {
         name: "the variable wins over the file",
         env: { PUBLISHER_SERVING_API_ENABLED: "true" },
         file: { servingApiEnabled: false },
         expect: value(true),
      },
      {
         name: "a ${VAR} in the file is read as a boolean",
         env: { SERVE: "no" },
         file: { servingApiEnabled: "${SERVE}" },
         expect: value(false),
      },
      {
         name: "blank",
         env: { PUBLISHER_SERVING_API_ENABLED: "" },
         expect: value(true),
      },
      {
         name: "an invalid variable",
         env: { PUBLISHER_SERVING_API_ENABLED: "enabled" },
         expect: fails("PUBLISHER_SERVING_API_ENABLED"),
      },
      {
         name: "a string in the file",
         file: { servingApiEnabled: "yes" },
         expect: fails("servingApiEnabled"),
      },
      {
         name: "an unreadable file fails startup",
         file: "{",
         expect: fails("Failed to parse"),
      },
   ],
   adminApiEnabled: [
      { name: "unset", expect: value(true) },
      {
         name: "PUBLISHER_ADMIN_API_ENABLED",
         env: { PUBLISHER_ADMIN_API_ENABLED: "0" },
         expect: value(false),
      },
      {
         name: "--no-admin-api-enabled",
         argv: ["--no-admin-api-enabled"],
         expect: value(false),
      },
      {
         name: "--admin-api-enabled wins over the variable",
         env: { PUBLISHER_ADMIN_API_ENABLED: "false" },
         argv: ["--admin-api-enabled"],
         expect: value(true),
      },
      {
         name: "the file",
         file: { adminApiEnabled: false },
         expect: value(false),
      },
      {
         name: "blank",
         env: { PUBLISHER_ADMIN_API_ENABLED: "" },
         expect: value(true),
      },
      {
         name: "an invalid variable",
         env: { PUBLISHER_ADMIN_API_ENABLED: "maybe" },
         expect: fails("PUBLISHER_ADMIN_API_ENABLED"),
      },
   ],
   port: [
      { name: "unset", expect: value(4000) },
      {
         name: "PUBLISHER_PORT",
         env: { PUBLISHER_PORT: "4100" },
         expect: value(4100),
      },
      { name: "--port", argv: ["--port", "4200"], expect: value(4200) },
      {
         name: "--port wins over the variable",
         env: { PUBLISHER_PORT: "4100" },
         argv: ["--port", "4200"],
         expect: value(4200),
      },
      {
         name: "--port with no value is ignored, with a warning",
         argv: ["--port"],
         expect: warned(4000, "--port has no value after it"),
      },
      { name: "blank", env: { PUBLISHER_PORT: "" }, expect: value(4000) },
      {
         name: "not a number fails",
         env: { PUBLISHER_PORT: "40a0" },
         expect: fails("PUBLISHER_PORT"),
      },
   ],
   host: [
      { name: "unset", expect: value("0.0.0.0") },
      {
         name: "PUBLISHER_HOST",
         env: { PUBLISHER_HOST: "127.0.0.1" },
         expect: value("127.0.0.1"),
      },
      {
         name: "--host",
         argv: ["--host", "10.0.0.1"],
         expect: value("10.0.0.1"),
      },
      {
         name: "--host wins over the variable",
         env: { PUBLISHER_HOST: "127.0.0.1" },
         argv: ["--host", "10.0.0.1"],
         expect: value("10.0.0.1"),
      },
      { name: "blank", env: { PUBLISHER_HOST: "" }, expect: value("0.0.0.0") },
   ],
   rateLimit: [
      { name: "unset is unlimited", expect: value(0) },
      {
         name: "PUBLISHER_RATE_LIMIT",
         env: { PUBLISHER_RATE_LIMIT: "120" },
         expect: value(120),
      },
      {
         name: "0 is unlimited",
         env: { PUBLISHER_RATE_LIMIT: "0" },
         expect: value(0),
      },
      { name: "blank", env: { PUBLISHER_RATE_LIMIT: "" }, expect: value(0) },
      {
         name: "negative",
         env: { PUBLISHER_RATE_LIMIT: "-1" },
         expect: fails("PUBLISHER_RATE_LIMIT"),
      },
      {
         name: "not a number",
         env: { PUBLISHER_RATE_LIMIT: "lots" },
         expect: fails("PUBLISHER_RATE_LIMIT"),
      },
   ],
   frameAncestors: [
      { name: "unset", expect: value("'self'") },
      {
         name: "PUBLISHER_FRAME_ANCESTORS",
         env: { PUBLISHER_FRAME_ANCESTORS: "https://a.example" },
         expect: value("https://a.example"),
      },
      {
         name: "blank",
         env: { PUBLISHER_FRAME_ANCESTORS: "" },
         expect: value("'self'"),
      },
   ],
   theme: [
      { name: "unset", expect: value(undefined) },
      {
         name: "the file",
         file: { theme: { defaultMode: "dark" } },
         expect: value({ defaultMode: "dark" }),
      },
   ],
   environments: [
      { name: "unset", expect: value([]) },
      {
         name: "the file",
         file: { environments: ONE_ENVIRONMENT },
         expect: value(["e"]),
      },
      {
         name: 'the old "projects" key is still read',
         file: { projects: ONE_ENVIRONMENT },
         expect: value(["e"]),
      },
   ],

   // ── Launch settings ───────────────────────────────────────────────────
   root: [
      { name: "unset", expect: value(".") },
      {
         name: "SERVER_ROOT",
         env: { SERVER_ROOT: "/srv/models" },
         expect: value("/srv/models"),
      },
      {
         name: "--server_root",
         argv: ["--server_root", "/x"],
         expect: value("/x"),
      },
      {
         name: "--server_root wins over the variable",
         env: { SERVER_ROOT: "/srv/models" },
         argv: ["--server_root", "/x"],
         expect: value("/x"),
      },
      { name: "blank", env: { SERVER_ROOT: "" }, expect: value(".") },
   ],
   config: [
      { name: "unset", expect: value(undefined) },
      {
         name: "PUBLISHER_CONFIG_PATH",
         env: { PUBLISHER_CONFIG_PATH: "/nonexistent/a.json" },
         expect: value("/nonexistent/a.json"),
      },
      {
         name: "--config",
         argv: ["--config", "/nonexistent/b.json"],
         expect: value("/nonexistent/b.json"),
      },
      {
         name: "--config wins over the variable",
         env: { PUBLISHER_CONFIG_PATH: "/nonexistent/a.json" },
         argv: ["--config", "/nonexistent/b.json"],
         expect: value("/nonexistent/b.json"),
      },
      {
         name: "blank",
         env: { PUBLISHER_CONFIG_PATH: "" },
         expect: value(undefined),
      },
   ],
   useBundledDefault: [
      { name: "never in tests", expect: value(false) },
      {
         name: "on with no flags",
         env: { NODE_ENV: "production" },
         expect: value(true),
      },
      {
         name: "--server_root turns it off",
         env: { NODE_ENV: "production" },
         argv: ["--server_root", "/x"],
         expect: value(false),
      },
      {
         name: "SERVER_ROOT does not turn it off",
         env: { NODE_ENV: "production", SERVER_ROOT: "/x" },
         expect: value(true),
      },
      {
         name: "the variable cannot turn it off",
         env: {
            NODE_ENV: "production",
            PUBLISHER_USE_BUNDLED_DEFAULT: "false",
         },
         expect: value(true),
      },
      {
         name: "the variable turns it on despite a flag",
         env: { NODE_ENV: "production", PUBLISHER_USE_BUNDLED_DEFAULT: "true" },
         argv: ["--server_root", "/x"],
         expect: value(true),
      },
      {
         name: 'only exactly "true" turns it on, with a warning for 1',
         env: { NODE_ENV: "production", PUBLISHER_USE_BUNDLED_DEFAULT: "1" },
         argv: ["--server_root", "/x"],
         expect: warned(
            false,
            "PUBLISHER_USE_BUNDLED_DEFAULT=1 is read as off",
         ),
      },
      {
         name: "the variable turns it on in tests",
         env: { PUBLISHER_USE_BUNDLED_DEFAULT: "true" },
         expect: value(true),
      },
   ],
   initializeStorage: [
      { name: "unset", expect: value(false) },
      {
         name: "INITIALIZE_STORAGE",
         env: { INITIALIZE_STORAGE: "true" },
         expect: value(true),
      },
      { name: "--init", argv: ["--init"], expect: value(true) },
      {
         name: '"1" is still off, with a warning',
         env: { INITIALIZE_STORAGE: "1" },
         expect: warned(false, "INITIALIZE_STORAGE=1 is read as off"),
      },
      {
         name: '"TRUE" is still off, with a warning',
         env: { INITIALIZE_STORAGE: "TRUE" },
         expect: warned(false, "INITIALIZE_STORAGE=TRUE is read as off"),
      },
      {
         name: '"yes" is still off, with a warning',
         env: { INITIALIZE_STORAGE: "yes" },
         expect: warned(false, "INITIALIZE_STORAGE=yes is read as off"),
      },
      { name: "blank", env: { INITIALIZE_STORAGE: "" }, expect: value(false) },
      {
         name: "not a boolean fails",
         env: { INITIALIZE_STORAGE: "enabled" },
         expect: fails("INITIALIZE_STORAGE"),
      },
   ],
   watchEnvironments: [
      { name: "unset", expect: value([]) },
      {
         name: "PUBLISHER_WATCH",
         env: { PUBLISHER_WATCH: "a,b" },
         expect: value(["a", "b"]),
      },
      { name: "--watch-env", argv: ["--watch-env", "c"], expect: value(["c"]) },
      {
         name: "--watch-env adds to the variable",
         env: { PUBLISHER_WATCH: "a" },
         argv: ["--watch-env", "c"],
         expect: value(["a", "c"]),
      },
      {
         name: "--watch-env repeats",
         argv: ["--watch-env", "c", "--watch-env", "d"],
         expect: value(["c", "d"]),
      },
      { name: "blank", env: { PUBLISHER_WATCH: "" }, expect: value([]) },
      {
         name: "spaces and empty items",
         env: { PUBLISHER_WATCH: " a , ,b" },
         expect: value(["a", "b"]),
      },
      {
         name: "--watch-env with no value is ignored, with a warning",
         argv: ["--watch-env"],
         expect: warned([], "--watch-env has no value after it"),
      },
   ],

   // ── mcp ───────────────────────────────────────────────────────────────
   "mcp.port": [
      { name: "unset", expect: value(4040) },
      { name: "MCP_PORT", env: { MCP_PORT: "4141" }, expect: value(4141) },
      { name: "--mcp_port", argv: ["--mcp_port", "4242"], expect: value(4242) },
      {
         name: "--mcp_port wins over the variable",
         env: { MCP_PORT: "4141" },
         argv: ["--mcp_port", "4242"],
         expect: value(4242),
      },
      {
         name: "0 is an ephemeral port",
         env: { MCP_PORT: "0" },
         expect: value(0),
      },
      { name: "blank", env: { MCP_PORT: "" }, expect: value(4040) },
      {
         name: "not a number fails",
         env: { MCP_PORT: "40a0" },
         expect: fails("MCP_PORT"),
      },
   ],
   "mcp.host": [
      { name: "unset is loopback", expect: value("127.0.0.1") },
      {
         name: "MCP_HOST",
         env: { MCP_HOST: "0.0.0.0" },
         expect: value("0.0.0.0"),
      },
      {
         name: "--mcp_host",
         argv: ["--mcp_host", "10.0.0.2"],
         expect: value("10.0.0.2"),
      },
      {
         name: "follows PUBLISHER_HOST",
         env: { PUBLISHER_HOST: "10.0.0.1" },
         expect: value("10.0.0.1"),
      },
      {
         name: "follows --host",
         argv: ["--host", "10.0.0.3"],
         expect: value("10.0.0.3"),
      },
      {
         name: "MCP_HOST wins over PUBLISHER_HOST",
         env: { PUBLISHER_HOST: "10.0.0.1", MCP_HOST: "10.0.0.2" },
         expect: value("10.0.0.2"),
      },
      {
         name: "blank follows PUBLISHER_HOST",
         env: { PUBLISHER_HOST: "10.0.0.1", MCP_HOST: "" },
         expect: value("10.0.0.1"),
      },
   ],
   "mcp.corsOrigins": [
      { name: "unset denies", expect: value(false) },
      {
         name: "* allows any",
         env: { MCP_CORS_ORIGINS: "*" },
         expect: value("*"),
      },
      {
         name: "a list",
         env: { MCP_CORS_ORIGINS: "https://a.example, https://b.example" },
         expect: value(["https://a.example", "https://b.example"]),
      },
      {
         name: "a list of nothing denies",
         env: { MCP_CORS_ORIGINS: "," },
         expect: value(false),
      },
      { name: "blank", env: { MCP_CORS_ORIGINS: "" }, expect: value(false) },
   ],
   "mcp.configFileEnabled": [
      { name: "unset", expect: value(true) },
      {
         name: "PUBLISHER_NO_MCP_CONFIG=1",
         env: { PUBLISHER_NO_MCP_CONFIG: "1" },
         expect: value(false),
      },
      {
         name: "PUBLISHER_NO_MCP_CONFIG=false",
         env: { PUBLISHER_NO_MCP_CONFIG: "false" },
         expect: value(true),
      },
      {
         name: "--no-mcp-config",
         argv: ["--no-mcp-config"],
         expect: value(false),
      },
      {
         name: "blank",
         env: { PUBLISHER_NO_MCP_CONFIG: "" },
         expect: value(true),
      },
      {
         name: "not a boolean",
         env: { PUBLISHER_NO_MCP_CONFIG: "maybe" },
         expect: fails("PUBLISHER_NO_MCP_CONFIG"),
      },
   ],
   "mcp.includeHiddenFilesAndSources": [
      { name: "unset", expect: value(false) },
      {
         name: "the file",
         file: { mcp: { includeHiddenFilesAndSources: true } },
         expect: value(true),
      },
      {
         name: "a string in the file is warned about and ignored",
         file: { mcp: { includeHiddenFilesAndSources: "yes" } },
         expect: warned(false, '"mcp.includeHiddenFilesAndSources": "yes"'),
      },
      {
         name: "an mcp that is not an object is warned about and ignored",
         file: { mcp: "on" },
         expect: warned(false, 'Invalid "mcp"'),
      },
      {
         name: "an unreadable file fails startup",
         file: "{",
         expect: fails("Failed to parse"),
      },
   ],
   "mcp.trace": [
      { name: "unset", expect: value(false) },
      {
         name: "retrieval",
         env: { PUBLISHER_MCP_TRACE: "retrieval" },
         expect: value(true),
      },
      {
         name: "any case",
         env: { PUBLISHER_MCP_TRACE: " Retrieval " },
         expect: value(true),
      },
      {
         name: "another value fails",
         env: { PUBLISHER_MCP_TRACE: "summary" },
         expect: fails("PUBLISHER_MCP_TRACE"),
      },
      { name: "blank", env: { PUBLISHER_MCP_TRACE: "" }, expect: value(false) },
   ],

   // ── admin ─────────────────────────────────────────────────────────────
   "admin.port": [
      { name: "unset is off", expect: value(null) },
      {
         name: "PUBLISHER_ADMIN_PORT",
         env: { PUBLISHER_ADMIN_PORT: "4100" },
         expect: value(4100),
      },
      {
         name: "--admin_port",
         argv: ["--admin_port", "4200"],
         expect: value(4200),
      },
      {
         name: "--admin_port wins over the variable",
         env: { PUBLISHER_ADMIN_PORT: "4100" },
         argv: ["--admin_port", "4200"],
         expect: value(4200),
      },
      { name: "blank", env: { PUBLISHER_ADMIN_PORT: "" }, expect: value(null) },
      {
         name: "not a port",
         env: { PUBLISHER_ADMIN_PORT: "abc" },
         expect: fails("PUBLISHER_ADMIN_PORT"),
      },
      {
         name: "out of range",
         env: { PUBLISHER_ADMIN_PORT: "70000" },
         expect: fails("PUBLISHER_ADMIN_PORT"),
      },
   ],
   "admin.host": [
      { name: "unset is loopback", expect: value("127.0.0.1") },
      {
         name: "PUBLISHER_ADMIN_HOST",
         env: { PUBLISHER_ADMIN_HOST: "0.0.0.0" },
         expect: value("0.0.0.0"),
      },
      {
         name: "--admin_host",
         argv: ["--admin_host", "10.0.0.4"],
         expect: value("10.0.0.4"),
      },
      {
         name: "does not follow PUBLISHER_HOST",
         env: { PUBLISHER_HOST: "0.0.0.0" },
         expect: value("127.0.0.1"),
      },
      {
         name: "blank",
         env: { PUBLISHER_ADMIN_HOST: "" },
         expect: value("127.0.0.1"),
      },
   ],
   "admin.catalogDatabase": [
      { name: "unset is off", expect: value(null) },
      {
         name: "PUBLISHER_ADMIN_CATALOG_DATABASE",
         env: CATALOG,
         expect: value("postgresql://h/catalog"),
      },
      {
         name: "blank",
         env: { PUBLISHER_ADMIN_CATALOG_DATABASE: "" },
         expect: value(null),
      },
      {
         name: "libpq options are refused",
         env: {
            PUBLISHER_ADMIN_CATALOG_DATABASE: "postgresql://h/db?options=-c",
         },
         expect: fails("PUBLISHER_ADMIN_CATALOG_DATABASE"),
      },
   ],
   "admin.storage.provider": [
      { name: "unset", env: CATALOG, expect: value(undefined) },
      {
         name: "s3",
         env: S3,
         expect: value({
            s3Connection: {
               provider: "config",
               accessKeyId: "AKID",
               secretAccessKey: "SECRET",
            },
         }),
      },
      {
         name: "s3_credential_chain",
         env: {
            ...CATALOG,
            PUBLISHER_ADMIN_STORAGE_PROVIDER: "s3_credential_chain",
         },
         expect: value({ s3Connection: { provider: "credential_chain" } }),
      },
      {
         name: "gcs",
         env: { ...S3, PUBLISHER_ADMIN_STORAGE_PROVIDER: "gcs" },
         expect: value({ gcsConnection: { keyId: "AKID", secret: "SECRET" } }),
      },
      {
         name: "blank",
         env: { ...CATALOG, PUBLISHER_ADMIN_STORAGE_PROVIDER: "" },
         expect: value(undefined),
      },
      {
         name: "an unknown provider",
         env: { ...CATALOG, PUBLISHER_ADMIN_STORAGE_PROVIDER: "azure" },
         expect: fails("PUBLISHER_ADMIN_STORAGE_PROVIDER"),
      },
   ],
   "admin.storage.keyId": [
      {
         name: "s3 needs one",
         env: { ...S3, PUBLISHER_ADMIN_STORAGE_KEY_ID: "" },
         expect: fails("PUBLISHER_ADMIN_STORAGE_KEY_ID"),
      },
   ],
   "admin.storage.secret": [
      {
         name: "gcs needs one",
         env: {
            ...S3,
            PUBLISHER_ADMIN_STORAGE_PROVIDER: "gcs",
            PUBLISHER_ADMIN_STORAGE_SECRET: "",
         },
         expect: fails("PUBLISHER_ADMIN_STORAGE_SECRET"),
      },
   ],
   "admin.storage.region": [
      {
         name: "s3",
         env: { ...S3, PUBLISHER_ADMIN_STORAGE_REGION: "us-east-1" },
         expect: value({
            s3Connection: {
               provider: "config",
               accessKeyId: "AKID",
               secretAccessKey: "SECRET",
               region: "us-east-1",
            },
         }),
      },
      {
         name: "s3_credential_chain",
         env: {
            ...CATALOG,
            PUBLISHER_ADMIN_STORAGE_PROVIDER: "s3_credential_chain",
            PUBLISHER_ADMIN_STORAGE_REGION: "us-west-2",
         },
         expect: value({
            s3Connection: { provider: "credential_chain", region: "us-west-2" },
         }),
      },
   ],
   "admin.storage.endpoint": [
      {
         name: "s3",
         env: { ...S3, PUBLISHER_ADMIN_STORAGE_ENDPOINT: "http://minio:9000" },
         expect: value({
            s3Connection: {
               provider: "config",
               accessKeyId: "AKID",
               secretAccessKey: "SECRET",
               endpoint: "http://minio:9000",
            },
         }),
      },
   ],

   // ── query, postgres, memory, packageLoad ──────────────────────────────
   "query.maxRows": [
      { name: "unset", expect: value(100_000) },
      {
         name: "PUBLISHER_MAX_QUERY_ROWS",
         env: { PUBLISHER_MAX_QUERY_ROWS: "500" },
         expect: value(500),
      },
      {
         name: "0 turns the cap off",
         env: { PUBLISHER_MAX_QUERY_ROWS: "0" },
         expect: value(0),
      },
      {
         name: "blank",
         env: { PUBLISHER_MAX_QUERY_ROWS: "" },
         expect: value(100_000),
      },
      {
         name: "negative",
         env: { PUBLISHER_MAX_QUERY_ROWS: "-1" },
         expect: fails("PUBLISHER_MAX_QUERY_ROWS"),
      },
      {
         name: "an exponent",
         env: { PUBLISHER_MAX_QUERY_ROWS: "1e3" },
         expect: fails("PUBLISHER_MAX_QUERY_ROWS"),
      },
      {
         name: "not a number",
         env: { PUBLISHER_MAX_QUERY_ROWS: "many" },
         expect: fails("PUBLISHER_MAX_QUERY_ROWS"),
      },
   ],
   "query.maxResponseBytes": [
      { name: "unset", expect: value(50_000_000) },
      {
         name: "PUBLISHER_MAX_RESPONSE_BYTES",
         env: { PUBLISHER_MAX_RESPONSE_BYTES: "1000" },
         expect: value(1000),
      },
      {
         name: "blank",
         env: { PUBLISHER_MAX_RESPONSE_BYTES: "" },
         expect: value(50_000_000),
      },
      {
         name: "negative",
         env: { PUBLISHER_MAX_RESPONSE_BYTES: "-5" },
         expect: fails("PUBLISHER_MAX_RESPONSE_BYTES"),
      },
   ],
   "query.defaultRowLimit": [
      { name: "unset", expect: value(1000) },
      {
         name: "PUBLISHER_DEFAULT_QUERY_ROW_LIMIT",
         env: { PUBLISHER_DEFAULT_QUERY_ROW_LIMIT: "50" },
         expect: value(50),
      },
      {
         name: "blank",
         env: { PUBLISHER_DEFAULT_QUERY_ROW_LIMIT: "" },
         expect: value(1000),
      },
      {
         name: "0 is refused",
         env: { PUBLISHER_DEFAULT_QUERY_ROW_LIMIT: "0" },
         expect: fails("PUBLISHER_DEFAULT_QUERY_ROW_LIMIT"),
      },
   ],
   "query.timeoutMs": [
      { name: "unset", expect: value(300_000) },
      {
         name: "PUBLISHER_QUERY_TIMEOUT_MS",
         env: { PUBLISHER_QUERY_TIMEOUT_MS: "1000" },
         expect: value(1000),
      },
      {
         name: "0 turns it off",
         env: { PUBLISHER_QUERY_TIMEOUT_MS: "0" },
         expect: value(0),
      },
      {
         name: "blank",
         env: { PUBLISHER_QUERY_TIMEOUT_MS: "" },
         expect: value(300_000),
      },
      {
         name: "negative",
         env: { PUBLISHER_QUERY_TIMEOUT_MS: "-1" },
         expect: fails("PUBLISHER_QUERY_TIMEOUT_MS"),
      },
   ],
   "query.maxConcurrent": [
      { name: "unset", expect: value(32) },
      {
         name: "PUBLISHER_MAX_CONCURRENT_QUERIES",
         env: { PUBLISHER_MAX_CONCURRENT_QUERIES: "4" },
         expect: value(4),
      },
      {
         name: "0 turns it off",
         env: { PUBLISHER_MAX_CONCURRENT_QUERIES: "0" },
         expect: value(0),
      },
      {
         name: "blank",
         env: { PUBLISHER_MAX_CONCURRENT_QUERIES: "" },
         expect: value(32),
      },
      {
         name: "negative",
         env: { PUBLISHER_MAX_CONCURRENT_QUERIES: "-1" },
         expect: fails("PUBLISHER_MAX_CONCURRENT_QUERIES"),
      },
   ],
   "query.metadataEnabled": [
      { name: "unset", expect: value("off") },
      {
         name: "on",
         env: { PUBLISHER_QUERY_METADATA: "on" },
         expect: value("on"),
      },
      {
         name: "any case",
         env: { PUBLISHER_QUERY_METADATA: " On " },
         expect: value("on"),
      },
      {
         name: "off",
         env: { PUBLISHER_QUERY_METADATA: "OFF" },
         expect: value("off"),
      },
      {
         name: "blank",
         env: { PUBLISHER_QUERY_METADATA: "" },
         expect: value("off"),
      },
      {
         name: '"true" is refused',
         env: { PUBLISHER_QUERY_METADATA: "true" },
         expect: fails("PUBLISHER_QUERY_METADATA"),
      },
   ],
   "postgres.poolMax": [
      { name: "unset", expect: value(5) },
      {
         name: "PUBLISHER_POSTGRES_POOL_MAX",
         env: { PUBLISHER_POSTGRES_POOL_MAX: "10" },
         expect: value(10),
      },
      {
         name: "blank",
         env: { PUBLISHER_POSTGRES_POOL_MAX: "" },
         expect: value(5),
      },
      {
         name: "0 is refused",
         env: { PUBLISHER_POSTGRES_POOL_MAX: "0" },
         expect: fails("PUBLISHER_POSTGRES_POOL_MAX"),
      },
   ],
   "memory.maxBytes": [
      { name: "unset turns the governor off", expect: value(null) },
      {
         name: "0 turns the governor off",
         env: { PUBLISHER_MAX_MEMORY_BYTES: "0" },
         expect: value(null),
      },
      {
         name: "PUBLISHER_MAX_MEMORY_BYTES",
         env: CEILING,
         expect: value(1_000_000),
      },
      {
         name: "blank",
         env: { PUBLISHER_MAX_MEMORY_BYTES: "" },
         expect: value(null),
      },
      {
         name: "negative",
         env: { PUBLISHER_MAX_MEMORY_BYTES: "-1" },
         expect: fails("PUBLISHER_MAX_MEMORY_BYTES"),
      },
   ],
   "memory.highWaterFraction": [
      { name: "unset", env: CEILING, expect: value(0.8) },
      {
         name: "PUBLISHER_MEMORY_HIGH_WATER_FRACTION",
         env: { ...CEILING, PUBLISHER_MEMORY_HIGH_WATER_FRACTION: "0.9" },
         expect: value(0.9),
      },
      {
         name: "blank",
         env: { ...CEILING, PUBLISHER_MEMORY_HIGH_WATER_FRACTION: "" },
         expect: value(0.8),
      },
      {
         name: "1 or more",
         env: { ...CEILING, PUBLISHER_MEMORY_HIGH_WATER_FRACTION: "1.5" },
         expect: fails("PUBLISHER_MEMORY_HIGH_WATER_FRACTION"),
      },
      {
         name: "not a number",
         env: { ...CEILING, PUBLISHER_MEMORY_HIGH_WATER_FRACTION: "high" },
         expect: fails("PUBLISHER_MEMORY_HIGH_WATER_FRACTION"),
      },
      {
         name: "at or below the low-water mark",
         env: { ...CEILING, PUBLISHER_MEMORY_HIGH_WATER_FRACTION: "0.6" },
         expect: fails("PUBLISHER_MEMORY_HIGH_WATER_FRACTION"),
      },
   ],
   "memory.lowWaterFraction": [
      { name: "unset", env: CEILING, expect: value(0.7) },
      {
         name: "PUBLISHER_MEMORY_LOW_WATER_FRACTION",
         env: { ...CEILING, PUBLISHER_MEMORY_LOW_WATER_FRACTION: "0.5" },
         expect: value(0.5),
      },
      {
         name: "blank",
         env: { ...CEILING, PUBLISHER_MEMORY_LOW_WATER_FRACTION: "" },
         expect: value(0.7),
      },
      {
         name: "0 is refused",
         env: { ...CEILING, PUBLISHER_MEMORY_LOW_WATER_FRACTION: "0" },
         expect: fails("PUBLISHER_MEMORY_LOW_WATER_FRACTION"),
      },
   ],
   "memory.checkIntervalMs": [
      { name: "unset", env: CEILING, expect: value(5000) },
      {
         name: "PUBLISHER_MEMORY_CHECK_INTERVAL_MS",
         env: { ...CEILING, PUBLISHER_MEMORY_CHECK_INTERVAL_MS: "1000" },
         expect: value(1000),
      },
      {
         name: "blank",
         env: { ...CEILING, PUBLISHER_MEMORY_CHECK_INTERVAL_MS: "" },
         expect: value(5000),
      },
      {
         name: "below 100",
         env: { ...CEILING, PUBLISHER_MEMORY_CHECK_INTERVAL_MS: "50" },
         expect: fails("PUBLISHER_MEMORY_CHECK_INTERVAL_MS"),
      },
   ],
   "memory.backpressureEnabled": [
      { name: "unset", env: CEILING, expect: value(true) },
      {
         name: "PUBLISHER_MEMORY_BACKPRESSURE",
         env: { ...CEILING, PUBLISHER_MEMORY_BACKPRESSURE: "false" },
         expect: value(false),
      },
      {
         name: "blank",
         env: { ...CEILING, PUBLISHER_MEMORY_BACKPRESSURE: "" },
         expect: value(true),
      },
      {
         name: "not a boolean",
         env: { ...CEILING, PUBLISHER_MEMORY_BACKPRESSURE: "maybe" },
         expect: fails("PUBLISHER_MEMORY_BACKPRESSURE"),
      },
   ],
   "packageLoad.workers": [
      { name: "unset", expect: value(1) },
      {
         name: "PACKAGE_LOAD_WORKERS",
         env: { PACKAGE_LOAD_WORKERS: "4" },
         expect: value(4),
      },
      {
         name: "blank is unset",
         env: { PACKAGE_LOAD_WORKERS: "" },
         expect: value(1),
      },
      {
         name: "0 is refused",
         env: { PACKAGE_LOAD_WORKERS: "0" },
         expect: fails("PACKAGE_LOAD_WORKERS"),
      },
      {
         name: "a number then text fails",
         env: { PACKAGE_LOAD_WORKERS: "5abc" },
         expect: fails("PACKAGE_LOAD_WORKERS"),
      },
   ],
   "packageLoad.jobTimeoutMs": [
      { name: "unset", expect: value(120_000) },
      {
         name: "PACKAGE_LOAD_JOB_TIMEOUT_MS",
         env: { PACKAGE_LOAD_JOB_TIMEOUT_MS: "5000" },
         expect: value(5000),
      },
      {
         name: "blank",
         env: { PACKAGE_LOAD_JOB_TIMEOUT_MS: "" },
         expect: value(120_000),
      },
      {
         name: "0 is refused",
         env: { PACKAGE_LOAD_JOB_TIMEOUT_MS: "0" },
         expect: fails("PACKAGE_LOAD_JOB_TIMEOUT_MS"),
      },
      {
         name: "not a number fails",
         env: { PACKAGE_LOAD_JOB_TIMEOUT_MS: "soon" },
         expect: fails("PACKAGE_LOAD_JOB_TIMEOUT_MS"),
      },
   ],
   "packageLoad.schemaCacheBytes": [
      { name: "unset", expect: value(16 * 1024 * 1024) },
      {
         name: "0 turns the cache off",
         env: { PACKAGE_LOAD_SCHEMA_CACHE_BYTES: "0" },
         expect: value(0),
      },
      {
         name: "PACKAGE_LOAD_SCHEMA_CACHE_BYTES",
         env: { PACKAGE_LOAD_SCHEMA_CACHE_BYTES: "1024" },
         expect: value(1024),
      },
      {
         name: "blank",
         env: { PACKAGE_LOAD_SCHEMA_CACHE_BYTES: "" },
         expect: value(16 * 1024 * 1024),
      },
      {
         name: "a fraction",
         env: { PACKAGE_LOAD_SCHEMA_CACHE_BYTES: "1.5" },
         expect: fails("PACKAGE_LOAD_SCHEMA_CACHE_BYTES"),
      },
   ],

   // ── duckdb, ducklake, persist, materialization ────────────────────────
   "duckdb.extensionFetchPolicy": [
      { name: "unset", expect: value("on-demand") },
      {
         name: "EXTENSION_FETCH_POLICY",
         env: { EXTENSION_FETCH_POLICY: "local-only" },
         expect: value("local-only"),
      },
      {
         name: "any case",
         env: { EXTENSION_FETCH_POLICY: " LOCAL-ONLY " },
         expect: value("local-only"),
      },
      {
         name: "blank",
         env: { EXTENSION_FETCH_POLICY: "" },
         expect: value("on-demand"),
      },
      {
         name: "an unknown policy",
         env: { EXTENSION_FETCH_POLICY: "never" },
         expect: fails("EXTENSION_FETCH_POLICY"),
      },
   ],
   "duckdb.memoryLimit": [
      { name: "unset", expect: value(undefined) },
      {
         name: "PUBLISHER_DUCKDB_MEMORY_LIMIT",
         env: { PUBLISHER_DUCKDB_MEMORY_LIMIT: "1GB" },
         expect: value("1GB"),
      },
      {
         name: "off",
         env: { PUBLISHER_DUCKDB_MEMORY_LIMIT: "OFF" },
         expect: value("off"),
      },
      {
         name: "blank",
         env: { PUBLISHER_DUCKDB_MEMORY_LIMIT: "" },
         expect: value(undefined),
      },
      {
         name: "not a size",
         env: { PUBLISHER_DUCKDB_MEMORY_LIMIT: "lots" },
         expect: fails("PUBLISHER_DUCKDB_MEMORY_LIMIT"),
      },
   ],
   "duckdb.tempDirectory": [
      { name: "unset", expect: value(undefined) },
      {
         name: "PUBLISHER_DUCKDB_TEMP_DIRECTORY",
         env: { PUBLISHER_DUCKDB_TEMP_DIRECTORY: "/var/spill" },
         expect: value("/var/spill"),
      },
      {
         name: "blank",
         env: { PUBLISHER_DUCKDB_TEMP_DIRECTORY: "" },
         expect: value(undefined),
      },
   ],
   "duckdb.partitionedWriteFlushThreshold": [
      { name: "unset", expect: value(8192) },
      {
         name: "PUBLISHER_PARTITIONED_WRITE_FLUSH_THRESHOLD",
         env: { PUBLISHER_PARTITIONED_WRITE_FLUSH_THRESHOLD: "1000" },
         expect: value(1000),
      },
      {
         name: "off turns the bounds off",
         env: { PUBLISHER_PARTITIONED_WRITE_FLUSH_THRESHOLD: "off" },
         expect: value(undefined),
      },
      {
         name: "blank",
         env: { PUBLISHER_PARTITIONED_WRITE_FLUSH_THRESHOLD: "" },
         expect: value(8192),
      },
      {
         name: "0 is refused",
         env: { PUBLISHER_PARTITIONED_WRITE_FLUSH_THRESHOLD: "0" },
         expect: fails("PUBLISHER_PARTITIONED_WRITE_FLUSH_THRESHOLD"),
      },
   ],
   "ducklake.rowGroupSizeBytes": [
      { name: "unset", expect: value(undefined) },
      {
         name: "PUBLISHER_DUCKLAKE_ROW_GROUP_SIZE_BYTES",
         env: { PUBLISHER_DUCKLAKE_ROW_GROUP_SIZE_BYTES: "16MB" },
         expect: value("16MB"),
      },
      {
         name: "blank",
         env: { PUBLISHER_DUCKLAKE_ROW_GROUP_SIZE_BYTES: "" },
         expect: value(undefined),
      },
      {
         name: "no unit",
         env: { PUBLISHER_DUCKLAKE_ROW_GROUP_SIZE_BYTES: "16" },
         expect: fails("PUBLISHER_DUCKLAKE_ROW_GROUP_SIZE_BYTES"),
      },
   ],
   "ducklake.targetFileSizeBytes": [
      { name: "unset", expect: value(undefined) },
      {
         name: "PUBLISHER_DUCKLAKE_TARGET_FILE_SIZE_BYTES",
         env: { PUBLISHER_DUCKLAKE_TARGET_FILE_SIZE_BYTES: "256MB" },
         expect: value("256MB"),
      },
      {
         name: "blank",
         env: { PUBLISHER_DUCKLAKE_TARGET_FILE_SIZE_BYTES: "" },
         expect: value(undefined),
      },
      {
         name: "not a size",
         env: { PUBLISHER_DUCKLAKE_TARGET_FILE_SIZE_BYTES: "big" },
         expect: fails("PUBLISHER_DUCKLAKE_TARGET_FILE_SIZE_BYTES"),
      },
   ],
   "persist.storageMode": [
      { name: "unset", expect: value("off") },
      {
         name: "PERSIST_STORAGE_MODE",
         env: { PERSIST_STORAGE_MODE: "on" },
         expect: value("on"),
      },
      {
         name: "any case",
         env: { PERSIST_STORAGE_MODE: "Write-Only" },
         expect: value("write-only"),
      },
      {
         name: "blank",
         env: { PERSIST_STORAGE_MODE: "" },
         expect: value("off"),
      },
      {
         name: "an unknown mode",
         env: { PERSIST_STORAGE_MODE: "always" },
         expect: fails("PERSIST_STORAGE_MODE"),
      },
   ],
   "persist.collisionEnforcementEnabled": [
      { name: "unset", expect: value(false) },
      {
         name: "PERSIST_COLLISION_ENFORCE",
         env: { PERSIST_COLLISION_ENFORCE: "1" },
         expect: value(true),
      },
      {
         name: "blank",
         env: { PERSIST_COLLISION_ENFORCE: "" },
         expect: value(false),
      },
      {
         name: "not a boolean",
         env: { PERSIST_COLLISION_ENFORCE: "strict" },
         expect: fails("PERSIST_COLLISION_ENFORCE"),
      },
   ],
   "materialization.schedulerEnabled": [
      { name: "unset", expect: value(false) },
      {
         name: "PUBLISHER_LOCAL_MATERIALIZATION_SCHEDULER",
         env: SCHEDULER,
         expect: value(true),
      },
      {
         name: "blank",
         env: { PUBLISHER_LOCAL_MATERIALIZATION_SCHEDULER: "" },
         expect: value(false),
      },
      {
         name: "not a boolean",
         env: { PUBLISHER_LOCAL_MATERIALIZATION_SCHEDULER: "local" },
         expect: fails("PUBLISHER_LOCAL_MATERIALIZATION_SCHEDULER"),
      },
   ],
   "materialization.schedulerIntervalMs": [
      { name: "unset", env: SCHEDULER, expect: value(60_000) },
      {
         name: "PUBLISHER_MATERIALIZATION_SCHEDULER_INTERVAL_MS",
         env: {
            ...SCHEDULER,
            PUBLISHER_MATERIALIZATION_SCHEDULER_INTERVAL_MS: "5000",
         },
         expect: value(5000),
      },
      {
         name: "blank",
         env: {
            ...SCHEDULER,
            PUBLISHER_MATERIALIZATION_SCHEDULER_INTERVAL_MS: "",
         },
         expect: value(60_000),
      },
      {
         name: "below a second",
         env: {
            ...SCHEDULER,
            PUBLISHER_MATERIALIZATION_SCHEDULER_INTERVAL_MS: "500",
         },
         expect: fails("PUBLISHER_MATERIALIZATION_SCHEDULER_INTERVAL_MS"),
      },
   ],
   "materialization.schedulerMaxFiresPerTick": [
      { name: "unset", env: SCHEDULER, expect: value(10) },
      {
         name: "PUBLISHER_MATERIALIZATION_SCHEDULER_MAX_FIRES_PER_TICK",
         env: {
            ...SCHEDULER,
            PUBLISHER_MATERIALIZATION_SCHEDULER_MAX_FIRES_PER_TICK: "3",
         },
         expect: value(3),
      },
      {
         name: "blank",
         env: {
            ...SCHEDULER,
            PUBLISHER_MATERIALIZATION_SCHEDULER_MAX_FIRES_PER_TICK: "",
         },
         expect: value(10),
      },
      {
         name: "0 is refused",
         env: {
            ...SCHEDULER,
            PUBLISHER_MATERIALIZATION_SCHEDULER_MAX_FIRES_PER_TICK: "0",
         },
         expect: fails(
            "PUBLISHER_MATERIALIZATION_SCHEDULER_MAX_FIRES_PER_TICK",
         ),
      },
   ],

   // ── allow, authorize, logging, shutdown ───────────────────────────────
   "allow.proxyConnections": [
      { name: "unset", expect: value(false) },
      {
         name: "PUBLISHER_ALLOW_PROXY_CONNECTIONS",
         env: { PUBLISHER_ALLOW_PROXY_CONNECTIONS: "true" },
         expect: value(true),
      },
      {
         name: '"1" is still off, with a warning',
         env: { PUBLISHER_ALLOW_PROXY_CONNECTIONS: "1" },
         expect: warned(
            false,
            "PUBLISHER_ALLOW_PROXY_CONNECTIONS=1 is read as off",
         ),
      },
      {
         name: '"yes" is still off, with a warning',
         env: { PUBLISHER_ALLOW_PROXY_CONNECTIONS: "yes" },
         expect: warned(
            false,
            "PUBLISHER_ALLOW_PROXY_CONNECTIONS=yes is read as off",
         ),
      },
      {
         name: "blank",
         env: { PUBLISHER_ALLOW_PROXY_CONNECTIONS: "" },
         expect: value(false),
      },
   ],
   "allow.duckdbSetupSql": [
      { name: "unset", expect: value(false) },
      {
         name: "PUBLISHER_ALLOW_DUCKDB_SETUP_SQL",
         env: { PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "yes" },
         expect: value(true),
      },
      {
         name: "blank",
         env: { PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "" },
         expect: value(false),
      },
      {
         name: "not a boolean",
         env: { PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "sure" },
         expect: fails("PUBLISHER_ALLOW_DUCKDB_SETUP_SQL"),
      },
   ],
   "allow.unverifiedSshHostKey": [
      {
         name: "unset",
         expect: value({ atConnect: false, atConfigLoad: "accepted" }),
      },
      {
         name: "PUBLISHER_ALLOW_UNVERIFIED_SSH_HOST_KEY",
         env: { PUBLISHER_ALLOW_UNVERIFIED_SSH_HOST_KEY: "yes" },
         expect: value({ atConnect: true, atConfigLoad: "accepted" }),
      },
      {
         name: "blank",
         env: { PUBLISHER_ALLOW_UNVERIFIED_SSH_HOST_KEY: "" },
         expect: value({ atConnect: false, atConfigLoad: "accepted" }),
      },
      {
         name: "a typo fails",
         env: { PUBLISHER_ALLOW_UNVERIFIED_SSH_HOST_KEY: "ture" },
         expect: fails("PUBLISHER_ALLOW_UNVERIFIED_SSH_HOST_KEY"),
      },
   ],
   "allow.s3CredentialChain": [
      { name: "unset", expect: value("any") },
      {
         name: "S3_CREDENTIAL_CHAIN_POLICY",
         env: { S3_CREDENTIAL_CHAIN_POLICY: "off" },
         expect: value("off"),
      },
      {
         name: "any case",
         env: { S3_CREDENTIAL_CHAIN_POLICY: " Destinations-Only " },
         expect: value("destinations-only"),
      },
      {
         name: "blank",
         env: { S3_CREDENTIAL_CHAIN_POLICY: "" },
         expect: value("any"),
      },
      {
         name: "an unknown policy",
         env: { S3_CREDENTIAL_CHAIN_POLICY: "none" },
         expect: fails("S3_CREDENTIAL_CHAIN_POLICY"),
      },
   ],
   "authorize.bypassSecret": [
      { name: "unset turns the bypass off", expect: value(false) },
      {
         name: "PUBLISHER_BYPASS_AUTHORIZE_SECRET",
         env: { PUBLISHER_BYPASS_AUTHORIZE_SECRET: "s3cret" },
         expect: value(true),
      },
      {
         name: "the variable is trimmed",
         env: { PUBLISHER_BYPASS_AUTHORIZE_SECRET: " s3cret " },
         expect: value(true),
      },
      {
         name: "another secret does not match",
         env: { PUBLISHER_BYPASS_AUTHORIZE_SECRET: "other" },
         expect: value(false),
      },
      {
         name: "blank",
         env: { PUBLISHER_BYPASS_AUTHORIZE_SECRET: "" },
         expect: value(false),
      },
   ],
   "logging.level": [
      { name: "unset", expect: value("debug") },
      { name: "LOG_LEVEL", env: { LOG_LEVEL: "info" }, expect: value("info") },
      { name: "any case", env: { LOG_LEVEL: "WARN" }, expect: value("warn") },
      { name: "blank", env: { LOG_LEVEL: "" }, expect: value("debug") },
      {
         name: "an unknown level is debug, with a warning",
         env: { LOG_LEVEL: "loud" },
         expect: warned("debug", "LOG_LEVEL"),
      },
   ],
   "logging.responseBodiesEnabled": [
      { name: "unset", expect: value(true) },
      {
         name: "DISABLE_RESPONSE_LOGGING=true",
         env: { DISABLE_RESPONSE_LOGGING: "true" },
         expect: value(false),
      },
      {
         name: "DISABLE_RESPONSE_LOGGING=1",
         env: { DISABLE_RESPONSE_LOGGING: "1" },
         expect: value(false),
      },
      {
         name: '"yes" still does not disable, with a warning',
         env: { DISABLE_RESPONSE_LOGGING: "yes" },
         expect: warned(true, "DISABLE_RESPONSE_LOGGING=yes is read as off"),
      },
      {
         name: "blank",
         env: { DISABLE_RESPONSE_LOGGING: "" },
         expect: value(true),
      },
   ],
   "shutdown.drainDurationSeconds": [
      { name: "unset", expect: value(0) },
      {
         name: "SHUTDOWN_DRAIN_DURATION_SECONDS",
         env: { SHUTDOWN_DRAIN_DURATION_SECONDS: "5" },
         expect: value(5),
      },
      {
         name: "--shutdown_drain_duration_seconds",
         argv: ["--shutdown_drain_duration_seconds", "7"],
         expect: value(7),
      },
      {
         name: "the flag wins over the variable",
         env: { SHUTDOWN_DRAIN_DURATION_SECONDS: "5" },
         argv: ["--shutdown_drain_duration_seconds", "7"],
         expect: value(7),
      },
      {
         name: "blank",
         env: { SHUTDOWN_DRAIN_DURATION_SECONDS: "" },
         expect: value(0),
      },
      {
         name: "not a number fails",
         env: { SHUTDOWN_DRAIN_DURATION_SECONDS: "abc" },
         expect: fails("SHUTDOWN_DRAIN_DURATION_SECONDS"),
      },
   ],
   "shutdown.gracefulCloseTimeoutSeconds": [
      { name: "unset", expect: value(0) },
      {
         name: "SHUTDOWN_GRACEFUL_CLOSE_TIMEOUT_SECONDS",
         env: { SHUTDOWN_GRACEFUL_CLOSE_TIMEOUT_SECONDS: "5" },
         expect: value(5),
      },
      {
         name: "--shutdown_graceful_close_timeout_seconds",
         argv: ["--shutdown_graceful_close_timeout_seconds", "7"],
         expect: value(7),
      },
      {
         name: "blank",
         env: { SHUTDOWN_GRACEFUL_CLOSE_TIMEOUT_SECONDS: "" },
         expect: value(0),
      },
      {
         name: "not a number fails",
         env: { SHUTDOWN_GRACEFUL_CLOSE_TIMEOUT_SECONDS: "abc" },
         expect: fails("SHUTDOWN_GRACEFUL_CLOSE_TIMEOUT_SECONDS"),
      },
   ],

   // ── retrieval ─────────────────────────────────────────────────────────
   "retrieval.embedding.apiKey": [
      { name: "unset turns semantic search off", expect: value(null) },
      {
         name: "EMBEDDING_API_KEY",
         env: { EMBEDDING_API_KEY: "sk-1" },
         expect: value("sk-1"),
      },
      {
         name: "trimmed",
         env: { EMBEDDING_API_KEY: " sk-1 " },
         expect: value("sk-1"),
      },
      { name: "blank", env: { EMBEDDING_API_KEY: "" }, expect: value(null) },
   ],
   "retrieval.embedding.model": [
      { name: "unset", env: KEY, expect: value("text-embedding-3-small") },
      {
         name: "EMBEDDING_MODEL",
         env: { ...KEY, EMBEDDING_MODEL: "m1" },
         expect: value("m1"),
      },
      {
         name: "the file",
         env: KEY,
         file: { retrieval: { embedding: { model: "m2" } } },
         expect: value("m2"),
      },
      {
         name: "the file wins over the variable",
         env: { ...KEY, EMBEDDING_MODEL: "m1" },
         file: { retrieval: { embedding: { model: "m2" } } },
         expect: value("m2"),
      },
      {
         name: "blank",
         env: { ...KEY, EMBEDDING_MODEL: "" },
         expect: value("text-embedding-3-small"),
      },
   ],
   "retrieval.embedding.baseUrl": [
      { name: "unset", env: KEY, expect: value("https://api.openai.com/v1") },
      {
         name: "EMBEDDING_API_BASE, without its trailing slash",
         env: { ...KEY, EMBEDDING_API_BASE: "http://e.example/v1/" },
         expect: value("http://e.example/v1"),
      },
      {
         name: "the file wins over the variable",
         env: { ...KEY, EMBEDDING_API_BASE: "http://e.example/v1" },
         file: { retrieval: { embedding: { baseUrl: "http://f.example/v1" } } },
         expect: value("http://f.example/v1"),
      },
      {
         name: "blank",
         env: { ...KEY, EMBEDDING_API_BASE: "" },
         expect: value("https://api.openai.com/v1"),
      },
      {
         name: "not a URL",
         env: { ...KEY, EMBEDDING_API_BASE: "not a url" },
         expect: fails("EMBEDDING_API_BASE"),
      },
   ],
   "retrieval.embedding.dimensions": [
      { name: "unset", env: KEY, expect: value(undefined) },
      {
         name: "EMBEDDING_DIMENSIONS",
         env: { ...KEY, EMBEDDING_DIMENSIONS: "256" },
         expect: value(256),
      },
      {
         name: "the file wins over the variable",
         env: { ...KEY, EMBEDDING_DIMENSIONS: "256" },
         file: { retrieval: { embedding: { dimensions: 512 } } },
         expect: value(512),
      },
      {
         name: "blank",
         env: { ...KEY, EMBEDDING_DIMENSIONS: "" },
         expect: value(undefined),
      },
      {
         name: "0 is refused",
         env: { ...KEY, EMBEDDING_DIMENSIONS: "0" },
         expect: fails("EMBEDDING_DIMENSIONS"),
      },
      {
         name: "not a number",
         env: { ...KEY, EMBEDDING_DIMENSIONS: "wide" },
         expect: fails("EMBEDDING_DIMENSIONS"),
      },
   ],
   "retrieval.embedding.minSimilarity": [
      { name: "unset", env: KEY, expect: value(0.2) },
      {
         name: "EMBEDDING_MIN_SIMILARITY",
         env: { ...KEY, EMBEDDING_MIN_SIMILARITY: "0.35" },
         expect: value(0.35),
      },
      {
         name: "blank",
         env: { ...KEY, EMBEDDING_MIN_SIMILARITY: "" },
         expect: value(0.2),
      },
      {
         name: "1 is refused",
         env: { ...KEY, EMBEDDING_MIN_SIMILARITY: "1" },
         expect: fails("EMBEDDING_MIN_SIMILARITY"),
      },
      {
         name: "not a number",
         env: { ...KEY, EMBEDDING_MIN_SIMILARITY: "close" },
         expect: fails("EMBEDDING_MIN_SIMILARITY"),
      },
   ],
   "retrieval.embedding.connectionSchemaIndexingEnabled": [
      { name: "unset", expect: value(false) },
      {
         name: "EMBEDDING_INDEX_CONNECTION_SCHEMA",
         env: { EMBEDDING_INDEX_CONNECTION_SCHEMA: "true" },
         expect: value(true),
      },
      {
         name: "blank",
         env: { EMBEDDING_INDEX_CONNECTION_SCHEMA: "" },
         expect: value(false),
      },
      {
         name: "not a boolean",
         env: { EMBEDDING_INDEX_CONNECTION_SCHEMA: "schemas" },
         expect: fails("EMBEDDING_INDEX_CONNECTION_SCHEMA"),
      },
   ],
   "retrieval.llm.apiKey": [
      { name: "unset turns the LLM off", expect: value(null) },
      {
         name: "LLM_API_KEY",
         env: { LLM_API_KEY: "sk-2" },
         expect: value("sk-2"),
      },
      {
         name: "trimmed",
         env: { LLM_API_KEY: " sk-2 " },
         expect: value("sk-2"),
      },
      { name: "blank", env: { LLM_API_KEY: "" }, expect: value(null) },
   ],
};

/** Read one case: its value and warnings, or the message it failed with. */
function outcome(
   key: string,
   input: SettingsInput,
): { value: unknown; warns: string[] } | { fails: string } {
   try {
      return withSettings(input, (context) => {
         const current = Settings.current();
         const startup = current.fileError
            ? [current.fileError]
            : current.errors();
         if (startup.length > 0) {
            return { fails: startup.map((error) => error.message).join("\n") };
         }
         return {
            value: PROBES[key](context),
            warns: current.warnings.map((warning) => warning.message),
         };
      });
   } catch (error) {
      return { fails: error instanceof Error ? error.message : String(error) };
   }
}

describe("settings contract", () => {
   it("has a probe for every row, and a row for every probe", () => {
      expect(Object.keys(CONTRACT).sort()).toEqual(Object.keys(PROBES).sort());
   });

   for (const [key, cases] of Object.entries(CONTRACT)) {
      describe(key, () => {
         for (const { name, expect: expected, ...input } of cases) {
            it(name, () => {
               const actual = outcome(key, input);
               if ("fails" in expected) {
                  expect(actual).toHaveProperty("fails");
                  expect((actual as { fails: string }).fails).toContain(
                     expected.fails,
                  );
                  return;
               }
               expect(actual).toHaveProperty("value");
               const read = actual as { value: unknown; warns: string[] };
               expect(read.value).toEqual(expected.value);
               if (expected.warns === undefined) {
                  expect(read.warns).toEqual([]);
               } else {
                  expect(read.warns.join("\n")).toContain(expected.warns);
               }
            });
         }
      });
   }
});

/**
 * Rules that relate settings to each other stay where they are today
 * (server_apis.ts, the memory governor's check in config.ts); these rows pin
 * the server APIs' rules, which read two settings and the admin variables.
 */
describe("settings contract: the server APIs' cross-setting rules", () => {
   const check = (input: SettingsInput): Expected => {
      try {
         withSettings(input, () => assertServerApis(getServerApiSettings()));
         return { value: "starts" };
      } catch (error) {
         return {
            fails: error instanceof Error ? error.message : String(error),
         };
      }
   };

   it("both APIs off is refused", () => {
      expect(
         check({
            env: {
               PUBLISHER_SERVING_API_ENABLED: "false",
               PUBLISHER_ADMIN_API_ENABLED: "false",
            },
         }),
      ).toEqual(
         fails(
            "servingApiEnabled and adminApiEnabled are both false, so there is nothing to run.",
         ),
      );
   });

   it("an admin port with the admin API off is refused", () => {
      const result = check({
         env: {
            PUBLISHER_ADMIN_API_ENABLED: "false",
            PUBLISHER_ADMIN_PORT: "4100",
         },
      });
      expect((result as { fails: string }).fails).toContain(
         "PUBLISHER_ADMIN_PORT",
      );
   });

   it("serving off needs an admin port", () => {
      const result = check({ argv: ["--no-serving-api-enabled"] });
      expect((result as { fails: string }).fails).toContain(
         "PUBLISHER_ADMIN_PORT is required",
      );
   });

   it("serving off with an admin port starts", () => {
      expect(
         check({ argv: ["--no-serving-api-enabled", "--admin_port", "4100"] }),
      ).toEqual(value("starts"));
   });
});
