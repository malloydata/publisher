// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import os from "os";
import path from "path";
import { REGISTRY, Settings } from ".";
import { availableCpus } from "./registry/storage";
import { assertServerApis } from "../server_apis";
import { getServerApiSettings } from "../config";
import { withSettings, type SettingsInput } from "../test_helpers/settings_env";
import { PROBES } from "../test_helpers/settings_probes";

/**
 * What each setting gives for each way an operator can set it. One entry per
 * setting, under its key in the configuration design
 * (docs/configuration-design.md), with its variable, flag and file key as the
 * sources. Every row is a case a deployment can hit: unset, each source, a
 * blank variable, and values that are wrong. A change to how a setting is read
 * shows up here as a changed row. The design's rules are such rows: a value of
 * another type fails startup, a blank variable is unset, an unreadable file
 * fails startup, and one precedence holds, flag then variable then file.
 *
 * A row expects a value or a failure whose message names the setting. A
 * failure is at startup, as server.ts reports it, or where the server reads
 * the setting.
 */
type Expected = { value: unknown } | { fails: string };

interface Case extends SettingsInput {
   name: string;
   expect: Expected;
}

const value = (v: unknown): Expected => ({ value: v });
const fails = (name: string): Expected => ({ fails: name });

/** Embedding settings are off without a key, so their rows set one. */
const KEY = { PUBLISHER_RETRIEVAL_EMBEDDING_API_KEY: "sk-test" };
/**
 * A `retrieval` block retrieval/config.ts rejects (an unknown LLM provider),
 * holding these embedding keys. The server runs retrieval on its defaults.
 */
const REJECTED_RETRIEVAL = (embedding: Record<string, unknown>) => ({
   // Vertex without projectId and location: the block's own check refuses
   // it, where every leaf alone reads.
   retrieval: { llm: { provider: "vertex", model: "m" }, embedding },
});
/** An `llm` block the file holds whole. */
const LLM = (fields: Record<string, unknown> = {}) => ({
   retrieval: { llm: { provider: "anthropic", model: "m", ...fields } },
});
const VERTEX_LLM = {
   provider: "vertex",
   model: "m",
   projectId: "p",
   location: "l",
};
/** An `embedding` block the file holds whole. */
const EMBEDDING = (fields: Record<string, unknown> = {}) => ({
   retrieval: { embedding: { ...fields } },
});
/** The memory governor's tunables are read only with a ceiling set. */
const CEILING = { PUBLISHER_MEMORY_MAX_BYTES: "1000000" };
/** The scheduler's tunables are read only with the scheduler on. */
const SCHEDULER = { PUBLISHER_MATERIALIZATION_SCHEDULER_ENABLED: "true" };
/** The admin API's storage is read only with a catalog database set. */
const CATALOG = { PUBLISHER_ADMIN_CATALOG_DATABASE: "postgresql://h/catalog" };
const S3 = {
   ...CATALOG,
   PUBLISHER_ADMIN_STORAGE_PROVIDER: "s3",
   PUBLISHER_ADMIN_STORAGE_KEY_ID: "AKID",
   PUBLISHER_ADMIN_STORAGE_SECRET: "SECRET",
};
/** A spill directory the DuckDB check can create. */
const SPILL = path.join(os.tmpdir(), "publisher-contract-spill");
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
         name: 'the string "false" is off, where it used to freeze the server',
         file: { frozenConfig: "false" },
         expect: value(false),
      },
      {
         name: "a ${VAR} is read as a boolean, where any value used to freeze the server",
         env: { FROZEN: "false" },
         file: { frozenConfig: "${FROZEN}" },
         expect: value(false),
      },
      {
         name: "a string of only whitespace is unset, where it used to freeze",
         file: { frozenConfig: " " },
         expect: value(false),
      },
      {
         name: "a number fails, where 1 used to freeze the server",
         file: { frozenConfig: 1 },
         expect: fails("expected true or false, got 1"),
      },
      {
         name: "and is told the file's spelling, for 0 as for 1",
         file: { frozenConfig: 0 },
         expect: fails("expected true or false, got 0"),
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
         name: "a string in the file is read as a boolean",
         file: { servingApiEnabled: "no" },
         expect: value(false),
      },
      {
         name: "a string in the file that is no boolean fails",
         file: { servingApiEnabled: "enabled" },
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
         name: "--port with no value fails",
         argv: ["--port"],
         expect: fails("--port has no value after it"),
      },
      { name: "blank", env: { PUBLISHER_PORT: "" }, expect: value(4000) },
      {
         name: "whitespace only is unset, where it used to be port 0",
         env: { PUBLISHER_PORT: "  " },
         expect: value(4000),
      },
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
      {
         name: "whitespace only is unset",
         env: { PUBLISHER_HOST: "   " },
         expect: value("0.0.0.0"),
      },
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
      {
         name: "an exponent, which Number() read, fails",
         env: { PUBLISHER_RATE_LIMIT: "1e2" },
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
      {
         name: "whitespace only is unset",
         env: { PUBLISHER_FRAME_ANCESTORS: "   " },
         expect: value("'self'"),
      },
   ],
   callerHeader: [
      { name: "unset", expect: value("x-publisher-caller") },
      {
         name: "PUBLISHER_CALLER_HEADER, lowercased",
         env: { PUBLISHER_CALLER_HEADER: "X-Gateway-User" },
         expect: value("x-gateway-user"),
      },
      {
         name: "surrounding space is dropped",
         env: { PUBLISHER_CALLER_HEADER: " x-gateway-user " },
         expect: value("x-gateway-user"),
      },
      {
         name: "blank",
         env: { PUBLISHER_CALLER_HEADER: "" },
         expect: value("x-publisher-caller"),
      },
      {
         name: "whitespace only is unset",
         env: { PUBLISHER_CALLER_HEADER: "   " },
         expect: value("x-publisher-caller"),
      },
   ],
   packageVersioning: [
      { name: "unset", expect: value(true) },
      {
         name: "PUBLISHER_PACKAGE_VERSIONING on",
         env: { PUBLISHER_PACKAGE_VERSIONING: "on" },
         expect: value(true),
      },
      {
         name: "off",
         env: { PUBLISHER_PACKAGE_VERSIONING: "off" },
         expect: value(false),
      },
      {
         name: "any case, with surrounding space",
         env: { PUBLISHER_PACKAGE_VERSIONING: " On " },
         expect: value(true),
      },
      {
         name: "blank",
         env: { PUBLISHER_PACKAGE_VERSIONING: "" },
         expect: value(true),
      },
      {
         name: "a boolean spelling is not one of its two values, and fails",
         env: { PUBLISHER_PACKAGE_VERSIONING: "true" },
         expect: fails("PUBLISHER_PACKAGE_VERSIONING"),
      },
      {
         name: "the file, under its key, under the default of on",
         file: { packageVersioning: "off" },
         expect: value(false),
      },
   ],
   versionPromotion: [
      { name: "unset", expect: value("on-publish") },
      {
         name: "PUBLISHER_VERSION_PROMOTION",
         env: { PUBLISHER_VERSION_PROMOTION: "explicit" },
         expect: value("explicit"),
      },
      {
         name: "any case, with surrounding space",
         env: { PUBLISHER_VERSION_PROMOTION: " Explicit " },
         expect: value("explicit"),
      },
      {
         name: "the file",
         file: { versionPromotion: "explicit" },
         expect: value("explicit"),
      },
      {
         name: "the variable wins over the file",
         env: { PUBLISHER_VERSION_PROMOTION: "on-publish" },
         file: { versionPromotion: "explicit" },
         expect: value("on-publish"),
      },
      {
         name: "a blank variable leaves the file's value",
         env: { PUBLISHER_VERSION_PROMOTION: "" },
         file: { versionPromotion: "explicit" },
         expect: value("explicit"),
      },
      {
         name: "a ${VAR} in the file",
         env: { PROMOTION: "explicit" },
         file: { versionPromotion: "${PROMOTION}" },
         expect: value("explicit"),
      },
      {
         name: "null in the file is unset",
         file: { versionPromotion: null },
         expect: value("on-publish"),
      },
      {
         name: "an unknown mode in the variable fails",
         env: { PUBLISHER_VERSION_PROMOTION: "later" },
         expect: fails("versionPromotion from PUBLISHER_VERSION_PROMOTION"),
      },
      {
         name: "an unknown mode in the file fails",
         file: { versionPromotion: "manual" },
         expect: fails(
            'expected one of "on-publish" or "explicit", got "manual"',
         ),
      },
      {
         name: "a number in the file fails",
         file: { versionPromotion: 1 },
         expect: fails('expected one of "on-publish" or "explicit", got 1'),
      },
      {
         name: "a file that cannot be parsed fails startup, where it gave the default",
         file: "{ not json",
         expect: fails("Failed to parse"),
      },
   ],
   packageInlineFields: [
      { name: "unset", expect: value(true) },
      {
         name: "PUBLISHER_PACKAGE_INLINE_FIELDS off",
         env: { PUBLISHER_PACKAGE_INLINE_FIELDS: "off" },
         expect: value(false),
      },
      {
         name: "on",
         env: { PUBLISHER_PACKAGE_INLINE_FIELDS: "on" },
         expect: value(true),
      },
      {
         name: "any case, with surrounding space",
         env: { PUBLISHER_PACKAGE_INLINE_FIELDS: " OFF " },
         expect: value(false),
      },
      {
         name: "blank",
         env: { PUBLISHER_PACKAGE_INLINE_FIELDS: "" },
         expect: value(true),
      },
      {
         name: "a boolean spelling is not one of its two values, and fails",
         env: { PUBLISHER_PACKAGE_INLINE_FIELDS: "false" },
         expect: fails("PUBLISHER_PACKAGE_INLINE_FIELDS"),
      },
      {
         name: "the file, under its key",
         file: { packageInlineFields: "off" },
         expect: value(false),
      },
   ],
   modelPage: [
      { name: "unset", expect: value("explorer") },
      {
         name: "PUBLISHER_MODEL_PAGE",
         env: { PUBLISHER_MODEL_PAGE: "viewer" },
         expect: value("viewer"),
      },
      {
         name: "any case, with surrounding space",
         env: { PUBLISHER_MODEL_PAGE: " Viewer " },
         expect: value("viewer"),
      },
      {
         name: "blank",
         env: { PUBLISHER_MODEL_PAGE: " " },
         expect: value("explorer"),
      },
      {
         name: "a value outside the set fails",
         env: { PUBLISHER_MODEL_PAGE: "both" },
         expect: fails(
            'modelPage from PUBLISHER_MODEL_PAGE: expected one of "explorer" or "viewer", got "both"',
         ),
      },
      {
         name: "the file, under its key",
         file: { modelPage: "viewer" },
         expect: value("viewer"),
      },
   ],
   theme: [
      { name: "unset", expect: value(undefined) },
      {
         name: "the file",
         file: { theme: { defaultMode: "dark" } },
         expect: value({ defaultMode: "dark" }),
      },
      {
         name: "a theme that is not an object is ignored",
         file: { theme: "dark" },
         expect: value(undefined),
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
         name: "environments that are not a list are none",
         file: { environments: { e: {} } },
         expect: value([]),
      },
   ],

   // ── Launch settings ───────────────────────────────────────────────────
   root: [
      { name: "unset", expect: value(".") },
      {
         name: "PUBLISHER_ROOT",
         env: { PUBLISHER_ROOT: "/srv/models" },
         expect: value("/srv/models"),
      },
      {
         name: "--root",
         argv: ["--root", "/x"],
         expect: value("/x"),
      },
      {
         name: "--root wins over the variable",
         env: { PUBLISHER_ROOT: "/srv/models" },
         argv: ["--root", "/x"],
         expect: value("/x"),
      },
      { name: "blank", env: { PUBLISHER_ROOT: "" }, expect: value(".") },
      {
         name: "whitespace only is unset",
         env: { PUBLISHER_ROOT: "   " },
         expect: value("."),
      },
   ],
   config: [
      { name: "unset", expect: value(undefined) },
      {
         name: "PUBLISHER_CONFIG",
         env: { PUBLISHER_CONFIG: "/nonexistent/a.json" },
         expect: value("/nonexistent/a.json"),
      },
      {
         name: "--config",
         argv: ["--config", "/nonexistent/b.json"],
         expect: value("/nonexistent/b.json"),
      },
      {
         name: "--config wins over the variable",
         env: { PUBLISHER_CONFIG: "/nonexistent/a.json" },
         argv: ["--config", "/nonexistent/b.json"],
         expect: value("/nonexistent/b.json"),
      },
      {
         name: "blank",
         env: { PUBLISHER_CONFIG: "" },
         expect: value(undefined),
      },
      {
         name: "whitespace only is unset",
         env: { PUBLISHER_CONFIG: "   " },
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
         name: "--root turns it off",
         env: { NODE_ENV: "production" },
         argv: ["--root", "/x"],
         expect: value(false),
      },
      {
         name: "PUBLISHER_ROOT turns it off too",
         env: { NODE_ENV: "production", PUBLISHER_ROOT: "/x" },
         expect: value(false),
      },
      {
         name: "the variable turns it off",
         env: {
            NODE_ENV: "production",
            PUBLISHER_USE_BUNDLED_DEFAULT: "false",
         },
         expect: value(false),
      },
      {
         name: "the variable turns it on despite a flag",
         env: { NODE_ENV: "production", PUBLISHER_USE_BUNDLED_DEFAULT: "true" },
         argv: ["--root", "/x"],
         expect: value(true),
      },
      {
         name: '"1" turns it on, where only exactly "true" used to',
         env: { NODE_ENV: "production", PUBLISHER_USE_BUNDLED_DEFAULT: "1" },
         argv: ["--root", "/x"],
         expect: value(true),
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
         name: "PUBLISHER_INITIALIZE_STORAGE",
         env: { PUBLISHER_INITIALIZE_STORAGE: "true" },
         expect: value(true),
      },
      { name: "--init", argv: ["--init"], expect: value(true) },
      {
         name: '"1" is on, where it used to be off',
         env: { PUBLISHER_INITIALIZE_STORAGE: "1" },
         expect: value(true),
      },
      {
         name: '"TRUE" is on, where it used to be off',
         env: { PUBLISHER_INITIALIZE_STORAGE: "TRUE" },
         expect: value(true),
      },
      {
         name: '"yes" is on, where it used to be off',
         env: { PUBLISHER_INITIALIZE_STORAGE: "yes" },
         expect: value(true),
      },
      {
         name: "blank",
         env: { PUBLISHER_INITIALIZE_STORAGE: "" },
         expect: value(false),
      },
      {
         name: "not a boolean fails",
         env: { PUBLISHER_INITIALIZE_STORAGE: "enabled" },
         expect: fails("PUBLISHER_INITIALIZE_STORAGE"),
      },
   ],
   watchEnvironments: [
      { name: "unset", expect: value([]) },
      {
         name: "PUBLISHER_WATCH_ENVIRONMENTS",
         env: { PUBLISHER_WATCH_ENVIRONMENTS: "a,b" },
         expect: value(["a", "b"]),
      },
      { name: "--watch-env", argv: ["--watch-env", "c"], expect: value(["c"]) },
      {
         name: "--watch-env wins over the variable",
         env: { PUBLISHER_WATCH_ENVIRONMENTS: "a" },
         argv: ["--watch-env", "c"],
         expect: value(["c"]),
      },
      {
         name: "--watch-env repeats",
         argv: ["--watch-env", "c", "--watch-env", "d"],
         expect: value(["c", "d"]),
      },
      {
         name: "blank",
         env: { PUBLISHER_WATCH_ENVIRONMENTS: "" },
         expect: value([]),
      },
      {
         name: "spaces and empty items",
         env: { PUBLISHER_WATCH_ENVIRONMENTS: " a , ,b" },
         expect: value(["a", "b"]),
      },
      {
         name: "--watch-env with no value fails",
         argv: ["--watch-env"],
         expect: fails("--watch-env has no value after it"),
      },
   ],

   // ── mcp ───────────────────────────────────────────────────────────────
   "mcp.port": [
      { name: "unset", expect: value(4040) },
      {
         name: "PUBLISHER_MCP_PORT",
         env: { PUBLISHER_MCP_PORT: "4141" },
         expect: value(4141),
      },
      { name: "--mcp-port", argv: ["--mcp-port", "4242"], expect: value(4242) },
      {
         name: "--mcp-port wins over the variable",
         env: { PUBLISHER_MCP_PORT: "4141" },
         argv: ["--mcp-port", "4242"],
         expect: value(4242),
      },
      {
         name: "0 is an ephemeral port",
         env: { PUBLISHER_MCP_PORT: "0" },
         expect: value(0),
      },
      { name: "blank", env: { PUBLISHER_MCP_PORT: "" }, expect: value(4040) },
      {
         name: "not a number fails",
         env: { PUBLISHER_MCP_PORT: "40a0" },
         expect: fails("PUBLISHER_MCP_PORT"),
      },
   ],
   "mcp.host": [
      { name: "unset is loopback", expect: value("127.0.0.1") },
      {
         name: "PUBLISHER_MCP_HOST",
         env: { PUBLISHER_MCP_HOST: "0.0.0.0" },
         expect: value("0.0.0.0"),
      },
      {
         name: "--mcp-host",
         argv: ["--mcp-host", "10.0.0.2"],
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
         name: "PUBLISHER_MCP_HOST wins over PUBLISHER_HOST",
         env: { PUBLISHER_HOST: "10.0.0.1", PUBLISHER_MCP_HOST: "10.0.0.2" },
         expect: value("10.0.0.2"),
      },
      {
         name: "blank follows PUBLISHER_HOST",
         env: { PUBLISHER_HOST: "10.0.0.1", PUBLISHER_MCP_HOST: "" },
         expect: value("10.0.0.1"),
      },
      {
         name: "whitespace only is unset",
         env: { PUBLISHER_MCP_HOST: "   " },
         expect: value("127.0.0.1"),
      },
   ],
   "mcp.corsOrigins": [
      { name: "unset denies", expect: value(false) },
      {
         name: "* allows any",
         env: { PUBLISHER_MCP_CORS_ORIGINS: "*" },
         expect: value("*"),
      },
      {
         name: "a list",
         env: {
            PUBLISHER_MCP_CORS_ORIGINS: "https://a.example, https://b.example",
         },
         expect: value(["https://a.example", "https://b.example"]),
      },
      {
         name: "a list of nothing denies",
         env: { PUBLISHER_MCP_CORS_ORIGINS: "," },
         expect: value(false),
      },
      {
         name: "blank",
         env: { PUBLISHER_MCP_CORS_ORIGINS: "" },
         expect: value(false),
      },
   ],
   "mcp.configFileEnabled": [
      { name: "unset", expect: value(true) },
      {
         name: "PUBLISHER_MCP_CONFIG_FILE_ENABLED=0",
         env: { PUBLISHER_MCP_CONFIG_FILE_ENABLED: "0" },
         expect: value(false),
      },
      {
         name: "PUBLISHER_MCP_CONFIG_FILE_ENABLED=true",
         env: { PUBLISHER_MCP_CONFIG_FILE_ENABLED: "true" },
         expect: value(true),
      },
      {
         name: "--no-mcp-config-file-enabled",
         argv: ["--no-mcp-config-file-enabled"],
         expect: value(false),
      },
      {
         name: "the file",
         file: { mcp: { configFileEnabled: false } },
         expect: value(false),
      },
      {
         name: "blank",
         env: { PUBLISHER_MCP_CONFIG_FILE_ENABLED: "" },
         expect: value(true),
      },
      {
         name: "not a boolean",
         env: { PUBLISHER_MCP_CONFIG_FILE_ENABLED: "maybe" },
         expect: fails("PUBLISHER_MCP_CONFIG_FILE_ENABLED"),
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
         name: "a string in the file is read as a boolean, where it used to fail",
         file: { mcp: { includeHiddenFilesAndSources: "yes" } },
         expect: value(true),
      },
      {
         name: "a value no boolean reader accepts fails",
         file: { mcp: { includeHiddenFilesAndSources: "maybe" } },
         expect: fails("mcp.includeHiddenFilesAndSources"),
      },
      {
         name: "a number fails, and is told the file's spelling",
         file: { mcp: { includeHiddenFilesAndSources: 1 } },
         expect: fails("expected true or false, got 1"),
      },
      {
         name: "an mcp that is not an object fails",
         file: { mcp: "on" },
         expect: fails('Invalid "mcp"'),
      },
      {
         name: "an unreadable file fails startup",
         file: "{",
         expect: fails("Failed to parse"),
      },
   ],
   "mcp.appOrigin": [
      {
         name: "unset is localhost on the REST port",
         expect: value("http://localhost:4000"),
      },
      {
         name: "follows PUBLISHER_PORT",
         env: { PUBLISHER_PORT: "4100" },
         expect: value("http://localhost:4100"),
      },
      {
         name: "PUBLISHER_MCP_APP_ORIGIN",
         env: { PUBLISHER_MCP_APP_ORIGIN: "https://publisher.example.com" },
         expect: value("https://publisher.example.com"),
      },
      {
         name: "a path is cut to the origin",
         env: {
            PUBLISHER_MCP_APP_ORIGIN: "https://publisher.example.com/base/",
         },
         expect: value("https://publisher.example.com"),
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
         name: "--admin-port",
         argv: ["--admin-port", "4200"],
         expect: value(4200),
      },
      {
         name: "--admin-port wins over the variable",
         env: { PUBLISHER_ADMIN_PORT: "4100" },
         argv: ["--admin-port", "4200"],
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
      {
         name: "a decimal point, which Number() read, fails",
         env: { PUBLISHER_ADMIN_PORT: "8080.0" },
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
         name: "--admin-host",
         argv: ["--admin-host", "10.0.0.4"],
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
      {
         name: "whitespace only is unset",
         env: { PUBLISHER_ADMIN_HOST: "   " },
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
      {
         name: "ignored without a provider",
         env: { ...CATALOG, PUBLISHER_ADMIN_STORAGE_REGION: "us-east-1" },
         expect: value(undefined),
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
      {
         name: "ignored without a provider",
         env: {
            ...CATALOG,
            PUBLISHER_ADMIN_STORAGE_ENDPOINT: "http://minio:9000",
         },
         expect: value(undefined),
      },
   ],

   // ── query, postgres, memory, packageLoad ──────────────────────────────
   "query.maxRows": [
      { name: "unset", expect: value(100_000) },
      {
         name: "PUBLISHER_QUERY_MAX_ROWS",
         env: { PUBLISHER_QUERY_MAX_ROWS: "500" },
         expect: value(500),
      },
      {
         name: "0 turns the cap off",
         env: { PUBLISHER_QUERY_MAX_ROWS: "0" },
         expect: value(0),
      },
      {
         name: "blank",
         env: { PUBLISHER_QUERY_MAX_ROWS: "" },
         expect: value(100_000),
      },
      {
         name: "negative",
         env: { PUBLISHER_QUERY_MAX_ROWS: "-1" },
         expect: fails("PUBLISHER_QUERY_MAX_ROWS"),
      },
      {
         name: "an exponent",
         env: { PUBLISHER_QUERY_MAX_ROWS: "1e3" },
         expect: fails("PUBLISHER_QUERY_MAX_ROWS"),
      },
      {
         name: "not a number",
         env: { PUBLISHER_QUERY_MAX_ROWS: "many" },
         expect: fails("PUBLISHER_QUERY_MAX_ROWS"),
      },
   ],
   "query.maxResponseBytes": [
      { name: "unset", expect: value(50_000_000) },
      {
         name: "PUBLISHER_QUERY_MAX_RESPONSE_BYTES",
         env: { PUBLISHER_QUERY_MAX_RESPONSE_BYTES: "1000" },
         expect: value(1000),
      },
      {
         name: "blank",
         env: { PUBLISHER_QUERY_MAX_RESPONSE_BYTES: "" },
         expect: value(50_000_000),
      },
      {
         name: "negative",
         env: { PUBLISHER_QUERY_MAX_RESPONSE_BYTES: "-5" },
         expect: fails("PUBLISHER_QUERY_MAX_RESPONSE_BYTES"),
      },
   ],
   "query.defaultRowLimit": [
      { name: "unset", expect: value(1000) },
      {
         name: "PUBLISHER_QUERY_DEFAULT_ROW_LIMIT",
         env: { PUBLISHER_QUERY_DEFAULT_ROW_LIMIT: "50" },
         expect: value(50),
      },
      {
         name: "blank",
         env: { PUBLISHER_QUERY_DEFAULT_ROW_LIMIT: "" },
         expect: value(1000),
      },
      {
         name: "0 is refused",
         env: { PUBLISHER_QUERY_DEFAULT_ROW_LIMIT: "0" },
         expect: fails("PUBLISHER_QUERY_DEFAULT_ROW_LIMIT"),
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
         name: "PUBLISHER_QUERY_MAX_CONCURRENT",
         env: { PUBLISHER_QUERY_MAX_CONCURRENT: "4" },
         expect: value(4),
      },
      {
         name: "0 turns it off",
         env: { PUBLISHER_QUERY_MAX_CONCURRENT: "0" },
         expect: value(0),
      },
      {
         name: "blank",
         env: { PUBLISHER_QUERY_MAX_CONCURRENT: "" },
         expect: value(32),
      },
      {
         name: "negative",
         env: { PUBLISHER_QUERY_MAX_CONCURRENT: "-1" },
         expect: fails("PUBLISHER_QUERY_MAX_CONCURRENT"),
      },
   ],
   "query.metadataEnabled": [
      { name: "unset", expect: value("off") },
      {
         name: "on",
         env: { PUBLISHER_QUERY_METADATA_ENABLED: "on" },
         expect: value("on"),
      },
      {
         name: "any case",
         env: { PUBLISHER_QUERY_METADATA_ENABLED: " On " },
         expect: value("on"),
      },
      {
         name: "off",
         env: { PUBLISHER_QUERY_METADATA_ENABLED: "OFF" },
         expect: value("off"),
      },
      {
         name: "blank",
         env: { PUBLISHER_QUERY_METADATA_ENABLED: "" },
         expect: value("off"),
      },
      {
         name: '"true" is on, where only "on" and "off" used to be read',
         env: { PUBLISHER_QUERY_METADATA_ENABLED: "true" },
         expect: value("on"),
      },
      {
         name: "not a boolean fails",
         env: { PUBLISHER_QUERY_METADATA_ENABLED: "full" },
         expect: fails("PUBLISHER_QUERY_METADATA_ENABLED"),
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
         env: { PUBLISHER_MEMORY_MAX_BYTES: "0" },
         expect: value(null),
      },
      {
         name: "PUBLISHER_MEMORY_MAX_BYTES",
         env: CEILING,
         expect: value(1_000_000),
      },
      {
         name: "blank",
         env: { PUBLISHER_MEMORY_MAX_BYTES: "" },
         expect: value(null),
      },
      {
         name: "negative",
         env: { PUBLISHER_MEMORY_MAX_BYTES: "-1" },
         expect: fails("PUBLISHER_MEMORY_MAX_BYTES"),
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
         name: "PUBLISHER_MEMORY_BACKPRESSURE_ENABLED",
         env: { ...CEILING, PUBLISHER_MEMORY_BACKPRESSURE_ENABLED: "false" },
         expect: value(false),
      },
      {
         name: "blank",
         env: { ...CEILING, PUBLISHER_MEMORY_BACKPRESSURE_ENABLED: "" },
         expect: value(true),
      },
      {
         name: "not a boolean",
         env: { ...CEILING, PUBLISHER_MEMORY_BACKPRESSURE_ENABLED: "maybe" },
         expect: fails("PUBLISHER_MEMORY_BACKPRESSURE_ENABLED"),
      },
   ],
   "packageLoad.workers": [
      { name: "unset", expect: value(1) },
      {
         name: "PUBLISHER_PACKAGE_LOAD_WORKERS",
         env: { PUBLISHER_PACKAGE_LOAD_WORKERS: "4" },
         expect: value(4),
      },
      {
         name: "blank is unset",
         env: { PUBLISHER_PACKAGE_LOAD_WORKERS: "" },
         expect: value(1),
      },
      {
         name: "0 is refused",
         env: { PUBLISHER_PACKAGE_LOAD_WORKERS: "0" },
         expect: fails("PUBLISHER_PACKAGE_LOAD_WORKERS"),
      },
      {
         name: "a number then text fails",
         env: { PUBLISHER_PACKAGE_LOAD_WORKERS: "5abc" },
         expect: fails("PUBLISHER_PACKAGE_LOAD_WORKERS"),
      },
   ],
   "packageLoad.jobTimeoutMs": [
      { name: "unset", expect: value(120_000) },
      {
         name: "PUBLISHER_PACKAGE_LOAD_JOB_TIMEOUT_MS",
         env: { PUBLISHER_PACKAGE_LOAD_JOB_TIMEOUT_MS: "5000" },
         expect: value(5000),
      },
      {
         name: "blank",
         env: { PUBLISHER_PACKAGE_LOAD_JOB_TIMEOUT_MS: "" },
         expect: value(120_000),
      },
      {
         name: "0 is refused",
         env: { PUBLISHER_PACKAGE_LOAD_JOB_TIMEOUT_MS: "0" },
         expect: fails("PUBLISHER_PACKAGE_LOAD_JOB_TIMEOUT_MS"),
      },
      {
         name: "not a number fails",
         env: { PUBLISHER_PACKAGE_LOAD_JOB_TIMEOUT_MS: "soon" },
         expect: fails("PUBLISHER_PACKAGE_LOAD_JOB_TIMEOUT_MS"),
      },
   ],
   "packageLoad.schemaCacheBytes": [
      { name: "unset", expect: value(16 * 1024 * 1024) },
      {
         name: "0 turns the cache off",
         env: { PUBLISHER_PACKAGE_LOAD_SCHEMA_CACHE_BYTES: "0" },
         expect: value(0),
      },
      {
         name: "PUBLISHER_PACKAGE_LOAD_SCHEMA_CACHE_BYTES",
         env: { PUBLISHER_PACKAGE_LOAD_SCHEMA_CACHE_BYTES: "1024" },
         expect: value(1024),
      },
      {
         name: "blank",
         env: { PUBLISHER_PACKAGE_LOAD_SCHEMA_CACHE_BYTES: "" },
         expect: value(16 * 1024 * 1024),
      },
      {
         name: "a fraction",
         env: { PUBLISHER_PACKAGE_LOAD_SCHEMA_CACHE_BYTES: "1.5" },
         expect: fails("PUBLISHER_PACKAGE_LOAD_SCHEMA_CACHE_BYTES"),
      },
      {
         name: "an exponent, which Number() read, fails",
         env: { PUBLISHER_PACKAGE_LOAD_SCHEMA_CACHE_BYTES: "16e6" },
         expect: fails("PUBLISHER_PACKAGE_LOAD_SCHEMA_CACHE_BYTES"),
      },
   ],

   // ── duckdb, ducklake, persist, materialization ────────────────────────
   "duckdb.extensionFetchPolicy": [
      { name: "unset", expect: value("on-demand") },
      {
         name: "PUBLISHER_DUCKDB_EXTENSION_FETCH_POLICY",
         env: { PUBLISHER_DUCKDB_EXTENSION_FETCH_POLICY: "local-only" },
         expect: value("local-only"),
      },
      {
         name: "any case",
         env: { PUBLISHER_DUCKDB_EXTENSION_FETCH_POLICY: " LOCAL-ONLY " },
         expect: value("local-only"),
      },
      {
         name: "blank",
         env: { PUBLISHER_DUCKDB_EXTENSION_FETCH_POLICY: "" },
         expect: value("on-demand"),
      },
      {
         name: "an unknown policy",
         env: { PUBLISHER_DUCKDB_EXTENSION_FETCH_POLICY: "never" },
         expect: fails("PUBLISHER_DUCKDB_EXTENSION_FETCH_POLICY"),
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
   "duckdb.threads": [
      {
         name: "unset: 4, or fewer on a smaller host",
         expect: value(String(Math.min(4, availableCpus()))),
      },
      {
         name: "PUBLISHER_DUCKDB_THREADS",
         env: { PUBLISHER_DUCKDB_THREADS: "8" },
         expect: value("8"),
      },
      {
         name: "off leaves DuckDB's default",
         env: { PUBLISHER_DUCKDB_THREADS: "OFF" },
         expect: value(undefined),
      },
      {
         name: "blank",
         env: { PUBLISHER_DUCKDB_THREADS: "" },
         expect: value(String(Math.min(4, availableCpus()))),
      },
      {
         name: "0 is refused",
         env: { PUBLISHER_DUCKDB_THREADS: "0" },
         expect: fails("PUBLISHER_DUCKDB_THREADS"),
      },
      {
         name: "not a count",
         env: { PUBLISHER_DUCKDB_THREADS: "many" },
         expect: fails("PUBLISHER_DUCKDB_THREADS"),
      },
   ],
   "duckdb.tempDirectory": [
      { name: "unset", expect: value(undefined) },
      {
         name: "PUBLISHER_DUCKDB_TEMP_DIRECTORY",
         env: { PUBLISHER_DUCKDB_TEMP_DIRECTORY: SPILL },
         expect: value(SPILL),
      },
      {
         name: "blank",
         env: { PUBLISHER_DUCKDB_TEMP_DIRECTORY: "" },
         expect: value(undefined),
      },
      {
         name: "a directory that cannot be made fails",
         // Under a file, where no OS can make a directory.
         env: {
            PUBLISHER_DUCKDB_TEMP_DIRECTORY: path.join(
               import.meta.path,
               "spill",
            ),
         },
         expect: fails("PUBLISHER_DUCKDB_TEMP_DIRECTORY"),
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
         name: "PUBLISHER_PERSIST_STORAGE_MODE",
         env: { PUBLISHER_PERSIST_STORAGE_MODE: "on" },
         expect: value("on"),
      },
      {
         name: "any case",
         env: { PUBLISHER_PERSIST_STORAGE_MODE: "Write-Only" },
         expect: value("write-only"),
      },
      {
         name: "blank",
         env: { PUBLISHER_PERSIST_STORAGE_MODE: "" },
         expect: value("off"),
      },
      {
         name: "an unknown mode",
         env: { PUBLISHER_PERSIST_STORAGE_MODE: "always" },
         expect: fails("PUBLISHER_PERSIST_STORAGE_MODE"),
      },
   ],
   "persist.addressIncludesDestination": [
      { name: "unset", expect: value(false) },
      {
         name: "PUBLISHER_PERSIST_ADDRESS_INCLUDES_DESTINATION",
         env: { PUBLISHER_PERSIST_ADDRESS_INCLUDES_DESTINATION: "true" },
         expect: value(true),
      },
      {
         name: "not a boolean",
         env: { PUBLISHER_PERSIST_ADDRESS_INCLUDES_DESTINATION: "sometimes" },
         expect: fails("PUBLISHER_PERSIST_ADDRESS_INCLUDES_DESTINATION"),
      },
   ],
   "materialize.enabled": [
      { name: "unset", expect: value(true) },
      {
         name: "PUBLISHER_MATERIALIZE_ENABLED",
         env: { PUBLISHER_MATERIALIZE_ENABLED: "false" },
         expect: value(false),
      },
      {
         name: "not a boolean",
         env: { PUBLISHER_MATERIALIZE_ENABLED: "sometimes" },
         expect: fails("PUBLISHER_MATERIALIZE_ENABLED"),
      },
   ],
   "materialize.gatedSources": [
      { name: "unset", expect: value(false) },
      {
         name: "PUBLISHER_MATERIALIZE_GATED_SOURCES",
         env: { PUBLISHER_MATERIALIZE_GATED_SOURCES: "true" },
         expect: value(true),
      },
      {
         name: "not a boolean",
         env: { PUBLISHER_MATERIALIZE_GATED_SOURCES: "sometimes" },
         expect: fails("PUBLISHER_MATERIALIZE_GATED_SOURCES"),
      },
   ],
   "persist.collisionEnforcementEnabled": [
      { name: "unset", expect: value(true) },
      {
         name: "PUBLISHER_PERSIST_COLLISION_ENFORCEMENT_ENABLED",
         env: { PUBLISHER_PERSIST_COLLISION_ENFORCEMENT_ENABLED: "0" },
         expect: value(false),
      },
      {
         name: "blank",
         env: { PUBLISHER_PERSIST_COLLISION_ENFORCEMENT_ENABLED: "" },
         expect: value(true),
      },
      {
         name: "not a boolean",
         env: { PUBLISHER_PERSIST_COLLISION_ENFORCEMENT_ENABLED: "strict" },
         expect: fails("PUBLISHER_PERSIST_COLLISION_ENFORCEMENT_ENABLED"),
      },
   ],
   "materialization.schedulerEnabled": [
      { name: "unset", expect: value(false) },
      {
         name: "PUBLISHER_MATERIALIZATION_SCHEDULER_ENABLED",
         env: SCHEDULER,
         expect: value(true),
      },
      {
         name: "blank",
         env: { PUBLISHER_MATERIALIZATION_SCHEDULER_ENABLED: "" },
         expect: value(false),
      },
      {
         name: "not a boolean",
         env: { PUBLISHER_MATERIALIZATION_SCHEDULER_ENABLED: "local" },
         expect: fails("PUBLISHER_MATERIALIZATION_SCHEDULER_ENABLED"),
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
         name: '"1" is on, where it used to be off',
         env: { PUBLISHER_ALLOW_PROXY_CONNECTIONS: "1" },
         expect: value(true),
      },
      {
         name: '"yes" is on, where it used to be off',
         env: { PUBLISHER_ALLOW_PROXY_CONNECTIONS: "yes" },
         expect: value(true),
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
         name: "PUBLISHER_ALLOW_S3_CREDENTIAL_CHAIN",
         env: { PUBLISHER_ALLOW_S3_CREDENTIAL_CHAIN: "off" },
         expect: value("off"),
      },
      {
         name: "any case",
         env: { PUBLISHER_ALLOW_S3_CREDENTIAL_CHAIN: " Destinations-Only " },
         expect: value("destinations-only"),
      },
      {
         name: "blank",
         env: { PUBLISHER_ALLOW_S3_CREDENTIAL_CHAIN: "" },
         expect: value("any"),
      },
      {
         name: "an unknown policy",
         env: { PUBLISHER_ALLOW_S3_CREDENTIAL_CHAIN: "none" },
         expect: fails("PUBLISHER_ALLOW_S3_CREDENTIAL_CHAIN"),
      },
   ],
   "authorize.bypassSecret": [
      { name: "unset turns the bypass off", expect: value(false) },
      {
         name: "PUBLISHER_AUTHORIZE_BYPASS_SECRET",
         env: { PUBLISHER_AUTHORIZE_BYPASS_SECRET: "s3cret" },
         expect: value(true),
      },
      {
         name: "the variable is trimmed",
         env: { PUBLISHER_AUTHORIZE_BYPASS_SECRET: " s3cret " },
         expect: value(true),
      },
      {
         name: "another secret does not match",
         env: { PUBLISHER_AUTHORIZE_BYPASS_SECRET: "other" },
         expect: value(false),
      },
      {
         name: "blank",
         env: { PUBLISHER_AUTHORIZE_BYPASS_SECRET: "" },
         expect: value(false),
      },
      {
         name: "whitespace only is unset",
         env: { PUBLISHER_AUTHORIZE_BYPASS_SECRET: "   " },
         expect: value(false),
      },
   ],
   "logging.level": [
      { name: "unset", expect: value("debug") },
      {
         name: "PUBLISHER_LOGGING_LEVEL",
         env: { PUBLISHER_LOGGING_LEVEL: "info" },
         expect: value("info"),
      },
      {
         name: "any case",
         env: { PUBLISHER_LOGGING_LEVEL: "WARN" },
         expect: value("warn"),
      },
      {
         name: "blank",
         env: { PUBLISHER_LOGGING_LEVEL: "" },
         expect: value("debug"),
      },
      {
         name: "surrounding whitespace is trimmed, where it used to be unknown",
         env: { PUBLISHER_LOGGING_LEVEL: " info" },
         expect: value("info"),
      },
      {
         name: "an unknown level fails",
         env: { PUBLISHER_LOGGING_LEVEL: "loud" },
         expect: fails("PUBLISHER_LOGGING_LEVEL"),
      },
   ],
   "logging.responseBodiesEnabled": [
      { name: "unset", expect: value(true) },
      {
         name: "PUBLISHER_LOGGING_RESPONSE_BODIES_ENABLED=false",
         env: { PUBLISHER_LOGGING_RESPONSE_BODIES_ENABLED: "false" },
         expect: value(false),
      },
      {
         name: "PUBLISHER_LOGGING_RESPONSE_BODIES_ENABLED=no",
         env: { PUBLISHER_LOGGING_RESPONSE_BODIES_ENABLED: "no" },
         expect: value(false),
      },
      {
         name: "--no-logging-response-bodies-enabled",
         argv: ["--no-logging-response-bodies-enabled"],
         expect: value(false),
      },
      {
         name: "blank",
         env: { PUBLISHER_LOGGING_RESPONSE_BODIES_ENABLED: "" },
         expect: value(true),
      },
      {
         name: "not a boolean fails",
         env: { PUBLISHER_LOGGING_RESPONSE_BODIES_ENABLED: "quiet" },
         expect: fails("PUBLISHER_LOGGING_RESPONSE_BODIES_ENABLED"),
      },
   ],
   "workloadLog.store": [
      { name: "unset", expect: value("off") },
      {
         name: "PUBLISHER_WORKLOAD_LOG_STORE on",
         env: { PUBLISHER_WORKLOAD_LOG_STORE: "on" },
         expect: value("on"),
      },
      {
         name: "any case, with surrounding space",
         env: { PUBLISHER_WORKLOAD_LOG_STORE: " On " },
         expect: value("on"),
      },
      {
         name: "blank",
         env: { PUBLISHER_WORKLOAD_LOG_STORE: "" },
         expect: value("off"),
      },
      {
         name: "another value fails",
         env: { PUBLISHER_WORKLOAD_LOG_STORE: "yes" },
         expect: fails("PUBLISHER_WORKLOAD_LOG_STORE"),
      },
   ],
   "workloadLog.retentionDays": [
      { name: "unset", expect: value(30) },
      {
         name: "PUBLISHER_WORKLOAD_LOG_RETENTION_DAYS",
         env: { PUBLISHER_WORKLOAD_LOG_RETENTION_DAYS: "7" },
         expect: value(7),
      },
      {
         name: "blank",
         env: { PUBLISHER_WORKLOAD_LOG_RETENTION_DAYS: "" },
         expect: value(30),
      },
      {
         name: "zero fails",
         env: { PUBLISHER_WORKLOAD_LOG_RETENTION_DAYS: "0" },
         expect: fails("PUBLISHER_WORKLOAD_LOG_RETENTION_DAYS"),
      },
      {
         name: "not a number fails",
         env: { PUBLISHER_WORKLOAD_LOG_RETENTION_DAYS: "a week" },
         expect: fails("PUBLISHER_WORKLOAD_LOG_RETENTION_DAYS"),
      },
   ],
   "workloadLog.maxRows": [
      { name: "unset", expect: value(1000000) },
      {
         name: "PUBLISHER_WORKLOAD_LOG_MAX_ROWS",
         env: { PUBLISHER_WORKLOAD_LOG_MAX_ROWS: "5000" },
         expect: value(5000),
      },
      {
         name: "blank",
         env: { PUBLISHER_WORKLOAD_LOG_MAX_ROWS: "" },
         expect: value(1000000),
      },
      {
         name: "not a number fails",
         env: { PUBLISHER_WORKLOAD_LOG_MAX_ROWS: "lots" },
         expect: fails("PUBLISHER_WORKLOAD_LOG_MAX_ROWS"),
      },
   ],
   "shutdown.drainDurationSeconds": [
      { name: "unset", expect: value(0) },
      {
         name: "PUBLISHER_SHUTDOWN_DRAIN_DURATION_SECONDS",
         env: { PUBLISHER_SHUTDOWN_DRAIN_DURATION_SECONDS: "5" },
         expect: value(5),
      },
      {
         name: "--shutdown-drain-duration-seconds",
         argv: ["--shutdown-drain-duration-seconds", "7"],
         expect: value(7),
      },
      {
         name: "the flag wins over the variable",
         env: { PUBLISHER_SHUTDOWN_DRAIN_DURATION_SECONDS: "5" },
         argv: ["--shutdown-drain-duration-seconds", "7"],
         expect: value(7),
      },
      {
         name: "blank",
         env: { PUBLISHER_SHUTDOWN_DRAIN_DURATION_SECONDS: "" },
         expect: value(0),
      },
      {
         name: "not a number fails",
         env: { PUBLISHER_SHUTDOWN_DRAIN_DURATION_SECONDS: "abc" },
         expect: fails("PUBLISHER_SHUTDOWN_DRAIN_DURATION_SECONDS"),
      },
   ],
   "shutdown.gracefulCloseTimeoutSeconds": [
      { name: "unset", expect: value(0) },
      {
         name: "PUBLISHER_SHUTDOWN_GRACEFUL_CLOSE_TIMEOUT_SECONDS",
         env: { PUBLISHER_SHUTDOWN_GRACEFUL_CLOSE_TIMEOUT_SECONDS: "5" },
         expect: value(5),
      },
      {
         name: "--shutdown-graceful-close-timeout-seconds",
         argv: ["--shutdown-graceful-close-timeout-seconds", "7"],
         expect: value(7),
      },
      {
         name: "blank",
         env: { PUBLISHER_SHUTDOWN_GRACEFUL_CLOSE_TIMEOUT_SECONDS: "" },
         expect: value(0),
      },
      {
         name: "not a number fails",
         env: { PUBLISHER_SHUTDOWN_GRACEFUL_CLOSE_TIMEOUT_SECONDS: "abc" },
         expect: fails("PUBLISHER_SHUTDOWN_GRACEFUL_CLOSE_TIMEOUT_SECONDS"),
      },
   ],

   // ── retrieval ─────────────────────────────────────────────────────────
   "retrieval.embedding.apiKey": [
      { name: "unset turns semantic search off", expect: value(null) },
      {
         name: "PUBLISHER_RETRIEVAL_EMBEDDING_API_KEY",
         env: { PUBLISHER_RETRIEVAL_EMBEDDING_API_KEY: "sk-1" },
         expect: value("sk-1"),
      },
      {
         name: "trimmed",
         env: { PUBLISHER_RETRIEVAL_EMBEDDING_API_KEY: " sk-1 " },
         expect: value("sk-1"),
      },
      {
         name: "blank",
         env: { PUBLISHER_RETRIEVAL_EMBEDDING_API_KEY: "" },
         expect: value(null),
      },
      {
         name: "whitespace only is unset",
         env: { PUBLISHER_RETRIEVAL_EMBEDDING_API_KEY: "   " },
         expect: value(null),
      },
   ],
   "retrieval.embedding.model": [
      { name: "unset", env: KEY, expect: value("text-embedding-3-small") },
      {
         name: "PUBLISHER_RETRIEVAL_EMBEDDING_MODEL",
         env: { ...KEY, PUBLISHER_RETRIEVAL_EMBEDDING_MODEL: "m1" },
         expect: value("m1"),
      },
      {
         name: "the file",
         env: KEY,
         file: { retrieval: { embedding: { model: "m2" } } },
         expect: value("m2"),
      },
      {
         name: "the variable wins over the file",
         env: { ...KEY, PUBLISHER_RETRIEVAL_EMBEDDING_MODEL: "m1" },
         file: { retrieval: { embedding: { model: "m2" } } },
         expect: value("m1"),
      },
      {
         name: "blank",
         env: { ...KEY, PUBLISHER_RETRIEVAL_EMBEDDING_MODEL: "" },
         expect: value("text-embedding-3-small"),
      },
      {
         name: "a model that is not a string fails",
         env: KEY,
         file: { retrieval: { embedding: { model: 5 } } },
         expect: fails("retrieval.embedding.model"),
      },
      {
         name: "a ${VAR} that is not set fails, like any value that cannot be read",
         env: KEY,
         file: {
            retrieval: { embedding: { model: "${UNSET_EMBEDDING_MODEL}" } },
         },
         expect: fails("${UNSET_EMBEDDING_MODEL} is not set"),
      },
      {
         name: "a rejected retrieval block's model is not used",
         env: KEY,
         file: REJECTED_RETRIEVAL({ model: "m2" }),
         expect: value("text-embedding-3-small"),
      },
   ],
   "retrieval.embedding.baseUrl": [
      { name: "unset", env: KEY, expect: value("https://api.openai.com/v1") },
      {
         name: "PUBLISHER_RETRIEVAL_EMBEDDING_BASE_URL, without its trailing slash",
         env: {
            ...KEY,
            PUBLISHER_RETRIEVAL_EMBEDDING_BASE_URL: "http://e.example/v1/",
         },
         expect: value("http://e.example/v1"),
      },
      {
         name: "the variable wins over the file",
         env: {
            ...KEY,
            PUBLISHER_RETRIEVAL_EMBEDDING_BASE_URL: "http://e.example/v1",
         },
         file: { retrieval: { embedding: { baseUrl: "http://f.example/v1" } } },
         expect: value("http://e.example/v1"),
      },
      {
         name: "blank",
         env: { ...KEY, PUBLISHER_RETRIEVAL_EMBEDDING_BASE_URL: "" },
         expect: value("https://api.openai.com/v1"),
      },
      {
         name: "not a URL",
         env: { ...KEY, PUBLISHER_RETRIEVAL_EMBEDDING_BASE_URL: "not a url" },
         expect: fails("PUBLISHER_RETRIEVAL_EMBEDDING_BASE_URL"),
      },
      {
         name: "a rejected retrieval block's base URL is not used",
         env: KEY,
         file: REJECTED_RETRIEVAL({ baseUrl: "http://f.example/v1" }),
         expect: value("https://api.openai.com/v1"),
      },
   ],
   "retrieval.embedding.dimensions": [
      { name: "unset", env: KEY, expect: value(undefined) },
      {
         name: "PUBLISHER_RETRIEVAL_EMBEDDING_DIMENSIONS",
         env: { ...KEY, PUBLISHER_RETRIEVAL_EMBEDDING_DIMENSIONS: "256" },
         expect: value(256),
      },
      {
         name: "the variable wins over the file",
         env: { ...KEY, PUBLISHER_RETRIEVAL_EMBEDDING_DIMENSIONS: "256" },
         file: { retrieval: { embedding: { dimensions: 512 } } },
         expect: value(256),
      },
      {
         name: "blank",
         env: { ...KEY, PUBLISHER_RETRIEVAL_EMBEDDING_DIMENSIONS: "" },
         expect: value(undefined),
      },
      {
         name: "0 is refused",
         env: { ...KEY, PUBLISHER_RETRIEVAL_EMBEDDING_DIMENSIONS: "0" },
         expect: fails("PUBLISHER_RETRIEVAL_EMBEDDING_DIMENSIONS"),
      },
      {
         name: "not a number",
         env: { ...KEY, PUBLISHER_RETRIEVAL_EMBEDDING_DIMENSIONS: "wide" },
         expect: fails("PUBLISHER_RETRIEVAL_EMBEDDING_DIMENSIONS"),
      },
      {
         name: "a rejected retrieval block's dimensions are not used",
         env: KEY,
         file: REJECTED_RETRIEVAL({ dimensions: 512 }),
         expect: value(undefined),
      },
   ],
   "retrieval.embedding.minSimilarity": [
      { name: "unset", env: KEY, expect: value(0.2) },
      {
         name: "PUBLISHER_RETRIEVAL_EMBEDDING_MIN_SIMILARITY",
         env: { ...KEY, PUBLISHER_RETRIEVAL_EMBEDDING_MIN_SIMILARITY: "0.35" },
         expect: value(0.35),
      },
      {
         name: "blank",
         env: { ...KEY, PUBLISHER_RETRIEVAL_EMBEDDING_MIN_SIMILARITY: "" },
         expect: value(0.2),
      },
      {
         name: "1 is refused",
         env: { ...KEY, PUBLISHER_RETRIEVAL_EMBEDDING_MIN_SIMILARITY: "1" },
         expect: fails("PUBLISHER_RETRIEVAL_EMBEDDING_MIN_SIMILARITY"),
      },
      {
         name: "not a number",
         env: { ...KEY, PUBLISHER_RETRIEVAL_EMBEDDING_MIN_SIMILARITY: "close" },
         expect: fails("PUBLISHER_RETRIEVAL_EMBEDDING_MIN_SIMILARITY"),
      },
   ],
   "retrieval.embedding.connectionSchemaIndexingEnabled": [
      { name: "unset", expect: value(false) },
      {
         name: "PUBLISHER_RETRIEVAL_EMBEDDING_CONNECTION_SCHEMA_INDEXING_ENABLED",
         env: {
            PUBLISHER_RETRIEVAL_EMBEDDING_CONNECTION_SCHEMA_INDEXING_ENABLED:
               "true",
         },
         expect: value(true),
      },
      {
         name: "blank",
         env: {
            PUBLISHER_RETRIEVAL_EMBEDDING_CONNECTION_SCHEMA_INDEXING_ENABLED:
               "",
         },
         expect: value(false),
      },
      {
         name: "not a boolean",
         env: {
            PUBLISHER_RETRIEVAL_EMBEDDING_CONNECTION_SCHEMA_INDEXING_ENABLED:
               "schemas",
         },
         expect: fails(
            "PUBLISHER_RETRIEVAL_EMBEDDING_CONNECTION_SCHEMA_INDEXING_ENABLED",
         ),
      },
   ],
   "retrieval.llm.apiKey": [
      { name: "unset turns the LLM off", expect: value(null) },
      {
         name: "PUBLISHER_RETRIEVAL_LLM_API_KEY",
         env: { PUBLISHER_RETRIEVAL_LLM_API_KEY: "sk-2" },
         expect: value("sk-2"),
      },
      {
         name: "trimmed",
         env: { PUBLISHER_RETRIEVAL_LLM_API_KEY: " sk-2 " },
         expect: value("sk-2"),
      },
      {
         name: "blank",
         env: { PUBLISHER_RETRIEVAL_LLM_API_KEY: "" },
         expect: value(null),
      },
      {
         name: "whitespace only is unset",
         env: { PUBLISHER_RETRIEVAL_LLM_API_KEY: "   " },
         expect: value(null),
      },
   ],
   "retrieval.llm.provider": [
      { name: "unset turns every LLM feature off", expect: value(null) },
      { name: "the file", file: LLM(), expect: value("anthropic") },
      {
         name: "PUBLISHER_RETRIEVAL_LLM_PROVIDER, with the file's model",
         env: { PUBLISHER_RETRIEVAL_LLM_PROVIDER: "openai" },
         file: { retrieval: { llm: { model: "m" } } },
         expect: value("openai"),
      },
      {
         name: "the variable over the file",
         env: { PUBLISHER_RETRIEVAL_LLM_PROVIDER: "ollama" },
         file: LLM(),
         expect: value("ollama"),
      },
      {
         name: "blank is unset",
         env: { PUBLISHER_RETRIEVAL_LLM_PROVIDER: "" },
         file: LLM(),
         expect: value("anthropic"),
      },
      {
         name: "a variable that switches the provider is checked against the file's other fields, naming each source",
         env: { PUBLISHER_RETRIEVAL_LLM_PROVIDER: "vertex" },
         file: LLM(),
         expect: fails(
            "Given: retrieval.llm.provider from PUBLISHER_RETRIEVAL_LLM_PROVIDER; retrieval.llm.model from ",
         ),
      },
      {
         name: "not a provider",
         env: { PUBLISHER_RETRIEVAL_LLM_PROVIDER: "cohere" },
         expect: fails(
            "retrieval.llm.provider from PUBLISHER_RETRIEVAL_LLM_PROVIDER",
         ),
      },
      {
         name: "not a provider, in the file",
         file: LLM({ provider: "cohere" }),
         expect: fails("retrieval.llm.provider from "),
      },
   ],
   "retrieval.llm.model": [
      { name: "the file", file: LLM(), expect: value("m") },
      {
         name: "PUBLISHER_RETRIEVAL_LLM_MODEL over the file",
         env: { PUBLISHER_RETRIEVAL_LLM_MODEL: "m2" },
         file: LLM(),
         expect: value("m2"),
      },
      {
         name: "from variables alone",
         env: {
            PUBLISHER_RETRIEVAL_LLM_PROVIDER: "anthropic",
            PUBLISHER_RETRIEVAL_LLM_MODEL: "m3",
         },
         expect: value("m3"),
      },
      {
         name: "a provider without one fails",
         file: { retrieval: { llm: { provider: "anthropic" } } },
         expect: fails("retrieval.llm.model"),
      },
   ],
   "retrieval.llm.baseUrl": [
      { name: "unset is the provider's", file: LLM(), expect: value(null) },
      {
         name: "the file, without its trailing slash",
         file: LLM({ baseUrl: "http://l.example/v1/" }),
         expect: value("http://l.example/v1"),
      },
      {
         name: "PUBLISHER_RETRIEVAL_LLM_BASE_URL over the file",
         env: { PUBLISHER_RETRIEVAL_LLM_BASE_URL: "http://v.example/v1" },
         file: LLM({ baseUrl: "http://l.example/v1" }),
         expect: value("http://v.example/v1"),
      },
      {
         name: "openai-compatible without one fails",
         file: LLM({ provider: "openai-compatible" }),
         expect: fails("retrieval.llm.baseUrl"),
      },
      {
         name: "openai-compatible with one from the variable",
         env: { PUBLISHER_RETRIEVAL_LLM_BASE_URL: "http://v.example/v1" },
         file: LLM({ provider: "openai-compatible" }),
         expect: value("http://v.example/v1"),
      },
      {
         name: "not a URL",
         env: { PUBLISHER_RETRIEVAL_LLM_BASE_URL: "nowhere" },
         file: LLM(),
         expect: fails("retrieval.llm.baseUrl"),
      },
   ],
   "retrieval.llm.projectId": [
      {
         name: "the file",
         file: { retrieval: { llm: VERTEX_LLM } },
         expect: value("p"),
      },
      {
         name: "PUBLISHER_RETRIEVAL_LLM_PROJECT_ID over the file",
         env: { PUBLISHER_RETRIEVAL_LLM_PROJECT_ID: "p2" },
         file: { retrieval: { llm: VERTEX_LLM } },
         expect: value("p2"),
      },
      {
         name: "vertex without one fails",
         file: LLM({ provider: "vertex", location: "l" }),
         expect: fails("needs projectId and location"),
      },
   ],
   "retrieval.llm.location": [
      {
         name: "the file",
         file: { retrieval: { llm: VERTEX_LLM } },
         expect: value("l"),
      },
      {
         name: "PUBLISHER_RETRIEVAL_LLM_LOCATION completes the file's vertex block",
         env: { PUBLISHER_RETRIEVAL_LLM_LOCATION: "europe-west4" },
         file: LLM({ provider: "vertex", projectId: "p" }),
         expect: value("europe-west4"),
      },
   ],
   "retrieval.llm.timeoutMs": [
      { name: "no provider, so no LLM", expect: value(null) },
      { name: "unset, with a provider", file: LLM(), expect: value(30000) },
      { name: "the file", file: LLM({ timeoutMs: 1000 }), expect: value(1000) },
      {
         name: "PUBLISHER_RETRIEVAL_LLM_TIMEOUT_MS over the file",
         env: { PUBLISHER_RETRIEVAL_LLM_TIMEOUT_MS: "2000" },
         file: LLM({ timeoutMs: 1000 }),
         expect: value(2000),
      },
      {
         name: "zero",
         env: { PUBLISHER_RETRIEVAL_LLM_TIMEOUT_MS: "0" },
         file: LLM(),
         expect: fails("PUBLISHER_RETRIEVAL_LLM_TIMEOUT_MS"),
      },
      {
         name: "not a number",
         env: { PUBLISHER_RETRIEVAL_LLM_TIMEOUT_MS: "soon" },
         file: LLM(),
         expect: fails("PUBLISHER_RETRIEVAL_LLM_TIMEOUT_MS"),
      },
   ],
   "retrieval.llm.concurrency": [
      { name: "unset, with a provider", file: LLM(), expect: value(4) },
      {
         name: "PUBLISHER_RETRIEVAL_LLM_CONCURRENCY",
         env: { PUBLISHER_RETRIEVAL_LLM_CONCURRENCY: "8" },
         file: LLM(),
         expect: value(8),
      },
      {
         name: "zero",
         file: LLM({ concurrency: 0 }),
         expect: fails("retrieval.llm.concurrency"),
      },
   ],
   "retrieval.llm.maxCallsPerSync": [
      { name: "unset, with a provider", file: LLM(), expect: value(300) },
      {
         name: "PUBLISHER_RETRIEVAL_LLM_MAX_CALLS_PER_SYNC",
         env: { PUBLISHER_RETRIEVAL_LLM_MAX_CALLS_PER_SYNC: "50" },
         file: LLM(),
         expect: value(50),
      },
      {
         name: "not a number",
         env: { PUBLISHER_RETRIEVAL_LLM_MAX_CALLS_PER_SYNC: "many" },
         file: LLM(),
         expect: fails("PUBLISHER_RETRIEVAL_LLM_MAX_CALLS_PER_SYNC"),
      },
   ],
   "retrieval.llm.maxCallsPerRequest": [
      { name: "unset, with a provider", file: LLM(), expect: value(20) },
      {
         name: "the file",
         file: LLM({ maxCallsPerRequest: 5 }),
         expect: value(5),
      },
      {
         name: "PUBLISHER_RETRIEVAL_LLM_MAX_CALLS_PER_REQUEST over the file",
         env: { PUBLISHER_RETRIEVAL_LLM_MAX_CALLS_PER_REQUEST: "10" },
         file: LLM({ maxCallsPerRequest: 5 }),
         expect: value(10),
      },
   ],
   "retrieval.embedding.provider": [
      { name: "unset", expect: value(null) },
      {
         name: "the file",
         file: EMBEDDING({ provider: "ollama", model: "nomic-embed-text" }),
         expect: value("ollama"),
      },
      {
         name: "PUBLISHER_RETRIEVAL_EMBEDDING_PROVIDER, with the file's model",
         env: { PUBLISHER_RETRIEVAL_EMBEDDING_PROVIDER: "google" },
         file: EMBEDDING({ model: "gemini-embedding-001" }),
         expect: value("google"),
      },
      {
         name: "PUBLISHER_RETRIEVAL_EMBEDDING_PROVIDER, with the model's variable",
         env: {
            PUBLISHER_RETRIEVAL_EMBEDDING_PROVIDER: "google",
            PUBLISHER_RETRIEVAL_EMBEDDING_MODEL: "gemini-embedding-001",
         },
         expect: value("google"),
      },
      {
         name: "anthropic has no embeddings API",
         env: { PUBLISHER_RETRIEVAL_EMBEDDING_PROVIDER: "anthropic" },
         expect: fails("PUBLISHER_RETRIEVAL_EMBEDDING_PROVIDER"),
      },
   ],
   "retrieval.embedding.projectId": [
      {
         name: "PUBLISHER_RETRIEVAL_EMBEDDING_PROJECT_ID completes the file's vertex block",
         env: { PUBLISHER_RETRIEVAL_EMBEDDING_PROJECT_ID: "p" },
         file: EMBEDDING({ provider: "vertex", model: "e", location: "l" }),
         expect: value("p"),
      },
      {
         name: "vertex without one fails",
         file: EMBEDDING({ provider: "vertex", model: "e", location: "l" }),
         expect: fails("needs projectId and location"),
      },
   ],
   "retrieval.embedding.location": [
      {
         name: "the file",
         file: EMBEDDING({
            provider: "vertex",
            model: "e",
            projectId: "p",
            location: "l",
         }),
         expect: value("l"),
      },
   ],
   "retrieval.embedding.queryPrefix": [
      { name: "unset", expect: value("") },
      {
         name: "the file, its trailing space kept",
         file: EMBEDDING({ queryPrefix: "search_query: " }),
         expect: value("search_query: "),
      },
      {
         name: "PUBLISHER_RETRIEVAL_EMBEDDING_QUERY_PREFIX, its trailing space kept",
         env: { PUBLISHER_RETRIEVAL_EMBEDDING_QUERY_PREFIX: "q: " },
         expect: value("q: "),
      },
   ],
   "retrieval.embedding.documentPrefix": [
      { name: "unset", expect: value("") },
      {
         name: "PUBLISHER_RETRIEVAL_EMBEDDING_DOCUMENT_PREFIX over the file",
         env: { PUBLISHER_RETRIEVAL_EMBEDDING_DOCUMENT_PREFIX: "d: " },
         file: EMBEDDING({ documentPrefix: "search_document: " }),
         expect: value("d: "),
      },
   ],
   "retrieval.egress.preset": [
      { name: "unset", expect: value("default") },
      {
         name: "the file",
         file: { retrieval: { egress: { preset: "full" } } },
         expect: value("full"),
      },
      {
         name: "PUBLISHER_RETRIEVAL_EGRESS_PRESET",
         env: { PUBLISHER_RETRIEVAL_EGRESS_PRESET: "full" },
         expect: value("full"),
      },
      {
         name: "not a preset",
         env: { PUBLISHER_RETRIEVAL_EGRESS_PRESET: "all" },
         expect: fails("PUBLISHER_RETRIEVAL_EGRESS_PRESET"),
      },
   ],
   "retrieval.dimensionValues.enabled": [
      { name: "unset", expect: value(true) },
      {
         name: "the file",
         file: { retrieval: { dimensionValues: { enabled: false } } },
         expect: value(false),
      },
      {
         name: "PUBLISHER_RETRIEVAL_DIMENSION_VALUES_ENABLED over the file",
         env: { PUBLISHER_RETRIEVAL_DIMENSION_VALUES_ENABLED: "false" },
         file: { retrieval: { dimensionValues: { enabled: true } } },
         expect: value(false),
      },
      {
         name: "a string in the file is refused by the block's parser",
         file: { retrieval: { dimensionValues: { enabled: "false" } } },
         expect: fails(
            'Invalid retrieval.dimensionValues.enabled: expected a boolean, got "false"',
         ),
      },
   ],
};

/** Read one case: its value, or the message it failed with. */
function outcome(
   key: string,
   input: SettingsInput,
): { value: unknown } | { fails: string } {
   try {
      return withSettings(input, (context) => {
         const current = Settings.current();
         const startup = current.fileError
            ? [current.fileError]
            : current.errors();
         if (startup.length > 0) {
            return { fails: startup.map((error) => error.message).join("\n") };
         }
         return { value: PROBES[key](context) };
      });
   } catch (error) {
      return { fails: error instanceof Error ? error.message : String(error) };
   }
}

describe("settings contract", () => {
   it("has a probe for every row, and a row for every probe", () => {
      expect(Object.keys(CONTRACT).sort()).toEqual(Object.keys(PROBES).sort());
   });

   it("has rows for every registered setting", () => {
      // The retrieval block is parsed whole by retrieval/config.ts, whose own
      // spec covers it; its leaves the registry reads have rows here.
      const unpinned = REGISTRY.map((entry) => entry.key).filter(
         (key) => !(key in CONTRACT),
      );
      expect(unpinned).toEqual(["retrieval"]);
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
               expect((actual as { value: unknown }).value).toEqual(
                  expected.value,
               );
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
         check({ argv: ["--no-serving-api-enabled", "--admin-port", "4100"] }),
      ).toEqual(value("starts"));
   });
});
