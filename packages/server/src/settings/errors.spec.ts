// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { SettingError, SettingsError, type SettingSource } from "./errors";

const FILE = "/srv/publisher.config.json";
const env = (name: string): SettingSource => ({ kind: "env", name });
const flag = (name: string): SettingSource => ({ kind: "flag", name });
const file: SettingSource = { kind: "file", name: FILE };

/**
 * The message for each type in the configuration design's table. Every
 * message a setting produces is built from one of these, so this is where the
 * wording is reviewed.
 */
const GOLDEN: Array<{
   type: string;
   error: SettingError;
   message: string;
}> = [
   {
      type: "boolean",
      error: new SettingError(
         "initializeStorage",
         env("INITIALIZE_STORAGE"),
         "a boolean (true/false, 1/0, yes/no or on/off)",
         "enabled",
      ),
      message:
         'initializeStorage from INITIALIZE_STORAGE: expected a boolean (true/false, 1/0, yes/no or on/off), got "enabled"',
   },
   {
      type: "boolean, in the file",
      error: new SettingError("frozenConfig", file, "true or false", 1),
      message: `frozenConfig from ${FILE}: expected true or false, got 1`,
   },
   {
      type: "integer with a range",
      error: new SettingError(
         "mcp.port",
         env("MCP_PORT"),
         "an integer from 0 to 65535",
         "40a0",
      ),
      message:
         'mcp.port from MCP_PORT: expected an integer from 0 to 65535, got "40a0"',
   },
   {
      type: "integer with a minimum",
      error: new SettingError(
         "query.maxRows",
         env("PUBLISHER_MAX_QUERY_ROWS"),
         "an integer of at least 0",
         "-1",
      ),
      message:
         'query.maxRows from PUBLISHER_MAX_QUERY_ROWS: expected an integer of at least 0, got "-1"',
   },
   {
      type: "integer, from a flag",
      error: new SettingError(
         "port",
         flag("--port"),
         "an integer from 0 to 65535",
         "40a0",
      ),
      message:
         'port from --port: expected an integer from 0 to 65535, got "40a0"',
   },
   {
      type: "number with an open range",
      error: new SettingError(
         "memory.highWaterFraction",
         env("PUBLISHER_MEMORY_HIGH_WATER_FRACTION"),
         "a number greater than 0 and less than 1",
         "1.5",
      ),
      message:
         'memory.highWaterFraction from PUBLISHER_MEMORY_HIGH_WATER_FRACTION: expected a number greater than 0 and less than 1, got "1.5"',
   },
   {
      type: "number with a half-open range",
      error: new SettingError(
         "retrieval.embedding.minSimilarity",
         env("EMBEDDING_MIN_SIMILARITY"),
         "a number of at least 0 and less than 1",
         "1",
      ),
      message:
         'retrieval.embedding.minSimilarity from EMBEDDING_MIN_SIMILARITY: expected a number of at least 0 and less than 1, got "1"',
   },
   {
      type: "enum of two",
      error: new SettingError(
         "duckdb.extensionFetchPolicy",
         env("EXTENSION_FETCH_POLICY"),
         'one of "on-demand" or "local-only"',
         "never",
      ),
      message:
         'duckdb.extensionFetchPolicy from EXTENSION_FETCH_POLICY: expected one of "on-demand" or "local-only", got "never"',
   },
   {
      type: "enum of three",
      error: new SettingError(
         "allow.s3CredentialChain",
         env("S3_CREDENTIAL_CHAIN_POLICY"),
         'one of "any", "destinations-only" or "off"',
         "none",
      ),
      message:
         'allow.s3CredentialChain from S3_CREDENTIAL_CHAIN_POLICY: expected one of "any", "destinations-only" or "off", got "none"',
   },
   {
      type: "list, in the file",
      error: new SettingError(
         "mcp.corsOrigins",
         file,
         "a list of strings",
         "https://a.example",
      ),
      message: `mcp.corsOrigins from ${FILE}: expected a list of strings, got "https://a.example"`,
   },
   {
      type: "string, in the file",
      error: new SettingError("admin.host", file, "a string", 4100),
      message: `admin.host from ${FILE}: expected a string, got 4100`,
   },
   {
      type: "a secret never shows its value",
      error: new SettingError(
         "admin.storage.secret",
         file,
         "a string",
         12345,
         true,
      ),
      message: `admin.storage.secret from ${FILE}: expected a string, got (set)`,
   },
];

describe("setting error messages", () => {
   for (const { type, error, message } of GOLDEN) {
      it(type, () => {
         expect(error.message).toBe(message);
      });
   }

   it("reports every error of one startup together, one per line", () => {
      const errors = GOLDEN.slice(0, 2).map((golden) => golden.error);
      expect(new SettingsError(errors).message).toBe(
         `${GOLDEN[0].message}\n${GOLDEN[1].message}`,
      );
   });
});
