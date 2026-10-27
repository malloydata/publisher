// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, describe, expect, it } from "bun:test";
import path from "path";
import { PROBES } from "../test_helpers/settings_probes";
import { withSettings } from "../test_helpers/settings_env";
import {
   derivedEnvName,
   derivedFlagName,
   REGISTRY,
   settings,
   Settings,
   type SettingKey,
} from ".";
import {
   BooleanType,
   EnumType,
   IntegerType,
   ListType,
   NumberType,
} from "./types";

/** A key, for the settings that need one to be read at all. */
const KEY = { EMBEDDING_API_KEY: "sk-test" };

/**
 * The variable the configuration design (docs/configuration-design.md, "The
 * mapping") gives each setting, read from step 2. A row the design marks
 * "unchanged" keeps today's name, which the derivation must reproduce.
 */
const DESIGN_VARIABLES: Partial<Record<SettingKey, string>> = {
   frozenConfig: "PUBLISHER_FROZEN_CONFIG",
   servingApiEnabled: "PUBLISHER_SERVING_API_ENABLED",
   adminApiEnabled: "PUBLISHER_ADMIN_API_ENABLED",
   port: "PUBLISHER_PORT",
   host: "PUBLISHER_HOST",
   rateLimit: "PUBLISHER_RATE_LIMIT",
   frameAncestors: "PUBLISHER_FRAME_ANCESTORS",
   root: "PUBLISHER_ROOT",
   config: "PUBLISHER_CONFIG",
   useBundledDefault: "PUBLISHER_USE_BUNDLED_DEFAULT",
   initializeStorage: "PUBLISHER_INITIALIZE_STORAGE",
   watchEnvironments: "PUBLISHER_WATCH_ENVIRONMENTS",
   "mcp.port": "PUBLISHER_MCP_PORT",
   "mcp.host": "PUBLISHER_MCP_HOST",
   "mcp.corsOrigins": "PUBLISHER_MCP_CORS_ORIGINS",
   "mcp.configFileEnabled": "PUBLISHER_MCP_CONFIG_FILE_ENABLED",
   "mcp.includeHiddenFilesAndSources":
      "PUBLISHER_MCP_INCLUDE_HIDDEN_FILES_AND_SOURCES",
   "mcp.trace": "PUBLISHER_MCP_TRACE",
   "admin.port": "PUBLISHER_ADMIN_PORT",
   "admin.host": "PUBLISHER_ADMIN_HOST",
   "admin.catalogDatabase": "PUBLISHER_ADMIN_CATALOG_DATABASE",
   "admin.storage.provider": "PUBLISHER_ADMIN_STORAGE_PROVIDER",
   "admin.storage.keyId": "PUBLISHER_ADMIN_STORAGE_KEY_ID",
   "admin.storage.secret": "PUBLISHER_ADMIN_STORAGE_SECRET",
   "admin.storage.region": "PUBLISHER_ADMIN_STORAGE_REGION",
   "admin.storage.endpoint": "PUBLISHER_ADMIN_STORAGE_ENDPOINT",
   "query.maxRows": "PUBLISHER_QUERY_MAX_ROWS",
   "query.maxResponseBytes": "PUBLISHER_QUERY_MAX_RESPONSE_BYTES",
   "query.defaultRowLimit": "PUBLISHER_QUERY_DEFAULT_ROW_LIMIT",
   "query.timeoutMs": "PUBLISHER_QUERY_TIMEOUT_MS",
   "query.maxConcurrent": "PUBLISHER_QUERY_MAX_CONCURRENT",
   "query.metadataEnabled": "PUBLISHER_QUERY_METADATA_ENABLED",
   "postgres.poolMax": "PUBLISHER_POSTGRES_POOL_MAX",
   "memory.maxBytes": "PUBLISHER_MEMORY_MAX_BYTES",
   "memory.highWaterFraction": "PUBLISHER_MEMORY_HIGH_WATER_FRACTION",
   "memory.lowWaterFraction": "PUBLISHER_MEMORY_LOW_WATER_FRACTION",
   "memory.checkIntervalMs": "PUBLISHER_MEMORY_CHECK_INTERVAL_MS",
   "memory.backpressureEnabled": "PUBLISHER_MEMORY_BACKPRESSURE_ENABLED",
   "packageLoad.workers": "PUBLISHER_PACKAGE_LOAD_WORKERS",
   "packageLoad.jobTimeoutMs": "PUBLISHER_PACKAGE_LOAD_JOB_TIMEOUT_MS",
   "packageLoad.schemaCacheBytes": "PUBLISHER_PACKAGE_LOAD_SCHEMA_CACHE_BYTES",
   "duckdb.extensionFetchPolicy": "PUBLISHER_DUCKDB_EXTENSION_FETCH_POLICY",
   "duckdb.memoryLimit": "PUBLISHER_DUCKDB_MEMORY_LIMIT",
   "duckdb.tempDirectory": "PUBLISHER_DUCKDB_TEMP_DIRECTORY",
   "duckdb.partitionedWriteFlushThreshold":
      "PUBLISHER_DUCKDB_PARTITIONED_WRITE_FLUSH_THRESHOLD",
   "ducklake.rowGroupSizeBytes": "PUBLISHER_DUCKLAKE_ROW_GROUP_SIZE_BYTES",
   "ducklake.targetFileSizeBytes": "PUBLISHER_DUCKLAKE_TARGET_FILE_SIZE_BYTES",
   "persist.storageMode": "PUBLISHER_PERSIST_STORAGE_MODE",
   "persist.collisionEnforcementEnabled":
      "PUBLISHER_PERSIST_COLLISION_ENFORCEMENT_ENABLED",
   "materialization.schedulerEnabled":
      "PUBLISHER_MATERIALIZATION_SCHEDULER_ENABLED",
   "materialization.schedulerIntervalMs":
      "PUBLISHER_MATERIALIZATION_SCHEDULER_INTERVAL_MS",
   "materialization.schedulerMaxFiresPerTick":
      "PUBLISHER_MATERIALIZATION_SCHEDULER_MAX_FIRES_PER_TICK",
   "allow.proxyConnections": "PUBLISHER_ALLOW_PROXY_CONNECTIONS",
   "allow.duckdbSetupSql": "PUBLISHER_ALLOW_DUCKDB_SETUP_SQL",
   "allow.unverifiedSshHostKey": "PUBLISHER_ALLOW_UNVERIFIED_SSH_HOST_KEY",
   "allow.s3CredentialChain": "PUBLISHER_ALLOW_S3_CREDENTIAL_CHAIN",
   "authorize.bypassSecret": "PUBLISHER_AUTHORIZE_BYPASS_SECRET",
   "logging.level": "PUBLISHER_LOGGING_LEVEL",
   "logging.responseBodiesEnabled": "PUBLISHER_LOGGING_RESPONSE_BODIES_ENABLED",
   "shutdown.drainDurationSeconds": "PUBLISHER_SHUTDOWN_DRAIN_DURATION_SECONDS",
   "shutdown.gracefulCloseTimeoutSeconds":
      "PUBLISHER_SHUTDOWN_GRACEFUL_CLOSE_TIMEOUT_SECONDS",
   "retrieval.embedding.apiKey": "PUBLISHER_RETRIEVAL_EMBEDDING_API_KEY",
   "retrieval.embedding.model": "PUBLISHER_RETRIEVAL_EMBEDDING_MODEL",
   "retrieval.embedding.baseUrl": "PUBLISHER_RETRIEVAL_EMBEDDING_BASE_URL",
   "retrieval.embedding.dimensions": "PUBLISHER_RETRIEVAL_EMBEDDING_DIMENSIONS",
   "retrieval.embedding.minSimilarity":
      "PUBLISHER_RETRIEVAL_EMBEDDING_MIN_SIMILARITY",
   "retrieval.embedding.connectionSchemaIndexingEnabled":
      "PUBLISHER_RETRIEVAL_EMBEDDING_CONNECTION_SCHEMA_INDEXING_ENABLED",
   "retrieval.llm.apiKey": "PUBLISHER_RETRIEVAL_LLM_API_KEY",
};

/** The flags the design's tables name outright. */
const DESIGN_FLAGS: Partial<Record<SettingKey, string>> = {
   frozenConfig: "--frozen-config",
   rateLimit: "--rate-limit",
   root: "--root",
   config: "--config",
   initializeStorage: "--initialize-storage",
   watchEnvironments: "--watch-environments",
   "mcp.port": "--mcp-port",
   "mcp.configFileEnabled": "--mcp-config-file-enabled",
   "mcp.includeHiddenFilesAndSources": "--mcp-include-hidden-files-and-sources",
   "admin.port": "--admin-port",
   "admin.storage.keyId": "--admin-storage-key-id",
   "query.maxRows": "--query-max-rows",
};

const named = REGISTRY.filter((entry) => entry.kind !== "structured");

describe("the registry", () => {
   it("derives the names the configuration design gives", () => {
      const derived = Object.fromEntries(
         named.map((entry) => [entry.key, derivedEnvName(entry.key)]),
      );
      expect(derived).toEqual(DESIGN_VARIABLES);
      for (const [key, flag] of Object.entries(DESIGN_FLAGS)) {
         expect(derivedFlagName(key)).toBe(flag);
      }
   });

   it("has one entry per key", () => {
      const keys = REGISTRY.map((entry) => entry.key);
      expect(new Set(keys).size).toBe(keys.length);
   });

   it("derives no variable or flag twice, and none that is another setting's alias", () => {
      const owners = new Map<string, string>();
      const claim = (name: string, key: string) => {
         const owner = owners.get(name);
         expect(
            owner === undefined || owner === key,
            `${name}: ${owner} and ${key}`,
         ).toBe(true);
         owners.set(name, key);
      };
      for (const entry of named) {
         for (const alias of [
            ...(entry.aliases.env ?? []),
            ...(entry.aliases.flags ?? []),
         ]) {
            claim(alias, entry.key);
         }
      }
      for (const entry of named) {
         claim(derivedEnvName(entry.key), entry.key);
         claim(derivedFlagName(entry.key), entry.key);
      }
   });

   it("gives a secret no flag, a launch setting no file key, and a structured one only a file key", () => {
      for (const entry of REGISTRY) {
         if (entry.secret) expect(entry.aliases.flags ?? []).toEqual([]);
         if (entry.kind === "launch") {
            expect(entry.aliases.file ?? []).toEqual([]);
         }
         if (entry.kind === "structured") {
            expect(entry.aliases.env ?? []).toEqual([]);
            expect(entry.aliases.flags ?? []).toEqual([]);
         }
      }
   });

   it("has defaults its own types accept", () => {
      for (const entry of REGISTRY) {
         const fallback: unknown = entry.default;
         if (fallback === undefined || typeof fallback === "function") continue;
         const read: unknown = entry.type.fromJson(fallback);
         expect(read, entry.key).toEqual(fallback);
      }
   });

   it("reads every variable it names, and only those, when nothing else is set", () => {
      // With the aliases gone, today's names must change nothing: no code
      // outside the registry reads one.
      const stripped = REGISTRY.map((entry) => ({ ...entry, aliases: {} }));
      const variables = named.flatMap((entry) => entry.aliases.env ?? []);
      const saved = { ...process.env };
      const observe = () =>
         Object.fromEntries(
            Object.entries(PROBES).map(([key, probe]) => {
               try {
                  return [key, probe({ serverRoot: "/nonexistent" })];
               } catch (error) {
                  return [key, `fails: ${(error as Error).message}`];
               }
            }),
         );
      const install = (env: Record<string, string | undefined>) =>
         Settings.install(
            Settings.load({ env, cwd: "/nonexistent", registry: stripped }),
         );
      const previous = install({ NODE_ENV: "test" });
      try {
         const quiet = observe();
         for (const name of variables) process.env[name] = "1";
         install({ ...process.env });
         expect(observe()).toEqual(quiet);
      } finally {
         for (const name of variables) {
            if (name in saved) process.env[name] = saved[name];
            else delete process.env[name];
         }
         Settings.install(previous);
      }
   });
});

describe("settings.get", () => {
   it("is typed by the registry", () => {
      withSettings({}, () => {
         const port: number = settings.get("mcp.port");
         const adminPort: number | undefined = settings.get("admin.port");
         const mode: "off" | "write-only" | "on" = settings.get(
            "persist.storageMode",
         );
         // @ts-expect-error an admin port may be unset
         const strict: number = settings.get("admin.port");
         // @ts-expect-error not a setting
         expect(() => settings.get("mcp.prot")).toThrow("No setting");
         expect([port, adminPort, mode, strict]).toEqual([
            4040,
            undefined,
            "off",
            undefined,
         ]);
      });
   });

   it("throws a value's error when the setting is read, naming where it came from", () => {
      withSettings({ env: { PUBLISHER_MAX_QUERY_ROWS: "-1" } }, () => {
         expect(() => settings.get("query.maxRows")).toThrow(
            'query.maxRows from PUBLISHER_MAX_QUERY_ROWS: expected an integer of at least 0, got "-1"',
         );
         expect(settings.source("query.maxRows")).toEqual({
            kind: "env",
            name: "PUBLISHER_MAX_QUERY_ROWS",
         });
      });
   });

   it("fails the setting whose file value names a variable that is not set", () => {
      withSettings({ file: { frozenConfig: "${UNSET_VAR}" } }, () => {
         expect(() => settings.get("frozenConfig")).toThrow(
            "${UNSET_VAR} is not set",
         );
      });
   });

   it("says where each value came from, and what a higher source hid", () => {
      withSettings(
         {
            env: { EMBEDDING_MODEL: "from-env" },
            argv: ["--port", "4200"],
            file: { retrieval: { embedding: { model: "from-file" } } },
         },
         ({ serverRoot }) => {
            expect(settings.source("port")).toEqual({
               kind: "flag",
               name: "--port",
            });
            expect(settings.get("retrieval.embedding.model")).toBe("from-file");
            expect(settings.source("retrieval.embedding.model")).toEqual({
               kind: "file",
               name: path.join(serverRoot, "publisher.config.json"),
            });
            expect(settings.shadowed("retrieval.embedding.model")).toEqual([
               {
                  source: { kind: "env", name: "EMBEDDING_MODEL" },
                  value: "from-env",
               },
            ]);
            expect(settings.isSet("port")).toBe(true);
            expect(settings.isSet("host")).toBe(false);
         },
      );
   });
});

describe("defaults that depend on another setting", () => {
   it("moves the MCP host with an explicit host, but not the admin host", () => {
      withSettings({ argv: ["--host", "10.0.0.1"] }, () => {
         expect(settings.get("mcp.host")).toBe("10.0.0.1");
         expect(settings.get("admin.host")).toBe("127.0.0.1");
      });
      withSettings({}, () => {
         expect(settings.get("mcp.host")).toBe("127.0.0.1");
      });
   });

   it("serves the bundled config only when no flag names a root or config, outside tests", () => {
      const bundled = (input: {
         env?: Record<string, string>;
         argv?: string[];
      }) =>
         withSettings(
            { ...input, env: { NODE_ENV: "production", ...input.env } },
            () => settings.get("useBundledDefault"),
         );
      expect(bundled({})).toBe(true);
      expect(bundled({ argv: ["--config", "/x.json"] })).toBe(false);
      expect(bundled({ env: { SERVER_ROOT: "/x" } })).toBe(true);
      expect(withSettings({}, () => settings.get("useBundledDefault"))).toBe(
         false,
      );
   });
});

describe("flags", () => {
   it("takes the next argument as a value, even one that looks like a flag", () => {
      withSettings({ argv: ["--port", "--host", "x"] }, () => {
         expect(() => settings.get("port")).toThrow(
            'port from --port: expected an integer from 0 to 65535, got "--host"',
         );
         expect(settings.isSet("host")).toBe(false);
      });
   });

   it("warns about a flag it does not know, naming the nearest, or today's name for a later one", () => {
      const warnings = Settings.load({
         argv: ["--mcp-port", "5000", "--server-root", "/x", "--help"],
         env: {},
         cwd: "/nonexistent",
      }).warnings.map((warning) => warning.message);
      expect(warnings).toEqual([
         "--mcp-port is not read before a later release, so it is ignored; until then use --mcp_port.",
         "Unknown flag --server-root, so it is ignored. Did you mean --server_root?",
      ]);
   });

   it("warns about a flag given without its value", () => {
      const warnings = Settings.load({
         argv: ["--admin_port"],
         env: {},
         cwd: "/nonexistent",
      }).warnings.map((warning) => warning.message);
      expect(warnings).toEqual([
         "--admin_port has no value after it, so it is ignored.",
      ]);
   });

   it("lets the last of a repeated flag win, except --watch-env, which adds", () => {
      withSettings(
         {
            argv: [
               "--port",
               "1",
               "--port",
               "2",
               "--watch-env",
               "a",
               "--watch-env",
               "b",
            ],
         },
         () => {
            expect(settings.get("port")).toBe(2);
            expect(settings.get("watchEnvironments")).toEqual(["a", "b"]);
         },
      );
   });

   it("sets a boolean with its flag alone, and off with --no-", () => {
      withSettings(
         { argv: ["--init", "--no-mcp-config", "--no-serving-api-enabled"] },
         () => {
            expect(settings.get("initializeStorage")).toBe(true);
            expect(settings.get("mcp.configFileEnabled")).toBe(false);
            expect(settings.get("servingApiEnabled")).toBe(false);
         },
      );
   });
});

describe("worker threads", () => {
   afterEach(() => Settings.install(undefined));

   it("get the same values, sources and errors as the thread that started them", () => {
      const parent = Settings.load({
         env: { PUBLISHER_MAX_QUERY_ROWS: "-1", MCP_PORT: "4141" },
         argv: ["--host", "10.0.0.1"],
         cwd: "/nonexistent",
      });
      const worker = Settings.fromWorkerData(
         structuredClone(parent.toWorkerData()),
      );
      for (const entry of REGISTRY) {
         const key = entry.key as SettingKey;
         let expected: unknown;
         try {
            expected = parent.get(key);
         } catch (error) {
            expect(() => worker.get(key)).toThrow((error as Error).message);
            continue;
         }
         expect(worker.get(key)).toEqual(expected as never);
         expect(worker.source(key)).toEqual(parent.source(key));
      }
   });
});

describe("step 1's rules", () => {
   const warningsFor = (input: Parameters<typeof withSettings>[0]) =>
      withSettings(input, () =>
         Settings.current().warnings.map((warning) => warning.message),
      );

   it("keeps a boolean's meaning where readers disagree, and says how to write it", () => {
      expect(warningsFor({ env: { INITIALIZE_STORAGE: "1" } })).toEqual([
         'initializeStorage: INITIALIZE_STORAGE=1 is read as off, as it always has been, though other boolean settings read "1" as on. Write true or false: a later release refuses "1" here.',
      ]);
      withSettings({ env: { INITIALIZE_STORAGE: "1" } }, () => {
         expect(settings.get("initializeStorage")).toBe(false);
      });
   });

   it("reads a negated variable's disagreement in the variable's own words", () => {
      withSettings({ env: { DISABLE_RESPONSE_LOGGING: "yes" } }, () => {
         expect(settings.get("logging.responseBodiesEnabled")).toBe(true);
         expect(Settings.current().warnings[0].message).toContain(
            "DISABLE_RESPONSE_LOGGING=yes is read as off",
         );
      });
   });

   it("fails a value no boolean reader accepts", () => {
      withSettings(
         { env: { PUBLISHER_ALLOW_PROXY_CONNECTIONS: "enabled" } },
         () => {
            expect(() => settings.get("allow.proxyConnections")).toThrow(
               'allow.proxyConnections from PUBLISHER_ALLOW_PROXY_CONNECTIONS: expected a boolean (true/false, 1/0, yes/no or on/off), got "enabled"',
            );
         },
      );
   });

   it("lists every bad value of one startup", () => {
      const errors = withSettings(
         { env: { MCP_PORT: "40a0", PUBLISHER_MAX_QUERY_ROWS: "-1" } },
         () =>
            Settings.current()
               .errors()
               .map((error) => error.message),
      );
      expect(errors).toEqual([
         'mcp.port from MCP_PORT: expected an integer from 0 to 65535, got "40a0"',
         'query.maxRows from PUBLISHER_MAX_QUERY_ROWS: expected an integer of at least 0, got "-1"',
      ]);
   });

   it("warns about a file key no setting reads, with the nearest one", () => {
      const messages = withSettings(
         {
            file: {
               adminApiEnable: false,
               query: { maxRows: 5 },
               mcp: { includeHiddenFiles: true },
               environments: [],
               retrieval: {},
            },
         },
         ({ serverRoot }) =>
            Settings.current().warnings.map((warning) =>
               warning.message.replace(
                  path.join(serverRoot, "publisher.config.json"),
                  "<file>",
               ),
            ),
      );
      expect(messages).toEqual([
         '<file> has "adminApiEnable", which no setting reads, so it is ignored. Did you mean "adminApiEnabled"?',
         '"query" in <file> holds settings that are not read from the file before a later release, so it is ignored; set them by their variables or flags.',
         '<file> has "mcp.includeHiddenFiles", which no setting reads, so it is ignored. Did you mean "mcp.includeHiddenFilesAndSources"?',
      ]);
   });

   it("says a later step reads a registered key from the file, and what to set until then", () => {
      const [message] = warningsFor({ file: { port: 4100 } });
      expect(message).toEndWith(
         "is not read from the file before a later release, so it is ignored; until then use --port.",
      );
   });

   it("leaves a structured setting's own keys to its parser", () => {
      expect(
         warningsFor({
            file: { theme: { anything: 1 }, retrieval: { indexing: {} } },
         }),
      ).toEqual([]);
   });
});

/**
 * Every setting, under each name it is read from today: a value its type
 * reads (the happy case) and one it does not (the sad case). The contract
 * table pins what each case means; these make sure no name is left out.
 */
describe("every setting, under each of its names", () => {
   const named = REGISTRY.filter((entry) => entry.kind !== "structured");

   /** A value each type reads, and one it does not, as a variable spells them. */
   function samples(entry: (typeof REGISTRY)[number]): {
      good: string;
      value: unknown;
      bad?: string;
   } {
      const type = entry.type;
      const today = entry.aliases.today?.reads;
      if (type instanceof BooleanType) {
         const good = today === "on-off" ? "on" : "true";
         return {
            good,
            value: entry.aliases.negated ? false : true,
            bad: "enabled",
         };
      }
      if (type instanceof IntegerType) {
         const n = [5000, 1000, 100, 10, 2, 1, 0].find((v) => type.allows(v))!;
         return { good: String(n), value: n, bad: "many" };
      }
      if (type instanceof NumberType) {
         return { good: "0.5", value: 0.5, bad: "half" };
      }
      if (type instanceof EnumType) {
         const v = type.values[type.values.length - 1];
         return { good: v, value: v, bad: "no-such-value" };
      }
      if (type instanceof ListType) return { good: "a,b", value: ["a", "b"] };
      // Any text is a string; a string has no sad case as a variable.
      return { good: "x", value: "x" };
   }

   for (const entry of named) {
      const key = entry.key as SettingKey;
      const { good, value, bad } = samples(entry);
      for (const name of entry.aliases.env ?? []) {
         it(`${key} reads ${name}`, () => {
            withSettings({ env: { [name]: good } }, () => {
               expect(settings.get(key) as unknown).toEqual(value);
               expect(settings.source(key)).toEqual({ kind: "env", name });
            });
         });
         if (bad === undefined) continue;
         it(`${key} refuses a bad ${name}`, () => {
            withSettings({ env: { [name]: bad } }, () => {
               const current = Settings.current();
               if (entry.aliases.today?.invalid === "warn") {
                  // Kept from today: a bad value is the default, with a warning.
                  expect(settings.get(key) as unknown).toEqual(entry.default);
                  expect(current.warnings[0].message).toContain(name);
                  return;
               }
               expect(() => settings.get(key)).toThrow(`${key} from ${name}`);
               expect(current.errors().length).toBe(1);
            });
         });
      }
      for (const flag of entry.aliases.flags ?? []) {
         const isSwitch = entry.type instanceof BooleanType;
         it(`${key} reads ${flag}`, () => {
            withSettings({ argv: isSwitch ? [flag] : [flag, good] }, () => {
               expect(settings.get(key) as unknown).toEqual(
                  isSwitch ? !flag.startsWith("--no-") : value,
               );
               expect(settings.source(key)).toEqual({
                  kind: "flag",
                  name: flag,
               });
            });
         });
         if (isSwitch || bad === undefined) continue;
         it(`${key} refuses a bad ${flag}`, () => {
            withSettings({ argv: [flag, bad] }, () => {
               expect(() => settings.get(key)).toThrow(`${key} from ${flag}`);
            });
         });
      }
      for (const filePath of entry.aliases.file ?? []) {
         const isBoolean = entry.type instanceof BooleanType;
         const nest = (leaf: unknown) => {
            const file: Record<string, unknown> = {};
            let node = file;
            const parts = filePath.split(".");
            for (const part of parts.slice(0, -1)) {
               node = (node[part] = {}) as Record<string, unknown>;
            }
            node[parts[parts.length - 1]] = leaf;
            return file;
         };
         const json = isBoolean
            ? true
            : entry.type instanceof IntegerType
              ? value
              : good;
         it(`${key} reads "${filePath}" in the file`, () => {
            withSettings({ env: KEY, file: nest(json) }, () => {
               expect(settings.get(key) as unknown).toEqual(
                  isBoolean ? true : value,
               );
               expect(settings.source(key).kind).toBe("file");
            });
         });
         it(`${key} refuses an object as "${filePath}" in the file`, () => {
            withSettings({ env: KEY, file: nest({ not: "a value" }) }, () => {
               expect(() => settings.get(key)).toThrow(key);
            });
         });
      }
   }
});
