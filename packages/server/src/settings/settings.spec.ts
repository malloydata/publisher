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
import { REMOVED_VARIABLES, removedVariableWarnings } from "./removed";
import { namesOf } from "./settings";
import {
   BooleanType,
   EnumType,
   IntegerType,
   ListType,
   NumberType,
} from "./types";

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
   // Not in the design's tables: a setting added since takes its derived name
   // from the start, as the design asks of a new setting.
   callerHeader: "PUBLISHER_CALLER_HEADER",
   packageInlineFields: "PUBLISHER_PACKAGE_INLINE_FIELDS",
   modelPage: "PUBLISHER_MODEL_PAGE",
   packageVersioning: "PUBLISHER_PACKAGE_VERSIONING",
   versionPromotion: "PUBLISHER_VERSION_PROMOTION",
   root: "PUBLISHER_ROOT",
   config: "PUBLISHER_CONFIG",
   useBundledDefault: "PUBLISHER_USE_BUNDLED_DEFAULT",
   initializeStorage: "PUBLISHER_INITIALIZE_STORAGE",
   watchEnvironments: "PUBLISHER_WATCH_ENVIRONMENTS",
   "mcp.port": "PUBLISHER_MCP_PORT",
   "mcp.host": "PUBLISHER_MCP_HOST",
   "mcp.corsOrigins": "PUBLISHER_MCP_CORS_ORIGINS",
   "mcp.appOrigin": "PUBLISHER_MCP_APP_ORIGIN",
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
   "duckdb.threads": "PUBLISHER_DUCKDB_THREADS",
   "duckdb.tempDirectory": "PUBLISHER_DUCKDB_TEMP_DIRECTORY",
   "ducklake.rowGroupSizeBytes": "PUBLISHER_DUCKLAKE_ROW_GROUP_SIZE_BYTES",
   "ducklake.targetFileSizeBytes": "PUBLISHER_DUCKLAKE_TARGET_FILE_SIZE_BYTES",
   "persist.storageMode": "PUBLISHER_PERSIST_STORAGE_MODE",
   "persist.addressIncludesDestination":
      "PUBLISHER_PERSIST_ADDRESS_INCLUDES_DESTINATION",
   "persist.collisionEnforcementEnabled":
      "PUBLISHER_PERSIST_COLLISION_ENFORCEMENT_ENABLED",
   "materialize.enabled": "PUBLISHER_MATERIALIZE_ENABLED",
   "materialize.gatedSources": "PUBLISHER_MATERIALIZE_GATED_SOURCES",
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
   "workloadLog.store": "PUBLISHER_WORKLOAD_LOG_STORE",
   "workloadLog.retentionDays": "PUBLISHER_WORKLOAD_LOG_RETENTION_DAYS",
   "workloadLog.maxRows": "PUBLISHER_WORKLOAD_LOG_MAX_ROWS",
   "retrieval.embedding.apiKey": "PUBLISHER_RETRIEVAL_EMBEDDING_API_KEY",
   "retrieval.embedding.model": "PUBLISHER_RETRIEVAL_EMBEDDING_MODEL",
   "retrieval.embedding.baseUrl": "PUBLISHER_RETRIEVAL_EMBEDDING_BASE_URL",
   "retrieval.embedding.dimensions": "PUBLISHER_RETRIEVAL_EMBEDDING_DIMENSIONS",
   "retrieval.embedding.minSimilarity":
      "PUBLISHER_RETRIEVAL_EMBEDDING_MIN_SIMILARITY",
   "retrieval.embedding.connectionSchemaIndexingEnabled":
      "PUBLISHER_RETRIEVAL_EMBEDDING_CONNECTION_SCHEMA_INDEXING_ENABLED",
   "retrieval.llm.apiKey": "PUBLISHER_RETRIEVAL_LLM_API_KEY",
   // "The other retrieval keys ... keep their file paths and gain derived
   // variables and flags."
   "retrieval.llm.provider": "PUBLISHER_RETRIEVAL_LLM_PROVIDER",
   "retrieval.llm.model": "PUBLISHER_RETRIEVAL_LLM_MODEL",
   "retrieval.llm.baseUrl": "PUBLISHER_RETRIEVAL_LLM_BASE_URL",
   "retrieval.llm.projectId": "PUBLISHER_RETRIEVAL_LLM_PROJECT_ID",
   "retrieval.llm.location": "PUBLISHER_RETRIEVAL_LLM_LOCATION",
   "retrieval.llm.timeoutMs": "PUBLISHER_RETRIEVAL_LLM_TIMEOUT_MS",
   "retrieval.llm.concurrency": "PUBLISHER_RETRIEVAL_LLM_CONCURRENCY",
   "retrieval.llm.maxCallsPerSync":
      "PUBLISHER_RETRIEVAL_LLM_MAX_CALLS_PER_SYNC",
   "retrieval.llm.maxCallsPerRequest":
      "PUBLISHER_RETRIEVAL_LLM_MAX_CALLS_PER_REQUEST",
   "retrieval.embedding.provider": "PUBLISHER_RETRIEVAL_EMBEDDING_PROVIDER",
   "retrieval.embedding.projectId": "PUBLISHER_RETRIEVAL_EMBEDDING_PROJECT_ID",
   "retrieval.embedding.location": "PUBLISHER_RETRIEVAL_EMBEDDING_LOCATION",
   "retrieval.embedding.queryPrefix":
      "PUBLISHER_RETRIEVAL_EMBEDDING_QUERY_PREFIX",
   "retrieval.embedding.documentPrefix":
      "PUBLISHER_RETRIEVAL_EMBEDDING_DOCUMENT_PREFIX",
   "retrieval.egress.preset": "PUBLISHER_RETRIEVAL_EGRESS_PRESET",
   "retrieval.dimensionValues.enabled":
      "PUBLISHER_RETRIEVAL_DIMENSION_VALUES_ENABLED",
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
   versionPromotion: "--version-promotion",
};

/** The variables step 3 removed, each now read by nothing. */
const REMOVED = Object.keys(REMOVED_VARIABLES);

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

   it("derives no variable or flag twice", () => {
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
         const names = namesOf(entry);
         for (const name of [...names.env, ...names.flags]) {
            claim(name, entry.key);
         }
      }
   });

   it("gives a secret no flag, a launch setting no file key, and a structured one only a file key", () => {
      for (const entry of REGISTRY) {
         const names = namesOf(entry);
         if (entry.secret || entry.noFlag) {
            expect(names.flags, entry.key).toEqual([]);
         }
         if (entry.kind === "launch") {
            expect(names.file, entry.key).toEqual([]);
         }
         if (entry.kind === "structured") {
            expect(names.env, entry.key).toEqual([]);
            expect(names.flags, entry.key).toEqual([]);
            expect(names.file, entry.key).toEqual([entry.key]);
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

   it("names a registered setting for every removed variable, and none a current name", () => {
      const current = new Set(named.flatMap((entry) => namesOf(entry).env));
      for (const [name, { key }] of Object.entries(REMOVED_VARIABLES)) {
         expect(
            REGISTRY.some((entry) => entry.key === key),
            name,
         ).toBe(true);
         expect(current.has(name), name).toBe(false);
      }
   });

   it("warns about each removed variable that is set, naming the one that replaced it", () => {
      expect(
         removedVariableWarnings({
            MCP_PORT: "4141",
            DISABLE_RESPONSE_LOGGING: "true",
            LOG_LEVEL: " ",
            PUBLISHER_MCP_PORT: "4242",
         }),
      ).toEqual([
         "MCP_PORT is set, but this release no longer reads it: set PUBLISHER_MCP_PORT (mcp.port) instead.",
         "DISABLE_RESPONSE_LOGGING is set, but this release no longer reads it: set PUBLISHER_LOGGING_RESPONSE_BODIES_ENABLED (logging.responseBodiesEnabled) instead, which says the opposite.",
      ]);
   });

   it("reads none of the variables step 3 removed", () => {
      // No code outside the registry reads one: setting each changes no
      // setting.
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
         Settings.install(Settings.load({ env, cwd: "/nonexistent" }));
      // The baseline is this process's own variables without the removed
      // names, so the comparison isolates them: a derived name the runner
      // sets for every suite (bunfig.toml preloads
      // PUBLISHER_PACKAGE_VERSIONING=on) is in both loads.
      const previous = install(
         Object.fromEntries(
            Object.entries(process.env).filter(
               ([name]) => !REMOVED.includes(name),
            ),
         ),
      );
      try {
         const quiet = observe();
         for (const name of REMOVED) process.env[name] = "1";
         install({ ...process.env });
         expect(observe()).toEqual(quiet);
         expect(Settings.current().errors()).toEqual([]);
      } finally {
         for (const name of REMOVED) {
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
      withSettings({ env: { PUBLISHER_QUERY_MAX_ROWS: "-1" } }, () => {
         expect(() => settings.get("query.maxRows")).toThrow(
            'query.maxRows from PUBLISHER_QUERY_MAX_ROWS: expected an integer of at least 0, got "-1"',
         );
         expect(settings.source("query.maxRows")).toEqual({
            kind: "env",
            name: "PUBLISHER_QUERY_MAX_ROWS",
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
            env: { PUBLISHER_RETRIEVAL_EMBEDDING_MODEL: "from-env" },
            argv: ["--port", "4200"],
            file: { retrieval: { embedding: { model: "from-file" } } },
         },
         ({ serverRoot }) => {
            expect(settings.source("port")).toEqual({
               kind: "flag",
               name: "--port",
            });
            expect(settings.get("retrieval.embedding.model")).toBe("from-env");
            expect(settings.source("retrieval.embedding.model")).toEqual({
               kind: "env",
               name: "PUBLISHER_RETRIEVAL_EMBEDDING_MODEL",
            });
            expect(settings.shadowed("retrieval.embedding.model")).toEqual([
               {
                  source: {
                     kind: "file",
                     name: path.join(serverRoot, "publisher.config.json"),
                  },
                  value: "from-file",
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

   it("serves the bundled config only when nothing names a root or config, outside tests", () => {
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
      expect(bundled({ env: { PUBLISHER_ROOT: "/x" } })).toBe(false);
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

   it("refuses a flag it does not know, naming the nearest", () => {
      const errors = Settings.load({
         argv: [
            "--mcp_prot",
            "5000",
            "--server-root",
            "/x",
            "--mcp_port",
            "4040",
            "--help",
         ],
         env: {},
         cwd: "/nonexistent",
      })
         .errors()
         .map((error) => error.message);
      expect(errors).toEqual([
         "Unknown flag --mcp_prot. Did you mean --mcp-port?",
         "Unknown flag --server-root.",
         "Unknown flag --mcp_port. Did you mean --mcp-port?",
      ]);
   });

   it("refuses a flag given without its value", () => {
      const errors = Settings.load({
         argv: ["--admin-port"],
         env: {},
         cwd: "/nonexistent",
      })
         .errors()
         .map((error) => error.message);
      expect(errors).toEqual(["--admin-port has no value after it."]);
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
         {
            argv: [
               "--init",
               "--no-mcp-config-file-enabled",
               "--no-serving-api-enabled",
            ],
         },
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
         env: { PUBLISHER_QUERY_MAX_ROWS: "-1", PUBLISHER_MCP_PORT: "4141" },
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

describe("what fails startup", () => {
   /** Startup's errors, with the config file's path shown as `<file>`. */
   const errorsFor = (input: Parameters<typeof withSettings>[0]) =>
      withSettings(input, ({ serverRoot }) =>
         Settings.current()
            .errors()
            .map((error) =>
               error.message.replace(
                  path.join(serverRoot, "publisher.config.json"),
                  "<file>",
               ),
            ),
      );

   it("fails a value that is no boolean", () => {
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
         {
            env: { PUBLISHER_MCP_PORT: "40a0", PUBLISHER_QUERY_MAX_ROWS: "-1" },
         },
         () =>
            Settings.current()
               .errors()
               .map((error) => error.message),
      );
      expect(errors).toEqual([
         'mcp.port from PUBLISHER_MCP_PORT: expected an integer from 0 to 65535, got "40a0"',
         'query.maxRows from PUBLISHER_QUERY_MAX_ROWS: expected an integer of at least 0, got "-1"',
      ]);
   });

   it("refuses a file key no setting reads, naming the nearest one", () => {
      expect(
         errorsFor({
            file: {
               adminApiEnable: false,
               query: { maxRows: 5, maxRow: 6 },
               mcp: { includeHiddenFiles: true },
               environments: [],
               retrieval: {},
               // The old name of "environments", which step 3 removed.
               projects: [],
            },
         }),
      ).toEqual([
         '<file> has "adminApiEnable", which no setting reads. Did you mean "adminApiEnabled"?',
         '<file> has "query.maxRow", which no setting reads. Did you mean "query.maxRows"?',
         '<file> has "mcp.includeHiddenFiles", which no setting reads. Did you mean "mcp.includeHiddenFilesAndSources"?',
         '<file> has "projects", which no setting reads.',
      ]);
   });

   it("refuses a launch setting in the file, and says what to set instead", () => {
      expect(errorsFor({ file: { root: "/srv" } })).toEqual([
         '"root" in <file> cannot be set there: it decides which file is read, or must not come from one, so it is a variable or a flag only (PUBLISHER_ROOT, --root).',
      ]);
   });

   it("leaves a structured setting's own keys to its parser", () => {
      const input = {
         file: { theme: { anything: 1 }, retrieval: { indexing: {} } },
      };
      expect(errorsFor(input)).toEqual([]);
   });
});

describe("the derived names", () => {
   /** A value each type reads, as a variable or flag and as the file holds it. */
   function sample(entry: (typeof REGISTRY)[number]): {
      raw: string;
      json: unknown;
      value: unknown;
   } {
      const type = entry.type;
      if (type instanceof BooleanType)
         return { raw: "true", json: true, value: true };
      if (type instanceof IntegerType) {
         const n = [5000, 1000, 100, 10, 2, 1, 0].find((v) => type.allows(v))!;
         return { raw: String(n), json: n, value: n };
      }
      if (type instanceof NumberType)
         return { raw: "0.5", json: 0.5, value: 0.5 };
      if (type instanceof EnumType) {
         const v = type.values[type.values.length - 1];
         return { raw: v, json: v, value: v };
      }
      if (type instanceof ListType)
         return { raw: "a,b", json: ["a", "b"], value: ["a", "b"] };
      return { raw: "x", json: "x", value: "x" };
   }
   const named = REGISTRY.filter((entry) => entry.kind !== "structured");

   it("reads every setting from its derived variable", () => {
      for (const entry of named) {
         const { raw, value } = sample(entry);
         const name = derivedEnvName(entry.key);
         withSettings({ env: { [name]: raw } }, () => {
            const key = entry.key as SettingKey;
            expect(settings.get(key) as unknown, name).toEqual(value);
            expect(settings.source(key)).toEqual({ kind: "env", name });
         });
      }
   });

   it("reads every setting but a secret from its derived flag, and a boolean's --no- form", () => {
      for (const entry of named.filter(
         (entry) => !entry.secret && !entry.noFlag,
      )) {
         const { raw, value } = sample(entry);
         const flag = derivedFlagName(entry.key);
         const isSwitch = entry.type instanceof BooleanType;
         withSettings({ argv: isSwitch ? [flag] : [flag, raw] }, () => {
            expect(
               settings.get(entry.key as SettingKey) as unknown,
               flag,
            ).toEqual(value);
         });
         if (isSwitch) {
            const off = `--no-${flag.slice(2)}`;
            withSettings({ argv: [off] }, () => {
               expect(
                  settings.get(entry.key as SettingKey) as unknown,
                  off,
               ).toBe(false);
            });
         }
      }
      expect(
         Settings.load({
            argv: ["--admin-catalog-database", "x"],
            env: {},
            cwd: "/nonexistent",
         })
            .errors()
            .map((error) => error.message),
      ).toContain("Unknown flag --admin-catalog-database.");
      // No secrets, but the design gives them no flag: the flag their key
      // would derive is unknown, so startup fails and the setting keeps its
      // default.
      const noFlag: Array<[SettingKey, string, string, string]> = [
         ["packageVersioning", "--package-versioning", "off", "on"],
         ["packageInlineFields", "--package-inline-fields", "off", "on"],
         ["modelPage", "--model-page", "viewer", "explorer"],
      ];
      for (const [key, flag, given, unchanged] of noFlag) {
         const loaded = Settings.load({
            argv: [flag, given],
            env: {},
            cwd: "/nonexistent",
         });
         expect(loaded.get(key)).toBe(unchanged);
         expect(
            loaded
               .errors()
               .some((error) =>
                  error.message.startsWith(`Unknown flag ${flag}`),
               ),
         ).toBe(true);
      }
   });

   it("reads every setting but a launch setting from its key in the file", () => {
      for (const entry of named.filter((entry) => entry.kind !== "launch")) {
         const { json, value } = sample(entry);
         const file: Record<string, unknown> = {};
         let node = file;
         const parts = entry.key.split(".");
         for (const part of parts.slice(0, -1)) {
            node = (node[part] ??= {}) as Record<string, unknown>;
         }
         node[parts[parts.length - 1]] = entry.secret
            ? "${SAMPLE_SECRET}"
            : json;
         withSettings({ env: { SAMPLE_SECRET: "x" }, file }, () => {
            expect(
               settings.get(entry.key as SettingKey) as unknown,
               entry.key,
            ).toEqual(entry.secret ? "x" : value);
            expect(settings.source(entry.key as SettingKey).kind).toBe("file");
         });
      }
   });

   it("refuses a bad value under every derived variable, flag and file key", () => {
      const bad = (entry: (typeof REGISTRY)[number]) => {
         const type = entry.type;
         if (type instanceof BooleanType) return { raw: "enabled", json: 1 };
         if (type instanceof IntegerType) return { raw: "many", json: "many" };
         if (type instanceof NumberType) return { raw: "half", json: "half" };
         if (type instanceof EnumType) return { raw: "nope", json: "nope" };
         if (type instanceof ListType) return { raw: undefined, json: 5 };
         return { raw: undefined, json: 5 };
      };
      for (const entry of named) {
         const key = entry.key as SettingKey;
         const { raw, json } = bad(entry);
         if (raw !== undefined) {
            const name = derivedEnvName(entry.key);
            withSettings({ env: { [name]: raw } }, () => {
               expect(() => settings.get(key), name).toThrow(
                  `${key} from ${name}`,
               );
            });
            if (
               !entry.secret &&
               !entry.noFlag &&
               !(entry.type instanceof BooleanType)
            ) {
               const flag = derivedFlagName(entry.key);
               withSettings({ argv: [flag, raw] }, () => {
                  expect(() => settings.get(key), flag).toThrow(
                     `${key} from ${flag}`,
                  );
               });
            }
         }
         if (entry.kind === "launch" || entry.secret) continue;
         const file: Record<string, unknown> = {};
         let node = file;
         const parts = entry.key.split(".");
         for (const part of parts.slice(0, -1)) {
            node = (node[part] ??= {}) as Record<string, unknown>;
         }
         node[parts[parts.length - 1]] = json;
         withSettings({ file }, () => {
            expect(() => settings.get(key), `${entry.key} in the file`).toThrow(
               entry.key,
            );
         });
      }
   });

   it("fails a default that reads a setting that failed, and reports the failure once", () => {
      withSettings({ file: { host: 5 } }, () => {
         expect(() => settings.get("mcp.host")).toThrow("host from ");
         expect(
            Settings.current()
               .errors()
               .map((error) => error.message),
         ).toEqual([
            expect.stringContaining("host from ") as unknown as string,
         ]);
      });
   });

   it("has one precedence for every setting: flag, then variable, then file", () => {
      withSettings(
         {
            env: { PUBLISHER_MCP_PORT: "4141" },
            argv: ["--mcp-port", "4242"],
            file: { mcp: { port: 4343 } },
         },
         () => expect(settings.get("mcp.port")).toBe(4242),
      );
      withSettings(
         { env: { PUBLISHER_MCP_PORT: "4141" }, file: { mcp: { port: 4343 } } },
         () => expect(settings.get("mcp.port")).toBe(4141),
      );
   });

   it("keeps the short flags the design keeps", () => {
      withSettings({ argv: ["--init", "--watch-env", "e"] }, () => {
         expect(settings.get("initializeStorage")).toBe(true);
         expect(settings.get("watchEnvironments")).toEqual(["e"]);
         expect(Settings.current().errors()).toEqual([]);
      });
   });

   it("fails a literal secret in the file, and reads one given as a ${VAR}", () => {
      withSettings({ file: { authorize: { bypassSecret: "s3cret" } } }, () => {
         expect(() => settings.get("authorize.bypassSecret")).toThrow(
            "a secret in the file must be one ${VAR} reference and nothing else, not the value itself or part of it",
         );
      });
      // A literal around a reference is still a literal in the file, even
      // when the variable is empty.
      withSettings(
         {
            env: { X: "" },
            file: { retrieval: { llm: { apiKey: "sk-live-abc${X}" } } },
         },
         () =>
            expect(() => settings.get("retrieval.llm.apiKey")).toThrow(
               "must be one ${VAR} reference and nothing else",
            ),
      );
      withSettings(
         {
            env: { BYPASS: "s3cret" },
            file: { authorize: { bypassSecret: "${BYPASS}" } },
         },
         () => expect(settings.get("authorize.bypassSecret")).toBe("s3cret"),
      );
   });

   it("fails an unset ${VAR} in the file only when no flag or variable gives the setting", () => {
      const file = { retrieval: { llm: { apiKey: "${NOPE}" } } };
      withSettings(
         { env: { PUBLISHER_RETRIEVAL_LLM_API_KEY: "sk-env" }, file },
         () => {
            expect(settings.get("retrieval.llm.apiKey")).toBe("sk-env");
            expect(Settings.current().errors()).toEqual([]);
         },
      );
      withSettings({ file }, () =>
         expect(() => settings.get("retrieval.llm.apiKey")).toThrow(
            "${NOPE} is not set",
         ),
      );
   });

   it("names the Kubernetes Service behind a tcp:// port variable", () => {
      withSettings({ env: { PUBLISHER_MCP_PORT: "tcp://10.0.0.1:4040" } }, () =>
         expect(() => settings.get("mcp.port")).toThrow(
            'mcp.port from PUBLISHER_MCP_PORT: expected an integer from 0 to 65535, got "tcp://10.0.0.1:4040". That is the variable Kubernetes sets for a Service named "publisher-mcp" in the pod\'s namespace: set enableServiceLinks: false in the pod spec, or rename the Service.',
         ),
      );
      withSettings({ env: { PUBLISHER_MCP_PORT: "40a0" } }, () => {
         let message = "";
         try {
            settings.get("mcp.port");
         } catch (error) {
            message = (error as Error).message;
         }
         expect(message).not.toContain("Kubernetes");
      });
   });

   it("refuses a value after a boolean flag, and names the flag that turns it off", () => {
      const loaded = Settings.load({
         argv: ["--allow-proxy-connections", "false"],
         env: {},
         cwd: "/nonexistent",
      });
      expect(loaded.errors().map((error) => error.message)).toEqual([
         '--allow-proxy-connections takes no value, so it cannot be given "false": --allow-proxy-connections alone turns allow.proxyConnections on. To turn it off, write --no-allow-proxy-connections.',
      ]);
      const plain = Settings.load({
         argv: ["--allow-proxy-connections", "--port", "4000"],
         env: {},
         cwd: "/nonexistent",
      });
      expect(plain.errors()).toEqual([]);
      expect(plain.get("allow.proxyConnections")).toBe(true);
   });
});
