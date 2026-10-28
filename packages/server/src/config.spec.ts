// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs";
import path from "path";
import {
   convertConnectionsToApiConnections,
   DEFAULT_EMBEDDING_MIN_SIMILARITY,
   getDefaultQueryRowLimit,
   getEmbeddingConfig,
   getMaxConcurrentQueries,
   getMaxQueryRows,
   getMaxResponseBytes,
   getMemoryGovernorConfig,
   getPersistCollisionEnforce,
   getPostgresPoolMax,
   getProcessedPublisherConfig,
   getPublisherConfig,
   getPublisherConfigDir,
   getQueryTimeoutMs,
   isPublisherConfigFrozen,
   type PublisherConfig,
} from "./config";
import {
   DEFAULT_MAX_CONCURRENT_QUERIES,
   DEFAULT_MAX_QUERY_ROWS,
   DEFAULT_MAX_RESPONSE_BYTES,
   DEFAULT_POSTGRES_POOL_MAX,
   DEFAULT_QUERY_ROW_LIMIT,
   DEFAULT_QUERY_TIMEOUT_MS,
   PUBLISHER_CONFIG_NAME,
} from "./constants";
import { withSettings } from "./test_helpers/settings_env";

describe("Config Environment Variable Substitution", () => {
   const testServerRoot = path.join(process.cwd(), "test-temp-config");
   const configPath = path.join(testServerRoot, PUBLISHER_CONFIG_NAME);

   beforeEach(() => {
      // Create test directory
      if (!fs.existsSync(testServerRoot)) {
         fs.mkdirSync(testServerRoot, { recursive: true });
      }
   });

   afterEach(() => {
      // Clean up test files and environment variables
      if (fs.existsSync(configPath)) {
         fs.unlinkSync(configPath);
      }
      if (fs.existsSync(testServerRoot)) {
         fs.rmdirSync(testServerRoot, { recursive: true });
      }

      // Clean up all test environment variables
      const testEnvVars = [
         "TEST_VAR",
         "BUCKET_NAME",
         "DB_HOST",
         "DB_PORT",
         "DB_NAME",
         "API_KEY",
         "API_HOST",
         "KEY_NAME",
         "VALUE_VAR",
         "EMPTY_VAR",
         "BUCKET_1",
         "BUCKET_2",
         "NORMAL_VALUE",
         "GCS_BUCKET",
         "PROJECT_ID",
         "CONNECTION_STRING",
         "PACKAGE_NAME",
         "DEFINED_VAR",
         "DEV_BUCKET",
         "PROD_BUCKET",
         "DB_CONNECTION",
         "ENV",
         "DATA_BUCKET",
         "TAG1",
         "TAG2",
         "CONFIG_VALUE",
      ];

      testEnvVars.forEach((varName) => {
         delete process.env[varName];
      });
   });

   describe("Scenario 1: ${VAR} present in config but value not available", () => {
      it("should throw error when environment variable is not defined", () => {
         // The correct behavior: throw an error when a required variable is missing
         const locationWithVar = "./path/${UNDEFINED_VAR}/end" as const;

         const config: PublisherConfig = {
            environments: [
               {
                  name: "test-project",
                  packages: [
                     {
                        name: "test-package",
                        location: locationWithVar,
                     },
                  ],
               },
            ],
         };

         fs.writeFileSync(configPath, JSON.stringify(config, null, 2));

         // Should throw an error about the missing environment variable
         expect(() => getPublisherConfig(testServerRoot)).toThrow(
            "Environment variable '${UNDEFINED_VAR}' is not set in configuration file",
         );
      });

      it("should throw error for undefined variables in gs:// URLs", () => {
         const locationWithVar = "gs://${BUCKET_NAME}/packages" as const;

         const config: PublisherConfig = {
            environments: [
               {
                  name: "test-project",
                  packages: [
                     {
                        name: "test-package",
                        location: locationWithVar,
                     },
                  ],
               },
            ],
         };

         fs.writeFileSync(configPath, JSON.stringify(config, null, 2));

         // Should throw an error about the missing environment variable
         expect(() => getPublisherConfig(testServerRoot)).toThrow(
            "Environment variable '${BUCKET_NAME}' is not set in configuration file",
         );
      });
   });

   describe("Scenario 2: ${VAR} present in config and value is available", () => {
      it("should substitute environment variable in package location with gs:// path", () => {
         process.env.BUCKET_NAME = "my-test-bucket";

         const config: PublisherConfig = {
            environments: [
               {
                  name: "test-project",
                  packages: [
                     {
                        name: "test-package",
                        location: "gs://${BUCKET_NAME}/packages",
                     },
                  ],
               },
            ],
         };

         fs.writeFileSync(configPath, JSON.stringify(config, null, 2));

         const result = getPublisherConfig(testServerRoot);

         expect(result.environments[0].packages[0].location).toBe(
            "gs://my-test-bucket/packages",
         );
      });

      it("should substitute environment variable in filesystem path", () => {
         process.env.PROJECT_ID = "analytics-2024";

         const config: PublisherConfig = {
            environments: [
               {
                  name: "test-project",
                  packages: [
                     {
                        name: "test-package",
                        location: "./environments/${PROJECT_ID}/models",
                     },
                  ],
               },
            ],
         };

         fs.writeFileSync(configPath, JSON.stringify(config, null, 2));

         const result = getPublisherConfig(testServerRoot);

         expect(result.environments[0].packages[0].location).toBe(
            "./environments/analytics-2024/models",
         );
      });

      it("should substitute multiple environment variables in single value", () => {
         process.env.BUCKET_NAME = "data-warehouse";
         process.env.PROJECT_ID = "prod-analytics";

         const config: PublisherConfig = {
            environments: [
               {
                  name: "test-project",
                  packages: [
                     {
                        name: "test-package",
                        location: "gs://${BUCKET_NAME}/${PROJECT_ID}/models",
                     },
                  ],
               },
            ],
         };

         fs.writeFileSync(configPath, JSON.stringify(config, null, 2));

         const result = getPublisherConfig(testServerRoot);

         expect(result.environments[0].packages[0].location).toBe(
            "gs://data-warehouse/prod-analytics/models",
         );
      });

      it("should substitute variables across multiple packages", () => {
         process.env.BUCKET_1 = "bucket-one";
         process.env.BUCKET_2 = "bucket-two";

         const config: PublisherConfig = {
            environments: [
               {
                  name: "test-project",
                  packages: [
                     {
                        name: "package-1",
                        location: "gs://${BUCKET_1}/path",
                     },
                     {
                        name: "package-2",
                        location: "gs://${BUCKET_2}/path",
                     },
                  ],
               },
            ],
         };

         fs.writeFileSync(configPath, JSON.stringify(config, null, 2));

         const result = getPublisherConfig(testServerRoot);

         expect(result.environments[0].packages[0].location).toBe(
            "gs://bucket-one/path",
         );
         expect(result.environments[0].packages[1].location).toBe(
            "gs://bucket-two/path",
         );
      });

      it("should substitute variables in connection names", () => {
         process.env.DB_HOST = "localhost";

         const config: PublisherConfig = {
            environments: [
               {
                  name: "test-project",
                  packages: [],
                  connections: [
                     {
                        name: "db-${DB_HOST}",
                        type: "postgres",
                     },
                  ],
               },
            ],
         };

         fs.writeFileSync(configPath, JSON.stringify(config, null, 2));

         const result = getPublisherConfig(testServerRoot);

         expect(result.environments[0].connections?.[0].name).toBe(
            "db-localhost",
         );
      });

      it("should substitute variables in nested configuration objects", () => {
         process.env.API_KEY = "secret-key-123";
         process.env.API_HOST = "api.example.com";

         const config = {
            frozenConfig: false,
            environments: [
               {
                  name: "test-project",
                  packages: [],
                  settings: {
                     apiEndpoint: "https://${API_HOST}/v1",
                     credentials: {
                        apiKey: "${API_KEY}",
                     },
                  },
               },
            ],
         };

         fs.writeFileSync(configPath, JSON.stringify(config, null, 2));

         const result = getPublisherConfig(testServerRoot);
         const projectWithSettings = result
            .environments[0] as (typeof result.environments)[0] & {
            settings: {
               apiEndpoint: string;
               credentials: {
                  apiKey: string;
               };
            };
         };

         expect(projectWithSettings.settings.apiEndpoint).toBe(
            "https://api.example.com/v1",
         );
         expect(projectWithSettings.settings.credentials.apiKey).toBe(
            "secret-key-123",
         );
      });

      it("should handle mixed substitution with literal text", () => {
         process.env.PROJECT_ID = "my-project";

         const config: PublisherConfig = {
            environments: [
               {
                  name: "test-project",
                  packages: [
                     {
                        name: "test-package",
                        location:
                           "gs://bucket/prefix-${PROJECT_ID}-suffix/models",
                     },
                  ],
               },
            ],
         };

         fs.writeFileSync(configPath, JSON.stringify(config, null, 2));

         const result = getPublisherConfig(testServerRoot);

         expect(result.environments[0].packages[0].location).toBe(
            "gs://bucket/prefix-my-project-suffix/models",
         );
      });

      it("should substitute variables across multiple environments", () => {
         process.env.DEV_BUCKET = "dev-data";
         process.env.PROD_BUCKET = "prod-data";

         const config: PublisherConfig = {
            environments: [
               {
                  name: "development",
                  packages: [
                     {
                        name: "dev-package",
                        location: "gs://${DEV_BUCKET}/models",
                     },
                  ],
               },
               {
                  name: "production",
                  packages: [
                     {
                        name: "prod-package",
                        location: "gs://${PROD_BUCKET}/models",
                     },
                  ],
               },
            ],
         };

         fs.writeFileSync(configPath, JSON.stringify(config, null, 2));

         const result = getPublisherConfig(testServerRoot);

         expect(result.environments[0].packages[0].location).toBe(
            "gs://dev-data/models",
         );
         expect(result.environments[1].packages[0].location).toBe(
            "gs://prod-data/models",
         );

         delete process.env.DEV_BUCKET;
         delete process.env.PROD_BUCKET;
      });
   });

   describe("Scenario 3: ${VAR} present in config as a key (not as a value)", () => {
      it("should NOT substitute environment variables in object keys", () => {
         process.env.KEY_NAME = "myKey";

         const config = {
            frozenConfig: false,
            environments: [
               {
                  name: "test-project",
                  packages: [],
                  "${KEY_NAME}": "some-value",
               },
            ],
         };

         fs.writeFileSync(configPath, JSON.stringify(config, null, 2));

         const result = getPublisherConfig(testServerRoot);

         // The key should remain as-is (not substituted)
         expect(result.environments[0]).toHaveProperty("${KEY_NAME}");
         expect(
            (result.environments[0] as Record<string, unknown>)["${KEY_NAME}"],
         ).toBe("some-value");
      });

      it("should preserve keys with variable syntax while substituting values", () => {
         process.env.NORMAL_VALUE = "substituted-value";

         const config = {
            frozenConfig: false,
            environments: [
               {
                  name: "test-project",
                  packages: [],
                  "${DYNAMIC_KEY}": "value1",
                  normal_key: "${NORMAL_VALUE}",
               },
            ],
         };

         fs.writeFileSync(configPath, JSON.stringify(config, null, 2));

         const result = getPublisherConfig(testServerRoot);

         // Key should not be substituted
         expect(result.environments[0]).toHaveProperty("${DYNAMIC_KEY}");
         expect(
            (result.environments[0] as Record<string, unknown>)[
               "${DYNAMIC_KEY}"
            ],
         ).toBe("value1");

         // Value should be substituted
         expect(
            (result.environments[0] as Record<string, unknown>)["normal_key"],
         ).toBe("substituted-value");
      });

      it("should handle mixed scenario: variable in key and different variable in value", () => {
         process.env.VALUE_VAR = "actual-value";

         const config = {
            frozenConfig: false,
            environments: [
               {
                  name: "test-project",
                  packages: [],
                  "${KEY_VAR}": "${VALUE_VAR}",
               },
            ],
         };

         fs.writeFileSync(configPath, JSON.stringify(config, null, 2));

         const result = getPublisherConfig(testServerRoot);

         // Key remains with variable syntax
         expect(result.environments[0]).toHaveProperty("${KEY_VAR}");

         // Value is substituted
         expect(
            (result.environments[0] as Record<string, unknown>)["${KEY_VAR}"],
         ).toBe("actual-value");
      });

      it("should substitute variables in package names since they are values", () => {
         process.env.PACKAGE_NAME = "my-package";
         process.env.BUCKET_NAME = "my-bucket";

         const config: PublisherConfig = {
            environments: [
               {
                  name: "test-project",
                  packages: [
                     {
                        name: "${PACKAGE_NAME}",
                        location: "gs://${BUCKET_NAME}/models",
                     },
                  ],
               },
            ],
         };

         fs.writeFileSync(configPath, JSON.stringify(config, null, 2));

         const result = getPublisherConfig(testServerRoot);

         // Package name is a property VALUE, so it WILL be substituted
         expect(result.environments[0].packages[0].name).toBe("my-package");

         // Location should also be substituted
         expect(result.environments[0].packages[0].location).toBe(
            "gs://my-bucket/models",
         );

         delete process.env.PACKAGE_NAME;
      });

      it("should handle nested objects with variable syntax in keys", () => {
         process.env.CONFIG_VALUE = "test-value";

         const config = {
            frozenConfig: false,
            environments: [
               {
                  name: "test-project",
                  packages: [],
                  metadata: {
                     "${DYNAMIC_PROP}": {
                        setting: "${CONFIG_VALUE}",
                     },
                  },
               },
            ],
         };

         fs.writeFileSync(configPath, JSON.stringify(config, null, 2));

         const result = getPublisherConfig(testServerRoot);
         const projectWithMetadata = result
            .environments[0] as (typeof result.environments)[0] & {
            metadata: Record<string, { setting: string }>;
         };

         // Key should remain unchanged
         expect(projectWithMetadata.metadata).toHaveProperty("${DYNAMIC_PROP}");

         // Nested value should be substituted
         expect(projectWithMetadata.metadata["${DYNAMIC_PROP}"].setting).toBe(
            "test-value",
         );

         delete process.env.CONFIG_VALUE;
      });
   });

   describe("Edge cases and special scenarios", () => {
      it("should handle empty string environment variable", () => {
         process.env.EMPTY_VAR = "";

         const config: PublisherConfig = {
            environments: [
               {
                  name: "test-project",
                  packages: [
                     {
                        name: "test-package",
                        location: "gs://bucket/${EMPTY_VAR}/path",
                     },
                  ],
               },
            ],
         };

         fs.writeFileSync(configPath, JSON.stringify(config, null, 2));

         const result = getPublisherConfig(testServerRoot);

         expect(result.environments[0].packages[0].location).toBe(
            "gs://bucket//path",
         );
      });

      it("should handle non-string values without modification", () => {
         const config = {
            frozenConfig: true,
            environments: [
               {
                  name: "test-project",
                  packages: [],
               },
            ],
         };

         // Add non-standard properties for testing
         const configWithExtras = {
            ...config,
            environments: [
               {
                  ...config.environments[0],
                  count: 42,
                  enabled: true,
                  ratio: 3.14,
                  metadata: null,
                  tags: ["tag1", "tag2"],
               },
            ],
         };

         fs.writeFileSync(
            configPath,
            JSON.stringify(configWithExtras, null, 2),
         );

         const result = getPublisherConfig(testServerRoot);
         const projectWithExtras = result
            .environments[0] as (typeof result.environments)[0] & {
            count: number;
            enabled: boolean;
            ratio: number;
            metadata: null;
            tags: string[];
         };

         // frozenConfig is a setting, which the settings read from the file.
         withSettings({ cwd: testServerRoot }, () => {
            expect(isPublisherConfigFrozen()).toBe(true);
         });
         expect(projectWithExtras.count).toBe(42);
         expect(projectWithExtras.enabled).toBe(true);
         expect(projectWithExtras.ratio).toBe(3.14);
         expect(projectWithExtras.metadata).toBe(null);
         expect(projectWithExtras.tags).toEqual(["tag1", "tag2"]);
      });

      it("should handle config with no environment variables", () => {
         const config: PublisherConfig = {
            environments: [
               {
                  name: "test-project",
                  packages: [
                     {
                        name: "test-package",
                        location: "./packages/test",
                     },
                  ],
               },
            ],
         };

         fs.writeFileSync(configPath, JSON.stringify(config, null, 2));

         const result = getPublisherConfig(testServerRoot);

         expect(result).toEqual(config);
      });

      it("should handle empty environments array", () => {
         const config: PublisherConfig = {
            environments: [],
         };

         fs.writeFileSync(configPath, JSON.stringify(config, null, 2));

         const result = getPublisherConfig(testServerRoot);

         expect(result.environments).toEqual([]);
      });

      it("should return default config when file does not exist", () => {
         // Don't create config file
         const result = getPublisherConfig(testServerRoot);

         expect(result).toEqual({
            environments: [],
         });
         // With no file, the frozenConfig setting is at its default.
         withSettings({ cwd: testServerRoot }, () => {
            expect(isPublisherConfigFrozen()).toBe(false);
         });
      });

      it("should handle whitespace around variable names", () => {
         process.env.BUCKET_NAME = "test-bucket";

         const config: PublisherConfig = {
            environments: [
               {
                  name: "test-project",
                  packages: [
                     {
                        name: "test-package",
                        location: "gs://${ BUCKET_NAME }/path",
                     },
                  ],
               },
            ],
         };

         fs.writeFileSync(configPath, JSON.stringify(config, null, 2));

         const result = getPublisherConfig(testServerRoot);

         // Whitespace around variable name means it won't match the pattern
         // Due to bug, the unmatched variable causes duplication
         expect(result.environments[0].packages[0].location).toBe(
            "gs://${ BUCKET_NAME }/path",
         );
      });

      it("should handle multiple environments with mixed variable usage", () => {
         process.env.PROD_BUCKET = "production-data";

         const config: PublisherConfig = {
            environments: [
               {
                  name: "development",
                  packages: [
                     {
                        name: "dev-package",
                        location: "./packages/dev",
                     },
                  ],
               },
               {
                  name: "production",
                  packages: [
                     {
                        name: "prod-package",
                        location: "gs://${PROD_BUCKET}/models",
                     },
                  ],
               },
            ],
         };

         fs.writeFileSync(configPath, JSON.stringify(config, null, 2));

         const result = getPublisherConfig(testServerRoot);

         expect(result.environments[0].packages[0].location).toBe(
            "./packages/dev",
         );
         expect(result.environments[1].packages[0].location).toBe(
            "gs://production-data/models",
         );

         delete process.env.PROD_BUCKET;
      });

      it("should preserve variable syntax if not matching pattern", () => {
         const config: PublisherConfig = {
            environments: [
               {
                  name: "test-project",
                  packages: [
                     {
                        name: "test-package",
                        location: "gs://bucket/${lowercase_var}/path",
                     },
                  ],
               },
            ],
         };

         fs.writeFileSync(configPath, JSON.stringify(config, null, 2));

         const result = getPublisherConfig(testServerRoot);

         // Variable won't match pattern, so won't be substituted
         expect(result.environments[0].packages[0].location).toBe(
            "gs://bucket/${lowercase_var}/path",
         );
      });

      it("should handle arrays of strings with variables", () => {
         process.env.TAG1 = "analytics";
         process.env.TAG2 = "production";

         const config = {
            frozenConfig: false,
            environments: [
               {
                  name: "test-project",
                  packages: [],
                  tags: ["${TAG1}", "${TAG2}", "static-tag"],
               },
            ],
         };

         fs.writeFileSync(configPath, JSON.stringify(config, null, 2));

         const result = getPublisherConfig(testServerRoot);
         const projectWithTags = result
            .environments[0] as (typeof result.environments)[0] & {
            tags: string[];
         };

         expect(projectWithTags.tags).toEqual([
            "analytics",
            "production",
            "static-tag",
         ]);

         delete process.env.TAG1;
         delete process.env.TAG2;
      });
   });

   describe("Real-world configuration scenarios", () => {
      it("should handle typical multi-environment setup", () => {
         process.env.ENV = "staging";
         process.env.DATA_BUCKET = "company-data-staging";
         process.env.PROJECT_ID = "company-project-staging";

         const config: PublisherConfig = {
            environments: [
               {
                  name: "${ENV}",
                  packages: [
                     {
                        name: "analytics",
                        location: "gs://${DATA_BUCKET}/${PROJECT_ID}/analytics",
                     },
                     {
                        name: "reporting",
                        location: "gs://${DATA_BUCKET}/${PROJECT_ID}/reporting",
                     },
                  ],
               },
            ],
         };

         fs.writeFileSync(configPath, JSON.stringify(config, null, 2));

         const result = getPublisherConfig(testServerRoot);

         expect(result.environments[0].name).toBe("staging");
         expect(result.environments[0].packages[0].location).toBe(
            "gs://company-data-staging/company-project-staging/analytics",
         );
         expect(result.environments[0].packages[1].location).toBe(
            "gs://company-data-staging/company-project-staging/reporting",
         );

         delete process.env.ENV;
         delete process.env.DATA_BUCKET;
      });

      it("should handle connection configurations with environment variables", () => {
         process.env.DB_CONNECTION = "production-db";

         const config: PublisherConfig = {
            environments: [
               {
                  name: "production",
                  packages: [],
                  connections: [
                     {
                        name: "${DB_CONNECTION}",
                        type: "bigquery",
                     },
                  ],
               },
            ],
         };

         fs.writeFileSync(configPath, JSON.stringify(config, null, 2));

         const result = getPublisherConfig(testServerRoot);

         expect(result.environments[0].connections?.[0].name).toBe(
            "production-db",
         );

         delete process.env.DB_CONNECTION;
      });
   });
});

// TODO: Remove this during projects cleanup
describe("Config legacy 'projects' key back-compat", () => {
   const testServerRoot = path.join(process.cwd(), "test-temp-legacy-config");
   const configPath = path.join(testServerRoot, PUBLISHER_CONFIG_NAME);

   beforeEach(() => {
      if (!fs.existsSync(testServerRoot)) {
         fs.mkdirSync(testServerRoot, { recursive: true });
      }
   });

   afterEach(() => {
      if (fs.existsSync(configPath)) {
         fs.unlinkSync(configPath);
      }
      if (fs.existsSync(testServerRoot)) {
         fs.rmdirSync(testServerRoot, { recursive: true });
      }
   });

   it("reads from legacy 'projects' key when 'environments' is absent", async () => {
      // Pre-rename on-disk shape: top-level key is `projects`, not
      // `environments`. Without back-compat this silently parses as empty.
      const legacyConfig = {
         frozenConfig: false,
         projects: [
            {
               name: "legacy-env",
               packages: [
                  {
                     name: "p1",
                     location: "./packages/p1",
                  },
               ],
            },
         ],
      };

      fs.writeFileSync(configPath, JSON.stringify(legacyConfig, null, 2));

      // Spy on logger.warn so we can assert the deprecation message fired.
      const { logger } = await import("./logger");
      const originalWarn = logger.warn;
      const warnings: string[] = [];
      logger.warn = ((msg: unknown, ..._rest: unknown[]) => {
         warnings.push(typeof msg === "string" ? msg : String(msg));
         return logger;
      }) as typeof logger.warn;

      try {
         const result = getPublisherConfig(testServerRoot);

         expect(result.environments.length).toBe(1);
         expect(result.environments[0].name).toBe("legacy-env");
         expect(result.environments[0].packages[0].name).toBe("p1");

         expect(
            warnings.some((w) => w.includes('uses deprecated "projects" key')),
         ).toBe(true);
      } finally {
         logger.warn = originalWarn;
      }
   });

   it("prefers the new 'environments' key when both are present", async () => {
      const config = {
         frozenConfig: false,
         environments: [
            {
               name: "new-env",
               packages: [{ name: "p1", location: "./packages/p1" }],
            },
         ],
         projects: [
            {
               name: "should-be-ignored",
               packages: [{ name: "p2", location: "./packages/p2" }],
            },
         ],
      };

      fs.writeFileSync(configPath, JSON.stringify(config, null, 2));

      const { logger } = await import("./logger");
      const originalWarn = logger.warn;
      const warnings: string[] = [];
      logger.warn = ((msg: unknown, ..._rest: unknown[]) => {
         warnings.push(typeof msg === "string" ? msg : String(msg));
         return logger;
      }) as typeof logger.warn;

      try {
         const result = getPublisherConfig(testServerRoot);

         expect(result.environments.length).toBe(1);
         expect(result.environments[0].name).toBe("new-env");

         // No deprecation warning should fire when `environments` is present.
         expect(
            warnings.some((w) => w.includes('uses deprecated "projects" key')),
         ).toBe(false);
      } finally {
         logger.warn = originalWarn;
      }
   });
});

describe("getProcessedPublisherConfig credential logging", () => {
   const testServerRoot = path.join(
      process.cwd(),
      "test-temp-config-log-redaction",
   );
   const configPath = path.join(testServerRoot, PUBLISHER_CONFIG_NAME);

   beforeEach(() => {
      if (!fs.existsSync(testServerRoot)) {
         fs.mkdirSync(testServerRoot, { recursive: true });
      }
   });

   afterEach(() => {
      if (fs.existsSync(configPath)) {
         fs.unlinkSync(configPath);
      }
      if (fs.existsSync(testServerRoot)) {
         fs.rmdirSync(testServerRoot, { recursive: true });
      }
      delete process.env.TEST_LOG_REDACTION_PASSWORD;
   });

   it("keeps connection credentials out of the log when an environment is missing its name", async () => {
      // The entry is skipped for a missing `name`, but it still carries every
      // connection and storage destination, and `${VAR}` references are already
      // substituted by the time the skip is logged. Nothing downstream saves it:
      // redactSensitive is a call-site helper, not a winston format.
      const secret = "pg-password-that-must-not-be-logged";
      process.env.TEST_LOG_REDACTION_PASSWORD = secret;

      const config = {
         frozenConfig: false,
         environments: [
            {
               // `name` deliberately absent: this is the path under test.
               packages: [{ name: "p1", location: "./packages/p1" }],
               connections: [
                  {
                     name: "pg",
                     type: "postgres",
                     postgresConnection: {
                        password: "${TEST_LOG_REDACTION_PASSWORD}",
                     },
                  },
               ],
            },
         ],
      };

      fs.writeFileSync(configPath, JSON.stringify(config, null, 2));

      const { logger } = await import("./logger");
      const originalWarn = logger.warn;
      const calls: unknown[][] = [];
      logger.warn = ((...args: unknown[]) => {
         calls.push(args);
         return logger;
      }) as typeof logger.warn;

      try {
         const result = getProcessedPublisherConfig(testServerRoot);

         expect(result.environments.length).toBe(0);

         const skipWarning = calls.find(
            (args) =>
               typeof args[0] === "string" &&
               args[0].includes('missing or invalid "name" field'),
         );
         expect(skipWarning).toBeDefined();

         // Asserts on the whole payload rather than on the absence of one key,
         // so re-introducing the config under a different key still fails here.
         expect(JSON.stringify(skipWarning)).not.toContain(secret);
      } finally {
         logger.warn = originalWarn;
      }
   });

   it("keeps connection credentials out of the log when a connection is missing its name", async () => {
      // Sibling of the environment case above: a connection skipped for a
      // missing `name` reaches the warning with its `${VAR}` references already
      // substituted, so logging the entry logs the credential.
      const secret = "conn-password-that-must-not-be-logged";

      const { logger } = await import("./logger");
      const originalWarn = logger.warn;
      const calls: unknown[][] = [];
      logger.warn = ((...args: unknown[]) => {
         calls.push(args);
         return logger;
      }) as typeof logger.warn;

      try {
         const result = convertConnectionsToApiConnections([
            {
               // `name` deliberately absent: this is the path under test.
               type: "postgres",
               postgresConnection: { password: secret },
            },
         ] as unknown as Parameters<
            typeof convertConnectionsToApiConnections
         >[0]);

         expect(result.length).toBe(0);

         const skipWarning = calls.find(
            (args) =>
               typeof args[0] === "string" &&
               args[0].includes('missing or invalid "name" field'),
         );
         expect(skipWarning).toBeDefined();

         // Whole-payload assertion, for the same reason as the environment case.
         expect(JSON.stringify(skipWarning)).not.toContain(secret);
         // The type still identifies the offending entry, since the name is
         // exactly what is missing.
         expect(JSON.stringify(skipWarning)).toContain("postgres");
      } finally {
         logger.warn = originalWarn;
      }
   });
});

describe("Committed example configs", () => {
   const serverDir = path.resolve(__dirname, "..");

   it.each([
      ["publisher.config.json", false],
      ["publisher.config.example.duckdb.json", false],
      ["publisher.config.example.bigquery.json", true],
   ])(
      "%s parses as a valid PublisherConfig",
      (filename, expectsBigQueryConnection) => {
         const filePath = path.join(serverDir, filename);
         expect(fs.existsSync(filePath)).toBe(true);

         const raw = fs.readFileSync(filePath, "utf-8");
         const parsed = JSON.parse(raw) as PublisherConfig;

         expect(parsed).toHaveProperty("environments");
         expect(Array.isArray(parsed.environments)).toBe(true);
         expect(parsed.environments.length).toBeGreaterThan(0);

         const env = parsed.environments[0];
         expect(env.name).toBeTruthy();
         expect(Array.isArray(env.packages)).toBe(true);
         expect(env.packages.length).toBeGreaterThan(0);

         if (expectsBigQueryConnection) {
            expect(
               (env.connections ?? []).some((c) => c.type === "bigquery"),
            ).toBe(true);
            expect(
               env.packages.some((p) => p.name === "bigquery-hackernews"),
            ).toBe(true);
         } else {
            expect(
               env.packages.some((p) => p.name === "bigquery-hackernews"),
            ).toBe(false);
         }
      },
   );
});

describe("Config path resolution (--config and bundled default)", () => {
   const emptyServerRoot = path.join(process.cwd(), "test-empty-server-root");
   const customConfigPath = path.join(
      process.cwd(),
      "test-custom-publisher.config.json",
   );

   beforeEach(() => {
      if (!fs.existsSync(emptyServerRoot)) {
         fs.mkdirSync(emptyServerRoot, { recursive: true });
      }
   });

   afterEach(() => {
      if (fs.existsSync(customConfigPath)) {
         fs.unlinkSync(customConfigPath);
      }
      if (fs.existsSync(emptyServerRoot)) {
         fs.rmSync(emptyServerRoot, { recursive: true, force: true });
      }
   });

   it("falls back to the bundled default config when serverRoot has no publisher.config.json and bundled-default opt-in is set", () => {
      // The bundled default is opt-in (useBundledDefault is on by default
      // only when no flag names a root or config, and never under
      // NODE_ENV=test) so embedded callers don't get a surprise filesystem
      // read.
      const result = withSettings(
         { env: { PUBLISHER_USE_BUNDLED_DEFAULT: "true" } },
         () => getPublisherConfig(emptyServerRoot),
      );

      expect(result.environments.length).toBeGreaterThan(0);
      const env = result.environments[0];
      expect(env.name).toBe("examples");
      expect(env.packages.some((p) => p.name === "storefront")).toBe(true);
      expect(env.packages.some((p) => p.name === "bigquery-hackernews")).toBe(
         false,
      );
   });

   it("does NOT fall back to the bundled default when opt-in flag is unset (programmatic construction)", () => {
      // No PUBLISHER_USE_BUNDLED_DEFAULT, no PUBLISHER_CONFIG_PATH, no
      // file in emptyServerRoot — result should be the original empty
      // shape, preserving prior behavior for embeds and tests.
      const result = withSettings({}, () =>
         getPublisherConfig(emptyServerRoot),
      );
      expect(result.environments).toEqual([]);
   });

   it("honors PUBLISHER_CONFIG_PATH (set by --config) over the server root", () => {
      const customConfig = {
         frozenConfig: false,
         environments: [
            { name: "custom-env", packages: [{ name: "foo", location: "/x" }] },
         ],
      };
      fs.writeFileSync(customConfigPath, JSON.stringify(customConfig));

      const result = withSettings(
         { env: { PUBLISHER_CONFIG_PATH: customConfigPath } },
         () => getPublisherConfig(emptyServerRoot),
      );
      expect(result.environments[0].name).toBe("custom-env");
   });

   it("returns empty environments (not the bundled default) when --config points at a missing file", () => {
      // Even with bundled-default opt-in, --config with a missing target
      // is a user error and we don't paper over it.
      const result = withSettings(
         {
            env: {
               PUBLISHER_CONFIG_PATH: path.join(
                  process.cwd(),
                  "does-not-exist.json",
               ),
               PUBLISHER_USE_BUNDLED_DEFAULT: "true",
            },
         },
         () => getPublisherConfig(emptyServerRoot),
      );
      expect(result.environments).toEqual([]);
   });
});

describe("getMemoryGovernorConfig", () => {
   /** The governor's settings with exactly `env` as the variables. */
   const governor = (env: Record<string, string>) =>
      withSettings({ env }, () => getMemoryGovernorConfig());

   it("returns null when PUBLISHER_MAX_MEMORY_BYTES is unset", () => {
      expect(governor({})).toBeNull();
   });

   it("parses defaults when only PUBLISHER_MAX_MEMORY_BYTES is set", () => {
      const cfg = governor({
         PUBLISHER_MAX_MEMORY_BYTES: String(2 * 1024 * 1024 * 1024),
      });
      expect(cfg).not.toBeNull();
      expect(cfg!.maxMemoryBytes).toBe(2 * 1024 * 1024 * 1024);
      expect(cfg!.backpressureEnabled).toBe(true);
      expect(cfg!.highWaterFraction).toBeGreaterThan(cfg!.lowWaterFraction);
   });

   it("honours fraction and interval overrides", () => {
      const cfg = governor({
         PUBLISHER_MAX_MEMORY_BYTES: "1000000000",
         PUBLISHER_MEMORY_HIGH_WATER_FRACTION: "0.85",
         PUBLISHER_MEMORY_LOW_WATER_FRACTION: "0.7",
         PUBLISHER_MEMORY_CHECK_INTERVAL_MS: "10000",
         PUBLISHER_MEMORY_BACKPRESSURE: "false",
      });
      expect(cfg).not.toBeNull();
      expect(cfg!.highWaterFraction).toBe(0.85);
      expect(cfg!.lowWaterFraction).toBe(0.7);
      expect(cfg!.checkIntervalMs).toBe(10000);
      expect(cfg!.backpressureEnabled).toBe(false);
   });

   it("treats PUBLISHER_MAX_MEMORY_BYTES=0 as disabled (returns null)", () => {
      expect(governor({ PUBLISHER_MAX_MEMORY_BYTES: "0" })).toBeNull();
   });

   it("rejects a negative PUBLISHER_MAX_MEMORY_BYTES", () => {
      expect(() => governor({ PUBLISHER_MAX_MEMORY_BYTES: "-1" })).toThrow();
   });

   it("rejects low >= high", () => {
      expect(() =>
         governor({
            PUBLISHER_MAX_MEMORY_BYTES: "1000000000",
            PUBLISHER_MEMORY_HIGH_WATER_FRACTION: "0.7",
            PUBLISHER_MEMORY_LOW_WATER_FRACTION: "0.8",
         }),
      ).toThrow();
   });

   it("rejects an out-of-range fraction", () => {
      expect(() =>
         governor({
            PUBLISHER_MAX_MEMORY_BYTES: "1000000000",
            PUBLISHER_MEMORY_HIGH_WATER_FRACTION: "1.5",
         }),
      ).toThrow();
   });

   it("rejects a check interval below the safety floor", () => {
      expect(() =>
         governor({
            PUBLISHER_MAX_MEMORY_BYTES: "1000000000",
            PUBLISHER_MEMORY_CHECK_INTERVAL_MS: "10",
         }),
      ).toThrow();
   });
});

/**
 * `read()` with `variable` set to `value`, or with nothing set when `value`
 * is undefined.
 */
const readWith = <T>(
   variable: string,
   value: string | undefined,
   read: () => T,
): T =>
   withSettings({ env: value === undefined ? {} : { [variable]: value } }, () =>
      read(),
   );

describe("getMaxQueryRows", () => {
   const rows = (value?: string) =>
      readWith("PUBLISHER_MAX_QUERY_ROWS", value, getMaxQueryRows);

   it("returns DEFAULT_MAX_QUERY_ROWS when the env var is unset", () => {
      expect(rows()).toBe(DEFAULT_MAX_QUERY_ROWS);
   });

   it("returns DEFAULT_MAX_QUERY_ROWS when the env var is empty", () => {
      expect(rows("")).toBe(DEFAULT_MAX_QUERY_ROWS);
   });

   it("returns the override when the env var is set", () => {
      expect(rows("5000")).toBe(5000);
   });

   it("accepts 0 (used to opt out of row cap)", () => {
      expect(rows("0")).toBe(0);
   });

   it("rejects a negative override", () => {
      expect(() => rows("-1")).toThrow();
   });

   it("rejects a non-integer override", () => {
      expect(() => rows("1.5")).toThrow();
   });

   it("rejects a non-numeric override", () => {
      expect(() => rows("lots")).toThrow();
   });
});

describe("getMaxResponseBytes", () => {
   const bytes = (value?: string) =>
      readWith("PUBLISHER_MAX_RESPONSE_BYTES", value, getMaxResponseBytes);

   it("returns DEFAULT_MAX_RESPONSE_BYTES when the env var is unset", () => {
      expect(bytes()).toBe(DEFAULT_MAX_RESPONSE_BYTES);
   });

   it("returns DEFAULT_MAX_RESPONSE_BYTES when the env var is empty", () => {
      expect(bytes("")).toBe(DEFAULT_MAX_RESPONSE_BYTES);
   });

   it("returns the override when the env var is set", () => {
      expect(bytes("1048576")).toBe(1048576);
   });

   it("accepts 0 (used to opt out of byte cap)", () => {
      expect(bytes("0")).toBe(0);
   });

   it("rejects a negative override", () => {
      expect(() => bytes("-1")).toThrow();
   });

   it("rejects a non-integer override", () => {
      expect(() => bytes("1.5")).toThrow();
   });

   it("rejects a non-numeric override", () => {
      expect(() => bytes("big")).toThrow();
   });
});

describe("getDefaultQueryRowLimit", () => {
   const limit = (value?: string) =>
      readWith(
         "PUBLISHER_DEFAULT_QUERY_ROW_LIMIT",
         value,
         getDefaultQueryRowLimit,
      );

   it("returns DEFAULT_QUERY_ROW_LIMIT when the env var is unset", () => {
      expect(limit()).toBe(DEFAULT_QUERY_ROW_LIMIT);
   });

   it("returns DEFAULT_QUERY_ROW_LIMIT when the env var is empty", () => {
      expect(limit("")).toBe(DEFAULT_QUERY_ROW_LIMIT);
   });

   it("returns the override when the env var is set", () => {
      expect(limit("250")).toBe(250);
   });

   it("rejects 0 (a default of 'zero rows' is always a misconfiguration)", () => {
      expect(() => limit("0")).toThrow();
   });

   it("rejects a negative override", () => {
      expect(() => limit("-1")).toThrow();
   });

   it("rejects a non-integer override", () => {
      expect(() => limit("1.5")).toThrow();
   });

   it("rejects a non-numeric override", () => {
      expect(() => limit("lots")).toThrow();
   });
});

describe("getQueryTimeoutMs", () => {
   const timeout = (value?: string) =>
      readWith("PUBLISHER_QUERY_TIMEOUT_MS", value, getQueryTimeoutMs);

   it("returns DEFAULT_QUERY_TIMEOUT_MS when the env var is unset", () => {
      expect(timeout()).toBe(DEFAULT_QUERY_TIMEOUT_MS);
   });

   it("returns the override when the env var is set", () => {
      expect(timeout("30000")).toBe(30000);
   });

   it("accepts 0 (used to opt out of the timeout)", () => {
      expect(timeout("0")).toBe(0);
   });

   it("rejects a negative override", () => {
      expect(() => timeout("-1")).toThrow();
   });

   it("rejects a non-integer override", () => {
      expect(() => timeout("1.5")).toThrow();
   });

   it("rejects a non-numeric override", () => {
      expect(() => timeout("slow")).toThrow();
   });
});

describe("getMaxConcurrentQueries", () => {
   const concurrent = (value?: string) =>
      readWith(
         "PUBLISHER_MAX_CONCURRENT_QUERIES",
         value,
         getMaxConcurrentQueries,
      );

   it("returns DEFAULT_MAX_CONCURRENT_QUERIES when the env var is unset", () => {
      expect(concurrent()).toBe(DEFAULT_MAX_CONCURRENT_QUERIES);
   });

   it("returns the override when the env var is set", () => {
      expect(concurrent("8")).toBe(8);
   });

   it("accepts 0 (used to opt out of the cap)", () => {
      expect(concurrent("0")).toBe(0);
   });

   it("rejects a negative override", () => {
      expect(() => concurrent("-1")).toThrow();
   });

   it("rejects a non-integer override", () => {
      expect(() => concurrent("1.5")).toThrow();
   });

   it("rejects a non-numeric override", () => {
      expect(() => concurrent("many")).toThrow();
   });
});

describe("getPostgresPoolMax", () => {
   const poolMax = (value?: string) =>
      readWith("PUBLISHER_POSTGRES_POOL_MAX", value, getPostgresPoolMax);

   it("returns DEFAULT_POSTGRES_POOL_MAX when the env var is unset", () => {
      expect(DEFAULT_POSTGRES_POOL_MAX).toBe(5);
      expect(poolMax()).toBe(DEFAULT_POSTGRES_POOL_MAX);
   });

   it("returns DEFAULT_POSTGRES_POOL_MAX when the env var is empty", () => {
      expect(poolMax("")).toBe(5);
   });

   it("returns the override when the env var is set", () => {
      expect(poolMax("12")).toBe(12);
   });

   it("rejects 0, which would leave the pool unable to open a session", () => {
      expect(() => poolMax("0")).toThrow(
         'postgres.poolMax from PUBLISHER_POSTGRES_POOL_MAX: expected an integer of at least 1, got "0"',
      );
   });

   it("rejects a negative override", () => {
      expect(() => poolMax("-1")).toThrow(
         'postgres.poolMax from PUBLISHER_POSTGRES_POOL_MAX: expected an integer of at least 1, got "-1"',
      );
   });

   it("rejects a non-integer override", () => {
      expect(() => poolMax("1.5")).toThrow(
         'postgres.poolMax from PUBLISHER_POSTGRES_POOL_MAX: expected an integer of at least 1, got "1.5"',
      );
   });
});

describe("getPublisherConfigDir", () => {
   const testRoot = path.join(process.cwd(), "test-temp-config-dir");
   const elsewhere = path.join(testRoot, "elsewhere");
   /** The anchor with exactly `env` as the variables. */
   const anchorWith = (env: Record<string, string>) =>
      withSettings({ env }, () => getPublisherConfigDir(testRoot));

   beforeEach(() => {
      fs.mkdirSync(elsewhere, { recursive: true });
   });

   afterEach(() => {
      fs.rmSync(testRoot, { recursive: true, force: true });
   });

   it("returns the server root when the config sits there", () => {
      fs.writeFileSync(
         path.join(testRoot, PUBLISHER_CONFIG_NAME),
         JSON.stringify({ environments: [] }),
      );
      expect(anchorWith({})).toBe(testRoot);
   });

   it("follows PUBLISHER_CONFIG_PATH to a config outside the server root", () => {
      // The --config case: a relative package location must anchor next to
      // the config that declares it, not at whatever cwd the server booted in.
      const configPath = path.join(elsewhere, PUBLISHER_CONFIG_NAME);
      fs.writeFileSync(configPath, JSON.stringify({ environments: [] }));
      expect(anchorWith({ PUBLISHER_CONFIG_PATH: configPath })).toBe(elsewhere);
   });

   it("returns null when no config resolves, so callers fall back to the server root", () => {
      expect(anchorWith({})).toBeNull();
   });

   it("returns an absolute anchor even when --config is relative", () => {
      // An anchor that is itself relative would re-resolve against the cwd of
      // whoever reads it, which is the bug this whole change is fixing.
      const rel = path.relative(
         process.cwd(),
         path.join(elsewhere, PUBLISHER_CONFIG_NAME),
      );
      fs.writeFileSync(
         path.join(elsewhere, PUBLISHER_CONFIG_NAME),
         JSON.stringify({ environments: [] }),
      );
      const anchor = anchorWith({ PUBLISHER_CONFIG_PATH: rel });
      expect(anchor).not.toBeNull();
      expect(path.isAbsolute(anchor as string)).toBe(true);
      expect(anchor).toBe(elsewhere);
   });

   it("returns null when --config names a directory instead of a file", () => {
      // existsSync is true for a directory, so without a file check the anchor
      // would silently become that directory's PARENT, for a config that never
      // loads (getPublisherConfig catches the read error and returns empty).
      expect(anchorWith({ PUBLISHER_CONFIG_PATH: elsewhere })).toBeNull();
   });

   it("returns null for the bundled default rather than anchoring inside the install", () => {
      // Zero-arg `npx @malloy-publisher/server` resolves the config that ships
      // inside the package. Anchoring a user's relative location under
      // node_modules would be meaningless, so the caller falls back to the
      // server root, which is what this did before the anchor moved.
      expect(anchorWith({ PUBLISHER_USE_BUNDLED_DEFAULT: "true" })).toBeNull();
   });
});

describe("PERSIST_COLLISION_ENFORCE", () => {
   // The flag decides whether a persist-target collision blocks a publish, so
   // mis-parsing it leaves the check warn-only — failing open in exactly the
   // direction it exists to prevent. An ad-hoc `=== "true"` did that for `1`,
   // `yes` and `on`.
   const enforce = (value?: string) =>
      readWith("PERSIST_COLLISION_ENFORCE", value, getPersistCollisionEnforce);

   it("defaults to warn-only when unset or empty", () => {
      expect(enforce()).toBe(false);
      expect(enforce("   ")).toBe(false);
   });

   it("enforces for every spelling of true an operator might use", () => {
      for (const raw of ["true", "TRUE", " True ", "1", "yes", "on"]) {
         expect(enforce(raw)).toBe(true);
      }
   });

   it("stays warn-only for every spelling of false", () => {
      for (const raw of ["false", "FALSE", "0", "no", "off"]) {
         expect(enforce(raw)).toBe(false);
      }
   });

   it("throws on a value that is neither, rather than guessing", () => {
      expect(() => enforce("enabled")).toThrow(
         'persist.collisionEnforcementEnabled from PERSIST_COLLISION_ENFORCE: expected a boolean (true/false, 1/0, yes/no or on/off), got "enabled"',
      );
   });
});

describe("EMBEDDING_MIN_SIMILARITY", () => {
   // The floor is a property of the embedding model, not of Publisher:
   // cosine similarity is not calibrated across models, so an operator
   // pointing EMBEDDING_API_BASE at a different endpoint needs to retune it
   // without a rebuild. It is an env var rather than a publisher.config.json
   // field because picking up a config change needs --init, which wipes
   // publisher_data and the embedding cache with it -- a full paid re-embed
   // on every tuning pass, for a value meant to be tuned iteratively.
   /** The embedding config, on, with the floor set to `value` if given. */
   const config = (value?: string) =>
      withSettings(
         {
            env: {
               EMBEDDING_API_KEY: "test-key",
               ...(value === undefined
                  ? {}
                  : { EMBEDDING_MIN_SIMILARITY: value }),
            },
         },
         () => getEmbeddingConfig(),
      );
   /** The message a floor outside [0, 1), or not a number, fails with. */
   const invalid = (value: string) =>
      `retrieval.embedding.minSimilarity from EMBEDDING_MIN_SIMILARITY: expected a number of at least 0 and less than 1, got "${value}"`;

   it("defaults to 0.20, the hosted pipeline's min_score", () => {
      expect(config()?.minSimilarity).toBe(DEFAULT_EMBEDDING_MIN_SIMILARITY);
      expect(DEFAULT_EMBEDDING_MIN_SIMILARITY).toBe(0.2);
   });

   it("takes an operator override", () => {
      expect(config("0.35")?.minSimilarity).toBe(0.35);
   });

   it("accepts 0, which disables the floor rather than meaning unset", () => {
      expect(config("0")?.minSimilarity).toBe(0);
   });

   // Rejected, never clamped and never ignored: a floor of 1 returns nothing
   // and a negative one returns everything, and both read as a broken index
   // rather than a bad setting. An invalid value must not go on to drive
   // retrieval.
   it.each(["1", "1.5", "-0.1", "high"])("rejects %s at startup", (value) => {
      expect(() => config(value)).toThrow(invalid(value));
   });

   it.each(["0.5abc", "0.5 0.6", "0x10", "1abc"])(
      "rejects %s rather than reading a prefix of it",
      (value) => {
         // Number.parseFloat stops at the first character it cannot read, so
         // these used to yield 0.5, 0.5, 0 and 1 -- an invalid setting that
         // went on to drive retrieval, which is exactly what the docs promise
         // does not happen.
         expect(() => config(value)).toThrow(invalid(value));
      },
   );

   it.each(["0.50", ".5", "5e-1"])("accepts %s", (value) => {
      // The forms a round-trip check like parseIntEnv's would wrongly reject.
      expect(config(value)?.minSimilarity).toBeCloseTo(0.5, 6);
   });

   it("names the variable and the range it takes in the error", () => {
      expect(() => config("2")).toThrow(invalid("2"));
   });
});
