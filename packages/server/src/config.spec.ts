import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs";
import path from "path";
import { getPublisherConfig, type PublisherConfig } from "./config";
import { PUBLISHER_CONFIG_NAME } from "./constants";

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
            frozenConfig: false,
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
            frozenConfig: false,
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
            frozenConfig: false,
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
            frozenConfig: false,
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
            frozenConfig: false,
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
            frozenConfig: false,
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
            frozenConfig: false,
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
            frozenConfig: false,
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
            frozenConfig: false,
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
            frozenConfig: false,
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
            frozenConfig: false,
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
         const config: PublisherConfig = {
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

         expect(result.frozenConfig).toBe(true);
         expect(projectWithExtras.count).toBe(42);
         expect(projectWithExtras.enabled).toBe(true);
         expect(projectWithExtras.ratio).toBe(3.14);
         expect(projectWithExtras.metadata).toBe(null);
         expect(projectWithExtras.tags).toEqual(["tag1", "tag2"]);
      });

      it("should handle config with no environment variables", () => {
         const config: PublisherConfig = {
            frozenConfig: false,
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
            frozenConfig: false,
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
            frozenConfig: false,
            environments: [],
         });
      });

      it("should handle whitespace around variable names", () => {
         process.env.BUCKET_NAME = "test-bucket";

         const config: PublisherConfig = {
            frozenConfig: false,
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
            frozenConfig: false,
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
            frozenConfig: false,
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
            frozenConfig: false,
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
            frozenConfig: false,
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
            warnings.some((w) =>
               w.includes('uses deprecated "projects" key'),
            ),
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
            warnings.some((w) =>
               w.includes('uses deprecated "projects" key'),
            ),
         ).toBe(false);
      } finally {
         logger.warn = originalWarn;
      }
   });
});
