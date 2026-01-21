import { describe, it, expect, beforeEach, afterEach } from "bun:test";
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
            projects: [
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
            projects: [
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
            projects: [
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

         expect(result.projects[0].packages[0].location).toBe(
            "gs://my-test-bucket/packages",
         );
      });

      it("should substitute environment variable in filesystem path", () => {
         process.env.PROJECT_ID = "analytics-2024";

         const config: PublisherConfig = {
            frozenConfig: false,
            projects: [
               {
                  name: "test-project",
                  packages: [
                     {
                        name: "test-package",
                        location: "./projects/${PROJECT_ID}/models",
                     },
                  ],
               },
            ],
         };

         fs.writeFileSync(configPath, JSON.stringify(config, null, 2));

         const result = getPublisherConfig(testServerRoot);

         expect(result.projects[0].packages[0].location).toBe(
            "./projects/analytics-2024/models",
         );
      });

      it("should substitute multiple environment variables in single value", () => {
         process.env.BUCKET_NAME = "data-warehouse";
         process.env.PROJECT_ID = "prod-analytics";

         const config: PublisherConfig = {
            frozenConfig: false,
            projects: [
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

         expect(result.projects[0].packages[0].location).toBe(
            "gs://data-warehouse/prod-analytics/models",
         );
      });

      it("should substitute variables across multiple packages", () => {
         process.env.BUCKET_1 = "bucket-one";
         process.env.BUCKET_2 = "bucket-two";

         const config: PublisherConfig = {
            frozenConfig: false,
            projects: [
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

         expect(result.projects[0].packages[0].location).toBe(
            "gs://bucket-one/path",
         );
         expect(result.projects[0].packages[1].location).toBe(
            "gs://bucket-two/path",
         );
      });

      it("should substitute variables in connection names", () => {
         process.env.DB_HOST = "localhost";

         const config: PublisherConfig = {
            frozenConfig: false,
            projects: [
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

         expect(result.projects[0].connections?.[0].name).toBe("db-localhost");
      });

      it("should substitute variables in nested configuration objects", () => {
         process.env.API_KEY = "secret-key-123";
         process.env.API_HOST = "api.example.com";

         const config = {
            frozenConfig: false,
            projects: [
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
            .projects[0] as (typeof result.projects)[0] & {
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
            projects: [
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

         expect(result.projects[0].packages[0].location).toBe(
            "gs://bucket/prefix-my-project-suffix/models",
         );
      });

      it("should substitute variables across multiple projects", () => {
         process.env.DEV_BUCKET = "dev-data";
         process.env.PROD_BUCKET = "prod-data";

         const config: PublisherConfig = {
            frozenConfig: false,
            projects: [
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

         expect(result.projects[0].packages[0].location).toBe(
            "gs://dev-data/models",
         );
         expect(result.projects[1].packages[0].location).toBe(
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
            projects: [
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
         expect(result.projects[0]).toHaveProperty("${KEY_NAME}");
         expect(
            (result.projects[0] as Record<string, unknown>)["${KEY_NAME}"],
         ).toBe("some-value");
      });

      it("should preserve keys with variable syntax while substituting values", () => {
         process.env.NORMAL_VALUE = "substituted-value";

         const config = {
            frozenConfig: false,
            projects: [
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
         expect(result.projects[0]).toHaveProperty("${DYNAMIC_KEY}");
         expect(
            (result.projects[0] as Record<string, unknown>)["${DYNAMIC_KEY}"],
         ).toBe("value1");

         // Value should be substituted
         expect(
            (result.projects[0] as Record<string, unknown>)["normal_key"],
         ).toBe("substituted-value");
      });

      it("should handle mixed scenario: variable in key and different variable in value", () => {
         process.env.VALUE_VAR = "actual-value";

         const config = {
            frozenConfig: false,
            projects: [
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
         expect(result.projects[0]).toHaveProperty("${KEY_VAR}");

         // Value is substituted
         expect(
            (result.projects[0] as Record<string, unknown>)["${KEY_VAR}"],
         ).toBe("actual-value");
      });

      it("should substitute variables in package names since they are values", () => {
         process.env.PACKAGE_NAME = "my-package";
         process.env.BUCKET_NAME = "my-bucket";

         const config: PublisherConfig = {
            frozenConfig: false,
            projects: [
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
         expect(result.projects[0].packages[0].name).toBe("my-package");

         // Location should also be substituted
         expect(result.projects[0].packages[0].location).toBe(
            "gs://my-bucket/models",
         );

         delete process.env.PACKAGE_NAME;
      });

      it("should handle nested objects with variable syntax in keys", () => {
         process.env.CONFIG_VALUE = "test-value";

         const config = {
            frozenConfig: false,
            projects: [
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
            .projects[0] as (typeof result.projects)[0] & {
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
            projects: [
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

         expect(result.projects[0].packages[0].location).toBe(
            "gs://bucket//path",
         );
      });

      it("should handle non-string values without modification", () => {
         const config: PublisherConfig = {
            frozenConfig: true,
            projects: [
               {
                  name: "test-project",
                  packages: [],
               },
            ],
         };

         // Add non-standard properties for testing
         const configWithExtras = {
            ...config,
            projects: [
               {
                  ...config.projects[0],
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
            .projects[0] as (typeof result.projects)[0] & {
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
            projects: [
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

      it("should handle empty projects array", () => {
         const config: PublisherConfig = {
            frozenConfig: false,
            projects: [],
         };

         fs.writeFileSync(configPath, JSON.stringify(config, null, 2));

         const result = getPublisherConfig(testServerRoot);

         expect(result.projects).toEqual([]);
      });

      it("should return default config when file does not exist", () => {
         // Don't create config file
         const result = getPublisherConfig(testServerRoot);

         expect(result).toEqual({
            frozenConfig: false,
            projects: [],
         });
      });

      it("should handle whitespace around variable names", () => {
         process.env.BUCKET_NAME = "test-bucket";

         const config: PublisherConfig = {
            frozenConfig: false,
            projects: [
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
         expect(result.projects[0].packages[0].location).toBe(
            "gs://${ BUCKET_NAME }/path",
         );
      });

      it("should handle multiple projects with mixed variable usage", () => {
         process.env.PROD_BUCKET = "production-data";

         const config: PublisherConfig = {
            frozenConfig: false,
            projects: [
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

         expect(result.projects[0].packages[0].location).toBe("./packages/dev");
         expect(result.projects[1].packages[0].location).toBe(
            "gs://production-data/models",
         );

         delete process.env.PROD_BUCKET;
      });

      it("should preserve variable syntax if not matching pattern", () => {
         const config: PublisherConfig = {
            frozenConfig: false,
            projects: [
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
         expect(result.projects[0].packages[0].location).toBe(
            "gs://bucket/${lowercase_var}/path",
         );
      });

      it("should handle arrays of strings with variables", () => {
         process.env.TAG1 = "analytics";
         process.env.TAG2 = "production";

         const config = {
            frozenConfig: false,
            projects: [
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
            .projects[0] as (typeof result.projects)[0] & {
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
            projects: [
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

         expect(result.projects[0].name).toBe("staging");
         expect(result.projects[0].packages[0].location).toBe(
            "gs://company-data-staging/company-project-staging/analytics",
         );
         expect(result.projects[0].packages[1].location).toBe(
            "gs://company-data-staging/company-project-staging/reporting",
         );

         delete process.env.ENV;
         delete process.env.DATA_BUCKET;
      });

      it("should handle connection configurations with environment variables", () => {
         process.env.DB_CONNECTION = "production-db";

         const config: PublisherConfig = {
            frozenConfig: false,
            projects: [
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

         expect(result.projects[0].connections?.[0].name).toBe("production-db");

         delete process.env.DB_CONNECTION;
      });
   });
});
