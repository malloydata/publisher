import { check, group, sleep } from "k6";
import {
   getConnectionsClient,
   getDatabasesClient,
   getModelsClient,
   getNotebooksClient,
   getPackagesClient,
   getProjectsClient,
} from "../utils/client_factory.ts";
import {
   AUTH_TOKEN,
   BASE_URL,
   getAvailablePackages,
   getModelData,
   getPackages,
   getViews,
   PROJECT_NAME,
   PUBLISHER_URL,
   queryModelView,
   validateServerIsUpAndInitialized,
} from "../utils/common.ts";
import { logger } from "../utils/logger.ts";
const projectsClient = getProjectsClient(BASE_URL, AUTH_TOKEN);
const packagesClient = getPackagesClient(BASE_URL, AUTH_TOKEN);
const modelsClient = getModelsClient(BASE_URL, AUTH_TOKEN);
const notebooksClient = getNotebooksClient(BASE_URL, AUTH_TOKEN);
const connectionsClient = getConnectionsClient(BASE_URL, AUTH_TOKEN);
const databasesClient = getDatabasesClient(BASE_URL, AUTH_TOKEN);

/**
 * Setup data structure containing discovered resources
 * Only basic resources are discovered in setup - detailed discovery happens during test execution
 */
type SetupData = {
   projectNames: string[];
   packageNames: string[];
   connectionNames: string[];
};

/**
 * Setup function - runs once before all VUs
 * Validates environment variables and discovers available resources
 */
export function setup(): SetupData {
   // Validate required environment variables
   if (!PUBLISHER_URL || PUBLISHER_URL === "") {
      throw new Error(
         "PUBLISHER_URL is required but not set. Set K6_PUBLISHER_URL environment variable.",
      );
   }

   if (!PROJECT_NAME || PROJECT_NAME === "") {
      throw new Error(
         "PROJECT_NAME is required but not set. Set K6_PROJECT_NAME environment variable.",
      );
   }
   validateServerIsUpAndInitialized();

   logger.setLogLevel("info");
   logger.info("Setup: Validating environment variables...");
   logger.info(`Setup: PUBLISHER_URL = ${PUBLISHER_URL}`);
   logger.info(`Setup: PROJECT_NAME = ${PROJECT_NAME}`);

   // Discover available projects
   logger.info("Setup: Discovering available projects...");
   const projectsResponse = projectsClient.listProjects({
      tags: { name: "setup_list_projects" },
   });
   const projectNames: string[] = [];
   if (
      projectsResponse.response.status === 200 &&
      Array.isArray(projectsResponse.data)
   ) {
      projectNames.push(
         ...projectsResponse.data.map((p) => p.name || "").filter(Boolean),
      );
      logger.info(
         `Setup: Found ${projectNames.length} projects: ${projectNames.join(", ")}`,
      );
   } else {
      logger.warn("Setup: Failed to list projects, using PROJECT_NAME only");
      projectNames.push(PROJECT_NAME);
   }

   // Discover packages for the main project
   logger.info(`Setup: Discovering packages in project ${PROJECT_NAME}...`);
   const packages = getPackages();
   const availablePackages = getAvailablePackages();
   const packageNames = packages
      .filter((pkg) => availablePackages.includes(pkg.name))
      .map((pkg) => pkg.name);
   logger.info(
      `Setup: Found ${packageNames.length} packages: ${packageNames.join(", ")}`,
   );

   // Discover connections
   logger.info(`Setup: Discovering connections in project ${PROJECT_NAME}...`);
   const connectionsResponse = connectionsClient.listConnections(PROJECT_NAME, {
      tags: { name: "setup_list_connections" },
   });
   const connectionNames: string[] = [];
   if (
      connectionsResponse.response.status === 200 &&
      Array.isArray(connectionsResponse.data)
   ) {
      connectionNames.push(
         ...connectionsResponse.data.map((c) => c.name || "").filter(Boolean),
      );
      logger.info(
         `Setup: Found ${connectionNames.length} connections: ${connectionNames.join(", ")}`,
      );
   } else {
      logger.warn(
         "Setup: Failed to list connections, will use 'duckdb' as default",
      );
      connectionNames.push("duckdb");
   }

   return {
      projectNames,
      packageNames,
      connectionNames,
   };
}

/**
 * Load Test - Comprehensive API Read Operations
 *
 * This test exercises all read operations and query execution endpoints
 * under normal load. It assumes projects, packages, connections are already loaded.
 *
 * Default configuration:
 * - Stages with ramp-up to 100 VUs
 * - 95th percentile response time < 2s
 * - Error rate < 2%
 */
export const loadTest: TestPreset = {
   defaultOptions: {
      stages: [
         { duration: "1m", target: 50 }, // warm-up
         { duration: "5m", target: 50 }, // sustained load
         { duration: "1m", target: 0 }, // ramp down
      ],
      thresholds: {
         http_req_duration: ["p(90)<600", "p(95)<800"],
         http_req_waiting: ["p(95)<800"],
         http_req_failed: ["rate<0.10"],
         checks: ["rate>0.85"],
         dropped_iterations: ["count==0"],
         "http_req_duration{name:list_projects}": ["p(95)<450", "p(99)<700"],
         "http_req_duration{name:get_project}": ["p(95)<450", "p(99)<700"],
         "http_req_duration{name:list_packages}": ["p(95)<450", "p(99)<700"],
         "http_req_duration{name:get_package}": ["p(95)<450", "p(99)<700"],
         "http_req_duration{name:list_models}": ["p(95)<450", "p(99)<700"],
         "http_req_duration{name:get_model}": ["p(95)<450", "p(99)<700"],
         "http_req_duration{name:list_notebooks}": ["p(95)<450", "p(99)<700"],
         "http_req_duration{name:get_notebook}": ["p(95)<450", "p(99)<700"],
         "http_req_duration{name:execute_notebook_cell}": [
            "p(95)<450",
            "p(99)<700",
         ],
         "http_req_duration{name:list_connections}": ["p(95)<300", "p(99)<600"],
         "http_req_duration{name:get_connection}": ["p(95)<2000", "p(99)<3500"],
         "http_req_duration{name:list_databases}": ["p(95)<300", "p(99)<600"],
         "http_req_duration{name:execute_query}": ["p(95)<1500", "p(99)<2000"],
         "http_req_duration{name:post_query_data}": [
            "p(95)<1500",
            "p(99)<2000",
         ],
         "http_req_duration{name:list_schemas}": ["p(95)<2500", "p(99)<4000"],
         "http_req_duration{name:list_tables}": ["p(95)<2000", "p(99)<3500"],
         "http_req_duration{name:get_table}": ["p(95)<1500", "p(99)<2000"],
         "http_req_duration{name:post_sql_source}": [
            "p(95)<1200",
            "p(99)<1500",
         ],
         "http_req_duration{name:post_temporary_table}": [
            "p(95)<400",
            "p(99)<800",
         ],
      },
   },
   run: (data: unknown | undefined) => {
      const setupData = data as SetupData;
      if (!setupData) {
         logger.error("Setup data not available, skipping load test");
         return;
      }

      const { projectNames, packageNames, connectionNames } = setupData;

      // Select a random project, package, connection for this iteration
      const projectName =
         PROJECT_NAME || projectNames[__VU % projectNames.length];
      if (!projectName) {
         logger.error("No project available, skipping test");
         return;
      }
      const packageName = packageNames[__VU % packageNames.length];
      const connectionName =
         connectionNames[__VU % connectionNames.length] || "duckdb";

      if (!packageName) {
         logger.error("No packages available, skipping test");
         return;
      }

      // ========================================================================
      // 1. Project Operations
      // ========================================================================
      group("Project Operations", () => {
         // List Projects (with reload=false)
         group("List Projects", () => {
            const response = projectsClient.listProjects({
               tags: { name: "list_projects" },
            });
            check(response.response, {
               "list projects request successful": (r) => r.status === 200,
            });
         });

         sleep(0.1);

         // Get Project
         group("Get Project", () => {
            const response = projectsClient.getProject(
               projectName,
               { reload: false },
               {
                  tags: { name: "get_project" },
               },
            );
            check(response.response, {
               "get project request successful": (r) => r.status === 200,
            });
         });
      });

      sleep(0.1);

      // ========================================================================
      // 2. Package Operations
      // ========================================================================
      group("Package Operations", () => {
         // List Packages
         group("List Packages", () => {
            const response = packagesClient.listPackages(projectName, {
               tags: { name: "list_packages" },
            });
            check(response.response, {
               "list packages request successful": (r) => r.status === 200,
            });
         });

         sleep(0.1);

         // Get Package
         if (packageName) {
            group("Get Package", () => {
               const response = packagesClient.getPackage(
                  projectName,
                  packageName,
                  {}, // params
                  { tags: { name: "get_package" } }, // requestParameters
               );
               check(response.response, {
                  "get package request successful": (r) => r.status === 200,
               });
            });
         }
      });

      sleep(0.1);

      // ========================================================================
      // 3. Model Operations
      // ========================================================================
      group("Model Operations", () => {
         // List Models
         group("List Models", () => {
            const response = modelsClient.listModels(
               projectName,
               packageName,
               {}, // params
               { tags: { name: "list_models" } }, // requestParameters
            );
            check(response.response, {
               "list models request successful": (r) => r.status === 200,
            });

            sleep(0.1);

            // Get Model - dynamically fetch from list results
            if (
               response.response.status === 200 &&
               Array.isArray(response.data) &&
               response.data.length > 0
            ) {
               // Select first model without errors
               const model = response.data.find((m) => m.path && !m.error);
               if (model && model.path) {
                  const modelPath = model.path;
                  group("Get Model", () => {
                     try {
                        const getModelResponse = modelsClient.getModel(
                           projectName,
                           packageName,
                           modelPath,
                           {},
                           { tags: { name: "get_model" } },
                        );
                        check(getModelResponse.response, {
                           "get model request successful": (r) =>
                              r.status === 200 || r.status === 424, // 424 is compilation error, acceptable
                        });
                     } catch (error) {
                        logger.warn(
                           `Failed to get model ${modelPath}: ${error}`,
                        );
                     }
                  });
               }
            }
         });
      });

      sleep(0.1);

      // ========================================================================
      // 4. Notebook Operations
      // ========================================================================
      group("Notebook Operations", () => {
         // List Notebooks
         group("List Notebooks", () => {
            const response = notebooksClient.listNotebooks(
               projectName,
               packageName,
               {}, // params
               {
                  tags: { name: "list_notebooks" },
               },
            );
            check(response.response, {
               "list notebooks request successful": (r) => r.status === 200,
            });

            sleep(0.1);

            // Get Notebook - dynamically fetch from list results
            if (
               response.response.status === 200 &&
               Array.isArray(response.data) &&
               response.data.length > 0
            ) {
               // Select first notebook
               const notebook = response.data.find((n) => n.path);
               if (notebook && notebook.path) {
                  const notebookPath = notebook.path;
                  group("Get Notebook", () => {
                     try {
                        const getNotebookResponse = notebooksClient.getNotebook(
                           projectName,
                           packageName,
                           notebookPath,
                           {},
                           { tags: { name: "get_notebook" } },
                        );
                        check(getNotebookResponse.response, {
                           "get notebook request successful": (r) =>
                              r.status === 200 || r.status === 424, // 424 is compilation error, acceptable
                        });

                        sleep(0.1);

                        // Execute Notebook Cell
                        group("Execute Notebook Cell", () => {
                           try {
                              const executeResponse =
                                 notebooksClient.executeNotebookCell(
                                    projectName,
                                    packageName,
                                    notebookPath,
                                    0, // First cell
                                    {},
                                    { tags: { name: "execute_notebook_cell" } },
                                 );
                              check(executeResponse.response, {
                                 "execute notebook cell request successful": (
                                    r,
                                 ) => r.status === 200 || r.status === 424, // 424 is compilation error, acceptable
                              });
                              if (executeResponse.response.status != 200) {
                                 logger.info(
                                    `executeResponse: ${JSON.stringify(executeResponse)}`,
                                 );
                              }
                           } catch (error) {
                              logger.warn(
                                 `Failed to execute notebook cell: ${error}`,
                              );
                           }
                        });
                     } catch (error) {
                        logger.warn(
                           `Failed to get notebook ${notebookPath}: ${error}`,
                        );
                     }
                  });
               }
            }
         });
      });

      sleep(0.1);

      // ========================================================================
      // 5. Connection Operations
      // ========================================================================
      group("Connection Operations", () => {
         // List Connections
         group("List Connections", () => {
            const response = connectionsClient.listConnections(projectName, {
               tags: { name: "list_connections" },
            });
            check(response.response, {
               "list connections request successful": (r) => r.status === 200,
            });
         });

         sleep(0.1);

         // Get Connection
         if (connectionName) {
            group("Get Connection", () => {
               const response = connectionsClient.getConnection(
                  projectName,
                  connectionName,
                  { tags: { name: "get_connection" } },
               );
               check(response.response, {
                  "get connection request successful": (r) => r.status === 200,
               });
            });
         }
      });

      sleep(0.1);

      // ========================================================================
      // 6. Database Operations
      // ========================================================================
      group("Database Operations", () => {
         // List Databases
         group("List Databases", () => {
            const response = databasesClient.listDatabases(
               projectName,
               packageName,
               {},
               {
                  tags: { name: "list_databases" },
               },
            );
            check(response.response, {
               "list databases request successful": (r) => r.status === 200,
            });
         });
      });

      sleep(0.1);

      // ========================================================================
      // 7. Query Operations
      // ========================================================================
      group("Query Operations", () => {
         // First, list models to get a model for query execution
         const listModelsResponse = modelsClient.listModels(
            projectName,
            packageName,
            {}, // params
            { tags: { name: "list_models" } }, // requestParameters
         );
         if (
            listModelsResponse.response.status === 200 &&
            Array.isArray(listModelsResponse.data) &&
            listModelsResponse.data.length > 0
         ) {
            // Select first model without errors
            const model = listModelsResponse.data.find(
               (m) => m.path && !m.error,
            );
            if (model && model.path) {
               const modelPath = model.path;
               // Execute Query (using queryModelView from common.ts)
               group("Execute Query", () => {
                  try {
                     const modelData = getModelData(packageName, modelPath);
                     if (
                        modelData &&
                        modelData.sources &&
                        modelData.sources.length > 0
                     ) {
                        const views = Array.from(getViews(modelData));
                        if (views.length > 0) {
                           const view = views[0];
                           if (view && view.viewName) {
                              const queryResponse = queryModelView(
                                 packageName,
                                 modelPath,
                                 view.sourceName || "",
                                 view.viewName,
                              );
                              check(queryResponse, {
                                 "execute query request successful": (r) =>
                                    r.status === 200 || r.status === 424, // 424 is compilation error
                              });
                           }
                        }
                     }
                  } catch (error) {
                     logger.warn(`Failed to execute query: ${error}`);
                  }
               });

               sleep(0.1);

               // Execute Query Model (POST API)
               group("Execute Query Model", () => {
                  try {
                     const modelData = getModelData(packageName, modelPath);
                     if (
                        modelData &&
                        modelData.sources &&
                        modelData.sources.length > 0
                     ) {
                        const views = Array.from(getViews(modelData));
                        if (views.length > 0) {
                           const view = views[0];
                           if (view && view.viewName) {
                              const queryRequest: {
                                 sourceName?: string;
                                 queryName: string;
                              } = {
                                 queryName: view.viewName,
                              };
                              if (
                                 view.sourceName &&
                                 view.sourceName.trim() !== ""
                              ) {
                                 queryRequest.sourceName = view.sourceName;
                              }

                              const response = modelsClient.executeQueryModel(
                                 projectName,
                                 packageName,
                                 modelPath,
                                 queryRequest,
                                 { tags: { name: "execute_query" } },
                              );
                              check(response.response, {
                                 "execute query model request successful": (
                                    r,
                                 ) => r.status === 200 || r.status === 424, // 424 is compilation error
                              });
                           }
                        }
                     }
                  } catch (error) {
                     logger.warn(`Failed to execute query model: ${error}`);
                  }
               });
            }
         }

         sleep(0.1);

         // Post Query Data (SQL query)
         if (connectionName) {
            group("Post Query Data", () => {
               try {
                  const response = connectionsClient.postQuerydata(
                     projectName,
                     connectionName,
                     {
                        sqlStatement: "SELECT 1 as test_column",
                     },
                     {},
                     { tags: { name: "post_query_data" } },
                  );
                  check(response.response, {
                     "post query data request successful": (r) =>
                        r.status === 200,
                  });
               } catch (error) {
                  logger.warn(`Failed to post query data: ${error}`);
               }
            });
         }
      });

      sleep(0.1);

      // ========================================================================
      // 8. SQL Source Operations
      // ========================================================================
      group("SQL Source Operations", () => {
         // doing to avoid being rate limited by bigquery
         if (connectionName === "bigquery" && __VU % 10 != 0) {
            return;
         }
         if (connectionName) {
            // List Schemas - dynamically discover schemas
            group("List Schemas", () => {
               try {
                  const schemasResponse = connectionsClient.listSchemas(
                     projectName,
                     connectionName,
                     { tags: { name: "list_schemas" } },
                  );
                  check(schemasResponse.response, {
                     "list schemas request successful": (r) => r.status === 200,
                  });

                  sleep(0.1);

                  // List Tables - dynamically discover tables from first schema
                  if (
                     schemasResponse.response.status === 200 &&
                     Array.isArray(schemasResponse.data) &&
                     schemasResponse.data.length > 0
                  ) {
                     const firstSchema = schemasResponse.data.find(
                        (s) => s.name,
                     );
                     if (firstSchema && firstSchema.name) {
                        const schemaName = firstSchema.name;
                        group("List Tables", () => {
                           try {
                              const tablesResponse =
                                 connectionsClient.listTables(
                                    projectName,
                                    connectionName,
                                    schemaName,
                                    { tags: { name: "list_tables" } },
                                 );
                              check(tablesResponse.response, {
                                 "list tables request successful": (r) =>
                                    r.status === 200,
                              });

                              sleep(0.1);

                              // Get Table - use resource field from listTables
                              if (
                                 tablesResponse.response.status === 200 &&
                                 Array.isArray(tablesResponse.data) &&
                                 tablesResponse.data.length > 0
                              ) {
                                 const table = tablesResponse.data.find(
                                    (t) => t.resource,
                                 );
                                 if (table && table.resource) {
                                    const tablePath = table.resource;
                                    group("Get Table", () => {
                                       try {
                                          const getTableResponse =
                                             connectionsClient.getTable(
                                                projectName,
                                                connectionName,
                                                schemaName,
                                                tablePath, // schemaName.tableName
                                                { tags: { name: "get_table" } },
                                             );
                                          check(getTableResponse.response, {
                                             "get table request successful": (
                                                r,
                                             ) =>
                                                r.status === 200 ||
                                                r.status === 404, // 404 if table doesn't exist
                                          });
                                       } catch (error) {
                                          logger.warn(
                                             `Failed to get table ${tablePath}: ${error}`,
                                          );
                                       }
                                    });
                                 }
                              }
                           } catch (error) {
                              logger.warn(
                                 `Failed to list tables for schema ${schemaName}: ${error}`,
                              );
                           }
                        });
                     }
                  }
               } catch (error) {
                  logger.warn(
                     `Failed to list schemas for ${connectionName}: ${error}`,
                  );
               }
            });

            sleep(0.1);

            // Post SQL Source
            group("Post SQL Source", () => {
               try {
                  const response = connectionsClient.postSqlsource(
                     projectName,
                     connectionName,
                     {
                        sqlStatement: "SELECT 1 as test_column",
                     },
                     { tags: { name: "post_sql_source" } },
                  );
                  check(response.response, {
                     "post sql source request successful": (r) =>
                        r.status === 200,
                  });
               } catch (error) {
                  logger.warn(`Failed to post sql source: ${error}`);
               }
            });

            sleep(0.1);

            // Post Temporary Table
            group("Post Temporary Table", () => {
               try {
                  const response = connectionsClient.postTemporarytable(
                     projectName,
                     connectionName,
                     {
                        sqlStatement: "SELECT 1 as test_column",
                     },
                     { tags: { name: "post_temporary_table" } },
                  );
                  check(response.response, {
                     "post temporary table request successful": (r) =>
                        r.status === 200,
                  });
               } catch (error) {
                  logger.warn(`Failed to post temporary table: ${error}`);
               }
            });
         }
      });

      sleep(0.1);
   },
};

export const options = loadTest.defaultOptions;
export default loadTest.run;
