import { check, group, sleep } from "k6";
import type { Connection } from "../clients/malloyPublisherSemanticModelServingAPI.schemas.ts";
import { ConnectionType as ConnectionTypeEnum } from "../clients/malloyPublisherSemanticModelServingAPI.schemas.ts";
import {
   getConnectionsClient,
   getProjectsClient,
} from "../utils/client_factory.ts";
import {
   AUTH_TOKEN,
   BASE_URL,
   generateTestName,
   HAS_BIGQUERY_CREDENTIALS,
   validateServerIsUpAndInitialized,
} from "../utils/common.ts";

const projectsClient = getProjectsClient(BASE_URL, AUTH_TOKEN);
const connectionsClient = getConnectionsClient(BASE_URL, AUTH_TOKEN);

/**
 * Setup data structure for connections test
 */
type SetupData = {
   projectName: string;
};

/**
 * Setup function - runs once before all VUs
 * Creates a project for connection CRUD testing
 */
export function setup(): SetupData {
   // Validate server is up and initialized before proceeding
   validateServerIsUpAndInitialized();

   const projectName = generateTestName("load-test-conn-project");
   console.log(
      `Setup: Creating project ${projectName} for connections testing`,
   );
   const setupProjectResponse = projectsClient.createProject(
      {
         name: projectName,
         location: "",
      },
      { tags: { name: "setup_create_project" } },
   );

   const status = setupProjectResponse.response.status;
   if (status !== 200 && status !== 201) {
      const errorBody =
         setupProjectResponse.response.body || "no error details";
      const errorMessage =
         status === 501
            ? `Project creation not available (config may be frozen or endpoint not implemented). Status: ${status}`
            : `Failed to setup project for connections. Status: ${status}, Body: ${errorBody}`;
      throw new Error(errorMessage);
   }

   console.log(`Setup: Created project ${projectName} for connections testing`);
   return { projectName };
}

/**
 * Teardown function - runs once after all VUs
 * Cleans up the project created in setup
 * Gracefully handles cases where server is down
 */
export function teardown(data: SetupData) {
   if (data && data.projectName) {
      console.log(`Teardown: Deleting project ${data.projectName}`);
      try {
         const deleteResponse = projectsClient.deleteProject(data.projectName, {
            tags: { name: "teardown_delete_project" },
         });
         if (deleteResponse.response.status === 0) {
            console.warn(
               `Teardown: Could not connect to server to delete project ${data.projectName} (server may be down)`,
            );
         } else if (
            deleteResponse.response.status !== 200 &&
            deleteResponse.response.status !== 204
         ) {
            console.warn(
               `Teardown: Failed to delete project ${data.projectName}: HTTP ${deleteResponse.response.status}`,
            );
         }
      } catch (error) {
         console.warn(
            `Teardown: Error deleting project ${data.projectName}: ${error}`,
         );
      }
   }
}

/**
 * Load Test - CRUD Connections
 *
 * This test verifies system performance for connection CRUD operations.
 * A single project is created at the beginning and reused across all iterations.
 * Cleanup happens at the end of the test run.
 *
 * Default configuration:
 * - Stages with ramp-up to 100 VUs
 * - 95th percentile response time < 1.5s
 * - Error rate < 2%
 */
export const loadTestConnections: TestPreset = {
   defaultOptions: {
      stages: [
         { duration: "1m", target: 20 }, // warm-up
         { duration: "2m", target: 50 }, // load
         { duration: "5m", target: 100 }, // sustained load
         { duration: "2m", target: 150 }, // near saturation
         { duration: "1m", target: 0 }, // ramp down
      ],
      thresholds: {
         // Global thresholds - updated with buffer for 50 VUs
         http_req_duration: ["p(90)<2000", "p(95)<2500", "p(99)<4000"],
         http_req_waiting: ["p(95)<2500"],
         http_req_failed: ["rate<0.02"],
         checks: ["rate>0.98"],
         dropped_iterations: ["count==0"],
         // Per-operation thresholds (C, R, U, D) - DuckDB (with buffer for 50 VUs)
         "http_req_duration{name:create_connection_duckdb}": [
            "p(90)<2500",
            "p(95)<3000",
            "p(99)<4000",
         ],
         "http_req_duration{name:get_connection_duckdb}": [
            "p(90)<2000",
            "p(95)<2500",
            "p(99)<3500",
         ],
         "http_req_duration{name:update_connection_duckdb}": [
            "p(90)<2500",
            "p(95)<3500",
            "p(99)<4500",
         ],
         "http_req_duration{name:delete_connection_duckdb}": [
            "p(90)<2000",
            "p(95)<2500",
            "p(99)<4000",
         ],
         // Per-operation thresholds (C, R, U, D) - BigQuery (with buffer for 50 VUs)
         "http_req_duration{name:create_connection_bigquery}": [
            "p(90)<2500",
            "p(95)<3000",
            "p(99)<4000",
         ],
         "http_req_duration{name:get_connection_bigquery}": [
            "p(90)<2000",
            "p(95)<2500",
            "p(99)<3500",
         ],
         "http_req_duration{name:update_connection_bigquery}": [
            "p(90)<2000",
            "p(95)<2500",
            "p(99)<4000",
         ],
         "http_req_duration{name:delete_connection_bigquery}": [
            "p(90)<2000",
            "p(95)<2500",
            "p(99)<4000",
         ],
         // Common operations
         "http_req_duration{name:list_connections}": [
            "p(90)<2000",
            "p(95)<2500",
            "p(99)<4000",
         ],
      },
   },
   run: (data: unknown | undefined) => {
      const setupData = data as SetupData;
      if (!setupData || !setupData.projectName) {
         console.error(
            "Setup data not available, skipping connection CRUD test",
         );
         return;
      }

      const { projectName } = setupData;

      // Build array of connection types to test
      // Always include DuckDB, add BigQuery if credentials are available
      const connectionTypes: Array<{
         type: ConnectionTypeEnum;
         name: string;
         tagSuffix: string;
      }> = [
         {
            type: ConnectionTypeEnum.duckdb,
            name: "duckdb",
            tagSuffix: "duckdb",
         },
      ];

      if (HAS_BIGQUERY_CREDENTIALS) {
         connectionTypes.push({
            type: ConnectionTypeEnum.bigquery,
            name: "bigquery",
            tagSuffix: "bigquery",
         });
      }

      // Pick a connection type based on VU number to cycle through them
      const connectionIndex = __VU % connectionTypes.length;
      const selectedConnection = connectionTypes[connectionIndex];
      if (!selectedConnection) {
         console.error("No connection type selected for load testing");
         return;
      }

      const connectionName = generateTestName(
         `load-test-connection-${selectedConnection.name}`,
      );

      group(`Connections CRUD (${selectedConnection.name})`, () => {
         // Create connection
         group("Create Connection", () => {
            let testConnection: Connection;

            if (selectedConnection.type === ConnectionTypeEnum.duckdb) {
               testConnection = {
                  name: connectionName,
                  type: ConnectionTypeEnum.duckdb,
                  attributes: {
                     dialectName: "duckdb",
                     isPool: false,
                     canPersist: true,
                     canStream: true,
                  },
                  duckdbConnection: {},
               };
            } else if (
               selectedConnection.type === ConnectionTypeEnum.bigquery
            ) {
               testConnection = {
                  name: connectionName,
                  type: ConnectionTypeEnum.bigquery,
                  attributes: {
                     dialectName: "bigquery",
                     isPool: false,
                     canPersist: true,
                     canStream: true,
                  },
                  bigqueryConnection: {
                     location: "US",
                     serviceAccountKeyJson:
                        __ENV.GOOGLE_APPLICATION_CREDENTIALS,
                  },
               };
            } else {
               console.error(
                  `Unsupported connection type: ${selectedConnection.type}`,
               );
               return;
            }

            const createConnectionResponse = connectionsClient.createConnection(
               projectName,
               connectionName,
               testConnection,
               {
                  tags: {
                     name: `create_connection_${selectedConnection.tagSuffix}`,
                  },
               },
            );
            check(createConnectionResponse.response, {
               "create connection request successful": (r) =>
                  r.status === 200 || r.status === 201,
            });
            // Check for connection failure (status 0) or HTTP errors
            const createStatus = createConnectionResponse.response.status;
            if (createStatus === 0) {
               return; // Skip remaining connection tests if server is down
            }
            if (createStatus !== 200 && createStatus !== 201) {
               return; // Skip remaining tests if create failed
            }
         });

         // Read connection
         group("Get Connection", () => {
            const getConnectionResponse = connectionsClient.getConnection(
               projectName,
               connectionName,
               {
                  tags: {
                     name: `get_connection_${selectedConnection.tagSuffix}`,
                  },
               },
            );
            const getStatus = getConnectionResponse.response.status;
            check(getConnectionResponse.response, {
               "get connection request successful": (r) => r.status === 200,
            });
            if (getStatus === 0) {
               return;
            }
         });

         // List connections
         group("List Connections", () => {
            const listConnectionsResponse = connectionsClient.listConnections(
               projectName,
               { tags: { name: "list_connections" } },
            );
            const listStatus = listConnectionsResponse.response.status;
            check(listConnectionsResponse.response, {
               "list connections request successful": (r) => r.status === 200,
            });
            if (listStatus === 0) {
               return;
            }
         });

         // Update connection
         group("Update Connection", () => {
            let updatePayload: Partial<Connection>;

            if (selectedConnection.type === ConnectionTypeEnum.duckdb) {
               updatePayload = {
                  duckdbConnection: {},
               };
            } else if (
               selectedConnection.type === ConnectionTypeEnum.bigquery
            ) {
               updatePayload = {
                  bigqueryConnection: {
                     location: "US",
                     serviceAccountKeyJson:
                        __ENV.GOOGLE_APPLICATION_CREDENTIALS,
                  },
               };
            } else {
               console.error(
                  `Unsupported connection type for update: ${selectedConnection.type}`,
               );
               return;
            }

            const updateConnectionResponse = connectionsClient.updateConnection(
               projectName,
               connectionName,
               updatePayload,
               {
                  tags: {
                     name: `update_connection_${selectedConnection.tagSuffix}`,
                  },
               },
            );
            const updateStatus = updateConnectionResponse.response.status;
            check(updateConnectionResponse.response, {
               "update connection request successful": (r) => r.status === 200,
            });
            if (updateStatus === 0) {
               return;
            }
         });

         // Delete connection
         group("Delete Connection", () => {
            const deleteConnectionResponse = connectionsClient.deleteConnection(
               projectName,
               connectionName,
               {
                  tags: {
                     name: `delete_connection_${selectedConnection.tagSuffix}`,
                  },
               },
            );
            check(deleteConnectionResponse.response, {
               "delete connection request successful": (r) =>
                  r.status === 200 || r.status === 204,
            });
         });
      });

      sleep(0.1);
   },
};

export const options = loadTestConnections.defaultOptions;
export default loadTestConnections.run;
