import { check, group, sleep } from "k6";
import {
   getConnectionsClient,
   getPackagesClient,
   getProjectsClient,
} from "../utils/client_factory.ts";
import type { Connection } from "../clients/malloyPublisherSemanticModelServingAPI.schemas.ts";
import { ConnectionType as ConnectionTypeEnum } from "../clients/malloyPublisherSemanticModelServingAPI.schemas.ts";
import {
   AUTH_TOKEN,
   BASE_URL,
   generateTestName,
   getAvailablePackages,
   HAS_BIGQUERY_CREDENTIALS,
   validateServerIsUpAndInitialized,
} from "../utils/common.ts";

const projectsClient = getProjectsClient(BASE_URL, AUTH_TOKEN);
const connectionsClient = getConnectionsClient(BASE_URL, AUTH_TOKEN);
const packagesClient = getPackagesClient(BASE_URL, AUTH_TOKEN);

/**
 * Setup data structure for packages test
 */
type SetupData = {
   projectName: string;
   bigqueryConnectionName: string | null;
};

/**
 * Setup function - runs once before all VUs
 * Creates a project and a BigQuery connection (if credentials available)
 */
export function setup(): SetupData {
   // Validate server is up and initialized before proceeding
   validateServerIsUpAndInitialized();

   console.log("Setup: Starting package load test setup...");
   const projectName = generateTestName("load-test-pkg-project");
   console.log(`Setup: Creating project ${projectName}...`);

   const setupProjectResponse = projectsClient.createProject(
      {
         name: projectName,
      },
      { tags: { name: "setup_create_project" } },
   );

   const status = setupProjectResponse.response.status;
   console.log(`Setup: Project creation response status: ${status}`);

   if (status !== 200 && status !== 201) {
      const errorBody =
         setupProjectResponse.response.body || "no error details";
      const errorMessage =
         status === 501
            ? `Project creation not available (config may be frozen or endpoint not implemented). Status: ${status}`
            : `Failed to setup project for packages. Status: ${status}, Body: ${errorBody}`;
      throw new Error(errorMessage);
   }

   console.log(
      `Setup: Project ${projectName} created successfully, verifying...`,
   );

   // Verify project was created and is accessible
   const verifyProjectResponse = projectsClient.getProject(
      projectName,
      undefined,
      { tags: { name: "setup_verify_project" } },
   );

   if (verifyProjectResponse.response.status !== 200) {
      const errorBody =
         verifyProjectResponse.response.body || "no error details";
      throw new Error(
         `Project ${projectName} was created but cannot be accessed. Status: ${verifyProjectResponse.response.status}, Body: ${errorBody}. This may indicate the database was reset during setup.`,
      );
   }

   const createdProject = verifyProjectResponse.data;
   console.log(
      `Setup: Created and verified project ${projectName} for packages testing. Location: ${createdProject.location || "default"}. All VUs will use this same project.`,
   );

   let bigqueryConnectionName: string | null = null;

   // Setup BigQuery connection if credentials are available
   if (HAS_BIGQUERY_CREDENTIALS) {
      console.log(
         "Setup: BigQuery credentials available, creating connection...",
      );
      bigqueryConnectionName = "bigquery";
      const bigqueryConnection: Connection = {
         name: bigqueryConnectionName,
         type: ConnectionTypeEnum.bigquery,
         attributes: {
            dialectName: "bigquery",
            isPool: false,
            canPersist: true,
            canStream: true,
         },
         bigqueryConnection: {
            location: "US",
            serviceAccountKeyJson: __ENV.GOOGLE_APPLICATION_CREDENTIALS,
         },
      };

      console.log(
         `Setup: Calling createConnection for ${bigqueryConnectionName}...`,
      );
      const createConnectionResponse = connectionsClient.createConnection(
         projectName,
         bigqueryConnectionName,
         bigqueryConnection,
         { tags: { name: "setup_create_connection" } },
      );
      console.log(
         `Setup: createConnection response status: ${createConnectionResponse.response.status}`,
      );

      if (
         createConnectionResponse.response.status !== 200 &&
         createConnectionResponse.response.status !== 201
      ) {
         console.warn(
            `Failed to create BigQuery connection: ${createConnectionResponse.response.status} - ${createConnectionResponse.response.body}. Continuing without BigQuery connection.`,
         );
         bigqueryConnectionName = null;
      } else {
         console.log(`Setup: BigQuery connection created, verifying...`);
         // Verify connection was created successfully
         const getConnectionResponse = connectionsClient.getConnection(
            projectName,
            bigqueryConnectionName,
            { tags: { name: "setup_verify_connection" } },
         );
         console.log(
            `Setup: getConnection response status: ${getConnectionResponse.response.status}`,
         );

         if (getConnectionResponse.response.status !== 200) {
            console.warn(
               `BigQuery connection creation succeeded but verification failed (status ${getConnectionResponse.response.status}). Setting bigqueryConnectionName to null to skip BigQuery packages.`,
            );
            bigqueryConnectionName = null;
         } else {
            console.log(
               `Setup: Created and verified BigQuery connection ${bigqueryConnectionName} for project ${projectName}`,
            );
         }
      }
   } else {
      console.log(
         "Setup: BigQuery credentials not available, skipping BigQuery connection",
      );
   }

   console.log(
      `Setup: Completed. Returning projectName: ${projectName}, bigqueryConnectionName: ${bigqueryConnectionName}`,
   );
   return { projectName, bigqueryConnectionName };
}

/**
 * Teardown function - runs once after all VUs
 * Cleans up the connection and project created in setup
 * Gracefully handles cases where server is down
 */
export function teardown(data: SetupData) {
   // Teardown runs once at the end of the entire test execution (not per iteration)
   // All packages created during iterations are already deleted in each iteration
   // Deleting the project will cascade delete all connections (including BigQuery)
   // via deleteConnectionsByProjectId in the database
   if (data && data.projectName) {
      console.log(
         `Teardown: Deleting project ${data.projectName} (this will cascade delete all connections including ${data.bigqueryConnectionName || "none"} and any remaining packages via database cascade delete)`,
      );
      try {
         const deleteResponse = projectsClient.deleteProject(data.projectName, {
            tags: { name: "teardown_delete_project" },
         });
         const deleteStatus = deleteResponse.response.status;
         if (deleteStatus === 0) {
            console.warn(
               `Teardown: Could not connect to server to delete project ${data.projectName} (server may be down)`,
            );
         } else if (deleteStatus !== 200 && deleteStatus !== 204) {
            const errorBody =
               typeof deleteResponse.response.body === "string"
                  ? deleteResponse.response.body
                  : String(deleteResponse.response.body || "");
            console.warn(
               `Teardown: Failed to delete project ${data.projectName}: HTTP ${deleteStatus}. Response: ${errorBody}`,
            );
         } else {
            console.log(
               `Teardown: Successfully deleted project ${data.projectName} (status ${deleteStatus}). All connections and packages were cascade deleted.`,
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
 * Load Test - CRUD Packages
 *
 * This test verifies system performance for package CRUD operations.
 * A single project and BigQuery connection (if available) are created at the beginning
 * and reused across all iterations. Cleanup happens at the end of the test run.
 *
 * Default configuration:
 * - Stages with ramp-up to 100 VUs
 * - 95th percentile response time < 1.5s
 * - Error rate < 2%
 */
export const loadTestPackages: TestPreset = {
   defaultOptions: {
      stages: [
         { duration: "1m", target: 10 }, // warm-up
         { duration: "5m", target: 25 }, // sustained load
         { duration: "1m", target: 0 }, // ramp down
      ],
      thresholds: {
         // Global thresholds
         http_req_duration: ["p(90)<1000", "p(95)<1500", "p(99)<2500"],
         http_req_waiting: ["p(95)<1200"],
         http_req_failed: ["rate<0.02"],
         checks: ["rate>0.98"],
         dropped_iterations: ["count==0"],
         // Per-operation thresholds (C, R, U, D)
         "http_req_duration{name:create_package}": [
            "p(90)<1000",
            "p(95)<1500",
            "p(99)<2500",
         ],
         "http_req_duration{name:get_package}": [
            "p(90)<1000",
            "p(95)<1500",
            "p(99)<2500",
         ],
         "http_req_duration{name:list_packages}": [
            "p(90)<1000",
            "p(95)<1500",
            "p(99)<2500",
         ],
         "http_req_duration{name:update_package}": [
            "p(90)<1000",
            "p(95)<1500",
            "p(99)<2500",
         ],
         "http_req_duration{name:delete_package}": [
            "p(90)<1000",
            "p(95)<1500",
            "p(99)<2500",
         ],
      },
   },
   run: (data: unknown | undefined) => {
      const setupData = data as SetupData;
      if (!setupData || !setupData.projectName) {
         console.error("Setup data not available, skipping package CRUD test");
         return;
      }

      const { projectName, bigqueryConnectionName } = setupData;

      // Ensure we're using the same project and connection throughout the test
      // All VUs will use the projectName and bigqueryConnectionName from setup
      if (!projectName) {
         console.error("Project name not available from setup");
         return;
      }

      // Verify project still exists before proceeding (defensive check)
      const verifyProjectResponse = projectsClient.getProject(
         projectName,
         undefined,
         { tags: { name: "verify_project_before_use" } },
      );
      if (verifyProjectResponse.response.status !== 200) {
         console.error(
            `Project ${projectName} is not accessible (status ${verifyProjectResponse.response.status}). Skipping this iteration. This may indicate the project was deleted or the server was reset.`,
         );
         return;
      }
      // Small delay after verification to reduce rapid requests
      sleep(0.2);

      // Get available packages that are both in the directory and whitelisted
      // Filter out BigQuery packages if BigQuery connection is not available
      let availablePackages = getAvailablePackages();
      if (!bigqueryConnectionName) {
         availablePackages = availablePackages.filter(
            (pkg) => !pkg.startsWith("bigquery-"),
         );
         console.log(
            `No BigQuery connection available. Filtered out BigQuery packages. Using project: ${projectName}`,
         );
      } else {
         console.log(
            `Using project: ${projectName} with BigQuery connection: ${bigqueryConnectionName}`,
         );
      }

      if (availablePackages.length === 0) {
         console.warn(
            "No available whitelisted packages found, skipping package CRUD tests",
         );
         return;
      }

      // Pick a package from available whitelisted packages (cycle through them)
      // Use VU iteration number to cycle through packages
      const packageIndex = __VU % availablePackages.length;
      const selectedPackage = availablePackages[packageIndex];
      if (!selectedPackage) {
         console.error("No package selected for load testing");
         return;
      }

      // Construct absolute path to the selected package
      const basePath = __ENV.PWD || "";
      const packageLocation = basePath
         ? `${basePath}/packages/${selectedPackage}`
         : `./packages/${selectedPackage}`;

      // Delay before package creation to avoid rapid-fire requests
      sleep(0.2);

      group("Packages CRUD", () => {
         // Create package with real location
         group("Create Package", () => {
            const createPackageResponse = packagesClient.createPackage(
               projectName,
               {
                  name: selectedPackage,
                  description: `Test package for load testing`,
                  location: packageLocation,
               },
               { tags: { name: "create_package" } },
            );
            const createStatus = createPackageResponse.response.status;
            check(createPackageResponse.response, {
               "create package request successful": (r) =>
                  r.status === 200 || r.status === 201,
            });
            // Skip remaining operations if server is down
            if (createStatus === 0) {
               return;
            }
            if (createStatus !== 200 && createStatus !== 201) {
               // Log error details for debugging
               const responseBody = createPackageResponse.response.body;
               const errorBody =
                  typeof responseBody === "string"
                     ? responseBody
                     : responseBody instanceof ArrayBuffer
                       ? new TextDecoder().decode(responseBody)
                       : String(responseBody || "");
               const isConnectionError =
                  errorBody.includes("connection") ||
                  errorBody.includes("Connection") ||
                  errorBody.includes("compiling model") ||
                  errorBody.includes("Error(s) compiling");
               if (
                  isConnectionError &&
                  selectedPackage.startsWith("bigquery-")
               ) {
                  console.warn(
                     `Package ${selectedPackage} failed due to connection/compilation error (status ${createStatus}) in project ${projectName}. This may indicate the BigQuery connection ${bigqueryConnectionName || "N/A"} is not properly configured.`,
                  );
               } else {
                  console.warn(
                     `Package ${selectedPackage} creation failed with status ${createStatus} in project ${projectName}. Server should have cleaned up the directory.`,
                  );
               }
               return; // Skip remaining tests if create failed, but continue with same project/connection
            }
            // Wait after package creation to allow compilation/loading to complete
            sleep(0.2);
         });

         // Read package
         group("Get Package", () => {
            const getPackageResponse = packagesClient.getPackage(
               projectName,
               selectedPackage,
               undefined,
               { tags: { name: "get_package" } },
            );
            const getStatus = getPackageResponse.response.status;
            check(getPackageResponse.response, {
               "get package request successful": (r) => r.status === 200,
            });
            if (getStatus === 0) {
               return;
            }
            sleep(1); // Delay between operations
         });

         // List packages
         group("List Packages", () => {
            const listPackagesResponse = packagesClient.listPackages(
               projectName,
               { tags: { name: "list_packages" } },
            );
            const listStatus = listPackagesResponse.response.status;
            check(listPackagesResponse.response, {
               "list packages request successful": (r) => r.status === 200,
            });
            if (listStatus === 0) {
               return;
            }
            sleep(0.2); // Delay between operations
         });

         // Update package
         group("Update Package", () => {
            const updatePackageResponse = packagesClient.updatePackage(
               projectName,
               selectedPackage,
               {
                  name: selectedPackage,
                  description: "Updated description for load testing",
               },
               { tags: { name: "update_package" } },
            );
            const updateStatus = updatePackageResponse.response.status;
            check(updatePackageResponse.response, {
               "update package request successful": (r) => r.status === 200,
            });
            if (updateStatus === 0) {
               return;
            }
            sleep(0.2); // Delay before delete to allow update to complete
         });

         // Delete package
         group("Delete Package", () => {
            const deletePackageResponse = packagesClient.deletePackage(
               projectName,
               selectedPackage,
               { tags: { name: "delete_package" } },
            );
            check(deletePackageResponse.response, {
               "delete package request successful": (r) =>
                  r.status === 200 || r.status === 204,
            });
            sleep(0.2); // Wait after delete to allow cleanup
         });
      });
   },
};

export const options = loadTestPackages.defaultOptions;
export default loadTestPackages.run;
