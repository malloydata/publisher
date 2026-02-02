import { sleep } from "k6";
import { validateServerIsUpAndInitialized } from "../utils/common.ts";
import {
   loadTestConnections,
   setup as setupConnections,
   teardown as teardownConnections,
} from "./load-test-crud-connections.ts";
import {
   loadTestPackages,
   setup as setupPackages,
   teardown as teardownPackages,
} from "./load-test-crud-packages.ts";
import { loadTestProjects } from "./load-test-crud-projects.ts";

/**
 * Combined setup data structure for all test suites
 */
type CombinedSetupData = {
   connections: { projectName: string };
   packages: { projectName: string; bigqueryConnectionName: string | null };
};

/**
 * Load Test - Testing CRUD operations under normal load
 *
 * This test combines all three CRUD test suites:
 * 1. Projects CRUD (each iteration)
 * 2. Connections CRUD (setup project once, cleanup at end)
 * 3. Packages CRUD (setup project + bigquery connection once, cleanup at end)
 *
 * Default configuration:
 * - Stages with ramp-up to 150 VUs
 * - 95th percentile response time < 1.5s
 * - Error rate < 2%
 *
 * Main test function - receives setup data from k6 when setup() is present
 */
function runTest(data?: CombinedSetupData) {
   // ========================================================================
   // 1. CRUD Projects (each iteration)
   // ========================================================================
   loadTestProjects.run();

   sleep(0.1);

   // ========================================================================
   // 2. CRUD Connections (reuse project from setup)
   // ========================================================================
   if (data && data.connections && data.connections.projectName) {
      // Extract the run function and call it with data
      // Type assertion needed because TestPreset interface doesn't reflect setup data parameter
      (loadTestConnections.run as (data: { projectName: string }) => void)(
         data.connections,
      );
   } else {
      console.error(
         "Connections setup data not available, skipping connections test",
      );
   }

   sleep(0.1);

   // ========================================================================
   // 3. CRUD Packages (reuse project and connection from setup)
   // ========================================================================
   if (data && data.packages && data.packages.projectName) {
      // Extract the run function and call it with data
      // Type assertion needed because TestPreset interface doesn't reflect setup data parameter
      (
         loadTestPackages.run as (data: {
            projectName: string;
            bigqueryConnectionName: string | null;
         }) => void
      )(data.packages);
   } else {
      console.error(
         "Packages setup data not available, skipping packages test",
      );
   }

   sleep(0.1);
}

export const loadTest: TestPreset = {
   defaultOptions: {
      stages: [
         { duration: "1m", target: 20 }, // warm-up
         { duration: "2m", target: 50 }, // load
         { duration: "5m", target: 100 }, // sustained load
         { duration: "2m", target: 150 }, // near saturation
         { duration: "1m", target: 0 }, // ramp down
      ],
      thresholds: {
         http_req_duration: ["p(90)<1000", "p(95)<1500", "p(99)<2500"],
         http_req_waiting: ["p(95)<1200"],
         http_req_failed: ["rate<0.01"],
         checks: ["rate>0.99"],
         dropped_iterations: ["count==0"],
      },
   },
   run: runTest as () => void,
};

/**
 * Combined setup function - runs once before all VUs
 * Sets up projects and connections for connections and packages tests
 */
export function setup(): CombinedSetupData {
   // Validate server is up and initialized before proceeding
   validateServerIsUpAndInitialized();

   console.log("Setting up test infrastructure...");

   const connectionsData = setupConnections();
   const packagesData = setupPackages();

   return {
      connections: connectionsData,
      packages: packagesData,
   };
}

/**
 * Combined teardown function - runs once after all VUs
 * Cleans up all resources created in setup
 */
export function teardown(data: CombinedSetupData) {
   console.log("Tearing down test infrastructure...");

   if (data) {
      if (data.connections) {
         teardownConnections(data.connections);
      }
      if (data.packages) {
         teardownPackages(data.packages);
      }
   }
}

export const options = loadTest.defaultOptions;
// k6 will pass setup data to the default export function when setup() is present
export default runTest;
