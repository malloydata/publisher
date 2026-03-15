import { check, sleep } from "k6";
import {
   getAvailablePackages,
   getModelData,
   getModels,
   getPackages,
   getViews,
   queryModelView,
   validateServerIsUpAndInitialized,
} from "../utils/common.ts";

/**
 * Setup function - runs once before all VUs
 * Validates server is up and initialized before running tests
 */
export function setup() {
   validateServerIsUpAndInitialized();
}

/**
 * Smoke Test - Basic functionality verification
 *
 * This test verifies that the system's core functionality works correctly
 * under minimal load. It's typically the first test run to ensure basic
 * connectivity and functionality before running more intensive tests.
 *
 * Default configuration:
 * - 5 virtual user
 * - 1 minute duration
 * - 90th percentile response time < 0.8s
 * - 95th percentile response time < 1s
 * - 99th percentile response time < 1.5s
 * - Error rate < 1%
 * - Dropped iterations should be 0
 */
export const smokeTest: TestPreset = {
   defaultOptions: {
      vus: 5,
      duration: "1m",
      thresholds: {
         http_req_duration: ["p(90)<800", "p(95)<1000", "p(99)<1500"],
         http_req_failed: ["rate<0.01"],
         http_req_waiting: ["p(95)<1200"],
         checks: ["rate>0.99"],
         dropped_iterations: ["count==0"],
         // Per-operation thresholds
         "http_req_duration{name:list_packages}": [
            "p(90)<800",
            "p(95)<1000",
            "p(99)<1500",
         ],
         "http_req_duration{name:list_models}": [
            "p(90)<800",
            "p(95)<1000",
            "p(99)<1500",
         ],
         "http_req_duration{name:get_model}": [
            "p(90)<800",
            "p(95)<1000",
            "p(99)<1500",
         ],
         "http_req_duration{name:execute_query}": [
            "p(90)<800",
            "p(95)<1000",
            "p(99)<1500",
         ],
      },
   },
   run: () => {
      // Test 1: GET /projects/{projectName}/packages - List packages endpoint
      const packages = getPackages();
      if (packages.length === 0) {
         throw new Error(
            "No packages found! Check PUBLISHER_URL and PROJECT_NAME environment variables.",
         );
      }

      const availablePackages = getAvailablePackages();
      const MAX_VIEWS_PER_MODEL = parseInt(__ENV.K6_MAX_VIEWS_PER_MODEL || "5");
      const DEBUG = __ENV.K6_DEBUG === "true";

      let packagesTested = 0;
      let modelsTested = 0;
      let queriesTested = 0;

      for (const pkg of packages) {
         if (!availablePackages.includes(pkg.name)) {
            continue;
         }

         if (DEBUG) {
            console.log(`Package: ${JSON.stringify(pkg)}`);
         }

         packagesTested++;

         // Test 2: GET /projects/{projectName}/packages/{packageName}/models - List models endpoint
         const models = getModels(pkg.name, pkg.versionId);
         if (models.length === 0) {
            continue;
         }

         // Test a sample of models (first model per package) to keep test fast
         const modelToTest = models[0];
         if (!modelToTest) {
            continue;
         }

         modelsTested++;

         // Test 3: GET /projects/{projectName}/packages/{packageName}/models/{modelPath} - Get model endpoint
         const modelData = getModelData(
            pkg.name,
            modelToTest.path,
            pkg.versionId,
         );

         // Test 4: POST /projects/{projectName}/packages/{packageName}/models/{modelPath}/query - Query endpoint
         const views = Array.from(getViews(modelData));
         if (views.length === 0) {
            continue;
         }

         // Test first N views per model (configurable via MAX_VIEWS_PER_MODEL env var)
         const viewsToTest = views.slice(0, MAX_VIEWS_PER_MODEL);
         for (const view of viewsToTest) {
            queriesTested++;
            const queryResponse = queryModelView(
               pkg.name,
               modelToTest.path,
               view.sourceName,
               view.viewName,
               pkg.versionId,
            );

            if (DEBUG) {
               console.log(
                  `Query response status: ${queryResponse.status} for ${pkg.name}/${modelToTest.path}/${view.sourceName}/${view.viewName}`,
               );
            }

            check(queryResponse, {
               "execute query request successful": (r) => r.status === 200,
            });
            sleep(1);
         }
      }

      if (DEBUG) {
         console.log(
            `Smoke test summary: ${packagesTested} packages, ${modelsTested} models, ${queriesTested} queries tested`,
         );
      }
   },
};

export const options = smokeTest.defaultOptions;
export default smokeTest.run;
