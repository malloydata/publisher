import { check, sleep } from "k6";
import {
   getModelData,
   getModels,
   getPackages,
   getViews,
   queryModelView,
} from "./common.ts";

/**
 * Load Test - Testing system under normal load
 *
 * This test verifies system performance under expected normal load conditions.
 * It simulates multiple concurrent users to ensure the system handles typical traffic.
 *
 * Default configuration:
 * - 50 virtual users
 * - 5 minutes duration
 * - 95th percentile response time < 1s
 * - Error rate < 5%
 */
export const loadTest: TestPreset = {
   defaultOptions: {
      vus: 50,
      duration: "5m",
      thresholds: {
         http_req_duration: ["p(95)<1000"],
         http_req_failed: ["rate<0.05"],
      },
   },
   run: () => {
      const packages = getPackages();
      for (const pkg of packages) {
         for (const model of getModels(pkg.name)) {
            const modelData = getModelData(pkg.name, model.path);
            for (const view of getViews(modelData)) {
               const queryResponse = queryModelView(
                  pkg.name,
                  model.path,
                  view.sourceName,
                  view.viewName,
               );

               check(queryResponse, {
                  "model view query successful": (r) => r.status === 200,
                  "model view query response time < 500ms": (r) =>
                     r.timings.duration < 500,
               });
               sleep(0.1);
            }
         }
      }
   },
};

export const options = loadTest.defaultOptions;
export default loadTest.run;
