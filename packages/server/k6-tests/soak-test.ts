import { check, sleep } from "k6";
import {
   queryModelView,
   getViews,
   getModelData,
   getModels,
   getPackages,
} from "./common.ts";

/**
 * Soak Test - Testing system under sustained load
 *
 * This test verifies system stability and performance over an extended period
 * to identify memory leaks, resource exhaustion, or degradation over time.
 *
 * Default configuration:
 * - 10 virtual users
 * - 1 hour duration
 * - 95th percentile response time < 1s
 * - Error rate < 1%
 */
export const soakTest: TestPreset = {
   defaultOptions: {
      vus: 10,
      duration: "1h",
      thresholds: {
         http_req_duration: ["p(95)<1000"],
         http_req_failed: ["rate<0.01"],
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
               sleep(1);
            }
         }
      }
   },
};

export const options = soakTest.defaultOptions;
export default soakTest.run;
