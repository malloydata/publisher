import { check, sleep } from "k6";
import {
   getModelData,
   getModels,
   getPackages,
   getViews,
   queryModelView,
} from "./common.ts";

/**
 * Stress Test - Testing system under extreme load
 *
 * This test pushes the system beyond normal load to identify breaking points
 * and understand system behavior under stress.
 *
 * Default configuration:
 * - 100 virtual users
 * - 10 minutes duration
 * - 95th percentile response time < 2s
 * - Error rate < 10%
 */
export const stressTest: TestPreset = {
   defaultOptions: {
      vus: 100,
      duration: "10m",
      thresholds: {
         http_req_duration: ["p(95)<2000"],
         http_req_failed: ["rate<0.1"],
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
               sleep(0.05);
            }
         }
      }
   },
};

export const options = stressTest.defaultOptions;

export default stressTest.run;
