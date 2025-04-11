import { check, sleep } from "k6";
import {
   getPackages,
   getModels,
   getModelData,
   getViews,
   queryModelView,
} from "./common.ts";

/**
 * Smoke Test - Basic functionality test with minimal load
 *
 * This test verifies that the system works under minimal load conditions.
 * It uses a single virtual user to ensure basic functionality.
 *
 * Default configuration:
 * - 1 virtual user
 * - 1 minute duration
 * - 95th percentile response time < 5s
 * - Error rate < 1%
 */

export const smokeTest: TestPreset = {
   defaultOptions: {
      vus: 1,
      duration: "1m",
      thresholds: {
         http_req_duration: ["p(95)<5000"],
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

export const options = smokeTest.defaultOptions;
export default smokeTest.run;
