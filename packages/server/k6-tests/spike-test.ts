import { check, sleep } from "k6";

import {
   getModelData,
   getModels,
   getPackages,
   getViews,
   queryModelView,
} from "./common.ts";

/**
 * Spike Test - Testing system under sudden spikes of load
 *
 * This test simulates sudden traffic spikes to verify system behavior
 * under rapid load changes and recovery capabilities.
 *
 * Default configuration:
 * - Stages:
 *   - 2 minutes ramp-up to 100 users
 *   - 1 minute at 100 users
 *   - 2 minutes ramp-down to 0 users
 * - 95th percentile response time < 2s
 * - Error rate < 10%
 */
export const spikeTest: TestPreset = {
   defaultOptions: {
      stages: [
         { duration: "2m", target: 100 }, // Ramp up to 100 users
         { duration: "1m", target: 100 }, // Stay at 100 users
         { duration: "2m", target: 0 }, // Ramp down to 0 users
      ],
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
               sleep(0.1);
            }
         }
      }
   },
};

export const options = spikeTest.defaultOptions;

export default spikeTest.run;
