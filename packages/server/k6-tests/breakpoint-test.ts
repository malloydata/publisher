import { check, sleep } from "k6";
import {
   getAvailablePackages,
   getModelData,
   getModels,
   getPackages,
   getViews,
   queryModelView,
} from "./common.ts";

/**
 * Breakpoint Test - Testing system to find its breaking point
 *
 * This test gradually increases load to identify the system's maximum capacity
 * and the point at which performance begins to degrade.
 *
 * Default configuration:
 * - Stages:
 *   - 2 minutes at 50 users
 *   - 2 minutes at 100 users
 *   - 2 minutes at 150 users
 *   - 2 minutes at 200 users
 *   - 2 minutes ramp-down to 0 users
 * - 95th percentile response time < 3s
 * - Error rate < 15%
 */
export const breakpointTest: TestPreset = {
   defaultOptions: {
      stages: [
         { duration: "2m", target: 50 }, // Ramp up to 50 users
         { duration: "2m", target: 100 }, // Ramp up to 100 users
         { duration: "2m", target: 150 }, // Ramp up to 150 users
         { duration: "2m", target: 200 }, // Ramp up to 200 users
         { duration: "2m", target: 0 }, // Ramp down to 0 users
      ],
      thresholds: {
         http_req_duration: ["p(95)<3000"],
         http_req_failed: ["rate<0.15"],
      },
   },
   run: () => {
      const packages = getPackages();
      const availablePackages = getAvailablePackages();
      for (const pkg of packages) {
         if (!availablePackages.includes(pkg.name)) {
            continue;
         }
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

export const options = breakpointTest.defaultOptions;
export default breakpointTest.run;
