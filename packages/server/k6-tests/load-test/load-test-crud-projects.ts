import { check, group, sleep } from "k6";
import { getProjectsClient } from "../utils/client_factory.ts";
import { AUTH_TOKEN, BASE_URL, generateTestName } from "../utils/common.ts";

const projectsClient = getProjectsClient(BASE_URL, AUTH_TOKEN);

/**
 * Load Test - CRUD Projects
 *
 * This test verifies system performance for project CRUD operations.
 * Each iteration performs a full CRUD cycle on a project.
 *
 * Default configuration:
 * - Stages with ramp-up to 100 VUs
 * - 95th percentile response time < 1.5s
 * - Error rate < 2%
 */
export const loadTestProjects: TestPreset = {
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
         "http_req_duration{name:create_project}": [
            "p(90)<1000",
            "p(95)<1500",
            "p(99)<2500",
         ],
         "http_req_duration{name:get_project}": [
            "p(90)<1000",
            "p(95)<1500",
            "p(99)<2500",
         ],
         "http_req_duration{name:list_projects}": [
            "p(90)<1000",
            "p(95)<1500",
            "p(99)<2500",
         ],
         "http_req_duration{name:update_project}": [
            "p(90)<1000",
            "p(95)<1500",
            "p(99)<2500",
         ],
         "http_req_duration{name:delete_project}": [
            "p(90)<1000",
            "p(95)<1500",
            "p(99)<2500",
         ],
      },
   },
   run: () => {
      const projectName = generateTestName("load-test-project");

      group("Projects CRUD", () => {
         // Create
         group("Create Project", () => {
            const createProjectResponse = projectsClient.createProject(
               {
                  name: projectName,
                  location: "",
               },
               { tags: { name: "create_project" } },
            );
            const createStatus = createProjectResponse.response.status;
            check(createProjectResponse.response, {
               "create project request successful": (r) =>
                  r.status === 200 || r.status === 201,
            });
            // Skip remaining operations if server is down
            if (createStatus === 0 || createStatus === 501) {
               console.log(`Create project request failed: ${createStatus}`);
               return;
            }
         });

         // Read
         group("Get Project", () => {
            const getProjectResponse = projectsClient.getProject(
               projectName,
               undefined,
               { tags: { name: "get_project" } },
            );
            const getStatus = getProjectResponse.response.status;
            check(getProjectResponse.response, {
               "get project request successful": (r) => r.status === 200,
            });
            if (getStatus === 0 || getStatus === 501) {
               console.log(`Get project request failed: ${getStatus}`);
               return;
            }
         });

         // List
         group("List Projects", () => {
            const listProjectsResponse = projectsClient.listProjects({
               tags: { name: "list_projects" },
            });
            const listStatus = listProjectsResponse.response.status;
            check(listProjectsResponse.response, {
               "list projects request successful": (r) => r.status === 200,
            });
            if (listStatus === 0 || listStatus === 501) {
               console.log(`List projects request failed: ${listStatus}`);
               return;
            }
         });

         // Update
         group("Update Project", () => {
            const updateProjectResponse = projectsClient.updateProject(
               projectName,
               {
                  name: projectName,
                  readme: "Updated readme for load testing",
               },
               { tags: { name: "update_project" } },
            );
            const updateStatus = updateProjectResponse.response.status;
            check(updateProjectResponse.response, {
               "update project request successful": (r) => r.status === 200,
            });
            if (updateStatus === 0 || updateStatus === 501) {
               console.log(`Update project request failed: ${updateStatus}`);
               return;
            }
         });

         // Delete
         group("Delete Project", () => {
            const deleteProjectResponse = projectsClient.deleteProject(
               projectName,
               { tags: { name: "delete_project" } },
            );
            const deleteStatus = deleteProjectResponse.response.status;
            check(deleteProjectResponse.response, {
               "delete project request successful": (r) =>
                  r.status === 200 || r.status === 204,
            });
            if (deleteStatus === 0 || deleteStatus === 501) {
               console.log(`Delete project request failed: ${deleteStatus}`);
               return;
            }
         });
      });

      sleep(0.1);
   },
};

export const options = loadTestProjects.defaultOptions;
export default loadTestProjects.run;
