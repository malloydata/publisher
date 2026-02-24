import { afterEach, beforeEach, describe, expect, it, mock } from "bun:test";
import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "fs";
import * as path from "path";
import * as sinon from "sinon";
import { components } from "../api";
import { isPublisherConfigFrozen } from "../config";
import { TEMP_DIR_PATH } from "../constants";
import { Project } from "./project";
import { ProjectStore } from "./project_store";

type MockData = Record<string, unknown>;

mock.module("../storage/StorageManager", () => {
   return {
      StorageManager: class MockStorageManager {
         async initialize(_reInit?: boolean): Promise<void> {
            return;
         }

         getRepository() {
            return {
               // ===== PROJECT METHODS =====
               listProjects: async (): Promise<unknown[]> => [],

               getProjectById: async (
                  id: string,
               ): Promise<MockData | null> => ({
                  id,
                  name: "test-project",
                  path: "/test/path",
                  description: "Test description",
                  metadata: {},
                  createdAt: new Date(),
                  updatedAt: new Date(),
               }),

               getProjectByName: async (
                  _name: string,
               ): Promise<MockData | null> => {
                  // Return null to simulate "project doesn't exist yet"
                  return null;
               },

               createProject: async (data: MockData): Promise<MockData> => ({
                  id: "test-project-id",
                  name: data.name,
                  path: data.path,
                  description: data.description,
                  metadata: data.metadata,
                  createdAt: new Date(),
                  updatedAt: new Date(),
               }),

               updateProject: async (
                  id: string,
                  data: MockData,
               ): Promise<MockData> => ({
                  id,
                  name: "test-project",
                  path: "/test/path",
                  description: data.description,
                  metadata: {
                     ...(data.metadata || {}),
                     readme: data.readme,
                  },
                  createdAt: new Date(),
                  updatedAt: new Date(),
               }),

               deleteProject: async (_id: string): Promise<void> => {},

               // ===== PACKAGE METHODS =====
               listPackages: async (
                  _projectId: string,
               ): Promise<unknown[]> => [],

               getPackageById: async (
                  id: string,
               ): Promise<MockData | null> => ({
                  id,
                  projectId: "test-project-id",
                  name: "test-package",
                  description: "Test package",
                  manifestPath: "/test/manifest.json",
                  metadata: {},
                  createdAt: new Date(),
                  updatedAt: new Date(),
               }),

               getPackageByName: async (
                  _projectId: string,
                  _name: string,
               ): Promise<MockData | null> => null,

               createPackage: async (data: MockData): Promise<MockData> => ({
                  id: "test-package-id",
                  projectId: data.projectId,
                  name: data.name,
                  description: data.description,
                  manifestPath: data.manifestPath,
                  metadata: data.metadata,
                  createdAt: new Date(),
                  updatedAt: new Date(),
               }),

               updatePackage: async (
                  id: string,
                  data: MockData,
               ): Promise<MockData> => ({
                  id,
                  projectId: "test-project-id",
                  name: "test-package",
                  description: data.description,
                  manifestPath: "/test/manifest.json",
                  metadata: data.metadata || {},
                  createdAt: new Date(),
                  updatedAt: new Date(),
               }),

               deletePackage: async (_id: string): Promise<void> => {},

               // ===== CONNECTION METHODS =====
               listConnections: async (
                  _projectId: string,
               ): Promise<unknown[]> => [],

               getConnectionById: async (
                  id: string,
               ): Promise<MockData | null> => ({
                  id,
                  projectId: "test-project-id",
                  name: "test-connection",
                  type: "postgres",
                  config: {},
                  createdAt: new Date(),
                  updatedAt: new Date(),
               }),

               getConnectionByName: async (
                  _projectId: string,
                  _name: string,
               ): Promise<MockData | null> => null,

               createConnection: async (data: MockData): Promise<MockData> => ({
                  id: "test-connection-id",
                  projectId: data.projectId,
                  name: data.name,
                  type: data.type,
                  config: data.config,
                  createdAt: new Date(),
                  updatedAt: new Date(),
               }),

               updateConnection: async (
                  id: string,
                  data: MockData,
               ): Promise<MockData> => ({
                  id,
                  projectId: "test-project-id",
                  name: "test-connection",
                  type: "postgres",
                  config: data.config || {},
                  createdAt: new Date(),
                  updatedAt: new Date(),
               }),

               deleteConnection: async (_id: string): Promise<void> => {},
            };
         }
      },
      StorageConfig: {} as Record<string, unknown>,
   };
});

type Connection = components["schemas"]["Connection"];

const serverRootPath = path.join(
   TEMP_DIR_PATH,
   "pathways-worker-publisher-project-store-test",
);
const projectName = "organizationName-projectName";

let sandbox: sinon.SinonSandbox;

describe("ProjectStore Service", () => {
   let projectStore: ProjectStore;

   beforeEach(async () => {
      // Clean up any existing test directory
      if (existsSync(serverRootPath)) {
         rmSync(serverRootPath, { recursive: true, force: true });
      }
      mkdirSync(serverRootPath);
      sandbox = sinon.createSandbox();

      // Mock the configuration to prevent initialization errors
      mock(isPublisherConfigFrozen).mockReturnValue(false);
      mock.module("../config", () => ({
         isPublisherConfigFrozen: () => false,
      }));

      // Create project store after mocking
      projectStore = new ProjectStore(serverRootPath);
   });

   afterEach(async () => {
      // Clean up the test directory after each test
      if (existsSync(serverRootPath)) {
         rmSync(serverRootPath, { recursive: true, force: true });
      }
      mkdirSync(serverRootPath);
      sandbox.restore();
   });

   it("should not load a package if the project does not exist", async () => {
      await expect(
         projectStore.getProject("non-existent-project"),
      ).rejects.toThrow();
   });

   it(
      "should create and manage projects with connections",
      async () => {
         // Create a project directory
         const projectPath = path.join(serverRootPath, projectName);
         mkdirSync(projectPath, { recursive: true });
         // Create publisher.json manifest file
         writeFileSync(
            path.join(projectPath, "publisher.json"),
            JSON.stringify({
               name: projectName,
               description: "Test package",
            }),
         );

         // Create publisher config
         const publisherConfigPath = path.join(
            serverRootPath,
            "publisher.config.json",
         );
         writeFileSync(
            publisherConfigPath,
            JSON.stringify({
               frozenConfig: false,
               projects: [
                  {
                     name: projectName,
                     packages: [
                        {
                           name: projectName,
                           location: projectPath,
                        },
                     ],
                     connections: [
                        {
                           name: "testConnection",
                           type: "postgres",
                        },
                     ],
                  },
               ],
            }),
         );

         // Test that the project can be retrieved
         const project = await projectStore.getProject(projectName);
         expect(project).toBeInstanceOf(Project);
         expect(project.metadata.name).toBe(projectName);
      },
      { timeout: 30000 },
   );

   it("should handle multiple projects", async () => {
      const projectName1 = "project1";
      const projectName2 = "project2";
      const projectPath1 = path.join(serverRootPath, projectName1);
      const projectPath2 = path.join(serverRootPath, projectName2);

      // Create project directories
      mkdirSync(projectPath1, { recursive: true });
      mkdirSync(projectPath2, { recursive: true });

      // Create publisher config
      const publisherConfigPath = path.join(
         serverRootPath,
         "publisher.config.json",
      );
      writeFileSync(
         publisherConfigPath,
         JSON.stringify({
            frozenConfig: false,
            projects: [
               {
                  name: projectName1,
                  packages: [
                     {
                        name: projectName1,
                        location: projectPath1,
                     },
                  ],
                  connections: [
                     {
                        name: "testConnection",
                        type: "postgres",
                     },
                  ],
               },
               {
                  name: projectName2,
                  packages: [
                     {
                        name: projectName2,
                        location: projectPath2,
                     },
                  ],
                  connections: [
                     {
                        name: "testConnection2",
                        type: "bigquery",
                        bigqueryConnection: {},
                     },
                  ],
               },
            ],
         }),
      );

      // Create a new project store that will read the configuration
      const newProjectStore = new ProjectStore(serverRootPath);
      await newProjectStore.finishedInitialization;

      // Test that both projects can be listed
      const projects = await newProjectStore.listProjects();
      expect(projects).toBeInstanceOf(Array);
      expect(projects.length).toBe(2);
      expect(projects.map((p) => p.name)).toContain(projectName1);
      expect(projects.map((p) => p.name)).toContain(projectName2);
   });

   it("should handle project updates", async () => {
      // Create a project directory
      const projectPath = path.join(serverRootPath, projectName);
      mkdirSync(projectPath, { recursive: true });
      // Create publisher.json manifest file
      writeFileSync(
         path.join(projectPath, "publisher.json"),
         JSON.stringify({
            name: projectName,
            description: "Test package",
         }),
      );
      // Create publisher config
      const publisherConfigPath = path.join(
         serverRootPath,
         "publisher.config.json",
      );
      writeFileSync(
         publisherConfigPath,
         JSON.stringify({
            frozenConfig: false,
            projects: [
               {
                  name: projectName,
                  packages: [
                     {
                        name: projectName,
                        location: projectPath,
                     },
                  ],
               },
            ],
         }),
      );

      await projectStore.finishedInitialization;

      // Get the project
      const project = await projectStore.getProject(projectName);

      // Update the project
      await project.update({
         name: projectName,
         readme: "Updated README content",
      });

      const readmePath = path.join(
         serverRootPath,
         "publisher_data",
         projectName,
         "README.md",
      );

      expect(existsSync(readmePath)).toBe(true);
      const readmeContent = readFileSync(readmePath, "utf-8");
      expect(readmeContent).toBe("Updated README content");
   });

   it(
      "should handle project reload",
      async () => {
         // Create a project directory
         const projectPath = path.join(serverRootPath, projectName);
         mkdirSync(projectPath, { recursive: true });
         // Create publisher.json manifest file
         writeFileSync(
            path.join(projectPath, "publisher.json"),
            JSON.stringify({
               name: projectName,
               description: "Test package",
            }),
         );

         // Create publisher config
         const publisherConfigPath = path.join(
            serverRootPath,
            "publisher.config.json",
         );
         writeFileSync(
            publisherConfigPath,
            JSON.stringify({
               projects: [
                  {
                     name: projectName,
                     packages: [
                        {
                           name: projectName,
                           location: projectPath,
                        },
                     ],
                  },
               ],
            }),
         );

         // Get the project
         const project1 = await projectStore.getProject(projectName);

         // Get the project again with reload=true
         const project2 = await projectStore.getProject(projectName, true);

         expect(project1).toBeInstanceOf(Project);
         expect(project2).toBeInstanceOf(Project);
         expect(project1.metadata.name).toBe(project2.metadata.name as string);
      },
      { timeout: 30000 },
   );

   it("should handle missing project paths", async () => {
      // Create publisher config with non-existent project path
      const publisherConfigPath = path.join(
         serverRootPath,
         "publisher.config.json",
      );
      writeFileSync(
         publisherConfigPath,
         JSON.stringify({
            projects: [
               {
                  name: projectName,
                  packages: [
                     {
                        name: projectName,
                        location: "/non/existent/path",
                     },
                  ],
               },
            ],
         }),
      );

      // Test that getting the project throws an error
      await expect(projectStore.getProject(projectName)).rejects.toThrow();
   });

   it("should handle invalid publisher config", async () => {
      // Create invalid publisher config
      const publisherConfigPath = path.join(
         serverRootPath,
         "publisher.config.json",
      );
      writeFileSync(publisherConfigPath, "invalid json");

      // Create a new project store that will read the invalid config
      const newProjectStore = new ProjectStore(serverRootPath);

      // Test that the project store handles invalid JSON gracefully by falling back to empty config
      await newProjectStore.finishedInitialization;
      const projects = await newProjectStore.listProjects();
      expect(projects).toEqual([]);
   });

   it("should handle invalid field names in publisher config without crashing", async () => {
      // Create publisher config with invalid field names (ramen instead of name, papa instead of packages)
      const publisherConfigPath = path.join(
         serverRootPath,
         "publisher.config.json",
      );
      writeFileSync(
         publisherConfigPath,
         JSON.stringify({
            frozenConfig: false,
            projects: [
               {
                  invalidKey1: "malloy-samples", // Invalid: should be "name"
                  invalidKey2: [
                     // Invalid: should be "packages"
                     {
                        name: "ecommerce",
                        location:
                           "https://github.com/credibledata/malloy-samples/tree/main/ecommerce",
                     },
                  ],
                  connections: [
                     {
                        name: "bigquery",
                        type: "bigquery",
                     },
                  ],
               },
            ],
         }),
      );

      // Create a new project store that will read the invalid config
      const newProjectStore = new ProjectStore(serverRootPath);

      // Test that the project store handles invalid fields gracefully without crashing
      await newProjectStore.finishedInitialization;
      const projects = await newProjectStore.listProjects();

      // Should not crash and should return empty array since invalid projects are filtered out
      expect(projects).toEqual([]);
   });

   it("should filter out invalid projects from publisher config", async () => {
      // Create publisher config with mix of valid and invalid projects
      const publisherConfigPath = path.join(
         serverRootPath,
         "publisher.config.json",
      );
      const validProjectPath = path.join(serverRootPath, "valid-project");
      mkdirSync(validProjectPath, { recursive: true });
      writeFileSync(
         path.join(validProjectPath, "publisher.json"),
         JSON.stringify({
            name: "valid-project",
            description: "Valid project",
         }),
      );

      writeFileSync(
         publisherConfigPath,
         JSON.stringify({
            frozenConfig: false,
            projects: [
               {
                  // Invalid project: missing "name" field
                  packages: [
                     {
                        name: "package1",
                        location: "./invalid-project",
                     },
                  ],
               },
               {
                  // Invalid project: "invalidKey1" instead of "name"
                  invalidKey1: "invalid-project-2",
                  packages: [
                     {
                        name: "package2",
                        location: "./invalid-project-2",
                     },
                  ],
               },
               {
                  // Invalid project: "invalidKey2" instead of "packages"
                  name: "invalid-project-3",
                  invalidKey2: [
                     {
                        name: "package3",
                        location: "./invalid-project-3",
                     },
                  ],
               },
               {
                  // Valid project
                  name: "valid-project",
                  packages: [
                     {
                        name: "valid-project",
                        location: "./valid-project",
                     },
                  ],
               },
            ],
         }),
      );

      // Create a new project store that will read the config
      const newProjectStore = new ProjectStore(serverRootPath);

      // Test that invalid projects are filtered out
      await newProjectStore.finishedInitialization;
      const projects = await newProjectStore.listProjects();

      // Should only have the valid project
      expect(projects.length).toBe(1);
      expect(projects[0].name).toBe("valid-project");
   });

   it(
      "should handle concurrent project access",
      async () => {
         // Create a project directory
         const projectPath = path.join(serverRootPath, projectName);
         mkdirSync(projectPath, { recursive: true });
         // Create publisher.json manifest file
         writeFileSync(
            path.join(projectPath, "publisher.json"),
            JSON.stringify({
               name: projectName,
               description: "Test package",
            }),
         );

         const publisherConfigPath = path.join(
            serverRootPath,
            "publisher.config.json",
         );
         writeFileSync(
            publisherConfigPath,
            JSON.stringify({
               frozenConfig: false,
               projects: [
                  {
                     name: projectName,
                     packages: [
                        {
                           name: projectName,
                           location: projectPath,
                        },
                     ],
                     connections: [
                        {
                           name: "testConnection",
                           type: "postgres",
                        },
                     ],
                  },
               ],
            }),
         );

         await projectStore.finishedInitialization;

         // Test concurrent access to the same project
         const promises = Array.from({ length: 5 }, () =>
            projectStore.getProject(projectName),
         );

         const projects = await Promise.all(promises);

         expect(projects).toHaveLength(5);
         projects.forEach((project) => {
            expect(project).toBeInstanceOf(Project);
            expect(project.metadata.name).toBe(projectName);
         });
      },
      { timeout: 30000 },
   );
});

describe("Project Service Error Recovery", () => {
   let sandbox: sinon.SinonSandbox;
   let projectStore: ProjectStore;
   const serverRootPath = path.join(
      TEMP_DIR_PATH,
      "pathways-worker-publisher-error-recovery-test",
   );
   const projectName = "organizationName-projectName-error-recovery";
   const testConnections: Connection[] = [
      {
         name: "testConnection",
         type: "postgres",
         postgresConnection: {
            host: "host",
            port: 1234,
            databaseName: "databaseName",
            userName: "userName",
            password: "password",
         },
      },
   ];

   beforeEach(async () => {
      sandbox = sinon.createSandbox();
      mkdirSync(serverRootPath, { recursive: true });

      // Mock the configuration to prevent initialization errors
      mock(isPublisherConfigFrozen).mockReturnValue(false);
      mock.module("../config", () => ({
         isPublisherConfigFrozen: () => false,
      }));

      // Create project store after mocking
      projectStore = new ProjectStore(serverRootPath);
   });

   afterEach(async () => {
      sandbox.restore();
      if (existsSync(serverRootPath)) {
         rmSync(serverRootPath, { recursive: true, force: true });
      }
   });

   describe("Project Loading Error Recovery", () => {
      it("should handle missing project directories gracefully", async () => {
         // Create publisher config with missing project directory
         const publisherConfigPath = path.join(
            serverRootPath,
            "publisher.config.json",
         );
         writeFileSync(
            publisherConfigPath,
            JSON.stringify({
               projects: [
                  {
                     name: projectName,
                     packages: [
                        {
                           name: projectName,
                           location: path.join(
                              serverRootPath,
                              "missing-project",
                           ),
                        },
                     ],
                  },
               ],
            }),
         );

         // Test that the project store handles the missing directory
         await expect(projectStore.getProject(projectName)).rejects.toThrow();
      });

      it(
         "should handle corrupted connection files",
         async () => {
            // Create a project directory
            const projectPath = path.join(serverRootPath, projectName);
            mkdirSync(projectPath, { recursive: true });
            // Create publisher.json manifest file
            writeFileSync(
               path.join(projectPath, "publisher.json"),
               JSON.stringify({
                  name: projectName,
                  description: "Test package",
               }),
            );

            // Create corrupted connections file
            const connectionsPath = path.join(
               projectPath,
               "publisher.connections.json",
            );
            writeFileSync(connectionsPath, "invalid json");

            // Create publisher config
            const publisherConfigPath = path.join(
               serverRootPath,
               "publisher.config.json",
            );
            writeFileSync(
               publisherConfigPath,
               JSON.stringify({
                  projects: [
                     {
                        name: projectName,
                        packages: [
                           {
                              name: projectName,
                              location: projectPath,
                           },
                        ],
                     },
                  ],
               }),
            );

            // Test that the project store handles corrupted connection files gracefully
            // (The current implementation loads the project even with corrupted connection files)
            const project = await projectStore.getProject(projectName);
            expect(project).toBeInstanceOf(Project);
            expect(project.metadata.name).toBe(projectName);
         },
         { timeout: 30000 },
      );
   });

   describe("Project Store State Management", () => {
      it(
         "should maintain consistent state after errors",
         async () => {
            // Create a valid project first
            const projectPath = path.join(serverRootPath, projectName);
            mkdirSync(projectPath, { recursive: true });
            // Create publisher.json manifest file
            writeFileSync(
               path.join(projectPath, "publisher.json"),
               JSON.stringify({
                  name: projectName,
                  description: "Test package",
               }),
            );
            writeFileSync(
               path.join(projectPath, "publisher.connections.json"),
               JSON.stringify(testConnections),
            );

            const publisherConfigPath = path.join(
               serverRootPath,
               "publisher.config.json",
            );
            writeFileSync(
               publisherConfigPath,
               JSON.stringify({
                  projects: [
                     {
                        name: projectName,
                        packages: [
                           {
                              name: projectName,
                              location: projectPath,
                           },
                        ],
                     },
                  ],
               }),
            );

            // Get the project successfully
            const project = await projectStore.getProject(projectName);
            expect(project).toBeInstanceOf(Project);

            // Try to get a non-existent project
            await expect(
               projectStore.getProject("non-existent"),
            ).rejects.toThrow();

            // Verify the original project is still accessible
            const projectAgain = await projectStore.getProject(projectName);
            expect(projectAgain).toBeInstanceOf(Project);
            expect(projectAgain.metadata.name).toBe(projectName);
         },
         { timeout: 30000 },
      );
   });
});
