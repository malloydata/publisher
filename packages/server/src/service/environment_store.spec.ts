import {
   afterEach,
   beforeEach,
   describe,
   expect,
   it,
   mock,
   spyOn,
} from "bun:test";
import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "fs";
import { promises as fsPromises } from "fs";
import * as path from "path";
import * as sinon from "sinon";
import { components } from "../api";
import { isPublisherConfigFrozen } from "../config";
import { TEMP_DIR_PATH } from "../constants";
import { BadRequestError } from "../errors";
import { Environment, PackageStatus } from "./environment";
import {
   CloneProgressReporter,
   cloneProgressLabel,
   EnvironmentStore,
   formatReadinessLine,
   GIT_CLONE_OPTIONS,
   resolvePackageLocation,
   stripGitProgressNoise,
} from "./environment_store";

type MockData = Record<string, unknown>;

// Environments the mock database reports at boot. Empty by default, which makes
// initialize() fall back to the config file — the path every other test here
// exercises. Set it to reach the database-restore branch instead, which is the
// one a real restart against an existing publisher.db takes. Read at call time,
// so a test can assign it before constructing the store.
let mockDbEnvironments: unknown[] = [];

mock.module("../storage/StorageManager", () => {
   return {
      StorageManager: class MockStorageManager {
         async initialize(_reInit?: boolean): Promise<void> {
            return;
         }

         getRepository() {
            return {
               // ===== PROJECT METHODS =====
               listEnvironments: async (): Promise<unknown[]> =>
                  mockDbEnvironments,

               getEnvironmentById: async (
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

               getEnvironmentByName: async (
                  _name: string,
               ): Promise<MockData | null> => {
                  // Return null to simulate "project doesn't exist yet"
                  return null;
               },

               createEnvironment: async (
                  data: MockData,
               ): Promise<MockData> => ({
                  id: "test-project-id",
                  name: data.name,
                  path: data.path,
                  description: data.description,
                  metadata: data.metadata,
                  createdAt: new Date(),
                  updatedAt: new Date(),
               }),

               updateEnvironment: async (
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

               deleteEnvironment: async (_id: string): Promise<void> => {},

               // ===== PACKAGE METHODS =====
               listPackages: async (
                  _environmentId: string,
               ): Promise<unknown[]> => [],

               getPackageById: async (
                  id: string,
               ): Promise<MockData | null> => ({
                  id,
                  environmentId: "test-project-id",
                  name: "test-package",
                  description: "Test package",
                  manifestPath: "/test/manifest.json",
                  metadata: {},
                  createdAt: new Date(),
                  updatedAt: new Date(),
               }),

               getPackageByName: async (
                  _environmentId: string,
                  _name: string,
               ): Promise<MockData | null> => null,

               createPackage: async (data: MockData): Promise<MockData> => ({
                  id: "test-package-id",
                  environmentId: data.environmentId,
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
                  environmentId: "test-project-id",
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
                  _environmentId: string,
               ): Promise<unknown[]> => [],

               getConnectionById: async (
                  id: string,
               ): Promise<MockData | null> => ({
                  id,
                  environmentId: "test-project-id",
                  name: "test-connection",
                  type: "postgres",
                  config: {},
                  createdAt: new Date(),
                  updatedAt: new Date(),
               }),

               getConnectionByName: async (
                  _environmentId: string,
                  _name: string,
               ): Promise<MockData | null> => null,

               createConnection: async (data: MockData): Promise<MockData> => ({
                  id: "test-connection-id",
                  environmentId: data.environmentId,
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
                  environmentId: "test-project-id",
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

describe("EnvironmentStore Service", () => {
   let environmentStore: EnvironmentStore;

   beforeEach(async () => {
      // Clean up any existing test directory
      if (existsSync(serverRootPath)) {
         rmSync(serverRootPath, { recursive: true, force: true });
      }
      mkdirSync(serverRootPath);
      // Default every test back to the config-file boot path. bun runs all specs
      // in one process, so a leaked value would silently switch later tests onto
      // the database-restore branch.
      mockDbEnvironments = [];
      sandbox = sinon.createSandbox();

      // Mock the configuration to prevent initialization errors
      mock(isPublisherConfigFrozen).mockReturnValue(false);
      mock.module("../config", () => ({
         isPublisherConfigFrozen: () => false,
      }));

      // Create project store after mocking
      environmentStore = new EnvironmentStore(serverRootPath);
   });

   afterEach(async () => {
      // Clean up the test directory after each test
      if (existsSync(serverRootPath)) {
         rmSync(serverRootPath, { recursive: true, force: true });
      }
      mkdirSync(serverRootPath);
      mockDbEnvironments = [];
      sandbox.restore();
   });

   it("should not load a package if the project does not exist", async () => {
      await expect(
         environmentStore.getEnvironment("non-existent-project"),
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
               environments: [
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
         const project = await environmentStore.getEnvironment(projectName);
         expect(project).toBeInstanceOf(Environment);
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
            environments: [
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
      const newEnvironmentStore = new EnvironmentStore(serverRootPath);
      await newEnvironmentStore.finishedInitialization;

      // Test that both projects can be listed
      const projects = await newEnvironmentStore.listEnvironments();
      expect(projects).toBeInstanceOf(Array);
      expect(projects.length).toBe(2);
      expect(projects.map((p) => p.name)).toContain(projectName1);
      expect(projects.map((p) => p.name)).toContain(projectName2);
   });

   it("should skip a project with invalid startup connection config", async () => {
      const validProjectName = "valid-project";
      const invalidProjectName = "invalid-motherduck-project";
      const validProjectPath = path.join(serverRootPath, validProjectName);
      const invalidProjectPath = path.join(serverRootPath, invalidProjectName);

      mkdirSync(validProjectPath, { recursive: true });
      mkdirSync(invalidProjectPath, { recursive: true });
      writeFileSync(
         path.join(validProjectPath, "publisher.json"),
         JSON.stringify({
            name: validProjectName,
            description: "Valid project",
         }),
      );
      writeFileSync(
         path.join(invalidProjectPath, "publisher.json"),
         JSON.stringify({
            name: invalidProjectName,
            description: "Invalid project",
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
            environments: [
               {
                  name: invalidProjectName,
                  packages: [
                     {
                        name: invalidProjectName,
                        location: invalidProjectPath,
                     },
                  ],
                  connections: [
                     {
                        name: "motherduck",
                        type: "motherduck",
                        motherduckConnection: {},
                     },
                  ],
               },
               {
                  name: validProjectName,
                  packages: [
                     {
                        name: validProjectName,
                        location: validProjectPath,
                     },
                  ],
                  connections: [],
               },
            ],
         }),
      );

      const newEnvironmentStore = new EnvironmentStore(serverRootPath);
      await newEnvironmentStore.finishedInitialization;

      const projects = await newEnvironmentStore.listEnvironments();
      expect(projects.map((p) => p.name)).toEqual([validProjectName]);
      await expect(
         newEnvironmentStore.getEnvironment(invalidProjectName),
      ).rejects.toThrow();

      // The skip is not fatal, so the server still serves. getStatus is the
      // only place that says the environment is missing rather than never
      // configured: without loadErrors the two are indistinguishable.
      const status = await newEnvironmentStore.getStatus();
      expect(status.operationalState).toBe("serving");
      expect(status.environments.map((e) => e.name)).toEqual([
         validProjectName,
      ]);
      expect(status.loadErrors).toHaveLength(1);
      expect(status.loadErrors?.[0]?.environment).toBe(invalidProjectName);
      expect(status.loadErrors?.[0]?.package).toBeUndefined();
      expect(status.loadErrors?.[0]?.message).toBeTruthy();
   });

   it("should omit loadErrors when every environment loads", async () => {
      const projectPath = path.join(serverRootPath, projectName);
      mkdirSync(projectPath, { recursive: true });
      writeFileSync(
         path.join(projectPath, "publisher.json"),
         JSON.stringify({ name: projectName, description: "Test package" }),
      );
      writeFileSync(
         path.join(serverRootPath, "publisher.config.json"),
         JSON.stringify({
            frozenConfig: false,
            environments: [
               {
                  name: projectName,
                  packages: [{ name: projectName, location: projectPath }],
                  connections: [],
               },
            ],
         }),
      );

      const newEnvironmentStore = new EnvironmentStore(serverRootPath);
      await newEnvironmentStore.finishedInitialization;

      // Absent, not an empty array: a healthy status is unchanged by this
      // field's existence.
      const status = await newEnvironmentStore.getStatus();
      expect(status.operationalState).toBe("serving");
      expect(status.loadErrors).toBeUndefined();
      expect("loadErrors" in status).toBe(false);
   });

   it("keeps good packages serving when a sibling's location is bad", async () => {
      // The headline case: one bad location in an environment must not take
      // down its healthy siblings. Two good packages plus one whose location
      // does not exist -> both good ones serve, the bad one is a per-package
      // loadError, and the environment is intact.
      const goodA = path.join(serverRootPath, "good-a");
      const goodB = path.join(serverRootPath, "good-b");
      for (const dir of [goodA, goodB]) {
         mkdirSync(dir, { recursive: true });
         writeFileSync(
            path.join(dir, "publisher.json"),
            JSON.stringify({ name: path.basename(dir) }),
         );
      }
      writeFileSync(
         path.join(serverRootPath, "publisher.config.json"),
         JSON.stringify({
            environments: [
               {
                  name: projectName,
                  packages: [
                     { name: "good-a", location: goodA },
                     { name: "bad", location: "/non/existent/path" },
                     { name: "good-b", location: goodB },
                  ],
                  connections: [],
               },
            ],
         }),
      );

      const newEnvironmentStore = new EnvironmentStore(serverRootPath);
      await newEnvironmentStore.finishedInitialization;

      const status = await newEnvironmentStore.getStatus();
      expect(status.operationalState).toBe("serving");
      expect(status.environments.map((e) => e.name)).toEqual([projectName]);

      const environment = await newEnvironmentStore.getEnvironment(projectName);
      const packages = await environment.listPackages();
      expect(packages.map((p) => p.name).sort()).toEqual(["good-a", "good-b"]);

      expect(status.loadErrors).toHaveLength(1);
      expect(status.loadErrors?.[0]?.environment).toBe(projectName);
      expect(status.loadErrors?.[0]?.package).toBe("bad");
      // The message has to name the location that is actually wrong. The
      // un-mounted package also fails its lazy load on the manifest that was
      // never copied, and reporting that instead points the reader at
      // publisher_data/ for what is really a typo in the config.
      expect(status.loadErrors?.[0]?.message).toContain("/non/existent/path");
      expect(status.loadErrors?.[0]?.message).not.toContain("publisher_data");
   });

   it("keeps sibling packages serving when one sharing their location fails to extract", async () => {
      // Packages grouped under ONE location share a single download, then each
      // is extracted separately. A failure in one extract must not strand the
      // siblings queued behind it: the group-level catch used to wrap the whole
      // loop, so with three packages from one repo a throw on the second meant
      // the third was never copied even though nothing was wrong with it.
      const shared = path.join(serverRootPath, "shared-src");
      mkdirSync(shared, { recursive: true });
      writeFileSync(
         path.join(shared, "publisher.json"),
         JSON.stringify({ name: "shared" }),
      );

      writeFileSync(
         path.join(serverRootPath, "publisher.config.json"),
         JSON.stringify({
            environments: [
               {
                  name: projectName,
                  packages: [
                     { name: "first", location: shared },
                     { name: "middle", location: shared },
                     { name: "last", location: shared },
                  ],
                  connections: [],
               },
            ],
         }),
      );

      // Fail exactly one package's copy. Everything reachable from config is
      // shared across the group (same location, hence the same source path), so
      // the copy is the only per-package step that can realistically fail on
      // its own — the same shape as a GitHub subdirectory that isn't there.
      const realCp = fsPromises.cp;
      const cpStub = sinon.stub(fsPromises, "cp").callsFake((async (
         src: Parameters<typeof realCp>[0],
         dest: Parameters<typeof realCp>[1],
         opts?: Parameters<typeof realCp>[2],
      ) => {
         if (String(dest).endsWith(`${path.sep}middle`)) {
            throw new Error("simulated extract failure for middle");
         }
         return realCp(src, dest, opts);
      }) as typeof realCp);

      try {
         const newEnvironmentStore = new EnvironmentStore(serverRootPath);
         await newEnvironmentStore.finishedInitialization;

         const environment =
            await newEnvironmentStore.getEnvironment(projectName);
         const packages = await environment.listPackages();
         // "last" is the one that regressed: it is queued behind the failure.
         expect(packages.map((p) => p.name).sort()).toEqual(["first", "last"]);

         const status = await newEnvironmentStore.getStatus();
         expect(status.operationalState).toBe("serving");
         expect(status.loadErrors).toHaveLength(1);
         expect(status.loadErrors?.[0]?.package).toBe("middle");
         expect(status.loadErrors?.[0]?.message).toContain(
            "simulated extract failure",
         );
      } finally {
         cpStub.restore();
      }
   });

   it("does not delete the environment directory when a package name is rejected", async () => {
      // A package name that fails the allowlist throws as the FIRST statement of
      // the extract, so the per-package catch runs before any path for it
      // exists. safeJoinUnderRoot deliberately allows a name that resolves to
      // the root, so re-deriving the cleanup target from the name would hand it
      // the whole environment directory and take the healthy siblings with it.
      const good = path.join(serverRootPath, "good-src");
      mkdirSync(good, { recursive: true });
      writeFileSync(
         path.join(good, "publisher.json"),
         JSON.stringify({ name: "good" }),
      );

      writeFileSync(
         path.join(serverRootPath, "publisher.config.json"),
         JSON.stringify({
            environments: [
               {
                  name: projectName,
                  packages: [
                     { name: "good", location: good },
                     { name: ".", location: good },
                  ],
                  connections: [],
               },
            ],
         }),
      );

      const newEnvironmentStore = new EnvironmentStore(serverRootPath);
      await newEnvironmentStore.finishedInitialization;

      // Boot pre-validates the names and skips the environment, so reach the
      // lazy path, which passes the config's package names through unchecked.
      await newEnvironmentStore.getEnvironment(projectName).catch(() => {});

      const envDir = path.join(serverRootPath, "publisher_data", projectName);
      // The environment directory and the healthy sibling must both survive.
      expect(existsSync(envDir)).toBe(true);
      expect(existsSync(path.join(envDir, "good"))).toBe(true);
   });

   it("should not report an environment that is serving even if its database sync fails", async () => {
      // An environment reaches this.environments before addEnvironmentToDatabase
      // runs, so a throw from that tail is caught by the same handler that
      // records load failures. The environment is live and listed, so reporting
      // it as a load failure would make the status contradict itself.
      const projectPath = path.join(serverRootPath, projectName);
      mkdirSync(projectPath, { recursive: true });
      writeFileSync(
         path.join(projectPath, "publisher.json"),
         JSON.stringify({ name: projectName }),
      );
      writeFileSync(
         path.join(serverRootPath, "publisher.config.json"),
         JSON.stringify({
            frozenConfig: false,
            environments: [
               {
                  name: projectName,
                  packages: [{ name: projectName, location: projectPath }],
                  connections: [],
               },
            ],
         }),
      );

      // Stub the prototype before constructing: the constructor starts
      // initialize() immediately, so stubbing the instance would race it.
      sandbox
         .stub(EnvironmentStore.prototype, "addEnvironmentToDatabase")
         .rejects(new Error("database write failed"));

      const newEnvironmentStore = new EnvironmentStore(serverRootPath);
      await newEnvironmentStore.finishedInitialization;

      const status = await newEnvironmentStore.getStatus();
      expect(status.operationalState).toBe("serving");
      // Still serving it, so it must not also be reported as not loaded.
      expect(status.environments.map((e) => e.name)).toEqual([projectName]);
      expect(status.loadErrors).toBeUndefined();
   });

   it("should report an environment that failed to restore from the database", async () => {
      // The database-restore branch, not the config branch: initialize() takes
      // this one whenever INITIALIZE_STORAGE is not "true" and the database
      // already holds environments, which is every restart of a server with a
      // persisted publisher.db. It has its own catch, so a failure recorded only
      // on the config path would be dropped on the path production actually
      // takes.
      const envName = "restored-env";
      const envPath = path.join(serverRootPath, envName);

      // Exists, so the "files missing" branch is skipped, but is a file rather
      // than a directory, so Environment.create throws. Stands in for any stored
      // path that goes bad between restarts.
      writeFileSync(envPath, "not a directory");

      writeFileSync(
         path.join(serverRootPath, "publisher.config.json"),
         JSON.stringify({
            frozenConfig: false,
            environments: [
               {
                  name: envName,
                  packages: [{ name: envName, location: envPath }],
                  connections: [],
               },
            ],
         }),
      );

      mockDbEnvironments = [
         {
            id: "restored-env-id",
            name: envName,
            path: envPath,
            metadata: {},
         },
      ];

      const newEnvironmentStore = new EnvironmentStore(serverRootPath);
      await newEnvironmentStore.finishedInitialization;

      const status = await newEnvironmentStore.getStatus();
      expect(status.operationalState).toBe("serving");
      expect(status.environments.map((e) => e.name)).toEqual([]);
      expect(status.loadErrors).toHaveLength(1);
      expect(status.loadErrors?.[0]?.environment).toBe(envName);
      expect(status.loadErrors?.[0]?.message).toBeTruthy();
   });

   it("should report a package that failed to load while its siblings serve", async () => {
      const goodPackageName = "good-package";
      const badPackageName = "bad-package";
      const goodPackagePath = path.join(serverRootPath, goodPackageName);
      const badPackagePath = path.join(serverRootPath, badPackageName);

      mkdirSync(goodPackagePath, { recursive: true });
      writeFileSync(
         path.join(goodPackagePath, "publisher.json"),
         JSON.stringify({ name: goodPackageName }),
      );
      // The directory exists, so the environment mounts; the package inside it
      // has no manifest, so only that package fails. This is the second, and
      // quieter, failure mode: the environment is present and serving, with a
      // package missing from it.
      mkdirSync(badPackagePath, { recursive: true });

      writeFileSync(
         path.join(serverRootPath, "publisher.config.json"),
         JSON.stringify({
            frozenConfig: false,
            environments: [
               {
                  name: projectName,
                  packages: [
                     { name: goodPackageName, location: goodPackagePath },
                     { name: badPackageName, location: badPackagePath },
                  ],
                  connections: [],
               },
            ],
         }),
      );

      const newEnvironmentStore = new EnvironmentStore(serverRootPath);
      await newEnvironmentStore.finishedInitialization;

      const environment = await newEnvironmentStore.getEnvironment(projectName);
      const packages = await environment.listPackages();
      expect(packages.map((p) => p.name)).toEqual([goodPackageName]);

      const status = await newEnvironmentStore.getStatus();
      expect(status.operationalState).toBe("serving");
      expect(status.loadErrors).toHaveLength(1);
      expect(status.loadErrors?.[0]?.environment).toBe(projectName);
      expect(status.loadErrors?.[0]?.package).toBe(badPackageName);
      expect(status.loadErrors?.[0]?.message).toBeTruthy();
   });

   it("should stop reporting a failed package once it is deleted", async () => {
      const goodPackageName = "good-package";
      const badPackageName = "bad-package";
      const goodPackagePath = path.join(serverRootPath, goodPackageName);
      const badPackagePath = path.join(serverRootPath, badPackageName);

      mkdirSync(goodPackagePath, { recursive: true });
      writeFileSync(
         path.join(goodPackagePath, "publisher.json"),
         JSON.stringify({ name: goodPackageName }),
      );
      mkdirSync(badPackagePath, { recursive: true });

      writeFileSync(
         path.join(serverRootPath, "publisher.config.json"),
         JSON.stringify({
            frozenConfig: false,
            environments: [
               {
                  name: projectName,
                  packages: [
                     { name: goodPackageName, location: goodPackagePath },
                     { name: badPackageName, location: badPackagePath },
                  ],
                  connections: [],
               },
            ],
         }),
      );

      const newEnvironmentStore = new EnvironmentStore(serverRootPath);
      await newEnvironmentStore.finishedInitialization;

      const environment = await newEnvironmentStore.getEnvironment(projectName);
      expect((await newEnvironmentStore.getStatus()).loadErrors).toHaveLength(
         1,
      );

      // A package that failed to load was evicted from `packages`, so the
      // delete takes deletePackage's early return. The failure entry has to be
      // cleared anyway: the caller goes on to drop the package's config row,
      // and a loadError naming a package that is no longer configured sends
      // whoever reads /status hunting for something that isn't there.
      await environment.deletePackage(badPackageName);

      const status = await newEnvironmentStore.getStatus();
      expect(status.operationalState).toBe("serving");
      expect(status.loadErrors).toBeUndefined();
      expect((await environment.listPackages()).map((p) => p.name)).toEqual([
         goodPackageName,
      ]);
   });

   it("should stop reporting a failed package once it loads", async () => {
      const badPackageName = "bad-package";
      const badPackagePath = path.join(serverRootPath, badPackageName);

      // No manifest, so it fails to load and lands in loadErrors.
      mkdirSync(badPackagePath, { recursive: true });

      writeFileSync(
         path.join(serverRootPath, "publisher.config.json"),
         JSON.stringify({
            frozenConfig: false,
            environments: [
               {
                  name: projectName,
                  packages: [
                     { name: badPackageName, location: badPackagePath },
                  ],
                  connections: [],
               },
            ],
         }),
      );

      const newEnvironmentStore = new EnvironmentStore(serverRootPath);
      await newEnvironmentStore.finishedInitialization;

      const environment = await newEnvironmentStore.getEnvironment(projectName);
      expect((await newEnvironmentStore.getStatus()).loadErrors).toHaveLength(
         1,
      );

      // Fix the package where the environment actually reads it, then re-add
      // it. A package that is serving must not still be reported as failed.
      writeFileSync(
         path.join(
            environment.metadata.location as string,
            badPackageName,
            "publisher.json",
         ),
         JSON.stringify({ name: badPackageName }),
      );
      await environment.addPackage(badPackageName);

      const status = await newEnvironmentStore.getStatus();
      expect(status.operationalState).toBe("serving");
      expect(status.loadErrors).toBeUndefined();
      expect((await environment.listPackages()).map((p) => p.name)).toEqual([
         badPackageName,
      ]);
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
            environments: [
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

      await environmentStore.finishedInitialization;

      // Get the project
      const project = await environmentStore.getEnvironment(projectName);

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
               environments: [
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
         const project1 = await environmentStore.getEnvironment(projectName);

         // Get the project again with reload=true
         const project2 = await environmentStore.getEnvironment(
            projectName,
            true,
         );

         expect(project1).toBeInstanceOf(Environment);
         expect(project2).toBeInstanceOf(Environment);
         expect(project1.metadata.name).toBe(project2.metadata.name as string);
      },
      { timeout: 30000 },
   );

   it("isolates a bad package location to that package, keeping the environment", async () => {
      // A package whose location does not exist used to abort the whole
      // environment. It now behaves like a bad manifest: the package is
      // dropped and reported per-package, and the environment still loads.
      const publisherConfigPath = path.join(
         serverRootPath,
         "publisher.config.json",
      );
      writeFileSync(
         publisherConfigPath,
         JSON.stringify({
            environments: [
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

      const newEnvironmentStore = new EnvironmentStore(serverRootPath);
      await newEnvironmentStore.finishedInitialization;

      // The environment loaded rather than being skipped.
      const environment = await newEnvironmentStore.getEnvironment(projectName);
      expect(environment.metadata.name).toBe(projectName);

      // And the failure is reported at the package level, not the env level.
      const status = await newEnvironmentStore.getStatus();
      expect(status.operationalState).toBe("serving");
      expect(status.loadErrors).toHaveLength(1);
      expect(status.loadErrors?.[0]?.environment).toBe(projectName);
      expect(status.loadErrors?.[0]?.package).toBe(projectName);
      expect(status.loadErrors?.[0]?.message).toBeTruthy();
   });

   it("should handle invalid publisher config", async () => {
      // Create invalid publisher config
      const publisherConfigPath = path.join(
         serverRootPath,
         "publisher.config.json",
      );
      writeFileSync(publisherConfigPath, "invalid json");

      // Create a new project store that will read the invalid config
      const newEnvironmentStore = new EnvironmentStore(serverRootPath);

      // Test that the project store handles invalid JSON gracefully by falling back to empty config
      await newEnvironmentStore.finishedInitialization;
      const projects = await newEnvironmentStore.listEnvironments();
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
            environments: [
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
      const newEnvironmentStore = new EnvironmentStore(serverRootPath);

      // Test that the project store handles invalid fields gracefully without crashing
      await newEnvironmentStore.finishedInitialization;
      const projects = await newEnvironmentStore.listEnvironments();

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
            environments: [
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
      const newEnvironmentStore = new EnvironmentStore(serverRootPath);

      // Test that invalid projects are filtered out
      await newEnvironmentStore.finishedInitialization;
      const projects = await newEnvironmentStore.listEnvironments();

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
               environments: [
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

         await environmentStore.finishedInitialization;

         // Test concurrent access to the same project
         const promises = Array.from({ length: 5 }, () =>
            environmentStore.getEnvironment(projectName),
         );

         const projects = await Promise.all(promises);

         expect(projects).toHaveLength(5);
         projects.forEach((project) => {
            expect(project).toBeInstanceOf(Environment);
            expect(project.metadata.name).toBe(projectName);
         });
      },
      { timeout: 30000 },
   );
});

describe("Project Service Error Recovery", () => {
   let sandbox: sinon.SinonSandbox;
   let environmentStore: EnvironmentStore;
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
      environmentStore = new EnvironmentStore(serverRootPath);
   });

   afterEach(async () => {
      sandbox.restore();
      if (existsSync(serverRootPath)) {
         rmSync(serverRootPath, { recursive: true, force: true });
      }
   });

   describe("Project Loading Error Recovery", () => {
      it("keeps the environment when a package directory is missing, reporting per-package", async () => {
         // Same isolation as above, for a relative-to-serverRoot missing dir:
         // the environment loads and the missing package is a per-package
         // loadError rather than an environment-level skip.
         const publisherConfigPath = path.join(
            serverRootPath,
            "publisher.config.json",
         );
         writeFileSync(
            publisherConfigPath,
            JSON.stringify({
               environments: [
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

         const newEnvironmentStore = new EnvironmentStore(serverRootPath);
         await newEnvironmentStore.finishedInitialization;

         const environment =
            await newEnvironmentStore.getEnvironment(projectName);
         expect(environment.metadata.name).toBe(projectName);

         const status = await newEnvironmentStore.getStatus();
         expect(status.operationalState).toBe("serving");
         expect(status.loadErrors).toHaveLength(1);
         expect(status.loadErrors?.[0]?.package).toBe(projectName);
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
                  environments: [
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
            const project = await environmentStore.getEnvironment(projectName);
            expect(project).toBeInstanceOf(Environment);
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
                  environments: [
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
            const project = await environmentStore.getEnvironment(projectName);
            expect(project).toBeInstanceOf(Environment);

            // Try to get a non-existent project
            await expect(
               environmentStore.getEnvironment("non-existent"),
            ).rejects.toThrow();

            // Verify the original project is still accessible
            const projectAgain =
               await environmentStore.getEnvironment(projectName);
            expect(projectAgain).toBeInstanceOf(Environment);
            expect(projectAgain.metadata.name).toBe(projectName);
         },
         { timeout: 30000 },
      );
   });
});

const TRAVERSAL_NAMES: ReadonlyArray<readonly [string, string]> = [
   ["leading traversal", "../etc"],
   ["embedded traversal", "foo/../../bar"],
   ["slash in name", "foo/bar"],
   ["backslash in name", "foo\\bar"],
   ["leading dot", ".staging"],
   ["bare dot-dot", ".."],
   ["bare dot", "."],
   ["empty", ""],
   ["NUL byte", "foo\0bar"],
   ["oversized", "a".repeat(256)],
   ["absolute", "/etc/passwd"],
] as const;

describe("EnvironmentStore path-injection guards", () => {
   let environmentStore: EnvironmentStore;

   beforeEach(async () => {
      if (existsSync(serverRootPath)) {
         rmSync(serverRootPath, { recursive: true, force: true });
      }
      mkdirSync(serverRootPath);
      mock(isPublisherConfigFrozen).mockReturnValue(false);
      mock.module("../config", () => ({
         isPublisherConfigFrozen: () => false,
      }));
      environmentStore = new EnvironmentStore(serverRootPath);
      await environmentStore.finishedInitialization;
   });

   afterEach(() => {
      if (existsSync(serverRootPath)) {
         rmSync(serverRootPath, { recursive: true, force: true });
      }
      mkdirSync(serverRootPath);
   });

   describe("addEnvironment", () => {
      it.each(TRAVERSAL_NAMES)(
         "rejects %s as environment.name (%p)",
         async (_label, name) => {
            await expect(
               environmentStore.addEnvironment({ name } as never, true),
            ).rejects.toBeInstanceOf(BadRequestError);
         },
      );

      it.each(TRAVERSAL_NAMES)(
         "rejects %s as packages[].name (%p)",
         async (_label, packageName) => {
            await expect(
               environmentStore.addEnvironment(
                  {
                     name: "ok-env",
                     packages: [
                        {
                           name: packageName,
                           location: "https://github.com/example/repo",
                        },
                     ],
                  } as never,
                  true,
               ),
            ).rejects.toBeInstanceOf(BadRequestError);
         },
      );
   });

   describe("updateEnvironment", () => {
      it.each(TRAVERSAL_NAMES)(
         "rejects %s as environment.name (%p)",
         async (_label, name) => {
            await expect(
               environmentStore.updateEnvironment({ name } as never),
            ).rejects.toBeInstanceOf(BadRequestError);
         },
      );
   });

   describe("deleteEnvironment", () => {
      it.each(TRAVERSAL_NAMES)(
         "rejects %s as environmentName (%p)",
         async (_label, name) => {
            await expect(
               environmentStore.deleteEnvironment(name),
            ).rejects.toBeInstanceOf(BadRequestError);
         },
      );
   });

   describe("getEnvironment", () => {
      it.each(TRAVERSAL_NAMES)(
         "rejects %s as environmentName (%p)",
         async (_label, name) => {
            await expect(
               environmentStore.getEnvironment(name),
            ).rejects.toBeInstanceOf(BadRequestError);
         },
      );
   });
});

describe("resolvePackageLocation", () => {
   // Built with `path`, not literals: the helper joins with the platform
   // separator and these specs run on windows-latest too (cross-platform-tests).
   const HOME = path.join(path.sep, "home", "tester");
   const CONFIG_DIR = path.join(path.sep, "etc", "publisher");

   it("expands ~/ against the home directory", () => {
      expect(
         resolvePackageLocation("~/my-packages/sales", CONFIG_DIR, HOME),
      ).toBe(path.join(HOME, "my-packages", "sales"));
   });

   it("does not anchor an expanded ~/ path at the config dir", () => {
      // Regression: `~/` satisfies isLocalPath but not path.isAbsolute, so an
      // unexpanded tilde used to be joined onto the anchor, yielding
      // `<anchor>/~/my-packages/sales`.
      const resolved = resolvePackageLocation("~/x", CONFIG_DIR, HOME);
      expect(resolved).not.toContain("~");
      expect(resolved.startsWith(CONFIG_DIR)).toBe(false);
   });

   it("returns an absolute location untouched", () => {
      const absolute = path.join(path.sep, "srv", "packages", "sales");
      expect(resolvePackageLocation(absolute, CONFIG_DIR, HOME)).toBe(absolute);
   });

   it("anchors a relative location at the config dir, not the cwd", () => {
      expect(resolvePackageLocation("./sales", CONFIG_DIR, HOME)).toBe(
         path.join(CONFIG_DIR, "sales"),
      );
   });

   it("anchors a ../ location at the config dir", () => {
      expect(
         resolvePackageLocation("../examples/storefront", CONFIG_DIR, HOME),
      ).toBe(path.join(CONFIG_DIR, "..", "examples", "storefront"));
   });

   it("throws when ~/ cannot be expanded because no home directory is set", () => {
      // An empty home must fail loudly: `path.join("", "x")` would otherwise
      // yield a relative "x" that silently anchors under the config dir.
      expect(() => resolvePackageLocation("~/x", CONFIG_DIR, "")).toThrow(
         /home directory is not set/,
      );
   });

   it("never consults the home directory for non-tilde locations", () => {
      // The same empty home that makes a ~/ location throw must be irrelevant
      // to ./ and absolute locations; home is resolved lazily, tilde-only.
      expect(resolvePackageLocation("./sales", CONFIG_DIR, "")).toBe(
         path.join(CONFIG_DIR, "sales"),
      );
      const absolute = path.join(path.sep, "srv", "packages", "sales");
      expect(resolvePackageLocation(absolute, CONFIG_DIR, "")).toBe(absolute);
   });

   it("expands only the POSIX ~/ prefix; bare ~ and ~user are not home paths", () => {
      // Neither form satisfies isLocalPath, so in practice they are rejected
      // upstream as non-local locations; pin here that the resolver itself
      // never treats them as home-relative either.
      expect(resolvePackageLocation("~", CONFIG_DIR, HOME)).toBe(
         path.join(CONFIG_DIR, "~"),
      );
      expect(resolvePackageLocation("~user/foo", CONFIG_DIR, HOME)).toBe(
         path.join(CONFIG_DIR, "~user", "foo"),
      );
   });
});

describe("Environment.getServingPackageCount", () => {
   const envRoot = path.join(TEMP_DIR_PATH, "serving-count-env");

   beforeEach(() => {
      rmSync(envRoot, { recursive: true, force: true });
      mkdirSync(envRoot, { recursive: true });
   });

   afterEach(() => {
      rmSync(envRoot, { recursive: true, force: true });
   });

   it("excludes a package that is registered SERVING but recorded as failed", async () => {
      // A mount-failed package is seeded SERVING at boot and only pruned by a
      // later side-effect load; if that prune is skipped (a transient DB or
      // memory-pressure error), packages= must still not count it, and it
      // must not overlap load_errors=. The serving count is registered-minus-
      // failed, so it holds regardless of whether the prune ran.
      //
      // Two good and one bad, not one-and-one: this catches both size-only
      // (would say 3) and a full inversion (would say 1), so the count really
      // is "registered minus failed".
      const env = await Environment.create("e", envRoot, []);
      env.setPackageStatus("good-1", PackageStatus.SERVING);
      env.setPackageStatus("good-2", PackageStatus.SERVING);
      env.setPackageStatus("bad", PackageStatus.SERVING);
      env.setPackageMountError("bad", "location does not exist");

      expect(env.getServingPackageCount()).toBe(2);
      // Disjoint from the failure set, so packages= + load_errors= never
      // double-counts "bad".
      expect(env.getFailedPackages().has("bad")).toBe(true);
   });
});

describe("GIT_CLONE_OPTIONS", () => {
   it("clones shallow and single-branch", () => {
      // The load-bearing content: packages serve the default branch's working
      // tree only, so history is pure download cost. simple-git serializes
      // this to `git clone --depth=1 --single-branch`.
      expect(GIT_CLONE_OPTIONS["--depth"]).toBe(1);
      expect(GIT_CLONE_OPTIONS).toHaveProperty("--single-branch", null);
   });
});

describe("stripGitProgressNoise", () => {
   it("drops progress lines and keeps the error", () => {
      const message = [
         "Cloning into '/tmp/x'...",
         "remote: Counting objects: 45% (9/20)",
         "Receiving objects: 99% (990/1000)",
         "fatal: early EOF",
         "fatal: fetch-pack: invalid index-pack output",
      ].join("\n");
      expect(stripGitProgressNoise(message)).toBe(
         "fatal: early EOF\nfatal: fetch-pack: invalid index-pack output",
      );
   });

   it("handles carriage-return separated progress spew", () => {
      // git rewrites progress in place with \r, so the captured stderr is one
      // long line unless split on \r too.
      const message =
         "Receiving objects: 10% (1/10)\rReceiving objects: 50% (5/10)\rfatal: the remote end hung up unexpectedly";
      expect(stripGitProgressNoise(message)).toBe(
         "fatal: the remote end hung up unexpectedly",
      );
   });

   it("returns the input when every line is progress noise", () => {
      const message = "Receiving objects: 10% (1/10)";
      expect(stripGitProgressNoise(message)).toBe(message);
   });
});

describe("cloneProgressLabel", () => {
   it("names the environment, repo, and packages", () => {
      expect(
         cloneProgressLabel("malloydata/publisher", {
            environmentName: "examples",
            packageNames: ["storefront", "governed-analytics"],
         }),
      ).toBe(
         "[examples] cloning malloydata/publisher (storefront, governed-analytics)",
      );
   });

   it("truncates long package lists", () => {
      expect(
         cloneProgressLabel("o/r", {
            environmentName: "e",
            packageNames: ["a", "b", "c", "d", "e", "f"],
         }),
      ).toBe("[e] cloning o/r (a, b, c, +3 more)");
   });

   it("labels by repo alone without context", () => {
      // The controller add-package path passes no context.
      expect(cloneProgressLabel("o/r")).toBe("cloning o/r");
   });
});

describe("CloneProgressReporter", () => {
   const event = (stage: string, progress: number, processed = 0, total = 0) =>
      ({ method: "clone", stage, progress, processed, total }) as Parameters<
         CloneProgressReporter["onProgress"]
      >[0];

   const fakeOut = (isTTY: boolean) => {
      const writes: string[] = [];
      const out = {
         isTTY,
         write: (chunk: string) => {
            writes.push(chunk);
            return true;
         },
      } as unknown as NodeJS.WriteStream;
      return { writes, out };
   };

   it("prints a line per stage and per 25-point step off a TTY", () => {
      const { writes, out } = fakeOut(false);
      const reporter = new CloneProgressReporter("cloning o/r", out);
      reporter.onProgress(event("receiving", 0));
      reporter.onProgress(event("receiving", 10)); // same milestone, suppressed
      reporter.onProgress(event("receiving", 24)); // same milestone, suppressed
      reporter.onProgress(event("receiving", 25, 25, 100));
      reporter.onProgress(event("receiving", 90));
      reporter.onProgress(event("resolving", 5)); // stage change, printed
      reporter.done(); // no in-place line to finish
      expect(writes).toEqual([
         "cloning o/r: receiving 0%\n",
         "cloning o/r: receiving 25% (25/100)\n",
         "cloning o/r: receiving 90%\n",
         "cloning o/r: resolving 5%\n",
      ]);
   });

   it("tolerates arbitrary stage strings", () => {
      // Server-side progress lines parse with stage "remote:".
      const { writes, out } = fakeOut(false);
      const reporter = new CloneProgressReporter("cloning o/r", out);
      reporter.onProgress(event("remote:", 50));
      expect(writes).toEqual(["cloning o/r: remote: 50%\n"]);
   });

   it("rewrites one line in place on a TTY and finishes it on done", () => {
      const { writes, out } = fakeOut(true);
      const reporter = new CloneProgressReporter("cloning o/r", out);
      reporter.onProgress(event("receiving", 10, 1, 10));
      reporter.onProgress(event("receiving", 11, 2, 10));
      reporter.done();
      expect(writes).toEqual([
         "\rcloning o/r: receiving 10% (1/10)\x1b[K",
         "\rcloning o/r: receiving 11% (2/10)\x1b[K",
         "\n",
      ]);
   });

   it("clamps the in-place line to the terminal width", () => {
      // An auto-wrapped line breaks the \r rewrite: the cursor lands on the
      // continuation row and every event scrolls a stale fragment. Too
      // narrow for even the suffix, so a plain head slice applies.
      const { writes, out } = fakeOut(true);
      (out as unknown as { columns: number }).columns = 20;
      const reporter = new CloneProgressReporter("cloning owner/repo", out);
      reporter.onProgress(event("receiving", 45, 100, 220));
      reporter.done();
      expect(writes[0]).toBe(
         "\r" + "cloning owner/repo:".slice(0, 19) + "\x1b[K",
      );
      expect(writes[0].length).toBe(1 + 19 + 3);
   });

   it("keeps the moving percentage visible when clamping a long label", () => {
      // The default npx label is longer than an 80-column terminal; the
      // label shortens, the stage and percentage stay on screen.
      const { writes, out } = fakeOut(true);
      (out as unknown as { columns: number }).columns = 60;
      const reporter = new CloneProgressReporter(
         "[examples] cloning malloydata/publisher (storefront, governed-analytics)",
         out,
      );
      reporter.onProgress(event("receiving", 45, 100, 220));
      reporter.done();
      const visible = writes[0].slice(1, -3); // strip \r and \x1b[K
      expect(visible.length).toBe(59);
      expect(visible.endsWith(": receiving 45% (100/220)")).toBe(true);
      expect(visible).toContain("...");
   });

   it("resets the milestone when the percentage restarts within a stage", () => {
      // git's server-side counting and compressing phases both parse as
      // stage "remote:"; the second starts back at 0%.
      const { writes, out } = fakeOut(false);
      const reporter = new CloneProgressReporter("cloning o/r", out);
      reporter.onProgress(event("remote:", 100));
      reporter.onProgress(event("remote:", 10));
      reporter.onProgress(event("remote:", 60));
      reporter.done();
      expect(writes).toEqual([
         "cloning o/r: remote: 100%\n",
         "cloning o/r: remote: 10%\n",
         "cloning o/r: remote: 60%\n",
      ]);
   });

   it("ignores a progress event that arrives after done()", () => {
      // simple-git can deliver a trailing event after the completion
      // callback ran done(); it must not re-write the row the finishing
      // newline scrolled past, nor re-claim TTY ownership.
      const { writes, out } = fakeOut(true);
      const reporter = new CloneProgressReporter("cloning o/r", out);
      reporter.onProgress(event("receiving", 50));
      reporter.done();
      const writesAfterDone = writes.length;
      reporter.onProgress(event("receiving", 90));
      expect(writes.length).toBe(writesAfterDone);
   });

   it("only one reporter owns a TTY's in-place line at a time", () => {
      // Environments load concurrently at boot; a second clone must not
      // rewrite the first one's line. It falls back to milestone lines.
      const { writes, out } = fakeOut(true);
      const first = new CloneProgressReporter("cloning a/a", out);
      const second = new CloneProgressReporter("cloning b/b", out);
      first.onProgress(event("receiving", 10));
      second.onProgress(event("receiving", 20));
      first.onProgress(event("receiving", 30));
      first.done();
      // After the owner finishes, the line is free to claim again.
      second.onProgress(event("receiving", 90));
      second.done();
      expect(writes).toEqual([
         "\rcloning a/a: receiving 10%\x1b[K",
         // The non-owner takes over the row cleanly before scrolling it, so
         // its milestone never concatenates onto the owner's in-place text.
         "\rcloning b/b: receiving 20%\x1b[K\n",
         "\rcloning a/a: receiving 30%\x1b[K",
         "\n",
         "\rcloning b/b: receiving 90%\x1b[K",
         "\n",
      ]);
   });
});

describe("formatReadinessLine", () => {
   const SAVED = ["PUBLISHER_HOST", "PUBLISHER_PORT", "MCP_PORT"] as const;
   let saved: Record<string, string | undefined>;

   beforeEach(() => {
      saved = {};
      for (const key of SAVED) {
         saved[key] = process.env[key];
         delete process.env[key];
      }
   });

   afterEach(() => {
      for (const key of SAVED) {
         if (saved[key] === undefined) delete process.env[key];
         else process.env[key] = saved[key];
      }
   });

   it("uses the server defaults and displays the wildcard bind as localhost", () => {
      expect(
         formatReadinessLine({ environments: 1, packages: 3, loadErrors: 0 }),
      ).toBe(
         "PUBLISHER_READY url=http://localhost:4000 mcp=http://localhost:4040 " +
            "environments=1 packages=3 load_errors=0",
      );
   });

   it("brackets an IPv6 literal host so the URL is dialable", () => {
      process.env.PUBLISHER_HOST = "::1";
      expect(
         formatReadinessLine({ environments: 1, packages: 1, loadErrors: 0 }),
      ).toBe(
         "PUBLISHER_READY url=http://[::1]:4000 mcp=http://[::1]:4040 " +
            "environments=1 packages=1 load_errors=0",
      );
   });

   it("displays the :: wildcard bind as localhost", () => {
      process.env.PUBLISHER_HOST = "::";
      expect(
         formatReadinessLine({ environments: 1, packages: 1, loadErrors: 0 }),
      ).toBe(
         "PUBLISHER_READY url=http://localhost:4000 mcp=http://localhost:4040 " +
            "environments=1 packages=1 load_errors=0",
      );
   });

   it("respects the flag-derived env vars", () => {
      // parseArgs writes --host/--port/--mcp_port here before the store exists.
      process.env.PUBLISHER_HOST = "127.0.0.1";
      process.env.PUBLISHER_PORT = "4321";
      process.env.MCP_PORT = "4361";
      expect(
         formatReadinessLine({ environments: 2, packages: 5, loadErrors: 1 }),
      ).toBe(
         "PUBLISHER_READY url=http://127.0.0.1:4321 mcp=http://127.0.0.1:4361 " +
            "environments=2 packages=5 load_errors=1",
      );
   });
});

describe("readiness line emission", () => {
   const readinessRoot = path.join(TEMP_DIR_PATH, "readiness-line-test");
   let stderrWrites: string[];
   let stderrSpy: ReturnType<typeof spyOn>;

   const readyLines = () =>
      stderrWrites
         .join("")
         .split("\n")
         .filter((line) => line.startsWith("PUBLISHER_READY"));

   const writePackage = (name: string) => {
      const dir = path.join(readinessRoot, "src-packages", name);
      mkdirSync(dir, { recursive: true });
      writeFileSync(path.join(dir, "publisher.json"), JSON.stringify({ name }));
      return dir;
   };

   beforeEach(() => {
      if (existsSync(readinessRoot)) {
         rmSync(readinessRoot, { recursive: true, force: true });
      }
      mkdirSync(readinessRoot, { recursive: true });
      mock.module("../config", () => ({
         isPublisherConfigFrozen: () => false,
      }));
      stderrWrites = [];
      stderrSpy = spyOn(process.stderr, "write").mockImplementation(
         (chunk: string | Uint8Array) => {
            stderrWrites.push(String(chunk));
            return true;
         },
      );
   });

   afterEach(() => {
      stderrSpy.mockRestore();
      rmSync(readinessRoot, { recursive: true, force: true });
   });

   it("emits exactly one line with counts after a successful boot", async () => {
      const pkgA = writePackage("pkg-a");
      const pkgB = writePackage("pkg-b");
      writeFileSync(
         path.join(readinessRoot, "publisher.config.json"),
         JSON.stringify({
            frozenConfig: false,
            environments: [
               {
                  name: "readiness-env",
                  packages: [
                     { name: "pkg-a", location: pkgA },
                     { name: "pkg-b", location: pkgB },
                  ],
                  connections: [],
               },
            ],
         }),
      );

      const store = new EnvironmentStore(readinessRoot);
      await store.finishedInitialization;

      const lines = readyLines();
      expect(lines).toHaveLength(1);
      expect(lines[0]).toContain("environments=1");
      expect(lines[0]).toContain("packages=2");
      expect(lines[0]).toContain("load_errors=0");
      expect(lines[0]).toMatch(
         /^PUBLISHER_READY url=http:\/\/\S+ mcp=http:\/\/\S+ environments=\d+ packages=\d+ load_errors=\d+$/,
      );
   });

   it("counts a mount failure in load_errors and still emits", async () => {
      // Post-#903 a bad location drops the package, not the boot; the line
      // must report the failure rather than stay silent or claim a clean load.
      const pkgA = writePackage("pkg-a");
      writeFileSync(
         path.join(readinessRoot, "publisher.config.json"),
         JSON.stringify({
            frozenConfig: false,
            environments: [
               {
                  name: "readiness-env",
                  packages: [
                     { name: "pkg-a", location: pkgA },
                     {
                        name: "pkg-missing",
                        location: path.join(readinessRoot, "does-not-exist"),
                     },
                  ],
                  connections: [],
               },
            ],
         }),
      );

      const store = new EnvironmentStore(readinessRoot);
      await store.finishedInitialization;

      const lines = readyLines();
      expect(lines).toHaveLength(1);
      expect(lines[0]).toContain("environments=1");
      // packages= is what the server actually serves: the boot-time database
      // sync prunes a package whose load failed (environment.ts getPackage
      // catch), so the failed one appears in load_errors, not in packages.
      expect(lines[0]).toContain("packages=1");
      expect(lines[0]).toContain("load_errors=1");
   });

   it("counts a failed environment in load_errors", async () => {
      // An environment that fails initialization outright lands in
      // failedEnvironments, not in environments; the line must count it
      // alongside per-package failures. Failure lever: a FILE squatting the
      // environment's publisher_data directory path makes the initial mkdir
      // throw before any package mounts.
      const pkgA = writePackage("pkg-a");
      mkdirSync(path.join(readinessRoot, "publisher_data"), {
         recursive: true,
      });
      writeFileSync(path.join(readinessRoot, "publisher_data", "bad-env"), "");
      writeFileSync(
         path.join(readinessRoot, "publisher.config.json"),
         JSON.stringify({
            frozenConfig: false,
            environments: [
               {
                  name: "good-env",
                  packages: [{ name: "pkg-a", location: pkgA }],
                  connections: [],
               },
               {
                  name: "bad-env",
                  packages: [{ name: "pkg-a", location: pkgA }],
                  connections: [],
               },
            ],
         }),
      );

      const store = new EnvironmentStore(readinessRoot);
      await store.finishedInitialization;

      const lines = readyLines();
      expect(lines).toHaveLength(1);
      expect(lines[0]).toContain("environments=1");
      expect(lines[0]).toContain("packages=1");
      expect(lines[0]).toContain("load_errors=1");
   });
});
