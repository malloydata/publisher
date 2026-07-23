import {
   afterEach,
   beforeEach,
   describe,
   expect,
   it,
   mock,
   spyOn,
} from "bun:test";
import { existsSync, mkdirSync, rmSync, writeFileSync } from "fs";
import * as path from "path";
import { TEMP_DIR_PATH } from "../constants";
import { logger } from "../logger";

/**
 * Wiring coverage for the package git clone: the shallow-clone options must
 * actually reach `simple-git`'s clone call, the factory must carry a progress
 * handler whose output lands on stderr with the right label, and the readiness
 * line must not print when boot fails. The pure helpers (options content,
 * throttling, label text) are covered in environment_store.spec.ts; this file
 * proves the plumbing.
 *
 * simple-git is module-mocked BEFORE the store is imported (the anchoring
 * spec's ordering idiom). The mock leaks forward in bun's shared module cache,
 * which is safe here: environment_store.ts is the only importer, and the only
 * spec that performs a real clone (connection.spec.ts) is credential-gated and
 * runs in its own workflow process.
 */

const serverRootPath = path.join(TEMP_DIR_PATH, "clone-spec-server-root");

const ENV_NAME = "clone-env";
const PKG_NAME = "pkg-a";

interface RecordedClone {
   repoUrl: string;
   dir: string;
   opts: Record<string, unknown>;
}

let recordedClones: RecordedClone[] = [];
let lastFactoryOpts: {
   progress?: (event: {
      method: string;
      stage: string;
      progress: number;
      processed: number;
      total: number;
   }) => void;
} | null = null;
let storageInitFails = false;
let storageInitMessage = "storage init failed (test)";
let cloneFailure: Error | null = null;

mock.module("simple-git", () => ({
   default: (factoryOpts?: typeof lastFactoryOpts) => {
      lastFactoryOpts = factoryOpts ?? null;
      return {
         clone: (
            repoUrl: string,
            dir: string,
            opts: Record<string, unknown>,
            cb: (err: Error | null) => void,
         ) => {
            recordedClones.push({ repoUrl, dir, opts });
            if (cloneFailure) {
               cb(cloneFailure);
               return;
            }
            // Materialize the fixture the extraction step expects: the repo
            // contains a subdirectory per package (pkg-a plus a sibling used
            // by the shared-clone counter test). Extraction only copies the
            // named subdir, so an unused one is harmless to single-package
            // tests.
            for (const name of [PKG_NAME, "pkg-b"]) {
               const pkgDir = path.join(dir, name);
               mkdirSync(pkgDir, { recursive: true });
               writeFileSync(
                  path.join(pkgDir, "publisher.json"),
                  JSON.stringify({ name }),
               );
            }
            // Drive the progress handler the way the real plugin would.
            lastFactoryOpts?.progress?.({
               method: "clone",
               stage: "receiving",
               progress: 50,
               processed: 1,
               total: 2,
            });
            cb(null);
         },
      };
   },
}));

mock.module("../storage/StorageManager", () => ({
   StorageManager: class MockStorageManager {
      async initialize(): Promise<void> {
         if (storageInitFails) {
            throw new Error(storageInitMessage);
         }
      }
      getRepository() {
         return {
            listEnvironments: async () => [],
            getEnvironmentByName: async () => null,
            createEnvironment: async (data: Record<string, unknown>) => ({
               id: "env-id",
               name: data.name,
               path: data.path,
            }),
            listPackages: async () => [],
            getPackageByName: async () => null,
            createPackage: async (data: Record<string, unknown>) => ({
               id: "pkg-id",
               name: data.name,
            }),
            listConnections: async () => [],
         };
      }
   },
   StorageConfig: {} as Record<string, unknown>,
}));

const { EnvironmentStore, GIT_CLONE_OPTIONS } = await import(
   "./environment_store"
);

describe("git clone wiring", () => {
   let stderrWrites: string[];
   let stderrSpy: ReturnType<typeof spyOn>;

   beforeEach(() => {
      rmSync(serverRootPath, { recursive: true, force: true });
      mkdirSync(serverRootPath, { recursive: true });
      recordedClones = [];
      lastFactoryOpts = null;
      storageInitFails = false;
      storageInitMessage = "storage init failed (test)";
      cloneFailure = null;
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
      rmSync(serverRootPath, { recursive: true, force: true });
   });

   it("boot clones shallow, streams labeled progress, and mounts the package", async () => {
      writeFileSync(
         path.join(serverRootPath, "publisher.config.json"),
         JSON.stringify({
            frozenConfig: false,
            environments: [
               {
                  name: ENV_NAME,
                  packages: [
                     {
                        name: PKG_NAME,
                        location: `https://github.com/example/repo/tree/main/${PKG_NAME}`,
                     },
                  ],
                  connections: [],
               },
            ],
         }),
      );

      const infoSpy = spyOn(logger, "info");
      const store = new EnvironmentStore(serverRootPath);
      await store.finishedInitialization;

      // The options object reaches .clone verbatim.
      expect(recordedClones).toHaveLength(1);
      expect(recordedClones[0].repoUrl).toBe("https://github.com/example/repo");
      expect(recordedClones[0].opts).toEqual(GIT_CLONE_OPTIONS);

      // The extraction log carries the mounted-of-total counter.
      const infoMessages = infoSpy.mock.calls.map((c) => String(c[0]));
      infoSpy.mockRestore();
      expect(
         infoMessages.some(
            (m) =>
               m.includes('Extracted package "pkg-a"') && m.includes("(1/1)"),
         ),
      ).toBe(true);

      // The progress event surfaced on stderr, labeled with env and package.
      const output = stderrWrites.join("");
      expect(output).toContain(
         `[${ENV_NAME}] cloning example/repo (${PKG_NAME}): receiving 50% (1/2)`,
      );

      // The package extracted from the (fake) clone and mounted.
      expect(
         existsSync(
            path.join(
               serverRootPath,
               "publisher_data",
               ENV_NAME,
               PKG_NAME,
               "publisher.json",
            ),
         ),
      ).toBe(true);

      // And the boot announced itself exactly once.
      const readyLines = output
         .split("\n")
         .filter((line) => line.startsWith("PUBLISHER_READY"));
      expect(readyLines).toHaveLength(1);
      expect(readyLines[0]).toContain("environments=1");
      expect(readyLines[0]).toContain("packages=1");
      expect(readyLines[0]).toContain("load_errors=0");
   });

   it("the mounted-of-total counter accumulates across packages in a shared clone", async () => {
      // Two packages share one repo clone; the counter is one variable across
      // every mount branch, so a single (1/2)->(2/2) run pins that it advances
      // rather than reporting a constant.
      writeFileSync(
         path.join(serverRootPath, "publisher.config.json"),
         JSON.stringify({
            frozenConfig: false,
            environments: [
               {
                  name: ENV_NAME,
                  packages: [
                     {
                        name: PKG_NAME,
                        location: `https://github.com/example/repo/tree/main/${PKG_NAME}`,
                     },
                     {
                        name: "pkg-b",
                        location: `https://github.com/example/repo/tree/main/pkg-b`,
                     },
                  ],
                  connections: [],
               },
            ],
         }),
      );

      const infoSpy = spyOn(logger, "info");
      const store = new EnvironmentStore(serverRootPath);
      await store.finishedInitialization;
      const infoMessages = infoSpy.mock.calls.map((c) => String(c[0]));
      infoSpy.mockRestore();

      // One clone serves both, and the counter runs 1/2 then 2/2.
      expect(recordedClones).toHaveLength(1);
      const counters = infoMessages
         .filter((m) => m.startsWith("Extracted package"))
         .map((m) => m.slice(m.lastIndexOf("(")));
      expect(counters).toEqual(["(1/2)", "(2/2)"]);
      expect(stderrWrites.join("")).toContain("packages=2");
   });

   it("direct download extracts the subdirectory and labels by repo alone", async () => {
      // The controller add-package path: downloadGitHubDirectory is called
      // with the raw /tree/... location and no progress context.
      const store = new EnvironmentStore(serverRootPath);
      await store.finishedInitialization;

      const target = path.join(serverRootPath, "publisher_data", "direct");
      await store.downloadGitHubDirectory(
         `https://github.com/example/repo/tree/main/${PKG_NAME}`,
         target,
      );

      expect(recordedClones).toHaveLength(1);
      expect(recordedClones[0].opts).toEqual(GIT_CLONE_OPTIONS);
      // Subdirectory contents were hoisted to the target root.
      expect(existsSync(path.join(target, "publisher.json"))).toBe(true);
      expect(existsSync(path.join(target, PKG_NAME))).toBe(false);
      // No environment context on this path, so the label is the repo alone.
      expect(stderrWrites.join("")).toContain(
         "cloning example/repo: receiving 50% (1/2)",
      );
   });

   it("a failed clone rejects with the progress noise stripped", async () => {
      const store = new EnvironmentStore(serverRootPath);
      await store.finishedInitialization;
      cloneFailure = new Error(
         "Cloning into '/x'...\nReceiving objects: 50% (5/10)\nfatal: early EOF",
      );

      const target = path.join(serverRootPath, "publisher_data", "direct");
      let caught: Error | undefined;
      try {
         await store.downloadGitHubDirectory(
            "https://github.com/example/repo",
            target,
         );
      } catch (e) {
         caught = e as Error;
      }
      expect(caught).toBeDefined();
      // The message keeps what went wrong and drops the --progress spew;
      // the stack (which V8 seeds with the message, prefixed "Error: ") is
      // stripped too, including the prefixed "Cloning into" first line.
      expect(caught!.message).toBe("fatal: early EOF");
      expect(caught!.stack ?? "").not.toContain("Receiving objects");
      expect(caught!.stack ?? "").not.toContain("Cloning into");
   });

   it("prints the failure token instead of the readiness line when boot fails", async () => {
      storageInitFails = true;
      const store = new EnvironmentStore(serverRootPath);
      // initialize() swallows the failure by design; the promise resolves.
      await store.finishedInitialization;
      const output = stderrWrites.join("");
      expect(output.includes("PUBLISHER_READY")).toBe(false);
      // A script waiting on the ready token fails fast instead of hanging.
      const failLines = output
         .split("\n")
         .filter((line) => line.startsWith("PUBLISHER_INIT_FAILED"));
      expect(failLines).toHaveLength(1);
      expect(failLines[0]).toContain("storage init failed (test)");
   });

   it("redacts a pg password in the failure token", async () => {
      // The init error can carry a connection string; the INIT_FAILED token
      // must redact the keyword-form password before it reaches stderr.
      // (The adjacent winston init-error log shares the raw-message gap with
      // every other extractErrorDataFromError site; redacting those centrally
      // is a separate security follow-up, tracked in npx-fast-first-boot.md.)
      storageInitFails = true;
      storageInitMessage =
         "attach failed: host=db port=5432 password=supersecret dbname=x";
      const store = new EnvironmentStore(serverRootPath);
      await store.finishedInitialization;

      const failLine = stderrWrites
         .join("")
         .split("\n")
         .find((line) => line.startsWith("PUBLISHER_INIT_FAILED"));
      expect(failLine).toBeDefined();
      expect(failLine).toContain("password=***");
      expect(failLine).not.toContain("supersecret");
   });
});
