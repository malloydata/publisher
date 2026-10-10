// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Mutex } from "async-mutex";
import { afterEach, beforeEach, describe, expect, it, mock } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { PackageVersionError } from "../../errors";
import { DuckDBConnection } from "../../storage/duckdb/DuckDBConnection";
import { DuckDBRepository } from "../../storage/duckdb/DuckDBRepository";
import { initializeSchema } from "../../storage/duckdb/schema";
import { VersionHost, VersionService } from "./version_service";
import { isUnversionedStage, VersionStore } from "./version_store";

/**
 * Publishing a version again, for every kind of location a publish fetches
 * from: a local folder, a local `.zip`, a `gs://` or `s3://` folder or
 * `.zip`, and a Git repository. Each is staged as a plain folder by the real
 * downloader (EnvironmentStore.downloadPackageInto) and hashed; only the
 * network is faked (the GCS and S3 clients, and the clone).
 *
 * The rule, the same for every kind: the same content under a published
 * version is placement (200, nothing replaced or written), even when the
 * fetched form differs (an archive packed again, a fresh clone); different
 * content under it is 409 VERSION_CONFLICT, and the first publish's files
 * stay as they were.
 *
 * simple-git and the storage manager are module-mocked BEFORE the store is
 * imported, as in environment_store_clone.spec.ts and
 * environment_store_unzip.spec.ts.
 */

/** What the fake clone writes: the repository's files, by relative path. */
let repoFiles: Record<string, string> = {};
let clones = 0;

mock.module("simple-git", () => ({
   simpleGit: () => ({
      clone: (
         _repoUrl: string,
         dir: string,
         _opts: unknown,
         cb: (err: Error | null) => void,
      ) => {
         clones++;
         writeTree(dir, repoFiles);
         // Repository state differs on every clone, as a fresh clone's does.
         writeTree(dir, {
            ".git/HEAD": "ref: refs/heads/main",
            [`.git/objects/pack/pack-${clones}.pack`]: `clone ${clones}`,
         });
         cb(null);
      },
   }),
}));

mock.module("../../storage/StorageManager", () => ({
   StorageManager: class MockStorageManager {
      async initialize(): Promise<void> {}
      getRepository() {
         return {
            listEnvironments: async () => [],
            listPackages: async () => [],
            listConnections: async () => [],
         };
      }
   },
   StorageConfig: {} as Record<string, unknown>,
}));

const { EnvironmentStore } = await import("../environment_store");

const ENV_ID = "env-1";
const ENV_NAME = "env";
const PKG = "sales";

/** The package as first published, and the same version with one change. */
const FIRST: Record<string, string> = {
   "publisher.json": JSON.stringify({ name: PKG, version: "1.0.0" }),
   "model.malloy": "source: s is x",
   "models/more.malloy": "source: t is x",
};
const CHANGED: Record<string, string> = {
   ...FIRST,
   "model.malloy": "source: s is y",
};

const hasZip =
   process.platform !== "win32" &&
   Bun.spawnSync(["which", "zip"]).exitCode === 0;

interface Loaded {
   versionId: string;
   path: string;
}

let root: string;
let envPath: string;
let sourcesPath: string;
let db: DuckDBConnection;
let repo: DuckDBRepository;
let downloads: InstanceType<typeof EnvironmentStore>;
let svc: VersionService<Loaded>;
/** Fake bucket contents, by "bucket/key". */
let objects: Map<string, Buffer>;
/** The store's server root; it holds publisher.config.json. */
let serverRoot: string;

function writeTree(dir: string, files: Record<string, string>): void {
   for (const [relative, content] of Object.entries(files)) {
      const file = path.join(dir, relative);
      fs.mkdirSync(path.dirname(file), { recursive: true });
      fs.writeFileSync(file, content);
   }
}

/** A source folder holding `files`, with every timestamp set to `at`. */
function sourceFolder(
   name: string,
   files: Record<string, string>,
   at = new Date("2026-01-01T00:00:00Z"),
): string {
   const dir = path.join(sourcesPath, name);
   writeTree(dir, files);
   for (const relative of Object.keys(files)) {
      fs.utimesSync(path.join(dir, relative), at, at);
   }
   return dir;
}

/** `files` zipped; `at` sets the timestamps the archive records. */
function zipOf(name: string, files: Record<string, string>, at?: Date): string {
   const dir = sourceFolder(`${name}-tree`, files, at);
   const archive = path.join(sourcesPath, `${name}.zip`);
   const result = Bun.spawnSync(["zip", "-q", "-X", "-r", archive, "."], {
      cwd: dir,
   });
   if (result.exitCode !== 0) {
      throw new Error(`zip failed: ${result.stderr.toString()}`);
   }
   return archive;
}

/** Put `files` into the fake bucket under `bucket/prefix/`. */
function upload(
   bucket: string,
   prefix: string,
   files: Record<string, string>,
): void {
   for (const [relative, content] of Object.entries(files)) {
      objects.set(`${bucket}/${prefix}/${relative}`, Buffer.from(content));
   }
}

function keysUnder(bucket: string, prefix: string): string[] {
   return [...objects.keys()]
      .filter((k) => k.startsWith(`${bucket}/${prefix}`))
      .map((k) => k.slice(bucket.length + 1));
}

/** A GCS client over the fake bucket, shaped as the store calls it. */
const fakeGcs = {
   bucket: (bucket: string) => ({
      getFiles: async ({ prefix }: { prefix: string }) => [
         keysUnder(bucket, prefix).map((name) => ({
            name,
            download: async () => [objects.get(`${bucket}/${name}`)!],
         })),
      ],
   }),
};

/** An S3 client over the fake bucket, shaped as the store calls it. */
const fakeS3 = {
   listObjectsV2: async ({
      Bucket,
      Prefix,
   }: {
      Bucket: string;
      Prefix: string;
   }) => ({ Contents: keysUnder(Bucket, Prefix).map((Key) => ({ Key })) }),
   send: async (command: { input: { Bucket: string; Key: string } }) => {
      const body = objects.get(`${command.input.Bucket}/${command.input.Key}`);
      return body
         ? {
              Body: {
                 transformToWebStream: () =>
                    new Response(new Uint8Array(body)).body!,
              },
           }
         : {};
   },
};

function host(): VersionHost<Loaded> {
   const packageLock = new Mutex();
   return {
      loadVersion: async (_packageName, versionPath, version) => ({
         versionId: version.versionId,
         path: versionPath,
      }),
      releaseVersion: () => {},
      bindVersionManifest: async () => {},
      downloaderFor: (packageName, location) => (stagingPath) =>
         downloads.downloadPackageInto(
            ENV_NAME,
            packageName,
            location,
            stagingPath,
         ),
      admit: () => {},
      withPackageLock: (_packageName, fn) => packageLock.runExclusive(fn),
      isWatchMounted: async () => false,
      retireUnversioned: () => {},
      ensurePackageRecord: async (packageName, description) => {
         if (await repo.getPackageByName(ENV_ID, packageName)) return false;
         await repo.createPackage({
            environmentId: ENV_ID,
            name: packageName,
            manifestPath: "",
            description: description ?? undefined,
         });
         return true;
      },
      removePackageRecord: async (packageName) => {
         const row = await repo.getPackageByName(ENV_ID, packageName);
         if (row) await repo.deletePackage(row.id);
      },
   };
}

/** Publish from `location` through the real downloader. */
function publish(location: string) {
   return svc.publish(
      PKG,
      (stagingPath) =>
         downloads.downloadPackageInto(ENV_NAME, PKG, location, stagingPath),
      { sourceLocation: location, promotion: "on-publish" },
   );
}

const modelFile = () => path.join(envPath, PKG, "1.0.0", "model.malloy");

/**
 * Publish `first`, then each of `same` (the same content, fetched again or in
 * another form), then `changed`: the first is the version, every `same` is
 * placement that leaves the placed files as they were, and `changed` is 409
 * with the first publish's content still in place.
 */
async function expectTheRule(
   first: string,
   same: string[],
   changed: string,
): Promise<void> {
   const published = await publish(first);
   expect(published.created).toBe(true);
   const placed = fs.statSync(modelFile());

   for (const location of [first, ...same]) {
      const again = await publish(location);
      expect([location, again.created]).toEqual([location, false]);
      const now = fs.statSync(modelFile());
      expect([location, now.ino, now.mtimeMs]).toEqual([
         location,
         placed.ino,
         placed.mtimeMs,
      ]);
   }

   const conflict = await publish(changed).then(
      () => undefined,
      (err: unknown) => err,
   );
   expect(conflict).toBeInstanceOf(PackageVersionError);
   expect((conflict as PackageVersionError).reason).toBe("VERSION_CONFLICT");
   expect(fs.readFileSync(modelFile(), "utf8")).toBe("source: s is x");
}

beforeEach(async () => {
   root = fs.realpathSync(
      fs.mkdtempSync(path.join(os.tmpdir(), "version-publish-locations-")),
   );
   envPath = path.join(root, "env");
   sourcesPath = path.join(root, "sources");
   fs.mkdirSync(envPath);
   serverRoot = path.join(root, "server");
   fs.mkdirSync(serverRoot);
   fs.writeFileSync(
      path.join(serverRoot, "publisher.config.json"),
      JSON.stringify({ environments: [] }),
   );

   db = new DuckDBConnection(":memory:");
   await db.initialize();
   await initializeSchema(db);
   await db.run(
      "INSERT INTO environments (id, name, path, created_at, updated_at) VALUES (?, ?, ?, now(), now())",
      [ENV_ID, ENV_NAME, envPath],
   );
   repo = new DuckDBRepository(db);
   svc = new VersionService<Loaded>(
      repo,
      ENV_ID,
      new VersionStore(envPath),
      host(),
   );

   downloads = new EnvironmentStore(serverRoot);
   Object.assign(downloads as unknown as Record<string, unknown>, {
      gcsClient: fakeGcs,
      s3Client: fakeS3,
   });
   objects = new Map();
   repoFiles = {};
   clones = 0;
});

afterEach(async () => {
   await db.close();
   fs.rmSync(root, { recursive: true, force: true });
});

describe("publishing a version again, by location kind", () => {
   it("a local folder: the same files copied elsewhere are placement; a changed file is 409", async () => {
      await expectTheRule(
         sourceFolder("first", FIRST),
         [sourceFolder("copy", FIRST, new Date("2026-02-02T00:00:00Z"))],
         sourceFolder("changed", CHANGED),
      );
   });

   it.skipIf(!hasZip)(
      "a local .zip: the same files packed again are placement; a changed file is 409",
      async () => {
         const first = zipOf("first", FIRST);
         const repacked = zipOf(
            "repacked",
            FIRST,
            new Date("2026-02-02T00:00:00Z"),
         );
         // The archive itself differs: only its contents are the same.
         expect(fs.readFileSync(repacked).equals(fs.readFileSync(first))).toBe(
            false,
         );
         await expectTheRule(first, [repacked], zipOf("changed", CHANGED));
      },
   );

   it("a gs:// folder: the same files uploaded again are placement; a changed file is 409", async () => {
      upload("bucket", "pkgs/first", FIRST);
      upload("bucket", "pkgs/again", FIRST);
      upload("bucket", "pkgs/changed", CHANGED);
      await expectTheRule(
         "gs://bucket/pkgs/first",
         ["gs://bucket/pkgs/again"],
         "gs://bucket/pkgs/changed",
      );
   });

   it.skipIf(!hasZip)(
      "a gs:// .zip: the same files packed again are placement; a changed file is 409",
      async () => {
         objects.set(
            "bucket/zips/first.zip",
            fs.readFileSync(zipOf("first", FIRST)),
         );
         objects.set(
            "bucket/zips/repacked.zip",
            fs.readFileSync(
               zipOf("repacked", FIRST, new Date("2026-02-02T00:00:00Z")),
            ),
         );
         objects.set(
            "bucket/zips/changed.zip",
            fs.readFileSync(zipOf("changed", CHANGED)),
         );
         await expectTheRule(
            "gs://bucket/zips/first.zip",
            ["gs://bucket/zips/repacked.zip"],
            "gs://bucket/zips/changed.zip",
         );
      },
   );

   it("an s3:// folder: the same files uploaded again are placement; a changed file is 409", async () => {
      upload("bucket", "pkgs/first", FIRST);
      upload("bucket", "pkgs/again", FIRST);
      upload("bucket", "pkgs/changed", CHANGED);
      await expectTheRule(
         "s3://bucket/pkgs/first",
         ["s3://bucket/pkgs/again"],
         "s3://bucket/pkgs/changed",
      );
   });

   it.skipIf(!hasZip)(
      "an s3:// .zip: the same files packed again are placement; a changed file is 409",
      async () => {
         objects.set(
            "bucket/zips/first.zip",
            fs.readFileSync(zipOf("first", FIRST)),
         );
         objects.set(
            "bucket/zips/repacked.zip",
            fs.readFileSync(
               zipOf("repacked", FIRST, new Date("2026-02-02T00:00:00Z")),
            ),
         );
         objects.set(
            "bucket/zips/changed.zip",
            fs.readFileSync(zipOf("changed", CHANGED)),
         );
         await expectTheRule(
            "s3://bucket/zips/first.zip",
            ["s3://bucket/zips/repacked.zip"],
            "s3://bucket/zips/changed.zip",
         );
      },
   );

   it("a Git repository: a fresh clone of the same files is placement; a changed file is 409", async () => {
      // A repository-root location keeps the clone's .git in the staged tree,
      // and every clone writes different repository state there.
      const location = "https://github.com/example/sales";
      repoFiles = FIRST;
      const published = await publish(location);
      expect(published.created).toBe(true);
      const placed = fs.statSync(modelFile());

      const again = await publish(location);
      expect(again.created).toBe(false);
      expect(clones).toBe(2);
      const now = fs.statSync(modelFile());
      expect([now.ino, now.mtimeMs]).toEqual([placed.ino, placed.mtimeMs]);

      repoFiles = CHANGED;
      const conflict = await publish(location).then(
         () => undefined,
         (err: unknown) => err,
      );
      expect(conflict).toBeInstanceOf(PackageVersionError);
      expect((conflict as PackageVersionError).reason).toBe("VERSION_CONFLICT");
      expect(fs.readFileSync(modelFile(), "utf8")).toBe("source: s is x");
   });
});

/** Every file under `dir`, relative and sorted, leaving out a clone's .git. */
function filesUnder(dir: string): string[] {
   if (!fs.existsSync(dir)) return [];
   const out: string[] = [];
   const walk = (at: string, prefix: string) => {
      for (const entry of fs.readdirSync(at, { withFileTypes: true })) {
         if (entry.name === ".git") continue;
         const relative = prefix ? `${prefix}/${entry.name}` : entry.name;
         if (entry.isDirectory()) walk(path.join(at, entry.name), relative);
         else out.push(relative);
      }
   };
   walk(dir, "");
   return out.sort();
}

const PACKAGE_FILES = Object.keys(FIRST).sort();

/** The package as FIRST, but with a publisher.json that declares no version. */
const NO_VERSION: Record<string, string> = {
   ...FIRST,
   "publisher.json": JSON.stringify({ name: PKG }),
};

/** Each kind of location a publish fetches from, and how to set it up. */
const LOCATION_KINDS: {
   kind: string;
   needsZip?: boolean;
   setUp: (files: Record<string, string>) => string;
}[] = [
   {
      kind: "an absolute folder",
      setUp: (files) => sourceFolder("folder", files),
   },
   {
      kind: "an absolute .zip",
      needsZip: true,
      setUp: (files) => zipOf("archive", files),
   },
   {
      kind: "a gs:// folder",
      setUp: (files) => {
         upload("bucket", "pkgs/sales", files);
         return "gs://bucket/pkgs/sales";
      },
   },
   {
      kind: "a gs:// .zip",
      needsZip: true,
      setUp: (files) => {
         objects.set(
            "bucket/zips/sales.zip",
            fs.readFileSync(zipOf("gcs", files)),
         );
         return "gs://bucket/zips/sales.zip";
      },
   },
   {
      kind: "an s3:// folder",
      setUp: (files) => {
         upload("bucket", "pkgs/sales", files);
         return "s3://bucket/pkgs/sales";
      },
   },
   {
      kind: "an s3:// .zip",
      needsZip: true,
      setUp: (files) => {
         objects.set(
            "bucket/zips/sales.zip",
            fs.readFileSync(zipOf("s3", files)),
         );
         return "s3://bucket/zips/sales.zip";
      },
   },
   {
      kind: "a Git repository",
      setUp: (files) => {
         repoFiles = files;
         return "https://github.com/example/sales";
      },
   },
   {
      // A top-level folder, as the location names it (`/tree/main/<folder>`).
      kind: "a folder of a Git repository",
      setUp: (files) => {
         repoFiles = Object.fromEntries(
            Object.entries(files).map(([relative, content]) => [
               `sales/${relative}`,
               content,
            ]),
         );
         return "https://github.com/example/repo/tree/main/sales";
      },
   },
];

describe("what a publish fetches, by location kind", () => {
   for (const { kind, needsZip, setUp } of LOCATION_KINDS) {
      it.skipIf(needsZip === true && !hasZip)(
         `${kind}: the package's files, as a plain folder`,
         async () => {
            const target = path.join(root, "staged");
            await downloads.downloadPackageInto(
               ENV_NAME,
               PKG,
               setUp(FIRST),
               target,
            );
            expect(filesUnder(target)).toEqual(PACKAGE_FILES);
            expect(
               fs.readFileSync(path.join(target, "model.malloy"), "utf8"),
            ).toBe("source: s is x");
         },
      );
   }

   it("a relative path: nothing, as before versions (only a configured package resolves one)", async () => {
      sourceFolder("relative", FIRST);
      const target = path.join(root, "staged");
      await downloads.downloadPackageInto(
         ENV_NAME,
         PKG,
         "./sources/relative",
         target,
      );
      expect(filesUnder(target)).toEqual([]);
   });
});

describe("a tree that declares no semantic version, by location kind", () => {
   for (const { kind, needsZip, setUp } of LOCATION_KINDS) {
      it.skipIf(needsZip === true && !hasZip)(
         `${kind}: comes back staged and whole, for an install in place`,
         async () => {
            const location = setUp(NO_VERSION);
            const staged = await svc.stageForPublish(PKG, (stagingPath) =>
               downloads.downloadPackageInto(
                  ENV_NAME,
                  PKG,
                  location,
                  stagingPath,
               ),
            );
            expect(isUnversionedStage(staged)).toBe(true);
            if (!isUnversionedStage(staged)) return;
            expect(staged.reason.reason).toBe("MANIFEST_VERSION_MISSING");
            expect(staged.hasManifest).toBe(true);
            expect(filesUnder(staged.stagingPath)).toEqual(PACKAGE_FILES);
            await svc.discardStage(staged);
            expect(fs.existsSync(staged.stagingPath)).toBe(false);
         },
      );
   }

   it("a version that is not semver comes back the same way, saying so", async () => {
      const location = sourceFolder("not-semver", {
         ...FIRST,
         "publisher.json": JSON.stringify({ name: PKG, version: "1.0" }),
      });
      const staged = await svc.stageForPublish(PKG, (stagingPath) =>
         downloads.downloadPackageInto(ENV_NAME, PKG, location, stagingPath),
      );
      expect(isUnversionedStage(staged)).toBe(true);
      if (!isUnversionedStage(staged)) return;
      expect(staged.reason.reason).toBe("MANIFEST_VERSION_INVALID");
      expect(staged.hasManifest).toBe(true);
      await svc.discardStage(staged);
   });

   it("a location that fetches no publisher.json says it has none", async () => {
      const location = sourceFolder("no-manifest", {
         "model.malloy": "source: s is x",
      });
      const staged = await svc.stageForPublish(PKG, (stagingPath) =>
         downloads.downloadPackageInto(ENV_NAME, PKG, location, stagingPath),
      );
      expect(isUnversionedStage(staged)).toBe(true);
      if (!isUnversionedStage(staged)) return;
      expect(staged.hasManifest).toBe(false);
      await svc.discardStage(staged);
   });
});

describe("a configured package's local location, absolute or relative", () => {
   /** The mount a package listed in publisher.config.json gets. */
   function mount(location: string, target: string): Promise<void> {
      return (
         downloads as unknown as {
            downloadOrMountLocation(
               location: string,
               targetPath: string,
               environmentName: string,
               packageName: string,
            ): Promise<void>;
         }
      ).downloadOrMountLocation(location, target, ENV_NAME, PKG);
   }

   /** A folder holding the package, under `base`. */
   function packageAt(base: string, relative: string): string {
      const dir = path.join(base, relative);
      writeTree(dir, FIRST);
      return dir;
   }

   it("an absolute folder", async () => {
      const target = path.join(root, "mounted");
      await mount(packageAt(root, "abs/sales"), target);
      expect(filesUnder(target)).toEqual(PACKAGE_FILES);
   });

   it("./ resolves against the folder that holds publisher.config.json", async () => {
      packageAt(serverRoot, "pkgs/sales");
      const target = path.join(root, "mounted");
      await mount("./pkgs/sales", target);
      expect(filesUnder(target)).toEqual(PACKAGE_FILES);
   });

   it("../ resolves against that folder too, so it may point outside it", async () => {
      packageAt(root, "beside/sales");
      const target = path.join(root, "mounted");
      await mount("../beside/sales", target);
      expect(filesUnder(target)).toEqual(PACKAGE_FILES);
   });

   // `~/` expands against the home folder before it mounts the same way;
   // resolvePackageLocation's own tests cover the expansion.

   it.skipIf(!hasZip)("an absolute .zip, extracted", async () => {
      const target = path.join(root, "mounted");
      await mount(zipOf("abs", FIRST), target);
      expect(filesUnder(target)).toEqual(PACKAGE_FILES);
   });

   it.skipIf(!hasZip)("a relative .zip, extracted", async () => {
      const archive = zipOf("rel", FIRST);
      fs.mkdirSync(path.join(serverRoot, "zips"), { recursive: true });
      fs.copyFileSync(archive, path.join(serverRoot, "zips", "sales.zip"));
      const target = path.join(root, "mounted");
      await mount("./zips/sales.zip", target);
      expect(filesUnder(target)).toEqual(PACKAGE_FILES);
   });
});
