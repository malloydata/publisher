// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import {
   PackageVersionError,
   type PackageVersionErrorReason,
} from "../../errors";
import { hashPackageTree } from "./package_content_hash";
import { VersionStore } from "./version_store";

let root: string;
let envPath: string;
let store: VersionStore;

beforeEach(() => {
   root = fs.realpathSync(
      fs.mkdtempSync(path.join(os.tmpdir(), "version-store-")),
   );
   envPath = path.join(root, "env");
   fs.mkdirSync(envPath);
   store = new VersionStore(envPath);
});

afterEach(() => {
   fs.rmSync(root, { recursive: true, force: true });
});

/** A downloader that writes a package tree with this publisher.json. */
function writes(
   manifest: Record<string, unknown> | string | null,
   files: Record<string, string> = { "model.malloy": "source: s is x" },
) {
   return async (target: string) => {
      fs.mkdirSync(target, { recursive: true });
      if (manifest !== null) {
         fs.writeFileSync(
            path.join(target, "publisher.json"),
            typeof manifest === "string" ? manifest : JSON.stringify(manifest),
         );
      }
      for (const [name, body] of Object.entries(files)) {
         fs.writeFileSync(path.join(target, name), body);
      }
   };
}

const stagingEntries = () =>
   fs.existsSync(path.join(envPath, ".staging"))
      ? fs.readdirSync(path.join(envPath, ".staging"))
      : [];

async function expectReason(
   promise: Promise<unknown>,
   reason: PackageVersionErrorReason,
) {
   const error = await promise.then(
      () => undefined,
      (err: unknown) => err,
   );
   expect(error).toBeInstanceOf(PackageVersionError);
   expect((error as PackageVersionError).reason).toBe(reason);
}

describe("VersionStore.stage", () => {
   it("reads the version and description from publisher.json and hashes the tree", async () => {
      const staged = await store.stage(
         "sales",
         writes({ version: "1.2.0+build.7", description: "Q3 cut" }),
      );
      expect(staged).toMatchObject({
         packageName: "sales",
         versionId: "1.2.0+build.7",
         dirName: "1.2.0_build.7",
         description: "Q3 cut",
      });
      expect(staged.contentHash).toBe(
         await hashPackageTree(staged.stagingPath),
      );
      expect(
         staged.stagingPath.startsWith(path.join(envPath, ".staging")),
      ).toBe(true);
   });

   it("refuses a tree without a version, or with one that is not semver, and cleans up", async () => {
      await expectReason(
         store.stage("sales", writes(null)),
         "MANIFEST_VERSION_MISSING",
      );
      await expectReason(
         store.stage("sales", writes({ description: "no version" })),
         "MANIFEST_VERSION_MISSING",
      );
      await expectReason(
         store.stage("sales", writes({ version: "v1" })),
         "MANIFEST_VERSION_INVALID",
      );
      await expectReason(
         store.stage("sales", writes({ version: 1 })),
         "MANIFEST_VERSION_INVALID",
      );
      await expectReason(
         store.stage("sales", writes("{ not json")),
         "MANIFEST_VERSION_INVALID",
      );
      expect(stagingEntries()).toEqual([]);
   });

   it("removes the staging folder when the download fails", async () => {
      await expect(
         store.stage("sales", async (target) => {
            fs.mkdirSync(target);
            fs.writeFileSync(path.join(target, "half"), "x");
            throw new Error("network");
         }),
      ).rejects.toThrow("network");
      expect(stagingEntries()).toEqual([]);
   });
});

describe("VersionStore.place", () => {
   it("moves the staged tree to <pkg>/<dir>/", async () => {
      const staged = await store.stage("sales", writes({ version: "1.0.0" }));
      expect(await store.place(staged)).toBe(true);
      const target = path.join(envPath, "sales", "1.0.0");
      expect(store.versionPath("sales", "1.0.0")).toBe(target);
      expect(fs.readFileSync(path.join(target, "model.malloy"), "utf8")).toBe(
         "source: s is x",
      );
      expect(fs.existsSync(staged.stagingPath)).toBe(false);
      expect(await store.isPlaced("sales", "1.0.0")).toBe(true);
   });

   it("never replaces a folder that is already there", async () => {
      await store.place(
         await store.stage("sales", writes({ version: "1.0.0" })),
      );
      const second = await store.stage(
         "sales",
         writes({ version: "1.0.0" }, { "model.malloy": "changed" }),
      );
      expect(await store.place(second)).toBe(false);
      expect(
         fs.readFileSync(
            path.join(envPath, "sales", "1.0.0", "model.malloy"),
            "utf8",
         ),
      ).toBe("source: s is x");
      expect(stagingEntries()).toEqual([]);
   });

   it("removes one version folder, or a package's whole folder", async () => {
      await store.place(
         await store.stage("sales", writes({ version: "1.0.0" })),
      );
      await store.place(
         await store.stage("sales", writes({ version: "2.0.0" })),
      );
      await store.remove("sales", "1.0.0");
      expect(fs.readdirSync(path.join(envPath, "sales"))).toEqual(["2.0.0"]);
      await store.removePackage("sales");
      expect(fs.existsSync(path.join(envPath, "sales"))).toBe(false);
   });

   it("refuses a folder name that is not a version's", () => {
      for (const bad of ["..", "../x", "latest", ".staging", "1.0.0/../.."]) {
         expect(() => store.versionPath("sales", bad)).toThrow();
      }
      expect(() => store.versionPath("../sales", "1.0.0")).toThrow();
   });
});

describe("VersionStore.restore", () => {
   it("fetches a missing folder again when it hashes to the published hash", async () => {
      const staged = await store.stage("sales", writes({ version: "1.0.0" }));
      const published = { ...staged };
      await store.place(staged);
      fs.rmSync(path.join(envPath, "sales", "1.0.0"), { recursive: true });

      expect(
         await store.restore("sales", published, writes({ version: "1.0.0" })),
      ).toBe(true);
      expect(await store.isPlaced("sales", "1.0.0")).toBe(true);
   });

   it("leaves it missing when the location now holds different content", async () => {
      const staged = await store.stage("sales", writes({ version: "1.0.0" }));
      const published = { ...staged };
      await store.place(staged);
      fs.rmSync(path.join(envPath, "sales", "1.0.0"), { recursive: true });

      expect(
         await store.restore(
            "sales",
            published,
            writes({ version: "1.0.0" }, { "model.malloy": "edited since" }),
         ),
      ).toBe(false);
      expect(
         await store.restore("sales", published, writes({ version: "1.0.1" })),
      ).toBe(false);
      expect(
         await store.restore("sales", published, async () => {
            throw new Error("gone");
         }),
      ).toBe(false);
      expect(await store.isPlaced("sales", "1.0.0")).toBe(false);
      expect(stagingEntries()).toEqual([]);
   });

   it("does nothing when the folder is there", async () => {
      const staged = await store.stage("sales", writes({ version: "1.0.0" }));
      await store.place(staged);
      let fetched = false;
      expect(
         await store.restore("sales", staged, async () => {
            fetched = true;
         }),
      ).toBe(true);
      expect(fetched).toBe(false);
   });
});

describe("VersionStore legacy trees", () => {
   it("holds an unversioned tree aside and puts it back intact", async () => {
      await writes(
         { name: "sales" },
         { "old.malloy": "old" },
      )(path.join(envPath, "sales"));
      const held = await store.holdLegacy("sales");
      expect(held).not.toBeNull();
      expect(fs.existsSync(path.join(envPath, "sales"))).toBe(false);

      // A failed first publish left a version folder behind.
      await store.place(
         await store.stage("sales", writes({ version: "1.0.0" })),
      );
      await store.restoreLegacy(held!);
      expect(fs.readdirSync(path.join(envPath, "sales")).sort()).toEqual([
         "old.malloy",
         "publisher.json",
      ]);
      expect(fs.readdirSync(path.join(envPath, ".legacy"))).toEqual([]);
   });

   it("drops a held tree once the publish commits", async () => {
      await writes({ name: "sales" })(path.join(envPath, "sales"));
      const held = await store.holdLegacy("sales");
      await store.dropLegacy(held!);
      expect(fs.readdirSync(path.join(envPath, ".legacy"))).toEqual([]);
   });

   it("holds nothing for a package with no tree", async () => {
      expect(await store.holdLegacy("sales")).toBeNull();
   });
});

describe("VersionStore.cleanup", () => {
   it("removes version folders no row owns, only in versioned packages", async () => {
      for (const v of ["1.0.0", "2.0.0", "3.0.0-rc.1"]) {
         await store.place(await store.stage("sales", writes({ version: v })));
      }
      // An unversioned package whose subfolders look like versions: never touched.
      await writes({ name: "plain" })(path.join(envPath, "plain"));
      fs.mkdirSync(path.join(envPath, "plain", "1.0.0"));
      // A non-version folder inside a versioned package is not ours to remove.
      fs.mkdirSync(path.join(envPath, "sales", "notes"));

      await store.cleanup(
         new Map([["sales", new Set(["1.0.0", "3.0.0-rc.1"])]]),
      );

      expect(fs.readdirSync(path.join(envPath, "sales")).sort()).toEqual([
         "1.0.0",
         "3.0.0-rc.1",
         "notes",
      ]);
      expect(fs.existsSync(path.join(envPath, "plain", "1.0.0"))).toBe(true);
   });

   it("drops a held tree whose package has versions, and puts back one whose package has none", async () => {
      await writes({ name: "a" }, { "a.malloy": "a" })(path.join(envPath, "a"));
      await writes({ name: "b" }, { "b.malloy": "b" })(path.join(envPath, "b"));
      await store.holdLegacy("a");
      await store.holdLegacy("b");
      // The crash: a's publish committed, b's did not (it left an orphan).
      await store.place(await store.stage("a", writes({ version: "1.0.0" })));
      await store.place(await store.stage("b", writes({ version: "1.0.0" })));

      await store.cleanup(new Map([["a", new Set(["1.0.0"])]]));

      expect(fs.readdirSync(path.join(envPath, ".legacy"))).toEqual([]);
      expect(fs.readdirSync(path.join(envPath, "a"))).toEqual(["1.0.0"]);
      expect(fs.readdirSync(path.join(envPath, "b")).sort()).toEqual([
         "b.malloy",
         "publisher.json",
      ]);
   });

   it("puts back only the newest of several trees held for one package", async () => {
      await writes(
         { name: "c" },
         { "old.malloy": "older" },
      )(path.join(envPath, "c"));
      const older = await store.holdLegacy("c");
      await writes(
         { name: "c" },
         { "new.malloy": "newer" },
      )(path.join(envPath, "c"));
      const newer = await store.holdLegacy("c");
      const past = new Date(Date.now() - 60_000);
      fs.utimesSync(older!.heldPath, past, past);

      await store.cleanup(new Map());

      expect(fs.readdirSync(path.join(envPath, "c")).sort()).toEqual([
         "new.malloy",
         "publisher.json",
      ]);
      expect(fs.readdirSync(path.join(envPath, ".legacy"))).toEqual([]);
      expect(fs.existsSync(newer!.heldPath)).toBe(false);
   });
});

describe("VersionStore path containment", () => {
   it("refuses to remove or move a staged or held path outside its folder", async () => {
      const outside = path.join(root, "outside");
      fs.mkdirSync(outside);
      fs.writeFileSync(path.join(outside, "keep"), "x");
      const staged = {
         packageName: "sales",
         stagingPath: outside,
         versionId: "1.0.0",
         dirName: "1.0.0",
         contentHash: "h",
         description: null,
      };
      await expect(store.discard(staged)).rejects.toThrow(/Not a path under/);
      await expect(store.place(staged)).rejects.toThrow(/Not a path under/);
      await expect(
         store.dropLegacy({ packageName: "sales", heldPath: outside }),
      ).rejects.toThrow(/Not a path under/);
      await expect(
         store.restoreLegacy({
            packageName: "sales",
            heldPath: path.join(envPath, ".legacy", "..", "..", "outside"),
         }),
      ).rejects.toThrow(/Not a path under/);
      expect(fs.readdirSync(outside)).toEqual(["keep"]);
   });
});
