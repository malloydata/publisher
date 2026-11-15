// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterAll, beforeAll, describe, expect, it, spyOn } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { pathToFileURL } from "url";
import {
   compiledImports,
   compiledTextReader,
   IMPORT_OUTSIDE_PACKAGE_MESSAGE,
   listPackageTree,
   packageTextReader,
   readPackageURL,
   readPackageURLSync,
} from "./utils";

/**
 * The package URL reader: a model reads its own package's files and nothing
 * else, and what it is told about a file outside is the same whether or not
 * that file exists.
 *
 *   <tmp>/env/pkg/              the package
 *   <tmp>/env/pkg/models/a.malloy, models/sub/b.malloy, top.malloy
 *   <tmp>/env/other/x.malloy    a sibling package
 *   <tmp>/env/pkg/out -> ../other          a link leading out
 *   <tmp>/env/pkg/gone -> ../missing.malloy  a dangling link leading out
 *   <tmp>/env/pkg/alias -> models          a link that stays inside
 *   <tmp>/mount -> env/pkg                 the package mounted as a link (watch mode)
 */
let tmp = "";
let pkg = "";
const url = (p: string) => pathToFileURL(p);

beforeAll(() => {
   tmp = fs.mkdtempSync(path.join(os.tmpdir(), "package-url-reader-"));
   pkg = path.join(tmp, "env", "pkg");
   fs.mkdirSync(path.join(pkg, "models", "sub"), { recursive: true });
   fs.mkdirSync(path.join(tmp, "env", "other"), { recursive: true });
   fs.writeFileSync(path.join(pkg, "top.malloy"), "top");
   fs.writeFileSync(path.join(pkg, "models", "a.malloy"), "a");
   fs.writeFileSync(path.join(pkg, "models", "sub", "b.malloy"), "b");
   fs.writeFileSync(path.join(tmp, "env", "other", "x.malloy"), "secret");
   fs.symlinkSync(path.join("..", "other"), path.join(pkg, "out"));
   fs.symlinkSync(path.join("..", "missing.malloy"), path.join(pkg, "gone"));
   fs.symlinkSync("models", path.join(pkg, "alias"));
   fs.symlinkSync(path.join("env", "pkg"), path.join(tmp, "mount"));
});

afterAll(() => {
   fs.rmSync(tmp, { recursive: true, force: true });
});

/** The message `readPackageURL` rejects with, or "READ:<text>" when it reads. */
async function outcome(root: string, target: URL): Promise<string> {
   try {
      return `READ:${await readPackageURL(root, target)}`;
   } catch (error) {
      return (error as Error).message;
   }
}

describe("readPackageURL", () => {
   it("reads in-package files, including a ../ that stays inside", async () => {
      expect(await outcome(pkg, url(path.join(pkg, "top.malloy")))).toBe(
         "READ:top",
      );
      // models/sub/b.malloy importing "../a.malloy"
      const fromSub = new URL(
         "../a.malloy",
         url(path.join(pkg, "models/sub/b.malloy")),
      );
      expect(await outcome(pkg, fromSub)).toBe("READ:a");
      const toRoot = new URL(
         "../../top.malloy",
         url(path.join(pkg, "models/sub/b.malloy")),
      );
      expect(await outcome(pkg, toRoot)).toBe("READ:top");
   });

   it("reads through a symlink inside the package that stays inside", async () => {
      expect(await outcome(pkg, url(path.join(pkg, "alias", "a.malloy")))).toBe(
         "READ:a",
      );
   });

   it("reads a package mounted as a symlink, as watch mode mounts it", async () => {
      const mount = path.join(tmp, "mount");
      expect(
         await outcome(mount, url(path.join(mount, "models", "a.malloy"))),
      ).toBe("READ:a");
      // The mount as the root, against the file by either the mount or its realpath.
      expect(
         await outcome(
            mount,
            url(path.join(fs.realpathSync(pkg), "top.malloy")),
         ),
      ).toBe("READ:top");
      // A spelling outside both the root as given and its realpath is
      // refused lexically, even when it leads back in: compiles name files
      // under the root as given, so only an absolute import spells one so.
      expect(await outcome(pkg, url(path.join(mount, "top.malloy")))).toBe(
         IMPORT_OUTSIDE_PACKAGE_MESSAGE,
      );
   });

   it("refuses a ../ that leaves the package, for a sibling package's file", async () => {
      const escape = new URL(
         "../other/x.malloy",
         url(path.join(pkg, "top.malloy")),
      );
      expect(await outcome(pkg, escape)).toBe(IMPORT_OUTSIDE_PACKAGE_MESSAGE);
      // From inside a mount too: <tmp>/mount/../env/other/x.malloy exists.
      const mount = path.join(tmp, "mount");
      const fromMount = new URL(
         "../env/other/x.malloy",
         url(path.join(mount, "top.malloy")),
      );
      expect(await outcome(mount, fromMount)).toBe(
         IMPORT_OUTSIDE_PACKAGE_MESSAGE,
      );
   });

   it("refuses an absolute path the same way whether or not it exists", async () => {
      const there = await outcome(
         pkg,
         url(path.join(tmp, "env", "other", "x.malloy")),
      );
      const absent = await outcome(
         pkg,
         url(path.join(tmp, "env", "other", "nope.malloy")),
      );
      const noDir = await outcome(pkg, url("/definitely/not/here.malloy"));
      const etc = await outcome(pkg, url("/etc/hosts"));
      for (const message of [there, absent, noDir, etc]) {
         expect(message).toBe(IMPORT_OUTSIDE_PACKAGE_MESSAGE);
      }
   });

   it("refuses a symlink inside the package that leads out, dangling or not", async () => {
      expect(await outcome(pkg, url(path.join(pkg, "out", "x.malloy")))).toBe(
         IMPORT_OUTSIDE_PACKAGE_MESSAGE,
      );
      expect(
         await outcome(pkg, url(path.join(pkg, "out", "nope.malloy"))),
      ).toBe(IMPORT_OUTSIDE_PACKAGE_MESSAGE);
      expect(await outcome(pkg, url(path.join(pkg, "gone")))).toBe(
         IMPORT_OUTSIDE_PACKAGE_MESSAGE,
      );
   });

   it("refuses any scheme but file:, even where it would read as a path inside the package", async () => {
      expect(await outcome(pkg, new URL("https://example.com/x.malloy"))).toBe(
         IMPORT_OUTSIDE_PACKAGE_MESSAGE,
      );
      // Read as a path, a non-file URL resolves against the working
      // directory; with the package there, it would land inside it.
      expect(
         await outcome(process.cwd(), new URL("https://example.com/x.malloy")),
      ).toBe(IMPORT_OUTSIDE_PACKAGE_MESSAGE);
   });

   it("refuses everything when the package root is gone", async () => {
      expect(
         await outcome(
            path.join(tmp, "env", "vanished"),
            url(path.join(pkg, "top.malloy")),
         ),
      ).toBe(IMPORT_OUTSIDE_PACKAGE_MESSAGE);
   });

   it("fails a missing in-package file as missing, not as outside", async () => {
      const message = await outcome(
         pkg,
         url(path.join(pkg, "models", "nope.malloy")),
      );
      expect(message).toContain("ENOENT");
      expect(message).not.toBe(IMPORT_OUTSIDE_PACKAGE_MESSAGE);
   });

   it("refuses a path outside the package before asking the filesystem about it", async () => {
      const realpath = spyOn(fs.promises, "realpath");
      const lstat = spyOn(fs.promises, "lstat");
      try {
         for (const target of [
            path.join(tmp, "env", "other", "x.malloy"),
            path.join(tmp, "env", "other", "nope.malloy"),
         ]) {
            realpath.mockClear();
            lstat.mockClear();
            expect(await outcome(pkg, url(target))).toBe(
               IMPORT_OUTSIDE_PACKAGE_MESSAGE,
            );
            // Only the root is resolved; the target is never looked at.
            expect(realpath.mock.calls.map((c) => String(c[0]))).toEqual([pkg]);
            expect(lstat).not.toHaveBeenCalled();
         }
      } finally {
         realpath.mockRestore();
         lstat.mockRestore();
      }
   });
});

describe("readPackageURLSync and packageTextReader", () => {
   it("read inside the package and refuse outside it, as the async reader does", () => {
      const fromSub = new URL(
         "../a.malloy",
         url(path.join(pkg, "models/sub/b.malloy")),
      );
      expect(readPackageURLSync(pkg, fromSub)).toBe("a");
      for (const target of [
         url(path.join(tmp, "env", "other", "x.malloy")),
         url(path.join(tmp, "env", "other", "nope.malloy")),
         url(path.join(pkg, "out", "x.malloy")),
         url(path.join(pkg, "gone")),
         new URL("https://example.com/x.malloy"),
      ]) {
         expect(() => readPackageURLSync(pkg, target)).toThrow(
            IMPORT_OUTSIDE_PACKAGE_MESSAGE,
         );
      }
      const read = packageTextReader(pkg);
      expect(read(url(path.join(pkg, "top.malloy")).href)).toBe("top");
      expect(read(url(path.join(pkg, "out", "x.malloy")).href)).toBeUndefined();
      expect(read("not a url")).toBeUndefined();
   });
});

describe("listPackageTree", () => {
   it("lists the package's files, every link out as itself, and nothing behind one", async () => {
      const listed = (await listPackageTree(pkg))
         .map((p) => path.relative(pkg, p))
         .sort();
      expect(listed).toEqual([
         "alias/a.malloy",
         "alias/sub/b.malloy",
         "gone",
         "models/a.malloy",
         "models/sub/b.malloy",
         // The directory link out, listed as itself: nothing behind it.
         "out",
         "top.malloy",
      ]);
   });
});

describe("compiledTextReader and compiledImports", () => {
   // The shape a compiled modelDef carries: the model's own file through a
   // declaration's location, and its import tree.
   const own = () => url(path.join(pkg, "models", "sub", "b.malloy")).href;
   const imported = () => url(path.join(pkg, "models", "a.malloy")).href;
   const modelDef = () => ({
      contents: { b: { location: { url: own() } } },
      dependencies: { [imported()]: {} },
   });

   it("reads only the files the model compiled from", () => {
      const read = compiledTextReader(modelDef(), pkg);
      expect(read(own())).toBe("b");
      expect(read(imported())).toBe("a");
      // In the package, but not a file this model read.
      expect(read(url(path.join(pkg, "top.malloy")).href)).toBeUndefined();
      expect(
         read(url(path.join(tmp, "env", "other", "x.malloy")).href),
      ).toBeUndefined();
   });

   it("answers a file's imports from the compiled tree", () => {
      const importsOf = compiledImports(modelDef());
      expect(importsOf(own())).toEqual([imported()]);
      expect(importsOf(imported())).toEqual([]);
   });
});
