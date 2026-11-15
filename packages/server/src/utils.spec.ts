// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterAll, beforeAll, describe, expect, it, spyOn } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { pathToFileURL } from "url";
import {
   confinedPackagePath,
   linkTextSteps,
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

describe("a .. after a symlinked directory", () => {
   // <tmp2>/pkg/x/y/sub -> <tmp2>/outside/a/b, and pkg/probe.json beside it.
   // The kernel resolves `sub/../../..` from sub's real target, so the link
   // `pkg/<name> -> x/y/sub/../../../probe.json` leads to <tmp2>/probe.json,
   // outside the package, whether or not that file exists; read by spelling
   // it would land on pkg/probe.json instead.
   let base = "";
   let pkg2 = "";
   beforeAll(() => {
      base = fs.mkdtempSync(path.join(os.tmpdir(), "dotdot-link-"));
      pkg2 = path.join(base, "pkg");
      fs.mkdirSync(path.join(pkg2, "x", "y"), { recursive: true });
      fs.mkdirSync(path.join(base, "outside", "a", "b"), { recursive: true });
      fs.symlinkSync(
         path.join(base, "outside", "a", "b"),
         path.join(pkg2, "x", "y", "sub"),
      );
      fs.writeFileSync(path.join(pkg2, "present.json"), "inside");
      fs.writeFileSync(path.join(pkg2, "absent_.json"), "inside");
      // Only `present.json` exists at the outside spot.
      fs.writeFileSync(path.join(base, "present.json"), "outside");
      for (const name of ["present.json", "absent_.json"]) {
         fs.symlinkSync(
            `x/y/sub/../../../${name}`,
            path.join(pkg2, `l-${name}`),
         );
      }
   });
   afterAll(() => fs.rmSync(base, { recursive: true, force: true }));

   it("refuses both, the same way, as the kernel would resolve them out", async () => {
      for (const name of ["present.json", "absent_.json"]) {
         const target = url(path.join(pkg2, `l-${name}`));
         expect(await outcome(pkg2, target)).toBe(
            IMPORT_OUTSIDE_PACKAGE_MESSAGE,
         );
         expect(() => readPackageURLSync(pkg2, target)).toThrow(
            IMPORT_OUTSIDE_PACKAGE_MESSAGE,
         );
      }
   });

   it("fails a path through a missing directory as missing, never landing on a file its spelling reaches", async () => {
      // pkg/nodir/../present.json: the kernel stops at nodir.
      fs.symlinkSync("nodir/../present.json", path.join(pkg2, "via-missing"));
      const message = await outcome(pkg2, url(path.join(pkg2, "via-missing")));
      expect(message).toContain("ENOENT");
      expect(() =>
         readPackageURLSync(pkg2, url(path.join(pkg2, "via-missing"))),
      ).toThrow("ENOENT");
   });
});

describe("a path that goes out of the package and comes back", () => {
   // <base>/pkg/lib/real.malloy, with <base>/outside/present a directory and
   // <base>/outside/absent not there. A link through either, back into the
   // package, must answer the same: the walk never looks outside.
   let base = "";
   let pkg3 = "";
   beforeAll(() => {
      base = fs.realpathSync(
         fs.mkdtempSync(path.join(os.tmpdir(), "out-back-")),
      );
      pkg3 = path.join(base, "pkg");
      fs.mkdirSync(path.join(pkg3, "lib"), { recursive: true });
      fs.mkdirSync(path.join(base, "outside", "present"), { recursive: true });
      fs.writeFileSync(path.join(pkg3, "lib", "real.malloy"), "real");
      for (const which of ["present", "absent"]) {
         fs.symlinkSync(
            `../outside/${which}/../../pkg/lib/real.malloy`,
            path.join(pkg3, `rel-${which}`),
         );
         fs.symlinkSync(
            // Spelled, not joined: path.join would collapse the `..`.
            `${base}/outside/${which}/../../pkg/lib/real.malloy`,
            path.join(pkg3, `abs-${which}`),
         );
      }
   });
   afterAll(() => fs.rmSync(base, { recursive: true, force: true }));

   it("refuses both, relative or absolute, without looking at what it passes through", async () => {
      const lstat = spyOn(fs.promises, "lstat");
      try {
         for (const form of ["rel", "abs"]) {
            for (const which of ["present", "absent"]) {
               lstat.mockClear();
               const target = url(path.join(pkg3, `${form}-${which}`));
               expect(await outcome(pkg3, target)).toBe(
                  IMPORT_OUTSIDE_PACKAGE_MESSAGE,
               );
               expect(() => readPackageURLSync(pkg3, target)).toThrow(
                  IMPORT_OUTSIDE_PACKAGE_MESSAGE,
               );
               const outside = path.join(base, "outside");
               expect(
                  lstat.mock.calls.some((c) =>
                     String(c[0]).startsWith(outside),
                  ),
               ).toBe(false);
            }
         }
      } finally {
         lstat.mockRestore();
      }
   });

   it("still follows an absolute link that walks down to the package through its own ancestors", async () => {
      fs.symlinkSync(
         path.join(pkg3, "lib", "real.malloy"),
         path.join(pkg3, "abs-inside"),
      );
      expect(await outcome(pkg3, url(path.join(pkg3, "abs-inside")))).toBe(
         "READ:real",
      );
   });
});

describe("linkTextSteps", () => {
   it("does not walk a Windows root's spelling as a component", () => {
      expect(linkTextSteps("C:\\pkg\\lib\\a.malloy", path.win32)).toEqual({
         root: "C:\\",
         parts: ["pkg", "lib", "a.malloy"],
      });
      expect(linkTextSteps("..\\lib\\a.malloy", path.win32)).toEqual({
         root: undefined,
         parts: ["..", "lib", "a.malloy"],
      });
      expect(linkTextSteps("/pkg/lib", path.posix)).toEqual({
         root: "/",
         parts: ["pkg", "lib"],
      });
   });
});

describe("listPackageTree on a directory link that goes out and comes back", () => {
   // pkg/d -> <base>/{present|absent}/../pkg/sub: only `present` exists.
   let base = "";
   let pkg4 = "";
   beforeAll(() => {
      base = fs.realpathSync(
         fs.mkdtempSync(path.join(os.tmpdir(), "tree-out-")),
      );
      pkg4 = path.join(base, "pkg");
      fs.mkdirSync(path.join(pkg4, "sub"), { recursive: true });
      fs.writeFileSync(path.join(pkg4, "sub", "a.malloy"), "x");
      fs.mkdirSync(path.join(base, "present"));
   });
   afterAll(() => fs.rmSync(base, { recursive: true, force: true }));

   it("lists the same files whether the outside directory exists, and never looks at it", async () => {
      const stat = spyOn(fs.promises, "stat");
      const realpath = spyOn(fs.promises, "realpath");
      try {
         const listings: string[] = [];
         for (const form of ["abs", "rel"]) {
            for (const which of ["present", "absent"]) {
               fs.rmSync(path.join(pkg4, "d"), { force: true });
               fs.symlinkSync(
                  form === "abs"
                     ? `${base}/${which}/../pkg/sub`
                     : `../${which}/../pkg/sub`,
                  path.join(pkg4, "d"),
               );
               stat.mockClear();
               realpath.mockClear();
               const listed = (await listPackageTree(pkg4))
                  .map((p) => path.relative(pkg4, p))
                  .sort();
               listings.push(`${form}:${listed.join(",")}`.replace(which, ""));
               const looked = [...stat.mock.calls, ...realpath.mock.calls].map(
                  (c) => String(c[0]),
               );
               expect(
                  looked.some(
                     (p) =>
                        p.startsWith(path.join(base, "present")) ||
                        p.startsWith(path.join(base, "absent")),
                  ),
               ).toBe(false);
            }
         }
         expect(listings[0]).toBe(listings[1]);
         expect(listings[2]).toBe(listings[3]);
         expect(listings[0]).toBe("abs:d,sub/a.malloy");
      } finally {
         stat.mockRestore();
         realpath.mockRestore();
      }
   });
});

describe("a walk that stops at a missing directory", () => {
   it("opens only that point and the last name, never the rest of the climb", async () => {
      const base = fs.realpathSync(
         fs.mkdtempSync(path.join(os.tmpdir(), "stop-race-")),
      );
      try {
         const pkg5 = path.join(base, "pkg");
         fs.mkdirSync(pkg5);
         fs.mkdirSync(path.join(base, "outside"));
         fs.writeFileSync(path.join(base, "outside", "secret"), "s");
         // newdir does not exist yet, so the walk stops there. Spelled
         // through a link: a URL would collapse the `..` itself.
         fs.symlinkSync("newdir/../../outside/secret", path.join(pkg5, "l"));
         const resolved = await confinedPackagePath(
            pkg5,
            url(path.join(pkg5, "l")),
         );
         expect(resolved).toBe(path.join(pkg5, "newdir", "secret"));
         // Were newdir to appear now, the open would stay inside it.
         fs.mkdirSync(path.join(pkg5, "newdir"));
         expect(() => fs.readFileSync(resolved, "utf8")).toThrow("ENOENT");
      } finally {
         fs.rmSync(base, { recursive: true, force: true });
      }
   });
});

describe("a link whose text steps .. past a file, or through a missing name", () => {
   it("fails as the kernel would: ENOTDIR after a file, ENOENT after a missing name", async () => {
      const base = fs.realpathSync(
         fs.mkdtempSync(path.join(os.tmpdir(), "stop-kinds-")),
      );
      try {
         const pkg6 = path.join(base, "pkg");
         fs.mkdirSync(pkg6);
         fs.writeFileSync(path.join(pkg6, "f.md"), "file");
         fs.symlinkSync("f.md/..", path.join(pkg6, "after-file"));
         fs.symlinkSync("missing/..", path.join(pkg6, "after-missing"));
         const afterFile = url(path.join(pkg6, "after-file"));
         const afterMissing = url(path.join(pkg6, "after-missing"));
         expect(await outcome(pkg6, afterFile)).toContain("ENOTDIR");
         expect(() => readPackageURLSync(pkg6, afterFile)).toThrow("ENOTDIR");
         expect(await outcome(pkg6, afterMissing)).toContain("ENOENT");
         expect(() => readPackageURLSync(pkg6, afterMissing)).toThrow("ENOENT");
      } finally {
         fs.rmSync(base, { recursive: true, force: true });
      }
   });
});
